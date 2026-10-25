//! Agent permissions in the plan chat: every tool call the chat makes is
//! asked of the policy gate first, approval cards show in the thread, and a
//! connection the person hasn't made is offered as a sign-in card.
//!
//! - An ask shows an approval card in the chat (an `approval` event). The
//!   model is told to stop and say what it wants to do; once the person
//!   approves, the next message runs the call (approve once leaves a pass
//!   for exactly that call in this chat).
//! - The chat offers `request_sign_in` when the team has connections this
//!   person can't use yet. Calling it shows a sign-in card (a `sign_in`
//!   event) once per connection per chat. After they connect, the dashboard
//!   sends the conversation on by itself, and the connection's tools are
//!   there. "Not now" is never asked again in that chat.

use std::sync::Arc;

use common::agent_policy::gate::Wait;
use common::agent_policy::signin::{self, Need};
use common::agent_policy::{Agent, Gate, GateCtx, Outcome, Request};
use common::mcp_remote::store::{AuthKind, Sharing, Status};
use serde_json::{json, Value};

use crate::AppState;

pub const SIGN_IN_TOOL: &str = "request_sign_in";

/// The gate for one plan chat message.
pub fn gate(
    state: &AppState,
    team: &str,
    email: &str,
    chat_id: &str,
    repo: &str,
    tools: &[Value],
) -> Gate {
    gate_for(state, Agent::PlanChat, team, email, chat_id, repo, tools)
}

/// The gate for one message of a chat agent (the plan chat, the brain chat).
pub fn gate_for(
    state: &AppState,
    agent: Agent,
    team: &str,
    email: &str,
    chat_id: &str,
    repo: &str,
    tools: &[Value],
) -> Gate {
    let thread = if chat_id.is_empty() {
        format!("chat:{}", email.to_ascii_lowercase())
    } else {
        chat_id.to_string()
    };
    Gate::new(
        super::agent_policy::store(state),
        GateCtx::new(team, agent)
            .repo(repo)
            .thread(&thread)
            .person(email)
            .task(if agent == Agent::BrainChat {
                "Answering a question in Chat"
            } else {
                "Planning in the plan chat"
            }),
    )
    .with_wait(Wait::Return)
    .offering(
        tools
            .iter()
            .filter_map(|t| t["name"].as_str().map(str::to_string)),
    )
}

/// What the chat does with one call.
#[derive(Debug, Clone, PartialEq)]
pub enum Checked {
    Run,
    /// Don't run it; the model reads `text`, and the chat shows `event`.
    Answer {
        text: String,
        event: Option<(String, Value)>,
    },
    /// End the reply with this note (agents are paused).
    Stop(String),
}

pub async fn check(gate: &Gate, name: &str, input: &Value) -> Checked {
    match gate
        .check(Request {
            tool: name,
            args: input,
            why: "",
        })
        .await
    {
        Outcome::Run { .. } => Checked::Run,
        Outcome::Refused { message, .. } => Checked::Answer {
            text: message,
            event: None,
        },
        Outcome::Pending { approval, message } => Checked::Answer {
            text: message,
            event: Some(("approval".into(), json!({ "approval": approval }))),
        },
        Outcome::Stop(stop) => Checked::Stop(stop.note()),
    }
}

/// A connection this person can't use in the chat yet.
#[derive(Debug, Clone, PartialEq)]
pub struct Missing {
    pub id: String,
    pub name: String,
    pub need: Need,
}

/// The team's connections with tools agents may use that this person can't
/// use right now: a personal one they haven't signed in to, or a shared one
/// nobody connected (or that needs reconnecting).
pub async fn missing_connections(state: &AppState, team: &str, email: &str) -> Vec<Missing> {
    let rt = super::mcp_servers::runtime(state, team);
    let usable: Vec<String> = rt
        .usable(Some(email))
        .await
        .into_iter()
        .map(|(s, _)| s.id)
        .collect();
    let servers = rt.store.list_servers(team).await.unwrap_or_default();
    servers
        .into_iter()
        .filter(|s| !common::mcp_remote::tools::effective(s).is_empty())
        .filter(|s| !usable.contains(&s.id))
        .filter(|s| s.auth == AuthKind::OAuth || s.status != Status::Connected)
        .map(|s| Missing {
            need: if s.sharing == Sharing::Personal {
                Need::Personal
            } else {
                Need::TeamMissing
            },
            id: s.id,
            name: s.name,
        })
        .collect()
}

/// The tool that raises a sign-in card, when there's something to sign in to.
pub fn sign_in_tool(missing: &[Missing]) -> Option<Value> {
    if missing.is_empty() {
        return None;
    }
    let ids: Vec<Value> = missing.iter().map(|m| json!(m.id)).collect();
    let list = missing
        .iter()
        .map(|m| format!("{} ({})", m.name, m.id))
        .collect::<Vec<_>>()
        .join(", ");
    Some(json!({
        "name": SIGN_IN_TOOL,
        "description": format!(
            "Ask the person to connect one of these, which you can't use yet: {list}. Use it \
             only when the answer really needs that connection (for example, reading a design \
             file or an issue). A sign-in card is shown to the person; stop after calling it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "server": { "type": "string", "enum": ids },
                "purpose": { "type": "string", "description": "What you need it for, as a short verb phrase, e.g. 'read this design file'" }
            },
            "required": ["server", "purpose"]
        }
    }))
}

/// Handles a `request_sign_in` call: a card the first time in this chat,
/// a note after a "not now".
pub async fn sign_in(
    state: &AppState,
    team: &str,
    thread: &str,
    missing: &[Missing],
    is_admin: bool,
    input: &Value,
) -> (String, Option<(String, Value)>) {
    let id = input["server"].as_str().unwrap_or("");
    let purpose = input["purpose"].as_str().unwrap_or("");
    let Some(m) = missing.iter().find(|m| m.id == id) else {
        return (
            "That connection is already available, or doesn't exist. Carry on.".into(),
            None,
        );
    };
    let p = signin::prompt(
        &m.id,
        &m.name,
        m.need,
        is_admin,
        common::truncate_str(purpose, 120),
    );
    let st = super::agent_policy::store(state);
    if signin::declined(&*st, team, thread, &m.id).await {
        return (signin::model_note(&p, true), None);
    }
    if !signin::should_prompt(&*st, team, thread, &m.id).await {
        // Already showing in this chat.
        return (signin::model_note(&p, false), None);
    }
    let note = signin::model_note(&p, false);
    (
        note,
        Some((
            "sign_in".into(),
            json!({ "prompt": p, "thread_id": thread }),
        )),
    )
}

/// Ends a sign-in prompt once the person has connected (the next message
/// finds the connection usable).
pub async fn settle_connected(state: &AppState, team: &str, thread: &str, email: &str) {
    let st: Arc<dyn common::agent_policy::store::PolicyStore> = super::agent_policy::store(state);
    let rt = super::mcp_servers::runtime(state, team);
    for (s, _) in rt.usable(Some(email)).await {
        signin::connected(&*st, team, thread, &s.id).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sign_in_tool_lists_only_missing_connections() {
        assert!(sign_in_tool(&[]).is_none());
        let t = sign_in_tool(&[Missing {
            id: "figma".into(),
            name: "Figma".into(),
            need: Need::Personal,
        }])
        .unwrap();
        assert_eq!(t["name"], SIGN_IN_TOOL);
        assert_eq!(
            t["input_schema"]["properties"]["server"]["enum"][0],
            "figma"
        );
        assert!(t["description"].as_str().unwrap().contains("Figma (figma)"));
    }

    /// Both plan chat loops ask the gate before any tool runs: in each loop,
    /// the first `chat_policy::check(` comes before every way a tool is run.
    #[test]
    fn plan_chat_loops_never_run_a_tool_before_the_gate() {
        let src = [include_str!("plans.rs"), include_str!("brain.rs")].concat();
        for start in [
            "pub async fn plan_chat(",
            "async fn run_anthropic_stream(",
            "async fn answer(",
        ] {
            let body = src.split(start).nth(1).expect("the loop exists");
            let body = &body[..body.find("\n}\n").unwrap_or(body.len())];
            let gate_at = body
                .find("chat_policy::check(")
                .expect("the loop asks the gate");
            for runs in [
                "plan_chat_tools::call(",
                "execute_github_tool(",
                "invoke_mcp_tool(",
            ] {
                for (at, _) in body.match_indices(runs) {
                    assert!(at > gate_at, "{start}: {runs} before the policy gate");
                }
            }
        }
    }

    #[tokio::test]
    async fn chat_asks_become_cards_and_refusals_become_answers() {
        use common::agent_policy::store::{MemStore, PolicyStore};
        use common::agent_policy::TeamPolicy;
        let store = Arc::new(MemStore::new());
        let bare = TeamPolicy {
            version: 1,
            ..Default::default()
        };
        store.save("T", &bare, 0).await.unwrap();
        let g = Gate::new(
            store.clone(),
            GateCtx::new("T", Agent::PlanChat).thread("c1"),
        )
        .with_wait(Wait::Return)
        .offering(["linear__create_issue", "read_file"]);
        let input = json!({"title": "x"});
        match check(&g, "linear__create_issue", &input).await {
            Checked::Answer {
                event: Some((kind, v)),
                text,
            } => {
                assert_eq!(kind, "approval");
                assert_eq!(v["approval"]["thread_id"], "c1");
                assert!(text.contains("approval card"));
            }
            other => panic!("expected a card, got {other:?}"),
        }
        assert_eq!(check(&g, "read_file", &input).await, Checked::Run);
        match check(&g, "not_offered", &input).await {
            Checked::Answer { event: None, text } => assert!(text.starts_with("Not allowed")),
            other => panic!("expected a refusal, got {other:?}"),
        }
        common::agent_policy::pause::pause(&*store, "T", "a", "freeze", None)
            .await
            .unwrap();
        let g = Gate::new(
            store.clone(),
            GateCtx::new("T", Agent::PlanChat).thread("c1"),
        )
        .with_wait(Wait::Return)
        .offering(["read_file"]);
        assert!(matches!(
            check(&g, "read_file", &input).await,
            Checked::Stop(_)
        ));
    }
}
