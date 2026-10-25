//! Just-in-time sign-in prompts: when an agent needs a connection the
//! person hasn't made, it says so once, with a way to connect.
//!
//! - A server that signs each person in (Figma, Linear, Sanity…) that this
//!   person hasn't connected: "Sign in to Figma to let me read this file",
//!   with a Connect button that runs the server's own OAuth sign-in in a
//!   popup. The chat carries on by itself once they're connected.
//! - A team connection nobody has set up: an admin gets a link straight to
//!   it; anyone else is told to ask an admin.
//!
//! A prompt is raised once per connection per thread (a chat, or a run).
//! Someone who says "not now" is never asked again in that thread.

use serde::{Deserialize, Serialize};

use super::audit::{self, ActorKind, Event};
use super::store::PolicyStore;

/// Why a connection can't be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Need {
    /// Each person signs in; this one hasn't.
    Personal,
    /// The team shares one connection and it isn't set up (or needs
    /// reconnecting).
    TeamMissing,
}

/// Where a connection stands in one thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadState {
    /// Prompted, not answered yet.
    Asked,
    /// The person said not now.
    Declined,
}

/// The prompt a chat card, a run page, Slack or Teams shows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prompt {
    pub server_id: String,
    pub server_name: String,
    pub need: Need,
    /// "Sign in to Figma to let me read this file."
    pub message: String,
    /// Shown as the button: "Connect", "Open settings", or nothing.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub action_label: String,
    /// For a personal sign-in, the API path that starts it
    /// (`/api/mcp-servers/<id>/connect`); for an admin, the dashboard page
    /// where the team connection is set up.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub action_path: String,
    /// The dashboard page to open from Slack, Teams or a run.
    pub link_path: String,
    /// True when the person can't fix it themselves (ask an admin).
    #[serde(default)]
    pub ask_admin: bool,
}

fn purpose_clause(purpose: &str) -> String {
    let p = purpose.trim().trim_end_matches('.');
    if p.is_empty() {
        "so I can use it".into()
    } else {
        format!("to let me {p}")
    }
}

/// The prompt for one connection. `purpose` is what the agent wanted it
/// for, as a verb phrase ("read this file").
pub fn prompt(
    server_id: &str,
    server_name: &str,
    need: Need,
    is_admin: bool,
    purpose: &str,
) -> Prompt {
    let name = if server_name.trim().is_empty() {
        server_id
    } else {
        server_name.trim()
    };
    let settings = "/settings/integrations/#mcp-servers".to_string();
    match need {
        Need::Personal => Prompt {
            server_id: server_id.into(),
            server_name: name.into(),
            need,
            message: format!("Sign in to {name} {}.", purpose_clause(purpose)),
            action_label: "Connect".into(),
            action_path: format!("/api/mcp-servers/{}/connect", urlencoding::encode(server_id)),
            link_path: settings,
            ask_admin: false,
        },
        Need::TeamMissing if is_admin => Prompt {
            server_id: server_id.into(),
            server_name: name.into(),
            need,
            message: format!(
                "{name} isn't connected for the team yet. Connect it {}.",
                purpose_clause(purpose)
            ),
            action_label: "Open settings".into(),
            action_path: settings.clone(),
            link_path: settings,
            ask_admin: false,
        },
        Need::TeamMissing => Prompt {
            server_id: server_id.into(),
            server_name: name.into(),
            need,
            message: format!(
                "{name} isn't connected for the team yet. Ask an admin to connect it, then try again."
            ),
            action_label: String::new(),
            action_path: String::new(),
            link_path: settings,
            ask_admin: true,
        },
    }
}

/// Should this thread be prompted about this connection now? Records that
/// it was, so the next call in the thread isn't prompted again. False when
/// already prompted or declined in this thread.
pub async fn should_prompt(
    store: &dyn PolicyStore,
    team: &str,
    thread: &str,
    server_id: &str,
) -> bool {
    if thread.is_empty() {
        return false;
    }
    match store.get_sign_in(team, thread, server_id).await {
        Ok(Some(_)) => false,
        Ok(None) => {
            let first = store
                .put_sign_in(team, thread, server_id, ThreadState::Asked, true)
                .await
                .unwrap_or(false);
            if first {
                store
                    .audit(
                        team,
                        Event::new(
                            "system",
                            ActorKind::System,
                            audit::action::SIGN_IN_ASKED,
                            "waiting",
                        )
                        .detail(format!("{server_id} in {thread}")),
                    )
                    .await;
            }
            first
        }
        Err(_) => false,
    }
}

/// The person said not now: never ask again in this thread.
pub async fn decline(store: &dyn PolicyStore, team: &str, thread: &str, server_id: &str, by: &str) {
    let _ = store
        .put_sign_in(team, thread, server_id, ThreadState::Declined, false)
        .await;
    store
        .audit(
            team,
            Event::new(
                by,
                ActorKind::Person,
                audit::action::SIGN_IN_DECLINED,
                "refused",
            )
            .detail(format!("{server_id} in {thread}")),
        )
        .await;
}

/// Declined in this thread?
pub async fn declined(store: &dyn PolicyStore, team: &str, thread: &str, server_id: &str) -> bool {
    matches!(
        store.get_sign_in(team, thread, server_id).await,
        Ok(Some(ThreadState::Declined))
    )
}

/// The person connected: the thread's prompt is done, so if the connection
/// later breaks they're told again.
pub async fn connected(store: &dyn PolicyStore, team: &str, thread: &str, server_id: &str) {
    if let Ok(Some(ThreadState::Asked)) = store.get_sign_in(team, thread, server_id).await {
        let _ = store.clear_sign_in(team, thread, server_id).await;
    }
}

/// What the model reads instead of the tool's answer.
pub fn model_note(p: &Prompt, declined: bool) -> String {
    if declined {
        format!(
            "The person chose not to connect {} in this conversation. Carry on without it and \
             don't ask again.",
            p.server_name
        )
    } else if p.ask_admin {
        format!(
            "{} isn't connected for the team. The person was told to ask an admin. Carry on \
             without it and say what you couldn't check.",
            p.server_name
        )
    } else {
        format!(
            "{} needs the person to sign in first. A sign-in card is showing. Stop here and tell \
             them in one short sentence that you'll continue once they connect.",
            p.server_name
        )
    }
}
