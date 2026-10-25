//! Agent permissions: one policy engine every agent loop asks before every
//! call it makes.
//!
//! Each tool call, MCP tool, data source query, sandbox command and network
//! request gets one decision, **allow**, **ask** or **deny**, and the record
//! of what decided it.
//!
//! - [`model`]: the words (decisions, risks, agents, scopes, rules,
//!   switches, the pause, the sandbox lists, the team's policy record).
//! - [`engine`]: the pure decision, and the explanation the settings page's
//!   "test a call" box shows.
//! - [`registry`]: a tool's risk. A stand-in until the plugins work's
//!   `common::tool_registry` is merged; then the registry implements
//!   [`registry::ToolRisk`].
//! - [`sandbox`]: command and network allowlists, and suggestions read from
//!   a repository that an admin accepts.
//! - [`gate`]: what a loop calls: decide, record, raise approval cards, wait.
//! - [`approvals`]: the cards and a person's answer (once, always for this
//!   repository, always for this tool).
//! - [`signin`]: just-in-time sign-in prompts, once per connection per thread.
//! - [`pause`]: "Pause all agents", with work kept and resumed.
//! - [`links`]: signed approval links for Slack, Teams and email.
//! - [`audit`]: decision events in the shape of the plugins `AUDIT#` stream.
//! - [`store`]: where it is all kept.
//!
//! Precedence, in one paragraph: a pause stops everything; a switched-off
//! agent is denied; an unknown tool is denied; a locked team rule is final;
//! otherwise the narrowest scope with a matching rule decides (agent in a
//! repository, agent, repository, team) and within it deny beats ask beats
//! allow; with no rule, reads run and everything else asks; and the team's
//! autonomy cap is a ceiling no rule can go above.

pub mod approvals;
pub mod audit;
pub mod engine;
pub mod gate;
pub mod links;
pub mod model;
pub mod notify;
pub mod pause;
pub mod presets;
pub mod registry;
pub mod sandbox;
pub mod signin;
pub mod store;

#[cfg(test)]
mod tests;

pub use gate::{AgentStopped, Gate, GateCtx, Outcome, Request, Stop, Wait};
pub use model::{
    Agent, Autonomy, Call, CallKind, Decision, Risk, Rule, Scope, TeamPolicy, Verdict,
};

/// Now, as the RFC 3339 text every record here uses.
pub fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A new id that sorts after every id made before it in this process (ULIDs
/// made in the same millisecond otherwise sort at random).
pub fn next_id() -> String {
    static GEN: std::sync::Mutex<Option<ulid::Generator>> = std::sync::Mutex::new(None);
    let mut g = GEN.lock().unwrap_or_else(|e| e.into_inner());
    g.get_or_insert_with(ulid::Generator::new)
        .generate()
        .unwrap_or_else(|_| ulid::Ulid::new())
        .to_string()
}

/// Where the dashboard lives (links in Slack, Teams and email).
pub const DASHBOARD_URL: &str = "https://app.coderhelm.com";
/// Where the gateway answers signed approval links.
pub const API_URL: &str = "https://api.coderhelm.com";
