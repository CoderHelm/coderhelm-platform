//! Plugins: packages a team installs (marketing, support, sales ops) that
//! bring their own instructions, agents, tools, triggers and outputs, run
//! with the same safety CoderHelm applies to code.
//!
//! A plugin is a folder in a git repository with a small manifest
//! ([`manifest`]) next to the Claude Code layout CoderHelm already reads for
//! skills (`skills/`, `agents/`, `commands/`). It is declarative: text plus
//! approved tools. Nothing from a plugin is ever run as code.
//!
//! - [`kv`]: where records live (DynamoDB in production, memory in tests),
//!   always under the team's own key.
//! - [`manifest`]: parsing and checking a manifest, what it grants, and
//!   whether a new version widens that grant.
//! - [`package`]: reading a plugin folder out of a repository tree, and the
//!   content hash a version is pinned by.
//! - [`source`]: the git sources a team adds (public by URL, or private
//!   through the team's GitHub App installation), and its private
//!   marketplace.
//! - [`install`]: installing (an admin, plus a second admin when there is
//!   one), updating (wording applies at once; a widened grant waits for an
//!   admin), pinning, rollback.
//! - [`run`]: one plugin run, with exactly the tools its manifest grants.
//! - [`drafts`]: drafts with versions, and the approval gate bound to the
//!   exact version; stops are sticky.
//! - [`publish`]: running a consequential tool after its approval.
//! - [`schedule`]: one-off and recurring EventBridge schedules.
//! - [`audit`]: the one `AUDIT#` stream every step writes to.
//! - [`limits`]: the per-plugin monthly token limit, next to the team's.
//! - [`signed`]: signed email approval links.
//! - [`chat`]: Slack and Teams cards, and signature checks on what they send.
//! - [`connect`]: the seam to connections (MCP servers, OAuth tokens).
//! - [`brain`]: plugins as a source in the brain chat.
//!
//! Everything sits behind the team's `plugins` flag ([`flag`]), off by
//! default.

pub mod audit;
pub mod brain;
pub mod chat;
pub mod connect;
pub mod drafts;
pub mod flag;
pub mod install;
pub mod kv;
pub mod limits;
pub mod manifest;
pub mod package;
pub mod publish;
#[cfg(test)]
mod release_check;
pub mod run;
pub mod schedule;
pub mod signed;
pub mod source;

use serde::Serialize;

/// Why something was refused, as a stable code the dashboard shows as a
/// plain state, and a sentence for people.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Refusal {
    pub code: &'static str,
    pub error: String,
}

impl Refusal {
    pub fn new(code: &'static str, error: impl Into<String>) -> Self {
        Self {
            code,
            error: error.into(),
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.error, self.code)
    }
}

/// The refusal codes, in one place (the dashboard matches on them).
pub mod code {
    pub const FLAG_OFF: &str = "flag_off";
    pub const FORBIDDEN: &str = "forbidden";
    pub const SAME_PERSON: &str = "same_person";
    pub const NOT_IN_GROUP: &str = "not_in_group";
    pub const STALE_VERSION: &str = "stale_version";
    pub const STOPPED: &str = "stopped";
    pub const LIMIT_REACHED: &str = "limit_reached";
    pub const NEEDS_SECOND_ADMIN: &str = "needs_second_admin";
    pub const NOT_GRANTED: &str = "not_granted";
    pub const PINNED: &str = "pinned";
    pub const NOT_FOUND: &str = "not_found";
    pub const INVALID: &str = "invalid";
    pub const CONFLICT: &str = "conflict";
    pub const WRONG_STATE: &str = "wrong_state";
    pub const STORE: &str = "store";
}

/// Turns a store error into a refusal.
pub fn store_err(e: impl std::fmt::Display) -> Refusal {
    Refusal::new(
        code::STORE,
        format!("Couldn't read or save plugin data: {e}"),
    )
}

/// A plugin id: lowercase letters, digits and dashes, optionally under a
/// namespace (`acme-marketing`). Ids name settings keys, schedules and usage
/// counters, so nothing else is accepted.
pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !id.starts_with('-')
        && !id.ends_with('-')
}

/// The plugin id for a manifest's name and namespace: `namespace-name`.
pub fn plugin_id(namespace: Option<&str>, name: &str) -> String {
    match namespace {
        Some(ns) if !ns.is_empty() => format!("{ns}-{name}"),
        _ => name.to_string(),
    }
}

/// Now, as RFC 3339 (UTC).
pub fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// A new sortable id.
pub fn new_id() -> String {
    ulid::Ulid::new().to_string().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids() {
        assert!(valid_id("marketing"));
        assert!(valid_id("acme-marketing"));
        assert!(!valid_id(""));
        assert!(!valid_id("Marketing"));
        assert!(!valid_id("a/b"));
        assert!(!valid_id("-a"));
        assert!(!valid_id(&"a".repeat(65)));
        assert_eq!(plugin_id(Some("acme"), "marketing"), "acme-marketing");
        assert_eq!(plugin_id(None, "marketing"), "marketing");
    }
}
