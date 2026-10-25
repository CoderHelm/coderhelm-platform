//! What each team role can do — the single source of truth the dashboard's
//! team page shows (served by `GET /api/users/permissions`).
//!
//! Every capability names the route handlers it covers and the lowest role
//! that may call them. The test at the bottom reads every routed handler's
//! role check out of the source and fails when one is missing here or when its
//! required role differs, so this table cannot drift from what the gateway
//! actually enforces.

use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

/// Numeric role level: owner=4, admin=3, billing=2, member=1, viewer=0.
/// Unknown roles get the lowest level.
pub fn role_level(role: &str) -> u8 {
    match role {
        "owner" => 4,
        "admin" => 3,
        "billing" => 2,
        "member" => 1,
        _ => 0,
    }
}

/// Roles the gateway accepts on a record. `billing` is kept for older records;
/// nothing requires it, so it has exactly a member's access and is not offered.
pub const VALID_ROLES: &[&str] = &["owner", "admin", "billing", "member", "viewer"];

#[derive(Serialize, Clone, Copy)]
pub struct RoleInfo {
    pub key: &'static str,
    pub label: &'static str,
    pub level: u8,
    pub summary: &'static str,
    /// Offered in the invite form and role selectors.
    pub assignable: bool,
}

pub const ROLES: &[RoleInfo] = &[
    RoleInfo {
        key: "viewer",
        label: "Viewer",
        level: 0,
        summary: "Read-only: sees runs, plans, reviews and settings, changes nothing.",
        assignable: true,
    },
    RoleInfo {
        key: "member",
        label: "Member",
        level: 1,
        summary: "Works with Coderhelm: plans, runs, reviews and recommendations.",
        assignable: true,
    },
    RoleInfo {
        key: "billing",
        label: "Billing",
        level: 2,
        summary: "Older role with the same access as Member.",
        assignable: false,
    },
    RoleInfo {
        key: "admin",
        label: "Admin",
        level: 3,
        summary:
            "Runs the workspace: repos, integrations, agent settings, the token limit and people.",
        assignable: true,
    },
    RoleInfo {
        key: "owner",
        label: "Owner",
        level: 4,
        summary: "Everything, plus account-level changes and managing admins.",
        assignable: true,
    },
];

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capability {
    pub key: &'static str,
    pub label: &'static str,
    /// Lowest role allowed (a key of `ROLES`).
    pub min_role: &'static str,
    /// Route handlers (`file::function`) whose role check this row describes.
    #[serde(skip)]
    pub handlers: &'static [&'static str],
}

/// In ascending order of the role they need.
pub const CAPABILITIES: &[Capability] = &[
    // ── Everyone on the team (viewer and up) ────────────────────────
    Capability {
        key: "view",
        label: "See and search runs, plans, reviews, analytics, repos, tests and settings",
        min_role: "viewer",
        handlers: &[
            "api::list_runs",
            "search::search",
            "api::get_run",
            "api::get_run_traces",
            "api::get_agent_log",
            "api::list_repos",
            "api::get_stats",
            "analytics::get_analytics",
            "plans::list_plans",
            "plans::get_plan",
            "reviewer::list_reviews",
            "reviewer::get_review",
            "reviewer::open_prs",
            "review_lessons::list_lessons",
            "learning::get_learning",
            "tests::summary",
            "tests::list_tests",
            "api::get_usage",
            "tests_setup::status",
        ],
    },
    Capability {
        key: "brain_read",
        label: "Read knowledge open to their role and chats shared with the team",
        min_role: "viewer",
        handlers: &[
            "knowledge::list_bases",
            "knowledge::get_base",
            "brain::list_threads",
            "brain::get_thread",
            "brain::sources",
            "brain::starters",
        ],
    },
    Capability {
        key: "view_team",
        label: "See who is on the team",
        min_role: "viewer",
        handlers: &["users::list_users", "roles::list_roles"],
    },
    Capability {
        key: "own_account",
        label: "Manage your own password, two-factor and notifications",
        min_role: "viewer",
        handlers: &[
            "users::change_password",
            "users::mfa_setup",
            "users::mfa_verify_setup",
            "users::mfa_disable",
            "api::update_notification_prefs",
        ],
    },
    // ── Members ─────────────────────────────────────────────────────
    Capability {
        key: "brain_ask",
        label: "Ask questions in Chat, attach files, rate answers, and rename, share or delete their own chats",
        min_role: "member",
        handlers: &[
            "brain::stream_token",
            "brain::create_attachment",
            "brain::update_thread",
            "brain::delete_thread",
            "brain::rate_message",
        ],
    },
    Capability {
        key: "plans",
        label: "Create, edit, approve and run plans and templates",
        min_role: "member",
        handlers: &[
            "plans::create_plan",
            "plans::update_plan",
            "plans::add_task",
            "plans::update_task",
            "plans::delete_task",
            "plans::approve_task",
            "plans::reject_task",
            "plans::approve_all_and_execute",
            "plans::force_run_task",
            "plans::execute_plan",
            "plans::generate_plan_openspec",
            "plans::plan_chat",
            "plans::stream_token",
            "plans::create_template",
            "plans::create_template_from_plan",
            "plans::delete_template",
            "plans::use_template",
        ],
    },
    Capability {
        key: "mcp_connect",
        label: "Connect your own account to MCP servers that use personal sign-in",
        min_role: "member",
        handlers: &[
            "mcp_servers::connect",
            "mcp_servers::oauth_callback",
            "mcp_servers::disconnect",
            "agent_policy::decline_sign_in",
        ],
    },
    Capability {
        key: "runs",
        label: "Retry, re-run, re-review, cancel, close or reopen runs",
        min_role: "member",
        handlers: &[
            "api::retry_run",
            "api::reset_and_rerun",
            "api::re_review_run",
            "api::cancel_run",
            "run_close::close_run",
            "run_close::close_runs",
            "run_close::reopen_run",
        ],
    },
    Capability {
        key: "reviews",
        label: "Ask for a re-review, rate reviews, send release notes and release now",
        min_role: "member",
        handlers: &[
            "reviewer::re_review",
            "reviewer::rate_review",
            "reviewer::send_release",
            "reviewer::release_now",
            "reviewer::control_gate",
        ],
    },
    Capability {
        key: "recommendations",
        label: "Act on log findings: create fix PRs or plans, read sample lines, acknowledge or ignore them",
        min_role: "member",
        handlers: &[
            "log_analyzer::create_plan_from_recommendation",
            "log_analyzer::dismiss_recommendation",
            "log_findings::get_samples",
            "log_findings::create_fix",
            "log_findings::ignore_finding",
            "log_findings::acknowledge_finding",
            "log_findings::reopen_finding",
        ],
    },
    Capability {
        key: "agents.approve",
        label: "Approve or deny what agents ask to do (once)",
        min_role: "member",
        handlers: &["agent_policy::decide"],
    },
    Capability {
        key: "view_alerts",
        label: "See alerts and where they are routed",
        min_role: "member",
        handlers: &[
            "alerts::list_routes",
            "alerts::list_topic_alarms",
            "alerts::list_events",
            "alerts::get_event",
        ],
    },
    Capability {
        key: "view_integrations",
        label: "See integrations, what each can do and whether it works",
        min_role: "member",
        handlers: &["integrations::list"],
    },
    // ── Admins ──────────────────────────────────────────────────────
    Capability {
        key: "knowledge_manage",
        label: "Create, change and delete knowledge bases, upload and remove their files, and attach them to agents",
        min_role: "admin",
        handlers: &[
            "knowledge::create_base",
            "knowledge::update_base",
            "knowledge::delete_base",
            "knowledge::start_upload",
            "knowledge::finish_upload",
            "knowledge::delete_file",
        ],
    },
    Capability {
        key: "people",
        label: "Invite people, resend or revoke any pending invite, link GitHub accounts, and change or remove members and viewers",
        min_role: "admin",
        handlers: &[
            "users::invite_user",
            "users::resend_invite",
            "users::update_role",
            "users::remove_user",
            "users::link_github",
        ],
    },
    Capability {
        key: "roles",
        label: "Create, edit and delete custom roles, give people a custom role, and see the log of role changes",
        min_role: "admin",
        // Each of these also checks for a built-in admin or owner itself
        // (`roles::role_manager`): a custom role never manages roles.
        handlers: &[
            "roles::create_role",
            "roles::update_role",
            "roles::delete_role",
            "roles::list_audit",
        ],
    },
    Capability {
        key: "api_keys",
        label: "Create and revoke the team's API keys, each with scopes no wider than your own",
        min_role: "admin",
        handlers: &["api_keys::list", "api_keys::create", "api_keys::revoke"],
    },
    Capability {
        key: "delete_plans",
        label: "Delete plans",
        min_role: "admin",
        handlers: &["plans::delete_plan"],
    },
    Capability {
        key: "repos",
        label: "Manage repositories: sync, turn on, roles, code graph (turn off, rebuild) and infrastructure scans",
        min_role: "admin",
        handlers: &[
            "api::sync_repos",
            "api::set_repo_role",
            "api::update_repo",
            "api::delete_repo",
            "api::regenerate_repo",
            "graph::update_config",
            "graph::rebuild",
            "infrastructure::refresh_infrastructure",
            "infrastructure::refresh_repo_infrastructure",
        ],
    },
    Capability {
        key: "agent_settings",
        label: "Edit instructions, rules, voice, agent context, workflow, git conventions and reviewer settings, and remove what the reviewer learned",
        min_role: "admin",
        handlers: &[
            "api::update_global_instructions",
            "api::update_repo_instructions",
            "api::update_global_rules",
            "api::update_repo_rules",
            "api::update_global_voice",
            "api::update_repo_voice",
            "api::update_global_agents",
            "api::update_repo_agents",
            "api::update_workflow_settings",
            "api::update_git_conventions",
            "reviewer::update_org_config",
            "reviewer::update_config",
            "review_lessons::delete_lesson",
        ],
    },
    Capability {
        key: "agents.permissions",
        label: "Edit agent permissions: rules, presets, the autonomy limit, agent switches, pause all agents, sandbox allowlists, and approve \"always\"",
        min_role: "admin",
        handlers: &[
            "agent_policy::save_settings",
            "agent_policy::add_rule",
            "agent_policy::remove_rule",
            "agent_policy::apply_preset",
            "agent_policy::set_switch",
            "agent_policy::set_commands",
            "agent_policy::decide_suggestion",
            "agent_policy::pause_all",
            "agent_policy::resume_all",
        ],
    },
    Capability {
        key: "learning",
        label: "Pin a prompt variant, revert or restore what Coderhelm learned, and tune the reward weights",
        min_role: "admin",
        handlers: &[
            "learning::pin_variant",
            "learning::revert_change",
            "learning::restore_change",
            "learning::put_weights",
        ],
    },
    Capability {
        key: "memory",
        label: "Add, edit and delete team memory",
        min_role: "admin",
        handlers: &[
            "memory::create_memory",
            "memory::update_memory",
            "memory::delete_memory",
        ],
    },
    Capability {
        key: "integrations",
        label: "Connect GitHub, Jira, AWS accounts, monitoring and docs tools, Teams and MCP servers (and the tools agents get from each), and choose what each may do",
        min_role: "admin",
        handlers: &[
            "github::link_installation",
            "api::get_jira_integration_check",
            "api::generate_jira_secret",
            "api::delete_jira_secret",
            "api::update_jira_config",
            "api::update_jira_projects",
            "log_analyzer::create_connection",
            "log_analyzer::update_connection",
            "log_analyzer::delete_connection",
            "log_analyzer::test_connection",
            "log_analyzer::get_cfn_url",
            "log_analyzer::get_rec_notify",
            "log_analyzer::put_rec_notify",
            "plugins::enable_plugin",
            "plugins::disable_plugin",
            "plugins::update_credentials",
            "plugins::update_prompt",
            "plugins::test_connection",
            "mcp_servers::detect_auth",
            "mcp_servers::create",
            "mcp_servers::remove",
            "mcp_servers::set_client",
            "mcp_servers::set_key",
            "mcp_servers::set_tools",
            "mcp_servers::approve",
            "mcp_servers::check",
            "mcp_servers::set_prompt",
            "mcp_servers::activity",
            "integrations::check_aws",
            "integrations::set_aws_abilities",
        ],
    },
    Capability {
        key: "data_sources",
        label: "Manage data sources, team tools and skills",
        min_role: "admin",
        handlers: &[
            "data_sources::list",
            "data_sources::activity",
            "data_sources::probe",
            "data_sources::check",
            "data_sources::create",
            "data_sources::create_rest",
            "data_sources::create_search",
            "data_sources::update",
            "data_sources::remove",
            "data_sources::replace_key",
            "data_sources::test",
            "data_sources::samples",
            "data_sources::try_lookup",
            "data_sources::hidden_suggestions",
            "data_sources::decide_hidden_suggestions",
            "data_sources::aws_setup",
            "data_sources::aws_connect",
            "data_sources::create_logs",
            "data_sources::create_database",
            "connectors::list",
            "connectors::save",
            "connectors::test",
            "connectors::remove",
            "connectors::aws_check",
            "tools::list",
            "tools::check",
            "tools::create",
            "tools::get",
            "tools::approve",
            "tools::update",
            "tools::remove",
            "tools::preview",
            "tools::asks",
            "tools::decide",
            "skills::connect",
            "skills::disconnect",
            "skills::sync",
            "skills::set_enabled",
            "skills::try_agent",
            "skills::recheck",
            "skills::set_use_translated",
            "skills::set_always_apply",
            "skills::set_reviews",
        ],
    },
    Capability {
        key: "tests",
        label: "Change test settings; quarantine, pin and fix tests; open setup pull requests",
        min_role: "admin",
        handlers: &[
            "tests::put_settings",
            "tests_setup::preview",
            "tests_setup::open_pr",
            "tests::quarantine",
            "tests::pin",
            "tests::fix",
            "tests::accept_recommendation",
            "tests::dismiss_recommendation",
        ],
    },
    Capability {
        key: "alerts",
        label: "Configure alert routes, the alert budget, alert notifications and automatic fixes for log findings",
        min_role: "admin",
        handlers: &[
            "log_findings::put_autofix",
            "alerts::put_route",
            "alerts::delete_route",
            "alerts::get_notify",
            "alerts::put_notify",
            "alerts::get_budget",
            "alerts::put_budget",
            "alerts::test_notify",
        ],
    },
    Capability {
        key: "model_provider",
        label: "See and change the model provider key",
        min_role: "admin",
        handlers: &["api::get_model_provider", "api::update_model_provider"],
    },
    Capability {
        key: "system",
        label: "See system health and retry or clear failed events",
        min_role: "admin",
        handlers: &[
            "api::health",
            "dlq::list_dlq_messages",
            "dlq::redrive_dlq_message",
            "dlq::delete_dlq_message",
            "dlq::purge_dlq",
        ],
    },
    Capability {
        key: "budget",
        label: "Set the monthly token limit",
        min_role: "admin",
        handlers: &["api::update_budget"],
    },
    Capability {
        key: "support",
        label: "Let Coderhelm support into the workspace, and manage every support ticket the team files",
        min_role: "admin",
        // Enforced inside the support handlers by the built-in role, so it
        // stays with built-in admins and owners (`NEVER_IN_CUSTOM_ROLES`).
        handlers: &[],
    },
    Capability {
        key: "system",
        label: "See this install's version, update it or roll it back, and send a test email (self-hosted installs; install admins only)",
        min_role: "admin",
        // Also checked inside each handler: the person must be an install
        // admin (PLATFORM_ADMIN_EMAILS).
        handlers: &[
            "system::version",
            "system::set_auto_update",
            "system::update_now",
            "system::rollback",
            "system::test_email",
        ],
    },
    // ── Owners ──────────────────────────────────────────────────────
    Capability {
        key: "manage_admins",
        label: "Make someone an admin or owner, and change or remove admins and owners",
        min_role: "owner",
        // Enforced inside the people handlers by `can_manage` / `can_grant`.
        handlers: &[],
    },
    Capability {
        key: "team_name",
        label: "Rename the team",
        min_role: "owner",
        handlers: &["api::rename_team"],
    },
    Capability {
        key: "disconnect",
        label: "Remove the Jira integration or the model provider key",
        min_role: "owner",
        handlers: &["api::delete_jira_integration", "api::delete_model_provider"],
    },
    Capability {
        key: "reset",
        label: "Reset account data",
        min_role: "owner",
        handlers: &["api::reset_account"],
    },
];

/// May `actor_role` change the role of, or remove, someone whose role is
/// `target_role`? Admins manage viewers, members (and billing); only owners
/// manage admins and owners.
pub fn can_manage(actor_role: &str, target_role: &str) -> bool {
    let actor = role_level(actor_role);
    actor >= 4 || (actor >= 3 && role_level(target_role) <= 2)
}

/// May `actor_role` give someone `new_role`? Same split as `can_manage`.
pub fn can_grant(actor_role: &str, new_role: &str) -> bool {
    VALID_ROLES.contains(&new_role) && can_manage(actor_role, new_role)
}

/// Roles `actor_role` may hand out from the selectors (assignable ones only).
pub fn grantable_roles(actor_role: &str) -> Vec<&'static str> {
    ROLES
        .iter()
        .filter(|r| r.assignable && can_grant(actor_role, r.key))
        .map(|r| r.key)
        .collect()
}

// ── Custom roles ────────────────────────────────────────────────────
//
// A team's admins can make their own roles: a name, a description and a set
// of capabilities picked from `CAPABILITIES`. A member's record then holds
// `custom:<id>` as its role. Every request to the protected API goes through
// `resolve_access` (called once, from the auth middleware), which decides from
// the capability the routed handler belongs to. Built-in roles get the same
// decision their handler's own check makes; a custom role is let through only
// to handlers whose capability it holds, and the handler then sees the lowest
// built-in role that capability needs.

/// The role a member record holds when it has a custom role.
pub const CUSTOM_PREFIX: &str = "custom:";

/// Capabilities a custom role can never hold, whatever the table says about
/// them: managing roles, and support (checked by built-in role inside the
/// support handlers), stay with built-in admins and owners.
pub const NEVER_IN_CUSTOM_ROLES: &[&str] = &["roles", "support", "system"];

/// The id in a `custom:<id>` role, or `None` for a built-in role.
pub fn custom_role_id(role: &str) -> Option<&str> {
    role.strip_prefix(CUSTOM_PREFIX).filter(|id| !id.is_empty())
}

pub fn capability(key: &str) -> Option<&'static Capability> {
    CAPABILITIES.iter().find(|c| c.key == key)
}

/// Everyone on the team has these (seeing the workspace and the team, and
/// managing their own account); a custom role always includes them.
pub fn is_baseline(cap: &Capability) -> bool {
    role_level(cap.min_role) == 0
}

/// May this capability be picked for a custom role? Not the owner-only ones
/// (account-level changes, managing admins and owners) and not role
/// management. A capability added to the table later is customizable by
/// default, so it shows up in the role editor without other changes.
pub fn customizable(cap: &Capability) -> bool {
    role_level(cap.min_role) < role_level("owner") && !NEVER_IN_CUSTOM_ROLES.contains(&cap.key)
}

/// What a built-in role holds: every capability at or below its level.
pub fn builtin_capabilities(role: &str) -> BTreeSet<&'static str> {
    let level = role_level(role);
    CAPABILITIES
        .iter()
        .filter(|c| role_level(c.min_role) <= level)
        .map(|c| c.key)
        .collect()
}

/// What a stored custom role holds: the keys it lists that are still in the
/// table and still customizable, plus the baseline. A key that was dropped
/// from the table or made owner-only since is ignored, never trusted.
pub fn custom_capabilities<'a>(
    stored: impl IntoIterator<Item = &'a str>,
) -> BTreeSet<&'static str> {
    let mut out: BTreeSet<&'static str> = CAPABILITIES
        .iter()
        .filter(|c| is_baseline(c))
        .map(|c| c.key)
        .collect();
    for key in stored {
        if let Some(cap) = capability(key).filter(|c| customizable(c)) {
            out.insert(cap.key);
        }
    }
    out
}

/// The highest built-in role whose capabilities are all in `caps`. A handler
/// outside the table (or in a baseline capability) runs as this role, so
/// whatever it does by role never goes past what the holder has.
pub fn floor_role(caps: &BTreeSet<&'static str>) -> &'static str {
    ["owner", "admin", "member"]
        .into_iter()
        .find(|r| builtin_capabilities(r).is_subset(caps))
        .unwrap_or("viewer")
}

/// The capability a routed handler (`file::function`) belongs to.
pub fn handler_capability(handler: &str) -> Option<&'static Capability> {
    CAPABILITIES.iter().find(|c| c.handlers.contains(&handler))
}

/// What someone holds: their stored role and the capabilities it gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    /// The role on their record: a built-in key or `custom:<id>`.
    pub role: String,
    pub caps: BTreeSet<&'static str>,
    /// Set for a custom role: its id and name.
    pub custom: Option<CustomRoleRef>,
}

/// A custom role, by id and name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct CustomRoleRef {
    /// The role's id.
    #[schema(example = "01JB7Z2R4T6V8X0Z2B4D6F8H0K")]
    pub id: String,
    /// The role's name, as the team page shows it.
    #[schema(example = "Release manager")]
    pub name: String,
}

impl Holder {
    pub fn builtin(role: &str) -> Self {
        Self {
            role: role.to_string(),
            caps: builtin_capabilities(role),
            custom: None,
        }
    }

    /// A custom role. `stored` is `None` when the role no longer exists:
    /// then only the baseline is held.
    pub fn custom(id: &str, stored: Option<(&str, &[String])>) -> Self {
        let (name, caps) = match stored {
            Some((name, caps)) => (name, custom_capabilities(caps.iter().map(String::as_str))),
            None => ("Removed role", custom_capabilities([])),
        };
        Self {
            role: format!("{CUSTOM_PREFIX}{id}"),
            caps,
            custom: Some(CustomRoleRef {
                id: id.to_string(),
                name: name.to_string(),
            }),
        }
    }

    pub fn floor(&self) -> &'static str {
        match &self.custom {
            None if VALID_ROLES.contains(&self.role.as_str()) => ROLES
                .iter()
                .find(|r| r.key == self.role)
                .map_or("viewer", |r| r.key),
            None => "viewer",
            Some(_) => floor_role(&self.caps),
        }
    }

    pub fn holds(&self, cap: &str) -> bool {
        self.caps.contains(cap)
    }

    /// Built-in admins and owners only. Custom roles never manage roles.
    pub fn manages_roles(&self) -> bool {
        self.custom.is_none() && role_level(&self.role) >= role_level("admin")
    }
}

/// The decision for one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Go on. For a custom role the handler runs as `effective_role`.
    Allow { effective_role: &'static str },
    /// The holder lacks the capability this handler belongs to.
    Deny { capability: &'static Capability },
}

/// The one place a request is decided. `handler` is the routed handler
/// (`file::function`), `None` when the route is not known.
///
/// A handler in no capability, or in a baseline one, is allowed and runs as
/// the holder's floor role (its own checks apply on top). A handler in a
/// capability the holder has is allowed and runs as the higher of the floor
/// and the capability's role; anything else is refused.
pub fn resolve_access(holder: &Holder, handler: Option<&str>) -> Access {
    let floor = holder.floor();
    let Some(cap) = handler.and_then(handler_capability) else {
        return Access::Allow {
            effective_role: floor,
        };
    };
    if !holder.holds(cap.key) {
        return Access::Deny { capability: cap };
    }
    let need = ROLES
        .iter()
        .find(|r| r.key == cap.min_role)
        .map_or("viewer", |r| r.key);
    Access::Allow {
        effective_role: if role_level(need) > role_level(floor) {
            need
        } else {
            floor
        },
    }
}

/// Why a set of capabilities can't be a custom role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleCapError {
    Unknown(String),
    /// Owner-only or role management.
    NotAllowed(&'static Capability),
    /// The person making the role doesn't hold it.
    MoreThanYouHave(&'static Capability),
}

impl RoleCapError {
    pub fn message(&self) -> String {
        match self {
            Self::Unknown(k) => format!("\"{k}\" isn't something a role can do."),
            Self::NotAllowed(c) if role_level(c.min_role) >= role_level("owner") => format!(
                "\"{}\" stays with owners and can't go into a custom role.",
                c.label
            ),
            Self::NotAllowed(c) => format!(
                "\"{}\" stays with admins and owners and can't go into a custom role.",
                c.label
            ),
            Self::MoreThanYouHave(c) => format!(
                "You can't give a role \"{}\", because your own role doesn't include it.",
                c.label
            ),
        }
    }
}

/// The capabilities a new or edited custom role will hold, checked: every key
/// is in the table, customizable, and held by `actor`. The baseline is added.
pub fn checked_role_capabilities(
    actor: &Holder,
    requested: &[String],
) -> Result<BTreeSet<&'static str>, RoleCapError> {
    for key in requested {
        let cap = capability(key).ok_or_else(|| RoleCapError::Unknown(key.clone()))?;
        if !customizable(cap) {
            return Err(RoleCapError::NotAllowed(cap));
        }
        if !actor.holds(cap.key) {
            return Err(RoleCapError::MoreThanYouHave(cap));
        }
    }
    Ok(custom_capabilities(requested.iter().map(String::as_str)))
}

/// May `actor` give someone a role holding `target_caps`? Only when the actor
/// holds all of it: nobody hands out more than they have.
pub fn within(actor: &Holder, target_caps: &BTreeSet<&'static str>) -> bool {
    target_caps.is_subset(&actor.caps)
}

/// A built-in role a custom role can start from: the role's capabilities
/// that a custom role may hold.
pub fn template_capabilities(role: &str) -> Option<Vec<&'static str>> {
    ROLES.iter().find(|r| r.key == role && r.assignable)?;
    Some(
        CAPABILITIES
            .iter()
            .filter(|c| role_level(c.min_role) <= role_level(role) && customizable(c))
            .map(|c| c.key)
            .collect(),
    )
}

// ── Routed handlers ─────────────────────────────────────────────────

/// `(METHOD, path)` → `file::function` for every route of the protected API
/// router, read from `main.rs` once. Paths are as written there (without the
/// `/api` prefix the router is nested under).
pub fn api_route_handlers() -> &'static HashMap<(String, String), String> {
    static ROUTES: OnceLock<HashMap<(String, String), String>> = OnceLock::new();
    ROUTES.get_or_init(|| parse_api_routes(include_str!("main.rs")))
}

/// The handler a request was routed to, from its method and matched path.
pub fn routed_handler(method: &str, matched_path: &str) -> Option<&'static str> {
    let routes = api_route_handlers();
    let find = |p: &str| {
        routes
            .get(&(method.to_ascii_uppercase(), p.to_string()))
            .map(String::as_str)
    };
    find(matched_path).or_else(|| matched_path.strip_prefix("/api").and_then(find))
}

fn parse_api_routes(src: &str) -> HashMap<(String, String), String> {
    let mut out = HashMap::new();
    let Some(start) = src.find("let api_routes") else {
        return out;
    };
    let end = src[start..]
        .find("middleware::auth::require_auth")
        .map_or(src.len(), |e| start + e);
    let mut rest = &src[start..end];
    while let Some(i) = rest.find(".route(") {
        rest = &rest[i + ".route(".len()..];
        let mut depth = 1;
        let mut close = rest.len();
        for (k, ch) in rest.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        close = k;
                        break;
                    }
                }
                _ => {}
            }
        }
        let call = &rest[..close];
        rest = &rest[close..];
        let Some(q1) = call.find('"') else { continue };
        let Some(len) = call[q1 + 1..].find('"') else {
            continue;
        };
        let path = &call[q1 + 1..q1 + 1 + len];
        let handlers = &call[q1 + 2 + len..];
        for method in ["get", "post", "put", "delete", "patch"] {
            let word = format!("{method}(routes::");
            for (at, _) in handlers.match_indices(&word) {
                if at > 0 && handlers[..at].ends_with(|c: char| c.is_alphanumeric() || c == '_') {
                    continue;
                }
                let name: String = handlers[at + word.len()..]
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
                    .collect();
                if name.contains("::") {
                    out.insert(
                        (method.to_ascii_uppercase(), path.to_string()),
                        name.trim_end_matches(':').to_string(),
                    );
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// The role check a handler body starts with, as a level. `None` = no check.
    fn guard_level(body: &str) -> Option<u8> {
        let mut found: Vec<(usize, u8)> = Vec::new();
        let mut push = |pat: &str, level: u8| {
            if let Some(i) = body.find(pat) {
                found.push((i, level));
            }
        };
        for n in 0..=4u8 {
            push(&format!("claims.require_role({n})"), n);
        }
        push("if !is_admin_or_owner(&claims.role) {", 3);
        push("admin(&claims)?", 3);
        push("role_manager(&claims)?", 3);
        push("authorize_gate_control(claims.role_level()", 1);
        push(
            "if claims.role != \"owner\" && claims.role != \"admin\" {",
            3,
        );
        push("if claims.role != \"owner\" {", 4);
        push("if role_level < 3 {", 3);
        found.sort();
        found.first().map(|&(_, l)| l)
    }

    /// `file::function` → its role check, for every handler main.rs routes.
    fn routed_guards() -> BTreeMap<String, Option<u8>> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let main = std::fs::read_to_string(dir.join("main.rs")).unwrap();
        let mut routed = BTreeSet::new();
        for chunk in main.split("routes::").skip(1) {
            let mut parts = chunk.splitn(3, "::");
            let file = parts.next().unwrap_or("");
            let rest = parts.next().unwrap_or("");
            let func: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            let is_ident = file.chars().all(|c| c.is_alphanumeric() || c == '_');
            if !file.is_empty() && is_ident && !func.is_empty() {
                routed.insert((file.to_string(), func));
            }
        }
        let mut out = BTreeMap::new();
        for (file, func) in routed {
            let path = dir.join("routes").join(format!("{file}.rs"));
            let Ok(src) = std::fs::read_to_string(&path) else {
                continue;
            };
            let sig = format!("\npub async fn {func}(");
            let Some(start) = src.find(&sig) else {
                continue;
            };
            let end = src[start..]
                .find("\n}\n")
                .map(|e| start + e)
                .unwrap_or(src.len());
            out.insert(format!("{file}::{func}"), guard_level(&src[start..end]));
        }
        out
    }

    #[test]
    fn every_role_checked_route_is_in_the_permissions_table_at_its_level() {
        let guards = routed_guards();
        assert!(guards.len() > 100, "parsed only {} routes", guards.len());
        let mut listed: BTreeMap<&str, u8> = BTreeMap::new();
        for cap in CAPABILITIES {
            let level = role_level(cap.min_role);
            assert!(
                ROLES.iter().any(|r| r.key == cap.min_role),
                "{}: unknown role {}",
                cap.key,
                cap.min_role
            );
            for h in cap.handlers {
                assert!(
                    listed.insert(h, level).is_none(),
                    "{h} is listed under more than one capability"
                );
                let Some(actual) = guards.get(*h) else {
                    panic!("{h} (capability {}) is not a routed handler", cap.key);
                };
                assert_eq!(
                    actual.unwrap_or(0),
                    level,
                    "{h} requires level {:?} in code but permissions.rs says {} ({}). \
                     Update CAPABILITIES so the team page stays truthful.",
                    actual,
                    cap.min_role,
                    level
                );
            }
        }
        for (h, level) in &guards {
            if let Some(l) = level {
                if *l > 0 {
                    assert!(
                        listed.contains_key(h.as_str()),
                        "{h} requires role level {l} but is missing from \
                         permissions::CAPABILITIES — add it to the capability it belongs to \
                         so the team page shows it"
                    );
                }
            }
        }
    }

    fn custom(caps: &[&str]) -> Holder {
        let caps: Vec<String> = caps.iter().map(|c| c.to_string()).collect();
        Holder::custom("r1", Some(("QA", &caps)))
    }

    #[test]
    fn built_in_roles_resolve_like_their_handlers_check() {
        // For every handler in the table, a built-in role is let through
        // exactly when its level reaches the capability's.
        for role in ["viewer", "member", "billing", "admin", "owner"] {
            let holder = Holder::builtin(role);
            for cap in CAPABILITIES {
                for h in cap.handlers {
                    let allowed = matches!(resolve_access(&holder, Some(h)), Access::Allow { .. });
                    assert_eq!(
                        allowed,
                        role_level(role) >= role_level(cap.min_role),
                        "{role} on {h}"
                    );
                }
            }
        }
        // Handlers outside the table are left to their own checks.
        assert_eq!(
            resolve_access(&Holder::builtin("viewer"), Some("api::me")),
            Access::Allow {
                effective_role: "viewer"
            }
        );
        assert_eq!(
            resolve_access(&Holder::builtin("admin"), None),
            Access::Allow {
                effective_role: "admin"
            }
        );
    }

    #[test]
    fn a_custom_role_runs_only_what_it_holds_at_that_capabilitys_role() {
        let qa = custom(&["plans", "runs", "tests"]);
        // Plans and runs are member capabilities, tests an admin one.
        assert_eq!(
            resolve_access(&qa, Some("plans::create_plan")),
            Access::Allow {
                effective_role: "member"
            }
        );
        assert_eq!(
            resolve_access(&qa, Some("tests::quarantine")),
            Access::Allow {
                effective_role: "admin"
            }
        );
        // Not held: refused, naming the capability.
        match resolve_access(&qa, Some("api::sync_repos")) {
            Access::Deny { capability } => assert_eq!(capability.key, "repos"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            resolve_access(&qa, Some("reviewer::rate_review")),
            Access::Deny { .. }
        ));
        // Everyone sees the workspace and manages their own account.
        assert!(matches!(
            resolve_access(&qa, Some("api::list_runs")),
            Access::Allow { .. }
        ));
        assert!(matches!(
            resolve_access(&qa, Some("users::change_password")),
            Access::Allow { .. }
        ));
        // Outside the table it runs as its floor: it lacks some member
        // capabilities, so it is a viewer there.
        assert_eq!(qa.floor(), "viewer");
        assert_eq!(
            resolve_access(&qa, Some("api::me")),
            Access::Allow {
                effective_role: "viewer"
            }
        );
        // With every member capability its floor is member, never more.
        let mut keys: Vec<&str> = builtin_capabilities("member").into_iter().collect();
        keys.push("repos");
        let wide = custom(&keys);
        assert_eq!(wide.floor(), "member");
        assert_eq!(
            resolve_access(&wide, Some("api::sync_repos")),
            Access::Allow {
                effective_role: "admin"
            }
        );
    }

    #[test]
    fn a_custom_role_never_holds_owner_only_capabilities_or_role_management() {
        let all: Vec<&str> = CAPABILITIES.iter().map(|c| c.key).collect();
        let everything = custom(&all);
        for cap in CAPABILITIES {
            let owner_only = role_level(cap.min_role) >= role_level("owner");
            if owner_only || NEVER_IN_CUSTOM_ROLES.contains(&cap.key) {
                assert!(!customizable(cap), "{}", cap.key);
                assert!(!everything.holds(cap.key), "{}", cap.key);
                for h in cap.handlers {
                    assert!(
                        matches!(resolve_access(&everything, Some(h)), Access::Deny { .. }),
                        "{h}"
                    );
                }
            } else {
                assert!(everything.holds(cap.key), "{}", cap.key);
            }
        }
        // The owner-only ones the team page names.
        for key in ["manage_admins", "team_name", "disconnect", "reset"] {
            assert!(!customizable(capability(key).unwrap()), "{key}");
        }
        assert!(!everything.manages_roles());
        assert!(Holder::builtin("admin").manages_roles());
        assert!(Holder::builtin("owner").manages_roles());
        assert!(!Holder::builtin("member").manages_roles());
        assert!(!Holder::builtin("viewer").manages_roles());
        // Asking for them is refused, by name.
        let owner = Holder::builtin("owner");
        for key in ["reset", "team_name", "manage_admins", "disconnect", "roles"] {
            assert!(
                matches!(
                    checked_role_capabilities(&owner, &[key.to_string()]),
                    Err(RoleCapError::NotAllowed(_))
                ),
                "{key}"
            );
        }
        assert!(matches!(
            checked_role_capabilities(&owner, &["superpowers".to_string()]),
            Err(RoleCapError::Unknown(_))
        ));
        // Templates drop them too.
        assert!(!template_capabilities("admin").unwrap().contains(&"roles"));
        assert!(template_capabilities("owner")
            .unwrap()
            .iter()
            .all(|k| customizable(capability(k).unwrap())));
        assert!(template_capabilities("billing").is_none());
    }

    #[test]
    fn nobody_makes_or_gives_a_role_with_more_than_they_hold() {
        let admin = Holder::builtin("admin");
        let ok = checked_role_capabilities(&admin, &["plans".into(), "repos".into()]).unwrap();
        assert!(ok.contains("plans") && ok.contains("repos"));
        // The baseline is always added.
        assert!(ok.contains("view") && ok.contains("own_account"));

        // A custom-role holder (were it ever let in) can't go past itself.
        let qa = custom(&["plans", "people"]);
        match checked_role_capabilities(&qa, &["plans".into(), "repos".into()]) {
            Err(RoleCapError::MoreThanYouHave(c)) => assert_eq!(c.key, "repos"),
            other => panic!("{other:?}"),
        }
        assert!(checked_role_capabilities(&qa, &["plans".into()]).is_ok());

        // Giving a role: only one that fits inside your own.
        assert!(within(&admin, &builtin_capabilities("member")));
        assert!(!within(&admin, &builtin_capabilities("owner")));
        assert!(!within(&qa, &builtin_capabilities("member")));
        assert!(within(&qa, &custom(&["plans"]).caps));
        assert!(!within(&qa, &custom(&["runs"]).caps));
        // Every custom role fits inside a built-in admin.
        let all: Vec<&str> = CAPABILITIES.iter().map(|c| c.key).collect();
        assert!(within(&admin, &custom(&all).caps));
    }

    #[test]
    fn checks_inside_handlers_follow_the_capability_too() {
        // The auto-merge gate's control runs as a member for a role holding
        // reviews, whatever its floor, and is refused without it.
        let reviewer = custom(&["reviews"]);
        assert_eq!(reviewer.floor(), "viewer");
        assert_eq!(
            resolve_access(&reviewer, Some("reviewer::control_gate")),
            Access::Allow {
                effective_role: "member"
            }
        );
        assert!(matches!(
            resolve_access(&custom(&["repos"]), Some("reviewer::control_gate")),
            Access::Deny { .. }
        ));
        // Support stays with built-in admins and owners.
        assert!(!customizable(capability("support").unwrap()));
        assert!(!custom(&["support"]).holds("support"));
        assert!(Holder::builtin("admin").holds("support"));
    }

    /// The chat is called Chat and its sources Knowledge. The old product
    /// name stays only in identifiers (routes, keys, module names), never in
    /// words people read: capability labels, messages, the model's own name
    /// for itself, or the public API document.
    #[test]
    fn the_old_chat_name_is_never_in_words_people_read() {
        // Built from parts so this test's own source doesn't trip it.
        let word = format!("{}{}", "bra", "in");
        let re = regex::Regex::new(&format!(
            r"(?i)(^|[^A-Za-z0-9_/#:\-]){word}([^A-Za-z0-9_/#:\-]|$)"
        ))
        .unwrap();
        for c in CAPABILITIES {
            assert!(
                !re.is_match(c.label),
                "capability {} label: {}",
                c.key,
                c.label
            );
        }
        let literal = regex::Regex::new(r#""((?:[^"\\]|\\.)*)""#).unwrap();
        let sources: &[(&str, &str)] = &[
            ("permissions.rs", include_str!("permissions.rs")),
            ("routes/brain.rs", include_str!("routes/brain.rs")),
            ("routes/knowledge.rs", include_str!("routes/knowledge.rs")),
            (
                "routes/chat_policy.rs",
                include_str!("routes/chat_policy.rs"),
            ),
            ("openapi.rs", include_str!("openapi.rs")),
            (
                "common agent_policy/model.rs",
                include_str!("../../common/src/agent_policy/model.rs"),
            ),
            (
                "common knowledge/answer.rs",
                include_str!("../../common/src/knowledge/answer.rs"),
            ),
        ];
        let mut found = Vec::new();
        for (file, src) in sources {
            for (n, line) in src.lines().enumerate() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                for m in literal.captures_iter(line) {
                    let text = &m[1];
                    // A bare key (the usage feature) or a source file name is an identifier.
                    if text.eq_ignore_ascii_case(&word) || text.ends_with(".rs") {
                        continue;
                    }
                    if re.is_match(text) {
                        found.push(format!("{file}:{}: {text}", n + 1));
                    }
                }
            }
        }
        let doc = include_str!("../../../docs/api/openapi.json");
        if re.is_match(doc) {
            found.push("docs/api/openapi.json".into());
        }
        assert!(
            found.is_empty(),
            "say Chat or Knowledge instead:\n{}",
            found.join("\n")
        );
    }

    #[test]
    fn brain_and_knowledge_routes_are_decided_by_their_capabilities() {
        let route = |m: &str, p: &str| routed_handler(m, p);
        assert_eq!(route("GET", "/knowledge"), Some("knowledge::list_bases"));
        assert_eq!(
            route("POST", "/knowledge/:kb_id/files"),
            Some("knowledge::start_upload")
        );
        assert_eq!(route("POST", "/brain/token"), Some("brain::stream_token"));
        assert_eq!(
            route("PUT", "/brain/threads/:thread_id"),
            Some("brain::update_thread")
        );
        assert_eq!(route("GET", "/brain/starters"), Some("brain::starters"));
        assert_eq!(
            route("PUT", "/brain/threads/:thread_id/messages/:seq/feedback"),
            Some("brain::rate_message")
        );
        assert_eq!(
            resolve_access(&Holder::builtin("viewer"), route("GET", "/brain/starters")),
            Access::Allow {
                effective_role: "viewer"
            }
        );
        assert!(matches!(
            resolve_access(
                &Holder::builtin("viewer"),
                route("PUT", "/brain/threads/:thread_id/messages/:seq/feedback")
            ),
            Access::Deny { .. }
        ));
        // A custom role can hold the brain capabilities, and only those it holds.
        let asker = custom(&["brain_ask"]);
        assert_eq!(
            resolve_access(&asker, route("POST", "/brain/token")),
            Access::Allow {
                effective_role: "member"
            }
        );
        assert!(matches!(
            resolve_access(&asker, route("POST", "/knowledge")),
            Access::Deny { .. }
        ));
        let librarian = custom(&["knowledge_manage"]);
        assert_eq!(
            resolve_access(
                &librarian,
                route("DELETE", "/knowledge/:kb_id/files/:file_id")
            ),
            Access::Allow {
                effective_role: "admin"
            }
        );
        assert!(matches!(
            resolve_access(&librarian, route("POST", "/brain/attachments")),
            Access::Deny { .. }
        ));
        // Reading is baseline: every role, custom or built-in, reads.
        assert!(is_baseline(capability("brain_read").unwrap()));
        assert!(matches!(
            resolve_access(&custom(&[]), route("GET", "/brain/threads")),
            Access::Allow { .. }
        ));
        // Viewers can't ask; members can't manage; admins do both.
        assert!(matches!(
            resolve_access(&Holder::builtin("viewer"), route("POST", "/brain/token")),
            Access::Deny { .. }
        ));
        assert!(matches!(
            resolve_access(
                &Holder::builtin("member"),
                route("PUT", "/knowledge/:kb_id")
            ),
            Access::Deny { .. }
        ));
        assert!(
            Holder::builtin("admin").holds("knowledge_manage")
                && Holder::builtin("admin").holds("brain_ask")
        );
        for key in ["brain_ask", "knowledge_manage"] {
            assert!(customizable(capability(key).unwrap()), "{key}");
        }
    }

    #[test]
    fn stored_keys_are_read_through_the_table() {
        // A key that is gone from the table, or owner-only, is dropped.
        let caps = custom_capabilities(["plans", "no_such_thing", "reset"]);
        assert!(caps.contains("plans"));
        assert!(!caps.contains("no_such_thing"));
        assert!(!caps.contains("reset"));
        assert_eq!(custom_role_id("custom:abc"), Some("abc"));
        assert_eq!(custom_role_id("custom:"), None);
        assert_eq!(custom_role_id("admin"), None);
    }

    #[test]
    fn every_table_handler_is_found_from_its_route() {
        let routes = api_route_handlers();
        assert!(routes.len() > 150, "read {} routes", routes.len());
        let routed: BTreeSet<&str> = routes.values().map(String::as_str).collect();
        for cap in CAPABILITIES {
            for h in cap.handlers {
                assert!(routed.contains(h), "{h} has no route in the API router");
            }
        }
        assert_eq!(
            routed_handler("PUT", "/users/:user_id/role"),
            Some("users::update_role")
        );
        assert_eq!(
            routed_handler("delete", "/api/roles/:role_id"),
            Some("roles::delete_role")
        );
        assert_eq!(routed_handler("GET", "/roles"), Some("roles::list_roles"));
        assert_eq!(routed_handler("POST", "/roles"), Some("roles::create_role"));
        assert_eq!(routed_handler("GET", "/nope"), None);
    }

    #[test]
    fn capabilities_ascend_by_role() {
        let levels: Vec<u8> = CAPABILITIES
            .iter()
            .map(|c| role_level(c.min_role))
            .collect();
        let mut sorted = levels.clone();
        sorted.sort();
        assert_eq!(levels, sorted);
    }

    #[test]
    fn admins_manage_members_owners_manage_everyone() {
        assert!(can_manage("admin", "member"));
        assert!(can_manage("admin", "viewer"));
        assert!(can_manage("admin", "billing"));
        assert!(!can_manage("admin", "admin"));
        assert!(!can_manage("admin", "owner"));
        assert!(can_manage("owner", "owner"));
        assert!(!can_manage("member", "viewer"));
        assert!(!can_manage("viewer", "viewer"));

        assert!(can_grant("admin", "member"));
        assert!(!can_grant("admin", "admin"));
        assert!(!can_grant("admin", "owner"));
        assert!(can_grant("owner", "owner"));
        assert!(!can_grant("owner", "superuser"));

        assert_eq!(grantable_roles("admin"), vec!["viewer", "member"]);
        assert_eq!(
            grantable_roles("owner"),
            vec!["viewer", "member", "admin", "owner"]
        );
        assert!(grantable_roles("member").is_empty());

        // Invites are not seats: an admin manages every pending invite,
        // including an admin invite an owner sent.
        assert!(Holder::builtin("admin").holds("people"));
        assert!(Holder::builtin("owner").holds("people"));
        assert!(!Holder::builtin("billing").holds("people"));
        assert!(!Holder::builtin("member").holds("people"));
        assert!(!Holder::builtin("viewer").holds("people"));
    }

    #[test]
    fn guard_parser_reads_each_check_style() {
        assert_eq!(guard_level("x claims.require_role(1)?; y"), Some(1));
        assert_eq!(
            guard_level("if !is_admin_or_owner(&claims.role) { return }"),
            Some(3)
        );
        assert_eq!(
            guard_level("let a = is_admin_or_owner(&claims.role);"),
            None
        );
        assert_eq!(guard_level("    admin(&claims)?;"), Some(3));
        assert_eq!(guard_level("if claims.role != \"owner\" {"), Some(4));
        assert_eq!(
            guard_level("if claims.role != \"owner\" && claims.role != \"admin\" {"),
            Some(3)
        );
        assert_eq!(guard_level("nothing here"), None);
    }
}
