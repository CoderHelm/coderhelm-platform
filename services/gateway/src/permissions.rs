//! What each team role can do — the single source of truth the dashboard's
//! team page shows (served by `GET /api/users/permissions`).
//!
//! Every capability names the route handlers it covers and the lowest role
//! that may call them. The test at the bottom reads every routed handler's
//! role check out of the source and fails when one is missing here or when its
//! required role differs, so this table cannot drift from what the gateway
//! actually enforces.

use serde::Serialize;

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

#[derive(Serialize, Clone, Copy)]
pub struct Capability {
    pub key: &'static str,
    pub label: &'static str,
    /// Lowest role allowed (a key of `ROLES`).
    pub min_role: &'static str,
    /// Route handlers (`file::function`) whose role check this row describes.
    #[serde(skip)]
    #[cfg_attr(not(test), allow(dead_code))]
    pub handlers: &'static [&'static str],
}

/// In ascending order of the role they need.
pub const CAPABILITIES: &[Capability] = &[
    // ── Everyone on the team (viewer and up) ────────────────────────
    Capability {
        key: "view",
        label: "See runs, plans, reviews, analytics, repos, tests and settings",
        min_role: "viewer",
        handlers: &[
            "api::list_runs",
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
            "review_lessons::list_lessons",
            "tests::summary",
            "tests::list_tests",
        ],
    },
    Capability {
        key: "view_team",
        label: "See who is on the team",
        min_role: "viewer",
        handlers: &["users::list_users"],
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
        key: "runs",
        label: "Retry, re-run, re-review or cancel runs",
        min_role: "member",
        handlers: &[
            "api::retry_run",
            "api::reset_and_rerun",
            "api::re_review_run",
            "api::cancel_run",
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
        ],
    },
    Capability {
        key: "recommendations",
        label: "Turn recommendations into plans or dismiss them",
        min_role: "member",
        handlers: &[
            "log_analyzer::create_plan_from_recommendation",
            "log_analyzer::dismiss_recommendation",
        ],
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
        key: "people",
        label: "Invite people, resend or revoke invites, link GitHub accounts, and change or remove members and viewers",
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
        key: "delete_plans",
        label: "Delete plans",
        min_role: "admin",
        handlers: &["plans::delete_plan"],
    },
    Capability {
        key: "repos",
        label: "Manage repositories: sync, turn on, roles, code graph and infrastructure scans",
        min_role: "admin",
        handlers: &[
            "api::sync_repos",
            "api::set_repo_role",
            "api::update_repo",
            "api::delete_repo",
            "api::regenerate_repo",
            "graph::update_config",
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
        label: "Connect GitHub, Jira, AWS accounts, monitoring and docs tools, Teams and MCP servers, and choose what each may do",
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
        ],
    },
    Capability {
        key: "tests",
        label: "Change test settings; quarantine, pin and fix tests",
        min_role: "admin",
        handlers: &[
            "tests::put_settings",
            "tests::quarantine",
            "tests::pin",
            "tests::fix",
            "tests::accept_recommendation",
            "tests::dismiss_recommendation",
        ],
    },
    Capability {
        key: "alerts",
        label: "Configure alert routes, the alert budget and alert notifications",
        min_role: "admin",
        handlers: &[
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
            "api::list_dlq_messages",
            "api::redrive_dlq_message",
            "api::delete_dlq_message",
            "api::purge_dlq",
        ],
    },
    Capability {
        key: "budget",
        label: "Set the monthly token limit",
        min_role: "admin",
        handlers: &["api::update_budget"],
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

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
