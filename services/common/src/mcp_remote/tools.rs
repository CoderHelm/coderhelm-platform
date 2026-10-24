//! Which of a remote server's tools agents get.
//!
//! An admin picks the allowed tools. When the team has more than one admin,
//! a widened list waits for a second admin (the same rule as team tools:
//! [`crate::live::tools::can_approve`]); narrowing applies at once. Agents
//! get exactly the tools that are listed by the server, allowed and
//! approved. Anything else doesn't exist for them: it is not offered to the
//! model, and a call to it is refused before anything is sent.

use std::collections::BTreeSet;

use super::client::RemoteTool;
use super::store::ServerRecord;

/// What a tool does, from the server's own hints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
    Unknown,
}

impl Access {
    pub fn as_str(self) -> &'static str {
        match self {
            Access::Read => "read",
            Access::Write => "write",
            Access::Unknown => "unknown",
        }
    }
}

pub fn access(t: &RemoteTool) -> Access {
    match (t.read_only, t.destructive) {
        (Some(true), _) => Access::Read,
        (Some(false), _) | (None, Some(true)) => Access::Write,
        _ => Access::Unknown,
    }
}

/// Name prefixes read as "reads only" for tools the server gives no hint
/// about (the rule the planner has always used for catalog servers).
pub fn name_looks_read_only(name: &str) -> bool {
    [
        "get_",
        "list_",
        "search_",
        "read_",
        "query_",
        "fetch_",
        "find_",
        "describe_",
    ]
    .iter()
    .any(|p| name.starts_with(p))
}

/// Whether a read-only agent (the planner, the reviewer) may get this tool:
/// the server says it only reads, or it gives no hint and its name says so.
pub fn read_only_agent_may_use(t: &RemoteTool) -> bool {
    match access(t) {
        Access::Read => true,
        Access::Write => false,
        Access::Unknown => name_looks_read_only(&t.name),
    }
}

/// The name an agent sees: `<server>__<tool>`. `None` when that name can't
/// be a model tool name (64 characters of letters, digits, `_` and `-`).
pub fn agent_tool_name(server_id: &str, tool: &str) -> Option<String> {
    let n = format!("{server_id}__{tool}");
    let ok = n.len() <= 64
        && n.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    ok.then_some(n)
}

/// Ids are short lowercase words, never containing `__` (the agent tool
/// name splits on it).
pub fn valid_server_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 24
        && id.as_bytes()[0].is_ascii_alphanumeric()
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// An id from a display name: `Acme Docs (prod)` → `acme-docs-prod`.
pub fn slug(name: &str) -> String {
    let mut out = String::new();
    for ch in name.chars().flat_map(|c| c.to_lowercase()) {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
        if out.len() >= 20 {
            break;
        }
    }
    let out = out.trim_matches('-').to_string();
    if out.is_empty() {
        "server".into()
    } else {
        out
    }
}

/// The tools agents get from this server, in the server's order.
pub fn effective(s: &ServerRecord) -> Vec<&RemoteTool> {
    s.tools
        .iter()
        .filter(|t| s.allowed.contains(&t.name) && s.approved.contains(&t.name))
        .filter(|t| agent_tool_name(&s.id, &t.name).is_some())
        .collect()
}

/// The model tool definitions for a server's usable tools: (name,
/// description, input schema). `read_only` keeps only tools a read-only agent
/// may use (the planner, the reviewer).
pub fn agent_tools(s: &ServerRecord, read_only: bool) -> Vec<(String, String, serde_json::Value)> {
    effective(s)
        .into_iter()
        .filter(|t| !read_only || read_only_agent_may_use(t))
        .filter_map(|t| {
            let name = agent_tool_name(&s.id, &t.name)?;
            let mut d = crate::truncate_str(&t.description, 1000).to_string();
            if d.is_empty() {
                d = t.title.clone().unwrap_or_else(|| t.name.clone());
            }
            let schema = if t.input_schema.is_object() {
                t.input_schema.clone()
            } else {
                serde_json::json!({"type": "object", "properties": {}})
            };
            Some((name, format!("[{}] {d}", s.name), schema))
        })
        .collect()
}

/// Allowed tools still waiting for a second admin.
pub fn awaiting_approval(s: &ServerRecord) -> Vec<String> {
    s.allowed.difference(&s.approved).cloned().collect()
}

/// Saves the admin's pick. Only tools the server actually lists can be
/// allowed. With one admin (or a catalog server) the pick applies at once;
/// otherwise removals apply at once and additions wait for approval.
pub fn save_allowed(
    s: &mut ServerRecord,
    pick: &BTreeSet<String>,
    editor: &str,
    admin_count: usize,
) {
    let listed: BTreeSet<String> = s.tools.iter().map(|t| t.name.clone()).collect();
    s.allowed = pick.intersection(&listed).cloned().collect();
    if admin_count <= 1 || !s.catalog_id.is_empty() {
        s.approved = s.allowed.clone();
        s.changed_by = editor.to_string();
        return;
    }
    s.approved = s.approved.intersection(&s.allowed).cloned().collect();
    if !s.allowed.is_subset(&s.approved) {
        s.changed_by = editor.to_string();
    }
}

/// A second admin approves the current pick.
pub fn approve(s: &mut ServerRecord, approver: &str, admin_count: usize) -> Result<(), String> {
    if awaiting_approval(s).is_empty() {
        return Ok(());
    }
    crate::live::tools::can_approve(&s.changed_by, approver, admin_count).map_err(|_| {
        "Another admin has to approve these tools: the person who picked them can't.".to_string()
    })?;
    s.approved = s.allowed.clone();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tool(name: &str, ro: Option<bool>) -> RemoteTool {
        RemoteTool {
            name: name.into(),
            title: None,
            description: String::new(),
            input_schema: json!({"type": "object"}),
            read_only: ro,
            destructive: None,
        }
    }

    fn server() -> ServerRecord {
        let mut s = ServerRecord::new("docs", "Docs", "https://mcp.example.com/mcp");
        s.tools = vec![
            tool("search", Some(true)),
            tool("create_page", Some(false)),
            tool("get_page", None),
            tool("rename", None),
        ];
        s
    }

    fn set(xs: &[&str]) -> BTreeSet<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn access_comes_from_the_hints() {
        assert_eq!(access(&tool("a", Some(true))), Access::Read);
        assert_eq!(access(&tool("a", Some(false))), Access::Write);
        assert_eq!(access(&tool("a", None)), Access::Unknown);
        let mut d = tool("a", None);
        d.destructive = Some(true);
        assert_eq!(access(&d), Access::Write);
        assert!(read_only_agent_may_use(&tool("search", Some(true))));
        assert!(!read_only_agent_may_use(&tool("get_x", Some(false))));
        assert!(read_only_agent_may_use(&tool("get_x", None)));
        assert!(!read_only_agent_may_use(&tool("rename", None)));
    }

    #[test]
    fn one_admin_picks_and_it_applies() {
        let mut s = server();
        save_allowed(
            &mut s,
            &set(&["search", "create_page", "not_listed"]),
            "a@x.com",
            1,
        );
        assert_eq!(s.allowed, set(&["create_page", "search"]));
        assert_eq!(
            effective(&s)
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>(),
            vec!["search", "create_page"]
        );
        assert!(awaiting_approval(&s).is_empty());
    }

    #[test]
    fn with_two_admins_additions_wait_and_removals_apply() {
        let mut s = server();
        save_allowed(&mut s, &set(&["search", "create_page"]), "a@x.com", 2);
        // Nothing reaches agents until a second admin approves.
        assert!(effective(&s).is_empty());
        assert_eq!(awaiting_approval(&s), vec!["create_page", "search"]);
        assert!(approve(&mut s, "A@x.com", 2).is_err());
        approve(&mut s, "b@x.com", 2).unwrap();
        assert_eq!(effective(&s).len(), 2);
        // Removing a tool applies at once.
        save_allowed(&mut s, &set(&["search"]), "a@x.com", 2);
        assert_eq!(effective(&s).len(), 1);
        assert!(awaiting_approval(&s).is_empty());
        // Widening again waits; the approved one keeps working meanwhile.
        save_allowed(&mut s, &set(&["search", "rename"]), "b@x.com", 2);
        assert_eq!(
            effective(&s)
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>(),
            vec!["search"]
        );
        assert_eq!(awaiting_approval(&s), vec!["rename"]);
        approve(&mut s, "a@x.com", 2).unwrap();
        assert_eq!(effective(&s).len(), 2);
    }

    #[test]
    fn catalog_servers_need_no_second_admin() {
        let mut s = server();
        s.catalog_id = "linear".into();
        save_allowed(&mut s, &set(&["search"]), "a@x.com", 3);
        assert_eq!(effective(&s).len(), 1);
    }

    #[test]
    fn tools_whose_names_cant_be_offered_are_left_out() {
        let mut s = server();
        let long = "x".repeat(70);
        s.tools.push(tool(&long, Some(true)));
        s.tools.push(tool("has space", Some(true)));
        save_allowed(&mut s, &set(&[&long, "has space"]), "a@x.com", 1);
        assert!(effective(&s).is_empty());
        assert_eq!(
            agent_tool_name("docs", "search").as_deref(),
            Some("docs__search")
        );
    }

    #[test]
    fn read_only_agents_get_only_reading_tools() {
        let mut s = server();
        save_allowed(
            &mut s,
            &set(&["search", "create_page", "get_page", "rename"]),
            "a@x.com",
            1,
        );
        let names = |ro| {
            agent_tools(&s, ro)
                .into_iter()
                .map(|(n, _, _)| n)
                .collect::<Vec<_>>()
        };
        assert_eq!(names(true), vec!["docs__search", "docs__get_page"]);
        assert_eq!(names(false).len(), 4);
        assert!(agent_tools(&s, true)[0].1.starts_with("[Docs]"));
    }

    #[test]
    fn ids_and_slugs() {
        assert_eq!(slug("Acme Docs (prod)"), "acme-docs-prod");
        assert_eq!(slug("  ***  "), "server");
        assert!(slug(&"a".repeat(50)).len() <= 20);
        assert!(valid_server_id("acme-docs"));
        assert!(!valid_server_id("acme__docs"));
        assert!(!valid_server_id("Acme"));
        assert!(!valid_server_id(""));
        assert!(!valid_server_id("-x"));
    }
}
