//! The tools a skill names, mapped to what the team has: its connected MCP
//! servers, stdio servers found in its repositories (run in the sandbox),
//! and CoderHelm's own connections that offer the same tool.
//!
//! A skill names a tool in backticks (`traffic_counts`, `mcp__acme__list`)
//! or in its front matter's `allowed-tools`. Every such name an offered tool
//! answers to becomes a grant under the name the skill uses; a name no
//! source offers is listed as missing, and the skill's own preflight decides
//! what to do without it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::risk::{Basis, Verdict};
use super::ToolGrant;
use crate::plugins::connect::ToolDef;
use crate::tool_registry::Risk;

/// A tool one source offers.
#[derive(Debug, Clone, PartialEq)]
pub struct Offered {
    /// `sandbox:<id>`, `coderhelm:<name>` or a connected server's id.
    pub source: String,
    /// The source's short name the skill may use in `mcp__<name>__<tool>`.
    pub alias: String,
    pub tool: ToolDef,
}

/// A grant as the schedule shows it: where it comes from and how its risk
/// was decided.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedTool {
    pub name: String,
    pub source: String,
    pub tool: String,
    pub risk: Risk,
    pub basis: Basis,
    #[serde(default)]
    pub why: String,
}

/// What a resolution found, kept on the schedule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Resolution {
    pub at: String,
    pub tools: Vec<ResolvedTool>,
    /// Names the skill gives that no source offers.
    #[serde(default)]
    pub missing: Vec<String>,
    /// Sandbox servers found for it, pinned.
    #[serde(default)]
    pub servers: Vec<super::stdio::StdioServer>,
    /// Why a source couldn't be read.
    #[serde(default)]
    pub problems: Vec<String>,
    /// The skill's memory folders, read from its text (`snapshots`).
    #[serde(default)]
    pub memory: Vec<String>,
}

/// The folders a skill keeps as its memory between runs, read from the
/// paths it names: a top folder called like a snapshot, a state, a memory
/// or a history (`snapshots/<date>.json` gives `snapshots`).
pub fn memory_dirs(skill_md: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut parts = skill_md.split('`');
    parts.next();
    for (i, p) in parts.enumerate() {
        if i % 2 != 0 || !p.contains('/') || p.contains(' ') {
            continue;
        }
        let first = p.trim_start_matches("./").split('/').next().unwrap_or("");
        let l = first.to_ascii_lowercase();
        if ["snapshot", "state", "memory", "history"]
            .iter()
            .any(|k| l.contains(k))
            && super::clean_dir(first).is_ok()
            && !out.contains(&first.to_string())
        {
            out.push(first.to_string());
        }
    }
    out
}

fn tool_like(s: &str) -> bool {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix("mcp__") {
        return rest.contains("__")
            && rest
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    }
    s.len() <= 64
        && s.contains('_')
        && s.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Every name in backticks that could be a tool, and the front matter's
/// `allowed-tools`, once each, in order.
pub fn tool_refs(skill_md: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut add = |s: &str| {
        let s = s.trim().trim_end_matches("()").to_string();
        if tool_like(&s) && !out.contains(&s) {
            out.push(s);
        }
    };
    for t in crate::skills::front_matter_tools(skill_md) {
        add(&t);
    }
    let mut parts = skill_md.split('`');
    parts.next();
    for (i, p) in parts.enumerate() {
        if i % 2 == 0 && !p.contains('\n') {
            add(p);
        }
    }
    out
}

/// Names the skill calls a tool in so many words: on a line that says
/// "tool", or written as `mcp__server__tool`. Only these are reported
/// missing; other backticked names (a JSON key, a script's flag) are not.
pub fn named_as_tools(skill_md: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in skill_md.lines() {
        let says_tool = line.to_ascii_lowercase().contains("tool");
        let mut parts = line.split('`');
        parts.next();
        for (i, p) in parts.enumerate() {
            if i % 2 == 0
                && tool_like(p)
                && (says_tool || p.starts_with("mcp__"))
                && !out.contains(&p.to_string())
            {
                out.push(p.to_string());
            }
        }
    }
    out
}

/// The grants for `refs` among `offered`: a name a tool answers to (its own
/// name, or `mcp__<alias>__<tool>`), first source wins. Returns the grants
/// and the names nothing answers to.
pub fn map_refs(refs: &[String], offered: &[Offered]) -> (Vec<ToolGrant>, Vec<String>) {
    let mut grants = Vec::new();
    let mut missing = Vec::new();
    for r in refs {
        let hit = offered
            .iter()
            .find(|o| &o.tool.name == r || format!("mcp__{}__{}", o.alias, o.tool.name) == *r);
        match hit {
            Some(o) => grants.push(ToolGrant {
                name: r.clone(),
                server: o.source.clone(),
                tool: if &o.tool.name == r {
                    String::new()
                } else {
                    o.tool.name.clone()
                },
                ..Default::default()
            }),
            None => missing.push(r.clone()),
        }
    }
    (grants, missing)
}

/// The prefixes of names that look like a tool family (`acme` of
/// `acme_cdn_top`), to look for the repository that serves them.
pub fn families(names: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for n in names {
        let n = n
            .strip_prefix("mcp__")
            .and_then(|r| r.split("__").next())
            .unwrap_or(n);
        if let Some((p, _)) = n.split_once('_') {
            if p.len() >= 3 && !out.contains(&p.to_string()) {
                out.push(p.to_string());
            }
        }
    }
    out
}

/// Merges the grants a resolution found with the schedule's own: an admin's
/// override (its risk) is kept, a tool the skill no longer names is dropped
/// unless an admin added it by hand.
pub fn merge(own: &[ToolGrant], found: &[ToolGrant]) -> Vec<ToolGrant> {
    let mut out: Vec<ToolGrant> = Vec::new();
    for f in found {
        match own.iter().find(|o| o.name == f.name) {
            Some(o) if o.server == f.server => out.push(ToolGrant {
                risk: o.risk.clone(),
                reads_only: o.reads_only,
                ..f.clone()
            }),
            _ => out.push(f.clone()),
        }
    }
    for o in own {
        if o.manual && !out.iter().any(|x| x.name == o.name) {
            out.push(o.clone());
        }
    }
    out
}

/// The verdicts per source for the schedule's page.
pub type Verdicts = BTreeMap<String, BTreeMap<String, Verdict>>;

#[cfg(test)]
mod tests {
    use super::*;

    const SKILL: &str = "---\nname: x\nallowed-tools: Read, mcp__acme__list_sites\n---\n\
The only tools this skill uses are `acme_cdn_top` and `acme_cdn_hits`. The only metrics tool is \
`run_query`.\n\nRun `plan --stage 1` and write `first_run` from the JSON, then `python3 x.py`.\n\
Call each tool named in `tool` with `args`, e.g. `mcp__acme__acme_cdn_top`.\n";

    fn offered(source: &str, alias: &str, name: &str) -> Offered {
        Offered {
            source: source.into(),
            alias: alias.into(),
            tool: ToolDef {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    #[test]
    fn the_skills_tool_names_are_read_from_its_text() {
        let refs = tool_refs(SKILL);
        assert_eq!(
            refs,
            [
                "mcp__acme__list_sites",
                "acme_cdn_top",
                "acme_cdn_hits",
                "run_query",
                "first_run",
                "mcp__acme__acme_cdn_top"
            ]
        );
        let named = named_as_tools(SKILL);
        assert_eq!(
            named,
            [
                "acme_cdn_top",
                "acme_cdn_hits",
                "run_query",
                "mcp__acme__acme_cdn_top"
            ]
        );
        assert_eq!(families(&named), ["acme", "run"]);
        assert_eq!(
            memory_dirs("Writes `snapshots/<date>.json` and `reports/<date>.html`; `snapshots/` is its memory. Run `python3 x/y.py`."),
            ["snapshots"]
        );
    }

    #[test]
    fn names_map_to_the_tools_that_answer_to_them() {
        let o = vec![
            offered("sandbox:acme-cli", "acme", "acme_cdn_top"),
            offered("sandbox:acme-cli", "acme", "acme_cdn_hits"),
            offered("sandbox:acme-cli", "acme", "list_sites"),
            offered("coderhelm:metrics", "metrics", "run_query"),
        ];
        let (g, missing) = map_refs(&tool_refs(SKILL), &o);
        let names: Vec<(&str, &str, &str)> = g
            .iter()
            .map(|g| (g.name.as_str(), g.server.as_str(), g.tool.as_str()))
            .collect();
        assert_eq!(
            names,
            [
                ("mcp__acme__list_sites", "sandbox:acme-cli", "list_sites"),
                ("acme_cdn_top", "sandbox:acme-cli", ""),
                ("acme_cdn_hits", "sandbox:acme-cli", ""),
                ("run_query", "coderhelm:metrics", ""),
                (
                    "mcp__acme__acme_cdn_top",
                    "sandbox:acme-cli",
                    "acme_cdn_top"
                ),
            ]
        );
        assert_eq!(missing, ["first_run"]);
    }

    #[test]
    fn an_admins_word_and_hand_added_tools_survive_a_new_resolution() {
        let own = vec![
            ToolGrant {
                name: "a_x".into(),
                server: "s".into(),
                risk: Some("read".into()),
                ..Default::default()
            },
            ToolGrant {
                name: "b_y".into(),
                server: "s".into(),
                manual: true,
                ..Default::default()
            },
            ToolGrant {
                name: "c_z".into(),
                server: "s".into(),
                ..Default::default()
            },
        ];
        let found = vec![ToolGrant {
            name: "a_x".into(),
            server: "s".into(),
            ..Default::default()
        }];
        let m = merge(&own, &found);
        assert_eq!(m.len(), 2);
        assert_eq!(m[0].risk.as_deref(), Some("read"));
        assert_eq!(m[1].name, "b_y");
    }
}
