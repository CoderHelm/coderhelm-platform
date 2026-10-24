//! A plugin's manifest: `plugin.yaml` at the plugin folder's root.
//!
//! ```yaml
//! name: marketing
//! namespace: acme            # optional: the id becomes acme-marketing
//! version: 1.2.0
//! description: Drafts launch posts, emails and blog updates from releases
//! skills: [skills/*/SKILL.md]
//! agents: [agents/*.md]
//! settings:
//!   - {key: brand_voice_doc, type: string}
//!   - {key: linkedin, type: connection, provider: oauth2, scopes: [w_member_social], sensitive: true}
//! tools:
//!   - {ref: "mcp:sanity", allow: [get_*, create_draft]}
//!   - {name: publish_post, kind: http, connection: linkedin, method: POST,
//!      url: "https://api.linkedin.com/rest/posts", risk: consequential}
//! triggers:
//!   - {on: github.release, agent: launch-writer}
//!   - {on: schedule, cron: "0 14 * * MON"}
//!   - {on: manual, form: {brief: text, channels: [linkedin, blog, email]}}
//! outputs:
//!   - {type: draft, kinds: [blog, social, email, image]}
//!   - {type: publish, via: publish_post, approval: {min: 1}}
//! results: {via: "mcp:posthog", tool: query_insight, after_days: 7}
//! budget: {monthly_tokens: 2000000}
//! ```
//!
//! Unknown fields are refused, so nothing can be added that isn't checked.
//! The manifest only names what a run may use; it never carries code, and a
//! value a team sets (a brand document, a connection) lives in the team's
//! settings, never in the package.
//!
//! [`Manifest::grant_lines`] lists everything the manifest grants: tools
//! with their risk, connection scopes, triggers and outputs. A new version
//! whose lines aren't all covered by the current version's [`widens`] the
//! grant and waits for an admin; anything else is wording and applies at
//! once.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::tool_registry::{classify_external, glob_match, Grant, Granted, Risk};

/// Where a plugin folder keeps its manifest, in the order they are looked for.
pub const MANIFEST_FILES: &[&str] = &["plugin.yaml", "plugin.yml", ".coderhelm/plugin.yaml"];
/// Largest manifest read, in bytes.
pub const MAX_MANIFEST_BYTES: usize = 32_000;
const MAX_TOOLS: usize = 40;
const MAX_TRIGGERS: usize = 10;
const MAX_SETTINGS: usize = 30;
const MAX_FORM_FIELDS: usize = 10;
/// The kinds of draft a plugin can make.
pub const DRAFT_KINDS: &[&str] = &[
    "blog", "social", "email", "image", "post", "page", "note", "reply", "ticket", "report",
];
/// The tools every plugin run has without asking: its own skills, and
/// drafts kept in CoderHelm until a person approves them.
pub const ALWAYS: &[&str] = &["read_plugin_skill", "save_draft"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_skills")]
    pub skills: Vec<String>,
    #[serde(default = "default_agents")]
    pub agents: Vec<String>,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub settings: Vec<Setting>,
    #[serde(default)]
    pub tools: Vec<ToolSpec>,
    #[serde(default)]
    pub triggers: Vec<Trigger>,
    #[serde(default)]
    pub outputs: Vec<Output>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub results: Option<Results>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<Budget>,
}

fn default_skills() -> Vec<String> {
    vec!["skills/*/SKILL.md".into()]
}
fn default_agents() -> Vec<String> {
    vec!["agents/*.md".into()]
}

/// A value the team sets for the plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Setting {
    pub key: String,
    /// `string`, `secret` or `connection`.
    #[serde(rename = "type")]
    pub kind: String,
    /// For a connection: how it signs in (`oauth2`, `mcp`, `key`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// For a connection: the scopes it asks for.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    /// Never shown again once saved, and never given to the model.
    #[serde(default)]
    pub sensitive: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

/// One tool the plugin may use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSpec {
    /// A connection's tools: `mcp:<server>`.
    #[serde(default, rename = "ref", skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    /// For `ref`: the tools allowed, by name or `*` pattern. Required.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
    /// A named tool: an HTTP call through a connection, or a built-in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `http` or `builtin`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// For `http`: the connection setting whose token it sends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The risk the plugin declares. It can raise a tool's risk, never
    /// lower it below what CoderHelm's registry says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<Risk>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

impl ToolSpec {
    /// The MCP server a `ref` names.
    pub fn server(&self) -> Option<&str> {
        self.reference.as_deref()?.strip_prefix("mcp:")
    }
}

/// What starts a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trigger {
    /// `manual`, `schedule` or `github.release`.
    pub on: String,
    /// For `schedule`: five-field cron, UTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// For `manual`: the brief form, field -> `text` or a list of choices.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub form: BTreeMap<String, FormField>,
    /// The agent (by its file's name) that answers; the first agent if unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// For `github.release`: only releases of these repositories.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repos: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FormField {
    /// `text`
    Kind(String),
    /// A list of choices.
    Choices(Vec<String>),
}

/// What a run produces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Output {
    /// `draft` or `publish`.
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<String>,
    /// For `publish`: the consequential tool that sends it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<ApprovalRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRule {
    /// A label for the approver group (the team picks its members).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default = "one")]
    pub min: u32,
}

fn one() -> u32 {
    1
}

/// Reading results back after a publish, to learn from them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Results {
    /// The connection whose read tool is asked (`mcp:posthog`).
    pub via: String,
    pub tool: String,
    #[serde(default = "seven")]
    pub after_days: u32,
}

fn seven() -> u32 {
    7
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    #[serde(default)]
    pub monthly_tokens: u64,
}

fn slug(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 40
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

fn tool_pattern(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 80
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '*')
}

/// Five cron fields (minute hour day month weekday), each digits, `*`, `/`,
/// `-`, `,` or a day or month name.
pub fn valid_cron(c: &str) -> bool {
    let f: Vec<&str> = c.split_whitespace().collect();
    f.len() == 5
        && f.iter().all(|x| {
            x.chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '*' | '/' | '-' | ','))
        })
}

/// A five-field cron as EventBridge Scheduler's six-field `cron(...)`: it
/// wants `?` in one of day-of-month and day-of-week, and a year.
pub fn eventbridge_cron(c: &str) -> Option<String> {
    if !valid_cron(c) {
        return None;
    }
    let f: Vec<&str> = c.split_whitespace().collect();
    let (dom, dow) = if f[4] != "*" {
        ("?", f[4])
    } else {
        (f[2], "?")
    };
    // EventBridge numbers weekdays 1-7 from Sunday; names work as they are.
    let dow = dow
        .split(',')
        .map(|d| match d {
            "0" | "7" => "1".to_string(),
            d if d.len() == 1 && d.chars().all(|c| c.is_ascii_digit()) => {
                (d.parse::<u8>().unwrap_or(0) + 1).to_string()
            }
            d => d.to_string(),
        })
        .collect::<Vec<_>>()
        .join(",");
    Some(format!(
        "cron({} {} {} {} {} *)",
        f[0], f[1], dom, f[3], dow
    ))
}

/// Parses and checks a manifest. Every problem is listed, in plain words.
pub fn parse(text: &str) -> Result<Manifest, Vec<String>> {
    if text.len() > MAX_MANIFEST_BYTES {
        return Err(vec![format!(
            "The manifest is larger than {} KB.",
            MAX_MANIFEST_BYTES / 1000
        )]);
    }
    let m: Manifest =
        serde_yaml::from_str(text).map_err(|e| vec![format!("The manifest can't be read: {e}")])?;
    let problems = m.problems();
    if problems.is_empty() {
        Ok(m)
    } else {
        Err(problems)
    }
}

impl Manifest {
    /// The plugin id: `namespace-name`.
    pub fn id(&self) -> String {
        super::plugin_id(self.namespace.as_deref(), &self.name)
    }

    pub fn setting(&self, key: &str) -> Option<&Setting> {
        self.settings.iter().find(|s| s.key == key)
    }

    /// A named tool by name.
    pub fn named_tool(&self, name: &str) -> Option<&ToolSpec> {
        self.tools.iter().find(|t| t.name.as_deref() == Some(name))
    }

    /// Everything wrong with the manifest.
    pub fn problems(&self) -> Vec<String> {
        let mut p = Vec::new();
        if !slug(&self.name) {
            p.push("`name` must be lowercase letters, digits and dashes.".into());
        }
        if let Some(ns) = &self.namespace {
            if !slug(ns) {
                p.push("`namespace` must be lowercase letters, digits and dashes.".into());
            }
        }
        if !super::valid_id(&self.id().replace('_', "-")) {
            p.push("The plugin's id (namespace and name) isn't valid.".into());
        }
        let v: Vec<&str> = self.version.split('.').collect();
        if v.len() < 2
            || v.len() > 3
            || v.iter()
                .any(|x| x.is_empty() || !x.chars().all(|c| c.is_ascii_digit()))
        {
            p.push("`version` must look like 1.2.0.".into());
        }
        if self.description.len() > 500 {
            p.push("`description` is longer than 500 characters.".into());
        }
        for (field, globs) in [
            ("skills", &self.skills),
            ("agents", &self.agents),
            ("commands", &self.commands),
        ] {
            for g in globs {
                if g.starts_with('/') || g.contains("..") || !g.ends_with(".md") {
                    p.push(format!(
                        "`{field}` entry {g} must be a relative path to .md files."
                    ));
                }
            }
        }
        // Settings.
        if self.settings.len() > MAX_SETTINGS {
            p.push(format!("At most {MAX_SETTINGS} settings."));
        }
        let mut keys = BTreeSet::new();
        for s in &self.settings {
            if !slug(&s.key) {
                p.push(format!(
                    "Setting key {} must be lowercase letters, digits, dashes or underscores.",
                    s.key
                ));
            }
            if !keys.insert(s.key.clone()) {
                p.push(format!("Setting {} is listed twice.", s.key));
            }
            match s.kind.as_str() {
                "string" | "secret" => {}
                "connection" => {
                    if s.provider.as_deref().map(str::is_empty).unwrap_or(true) {
                        p.push(format!("Connection setting {} needs a `provider`.", s.key));
                    }
                }
                other => p.push(format!(
                    "Setting {} has unknown type {other} (string, secret or connection).",
                    s.key
                )),
            }
            for sc in &s.scopes {
                if sc.is_empty() || sc.len() > 100 || sc.chars().any(char::is_whitespace) {
                    p.push(format!("Setting {} has a scope that isn't valid.", s.key));
                }
            }
        }
        // Tools.
        if self.tools.len() > MAX_TOOLS {
            p.push(format!("At most {MAX_TOOLS} tools."));
        }
        let mut names = BTreeSet::new();
        for t in &self.tools {
            match (&t.reference, &t.name) {
                (Some(r), None) => {
                    let Some(server) = r.strip_prefix("mcp:") else {
                        p.push(format!("Tool ref {r} must be mcp:<server>."));
                        continue;
                    };
                    if !slug(server) {
                        p.push(format!("Tool ref {r} names a server that isn't valid."));
                    }
                    if t.allow.is_empty() {
                        p.push(format!("Tool ref {r} needs an `allow` list: a plugin never gets every tool of a server."));
                    }
                    for a in &t.allow {
                        if !tool_pattern(a) || a == "*" {
                            p.push(format!("Tool ref {r} allows {a}, which isn't a tool name or a narrow pattern."));
                        }
                    }
                    if t.kind.is_some()
                        || t.method.is_some()
                        || t.url.is_some()
                        || t.connection.is_some()
                    {
                        p.push(format!(
                            "Tool ref {r} takes only `allow`, `risk` and `description`."
                        ));
                    }
                }
                (None, Some(n)) => {
                    if !slug(n) || n.contains("__") {
                        p.push(format!(
                            "Tool name {n} must be lowercase letters, digits and underscores."
                        ));
                    }
                    if !names.insert(n.clone()) || ALWAYS.contains(&n.as_str()) {
                        p.push(format!(
                            "Tool {n} is listed twice or takes a built-in name."
                        ));
                    }
                    match t.kind.as_deref() {
                        Some("http") => {
                            match t.connection.as_deref().and_then(|c| self.setting(c)) {
                                Some(s) if s.kind == "connection" => {}
                                _ => p.push(format!("HTTP tool {n} needs `connection` naming a connection setting.")),
                            }
                            let m = t.method.as_deref().unwrap_or("");
                            if !matches!(m, "GET" | "POST" | "PUT" | "PATCH" | "DELETE") {
                                p.push(format!("HTTP tool {n} needs a `method` (GET, POST, PUT, PATCH or DELETE)."));
                            }
                            match t.url.as_deref().map(url::Url::parse) {
                                Some(Ok(u)) if u.scheme() == "https" && u.host_str().is_some() && u.username().is_empty() => {}
                                _ => p.push(format!("HTTP tool {n} needs an https `url`.")),
                            }
                        }
                        Some("builtin") => match crate::tool_registry::builtin(n) {
                            Some(Risk::Read) => {}
                            Some(_) => p.push(format!("Built-in tool {n} writes; a plugin may only add read-only built-ins.")),
                            None => p.push(format!("{n} isn't a CoderHelm tool.")),
                        },
                        _ => p.push(format!("Tool {n} needs `kind`: http or builtin.")),
                    }
                }
                _ => p.push("Each tool has either `ref` (a connection's tools) or `name`.".into()),
            }
        }
        // Triggers.
        if self.triggers.len() > MAX_TRIGGERS {
            p.push(format!("At most {MAX_TRIGGERS} triggers."));
        }
        for t in &self.triggers {
            match t.on.as_str() {
                "manual" => {
                    if t.form.len() > MAX_FORM_FIELDS {
                        p.push(format!("A brief form has at most {MAX_FORM_FIELDS} fields."));
                    }
                    for (k, f) in &t.form {
                        if !slug(k) {
                            p.push(format!("Form field {k} must be lowercase letters, digits or underscores."));
                        }
                        if let FormField::Kind(kind) = f {
                            if kind != "text" {
                                p.push(format!("Form field {k} must be `text` or a list of choices."));
                            }
                        }
                    }
                }
                "schedule" => match &t.cron {
                    Some(c) if valid_cron(c) => {}
                    _ => p.push("A schedule trigger needs `cron` with five fields (minute hour day month weekday).".into()),
                },
                "github.release" => {
                    for r in &t.repos {
                        if !crate::skills::valid_repo(r) {
                            p.push(format!("Release trigger repo {r} must be owner/name."));
                        }
                    }
                }
                other => p.push(format!("Unknown trigger {other} (manual, schedule or github.release).")),
            }
            if t.on != "manual" && !t.form.is_empty() {
                p.push("Only a manual trigger has a form.".into());
            }
            if t.on != "schedule" && t.cron.is_some() {
                p.push("Only a schedule trigger has `cron`.".into());
            }
        }
        // Outputs.
        for o in &self.outputs {
            match o.kind.as_str() {
                "draft" => {
                    for k in &o.kinds {
                        if !DRAFT_KINDS.contains(&k.as_str()) {
                            p.push(format!(
                                "Draft kind {k} isn't one of {}.",
                                DRAFT_KINDS.join(", ")
                            ));
                        }
                    }
                }
                "publish" => {
                    let via = o.via.as_deref().unwrap_or("");
                    match self.named_tool(via) {
                        Some(t) if self.tool_risk(t, via) == Risk::Consequential => {}
                        Some(_) => p.push(format!("Publish output via {via} must be a consequential tool (declare `risk: consequential`).")),
                        None => p.push(format!("Publish output needs `via` naming one of the plugin's tools (got {via}).")),
                    }
                    if o.approval.as_ref().map(|a| a.min == 0).unwrap_or(false) {
                        p.push("A publish output needs at least one approval.".into());
                    }
                }
                other => p.push(format!("Unknown output {other} (draft or publish).")),
            }
        }
        if let Some(r) = &self.results {
            match r.via.strip_prefix("mcp:") {
                Some(server) if self.tools.iter().any(|t| t.server() == Some(server)) => {}
                _ => p.push("`results.via` must name an mcp: server the plugin lists.".into()),
            }
            if classify_external(&r.tool) != Risk::Read {
                p.push("`results.tool` must be a read-only tool.".into());
            }
            if r.after_days == 0 || r.after_days > 90 {
                p.push("`results.after_days` must be 1 to 90.".into());
            }
        }
        p
    }

    /// The risk a named tool runs at.
    pub fn tool_risk(&self, t: &ToolSpec, name: &str) -> Risk {
        let base = match t.kind.as_deref() {
            Some("builtin") => crate::tool_registry::builtin(name).unwrap_or(Risk::Write),
            // An HTTP call that isn't a GET changes something: at least write.
            Some("http") if t.method.as_deref() != Some("GET") => {
                classify_external(name).max(Risk::Write)
            }
            _ => classify_external(name),
        };
        match t.risk {
            Some(d) if d > base => d,
            _ => base,
        }
    }

    /// What a run may call: the always-on tools, the named tools, and each
    /// server's allowed tools as `<server>__<pattern>`.
    pub fn grant(&self) -> Grant {
        let mut tools: Vec<Granted> = ALWAYS
            .iter()
            .map(|n| Granted {
                pattern: n.to_string(),
                declared: None,
            })
            .collect();
        for t in &self.tools {
            if let Some(server) = t.server() {
                for a in &t.allow {
                    tools.push(Granted {
                        pattern: format!("{server}__{a}"),
                        declared: t.risk,
                    });
                }
            } else if let Some(n) = &t.name {
                tools.push(Granted {
                    pattern: n.clone(),
                    declared: Some(self.tool_risk(t, n)),
                });
            }
        }
        Grant { tools }
    }

    /// Every capability the manifest grants, one line each, for comparing
    /// versions. Wording (descriptions, skill and agent text, the version
    /// string, the budget) is not in it.
    pub fn grant_lines(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for t in &self.tools {
            if let Some(server) = t.server() {
                for a in &t.allow {
                    let risk = match t.risk {
                        Some(d) if d > classify_external(a) => d,
                        _ => classify_external(a),
                    };
                    out.insert(format!("tool {server}__{a} ({})", risk.as_str()));
                }
            } else if let Some(n) = &t.name {
                let mut line = format!("tool {n} ({})", self.tool_risk(t, n).as_str());
                if t.kind.as_deref() == Some("http") {
                    let host = t
                        .url
                        .as_deref()
                        .and_then(|u| url::Url::parse(u).ok())
                        .map(|u| format!("{}{}", u.host_str().unwrap_or(""), u.path()))
                        .unwrap_or_default();
                    line.push_str(&format!(
                        " {} {host} with {}",
                        t.method.as_deref().unwrap_or(""),
                        t.connection.as_deref().unwrap_or("")
                    ));
                }
                out.insert(line);
            }
        }
        for s in &self.settings {
            if s.kind == "connection" {
                out.insert(format!(
                    "connection {} ({})",
                    s.key,
                    s.provider.as_deref().unwrap_or("")
                ));
                for sc in &s.scopes {
                    out.insert(format!("scope {} on {}", sc, s.key));
                }
            }
        }
        for t in &self.triggers {
            let mut line = format!("trigger {}", t.on);
            if let Some(c) = &t.cron {
                line.push_str(&format!(" at {c}"));
            }
            if !t.repos.is_empty() {
                line.push_str(&format!(" of {}", t.repos.join(", ")));
            }
            out.insert(line);
        }
        for o in &self.outputs {
            match o.kind.as_str() {
                "publish" => {
                    out.insert(format!(
                        "output publish via {}",
                        o.via.as_deref().unwrap_or("")
                    ));
                }
                _ => {
                    for k in &o.kinds {
                        out.insert(format!("output draft {k}"));
                    }
                }
            }
        }
        if let Some(r) = &self.results {
            out.insert(format!("results via {} {}", r.via, r.tool));
        }
        out
    }
}

/// What changed in the grant from `old` to `new`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

impl GrantDiff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

/// True when `line` is covered by one of `old`: the same line, or an old
/// tool pattern that already allows this tool at the same or higher risk.
fn covered(line: &str, old: &BTreeSet<String>) -> bool {
    if old.contains(line) {
        return true;
    }
    let Some((name, risk)) = parse_tool_line(line) else {
        return false;
    };
    old.iter().any(|o| match parse_tool_line(o) {
        Some((pat, orisk)) => orisk >= risk && pat.contains('*') && glob_match(pat, name),
        None => false,
    })
}

/// `tool <name> (<risk>)` with nothing after it (MCP tools).
fn parse_tool_line(l: &str) -> Option<(&str, Risk)> {
    let rest = l.strip_prefix("tool ")?;
    let (name, risk) = rest.split_once(" (")?;
    let risk = risk.strip_suffix(')')?;
    Some((name, Risk::parse(risk)?))
}

/// The grant lines `new` adds that `old` doesn't cover. Non-empty means the
/// new version widens what the plugin may do and must wait for an admin.
pub fn widens(old: &Manifest, new: &Manifest) -> Vec<String> {
    let o = old.grant_lines();
    new.grant_lines()
        .into_iter()
        .filter(|l| !covered(l, &o))
        .collect()
}

/// Added and removed grant lines, for the dashboard's diff.
pub fn diff(old: &Manifest, new: &Manifest) -> GrantDiff {
    let (o, n) = (old.grant_lines(), new.grant_lines());
    GrantDiff {
        added: n.difference(&o).cloned().collect(),
        removed: o.difference(&n).cloned().collect(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub const MARKETING: &str = r#"
name: marketing
version: 1.2.0
description: Drafts launch posts from releases
skills: [skills/*/SKILL.md]
agents: [agents/*.md]
settings:
  - {key: brand_voice_doc, type: string}
  - {key: linkedin, type: connection, provider: oauth2, scopes: [w_member_social], sensitive: true}
tools:
  - {ref: "mcp:sanity", allow: [get_*, create_draft]}
  - {name: publish_post, kind: http, connection: linkedin, method: POST, url: "https://api.linkedin.com/rest/posts", risk: consequential}
triggers:
  - {on: github.release}
  - {on: schedule, cron: "0 14 * * MON"}
  - {on: manual, form: {brief: text, channels: [linkedin, blog, email]}}
outputs:
  - {type: draft, kinds: [blog, social, email, image]}
  - {type: publish, via: publish_post, approval: {role: marketing_editor, min: 1}}
budget: {monthly_tokens: 2000000}
"#;

    #[test]
    fn the_plan_example_parses_and_checks() {
        let m = parse(MARKETING).unwrap();
        assert_eq!(m.id(), "marketing");
        assert_eq!(m.triggers.len(), 3);
        let g = m.grant();
        assert!(g.allows("sanity__get_document"));
        assert!(g.allows("sanity__create_draft"));
        assert!(g.allows("publish_post"));
        assert!(g.allows("save_draft"));
        assert!(!g.allows("sanity__delete_document"));
        assert!(!g.allows("write_file"));
        assert_eq!(g.risk("publish_post"), Some(Risk::Consequential));
    }

    #[test]
    fn bad_manifests_list_every_problem() {
        let bad = r#"
name: Marketing
version: one
tools:
  - {ref: "mcp:sanity"}
  - {ref: "mcp:sanity", allow: ["*"]}
  - {name: publish_post, kind: http, method: POST, url: "http://x.io"}
  - {name: write_file, kind: builtin}
  - {name: nope, kind: script}
triggers:
  - {on: schedule}
  - {on: webhook}
outputs:
  - {type: publish, via: missing}
  - {type: draft, kinds: [video]}
"#;
        let p = parse(bad).unwrap_err();
        let all = p.join("\n");
        for want in [
            "`name`",
            "`version`",
            "needs an `allow` list",
            "isn't a tool name or a narrow pattern",
            "needs `connection`",
            "https `url`",
            "may only add read-only built-ins",
            "needs `kind`",
            "five fields",
            "Unknown trigger webhook",
            "naming one of the plugin's tools",
            "Draft kind video",
        ] {
            assert!(all.contains(want), "missing {want:?} in:\n{all}");
        }
    }

    #[test]
    fn unknown_fields_are_refused() {
        let p = parse("name: a\nversion: 1.0.0\nrun: rm -rf /\n").unwrap_err();
        assert!(p[0].contains("unknown field"), "{p:?}");
        let p = parse("name: a\nversion: 1.0.0\ntools:\n  - {name: x, kind: http, script: y}\n")
            .unwrap_err();
        assert!(p[0].contains("unknown field"), "{p:?}");
    }

    #[test]
    fn a_publish_output_must_go_through_a_consequential_tool() {
        let m = r#"
name: a
version: 1.0.0
settings: [{key: c, type: connection, provider: oauth2}]
tools: [{name: get_stats, kind: http, connection: c, method: GET, url: "https://x.io/stats"}]
outputs: [{type: publish, via: get_stats}]
"#;
        assert!(parse(m)
            .unwrap_err()
            .join(" ")
            .contains("must be a consequential tool"));
    }

    fn with(m: &str, f: impl FnOnce(&mut Manifest)) -> Manifest {
        let mut x = parse(m).unwrap();
        f(&mut x);
        x
    }

    #[test]
    fn wording_changes_do_not_widen() {
        let old = parse(MARKETING).unwrap();
        let new = with(MARKETING, |m| {
            m.version = "1.2.1".into();
            m.description = "New words".into();
            m.budget = Some(Budget { monthly_tokens: 9 });
            m.tools[1].description = "Posts to LinkedIn".into();
        });
        assert!(widens(&old, &new).is_empty());
        // Narrowing doesn't widen either.
        let narrower = with(MARKETING, |m| {
            m.tools[0].allow = vec!["get_document".into()]
        });
        assert!(widens(&old, &narrower).is_empty());
        assert_eq!(diff(&old, &narrower).removed.len(), 2);
    }

    #[test]
    fn new_tools_scopes_triggers_connections_and_outputs_widen() {
        let old = parse(MARKETING).unwrap();
        let cases: Vec<(&str, Manifest)> = vec![
            (
                "tool sanity__publish_document (consequential)",
                with(MARKETING, |m| {
                    m.tools[0].allow.push("publish_document".into())
                }),
            ),
            (
                "tool sanity__update_*",
                with(MARKETING, |m| m.tools[0].allow.push("update_*".into())),
            ),
            (
                "tool send_email",
                with(MARKETING, |m| {
                    let mut t = m.tools[1].clone();
                    t.name = Some("send_email".into());
                    m.tools.push(t);
                }),
            ),
            (
                "scope r_ads on linkedin",
                with(MARKETING, |m| m.settings[1].scopes.push("r_ads".into())),
            ),
            (
                "connection crm (oauth2)",
                with(MARKETING, |m| {
                    m.settings.push(Setting {
                        key: "crm".into(),
                        kind: "connection".into(),
                        provider: Some("oauth2".into()),
                        scopes: vec![],
                        sensitive: true,
                        description: String::new(),
                    })
                }),
            ),
            (
                "trigger schedule at 0 * * * *",
                with(MARKETING, |m| m.triggers[1].cron = Some("0 * * * *".into())),
            ),
            (
                "output draft page",
                with(MARKETING, |m| m.outputs[0].kinds.push("page".into())),
            ),
            (
                "api.linkedin.com/rest/shares",
                with(MARKETING, |m| {
                    m.tools[1].url = Some("https://api.linkedin.com/rest/shares".into())
                }),
            ),
        ];
        for (want, new) in cases {
            let w = widens(&old, &new);
            assert!(w.iter().any(|l| l.contains(want)), "{want}: {w:?}");
        }
    }

    #[test]
    fn a_raised_risk_widens_and_a_covered_tool_does_not() {
        let old = parse(MARKETING).unwrap();
        // get_document is already allowed by get_*.
        let same = with(MARKETING, |m| m.tools[0].allow.push("get_document".into()));
        assert!(widens(&old, &same).is_empty());
        // Declaring the reads as write raises their risk.
        let raised = with(MARKETING, |m| m.tools[0].risk = Some(Risk::Write));
        assert!(!widens(&old, &raised).is_empty());
    }

    #[test]
    fn crons_convert_to_eventbridge() {
        assert_eq!(
            eventbridge_cron("0 14 * * MON").unwrap(),
            "cron(0 14 ? * MON *)"
        );
        assert_eq!(
            eventbridge_cron("30 9 1 * *").unwrap(),
            "cron(30 9 1 * ? *)"
        );
        assert_eq!(eventbridge_cron("0 9 * * 1").unwrap(), "cron(0 9 ? * 2 *)");
        assert_eq!(
            eventbridge_cron("0 9 * * 0,6").unwrap(),
            "cron(0 9 ? * 1,7 *)"
        );
        assert!(eventbridge_cron("0 9 * *").is_none());
        assert!(eventbridge_cron("0 9 * * $(x)").is_none());
    }
}
