//! A team's skills, kept in a repository and read by the agents.
//!
//! An admin connects a repository the team's GitHub App can read. Its default
//! branch is read (merged, reviewed code only) for:
//!
//! - `skills/<name>/SKILL.md` and `.claude/skills/<name>/SKILL.md`: a skill,
//!   plus the text files next to it (references, examples);
//! - `agents/<name>.md` and `.claude/agents/<name>.md`: an agent's brief;
//! - `commands/<name>.md` and `.claude/commands/<name>.md`: a command's steps.
//!
//! Every item is text. Nothing from a skill repository is ever run: no script,
//! no command, no new tool. An agent can only read an item, as guidance, and
//! every guard it works under stays as it is.
//!
//! Kept in the settings table, under the team:
//!
//! - `SKILLREPO#{owner/name}`: a connected repository and its last sync;
//! - `SKILL#{owner/name}#{path}`: one item, its text and its switch;
//! - `SKILLFILE#{owner/name}#{path}`: one text file next to a skill;
//! - `SKILLASK#{owner/name}#{path}#{ulid}`: one question put to an agent and
//!   its answer, kept for [`ASK_DAYS`] days;
//! - `SKILLDAY#{yyyy-mm-dd}#{owner/name}#{path}`: how many times each agent
//!   read one item that day, kept for [`STAT_KEEP_DAYS`] days.
//!
//! And one row per connected repository under the `SKILLREPO_INDEX`
//! partition (`{team}|{owner/name}`), for the periodic check.

use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

type Item = HashMap<String, AttributeValue>;

const REPO: &str = "SKILLREPO#";
const SKILL: &str = "SKILL#";
const FILE: &str = "SKILLFILE#";
const ASK: &str = "SKILLASK#";
const DAY: &str = "SKILLDAY#";
/// One read or ask, for the access log: `SKILLUSE#<ulid>`.
const USE: &str = "SKILLUSE#";
/// One partition listing every team's connected repositories, so the periodic
/// check reads a few rows instead of scanning the table.
const INDEX_PK: &str = "SKILLREPO_INDEX";

/// Largest item read (SKILL.md, an agent or a command), in bytes.
pub const MAX_ITEM_BYTES: usize = 64_000;
/// Largest file kept next to a skill, in bytes.
pub const MAX_FILE_BYTES: usize = 32_000;
/// Items kept from one repository.
pub const MAX_ITEMS: usize = 200;
/// Files kept next to one skill.
pub const MAX_FILES_PER_SKILL: usize = 20;
/// Everything kept from one repository, in bytes.
pub const MAX_REPO_BYTES: usize = 2_000_000;
/// Repositories one team can connect.
pub const MAX_REPOS: usize = 5;
/// How much of a description goes into an agent's list.
pub const DESCRIPTION_CHARS: usize = 240;
/// How long a question put to an agent, and its answer, is kept.
pub const ASK_DAYS: i64 = 30;
/// How much of a question and of an answer is kept for the history.
pub const ASK_KEEP_QUESTION_CHARS: usize = 4_000;
pub const ASK_KEEP_ANSWER_CHARS: usize = 12_000;
/// Days the usage stats cover.
pub const STAT_DAYS: i64 = 30;
/// Days a day's read counts are kept, a little past what the stats show.
pub const STAT_KEEP_DAYS: i64 = 35;
/// The agents whose reads are counted.
pub const READERS: [&str; 3] = ["planner", "coder", "reviewer"];

/// Files next to a skill are kept only when they are text an agent can read.
const TEXT_EXTENSIONS: &[&str] = &[
    "md", "markdown", "txt", "json", "yaml", "yml", "toml", "csv", "sql", "py", "ts", "tsx", "js",
    "jsx", "sh", "rs", "go", "rb", "html", "css", "graphql", "gql", "xml", "ini", "cfg",
];

/// Asks the worker to read a connected repository's default branch again (a
/// push to it, a new connection, or an admin's "Sync now"). `force` reads it
/// even when the commit is the one already read.
///
/// With `recheck`, the repository isn't read: its items are checked again
/// against what the team has in Coderhelm (one item when `path` is set).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct SkillSyncMessage {
    pub team_id: String,
    pub repo: String,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub recheck: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
}

/// Asks the worker to put an admin's question to a team agent from the
/// Skills page. The answer is written to the ask record `id` names.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SkillAskMessage {
    pub team_id: String,
    pub repo: String,
    pub path: String,
    pub id: String,
    pub question: String,
    #[serde(default)]
    pub context: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Skill,
    Agent,
    Command,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Skill => "skill",
            Kind::Agent => "agent",
            Kind::Command => "command",
        }
    }

    pub fn parse(s: &str) -> Option<Kind> {
        match s {
            "skill" => Some(Kind::Skill),
            "agent" => Some(Kind::Agent),
            "command" => Some(Kind::Command),
            _ => None,
        }
    }
}

/// `owner/name`, each part letters, digits, `-`, `_` or `.`, never `.` or `..`.
pub fn valid_repo(repo: &str) -> bool {
    let mut parts = repo.split('/');
    let (Some(owner), Some(name), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    let ok = |p: &str| {
        !p.is_empty()
            && p.len() <= 100
            && p != "."
            && p != ".."
            && p.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    ok(owner) && ok(name)
}

/// A path that stays inside the repository: relative, `/`-separated, with no
/// empty, `.` or `..` part and no backslash.
fn clean_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.contains('\0')
        && path
            .split('/')
            .all(|p| !p.is_empty() && p != "." && p != "..")
}

/// What a path in the repository is, if it is one of the files read.
/// Returns the kind, the item's name from the path, and for a skill its
/// folder.
pub fn classify(path: &str) -> Option<(Kind, String, Option<String>)> {
    if !clean_path(path) {
        return None;
    }
    let rest = path.strip_prefix(".claude/").unwrap_or(path);
    let parts: Vec<&str> = rest.split('/').collect();
    match parts.as_slice() {
        ["skills", name, "SKILL.md"] => {
            let dir = &path[..path.len() - "/SKILL.md".len()];
            Some((Kind::Skill, name.to_string(), Some(dir.to_string())))
        }
        ["agents", file] => file
            .strip_suffix(".md")
            .filter(|n| !n.is_empty())
            .map(|n| (Kind::Agent, n.to_string(), None)),
        ["commands", file] => file
            .strip_suffix(".md")
            .filter(|n| !n.is_empty())
            .map(|n| (Kind::Command, n.to_string(), None)),
        _ => None,
    }
}

fn is_text_path(path: &str) -> bool {
    path.rsplit_once('.')
        .map(|(_, ext)| TEXT_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
        .unwrap_or(false)
}

/// A file's front matter (the text between the `---` lines) and the text
/// after it. `None` when the file has no front matter.
fn split_front_matter(text: &str) -> Option<(&str, String)> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let after = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))?;
    let end = after
        .find("\n---\n")
        .or_else(|| after.find("\n---\r\n"))
        .or_else(|| after.strip_suffix("\n---").map(|s| s.len()))?;
    let head = &after[..end];
    let body = after[end..]
        .trim_start_matches('\n')
        .trim_start_matches("---")
        .trim_start_matches(['\r', '\n'])
        .to_string();
    Some((head, body))
}

/// The front matter's name and description, and the text after it.
pub fn parse_front_matter(text: &str) -> (Option<String>, Option<String>, String) {
    let Some((head, body)) = split_front_matter(text) else {
        let text = text.strip_prefix('\u{feff}').unwrap_or(text);
        return (None, None, text.to_string());
    };
    let field = |k: &str| -> Option<String> {
        if let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(head) {
            if let Some(s) = v.get(k).and_then(|x| x.as_str()) {
                return Some(s.trim().to_string()).filter(|s| !s.is_empty());
            }
        }
        // Front matter that isn't valid YAML still often has `key: value` lines.
        head.lines().find_map(|l| {
            let (key, value) = l.split_once(':')?;
            (key.trim() == k)
                .then(|| {
                    value
                        .trim()
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_string()
                })
                .filter(|s| !s.is_empty())
        })
    };
    (field("name"), field("description"), body)
}

/// The tools an item asks for in its front matter, from `tools:` and
/// `allowed-tools:`. Either may be a YAML list or one string of names split by
/// commas or spaces. A pattern such as `Bash(git status:*)` counts as its base
/// name (`Bash`); an MCP tool (`mcp__server__tool`) keeps its full name. Each
/// name appears once, in the order first asked for.
pub fn front_matter_tools(text: &str) -> Vec<String> {
    let Some((head, _)) = split_front_matter(text) else {
        return Vec::new();
    };
    let mut raw: Vec<String> = Vec::new();
    match serde_yaml::from_str::<serde_yaml::Value>(head) {
        Ok(v) if v.is_mapping() => {
            for key in ["tools", "allowed-tools"] {
                match v.get(key) {
                    Some(serde_yaml::Value::Sequence(list)) => {
                        for x in list {
                            if let Some(s) = x.as_str() {
                                raw.extend(split_tool_list(s));
                            }
                        }
                    }
                    Some(serde_yaml::Value::String(s)) => raw.extend(split_tool_list(s)),
                    _ => {}
                }
            }
        }
        // Front matter that isn't valid YAML: `key: a, b` lines, or a key
        // followed by `- name` lines.
        _ => {
            let lines: Vec<&str> = head.lines().collect();
            for (i, l) in lines.iter().enumerate() {
                let Some((key, value)) = l.split_once(':') else {
                    continue;
                };
                if l.starts_with([' ', '\t']) || !matches!(key.trim(), "tools" | "allowed-tools") {
                    continue;
                }
                let value = value.trim();
                if !value.is_empty() {
                    let value = value.trim_start_matches('[').trim_end_matches(']');
                    raw.extend(split_tool_list(value));
                    continue;
                }
                for next in &lines[i + 1..] {
                    let Some(item) = next.trim_start().strip_prefix("- ") else {
                        break;
                    };
                    raw.extend(split_tool_list(item));
                }
            }
        }
    }
    let mut seen = HashSet::new();
    raw.into_iter()
        .filter_map(|t| tool_base_name(&t))
        .filter(|t| seen.insert(t.clone()))
        .collect()
}

/// Splits `Read, Grep Bash(git status:*)` into its names: commas and spaces
/// separate them, except inside a pattern's parentheses.
fn split_tool_list(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut depth = 0usize;
    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                cur.push(c);
            }
            ',' | ' ' | '\t' if depth == 0 => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// `Bash(git status:*)` gives `Bash`. Quotes are dropped; `*` (any tool) and
/// empty names are no tool.
fn tool_base_name(t: &str) -> Option<String> {
    let t = t.trim().trim_matches(['"', '\'']).trim();
    let base = t.split('(').next().unwrap_or("").trim();
    (!base.is_empty() && base != "*").then(|| base.to_string())
}

/// Claude Code's tool names, and the Coderhelm tools that do the same job.
/// Only real equivalents are listed: Coderhelm's agents read, search, edit
/// and write a repository's files. A test in the worker checks that every
/// Coderhelm name here is a tool its coding agent is given.
pub const TOOL_EQUIVALENTS: &[(&str, &[&str])] = &[
    ("Read", &["read_file", "read_file_lines"]),
    ("Edit", &["edit_file"]),
    ("MultiEdit", &["edit_file"]),
    ("Write", &["write_file", "batch_write"]),
    ("Grep", &["search_code"]),
    ("Glob", &["read_tree", "list_directory"]),
    ("LS", &["list_directory"]),
];

/// The tools an item asks for that Coderhelm's agents don't have. A Claude
/// Code name counts as had when [`TOOL_EQUIVALENTS`] lists an equivalent, and
/// Coderhelm's own tool names count as they are. Everything else (a shell,
/// the web, sub agents, notebooks, MCP tools) is missing: Coderhelm runs the
/// item without it.
pub fn missing_tools(asked: &[String]) -> Vec<String> {
    asked
        .iter()
        .filter(|t| {
            !TOOL_EQUIVALENTS
                .iter()
                .any(|(name, ours)| name == t || ours.contains(&t.as_str()))
        })
        .cloned()
        .collect()
}

/// When there's no description, the first line of text that isn't a heading.
fn first_line(body: &str) -> String {
    body.lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap_or("")
        .to_string()
}

fn one_line(s: &str, max: usize) -> String {
    let flat = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        flat
    } else {
        let mut cut: String = flat.chars().take(max.saturating_sub(1)).collect();
        cut.push('…');
        cut
    }
}

/// An item's name an agent asks for: letters, digits, `-`, `_` and `.`.
fn tidy_name(name: &str) -> String {
    let n: String = name
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    n.trim_matches('-').chars().take(64).collect()
}

pub fn content_hash(text: &str) -> String {
    let d = Sha256::digest(text.as_bytes());
    d.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// Which files of a repository's tree to read: the items, and the text files
/// next to each skill. `tree` is every file's path and size in bytes.
#[derive(Debug, Default, PartialEq)]
pub struct Selection {
    /// Item paths, in order.
    pub items: Vec<String>,
    /// For each skill's path, the files next to it to read.
    pub files: HashMap<String, Vec<String>>,
    /// What was left out and why, for the admin to see.
    pub skipped: Vec<String>,
}

pub fn select(tree: &[(String, u64)]) -> Selection {
    let mut sel = Selection::default();
    let mut paths: Vec<&(String, u64)> = tree.iter().collect();
    paths.sort_by(|a, b| a.0.cmp(&b.0));
    let mut skill_dirs: Vec<(String, String)> = Vec::new();
    for (path, size) in &paths {
        let Some((kind, _, dir)) = classify(path) else {
            continue;
        };
        if *size as usize > MAX_ITEM_BYTES {
            sel.skipped
                .push(format!("{path}: larger than {} KB", MAX_ITEM_BYTES / 1000));
            continue;
        }
        if sel.items.len() >= MAX_ITEMS {
            sel.skipped.push(format!(
                "{path}: more than {MAX_ITEMS} items in the repository"
            ));
            continue;
        }
        sel.items.push(path.clone());
        if kind == Kind::Skill {
            if let Some(d) = dir {
                skill_dirs.push((d, path.clone()));
            }
        }
    }
    for (dir, skill_path) in skill_dirs {
        let prefix = format!("{dir}/");
        let mut files = Vec::new();
        for (path, size) in &paths {
            if !path.starts_with(&prefix) || *path == skill_path || !clean_path(path) {
                continue;
            }
            if !is_text_path(path) {
                continue;
            }
            if *size as usize > MAX_FILE_BYTES {
                sel.skipped
                    .push(format!("{path}: larger than {} KB", MAX_FILE_BYTES / 1000));
                continue;
            }
            if files.len() >= MAX_FILES_PER_SKILL {
                sel.skipped.push(format!(
                    "{path}: more than {MAX_FILES_PER_SKILL} files next to one skill"
                ));
                continue;
            }
            files.push(path.clone());
        }
        if !files.is_empty() {
            sel.files.insert(skill_path, files);
        }
    }
    sel
}

/// One item as read from the repository, ready to keep.
#[derive(Debug, Clone, PartialEq)]
pub struct Parsed {
    pub kind: Kind,
    pub name: String,
    pub description: String,
    pub path: String,
    pub body: String,
    pub hash: String,
    /// Files kept next to a skill (paths in the repository).
    pub files: Vec<String>,
    /// Tools its front matter asks for that Coderhelm's agents don't have.
    pub missing_tools: Vec<String>,
}

/// The tools an item asks for that it won't get. A team agent answers with
/// no tools at all (see [`agent_system_prompt`]), so every tool it lists is
/// missing; a skill or command is read by Coderhelm's agents, who have the
/// tools [`missing_tools`] knows.
pub fn item_missing_tools(kind: Kind, text: &str) -> Vec<String> {
    let asked = front_matter_tools(text);
    match kind {
        Kind::Agent => asked,
        Kind::Skill | Kind::Command => missing_tools(&asked),
    }
}

/// Turns an item's text into what's kept. `None` when the path isn't an item.
pub fn parse_item(path: &str, text: &str) -> Option<Parsed> {
    let (kind, path_name, _) = classify(path)?;
    let (name, description, body) = parse_front_matter(text);
    let name = tidy_name(name.as_deref().unwrap_or(&path_name));
    let name = if name.is_empty() {
        tidy_name(&path_name)
    } else {
        name
    };
    let description = one_line(
        description.as_deref().unwrap_or(&first_line(&body)),
        DESCRIPTION_CHARS,
    );
    Some(Parsed {
        kind,
        name,
        description,
        path: path.to_string(),
        hash: content_hash(text),
        body: text.to_string(),
        files: Vec::new(),
        missing_tools: item_missing_tools(kind, text),
    })
}

/// Keeps items and files in order until the repository's byte budget is used.
/// Returns what's kept and what was left out.
pub fn within_budget(
    items: Vec<Parsed>,
    files: Vec<(String, String)>,
) -> (Vec<Parsed>, Vec<(String, String)>, Vec<String>) {
    let mut used = 0usize;
    let mut skipped = Vec::new();
    let mut kept_items = Vec::new();
    for it in items {
        if used + it.body.len() > MAX_REPO_BYTES {
            skipped.push(format!(
                "{}: over the {} MB a repository may hold",
                it.path,
                MAX_REPO_BYTES / 1_000_000
            ));
            continue;
        }
        used += it.body.len();
        kept_items.push(it);
    }
    let kept_paths: HashSet<&str> = kept_items.iter().map(|i| i.path.as_str()).collect();
    let mut kept_files = Vec::new();
    for (path, text) in files {
        // A file belongs to a kept skill: its folder is a kept skill's folder.
        let owner_kept = kept_paths.iter().any(|p| {
            p.strip_suffix("SKILL.md")
                .is_some_and(|dir| path.starts_with(dir))
        });
        if !owner_kept {
            continue;
        }
        if used + text.len() > MAX_REPO_BYTES {
            skipped.push(format!(
                "{path}: over the {} MB a repository may hold",
                MAX_REPO_BYTES / 1_000_000
            ));
            continue;
        }
        used += text.len();
        kept_files.push((path, text));
    }
    for it in &mut kept_items {
        if let Some(dir) = it.path.strip_suffix("SKILL.md") {
            it.files = kept_files
                .iter()
                .filter(|(p, _)| p.starts_with(dir))
                .map(|(p, _)| p.clone())
                .collect();
        }
    }
    (kept_items, kept_files, skipped)
}

/// The file an agent asked for next to a skill, as a repository path, or
/// `None` when it would leave the skill's folder.
pub fn file_in_skill(skill_path: &str, requested: &str) -> Option<String> {
    let dir = skill_path.strip_suffix("/SKILL.md")?;
    let requested = requested.trim();
    let rel = requested
        .strip_prefix(&format!("{dir}/"))
        .unwrap_or(requested);
    if !clean_path(rel) {
        return None;
    }
    Some(format!("{dir}/{rel}"))
}

// ─── Records ────────────────────────────────────────────────

fn s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}
fn n(i: &Item, k: &str) -> i64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|x| x.parse().ok())
        .unwrap_or(0)
}
fn av(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// A connected repository and its last sync.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SkillRepo {
    pub repo: String,
    pub installation_id: u64,
    pub added_by: String,
    pub added_at: String,
    /// The commit last read. Empty before the first sync.
    pub sha: String,
    pub synced_at: String,
    /// Why the last sync failed, or empty.
    pub error: String,
    pub items: i64,
    /// What the last sync left out and why.
    pub skipped: Vec<String>,
}

impl SkillRepo {
    fn from_item(i: &Item) -> Option<Self> {
        Some(Self {
            repo: s(i, "sk").strip_prefix(REPO)?.to_string(),
            installation_id: n(i, "installation_id").max(0) as u64,
            added_by: s(i, "added_by"),
            added_at: s(i, "added_at"),
            sha: s(i, "sha"),
            synced_at: s(i, "synced_at"),
            error: s(i, "error"),
            items: n(i, "items"),
            skipped: i
                .get("skipped")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
                .unwrap_or_default(),
        })
    }

    pub fn json(&self) -> Value {
        json!({
            "repo": self.repo, "added_by": self.added_by, "added_at": self.added_at,
            "sha": self.sha, "synced_at": self.synced_at, "error": self.error,
            "items": self.items, "skipped": self.skipped,
        })
    }
}

/// One item, without its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillMeta {
    pub repo: String,
    pub path: String,
    pub kind: Kind,
    pub name: String,
    pub description: String,
    pub sha: String,
    pub hash: String,
    pub enabled: bool,
    pub files: Vec<String>,
    /// Tools its front matter asks for that Coderhelm's agents don't have.
    pub missing_tools: Vec<String>,
    pub uses: i64,
    pub last_used_at: String,
    pub last_used_by: String,
    pub updated_at: String,
    /// Questions Coderhelm's agents put to it (an admin's tries not counted).
    pub asks: i64,
    pub last_asked_at: String,
    /// Who last flipped its switch, and when. Empty if never.
    pub switched_by: String,
    pub switched_at: String,
    /// How Coderhelm runs it, from its last check (see `skill_compat`).
    pub compat: CompatMeta,
}

/// The short form of an item's last check, for lists. The full check and
/// the translated text are read with the item.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompatMeta {
    /// "ready", "translated" or "needs_tools"; empty before the first check.
    pub status: String,
    pub summary: String,
    /// Names of what it needs that the team hasn't connected.
    pub needs: Vec<String>,
    /// The item's text hash and the team's inventory fingerprint it was
    /// checked against.
    pub hash: String,
    pub fingerprint: String,
    pub at: String,
    /// Why the last check failed, or empty.
    pub error: String,
    /// An admin chose to give agents the translated version.
    pub use_translated: bool,
    pub translated_by: String,
    pub translated_at: String,
}

impl CompatMeta {
    fn from_item(i: &Item) -> Self {
        Self {
            status: s(i, "compat_status"),
            summary: s(i, "compat_summary"),
            needs: i
                .get("compat_needs")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
                .unwrap_or_default(),
            hash: s(i, "compat_hash"),
            fingerprint: s(i, "compat_fp"),
            at: s(i, "compat_at"),
            error: s(i, "compat_error"),
            use_translated: i
                .get("use_translated")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false),
            translated_by: s(i, "translated_by"),
            translated_at: s(i, "translated_at"),
        }
    }
}

impl SkillMeta {
    /// Whether the last check was of the item's current text.
    pub fn compat_current(&self) -> bool {
        !self.compat.hash.is_empty() && self.compat.hash == self.hash
    }

    /// The short check for lists, or null before any check.
    pub fn compat_json(&self) -> Value {
        if self.compat.at.is_empty() {
            return Value::Null;
        }
        json!({
            "status": self.compat.status, "summary": self.compat.summary,
            "needs": self.compat.needs, "at": self.compat.at,
            "error": self.compat.error, "current": self.compat_current(),
        })
    }

    fn from_item(i: &Item) -> Option<Self> {
        let sk = s(i, "sk");
        let rest = sk.strip_prefix(SKILL)?;
        Some(Self {
            repo: s(i, "repo"),
            path: s(i, "path"),
            kind: Kind::parse(&s(i, "kind"))?,
            name: s(i, "name"),
            description: s(i, "description"),
            sha: s(i, "sha"),
            hash: s(i, "hash"),
            // Kept switched on unless an admin turned it off.
            enabled: i
                .get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
            files: i
                .get("files")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
                .unwrap_or_default(),
            missing_tools: i
                .get("missing_tools")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
                .unwrap_or_default(),
            uses: n(i, "uses"),
            last_used_at: s(i, "last_used_at"),
            last_used_by: s(i, "last_used_by"),
            updated_at: s(i, "updated_at"),
            asks: n(i, "asks"),
            last_asked_at: s(i, "last_asked_at"),
            switched_by: s(i, "switched_by"),
            switched_at: s(i, "switched_at"),
            compat: CompatMeta::from_item(i),
        })
        .filter(|m| !rest.is_empty() && !m.repo.is_empty() && !m.path.is_empty())
    }

    pub fn json(&self) -> Value {
        json!({
            "repo": self.repo, "path": self.path, "kind": self.kind.as_str(),
            "name": self.name, "description": self.description, "sha": self.sha,
            "hash": self.hash, "enabled": self.enabled, "files": self.files,
            "missing_tools": self.missing_tools,
            "uses": self.uses, "last_used_at": self.last_used_at,
            "last_used_by": self.last_used_by, "updated_at": self.updated_at,
            "asks": self.asks, "last_asked_at": self.last_asked_at,
            "switched_by": self.switched_by, "switched_at": self.switched_at,
            "compat": self.compat_json(),
            "use_translated": self.compat.use_translated,
            "translated_by": self.compat.translated_by,
            "translated_at": self.compat.translated_at,
        })
    }
}

/// One question put to a team agent, and how it went. The context the asker
/// sent (code, a diff) is not kept, only its size.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentAsk {
    pub id: String,
    pub repo: String,
    pub path: String,
    /// Who asked: "reviewer", "coder", "planner", or "<email> (try)".
    pub by: String,
    /// The work it was asked during, e.g. "owner/name#12". Empty for a try.
    pub origin: String,
    /// The run it was asked during, for a link to its page. Empty if none.
    pub run_id: String,
    /// The commit of the agent's file that answered.
    pub sha: String,
    pub question: String,
    pub context_chars: i64,
    /// "pending", "answered", "failed" or "timed_out".
    pub outcome: String,
    pub answer: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub ms: i64,
    pub at: String,
}

impl AgentAsk {
    fn from_item(i: &Item) -> Option<Self> {
        let sk = s(i, "sk");
        let id = sk.strip_prefix(ASK)?.rsplit('#').next()?.to_string();
        Some(Self {
            id,
            repo: s(i, "repo"),
            path: s(i, "path"),
            by: s(i, "by"),
            origin: s(i, "origin"),
            run_id: s(i, "run_id"),
            sha: s(i, "sha"),
            question: s(i, "question"),
            context_chars: n(i, "context_chars"),
            outcome: s(i, "outcome"),
            answer: s(i, "answer"),
            model: s(i, "model"),
            input_tokens: n(i, "input_tokens"),
            output_tokens: n(i, "output_tokens"),
            ms: n(i, "ms"),
            at: s(i, "at"),
        })
    }

    pub fn json(&self) -> Value {
        json!({
            "id": self.id, "repo": self.repo, "path": self.path,
            "by": self.by, "origin": self.origin,
            "run_id": self.run_id, "sha": self.sha,
            "question": self.question, "context_chars": self.context_chars,
            "outcome": self.outcome, "answer": self.answer, "model": self.model,
            "input_tokens": self.input_tokens, "output_tokens": self.output_tokens,
            "ms": self.ms, "at": self.at,
        })
    }
}

/// One time an item was read or asked: who, during which work, why, and how
/// it went. Never what was read or answered; only names, ids and the reason.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SkillAccess {
    /// A ULID: sorts by time, newest last.
    pub id: String,
    pub at: String,
    pub repo: String,
    pub path: String,
    /// "skill", "agent" or "command".
    pub kind: String,
    pub name: String,
    /// "read" (the item), "read_file" (a file kept with a skill) or "ask".
    pub action: String,
    /// For `read_file`: which file, by path.
    pub file: String,
    /// "planner", "coder", "reviewer", or "<email> (try)".
    pub by: String,
    /// The work it happened during, e.g. "owner/name#12". Empty for a try.
    pub origin: String,
    /// The run it happened during, for a link. Empty for a review or a try.
    pub run_id: String,
    /// Why, in the reader's words: the reason it gave, its question, or the
    /// task's title. Clipped and redacted.
    pub why: String,
    /// For reads: "ok", "off" (switched off or removed), "limit" (the pass
    /// used all its reads) or "failed". For asks: "answered", "failed",
    /// "timed_out", "limit" or "off".
    pub outcome: String,
    /// For an ask: its id, to open the question and answer.
    pub ask_id: String,
    pub ms: i64,
}

/// How long access records are kept.
pub const ACCESS_DAYS: i64 = 180;
/// Longest reason kept on an access record.
pub const ACCESS_WHY_CHARS: usize = 300;
/// Records looked at for one page of a filtered list, so a filter that
/// matches little can't read the whole log in one request.
pub const ACCESS_SCAN_CAP: usize = 3_000;

/// Whether an outcome means it went wrong.
pub fn access_problem(outcome: &str) -> bool {
    !matches!(outcome, "ok" | "answered" | "pending")
}

/// What to show of the access log. Empty fields match everything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessFilter {
    pub repo: String,
    pub path: String,
    /// "planner", "coder", "reviewer", or "people" for anyone else.
    pub by: String,
    /// "read" (either kind of read) or "ask".
    pub action: String,
    /// An outcome, or "problems" for any that went wrong.
    pub outcome: String,
    /// Words to find in the name, reason, work, run or file.
    pub q: String,
    /// RFC 3339 bounds, inclusive.
    pub since: String,
    pub until: String,
}

impl AccessFilter {
    pub fn matches(&self, a: &SkillAccess) -> bool {
        if !self.repo.is_empty() && a.repo != self.repo {
            return false;
        }
        if !self.path.is_empty() && a.path != self.path {
            return false;
        }
        match self.by.as_str() {
            "" => {}
            "people" if READERS.contains(&a.by.as_str()) => return false,
            "people" => {}
            who if a.by != who => return false,
            _ => {}
        }
        match self.action.as_str() {
            "" => {}
            "read" if !a.action.starts_with("read") => return false,
            "ask" if a.action != "ask" => return false,
            _ => {}
        }
        match self.outcome.as_str() {
            "" => {}
            "problems" if !access_problem(&a.outcome) => return false,
            "problems" => {}
            o if a.outcome != o => return false,
            _ => {}
        }
        let q = self.q.trim().to_lowercase();
        if !q.is_empty() {
            let hay = format!(
                "{} {}:{} {} {} {} {} {}",
                a.name, a.kind, a.name, a.path, a.why, a.origin, a.run_id, a.file
            )
            .to_lowercase();
            if !q.split_whitespace().all(|w| hay.contains(w)) {
                return false;
            }
        }
        true
    }

    /// The sort key range the time bounds cover, as `(low, high)`.
    fn key_range(&self) -> (String, String) {
        let ms = |t: &str| {
            chrono::DateTime::parse_from_rfc3339(t)
                .ok()
                .map(|d| d.timestamp_millis().max(0) as u64)
        };
        let low = ms(&self.since)
            .map(|m| format!("{USE}{}", ulid::Ulid::from_parts(m, 0)))
            .unwrap_or_else(|| USE.to_string());
        let high = ms(&self.until)
            .map(|m| format!("{USE}{}", ulid::Ulid::from_parts(m, u128::MAX)))
            .unwrap_or_else(|| format!("{USE}~"));
        (low, high)
    }
}

impl SkillAccess {
    fn from_item(i: &Item) -> Option<Self> {
        let id = s(i, "sk").strip_prefix(USE)?.to_string();
        Some(Self {
            id,
            at: s(i, "at"),
            repo: s(i, "repo"),
            path: s(i, "path"),
            kind: s(i, "kind"),
            name: s(i, "name"),
            action: s(i, "action"),
            file: s(i, "file"),
            by: s(i, "by"),
            origin: s(i, "origin"),
            run_id: s(i, "run_id"),
            why: s(i, "why"),
            outcome: s(i, "outcome"),
            ask_id: s(i, "ask_id"),
            ms: n(i, "ms"),
        })
    }

    pub fn json(&self) -> Value {
        json!({
            "id": self.id, "at": self.at, "repo": self.repo, "path": self.path,
            "kind": self.kind, "name": self.name, "action": self.action,
            "file": self.file, "by": self.by, "origin": self.origin,
            "run_id": self.run_id, "why": self.why, "outcome": self.outcome,
            "ask_id": self.ask_id, "ms": self.ms, "cursor": format!("{USE}{}", self.id),
        })
    }
}

/// The reason kept on an access record: whitespace folded, secrets taken
/// out, clipped.
pub fn access_why(text: &str) -> String {
    let folded = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let (clean, _) = crate::live::redact::redact_text(&folded);
    crate::truncate_str(&clean, ACCESS_WHY_CHARS).to_string()
}

/// Counts of the access log over a window, for the summary at the top of
/// the activity pages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessCounts {
    pub reads: i64,
    pub asks: i64,
    pub read_problems: i64,
    pub ask_problems: i64,
}

impl AccessCounts {
    pub fn add(&mut self, action: &str, outcome: &str) {
        let problem = access_problem(outcome);
        if action == "ask" {
            self.asks += 1;
            self.ask_problems += i64::from(problem);
        } else {
            self.reads += 1;
            self.read_problems += i64::from(problem);
        }
    }
}

/// How much of an agent's file it is given as instructions.
pub const AGENT_BRIEF_CHARS: usize = 24_000;

/// The instructions a team agent answers under, and its `model:` hint. Only
/// the text after the front matter is used: the file's `tools:`, `model:` and
/// any other settings never reach the model as instructions. The Skills page
/// shows this exact text, so what an admin sees is what the agent gets.
pub fn agent_system_prompt(brief: &str, asker: &str) -> (Option<String>, String) {
    let (_, _, body) = parse_front_matter(brief);
    let hint = front_matter_value(brief, "model").map(|v| v.to_ascii_lowercase());
    let system = format!(
        "You are one of this team's own agents, asked a question by Coderhelm's {}. Answer from \
         your brief below and the question and context you're given. You have no tools: you \
         can't read files, run commands or look anything up, so if you'd need something you \
         weren't given, say what. Keep the answer focused, under about 600 words.\n\n\
         ## Your brief\n{}",
        asker,
        crate::truncate_str(&body, AGENT_BRIEF_CHARS)
    );
    (hint, system)
}

/// One `key: value` from a file's front matter, unquoted. `None` when the
/// file has no front matter or the key is missing or empty.
pub fn front_matter_value(text: &str, key: &str) -> Option<String> {
    let rest = text.strip_prefix("---")?;
    let end = rest.find("\n---")?;
    rest[..end].lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        (k.trim() == key)
            .then(|| v.trim().trim_matches('"').trim_matches('\'').to_string())
            .filter(|v| !v.is_empty())
    })
}

/// A new ask id. ULIDs sort by time, so the newest asks come first.
pub fn new_ask_id() -> String {
    ulid::Ulid::new().to_string()
}

/// The name agents ask for: `kind:name`, with the repository added when two
/// connected repositories share one.
pub fn handles(items: &[SkillMeta]) -> Vec<String> {
    let mut count: HashMap<String, usize> = HashMap::new();
    for m in items {
        *count
            .entry(format!("{}:{}", m.kind.as_str(), m.name))
            .or_default() += 1;
    }
    items
        .iter()
        .map(|m| {
            let base = format!("{}:{}", m.kind.as_str(), m.name);
            if count[&base] > 1 {
                format!("{}@{}", base, m.repo)
            } else {
                base
            }
        })
        .collect()
}

/// The items agents may read: switched on, from a connected repository.
pub fn usable(items: Vec<SkillMeta>, repos: &[SkillRepo]) -> Vec<SkillMeta> {
    let connected: HashSet<&str> = repos.iter().map(|r| r.repo.as_str()).collect();
    items
        .into_iter()
        .filter(|m| m.enabled && connected.contains(m.repo.as_str()))
        .collect()
}

pub struct SkillStore<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
    pub team: &'a str,
}

impl SkillStore<'_> {
    async fn get(&self, sk: &str) -> Result<Option<Item>, String> {
        self.dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(sk))
            .send()
            .await
            .map(|o| o.item().cloned())
            .map_err(|e| format!("skill store get: {e}"))
    }

    async fn query(&self, prefix: &str, projection: Option<&str>) -> Result<Vec<Item>, String> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let mut q = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :p AND begins_with(sk, :s)")
                .expression_attribute_values(":p", av(self.team))
                .expression_attribute_values(":s", av(prefix))
                .set_exclusive_start_key(start);
            if let Some(p) = projection {
                q = q.projection_expression(p);
                // DynamoDB refuses a name the expression doesn't use.
                if p.contains("#n") {
                    q = q.expression_attribute_names("#n", "name");
                }
                if p.contains("#p") {
                    q = q.expression_attribute_names("#p", "path");
                }
                if p.contains("#h") {
                    q = q.expression_attribute_names("#h", "hash");
                }
            }
            let r = q
                .send()
                .await
                .map_err(|e| format!("skill store query: {e}"))?;
            out.extend(r.items().iter().cloned());
            start = r.last_evaluated_key().cloned();
            if start.is_none() {
                return Ok(out);
            }
        }
    }

    async fn delete_key(&self, sk: &str) -> Result<(), String> {
        self.dynamo
            .delete_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(sk))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("skill store delete: {e}"))
    }

    pub async fn repos(&self) -> Result<Vec<SkillRepo>, String> {
        let mut v: Vec<SkillRepo> = self
            .query(REPO, None)
            .await?
            .iter()
            .filter_map(SkillRepo::from_item)
            .collect();
        v.sort_by(|a, b| a.repo.cmp(&b.repo));
        Ok(v)
    }

    pub async fn repo(&self, repo: &str) -> Result<Option<SkillRepo>, String> {
        Ok(self
            .get(&format!("{REPO}{repo}"))
            .await?
            .as_ref()
            .and_then(SkillRepo::from_item))
    }

    /// Connects a repository. Keeps its last sync when it was already connected.
    pub async fn connect(&self, repo: &str, installation_id: u64, by: &str) -> Result<(), String> {
        if !valid_repo(repo) {
            return Err("That isn't an owner/name repository.".into());
        }
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{REPO}{repo}")))
            .update_expression(
                "SET installation_id = :i, added_by = if_not_exists(added_by, :by), \
                 added_at = if_not_exists(added_at, :at)",
            )
            .expression_attribute_values(":i", AttributeValue::N(installation_id.to_string()))
            .expression_attribute_values(":by", av(by))
            .expression_attribute_values(":at", av(&now()))
            .send()
            .await
            .map_err(|e| format!("skill store connect: {e}"))?;
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", av(INDEX_PK))
            .item("sk", av(&format!("{}|{repo}", self.team)))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("skill store index: {e}"))
    }

    /// Disconnects a repository and forgets everything read from it.
    pub async fn disconnect(&self, repo: &str) -> Result<(), String> {
        self.dynamo
            .delete_item()
            .table_name(self.table)
            .key("pk", av(INDEX_PK))
            .key("sk", av(&format!("{}|{repo}", self.team)))
            .send()
            .await
            .map_err(|e| format!("skill store index: {e}"))?;
        self.delete_key(&format!("{REPO}{repo}")).await?;
        for prefix in [
            format!("{SKILL}{repo}#"),
            format!("{FILE}{repo}#"),
            format!("{ASK}{repo}#"),
        ] {
            for i in self.query(&prefix, Some("sk")).await? {
                self.delete_key(&s(&i, "sk")).await?;
            }
        }
        Ok(())
    }

    /// Every item (without its text), sorted by kind then name.
    pub async fn items(&self) -> Result<Vec<SkillMeta>, String> {
        let mut v: Vec<SkillMeta> = self
            .query(
                SKILL,
                Some(
                    "sk, repo, #p, kind, #n, description, sha, #h, enabled, files, uses, \
                     last_used_at, last_used_by, updated_at, asks, last_asked_at, \
                     switched_by, switched_at, missing_tools, compat_status, compat_summary, \
                     compat_needs, compat_hash, compat_fp, compat_at, compat_error, \
                     use_translated, translated_by, translated_at",
                ),
            )
            .await?
            .iter()
            .filter_map(SkillMeta::from_item)
            .collect();
        v.sort_by(|a, b| {
            (a.kind.as_str(), &a.name, &a.repo).cmp(&(b.kind.as_str(), &b.name, &b.repo))
        });
        Ok(v)
    }

    /// One item with its text.
    pub async fn item(
        &self,
        repo: &str,
        path: &str,
    ) -> Result<Option<(SkillMeta, String)>, String> {
        let Some(i) = self.get(&format!("{SKILL}{repo}#{path}")).await? else {
            return Ok(None);
        };
        Ok(SkillMeta::from_item(&i).map(|m| (m, s(&i, "body"))))
    }

    /// A text file kept next to a skill.
    pub async fn file(&self, repo: &str, path: &str) -> Result<Option<String>, String> {
        Ok(self
            .get(&format!("{FILE}{repo}#{path}"))
            .await?
            .map(|i| s(&i, "body")))
    }

    pub async fn set_enabled(
        &self,
        repo: &str,
        path: &str,
        on: bool,
        by: &str,
    ) -> Result<bool, String> {
        let r = self
            .dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{SKILL}{repo}#{path}")))
            .update_expression("SET enabled = :on, switched_by = :by, switched_at = :at")
            .condition_expression("attribute_exists(sk)")
            .expression_attribute_values(":on", AttributeValue::Bool(on))
            .expression_attribute_values(":by", av(by))
            .expression_attribute_values(":at", av(&now()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(e) => Err(format!("skill store switch: {e}")),
        }
    }

    /// One item with its text, its full last check (JSON, empty before one)
    /// and its translated text (empty before one).
    pub async fn item_with_compat(
        &self,
        repo: &str,
        path: &str,
    ) -> Result<Option<(SkillMeta, String, String, String)>, String> {
        let Some(i) = self.get(&format!("{SKILL}{repo}#{path}")).await? else {
            return Ok(None);
        };
        Ok(SkillMeta::from_item(&i)
            .map(|m| (m, s(&i, "body"), s(&i, "compat"), s(&i, "translated"))))
    }

    /// Every item of `repo` with what a check reads: its text, its files and
    /// what it was last checked against.
    pub async fn compat_queue(&self, repo: &str) -> Result<Vec<(SkillMeta, String)>, String> {
        Ok(self
            .query(&format!("{SKILL}{repo}#"), None)
            .await?
            .iter()
            .filter_map(|i| SkillMeta::from_item(i).map(|m| (m, s(i, "body"))))
            .collect())
    }

    /// Keeps one item's check: the full check as JSON, its short form for
    /// lists, the translated text, and what it was checked against. Only
    /// written while the item still has the text that was checked. With an
    /// error, keeps why the check failed; the last check stays unless
    /// `drop_old` (it was of other text or another inventory, so showing it
    /// would mislead).
    #[allow(clippy::too_many_arguments)]
    pub async fn put_compat(
        &self,
        repo: &str,
        path: &str,
        hash: &str,
        fingerprint: &str,
        result: Result<(&crate::skill_compat::Analysis, &str), (&str, bool)>,
    ) -> Result<bool, String> {
        let mut req = self
            .dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{SKILL}{repo}#{path}")))
            .condition_expression("attribute_exists(sk) AND #h = :h")
            .expression_attribute_names("#h", "hash")
            .expression_attribute_values(":h", av(hash))
            .expression_attribute_values(":fp", av(fingerprint))
            .expression_attribute_values(":at", av(&now()));
        req = match result {
            Ok((a, translated)) => req
                .update_expression(
                    "SET compat = :c, compat_status = :st, compat_summary = :su, \
                     compat_needs = :n, translated = :t, compat_hash = :h, compat_fp = :fp, \
                     compat_at = :at, compat_error = :e",
                )
                .expression_attribute_values(
                    ":c",
                    av(&serde_json::to_string(a).map_err(|e| format!("compat json: {e}"))?),
                )
                .expression_attribute_values(":st", av(&a.status))
                .expression_attribute_values(":su", av(&a.summary))
                .expression_attribute_values(
                    ":n",
                    AttributeValue::L(a.needs.iter().map(|n| av(&n.name)).collect()),
                )
                .expression_attribute_values(":t", av(translated))
                .expression_attribute_values(":e", av("")),
            Err((error, drop_old)) => req
                .update_expression(if drop_old {
                    "SET compat_error = :e, compat_hash = :h, compat_fp = :fp, compat_at = :at \
                     REMOVE compat, compat_status, compat_summary, compat_needs, translated"
                } else {
                    "SET compat_error = :e, compat_hash = :h, compat_fp = :fp, compat_at = :at"
                })
                .expression_attribute_values(":e", av(crate::truncate_str(error, 500))),
        };
        match req.send().await {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(e) => Err(format!("skill store compat: {e}")),
        }
    }

    /// An admin's choice to give agents the translated version of an item.
    pub async fn set_use_translated(
        &self,
        repo: &str,
        path: &str,
        on: bool,
        by: &str,
    ) -> Result<bool, String> {
        let r = self
            .dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{SKILL}{repo}#{path}")))
            .update_expression("SET use_translated = :on, translated_by = :by, translated_at = :at")
            .condition_expression("attribute_exists(sk)")
            .expression_attribute_values(":on", AttributeValue::Bool(on))
            .expression_attribute_values(":by", av(by))
            .expression_attribute_values(":at", av(&now()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(e) => Err(format!("skill store translated switch: {e}")),
        }
    }

    /// Counts one question Coderhelm's agents put to an agent.
    pub async fn count_ask(&self, repo: &str, path: &str) -> Result<(), String> {
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{SKILL}{repo}#{path}")))
            .update_expression("ADD asks :one SET last_asked_at = :at")
            .condition_expression("attribute_exists(sk)")
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":at", av(&now()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("skill store ask count: {e}"))
    }

    /// Counts one read by an agent, so admins see which items are used.
    pub async fn record_use(&self, repo: &str, path: &str, by: &str) -> Result<(), String> {
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{SKILL}{repo}#{path}")))
            .update_expression("ADD uses :one SET last_used_at = :at, last_used_by = :by")
            .condition_expression("attribute_exists(sk)")
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":at", av(&now()))
            .expression_attribute_values(":by", av(by))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("skill store use: {e}"))
    }

    /// Counts one read of an item by one of [`READERS`] on today's row.
    pub async fn count_read(&self, repo: &str, path: &str, reader: &str) -> Result<(), String> {
        if !READERS.contains(&reader) {
            return Err(format!("skill store read count: unknown reader {reader}"));
        }
        let today = chrono::Utc::now().format("%Y-%m-%d");
        let ttl = chrono::Utc::now().timestamp() + STAT_KEEP_DAYS * 86_400;
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{DAY}{today}#{repo}#{path}")))
            .update_expression("ADD #r :one SET repo = :repo, #p = :path, #t = :ttl")
            .expression_attribute_names("#r", reader)
            .expression_attribute_names("#p", "path")
            .expression_attribute_names("#t", "ttl")
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":repo", av(repo))
            .expression_attribute_values(":path", av(path))
            .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("skill store read count: {e}"))
    }

    /// Every day's read counts from `since` (yyyy-mm-dd) on.
    pub async fn read_days(&self, since: &str) -> Result<Vec<ReadDay>, String> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let r = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :p AND sk BETWEEN :a AND :b")
                .expression_attribute_values(":p", av(self.team))
                .expression_attribute_values(":a", av(&format!("{DAY}{since}")))
                // '$' sorts after '#' and every digit, closing the range.
                .expression_attribute_values(":b", av(&format!("{DAY}$")))
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(|e| format!("skill store read days: {e}"))?;
            out.extend(r.items().iter().filter_map(ReadDay::from_item));
            start = r.last_evaluated_key().cloned();
            if start.is_none() {
                break;
            }
        }
        Ok(out)
    }

    /// Every ask still kept (the last [`ASK_DAYS`] days), for the stats.
    pub async fn recent_asks(&self) -> Result<Vec<AgentAsk>, String> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let r = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :p AND begins_with(sk, :s)")
                .expression_attribute_values(":p", av(self.team))
                .expression_attribute_values(":s", av(ASK))
                .projection_expression(
                    "sk, repo, #p, #b, outcome, input_tokens, output_tokens, ms, #a",
                )
                .expression_attribute_names("#p", "path")
                .expression_attribute_names("#b", "by")
                .expression_attribute_names("#a", "at")
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(|e| format!("skill store recent asks: {e}"))?;
            out.extend(r.items().iter().filter_map(AgentAsk::from_item));
            start = r.last_evaluated_key().cloned();
            if start.is_none() {
                break;
            }
        }
        Ok(out)
    }

    fn ask_key(repo: &str, path: &str, id: &str) -> String {
        format!("{ASK}{repo}#{path}#{id}")
    }

    /// Writes one ask, new or finished. The question and answer are kept
    /// clipped, and the record expires after [`ASK_DAYS`] days.
    pub async fn put_ask(&self, a: &AgentAsk) -> Result<(), String> {
        let ttl = chrono::Utc::now().timestamp() + ASK_DAYS * 86_400;
        let num = |v: i64| AttributeValue::N(v.to_string());
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", av(self.team))
            .item("sk", av(&Self::ask_key(&a.repo, &a.path, &a.id)))
            .item("repo", av(&a.repo))
            .item("path", av(&a.path))
            .item("by", av(&a.by))
            .item("origin", av(&a.origin))
            .item("run_id", av(&a.run_id))
            .item("sha", av(&a.sha))
            .item(
                "question",
                av(crate::truncate_str(&a.question, ASK_KEEP_QUESTION_CHARS)),
            )
            .item("context_chars", num(a.context_chars))
            .item("outcome", av(&a.outcome))
            .item(
                "answer",
                av(crate::truncate_str(&a.answer, ASK_KEEP_ANSWER_CHARS)),
            )
            .item("model", av(&a.model))
            .item("input_tokens", num(a.input_tokens))
            .item("output_tokens", num(a.output_tokens))
            .item("ms", num(a.ms))
            .item(
                "at",
                av(if a.at.is_empty() { now() } else { a.at.clone() }.as_str()),
            )
            .item("ttl", num(ttl))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("skill store ask: {e}"))
    }

    /// One ask, for the Skills page to wait on a try.
    pub async fn ask(&self, repo: &str, path: &str, id: &str) -> Result<Option<AgentAsk>, String> {
        Ok(self
            .get(&Self::ask_key(repo, path, id))
            .await?
            .as_ref()
            .and_then(AgentAsk::from_item))
    }

    /// An agent's most recent asks, newest first.
    pub async fn asks(&self, repo: &str, path: &str, limit: i32) -> Result<Vec<AgentAsk>, String> {
        let r = self
            .dynamo
            .query()
            .table_name(self.table)
            .key_condition_expression("pk = :p AND begins_with(sk, :s)")
            .expression_attribute_values(":p", av(self.team))
            .expression_attribute_values(":s", av(&format!("{ASK}{repo}#{path}#")))
            .scan_index_forward(false)
            .limit(limit.clamp(1, 50))
            .send()
            .await
            .map_err(|e| format!("skill store asks: {e}"))?;
        Ok(r.items().iter().filter_map(AgentAsk::from_item).collect())
    }

    /// Every ask made during one run, oldest first, for the run's page. Asks
    /// expire after 30 days, so this reads a bounded set.
    pub async fn asks_for_run(&self, run_id: &str) -> Result<Vec<AgentAsk>, String> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let r = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :p AND begins_with(sk, :s)")
                .filter_expression("run_id = :r")
                .expression_attribute_values(":p", av(self.team))
                .expression_attribute_values(":s", av(ASK))
                .expression_attribute_values(":r", av(run_id))
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(|e| format!("skill store run asks: {e}"))?;
            out.extend(r.items().iter().filter_map(AgentAsk::from_item));
            start = r.last_evaluated_key().cloned();
            if start.is_none() {
                break;
            }
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    /// Records one read or ask on the access log, kept [`ACCESS_DAYS`] days.
    /// The reason is clipped and redacted here, whoever calls.
    pub async fn put_access(&self, a: &SkillAccess) -> Result<(), String> {
        let id = if a.id.is_empty() {
            new_ask_id()
        } else {
            a.id.clone()
        };
        let ttl = chrono::Utc::now().timestamp() + ACCESS_DAYS * 86_400;
        let mut req = self
            .dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", av(self.team))
            .item("sk", av(&format!("{USE}{id}")))
            .item(
                "at",
                av(if a.at.is_empty() { now() } else { a.at.clone() }.as_str()),
            )
            .item("repo", av(&a.repo))
            .item("path", av(&a.path))
            .item("kind", av(&a.kind))
            .item("name", av(&a.name))
            .item("action", av(&a.action))
            .item("by", av(&a.by))
            .item("outcome", av(&a.outcome))
            .item("why", av(&access_why(&a.why)))
            .item("ms", AttributeValue::N(a.ms.to_string()))
            .item("ttl", AttributeValue::N(ttl.to_string()));
        for (k, v) in [
            ("file", &a.file),
            ("origin", &a.origin),
            ("run_id", &a.run_id),
            ("ask_id", &a.ask_id),
        ] {
            if !v.is_empty() {
                req = req.item(k, av(crate::truncate_str(v, 300)));
            }
        }
        req.send()
            .await
            .map(|_| ())
            .map_err(|e| format!("skill store access: {e}"))
    }

    /// One page of the access log, newest first, matching `f`. `before` is
    /// the `cursor` of the last record already shown. Returns the page and
    /// the cursor to read further back, or `None` at the end. A page can come
    /// back short with a cursor when a filter matched little of what was
    /// looked at ([`ACCESS_SCAN_CAP`]).
    pub async fn access_page(
        &self,
        f: &AccessFilter,
        limit: usize,
        before: Option<&str>,
    ) -> Result<(Vec<SkillAccess>, Option<String>), String> {
        let limit = limit.clamp(1, 200);
        let (low, high) = f.key_range();
        let mut start: Option<String> = before
            .filter(|b| b.starts_with(USE) && *b <= high.as_str())
            .map(str::to_string);
        let mut out = Vec::new();
        let mut looked = 0usize;
        loop {
            let mut q = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :p AND sk BETWEEN :lo AND :hi")
                .expression_attribute_values(":p", av(self.team))
                .expression_attribute_values(":lo", av(&low))
                .expression_attribute_values(":hi", av(&high))
                .scan_index_forward(false)
                .limit(200);
            if let Some(sk) = &start {
                q = q
                    .exclusive_start_key("pk", av(self.team))
                    .exclusive_start_key("sk", av(sk));
            }
            let r = q
                .send()
                .await
                .map_err(|e| format!("skill store access log: {e}"))?;
            for i in r.items() {
                looked += 1;
                let sk = s(i, "sk");
                if let Some(a) = SkillAccess::from_item(i).filter(|a| f.matches(a)) {
                    out.push(a);
                }
                if out.len() >= limit || looked >= ACCESS_SCAN_CAP {
                    return Ok((out, Some(sk)));
                }
            }
            match r.last_evaluated_key().and_then(|k| k.get("sk")) {
                Some(AttributeValue::S(sk)) => start = Some(sk.clone()),
                _ => return Ok((out, None)),
            }
        }
    }

    /// Counts of reads and asks since `since` (RFC 3339).
    pub async fn access_counts(&self, since: &str) -> Result<AccessCounts, String> {
        let (low, high) = AccessFilter {
            since: since.to_string(),
            ..Default::default()
        }
        .key_range();
        let mut counts = AccessCounts::default();
        let mut start = None;
        loop {
            let r = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :p AND sk BETWEEN :lo AND :hi")
                .expression_attribute_values(":p", av(self.team))
                .expression_attribute_values(":lo", av(&low))
                .expression_attribute_values(":hi", av(&high))
                .projection_expression("#a, outcome")
                .expression_attribute_names("#a", "action")
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(|e| format!("skill store access counts: {e}"))?;
            for i in r.items() {
                counts.add(&s(i, "action"), &s(i, "outcome"));
            }
            start = r.last_evaluated_key().cloned();
            if start.is_none() {
                return Ok(counts);
            }
        }
    }

    /// Records a failed sync without touching what was read before.
    pub async fn sync_failed(&self, repo: &str, error: &str) -> Result<(), String> {
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{REPO}{repo}")))
            .update_expression("SET #e = :e, synced_at = :at")
            .condition_expression("attribute_exists(sk)")
            .expression_attribute_names("#e", "error")
            .expression_attribute_values(":e", av(error))
            .expression_attribute_values(":at", av(&now()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("skill store sync failed: {e}"))
    }

    /// Replaces what was read from `repo` with this sync's items and files:
    /// changed ones are written, ones no longer in the repository removed.
    /// An admin's switch on an item is kept across syncs.
    pub async fn apply_sync(
        &self,
        repo: &str,
        sha: &str,
        items: &[Parsed],
        files: &[(String, String)],
        skipped: &[String],
    ) -> Result<(), String> {
        // The repository can be disconnected while a sync runs; then nothing
        // is written.
        if self.repo(repo).await?.is_none() {
            return Ok(());
        }
        let at = now();
        // Each kept item's text and commit, and the tools it was last found
        // to be missing (`None` for an item kept before that was worked out).
        let existing: HashMap<String, (String, Option<Vec<String>>)> = self
            .query(
                &format!("{SKILL}{repo}#"),
                Some("sk, #h, sha, missing_tools"),
            )
            .await?
            .iter()
            .map(|i| {
                let missing = i.get("missing_tools").and_then(|v| v.as_l().ok()).map(|l| {
                    l.iter()
                        .filter_map(|x| x.as_s().ok().cloned())
                        .collect::<Vec<_>>()
                });
                (
                    s(i, "sk"),
                    (format!("{}|{}", s(i, "hash"), s(i, "sha")), missing),
                )
            })
            .collect();
        let mut keep: HashSet<String> = HashSet::new();
        for it in items {
            let sk = format!("{SKILL}{repo}#{}", it.path);
            keep.insert(sk.clone());
            let missing_av = AttributeValue::L(it.missing_tools.iter().map(|t| av(t)).collect());
            if let Some((same, missing)) = existing.get(&sk) {
                if *same == format!("{}|{sha}", it.hash) {
                    if missing.as_ref() != Some(&it.missing_tools) {
                        // Same text, but its missing tools were never kept or
                        // Coderhelm's tools changed: only those are written.
                        self.dynamo
                            .update_item()
                            .table_name(self.table)
                            .key("pk", av(self.team))
                            .key("sk", av(&sk))
                            .update_expression("SET missing_tools = :m")
                            .condition_expression("attribute_exists(sk)")
                            .expression_attribute_values(":m", missing_av)
                            .send()
                            .await
                            .map_err(|e| format!("skill store write: {e}"))?;
                    }
                    continue;
                }
            }
            let files_av = AttributeValue::L(it.files.iter().map(|f| av(f)).collect());
            self.dynamo
                .update_item()
                .table_name(self.table)
                .key("pk", av(self.team))
                .key("sk", av(&sk))
                .update_expression(
                    "SET repo = :r, #p = :p, kind = :k, #n = :n, description = :d, body = :b, \
                     sha = :sha, #h = :h, files = :f, missing_tools = :m, updated_at = :at",
                )
                .expression_attribute_names("#h", "hash")
                .expression_attribute_names("#p", "path")
                .expression_attribute_names("#n", "name")
                .expression_attribute_values(":r", av(repo))
                .expression_attribute_values(":p", av(&it.path))
                .expression_attribute_values(":k", av(it.kind.as_str()))
                .expression_attribute_values(":n", av(&it.name))
                .expression_attribute_values(":d", av(&it.description))
                .expression_attribute_values(":b", av(&it.body))
                .expression_attribute_values(":sha", av(sha))
                .expression_attribute_values(":h", av(&it.hash))
                .expression_attribute_values(":f", files_av)
                .expression_attribute_values(":m", missing_av)
                .expression_attribute_values(":at", av(&at))
                .send()
                .await
                .map_err(|e| format!("skill store write: {e}"))?;
        }
        for sk in existing.keys().filter(|k| !keep.contains(*k)) {
            self.delete_key(sk).await?;
        }
        let old_files: HashSet<String> = self
            .query(&format!("{FILE}{repo}#"), Some("sk"))
            .await?
            .iter()
            .map(|i| s(i, "sk"))
            .collect();
        let mut keep_files = HashSet::new();
        for (path, text) in files {
            let sk = format!("{FILE}{repo}#{path}");
            keep_files.insert(sk.clone());
            self.dynamo
                .put_item()
                .table_name(self.table)
                .item("pk", av(self.team))
                .item("sk", av(&sk))
                .item("body", av(text))
                .item("sha", av(sha))
                .send()
                .await
                .map_err(|e| format!("skill store file: {e}"))?;
        }
        for sk in old_files.iter().filter(|k| !keep_files.contains(*k)) {
            self.delete_key(sk).await?;
        }
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{REPO}{repo}")))
            .update_expression(
                "SET sha = :sha, synced_at = :at, #e = :none, #i = :n, skipped = :sk",
            )
            .condition_expression("attribute_exists(sk)")
            .expression_attribute_names("#e", "error")
            .expression_attribute_names("#i", "items")
            .expression_attribute_values(":sha", av(sha))
            .expression_attribute_values(":at", av(&at))
            .expression_attribute_values(":none", av(""))
            .expression_attribute_values(":n", AttributeValue::N(items.len().to_string()))
            .expression_attribute_values(
                ":sk",
                AttributeValue::L(skipped.iter().take(50).map(|x| av(x)).collect()),
            )
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("skill store sync result: {e}"))
    }
}

/// Every connected skill repository across teams, as `(team, repo)`. For the
/// periodic check that a repository's default branch hasn't moved.
pub async fn all_repos(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
) -> Result<Vec<(String, SkillRepo)>, String> {
    let mut keys = Vec::new();
    let mut start = None;
    loop {
        let r = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :p")
            .expression_attribute_values(":p", av(INDEX_PK))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("skill repo index: {e}"))?;
        keys.extend(r.items().iter().filter_map(|i| {
            let sk = s(i, "sk");
            let (team, repo) = sk.split_once('|')?;
            Some((team.to_string(), repo.to_string()))
        }));
        start = r.last_evaluated_key().cloned();
        if start.is_none() {
            break;
        }
    }
    let mut out = Vec::new();
    for (team, repo) in keys {
        let st = SkillStore {
            dynamo,
            table,
            team: &team,
        };
        if let Some(r) = st.repo(&repo).await? {
            out.push((team.clone(), r));
        }
    }
    Ok(out)
}

/// One item's reads on one day, by each of [`READERS`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadDay {
    pub date: String,
    pub repo: String,
    pub path: String,
    pub planner: i64,
    pub coder: i64,
    pub reviewer: i64,
}

impl ReadDay {
    fn from_item(i: &Item) -> Option<Self> {
        let sk = s(i, "sk");
        let date = sk.strip_prefix(DAY)?.split('#').next()?.to_string();
        Some(Self {
            date,
            repo: s(i, "repo"),
            path: s(i, "path"),
            planner: n(i, "planner"),
            coder: n(i, "coder"),
            reviewer: n(i, "reviewer"),
        })
    }

    fn total(&self) -> i64 {
        self.planner + self.coder + self.reviewer
    }

    fn by(&self, reader: &str) -> i64 {
        match reader {
            "planner" => self.planner,
            "coder" => self.coder,
            "reviewer" => self.reviewer,
            _ => 0,
        }
    }
}

/// How a team's skills and agents were used over the last [`STAT_DAYS`] days
/// up to `today`: per day, per agent reading, and per item. Only items still
/// kept count, so a removed item's leftover rows drop out. An admin's tries
/// are counted apart from the asks Coderhelm's agents made.
pub fn usage_stats(
    items: &[SkillMeta],
    reads: &[ReadDay],
    asks: &[AgentAsk],
    today: chrono::NaiveDate,
) -> Value {
    let first = today - chrono::Duration::days(STAT_DAYS - 1);
    let in_window = |date: &str| {
        chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .map(|d| d >= first && d <= today)
            .unwrap_or(false)
    };
    let known: HashMap<(&str, &str), &SkillMeta> = items
        .iter()
        .map(|m| ((m.repo.as_str(), m.path.as_str()), m))
        .collect();
    let reads: Vec<&ReadDay> = reads
        .iter()
        .filter(|r| in_window(&r.date) && known.contains_key(&(r.repo.as_str(), r.path.as_str())))
        .collect();
    let ask_day = |a: &AgentAsk| a.at.get(..10).unwrap_or("").to_string();
    let (tries, asks): (Vec<&AgentAsk>, Vec<&AgentAsk>) = asks
        .iter()
        .filter(|a| {
            in_window(&ask_day(a)) && known.contains_key(&(a.repo.as_str(), a.path.as_str()))
        })
        .partition(|a| a.by.ends_with("(try)"));
    let answered = |a: &AgentAsk| a.outcome == "answered";
    let finished = |a: &AgentAsk| a.outcome != "pending";

    let days: Vec<Value> = (0..STAT_DAYS)
        .map(|i| {
            let d = (first + chrono::Duration::days(i))
                .format("%Y-%m-%d")
                .to_string();
            let on = |a: &&&AgentAsk| ask_day(a) == d;
            json!({
                "date": d,
                "reads": reads.iter().filter(|r| r.date == d).map(|r| r.total()).sum::<i64>(),
                "asks": asks.iter().filter(on).count(),
                "answered": asks.iter().filter(on).filter(|a| answered(a)).count(),
            })
        })
        .collect();
    let by_reader: Vec<Value> = READERS
        .iter()
        .map(|who| {
            json!({
                "reader": who,
                "reads": reads.iter().map(|r| r.by(who)).sum::<i64>(),
                "asks": asks.iter().filter(|a| a.by == *who).count(),
            })
        })
        .collect();

    let mut per: HashMap<(&str, &str), (i64, usize, usize)> = HashMap::new();
    for r in &reads {
        per.entry((r.repo.as_str(), r.path.as_str())).or_default().0 += r.total();
    }
    for a in &asks {
        let e = per.entry((a.repo.as_str(), a.path.as_str())).or_default();
        e.1 += 1;
        e.2 += usize::from(answered(a));
    }
    let mut top: Vec<(&SkillMeta, (i64, usize, usize))> = per
        .iter()
        .filter_map(|(k, v)| known.get(k).map(|m| (*m, *v)))
        .collect();
    top.sort_by(|(am, a), (bm, b)| {
        (b.0 + b.1 as i64)
            .cmp(&(a.0 + a.1 as i64))
            .then_with(|| am.name.cmp(&bm.name))
    });
    let unused: Vec<&SkillMeta> = items
        .iter()
        .filter(|m| m.enabled && !per.contains_key(&(m.repo.as_str(), m.path.as_str())))
        .collect();
    let brief = |m: &SkillMeta| json!({ "repo": m.repo, "path": m.path, "name": m.name, "kind": m.kind.as_str() });
    let mut ms: Vec<i64> = asks.iter().filter(|a| answered(a)).map(|a| a.ms).collect();
    ms.sort_unstable();

    json!({
        "days_covered": STAT_DAYS,
        "since": first.format("%Y-%m-%d").to_string(),
        "reads": reads.iter().map(|r| r.total()).sum::<i64>(),
        "asks": asks.len(),
        "answered": asks.iter().filter(|a| answered(a)).count(),
        "no_answer": asks.iter().filter(|a| finished(a) && !answered(a)).count(),
        "tries": tries.len(),
        "input_tokens": asks.iter().map(|a| a.input_tokens).sum::<i64>(),
        "output_tokens": asks.iter().map(|a| a.output_tokens).sum::<i64>(),
        "median_answer_ms": ms.get(ms.len() / 2).copied(),
        "days": days,
        "by_reader": by_reader,
        "top": top.iter().take(8).map(|(m, (reads, asks, answered))| {
            let mut v = brief(m);
            v["reads"] = json!(reads);
            v["asks"] = json!(asks);
            v["answered"] = json!(answered);
            v
        }).collect::<Vec<_>>(),
        "unused_count": unused.len(),
        "unused": unused.iter().take(12).map(|m| brief(m)).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn access(by: &str, action: &str, outcome: &str) -> SkillAccess {
        SkillAccess {
            repo: "acme/playbook".into(),
            path: "skills/debug/SKILL.md".into(),
            kind: "skill".into(),
            name: "debug".into(),
            action: action.into(),
            by: by.into(),
            origin: "acme/web#12".into(),
            why: "The flaky checkout test".into(),
            outcome: outcome.into(),
            ..Default::default()
        }
    }

    #[test]
    fn the_access_filter_matches_reader_action_outcome_and_words() {
        let read = access("coder", "read", "ok");
        let file = access("planner", "read_file", "off");
        let ask = access("a@b.co (try)", "ask", "timed_out");
        let all = AccessFilter::default();
        assert!(all.matches(&read) && all.matches(&file) && all.matches(&ask));

        let by = |b: &str| AccessFilter {
            by: b.into(),
            ..Default::default()
        };
        assert!(by("coder").matches(&read));
        assert!(!by("coder").matches(&file));
        assert!(
            by("people").matches(&ask),
            "a person is anyone but an agent"
        );
        assert!(!by("people").matches(&read));

        let action = |x: &str| AccessFilter {
            action: x.into(),
            ..Default::default()
        };
        assert!(action("read").matches(&file), "a file read is a read");
        assert!(!action("read").matches(&ask));
        assert!(action("ask").matches(&ask));

        let problems = AccessFilter {
            outcome: "problems".into(),
            ..Default::default()
        };
        assert!(!problems.matches(&read));
        assert!(problems.matches(&file) && problems.matches(&ask));

        let words = |q: &str| AccessFilter {
            q: q.into(),
            ..Default::default()
        };
        assert!(words("FLAKY checkout").matches(&read));
        assert!(words("skill:debug").matches(&read));
        assert!(words("web#12").matches(&read));
        assert!(
            !words("flaky deploy").matches(&read),
            "every word must be found"
        );

        let other = AccessFilter {
            repo: "acme/playbook".into(),
            path: "agents/other.md".into(),
            ..Default::default()
        };
        assert!(!other.matches(&read));
    }

    #[test]
    fn the_access_key_range_follows_the_time_bounds() {
        let (lo, hi) = AccessFilter::default().key_range();
        assert_eq!((lo.as_str(), hi.as_str()), ("SKILLUSE#", "SKILLUSE#~"));
        let f = AccessFilter {
            since: "2026-10-01T00:00:00Z".into(),
            until: "2026-10-02T00:00:00Z".into(),
            ..Default::default()
        };
        let (lo, hi) = f.key_range();
        let at = |t: &str| {
            let ms = chrono::DateTime::parse_from_rfc3339(t)
                .unwrap()
                .timestamp_millis() as u64;
            format!("SKILLUSE#{}", ulid::Ulid::from_parts(ms, 12345))
        };
        let inside = at("2026-10-01T12:00:00Z");
        assert!(lo < inside && inside < hi);
        assert!(at("2026-09-30T23:59:59Z") < lo);
        assert!(at("2026-10-02T00:00:01Z") > hi);
    }

    #[test]
    fn a_reason_is_folded_clipped_and_counts_add_up() {
        let long = format!("Why:\n  {}", "word ".repeat(200));
        let w = access_why(&long);
        assert!(w.len() <= ACCESS_WHY_CHARS);
        assert!(w.starts_with("Why: word word"));
        assert!(!w.contains('\n'));

        let mut c = AccessCounts::default();
        c.add("read", "ok");
        c.add("read_file", "limit");
        c.add("ask", "answered");
        c.add("ask", "failed");
        c.add("ask", "pending");
        assert_eq!(
            c,
            AccessCounts {
                reads: 2,
                asks: 3,
                read_problems: 1,
                ask_problems: 1
            }
        );
        let a = SkillAccess {
            id: "01J".into(),
            ..access("coder", "read", "ok")
        };
        assert_eq!(a.json()["cursor"], "SKILLUSE#01J");
    }

    #[test]
    fn usage_stats_count_reads_and_asks_in_the_window_only() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let items = vec![
            meta("acme/playbook", Kind::Skill, "api-conventions", true),
            meta("acme/playbook", Kind::Agent, "spec-checker", true),
            meta("acme/playbook", Kind::Agent, "copy-tone", true),
        ];
        let read = |date: &str, path: &str, coder: i64, reviewer: i64| ReadDay {
            date: date.into(),
            repo: "acme/playbook".into(),
            path: path.into(),
            coder,
            reviewer,
            ..Default::default()
        };
        let reads = vec![
            read("2026-03-31", "skills/api-conventions.md", 2, 1),
            read("2026-03-02", "skills/api-conventions.md", 1, 0),
            // Before the 30 days, and for an item since removed: both left out.
            read("2026-03-01", "skills/api-conventions.md", 9, 0),
            read("2026-03-31", "skills/gone.md", 5, 0),
        ];
        let ask = |by: &str, outcome: &str, ms: i64| AgentAsk {
            repo: "acme/playbook".into(),
            path: "agents/spec-checker.md".into(),
            by: by.into(),
            outcome: outcome.into(),
            ms,
            at: "2026-03-30T10:00:00Z".into(),
            ..Default::default()
        };
        let asks = vec![
            ask("reviewer", "answered", 3000),
            ask("planner", "answered", 5000),
            ask("coder", "timed_out", 0),
            ask("admin@example.com (try)", "answered", 1000),
        ];
        let v = usage_stats(&items, &reads, &asks, today);
        assert_eq!(v["since"], "2026-03-02");
        assert_eq!(v["days"].as_array().unwrap().len(), 30);
        assert_eq!(v["reads"], 4);
        assert_eq!(v["asks"], 3);
        assert_eq!(v["answered"], 2);
        assert_eq!(v["no_answer"], 1);
        assert_eq!(v["tries"], 1);
        assert_eq!(v["median_answer_ms"], 5000);
        assert_eq!(
            v["days"][29],
            json!({"date": "2026-03-31", "reads": 3, "asks": 0, "answered": 0})
        );
        assert_eq!(v["days"][28]["asks"], 3);
        let coder = &v["by_reader"][1];
        assert_eq!(
            (
                coder["reader"].as_str(),
                coder["reads"].as_i64(),
                coder["asks"].as_i64()
            ),
            (Some("coder"), Some(3), Some(1))
        );
        assert_eq!(v["top"][0]["name"], "api-conventions");
        assert_eq!(v["top"][1]["name"], "spec-checker");
        assert_eq!(v["top"][1]["answered"], 2);
        assert_eq!(v["unused_count"], 1);
        assert_eq!(v["unused"][0]["name"], "copy-tone");
    }

    #[test]
    fn repo_names_are_owner_slash_name() {
        assert!(valid_repo("acme/playbook"));
        assert!(valid_repo("acme-co/play_book.v2"));
        for bad in [
            "acme",
            "acme/",
            "/playbook",
            "acme/play/book",
            "acme/..",
            "../playbook",
            "acme/play book",
            "acme/play%2fbook",
        ] {
            assert!(!valid_repo(bad), "{bad}");
        }
    }

    #[test]
    fn only_the_known_layouts_are_read() {
        assert_eq!(
            classify("skills/debug/SKILL.md"),
            Some((Kind::Skill, "debug".into(), Some("skills/debug".into())))
        );
        assert_eq!(
            classify(".claude/skills/debug/SKILL.md"),
            Some((
                Kind::Skill,
                "debug".into(),
                Some(".claude/skills/debug".into())
            ))
        );
        assert_eq!(
            classify("agents/triager.md"),
            Some((Kind::Agent, "triager".into(), None))
        );
        assert_eq!(
            classify(".claude/commands/release.md"),
            Some((Kind::Command, "release".into(), None))
        );
        for no in [
            "README.md",
            "skills/debug/notes.md",
            "skills/SKILL.md",
            "skills/a/b/SKILL.md",
            "agents/nested/x.md",
            "agents/.md",
            "commands/run.sh",
            "src/skills/debug/SKILL.md",
            "skills/../SKILL.md",
            "/skills/debug/SKILL.md",
            "skills/de\\bug/SKILL.md",
        ] {
            assert_eq!(classify(no), None, "{no}");
        }
    }

    #[test]
    fn front_matter_gives_name_and_description() {
        let (name, desc, body) = parse_front_matter(
            "---\nname: Debug Things\ndescription: Find the cause before the fix.\n---\n# Debug\nSteps",
        );
        assert_eq!(name.as_deref(), Some("Debug Things"));
        assert_eq!(desc.as_deref(), Some("Find the cause before the fix."));
        assert_eq!(body, "# Debug\nSteps");
        // Not valid YAML (an unquoted colon), still read line by line.
        let (name, desc, _) =
            parse_front_matter("---\nname: x\ndescription: Use when: things break\n---\nbody");
        assert_eq!(name.as_deref(), Some("x"));
        assert_eq!(desc.as_deref(), Some("Use when: things break"));
        // No front matter.
        let (name, desc, body) = parse_front_matter("# Title\ntext");
        assert_eq!((name, desc), (None, None));
        assert_eq!(body, "# Title\ntext");
        // An opening line with no close isn't front matter.
        let (name, _, _) = parse_front_matter("---\nname: x\nno end");
        assert_eq!(name, None);
    }

    fn tools(front: &str) -> Vec<String> {
        front_matter_tools(&format!("---\n{front}\n---\nbody"))
    }

    #[test]
    fn front_matter_tools_reads_lists_and_strings() {
        // A YAML list.
        assert_eq!(
            tools("name: a\ntools:\n  - Read\n  - Grep\n  - WebFetch"),
            ["Read", "Grep", "WebFetch"]
        );
        // A flow list.
        assert_eq!(tools("tools: [Read, Bash]"), ["Read", "Bash"]);
        // One string, split by commas or spaces.
        assert_eq!(tools("tools: Read, Grep,Glob"), ["Read", "Grep", "Glob"]);
        assert_eq!(tools("allowed-tools: Read Write"), ["Read", "Write"]);
        // Both keys, each name once.
        assert_eq!(
            tools("tools: Read, Grep\nallowed-tools: [Grep, Edit]"),
            ["Read", "Grep", "Edit"]
        );
    }

    #[test]
    fn front_matter_tools_reduces_patterns_and_keeps_mcp_names() {
        assert_eq!(
            tools("allowed-tools: Bash(git status:*), Bash(git diff:*), Read"),
            ["Bash", "Read"]
        );
        assert_eq!(
            tools("allowed-tools:\n  - \"Bash(npm run test:*)\"\n  - mcp__tracker__get_issue"),
            ["Bash", "mcp__tracker__get_issue"]
        );
        assert_eq!(
            tools("tools: mcp__docs__search mcp__docs__fetch"),
            ["mcp__docs__search", "mcp__docs__fetch"]
        );
    }

    #[test]
    fn front_matter_tools_when_there_are_none() {
        assert!(tools("name: a\ndescription: b").is_empty());
        assert!(tools("tools:").is_empty());
        assert!(tools("tools: \"*\"").is_empty());
        assert!(front_matter_tools("# No front matter\ntools: Bash").is_empty());
        assert!(front_matter_tools("---\ntools: Bash\nno end").is_empty());
    }

    #[test]
    fn front_matter_tools_reads_front_matter_that_isnt_yaml() {
        // An unquoted `: ` in the description makes this invalid YAML.
        assert_eq!(
            tools("description: Use when: tests fail\ntools: Read, Bash(git log:*)"),
            ["Read", "Bash"]
        );
        assert_eq!(
            tools("description: Use when: tests fail\nallowed-tools:\n  - Grep\n  - WebSearch\nmodel: x"),
            ["Grep", "WebSearch"]
        );
        assert_eq!(
            tools("description: a: b\ntools: [Read, Task]"),
            ["Read", "Task"]
        );
    }

    #[test]
    fn missing_tools_are_the_ones_without_an_equivalent() {
        let asked: Vec<String> = [
            "Read",
            "Edit",
            "MultiEdit",
            "Write",
            "Grep",
            "Glob",
            "LS",
            "read_file",
            "Bash",
            "WebFetch",
            "WebSearch",
            "Task",
            "Agent",
            "NotebookEdit",
            "TodoWrite",
            "mcp__tracker__get_issue",
            "read",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            missing_tools(&asked),
            [
                "Bash",
                "WebFetch",
                "WebSearch",
                "Task",
                "Agent",
                "NotebookEdit",
                "TodoWrite",
                "mcp__tracker__get_issue",
                "read",
            ]
        );
        assert!(missing_tools(&[]).is_empty());
    }

    #[test]
    fn a_parsed_item_keeps_its_missing_tools() {
        let text =
            "---\nname: fix\nallowed-tools: Read, Edit, Bash(cargo test:*), WebFetch\n---\nSteps";
        let p = parse_item("commands/fix.md", text).unwrap();
        assert_eq!(p.missing_tools, ["Bash", "WebFetch"]);
        let p = parse_item("skills/fix/SKILL.md", text).unwrap();
        assert_eq!(p.missing_tools, ["Bash", "WebFetch"]);
        // A team agent answers with no tools, so all of them are missing.
        let p = parse_item("agents/fix.md", text).unwrap();
        assert_eq!(p.missing_tools, ["Read", "Edit", "Bash", "WebFetch"]);
        let p = parse_item("agents/plain.md", "---\nname: plain\n---\nAnswer.").unwrap();
        assert!(p.missing_tools.is_empty());
    }

    #[test]
    fn missing_tools_reach_the_api() {
        let mut m = meta("acme/playbook", Kind::Command, "fix", true);
        m.missing_tools = vec!["Bash".into()];
        assert_eq!(m.json()["missing_tools"], json!(["Bash"]));
    }

    #[test]
    fn an_item_without_front_matter_uses_its_path_and_first_line() {
        let p = parse_item(
            "commands/Merge Rate.md",
            "# Merge rate\n\nCharts the merge rate.\n",
        )
        .unwrap();
        assert_eq!(p.kind, Kind::Command);
        assert_eq!(p.name, "merge-rate");
        assert_eq!(p.description, "Charts the merge rate.");
        let long = format!("---\ndescription: {}\n---\n", "word ".repeat(200));
        let p = parse_item("agents/a.md", &long).unwrap();
        assert!(p.description.chars().count() <= DESCRIPTION_CHARS);
        assert!(parse_item("README.md", "x").is_none());
    }

    #[test]
    fn selection_takes_items_and_text_files_next_to_skills() {
        let tree: Vec<(String, u64)> = [
            ("skills/debug/SKILL.md", 500),
            ("skills/debug/reference.md", 900),
            ("skills/debug/scripts/check.py", 400),
            ("skills/debug/diagram.png", 400),
            ("skills/debug/huge.md", 900_000),
            ("agents/triager.md", 300),
            ("commands/release.md", 300),
            ("commands/too-big.md", 100_000),
            ("src/main.rs", 100),
        ]
        .iter()
        .map(|(p, s)| (p.to_string(), *s))
        .collect();
        let sel = select(&tree);
        assert_eq!(
            sel.items,
            vec![
                "agents/triager.md",
                "commands/release.md",
                "skills/debug/SKILL.md"
            ]
        );
        assert_eq!(
            sel.files["skills/debug/SKILL.md"],
            vec!["skills/debug/reference.md", "skills/debug/scripts/check.py"]
        );
        assert!(sel
            .skipped
            .iter()
            .any(|s| s.starts_with("commands/too-big.md")));
        assert!(sel
            .skipped
            .iter()
            .any(|s| s.starts_with("skills/debug/huge.md")));
    }

    #[test]
    fn selection_stops_at_the_item_and_file_caps() {
        let mut tree: Vec<(String, u64)> = (0..MAX_ITEMS + 5)
            .map(|i| (format!("commands/c{i:04}.md"), 10))
            .collect();
        tree.push(("skills/s/SKILL.md".into(), 10));
        let sel = select(&tree);
        assert_eq!(sel.items.len(), MAX_ITEMS);
        assert!(sel.skipped.len() >= 5);
        let tree: Vec<(String, u64)> = std::iter::once(("skills/s/SKILL.md".to_string(), 10))
            .chain((0..MAX_FILES_PER_SKILL + 3).map(|i| (format!("skills/s/f{i:03}.md"), 10)))
            .collect();
        let sel = select(&tree);
        assert_eq!(sel.files["skills/s/SKILL.md"].len(), MAX_FILES_PER_SKILL);
    }

    #[test]
    fn the_repository_budget_drops_what_doesnt_fit() {
        let big = "x".repeat(MAX_REPO_BYTES / 2 + 10);
        let items = vec![
            parse_item("commands/a.md", &big).unwrap(),
            parse_item("commands/b.md", &big).unwrap(),
            parse_item("skills/s/SKILL.md", "small").unwrap(),
        ];
        let files = vec![
            ("skills/s/ref.md".to_string(), "ref".to_string()),
            ("skills/gone/ref.md".to_string(), "orphan".to_string()),
        ];
        let (kept, kept_files, skipped) = within_budget(items, files);
        let paths: Vec<&str> = kept.iter().map(|i| i.path.as_str()).collect();
        assert_eq!(paths, vec!["commands/a.md", "skills/s/SKILL.md"]);
        assert_eq!(skipped.len(), 1);
        assert_eq!(kept_files.len(), 1);
        assert_eq!(kept[1].files, vec!["skills/s/ref.md"]);
    }

    #[test]
    fn a_file_request_cant_leave_the_skill_folder() {
        let skill = "skills/debug/SKILL.md";
        assert_eq!(
            file_in_skill(skill, "reference.md").as_deref(),
            Some("skills/debug/reference.md")
        );
        assert_eq!(
            file_in_skill(skill, "skills/debug/scripts/check.py").as_deref(),
            Some("skills/debug/scripts/check.py")
        );
        for bad in [
            "../other/SKILL.md",
            "/etc/passwd",
            "scripts/../../x.md",
            "a\\b.md",
            "",
            "./x.md",
        ] {
            assert_eq!(file_in_skill(skill, bad), None, "{bad}");
        }
        assert_eq!(file_in_skill("agents/a.md", "x.md"), None);
    }

    fn meta(repo: &str, kind: Kind, name: &str, enabled: bool) -> SkillMeta {
        SkillMeta {
            repo: repo.into(),
            path: format!("{}s/{name}.md", kind.as_str()),
            kind,
            name: name.into(),
            description: String::new(),
            sha: "abc".into(),
            hash: "h".into(),
            enabled,
            files: vec![],
            missing_tools: vec![],
            uses: 0,
            last_used_at: String::new(),
            last_used_by: String::new(),
            updated_at: String::new(),
            asks: 0,
            last_asked_at: String::new(),
            switched_by: String::new(),
            switched_at: String::new(),
            compat: Default::default(),
        }
    }

    #[test]
    fn the_prompt_an_agent_gets_leaves_its_settings_out() {
        let brief = "---\nname: x\ntools: Bash, Write\nmodel: \"Opus\"\n---\nCheck the spec.\n";
        let (hint, system) = agent_system_prompt(brief, "reviewer");
        assert_eq!(hint.as_deref(), Some("opus"));
        assert!(system.contains("Coderhelm's reviewer"));
        assert!(system.ends_with("## Your brief\nCheck the spec.\n"));
        assert!(!system.contains("tools: Bash"));
        assert_eq!(
            front_matter_value(brief, "tools").as_deref(),
            Some("Bash, Write")
        );
        assert_eq!(front_matter_value("no front matter", "tools"), None);
    }

    #[test]
    fn switched_off_and_disconnected_items_are_hidden_from_agents() {
        let repos = vec![SkillRepo {
            repo: "acme/playbook".into(),
            ..Default::default()
        }];
        let got = usable(
            vec![
                meta("acme/playbook", Kind::Skill, "on", true),
                meta("acme/playbook", Kind::Skill, "off", false),
                meta("acme/gone", Kind::Skill, "orphan", true),
            ],
            &repos,
        );
        let names: Vec<&str> = got.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, vec!["on"]);
    }

    #[test]
    fn handles_are_kind_and_name_and_add_the_repo_on_a_clash() {
        let items = vec![
            meta("acme/playbook", Kind::Skill, "debug", true),
            meta("acme/playbook", Kind::Command, "debug", true),
            meta("acme/other", Kind::Skill, "debug", true),
        ];
        assert_eq!(
            handles(&items),
            vec![
                "skill:debug@acme/playbook",
                "command:debug",
                "skill:debug@acme/other"
            ]
        );
    }
}
