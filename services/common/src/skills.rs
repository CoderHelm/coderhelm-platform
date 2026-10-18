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
//!   its answer, kept for [`ASK_DAYS`] days.
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

/// Files next to a skill are kept only when they are text an agent can read.
const TEXT_EXTENSIONS: &[&str] = &[
    "md", "markdown", "txt", "json", "yaml", "yml", "toml", "csv", "sql", "py", "ts", "tsx", "js",
    "jsx", "sh", "rs", "go", "rb", "html", "css", "graphql", "gql", "xml", "ini", "cfg",
];

/// Asks the worker to read a connected repository's default branch again (a
/// push to it, a new connection, or an admin's "Sync now"). `force` reads it
/// even when the commit is the one already read.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SkillSyncMessage {
    pub team_id: String,
    pub repo: String,
    #[serde(default)]
    pub force: bool,
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

/// The front matter's name and description, and the text after it.
pub fn parse_front_matter(text: &str) -> (Option<String>, Option<String>, String) {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let Some(after) = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
    else {
        return (None, None, text.to_string());
    };
    let Some(end) = after
        .find("\n---\n")
        .or_else(|| after.find("\n---\r\n"))
        .or_else(|| after.strip_suffix("\n---").map(|s| s.len()))
    else {
        return (None, None, text.to_string());
    };
    let head = &after[..end];
    let body = after[end..]
        .trim_start_matches('\n')
        .trim_start_matches("---")
        .trim_start_matches(['\r', '\n'])
        .to_string();
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
    pub uses: i64,
    pub last_used_at: String,
    pub last_used_by: String,
    pub updated_at: String,
}

impl SkillMeta {
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
            uses: n(i, "uses"),
            last_used_at: s(i, "last_used_at"),
            last_used_by: s(i, "last_used_by"),
            updated_at: s(i, "updated_at"),
        })
        .filter(|m| !rest.is_empty() && !m.repo.is_empty() && !m.path.is_empty())
    }

    pub fn json(&self) -> Value {
        json!({
            "repo": self.repo, "path": self.path, "kind": self.kind.as_str(),
            "name": self.name, "description": self.description, "sha": self.sha,
            "hash": self.hash, "enabled": self.enabled, "files": self.files,
            "uses": self.uses, "last_used_at": self.last_used_at,
            "last_used_by": self.last_used_by, "updated_at": self.updated_at,
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
            "id": self.id, "by": self.by, "origin": self.origin,
            "question": self.question, "context_chars": self.context_chars,
            "outcome": self.outcome, "answer": self.answer, "model": self.model,
            "input_tokens": self.input_tokens, "output_tokens": self.output_tokens,
            "ms": self.ms, "at": self.at,
        })
    }
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
        for prefix in [format!("{SKILL}{repo}#"), format!("{FILE}{repo}#")] {
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
                     last_used_at, last_used_by, updated_at",
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

    pub async fn set_enabled(&self, repo: &str, path: &str, on: bool) -> Result<bool, String> {
        let r = self
            .dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{SKILL}{repo}#{path}")))
            .update_expression("SET enabled = :on")
            .condition_expression("attribute_exists(sk)")
            .expression_attribute_values(":on", AttributeValue::Bool(on))
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
        let existing: HashMap<String, String> = self
            .query(&format!("{SKILL}{repo}#"), Some("sk, #h, sha"))
            .await?
            .iter()
            .map(|i| (s(i, "sk"), format!("{}|{}", s(i, "hash"), s(i, "sha"))))
            .collect();
        let mut keep: HashSet<String> = HashSet::new();
        for it in items {
            let sk = format!("{SKILL}{repo}#{}", it.path);
            keep.insert(sk.clone());
            if existing.get(&sk) == Some(&format!("{}|{sha}", it.hash)) {
                continue;
            }
            let files_av = AttributeValue::L(it.files.iter().map(|f| av(f)).collect());
            self.dynamo
                .update_item()
                .table_name(self.table)
                .key("pk", av(self.team))
                .key("sk", av(&sk))
                .update_expression(
                    "SET repo = :r, #p = :p, kind = :k, #n = :n, description = :d, body = :b, \
                     sha = :sha, #h = :h, files = :f, updated_at = :at",
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

#[cfg(test)]
mod tests {
    use super::*;

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
            uses: 0,
            last_used_at: String::new(),
            last_used_by: String::new(),
            updated_at: String::new(),
        }
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
