//! The six built-in bases every team has with nothing to set up (Code,
//! Work, Infrastructure, Logs, Security, Knowledge; `lookups::BASES`): who
//! may read each, how much is in it and how fresh it is.
//!
//! **One permission decision** ([`decide`]) for every door a read comes
//! through: Chat, the Knowledge page, share links and the AI tools on
//! `/mcp`. A base is open to a person when
//!
//! 1. their role opens what the base holds in the dashboard (the same
//!    `permissions::resolve_access` check the pages make),
//! 2. the base's audience includes them ([`Audience`]: roles, people, and
//!    whether readers from AI tools may read it), and it isn't paused,
//! 3. on `/mcp`, the connection holds the base's scope.
//!
//! Items inside a base are then mirrored where a mapping exists: code is
//! read only from repositories the person's linked GitHub account can read
//! ([`visible_repos`]). Anything withheld is counted, never named.
//!
//! Storage, in the settings table, all under the team's own key:
//! - `pk = <team>`, `sk = KB_BUILTIN#<key>`: a base's audience, when an
//!   admin changed it (otherwise [`default_audience`]);
//! - `pk = <team>`, `sk = KB_READERS`: whether people may connect AI tools
//!   as readers without an account;
//! - `pk = <team>`, `sk = GH_REPO_ACCESS#<login>`: which repositories a
//!   GitHub login could read, checked within [`ACCESS_TTL_SECS`].
//!
//! Nothing here is cached in the process: every read names its team.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use common::repo_read::RepoInfo;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::warn;

use crate::lookups::{self, Base, BASES};
use crate::permissions::{resolve_access, Access, Holder};
use crate::routes::chat_work;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

pub const AUDIENCE_PREFIX: &str = "KB_BUILTIN#";
pub const READERS_SK: &str = "KB_READERS";
pub const REPO_ACCESS_PREFIX: &str = "GH_REPO_ACCESS#";
/// A person's GitHub repository access is checked again after this long.
pub const ACCESS_TTL_SECS: i64 = 600;
/// Most repositories checked for one person in one read; the rest wait for
/// the next read (and are withheld meanwhile).
pub const MAX_ACCESS_CHECKS: usize = 60;
/// Most rows read to count one kind of record.
const MAX_COUNT_ROWS: usize = 5_000;

/// The role a person connected from an AI tool gets: read-only, not a seat
/// (`routes::auth`, connected readers).
pub const READER_ROLE: &str = "reader";

/// The dashboard route that decides reading code and the team's documents:
/// reading knowledge, which every member may do (`brain_read`).
pub const READ_ROUTE: &str = "knowledge_ask::search_base";

// ── Audiences ───────────────────────────────────────────────────────────────

/// Who may read a base. It only narrows what roles already open: widening
/// past a role is done on the role, so the dashboard and the bases agree.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Audience {
    /// Roles that may read it (`member`, `viewer`, `custom:<id>`, ...).
    /// Empty: every role that opens what it holds.
    #[serde(default)]
    pub roles: Vec<String>,
    /// People named by email, whatever their role.
    #[serde(default)]
    pub people: Vec<String>,
    /// Readers connected from AI tools may read it.
    #[serde(default)]
    pub readers: bool,
    /// Nobody reads it while paused.
    #[serde(default)]
    pub paused: bool,
}

/// What a base starts with: everyone whose role opens it; readers from AI
/// tools read code, work and the team's documents, never AWS, logs or
/// security findings until an admin says so.
pub fn default_audience(key: &str) -> Audience {
    Audience {
        readers: matches!(key, lookups::CODE | lookups::WORK | lookups::TEAM),
        ..Default::default()
    }
}

fn is_admin(role: &str) -> bool {
    matches!(role, "owner" | "admin")
}

impl Audience {
    /// Whether the audience names this person. Owners and admins are always
    /// in it, so nobody can lock the team's admins out.
    pub fn open_to(&self, role: &str, email: &str) -> bool {
        let named = self
            .people
            .iter()
            .any(|p| !email.is_empty() && p.eq_ignore_ascii_case(email.trim()));
        if is_admin(role) || named {
            return true;
        }
        if role == READER_ROLE {
            return self.readers;
        }
        self.roles.is_empty() || self.roles.iter().any(|r| r == role)
    }

    /// The audience in a few words, as the Knowledge page shows it.
    pub fn label(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if self.roles.is_empty() {
            parts.push("Everyone whose role opens it".into());
        } else {
            parts.push(format!("Admins and {}", self.roles.join(", ")));
        }
        if !self.people.is_empty() {
            parts.push(format!(
                "{} {} by name",
                self.people.len(),
                if self.people.len() == 1 {
                    "person"
                } else {
                    "people"
                }
            ));
        }
        parts.push(if self.readers {
            "readers from AI tools".into()
        } else {
            "not readers from AI tools".into()
        });
        let mut s = parts.join("; ");
        if self.paused {
            s = format!("Paused. {s}");
        }
        s
    }

    /// Checks an edit: known roles, plain emails, at most 200 names.
    pub fn checked(mut self) -> Result<Audience, String> {
        self.roles.retain(|r| !r.trim().is_empty());
        for r in &mut self.roles {
            *r = r.trim().to_string();
            let known = crate::permissions::VALID_ROLES.contains(&r.as_str())
                || crate::permissions::custom_role_id(r).is_some();
            if !known {
                return Err(format!("\"{r}\" isn't a role."));
            }
        }
        self.roles.sort();
        self.roles.dedup();
        let mut people = Vec::new();
        for p in &self.people {
            let p = p.trim().to_ascii_lowercase();
            if p.is_empty() {
                continue;
            }
            if !p.contains('@') || p.len() > 254 || p.contains(char::is_whitespace) {
                return Err(format!("\"{p}\" isn't an email address."));
            }
            people.push(p);
        }
        people.sort();
        people.dedup();
        if people.len() + self.roles.len() > 200 {
            return Err("Name at most 200 roles and people.".into());
        }
        self.people = people;
        Ok(self)
    }
}

// ── The decision ────────────────────────────────────────────────────────────

/// Whether a person's role opens what a base holds, as the dashboard pages
/// decide it.
pub fn role_reads(holder: &Holder, base: &str) -> bool {
    let route_ok = |h: &str| matches!(resolve_access(holder, Some(h)), Access::Allow { .. });
    match base {
        lookups::CODE | lookups::TEAM => route_ok(READ_ROUTE),
        b => chat_work::KINDS
            .iter()
            .filter(|k| lookups::base_of(k.key).key == b)
            .any(|k| route_ok(k.handler)),
    }
}

/// Who is asking.
#[derive(Debug, Clone)]
pub struct Asker<'a> {
    pub holder: &'a Holder,
    pub email: &'a str,
}

/// The answer for one person and one base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Open,
    /// The connection to an AI tool doesn't hold the base's scope.
    Scope(&'static str),
    /// The person's role doesn't open what the base holds.
    Role,
    /// The base's audience doesn't include them.
    Audience,
    Paused,
}

impl Decision {
    pub fn is_open(&self) -> bool {
        *self == Decision::Open
    }

    /// Why it's closed, in one sentence; `None` when open.
    pub fn why(&self, base: &Base) -> Option<String> {
        match self {
            Decision::Open => None,
            Decision::Scope(s) => Some(format!(
                "This connection wasn't given {s}, which {} needs.",
                base.name
            )),
            Decision::Role => Some(format!(
                "Your role doesn't open what {} holds. Ask an admin.",
                base.name
            )),
            Decision::Audience => Some(format!(
                "{} is open to a narrower audience. Ask an admin for access.",
                base.name
            )),
            Decision::Paused => Some(format!("An admin paused {}.", base.name)),
        }
    }
}

/// THE decision, for every door: paused, then (on `/mcp`) the connection's
/// scope, then the role, then the audience.
pub fn decide(
    base: &Base,
    who: &Asker<'_>,
    audience: &Audience,
    scopes: Option<&BTreeSet<&'static str>>,
) -> Decision {
    if audience.paused {
        return Decision::Paused;
    }
    if let Some(s) = scopes {
        if !s.contains(base.scope) {
            return Decision::Scope(base.scope);
        }
    }
    if !role_reads(who.holder, base.key) {
        return Decision::Role;
    }
    if !audience.open_to(&who.holder.role, who.email) {
        return Decision::Audience;
    }
    Decision::Open
}

/// Every base's decision for one person.
pub fn decide_all(
    who: &Asker<'_>,
    audiences: &BTreeMap<String, Audience>,
    scopes: Option<&BTreeSet<&'static str>>,
) -> BTreeMap<&'static str, Decision> {
    BASES
        .iter()
        .map(|b| {
            let a = audiences
                .get(b.key)
                .cloned()
                .unwrap_or_else(|| default_audience(b.key));
            (b.key, decide(b, who, &a, scopes))
        })
        .collect()
}

/// The record kinds a person may read in Chat and on `/mcp`: those their
/// role opens (`chat_work::readable`) whose base is open to them.
pub fn readable_kinds(
    holder: &Holder,
    open: &BTreeMap<&'static str, Decision>,
) -> Vec<&'static str> {
    chat_work::readable(holder)
        .into_iter()
        .filter(|k| {
            open.get(lookups::base_of(k).key)
                .is_some_and(Decision::is_open)
        })
        .collect()
}

// ── Storage ─────────────────────────────────────────────────────────────────

fn s(v: impl Into<String>) -> AttributeValue {
    AttributeValue::S(v.into())
}

fn attr<'a>(i: &'a Item, k: &str) -> &'a str {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .map_or("", |v| v.as_str())
}

/// Every base's audience for a team: what admins saved, else the default.
pub async fn audiences(state: &AppState, team_id: &str) -> BTreeMap<String, Audience> {
    let mut out: BTreeMap<String, Audience> = BASES
        .iter()
        .map(|b| (b.key.to_string(), default_audience(b.key)))
        .collect();
    let rows = rows(
        state,
        &state.config.settings_table_name,
        "pk",
        team_id,
        AUDIENCE_PREFIX,
        20,
    )
    .await;
    for i in rows {
        let key = attr(&i, "sk").trim_start_matches(AUDIENCE_PREFIX);
        if let (true, Ok(a)) = (
            lookups::base(key).is_some(),
            serde_json::from_str::<Audience>(attr(&i, "json")),
        ) {
            out.insert(key.to_string(), a);
        }
    }
    out
}

pub async fn save_audience(
    state: &AppState,
    team_id: &str,
    key: &str,
    a: &Audience,
    by: &str,
) -> Result<(), String> {
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(team_id))
        .item("sk", s(format!("{AUDIENCE_PREFIX}{key}")))
        .item("json", s(serde_json::to_string(a).unwrap_or_default()))
        .item("updated_by", s(by))
        .item("updated_at", s(chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Whether the team lets people connect AI tools as readers.
pub async fn readers_enabled(state: &AppState, team_id: &str) -> bool {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(team_id))
        .key("sk", s(READERS_SK))
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| i.get("enabled").and_then(|v| v.as_bool().ok()).copied())
        .unwrap_or(false)
}

pub async fn set_readers(
    state: &AppState,
    team_id: &str,
    enabled: bool,
    by: &str,
) -> Result<(), String> {
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(team_id))
        .item("sk", s(READERS_SK))
        .item("enabled", AttributeValue::Bool(enabled))
        .item("updated_by", s(by))
        .item("updated_at", s(chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// The rows of one team under a key prefix, at most `max_pages` pages.
async fn rows(
    state: &AppState,
    table: &str,
    pk_name: &str,
    team_id: &str,
    prefix: &str,
    max_pages: usize,
) -> Vec<Item> {
    let mut out = Vec::new();
    if table.is_empty() {
        return out;
    }
    let mut start: Option<Item> = None;
    for _ in 0..max_pages.max(1) {
        let mut q = state
            .dynamo
            .query()
            .table_name(table)
            .expression_attribute_values(":pk", s(team_id))
            .set_exclusive_start_key(start.take());
        q = if prefix.is_empty() {
            q.key_condition_expression(format!("{pk_name} = :pk"))
        } else {
            q.key_condition_expression(format!("{pk_name} = :pk AND begins_with(sk, :p)"))
                .expression_attribute_values(":p", s(prefix))
        };
        match q.send().await {
            Ok(r) => {
                out.extend(r.items().iter().cloned());
                match r.last_evaluated_key() {
                    Some(k) if out.len() < MAX_COUNT_ROWS => start = Some(k.clone()),
                    _ => break,
                }
            }
            Err(e) => {
                warn!(error = %e, prefix, "bases: rows not read");
                break;
            }
        }
    }
    out
}

// ── Counts and freshness ────────────────────────────────────────────────────

const TIME_FIELDS: &[&str] = &[
    "updated_at",
    "last_seen_at",
    "last_seen",
    "seen_at",
    "read_at",
    "received_at",
    "created_at",
];

/// The newest time a row carries, on the row or in its stored JSON.
pub fn row_time(i: &Item) -> Option<String> {
    let mut best: Option<String> = None;
    let mut see = |t: &str| {
        if chrono::DateTime::parse_from_rfc3339(t).is_ok() && best.as_deref().is_none_or(|b| t > b)
        {
            best = Some(t.to_string());
        }
    };
    for f in TIME_FIELDS {
        see(attr(i, f));
    }
    for k in ["json", "finding", "state"] {
        if let Ok(v) = serde_json::from_str::<Value>(attr(i, k)) {
            for f in TIME_FIELDS {
                if let Some(t) = v[*f].as_str() {
                    see(t);
                }
            }
        }
    }
    best
}

fn newest(rows: &[Item]) -> Option<String> {
    rows.iter().filter_map(row_time).max()
}

/// What a base holds now: counts by kind, and when it last changed.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Size {
    pub counts: Vec<(String, usize)>,
    pub updated_at: Option<String>,
    /// Read live at the source on every question (code, tickets).
    pub live: bool,
}

impl Size {
    fn add(&mut self, label: &str, rows: &[Item]) {
        self.counts.push((label.to_string(), rows.len()));
        if let Some(t) = newest(rows) {
            if self.updated_at.as_deref().is_none_or(|u| t.as_str() > u) {
                self.updated_at = Some(t);
            }
        }
    }

    pub fn total(&self) -> usize {
        self.counts.iter().map(|(_, n)| n).sum()
    }
}

/// How much one base holds for a team, read from the team's own records.
pub async fn size(state: &AppState, team_id: &str, key: &str) -> Size {
    let settings = state.config.settings_table_name.as_str();
    let insights = state.config.aws_insights_table_name.as_str();
    let mut out = Size::default();
    match key {
        lookups::CODE => {
            let repos = crate::routes::brain::load_repos(state, team_id).await;
            out.counts.push(("repositories".into(), repos.len()));
            out.live = true;
        }
        lookups::WORK => {
            let (runs, plans, reviews) = tokio::join!(
                rows(
                    state,
                    &state.config.runs_table_name,
                    "team_id",
                    team_id,
                    "",
                    10
                ),
                rows(
                    state,
                    &state.config.plans_table_name,
                    "pk",
                    team_id,
                    "PLAN#",
                    10
                ),
                rows(state, settings, "pk", team_id, "REVIEW#", 10),
            );
            let plans: Vec<Item> = plans
                .into_iter()
                .filter(|i| !attr(i, "sk").contains("#TASK#"))
                .collect();
            out.add("runs", &runs);
            out.add("plans", &plans);
            out.add("reviews", &reviews);
            out.live = true;
        }
        lookups::INFRA => {
            let (res, reads, found) = tokio::join!(
                rows(state, insights, "pk", team_id, "RES#", 20),
                rows(state, insights, "pk", team_id, "RES_READ#", 2),
                rows(
                    state,
                    settings,
                    "pk",
                    team_id,
                    common::infra_vuln::records::FINDING_PREFIX,
                    10
                ),
            );
            out.add("AWS resources", &res);
            out.add("AWS accounts read", &reads);
            out.add("infrastructure findings", &found);
        }
        lookups::LOGS => {
            let (logs, alerts) = tokio::join!(
                rows(
                    state,
                    insights,
                    "pk",
                    team_id,
                    common::log_findings::REC_PREFIX,
                    10
                ),
                rows(state, settings, "pk", team_id, "ALERT_EVENT#", 10),
            );
            out.add("log findings", &logs);
            out.add("alerts", &alerts);
        }
        lookups::SECURITY => {
            let found = rows(
                state,
                settings,
                "pk",
                team_id,
                common::security_fixes::FINDING_PREFIX,
                10,
            )
            .await;
            out.add("security findings", &found);
        }
        lookups::TEAM => {
            use common::knowledge::search::Store;
            let st = crate::routes::knowledge::store(state);
            if let Ok(bases) = st.bases(team_id).await {
                out.counts.push(("knowledge bases".into(), bases.len()));
                out.updated_at = bases
                    .iter()
                    .map(|b| b.updated_at.clone())
                    .filter(|t| !t.is_empty())
                    .max();
            }
        }
        _ => {}
    }
    out
}

// ── GitHub mirroring ────────────────────────────────────────────────────────

/// Whether GitHub's permission word lets a person read a repository.
pub fn grants_read(permission: &str) -> bool {
    matches!(
        permission,
        "admin" | "maintain" | "write" | "triage" | "read" | "push" | "pull"
    )
}

/// What was stored of a login's access: repository to may-read, and when.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RepoAccess {
    #[serde(default)]
    pub repos: BTreeMap<String, bool>,
    #[serde(default)]
    pub checked_at: i64,
}

impl RepoAccess {
    pub fn fresh(&self, now: i64) -> bool {
        now - self.checked_at < ACCESS_TTL_SECS
    }
}

/// The repositories to show, from what is known: a repository known to be
/// readable is kept; unknown and refused ones are withheld.
pub fn mirror(repos: Vec<RepoInfo>, known: &RepoAccess) -> (Vec<RepoInfo>, usize) {
    let (keep, held): (Vec<_>, Vec<_>) = repos.into_iter().partition(|r| {
        known
            .repos
            .get(&r.name.to_ascii_lowercase())
            .copied()
            .unwrap_or(false)
    });
    (keep, held.len())
}

/// The team's repositories this person may read: all of them when no
/// GitHub login is linked to a member (no mapping to mirror), only those
/// the login can read on GitHub otherwise. Readers without a login see none.
/// Gives the repositories and how many were withheld.
pub async fn visible_repos(
    state: &AppState,
    team_id: &str,
    role: &str,
    github_login: Option<&str>,
    github_token: Option<&str>,
    repos: Vec<RepoInfo>,
) -> (Vec<RepoInfo>, usize) {
    let login = github_login
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_ascii_lowercase);
    let Some(login) = login else {
        if role == READER_ROLE {
            let n = repos.len();
            return (Vec::new(), n);
        }
        return (repos, 0);
    };
    let Some(token) = github_token else {
        // GitHub can't be asked: what was checked before still counts.
        let known = load_access(state, team_id, &login).await;
        return mirror(repos, &known);
    };
    let now = chrono::Utc::now().timestamp();
    let mut known = load_access(state, team_id, &login).await;
    if !known.fresh(now) {
        known = RepoAccess {
            repos: BTreeMap::new(),
            checked_at: now,
        };
    }
    let mut asked = 0usize;
    let mut changed = false;
    for r in &repos {
        let name = r.name.to_ascii_lowercase();
        if known.repos.contains_key(&name) || asked >= MAX_ACCESS_CHECKS {
            continue;
        }
        asked += 1;
        if let Some(ok) = ask_permission(state, token, &r.name, &login).await {
            known.repos.insert(name, ok);
            changed = true;
        }
    }
    if changed {
        save_access(state, team_id, &login, &known).await;
    }
    mirror(repos, &known)
}

/// GitHub's answer for one repository: `None` when GitHub couldn't say.
async fn ask_permission(state: &AppState, token: &str, repo: &str, login: &str) -> Option<bool> {
    let url = format!(
        "https://api.github.com/repos/{repo}/collaborators/{}/permission",
        urlencoding::encode(login)
    );
    let r = state
        .http
        .get(url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm")
        .send()
        .await
        .ok()?;
    match r.status().as_u16() {
        200 => {
            let v: Value = r.json().await.ok()?;
            let p = v["user"]["permissions"].clone();
            let by_flags = ["admin", "maintain", "push", "triage", "pull"]
                .iter()
                .any(|k| p[*k].as_bool() == Some(true));
            Some(by_flags || grants_read(v["permission"].as_str().unwrap_or("")))
        }
        403 | 404 => Some(false),
        _ => None,
    }
}

async fn load_access(state: &AppState, team_id: &str, login: &str) -> RepoAccess {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(team_id))
        .key("sk", s(format!("{REPO_ACCESS_PREFIX}{login}")))
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| serde_json::from_str(attr(&i, "json")).ok())
        .unwrap_or_default()
}

async fn save_access(state: &AppState, team_id: &str, login: &str, a: &RepoAccess) {
    let ttl = a.checked_at + 86_400;
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(team_id))
        .item("sk", s(format!("{REPO_ACCESS_PREFIX}{login}")))
        .item("json", s(serde_json::to_string(a).unwrap_or_default()))
        .item("ttl", AttributeValue::N(ttl.to_string()))
        .send()
        .await
    {
        warn!(error = %e, "bases: GitHub access not saved");
    }
}

// ── The listing ─────────────────────────────────────────────────────────────

/// What the mirroring note says for a base.
pub fn mirroring(key: &str) -> Option<&'static str> {
    match key {
        lookups::CODE => Some("Code is read only from repositories your linked GitHub account can read."),
        lookups::WORK => Some("Pull requests follow your GitHub access; tickets follow your CoderHelm role until Jira project access is mapped."),
        lookups::TEAM => Some("Each knowledge base keeps its own readers."),
        _ => None,
    }
}

/// One base as the Knowledge page and `list_bases` show it.
pub fn view(base: &Base, size: &Size, audience: &Audience, decision: &Decision) -> Value {
    json!({
        "key": base.key,
        "name": base.name,
        "about": base.about,
        "scope": base.scope,
        "builtin": true,
        "counts": size.counts.iter().map(|(l, n)| json!({ "label": l, "count": n })).collect::<Vec<_>>(),
        "total": size.total(),
        "updated_at": size.updated_at,
        "live": size.live,
        "access": { "open": decision.is_open(), "why": decision.why(base) },
        "audience": audience,
        "audience_label": audience.label(),
        "mirroring": mirroring(base.key),
    })
}

/// Every base for one person: sizes are read only for bases open to them,
/// so a closed base shows its name and why, never what is in it.
pub async fn list(
    state: &AppState,
    team_id: &str,
    who: &Asker<'_>,
    scopes: Option<&BTreeSet<&'static str>>,
) -> Vec<Value> {
    let auds = audiences(state, team_id).await;
    let decisions = decide_all(who, &auds, scopes);
    let mut out = Vec::new();
    for b in BASES.iter() {
        let d = decisions.get(b.key).cloned().unwrap_or(Decision::Role);
        let a = auds.get(b.key).cloned().unwrap_or_default();
        let sz = if d.is_open() {
            Box::pin(size(state, team_id, b.key)).await
        } else {
            Size::default()
        };
        out.push(view(b, &sz, &a, &d));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(role: &str) -> Holder {
        Holder::builtin(role)
    }

    fn base(k: &str) -> &'static Base {
        lookups::base(k).unwrap()
    }

    #[test]
    fn every_base_starts_open_to_every_role_that_opens_it() {
        for role in ["owner", "admin", "member", "viewer"] {
            let holder = h(role);
            let who = Asker {
                holder: &holder,
                email: "dev@example.com",
            };
            for b in BASES.iter() {
                let d = decide(b, &who, &default_audience(b.key), None);
                assert_eq!(d.is_open(), role_reads(&holder, b.key), "{role} {}", b.key);
            }
        }
        // Everyone on the team reads code, work and the team's documents.
        let v = h("viewer");
        for k in [lookups::CODE, lookups::WORK, lookups::TEAM] {
            assert!(role_reads(&v, k), "{k}");
        }
    }

    #[test]
    fn readers_read_code_work_and_knowledge_until_an_admin_widens_it() {
        let r = h(READER_ROLE);
        let who = Asker {
            holder: &r,
            email: "reader@example.com",
        };
        let open: Vec<&str> = BASES
            .iter()
            .filter(|b| decide(b, &who, &default_audience(b.key), None).is_open())
            .map(|b| b.key)
            .collect();
        assert_eq!(open, [lookups::CODE, lookups::WORK, lookups::TEAM]);
        // An admin names the reader on Security: still only what their role
        // opens, never more.
        let mut a = default_audience(lookups::SECURITY);
        a.people = vec!["reader@example.com".into()];
        assert_eq!(
            decide(base(lookups::SECURITY), &who, &a, None),
            Decision::Open
        );
        let mut a = default_audience(lookups::SECURITY);
        a.readers = true;
        assert!(decide(base(lookups::SECURITY), &who, &a, None).is_open());
    }

    #[test]
    fn a_narrowed_audience_closes_a_base_but_never_to_admins() {
        let mut a = default_audience(lookups::INFRA);
        a.roles = vec!["member".into()];
        let (m, v, ad) = (h("member"), h("viewer"), h("admin"));
        let ask = |holder: &Holder, a: &Audience| {
            decide(
                base(lookups::INFRA),
                &Asker {
                    holder,
                    email: "x@example.com",
                },
                a,
                None,
            )
        };
        assert_eq!(ask(&m, &a), Decision::Open);
        assert_eq!(ask(&v, &a), Decision::Audience);
        assert_eq!(ask(&ad, &a), Decision::Open);
        // Named people are in, whatever their role.
        a.people = vec!["x@example.com".into()];
        assert_eq!(ask(&v, &a), Decision::Open);
        // Paused: closed to everyone.
        a.paused = true;
        assert_eq!(ask(&ad, &a), Decision::Paused);
        assert!(Decision::Paused
            .why(base(lookups::INFRA))
            .unwrap()
            .contains("paused"));
    }

    #[test]
    fn a_connection_reads_only_the_bases_its_scopes_name() {
        let ad = h("admin");
        let who = Asker {
            holder: &ad,
            email: "",
        };
        let scopes: BTreeSet<&'static str> = [crate::mcp_oauth::CODE_SCOPE].into();
        let auds = BTreeMap::new();
        let d = decide_all(&who, &auds, Some(&scopes));
        for b in BASES.iter() {
            let want = b.key == lookups::CODE;
            assert_eq!(d[b.key].is_open(), want, "{}", b.key);
            if !want {
                assert_eq!(d[b.key], Decision::Scope(b.scope));
            }
        }
    }

    #[test]
    fn closed_bases_take_their_kinds_out_of_chat() {
        let ad = h("admin");
        let who = Asker {
            holder: &ad,
            email: "",
        };
        let mut auds = BTreeMap::new();
        let mut a = default_audience(lookups::LOGS);
        a.paused = true;
        auds.insert(lookups::LOGS.to_string(), a);
        let d = decide_all(&who, &auds, None);
        let kinds = readable_kinds(&ad, &d);
        assert!(!kinds.contains(&"alerts") && !kinds.contains(&"log_findings"));
        assert!(kinds.contains(&"resources") && kinds.contains(&"runs"));
    }

    #[test]
    fn audience_edits_are_checked() {
        let a = Audience {
            roles: vec![" member ".into(), "member".into()],
            people: vec!["Dev@Example.com".into(), "".into()],
            ..Default::default()
        }
        .checked()
        .unwrap();
        assert_eq!(a.roles, ["member"]);
        assert_eq!(a.people, ["dev@example.com"]);
        assert!(Audience {
            roles: vec!["boss".into()],
            ..Default::default()
        }
        .checked()
        .is_err());
        assert!(Audience {
            people: vec!["not an email".into()],
            ..Default::default()
        }
        .checked()
        .is_err());
        assert!(Audience::default().label().contains("Everyone"));
    }

    #[test]
    fn code_follows_the_persons_github_access() {
        let repos = vec![
            RepoInfo::new("acme/web", "main"),
            RepoInfo::new("acme/api", "main"),
            RepoInfo::new("acme/infra", "main"),
        ];
        let known = RepoAccess {
            repos: [
                ("acme/web".to_string(), true),
                ("acme/api".to_string(), false),
            ]
            .into(),
            checked_at: 100,
        };
        let (keep, held) = mirror(repos, &known);
        assert_eq!(
            keep.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            ["acme/web"]
        );
        // Refused and not yet known: withheld, counted.
        assert_eq!(held, 2);
        assert!(known.fresh(100 + ACCESS_TTL_SECS - 1));
        assert!(!known.fresh(100 + ACCESS_TTL_SECS));
        for p in ["admin", "maintain", "write", "triage", "read"] {
            assert!(grants_read(p), "{p}");
        }
        assert!(!grants_read("none") && !grants_read(""));
    }

    #[test]
    fn freshness_is_the_newest_time_a_row_carries() {
        let row = |t: &str, inner: &str| -> Item {
            HashMap::from([
                ("updated_at".to_string(), s(t)),
                (
                    "json".to_string(),
                    s(json!({ "seen_at": inner }).to_string()),
                ),
            ])
        };
        let rows = [
            row("2026-11-01T00:00:00Z", "2026-11-03T00:00:00Z"),
            row("2026-11-02T00:00:00Z", "not a time"),
        ];
        assert_eq!(newest(&rows).as_deref(), Some("2026-11-03T00:00:00Z"));
        let mut sz = Size::default();
        sz.add("rows", &rows);
        assert_eq!(sz.total(), 2);
        let b = base(lookups::INFRA);
        let v = view(b, &sz, &default_audience(b.key), &Decision::Role);
        assert_eq!(v["access"]["open"], false);
        assert!(v["access"]["why"].as_str().unwrap().contains("role"));
        assert_eq!(v["scope"], "infra:read");
    }
}
