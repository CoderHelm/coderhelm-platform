//! Approval cards: what an agent wants to do when the answer is ask, and a
//! person's answer.
//!
//! An approver chooses one of three: **once** (this exact call, in this run
//! or chat), **always for this repository** or **always for this tool**. An
//! "always" writes a rule that shows on the settings page with where it
//! came from, and only someone who may edit permissions can choose it. A
//! card nobody answers expires; the run ends cleanly with a note.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::audit::{self, ActorKind, Event, PolicyFields};
use super::model::*;
use super::store::{PolicyStore, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Pending,
    Approved,
    Denied,
    Expired,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Pending => "pending",
            Status::Approved => "approved",
            Status::Denied => "denied",
            Status::Expired => "expired",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Choice {
    /// This exact call, this once.
    #[default]
    Once,
    /// Always, for this tool in this repository.
    AlwaysRepo,
    /// Always, for this tool everywhere in the team.
    AlwaysTool,
}

impl Choice {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "once" => Some(Choice::Once),
            "always_repo" => Some(Choice::AlwaysRepo),
            "always_tool" => Some(Choice::AlwaysTool),
            _ => None,
        }
    }
}

/// Where the answer came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Via {
    RunPage,
    Chat,
    Settings,
    Slack,
    Teams,
    Email,
}

impl Via {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "run_page" => Some(Via::RunPage),
            "chat" => Some(Via::Chat),
            "settings" => Some(Via::Settings),
            "slack" => Some(Via::Slack),
            "teams" => Some(Via::Teams),
            "email" => Some(Via::Email),
            _ => None,
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Via::RunPage => "the run page",
            Via::Chat => "the chat",
            Via::Settings => "the approvals queue",
            Via::Slack => "Slack",
            Via::Teams => "Teams",
            Via::Email => "email",
        }
    }
}

/// One approval card.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Approval {
    pub id: String,
    pub status: Status,
    pub agent: Agent,
    pub kind: CallKind,
    pub tool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// The run it belongs to, when it came from a run.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    /// The chat thread it belongs to, when it came from a chat.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub thread_id: String,
    /// The person the agent works for, when there is one.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub requested_for: String,
    /// The exact arguments, secrets hidden, long text shortened.
    pub args: Value,
    pub args_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<Risk>,
    /// Why the agent wants to do it (its own words, or the task).
    #[serde(default)]
    pub why: String,
    /// Why it was asked: the rule or default that said ask.
    pub asked_because: DecidedBy,
    pub reason: String,
    pub created_at: String,
    pub expires_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub decided_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided_via: Option<Via>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub choice: Option<Choice>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
    /// The rule an "always" wrote.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub rule_id: String,
}

impl Approval {
    pub fn expired_at(&self, now: &str) -> bool {
        match (
            chrono::DateTime::parse_from_rfc3339(&self.expires_at),
            chrono::DateTime::parse_from_rfc3339(now),
        ) {
            (Ok(e), Ok(n)) => n >= e,
            _ => false,
        }
    }

    /// What the agent wants to do, in one line.
    pub fn headline(&self) -> String {
        match self.kind {
            CallKind::Command => format!(
                "Run `{}` in the sandbox",
                self.command.as_deref().unwrap_or("")
            ),
            CallKind::Mcp => format!(
                "Use {} on {}",
                self.tool
                    .split_once("__")
                    .map_or(self.tool.as_str(), |(_, t)| t),
                self.server.as_deref().unwrap_or("a connection")
            ),
            _ => format!("Use {}", self.tool),
        }
    }

    /// Where the work this card holds up lives: the run, else the chat.
    pub fn scope_key(&self) -> String {
        if !self.run_id.is_empty() {
            format!("run:{}", self.run_id)
        } else {
            format!("thread:{}", self.thread_id)
        }
    }
}

/// Argument names whose values are never shown.
const SECRET_WORDS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "private_key",
    "cookie",
    "session",
];

const MAX_STRING: usize = 2000;
const MAX_ITEMS: usize = 50;

/// The arguments as a card shows them: secrets hidden, long text cut.
pub fn redact(v: &Value) -> Value {
    fn go(v: &Value, depth: usize) -> Value {
        if depth > 6 {
            return Value::String("…".into());
        }
        match v {
            Value::String(s) if s.len() > MAX_STRING => Value::String(format!(
                "{}… ({} characters in all)",
                crate::truncate_str(s, MAX_STRING),
                s.chars().count()
            )),
            Value::Array(a) => {
                let mut out: Vec<Value> =
                    a.iter().take(MAX_ITEMS).map(|x| go(x, depth + 1)).collect();
                if a.len() > MAX_ITEMS {
                    out.push(Value::String(format!("… {} more", a.len() - MAX_ITEMS)));
                }
                Value::Array(out)
            }
            Value::Object(m) => Value::Object(
                m.iter()
                    .map(|(k, x)| {
                        let lower = k.to_ascii_lowercase();
                        if SECRET_WORDS.iter().any(|w| lower.contains(w)) {
                            (k.clone(), Value::String("(hidden)".into()))
                        } else {
                            (k.clone(), go(x, depth + 1))
                        }
                    })
                    .collect(),
            ),
            other => other.clone(),
        }
    }
    go(v, 0)
}

/// A stable hash of the exact call, so "once" covers this call only.
pub fn args_hash(tool: &str, command: Option<&str>, args: &Value) -> String {
    crate::sha256_hex(&[tool, command.unwrap_or(""), &canonical(args)])
}

/// JSON with object keys sorted, for hashing.
fn canonical(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap_or_default(),
                        canonical(&m[k])
                    )
                })
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(canonical).collect::<Vec<_>>().join(",")
        ),
        other => other.to_string(),
    }
}

/// The key of a one-time grant: this call, in this run or chat.
pub fn grant_key(scope_key: &str, args_hash: &str) -> String {
    format!("{scope_key}#{args_hash}")
}

/// The person answering, and what they may do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Approver {
    pub email: String,
    /// Holds `agents.approve`.
    pub can_approve: bool,
    /// Holds `agents.permissions`, so may choose "always".
    pub can_edit_rules: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecideError {
    NotFound,
    NotAllowed(String),
    AlreadyDecided(Status),
    Expired,
    Invalid(String),
    Store(String),
}

impl std::fmt::Display for DecideError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecideError::NotFound => f.write_str("That approval doesn't exist."),
            DecideError::NotAllowed(m) | DecideError::Invalid(m) | DecideError::Store(m) => {
                f.write_str(m)
            }
            DecideError::AlreadyDecided(s) => {
                write!(f, "Someone already answered this ({}).", s.as_str())
            }
            DecideError::Expired => f.write_str("This approval expired before anyone answered."),
        }
    }
}

/// The rule an "always" writes.
pub fn always_rule(a: &Approval, choice: Choice, by: &str, now: &str) -> Option<Rule> {
    let (scope, repo) = match choice {
        Choice::Once => return None,
        Choice::AlwaysRepo => (Scope::Repo, a.repo.clone()),
        Choice::AlwaysTool => (Scope::Team, None),
    };
    let (tool, command) = match a.kind {
        CallKind::Command => (None, a.command.clone()),
        _ => (
            Some(
                a.server
                    .as_ref()
                    .and_then(|_| a.tool.split_once("__").map(|(_, t)| t.to_string()))
                    .unwrap_or_else(|| a.tool.clone()),
            ),
            None,
        ),
    };
    Some(Rule {
        id: format!("r_{}", ulid::Ulid::new().to_string().to_ascii_lowercase()),
        scope,
        repo,
        agent: None,
        kind: Some(a.kind),
        server: a.server.clone(),
        tool,
        risk: None,
        command,
        decision: Decision::Allow,
        locked: false,
        note: format!("Added when {by} approved {} \"always\".", a.headline()),
        source: RuleSource::Approval,
        created_by: by.to_string(),
        created_at: now.to_string(),
    })
}

/// A person answers a card. Checks what they may do, records the answer once
/// (a second answer is refused), writes the rule for "always", leaves a
/// one-time grant for "once", and audits it.
#[allow(clippy::too_many_arguments)]
pub async fn decide(
    store: &dyn PolicyStore,
    team: &str,
    id: &str,
    approver: &Approver,
    approve: bool,
    choice: Choice,
    via: Via,
    note: &str,
    now: &str,
) -> Result<Approval, DecideError> {
    if !approver.can_approve {
        return Err(DecideError::NotAllowed(
            "You can't answer approvals. Ask someone who can approve agent actions.".into(),
        ));
    }
    if approve && choice != Choice::Once && !approver.can_edit_rules {
        return Err(DecideError::NotAllowed(
            "Only people who can edit agent permissions can approve \"always\". Approve once instead."
                .into(),
        ));
    }
    if note.len() > 500 {
        return Err(DecideError::Invalid(
            "Keep the note under 500 characters.".into(),
        ));
    }
    let mut a = store
        .get_approval(team, id)
        .await
        .map_err(DecideError::Store)?
        .ok_or(DecideError::NotFound)?;
    if a.status != Status::Pending {
        return Err(DecideError::AlreadyDecided(a.status));
    }
    if a.expired_at(now) {
        a.status = Status::Expired;
        let _ = store.update_approval(team, &a, Status::Pending).await;
        return Err(DecideError::Expired);
    }

    // Write the rule first: if the cap refuses it, nothing is approved.
    let mut written: Option<Rule> = None;
    if approve {
        if let Some(rule) = always_rule(&a, choice, &approver.email, now) {
            let mut policy = store.load(team).await.map_err(DecideError::Store)?;
            if let Some(r) = a.risk {
                if r > policy.autonomy.max_allowed() {
                    return Err(DecideError::NotAllowed(format!(
                        "The team's autonomy limit doesn't let {} actions run without asking, so \
                         \"always\" isn't possible. Approve once instead.",
                        r.as_str()
                    )));
                }
            }
            let expect = policy.version;
            policy.rules.push(rule.clone());
            policy.version += 1;
            policy.updated_by = approver.email.clone();
            policy.updated_at = now.to_string();
            store
                .save(team, &policy, expect)
                .await
                .map_err(|e| DecideError::Store(e.to_string()))?;
            written = Some(rule);
        }
    }

    a.status = if approve {
        Status::Approved
    } else {
        Status::Denied
    };
    a.decided_by = approver.email.clone();
    a.decided_via = Some(via);
    a.decided_at = Some(now.to_string());
    a.choice = Some(if approve { choice } else { Choice::Once });
    a.note = note.trim().to_string();
    a.rule_id = written.as_ref().map(|r| r.id.clone()).unwrap_or_default();
    let ok = store
        .update_approval(team, &a, Status::Pending)
        .await
        .map_err(DecideError::Store)?;
    if !ok {
        // Someone else answered between our read and write.
        let now_status = store
            .get_approval(team, id)
            .await
            .ok()
            .flatten()
            .map_or(Status::Expired, |x| x.status);
        return Err(DecideError::AlreadyDecided(now_status));
    }
    if approve && choice == Choice::Once {
        let _ = store
            .put_grant(
                team,
                &grant_key(&a.scope_key(), &a.args_hash),
                &a.id,
                &a.expires_at,
            )
            .await;
    }

    let mut ev = Event::new(
        &approver.email,
        ActorKind::Person,
        if approve {
            audit::action::APPROVED
        } else {
            audit::action::DENIED
        },
        if approve { "ok" } else { "refused" },
    )
    .approval(&a.id)
    .run(&a.run_id)
    .tool(&a.tool, a.risk.map_or("", |r| r.as_str()))
    .detail(format!(
        "{} from {}{}",
        match (approve, choice) {
            (false, _) => "Denied",
            (true, Choice::Once) => "Approved once",
            (true, Choice::AlwaysRepo) => "Approved always for this repository",
            (true, Choice::AlwaysTool) => "Approved always for this tool",
        },
        via.label(),
        if a.note.is_empty() {
            String::new()
        } else {
            format!(": {}", a.note)
        }
    ))
    .policy(PolicyFields {
        agent: a.agent.key().into(),
        repo: a.repo.clone().unwrap_or_default(),
        decision: if approve {
            "allow".into()
        } else {
            "deny".into()
        },
        rule: a.rule_id.clone(),
        thread_id: a.thread_id.clone(),
    });
    if ev.run_id.is_empty() {
        ev.run_id = String::new();
    }
    store.audit(team, ev).await;
    if let Some(r) = &written {
        store
            .audit(
                team,
                Event::new(
                    &approver.email,
                    ActorKind::Person,
                    audit::action::RULE_ADDED,
                    "ok",
                )
                .approval(&a.id)
                .detail(r.label()),
            )
            .await;
    }
    Ok(a)
}

/// Marks every pending card past its time expired, and returns them.
pub async fn expire_due(
    store: &dyn PolicyStore,
    team: &str,
    now: &str,
) -> Result<Vec<Approval>, StoreError> {
    let mut out = Vec::new();
    for mut a in store
        .list_approvals(team, true)
        .await
        .map_err(StoreError::Other)?
    {
        if a.status == Status::Pending && a.expired_at(now) {
            a.status = Status::Expired;
            a.decided_at = Some(now.to_string());
            if store
                .update_approval(team, &a, Status::Pending)
                .await
                .unwrap_or(false)
            {
                store
                    .audit(
                        team,
                        Event::new(
                            "system",
                            ActorKind::System,
                            audit::action::EXPIRED,
                            "expired",
                        )
                        .approval(&a.id)
                        .run(&a.run_id)
                        .tool(&a.tool, a.risk.map_or("", |r| r.as_str())),
                    )
                    .await;
                out.push(a);
            }
        }
    }
    Ok(out)
}
