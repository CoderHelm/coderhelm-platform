//! The words of agent permissions: decisions, risks, agents, scopes, rules,
//! switches and the team's policy record.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// What happens to one call.
///
/// Ordered by strictness: `Allow < Ask < Deny`, so the strictest of a set of
/// matching rules is their `max()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Decision {
    Allow,
    Ask,
    Deny,
}

impl Decision {
    pub fn as_str(self) -> &'static str {
        match self {
            Decision::Allow => "allow",
            Decision::Ask => "ask",
            Decision::Deny => "deny",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "allow" => Some(Decision::Allow),
            "ask" => Some(Decision::Ask),
            "deny" => Some(Decision::Deny),
            _ => None,
        }
    }
}

/// How much a tool can change. The same three words, in the same order, as
/// the tool registry that the plugins work adds (`common::tool_registry`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Risk {
    /// Looks something up and changes nothing.
    Read,
    /// Changes something that is reviewed before it reaches anyone (a file on
    /// the working branch, a draft).
    Write,
    /// Reaches people or can't be taken back (send, publish, delete, merge,
    /// deploy, pay).
    Consequential,
}

impl Risk {
    pub fn as_str(self) -> &'static str {
        match self {
            Risk::Read => "read",
            Risk::Write => "write",
            Risk::Consequential => "consequential",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "read" => Some(Risk::Read),
            "write" => Some(Risk::Write),
            "consequential" => Some(Risk::Consequential),
            _ => None,
        }
    }
}

/// Every agent loop CoderHelm runs. Each one asks the engine before every
/// call it makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Agent {
    /// Reads the ticket and the code and writes the plan.
    Planner,
    /// Writes the change and opens the pull request.
    Coder,
    /// Reviews pull requests.
    Reviewer,
    /// The plan chat on the Plans page.
    PlanChat,
    /// The company brain chat.
    BrainChat,
    /// Proposes fixes for alerts.
    AlertFixer,
    /// Fixes log findings on its own when a team turns that on.
    LogAutofix,
    /// A plugin's run.
    Plugin,
}

impl Agent {
    pub const ALL: [Agent; 8] = [
        Agent::Planner,
        Agent::Coder,
        Agent::Reviewer,
        Agent::PlanChat,
        Agent::BrainChat,
        Agent::AlertFixer,
        Agent::LogAutofix,
        Agent::Plugin,
    ];

    pub fn key(self) -> &'static str {
        match self {
            Agent::Planner => "planner",
            Agent::Coder => "coder",
            Agent::Reviewer => "reviewer",
            Agent::PlanChat => "plan_chat",
            Agent::BrainChat => "brain_chat",
            Agent::AlertFixer => "alert_fixer",
            Agent::LogAutofix => "log_autofix",
            Agent::Plugin => "plugin",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Agent::Planner => "Planner",
            Agent::Coder => "Coder",
            Agent::Reviewer => "Reviewer",
            Agent::PlanChat => "Plan chat",
            Agent::BrainChat => "Chat",
            Agent::AlertFixer => "Alert fixer",
            Agent::LogAutofix => "Log auto-fix",
            Agent::Plugin => "Plugin runs",
        }
    }

    pub fn summary(self) -> &'static str {
        match self {
            Agent::Planner => "Reads tickets and code, and writes plans.",
            Agent::Coder => "Writes changes on a branch and opens pull requests.",
            Agent::Reviewer => "Reviews pull requests and leaves comments.",
            Agent::PlanChat => "Answers questions and drafts plans in the plan chat.",
            Agent::BrainChat => "Answers questions from the team's knowledge.",
            Agent::AlertFixer => "Looks into alerts and proposes fixes.",
            Agent::LogAutofix => "Fixes log findings on its own when turned on.",
            Agent::Plugin => "Runs the team's installed plugins.",
        }
    }

    /// The chat agents: a person is in the conversation, so an ask is shown
    /// inline and answered there.
    pub fn is_chat(self) -> bool {
        matches!(self, Agent::PlanChat | Agent::BrainChat)
    }

    pub fn parse(s: &str) -> Option<Self> {
        Agent::ALL.into_iter().find(|a| a.key() == s.trim())
    }
}

/// What kind of thing a call is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallKind {
    /// A built-in tool (read a file, write a file, run the checks).
    Tool,
    /// A tool on one of the team's MCP servers.
    Mcp,
    /// A query against one of the team's data sources.
    DataSource,
    /// A shell command in the sandbox.
    Command,
    /// A network request from the sandbox.
    Egress,
    /// Something sent outside CoderHelm (a message, a comment, a release).
    Outbound,
}

impl CallKind {
    pub const ALL: [CallKind; 6] = [
        CallKind::Tool,
        CallKind::Mcp,
        CallKind::DataSource,
        CallKind::Command,
        CallKind::Egress,
        CallKind::Outbound,
    ];
    pub fn key(self) -> &'static str {
        match self {
            CallKind::Tool => "tool",
            CallKind::Mcp => "mcp",
            CallKind::DataSource => "data_source",
            CallKind::Command => "command",
            CallKind::Egress => "egress",
            CallKind::Outbound => "outbound",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        CallKind::ALL.into_iter().find(|k| k.key() == s.trim())
    }
}

/// The highest risk agents may run without asking anyone. Nobody can set a
/// rule above it: a rule that would allow more is held to ask.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Autonomy {
    /// Only reads run on their own; every change is asked.
    ReadOnly,
    /// Reads and reviewable changes (a branch, a draft) run on their own;
    /// anything that reaches people is asked.
    #[default]
    Standard,
    /// Rules may allow anything, including what reaches people.
    Full,
}

impl Autonomy {
    /// The highest risk a rule may allow under this cap.
    pub fn max_allowed(self) -> Risk {
        match self {
            Autonomy::ReadOnly => Risk::Read,
            Autonomy::Standard => Risk::Write,
            Autonomy::Full => Risk::Consequential,
        }
    }
    pub fn key(self) -> &'static str {
        match self {
            Autonomy::ReadOnly => "read_only",
            Autonomy::Standard => "standard",
            Autonomy::Full => "full",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "read_only" => Some(Autonomy::ReadOnly),
            "standard" => Some(Autonomy::Standard),
            "full" => Some(Autonomy::Full),
            _ => None,
        }
    }
}

/// Where a rule applies. A narrower scope beats a wider one; at the same
/// scope, deny beats ask and ask beats allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Team,
    Repo,
    Agent,
}

impl Scope {
    pub fn key(self) -> &'static str {
        match self {
            Scope::Team => "team",
            Scope::Repo => "repo",
            Scope::Agent => "agent",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "team" => Some(Scope::Team),
            "repo" => Some(Scope::Repo),
            "agent" => Some(Scope::Agent),
            _ => None,
        }
    }
}

/// Where a rule came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuleSource {
    /// Someone wrote it on the settings page.
    #[default]
    Manual,
    /// Someone chose "always" on an approval card.
    Approval,
    /// A preset (Cautious, Standard, Autonomous) wrote it.
    Preset,
}

/// One rule: what it matches and what it decides. Every matcher left empty
/// matches anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub id: String,
    pub scope: Scope,
    /// The repository (`owner/name`). Required for a repo rule, optional for
    /// an agent rule (that agent in that repository only), unused for a team
    /// rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// Required for an agent rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<Agent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<CallKind>,
    /// The MCP server or data source, as a pattern (`linear`, `*`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    /// The tool name, as a pattern (`write_file`, `get_*`, `*_draft`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<Risk>,
    /// A sandbox command, as a pattern (`npm run test*`), or a domain for a
    /// network rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    pub decision: Decision,
    /// A team rule nothing narrower can override.
    #[serde(default)]
    pub locked: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
    #[serde(default)]
    pub source: RuleSource,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

impl Rule {
    /// How narrow the rule is: team 0, repo 1, agent 2, agent in one repo 3.
    pub fn specificity(&self) -> u8 {
        match (self.scope, self.repo.is_some()) {
            (Scope::Team, _) => 0,
            (Scope::Repo, _) => 1,
            (Scope::Agent, false) => 2,
            (Scope::Agent, true) => 3,
        }
    }

    /// One line in plain words, for the decision record and the settings
    /// page: "Team rule: ask for linear tools named create_*".
    pub fn label(&self) -> String {
        let mut what: Vec<String> = Vec::new();
        if let Some(k) = self.kind {
            what.push(match k {
                CallKind::Tool => "built-in tools".to_string(),
                CallKind::Mcp => "MCP tools".to_string(),
                CallKind::DataSource => "data source queries".to_string(),
                CallKind::Command => "sandbox commands".to_string(),
                CallKind::Egress => "network requests".to_string(),
                CallKind::Outbound => "outbound actions".to_string(),
            });
        }
        if let Some(s) = &self.server {
            what.push(format!("on {s}"));
        }
        if let Some(t) = &self.tool {
            what.push(format!("named {t}"));
        }
        if let Some(r) = self.risk {
            what.push(format!("of {} risk", r.as_str()));
        }
        if let Some(c) = &self.command {
            what.push(format!("matching \"{c}\""));
        }
        if what.is_empty() {
            what.push("everything".to_string());
        }
        let scope = match (self.scope, &self.repo, self.agent) {
            (Scope::Team, _, _) => "Team rule".to_string(),
            (Scope::Repo, Some(r), _) => format!("Rule for {r}"),
            (Scope::Repo, None, _) => "Repository rule".to_string(),
            (Scope::Agent, Some(r), Some(a)) => format!("{} rule in {r}", a.label()),
            (Scope::Agent, None, Some(a)) => format!("{} rule", a.label()),
            (Scope::Agent, _, None) => "Agent rule".to_string(),
        };
        format!(
            "{scope}{}: {} {}",
            if self.locked { " (locked)" } else { "" },
            self.decision.as_str(),
            what.join(" ")
        )
    }
}

/// Agents switched off, for the whole team or for one repository.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Switches {
    #[serde(default)]
    pub team_off: BTreeSet<Agent>,
    /// `owner/name` to the agents off there.
    #[serde(default)]
    pub repo_off: BTreeMap<String, BTreeSet<Agent>>,
}

impl Switches {
    /// Off for the team, or for this repository. `Some(scope)` says where.
    pub fn off(&self, agent: Agent, repo: Option<&str>) -> Option<Scope> {
        if self.team_off.contains(&agent) {
            return Some(Scope::Team);
        }
        let repo = repo?;
        self.repo_off
            .get(&repo.to_ascii_lowercase())
            .filter(|s| s.contains(&agent))
            .map(|_| Scope::Repo)
    }
}

/// "Pause all agents": every agent stops starting new work until it is
/// lifted or `until` passes. Work that arrives meanwhile is kept and resumed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pause {
    pub reason: String,
    pub by: String,
    pub at: String,
    /// RFC 3339. None: until someone lifts it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
}

impl Pause {
    /// Still in force at `now` (RFC 3339 strings compare in time order when
    /// they share a format; both come from `chrono` in UTC).
    pub fn active(&self, now: &str) -> bool {
        match &self.until {
            None => true,
            Some(u) => match (
                chrono::DateTime::parse_from_rfc3339(u),
                chrono::DateTime::parse_from_rfc3339(now),
            ) {
                (Ok(u), Ok(n)) => n < u,
                _ => true,
            },
        }
    }
}

/// A build or test command a repository's sandbox may run without asking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllowedCommand {
    /// The command, or a pattern ending in `*` (`npm run test*`).
    pub command: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub added_by: String,
    /// `manual`, or where a suggestion came from (`package.json`,
    /// `AGENTS.md`) when an admin accepted it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin: String,
}

/// A command found in the repository that an admin hasn't accepted yet.
/// Suggestions never allow anything on their own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSuggestion {
    pub command: String,
    /// `package.json`, `AGENTS.md`.
    pub origin: String,
}

/// The sandbox: commands per repository, and where it may connect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sandbox {
    /// `owner/name` to its allowed commands.
    #[serde(default)]
    pub commands: BTreeMap<String, Vec<AllowedCommand>>,
    #[serde(default)]
    pub suggestions: BTreeMap<String, Vec<CommandSuggestion>>,
    /// Domains the sandbox may reach (a domain covers its subdomains).
    #[serde(default = "default_egress")]
    pub egress: Vec<String>,
    /// When false the sandbox network is open (the setting before this one
    /// existed). On by default.
    #[serde(default = "yes")]
    pub limit_egress: bool,
}

fn yes() -> bool {
    true
}

/// Package registries and code hosts a build needs, the same kind of list
/// coding agents ship as their recommended firewall allowlist.
pub const DEFAULT_EGRESS: &[&str] = &[
    "registry.npmjs.org",
    "registry.yarnpkg.com",
    "npm.pkg.github.com",
    "pypi.org",
    "files.pythonhosted.org",
    "crates.io",
    "static.crates.io",
    "index.crates.io",
    "proxy.golang.org",
    "sum.golang.org",
    "rubygems.org",
    "repo.maven.apache.org",
    "repo1.maven.org",
    "plugins.gradle.org",
    "services.gradle.org",
    "api.nuget.org",
    "github.com",
    "codeload.github.com",
    "objects.githubusercontent.com",
    "raw.githubusercontent.com",
    "nodejs.org",
    "releases.hashicorp.com",
    "www.hashicorp.com",
    "playwright.azureedge.net",
    "cdn.playwright.dev",
];

pub fn default_egress() -> Vec<String> {
    DEFAULT_EGRESS.iter().map(|s| s.to_string()).collect()
}

impl Default for Sandbox {
    fn default() -> Self {
        Self {
            commands: BTreeMap::new(),
            suggestions: BTreeMap::new(),
            egress: default_egress(),
            limit_egress: true,
        }
    }
}

/// How long a person has to answer an approval card before the run ends.
pub const DEFAULT_ASK_TIMEOUT_SECS: u64 = 15 * 60;
pub const MIN_ASK_TIMEOUT_SECS: u64 = 60;
pub const MAX_ASK_TIMEOUT_SECS: u64 = 7 * 86_400;

fn default_ask_timeout() -> u64 {
    DEFAULT_ASK_TIMEOUT_SECS
}

/// A team's whole agent policy, one record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamPolicy {
    /// Bumped on every save; a save that read an older one is refused.
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub autonomy: Autonomy,
    #[serde(default)]
    pub rules: Vec<Rule>,
    #[serde(default)]
    pub switches: Switches,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause: Option<Pause>,
    #[serde(default)]
    pub sandbox: Sandbox,
    #[serde(default = "default_ask_timeout")]
    pub ask_timeout_secs: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub updated_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub updated_at: String,
}

impl Default for TeamPolicy {
    fn default() -> Self {
        Self {
            version: 0,
            autonomy: Autonomy::default(),
            rules: Vec::new(),
            switches: Switches::default(),
            pause: None,
            sandbox: Sandbox::default(),
            ask_timeout_secs: DEFAULT_ASK_TIMEOUT_SECS,
            updated_by: String::new(),
            updated_at: String::new(),
        }
    }
}

impl TeamPolicy {
    /// What a team starts with before anyone saves anything: the Standard
    /// preset (agents work on their own branch on their own, anything that
    /// reaches people asks), shown on the settings page as two preset rules.
    /// With every rule removed the engine's safe defaults remain: reads run,
    /// writes ask, consequential actions ask, unknown tools are denied.
    pub fn starting() -> Self {
        let mut p = TeamPolicy::default();
        super::presets::apply(&mut p, super::presets::Preset::Standard, "CoderHelm", "");
        p
    }

    /// The pause in force at `now`, if any.
    pub fn paused(&self, now: &str) -> Option<&Pause> {
        self.pause.as_ref().filter(|p| p.active(now))
    }

    /// The ask timeout, held inside its bounds.
    pub fn ask_timeout(&self) -> u64 {
        self.ask_timeout_secs
            .clamp(MIN_ASK_TIMEOUT_SECS, MAX_ASK_TIMEOUT_SECS)
    }
}

/// One call an agent wants to make, as the engine sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Call {
    pub agent: Agent,
    pub kind: CallKind,
    /// The tool's name as the agent calls it (`write_file`, `linear__create_issue`),
    /// or `run_command` / `connect` for commands and network requests.
    pub tool: String,
    /// The MCP server or data source, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// The sandbox command, or the host of a network request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

impl Call {
    pub fn tool(agent: Agent, tool: &str) -> Self {
        Self {
            agent,
            kind: CallKind::Tool,
            tool: tool.to_string(),
            server: None,
            repo: None,
            command: None,
        }
    }
    pub fn in_repo(mut self, repo: Option<&str>) -> Self {
        self.repo = repo
            .map(|r| r.trim().to_ascii_lowercase())
            .filter(|r| !r.is_empty());
        self
    }
    pub fn on_server(mut self, kind: CallKind, server: &str) -> Self {
        self.kind = kind;
        self.server = Some(server.to_string());
        self
    }
    pub fn command(agent: Agent, command: &str) -> Self {
        Self {
            agent,
            kind: CallKind::Command,
            tool: "run_command".into(),
            server: None,
            repo: None,
            command: Some(command.trim().to_string()),
        }
    }
    pub fn egress(agent: Agent, host: &str) -> Self {
        Self {
            agent,
            kind: CallKind::Egress,
            tool: "connect".into(),
            server: None,
            repo: None,
            command: Some(host.trim().to_ascii_lowercase()),
        }
    }
    /// The name a rule's `tool` pattern is matched against: the tool's own
    /// name without its server prefix (`create_issue` for
    /// `linear__create_issue`), and the full name as well.
    pub fn bare_tool(&self) -> &str {
        match (&self.server, self.tool.split_once("__")) {
            (Some(_), Some((_, t))) => t,
            _ => &self.tool,
        }
    }
}

/// What decided a call, kept on the decision record and shown on the
/// settings page's "test a call" box.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "snake_case")]
pub enum DecidedBy {
    Rule {
        id: String,
        scope: Scope,
        label: String,
    },
    /// No rule matched: the safe default for the call's risk.
    Default { risk: Risk },
    /// The agent is switched off for the team or the repository.
    AgentOff { scope: Scope },
    /// "Pause all agents" is on.
    Paused { reason: String },
    /// Nothing knows what this tool does, so it doesn't run.
    Unknown,
    /// The repository's sandbox allowlist.
    CommandAllowlist { command: String },
    /// The sandbox's network allowlist.
    EgressAllowlist,
    /// A person approved this exact call once.
    Grant { approval_id: String },
}

impl DecidedBy {
    /// The id of the rule that decided, when a rule did.
    pub fn rule_id(&self) -> Option<&str> {
        match self {
            DecidedBy::Rule { id, .. } => Some(id),
            _ => None,
        }
    }
}

/// The engine's answer for one call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verdict {
    pub decision: Decision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<Risk>,
    pub decided_by: DecidedBy,
    /// One sentence in plain words.
    pub reason: String,
    /// True when a rule allowed the call but the team's autonomy cap held it
    /// to ask.
    #[serde(default)]
    pub capped: bool,
}

impl Verdict {
    pub fn paused(&self) -> bool {
        matches!(self.decided_by, DecidedBy::Paused { .. })
    }
}
