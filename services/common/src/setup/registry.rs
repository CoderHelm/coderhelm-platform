//! Every setting Setup manages, in one list.
//!
//! Each [`Entry`] names where its value lives, who may change it, its
//! default, the release that added it, and how Setup recommends a value.
//! [`read`] and [`write`] go through the same view and validation code as
//! the setting's own page (`crate::review_config`, `StaleConfig::sanitized`,
//! `SecurityConfig::validate`, `e2e::model::Settings::validate`,
//! `crate::instructions`, `git_conventions::validate`, `crate::code_graph`,
//! `crate::alert_budget`), so a value Setup writes is one the page could
//! have saved, and a default here is the page's default.
//!
//! Left out on purpose: personal settings (notification preferences), the
//! model provider key and every other secret, webhooks (reminder channels,
//! the release notes email webhook and its secret), integrations
//! (Confluence, Jira), agent permissions, the token limit, and team rules
//! and voice. Setup never recommends or writes any of them; a write to a
//! reviewer setting carries the stored webhooks and secret over untouched.

use std::collections::HashMap;

use serde::Serialize;
use serde_json::{json, Value};

use super::store::{config_item, Item, Store, E2E_ROW};
use crate::e2e::model::Settings as E2eSettings;
use crate::security_fixes::{self as sec, Repos, SecurityConfig};
use crate::stale_prs::{self as stale, StaleConfig};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    Reviewer,
    Tests,
    CiLabels,
    AutoMerge,
    Tags,
    ReleaseNotes,
    Reminders,
    StalePrs,
    Security,
    Instructions,
    AgentFile,
    GitConventions,
    Alerts,
    CodeGraph,
}

impl Feature {
    pub fn title(self) -> &'static str {
        match self {
            Feature::Reviewer => "Code review",
            Feature::Tests => "Tests",
            Feature::CiLabels => "CI labels",
            Feature::AutoMerge => "Auto-merge",
            Feature::Tags => "Release tags",
            Feature::ReleaseNotes => "Release notes",
            Feature::Reminders => "Review reminders",
            Feature::StalePrs => "Stale pull requests",
            Feature::Security => "Security fixes",
            Feature::Instructions => "Coder instructions",
            Feature::AgentFile => "Agent file",
            Feature::GitConventions => "Git conventions",
            Feature::Alerts => "Alerts",
            Feature::CodeGraph => "Code graph",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// One value for the whole team: a change applies to every repo.
    Team,
    Repo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Risk {
    Safe,
    /// It closes or merges things, or posts outside CoderHelm. Its
    /// recommendation carries evidence of what it would do.
    Acts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Recommend {
    /// A fact read from the repo by code.
    Detector,
    /// A field of the AI read.
    Ai,
    /// CoderHelm's own pick.
    Default,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Kind {
    Bool,
    Int { min: i64, max: i64 },
    Text { max: usize, multiline: bool },
    Choice { options: &'static [&'static str] },
}

/// Where a value is kept, for people (the settings key and field).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Storage {
    pub key: &'static str,
    pub field: &'static str,
}

/// How a value is read and written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binding {
    /// A field of the repo's reviewer settings (`crate::review_config`).
    Reviewer(&'static str),
    /// A field of the stale pull request settings the repo uses; a write
    /// gives the repo its own settings (they replace the team's whole).
    Stale(&'static str),
    /// Whether security fixes cover the repo: on, and the repo in `repos`.
    SecurityRepo,
    /// Security fixes open pull requests (not a dry run).
    SecurityOpenPrs,
    /// Whether the team's E2E settings cover the repo.
    TestsRepo,
    /// A team switch in the E2E settings.
    Tests(&'static str),
    /// The repo's custom instructions.
    Instructions,
    /// One git convention template (`branch`, `commit`, `pr_title`).
    GitConvention(&'static str),
    /// The repo's code graph switch.
    CodeGraph,
    /// The team's daily alert token budget.
    AlertBudget,
    /// Automatic fix PRs for log findings: the switch of the team's
    /// `LOG_AUTOFIX` setting (`crate::log_findings::AutoFix`); a write keeps
    /// the rest of it (severity, daily limit, log groups).
    LogAutofix,
}

/// One managed setting.
#[derive(Debug, Clone, Serialize)]
pub struct Entry {
    pub id: &'static str,
    pub feature: Feature,
    pub title: &'static str,
    /// One sentence.
    pub help: &'static str,
    pub scope: Scope,
    pub storage: Storage,
    /// The CoderHelm release that added the setting.
    pub since: &'static str,
    pub risk: Risk,
    pub recommend: Recommend,
    pub kind: Kind,
    /// The capability (`gateway::permissions`) a change to it needs.
    pub capability: &'static str,
    /// The feature's main switch (counted in "features on").
    pub primary: bool,
    /// CoderHelm's pick when nothing in the repo says otherwise, when it
    /// differs from the default (progress first: the reviewer on, security
    /// fixes opening pull requests).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<bool>,
    /// It merges or closes things on its own: Setup recommends it with its
    /// evidence and leaves it off until a person picks it or switches the
    /// preview on. No choice applies it by itself.
    pub preview_only: bool,
    #[serde(skip)]
    pub binding: Binding,
}

const REVIEW_KEY: &str = "REVIEW_CONFIG#REPO#<repo>";
const STALE_KEY: &str = "STALE_PR_CONFIG#REPO#<repo>";
const SECURITY_KEY: &str = "SECURITY_CONFIG#TEAM";
const E2E_KEY: &str = "E2E settings (T#<team>, SETTINGS)";

#[allow(clippy::too_many_arguments)]
const fn entry(
    id: &'static str,
    feature: Feature,
    title: &'static str,
    help: &'static str,
    scope: Scope,
    storage: (&'static str, &'static str),
    since: &'static str,
    risk: Risk,
    recommend: Recommend,
    kind: Kind,
    capability: &'static str,
    binding: Binding,
) -> Entry {
    Entry {
        id,
        feature,
        title,
        help,
        scope,
        storage: Storage {
            key: storage.0,
            field: storage.1,
        },
        since,
        risk,
        recommend,
        kind,
        capability,
        primary: false,
        policy: None,
        preview_only: false,
        binding,
    }
}

const fn preview_only(mut e: Entry) -> Entry {
    e.preview_only = true;
    e
}

const fn primary(mut e: Entry) -> Entry {
    e.primary = true;
    e
}

const fn policy(mut e: Entry, v: bool) -> Entry {
    e.policy = Some(v);
    e
}

const AGENT: &str = "agent_settings";
const BOOL: Kind = Kind::Bool;
const V07: &str = "v0.7.0";

use Binding as B;
use Feature as F;
use Recommend as R;
use Risk::{Acts, Safe};
use Scope::{Repo, Team};

/// Every managed setting, in the order Review shows them.
pub static REGISTRY: &[Entry] = &[
    // ── Code review ──
    policy(
        primary(entry(
            "reviewer.enabled",
            F::Reviewer,
            "Review pull requests",
            "CoderHelm reviews pull requests that carry the review label.",
            Repo,
            (REVIEW_KEY, "enabled"),
            V07,
            Safe,
            R::Detector,
            BOOL,
            AGENT,
            B::Reviewer("enabled"),
        )),
        true,
    ),
    entry(
        "reviewer.label",
        F::Reviewer,
        "Review label",
        "The label that asks CoderHelm for a review.",
        Repo,
        (REVIEW_KEY, "label"),
        V07,
        Safe,
        R::Detector,
        Kind::Text {
            max: 100,
            multiline: false,
        },
        AGENT,
        B::Reviewer("label"),
    ),
    entry(
        "reviewer.instructions",
        F::Reviewer,
        "Review focus",
        "What reviews of this repo look at first.",
        Repo,
        (REVIEW_KEY, "instructions"),
        V07,
        Safe,
        R::Ai,
        Kind::Text {
            max: crate::review_config::MAX_INSTRUCTIONS,
            multiline: true,
        },
        AGENT,
        B::Reviewer("instructions"),
    ),
    // ── Tests ──
    primary(entry(
        "tests.verify_tests",
        F::Tests,
        "Check the tests in a review",
        "Reviews check that the tests a pull request changes still test what it changes.",
        Repo,
        (REVIEW_KEY, "verify_tests"),
        V07,
        Safe,
        R::Detector,
        BOOL,
        AGENT,
        B::Reviewer("verify_tests"),
    )),
    entry(
        "tests.require_tests",
        F::Tests,
        "Require tests",
        "A pull request that changes code without changing tests gets a blocking finding.",
        Repo,
        (REVIEW_KEY, "require_tests"),
        V07,
        Safe,
        R::Ai,
        BOOL,
        AGENT,
        B::Reviewer("require_tests"),
    ),
    entry(
        "tests.e2e",
        F::Tests,
        "Read test results",
        "CoderHelm reads this repo's test reports from CI and tracks flaky tests.",
        Repo,
        (E2E_KEY, "enabled, repos"),
        V07,
        Safe,
        R::Detector,
        BOOL,
        "tests",
        B::TestsRepo,
    ),
    entry(
        "tests.auto_quarantine",
        F::Tests,
        "Quarantine flaky tests",
        "A test that fails and then passes on retry is quarantined so it stops blocking merges.",
        Team,
        (E2E_KEY, "auto_quarantine"),
        V07,
        Safe,
        R::Default,
        BOOL,
        "tests",
        B::Tests("auto_quarantine"),
    ),
    entry(
        "tests.fix_prs",
        F::Tests,
        "Open fix pull requests for flaky tests",
        "CoderHelm opens a pull request that fixes a quarantined test.",
        Team,
        (E2E_KEY, "fix_prs"),
        V07,
        Acts,
        R::Default,
        BOOL,
        "tests",
        B::Tests("fix_prs"),
    ),
    // ── CI labels ──
    primary(entry(
        "ci_labels.enabled",
        F::CiLabels,
        "Add CI labels",
        "The reviewer adds the labels this repo's workflows run on, from the allowed list only.",
        Repo,
        (REVIEW_KEY, "auto_labels"),
        V07,
        Safe,
        R::Detector,
        BOOL,
        AGENT,
        B::Reviewer("auto_labels"),
    )),
    entry(
        "ci_labels.allow",
        F::CiLabels,
        "Allowed CI labels",
        "The only labels the reviewer may add, comma separated.",
        Repo,
        (REVIEW_KEY, "auto_label_allow"),
        V07,
        Safe,
        R::Ai,
        Kind::Text {
            max: 1_000,
            multiline: false,
        },
        AGENT,
        B::Reviewer("auto_label_allow"),
    ),
    // ── Auto-merge ──
    preview_only(primary(entry(
        "auto_merge.enabled",
        F::AutoMerge,
        "Merge approved pull requests",
        "A pull request the reviewer approves is merged once every required check passes.",
        Repo,
        (REVIEW_KEY, "auto_merge"),
        V07,
        Acts,
        R::Ai,
        BOOL,
        AGENT,
        B::Reviewer("auto_merge"),
    ))),
    entry(
        "auto_merge.merge_method",
        F::AutoMerge,
        "Merge method",
        "How CoderHelm merges: squash, merge commit or rebase.",
        Repo,
        (REVIEW_KEY, "merge_method"),
        V07,
        Safe,
        R::Detector,
        Kind::Choice {
            options: &["squash", "merge", "rebase"],
        },
        AGENT,
        B::Reviewer("merge_method"),
    ),
    entry(
        "auto_merge.require_human_approval",
        F::AutoMerge,
        "Wait for a person's approval",
        "Auto-merge also waits for an approval from a person.",
        Repo,
        (REVIEW_KEY, "require_human_approval"),
        V07,
        Safe,
        R::Default,
        BOOL,
        AGENT,
        B::Reviewer("require_human_approval"),
    ),
    // ── Release tags ──
    primary(entry(
        "tags.auto_tag",
        F::Tags,
        "Tag releases",
        "CoderHelm pushes a release tag after merges to the default branch.",
        Repo,
        (REVIEW_KEY, "auto_tag"),
        V07,
        Acts,
        R::Default,
        BOOL,
        AGENT,
        B::Reviewer("auto_tag"),
    )),
    entry(
        "tags.tag_mode",
        F::Tags,
        "Tag style",
        "Version numbers (semver) or dates.",
        Repo,
        (REVIEW_KEY, "tag_mode"),
        V07,
        Safe,
        R::Detector,
        Kind::Choice {
            options: &["semver", "date"],
        },
        AGENT,
        B::Reviewer("tag_mode"),
    ),
    entry(
        "tags.tag_prefix",
        F::Tags,
        "Tag prefix",
        "Text in front of every tag, such as v.",
        Repo,
        (REVIEW_KEY, "tag_prefix"),
        V07,
        Safe,
        R::Detector,
        Kind::Text {
            max: 50,
            multiline: false,
        },
        AGENT,
        B::Reviewer("tag_prefix"),
    ),
    entry(
        "tags.tag_batch_minutes",
        F::Tags,
        "Merges per tag",
        "Merges within this many minutes share one tag; 0 tags every merge.",
        Repo,
        (REVIEW_KEY, "tag_batch_minutes"),
        V07,
        Safe,
        R::Default,
        Kind::Int { min: 0, max: 360 },
        AGENT,
        B::Reviewer("tag_batch_minutes"),
    ),
    entry(
        "tags.health_check",
        F::Tags,
        "Check health after a merge",
        "After a merge, CoderHelm watches the deploy checks and says when new ones fail.",
        Repo,
        (REVIEW_KEY, "health_check"),
        V07,
        Safe,
        R::Default,
        BOOL,
        AGENT,
        B::Reviewer("health_check"),
    ),
    // ── Release notes ──
    primary(entry(
        "release_notes.enabled",
        F::ReleaseNotes,
        "Write release notes",
        "Each release tag gets notes on GitHub and wherever the release notes settings send them.",
        Repo,
        (REVIEW_KEY, "release_notes"),
        V07,
        Acts,
        R::Default,
        BOOL,
        AGENT,
        B::Reviewer("release_notes"),
    )),
    // ── Review reminders ──
    primary(entry(
        "reminders.enabled",
        F::Reminders,
        "Remind reviewers",
        "Pull requests waiting on a review get a reminder in the team's chat channel.",
        Repo,
        (REVIEW_KEY, "reminders_enabled"),
        V07,
        Acts,
        R::Default,
        BOOL,
        AGENT,
        B::Reviewer("reminders_enabled"),
    )),
    entry(
        "reminders.cooldown_hours",
        F::Reminders,
        "Hours between reminders",
        "The shortest time between two reminders for one pull request.",
        Repo,
        (REVIEW_KEY, "reminder_cooldown_hours"),
        V07,
        Safe,
        R::Default,
        Kind::Int { min: 1, max: 168 },
        AGENT,
        B::Reviewer("reminder_cooldown_hours"),
    ),
    // ── Stale pull requests ──
    primary(entry(
        "stale_prs.enabled",
        F::StalePrs,
        "Clean up stale pull requests",
        "Quiet pull requests are marked stale, then closed if nothing happens.",
        Repo,
        (STALE_KEY, "enabled"),
        "v0.8.76",
        Acts,
        R::Detector,
        BOOL,
        AGENT,
        B::Stale("enabled"),
    )),
    entry(
        "stale_prs.dry_run",
        F::StalePrs,
        "Preview only",
        "Work out what would be marked and closed, and change nothing on GitHub.",
        Repo,
        (STALE_KEY, "dry_run"),
        "v0.8.76",
        Safe,
        R::Default,
        BOOL,
        AGENT,
        B::Stale("dry_run"),
    ),
    entry(
        "stale_prs.stale_days",
        F::StalePrs,
        "Days until stale",
        "Days without activity before a pull request is marked stale.",
        Repo,
        (STALE_KEY, "stale_days"),
        "v0.8.76",
        Safe,
        R::Ai,
        Kind::Int { min: 1, max: 365 },
        AGENT,
        B::Stale("stale_days"),
    ),
    entry(
        "stale_prs.close_days",
        F::StalePrs,
        "Days until closed",
        "Days after the stale warning before the pull request is closed.",
        Repo,
        (STALE_KEY, "close_days"),
        "v0.8.76",
        Safe,
        R::Default,
        Kind::Int { min: 1, max: 90 },
        AGENT,
        B::Stale("close_days"),
    ),
    // ── Security fixes ──
    primary(entry(
        "security.enabled",
        F::Security,
        "Fix vulnerable dependencies",
        "CoderHelm watches this repo's dependencies for advisories and triages each one.",
        Repo,
        (SECURITY_KEY, "enabled, repos"),
        "v0.8.78",
        Safe,
        R::Detector,
        BOOL,
        "security",
        B::SecurityRepo,
    )),
    policy(
        entry(
            "security.open_prs",
            F::Security,
            "Open fix pull requests",
            "Security fixes open pull requests on their schedule instead of only showing what they would do.",
            Team,
            (SECURITY_KEY, "dry_run"),
            "v0.8.78",
            Acts,
            R::Default,
            BOOL,
            "security",
            B::SecurityOpenPrs,
        ),
        true,
    ),
    // ── Coder instructions ──
    primary(entry(
        "instructions.repo",
        F::Instructions,
        "Coder instructions",
        "What CoderHelm's coder follows in this repo: commands, conventions and what to avoid.",
        Repo,
        ("INSTRUCTIONS#REPO#<repo>", "content"),
        "v0.1.0",
        Safe,
        R::Ai,
        Kind::Text {
            max: crate::instructions::MAX_BYTES,
            multiline: true,
        },
        AGENT,
        B::Instructions,
    )),
    // ── Git conventions ──
    entry(
        "git_conventions.branch",
        F::GitConventions,
        "Branch name",
        "The template CoderHelm names its branches with; empty keeps CoderHelm's default.",
        Team,
        (crate::git_conventions::SETTINGS_SK, "branch"),
        V07,
        Safe,
        R::Default,
        Kind::Text {
            max: crate::git_conventions::MAX_TEMPLATE_CHARS,
            multiline: false,
        },
        AGENT,
        B::GitConvention("branch"),
    ),
    entry(
        "git_conventions.commit",
        F::GitConventions,
        "Commit message",
        "The template for CoderHelm's commit messages; empty keeps CoderHelm's default.",
        Team,
        (crate::git_conventions::SETTINGS_SK, "commit"),
        V07,
        Safe,
        R::Default,
        Kind::Text {
            max: crate::git_conventions::MAX_TEMPLATE_CHARS,
            multiline: false,
        },
        AGENT,
        B::GitConvention("commit"),
    ),
    entry(
        "git_conventions.pr_title",
        F::GitConventions,
        "Pull request title",
        "The template for CoderHelm's pull request titles; empty keeps CoderHelm's default.",
        Team,
        (crate::git_conventions::SETTINGS_SK, "pr_title"),
        V07,
        Safe,
        R::Default,
        Kind::Text {
            max: crate::git_conventions::MAX_TEMPLATE_CHARS,
            multiline: false,
        },
        AGENT,
        B::GitConvention("pr_title"),
    ),
    // ── Alerts ──
    primary(entry(
        "alerts.daily_budget",
        F::Alerts,
        "Daily alert token budget",
        "The most tokens alert runs may use in 24 hours; 0 stops alert runs.",
        Team,
        (crate::alert_budget::SK, "daily_tokens"),
        V07,
        Safe,
        R::Default,
        Kind::Int {
            min: 0,
            max: crate::alert_budget::MAX_DAILY_TOKENS as i64,
        },
        "alerts",
        B::AlertBudget,
    )),
    entry(
        "logs.autofix.enabled",
        F::Alerts,
        "Automatic fix PRs for log findings",
        "CoderHelm opens a fix PR for new critical log findings, up to 3 a day, and posts it to the alerts channel.",
        Team,
        (crate::log_findings::AUTOFIX_SK, "enabled"),
        V07,
        Acts,
        R::Detector,
        BOOL,
        "alerts",
        B::LogAutofix,
    ),
    // ── Code graph ──
    policy(
        primary(entry(
            "code_graph.enabled",
            F::CodeGraph,
            "Build the code graph",
            "CoderHelm keeps a map of this repo's code that its agents search.",
            Repo,
            ("GRAPH_CONFIG#REPO#<repo>", "enabled"),
            V07,
            Safe,
            R::Default,
            BOOL,
            "repos",
            B::CodeGraph,
        )),
        true,
    ),
];

pub fn get(id: &str) -> Option<&'static Entry> {
    REGISTRY.iter().find(|e| e.id == id)
}

/// The features with a main switch, in registry order.
pub fn features() -> Vec<Feature> {
    let mut out: Vec<Feature> = Vec::new();
    for e in REGISTRY.iter().filter(|e| e.primary) {
        if !out.contains(&e.feature) {
            out.push(e.feature);
        }
    }
    out
}

// ── Values ──────────────────────────────────────────────────────────────

/// A value as read now.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
pub struct Current {
    pub value: Value,
    /// Someone saved this value (it isn't only the default because nothing
    /// was ever saved).
    pub stored: bool,
    /// The row that holds it exists.
    #[serde(default)]
    pub row: bool,
}

/// What a write is for.
#[derive(Debug, Clone)]
pub struct Ctx<'a> {
    pub repo: &'a str,
    /// Every connected repo of the team (turning a team-wide switch off for
    /// one repo keeps it on for the others).
    pub all_repos: &'a [String],
    pub by: &'a str,
    pub now: String,
}

/// Work the gateway does after a write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Effect {
    /// Build the repo's code graph if it is on (a reviewer config was saved
    /// or the graph was turned on).
    BuildGraph { repo: String },
}

/// The value is not one the setting takes. Never shown as a crash: the
/// text is for people.
pub fn check(e: &Entry, v: &Value) -> Result<Value, String> {
    match e.kind {
        Kind::Bool => v
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| format!("{} is on or off.", e.title)),
        Kind::Int { min, max } => match v.as_i64() {
            Some(n) if (min..=max).contains(&n) => Ok(json!(n)),
            _ => Err(format!(
                "{} is a whole number from {min} to {max}.",
                e.title
            )),
        },
        Kind::Text { max, multiline } => {
            let t = v.as_str().ok_or_else(|| format!("{} is text.", e.title))?;
            if t.chars().count() > max {
                return Err(format!("{} is at most {max} characters.", e.title));
            }
            if t.chars()
                .any(|c| c.is_control() && !(multiline && (c == '\n' || c == '\t' || c == '\r')))
            {
                return Err(format!("{} has characters it can't keep.", e.title));
            }
            Ok(Value::String(t.to_string()))
        }
        Kind::Choice { options } => match v.as_str() {
            Some(o) if options.contains(&o) => Ok(json!(o)),
            _ => Err(format!("{} is one of {}.", e.title, options.join(", "))),
        },
    }
}

/// The row a value lives in, for this repo (rows created by a Setup run are
/// removed whole when the run is undone).
pub fn row(e: &Entry, repo: &str) -> String {
    match e.binding {
        B::Reviewer(_) => crate::review_config::sk(repo),
        B::Stale(_) => stale::repo_sk(repo),
        B::SecurityRepo | B::SecurityOpenPrs => sec::CONFIG_SK.to_string(),
        B::TestsRepo | B::Tests(_) => E2E_ROW.to_string(),
        B::Instructions => crate::instructions::repo_sk(repo),
        B::GitConvention(_) => crate::git_conventions::SETTINGS_SK.to_string(),
        B::CodeGraph => crate::code_graph::config_sk(repo),
        B::AlertBudget => crate::alert_budget::SK.to_string(),
        B::LogAutofix => crate::log_findings::AUTOFIX_SK.to_string(),
    }
}

// Pure readings: the stored row(s) in, the page's value out. The defaults
// are these with nothing stored.

fn reviewer_value(item: Option<&Item>, field: &str) -> Current {
    let empty = Item::new();
    let it = item.unwrap_or(&empty);
    Current {
        value: crate::review_config::view(it)[field].clone(),
        stored: it.contains_key(field),
        row: item.is_some(),
    }
}

fn stale_of(item: Option<&Item>) -> Option<StaleConfig> {
    item.and_then(|i| i.get(stale::ATTR_CONFIG))
        .and_then(|v| v.as_s().ok())
        .and_then(|t| StaleConfig::from_json(t))
}

fn stale_value(team: Option<&StaleConfig>, own: Option<&StaleConfig>, field: &str) -> Current {
    let eff = stale::effective(team, own);
    Current {
        value: serde_json::to_value(&eff).unwrap_or(Value::Null)[field].clone(),
        stored: team.is_some() || own.is_some(),
        row: own.is_some(),
    }
}

fn security_of(item: Option<&Item>) -> Option<SecurityConfig> {
    item.and_then(|i| i.get("config"))
        .and_then(|v| v.as_s().ok())
        .and_then(|t| SecurityConfig::from_json(t))
}

/// Whether someone chose a value for this repo in the team's security
/// policy: the repo named in its list, or the policy on for every repo. A
/// repo missing from a list another repo is in was never chosen for; nor
/// was a team switch still at its default.
fn security_value(cfg: Option<&SecurityConfig>, binding: Binding, repo: &str) -> Current {
    let c = cfg.cloned().unwrap_or_default();
    let d = SecurityConfig::default();
    let (value, chosen) = match binding {
        B::SecurityRepo => (
            c.enabled && c.repos.includes(repo),
            match &c.repos {
                Repos::All => c.enabled,
                Repos::List(l) => c.enabled && l.iter().any(|r| r.eq_ignore_ascii_case(repo)),
            },
        ),
        _ => (!c.dry_run, c.dry_run != d.dry_run),
    };
    Current {
        value: Value::Bool(value),
        stored: cfg.is_some() && chosen,
        row: cfg.is_some(),
    }
}

/// The same for the team's E2E settings: the repo named in `repos`, or the
/// settings on for every repo (an empty list); a team switch counts as
/// chosen once it differs from its default.
fn tests_value(s: Option<&E2eSettings>, binding: Binding, repo: &str) -> Current {
    let st = s.cloned().unwrap_or_default();
    let (value, chosen) = match binding {
        B::TestsRepo => (
            st.covers(repo),
            st.enabled
                && (st.repos.is_empty() || st.repos.iter().any(|r| r.eq_ignore_ascii_case(repo))),
        ),
        B::Tests(field) => {
            let read = |x: &E2eSettings| {
                serde_json::to_value(x).unwrap_or(Value::Null)[field]
                    .as_bool()
                    .unwrap_or(false)
            };
            let v = read(&st);
            (v, v != read(&E2eSettings::default()))
        }
        _ => (false, false),
    };
    Current {
        value: Value::Bool(value),
        stored: s.is_some() && chosen,
        row: s.is_some(),
    }
}

fn git_value(item: Option<&Item>, field: &str) -> Current {
    let c = item
        .map(crate::git_conventions::GitConventions::from_item)
        .unwrap_or_default();
    let v = match field {
        "branch" => c.branch,
        "commit" => c.commit,
        _ => c.pr_title,
    };
    Current {
        stored: v.is_some(),
        value: Value::String(v.unwrap_or_default()),
        row: item.is_some(),
    }
}

fn graph_value(item: Option<&Item>) -> Current {
    let own = crate::code_graph::own_switch(item);
    Current {
        value: Value::Bool(crate::code_graph::effective_enabled(own)),
        stored: own.is_some(),
        row: item.is_some(),
    }
}

fn log_autofix_value(item: Option<&Item>) -> Current {
    Current {
        value: Value::Bool(crate::log_findings::AutoFix::from_item(item).enabled),
        stored: item.is_some(),
        row: item.is_some(),
    }
}

fn budget_value(item: Option<&Item>) -> Current {
    let saved = crate::alert_budget::saved(item);
    Current {
        value: json!(saved.unwrap_or(crate::alert_budget::DEFAULT_DAILY_TOKENS)),
        stored: saved.is_some(),
        row: item.is_some(),
    }
}

/// The value with nothing saved: the settings page's own default.
pub fn default_value(e: &Entry) -> Value {
    match e.binding {
        B::Reviewer(f) => reviewer_value(None, f).value,
        B::Stale(f) => stale_value(None, None, f).value,
        B::SecurityRepo | B::SecurityOpenPrs => security_value(None, e.binding, "").value,
        B::TestsRepo | B::Tests(_) => tests_value(None, e.binding, "").value,
        B::Instructions => Value::String(String::new()),
        B::GitConvention(f) => git_value(None, f).value,
        B::CodeGraph => graph_value(None).value,
        B::AlertBudget => budget_value(None).value,
        B::LogAutofix => log_autofix_value(None).value,
    }
}

/// CoderHelm's pick when nothing in the repo says otherwise.
pub fn policy_value(e: &Entry) -> Value {
    e.policy
        .map(Value::Bool)
        .unwrap_or_else(|| default_value(e))
}

/// The value now.
pub async fn read(store: &dyn Store, e: &Entry, repo: &str) -> Result<Current, String> {
    Ok(match e.binding {
        B::Reviewer(f) => reviewer_value(
            store.get(&crate::review_config::sk(repo)).await?.as_ref(),
            f,
        ),
        B::Stale(f) => {
            let team = stale_of(store.get(stale::TEAM_SK).await?.as_ref());
            let own = stale_of(store.get(&stale::repo_sk(repo)).await?.as_ref());
            stale_value(team.as_ref(), own.as_ref(), f)
        }
        B::SecurityRepo | B::SecurityOpenPrs => security_value(
            security_of(store.get(sec::CONFIG_SK).await?.as_ref()).as_ref(),
            e.binding,
            repo,
        ),
        B::TestsRepo | B::Tests(_) => {
            tests_value(store.e2e_settings().await?.as_ref(), e.binding, repo)
        }
        B::Instructions => {
            let item = store.get(&crate::instructions::repo_sk(repo)).await?;
            Current {
                value: Value::String(crate::instructions::content_of(item.as_ref())),
                stored: item.is_some(),
                row: item.is_some(),
            }
        }
        B::GitConvention(f) => git_value(
            store
                .get(crate::git_conventions::SETTINGS_SK)
                .await?
                .as_ref(),
            f,
        ),
        B::CodeGraph => graph_value(
            store
                .get(&crate::code_graph::config_sk(repo))
                .await?
                .as_ref(),
        ),
        B::AlertBudget => budget_value(store.get(crate::alert_budget::SK).await?.as_ref()),
        B::LogAutofix => {
            log_autofix_value(store.get(crate::log_findings::AUTOFIX_SK).await?.as_ref())
        }
    })
}

/// Why a write didn't happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// The setting's own validation refused the value; the text says why.
    Invalid(String),
    /// Storage failed.
    Store(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Invalid(m) => write!(f, "{m}"),
            WriteError::Store(m) => write!(f, "Couldn't save: {m}"),
        }
    }
}

fn store_err(e: String) -> WriteError {
    WriteError::Store(e)
}

/// One repo joins or leaves a team-wide list whose "everything" form is
/// `all` (security fixes: `Repos::All`; E2E: an empty list). Off for one
/// repo keeps the others on.
fn membership(
    enabled: &mut bool,
    list: Option<&mut Vec<String>>,
    repo: &str,
    on: bool,
    all_repos: &[String],
) -> Option<Vec<String>> {
    let same = |a: &str| a.eq_ignore_ascii_case(repo);
    match (on, *enabled, list) {
        (true, false, _) => {
            *enabled = true;
            Some(vec![repo.to_string()])
        }
        (true, true, None) => None,
        (true, true, Some(l)) => {
            if !l.iter().any(|r| same(r)) {
                l.push(repo.to_string());
            }
            None
        }
        (false, false, _) => None,
        (false, true, None) => Some(all_repos.iter().filter(|r| !same(r)).cloned().collect()),
        (false, true, Some(l)) => {
            l.retain(|r| !same(r));
            None
        }
    }
}

/// Writes `value` (already [`check`]ed) through the setting's own
/// validation and storage. Only this setting changes: the rest of its row
/// is written back as it was.
pub async fn write(
    store: &dyn Store,
    e: &Entry,
    ctx: &Ctx<'_>,
    value: &Value,
) -> Result<Vec<Effect>, WriteError> {
    let value = check(e, value).map_err(WriteError::Invalid)?;
    let team = store.team().to_string();
    match e.binding {
        B::Reviewer(field) => {
            let sk = crate::review_config::sk(ctx.repo);
            let prior = store.get(&sk).await.map_err(store_err)?;
            let empty = Item::new();
            let mut body = crate::review_config::view(prior.as_ref().unwrap_or(&empty));
            body[field] = value;
            let mut item =
                crate::review_config::build(&team, ctx.repo, &body, prior.as_ref(), &ctx.now)
                    .map_err(|r| WriteError::Invalid(r.to_string()))?;
            // The reminder webhooks (and anything else the form doesn't
            // carry) stay exactly as stored.
            for (k, v) in prior.unwrap_or_default() {
                item.entry(k).or_insert(v);
            }
            store.put(item).await.map_err(store_err)?;
            if let Err(err) = store.index(crate::review_config_index::NAME).await {
                tracing::warn!(error = %err, "Setup: could not add the team to the reviewer config index");
            }
            Ok(vec![Effect::BuildGraph {
                repo: ctx.repo.to_string(),
            }])
        }
        B::Stale(field) => {
            let team_cfg = stale_of(store.get(stale::TEAM_SK).await.map_err(store_err)?.as_ref());
            let own = stale_of(
                store
                    .get(&stale::repo_sk(ctx.repo))
                    .await
                    .map_err(store_err)?
                    .as_ref(),
            );
            let mut v = serde_json::to_value(stale::effective(team_cfg.as_ref(), own.as_ref()))
                .map_err(|x| store_err(x.to_string()))?;
            v[field] = value;
            let cfg = serde_json::from_value::<StaleConfig>(v)
                .map_err(|x| WriteError::Invalid(x.to_string()))?
                .sanitized();
            let text = serde_json::to_string(&cfg).map_err(|x| store_err(x.to_string()))?;
            store
                .put(config_item(
                    &team,
                    &stale::repo_sk(ctx.repo),
                    &text,
                    ctx.by,
                    &ctx.now,
                ))
                .await
                .map_err(store_err)?;
            Ok(vec![])
        }
        B::SecurityRepo | B::SecurityOpenPrs => {
            let mut cfg = security_of(store.get(sec::CONFIG_SK).await.map_err(store_err)?.as_ref())
                .unwrap_or_default();
            let on = value.as_bool().unwrap_or(false);
            if e.binding == B::SecurityOpenPrs {
                cfg.dry_run = !on;
            } else {
                let mut enabled = cfg.enabled;
                let replaced = match &mut cfg.repos {
                    Repos::All => membership(&mut enabled, None, ctx.repo, on, ctx.all_repos),
                    Repos::List(l) => {
                        membership(&mut enabled, Some(l), ctx.repo, on, ctx.all_repos)
                    }
                };
                if let Some(l) = replaced {
                    cfg.repos = Repos::List(l);
                }
                if matches!(&cfg.repos, Repos::List(l) if l.is_empty()) {
                    enabled = false;
                    cfg.repos = Repos::All;
                }
                cfg.enabled = enabled;
            }
            cfg.validate().map_err(WriteError::Invalid)?;
            let cfg = cfg.cleaned();
            let text = serde_json::to_string(&cfg).map_err(|x| store_err(x.to_string()))?;
            store
                .put(config_item(&team, sec::CONFIG_SK, &text, ctx.by, &ctx.now))
                .await
                .map_err(store_err)?;
            if let Err(err) = store.index(sec::INDEX_NAME).await {
                tracing::warn!(error = %err, "Setup: could not index the team for security fixes");
            }
            Ok(vec![])
        }
        B::TestsRepo | B::Tests(_) => {
            let mut st = store
                .e2e_settings()
                .await
                .map_err(store_err)?
                .unwrap_or_default();
            let on = value.as_bool().unwrap_or(false);
            if let B::Tests(field) = e.binding {
                let mut v = serde_json::to_value(&st).map_err(|x| store_err(x.to_string()))?;
                v[field] = Value::Bool(on);
                st = serde_json::from_value(v).map_err(|x| WriteError::Invalid(x.to_string()))?;
            } else {
                let mut enabled = st.enabled;
                let list = (!st.repos.is_empty()).then_some(&mut st.repos);
                if let Some(l) = membership(&mut enabled, list, ctx.repo, on, ctx.all_repos) {
                    st.repos = l;
                }
                // An empty list means every repo: with none left, it's off.
                if enabled && !on && st.repos.is_empty() {
                    enabled = false;
                }
                st.enabled = enabled;
            }
            st.validate().map_err(WriteError::Invalid)?;
            store
                .put_e2e_settings(&st.sanitized())
                .await
                .map_err(store_err)?;
            Ok(vec![])
        }
        B::Instructions => {
            let item = crate::instructions::item(
                &team,
                &crate::instructions::repo_sk(ctx.repo),
                value.as_str().unwrap_or(""),
                &ctx.now,
            )
            .map_err(|_| {
                WriteError::Invalid(format!(
                    "Coder instructions are at most {} bytes.",
                    crate::instructions::MAX_BYTES
                ))
            })?;
            store.put(item).await.map_err(store_err)?;
            Ok(vec![])
        }
        B::GitConvention(field) => {
            use crate::git_conventions::{validate, GitConventions, Kind as GitKind};
            let prior = store
                .get(crate::git_conventions::SETTINGS_SK)
                .await
                .map_err(store_err)?;
            let mut c = prior
                .as_ref()
                .map(GitConventions::from_item)
                .unwrap_or_default();
            let t = value.as_str().unwrap_or("").trim().to_string();
            let kind = match field {
                "branch" => GitKind::Branch,
                "commit" => GitKind::Commit,
                _ => GitKind::PrTitle,
            };
            validate(kind, &t).map_err(WriteError::Invalid)?;
            let t = (!t.is_empty()).then_some(t);
            match field {
                "branch" => c.branch = t,
                "commit" => c.commit = t,
                _ => c.pr_title = t,
            }
            store
                .put(c.to_item(&team, &ctx.now))
                .await
                .map_err(store_err)?;
            Ok(vec![])
        }
        B::CodeGraph => {
            let on = value.as_bool().unwrap_or(true);
            store
                .put(crate::code_graph::config_item(
                    &team, ctx.repo, on, ctx.by, &ctx.now,
                ))
                .await
                .map_err(store_err)?;
            Ok(if on {
                vec![Effect::BuildGraph {
                    repo: ctx.repo.to_string(),
                }]
            } else {
                vec![]
            })
        }
        B::AlertBudget => {
            let n = value.as_u64().unwrap_or(0);
            let item = crate::alert_budget::item(&team, n, ctx.by, &ctx.now).map_err(|_| {
                WriteError::Invalid(format!(
                    "The alert budget is at most {} tokens.",
                    crate::alert_budget::MAX_DAILY_TOKENS
                ))
            })?;
            store.put(item).await.map_err(store_err)?;
            Ok(vec![])
        }
        B::LogAutofix => {
            use crate::log_findings as lf;
            let on = value.as_bool().unwrap_or(false);
            let mut cfg = lf::AutoFix::from_item(
                store.get(lf::AUTOFIX_SK).await.map_err(store_err)?.as_ref(),
            );
            // As the Logs page saves it: `enabled_at` moves only when it is
            // turned on, so the backlog never gets fix PRs.
            cfg.enabled_at = match (on, cfg.enabled) {
                (true, false) => ctx.now.clone(),
                (true, true) => cfg.enabled_at,
                (false, _) => String::new(),
            };
            cfg.enabled = on;
            cfg.validate().map_err(WriteError::Invalid)?;
            let json = serde_json::to_string(&cfg).map_err(|e| store_err(e.to_string()))?;
            store
                .put(config_item(&team, lf::AUTOFIX_SK, &json, ctx.by, &ctx.now))
                .await
                .map_err(store_err)?;
            store.index(lf::AUTOFIX_INDEX).await.map_err(store_err)?;
            Ok(vec![])
        }
    }
}

/// Takes the value back to "never saved" where the row keeps one field per
/// setting (undo of a value that was never set). `false` when the setting
/// can't be unset alone; the caller then writes the old value.
pub async fn unset(store: &dyn Store, e: &Entry, repo: &str) -> Result<bool, String> {
    let (sk, attr) = match e.binding {
        B::Reviewer(field) => (crate::review_config::sk(repo), field),
        B::GitConvention(field) => (crate::git_conventions::SETTINGS_SK.to_string(), field),
        _ => return Ok(false),
    };
    if let Some(mut item) = store.get(&sk).await? {
        item.remove(attr);
        store.put(item).await?;
    }
    Ok(true)
}

/// Removes a row a Setup run created (undo, when every change in it is
/// taken back).
pub async fn remove_row(store: &dyn Store, e: &Entry, repo: &str) -> Result<(), String> {
    match e.binding {
        B::TestsRepo | B::Tests(_) => store.delete_e2e_settings().await,
        _ => store.delete(&row(e, repo)).await,
    }
}

/// The setting that turns a preview into the real thing, and its value:
/// stale cleanup leaves its dry run, security fixes open pull requests,
/// auto-merge goes on. `None` when `id` has no preview.
pub fn switch_on_target(id: &str) -> Option<(&'static Entry, Value)> {
    let (target, v) = match id {
        "stale_prs.enabled" | "stale_prs.dry_run" => ("stale_prs.dry_run", json!(false)),
        "security.enabled" | "security.open_prs" => ("security.open_prs", json!(true)),
        "auto_merge.enabled" => ("auto_merge.enabled", json!(true)),
        _ => return None,
    };
    get(target).map(|e| (e, v))
}

/// The entries sharing one row with `e` for a repo.
pub fn same_row(e: &Entry) -> Vec<&'static Entry> {
    let probe = "o/r";
    let mine = row(e, probe);
    REGISTRY.iter().filter(|x| row(x, probe) == mine).collect()
}

/// Who last saved the row a value lives in, and when, where the row keeps
/// it (`updated_by`, `updated_at`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct SetBy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    /// The page the value is saved on, for "Set on the Tests page".
    pub page: String,
}

/// The page a setting is saved on, in words.
pub fn page_name(e: &Entry) -> &'static str {
    match e.binding {
        B::Reviewer(_) => "Reviewer settings",
        B::Stale(_) => "Stale pull requests",
        B::SecurityRepo | B::SecurityOpenPrs => "Security policy",
        B::TestsRepo | B::Tests(_) => "Tests",
        B::Instructions => "Instructions",
        B::GitConvention(_) => "Workflow",
        B::CodeGraph => "Code graph",
        B::AlertBudget => "Alerts",
        B::LogAutofix => "Logs",
    }
}

/// Who last saved `e`'s row for `repo`, when the row says.
pub async fn set_by(store: &dyn Store, e: &Entry, repo: &str) -> SetBy {
    let item = match e.binding {
        B::TestsRepo | B::Tests(_) => None,
        _ => store.get(&row(e, repo)).await.ok().flatten(),
    };
    let attr = |k: &str| {
        item.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };
    SetBy {
        by: attr("updated_by"),
        at: attr("updated_at"),
        page: page_name(e).to_string(),
    }
}

/// Every entry's current value for one repo, reading each row once.
pub async fn read_all(
    store: &dyn Store,
    repo: &str,
) -> Result<HashMap<&'static str, Current>, String> {
    let cached = super::store::Cached::new(store);
    let mut out = HashMap::new();
    for e in REGISTRY {
        out.insert(e.id, read(&cached, e, repo).await?);
    }
    Ok(out)
}
