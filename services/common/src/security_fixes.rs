//! Security fixes: advisories that touch a team's dependencies, triaged by
//! whether the code reaches the vulnerable part, fixed with one pull request
//! per fix, and posted to the alerts channel when urgent. Shared by the
//! gateway (the API, webhooks) and the worker (inventory, sources, triage,
//! fix runs, cards: `worker::passes::security_fixes`).
//!
//! The rules:
//!
//! - Off until a team turns it on, and then a dry run (a preview, nothing
//!   opened or posted) until someone turns the dry run off.
//! - One finding per (alias cluster, repository, manifest, package): the
//!   same flaw under its CVE, GHSA and ecosystem ids counts once. Clusters
//!   are the union of OSV `aliases` ([`clusters`]); a new id that joins an
//!   existing finding's ids is merged into it ([`merge_target`]).
//! - A verdict never closes an alert. It sets the tier ([`tier_of`]) and is
//!   shown with its evidence; only a person dismisses.
//! - One pull request per fix: every finding a bump clears goes in it. P2
//!   and P3 are grouped per repository and ecosystem, daily and weekly.
//! - One card per thing: each card is claimed once (`ALERT_POSTED#SEC#...`),
//!   and a dismissed, fixed or withdrawn finding never posts.
//!
//! Records live in the settings table under the team (`pk` = team id):
//! [`CONFIG_SK`] the settings, [`finding_sk`] each finding, [`event_sk`] its
//! timeline (kept [`EVENT_TTL_DAYS`]), [`deps_sk`] each repository's
//! dependency list, [`STATE_SK`] the team's source status, [`ticket_sk`] a
//! fix run's plan. Global data (the CISA list) lives under [`GLOBAL_PK`].

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::notify::{Block, Button, Message, Size, Spacing, Text, Tone};

// ── Keys ────────────────────────────────────────────────────────────────

/// The team's settings.
pub const CONFIG_SK: &str = "SECURITY_CONFIG#TEAM";
/// The index of teams with settings (`INDEX#SECURITY_CONFIG`).
pub const INDEX_NAME: &str = "SECURITY_CONFIG";
pub const FINDING_PREFIX: &str = "SECFINDING#";
pub const EVENT_PREFIX: &str = "SECEVENT#";
pub const DEPS_PREFIX: &str = "DEPS#";
/// The team's source status: Dependabot access, when each source last synced.
pub const STATE_SK: &str = "SECURITY_STATE";
/// A fix run's plan, by ticket.
pub const TICKET_SK_PREFIX: &str = "SEC_TICKET#";
/// Global records (the CISA list).
pub const GLOBAL_PK: &str = "SECURITY#GLOBAL";
pub const KEV_SK: &str = "KEV";
/// Ticket ids of security fix runs.
pub const TICKET_PREFIX: &str = "SEC-";
/// Timeline entries are kept this long.
pub const EVENT_TTL_DAYS: i64 = 180;
/// Card claims: one card per stage, ever.
pub const CARD_CLAIM_SECS: u64 = 400 * 86_400;
/// Written into every comment CoderHelm posts for this.
pub const MARKER: &str = "<!-- coderhelm:security -->";
/// Credit for the advisory data (CC-BY).
pub const ATTRIBUTION: &str = "Advisory data: GitHub, OSV (CC-BY)";

/// The outbound hosts the feature calls (self-host docs list them).
pub const OSV_API: &str = "https://api.osv.dev";
pub const KEV_URL: &str =
    "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json";
pub const EPSS_API: &str = "https://api.first.org/data/v1/epss";

pub fn finding_sk(id: &str) -> String {
    format!("{FINDING_PREFIX}{id}")
}

pub fn event_prefix(id: &str) -> String {
    format!("{EVENT_PREFIX}{id}#")
}

pub fn event_sk(id: &str, at: &DateTime<Utc>, n: u32) -> String {
    format!(
        "{EVENT_PREFIX}{id}#{}#{n:03}",
        at.format("%Y-%m-%dT%H:%M:%S%.3fZ")
    )
}

pub fn deps_sk(repo: &str) -> String {
    format!("{DEPS_PREFIX}{}", repo.to_ascii_lowercase())
}

pub fn ticket_sk(ticket: &str) -> String {
    format!("{TICKET_SK_PREFIX}{ticket}")
}

/// The claim that one card is posted for one finding (or pull request) and
/// stage: `ALERT_POSTED#SEC#<thing>#<stage>`.
pub fn card_claim_sk(thing: &str, stage: &str) -> String {
    format!("ALERT_POSTED#SEC#{thing}#{stage}")
}

pub fn is_ticket(ticket_id: &str) -> bool {
    ticket_id.starts_with(TICKET_PREFIX)
}

// ── Enums ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Tier {
    P0,
    P1,
    P2,
    P3,
}

impl Tier {
    pub const ALL: [Tier; 4] = [Tier::P0, Tier::P1, Tier::P2, Tier::P3];
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::P0 => "P0",
            Tier::P1 => "P1",
            Tier::P2 => "P2",
            Tier::P3 => "P3",
        }
    }
    pub fn parse(s: &str) -> Option<Tier> {
        Tier::ALL
            .into_iter()
            .find(|t| t.as_str().eq_ignore_ascii_case(s.trim()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Reachable,
    NotReachable,
    NotLoaded,
    Unknown,
    #[default]
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    #[default]
    Open,
    Fixing,
    PrOpen,
    Fixed,
    Dismissed,
    Withdrawn,
    NoFix,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Open => "open",
            Status::Fixing => "fixing",
            Status::PrOpen => "pr_open",
            Status::Fixed => "fixed",
            Status::Dismissed => "dismissed",
            Status::Withdrawn => "withdrawn",
            Status::NoFix => "no_fix",
        }
    }
    /// Still needs something: counted as open.
    pub fn is_open(self) -> bool {
        matches!(
            self,
            Status::Open | Status::Fixing | Status::PrOpen | Status::NoFix
        )
    }
    /// Handled: never posts again.
    pub fn is_handled(self) -> bool {
        matches!(self, Status::Fixed | Status::Dismissed | Status::Withdrawn)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Runtime,
    Dev,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Relation {
    Direct,
    Transitive,
    #[default]
    Unknown,
}

/// When a tier's pull requests open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cadence {
    Now,
    Hour,
    Daily,
    Weekly,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MajorBumps {
    #[default]
    FixInPr,
    IssueOnly,
}

// ── The finding (the API's shape) ───────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Advisory {
    pub ids: Vec<String>,
    pub primary: String,
    pub title: String,
    pub summary: String,
    pub cvss: Option<f64>,
    pub cvss_vector: Option<String>,
    pub epss: Option<f64>,
    pub epss_percentile: Option<f64>,
    pub kev: bool,
    pub kev_due: Option<String>,
    pub malware: bool,
    pub cwe: Vec<String>,
    pub published_at: Option<String>,
    pub urls: Vec<String>,
    /// `dependabot`, `osv` or `github_advisory`.
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct EvidenceLine {
    pub file: String,
    pub line: u64,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Evidence {
    pub summary: String,
    pub lines: Vec<EvidenceLine>,
    /// What was checked, in plain words.
    pub checked: String,
    /// `high`, `medium` or `low`.
    pub confidence: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PrRef {
    pub number: u64,
    pub url: String,
    /// `pending`, `passing`, `failing`, or none yet.
    pub ci: Option<String>,
    /// `open`, `merged` or `closed`.
    pub state: String,
    pub adopted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AlertRef {
    pub number: u64,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Dismissed {
    pub by: String,
    pub reason: String,
    pub until: Option<String>,
    pub at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Finding {
    pub id: String,
    pub repo: String,
    pub manifest: String,
    pub ecosystem: String,
    pub package: String,
    pub version: String,
    pub fixed_version: Option<String>,
    pub advisory: Advisory,
    pub scope: Scope,
    pub relation: Relation,
    pub path: Vec<String>,
    pub verdict: Verdict,
    pub evidence: Option<Evidence>,
    pub tier: Option<Tier>,
    /// The verdict is still being checked, so the tier may change once it is.
    pub tier_provisional: bool,
    /// Why its fix hasn't started though it is due (a daily limit), in
    /// plain words; None when nothing holds it.
    pub waiting: Option<String>,
    pub due_at: String,
    pub status: Status,
    pub pr: Option<PrRef>,
    pub dependabot_alert: Option<AlertRef>,
    pub dismissed: Option<Dismissed>,
    pub first_seen_at: String,
    pub updated_at: String,
    /// A private report against the repository's own code (a repository
    /// advisory still in triage or draft): never posted to a channel, and
    /// never fixed in a public pull request or issue.
    pub private_report: bool,
}

/// What CoderHelm keeps about a finding beyond what the API shows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FindingState {
    /// GHSA's severity word, used when there is no CVSS score.
    pub severity: Option<String>,
    /// Vulnerable ranges for this package, as the sources wrote them.
    pub ranges: Vec<String>,
    /// Fixed versions the sources name.
    pub fixed: Vec<String>,
    /// Vulnerable functions the sources name.
    pub symbols: Vec<String>,
    /// Advisory ids that were withdrawn.
    pub withdrawn: Vec<String>,
    /// The fix run working on it.
    pub ticket: Option<String>,
    /// What the last triage read (so it isn't read twice).
    pub triaged: Option<String>,
    /// A person closed its pull request unmerged: not opened again until.
    pub hold_until: Option<String>,
    pub merged_at: Option<String>,
    /// Stages whose card posted (`new`, `half`, `deadline`, ...).
    pub cards: Vec<String>,
    /// When its fix pull request opened (or was adopted).
    pub pr_opened_at: Option<String>,
    /// A person asked for its fix pull request (their email): it starts on
    /// the next tick with room under the day's limits, dry run or not.
    pub requested_by: Option<String>,
}

// ── Settings ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierMap<T> {
    #[serde(rename = "P0")]
    pub p0: T,
    #[serde(rename = "P1")]
    pub p1: T,
    #[serde(rename = "P2")]
    pub p2: T,
    #[serde(rename = "P3")]
    pub p3: T,
}

impl<T: Copy> TierMap<T> {
    pub fn get(&self, t: Tier) -> T {
        match t {
            Tier::P0 => self.p0,
            Tier::P1 => self.p1,
            Tier::P2 => self.p2,
            Tier::P3 => self.p3,
        }
    }
}

/// Which repositories: every enabled one, or a list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Repos {
    #[default]
    All,
    List(Vec<String>),
}

impl Serialize for Repos {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Repos::All => s.serialize_str("all"),
            Repos::List(l) => l.serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for Repos {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match Value::deserialize(d)? {
            Value::String(s) if s == "all" => Ok(Repos::All),
            Value::Array(a) => Ok(Repos::List(
                a.into_iter()
                    .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
                    .filter(|s| !s.is_empty())
                    .collect(),
            )),
            _ => Err(serde::de::Error::custom(
                "repos is \"all\" or a list of owner/name",
            )),
        }
    }
}

impl Repos {
    pub fn includes(&self, repo: &str) -> bool {
        match self {
            Repos::All => true,
            Repos::List(l) => l.iter().any(|r| r.eq_ignore_ascii_case(repo)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Ignore {
    pub advisory: String,
    pub repo: Option<String>,
    pub reason: String,
    pub until: String,
    pub by: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    pub enabled: bool,
    pub dry_run: bool,
    pub open_prs: TierMap<Cadence>,
    pub post_alerts: TierMap<bool>,
    pub escalate_half: bool,
    pub due_days: TierMap<u32>,
    pub use_kev_due: bool,
    pub labels: Vec<String>,
    pub severity_labels: bool,
    pub reachability_labels: bool,
    pub ecosystem_labels: bool,
    pub major_bumps: MajorBumps,
    pub adopt_bot_prs: bool,
    pub repos: Repos,
    pub ignore: Vec<Ignore>,
    /// Model tokens reachability checks may spend per day (the agent reads
    /// how the code uses a vulnerable package). Past it, the rest wait for
    /// the next day's check.
    pub triage_tokens_per_day: u64,
    /// Fix runs (coding runs that open a pull request) started per day, at
    /// most; the rest wait, the most urgent first. Adopting a bot's pull
    /// request or opening an issue doesn't count.
    pub max_fix_runs_per_day: u32,
    /// Model tokens fix runs may spend per day; past it, no new run starts.
    pub max_fix_tokens_per_day: u64,
    /// Cards posted per day, at most; past it, one digest card says how
    /// many more are waiting.
    pub max_cards_per_day: u32,
}

pub const MAX_FIX_RUNS_PER_DAY: u32 = 3;
pub const MAX_FIX_RUNS_LIMIT: u32 = 20;
pub const MAX_FIX_TOKENS_PER_DAY: u64 = 2_000_000;
pub const MAX_FIX_TOKENS_LIMIT: u64 = 50_000_000;
pub const MAX_CARDS_PER_DAY: u32 = 10;
pub const MAX_CARDS_LIMIT: u32 = 100;

/// The default daily limit for reachability checks.
pub const TRIAGE_TOKENS_PER_DAY: u64 = 400_000;
/// The most a team may set it to.
pub const MAX_TRIAGE_TOKENS_PER_DAY: u64 = 5_000_000;

impl Default for SecurityConfig {
    fn default() -> Self {
        // Off until a team turns it on. Turned on, it opens fix pull
        // requests and posts cards per the policy (the recommended choice);
        // a team that wants to look first picks "Preview first".
        SecurityConfig {
            enabled: false,
            dry_run: false,
            open_prs: TierMap {
                p0: Cadence::Now,
                p1: Cadence::Hour,
                p2: Cadence::Daily,
                p3: Cadence::Weekly,
            },
            post_alerts: TierMap {
                p0: true,
                p1: true,
                p2: false,
                p3: false,
            },
            escalate_half: true,
            due_days: TierMap {
                p0: 3,
                p1: 7,
                p2: 30,
                p3: 90,
            },
            use_kev_due: true,
            labels: vec!["security".into(), "dependencies".into()],
            severity_labels: true,
            reachability_labels: true,
            ecosystem_labels: true,
            major_bumps: MajorBumps::FixInPr,
            adopt_bot_prs: true,
            repos: Repos::All,
            ignore: vec![],
            triage_tokens_per_day: TRIAGE_TOKENS_PER_DAY,
            max_fix_runs_per_day: MAX_FIX_RUNS_PER_DAY,
            max_fix_tokens_per_day: MAX_FIX_TOKENS_PER_DAY,
            max_cards_per_day: MAX_CARDS_PER_DAY,
        }
    }
}

pub const MAX_LABELS: usize = 20;
pub const MAX_LABEL_CHARS: usize = 50;
const MAX_IGNORES: usize = 200;

impl SecurityConfig {
    /// The checks a saved config must pass: due days 1..365, at most 20
    /// labels of 1..50 characters, repositories as owner/name, each ignore
    /// with an advisory, a reason and an end date.
    pub fn validate(&self) -> Result<(), String> {
        for t in Tier::ALL {
            let d = self.due_days.get(t);
            if !(1..=365).contains(&d) {
                return Err(format!("due_days.{} must be 1 to 365", t.as_str()));
            }
        }
        if self.labels.len() > MAX_LABELS {
            return Err(format!("at most {MAX_LABELS} labels"));
        }
        for l in &self.labels {
            let n = l.trim().chars().count();
            if n == 0 || n > MAX_LABEL_CHARS || l.contains(',') || l.chars().any(char::is_control) {
                return Err(format!(
                    "each label is 1 to {MAX_LABEL_CHARS} characters, without commas"
                ));
            }
        }
        if let Repos::List(l) = &self.repos {
            if l.len() > 500 || l.iter().any(|r| !valid_repo(r)) {
                return Err("repos is \"all\" or a list of owner/name".into());
            }
        }
        if self.triage_tokens_per_day > MAX_TRIAGE_TOKENS_PER_DAY {
            return Err(format!(
                "triage_tokens_per_day is 0 to {MAX_TRIAGE_TOKENS_PER_DAY}"
            ));
        }
        if self.max_fix_runs_per_day > MAX_FIX_RUNS_LIMIT {
            return Err(format!("max_fix_runs_per_day is 0 to {MAX_FIX_RUNS_LIMIT}"));
        }
        if self.max_fix_tokens_per_day > MAX_FIX_TOKENS_LIMIT {
            return Err(format!(
                "max_fix_tokens_per_day is 0 to {MAX_FIX_TOKENS_LIMIT}"
            ));
        }
        if self.max_cards_per_day > MAX_CARDS_LIMIT {
            return Err(format!("max_cards_per_day is 0 to {MAX_CARDS_LIMIT}"));
        }
        if self.ignore.len() > MAX_IGNORES {
            return Err(format!("at most {MAX_IGNORES} ignores"));
        }
        for i in &self.ignore {
            if i.advisory.trim().is_empty() || i.reason.trim().is_empty() {
                return Err("each ignore needs an advisory and a reason".into());
            }
            if NaiveDate::parse_from_str(i.until.get(..10).unwrap_or(""), "%Y-%m-%d").is_err() {
                return Err("each ignore needs an end date (YYYY-MM-DD)".into());
            }
            if let Some(r) = &i.repo {
                if !valid_repo(r) {
                    return Err("an ignore's repo is owner/name".into());
                }
            }
        }
        Ok(())
    }

    /// Labels trimmed and without duplicates (after [`Self::validate`]).
    pub fn cleaned(mut self) -> Self {
        let mut out: Vec<String> = vec![];
        for l in &self.labels {
            let l = l.trim().to_string();
            if !out.iter().any(|o| o.eq_ignore_ascii_case(&l)) {
                out.push(l);
            }
        }
        self.labels = out;
        for i in &mut self.ignore {
            i.advisory = i.advisory.trim().to_string();
            i.reason = crate::truncate_str(i.reason.trim(), 500).to_string();
        }
        self
    }

    pub fn from_json(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }

    /// Fix runs and cards happen: on, and not a dry run.
    pub fn acts(&self) -> bool {
        self.enabled && !self.dry_run
    }

    /// The ignore that covers this finding now, if any.
    pub fn ignored(&self, f: &Finding, now: DateTime<Utc>) -> Option<&Ignore> {
        let today = now.date_naive();
        self.ignore.iter().find(|i| {
            let live = NaiveDate::parse_from_str(i.until.get(..10).unwrap_or(""), "%Y-%m-%d")
                .is_ok_and(|d| d >= today);
            let repo = i
                .repo
                .as_deref()
                .is_none_or(|r| r.eq_ignore_ascii_case(&f.repo));
            live && repo
                && f.advisory
                    .ids
                    .iter()
                    .any(|id| id.eq_ignore_ascii_case(i.advisory.trim()))
        })
    }
}

pub fn valid_repo(r: &str) -> bool {
    let Some((o, n)) = r.split_once('/') else {
        return false;
    };
    let ok = |s: &str| {
        !s.is_empty()
            && s.len() <= 100
            && s != "."
            && s != ".."
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    ok(o) && ok(n)
}

// ── Versions ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Num(u64),
    Str(String),
}

fn version_tokens(v: &str) -> Vec<Tok> {
    let v = v.trim();
    let v = v.strip_prefix(['v', 'V']).unwrap_or(v);
    let v = v.split('+').next().unwrap_or("");
    let mut out = vec![];
    let mut cur = String::new();
    let mut num: Option<bool> = None;
    let flush = |cur: &mut String, num: Option<bool>, out: &mut Vec<Tok>| {
        if cur.is_empty() {
            return;
        }
        if num == Some(true) {
            out.push(Tok::Num(cur.parse().unwrap_or(u64::MAX)));
        } else {
            out.push(Tok::Str(cur.to_ascii_lowercase()));
        }
        cur.clear();
    };
    for c in v.chars() {
        if c.is_ascii_digit() {
            if num == Some(false) {
                flush(&mut cur, num, &mut out);
            }
            num = Some(true);
            cur.push(c);
        } else if c.is_ascii_alphabetic() {
            if num == Some(true) {
                flush(&mut cur, num, &mut out);
            }
            num = Some(false);
            cur.push(c);
        } else {
            flush(&mut cur, num, &mut out);
            num = None;
        }
    }
    flush(&mut cur, num, &mut out);
    out
}

/// A word that marks a version as before its release (`1.0.0-rc1 < 1.0.0`).
fn pre_release(s: &str) -> bool {
    matches!(
        s,
        "a" | "alpha"
            | "b"
            | "beta"
            | "c"
            | "rc"
            | "cr"
            | "pre"
            | "preview"
            | "dev"
            | "snapshot"
            | "m"
            | "milestone"
            | "ea"
    )
}

/// Compares two versions of any common scheme (SemVer, PyPI, Maven-like):
/// numbers numerically, a pre-release word below the release it precedes.
pub fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let (x, y) = (version_tokens(a), version_tokens(b));
    let n = x.len().max(y.len());
    for i in 0..n {
        match (x.get(i), y.get(i)) {
            (Some(Tok::Num(p)), Some(Tok::Num(q))) if p != q => return p.cmp(q),
            (Some(Tok::Str(p)), Some(Tok::Str(q))) if p != q => {
                let rank = |s: &str| match s {
                    "dev" | "snapshot" => 0,
                    "a" | "alpha" | "ea" => 1,
                    "b" | "beta" => 2,
                    "m" | "milestone" => 3,
                    "c" | "rc" | "cr" | "pre" | "preview" => 4,
                    _ => 5,
                };
                return rank(p).cmp(&rank(q)).then_with(|| p.cmp(q));
            }
            (Some(Tok::Num(_)), Some(Tok::Str(_))) => return Greater,
            (Some(Tok::Str(_)), Some(Tok::Num(_))) => return Less,
            (Some(Tok::Num(p)), None) => {
                return if *p == 0 && x[i..].iter().all(|t| *t == Tok::Num(0)) {
                    Equal
                } else {
                    Greater
                }
            }
            (None, Some(Tok::Num(q))) => {
                return if *q == 0 && y[i..].iter().all(|t| *t == Tok::Num(0)) {
                    Equal
                } else {
                    Less
                }
            }
            (Some(Tok::Str(s)), None) => return if pre_release(s) { Less } else { Greater },
            (None, Some(Tok::Str(s))) => return if pre_release(s) { Greater } else { Less },
            _ => {}
        }
    }
    Equal
}

/// Whether `version` falls in a GitHub advisory range: comma-separated
/// constraints (`>= 1.0.0, < 1.2.3`, `= 2.0`, `<= 0.9`). An empty or
/// unreadable range matches nothing.
pub fn in_range(version: &str, range: &str) -> bool {
    use std::cmp::Ordering::*;
    let mut any = false;
    for part in range.split(',') {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        let (op, v) = ["<=", ">=", "==", "!=", "<", ">", "="]
            .iter()
            .find_map(|op| p.strip_prefix(op).map(|v| (*op, v.trim())))
            .unwrap_or(("=", p));
        if v.is_empty() {
            return false;
        }
        let c = compare_versions(version, v);
        let ok = match op {
            "<" => c == Less,
            "<=" => c != Greater,
            ">" => c == Greater,
            ">=" => c != Less,
            "!=" => c != Equal,
            _ => c == Equal,
        };
        if !ok {
            return false;
        }
        any = true;
    }
    any
}

/// The major version (the first number, or the second for 0.x, which
/// SemVer treats as breaking).
fn major_of(v: &str) -> (u64, u64) {
    let nums: Vec<u64> = version_tokens(v)
        .into_iter()
        .filter_map(|t| match t {
            Tok::Num(n) => Some(n),
            Tok::Str(_) => None,
        })
        .collect();
    match nums.as_slice() {
        [0, m, ..] => (0, *m),
        [a, ..] => (*a, 0),
        [] => (0, 0),
    }
}

/// Moving from `a` to `b` crosses a major version.
pub fn is_major_bump(a: &str, b: &str) -> bool {
    !a.is_empty() && !b.is_empty() && major_of(a) != major_of(b)
}

/// The version to move to: the lowest fixed version above the current one
/// for each advisory, and the highest of those, so one bump clears them all.
pub fn pick_fixed(current: &str, fixed_per_advisory: &[Vec<String>]) -> Option<String> {
    let mut best: Option<String> = None;
    for fixed in fixed_per_advisory {
        let lowest = fixed
            .iter()
            .filter(|f| compare_versions(f, current) == std::cmp::Ordering::Greater)
            .min_by(|a, b| compare_versions(a, b))?;
        if best
            .as_deref()
            .is_none_or(|b| compare_versions(lowest, b) == std::cmp::Ordering::Greater)
        {
            best = Some(lowest.clone());
        }
    }
    best
}

// ── Alias clusters ──────────────────────────────────────────────────────

fn norm_id(id: &str) -> String {
    id.trim().to_ascii_uppercase()
}

/// Groups advisory ids that are aliases of each other (union-find over
/// each record's id and aliases; aliases are symmetric and transitive).
/// Each cluster is sorted; clusters come out sorted by their first id.
pub fn clusters(records: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut idx: HashMap<String, usize> = HashMap::new();
    let mut parent: Vec<usize> = vec![];
    let mut names: Vec<String> = vec![];
    fn root(p: &mut [usize], mut i: usize) -> usize {
        while p[i] != i {
            p[i] = p[p[i]];
            i = p[i];
        }
        i
    }
    for rec in records {
        let mut first: Option<usize> = None;
        for id in rec.iter().map(|s| norm_id(s)).filter(|s| !s.is_empty()) {
            let i = *idx.entry(id.clone()).or_insert_with(|| {
                parent.push(parent.len());
                names.push(id.clone());
                parent.len() - 1
            });
            match first {
                None => first = Some(i),
                Some(f) => {
                    let (a, b) = (root(&mut parent, f), root(&mut parent, i));
                    if a != b {
                        parent[b] = a;
                    }
                }
            }
        }
    }
    let mut groups: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
    for (i, name) in names.iter().enumerate() {
        let r = root(&mut parent, i);
        groups.entry(r).or_default().insert(name.clone());
    }
    let mut out: Vec<Vec<String>> = groups
        .into_values()
        .map(|s| s.into_iter().collect())
        .collect();
    out.sort();
    out
}

/// The id a cluster is shown and keyed by: its CVE, else its GHSA, else its
/// first id.
pub fn primary_id(ids: &[String]) -> String {
    let ids: Vec<String> = ids.iter().map(|s| norm_id(s)).collect();
    let pick = |p: &str| ids.iter().filter(|i| i.starts_with(p)).min().cloned();
    pick("CVE-")
        .or_else(|| pick("GHSA-"))
        .or_else(|| ids.iter().min().cloned())
        .unwrap_or_default()
}

/// Shown ids keep GitHub's lowercase GHSA body (`GHSA-x4q2-...`).
fn display_id(id: &str) -> String {
    let u = norm_id(id);
    match u.strip_prefix("GHSA-") {
        Some(rest) => format!("GHSA-{}", rest.to_ascii_lowercase()),
        None => u,
    }
}

/// A finding's stable id: from its cluster, repository, manifest and package.
pub fn finding_id(cluster_ids: &[String], repo: &str, manifest: &str, package: &str) -> String {
    let key = primary_id(cluster_ids);
    crate::sha256_hex(&[
        &key,
        &repo.to_ascii_lowercase(),
        manifest,
        &package.to_ascii_lowercase(),
    ])[..24]
        .to_string()
}

/// A manifest of "sbom" (the dependency graph, which doesn't say which
/// lockfile) matches any manifest of the same repository.
fn same_manifest(a: &str, b: &str) -> bool {
    a == b || a == SBOM_MANIFEST || b == SBOM_MANIFEST
}

/// The existing finding a new observation belongs to: same repository,
/// package and manifest, and an advisory id in common.
pub fn merge_target<'a>(
    existing: &'a [Finding],
    ids: &[String],
    repo: &str,
    manifest: &str,
    package: &str,
) -> Option<&'a Finding> {
    existing
        .iter()
        .find(|f| merges_into(f, ids, repo, manifest, package))
}

/// Whether an observation of `ids` for this repository, manifest and
/// package belongs to finding `f` (see [`merge_target`]).
pub fn merges_into(f: &Finding, ids: &[String], repo: &str, manifest: &str, package: &str) -> bool {
    f.repo.eq_ignore_ascii_case(repo)
        && f.package.eq_ignore_ascii_case(package)
        && same_manifest(&f.manifest, manifest)
        && f.advisory
            .ids
            .iter()
            .any(|i| ids.iter().any(|o| o.trim().eq_ignore_ascii_case(i.trim())))
}

// ── What a source saw ──────────────────────────────────────────────────

/// One advisory as a source describes it, for one package.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AdvisoryRecord {
    /// Its id and every alias.
    pub ids: Vec<String>,
    pub title: String,
    pub summary: String,
    pub cvss: Option<f64>,
    pub cvss_vector: Option<String>,
    /// GHSA's word: critical, high, moderate/medium, low.
    pub severity: Option<String>,
    pub epss: Option<f64>,
    pub epss_percentile: Option<f64>,
    pub malware: bool,
    pub cwe: Vec<String>,
    pub published_at: Option<String>,
    pub urls: Vec<String>,
    pub source: String,
    pub withdrawn: bool,
    /// Vulnerable ranges for the package (GHSA form).
    pub ranges: Vec<String>,
    pub fixed: Vec<String>,
    pub symbols: Vec<String>,
}

/// One package in one repository, matched to one advisory.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Observation {
    pub repo: String,
    pub manifest: String,
    pub ecosystem: String,
    pub package: String,
    pub version: String,
    pub advisory: AdvisoryRecord,
    pub scope: Scope,
    pub relation: Relation,
    pub path: Vec<String>,
    pub alert: Option<AlertRef>,
    /// The Dependabot alert's own state (`open`, `fixed`, `dismissed`, ...).
    pub alert_state: Option<String>,
    pub alert_dismissed: Option<Dismissed>,
    /// A repository advisory not yet published (see `Finding::private_report`).
    pub private_report: bool,
}

/// Applies an observation: a new finding, or the existing one updated with
/// the union of what the sources say. Status moves only for what the source
/// itself decided (a fixed or dismissed Dependabot alert).
pub fn apply(
    existing: Option<(Finding, FindingState)>,
    o: &Observation,
    now: DateTime<Utc>,
) -> (Finding, FindingState, bool) {
    let now_s = now.to_rfc3339();
    let created = existing.is_none();
    let (mut f, mut st) = existing.unwrap_or_else(|| {
        let ids = &o.advisory.ids;
        (
            Finding {
                id: finding_id(ids, &o.repo, &o.manifest, &o.package),
                repo: o.repo.clone(),
                manifest: o.manifest.clone(),
                ecosystem: o.ecosystem.clone(),
                package: o.package.clone(),
                version: o.version.clone(),
                first_seen_at: now_s.clone(),
                ..Default::default()
            },
            FindingState::default(),
        )
    });
    // A real manifest replaces the dependency graph's placeholder.
    if f.manifest == SBOM_MANIFEST && o.manifest != SBOM_MANIFEST {
        f.manifest = o.manifest.clone();
    }
    if !o.version.is_empty() {
        f.version = o.version.clone();
    }
    if f.ecosystem.is_empty() {
        f.ecosystem = o.ecosystem.clone();
    }
    let a = &o.advisory;
    let mut ids: BTreeSet<String> = f.advisory.ids.iter().map(|s| display_id(s)).collect();
    ids.extend(a.ids.iter().map(|s| display_id(s)));
    f.advisory.ids = ids.into_iter().collect();
    f.advisory.primary = display_id(&primary_id(&f.advisory.ids));
    // Reviewed GitHub data first for the words; the higher score wins.
    let prefer = f.advisory.title.is_empty() || a.source != "osv";
    if prefer && !a.title.is_empty() {
        f.advisory.title = crate::truncate_str(&a.title, 300).to_string();
    }
    if prefer && !a.summary.is_empty() {
        f.advisory.summary = crate::truncate_str(&a.summary, 2000).to_string();
    }
    if f.advisory.source.is_empty() || (a.source == "dependabot") {
        f.advisory.source = a.source.clone();
    }
    if let Some(c) = a.cvss {
        if f.advisory.cvss.is_none_or(|old| c > old) {
            f.advisory.cvss = Some(c);
            f.advisory.cvss_vector = a.cvss_vector.clone().or(f.advisory.cvss_vector.take());
        }
    } else if f.advisory.cvss_vector.is_none() {
        // OSV gives the vector without a score: keep it.
        f.advisory.cvss_vector = a.cvss_vector.clone();
    }
    if a.severity.is_some() && st.severity.is_none() {
        st.severity = a.severity.clone();
    }
    if a.epss.is_some() {
        f.advisory.epss = a.epss;
        f.advisory.epss_percentile = a.epss_percentile;
    }
    f.advisory.malware |= a.malware;
    union(&mut f.advisory.cwe, &a.cwe);
    union(&mut f.advisory.urls, &a.urls);
    f.advisory.urls.truncate(20);
    if f.advisory.published_at.is_none() {
        f.advisory.published_at = a.published_at.clone();
    }
    union(&mut st.ranges, &a.ranges);
    union(&mut st.fixed, &a.fixed);
    union(&mut st.symbols, &a.symbols);
    st.symbols.truncate(30);
    if a.withdrawn {
        union(
            &mut st.withdrawn,
            &a.ids.iter().map(|s| display_id(s)).collect::<Vec<_>>(),
        );
    }
    if o.scope != Scope::Unknown {
        f.scope = o.scope;
    }
    if o.relation != Relation::Unknown {
        f.relation = o.relation;
    }
    if !o.path.is_empty() {
        f.path = o.path.clone();
    }
    if o.alert.is_some() {
        f.dependabot_alert = o.alert.clone();
    }
    if a.source == REPOSITORY_ADVISORY {
        f.private_report = o.private_report;
    }
    f.fixed_version = pick_fixed(&f.version, &[st.fixed.clone()]).or(f.fixed_version.take());
    match o.alert_state.as_deref() {
        Some("fixed") if f.status != Status::Dismissed => f.status = Status::Fixed,
        Some("dismissed" | "auto_dismissed") if !f.status.is_handled() => {
            f.status = Status::Dismissed;
            f.dismissed = o.alert_dismissed.clone().or(Some(Dismissed {
                by: "GitHub".into(),
                reason: "Dismissed on GitHub".into(),
                until: None,
                at: now_s.clone(),
            }));
        }
        Some("open" | "reintroduced" | "auto_reopened" | "reopened")
            if matches!(f.status, Status::Fixed | Status::Dismissed) =>
        {
            f.status = Status::Open;
            f.dismissed = None;
        }
        _ => {}
    }
    // Every id withdrawn: the finding is withdrawn.
    if !f.advisory.ids.is_empty()
        && f.advisory
            .ids
            .iter()
            .all(|i| st.withdrawn.iter().any(|w| w.eq_ignore_ascii_case(i)))
        && !matches!(f.status, Status::Fixed)
    {
        f.status = Status::Withdrawn;
    }
    f.updated_at = now_s;
    (f, st, created)
}

fn union(into: &mut Vec<String>, more: &[String]) {
    for m in more {
        let m = m.trim();
        if !m.is_empty() && !into.iter().any(|x| x.eq_ignore_ascii_case(m)) {
            into.push(m.to_string());
        }
    }
}

// ── Policy ──────────────────────────────────────────────────────────────

/// The CVSS score a finding is judged by: its own, else its severity word.
pub fn score(f: &Finding, st: &FindingState) -> f64 {
    if let Some(c) = f.advisory.cvss {
        return c;
    }
    match st
        .severity
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("critical") => 9.0,
        Some("high") => 7.0,
        Some("moderate" | "medium") => 5.0,
        Some("low") => 2.0,
        _ => 0.0,
    }
}

/// `critical`, `high`, `medium` or `low`.
pub fn severity_word(score: f64) -> &'static str {
    if score >= 9.0 {
        "critical"
    } else if score >= 7.0 {
        "high"
    } else if score >= 4.0 {
        "medium"
    } else {
        "low"
    }
}

/// The tier, from the verdict, scope, score, the exploited list and EPSS.
/// P0 needs evidence: malware; on CISA's exploited list and loaded at
/// runtime (reachable, or a runtime dependency not yet read); or CVSS 9+ and
/// reachable. A verdict not yet known ("unknown", or "pending" while it is
/// checked) is never P0 on its score alone: at most P1, when the score is
/// high and the dependency is a runtime one (or EPSS is 0.1+), otherwise P2
/// or P3. A pending verdict's tier is provisional ([`Finding::tier_provisional`]).
pub fn tier_of(f: &Finding, st: &FindingState) -> Tier {
    let a = &f.advisory;
    if a.malware {
        return Tier::P0;
    }
    if f.verdict == Verdict::NotLoaded || f.scope == Scope::Dev {
        return Tier::P3;
    }
    let s = score(f, st);
    let runtime = f.scope == Scope::Runtime;
    let likely = a.epss.is_some_and(|e| e >= 0.1);
    match f.verdict {
        Verdict::Reachable => {
            if a.kev || s >= 9.0 {
                Tier::P0
            } else if s >= 7.0 || likely {
                Tier::P1
            } else if s >= 4.0 {
                Tier::P2
            } else {
                Tier::P3
            }
        }
        Verdict::NotReachable => {
            if a.kev || likely || s >= 9.0 {
                Tier::P2
            } else {
                Tier::P3
            }
        }
        // Unknown or still being checked: no evidence either way yet.
        _ => {
            if a.kev && runtime {
                Tier::P0
            } else if a.kev || likely || (s >= 7.0 && runtime) {
                Tier::P1
            } else if s >= 4.0 {
                Tier::P2
            } else {
                Tier::P3
            }
        }
    }
}

/// When a finding is due: its first sighting plus the tier's days, or the
/// exploited list's date when that is sooner (and the team uses it).
pub fn due_at(f: &Finding, tier: Tier, cfg: &SecurityConfig) -> DateTime<Utc> {
    let seen = parse_time(&f.first_seen_at).unwrap_or_else(Utc::now);
    let mut due = seen + Duration::days(cfg.due_days.get(tier) as i64);
    if cfg.use_kev_due {
        if let Some(kev) = f
            .advisory
            .kev_due
            .as_deref()
            .and_then(|d| NaiveDate::parse_from_str(d.get(..10)?, "%Y-%m-%d").ok())
            .and_then(|d| d.and_hms_opt(23, 59, 59))
        {
            let kev = kev.and_utc();
            if kev < due {
                due = kev;
            }
        }
    }
    due
}

/// Applies the policy to a finding: its tier and due date. Returns whether
/// the tier rose.
pub fn retier(f: &mut Finding, st: &FindingState, cfg: &SecurityConfig) -> bool {
    let t = tier_of(f, st);
    f.tier_provisional = f.verdict == Verdict::Pending;
    let rose = f.tier.is_some_and(|old| t < old);
    f.tier = Some(t);
    f.due_at = due_at(f, t, cfg).to_rfc3339();
    rose
}

pub fn parse_time(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

// ── Cards ───────────────────────────────────────────────────────────────

/// What a card says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    New,
    PrOpen,
    CiPassing,
    Merged,
    Half,
    Deadline,
    Withdrawn,
}

impl Stage {
    pub fn key(self) -> &'static str {
        match self {
            Stage::New => "new",
            Stage::PrOpen => "pr_open",
            Stage::CiPassing => "ci_passing",
            Stage::Merged => "merged",
            Stage::Half => "half",
            Stage::Deadline => "deadline",
            Stage::Withdrawn => "withdrawn",
        }
    }
}

/// Whether a finding's tier posts cards at all.
pub fn posts(f: &Finding, cfg: &SecurityConfig) -> bool {
    !f.private_report && f.tier.is_some_and(|t| cfg.post_alerts.get(t))
}

/// The escalation a finding is due now, if any: at the deadline, or at half
/// of it (when the team asked for that). Only for an open finding whose new
/// card posted, and never one already posted.
pub fn escalation(
    f: &Finding,
    st: &FindingState,
    cfg: &SecurityConfig,
    now: DateTime<Utc>,
) -> Option<Stage> {
    if !f.status.is_open() || !posts(f, cfg) || !st.cards.iter().any(|c| c == "new") {
        return None;
    }
    let (seen, due) = (parse_time(&f.first_seen_at)?, parse_time(&f.due_at)?);
    if now >= due && !st.cards.iter().any(|c| c == "deadline") {
        return Some(Stage::Deadline);
    }
    let half = seen + (due - seen) / 2;
    if cfg.escalate_half && now >= half && now < due && !st.cards.iter().any(|c| c == "half") {
        return Some(Stage::Half);
    }
    None
}

fn hours_left(due: &str, now: DateTime<Utc>) -> String {
    match parse_time(due) {
        Some(d) if d > now => {
            let h = (d - now).num_hours();
            if h >= 48 {
                format!("in {} days", h / 24)
            } else {
                format!("in {h} h")
            }
        }
        Some(_) => "overdue".to_string(),
        None => "unknown".to_string(),
    }
}

/// The card for one finding at one stage.
pub fn card(f: &Finding, stage: Stage, finding_url: &str, now: DateTime<Utc>) -> Message {
    let tier = f.tier.map(Tier::as_str).unwrap_or("P3");
    let repo_name = f.repo.split('/').nth(1).unwrap_or(&f.repo);
    let id = &f.advisory.primary;
    let (tone, head) = match stage {
        Stage::New => (
            Tone::Attention,
            format!("🔴 {tier} security · {repo_name} is affected by {id}"),
        ),
        Stage::PrOpen => (
            Tone::Accent,
            format!("🛠️ {tier} security · fix opened for {id}"),
        ),
        Stage::CiPassing => (
            Tone::Accent,
            format!("🛠️ {tier} security · fix for {id} passes CI"),
        ),
        Stage::Merged => (
            Tone::Good,
            format!("✅ {tier} security · fix for {id} merged"),
        ),
        Stage::Half => (
            Tone::Warning,
            format!("🟠 {tier} security · half the deadline gone for {id}"),
        ),
        Stage::Deadline => (
            Tone::Attention,
            format!("🔴 {tier} security · {id} is past its deadline"),
        ),
        Stage::Withdrawn => (Tone::Good, format!("✅ {id} was withdrawn")),
    };
    let mut line = if f.advisory.title.is_empty() {
        format!("{} in {}.", id, f.package)
    } else {
        format!(
            "{} in {}.",
            f.advisory.title.trim_end_matches('.'),
            f.package
        )
    };
    match f.verdict {
        Verdict::Reachable => line.push_str(&format!(" {repo_name} calls the vulnerable code.")),
        Verdict::Unknown | Verdict::Pending => line.push_str(" Reachability not settled yet."),
        Verdict::NotReachable => line.push_str(" Present, but nothing calls the vulnerable code."),
        Verdict::NotLoaded => line.push_str(" Not loaded at runtime."),
    }
    if f.advisory.kev {
        line.push_str(" It is on CISA's list of exploited vulnerabilities.");
    }
    let fix = match (&f.pr, f.status) {
        (Some(p), _) if p.state == "merged" => format!("PR #{} merged", p.number),
        (Some(p), _) => {
            let ci = match p.ci.as_deref() {
                Some("passing") => ", CI passing",
                Some("failing") => ", CI failing (CoderHelm is fixing it)",
                _ => "",
            };
            format!("PR #{}{ci}", p.number)
        }
        (None, Status::NoFix) => "No fixed version yet".to_string(),
        (None, Status::Fixing) => "A fix is being prepared".to_string(),
        _ => "Not opened yet".to_string(),
    };
    let pkg = match &f.fixed_version {
        Some(to) => format!("{} {} to {to}", f.package, f.version),
        None => format!("{} {}", f.package, f.version),
    };
    let due = match parse_time(&f.due_at) {
        Some(d) => format!(
            "{} ({})",
            d.format("%b %-d, %H:%M UTC"),
            hours_left(&f.due_at, now)
        ),
        None => "unknown".to_string(),
    };
    let mut buttons = vec![];
    if let Some(p) = &f.pr {
        if p.url.starts_with("https://") {
            buttons.push(Button::new("Open the PR", &p.url));
        }
    }
    buttons.push(Button::new("See why", finding_url));
    Message {
        summary: head.clone(),
        blocks: vec![
            Block::Banner {
                text: head,
                tone,
                size: Size::Medium,
            },
            Block::Text(Text::new(line).spacing(Spacing::Small)),
            Block::Facts {
                facts: vec![
                    ("Repository".into(), f.repo.clone()),
                    ("Fix".into(), fix),
                    ("Due".into(), due),
                    ("Package".into(), pkg),
                ],
                spacing: Spacing::Small,
            },
        ],
        buttons,
        full_width: true,
    }
}

/// The one card that stands for the cards past the day's limit.
pub fn digest_card(waiting: &[&Finding], security_url: &str, limit: u32) -> Message {
    let mut tiers: BTreeMap<Tier, usize> = BTreeMap::new();
    for f in waiting {
        *tiers.entry(f.tier.unwrap_or(Tier::P3)).or_default() += 1;
    }
    let by = tiers
        .iter()
        .map(|(t, n)| format!("{n} {}", t.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    let head = format!(
        "🟠 Security · {} more finding{} today",
        waiting.len(),
        if waiting.len() == 1 { "" } else { "s" }
    );
    let mut lines: Vec<String> = waiting
        .iter()
        .take(5)
        .map(|f| {
            format!(
                "{} {} {} in {} ({})",
                f.tier.map(Tier::as_str).unwrap_or("P3"),
                f.package,
                f.version,
                f.repo.split('/').nth(1).unwrap_or(&f.repo),
                f.advisory.primary
            )
        })
        .collect();
    if waiting.len() > 5 {
        lines.push(format!("and {} more", waiting.len() - 5));
    }
    Message {
        summary: head.clone(),
        blocks: vec![
            Block::Banner {
                text: head,
                tone: Tone::Warning,
                size: Size::Medium,
            },
            Block::Text(
                Text::new(format!(
                    "Today's {limit} cards are posted, so these are in one card: {by}.\n{}",
                    lines.join("\n")
                ))
                .spacing(Spacing::Small),
            ),
        ],
        buttons: vec![Button::new("See them all", security_url)],
        full_width: true,
    }
}

/// The note a finding carries while a daily fix limit holds it.
pub fn waiting_runs(limit: u32) -> String {
    format!(
        "Waiting: today's {limit} fix run{} {} used. Next starts tomorrow.",
        if limit == 1 { "" } else { "s" },
        if limit == 1 { "is" } else { "are" }
    )
}

pub fn waiting_tokens(limit: u64) -> String {
    format!(
        "Waiting: today's fix runs used their {} tokens. Next starts tomorrow.",
        fmt_tokens(limit)
    )
}

pub const WAITING_REPO: &str =
    "Waiting: a fix run for this repo is in progress. Next starts when it ends.";
pub const WAITING_TRIAGE: &str =
    "Waiting for tomorrow's check: today's reachability limit is used.";

/// `400k`, `2M`.
pub fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 && n.is_multiple_of(1_000_000) {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1000 {
        format!("{}k", n / 1000)
    } else {
        n.to_string()
    }
}

/// Fix units in the order they start: tier, then due date. P0 always first.
pub fn order_units(units: &mut [FixUnit], findings: &[(Finding, FindingState)]) {
    let key = |u: &FixUnit| {
        let members: Vec<&Finding> = u
            .finding_ids
            .iter()
            .filter_map(|id| findings.iter().find(|(f, _)| &f.id == id).map(|(f, _)| f))
            .collect();
        let tier = members
            .iter()
            .filter_map(|f| f.tier)
            .min()
            .unwrap_or(Tier::P3);
        let due = members
            .iter()
            .filter_map(|f| parse_time(&f.due_at))
            .min()
            .unwrap_or_else(|| Utc::now() + Duration::days(3650));
        (tier, due)
    };
    units.sort_by_key(|u| key(u));
}

// ── Daily limits ────────────────────────────────────────────────────────

/// How a due fix unit is handled: a full coding run, or work that costs no
/// run (adopting a bot's pull request, opening an issue).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunKind {
    Run,
    Adopt,
    IssueOnly,
}

impl RunKind {
    /// Only a coding run counts against the day's limits.
    pub fn counts(self) -> bool {
        self == RunKind::Run
    }
}

/// Whether a run may start now, or why it waits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admit {
    Start,
    Wait(String),
}

/// The day's counters, shared by every tick: taking a run slot is atomic
/// (DynamoDB's conditional ADD on `SECRUNS#<yyyymmdd>`), so ticks running
/// at once never start more than the limit.
pub trait DailySlots {
    /// Takes one of today's `limit` run slots; false when none is left.
    fn take_run(&self, day: &str, limit: u32) -> impl std::future::Future<Output = bool> + Send;
    /// Gives a slot back (the run could not be queued).
    fn give_back(&self, day: &str) -> impl std::future::Future<Output = ()> + Send;
    /// Tokens fix runs spent today.
    fn fix_tokens(&self, day: &str) -> impl std::future::Future<Output = u64> + Send;
    /// Takes a slot past the limit: an admin raised today's limit for one
    /// request (the caller records it in the audit log).
    fn take_run_over_limit(&self, day: &str) -> impl std::future::Future<Output = bool> + Send;
}

/// The day key of the limits (UTC).
pub fn limit_day(now: DateTime<Utc>) -> String {
    now.format("%Y%m%d").to_string()
}

/// Decides one coding run against the day's limits: one run in progress per
/// repository, the token limit, then a run slot.
pub async fn admit_run<S: DailySlots>(
    slots: &S,
    cfg: &SecurityConfig,
    day: &str,
    repo_busy: bool,
) -> Admit {
    if repo_busy {
        return Admit::Wait(WAITING_REPO.into());
    }
    if cfg.max_fix_runs_per_day == 0 {
        return Admit::Wait("Waiting: fix runs are set to 0 a day in the policy.".into());
    }
    if slots.fix_tokens(day).await >= cfg.max_fix_tokens_per_day {
        return Admit::Wait(waiting_tokens(cfg.max_fix_tokens_per_day));
    }
    if !slots.take_run(day, cfg.max_fix_runs_per_day).await {
        return Admit::Wait(waiting_runs(cfg.max_fix_runs_per_day));
    }
    Admit::Start
}

/// In-memory [`DailySlots`] with the same conditional semantics, for tests
/// and previews.
#[derive(Default)]
pub struct MemorySlots {
    pub runs: std::sync::Mutex<HashMap<String, u32>>,
    pub tokens: std::sync::Mutex<HashMap<String, u64>>,
}

impl DailySlots for MemorySlots {
    async fn take_run(&self, day: &str, limit: u32) -> bool {
        // Yield first, so tasks running at once interleave here.
        tokio::task::yield_now().await;
        let mut g = self.runs.lock().unwrap();
        let n = g.entry(day.to_string()).or_insert(0);
        if *n < limit {
            *n += 1;
            true
        } else {
            false
        }
    }
    async fn give_back(&self, day: &str) {
        let mut g = self.runs.lock().unwrap();
        if let Some(n) = g.get_mut(day) {
            *n = n.saturating_sub(1);
        }
    }
    async fn fix_tokens(&self, day: &str) -> u64 {
        *self.tokens.lock().unwrap().get(day).unwrap_or(&0)
    }
    async fn take_run_over_limit(&self, day: &str) -> bool {
        *self
            .runs
            .lock()
            .unwrap()
            .entry(day.to_string())
            .or_insert(0) += 1;
        true
    }
}

/// Which findings' cards post today and which go into the digest, given
/// how many posted already: the most urgent first.
pub fn card_budget(
    due: Vec<&Finding>,
    posted_today: u32,
    limit: u32,
) -> (Vec<&Finding>, Vec<&Finding>) {
    let mut due = due;
    due.sort_by(|a, b| {
        (a.tier.unwrap_or(Tier::P3), &a.due_at).cmp(&(b.tier.unwrap_or(Tier::P3), &b.due_at))
    });
    let room = limit.saturating_sub(posted_today) as usize;
    let rest = due.split_off(room.min(due.len()));
    (due, rest)
}

/// [`DailySlots`] on DynamoDB: `SECRUNS#<yyyymmdd>` in the settings table
/// (a conditional ADD, so ticks and requests at once never pass the limit)
/// and the analytics day item for the tokens fix runs spent.
pub struct DynamoSlots<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub settings_table: &'a str,
    pub analytics_table: &'a str,
    pub team_id: &'a str,
}

fn n_attr(n: i64) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::N(n.to_string())
}

fn s_attr(s: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(s.to_string())
}

impl DynamoSlots<'_> {
    async fn take_over(&self, day: &str) -> bool {
        self.dynamo
            .update_item()
            .table_name(self.settings_table)
            .key("pk", s_attr(self.team_id))
            .key("sk", s_attr(&format!("SECRUNS#{day}")))
            .update_expression("SET #ttl = if_not_exists(#ttl, :ttl) ADD runs :one, raised :one")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":one", n_attr(1))
            .expression_attribute_values(":ttl", n_attr(Utc::now().timestamp() + 400 * 86_400))
            .send()
            .await
            .is_ok()
    }
}

impl DailySlots for DynamoSlots<'_> {
    async fn take_run(&self, day: &str, limit: u32) -> bool {
        // Fails closed: a failed write is no slot.
        self.dynamo
            .update_item()
            .table_name(self.settings_table)
            .key("pk", s_attr(self.team_id))
            .key("sk", s_attr(&format!("SECRUNS#{day}")))
            .update_expression("SET #ttl = if_not_exists(#ttl, :ttl) ADD runs :one")
            .condition_expression("attribute_not_exists(runs) OR runs < :cap")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":one", n_attr(1))
            .expression_attribute_values(":cap", n_attr(limit as i64))
            .expression_attribute_values(":ttl", n_attr(Utc::now().timestamp() + 400 * 86_400))
            .send()
            .await
            .is_ok()
    }
    async fn give_back(&self, day: &str) {
        let _ = self
            .dynamo
            .update_item()
            .table_name(self.settings_table)
            .key("pk", s_attr(self.team_id))
            .key("sk", s_attr(&format!("SECRUNS#{day}")))
            .update_expression("ADD runs :minus")
            .condition_expression("runs > :zero")
            .expression_attribute_values(":minus", n_attr(-1))
            .expression_attribute_values(":zero", n_attr(0))
            .send()
            .await;
    }
    async fn fix_tokens(&self, day: &str) -> u64 {
        if day.len() != 8 {
            return u64::MAX;
        }
        let date = format!("{}-{}-{}", &day[..4], &day[4..6], &day[6..8]);
        match self
            .dynamo
            .get_item()
            .table_name(self.analytics_table)
            .key("team_id", s_attr(self.team_id))
            .key("period", s_attr(&format!("DAY#{date}")))
            .send()
            .await
        {
            Ok(o) => crate::usage::feature_totals(o.item())
                .into_iter()
                .find(|(f, _)| f == crate::usage::feature::SECURITY_FIX)
                .map(|(_, u)| u.total())
                .unwrap_or(0),
            // Can't be read: count it as used up.
            Err(_) => u64::MAX,
        }
    }
    async fn take_run_over_limit(&self, day: &str) -> bool {
        self.take_over(day).await
    }
}

/// What a person's request for fix pull requests did: the units that start
/// now and those that wait for a daily limit, with the reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Requested {
    pub unit: String,
    pub ticket: String,
    pub repo: String,
    pub package: String,
    pub finding_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Decides which units of a person's request start now: the most urgent
/// first, within the day's run and token limits and one run per repository
/// at a time, unless an admin raised today's limit for this request (then
/// only the repository rule holds). Dry run never holds them: the person asked.
pub async fn admit_request<S: DailySlots>(
    slots: &S,
    cfg: &SecurityConfig,
    day: &str,
    units: &mut [FixUnit],
    findings: &[(Finding, FindingState)],
    raise: bool,
) -> (Vec<Requested>, Vec<Requested>) {
    order_units(units, findings);
    let mut busy: HashSet<String> = findings
        .iter()
        .filter(|(f, _)| f.status == Status::Fixing)
        .map(|(f, _)| f.repo.to_ascii_lowercase())
        .collect();
    let (mut started, mut queued) = (vec![], vec![]);
    for u in units.iter() {
        let r = |reason: Option<String>| Requested {
            unit: u.key.clone(),
            ticket: u.ticket.clone(),
            repo: u.repo.clone(),
            package: u
                .bumps
                .first()
                .map(|b| b.package.clone())
                .unwrap_or_default(),
            finding_ids: u.finding_ids.clone(),
            reason,
        };
        let repo_busy = busy.contains(&u.repo.to_ascii_lowercase());
        let admit = if raise && !repo_busy {
            if slots.take_run_over_limit(day).await {
                Admit::Start
            } else {
                Admit::Wait(
                    "Waiting: the day's counter couldn't be written; it starts on the next check."
                        .into(),
                )
            }
        } else {
            admit_run(slots, cfg, day, repo_busy).await
        };
        match admit {
            Admit::Start => {
                busy.insert(u.repo.to_ascii_lowercase());
                started.push(r(None));
            }
            Admit::Wait(why) => queued.push(r(Some(why))),
        }
    }
    (started, queued)
}

// ── Fix PRs a person asks for ───────────────────────────────────────────

/// Why a finding a person picked can't get a fix pull request now; with the
/// open pull request's link when that is why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Skipped {
    pub finding_id: String,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pr_url: Option<String>,
}

/// The fix units for the findings a person picked: one pull request per
/// repository, package and version (every advisory it has, bumped to the
/// version that clears them all), never one per advisory. Dry run doesn't
/// matter here: the person asked. Each finding that can't be fixed is
/// skipped with the reason.
pub fn manual_units(
    findings: &[(Finding, FindingState)],
    ids: &[String],
    cfg: &SecurityConfig,
    now: DateTime<Utc>,
) -> (Vec<FixUnit>, Vec<Skipped>) {
    let mut skipped = vec![];
    let mut picked: Vec<&(Finding, FindingState)> = vec![];
    let mut seen = HashSet::new();
    for id in ids {
        if !seen.insert(id.clone()) {
            continue;
        }
        let Some(pair) = findings.iter().find(|(f, _)| &f.id == id) else {
            skipped.push(Skipped {
                finding_id: id.clone(),
                reason: "not found".into(),
                pr_url: None,
            });
            continue;
        };
        let f = &pair.0;
        let reason = match (f.status, &f.pr) {
            (_, Some(p)) if p.state == "open" => Some("already has an open PR"),
            (Status::Dismissed | Status::Withdrawn, _) => Some("dismissed"),
            (Status::Fixed, _) => Some("already fixed"),
            (Status::Fixing, _) => Some("a fix run is already working on it"),
            _ if f.fixed_version.is_none() => Some("no fixed version"),
            _ => None,
        };
        match reason {
            Some(r) => skipped.push(Skipped {
                finding_id: id.clone(),
                reason: r.into(),
                pr_url: f
                    .pr
                    .as_ref()
                    .filter(|p| p.state == "open")
                    .map(|p| p.url.clone()),
            }),
            None => picked.push(pair),
        }
    }
    // One unit per group (repository, package, version).
    let mut by: BTreeMap<String, Vec<&(Finding, FindingState)>> = BTreeMap::new();
    for p in picked {
        by.entry(group_key(&p.0)).or_default().push(p);
    }
    let units = by
        .into_values()
        .map(|members| {
            let first = &members[0].0;
            let to = members
                .iter()
                .filter_map(|(f, _)| f.fixed_version.clone())
                .max_by(|a, b| compare_versions(a, b))
                .unwrap_or_default();
            let tier = members.iter().filter_map(|(f, _)| f.tier).min();
            let mut u = FixUnit {
                repo: first.repo.clone(),
                ecosystem: first.ecosystem.clone(),
                grouped: false,
                key: format!("{}@{}", first.package.to_ascii_lowercase(), to),
                tier,
                finding_ids: members.iter().map(|(f, _)| f.id.clone()).collect(),
                bumps: vec![Bump {
                    package: first.package.clone(),
                    from: first.version.clone(),
                    to: to.clone(),
                    major: is_major_bump(&first.version, &to),
                    manifest: first.manifest.clone(),
                }],
                ..Default::default()
            };
            let mut primaries: Vec<String> = members
                .iter()
                .map(|(f, _)| f.advisory.primary.clone())
                .collect();
            primaries.sort();
            primaries.dedup();
            let pairs: Vec<(&Finding, &FindingState)> =
                members.iter().map(|(f, s)| (f, s)).collect();
            u.ticket = ticket_id(&u.repo, &u.key);
            u.branch = branch(&u, None, now);
            u.title = pr_title("fix(deps)", &u, &primaries);
            u.labels = labels(cfg, &pairs, u.bumps[0].major);
            u
        })
        .collect();
    (units, skipped)
}

/// The findings a person asked a fix for that still wait (queued past a
/// daily limit): their units, the most urgent first.
pub fn requested_units(
    findings: &[(Finding, FindingState)],
    cfg: &SecurityConfig,
    now: DateTime<Utc>,
) -> Vec<FixUnit> {
    let ids: Vec<String> = findings
        .iter()
        .filter(|(f, st)| st.requested_by.is_some() && f.status == Status::Open && f.pr.is_none())
        .map(|(f, _)| f.id.clone())
        .collect();
    let mut units = manual_units(findings, &ids, cfg, now).0;
    order_units(&mut units, findings);
    units
}

// ── Fix units: what one pull request fixes ──────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Bump {
    pub package: String,
    pub from: String,
    pub to: String,
    pub major: bool,
    pub manifest: String,
}

/// One pull request's worth of fixes.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FixUnit {
    pub repo: String,
    pub ecosystem: String,
    /// Grouped (daily or weekly batch) or one fix.
    pub grouped: bool,
    /// What keys it: `pkg@to`, or `eco:daily:2026-11-10`.
    pub key: String,
    pub tier: Option<Tier>,
    pub finding_ids: Vec<String>,
    pub bumps: Vec<Bump>,
    pub ticket: String,
    pub branch: String,
    pub title: String,
    pub labels: Vec<String>,
}

/// The batch period a cadence opens in: the day, or the ISO week.
pub fn period(cadence: Cadence, now: DateTime<Utc>) -> String {
    match cadence {
        Cadence::Weekly => {
            let w = now.iso_week();
            format!("{}-w{:02}", w.year(), w.week())
        }
        _ => now.format("%Y-%m-%d").to_string(),
    }
}

fn slug_part(s: &str) -> String {
    let mut out = String::new();
    for c in s.trim_start_matches('@').chars() {
        if c.is_ascii_alphanumeric() || c == '.' {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    out.trim_matches(['-', '.']).to_string()
}

pub fn ticket_id(repo: &str, key: &str) -> String {
    format!(
        "{TICKET_PREFIX}{}",
        crate::sha256_hex(&[&repo.to_ascii_lowercase(), key])[..12].to_ascii_uppercase()
    )
}

/// `coderhelm/security-<pkg>-<version>`, or `coderhelm/security-<eco>-<period>`
/// for a grouped one.
pub fn branch(unit: &FixUnit, cadence: Option<Cadence>, now: DateTime<Utc>) -> String {
    let name = if unit.grouped {
        let c = match cadence {
            Some(Cadence::Weekly) => "weekly",
            _ => "daily",
        };
        format!(
            "coderhelm/security-{}-{c}-{}",
            slug_part(&unit.ecosystem),
            period(cadence.unwrap_or(Cadence::Daily), now)
        )
    } else {
        let b = unit.bumps.first().cloned().unwrap_or_default();
        format!(
            "coderhelm/security-{}-{}",
            slug_part(&b.package),
            slug_part(&b.to)
        )
    };
    crate::git_conventions::branch(&name, &crate::git_conventions::Tokens::default(), "")
}

/// `fix(deps): bump <pkg> <a> to <b> [SECURITY] (<primary id>)`, or for a
/// group `fix(deps): bump <n> <ecosystem> dependencies [SECURITY] (<ids>)`.
pub fn pr_title(prefix: &str, unit: &FixUnit, primaries: &[String]) -> String {
    let ids = match primaries {
        [] => String::new(),
        [one] => format!(" ({one})"),
        [one, rest @ ..] => format!(" ({one}, +{} more)", rest.len()),
    };
    let prefix = if prefix.trim().is_empty() {
        "fix(deps)"
    } else {
        prefix.trim().trim_end_matches(':')
    };
    let body = match unit.bumps.as_slice() {
        [b] if !unit.grouped => format!("bump {} {} to {}", b.package, b.from, b.to),
        bumps => format!("bump {} {} dependencies", bumps.len(), unit.ecosystem),
    };
    let title = format!("{prefix}: {body} [SECURITY]{ids}");
    if title.chars().count() > 120 {
        let head: String = title.chars().take(117).collect();
        format!("{}...", head.trim_end())
    } else {
        title
    }
}

/// The label Dependabot gives an ecosystem, so existing filters keep working.
pub fn ecosystem_label(ecosystem: &str) -> Option<&'static str> {
    Some(match ecosystem.to_ascii_lowercase().as_str() {
        "npm" | "yarn" | "pnpm" => "javascript",
        "pip" | "pypi" | "poetry" | "uv" => "python",
        "go" | "golang" | "gomod" => "go",
        "maven" | "gradle" => "java",
        "rubygems" | "gem" | "bundler" => "ruby",
        "cargo" | "crates.io" | "rust" => "rust",
        "docker" => "docker",
        "actions" | "github actions" | "github" => "github_actions",
        "nuget" => ".NET",
        "composer" | "packagist" => "php",
        "hex" | "mix" => "elixir",
        "pub" => "dart",
        "swift" | "swifturl" => "swift",
        _ => return None,
    })
}

/// A pull request's labels: the team's, then severity, reachability,
/// `exploited`, `breaking` and the ecosystem, as the settings say.
pub fn labels(
    cfg: &SecurityConfig,
    findings: &[(&Finding, &FindingState)],
    major: bool,
) -> Vec<String> {
    let mut out: Vec<String> = cfg.labels.clone();
    let mut push = |l: &str| {
        if !out.iter().any(|o| o.eq_ignore_ascii_case(l)) {
            out.push(l.to_string());
        }
    };
    if cfg.severity_labels {
        let top = findings
            .iter()
            .map(|(f, s)| score(f, s))
            .fold(0.0, f64::max);
        push(&format!("severity:{}", severity_word(top)));
    }
    if cfg.reachability_labels {
        let verdicts: Vec<Verdict> = findings.iter().map(|(f, _)| f.verdict).collect();
        let label = if verdicts.contains(&Verdict::Reachable) {
            "reachable"
        } else if !verdicts.is_empty()
            && verdicts
                .iter()
                .all(|v| matches!(v, Verdict::NotReachable | Verdict::NotLoaded))
        {
            "not-reachable"
        } else {
            "reachability:unknown"
        };
        push(label);
    }
    if findings.iter().any(|(f, _)| f.advisory.kev) {
        push("exploited");
    }
    if major {
        push("breaking");
    }
    if cfg.ecosystem_labels {
        if let Some(e) = findings
            .first()
            .and_then(|(f, _)| ecosystem_label(&f.ecosystem))
        {
            push(e);
        }
    }
    out
}

/// Whether a finding may get a pull request now: open, not held, not
/// ignored, with a version to move to, its triage done, and its tier's
/// cadence not off.
pub fn fixable(f: &Finding, st: &FindingState, cfg: &SecurityConfig, now: DateTime<Utc>) -> bool {
    let held = st
        .hold_until
        .as_deref()
        .and_then(parse_time)
        .is_some_and(|t| t > now);
    f.status == Status::Open
        && !f.private_report
        && f.ecosystem != OWN_CODE
        && !held
        && f.verdict != Verdict::Pending
        && !f.version.is_empty()
        && f.fixed_version.is_some()
        && cfg.repos.includes(&f.repo)
        && cfg.ignored(f, now).is_none()
        && f.tier.is_some_and(|t| cfg.open_prs.get(t) != Cadence::Off)
}

/// The fix units the findings make: P0 and P1 one per (repository,
/// package, target version); P2 and P3 grouped per repository and
/// ecosystem. Every finding a unit clears is in it.
pub fn plan_units(
    findings: &[(Finding, FindingState)],
    cfg: &SecurityConfig,
    now: DateTime<Utc>,
) -> Vec<FixUnit> {
    let mut units: BTreeMap<(String, String), FixUnit> = BTreeMap::new();
    let eligible: Vec<&Finding> = findings
        .iter()
        .filter(|(f, st)| fixable(f, st, cfg, now))
        .map(|(f, _)| f)
        .collect();
    let single = |f: &Finding| {
        !matches!(
            cfg.open_prs.get(f.tier.unwrap_or(Tier::P3)),
            Cadence::Daily | Cadence::Weekly
        )
    };
    // Urgent ones first: a package with a fix of its own takes the
    // package's lower-tier findings too, so nothing bumps it twice.
    let mut ordered: Vec<&Finding> = eligible.iter().copied().filter(|f| single(f)).collect();
    ordered.extend(eligible.iter().copied().filter(|f| !single(f)));
    for f in ordered {
        let tier = f.tier.unwrap_or(Tier::P3);
        let cadence = cfg.open_prs.get(tier);
        let to = f.fixed_version.clone().unwrap_or_default();
        let repo = f.repo.to_ascii_lowercase();
        let own = format!("pkg:{}", f.package.to_ascii_lowercase());
        let grouped = !single(f) && !units.contains_key(&(repo.clone(), own.clone()));
        let key = if grouped {
            format!(
                "{}:{}:{}",
                f.ecosystem.to_ascii_lowercase(),
                if cadence == Cadence::Weekly {
                    "weekly"
                } else {
                    "daily"
                },
                period(cadence, now)
            )
        } else {
            own
        };
        let u = units.entry((repo, key.clone())).or_insert_with(|| FixUnit {
            repo: f.repo.clone(),
            ecosystem: f.ecosystem.clone(),
            grouped,
            key: key.clone(),
            ..Default::default()
        });
        u.tier = Some(u.tier.map_or(tier, |t| t.min(tier)));
        u.finding_ids.push(f.id.clone());
        match u
            .bumps
            .iter_mut()
            .find(|b| b.package.eq_ignore_ascii_case(&f.package))
        {
            Some(b) => {
                if compare_versions(&to, &b.to) == std::cmp::Ordering::Greater {
                    b.to = to.clone();
                    b.major = is_major_bump(&b.from, &to);
                }
            }
            None => u.bumps.push(Bump {
                package: f.package.clone(),
                from: f.version.clone(),
                to: to.clone(),
                major: is_major_bump(&f.version, &to),
                manifest: f.manifest.clone(),
            }),
        }
    }
    let by_id: HashMap<&str, (&Finding, &FindingState)> = findings
        .iter()
        .map(|(f, s)| (f.id.as_str(), (f, s)))
        .collect();
    units
        .into_values()
        .map(|mut u| {
            let members: Vec<(&Finding, &FindingState)> = u
                .finding_ids
                .iter()
                .filter_map(|id| by_id.get(id.as_str()).copied())
                .collect();
            let mut primaries: Vec<String> = members
                .iter()
                .map(|(f, _)| f.advisory.primary.clone())
                .collect();
            primaries.sort();
            primaries.dedup();
            let major = u.bumps.iter().any(|b| b.major);
            if !u.grouped {
                if let Some(b) = u.bumps.first() {
                    u.key = format!("{}@{}", b.package.to_ascii_lowercase(), b.to);
                }
            }
            let cadence = u.tier.map(|t| cfg.open_prs.get(t));
            u.ticket = ticket_id(&u.repo, &u.key);
            u.branch = branch(&u, cadence, now);
            u.title = pr_title("fix(deps)", &u, &primaries);
            u.labels = labels(cfg, &members, major);
            u
        })
        .collect()
}

/// Whether a unit opens on this tick: now and hourly at once, daily and
/// weekly once per period (the caller claims the period).
pub fn unit_due(unit: &FixUnit, cfg: &SecurityConfig) -> bool {
    unit.tier
        .map(|t| cfg.open_prs.get(t))
        .is_some_and(|c| c != Cadence::Off)
}

/// The plan a fix run follows, given to the agent as the ticket's body. The
/// marker line carries the branch.
pub fn ticket_body(unit: &FixUnit, members: &[&Finding], cfg: &SecurityConfig) -> String {
    if unit.ecosystem == crate::base_images::ECOSYSTEM {
        return crate::base_images::ticket_body(unit, members);
    }
    let mut s = String::new();
    s.push_str(&format!(
        "{MARKER}\n<!-- coderhelm:security-branch {} -->\n\n",
        unit.branch
    ));
    s.push_str("Fix these security advisories in the dependencies. Change only what the fix needs.\n\n## Bumps\n");
    for b in &unit.bumps {
        s.push_str(&format!(
            "- `{}` {} to {} ({}){}\n",
            b.package,
            b.from,
            b.to,
            if b.manifest.is_empty() {
                "manifest unknown"
            } else {
                &b.manifest
            },
            if b.major {
                " — a major version: fix the breaking changes in this same change"
            } else {
                ""
            }
        ));
    }
    s.push_str("\n## How\n");
    s.push_str("1. Prefer a lockfile-only update within the allowed range (for example `npm update <pkg>`, `pnpm update <pkg>`, `cargo update -p <pkg> --precise <version>`, `bundle update --conservative <pkg>`).\n");
    s.push_str("2. If the package is transitive and the lockfile alone can't reach the version, add the ecosystem's override (npm `overrides`, pnpm `pnpm.overrides`, Yarn `resolutions`, Poetry or uv constraints, Maven `dependencyManagement`, Gradle `constraints`) with a comment saying it can be removed once the parent ships the fixed version.\n");
    s.push_str("3. Otherwise bump the version in the manifest.\n");
    s.push_str("Regenerate lockfiles with the repository's own package manager and its pinned version; never edit a lockfile by hand.\n");
    if cfg.major_bumps == MajorBumps::FixInPr && unit.bumps.iter().any(|b| b.major) {
        s.push_str("A major version is needed: follow the upgrade guide and fix the code it breaks so the tests pass.\n");
    }
    s.push_str("\n## Advisories\n");
    for f in members {
        s.push_str(&format!(
            "- {} ({}) in `{}` {}: {}\n",
            f.advisory.primary,
            f.advisory.ids.join(", "),
            f.package,
            f.version,
            crate::truncate_str(&f.advisory.title, 200)
        ));
        if let Some(e) = &f.evidence {
            for l in e.lines.iter().take(5) {
                s.push_str(&format!(
                    "  - `{}:{}` {}\n",
                    l.file,
                    l.line,
                    crate::truncate_str(&l.text, 160)
                ));
            }
        }
    }
    s
}

/// The branch a security ticket's body names.
pub fn branch_in_body(ticket_id: &str, body: &str) -> Option<String> {
    if !is_ticket(ticket_id) {
        return None;
    }
    let rest = body.split("<!-- coderhelm:security-branch ").nth(1)?;
    let b = rest.split(" -->").next()?.trim();
    (b.starts_with("coderhelm/security-") && crate::git_conventions::is_valid_branch(b))
        .then(|| b.to_string())
}

fn fmt_num(n: f64) -> String {
    let s = format!("{n:.2}");
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// The pull request's description: Summary, Advisory, Affected,
/// Reachability, Change (the run's own description), Proof, Closes, and the
/// data's credit. Public advisory facts only: how exposed the team's own
/// production is stays in the dashboard and the card.
pub fn pr_body(unit: &FixUnit, members: &[(&Finding, &FindingState)], change: &str) -> String {
    let mut s = String::new();
    let top = members
        .iter()
        .filter_map(|(f, _)| f.tier)
        .min()
        .unwrap_or(Tier::P3);
    let due = members
        .iter()
        .filter_map(|(f, _)| parse_time(&f.due_at))
        .min()
        .map(|d| d.format("%b %-d").to_string());
    let what: Vec<String> = members
        .iter()
        .map(|(f, _)| {
            if f.advisory.title.is_empty() {
                format!("{} in {}", f.advisory.primary, f.package)
            } else {
                format!(
                    "{} in {}",
                    f.advisory.title.trim_end_matches('.'),
                    f.package
                )
            }
        })
        .collect();
    s.push_str(&format!(
        "## Summary\n\nFixes {}. {}{}.\n\n",
        what.join("; "),
        top.as_str(),
        due.map(|d| format!(", due {d}")).unwrap_or_default()
    ));
    s.push_str("## Advisory\n\n");
    for (f, st) in members {
        let a = &f.advisory;
        let mut facts = vec![a.ids.join(", ")];
        let sc = score(f, st);
        match a.cvss {
            Some(c) => facts.push(format!("CVSS {}", fmt_num(c))),
            None if sc > 0.0 => facts.push(format!("severity {}", severity_word(sc))),
            None => {}
        }
        if let Some(e) = a.epss {
            facts.push(format!("EPSS {}", fmt_num(e)));
        }
        if a.kev {
            facts.push(match &a.kev_due {
                Some(d) => format!(
                    "on CISA's exploited list (due {})",
                    d.get(..10).unwrap_or(d)
                ),
                None => "on CISA's exploited list".to_string(),
            });
        }
        if a.malware {
            facts.push("malicious package".into());
        }
        if !a.cwe.is_empty() {
            facts.push(a.cwe.join(", "));
        }
        if let Some(p) = &a.published_at {
            facts.push(format!("published {}", p.get(..10).unwrap_or(p)));
        }
        s.push_str(&format!("- {}\n", facts.join(" · ")));
        for u in a.urls.iter().take(3) {
            s.push_str(&format!("  - {u}\n"));
        }
    }
    s.push_str("\n## Affected\n\n");
    for (f, _) in members {
        let path = if f.path.is_empty() {
            f.package.clone()
        } else {
            f.path.join(" > ")
        };
        let scope = match f.scope {
            Scope::Runtime => "runtime",
            Scope::Dev => "development only",
            Scope::Unknown => "scope unknown",
        };
        let rel = match f.relation {
            Relation::Direct => "direct",
            Relation::Transitive => "transitive",
            Relation::Unknown => "relation unknown",
        };
        s.push_str(&format!(
            "- `{path}` {} in `{}`, {scope}, {rel}\n",
            f.version, f.manifest
        ));
    }
    s.push_str("\n## Reachability\n\n");
    for (f, _) in members {
        let v = match f.verdict {
            Verdict::Reachable => "Reachable",
            Verdict::NotReachable => "Present, not reachable",
            Verdict::NotLoaded => "Not loaded at runtime",
            Verdict::Unknown | Verdict::Pending => "Unknown",
        };
        match &f.evidence {
            Some(e) => {
                s.push_str(&format!(
                    "- {}: **{v}**. {}\n",
                    f.advisory.primary,
                    e.summary.trim()
                ));
                for l in e.lines.iter().take(8) {
                    s.push_str(&format!(
                        "  - `{}:{}` {}\n",
                        l.file,
                        l.line,
                        crate::truncate_str(l.text.trim(), 160)
                    ));
                }
                if !e.checked.is_empty() {
                    s.push_str(&format!(
                        "  - Checked: {} Confidence {}.\n",
                        e.checked.trim(),
                        e.confidence
                    ));
                }
            }
            None => s.push_str(&format!("- {}: **{v}**.\n", f.advisory.primary)),
        }
    }
    s.push_str("\n## Change\n\n");
    for b in &unit.bumps {
        s.push_str(&format!(
            "- `{}` {} to {}{}\n",
            b.package,
            b.from,
            b.to,
            if b.major { " (major version)" } else { "" }
        ));
    }
    let change = change.trim();
    if !change.is_empty() {
        s.push('\n');
        s.push_str(change);
        s.push('\n');
    }
    s.push_str("\n## Proof\n\nCI runs on this pull request. After it merges, the dependency list is read again and the advisory clears from Security.\n");
    s.push_str("\n## Closes\n\n");
    let mut closes = 0;
    for (f, _) in members {
        if let Some(a) = &f.dependabot_alert {
            s.push_str(&format!("- Dependabot alert [#{}]({})\n", a.number, a.url));
            closes += 1;
        }
    }
    if closes == 0 {
        s.push_str("- No Dependabot alert is open for these.\n");
    }
    s.push_str(&format!("\n{ATTRIBUTION}\n\n{MARKER}\n"));
    s
}

// ── Inventory ───────────────────────────────────────────────────────────

/// The manifest of a dependency read from the dependency graph's SBOM,
/// which doesn't say which lockfile it came from.
pub const SBOM_MANIFEST: &str = "sbom";

/// One dependency of a repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Dep {
    /// OSV's ecosystem name (`npm`, `PyPI`, `Go`, `crates.io`, ...), or
    /// `docker` for a base image.
    pub ecosystem: String,
    pub name: String,
    pub version: String,
    pub manifest: String,
    pub scope: Scope,
}

/// OSV's ecosystem for a package URL type.
pub fn osv_ecosystem(purl_type: &str) -> Option<&'static str> {
    Some(match purl_type.to_ascii_lowercase().as_str() {
        "npm" => "npm",
        "pypi" => "PyPI",
        "golang" => "Go",
        "cargo" => "crates.io",
        "maven" => "Maven",
        "nuget" => "NuGet",
        "gem" => "RubyGems",
        "composer" => "Packagist",
        "hex" => "Hex",
        "pub" => "Pub",
        "githubactions" => "GitHub Actions",
        "swift" => "SwiftURL",
        _ => return None,
    })
}

/// GitHub's ecosystem names (Dependabot, advisories) as OSV's.
pub fn osv_ecosystem_of_github(e: &str) -> &str {
    match e.to_ascii_lowercase().as_str() {
        "npm" => "npm",
        "pip" | "pypi" => "PyPI",
        "go" => "Go",
        "rust" | "cargo" => "crates.io",
        "maven" => "Maven",
        "nuget" => "NuGet",
        "rubygems" => "RubyGems",
        "composer" => "Packagist",
        "erlang" | "hex" => "Hex",
        "pub" => "Pub",
        "actions" => "GitHub Actions",
        "swift" => "SwiftURL",
        _ => e,
    }
}

/// Package names compare as the ecosystem does (PyPI ignores case, `_` and `.`).
pub fn same_package(ecosystem: &str, a: &str, b: &str) -> bool {
    if ecosystem.eq_ignore_ascii_case("pypi") {
        let n = |s: &str| s.to_ascii_lowercase().replace(['_', '.'], "-");
        n(a) == n(b)
    } else {
        a.eq_ignore_ascii_case(b)
    }
}

/// Reads a package URL: (type, name, version). Maven names are
/// `group:artifact`, npm scopes keep their `@`.
pub fn parse_purl(purl: &str) -> Option<(String, String, String)> {
    let rest = purl.strip_prefix("pkg:")?;
    let rest = rest.split(['?', '#']).next()?;
    let (ty, rest) = rest.split_once('/')?;
    let (path, version) = match rest.rsplit_once('@') {
        Some((p, v)) if !p.is_empty() => (p, v),
        _ => (rest, ""),
    };
    let path = urlencoding::decode(path).ok()?.to_string();
    let version = urlencoding::decode(version).ok()?.to_string();
    let name = if ty.eq_ignore_ascii_case("maven") {
        path.replacen('/', ":", 1)
    } else {
        path
    };
    Some((ty.to_ascii_lowercase(), name, version))
}

/// Dependencies from GitHub's dependency graph SBOM (SPDX JSON, the
/// response of `GET /repos/{o}/{r}/dependency-graph/sbom`). The repository's
/// own package and anything without a version are left out.
pub fn deps_from_sbom(sbom: &Value) -> Vec<Dep> {
    inventory_from_sbom(sbom).0
}

/// Whether a version is one exact release (not a range, a branch or a
/// partial like `3`): OSV matches only those. npm needs all three parts.
pub fn exact_version(ecosystem: &str, v: &str) -> bool {
    if v.is_empty() || v.contains(['<', '>', '^', '~', '*', ' ', '|', '=']) {
        return false;
    }
    let v = v.strip_prefix('v').unwrap_or(v);
    if ecosystem == "npm" {
        let core = v.split(['-', '+']).next().unwrap_or("");
        let parts: Vec<&str> = core.split('.').collect();
        return parts.len() == 3
            && parts
                .iter()
                .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()));
    }
    v.chars().next().is_some_and(|c| c.is_ascii_digit())
}

/// [`deps_from_sbom`], and how many packages were left out for having no
/// exact version (a range or a partial one, which OSV can't match).
pub fn inventory_from_sbom(sbom: &Value) -> (Vec<Dep>, usize) {
    let doc = if sbom["sbom"].is_object() {
        &sbom["sbom"]
    } else {
        sbom
    };
    let packages = doc["packages"].as_array();
    // The repository itself: what the document describes.
    let described: Vec<&str> = doc["documentDescribes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let mut out: Vec<Dep> = vec![];
    let mut skipped = 0;
    for p in packages.into_iter().flatten() {
        if p["SPDXID"]
            .as_str()
            .is_some_and(|id| described.contains(&id))
        {
            continue;
        }
        let purl = p["externalRefs"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|r| r["referenceType"].as_str() == Some("purl"))
            .and_then(|r| r["referenceLocator"].as_str());
        let Some((ty, name, version)) = purl.and_then(parse_purl) else {
            continue;
        };
        // `pkg:github/...` is the repository's own package, not an Action.
        if ty == "github" {
            continue;
        }
        let Some(eco) = osv_ecosystem(&ty) else {
            continue;
        };
        let version = if version.is_empty() {
            p["versionInfo"].as_str().unwrap_or("").to_string()
        } else {
            version
        };
        if !exact_version(eco, &version) {
            skipped += 1;
            continue;
        }
        let d = Dep {
            ecosystem: eco.to_string(),
            name,
            version,
            manifest: SBOM_MANIFEST.to_string(),
            scope: Scope::Unknown,
        };
        if !out.contains(&d) {
            out.push(d);
        }
    }
    (out, skipped)
}

/// The base images of a Dockerfile: each `FROM` that isn't `scratch` or an
/// earlier stage, as (image, tag or digest).
pub fn dockerfile_images(text: &str) -> Vec<(String, String)> {
    let mut stages: Vec<String> = vec![];
    let mut out = vec![];
    for line in text.lines() {
        let l = line.trim();
        let Some(rest) = l
            .strip_prefix("FROM ")
            .or_else(|| l.strip_prefix("from "))
            .or_else(|| l.strip_prefix("From "))
        else {
            continue;
        };
        let words: Vec<&str> = rest
            .split_whitespace()
            .filter(|w| !w.starts_with("--"))
            .collect();
        let Some(image) = words.first() else { continue };
        if let Some(pos) = words.iter().position(|w| w.eq_ignore_ascii_case("as")) {
            if let Some(name) = words.get(pos + 1) {
                stages.push(name.to_ascii_lowercase());
            }
        }
        let image = *image;
        if image.eq_ignore_ascii_case("scratch")
            || stages[..stages.len().saturating_sub(1)]
                .iter()
                .any(|s| s.eq_ignore_ascii_case(image))
            || image.contains('$')
        {
            continue;
        }
        let (name, tag) = if let Some((n, d)) = image.split_once('@') {
            (n.to_string(), d.to_string())
        } else {
            match image.rsplit_once(':') {
                Some((n, t)) if !t.contains('/') => (n.to_string(), t.to_string()),
                _ => (image.to_string(), "latest".to_string()),
            }
        };
        out.push((name, tag));
    }
    out
}

/// Whether a path is a Dockerfile.
pub fn is_dockerfile(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    name == "dockerfile" || name.starts_with("dockerfile.") || name.ends_with(".dockerfile")
}

/// Files whose change means the dependency list changed.
pub fn is_dependency_file(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    is_dockerfile(path)
        || matches!(
            name.as_str(),
            "package.json"
                | "package-lock.json"
                | "npm-shrinkwrap.json"
                | "pnpm-lock.yaml"
                | "yarn.lock"
                | "requirements.txt"
                | "poetry.lock"
                | "pyproject.toml"
                | "uv.lock"
                | "pipfile.lock"
                | "go.mod"
                | "go.sum"
                | "cargo.toml"
                | "cargo.lock"
                | "gemfile.lock"
                | "composer.lock"
                | "pom.xml"
                | "build.gradle"
                | "build.gradle.kts"
                | "gradle.lockfile"
                | "packages.lock.json"
                | "mix.lock"
                | "pubspec.lock"
        )
}

/// Scopes a lockfile records: npm's `package-lock.json` (v2 and v3 mark
/// `dev` and `devOptional`), as package name to scope.
pub fn npm_lock_scopes(lock: &Value) -> HashMap<String, Scope> {
    let mut out = HashMap::new();
    for (path, p) in lock["packages"].as_object().into_iter().flatten() {
        let Some(name) = path
            .rsplit("node_modules/")
            .next()
            .filter(|n| !n.is_empty())
        else {
            continue;
        };
        if path.is_empty() {
            continue;
        }
        let dev = p["dev"].as_bool() == Some(true) || p["devOptional"].as_bool() == Some(true);
        let scope = if dev { Scope::Dev } else { Scope::Runtime };
        // One runtime copy makes the package runtime.
        out.entry(name.to_string())
            .and_modify(|s| {
                if scope == Scope::Runtime {
                    *s = Scope::Runtime
                }
            })
            .or_insert(scope);
    }
    out
}

/// Scopes from `poetry.lock` (`category = "dev"` on older lockfiles).
pub fn poetry_lock_scopes(text: &str) -> HashMap<String, Scope> {
    let mut out = HashMap::new();
    let mut name: Option<String> = None;
    for line in text.lines() {
        let l = line.trim();
        if l == "[[package]]" {
            name = None;
        } else if let Some(v) = l.strip_prefix("name = ") {
            name = Some(v.trim_matches('"').to_ascii_lowercase());
        } else if let Some(v) = l.strip_prefix("category = ") {
            if let Some(n) = &name {
                let s = if v.trim_matches('"') == "dev" {
                    Scope::Dev
                } else {
                    Scope::Runtime
                };
                out.insert(n.clone(), s);
            }
        }
    }
    out
}

// ── Source parsers ──────────────────────────────────────────────────────

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s.as_str().map(String::from))
        .collect()
}

fn cvss_of(adv: &Value) -> (Option<f64>, Option<String>) {
    let sev = &adv["cvss_severities"];
    for k in ["cvss_v4", "cvss_v3"] {
        if let Some(s) = sev[k]["score"].as_f64().filter(|s| *s > 0.0) {
            return (Some(s), sev[k]["vector_string"].as_str().map(String::from));
        }
    }
    match adv["cvss"]["score"].as_f64().filter(|s| *s > 0.0) {
        Some(s) => (
            Some(s),
            adv["cvss"]["vector_string"].as_str().map(String::from),
        ),
        None => (None, None),
    }
}

/// A GitHub advisory (the Dependabot alert's `security_advisory`, the
/// global advisories API, or the `security_advisory` webhook) as a record,
/// for one package.
pub fn advisory_from_github(adv: &Value, package: Option<&str>, source: &str) -> AdvisoryRecord {
    let mut ids: Vec<String> = vec![];
    for k in ["ghsa_id", "cve_id"] {
        if let Some(s) = adv[k].as_str().filter(|s| !s.is_empty()) {
            ids.push(s.to_string());
        }
    }
    for i in adv["identifiers"].as_array().into_iter().flatten() {
        if let Some(v) = i["value"].as_str() {
            ids.push(v.to_string());
        }
    }
    let (cvss, vector) = cvss_of(adv);
    let mut ranges = vec![];
    let mut fixed = vec![];
    let mut symbols = vec![];
    for v in adv["vulnerabilities"].as_array().into_iter().flatten() {
        let name = v["package"]["name"].as_str().unwrap_or("");
        if package.is_some_and(|p| !p.eq_ignore_ascii_case(name)) {
            continue;
        }
        if let Some(r) = v["vulnerable_version_range"].as_str() {
            ranges.push(r.to_string());
        }
        let fp = &v["first_patched_version"];
        if let Some(f) = fp.as_str().or_else(|| fp["identifier"].as_str()) {
            fixed.push(f.to_string());
        }
        symbols.extend(strings(&v["vulnerable_functions"]));
    }
    let cwe = adv["cwes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c["cwe_id"].as_str().map(String::from))
        .collect();
    let mut urls: Vec<String> = vec![];
    if let Some(u) = adv["html_url"].as_str() {
        urls.push(u.to_string());
    }
    for r in adv["references"].as_array().into_iter().flatten() {
        if let Some(u) = r.as_str().or_else(|| r["url"].as_str()) {
            urls.push(u.to_string());
        }
    }
    urls.truncate(10);
    AdvisoryRecord {
        ids,
        title: adv["summary"].as_str().unwrap_or("").to_string(),
        summary: crate::truncate_str(adv["description"].as_str().unwrap_or(""), 2000).to_string(),
        cvss,
        cvss_vector: vector,
        severity: adv["severity"].as_str().map(String::from),
        epss: adv["epss"]["percentage"]
            .as_f64()
            .or_else(|| adv["epss"][0]["percentage"].as_f64()),
        epss_percentile: adv["epss"]["percentile"]
            .as_f64()
            .or_else(|| adv["epss"][0]["percentile"].as_f64()),
        malware: adv["type"].as_str() == Some("malware")
            || adv["classification"]
                .as_str()
                .is_some_and(|c| c.eq_ignore_ascii_case("malware")),
        cwe,
        published_at: adv["published_at"].as_str().map(String::from),
        urls,
        source: source.to_string(),
        withdrawn: adv["withdrawn_at"].as_str().is_some_and(|s| !s.is_empty()),
        ranges,
        fixed,
        symbols,
    }
}

/// A Dependabot alert (webhook `alert` or the alerts API) as an observation.
/// `Advisory::source` of a repository's own security advisory.
pub const REPOSITORY_ADVISORY: &str = "repository_advisory";
/// The ecosystem of a finding in the repository's own code (no dependency
/// list names it, so the inventory never calls it fixed).
pub const OWN_CODE: &str = "repository";
/// The manifest of a finding in the repository's own code.
pub const OWN_CODE_MANIFEST: &str = "(this repository's code)";

/// How the repositories' own advisories were last read (the team state's
/// `repository_advisories`), for the Security page: `ok`, `no_permission`,
/// `error` or `unread`, and when CoderHelm can't read them, a sentence saying
/// so and how to add the permission.
pub fn repo_advisories_source(state: &str) -> Value {
    let state = match state {
        "ok" | "no_permission" | "error" => state,
        _ => "unread",
    };
    json!({
        "state": state,
        "note": (state == "no_permission").then(|| crate::github_grants::limit_note(
            "repository_advisories",
            "Your repositories' own advisories and private vulnerability reports aren't read",
        )),
    })
}

/// The repository's own advisories (`GET /repos/{o}/{r}/security-advisories`,
/// the `repository_advisories` permission).
pub fn repo_advisories_path(repo: &str) -> String {
    format!("/repos/{repo}/security-advisories?per_page=100&sort=updated")
}

/// A repository's own security advisory as an observation: a flaw in its
/// own code, not a dependency. One still in `triage` (a private
/// vulnerability report) or `draft` is a private report; `closed` or
/// withdrawn is withdrawn. Its vulnerable functions are what triage looks
/// for in the code.
pub fn observation_from_repo_advisory(repo: &str, adv: &Value) -> Option<Observation> {
    let state = adv["state"].as_str().unwrap_or("");
    let mut a = advisory_from_github(adv, None, REPOSITORY_ADVISORY);
    if a.ids.is_empty() {
        return None;
    }
    a.withdrawn |= state == "closed" || adv["withdrawn_at"].is_string();
    if a.severity.is_none() {
        a.severity = adv["severity"].as_str().map(String::from);
    }
    let name = repo.rsplit('/').next().unwrap_or(repo);
    let private = matches!(state, "triage" | "draft");
    Some(Observation {
        repo: repo.to_string(),
        manifest: OWN_CODE_MANIFEST.to_string(),
        ecosystem: OWN_CODE.to_string(),
        package: name.to_string(),
        version: String::new(),
        advisory: a,
        private_report: private,
        ..Default::default()
    })
}

pub fn observation_from_alert(repo: &str, alert: &Value) -> Option<Observation> {
    let dep = &alert["dependency"];
    let pkg = dep["package"]["name"]
        .as_str()
        .or_else(|| alert["security_vulnerability"]["package"]["name"].as_str())?;
    let eco = dep["package"]["ecosystem"]
        .as_str()
        .or_else(|| alert["security_vulnerability"]["package"]["ecosystem"].as_str())
        .unwrap_or("");
    let mut adv = advisory_from_github(&alert["security_advisory"], Some(pkg), "dependabot");
    let vuln = &alert["security_vulnerability"];
    if let Some(r) = vuln["vulnerable_version_range"].as_str() {
        if !adv.ranges.iter().any(|x| x == r) {
            adv.ranges.push(r.to_string());
        }
    }
    if let Some(f) = vuln["first_patched_version"]["identifier"].as_str() {
        if !adv.fixed.iter().any(|x| x == f) {
            adv.fixed.push(f.to_string());
        }
    }
    if adv.severity.is_none() {
        adv.severity = vuln["severity"].as_str().map(String::from);
    }
    let scope = match dep["scope"].as_str() {
        Some("runtime") => Scope::Runtime,
        Some("development") => Scope::Dev,
        _ => Scope::Unknown,
    };
    let relation = match dep["relationship"].as_str() {
        Some("direct") => Relation::Direct,
        Some("transitive") => Relation::Transitive,
        _ => Relation::Unknown,
    };
    let state = alert["state"].as_str().map(String::from);
    let dismissed = (state.as_deref() == Some("dismissed")).then(|| Dismissed {
        by: alert["dismissed_by"]["login"]
            .as_str()
            .unwrap_or("GitHub")
            .to_string(),
        reason: format!(
            "{}{}",
            alert["dismissed_reason"]
                .as_str()
                .unwrap_or("dismissed on GitHub"),
            alert["dismissed_comment"]
                .as_str()
                .filter(|c| !c.is_empty())
                .map(|c| format!(": {c}"))
                .unwrap_or_default()
        ),
        until: None,
        at: alert["dismissed_at"].as_str().unwrap_or("").to_string(),
    });
    Some(Observation {
        repo: repo.to_string(),
        manifest: dep["manifest_path"]
            .as_str()
            .unwrap_or(SBOM_MANIFEST)
            .to_string(),
        ecosystem: osv_ecosystem_of_github(eco).to_string(),
        package: pkg.to_string(),
        // The alert doesn't carry the installed version; the inventory does.
        version: String::new(),
        advisory: adv,
        scope,
        relation,
        path: vec![],
        alert: alert["number"].as_u64().map(|n| AlertRef {
            number: n,
            url: alert["html_url"].as_str().unwrap_or("").to_string(),
        }),
        alert_state: state,
        alert_dismissed: dismissed,
        private_report: false,
    })
}

/// Whether an OSV `affected` entry's ranges or versions cover `version`.
pub fn osv_affects(affected: &Value, version: &str) -> bool {
    use std::cmp::Ordering::*;
    if strings(&affected["versions"]).iter().any(|v| v == version) {
        return true;
    }
    for r in affected["ranges"].as_array().into_iter().flatten() {
        let ty = r["type"].as_str().unwrap_or("");
        if ty != "SEMVER" && ty != "ECOSYSTEM" {
            continue;
        }
        let mut events: Vec<(String, String)> = r["events"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|e| {
                let (k, v) = e.as_object()?.iter().next()?;
                Some((k.clone(), v.as_str()?.to_string()))
            })
            .collect();
        events.sort_by(|a, b| {
            if a.1 == "0" {
                return Less;
            }
            if b.1 == "0" {
                return Greater;
            }
            compare_versions(&a.1, &b.1)
        });
        let mut inside = false;
        for (kind, v) in &events {
            let c = if v == "0" {
                Greater
            } else {
                compare_versions(version, v)
            };
            match kind.as_str() {
                "introduced" if c != Less => inside = true,
                "fixed" if c != Less => inside = false,
                "last_affected" if c == Greater => inside = false,
                _ => {}
            }
        }
        if inside {
            return true;
        }
    }
    false
}

/// An OSV record as an advisory record for one package (ecosystem and
/// name), when the record names it. Chainguard and Wolfi records are left
/// out (their license isn't for commercial use).
pub fn advisory_from_osv(v: &Value, ecosystem: &str, package: &str) -> Option<AdvisoryRecord> {
    let id = v["id"].as_str()?;
    if id.starts_with("CGA-") || id.to_ascii_lowercase().contains("wolfi") {
        return None;
    }
    let mut ranges_fixed = vec![];
    let mut symbols = vec![];
    let mut any = false;
    for a in v["affected"].as_array().into_iter().flatten() {
        let e = a["package"]["ecosystem"].as_str().unwrap_or("");
        let n = a["package"]["name"].as_str().unwrap_or("");
        let e_base = e.split(':').next().unwrap_or(e);
        if !e_base.eq_ignore_ascii_case(ecosystem) || !same_package(ecosystem, n, package) {
            continue;
        }
        any = true;
        for r in a["ranges"].as_array().into_iter().flatten() {
            for ev in r["events"].as_array().into_iter().flatten() {
                if let Some(f) = ev["fixed"].as_str() {
                    ranges_fixed.push(f.to_string());
                }
            }
        }
        for imp in a["ecosystem_specific"]["imports"]
            .as_array()
            .into_iter()
            .flatten()
        {
            symbols.extend(strings(&imp["symbols"]));
        }
        symbols.extend(strings(&a["ecosystem_specific"]["affects"]["functions"]));
        symbols.extend(strings(&a["database_specific"]["vulnerable_functions"]));
    }
    if !any {
        return None;
    }
    let mut ids = vec![id.to_string()];
    ids.extend(strings(&v["aliases"]));
    let mut cvss_vector = None;
    for s in v["severity"].as_array().into_iter().flatten() {
        if s["type"].as_str().is_some_and(|t| t.starts_with("CVSS")) {
            cvss_vector = s["score"].as_str().map(String::from);
        }
    }
    let severity = v["database_specific"]["severity"]
        .as_str()
        .map(String::from);
    let cwe = strings(&v["database_specific"]["cwe_ids"]);
    let urls = v["references"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| r["url"].as_str().map(String::from))
        .take(10)
        .collect();
    Some(AdvisoryRecord {
        ids,
        title: v["summary"].as_str().unwrap_or("").to_string(),
        summary: crate::truncate_str(v["details"].as_str().unwrap_or(""), 2000).to_string(),
        cvss: None,
        cvss_vector,
        severity,
        malware: id.starts_with("MAL-"),
        cwe,
        published_at: v["published"].as_str().map(String::from),
        urls,
        source: "osv".into(),
        withdrawn: v["withdrawn"].as_str().is_some_and(|s| !s.is_empty()),
        fixed: ranges_fixed,
        symbols,
        ..Default::default()
    })
}

/// The CISA list as CVE id to due date (`YYYY-MM-DD`).
pub fn kev_map(feed: &Value) -> BTreeMap<String, String> {
    feed["vulnerabilities"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| {
            Some((
                v["cveID"].as_str()?.to_ascii_uppercase(),
                v["dueDate"].as_str().unwrap_or("").to_string(),
            ))
        })
        .collect()
}

/// FIRST's EPSS answer as CVE id to (score, percentile).
pub fn epss_map(resp: &Value) -> HashMap<String, (f64, f64)> {
    let num = |v: &Value| {
        v.as_f64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    };
    resp["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|d| {
            Some((
                d["cve"].as_str()?.to_ascii_uppercase(),
                (num(&d["epss"])?, num(&d["percentile"]).unwrap_or(0.0)),
            ))
        })
        .collect()
}

/// EPSS query strings of at most 2,000 characters of CVE ids each.
pub fn epss_batches(cves: &[String]) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    let mut cur = String::new();
    for c in cves {
        if !cur.is_empty() && cur.len() + c.len() + 1 > 1900 {
            out.push(std::mem::take(&mut cur));
        }
        if !cur.is_empty() {
            cur.push(',');
        }
        cur.push_str(c);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Applies the CISA list to a finding; whether it changed.
pub fn apply_kev(f: &mut Finding, kev: &BTreeMap<String, String>) -> bool {
    let hit = f
        .advisory
        .ids
        .iter()
        .find_map(|i| kev.get(&i.to_ascii_uppercase()));
    let (now_kev, due) = match hit {
        Some(d) => (true, (!d.is_empty()).then(|| d.clone())),
        None => (false, None),
    };
    let changed = now_kev != f.advisory.kev || due != f.advisory.kev_due;
    f.advisory.kev = now_kev;
    f.advisory.kev_due = due;
    changed
}

/// Matches a GitHub advisory against a repository's dependencies.
pub fn match_github_advisory(repo: &str, adv: &Value, deps: &[Dep]) -> Vec<Observation> {
    let mut out = vec![];
    for v in adv["vulnerabilities"].as_array().into_iter().flatten() {
        let eco = osv_ecosystem_of_github(v["package"]["ecosystem"].as_str().unwrap_or(""));
        let name = v["package"]["name"].as_str().unwrap_or("");
        let range = v["vulnerable_version_range"].as_str().unwrap_or("");
        for d in deps {
            if !d.ecosystem.eq_ignore_ascii_case(eco) || !same_package(eco, &d.name, name) {
                continue;
            }
            if !in_range(&d.version, range) {
                continue;
            }
            out.push(Observation {
                repo: repo.to_string(),
                manifest: d.manifest.clone(),
                ecosystem: d.ecosystem.clone(),
                package: d.name.clone(),
                version: d.version.clone(),
                advisory: advisory_from_github(adv, Some(name), "github_advisory"),
                scope: d.scope,
                ..Default::default()
            });
        }
    }
    out
}

/// The OSV `querybatch` body for a repository's dependencies.
pub fn osv_queries(deps: &[Dep]) -> Vec<Value> {
    deps.iter()
        .filter(|d| d.ecosystem != "docker")
        .map(|d| json!({"package": {"name": d.name, "ecosystem": d.ecosystem}, "version": d.version}))
        .collect()
}

/// Whether the repository's dependency list shows the finding fixed: the
/// package is gone, or every copy of it is at or past the fixed version.
pub fn fixed_by_inventory(f: &Finding, deps: &[Dep]) -> bool {
    // Only a list that has the ecosystem at all can say a package is gone
    // (a repository whose dependency graph is off lists only base images).
    if f.ecosystem.is_empty()
        || !deps
            .iter()
            .any(|d| d.ecosystem.eq_ignore_ascii_case(&f.ecosystem))
    {
        return false;
    }
    let copies: Vec<&Dep> = deps
        .iter()
        .filter(|d| {
            d.ecosystem.eq_ignore_ascii_case(&f.ecosystem)
                && same_package(&f.ecosystem, &d.name, &f.package)
        })
        .collect();
    match (&f.fixed_version, copies.is_empty()) {
        (_, true) => true,
        (Some(to), false) => copies
            .iter()
            .all(|d| compare_versions(&d.version, to) != std::cmp::Ordering::Less),
        (None, false) => false,
    }
}

/// The installed version of a package, from the dependency list (the
/// manifest's copy when there is one).
pub fn installed_version(
    deps: &[Dep],
    ecosystem: &str,
    package: &str,
    manifest: &str,
) -> Option<String> {
    let copies: Vec<&Dep> = deps
        .iter()
        .filter(|d| {
            d.ecosystem.eq_ignore_ascii_case(ecosystem) && same_package(ecosystem, &d.name, package)
        })
        .collect();
    copies
        .iter()
        .find(|d| d.manifest == manifest)
        .or_else(|| {
            copies
                .iter()
                .min_by(|a, b| compare_versions(&a.version, &b.version))
        })
        .map(|d| d.version.clone())
}

/// The index of teams with settings: a member is the settings record's own
/// partition key.
pub fn index_spec() -> crate::settings_index::Spec<'static> {
    fn member_of(item: &crate::settings_index::Item) -> Option<String> {
        let sk = item.get("sk")?.as_s().ok()?;
        if sk != CONFIG_SK {
            return None;
        }
        let pk = item.get("pk")?.as_s().ok()?;
        (!pk.is_empty() && !pk.starts_with("INDEX#")).then(|| pk.clone())
    }
    crate::settings_index::Spec {
        name: INDEX_NAME,
        filter: "sk = :sk",
        values: vec![(
            ":sk",
            aws_sdk_dynamodb::types::AttributeValue::S(CONFIG_SK.to_string()),
        )],
        member_of,
    }
}

// ── Groups: one fix, one row ───────────────────────────────────────────

/// The findings one package bump fixes: a repository's package at one
/// version, with every advisory it clears.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Group {
    pub key: String,
    pub repo: String,
    pub ecosystem: String,
    pub package: String,
    pub version: String,
    /// The version that clears every advisory in the group (the highest any
    /// of them needs), when each has one.
    pub fixed_version: Option<String>,
    /// How many advisories have no fixed version yet.
    pub no_fix: usize,
    /// The worst member's tier and the soonest due date.
    pub tier: Option<Tier>,
    pub tier_provisional: bool,
    pub due_at: String,
    /// The group's state: the most active member's.
    pub status: Status,
    pub verdict: Verdict,
    pub scope: Scope,
    pub kev: bool,
    pub malware: bool,
    pub epss: Option<f64>,
    pub manifests: Vec<String>,
    pub pr: Option<PrRef>,
    pub finding_ids: Vec<String>,
    pub advisories: Vec<GroupAdvisory>,
}

/// One advisory in a group, for the expanded row.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GroupAdvisory {
    pub finding_id: String,
    pub primary: String,
    pub ids: Vec<String>,
    pub title: String,
    pub tier: Option<Tier>,
    pub fixed_version: Option<String>,
    pub status: Status,
}

/// The key of a finding's group: repository, ecosystem, package, version.
pub fn group_key(f: &Finding) -> String {
    format!(
        "{}|{}|{}|{}",
        f.repo.to_ascii_lowercase(),
        f.ecosystem.to_ascii_lowercase(),
        if f.ecosystem.eq_ignore_ascii_case("pypi") {
            f.package.to_ascii_lowercase().replace(['_', '.'], "-")
        } else {
            f.package.to_ascii_lowercase()
        },
        f.version
    )
}

fn status_rank(s: Status) -> u8 {
    match s {
        Status::PrOpen => 0,
        Status::Fixing => 1,
        Status::Open => 2,
        Status::NoFix => 3,
        Status::Fixed => 4,
        Status::Dismissed => 5,
        Status::Withdrawn => 6,
    }
}

fn verdict_rank(v: Verdict) -> u8 {
    match v {
        Verdict::Reachable => 0,
        Verdict::Pending => 1,
        Verdict::Unknown => 2,
        Verdict::NotReachable => 3,
        Verdict::NotLoaded => 4,
    }
}

/// The findings as groups, the worst first (tier, then due date, then
/// package). Open members decide an open group's tier and due date.
pub fn groups(findings: &[(Finding, FindingState)]) -> Vec<Group> {
    let mut by: BTreeMap<String, Vec<&Finding>> = BTreeMap::new();
    for (f, _) in findings {
        by.entry(group_key(f)).or_default().push(f);
    }
    let mut out: Vec<Group> = by
        .into_iter()
        .map(|(key, members)| {
            let status = members
                .iter()
                .map(|f| f.status)
                .min_by_key(|s| status_rank(*s))
                .unwrap_or(Status::Open);
            let live: Vec<&&Finding> = members
                .iter()
                .filter(|f| f.status.is_open() == status.is_open())
                .collect();
            let tier = live.iter().filter_map(|f| f.tier).min();
            let due_at = live
                .iter()
                .filter_map(|f| parse_time(&f.due_at).map(|d| (d, f.due_at.clone())))
                .min_by_key(|(d, _)| *d)
                .map(|(_, s)| s)
                .unwrap_or_default();
            let fixes: Vec<Option<&String>> =
                live.iter().map(|f| f.fixed_version.as_ref()).collect();
            let no_fix = fixes.iter().filter(|x| x.is_none()).count();
            let fixed_version = fixes
                .iter()
                .flatten()
                .max_by(|a, b| compare_versions(a, b))
                .map(|s| (*s).clone());
            let first = members[0];
            let mut manifests: Vec<String> = members.iter().map(|f| f.manifest.clone()).collect();
            manifests.sort();
            manifests.dedup();
            let mut advisories: Vec<GroupAdvisory> = members
                .iter()
                .map(|f| GroupAdvisory {
                    finding_id: f.id.clone(),
                    primary: f.advisory.primary.clone(),
                    ids: f.advisory.ids.clone(),
                    title: f.advisory.title.clone(),
                    tier: f.tier,
                    fixed_version: f.fixed_version.clone(),
                    status: f.status,
                })
                .collect();
            advisories.sort_by(|a, b| (a.tier, &a.primary).cmp(&(b.tier, &b.primary)));
            Group {
                key,
                repo: first.repo.clone(),
                ecosystem: first.ecosystem.clone(),
                package: first.package.clone(),
                version: first.version.clone(),
                fixed_version,
                no_fix,
                tier,
                tier_provisional: live
                    .iter()
                    .filter(|f| f.tier == tier)
                    .all(|f| f.tier_provisional),
                due_at,
                status,
                verdict: live
                    .iter()
                    .map(|f| f.verdict)
                    .min_by_key(|v| verdict_rank(*v))
                    .unwrap_or(Verdict::Pending),
                scope: first.scope,
                kev: live.iter().any(|f| f.advisory.kev),
                malware: live.iter().any(|f| f.advisory.malware),
                epss: live
                    .iter()
                    .filter_map(|f| f.advisory.epss)
                    .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)),
                manifests,
                pr: members.iter().find_map(|f| f.pr.clone()),
                finding_ids: members.iter().map(|f| f.id.clone()).collect(),
                advisories,
            }
        })
        .collect();
    out.sort_by(|a, b| {
        (a.tier.unwrap_or(Tier::P3), &a.due_at, &a.package).cmp(&(
            b.tier.unwrap_or(Tier::P3),
            &b.due_at,
            &b.package,
        ))
    });
    out
}

// ── Inventory status ────────────────────────────────────────────────────

/// What reading a repository's dependency list said, in plain words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct InventoryStatus {
    /// `ok`, `graph_off`, `error`.
    pub state: String,
    pub reason: String,
}

/// A failed dependency-graph SBOM read, as a state and a reason with the fix.
pub fn sbom_failure(code: u16, body: &Value) -> InventoryStatus {
    let msg = body["message"].as_str().unwrap_or("").to_string();
    let lower = msg.to_ascii_lowercase();
    if code == 404 || lower.contains("not enabled") || lower.contains("disabled") {
        return InventoryStatus {
            state: "graph_off".into(),
            reason: "GitHub's dependency graph is off for this repo; turn it on in the repo's Settings, Code security.".into(),
        };
    }
    if code == 403 {
        return InventoryStatus {
            state: "error".into(),
            reason: format!("GitHub refused the dependency list (403{}); the app needs Contents: read on this repo.", if msg.is_empty() { String::new() } else { format!(": {msg}") }),
        };
    }
    InventoryStatus {
        state: "error".into(),
        reason: format!(
            "GitHub couldn't build the dependency list ({code}{}); it is tried again on the next check.",
            if msg.is_empty() { String::new() } else { format!(": {msg}") }
        ),
    }
}

/// Packages per ecosystem in a dependency list.
pub fn ecosystems(deps: &[Dep]) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for d in deps {
        *out.entry(d.ecosystem.clone()).or_insert(0) += 1;
    }
    out
}

// ── Dependabot access ───────────────────────────────────────────────────

/// What reading a repository's Dependabot alerts said: `on`, `off` (turned
/// off for the repo), `not_accepted` (the app asks for it, the installation
/// hasn't accepted it yet), `no_permission` (the app doesn't have it), or
/// `error`; with a reason and its fix in plain words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DependabotAccess {
    pub state: String,
    pub reason: String,
}

/// Classifies an answer of `GET /repos/{o}/{r}/dependabot/alerts`.
/// `token_has`: whether the token carries Dependabot alerts (None: unknown).
/// `installation_lacks`: the installation is known not to have granted it.
pub fn dependabot_access(
    code: u16,
    body: &Value,
    token_has: Option<bool>,
    installation_lacks: bool,
) -> DependabotAccess {
    let msg = body["message"].as_str().unwrap_or("").to_ascii_lowercase();
    let a = |state: &str, reason: &str| DependabotAccess {
        state: state.into(),
        reason: reason.into(),
    };
    match code {
        200..=299 => a("on", ""),
        403 | 404 if msg.contains("disabled") || msg.contains("not enabled") => a(
            "off",
            "Dependabot alerts are turned off for this repo; turn them on in the repo's Settings, Code security.",
        ),
        403 | 404 if installation_lacks || token_has == Some(false) => a(
            "not_accepted",
            "The GitHub App asks for Dependabot alerts: read, but the organization hasn't accepted it yet; an owner accepts it in the organization's Settings, GitHub Apps, then Review request.",
        ),
        403 if msg.contains("not accessible by integration") => a(
            "no_permission",
            "The GitHub App doesn't have Dependabot alerts: read; add it on the app's Permissions and events page, then accept it on the installation.",
        ),
        403 => a(
            "no_permission",
            "GitHub refused Dependabot's alerts for this repo; the app needs Dependabot alerts: read.",
        ),
        404 => a(
            "off",
            "GitHub didn't find Dependabot alerts for this repo; turn on the dependency graph and Dependabot alerts in the repo's Settings, Code security.",
        ),
        _ => a("error", "GitHub didn't answer for Dependabot's alerts; it is tried again on the next check."),
    }
}

/// The team's Dependabot state from each repository's: the one that needs a
/// person's action first.
pub fn team_dependabot<'a>(
    per_repo: impl IntoIterator<Item = &'a DependabotAccess>,
) -> Option<DependabotAccess> {
    let rank = |s: &str| match s {
        "not_accepted" => 0,
        "no_permission" => 1,
        "error" => 2,
        "off" => 3,
        _ => 4,
    };
    per_repo.into_iter().min_by_key(|a| rank(&a.state)).cloned()
}

// ── Triage without the model ────────────────────────────────────────────

/// A verdict that needs no model call: dev-only, withdrawn, or no code line
/// names the package or the advisory's functions (the model would only be
/// reading nothing). None: ask the model.
pub fn settle_without_model(
    f: &Finding,
    st: &FindingState,
    hits: usize,
    files: usize,
    searched: &[String],
) -> Option<(Verdict, Evidence)> {
    let checked = format!(
        "Searched the code for {}.",
        searched
            .iter()
            .map(|t| format!("`{t}`"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    if f.scope == Scope::Dev {
        return Some((
            Verdict::NotLoaded,
            Evidence {
                summary: "A development dependency only (the lockfile or Dependabot says so), so it isn't loaded at runtime.".into(),
                lines: vec![],
                checked: "The dependency's scope.".into(),
                confidence: "high".into(),
            },
        ));
    }
    if f.status == Status::Withdrawn {
        return Some((
            Verdict::Unknown,
            Evidence {
                summary: "The advisory was withdrawn, so it isn't checked.".into(),
                lines: vec![],
                checked: "The advisory's status.".into(),
                confidence: "high".into(),
            },
        ));
    }
    if hits == 0 {
        let why = if st.symbols.is_empty() {
            format!(
                "No file in the repo's code names {}, so it comes in through another package, and the advisory names no functions to look for.",
                f.package
            )
        } else {
            format!(
                "No file in the repo's code names {} or the functions the advisory names, so it comes in through another package; whether that package calls the vulnerable code isn't known.",
                f.package
            )
        };
        return Some((
            Verdict::Unknown,
            Evidence {
                summary: why,
                lines: vec![],
                checked,
                confidence: "low".into(),
            },
        ));
    }
    let _ = files;
    None
}

/// The reason an "unknown" verdict from the model carries, from what the
/// search found.
pub fn unknown_reason(package: &str, files: usize, symbols: &[String]) -> String {
    let where_ = match files {
        0 => format!("{package} isn't named in the repo's code"),
        1 => format!("{package} is imported in 1 file"),
        n => format!("{package} is imported in {n} files"),
    };
    if symbols.is_empty() {
        format!("{where_}, but the advisory names no functions to look for.")
    } else {
        format!(
            "{where_}, but no line shows whether {} is called with untrusted input.",
            symbols
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// The triage key of a group: what the verdict depends on (the advisories,
/// the version, the scope and the code lines that name the package). The
/// same key again reuses the verdict without a model call.
pub fn group_triage_key(members: &[&Finding], hits: &[EvidenceLine]) -> String {
    let mut ids: Vec<String> = members
        .iter()
        .flat_map(|f| f.advisory.ids.iter().cloned())
        .collect();
    ids.sort();
    ids.dedup();
    let lines: Vec<String> = hits
        .iter()
        .map(|h| format!("{}:{}", h.file, h.text))
        .collect();
    crate::content_hash(&format!(
        "{}|{}|{:?}|{}",
        ids.join(","),
        members.first().map(|f| f.version.as_str()).unwrap_or(""),
        members.first().map(|f| f.scope).unwrap_or_default(),
        lines.join("\n")
    ))
}

// ── Insights ────────────────────────────────────────────────────────────

/// Analytics' security section: findings opened and fixed per day, fix pull
/// requests opened and merged per day, the median hours to fix per tier,
/// and the model tokens spent per day (`spend`: day `YYYY-MM-DD` to tokens).
pub fn insights(
    findings: &[(Finding, FindingState)],
    spend: &BTreeMap<String, u64>,
    now: DateTime<Utc>,
    days: i64,
) -> Value {
    let start = (now - Duration::days(days - 1)).date_naive();
    let day_of = |s: &str| parse_time(s).map(|t| t.date_naive());
    let mut series: BTreeMap<NaiveDate, [u64; 5]> = BTreeMap::new();
    let mut d = start;
    while d <= now.date_naive() {
        series.insert(d, [0; 5]);
        d += Duration::days(1);
    }
    let mut bump = |day: Option<NaiveDate>, i: usize, n: u64| {
        if let Some(e) = day.and_then(|d| series.get_mut(&d)) {
            e[i] += n;
        }
    };
    let mut prs_seen: HashSet<(String, u64)> = HashSet::new();
    let mut hours: BTreeMap<Tier, Vec<f64>> = BTreeMap::new();
    for (f, st) in findings {
        bump(day_of(&f.first_seen_at), 0, 1);
        if f.status == Status::Fixed {
            let at = st.merged_at.clone().unwrap_or_else(|| f.updated_at.clone());
            bump(day_of(&at), 1, 1);
            if let (Some(t), Some(m), Some(s)) =
                (f.tier, parse_time(&at), parse_time(&f.first_seen_at))
            {
                if m >= s && day_of(&at).is_some_and(|d| d >= start) {
                    hours
                        .entry(t)
                        .or_default()
                        .push((m - s).num_minutes() as f64 / 60.0);
                }
            }
        }
        if let Some(pr) = &f.pr {
            if prs_seen.insert((f.repo.to_ascii_lowercase(), pr.number)) {
                if let Some(o) = st.pr_opened_at.as_deref() {
                    bump(day_of(o), 2, 1);
                }
                if pr.state == "merged" {
                    if let Some(m) = st.merged_at.as_deref() {
                        bump(day_of(m), 3, 1);
                    }
                }
            }
        }
    }
    for (day, tokens) in spend {
        if let Ok(d) = NaiveDate::parse_from_str(day, "%Y-%m-%d") {
            bump(Some(d), 4, *tokens);
        }
    }
    let median = |v: &mut Vec<f64>| -> Value {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        match v.len() {
            0 => Value::Null,
            n if n % 2 == 1 => json!((v[n / 2] * 10.0).round() / 10.0),
            n => json!(((v[n / 2 - 1] + v[n / 2]) / 2.0 * 10.0).round() / 10.0),
        }
    };
    let mut by_tier = serde_json::Map::new();
    for t in Tier::ALL {
        let mut v = hours.remove(&t).unwrap_or_default();
        by_tier.insert(
            t.as_str().into(),
            json!({ "fixed": v.len(), "median_hours": median(&mut v) }),
        );
    }
    let month = now.format("%Y-%m").to_string();
    let rows: Vec<Value> = series
        .iter()
        .map(|(d, v)| json!({
            "date": d.format("%Y-%m-%d").to_string(),
            "opened": v[0], "fixed": v[1], "prs_opened": v[2], "prs_merged": v[3], "tokens": v[4],
        }))
        .collect();
    let sum = |i: usize| series.values().map(|v| v[i]).sum::<u64>();
    json!({
        "days": rows,
        "totals": {
            "opened": sum(0), "fixed": sum(1), "prs_opened": sum(2), "prs_merged": sum(3), "tokens": sum(4),
            "tokens_month": spend.iter().filter(|(d, _)| d.starts_with(&month)).map(|(_, t)| *t).sum::<u64>(),
        },
        "median_hours_to_fix": Value::Object(by_tier),
    })
}

// ── Triage ──────────────────────────────────────────────────────────────

/// The agent's instructions for a reachability verdict.
pub const TRIAGE_SYSTEM: &str = "You decide whether a repository is affected by a published vulnerability in one of its dependencies. \
You get the advisory, the package and the code search results from the repository (file, line, text). \
Answer with one JSON object and nothing else: {\"verdict\": \"reachable\" | \"not_reachable\" | \"unknown\", \"summary\": \"two sentences at most\", \"lines\": [{\"file\": \"...\", \"line\": 1}], \"confidence\": \"high\" | \"medium\" | \"low\"}. \
reachable: the repository's own code calls the vulnerable function or turns on the vulnerable feature, and the lines show it. \
not_reachable: the package is used, but nothing shown calls the vulnerable part. \
unknown: the evidence is missing or the use is dynamic. Never guess reachable or not_reachable without lines that show it. \
Only cite lines from the search results given.";

/// The question for one finding, bounded to `max_chars`.
pub fn triage_prompt(
    f: &Finding,
    st: &FindingState,
    hits: &[EvidenceLine],
    max_chars: usize,
) -> String {
    let mut s = format!(
        "Advisory: {} ({})\nTitle: {}\nSummary: {}\nPackage: {} {} ({}, {}, {})\nFixed in: {}\nVulnerable functions named by the advisory: {}\n\nCode search results:\n",
        f.advisory.primary,
        f.advisory.ids.join(", "),
        f.advisory.title,
        crate::truncate_str(&f.advisory.summary, 1500),
        f.package,
        f.version,
        f.ecosystem,
        match f.relation { Relation::Direct => "direct", Relation::Transitive => "transitive", Relation::Unknown => "relation unknown" },
        match f.scope { Scope::Runtime => "runtime", Scope::Dev => "dev", Scope::Unknown => "scope unknown" },
        f.fixed_version.as_deref().unwrap_or("none yet"),
        if st.symbols.is_empty() { "none".to_string() } else { st.symbols.join(", ") },
    );
    if hits.is_empty() {
        s.push_str("(no matches in the repository's code)\n");
    }
    for h in hits {
        let line = format!(
            "{}:{}: {}\n",
            h.file,
            h.line,
            crate::truncate_str(h.text.trim(), 200)
        );
        if s.len() + line.len() > max_chars {
            break;
        }
        s.push_str(&line);
    }
    s
}

/// Reads the agent's verdict. Lines it cites that aren't among the search
/// results are dropped; a reachable or not-reachable verdict with nothing to
/// show for it becomes unknown.
pub fn parse_triage(answer: &str, hits: &[EvidenceLine]) -> (Verdict, Evidence) {
    let json_text = answer
        .find('{')
        .and_then(|a| answer.rfind('}').map(|b| &answer[a..=b]))
        .unwrap_or("");
    let v: Value = serde_json::from_str(json_text).unwrap_or(Value::Null);
    let mut verdict = match v["verdict"].as_str() {
        Some("reachable") => Verdict::Reachable,
        Some("not_reachable") => Verdict::NotReachable,
        _ => Verdict::Unknown,
    };
    let lines: Vec<EvidenceLine> = v["lines"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| {
            let file = l["file"].as_str()?;
            let line = l["line"].as_u64()?;
            hits.iter()
                .find(|h| h.file == file && h.line == line)
                .cloned()
        })
        .take(10)
        .collect();
    if verdict == Verdict::Reachable && lines.is_empty() {
        verdict = Verdict::Unknown;
    }
    let confidence = match v["confidence"].as_str() {
        Some(c @ ("high" | "medium" | "low")) if verdict != Verdict::Unknown => c.to_string(),
        _ => "low".to_string(),
    };
    let summary = v["summary"]
        .as_str()
        .map(|s| crate::truncate_str(s.trim(), 600).to_string())
        .unwrap_or_else(|| "The agent gave no verdict.".to_string());
    (
        verdict,
        Evidence {
            summary,
            lines: if lines.is_empty() {
                hits.iter().take(5).cloned().collect()
            } else {
                lines
            },
            checked: String::new(),
            confidence,
        },
    )
}

/// The search terms for a finding: the package as code imports it, then up
/// to three named functions (their last part: `Parser.onHeadersComplete`
/// is searched as `onHeadersComplete`).
pub fn search_terms(f: &Finding, st: &FindingState) -> Vec<String> {
    let pkg = match f.ecosystem.as_str() {
        "Maven" => f
            .package
            .rsplit(':')
            .next()
            .unwrap_or(&f.package)
            .to_string(),
        "Go" => f.package.clone(),
        _ => f.package.clone(),
    };
    let mut out = vec![pkg];
    for sym in &st.symbols {
        let last = sym
            .rsplit(['.', ':', ')'])
            .find(|p| !p.trim().is_empty())
            .unwrap_or(sym)
            .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .to_string();
        if last.len() >= 3 && !out.contains(&last) {
            out.push(last);
        }
        if out.len() >= 4 {
            break;
        }
    }
    out
}

// ── The overview ────────────────────────────────────────────────────────

/// The overview's numbers, from every finding.
pub fn overview_counts(findings: &[(Finding, FindingState)], now: DateTime<Utc>) -> Value {
    let open: Vec<&Finding> = findings
        .iter()
        .map(|(f, _)| f)
        .filter(|f| f.status.is_open())
        .collect();
    let tier = |t: Tier| open.iter().filter(|f| f.tier == Some(t)).count();
    let week = now + Duration::days(7);
    let month_ago = now - Duration::days(30);
    let due_7d = open
        .iter()
        .filter(|f| parse_time(&f.due_at).is_some_and(|d| d <= week))
        .count();
    let fixed_recent: Vec<&(Finding, FindingState)> = findings
        .iter()
        .filter(|(f, _)| {
            f.status == Status::Fixed && parse_time(&f.updated_at).is_some_and(|t| t >= month_ago)
        })
        .collect();
    let mut hours: Vec<f64> = fixed_recent
        .iter()
        .filter_map(|(f, st)| {
            let m = parse_time(st.merged_at.as_deref()?)?;
            let s = parse_time(&f.first_seen_at)?;
            Some((m - s).num_minutes() as f64 / 60.0)
        })
        .collect();
    hours.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = match hours.len() {
        0 => Value::Null,
        n if n % 2 == 1 => json!((hours[n / 2] * 10.0).round() / 10.0),
        n => json!(((hours[n / 2 - 1] + hours[n / 2]) / 2.0 * 10.0).round() / 10.0),
    };
    let next_due = open
        .iter()
        .filter_map(|f| parse_time(&f.due_at))
        .min()
        .map(|d| d.to_rfc3339());
    let noise = findings
        .iter()
        .filter(|(f, _)| matches!(f.verdict, Verdict::NotReachable | Verdict::NotLoaded))
        .count();
    let open_groups: Vec<Group> = groups(findings)
        .into_iter()
        .filter(|g| g.status.is_open())
        .collect();
    let gtier = |t: Tier| open_groups.iter().filter(|g| g.tier == Some(t)).count();
    let settled = open
        .iter()
        .filter(|f| !matches!(f.verdict, Verdict::Pending))
        .count();
    json!({
        "groups": {
            "open": open_groups.len(),
            "p0": gtier(Tier::P0),
            "p1": gtier(Tier::P1),
            "p2": gtier(Tier::P2),
            "p3": gtier(Tier::P3),
            "due_7d": open_groups
                .iter()
                .filter(|g| parse_time(&g.due_at).is_some_and(|d| d <= week))
                .count(),
        },
        "verdicts": {
            "settled": settled,
            "checking": open.len() - settled,
        },
        "counts": {
            "open": open.len(),
            "p0": tier(Tier::P0),
            "p1": tier(Tier::P1),
            "p2": tier(Tier::P2),
            "p3": tier(Tier::P3),
            "due_7d": due_7d,
            "fixed_30d": fixed_recent.len(),
            "dismissed": findings.iter().filter(|(f, _)| f.status == Status::Dismissed).count(),
            "noise_removed": noise,
        },
        "median_hours_to_merge_30d": median,
        "next_due_at": next_due,
    })
}

/// What a dry run would do now: the pull requests it would open and the
/// cards it would post.
pub fn preview(
    findings: &[(Finding, FindingState)],
    cfg: &SecurityConfig,
    now: DateTime<Utc>,
) -> Value {
    let units = plan_units(findings, cfg, now);
    let would_open: Vec<Value> = units
        .iter()
        .flat_map(|u| {
            u.finding_ids.iter().map(move |id| {
                json!({"finding_id": id, "repo": u.repo, "title": u.title, "tier": u.tier.map(Tier::as_str), "branch": u.branch, "labels": u.labels})
            })
        })
        .collect();
    let would_post: Vec<Value> = findings
        .iter()
        .filter(|(f, st)| {
            f.status.is_open()
                && posts(f, cfg)
                && cfg.repos.includes(&f.repo)
                && cfg.ignored(f, now).is_none()
                && !st.cards.iter().any(|c| c == "new")
        })
        .map(|(f, _)| {
            json!({"finding_id": f.id, "repo": f.repo, "title": format!("{} security · {} is affected by {}", f.tier.map(Tier::as_str).unwrap_or(""), f.repo, f.advisory.primary), "tier": f.tier.map(Tier::as_str)})
        })
        .collect();
    json!({ "would_open": would_open, "would_post": would_post })
}

/// The withdrawn ids' findings: what to do with each. A pull request that
/// existed only for withdrawn findings is closed; a merged one is left.
pub fn prs_to_close_on_withdraw(findings: &[(Finding, FindingState)]) -> Vec<(String, u64)> {
    let mut by_pr: BTreeMap<(String, u64), Vec<&Finding>> = BTreeMap::new();
    for (f, _) in findings {
        if let Some(p) = &f.pr {
            if p.state == "open" && !p.adopted {
                by_pr
                    .entry((f.repo.to_ascii_lowercase(), p.number))
                    .or_default()
                    .push(f);
            }
        }
    }
    by_pr
        .into_iter()
        .filter(|(_, fs)| fs.iter().all(|f| f.status == Status::Withdrawn))
        .map(|(k, _)| k)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_private_report_is_a_finding_that_never_goes_public() {
        let adv = serde_json::json!({
            "ghsa_id": "GHSA-aaaa-bbbb-cccc", "cve_id": null, "state": "triage",
            "summary": "Path traversal in the upload handler", "severity": "high",
            "vulnerabilities": [{"package": {"ecosystem": "npm", "name": "web"},
                                 "vulnerable_version_range": "< 2.0.0",
                                 "patched_versions": null,
                                 "vulnerable_functions": ["saveUpload"]}],
            "html_url": "https://github.com/acme/web/security/advisories/GHSA-aaaa-bbbb-cccc"
        });
        let o = observation_from_repo_advisory("acme/web", &adv).unwrap();
        assert!(o.private_report);
        assert_eq!(o.ecosystem, OWN_CODE);
        assert_eq!(o.manifest, OWN_CODE_MANIFEST);
        assert_eq!(o.advisory.symbols, ["saveUpload"]);
        let (mut f, st, created) = apply(None, &o, Utc::now());
        assert!(created && f.private_report);
        f.tier = Some(Tier::P0);
        f.verdict = Verdict::Reachable;
        let cfg = SecurityConfig::default();
        assert!(
            !posts(&f, &cfg),
            "a private report is never posted to a channel"
        );
        f.version = "1.0.0".into();
        f.fixed_version = Some("2.0.0".into());
        assert!(
            !fixable(&f, &st, &cfg, Utc::now()),
            "never fixed in a public pull request"
        );
        // Inventory never calls own code fixed.
        assert!(!fixed_by_inventory(&f, &[]));
    }

    #[test]
    fn the_page_says_when_own_advisories_cant_be_read() {
        let v = repo_advisories_source("no_permission");
        assert_eq!(v["state"], "no_permission");
        assert!(v["note"]
            .as_str()
            .unwrap()
            .contains("add Repository security advisories: read"));
        assert!(repo_advisories_source("ok")["note"].is_null());
        assert_eq!(repo_advisories_source("")["state"], "unread");
    }

    #[test]
    fn a_published_or_closed_advisory_is_not_private() {
        let published = serde_json::json!({"ghsa_id": "GHSA-1111-2222-3333", "state": "published"});
        assert!(
            !observation_from_repo_advisory("acme/web", &published)
                .unwrap()
                .private_report
        );
        let closed = serde_json::json!({"ghsa_id": "GHSA-1111-2222-3333", "state": "closed"});
        let o = observation_from_repo_advisory("acme/web", &closed).unwrap();
        assert!(o.advisory.withdrawn);
        assert!(
            observation_from_repo_advisory("acme/web", &serde_json::json!({"state": "draft"}))
                .is_none()
        );
        // Publishing a report makes the finding public.
        let draft = serde_json::json!({"ghsa_id": "GHSA-1111-2222-3333", "state": "draft"});
        let (f, st, _) = apply(
            None,
            &observation_from_repo_advisory("acme/web", &draft).unwrap(),
            Utc::now(),
        );
        assert!(f.private_report);
        let (f, _, _) = apply(
            Some((f, st)),
            &observation_from_repo_advisory("acme/web", &published).unwrap(),
            Utc::now(),
        );
        assert!(!f.private_report);
    }

    fn t(s: &str) -> DateTime<Utc> {
        parse_time(s).unwrap()
    }

    fn finding(id: &str) -> Finding {
        Finding {
            id: id.into(),
            repo: "acme/web".into(),
            manifest: "pnpm-lock.yaml".into(),
            ecosystem: "npm".into(),
            package: "undici".into(),
            version: "6.19.2".into(),
            fixed_version: Some("6.21.1".into()),
            advisory: Advisory {
                ids: vec!["CVE-2026-31337".into(), "GHSA-q9fx-2w5c-8m4r".into()],
                primary: "CVE-2026-31337".into(),
                title: "Request smuggling in HTTP parser".into(),
                cvss: Some(9.1),
                ..Default::default()
            },
            scope: Scope::Runtime,
            verdict: Verdict::Reachable,
            first_seen_at: "2026-11-10T10:00:00Z".into(),
            ..Default::default()
        }
    }

    #[test]
    fn aliases_union_into_one_cluster_transitively() {
        let c = clusters(&[
            vec!["GHSA-aaaa-bbbb-cccc".into(), "CVE-2026-1".into()],
            vec!["GO-2026-0001".into(), "cve-2026-1".into()],
            vec!["PYSEC-2026-9".into()],
            vec!["RUSTSEC-2026-0002".into(), "GO-2026-0001".into()],
        ]);
        assert_eq!(c.len(), 2);
        let big = c.iter().find(|g| g.len() == 4).unwrap();
        assert!(big.contains(&"CVE-2026-1".to_string()));
        assert!(big.contains(&"RUSTSEC-2026-0002".to_string()));
        assert_eq!(primary_id(big), "CVE-2026-1");
        assert_eq!(primary_id(&["GHSA-zz".into(), "GO-1".into()]), "GHSA-ZZ");
    }

    #[test]
    fn the_same_flaw_from_two_sources_is_one_finding() {
        let now = t("2026-11-10T10:00:00Z");
        let dependabot = Observation {
            repo: "acme/web".into(),
            manifest: "pnpm-lock.yaml".into(),
            ecosystem: "npm".into(),
            package: "undici".into(),
            advisory: AdvisoryRecord {
                ids: vec!["GHSA-q9fx-2w5c-8m4r".into(), "CVE-2026-31337".into()],
                source: "dependabot".into(),
                fixed: vec!["6.21.1".into()],
                ..Default::default()
            },
            ..Default::default()
        };
        let (f, st, created) = apply(None, &dependabot, now);
        assert!(created);
        // OSV names it by another alias, through the dependency graph.
        let osv_ids = vec!["GHSA-Q9FX-2W5C-8M4R".to_string(), "PYSEC-X".to_string()];
        let existing = vec![f.clone()];
        let target =
            merge_target(&existing, &osv_ids, "acme/web", SBOM_MANIFEST, "undici").unwrap();
        assert_eq!(target.id, f.id);
        let osv = Observation {
            manifest: SBOM_MANIFEST.into(),
            version: "6.19.2".into(),
            advisory: AdvisoryRecord {
                ids: osv_ids,
                source: "osv".into(),
                ..Default::default()
            },
            ..dependabot.clone()
        };
        let (f2, _, created) = apply(Some((f.clone(), st)), &osv, now);
        assert!(!created);
        assert_eq!(f2.id, f.id);
        assert_eq!(f2.manifest, "pnpm-lock.yaml");
        assert_eq!(f2.advisory.primary, "CVE-2026-31337");
        assert_eq!(f2.advisory.ids.len(), 3);
        assert_eq!(f2.fixed_version.as_deref(), Some("6.21.1"));
        // Another package, or no id in common: a different finding.
        assert!(merge_target(
            &existing,
            &["CVE-2026-9".into()],
            "acme/web",
            "pnpm-lock.yaml",
            "undici"
        )
        .is_none());
        assert!(merge_target(
            &existing,
            &["CVE-2026-31337".into()],
            "acme/web",
            "pnpm-lock.yaml",
            "tar"
        )
        .is_none());
        // The id is stable whatever order the aliases come in.
        assert_eq!(
            finding_id(&["GHSA-1".into(), "CVE-1".into()], "Acme/Web", "a", "X"),
            finding_id(&["cve-1".into()], "acme/web", "a", "x")
        );
    }

    #[test]
    fn versions_compare_across_schemes() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_versions("6.19.2", "6.21.1"), Less);
        assert_eq!(compare_versions("v1.10.0", "1.9.9"), Greater);
        assert_eq!(compare_versions("1.0.0-rc.1", "1.0.0"), Less);
        assert_eq!(compare_versions("1.0", "1.0.0"), Equal);
        assert_eq!(compare_versions("2.0.0b1", "2.0.0"), Less);
        assert_eq!(compare_versions("1.0.0.post1", "1.0.0"), Greater);
        assert!(in_range("6.19.2", ">= 6.0.0, < 6.21.1"));
        assert!(!in_range("6.21.1", ">= 6.0.0, < 6.21.1"));
        assert!(in_range("1.2", "= 1.2"));
        assert!(!in_range("1.2", ""));
        assert_eq!(
            pick_fixed(
                "1.0.0",
                &[vec!["1.0.5".into(), "2.0.1".into()], vec!["1.1.0".into()]]
            )
            .as_deref(),
            Some("1.1.0")
        );
        assert_eq!(pick_fixed("3.0.0", &[vec!["1.0.5".into()]]), None);
        assert!(is_major_bump("6.19.2", "7.0.0"));
        assert!(!is_major_bump("6.19.2", "6.21.1"));
        assert!(is_major_bump("0.3.1", "0.4.0"));
    }

    #[test]
    fn osv_ranges_and_records_are_read() {
        let v = json!({
            "id": "GHSA-q9fx-2w5c-8m4r", "aliases": ["CVE-2026-31337"], "summary": "Smuggling",
            "affected": [{"package": {"ecosystem": "npm", "name": "undici"},
                "ranges": [{"type": "SEMVER", "events": [{"introduced": "0"}, {"fixed": "6.21.1"}]}],
                "database_specific": {"vulnerable_functions": ["Parser.onHeadersComplete"]}}],
            "database_specific": {"severity": "CRITICAL", "cwe_ids": ["CWE-444"]}
        });
        let a = &v["affected"][0];
        assert!(osv_affects(a, "6.19.2"));
        assert!(!osv_affects(a, "6.21.1"));
        let r = advisory_from_osv(&v, "npm", "undici").unwrap();
        assert_eq!(r.fixed, vec!["6.21.1"]);
        assert_eq!(r.symbols, vec!["Parser.onHeadersComplete"]);
        assert_eq!(r.cwe, vec!["CWE-444"]);
        assert!(advisory_from_osv(&v, "npm", "tar").is_none());
        let last = json!({"ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "1.0"}, {"last_affected": "1.4"}]}]});
        assert!(osv_affects(&last, "1.4"));
        assert!(!osv_affects(&last, "1.5"));
        assert!(!osv_affects(&last, "0.9"));
        let cg = json!({"id": "CGA-xxxx", "affected": [{"package": {"ecosystem": "npm", "name": "undici"}}]});
        assert!(advisory_from_osv(&cg, "npm", "undici").is_none());
    }

    #[test]
    fn tiers_follow_the_policy() {
        let st = FindingState::default();
        let mut f = finding("a");
        f.verdict = Verdict::Reachable;
        f.advisory.cvss = Some(9.8);
        assert_eq!(tier_of(&f, &st), Tier::P0, "CVSS 9+ and reachable");
        f.advisory.cvss = Some(7.5);
        assert_eq!(tier_of(&f, &st), Tier::P1, "CVSS 7+ and reachable");
        f.verdict = Verdict::NotReachable;
        assert_eq!(tier_of(&f, &st), Tier::P3, "high, not reachable");
        f.advisory.epss = Some(0.23);
        assert_eq!(
            tier_of(&f, &st),
            Tier::P2,
            "likely exploited, not reachable"
        );
        f.advisory.epss = None;
        f.verdict = Verdict::Reachable;
        f.advisory.kev = true;
        assert_eq!(tier_of(&f, &st), Tier::P0, "exploited and loaded");
        f.scope = Scope::Dev;
        assert_eq!(tier_of(&f, &st), Tier::P3, "dev only: lowest, no agent");
        f.advisory.malware = true;
        assert_eq!(tier_of(&f, &st), Tier::P0, "malware anywhere");
        let mut g = finding("b");
        g.verdict = Verdict::Reachable;
        g.advisory.cvss = Some(5.0);
        assert_eq!(tier_of(&g, &st), Tier::P2, "reachable medium");
        g.verdict = Verdict::NotReachable;
        assert_eq!(tier_of(&g, &st), Tier::P3, "medium, not reachable");
        // A severity word stands in for a missing score.
        g.advisory.cvss = None;
        let high = FindingState {
            severity: Some("high".into()),
            ..Default::default()
        };
        g.verdict = Verdict::Reachable;
        assert_eq!(tier_of(&g, &high), Tier::P1);
    }

    /// An unknown or still-checking verdict is no evidence: a critical score
    /// alone never makes it P0, and a missing score is never treated as high.
    #[test]
    fn an_unknown_verdict_is_never_p0_on_its_score_alone() {
        let st = FindingState::default();
        let crit = FindingState {
            severity: Some("critical".into()),
            ..Default::default()
        };
        for v in [Verdict::Unknown, Verdict::Pending] {
            let mut f = finding("u");
            f.verdict = v;
            f.advisory.cvss = Some(9.8);
            f.advisory.epss = Some(0.01);
            f.scope = Scope::Unknown;
            assert_eq!(tier_of(&f, &st), Tier::P2, "{v:?}: critical, scope unknown");
            f.scope = Scope::Runtime;
            assert_eq!(tier_of(&f, &st), Tier::P1, "{v:?}: critical, runtime");
            f.advisory.cvss = None;
            assert_eq!(
                tier_of(&f, &crit),
                Tier::P1,
                "{v:?}: critical word, runtime"
            );
            assert_eq!(
                tier_of(&f, &st),
                Tier::P3,
                "{v:?}: no score is not a high score"
            );
            f.advisory.kev = true;
            assert_eq!(
                tier_of(&f, &st),
                Tier::P0,
                "{v:?}: exploited and a runtime dependency"
            );
            f.scope = Scope::Unknown;
            assert_eq!(
                tier_of(&f, &st),
                Tier::P1,
                "{v:?}: exploited, scope unknown"
            );
        }
        let mut f = finding("p");
        f.verdict = Verdict::Pending;
        f.advisory.cvss = Some(9.8);
        retier(&mut f, &st, &SecurityConfig::default());
        assert!(
            f.tier_provisional,
            "a pending verdict's tier is provisional"
        );
        f.verdict = Verdict::Reachable;
        retier(&mut f, &st, &SecurityConfig::default());
        assert!(!f.tier_provisional);
        assert_eq!(f.tier, Some(Tier::P0));
    }

    #[test]
    fn due_dates_use_the_tier_and_the_exploited_list() {
        let cfg = SecurityConfig::default();
        let mut f = finding("a");
        assert_eq!(due_at(&f, Tier::P0, &cfg), t("2026-11-13T10:00:00Z"));
        assert_eq!(due_at(&f, Tier::P2, &cfg), t("2026-12-10T10:00:00Z"));
        f.advisory.kev_due = Some("2026-11-11".into());
        assert_eq!(due_at(&f, Tier::P0, &cfg), t("2026-11-11T23:59:59Z"));
        let later = SecurityConfig {
            use_kev_due: false,
            ..Default::default()
        };
        assert_eq!(due_at(&f, Tier::P0, &later), t("2026-11-13T10:00:00Z"));
    }

    #[test]
    fn the_label_set() {
        let cfg = SecurityConfig::default();
        let mut f = finding("a");
        f.advisory.kev = true;
        let st = FindingState::default();
        assert_eq!(
            labels(&cfg, &[(&f, &st)], true),
            vec![
                "security",
                "dependencies",
                "severity:critical",
                "reachable",
                "exploited",
                "breaking",
                "javascript"
            ]
        );
        f.advisory.kev = false;
        f.verdict = Verdict::NotLoaded;
        f.advisory.cvss = Some(5.0);
        assert_eq!(
            labels(&cfg, &[(&f, &st)], false),
            vec![
                "security",
                "dependencies",
                "severity:medium",
                "not-reachable",
                "javascript"
            ]
        );
        f.verdict = Verdict::Unknown;
        let off = SecurityConfig {
            severity_labels: false,
            ecosystem_labels: false,
            labels: vec!["sec".into()],
            ..Default::default()
        };
        assert_eq!(
            labels(&off, &[(&f, &st)], false),
            vec!["sec", "reachability:unknown"]
        );
        let none = SecurityConfig {
            reachability_labels: false,
            ..off
        };
        assert_eq!(labels(&none, &[(&f, &st)], false), vec!["sec"]);
    }

    #[test]
    fn the_title_and_branch_format() {
        let unit = FixUnit {
            repo: "acme/web".into(),
            ecosystem: "npm".into(),
            bumps: vec![Bump {
                package: "undici".into(),
                from: "6.19.2".into(),
                to: "6.21.1".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            pr_title("fix(deps)", &unit, &["CVE-2026-31337".into()]),
            "fix(deps): bump undici 6.19.2 to 6.21.1 [SECURITY] (CVE-2026-31337)"
        );
        let now = t("2026-11-10T10:00:00Z");
        assert_eq!(
            branch(&unit, Some(Cadence::Now), now),
            "coderhelm/security-undici-6.21.1"
        );
        let scoped = FixUnit {
            bumps: vec![Bump {
                package: "@types/node".into(),
                from: "1".into(),
                to: "2.0.1".into(),
                ..Default::default()
            }],
            ..unit.clone()
        };
        assert_eq!(
            branch(&scoped, None, now),
            "coderhelm/security-types-node-2.0.1"
        );
        let group = FixUnit {
            grouped: true,
            bumps: vec![Bump::default(), Bump::default(), Bump::default()],
            ..unit.clone()
        };
        assert_eq!(
            pr_title("", &group, &["CVE-1".into(), "CVE-2".into()]),
            "fix(deps): bump 3 npm dependencies [SECURITY] (CVE-1, +1 more)"
        );
        assert_eq!(
            branch(&group, Some(Cadence::Weekly), now),
            "coderhelm/security-npm-weekly-2026-w46"
        );
        let body = ticket_body(
            &FixUnit {
                branch: "coderhelm/security-undici-6.21.1".into(),
                ..unit
            },
            &[],
            &SecurityConfig::default(),
        );
        assert_eq!(
            branch_in_body("SEC-ABC", &body).as_deref(),
            Some("coderhelm/security-undici-6.21.1")
        );
        assert_eq!(branch_in_body("ALERT-1", &body), None);
        assert!(!body.contains('→') && !body.to_lowercase().contains("phase"));
    }

    #[test]
    fn the_pr_body_has_its_sections_and_credit() {
        let mut f = finding("a");
        f.dependabot_alert = Some(AlertRef {
            number: 88,
            url: "https://github.com/acme/web/security/dependabot/88".into(),
        });
        f.tier = Some(Tier::P0);
        f.due_at = "2026-11-12T18:00:00Z".into();
        f.evidence = Some(Evidence {
            summary: "Called on every proxied request.".into(),
            lines: vec![EvidenceLine {
                file: "src/proxy.ts".into(),
                line: 44,
                text: "fetch(upstream)".into(),
            }],
            checked: "3 symbols, 2 call sites.".into(),
            confidence: "high".into(),
        });
        let st = FindingState::default();
        let unit = FixUnit {
            bumps: vec![Bump {
                package: "undici".into(),
                from: "6.19.2".into(),
                to: "6.21.1".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let body = pr_body(&unit, &[(&f, &st)], "Overrides undici in package.json.");
        for h in [
            "## Summary",
            "## Advisory",
            "## Affected",
            "## Reachability",
            "## Change",
            "## Proof",
            "## Closes",
        ] {
            assert!(body.contains(h), "{h}");
        }
        assert!(body.contains(ATTRIBUTION));
        assert!(body.contains("`src/proxy.ts:44`"));
        assert!(body.contains("Dependabot alert [#88]"));
        assert!(body.contains("P0, due Nov 12"));
        assert!(!body.contains('→') && !body.to_lowercase().contains("phase"));
    }

    #[test]
    fn units_group_by_tier_cadence_and_skip_what_cannot_open() {
        let cfg = SecurityConfig {
            enabled: true,
            dry_run: false,
            ..Default::default()
        };
        let now = t("2026-11-10T10:00:00Z");
        let st = FindingState::default();
        let mut a = finding("a");
        a.tier = Some(Tier::P0);
        let mut b = finding("b");
        b.tier = Some(Tier::P0);
        b.advisory.ids = vec!["CVE-2026-2".into()];
        b.advisory.primary = "CVE-2026-2".into();
        b.fixed_version = Some("6.22.0".into());
        let mut c = finding("c");
        c.tier = Some(Tier::P2);
        c.package = "tar".into();
        let mut d = finding("d");
        d.tier = Some(Tier::P2);
        d.package = "ws".into();
        let mut pending = finding("e");
        pending.verdict = Verdict::Pending;
        pending.tier = Some(Tier::P0);
        let mut nofix = finding("f");
        nofix.fixed_version = None;
        nofix.tier = Some(Tier::P0);
        let mut dismissed = finding("g");
        dismissed.status = Status::Dismissed;
        dismissed.tier = Some(Tier::P0);
        let all: Vec<(Finding, FindingState)> = [a, b, c, d, pending, nofix, dismissed]
            .into_iter()
            .map(|f| (f, st.clone()))
            .collect();
        let units = plan_units(&all, &cfg, now);
        assert_eq!(units.len(), 2);
        let one = units.iter().find(|u| !u.grouped).unwrap();
        // Two advisories on undici: one PR, to the version that clears both.
        assert_eq!(one.finding_ids, vec!["a", "b"]);
        assert_eq!(one.bumps.len(), 1);
        assert_eq!(one.bumps[0].to, "6.22.0");
        assert_eq!(one.key, "undici@6.22.0");
        let group = units.iter().find(|u| u.grouped).unwrap();
        assert_eq!(group.finding_ids, vec!["c", "d"]);
        assert_eq!(group.bumps.len(), 2);
        assert!(group.ticket.starts_with("SEC-"));
        // An ignore keeps it out.
        let ignoring = SecurityConfig {
            ignore: vec![Ignore {
                advisory: "cve-2026-31337".into(),
                repo: None,
                reason: "build only".into(),
                until: "2026-12-31".into(),
                by: "a@b".into(),
            }],
            ..cfg.clone()
        };
        let units = plan_units(&all, &ignoring, now);
        assert!(units
            .iter()
            .all(|u| !u.finding_ids.contains(&"a".to_string())));
        // A tier turned off opens nothing.
        let off = SecurityConfig {
            open_prs: TierMap {
                p0: Cadence::Off,
                p1: Cadence::Off,
                p2: Cadence::Off,
                p3: Cadence::Off,
            },
            ..cfg
        };
        assert!(plan_units(&all, &off, now).is_empty());
    }

    #[test]
    fn a_dry_run_previews_and_opens_nothing() {
        let cfg = SecurityConfig {
            enabled: true,
            dry_run: true,
            ..Default::default()
        };
        assert!(!cfg.acts(), "preview first opens and posts nothing");
        assert!(!SecurityConfig::default().acts(), "off until turned on");
        let mut f = finding("a");
        f.tier = Some(Tier::P0);
        let p = preview(
            &[(f, FindingState::default())],
            &cfg,
            t("2026-11-10T10:00:00Z"),
        );
        assert_eq!(p["would_open"].as_array().unwrap().len(), 1);
        assert_eq!(
            p["would_open"][0]["title"],
            "fix(deps): bump undici 6.19.2 to 6.21.1 [SECURITY] (CVE-2026-31337)"
        );
        assert_eq!(p["would_post"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn never_a_second_card() {
        let cfg = SecurityConfig::default();
        let mut f = finding("a");
        f.tier = Some(Tier::P0);
        f.due_at = "2026-11-13T10:00:00Z".into();
        let mut st = FindingState::default();
        // No escalation before the first card.
        assert_eq!(escalation(&f, &st, &cfg, t("2026-11-12T00:00:00Z")), None);
        st.cards.push("new".into());
        assert_eq!(escalation(&f, &st, &cfg, t("2026-11-11T09:00:00Z")), None);
        assert_eq!(
            escalation(&f, &st, &cfg, t("2026-11-11T22:00:00Z")),
            Some(Stage::Half)
        );
        st.cards.push("half".into());
        assert_eq!(escalation(&f, &st, &cfg, t("2026-11-12T00:00:00Z")), None);
        assert_eq!(
            escalation(&f, &st, &cfg, t("2026-11-13T10:00:00Z")),
            Some(Stage::Deadline)
        );
        st.cards.push("deadline".into());
        assert_eq!(escalation(&f, &st, &cfg, t("2026-11-20T00:00:00Z")), None);
        // A handled finding never posts, whatever is left.
        let fresh = FindingState {
            cards: vec!["new".into()],
            ..Default::default()
        };
        for s in [Status::Dismissed, Status::Fixed, Status::Withdrawn] {
            f.status = s;
            assert_eq!(
                escalation(&f, &fresh, &cfg, t("2026-11-20T00:00:00Z")),
                None
            );
            let p = preview(
                &[(f.clone(), FindingState::default())],
                &cfg,
                t("2026-11-10T10:00:00Z"),
            );
            assert!(p["would_post"].as_array().unwrap().is_empty());
        }
        // A tier that doesn't post has no card to escalate.
        f.status = Status::Open;
        f.tier = Some(Tier::P2);
        assert_eq!(
            escalation(&f, &fresh, &cfg, t("2026-12-20T00:00:00Z")),
            None
        );
        assert_eq!(card_claim_sk("abc", "new"), "ALERT_POSTED#SEC#abc#new");
    }

    #[test]
    fn a_withdrawn_advisory_closes_only_its_own_open_prs() {
        let now = t("2026-11-10T10:00:00Z");
        let mut f = finding("a");
        f.status = Status::PrOpen;
        f.pr = Some(PrRef {
            number: 7,
            state: "open".into(),
            ..Default::default()
        });
        let st = FindingState::default();
        let w = Observation {
            repo: f.repo.clone(),
            manifest: f.manifest.clone(),
            package: f.package.clone(),
            advisory: AdvisoryRecord {
                ids: f.advisory.ids.clone(),
                withdrawn: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let (f, st, _) = apply(Some((f, st)), &w, now);
        assert_eq!(f.status, Status::Withdrawn);
        assert_eq!(
            prs_to_close_on_withdraw(&[(f.clone(), st.clone())]),
            vec![("acme/web".to_string(), 7)]
        );
        // The same PR also fixes a live advisory: kept.
        let mut live = finding("b");
        live.status = Status::PrOpen;
        live.pr = f.pr.clone();
        assert!(prs_to_close_on_withdraw(&[
            (f.clone(), st.clone()),
            (live, FindingState::default())
        ])
        .is_empty());
        // Merged is left alone.
        let mut merged = f.clone();
        merged.pr = Some(PrRef {
            number: 7,
            state: "merged".into(),
            ..Default::default()
        });
        assert!(prs_to_close_on_withdraw(&[(merged, st)]).is_empty());
        // Only one of two ids withdrawn: still open.
        let mut g = finding("c");
        g.status = Status::Open;
        let half = Observation {
            advisory: AdvisoryRecord {
                ids: vec!["CVE-2026-31337".into()],
                withdrawn: true,
                ..Default::default()
            },
            ..w
        };
        let (g, _, _) = apply(Some((g, FindingState::default())), &half, now);
        assert_eq!(g.status, Status::Open);
    }

    #[test]
    fn settings_defaults_and_validation() {
        let c = SecurityConfig::default();
        assert!(!c.enabled && !c.dry_run);
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["open_prs"]["P1"], "hour");
        assert_eq!(v["repos"], "all");
        assert_eq!(v["major_bumps"], "fix_in_pr");
        let back: SecurityConfig =
            serde_json::from_value(json!({"enabled": true, "repos": ["acme/web"]})).unwrap();
        // Turned on without naming a choice: open PRs (the recommended
        // one), within the day's run, token and card limits.
        assert!(back.enabled && !back.dry_run);
        assert_eq!(back.max_fix_runs_per_day, MAX_FIX_RUNS_PER_DAY);
        assert_eq!(back.repos, Repos::List(vec!["acme/web".into()]));
        assert!(back.repos.includes("ACME/web") && !back.repos.includes("acme/api"));
        assert!(c.validate().is_ok());
        let bad = SecurityConfig {
            due_days: TierMap {
                p0: 0,
                ..c.due_days.clone()
            },
            ..c.clone()
        };
        assert!(bad.validate().is_err());
        let bad = SecurityConfig {
            due_days: TierMap {
                p3: 366,
                ..c.due_days.clone()
            },
            ..c.clone()
        };
        assert!(bad.validate().is_err());
        let bad = SecurityConfig {
            labels: (0..21).map(|i| format!("l{i}")).collect(),
            ..c.clone()
        };
        assert!(bad.validate().is_err());
        let bad = SecurityConfig {
            labels: vec!["x".repeat(51)],
            ..c.clone()
        };
        assert!(bad.validate().is_err());
        let bad = SecurityConfig {
            labels: vec![" ".into()],
            ..c.clone()
        };
        assert!(bad.validate().is_err());
        let bad = SecurityConfig {
            ignore: vec![Ignore {
                advisory: "CVE-1".into(),
                reason: "".into(),
                until: "2027-01-01".into(),
                ..Default::default()
            }],
            ..c.clone()
        };
        assert!(bad.validate().is_err());
        assert!(serde_json::from_value::<SecurityConfig>(json!({"repos": "some"})).is_err());
    }

    #[test]
    fn inventory_parsers() {
        let sbom = json!({"sbom": {"packages": [
            {"name": "npm:undici", "versionInfo": "6.19.2", "externalRefs": [{"referenceType": "purl", "referenceLocator": "pkg:npm/undici@6.19.2"}]},
            {"name": "npm:@types/node", "versionInfo": "20.1.0", "externalRefs": [{"referenceType": "purl", "referenceLocator": "pkg:npm/%40types/node@20.1.0"}]},
            {"name": "maven", "externalRefs": [{"referenceType": "purl", "referenceLocator": "pkg:maven/org.apache.logging.log4j/log4j-core@2.14.1"}]},
            {"name": "range", "versionInfo": "^1.0", "externalRefs": [{"referenceType": "purl", "referenceLocator": "pkg:npm/left-pad"}]},
            {"name": "com.github.acme/web", "externalRefs": []}
        ]}});
        let deps = deps_from_sbom(&sbom);
        assert_eq!(deps.len(), 3);
        assert_eq!(deps[1].name, "@types/node");
        assert_eq!(deps[2].ecosystem, "Maven");
        assert_eq!(deps[2].name, "org.apache.logging.log4j:log4j-core");
        let df = "FROM --platform=$BUILDPLATFORM node:20.18.1-bookworm-slim AS build\nRUN x\nFROM build AS test\nFROM gcr.io/distroless/nodejs20@sha256:abc\nFROM scratch\nFROM ubuntu";
        assert_eq!(
            dockerfile_images(df),
            vec![
                ("node".to_string(), "20.18.1-bookworm-slim".to_string()),
                (
                    "gcr.io/distroless/nodejs20".to_string(),
                    "sha256:abc".to_string()
                ),
                ("ubuntu".to_string(), "latest".to_string()),
            ]
        );
        let lock = json!({"packages": {"": {}, "node_modules/undici": {"version": "6.19.2"}, "node_modules/jest": {"dev": true}, "node_modules/a/node_modules/jest": {}}});
        let s = npm_lock_scopes(&lock);
        assert_eq!(s["undici"], Scope::Runtime);
        assert_eq!(s["jest"], Scope::Runtime);
        let lock = json!({"packages": {"node_modules/vitest": {"devOptional": true}}});
        assert_eq!(npm_lock_scopes(&lock)["vitest"], Scope::Dev);
        let poetry = "[[package]]\nname = \"pytest\"\ncategory = \"dev\"\n\n[[package]]\nname = \"Django\"\ncategory = \"main\"\n";
        let p = poetry_lock_scopes(poetry);
        assert_eq!(p["pytest"], Scope::Dev);
        assert_eq!(p["django"], Scope::Runtime);
        assert!(
            is_dependency_file("web/pnpm-lock.yaml")
                && is_dependency_file("deploy/Dockerfile.prod")
        );
        assert!(!is_dependency_file("src/main.rs"));
    }

    #[test]
    fn dependabot_alerts_and_github_advisories_are_read() {
        let alert = json!({
            "number": 88, "state": "open", "html_url": "https://github.com/acme/web/security/dependabot/88",
            "dependency": {"package": {"ecosystem": "npm", "name": "undici"}, "manifest_path": "pnpm-lock.yaml", "scope": "runtime", "relationship": "transitive"},
            "security_advisory": {"ghsa_id": "GHSA-q9fx-2w5c-8m4r", "cve_id": "CVE-2026-31337", "summary": "Smuggling", "severity": "critical",
                "identifiers": [{"type": "GHSA", "value": "GHSA-q9fx-2w5c-8m4r"}, {"type": "CVE", "value": "CVE-2026-31337"}],
                "cvss_severities": {"cvss_v3": {"score": 9.1, "vector_string": "CVSS:3.1/AV:N"}, "cvss_v4": {"score": 0.0}},
                "epss": [{"percentage": 0.71, "percentile": 0.99}], "cwes": [{"cwe_id": "CWE-444"}]},
            "security_vulnerability": {"package": {"name": "undici"}, "vulnerable_version_range": "< 6.21.1", "first_patched_version": {"identifier": "6.21.1"}}
        });
        let o = observation_from_alert("acme/web", &alert).unwrap();
        assert_eq!(o.scope, Scope::Runtime);
        assert_eq!(o.relation, Relation::Transitive);
        assert_eq!(o.advisory.cvss, Some(9.1));
        assert_eq!(o.advisory.epss, Some(0.71));
        assert_eq!(o.advisory.fixed, vec!["6.21.1"]);
        assert_eq!(o.alert.as_ref().unwrap().number, 88);
        let deps = vec![
            Dep {
                ecosystem: "npm".into(),
                name: "undici".into(),
                version: "6.19.2".into(),
                manifest: "sbom".into(),
                scope: Scope::Unknown,
            },
            Dep {
                ecosystem: "npm".into(),
                name: "undici".into(),
                version: "6.21.1".into(),
                manifest: "web/package-lock.json".into(),
                scope: Scope::Runtime,
            },
        ];
        let adv = json!({"ghsa_id": "GHSA-q9fx-2w5c-8m4r", "vulnerabilities": [{"package": {"ecosystem": "npm", "name": "undici"}, "vulnerable_version_range": "< 6.21.1", "first_patched_version": "6.21.1"}]});
        let m = match_github_advisory("acme/web", &adv, &deps);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].version, "6.19.2");
        assert_eq!(m[0].advisory.fixed, vec!["6.21.1"]);
        let kev = kev_map(
            &json!({"vulnerabilities": [{"cveID": "CVE-2026-31337", "dueDate": "2026-11-24"}]}),
        );
        let mut f = finding("a");
        assert!(apply_kev(&mut f, &kev));
        assert!(f.advisory.kev && f.advisory.kev_due.as_deref() == Some("2026-11-24"));
        assert!(!apply_kev(&mut f, &kev));
        let e = epss_map(
            &json!({"data": [{"cve": "CVE-2026-31337", "epss": "0.71", "percentile": "0.99"}]}),
        );
        assert_eq!(e["CVE-2026-31337"], (0.71, 0.99));
        let many: Vec<String> = (0..400).map(|i| format!("CVE-2026-{i:05}")).collect();
        assert!(epss_batches(&many).iter().all(|b| b.len() <= 1900));
    }

    #[test]
    fn triage_answers_are_held_to_the_evidence() {
        let hits = vec![EvidenceLine {
            file: "src/proxy.ts".into(),
            line: 44,
            text: "fetch(upstream)".into(),
        }];
        let (v, e) = parse_triage(
            r#"Sure. {"verdict":"reachable","summary":"Called per request.","lines":[{"file":"src/proxy.ts","line":44},{"file":"made/up.ts","line":1}],"confidence":"high"}"#,
            &hits,
        );
        assert_eq!(v, Verdict::Reachable);
        assert_eq!(e.lines.len(), 1);
        assert_eq!(e.confidence, "high");
        // Reachable with nothing real to show: unknown.
        let (v, e) = parse_triage(
            r#"{"verdict":"reachable","lines":[{"file":"x","line":2}],"confidence":"high"}"#,
            &hits,
        );
        assert_eq!(v, Verdict::Unknown);
        assert_eq!(e.confidence, "low");
        let (v, _) = parse_triage("no json", &[]);
        assert_eq!(v, Verdict::Unknown);
        let mut f = finding("a");
        let st = FindingState {
            symbols: vec![
                "Parser.onHeadersComplete".into(),
                "(*Decoder).Decode".into(),
            ],
            ..Default::default()
        };
        assert_eq!(
            search_terms(&f, &st),
            vec!["undici", "onHeadersComplete", "Decode"]
        );
        let p = triage_prompt(&f, &st, &hits, 4000);
        assert!(p.contains("src/proxy.ts:44") && p.len() <= 4000);
        // The dependency list shows it fixed, or still vulnerable.
        let dep = |v: &str| Dep {
            ecosystem: "npm".into(),
            name: "undici".into(),
            version: v.into(),
            manifest: "sbom".into(),
            scope: Scope::Unknown,
        };
        assert!(fixed_by_inventory(&f, &[dep("6.21.1")]));
        assert!(!fixed_by_inventory(&f, &[dep("6.21.1"), dep("6.19.2")]));
        assert!(fixed_by_inventory(
            &f,
            &[Dep {
                name: "tar".into(),
                ..dep("1.0.0")
            }]
        ));
        assert!(!fixed_by_inventory(&f, &[]));
        let image = Dep {
            ecosystem: "docker".into(),
            name: "node".into(),
            ..dep("20")
        };
        assert!(
            !fixed_by_inventory(&f, &[image]),
            "no npm list says nothing"
        );
        assert_eq!(
            installed_version(&[dep("6.19.2"), dep("5.0.0")], "npm", "undici", "x").as_deref(),
            Some("5.0.0")
        );
        f.version.clear();
        assert!(!fixable(
            &f,
            &FindingState::default(),
            &SecurityConfig::default(),
            t("2026-11-10T10:00:00Z")
        ));
    }

    #[test]
    fn overview_counts_and_cards() {
        let now = t("2026-11-10T12:00:00Z");
        let mut a = finding("a");
        a.tier = Some(Tier::P0);
        a.due_at = "2026-11-12T10:00:00Z".into();
        let mut b = finding("b");
        b.status = Status::Fixed;
        b.updated_at = "2026-11-09T00:00:00Z".into();
        b.first_seen_at = "2026-11-09T10:00:00Z".into();
        b.verdict = Verdict::NotReachable;
        let st_b = FindingState {
            merged_at: Some("2026-11-10T05:00:00Z".into()),
            ..Default::default()
        };
        let v = overview_counts(&[(a.clone(), FindingState::default()), (b, st_b)], now);
        assert_eq!(v["counts"]["open"], 1);
        assert_eq!(v["counts"]["p0"], 1);
        assert_eq!(v["counts"]["due_7d"], 1);
        assert_eq!(v["counts"]["fixed_30d"], 1);
        assert_eq!(v["counts"]["noise_removed"], 1);
        assert_eq!(v["median_hours_to_merge_30d"], 19.0);
        let m = card(&a, Stage::New, "https://app/security/detail/?id=a", now);
        assert!(m
            .summary
            .contains("P0 security · web is affected by CVE-2026-31337"));
        assert!(!m.summary.contains('→'));
    }

    // ── Inventory to OSV, on GitHub's real shapes ──────────────────────

    const SBOM: &str = include_str!("security_testdata/github_sbom.json");
    const OSV_BATCH: &str = include_str!("security_testdata/osv_querybatch.json");
    const OSV_LODASH: &str = include_str!("security_testdata/osv_GHSA-35jh-r3h4-6jhm.json");
    const OSV_AXIOS: &str = include_str!("security_testdata/osv_GHSA-cph5-m8f7-6c5x.json");

    /// GitHub's SBOM (SPDX JSON with purl external refs): the repository's
    /// own package (`pkg:github/...`) is not a dependency, and versions that
    /// are ranges or partial (`3`, `^1.2.0`, `4.*.*`) are counted, not queried.
    #[test]
    fn github_sbom_lists_the_packages_and_leaves_out_the_repo_itself() {
        let sbom: Value = serde_json::from_str(SBOM).unwrap();
        let (deps, unpinned) = inventory_from_sbom(&sbom);
        let names: Vec<(&str, &str, &str)> = deps
            .iter()
            .map(|d| (d.ecosystem.as_str(), d.name.as_str(), d.version.as_str()))
            .collect();
        assert_eq!(
            names,
            vec![
                ("npm", "lodash", "4.17.20"),
                ("npm", "axios", "0.21.0"),
                ("npm", "left-pad", "1.3.0"),
                ("GitHub Actions", "example/deploy-action", "2.6.2"),
            ]
        );
        assert_eq!(
            unpinned, 3,
            "@scope/partial 3, ranged ^1.2.0, checkout 4.*.*"
        );
        assert!(!deps.iter().any(|d| d.name.contains("example-org/web")));
        let q = osv_queries(&deps);
        assert_eq!(
            q[0],
            json!({"package": {"name": "lodash", "ecosystem": "npm"}, "version": "4.17.20"})
        );
        assert!(
            exact_version("npm", "1.4.4-lts.1")
                && !exact_version("npm", "3")
                && !exact_version("npm", "main")
        );
    }

    /// The recorded answers of OSV's API (`querybatch`, then each record)
    /// for lodash 4.17.20 and axios 0.21.0 make findings: known vulnerable
    /// versions are matched, and an unaffected package gets nothing.
    #[test]
    fn known_vulnerable_versions_match_osvs_recorded_answers() {
        let sbom: Value = serde_json::from_str(SBOM).unwrap();
        let deps = deps_from_sbom(&sbom);
        let npm: Vec<Dep> = deps
            .iter()
            .filter(|d| d.ecosystem == "npm")
            .cloned()
            .collect();
        // The recorded batch was asked for lodash, axios, left-pad in order.
        let batch: Value = serde_json::from_str(OSV_BATCH).unwrap();
        let ids = |i: usize| -> Vec<String> {
            batch["results"][i]["vulns"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v["id"].as_str().map(String::from))
                .collect()
        };
        assert!(
            ids(0).contains(&"GHSA-35jh-r3h4-6jhm".to_string()),
            "lodash command injection"
        );
        assert!(
            ids(1).contains(&"GHSA-cph5-m8f7-6c5x".to_string()),
            "axios ReDoS"
        );
        assert!(ids(1).len() >= 20, "axios 0.21.0 has many advisories");
        assert!(ids(2).is_empty(), "left-pad 1.3.0 has none");
        let lodash: Value = serde_json::from_str(OSV_LODASH).unwrap();
        let axios: Value = serde_json::from_str(OSV_AXIOS).unwrap();
        let mut findings: Vec<(Finding, FindingState)> = vec![];
        for (dep, rec) in [(&npm[0], &lodash), (&npm[1], &axios)] {
            let adv = advisory_from_osv(rec, &dep.ecosystem, &dep.name)
                .expect("the record names the package");
            let affected = rec["affected"]
                .as_array()
                .unwrap()
                .iter()
                .find(|a| a["package"]["name"] == dep.name.as_str())
                .unwrap();
            assert!(
                osv_affects(affected, &dep.version),
                "{} {}",
                dep.name,
                dep.version
            );
            let o = Observation {
                repo: "example-org/web".into(),
                manifest: dep.manifest.clone(),
                ecosystem: dep.ecosystem.clone(),
                package: dep.name.clone(),
                version: dep.version.clone(),
                advisory: adv,
                scope: dep.scope,
                ..Default::default()
            };
            let (f, st, created) = apply(None, &o, Utc::now());
            assert!(created);
            findings.push((f, st));
        }
        let (l, _) = &findings[0];
        assert!(l.advisory.ids.contains(&"CVE-2021-23337".to_string()));
        assert_eq!(l.fixed_version.as_deref(), Some("4.17.21"));
        assert!(
            l.advisory
                .cvss_vector
                .as_deref()
                .is_some_and(|v| v.starts_with("CVSS:3.1/")),
            "OSV's vector is kept without a score"
        );
        let (a, _) = &findings[1];
        assert_eq!(a.fixed_version.as_deref(), Some("0.21.2"));
        assert_eq!(a.advisory.source, "osv");
        // Not yet read for reachability: high, but never P0 on its own.
        let cfg = SecurityConfig::default();
        for (f, st) in findings.iter_mut() {
            retier(f, st, &cfg);
            assert_ne!(f.tier, Some(Tier::P0));
            assert!(f.tier_provisional);
        }
    }

    /// The live call, for a person to run: `cargo test -p common -- --ignored osv_live`.
    #[tokio::test]
    #[ignore]
    async fn osv_live_matches_lodash_and_axios() {
        let r: Value = reqwest::Client::new()
            .post(format!("{OSV_API}/v1/querybatch"))
            .json(&json!({"queries": [
                {"package": {"name": "lodash", "ecosystem": "npm"}, "version": "4.17.20"},
                {"package": {"name": "axios", "ecosystem": "npm"}, "version": "0.21.0"}]}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let has = |i: usize, id: &str| {
            r["results"][i]["vulns"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["id"] == id)
        };
        assert!(has(0, "GHSA-35jh-r3h4-6jhm") && has(1, "GHSA-cph5-m8f7-6c5x"));
    }

    #[test]
    fn a_failed_sbom_read_says_why_in_plain_words() {
        let off = sbom_failure(404, &json!({"message": "Not Found"}));
        assert_eq!(off.state, "graph_off");
        assert!(off.reason.contains("Settings, Code security"));
        let slow = sbom_failure(500, &json!({"message": "Request timed out"}));
        assert_eq!(slow.state, "error");
        assert!(slow.reason.contains("500") && slow.reason.contains("timed out"));
    }

    // ── Groups ─────────────────────────────────────────────────────────

    fn adv_finding(
        id: &str,
        pkg: &str,
        ver: &str,
        fixed: Option<&str>,
        tier: Tier,
        due: &str,
    ) -> (Finding, FindingState) {
        let mut f = finding(id);
        f.package = pkg.into();
        f.version = ver.into();
        f.fixed_version = fixed.map(String::from);
        f.advisory.primary = format!("GHSA-{id}");
        f.advisory.ids = vec![format!("GHSA-{id}")];
        f.tier = Some(tier);
        f.due_at = due.into();
        (f, FindingState::default())
    }

    /// One fix is one row: a package at one version with every advisory the
    /// bump clears; the worst advisory sets the tier and the due date, and
    /// the fix is the highest version any advisory needs.
    #[test]
    fn findings_group_by_package_version_with_the_worst_tier() {
        let fs = vec![
            adv_finding(
                "a",
                "tar",
                "6.2.1",
                Some("6.2.2"),
                Tier::P2,
                "2026-12-10T00:00:00Z",
            ),
            adv_finding(
                "b",
                "tar",
                "6.2.1",
                Some("7.5.21"),
                Tier::P1,
                "2026-11-17T00:00:00Z",
            ),
            adv_finding("c", "tar", "6.2.1", None, Tier::P3, "2027-02-08T00:00:00Z"),
            adv_finding(
                "d",
                "qs",
                "6.14.0",
                Some("6.14.2"),
                Tier::P3,
                "2027-02-08T00:00:00Z",
            ),
        ];
        let g = groups(&fs);
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].package, "tar");
        assert_eq!(g[0].tier, Some(Tier::P1));
        assert_eq!(g[0].due_at, "2026-11-17T00:00:00Z");
        assert_eq!(g[0].fixed_version.as_deref(), Some("7.5.21"));
        assert_eq!(g[0].no_fix, 1);
        assert_eq!(g[0].advisories.len(), 3);
        assert_eq!(
            g[0].advisories[0].tier,
            Some(Tier::P1),
            "worst advisory first"
        );
        let o = overview_counts(&fs, Utc::now());
        assert_eq!(o["groups"]["open"], 2);
        assert_eq!(o["groups"]["p1"], 1);
        assert_eq!(o["counts"]["open"], 4, "advisories still counted second");
    }

    // ── Dependabot access ──────────────────────────────────────────────

    #[test]
    fn dependabot_access_tells_the_three_causes_apart() {
        let not_accessible = json!({"message": "Resource not accessible by integration"});
        let lacks = dependabot_access(403, &not_accessible, Some(false), true);
        assert_eq!(lacks.state, "not_accepted");
        assert!(lacks.reason.contains("hasn't accepted"));
        let app = dependabot_access(403, &not_accessible, None, false);
        assert_eq!(app.state, "no_permission");
        assert!(app.reason.contains("Permissions and events"));
        let off = dependabot_access(
            403,
            &json!({"message": "Dependabot alerts are disabled for this repository."}),
            Some(true),
            false,
        );
        assert_eq!(off.state, "off");
        assert!(off.reason.contains("turned off for this repo"));
        assert_eq!(
            dependabot_access(200, &Value::Null, Some(true), false).state,
            "on"
        );
        // The team shows the one that needs a person first.
        let t = team_dependabot([&off, &lacks]).unwrap();
        assert_eq!(t.state, "not_accepted");
    }

    // ── Triage without the model ───────────────────────────────────────

    #[test]
    fn triage_settles_without_the_model_where_it_can() {
        let st = FindingState::default();
        let mut f = finding("t");
        f.scope = Scope::Dev;
        assert_eq!(
            settle_without_model(&f, &st, 5, 2, &[]).unwrap().0,
            Verdict::NotLoaded
        );
        f.scope = Scope::Unknown;
        let (v, e) = settle_without_model(&f, &st, 0, 0, &["undici".into()]).unwrap();
        assert_eq!(v, Verdict::Unknown);
        assert!(e.summary.contains("names no functions"), "{}", e.summary);
        assert!(
            settle_without_model(&f, &st, 3, 2, &["undici".into()]).is_none(),
            "code names it: the model reads it"
        );
        assert_eq!(
            unknown_reason("tar", 3, &[]),
            "tar is imported in 3 files, but the advisory names no functions to look for."
        );
        // The same advisories and code lines give the same key; new code doesn't.
        let a = finding("k");
        let line = |t: &str| EvidenceLine {
            file: "src/a.ts".into(),
            line: 1,
            text: t.into(),
        };
        assert_eq!(
            group_triage_key(&[&a], &[line("import x")]),
            group_triage_key(&[&a], &[line("import x")])
        );
        assert_ne!(
            group_triage_key(&[&a], &[line("import x")]),
            group_triage_key(&[&a], &[line("import y")])
        );
    }

    // ── Daily limits ───────────────────────────────────────────────────

    /// Two sweeps at once never start more runs than the day's limit: the
    /// slot is taken atomically (a conditional ADD in DynamoDB).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sweeps_at_once_never_start_more_than_the_limit() {
        let slots = std::sync::Arc::new(MemorySlots::default());
        let cfg = std::sync::Arc::new(SecurityConfig::default());
        let mut tasks = vec![];
        for _ in 0..2 {
            let (slots, cfg) = (slots.clone(), cfg.clone());
            tasks.push(tokio::spawn(async move {
                let mut started = 0;
                for _ in 0..20 {
                    if admit_run(slots.as_ref(), &cfg, "20261111", false).await == Admit::Start {
                        started += 1;
                    }
                }
                started
            }));
        }
        let mut total = 0;
        for t in tasks {
            total += t.await.unwrap();
        }
        assert_eq!(total, MAX_FIX_RUNS_PER_DAY as usize);
        // A new day starts again.
        assert_eq!(
            admit_run(slots.as_ref(), &cfg, "20261112", false).await,
            Admit::Start
        );
        assert_eq!(
            admit_run(slots.as_ref(), &cfg, "20261111", false).await,
            Admit::Wait("Waiting: today's 3 fix runs are used. Next starts tomorrow.".into())
        );
    }

    #[tokio::test]
    async fn the_token_limit_and_a_busy_repo_stop_new_runs() {
        let slots = MemorySlots::default();
        let cfg = SecurityConfig::default();
        slots
            .tokens
            .lock()
            .unwrap()
            .insert("20261111".into(), MAX_FIX_TOKENS_PER_DAY);
        let w = admit_run(&slots, &cfg, "20261111", false).await;
        assert_eq!(w, Admit::Wait(waiting_tokens(MAX_FIX_TOKENS_PER_DAY)));
        assert_eq!(
            *slots.runs.lock().unwrap().get("20261111").unwrap_or(&0),
            0,
            "no slot taken"
        );
        assert_eq!(
            admit_run(&slots, &cfg, "20261112", true).await,
            Admit::Wait(WAITING_REPO.into())
        );
        let none = SecurityConfig {
            max_fix_runs_per_day: 0,
            ..Default::default()
        };
        assert!(matches!(
            admit_run(&slots, &none, "20261113", false).await,
            Admit::Wait(_)
        ));
        assert!(
            !RunKind::Adopt.counts() && !RunKind::IssueOnly.counts() && RunKind::Run.counts(),
            "adopting or an issue costs no run"
        );
    }

    #[test]
    fn p0_runs_first_then_the_soonest_due() {
        let fs = vec![
            adv_finding(
                "p2",
                "qs",
                "6.14.0",
                Some("6.14.2"),
                Tier::P2,
                "2026-11-12T00:00:00Z",
            ),
            adv_finding(
                "p0",
                "tar",
                "6.2.1",
                Some("7.5.21"),
                Tier::P0,
                "2026-11-14T00:00:00Z",
            ),
            adv_finding(
                "p1",
                "undici",
                "6.19.2",
                Some("6.21.1"),
                Tier::P1,
                "2026-11-13T00:00:00Z",
            ),
        ];
        let mut units: Vec<FixUnit> = fs
            .iter()
            .map(|(f, _)| FixUnit {
                finding_ids: vec![f.id.clone()],
                key: f.id.clone(),
                ..Default::default()
            })
            .collect();
        order_units(&mut units, &fs);
        let order: Vec<&str> = units.iter().map(|u| u.key.as_str()).collect();
        assert_eq!(order, vec!["p0", "p1", "p2"]);
    }

    #[test]
    fn cards_past_the_days_limit_go_into_one_digest() {
        let fs: Vec<(Finding, FindingState)> = (0..14)
            .map(|i| {
                adv_finding(
                    &format!("c{i}"),
                    "pkg",
                    &format!("1.0.{i}"),
                    None,
                    if i < 2 { Tier::P0 } else { Tier::P1 },
                    "2026-11-17T00:00:00Z",
                )
            })
            .collect();
        let refs: Vec<&Finding> = fs.iter().rev().map(|(f, _)| f).collect();
        let (now, later) = card_budget(refs, 0, MAX_CARDS_PER_DAY);
        assert_eq!(now.len(), 10);
        assert_eq!(later.len(), 4);
        assert!(
            now.iter().take(2).all(|f| f.tier == Some(Tier::P0)),
            "P0 cards first"
        );
        let (now2, later2) =
            card_budget(fs.iter().map(|(f, _)| f).collect(), 10, MAX_CARDS_PER_DAY);
        assert!(now2.is_empty() && later2.len() == 14);
        let d = digest_card(&later, "https://app.example.com/security", 10);
        assert!(d.summary.contains("4 more findings"));
    }

    #[test]
    fn the_policy_bounds_the_limits_and_defaults_to_opening_prs() {
        let d = SecurityConfig::default();
        assert!(!d.enabled, "off until a team turns it on");
        assert!(!d.dry_run, "turned on, it opens fix PRs (recommended)");
        assert_eq!(
            (
                d.max_fix_runs_per_day,
                d.max_fix_tokens_per_day,
                d.triage_tokens_per_day,
                d.max_cards_per_day
            ),
            (3, 2_000_000, 400_000, 10)
        );
        assert!(SecurityConfig {
            max_fix_runs_per_day: 21,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(SecurityConfig {
            max_fix_runs_per_day: 20,
            ..Default::default()
        }
        .validate()
        .is_ok());
        assert!(SecurityConfig {
            triage_tokens_per_day: MAX_TRIAGE_TOKENS_PER_DAY + 1,
            ..Default::default()
        }
        .validate()
        .is_err());
        // A config saved before the limits existed reads with the defaults.
        let old = SecurityConfig::from_json(r#"{"enabled": true, "dry_run": false}"#).unwrap();
        assert_eq!(old.max_fix_runs_per_day, 3);
    }

    #[test]
    fn insights_count_opened_fixed_prs_and_spend_per_day() {
        let now = DateTime::parse_from_rfc3339("2026-11-20T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut a = adv_finding(
            "i1",
            "tar",
            "6.2.1",
            Some("7.5.21"),
            Tier::P1,
            "2026-11-17T00:00:00Z",
        );
        a.0.first_seen_at = "2026-11-10T10:00:00Z".into();
        a.0.status = Status::Fixed;
        a.0.pr = Some(PrRef {
            number: 7,
            url: String::new(),
            ci: None,
            state: "merged".into(),
            adopted: false,
        });
        a.1.pr_opened_at = Some("2026-11-10T12:00:00Z".into());
        a.1.merged_at = Some("2026-11-11T10:00:00Z".into());
        let mut b = adv_finding("i2", "qs", "6.14.0", None, Tier::P3, "2027-02-08T00:00:00Z");
        b.0.first_seen_at = "2026-11-10T11:00:00Z".into();
        let spend = BTreeMap::from([("2026-11-10".to_string(), 13_516u64)]);
        let v = insights(&[a, b], &spend, now, 30);
        let day = |d: &str| {
            v["days"]
                .as_array()
                .unwrap()
                .iter()
                .find(|x| x["date"] == d)
                .unwrap()
                .clone()
        };
        assert_eq!(day("2026-11-10")["opened"], 2);
        assert_eq!(day("2026-11-10")["prs_opened"], 1);
        assert_eq!(day("2026-11-10")["tokens"], 13_516);
        assert_eq!(day("2026-11-11")["fixed"], 1);
        assert_eq!(day("2026-11-11")["prs_merged"], 1);
        assert_eq!(v["median_hours_to_fix"]["P1"]["median_hours"], 24.0);
        assert_eq!(v["totals"]["tokens_month"], 13_516);
    }

    /// A replay of stored findings (a DynamoDB query's JSON, with `finding`
    /// and `state` strings) through today's tiering:
    /// `SECFIX_REPLAY=findings.json cargo test -p common -- --ignored replay --nocapture`.
    #[test]
    #[ignore]
    fn replay_stored_findings_through_the_tiers() {
        let Ok(path) = std::env::var("SECFIX_REPLAY") else {
            return;
        };
        let v: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let mut fs: Vec<(Finding, FindingState)> = vec![];
        for i in v["Items"].as_array().unwrap() {
            let f: Finding = serde_json::from_str(i["finding"]["S"].as_str().unwrap()).unwrap();
            let st: FindingState =
                serde_json::from_str(i["state"]["S"].as_str().unwrap_or("{}")).unwrap_or_default();
            fs.push((f, st));
        }
        let cfg = SecurityConfig::default();
        let mut repos: Vec<String> = fs.iter().map(|(f, _)| f.repo.clone()).collect();
        repos.sort();
        repos.dedup();
        for repo in repos {
            let mut before: BTreeMap<String, usize> = BTreeMap::new();
            let mut after: BTreeMap<String, usize> = BTreeMap::new();
            let mut mine: Vec<(Finding, FindingState)> = vec![];
            for (f, st) in fs.iter().filter(|(f, _)| f.repo == repo) {
                *before
                    .entry(f.tier.map(|t| t.as_str()).unwrap_or("-").to_string())
                    .or_default() += 1;
                let mut f2 = f.clone();
                retier(&mut f2, st, &cfg);
                *after
                    .entry(f2.tier.map(|t| t.as_str()).unwrap_or("-").to_string())
                    .or_default() += 1;
                mine.push((f2, st.clone()));
            }
            let g = groups(&mine);
            let mut gt: BTreeMap<String, usize> = BTreeMap::new();
            for x in &g {
                *gt.entry(x.tier.map(|t| t.as_str()).unwrap_or("-").to_string())
                    .or_default() += 1;
            }
            println!(
                "{repo}: advisories before {before:?} after {after:?}; groups {} {gt:?}",
                g.len()
            );
        }
    }

    // ── Fix PRs a person asks for ──────────────────────────────────────

    fn asked() -> Vec<(Finding, FindingState)> {
        let mut v = vec![
            adv_finding(
                "t1",
                "tar",
                "6.2.1",
                Some("6.2.2"),
                Tier::P2,
                "2026-12-10T00:00:00Z",
            ),
            adv_finding(
                "t2",
                "tar",
                "6.2.1",
                Some("7.5.21"),
                Tier::P1,
                "2026-11-17T00:00:00Z",
            ),
            adv_finding(
                "t3",
                "tar",
                "6.2.1",
                Some("6.2.2"),
                Tier::P3,
                "2027-02-08T00:00:00Z",
            ),
            adv_finding(
                "p1",
                "protobufjs",
                "7.4.0",
                Some("7.5.4"),
                Tier::P2,
                "2026-12-10T00:00:00Z",
            ),
            adv_finding("nf", "qs", "6.14.0", None, Tier::P3, "2027-02-08T00:00:00Z"),
            adv_finding(
                "pr",
                "undici",
                "6.19.2",
                Some("6.21.1"),
                Tier::P1,
                "2026-11-17T00:00:00Z",
            ),
            adv_finding(
                "dm",
                "minimist",
                "1.2.5",
                Some("1.2.6"),
                Tier::P3,
                "2027-02-08T00:00:00Z",
            ),
        ];
        v[5].0.pr = Some(PrRef {
            number: 123,
            url: "https://github.com/acme/web/pull/123".into(),
            ci: Some("passing".into()),
            state: "open".into(),
            adopted: false,
        });
        v[5].0.status = Status::PrOpen;
        v[6].0.status = Status::Dismissed;
        v
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// Five advisories picked become two PRs: one per repository, package
    /// and version, bumped to the version that clears them all.
    #[test]
    fn picked_findings_become_one_pr_per_package_version() {
        let fs = asked();
        let (units, skipped) = manual_units(
            &fs,
            &ids(&["t1", "t2", "t3", "p1"]),
            &SecurityConfig::default(),
            Utc::now(),
        );
        assert_eq!(units.len(), 2);
        let tar = units.iter().find(|u| u.bumps[0].package == "tar").unwrap();
        assert_eq!(tar.finding_ids, ids(&["t1", "t2", "t3"]));
        assert_eq!(
            tar.bumps[0].to, "7.5.21",
            "the version that clears every advisory"
        );
        assert_eq!(tar.key, "tar@7.5.21");
        assert!(tar.ticket.starts_with(TICKET_PREFIX));
        assert!(skipped.is_empty());
    }

    /// An open PR is linked instead of a new one; no fix, dismissed and fixed
    /// are skipped with the reason.
    #[test]
    fn picked_findings_that_cant_get_a_pr_are_skipped_with_why() {
        let fs = asked();
        let (units, skipped) = manual_units(
            &fs,
            &ids(&["pr", "nf", "dm", "gone"]),
            &SecurityConfig::default(),
            Utc::now(),
        );
        assert!(units.is_empty());
        assert_eq!(
            skipped,
            vec![
                Skipped {
                    finding_id: "pr".into(),
                    reason: "already has an open PR".into(),
                    pr_url: Some("https://github.com/acme/web/pull/123".into())
                },
                Skipped {
                    finding_id: "nf".into(),
                    reason: "no fixed version".into(),
                    pr_url: None
                },
                Skipped {
                    finding_id: "dm".into(),
                    reason: "dismissed".into(),
                    pr_url: None
                },
                Skipped {
                    finding_id: "gone".into(),
                    reason: "not found".into(),
                    pr_url: None
                },
            ]
        );
    }

    /// A person's request starts even in a dry run, within the day's limit:
    /// past it the units are queued with the reason, unless an admin raised
    /// today's limit for the request.
    #[tokio::test]
    async fn a_request_bypasses_dry_run_but_not_the_limit() {
        let fs: Vec<(Finding, FindingState)> = ["a", "b", "c", "d"]
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let mut f = adv_finding(
                    p,
                    p,
                    "1.0.0",
                    Some("1.0.1"),
                    Tier::P2,
                    "2026-12-10T00:00:00Z",
                );
                f.0.repo = format!("acme/r{i}");
                f
            })
            .collect();
        let cfg = SecurityConfig {
            enabled: true,
            dry_run: true,
            ..Default::default()
        };
        let all = ids(&["a", "b", "c", "d"]);
        let slots = MemorySlots::default();
        let (mut units, _) = manual_units(&fs, &all, &cfg, Utc::now());
        let (started, queued) =
            admit_request(&slots, &cfg, "20261111", &mut units, &fs, false).await;
        assert_eq!(started.len(), 3, "dry run doesn't hold an explicit request");
        assert_eq!(queued.len(), 1);
        assert_eq!(
            queued[0].reason.as_deref(),
            Some("Waiting: today's 3 fix runs are used. Next starts tomorrow.")
        );
        // Raised for this request: it starts, past the limit.
        let (mut again, _) = manual_units(&fs, &ids(&["d"]), &cfg, Utc::now());
        let (started, queued) =
            admit_request(&slots, &cfg, "20261111", &mut again, &fs, true).await;
        assert_eq!((started.len(), queued.len()), (1, 0));
        assert_eq!(*slots.runs.lock().unwrap().get("20261111").unwrap(), 4);
        // The automatic path still respects dry run.
        assert!(!cfg.acts());
    }

    /// One run per repository at a time, even for a request.
    #[tokio::test]
    async fn a_request_waits_for_a_repo_with_a_run_in_progress() {
        let mut fs = asked();
        fs[3].0.status = Status::Fixing;
        fs[3].0.package = "other".into();
        let (mut units, _) =
            manual_units(&fs, &ids(&["t2"]), &SecurityConfig::default(), Utc::now());
        let (started, queued) = admit_request(
            &MemorySlots::default(),
            &SecurityConfig::default(),
            "20261111",
            &mut units,
            &fs,
            true,
        )
        .await;
        assert!(started.is_empty());
        assert_eq!(queued[0].reason.as_deref(), Some(WAITING_REPO));
    }
}
