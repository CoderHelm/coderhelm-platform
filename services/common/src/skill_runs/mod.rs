//! Scheduled skill runs: a team's synced skill, run by CoderHelm's agent on
//! a schedule or on "Run now", read-only by default.
//!
//! A team admin picks a skill from the team's skill repositories, a cron
//! line in the team's time zone ([`cron`]), the tools the skill may call
//! (each one a tool of a connected MCP server, or Athena through the team's
//! AWS reader role), the folder that is the skill's memory between runs, a
//! budget, and where the result goes. Each run is a CoderHelm run of its own
//! kind (`skill_run`) listed with the team's other runs; its report and its
//! snapshot are the run's outputs.
//!
//! - [`Schedule`]: one schedule, as stored and as the dashboard edits it.
//! - [`cron`]: the cron line, its EventBridge form and its next run.
//! - [`engine`]: one run, behind traits, so the whole flow runs in tests
//!   against fakes (a local sandbox included).
//!
//! Records live in the settings table under the team (the plugins' [`Kv`]):
//! `SKILLSCHED#<id>` for a schedule and `SKILLSCHEDRUN#<id>#<run>` for each
//! run of it, newest last. The memory folder lives in the data bucket under
//! [`state_prefix`].
//!
//! [`Kv`]: crate::plugins::kv::Kv

pub mod aws;
pub mod cron;
pub mod engine;
pub mod resolve;
pub mod risk;
pub mod stdio;

use serde::{Deserialize, Serialize};

use crate::plugins::kv::{self, Expect, Kv, KvError};
use crate::tool_registry::{classify_external, Risk};

/// A schedule's record key.
pub const SCHEDULE_PREFIX: &str = "SKILLSCHED#";
/// A run's record key prefix, before the schedule id.
pub const RUN_PREFIX: &str = "SKILLSCHEDRUN#";
/// Schedules one team keeps at most.
pub const MAX_SCHEDULES: usize = 25;
/// Tools one schedule grants at most.
pub const MAX_TOOLS: usize = 30;
/// Runs of one schedule its page lists.
pub const HISTORY: usize = 30;
/// The largest budget a schedule may set.
pub const MAX_BUDGET_USD: f64 = 50.0;
pub const MAX_MINUTES: u32 = 120;
/// What a new schedule gets.
pub const DEFAULT_BUDGET_USD: f64 = 5.0;
pub const DEFAULT_MINUTES: u32 = 30;
/// Athena data one run may scan at most, in GB.
pub const MAX_ATHENA_GB: f64 = 200.0;
/// A run still marked running after this long no longer blocks the next.
pub const STALE_RUN_SECS: i64 = 2 * 60 * 60;
/// The built-in tool that queries Athena through the team's reader role.
pub const ATHENA_TOOL: &str = "aws_athena_query";

/// One tool the skill calls, by the name the skill uses, and where it comes
/// from: `server` is a connected MCP server's id, `sandbox:<id>` for a stdio
/// server the run starts in its sandbox, or `coderhelm:<name>` for one of
/// CoderHelm's own connections; `tool` is its name there.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ToolGrant {
    /// The name the skill (and so the agent) calls it by:
    /// `traffic_counts`, `run_query`.
    pub name: String,
    pub server: String,
    /// The server's own name for it; empty means the same as `name`.
    #[serde(default)]
    pub tool: String,
    /// An admin's word on its risk (`read` or `write`), over the server's
    /// annotations and the model's reading. Never lowers a tool that sends,
    /// publishes or deletes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<String>,
    /// Older schedules' "only reads" tick: the same as `risk: read`.
    #[serde(default)]
    pub reads_only: bool,
    /// Added by an admin, not found in the skill: kept when the skill's
    /// tools are read again.
    #[serde(default)]
    pub manual: bool,
}

impl ToolGrant {
    pub fn server_tool(&self) -> &str {
        if self.tool.trim().is_empty() {
            &self.name
        } else {
            &self.tool
        }
    }

    /// The admin's word on its risk, if any.
    pub fn admin_risk(&self) -> Option<Risk> {
        match self.risk.as_deref().and_then(Risk::parse) {
            Some(r) => Some(r),
            None if self.reads_only => Some(Risk::Read),
            None => None,
        }
    }

    /// Its risk by name alone: what can be checked before its server is
    /// asked (a tool whose name says it sends or deletes is never granted).
    pub fn risk(&self) -> Risk {
        let by_name = classify_external(self.server_tool());
        match by_name {
            Risk::Consequential => Risk::Consequential,
            Risk::Write => self.admin_risk().unwrap_or(Risk::Write),
            Risk::Read => self.admin_risk().unwrap_or(Risk::Read),
        }
    }
}

/// Athena through the team's AWS reader role (`CoderHelmLogReader`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AwsGrant {
    /// The connected AWS account (12 digits).
    pub account: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// The one Athena workgroup queries run in.
    pub workgroup: String,
    /// Data one run may scan, in GB.
    #[serde(default = "default_gb")]
    pub max_gb: f64,
}

fn yes() -> bool {
    true
}

fn default_region() -> String {
    "us-east-1".into()
}
fn default_gb() -> f64 {
    30.0
}

/// What one run may spend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Budget {
    /// Model spend, in USD (the run's allowance: every model call is
    /// refused once it is used).
    pub max_usd: f64,
    /// Wall-clock minutes.
    pub max_minutes: u32,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_usd: DEFAULT_BUDGET_USD,
            max_minutes: DEFAULT_MINUTES,
        }
    }
}

/// Where a run's result goes besides its run page. Each `*_on` is `off`,
/// `always` or `failure`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Destinations {
    /// The team's chat channels (Slack, Teams).
    #[serde(default)]
    pub chat: String,
    /// Open an issue in the skill's repository when a scheduled run fails.
    #[serde(default)]
    pub issue_on_failure: bool,
    /// People emailed through CoderHelm's mailer.
    #[serde(default)]
    pub emails: Vec<String>,
    #[serde(default)]
    pub email_on: String,
    /// A Jira ticket each run comments on, through the team's Jira app.
    #[serde(default)]
    pub jira_ticket: String,
    #[serde(default)]
    pub jira_comment_on: String,
    /// A Jira project a failed scheduled run files a ticket in.
    #[serde(default)]
    pub jira_project: String,
}

/// Whether a destination set to `on` hears about a run that `failed`.
pub fn wants(on: &str, failed: bool) -> bool {
    on == "always" || (on == "failure" && failed)
}

/// The last run, kept on the schedule for the list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LastRun {
    pub run_id: String,
    pub status: String,
    pub at: String,
}

/// One schedule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Schedule {
    pub id: String,
    /// The skill: its repository and the path of its `SKILL.md`.
    pub skill_repo: String,
    pub skill_path: String,
    pub skill_name: String,
    /// Five-field cron line, read in `timezone`.
    pub cron: String,
    pub timezone: String,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub tools: Vec<ToolGrant>,
    /// Read the skill's tools again before each run and map any new one
    /// (on unless an admin turned it off).
    #[serde(default = "yes")]
    pub auto_tools: bool,
    /// Stdio MCP servers the run starts in its sandbox, pinned.
    #[serde(default)]
    pub stdio: Vec<stdio::StdioServer>,
    /// What the last reading of the skill's tools found.
    #[serde(default)]
    pub resolution: Option<resolve::Resolution>,
    #[serde(default)]
    pub aws: Option<AwsGrant>,
    /// Off by default: a tool that writes is refused before it runs.
    #[serde(default)]
    pub allow_writes: bool,
    /// The skill's memory folder(s) inside its output folder, kept between
    /// runs (`snapshots`).
    #[serde(default)]
    pub state_dirs: Vec<String>,
    #[serde(default)]
    pub budget: Budget,
    #[serde(default)]
    pub notify: Destinations,
    /// What the run is asked, besides the skill (the skill's own words for
    /// a run, like "Run the weekly visit count").
    #[serde(default)]
    pub instructions: String,
    #[serde(default)]
    pub created_by: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    /// The run in progress, so two never run at once.
    #[serde(default)]
    pub running: Option<LastRun>,
    #[serde(default)]
    pub last_run: Option<LastRun>,
}

/// A run of a schedule, as its history lists it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RunEntry {
    pub run_id: String,
    pub schedule_id: String,
    /// `manual` or `schedule`.
    pub trigger: String,
    pub requested_by: String,
    /// `queued`, `running`, `completed`, `failed` or `needs_access`.
    pub status: String,
    pub started_at: String,
    #[serde(default)]
    pub finished_at: String,
    #[serde(default)]
    pub summary: String,
    /// The run's outputs, and the report among them (an `.html`).
    #[serde(default)]
    pub outputs: Vec<String>,
    #[serde(default)]
    pub report: String,
    #[serde(default)]
    pub needs_access: Vec<String>,
    #[serde(default)]
    pub cost_usd: f64,
    #[serde(default)]
    pub tokens: u64,
}

/// One run asked of the worker: "Run now" carries the run id the dashboard
/// opens; a schedule's fire carries none and the worker makes one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SkillRunMessage {
    pub team_id: String,
    pub schedule_id: String,
    #[serde(default)]
    pub run_id: String,
    /// `manual` or `schedule`.
    pub trigger: String,
    #[serde(default)]
    pub requested_by: String,
    /// The step a long run goes on from (its saved state); empty for its
    /// first step.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub resume: String,
    /// `run` (default) or `resolve`: only read the skill's tools and map
    /// them, for the schedule's page.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mode: String,
}

/// A schedule being made: its skill, and what reading the skill's tools
/// found, for the form (`SKILLRESOLVE#<id>`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Draft {
    pub id: String,
    pub skill_repo: String,
    pub skill_path: String,
    pub skill_name: String,
    #[serde(default)]
    pub aws: Option<AwsGrant>,
    #[serde(default)]
    pub resolution: Option<resolve::Resolution>,
    #[serde(default)]
    pub done: bool,
    #[serde(default)]
    pub created_at: String,
}

pub fn resolve_sk(id: &str) -> String {
    format!("SKILLRESOLVE#{id}")
}

pub fn schedule_sk(id: &str) -> String {
    format!("{SCHEDULE_PREFIX}{id}")
}

pub fn run_sk(schedule: &str, run: &str) -> String {
    format!("{RUN_PREFIX}{schedule}#{run}")
}

/// Where a schedule's memory folder is kept: `teams/<team>/skill-state/<id>/`.
pub fn state_prefix(team: &str, schedule: &str) -> String {
    format!("teams/{team}/skill-state/{}/", safe(schedule))
}

fn safe(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// A folder name inside the output folder: letters, digits, `-`, `_`, `.`
/// and `/`, never `..`, and not the run's own scratch folder.
pub fn clean_dir(d: &str) -> Result<String, String> {
    let d = d.trim().trim_matches('/').to_string();
    if d.is_empty() || d.len() > 100 {
        return Err("Name the memory folder, like snapshots.".into());
    }
    if d.split('/').any(|p| p.is_empty() || p == "." || p == "..")
        || !d
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'))
    {
        return Err(format!(
            "'{d}' isn't a plain folder name inside the output folder."
        ));
    }
    if d == engine::WORK_DIR || d.starts_with(&format!("{}/", engine::WORK_DIR)) {
        return Err(format!(
            "'{}' is the run's scratch folder; it is deleted after every run.",
            engine::WORK_DIR
        ));
    }
    Ok(d)
}

/// A tool name the agent can be given.
pub fn valid_tool_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= 64
        && n.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Checks a schedule an admin saved and puts it in its stored form, or says
/// what to change.
pub fn validate(s: &mut Schedule) -> Result<(), String> {
    if s.skill_repo.trim().is_empty() || !s.skill_path.ends_with("SKILL.md") {
        return Err("Pick a skill.".into());
    }
    cron::parse(&s.cron)?;
    s.cron = s.cron.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.timezone.trim().is_empty() {
        s.timezone = "UTC".into();
    }
    cron::time_zone(&s.timezone)?;
    s.timezone = s.timezone.trim().to_string();
    if s.tools.len() > MAX_TOOLS {
        return Err(format!("A schedule grants at most {MAX_TOOLS} tools."));
    }
    let mut names = std::collections::BTreeSet::new();
    for t in &mut s.tools {
        t.name = t.name.trim().to_string();
        t.server = t.server.trim().to_string();
        t.tool = t.tool.trim().to_string();
        if !valid_tool_name(&t.name) || (!t.tool.is_empty() && !valid_tool_name(&t.tool)) {
            return Err(format!(
                "'{}' isn't a tool name: use letters, digits, '_' and '-'.",
                t.name
            ));
        }
        if t.server.is_empty() {
            return Err(format!("Pick the connected server that offers {}.", t.name));
        }
        if engine::is_own_tool(&t.name) || t.name == ATHENA_TOOL {
            return Err(format!("'{}' is one of CoderHelm's own tools.", t.name));
        }
        if !names.insert(t.name.clone()) {
            return Err(format!("{} is listed twice.", t.name));
        }
        if t.risk() == Risk::Consequential {
            return Err(format!(
                "{} sends, publishes or deletes; a skill run never calls such a tool.",
                t.name
            ));
        }
    }
    if let Some(a) = &mut s.aws {
        a.account = a.account.trim().to_string();
        a.workgroup = a.workgroup.trim().to_string();
        if a.account.len() != 12 || !a.account.chars().all(|c| c.is_ascii_digit()) {
            return Err("Pick the connected AWS account (12 digits).".into());
        }
        if a.workgroup.is_empty()
            || a.workgroup.len() > 128
            || !a
                .workgroup
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            return Err("Name the Athena workgroup the queries run in.".into());
        }
        if !(a.max_gb > 0.0 && a.max_gb <= MAX_ATHENA_GB) {
            return Err(format!(
                "Athena data per run is between 0 and {MAX_ATHENA_GB} GB."
            ));
        }
    }
    let mut dirs = Vec::new();
    for d in &s.state_dirs {
        if d.trim().is_empty() {
            continue;
        }
        let d = clean_dir(d)?;
        if !dirs.contains(&d) {
            dirs.push(d);
        }
    }
    s.state_dirs = dirs;
    if !(s.budget.max_usd > 0.0 && s.budget.max_usd <= MAX_BUDGET_USD) {
        return Err(format!(
            "The model budget per run is between $0 and ${MAX_BUDGET_USD}."
        ));
    }
    if s.budget.max_minutes == 0 || s.budget.max_minutes > MAX_MINUTES {
        return Err(format!(
            "The time budget per run is 1 to {MAX_MINUTES} minutes."
        ));
    }
    let when = |v: &str| -> Result<String, String> {
        match v.trim() {
            "" | "off" => Ok("off".into()),
            "always" | "failure" => Ok(v.trim().into()),
            other => Err(format!("'{other}' isn't off, always or failure.")),
        }
    };
    s.notify.chat = when(&s.notify.chat)?;
    s.notify.email_on = when(&s.notify.email_on)?;
    s.notify.jira_comment_on = when(&s.notify.jira_comment_on)?;
    let mut emails = Vec::new();
    for e in &s.notify.emails {
        let e = e.trim().to_ascii_lowercase();
        if e.is_empty() {
            continue;
        }
        if !crate::mail::relay::valid_address(&e) {
            return Err(format!("'{e}' isn't an email address."));
        }
        if !emails.contains(&e) {
            emails.push(e);
        }
    }
    if emails.len() > 20 {
        return Err("Email at most 20 people.".into());
    }
    s.notify.emails = emails;
    s.notify.jira_ticket = s.notify.jira_ticket.trim().to_ascii_uppercase();
    if !s.notify.jira_ticket.is_empty()
        && !crate::git_conventions::is_ticket_key(&s.notify.jira_ticket)
    {
        return Err(format!(
            "'{}' isn't a Jira ticket key.",
            s.notify.jira_ticket
        ));
    }
    s.notify.jira_project = s.notify.jira_project.trim().to_ascii_uppercase();
    if !s.notify.jira_project.is_empty()
        && !(s.notify.jira_project.len() <= 20
            && s.notify
                .jira_project
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
    {
        return Err(format!(
            "'{}' isn't a Jira project key.",
            s.notify.jira_project
        ));
    }
    if s.stdio.len() > 5 {
        return Err("A schedule runs at most 5 sandbox servers.".into());
    }
    if s.instructions.len() > 2_000 {
        return Err("Keep the instructions under 2,000 characters.".into());
    }
    Ok(())
}

/// The schedule's message to the worker, as its EventBridge schedule sends it.
pub fn fire_message(team: &str, schedule: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "skill_run",
        "team_id": team,
        "schedule_id": schedule,
        "trigger": "schedule",
        "requested_by": "schedule",
    })
}

/// The EventBridge schedule's name for one schedule.
pub fn eventbridge_name(team: &str, schedule: &str) -> String {
    crate::plugins::schedule::name("skill", team, schedule)
}

// ── Records ─────────────────────────────────────────────────────────────────

pub async fn get(kv: &dyn Kv, team: &str, id: &str) -> Result<Option<(Schedule, u64)>, String> {
    kv::get_as(kv, team, &schedule_sk(id)).await
}

pub async fn list(kv: &dyn Kv, team: &str) -> Result<Vec<Schedule>, String> {
    let recs = kv
        .query(team, SCHEDULE_PREFIX, false, Some(MAX_SCHEDULES + 5), None)
        .await?;
    Ok(recs
        .into_iter()
        .filter_map(|r| serde_json::from_value(r.data).ok())
        .collect())
}

pub async fn put(kv: &dyn Kv, team: &str, s: &Schedule, expect: Expect) -> Result<u64, KvError> {
    kv::put_as(kv, team, &schedule_sk(&s.id), s, expect).await
}

pub async fn delete(kv: &dyn Kv, team: &str, id: &str) -> Result<(), String> {
    kv.delete(team, &schedule_sk(id)).await
}

pub async fn put_run(kv: &dyn Kv, team: &str, r: &RunEntry) -> Result<(), String> {
    kv::put_as(kv, team, &run_sk(&r.schedule_id, &r.run_id), r, Expect::Any)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

pub async fn get_run(
    kv: &dyn Kv,
    team: &str,
    schedule: &str,
    run: &str,
) -> Result<Option<RunEntry>, String> {
    Ok(kv::get_as(kv, team, &run_sk(schedule, run))
        .await?
        .map(|(r, _)| r))
}

/// A schedule's runs, newest first.
pub async fn runs(kv: &dyn Kv, team: &str, schedule: &str) -> Result<Vec<RunEntry>, String> {
    let recs = kv
        .query(
            team,
            &format!("{RUN_PREFIX}{schedule}#"),
            true,
            Some(HISTORY),
            None,
        )
        .await?;
    Ok(recs
        .into_iter()
        .filter_map(|r| serde_json::from_value(r.data).ok())
        .collect())
}

/// Marks `run` as the schedule's run in progress, unless another run still
/// is (and isn't stale). Conditional on the revision read, so two starts at
/// once can't both win. `Err` says why it can't start.
pub async fn claim(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    run: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Schedule, String> {
    for _ in 0..3 {
        let Some((mut s, rev)) = get(kv, team, id).await? else {
            return Err("That schedule was deleted.".into());
        };
        if let Some(r) = &s.running {
            let fresh = chrono::DateTime::parse_from_rfc3339(&r.at)
                .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds() < STALE_RUN_SECS)
                .unwrap_or(false);
            if fresh && r.run_id != run {
                return Err(format!(
                    "Run {} of this schedule is still working; it runs one at a time.",
                    r.run_id
                ));
            }
        }
        s.running = Some(LastRun {
            run_id: run.to_string(),
            status: "running".into(),
            at: now.to_rfc3339(),
        });
        match put(kv, team, &s, Expect::Rev(rev)).await {
            Ok(_) => return Ok(s),
            Err(KvError::Conflict) => continue,
            Err(e) => return Err(e.to_string()),
        }
    }
    Err("The schedule kept changing; try again.".into())
}

/// The run ended: the schedule is free and shows it as its last run.
pub async fn release(kv: &dyn Kv, team: &str, id: &str, last: LastRun) {
    for _ in 0..3 {
        let Ok(Some((mut s, rev))) = get(kv, team, id).await else {
            return;
        };
        if s.running.as_ref().is_some_and(|r| r.run_id == last.run_id) {
            s.running = None;
        }
        s.last_run = Some(last.clone());
        match put(kv, team, &s, Expect::Rev(rev)).await {
            Err(KvError::Conflict) => continue,
            _ => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::kv::Memory;

    pub fn sample() -> Schedule {
        Schedule {
            id: "01SCHED".into(),
            skill_repo: "acme/skills".into(),
            skill_path: "skills/visit-count/SKILL.md".into(),
            skill_name: "visit-count".into(),
            cron: " 23  7 * * MON ".into(),
            timezone: "America/New_York".into(),
            tools: vec![ToolGrant {
                name: "traffic_counts".into(),
                server: "acme-traffic".into(),
                ..Default::default()
            }],
            state_dirs: vec!["snapshots/".into(), "".into(), "snapshots".into()],
            ..Default::default()
        }
    }

    #[test]
    fn a_schedule_is_checked_and_kept_in_its_stored_form() {
        let mut s = sample();
        validate(&mut s).unwrap();
        assert_eq!(s.cron, "23 7 * * MON");
        assert_eq!(s.state_dirs, ["snapshots"]);
        assert_eq!(s.notify.chat, "off");
        assert_eq!(s.budget, Budget::default());
    }

    #[test]
    fn schedules_that_cant_run_safely_are_refused() {
        type Change = Box<dyn Fn(&mut Schedule)>;
        let cases: Vec<(Change, &str)> = vec![
            (Box::new(|s| s.cron = "* * *".into()), "five fields"),
            (
                Box::new(|s| s.timezone = "Nowhere/Land".into()),
                "time zone",
            ),
            (
                Box::new(|s| s.tools[0].name = "send_email".into()),
                "never calls",
            ),
            (
                Box::new(|s| s.tools[0].server = "".into()),
                "connected server",
            ),
            (
                Box::new(|s| s.tools[0].name = "run_skill_script".into()),
                "own tools",
            ),
            (
                Box::new(|s| s.state_dirs = vec!["../etc".into()]),
                "plain folder",
            ),
            (Box::new(|s| s.state_dirs = vec!["work".into()]), "scratch"),
            (Box::new(|s| s.budget.max_usd = 500.0), "model budget"),
            (Box::new(|s| s.budget.max_minutes = 0), "time budget"),
            (Box::new(|s| s.notify.chat = "sometimes".into()), "always"),
            (
                Box::new(|s| {
                    s.aws = Some(AwsGrant {
                        account: "12".into(),
                        workgroup: "wg".into(),
                        ..Default::default()
                    })
                }),
                "AWS account",
            ),
        ];
        for (change, says) in cases {
            let mut s = sample();
            change(&mut s);
            let e = validate(&mut s).unwrap_err();
            assert!(e.contains(says), "{says}: {e}");
        }
    }

    #[test]
    fn a_tool_whose_name_doesnt_say_it_reads_runs_as_a_read_only_when_an_admin_vouches() {
        let mut t = ToolGrant {
            name: "run_query".into(),
            server: "metrics".into(),
            ..Default::default()
        };
        assert_eq!(t.risk(), Risk::Write);
        t.reads_only = true;
        assert_eq!(t.risk(), Risk::Read);
        let send = ToolGrant {
            name: "notify_team".into(),
            server: "x".into(),
            reads_only: true,
            ..Default::default()
        };
        assert_eq!(send.risk(), Risk::Consequential);
        assert_eq!(
            ToolGrant {
                name: "top_pages".into(),
                tool: "get_top".into(),
                server: "x".into(),
                ..Default::default()
            }
            .risk(),
            Risk::Read
        );
    }

    #[tokio::test]
    async fn one_run_at_a_time_and_a_stale_one_doesnt_block() {
        let kv = Memory::default();
        let mut s = sample();
        validate(&mut s).unwrap();
        put(&kv, "T", &s, Expect::Absent).await.unwrap();
        let now = chrono::Utc::now();
        claim(&kv, "T", &s.id, "r1", now).await.unwrap();
        let e = claim(&kv, "T", &s.id, "r2", now).await.unwrap_err();
        assert!(e.contains("one at a time"), "{e}");
        // Three hours later the first no longer blocks.
        claim(&kv, "T", &s.id, "r2", now + chrono::Duration::hours(3))
            .await
            .unwrap();
        release(
            &kv,
            "T",
            &s.id,
            LastRun {
                run_id: "r2".into(),
                status: "completed".into(),
                at: now.to_rfc3339(),
            },
        )
        .await;
        let (s, _) = get(&kv, "T", &s.id).await.unwrap().unwrap();
        assert!(s.running.is_none());
        assert_eq!(s.last_run.unwrap().status, "completed");
        assert!(claim(&kv, "T", "gone", "r3", now).await.is_err());
    }

    #[test]
    fn the_fire_message_is_the_worker_message() {
        let m: crate::messages::WorkerMessage =
            serde_json::from_value(fire_message("TEAM#a", "01S")).unwrap();
        match m {
            crate::messages::WorkerMessage::SkillRun(m) => {
                assert_eq!(m.schedule_id, "01S");
                assert_eq!(m.trigger, "schedule");
                assert!(m.run_id.is_empty());
            }
            _ => panic!("not a skill run"),
        }
        assert!(eventbridge_name("TEAM#a", "01S").len() <= 64);
    }
}
