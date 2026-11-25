//! One skill run, start to end, behind traits so it runs in tests against
//! fakes and in the worker against AWS.
//!
//! 1. **Claim** the schedule (one run at a time) and list the run with the
//!    team's runs (`ticket_source` `skill_run`).
//! 2. **Read the skill's folder** at the synced commit: its `SKILL.md` and
//!    every file next to it (the scripts too, whatever their size).
//! 3. **Find its tools** ([`super::resolve`]): every tool the skill names,
//!    mapped to the team's connected MCP servers, to stdio servers found in
//!    its repositories (started in the run's sandbox, [`super::stdio`]), and
//!    to CoderHelm's own connections. Each tool's risk comes from its
//!    server's annotations, else one cached model check, else an admin's
//!    word ([`super::risk`]); nobody ticks boxes.
//! 4. **Check access** before the agent starts: every tool must be offered
//!    and every role taken on. Anything missing ends the run as
//!    `needs_access`, naming each one, with nothing else written.
//! 5. **Restore the memory folder** kept by the last run.
//! 6. **Run the agent** with the skill as its instructions and only this
//!    run's tools, writes refused unless the schedule allows them. A run
//!    longer than one worker step saves where it is and goes on in the next
//!    one ([`Step::More`]), its sandbox servers still running.
//! 7. **Keep the result**: the files the run made are its outputs, the
//!    memory folder is kept for the next run.
//! 8. **Tell** the run page, the schedule's history and, as chosen, chat,
//!    email, Jira and an issue on a failed scheduled run.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::resolve::{self, Offered, Resolution, ResolvedTool};
use super::risk::{self, Basis, RiskJudge, Verdict};
use super::stdio::{RoleKeys, Session, StdioHost, StdioServer, SOURCE_PREFIX};
use super::{AwsGrant, LastRun, RunEntry, Schedule, SkillRunMessage, ToolGrant, ATHENA_TOOL};
use crate::plugins::connect::{Connections, ToolDef};
use crate::plugins::kv::{Expect, Kv};
use crate::plugins::run::ToolExec;
use crate::tool_registry::Risk;

/// The output folder's scratch folder (`mktemp -d` on a laptop): dropped
/// when the run ends, never an output.
pub const WORK_DIR: &str = "work";
/// Script runs one skill run may start.
pub const MAX_SCRIPTS: usize = 30;
/// Files the output folder holds at most, and their size together.
pub const MAX_FILES: usize = 400;
pub const MAX_BYTES: usize = 20 * 1024 * 1024;
/// Memory files kept per memory folder (a year of weekly snapshots).
pub const MAX_STATE_FILES: usize = 60;
/// Characters of a tool's result handed to the model.
const MAX_BACK: usize = 20_000;
/// Characters shown when a result went to a file instead.
const PREVIEW: usize = 1_500;
/// How long one tool call may run past the step's end.
const CALL_GRACE: Duration = Duration::from_secs(4 * 60);
/// The source of CoderHelm's own connections in a grant.
pub const OWN_PREFIX: &str = "coderhelm:";

pub const RUN_SCRIPT: &str = "run_skill_script";
pub const READ_SKILL_FILE: &str = "read_skill_file";
pub const WRITE_FILE: &str = "write_work_file";
pub const READ_FILE: &str = "read_work_file";
pub const CALL_TO_FILE: &str = "call_to_file";
pub const NEEDS_ACCESS: &str = "stop_needs_access";

/// The tools every skill run has, besides the granted ones.
pub fn is_own_tool(name: &str) -> bool {
    matches!(
        name,
        RUN_SCRIPT | READ_SKILL_FILE | WRITE_FILE | READ_FILE | CALL_TO_FILE | NEEDS_ACCESS
    )
}

// ── The seams ───────────────────────────────────────────────────────────────

/// The skill's folder at the commit the team synced.
#[async_trait::async_trait]
pub trait SkillSource: Send + Sync {
    /// Every file in the folder of `skill_path`, by its path inside that
    /// folder (`SKILL.md`, `scan.py`, `lib/util.py`).
    async fn files(
        &self,
        team: &str,
        repo: &str,
        skill_path: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, String>;
}

/// What one script run came to. `files` is the whole output folder after
/// it; `complete` is false when some files couldn't be brought back (then
/// the folder is merged, never replaced).
#[derive(Debug, Default, Clone)]
pub struct ScriptResult {
    pub ran: bool,
    pub exit_code: Option<i32>,
    pub output: String,
    pub files: BTreeMap<String, String>,
    pub complete: bool,
}

/// The code sandbox: runs `cmd` from the skill's folder with the output
/// folder at `$CODERHELM_OUT`.
#[async_trait::async_trait]
pub trait Sandbox: Send + Sync {
    async fn run(
        &self,
        key: &str,
        skill: &[(String, Vec<u8>)],
        out: &BTreeMap<String, String>,
        cmd: &str,
        deadline: Instant,
    ) -> Result<ScriptResult, String>;
}

/// What Athena said.
#[derive(Debug, Clone, PartialEq)]
pub struct AthenaResult {
    /// Rows as tab-separated lines, the header first.
    pub text: String,
    pub scanned_bytes: u64,
}

/// A refusal a run can't go on from, or another error.
#[derive(Debug, Clone, PartialEq)]
pub enum AwsError {
    Access(String),
    Other(String),
}

/// Athena through the team's AWS reader role.
#[async_trait::async_trait]
pub trait Athena: Send + Sync {
    /// Takes on the role (and checks the workgroup can be read).
    async fn check(&self, team: &str, grant: &AwsGrant) -> Result<(), AwsError>;
    async fn query(
        &self,
        team: &str,
        grant: &AwsGrant,
        sql: &str,
        database: &str,
    ) -> Result<AthenaResult, AwsError>;
}

/// Files in the data bucket: the memory folder, the run's outputs and a
/// long run's saved state.
#[async_trait::async_trait]
pub trait Files: Send + Sync {
    async fn list(&self, prefix: &str) -> Result<Vec<String>, String>;
    async fn get(&self, key: &str) -> Result<Option<String>, String>;
    async fn put(&self, key: &str, body: &str) -> Result<(), String>;
    async fn delete(&self, key: &str) -> Result<(), String>;
}

/// The run as the team's run list shows it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunRow {
    pub run_id: String,
    pub schedule_id: String,
    pub skill_name: String,
    pub repo: String,
    /// `queued`, `running`, `completed` or `failed` (a run that needs
    /// access is `failed` there, with the reason).
    pub status: String,
    pub error: String,
    pub requested_by: String,
    pub created_at: String,
    pub tokens: u64,
    pub cost_usd: f64,
}

/// The team's runs table, a run's progress notes, and the queue a long run
/// goes on through.
#[async_trait::async_trait]
pub trait RunLog: Send + Sync {
    async fn write(&self, team: &str, row: &RunRow);
    async fn note(&self, team: &str, run: &str, text: &str);
    /// Sends `msg` (a long run's next step) to the worker.
    async fn continue_later(&self, msg: &SkillRunMessage) -> Result<(), String>;
}

/// Where a result goes besides the run page.
#[async_trait::async_trait]
pub trait Notify: Send + Sync {
    /// The team's chat channels; `false` when it has none.
    async fn chat(&self, team: &str, title: &str, lines: &[String], url: &str) -> bool;
    /// An issue in `repo`; returns its link.
    async fn issue(
        &self,
        team: &str,
        repo: &str,
        title: &str,
        body: &str,
    ) -> Result<String, String>;
    /// One email to each of `to`, through CoderHelm's mailer.
    async fn email(&self, team: &str, to: &[String], title: &str, lines: &[String], url: &str);
    /// A comment on a Jira ticket, through the team's Jira app.
    async fn jira_comment(&self, team: &str, ticket: &str, body: &str) -> Result<(), String>;
    /// A new Jira ticket in `project`; returns its key.
    async fn jira_ticket(
        &self,
        team: &str,
        project: &str,
        title: &str,
        body: &str,
    ) -> Result<String, String>;
}

/// How far one agent step got.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// The agent finished; its final text.
    Done(String),
    /// The worker step's time is up: the conversation is saved and goes on
    /// in the next step.
    More,
}

/// The agent loop, held to `budget_usd` of model spend and `deadline`. The
/// conversation (`convo`, Messages API `{role, content}` entries) is the
/// run's own: a step appends to it, and the next step goes on from it.
#[async_trait::async_trait]
pub trait Agent: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn run(
        &self,
        team: &str,
        system: &str,
        tools: &[ToolDef],
        exec: &dyn ToolExec,
        budget_usd: f64,
        deadline: Instant,
        convo: &mut Vec<Value>,
    ) -> Result<(Step, u64, f64), String>;
}

/// CoderHelm's own connections offering a tool a skill names in the shape a
/// well-known MCP server gives it (`coderhelm:<name>`).
#[async_trait::async_trait]
pub trait Builtins: Send + Sync {
    async fn offered(&self, team: &str) -> Vec<Offered>;
    async fn call(
        &self,
        team: &str,
        source: &str,
        tool: &str,
        args: &Value,
    ) -> Result<String, String>;
}

/// The team's servers: connected MCP servers, its connected AWS accounts,
/// and stdio servers its repositories declare.
#[async_trait::async_trait]
pub trait Servers: Send + Sync {
    /// Connected MCP servers: (id, short name a skill may use).
    async fn connected(&self, team: &str) -> Vec<(String, String)>;
    /// Connected AWS accounts with CoderHelm's read-only role.
    async fn aws_accounts(&self, team: &str) -> Vec<String>;
    /// Stdio servers in the team's repositories for tool families
    /// (`acme` of `acme_cdn_top`), pinned at their default branch's head,
    /// each with whether it reads AWS; and why a repository couldn't be read.
    async fn find(
        &self,
        team: &str,
        families: &[String],
    ) -> (Vec<(StdioServer, bool)>, Vec<String>);
}

/// Records a read CoderHelm's role was refused, for the install's role
/// update (`aws_role_reads`).
#[async_trait::async_trait]
pub trait AccessGaps: Send + Sync {
    async fn record(&self, team: &str, account: &str, actions: &[String], needed_for: &str);
}

pub struct Deps<'a> {
    pub kv: &'a dyn Kv,
    pub source: &'a dyn SkillSource,
    pub conns: &'a dyn Connections,
    pub athena: &'a dyn Athena,
    pub sandbox: &'a dyn Sandbox,
    pub files: &'a dyn Files,
    pub runs: &'a dyn RunLog,
    pub notify: &'a dyn Notify,
    pub agent: &'a dyn Agent,
    pub stdio: &'a dyn StdioHost,
    pub keys: &'a dyn RoleKeys,
    pub judge: &'a dyn RiskJudge,
    pub builtins: &'a dyn Builtins,
    pub servers: &'a dyn Servers,
    pub gaps: &'a dyn AccessGaps,
    /// The dashboard, for links (`https://app.example.com`).
    pub app_url: &'a str,
    /// How long one worker step runs the agent before saving and going on.
    pub step: Duration,
}

// ── Pure parts ──────────────────────────────────────────────────────────────

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// One argument for bash, with `{out}`, `{work}` and `{skill}` left to the
/// shell as the output folder, the scratch folder and the skill's folder.
fn arg(a: &str) -> String {
    let mut out = String::new();
    let mut rest = a;
    let mut lit = String::new();
    while !rest.is_empty() {
        let hit = [
            ("{out}", "\"$CODERHELM_OUT\""),
            ("{work}", "\"$CODERHELM_OUT/work\""),
            ("{skill}", "\"$CLAUDE_SKILL_DIR\""),
        ]
        .into_iter()
        .find(|(k, _)| rest.starts_with(k));
        match hit {
            Some((k, v)) => {
                if !lit.is_empty() {
                    out.push_str(&quote(&lit));
                    lit.clear();
                }
                out.push_str(v);
                rest = &rest[k.len()..];
            }
            None => {
                let c = rest.chars().next().unwrap();
                lit.push(c);
                rest = &rest[c.len_utf8()..];
            }
        }
    }
    if !lit.is_empty() || out.is_empty() {
        out.push_str(&quote(&lit));
    }
    out
}

/// The command that runs `script` (a file in the skill's folder) with
/// `args`, every argument quoted, from the skill's folder with
/// `CLAUDE_SKILL_DIR` set to it and the scratch folder made.
pub fn script_command(script: &str, args: &[String]) -> Result<String, String> {
    let ext = script.rsplit('.').next().unwrap_or("").to_lowercase();
    let runner = match ext.as_str() {
        "py" => "python3",
        "sh" | "bash" => "bash",
        "js" | "mjs" | "cjs" => "node",
        "rb" => "ruby",
        _ => {
            return Err(format!(
                "'{script}' isn't a script the sandbox runs: use a .py, .sh, .js or .rb file."
            ))
        }
    };
    let mut cmd = vec![
        "export CLAUDE_SKILL_DIR=\"$PWD\";".to_string(),
        format!("mkdir -p \"$CODERHELM_OUT/{WORK_DIR}\";"),
        runner.to_string(),
        quote(script),
    ];
    cmd.extend(args.iter().map(|a| arg(a)));
    Ok(cmd.join(" "))
}

/// A path inside the output folder, or why it isn't one.
pub fn clean_file(name: &str) -> Result<String, String> {
    crate::outputs::clean_name(name)
}

/// Pure: whether an error a tool returned means access is missing (a key,
/// a sign-in, a permission), not a passing failure.
pub fn is_access_error(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    [
        "accessdenied",
        "access denied",
        "is not authorized to perform",
        "unauthorizedoperation",
        "expiredtoken",
        "invalidclienttokenid",
        "unrecognizedclientexception",
        "security token included in the request is",
        "unable to locate credentials",
        "401 unauthorized",
        "403 forbidden",
        "status 401",
        "status 403",
        "invalid api key",
        "api key is invalid",
        "not connected on the connections page",
        "sign in again",
        "needs a reconnect",
    ]
    .iter()
    .any(|p| t.contains(p))
}

/// Pure: the IAM actions an AWS refusal names (`... is not authorized to
/// perform: cloudfront:ListDistributions on resource ...`).
pub fn refused_actions(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for part in text.split("not authorized to perform:").skip(1) {
        let a: String = part
            .trim_start()
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == ':' || *c == '-')
            .collect();
        if a.contains(':') && !out.contains(&a) {
            out.push(a);
        }
    }
    out
}

/// Pure: an Athena statement that only reads.
pub fn read_only_sql(sql: &str) -> bool {
    let s = sql.trim_start().to_ascii_lowercase();
    let first = s.split_whitespace().next().unwrap_or("");
    matches!(first, "select" | "with" | "show" | "describe")
        && !s.trim_end().trim_end_matches(';').contains(';')
}

/// The memory files to bring back: per memory folder, the newest
/// [`MAX_STATE_FILES`] by name (snapshots are named by date).
pub fn keep_newest(paths: &[String], dirs: &[String]) -> Vec<String> {
    let mut keep = Vec::new();
    for d in dirs {
        let prefix = format!("{d}/");
        let mut mine: Vec<&String> = paths.iter().filter(|p| p.starts_with(&prefix)).collect();
        mine.sort_by(|a, b| b.cmp(a));
        keep.extend(mine.into_iter().take(MAX_STATE_FILES).cloned());
    }
    keep.sort();
    keep
}

/// The run's outputs: the output folder's files the run made or changed,
/// the scratch folder left out.
pub fn made_by_run(
    now: &BTreeMap<String, String>,
    restored: &BTreeMap<String, String>,
) -> Vec<String> {
    now.iter()
        .filter(|(p, _)| *p != WORK_DIR && !p.starts_with(&format!("{WORK_DIR}/")))
        .filter(|(p, body)| restored.get(*p) != Some(*body))
        .map(|(p, _)| p.clone())
        .collect()
}

/// The report among the outputs: an HTML file, under `reports/` first.
pub fn pick_report(outputs: &[String]) -> String {
    let html: Vec<&String> = outputs
        .iter()
        .filter(|o| o.ends_with(".html") || o.ends_with(".htm"))
        .collect();
    html.iter()
        .find(|o| o.starts_with("reports/"))
        .or(html.first())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

fn rel(state_prefix: &str, key: &str) -> Option<String> {
    key.strip_prefix(state_prefix).map(str::to_string)
}

/// Where a long run's state is kept between its steps.
pub fn state_key(team: &str, run: &str) -> String {
    format!("teams/{team}/skill-runs/{run}/state.json")
}

// ── The tools ───────────────────────────────────────────────────────────────

fn tool(name: &str, description: &str, schema: Value) -> ToolDef {
    ToolDef {
        name: name.into(),
        description: description.into(),
        input_schema: schema,
        ..Default::default()
    }
}

/// One granted tool with what it is and how far it may go.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Granted {
    pub grant: ToolGrant,
    pub def: ToolDef,
    pub risk: Risk,
    pub basis: Basis,
}

/// The tools of a run: CoderHelm's own and the granted ones (a tool that
/// sends, publishes or deletes is never offered).
pub fn tools(s: &Schedule, granted: &[Granted]) -> Vec<ToolDef> {
    let mut v = vec![
        tool(
            RUN_SCRIPT,
            "Run a script of the skill's folder in the code sandbox, the way the skill runs it. \
             The script path is relative to the skill's folder (CLAUDE_SKILL_DIR). In the \
             arguments write {out} for the skill's output folder (kept between runs: its memory \
             folder comes back from the last run), {work} for the run's scratch folder and \
             {skill} for the skill's folder. Returns the exit code, what it printed and the \
             output folder's files after it.",
            json!({"type":"object","properties":{
                "script":{"type":"string"},
                "args":{"type":"array","items":{"type":"string"}},
                "reason":{"type":"string"}},"required":["script"]}),
        ),
        tool(
            READ_SKILL_FILE,
            "Read a file of the skill's folder (a script, a JSON baseline).",
            json!({"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}),
        ),
        tool(
            CALL_TO_FILE,
            "Call one of this run's tools and save its full output, unchanged, to a file in the \
             output folder (like work/s1_bot.txt). Use this whenever the skill says to write a \
             tool's output to a file: the output never passes through you. Returns its size and \
             its first lines.",
            json!({"type":"object","properties":{
                "tool":{"type":"string"},
                "args":{"type":"object"},
                "file":{"type":"string"}},"required":["tool","file"]}),
        ),
        tool(
            WRITE_FILE,
            "Write a small text file you made (a route map, notes) in the output folder, like \
             work/notes.json.",
            json!({"type":"object","properties":{
                "name":{"type":"string"},"content":{"type":"string"}},
                "required":["name","content"]}),
        ),
        tool(
            READ_FILE,
            "Read a file of the output folder; without a name, list them.",
            json!({"type":"object","properties":{"name":{"type":"string"}}}),
        ),
        tool(
            NEEDS_ACCESS,
            "Stop the run because access is missing (a sign-in, a key, a permission), naming \
             each check that failed and its error's first line. Nothing else is kept.",
            json!({"type":"object","properties":{
                "missing":{"type":"array","items":{"type":"string"}}},"required":["missing"]}),
        ),
    ];
    if let Some(a) = &s.aws {
        v.push(tool(
            ATHENA_TOOL,
            &format!(
                "One read-only Athena query (SELECT, WITH, SHOW or DESCRIBE) through the team's \
                 AWS reader role, in workgroup {}. Queries run one at a time; this run may scan \
                 {} GB in all. Returns rows as tab-separated lines and a 'Scanned N GB (about \
                 $X)' line.",
                a.workgroup, a.max_gb
            ),
            json!({"type":"object","properties":{
                "sql":{"type":"string"},"database":{"type":"string"}},"required":["sql"]}),
        ));
    }
    for g in granted.iter().filter(|g| g.risk != Risk::Consequential) {
        v.push(ToolDef {
            name: g.grant.name.clone(),
            description: if g.def.description.is_empty() {
                format!("{} of {}", g.grant.server_tool(), g.grant.server)
            } else {
                g.def.description.clone()
            },
            input_schema: if g.def.input_schema.is_object() {
                g.def.input_schema.clone()
            } else {
                json!({"type":"object"})
            },
            ..Default::default()
        });
    }
    v
}

/// What the run tells the agent: the rules of an unattended run on
/// CoderHelm, how a laptop's words map here, then the skill.
pub fn system_prompt(
    s: &Schedule,
    granted: &[Granted],
    missing: &[String],
    skill_md: &str,
    files: &[String],
    today: &str,
) -> String {
    let dirs = if s.state_dirs.is_empty() {
        "none".to_string()
    } else {
        s.state_dirs
            .iter()
            .map(|d| format!("`{d}/`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let tools = granted
        .iter()
        .map(|g| {
            format!(
                "- `{}`: {}",
                g.grant.name,
                match g.risk {
                    Risk::Read => "reads",
                    Risk::Write if s.allow_writes => "may change things; allowed by this schedule",
                    Risk::Write => "refused: this run is read-only",
                    Risk::Consequential => "never available: it sends, publishes or deletes",
                }
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let missing = if missing.is_empty() {
        String::new()
    } else {
        format!(
            "- Not available on CoderHelm (no connected server offers them): {}. Where the \
             skill needs one of these, treat it as missing access.\n",
            missing
                .iter()
                .map(|m| format!("`{m}`"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "You run a team skill on CoderHelm, unattended, on its schedule. Nobody watches or \
         answers questions: do the skill's procedure end to end, then write the summary it asks \
         for. Today is {today}.\n\n\
         ## How the skill's laptop words map here\n\
         - `${{CLAUDE_SKILL_DIR}}` is the skill's folder. Its scripts run in the code sandbox with \
           `{RUN_SCRIPT}` (path relative to that folder). Files: {files}.\n\
         - The output folder the skill keeps every week (`--out DIR`, `$DIR`) is `{{out}}`. Its \
           memory folder ({dirs}) was brought back from the last run and is kept after this one: \
           never delete it.\n\
         - A work directory (`mktemp -d`, `$WORK`) is `{{work}}`, the output folder's `{WORK_DIR}/`. \
           Where the skill says to write a tool's output to a file, use `{CALL_TO_FILE}` with a \
           file under `{WORK_DIR}/`; a small file you write yourself goes with `{WRITE_FILE}`. The \
           scratch folder is deleted when the run ends, so the skill's cleanup step is done for \
           you.\n\
         - Call tools by the names the skill uses (`mcp__server__tool` is the same tool by its \
           name). This run's tools:\n{tools}\n{athena}{missing}\
         - When access is missing (a sign-in, a key, a permission), call `{NEEDS_ACCESS}` with \
           each failed check and stop. Never write a partial report.\n\
         - Writes are {writes}. Nothing you do reaches people: the run's outputs are kept on \
           its run page.\n\n\
         {steps}\n\n## The skill\n\n{skill_md}",
        steps = crate::host_steps::section(
            &[crate::host_steps::Reader::TeamItems],
            &crate::host_steps::standard(true)
        ),
        files = files.join(", "),
        athena = match &s.aws {
            Some(a) => format!(
                "- `{ATHENA_TOOL}`: Athena through the team's AWS reader role, workgroup `{}`, \
                 at most {} GB per run, one query at a time.\n",
                a.workgroup, a.max_gb
            ),
            None => String::new(),
        },
        writes = if s.allow_writes {
            "allowed by this schedule for tools that write"
        } else {
            "refused: this run only reads"
        },
    )
}

// ── Finding the tools ───────────────────────────────────────────────────────

/// The sessions a run's stdio servers run in, by server id.
type Sessions = tokio::sync::Mutex<BTreeMap<String, Session>>;

/// The keys a server reads AWS with, or why there are none.
async fn server_keys(
    deps: &Deps<'_>,
    team: &str,
    s: &StdioServer,
) -> Result<Option<BTreeMap<String, String>>, String> {
    if s.aws_account.is_empty() {
        return Ok(None);
    }
    deps.keys
        .keys(team, &s.aws_account)
        .await
        .map(Some)
        .map_err(|e| {
            format!(
                "{}: CoderHelm's read-only role in AWS account {} ({})",
                s.id,
                s.aws_account,
                first_line(&e)
            )
        })
}

/// Starts `s` and lists its tools.
async fn start_server(
    deps: &Deps<'_>,
    team: &str,
    run: &str,
    s: &StdioServer,
    sessions: &Sessions,
    deadline: Instant,
) -> Result<Vec<ToolDef>, String> {
    let keys = server_keys(deps, team, s).await?;
    let key = format!("{run}-{}", s.id);
    let mut session = deps
        .stdio
        .start(team, &key, s, keys.as_ref(), deadline)
        .await
        .map_err(|e| {
            format!(
                "{}: the server didn't start in the sandbox ({})",
                s.id,
                first_line(&e)
            )
        })?;
    let listed = deps
        .stdio
        .request(&mut session, &json!({"op": "list"}), deadline)
        .await;
    sessions.lock().await.insert(s.id.clone(), session.clone());
    let list = listed.map_err(|e| format!("{}: {}", s.id, first_line(&e)))?;
    Ok(list
        .as_array()
        .map(|a| a.iter().filter_map(ToolDef::from_mcp).collect())
        .unwrap_or_default())
}

/// What finding the tools came to.
struct Found {
    granted: Vec<Granted>,
    missing: Vec<String>,
    needs: Vec<String>,
    resolution: Resolution,
    stdio: Vec<StdioServer>,
}

/// Reads the skill's tools and maps them to the team's sources; starts the
/// stdio servers that offer them; decides each tool's risk.
async fn find_tools(
    deps: &Deps<'_>,
    team: &str,
    run: &str,
    s: &Schedule,
    skill_md: &str,
    sessions: &Sessions,
    deadline: Instant,
) -> Found {
    let refs = resolve::tool_refs(skill_md);
    let named = resolve::named_as_tools(skill_md);
    let mut offered: Vec<Offered> = Vec::new();
    let mut problems = Vec::new();
    let mut needs = Vec::new();
    // Connected MCP servers.
    for (id, alias) in deps.servers.connected(team).await {
        match deps.conns.tools(team, &id).await {
            Ok(ts) => offered.extend(ts.into_iter().map(|t| Offered {
                source: id.clone(),
                alias: alias.clone(),
                tool: t,
            })),
            Err(e) => problems.push(format!("{id}: {}", first_line(&e))),
        }
    }
    // Stdio servers: the schedule's own, and any its repositories declare
    // for tool families nothing else offers.
    let mut stdio = s.stdio.clone();
    if s.auto_tools {
        let unmet: Vec<String> = named
            .iter()
            .filter(|n| !offered.iter().any(|o| &o.tool.name == *n))
            .cloned()
            .collect();
        let fams = resolve::families(&unmet);
        let fams: Vec<String> = fams
            .into_iter()
            .filter(|f| !stdio.iter().any(|x| x.id.starts_with(f.as_str())))
            .collect();
        if !fams.is_empty() {
            let (found, probs) = deps.servers.find(team, &fams).await;
            problems.extend(probs);
            let accounts = deps.servers.aws_accounts(team).await;
            for (mut srv, aws) in found {
                if stdio.iter().any(|x| x.id == srv.id) {
                    continue;
                }
                if aws {
                    srv.aws_account = s
                        .aws
                        .as_ref()
                        .map(|a| a.account.clone())
                        .or_else(|| accounts.first().cloned())
                        .unwrap_or_default();
                    if srv.aws_account.is_empty() {
                        needs.push(format!(
                            "{}: reads AWS, and no AWS account is connected with CoderHelm's \
                             read-only role",
                            srv.id
                        ));
                        continue;
                    }
                }
                stdio.push(srv);
            }
        }
    }
    for srv in &stdio {
        match start_server(deps, team, run, srv, sessions, deadline).await {
            Ok(ts) => offered.extend(ts.into_iter().map(|t| {
                Offered {
                    source: srv.source_id(),
                    alias: srv
                        .id
                        .split(['-', '_'])
                        .next()
                        .unwrap_or(&srv.id)
                        .to_string(),
                    tool: t,
                }
            })),
            Err(e) => needs.push(e),
        }
    }
    offered.extend(deps.builtins.offered(team).await);

    // The grants: what the skill names, kept with an admin's word.
    let (mapped, missing) = resolve::map_refs(&refs, &offered);
    let grants = if s.auto_tools {
        resolve::merge(&s.tools, &mapped)
    } else {
        s.tools.clone()
    };
    let missing: Vec<String> = missing.into_iter().filter(|m| named.contains(m)).collect();

    // Each grant's tool, and its risk.
    let mut by_source: BTreeMap<String, Vec<ToolDef>> = BTreeMap::new();
    for o in &offered {
        by_source
            .entry(o.source.clone())
            .or_default()
            .push(o.tool.clone());
    }
    let mut verdicts: BTreeMap<String, BTreeMap<String, Verdict>> = BTreeMap::new();
    for (source, ts) in &by_source {
        let unsaid: Vec<ToolDef> = ts
            .iter()
            .filter(|t| t.read_only.is_none() && t.destructive.is_none())
            .filter(|t| {
                grants
                    .iter()
                    .any(|g| &g.server == source && g.server_tool() == t.name)
            })
            .cloned()
            .collect();
        if unsaid.is_empty() {
            continue;
        }
        let version = match stdio.iter().find(|x| &x.source_id() == source) {
            Some(x) => x.version_key(),
            None => risk::list_version(source, ts),
        };
        verdicts.insert(
            source.clone(),
            deps.judge.judge(team, &version, &unsaid).await,
        );
    }
    let mut granted = Vec::new();
    for g in grants {
        let def = by_source
            .get(&g.server)
            .and_then(|ts| ts.iter().find(|t| t.name == g.server_tool()))
            .cloned();
        let Some(def) = def else {
            needs.push(format!(
                "{}: {} offers no tool '{}'",
                g.name,
                if g.server.starts_with(SOURCE_PREFIX) {
                    format!("the sandbox server {}", &g.server[SOURCE_PREFIX.len()..])
                } else {
                    format!("the server '{}'", g.server)
                },
                g.server_tool()
            ));
            continue;
        };
        let judged = verdicts.get(&g.server).and_then(|v| v.get(&def.name));
        let (risk, basis) = risk::decide(&def, g.admin_risk(), judged);
        granted.push(Granted {
            grant: g,
            def,
            risk,
            basis,
        });
    }
    let resolution = Resolution {
        memory: resolve::memory_dirs(skill_md),
        at: chrono::Utc::now().to_rfc3339(),
        tools: granted
            .iter()
            .map(|g| ResolvedTool {
                name: g.grant.name.clone(),
                source: g.grant.server.clone(),
                tool: g.grant.server_tool().to_string(),
                risk: g.risk,
                basis: g.basis,
                why: verdicts
                    .get(&g.grant.server)
                    .and_then(|v| v.get(&g.def.name))
                    .map(|v| v.why.clone())
                    .unwrap_or_default(),
            })
            .collect(),
        missing: missing.clone(),
        servers: stdio.clone(),
        problems,
    };
    Found {
        granted,
        missing,
        needs,
        resolution,
        stdio,
    }
}

// ── One run's tool calls ────────────────────────────────────────────────────

struct Exec<'a> {
    deps: &'a Deps<'a>,
    team: &'a str,
    run_id: &'a str,
    s: &'a Schedule,
    skill: &'a [(String, Vec<u8>)],
    granted: &'a [Granted],
    servers: &'a [StdioServer],
    sessions: &'a Sessions,
    out: Mutex<BTreeMap<String, String>>,
    scripts: AtomicUsize,
    athena_bytes: Mutex<u64>,
    athena_lock: tokio::sync::Mutex<()>,
    needs: Mutex<Vec<String>>,
    stopped: AtomicBool,
    /// When this worker step ends (the agent goes on in the next).
    step_end: Instant,
    /// When the whole run's time is used.
    deadline: Instant,
}

impl Exec<'_> {
    fn grant(&self, name: &str) -> Option<&Granted> {
        self.granted.iter().find(|g| {
            g.grant.name == name
                || format!(
                    "mcp__{}__{}",
                    g.grant.server.trim_start_matches(SOURCE_PREFIX),
                    g.grant.server_tool()
                ) == name
        })
    }

    fn call_deadline(&self) -> Instant {
        (self.step_end + CALL_GRACE).min(self.deadline)
    }

    fn need(&self, what: String) {
        let mut n = self.needs.lock().unwrap();
        if !n.contains(&what) {
            n.push(what);
        }
        self.stopped.store(true, Ordering::SeqCst);
    }

    fn put_file(&self, name: &str, body: String) -> Result<String, String> {
        let name = clean_file(name)?;
        let mut out = self.out.lock().unwrap();
        let total: usize = out
            .iter()
            .filter(|(k, _)| **k != name)
            .map(|(_, v)| v.len())
            .sum();
        if total + body.len() > MAX_BYTES {
            return Err(format!(
                "The output folder would pass {} MB.",
                MAX_BYTES / (1024 * 1024)
            ));
        }
        if !out.contains_key(&name) && out.len() >= MAX_FILES {
            return Err(format!(
                "The output folder holds {MAX_FILES} files already."
            ));
        }
        out.insert(name.clone(), body);
        Ok(name)
    }

    /// An AWS refusal from a tool: the role's missing reads, recorded for
    /// the install's role update.
    async fn aws_refusal(&self, tool: &str, account: &str, text: &str) {
        let actions = refused_actions(text);
        if actions.is_empty() {
            self.need(format!("{tool}: {}", first_line(text)));
            return;
        }
        self.deps
            .gaps
            .record(
                self.team,
                account,
                &actions,
                &format!("the skill {}", self.s.skill_name),
            )
            .await;
        self.need(format!(
            "{tool}: CoderHelm's read-only role in AWS account {account} lacks {}. The role's \
             update in Settings adds it.",
            actions.join(", ")
        ));
    }

    /// One call to a granted tool's source.
    async fn source_call(&self, g: &Granted, args: &Value) -> Result<String, String> {
        let server = g.grant.server.as_str();
        if let Some(id) = server.strip_prefix(SOURCE_PREFIX) {
            let srv = self.servers.iter().find(|x| x.id == id);
            let mut sessions = self.sessions.lock().await;
            let Some(session) = sessions.get_mut(id) else {
                return Err(format!("The sandbox server {id} isn't running."));
            };
            let r = self
                .deps
                .stdio
                .request(
                    session,
                    &json!({"op": "call", "name": g.grant.server_tool(), "arguments": args}),
                    self.call_deadline(),
                )
                .await?;
            drop(sessions);
            let text = r["text"].as_str().unwrap_or("").to_string();
            if r["is_error"].as_bool() == Some(true) {
                if is_access_error(&text) {
                    let account = srv.map(|x| x.aws_account.as_str()).unwrap_or("");
                    self.aws_refusal(&g.grant.name, account, &text).await;
                }
                return Err(text);
            }
            return Ok(text);
        }
        if server.starts_with(OWN_PREFIX) {
            return self
                .deps
                .builtins
                .call(self.team, server, g.grant.server_tool(), args)
                .await;
        }
        self.deps
            .conns
            .call(self.team, server, g.grant.server_tool(), args)
            .await
    }

    /// A granted tool or Athena, through the gate. `Err` is the refusal or
    /// the error as the agent sees it.
    async fn gated(&self, name: &str, args: &Value) -> Result<String, String> {
        if name == ATHENA_TOOL {
            return self.athena(args).await;
        }
        let Some(g) = self.grant(name) else {
            return Err(format!("'{name}' isn't one of this run's tools."));
        };
        match g.risk {
            Risk::Read => {}
            Risk::Write if self.s.allow_writes => {}
            Risk::Write => {
                return Err(format!(
                    "Refused: {} may change something, and this run is read-only. The \
                     schedule's settings allow writes; this one doesn't.",
                    g.grant.name
                ))
            }
            Risk::Consequential => {
                return Err(format!(
                    "Refused: {} sends, publishes or deletes.",
                    g.grant.name
                ))
            }
        }
        let args = if args.is_object() {
            args.clone()
        } else {
            json!({})
        };
        match self.source_call(g, &args).await {
            Ok(t) => {
                if t.len() < 400 && is_access_error(&t) && !self.stopped.load(Ordering::SeqCst) {
                    self.need(format!("{}: {}", g.grant.name, first_line(&t)));
                }
                Ok(t)
            }
            Err(e) => {
                if is_access_error(&e) && !self.stopped.load(Ordering::SeqCst) {
                    self.need(format!("{}: {}", g.grant.name, first_line(&e)));
                }
                Err(e)
            }
        }
    }

    async fn athena(&self, args: &Value) -> Result<String, String> {
        let Some(grant) = &self.s.aws else {
            return Err("This run has no AWS role.".into());
        };
        let sql = args["sql"].as_str().unwrap_or("").trim();
        if !read_only_sql(sql) {
            return Err("Refused: only one SELECT, WITH, SHOW or DESCRIBE statement runs.".into());
        }
        let _one = self.athena_lock.lock().await;
        let used = *self.athena_bytes.lock().unwrap();
        let cap = (grant.max_gb * 1e9) as u64;
        if used >= cap {
            return Err(format!(
                "Refused: this run has scanned its {} GB of Athena data.",
                grant.max_gb
            ));
        }
        let db = args["database"].as_str().unwrap_or("");
        match self.deps.athena.query(self.team, grant, sql, db).await {
            Ok(r) => {
                *self.athena_bytes.lock().unwrap() += r.scanned_bytes;
                let gb = r.scanned_bytes as f64 / 1e9;
                Ok(format!(
                    "{}\nScanned {gb:.2} GB (about ${:.2})",
                    r.text,
                    gb * 5.0 / 1000.0
                ))
            }
            Err(AwsError::Access(e)) => {
                self.aws_refusal(ATHENA_TOOL, &grant.account, &e).await;
                Err(e)
            }
            Err(AwsError::Other(e)) => Err(e),
        }
    }

    async fn run_script(&self, input: &Value) -> String {
        let script = input["script"].as_str().unwrap_or("").trim();
        let script = script
            .trim_start_matches("${CLAUDE_SKILL_DIR}/")
            .trim_start_matches("$CLAUDE_SKILL_DIR/")
            .trim_start_matches("./");
        if !self.skill.iter().any(|(p, _)| p == script) {
            return format!(
                "'{script}' isn't a file of the skill's folder. Files: {}",
                self.skill
                    .iter()
                    .map(|(p, _)| p.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if self.scripts.fetch_add(1, Ordering::SeqCst) >= MAX_SCRIPTS {
            return format!("All {MAX_SCRIPTS} script runs of this run are used.");
        }
        let args: Vec<String> = input["args"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let cmd = match script_command(script, &args) {
            Ok(c) => c,
            Err(e) => return e,
        };
        let before = self.out.lock().unwrap().clone();
        let n = self.scripts.load(Ordering::SeqCst);
        let key = format!("{}-{n}", self.run_id);
        let r = match self
            .deps
            .sandbox
            .run(&key, self.skill, &before, &cmd, self.call_deadline())
            .await
        {
            Ok(r) => r,
            Err(e) => return format!("The sandbox couldn't run the script: {e}"),
        };
        {
            let mut out = self.out.lock().unwrap();
            if r.complete {
                *out = r.files.clone();
            } else {
                out.extend(r.files.clone());
            }
        }
        let after = self.out.lock().unwrap().clone();
        let changed: Vec<String> = after
            .keys()
            .filter(|k| before.get(*k) != after.get(*k))
            .cloned()
            .collect();
        self.deps
            .runs
            .note(
                self.team,
                self.run_id,
                &format!(
                    "Ran {script} {}: exit {}",
                    args.first().map(String::as_str).unwrap_or(""),
                    r.exit_code
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "none".into())
                ),
            )
            .await;
        format!(
            "{}\n\nFiles it wrote or changed: {}\nOutput folder: {} files.\n\nWhat it printed:\n{}",
            match (r.ran, r.exit_code) {
                (true, Some(0)) => "The script ran and exited 0.".to_string(),
                (true, Some(c)) => format!("The script ran and exited {c}."),
                _ => "The script didn't run to the end.".to_string(),
            },
            if changed.is_empty() {
                "none".to_string()
            } else {
                changed.join(", ")
            },
            after.len(),
            crate::tail_str(&r.output, MAX_BACK)
        )
    }
}

fn first_line(s: &str) -> String {
    crate::truncate_str(
        s.lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("error")
            .trim(),
        200,
    )
    .to_string()
}

#[async_trait::async_trait]
impl ToolExec for Exec<'_> {
    fn stop(&self) -> bool {
        self.stopped.load(Ordering::SeqCst) || Instant::now() >= self.deadline
    }

    fn step_over(&self) -> bool {
        Instant::now() >= self.step_end
    }

    async fn call(&self, name: &str, input: &Value) -> String {
        if self.stopped.load(Ordering::SeqCst) {
            return format!(
                "Stopped: this run needs access ({}). End now without a report.",
                self.needs.lock().unwrap().join("; ")
            );
        }
        if Instant::now() >= self.deadline {
            return "Stopped: the run's time budget is used. End now.".into();
        }
        match name {
            RUN_SCRIPT => self.run_script(input).await,
            READ_SKILL_FILE => {
                let p = input["path"].as_str().unwrap_or("").trim();
                match self.skill.iter().find(|(f, _)| f == p) {
                    Some((_, b)) => {
                        crate::truncate_str(&String::from_utf8_lossy(b), MAX_BACK).to_string()
                    }
                    None => format!("The skill's folder has no '{p}'."),
                }
            }
            WRITE_FILE => match self.put_file(
                input["name"].as_str().unwrap_or(""),
                input["content"].as_str().unwrap_or("").to_string(),
            ) {
                Ok(n) => format!("Wrote {n}."),
                Err(e) => e,
            },
            READ_FILE => match input["name"].as_str().filter(|n| !n.trim().is_empty()) {
                Some(n) => match self.out.lock().unwrap().get(n.trim()) {
                    Some(b) => crate::truncate_str(b, MAX_BACK).to_string(),
                    None => format!("The output folder has no '{n}'."),
                },
                None => {
                    let out = self.out.lock().unwrap();
                    if out.is_empty() {
                        "The output folder is empty.".into()
                    } else {
                        out.iter()
                            .map(|(k, v)| format!("- {k} ({} bytes)", v.len()))
                            .collect::<Vec<_>>()
                            .join("\n")
                    }
                }
            },
            CALL_TO_FILE => {
                let t = input["tool"].as_str().unwrap_or("").trim();
                let file = input["file"].as_str().unwrap_or("");
                if let Err(e) = clean_file(file) {
                    return e;
                }
                let body = match self.gated(t, &input["args"]).await {
                    Ok(b) => b,
                    Err(e) => return e,
                };
                let len = body.len();
                let preview = crate::truncate_str(&body, PREVIEW).to_string();
                match self.put_file(file, body) {
                    Ok(n) => {
                        self.deps
                            .runs
                            .note(self.team, self.run_id, &format!("Called {t}, saved {n}"))
                            .await;
                        format!("Saved {len} bytes to {n}. It starts:\n{preview}")
                    }
                    Err(e) => e,
                }
            }
            NEEDS_ACCESS => {
                let missing: Vec<String> = input["missing"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(first_line))
                            .collect()
                    })
                    .unwrap_or_default();
                if missing.is_empty() {
                    self.need("access the skill checks for".into());
                }
                for m in missing {
                    self.need(m);
                }
                "Stopped. Nothing of this run is kept but the list of what it needs.".into()
            }
            other => match self.gated(other, input).await {
                Ok(t) => crate::truncate_str(&t, MAX_BACK).to_string(),
                Err(e) => e,
            },
        }
    }
}

// ── The run ─────────────────────────────────────────────────────────────────

fn row(s: &Schedule, e: &RunEntry, error: &str) -> RunRow {
    RunRow {
        run_id: e.run_id.clone(),
        schedule_id: s.id.clone(),
        skill_name: s.skill_name.clone(),
        repo: s.skill_repo.clone(),
        status: match e.status.as_str() {
            "needs_access" => "failed".into(),
            other => other.into(),
        },
        error: error.into(),
        requested_by: e.requested_by.clone(),
        created_at: e.started_at.clone(),
        tokens: e.tokens,
        cost_usd: e.cost_usd,
    }
}

/// The run page's link.
pub fn run_link(app_url: &str, run: &str) -> String {
    format!("{}/runs/detail?id={run}", app_url.trim_end_matches('/'))
}

/// The lines every destination says about an ended run.
pub fn result_lines(e: &RunEntry, error: &str) -> Vec<String> {
    let mut lines = vec![match e.status.as_str() {
        "completed" => format!("Finished: {}", crate::truncate_str(&e.summary, 300)),
        "needs_access" => format!("Needs access: {}", e.needs_access.join("; ")),
        _ => format!("Failed: {error}"),
    }];
    if !e.report.is_empty() {
        lines.push(format!("Report: {}", e.report));
    }
    lines
}

async fn stop_sessions(deps: &Deps<'_>, sessions: &Sessions) {
    for s in sessions.lock().await.values() {
        deps.stdio.stop(s).await;
    }
}

async fn finish(deps: &Deps<'_>, team: &str, s: &Schedule, e: &mut RunEntry, error: &str) {
    e.finished_at = chrono::Utc::now().to_rfc3339();
    let _ = super::put_run(deps.kv, team, e).await;
    deps.runs.write(team, &row(s, e, error)).await;
    deps.runs
        .note(
            team,
            &e.run_id,
            &match e.status.as_str() {
                "completed" => format!("Done: {} outputs kept", e.outputs.len()),
                "needs_access" => format!("Stopped: needs access: {}", e.needs_access.join("; ")),
                _ => format!("Stopped: {error}"),
            },
        )
        .await;
    super::release(
        deps.kv,
        team,
        &s.id,
        LastRun {
            run_id: e.run_id.clone(),
            status: e.status.clone(),
            at: e.finished_at.clone(),
        },
    )
    .await;
    let _ = deps.files.delete(&state_key(team, &e.run_id)).await;
    let failed = e.status != "completed";
    let link = run_link(deps.app_url, &e.run_id);
    let title = format!("Skill run: {}", s.skill_name);
    let lines = result_lines(e, error);
    if super::wants(&s.notify.chat, failed) {
        deps.notify.chat(team, &title, &lines, &link).await;
    }
    if super::wants(&s.notify.email_on, failed) && !s.notify.emails.is_empty() {
        deps.notify
            .email(team, &s.notify.emails, &title, &lines, &link)
            .await;
    }
    if super::wants(&s.notify.jira_comment_on, failed) && !s.notify.jira_ticket.is_empty() {
        let body = format!("{}\n\n{}\n\nRun: {link}", title, lines.join("\n"));
        if let Err(why) = deps
            .notify
            .jira_comment(team, &s.notify.jira_ticket, &body)
            .await
        {
            deps.runs
                .note(
                    team,
                    &e.run_id,
                    &format!("Couldn't comment on {}: {why}", s.notify.jira_ticket),
                )
                .await;
        }
    }
    let issue_title = format!(
        "{} failed {}",
        s.skill_name,
        e.started_at.get(..10).unwrap_or("")
    );
    let issue_body = format!(
        "The scheduled run of the skill `{}` didn't finish.\n\n{}\n\nRun: {link}",
        s.skill_name,
        lines.join("\n")
    );
    if failed && e.trigger == "schedule" && !s.notify.jira_project.is_empty() {
        match deps
            .notify
            .jira_ticket(team, &s.notify.jira_project, &issue_title, &issue_body)
            .await
        {
            Ok(key) => {
                deps.runs
                    .note(team, &e.run_id, &format!("Filed {key}"))
                    .await
            }
            Err(why) => {
                deps.runs
                    .note(
                        team,
                        &e.run_id,
                        &format!("Couldn't file a Jira ticket: {why}"),
                    )
                    .await
            }
        }
    }
    if failed && s.notify.issue_on_failure && e.trigger == "schedule" {
        let _ = deps
            .notify
            .issue(team, &s.skill_repo, &issue_title, &issue_body)
            .await;
    }
}

/// A long run's state between its worker steps.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Saved {
    entry: RunEntry,
    schedule: Schedule,
    granted: Vec<Granted>,
    missing: Vec<String>,
    stdio: Vec<StdioServer>,
    sessions: BTreeMap<String, Session>,
    system: String,
    convo: Vec<Value>,
    restored: BTreeMap<String, String>,
    out: BTreeMap<String, String>,
    athena_bytes: u64,
    scripts: usize,
    /// When the run started, in epoch seconds (its time budget counts from
    /// there across steps).
    started: i64,
    steps: u32,
}

/// One run of a schedule, or its next step. Returns its history entry
/// (`None` when nothing ran: a paused schedule's fire, a deleted schedule,
/// a schedule's fire while a run is still working, or a resolution).
pub async fn execute(deps: &Deps<'_>, msg: &SkillRunMessage) -> Option<RunEntry> {
    if msg.mode == "resolve" {
        resolve_only(deps, msg).await;
        return None;
    }
    if !msg.resume.is_empty() {
        return resume(deps, msg).await;
    }
    let team = msg.team_id.as_str();
    let now = chrono::Utc::now();
    let (s, _) = super::get(deps.kv, team, &msg.schedule_id).await.ok()??;
    if s.paused && msg.trigger == "schedule" {
        return None;
    }
    let run_id = if msg.run_id.is_empty() {
        ulid::Ulid::new().to_string()
    } else {
        msg.run_id.clone()
    };
    let mut e = RunEntry {
        run_id: run_id.clone(),
        schedule_id: s.id.clone(),
        trigger: msg.trigger.clone(),
        requested_by: if msg.requested_by.is_empty() {
            msg.trigger.clone()
        } else {
            msg.requested_by.clone()
        },
        status: "running".into(),
        started_at: now.to_rfc3339(),
        ..Default::default()
    };
    if let Err(why) = super::claim(deps.kv, team, &s.id, &run_id, now).await {
        if msg.trigger == "schedule" {
            return None;
        }
        e.status = "failed".into();
        e.summary = why.clone();
        e.finished_at = now.to_rfc3339();
        let _ = super::put_run(deps.kv, team, &e).await;
        deps.runs.write(team, &row(&s, &e, &why)).await;
        return Some(e);
    }
    let _ = super::put_run(deps.kv, team, &e).await;
    deps.runs.write(team, &row(&s, &e, "")).await;
    deps.runs
        .note(
            team,
            &run_id,
            &format!("Started the skill {} ({})", s.skill_name, msg.trigger),
        )
        .await;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(s.budget.max_minutes as u64 * 60);
    let step_end = started + deps.step;

    // The skill's folder.
    let skill = match deps.source.files(team, &s.skill_repo, &s.skill_path).await {
        Ok(f) if f.iter().any(|(p, _)| p == "SKILL.md") => f,
        Ok(_) => {
            e.status = "failed".into();
            let why = format!("{} has no SKILL.md any more.", s.skill_path);
            e.summary = why.clone();
            finish(deps, team, &s, &mut e, &why).await;
            return Some(e);
        }
        Err(why) => {
            e.status = "failed".into();
            let why = format!("The skill's files couldn't be read: {why}");
            e.summary = why.clone();
            finish(deps, team, &s, &mut e, &why).await;
            return Some(e);
        }
    };
    let skill_md = skill
        .iter()
        .find(|(p, _)| p == "SKILL.md")
        .map(|(_, b)| String::from_utf8_lossy(b).to_string())
        .unwrap_or_default();
    deps.runs
        .note(
            team,
            &run_id,
            &format!("Read the skill's folder: {} files", skill.len()),
        )
        .await;

    // Its tools, and access to each.
    let sessions: Sessions = tokio::sync::Mutex::new(BTreeMap::new());
    let found = find_tools(
        deps,
        team,
        &run_id,
        &s,
        &skill_md,
        &sessions,
        (step_end + CALL_GRACE).min(deadline),
    )
    .await;
    save_resolution(deps, team, &s.id, &found).await;
    let mut needs = found.needs.clone();
    if let Some(a) = &s.aws {
        if let Err(AwsError::Access(why) | AwsError::Other(why)) = deps.athena.check(team, a).await
        {
            needs.push(format!(
                "{ATHENA_TOOL}: the AWS reader role in {} ({})",
                a.account,
                first_line(&why)
            ));
        }
    }
    if !needs.is_empty() {
        stop_sessions(deps, &sessions).await;
        return Some(needs_access(deps, team, &s, e, needs).await);
    }
    deps.runs
        .note(
            team,
            &run_id,
            &format!(
                "Found the skill's tools: {}",
                found
                    .granted
                    .iter()
                    .map(|g| format!("{} ({})", g.grant.name, g.risk.as_str()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )
        .await;

    // The memory folder, as the last run left it: the schedule's, else the
    // one the skill names.
    let mut s = s;
    if s.state_dirs.is_empty() {
        s.state_dirs = found.resolution.memory.clone();
    }
    let prefix = super::state_prefix(team, &s.id);
    let mut restored = BTreeMap::new();
    if !s.state_dirs.is_empty() {
        let keys: Vec<String> = deps
            .files
            .list(&prefix)
            .await
            .unwrap_or_default()
            .iter()
            .filter_map(|k| rel(&prefix, k))
            .collect();
        for p in keep_newest(&keys, &s.state_dirs) {
            if let Ok(Some(body)) = deps.files.get(&format!("{prefix}{p}")).await {
                restored.insert(p, body);
            }
        }
        deps.runs
            .note(
                team,
                &run_id,
                &format!(
                    "Restored {} memory files from the last run ({})",
                    restored.len(),
                    s.state_dirs.join(", ")
                ),
            )
            .await;
    }

    let names: Vec<String> = skill.iter().map(|(p, _)| p.clone()).collect();
    let today = match super::cron::time_zone(&s.timezone) {
        Ok(tz) => now
            .with_timezone(&tz)
            .format("%Y-%m-%d (%A) %Z")
            .to_string(),
        Err(_) => now.format("%Y-%m-%d UTC").to_string(),
    };
    let system = system_prompt(
        &s,
        &found.granted,
        &found.missing,
        &skill_md,
        &names,
        &today,
    );
    let user = if s.instructions.trim().is_empty() {
        format!("Run the skill {} now, end to end.", s.skill_name)
    } else {
        s.instructions.clone()
    };
    let saved = Saved {
        entry: e,
        schedule: s,
        granted: found.granted,
        missing: found.missing,
        stdio: found.stdio,
        sessions: sessions.into_inner(),
        system,
        convo: vec![json!({"role": "user", "content": [{"type": "text", "text": user}]})],
        restored: restored.clone(),
        out: restored,
        athena_bytes: 0,
        scripts: 0,
        started: now.timestamp(),
        steps: 0,
    };
    Some(step(deps, team, saved, skill, step_end, deadline).await)
}

/// Saves what finding the tools came to on the schedule, for its page.
async fn save_resolution(deps: &Deps<'_>, team: &str, id: &str, found: &Found) {
    for _ in 0..3 {
        let Ok(Some((mut s, rev))) = super::get(deps.kv, team, id).await else {
            return;
        };
        s.resolution = Some(found.resolution.clone());
        if s.auto_tools {
            s.tools = found.granted.iter().map(|g| g.grant.clone()).collect();
            s.stdio = found.stdio.clone();
        }
        if super::put(deps.kv, team, &s, Expect::Rev(rev))
            .await
            .is_ok()
        {
            return;
        }
    }
}

/// Reads a draft's or a schedule's tools for its page, without running.
async fn resolve_only(deps: &Deps<'_>, msg: &SkillRunMessage) {
    let team = msg.team_id.as_str();
    let sk = super::resolve_sk(&msg.schedule_id);
    let Ok(Some((mut d, rev))) =
        crate::plugins::kv::get_as::<super::Draft>(deps.kv, team, &sk).await
    else {
        return;
    };
    let s = Schedule {
        id: msg.schedule_id.clone(),
        skill_repo: d.skill_repo.clone(),
        skill_path: d.skill_path.clone(),
        skill_name: d.skill_name.clone(),
        auto_tools: true,
        aws: d.aws.clone(),
        ..Default::default()
    };
    let deadline = Instant::now() + deps.step;
    let res = match deps.source.files(team, &s.skill_repo, &s.skill_path).await {
        Ok(files) => {
            let md = files
                .iter()
                .find(|(p, _)| p == "SKILL.md")
                .map(|(_, b)| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();
            let sessions: Sessions = tokio::sync::Mutex::new(BTreeMap::new());
            let mut found =
                find_tools(deps, team, &msg.schedule_id, &s, &md, &sessions, deadline).await;
            stop_sessions(deps, &sessions).await;
            found.resolution.problems.extend(found.needs.clone());
            found.resolution
        }
        Err(why) => Resolution {
            at: chrono::Utc::now().to_rfc3339(),
            problems: vec![format!("The skill's files couldn't be read: {why}")],
            ..Default::default()
        },
    };
    d.resolution = Some(res);
    d.done = true;
    let _ = crate::plugins::kv::put_as(deps.kv, team, &sk, &d, Expect::Rev(rev)).await;
}

/// The next step of a long run.
async fn resume(deps: &Deps<'_>, msg: &SkillRunMessage) -> Option<RunEntry> {
    let team = msg.team_id.as_str();
    let body = deps.files.get(&msg.resume).await.ok()??;
    let saved: Saved = serde_json::from_str(&body).ok()?;
    let skill = match deps
        .source
        .files(team, &saved.schedule.skill_repo, &saved.schedule.skill_path)
        .await
    {
        Ok(f) => f,
        Err(why) => {
            let mut e = saved.entry;
            e.status = "failed".into();
            let why = format!("The skill's files couldn't be read again: {why}");
            e.summary = why.clone();
            let sessions: Sessions = tokio::sync::Mutex::new(saved.sessions);
            stop_sessions(deps, &sessions).await;
            finish(deps, team, &saved.schedule, &mut e, &why).await;
            return Some(e);
        }
    };
    let elapsed = (chrono::Utc::now().timestamp() - saved.started).max(0) as u64;
    let budget = saved.schedule.budget.max_minutes as u64 * 60;
    let now = Instant::now();
    let deadline = now + Duration::from_secs(budget.saturating_sub(elapsed));
    let step_end = now + deps.step;
    Some(step(deps, team, saved, skill, step_end, deadline).await)
}

/// One worker step of the agent: it ends the run, or saves it and sends the
/// next step.
async fn step(
    deps: &Deps<'_>,
    team: &str,
    mut saved: Saved,
    skill: Vec<(String, Vec<u8>)>,
    step_end: Instant,
    deadline: Instant,
) -> RunEntry {
    let s = saved.schedule.clone();
    let mut e = saved.entry.clone();
    let run_id = e.run_id.clone();
    let sessions: Sessions = tokio::sync::Mutex::new(std::mem::take(&mut saved.sessions));
    if Instant::now() >= deadline {
        stop_sessions(deps, &sessions).await;
        e.status = "failed".into();
        let why = format!(
            "The run used its {} minutes before it finished.",
            s.budget.max_minutes
        );
        e.summary = why.clone();
        finish(deps, team, &s, &mut e, &why).await;
        return e;
    }
    let left_usd = s.budget.max_usd - e.cost_usd;
    if left_usd <= 0.0 {
        stop_sessions(deps, &sessions).await;
        e.status = "failed".into();
        let why = format!(
            "The run used its model budget (${:.2}) before it finished.",
            s.budget.max_usd
        );
        e.summary = why.clone();
        finish(deps, team, &s, &mut e, &why).await;
        return e;
    }
    let exec = Exec {
        deps,
        team,
        run_id: &run_id,
        s: &s,
        skill: &skill,
        granted: &saved.granted,
        servers: &saved.stdio,
        sessions: &sessions,
        out: Mutex::new(saved.out.clone()),
        scripts: AtomicUsize::new(saved.scripts),
        athena_bytes: Mutex::new(saved.athena_bytes),
        athena_lock: tokio::sync::Mutex::new(()),
        needs: Mutex::new(Vec::new()),
        stopped: AtomicBool::new(false),
        step_end,
        deadline,
    };
    let mut convo = std::mem::take(&mut saved.convo);
    let result = deps
        .agent
        .run(
            team,
            &saved.system,
            &tools(&s, &saved.granted),
            &exec,
            left_usd,
            deadline,
            &mut convo,
        )
        .await;
    if let Ok((_, t, c)) = &result {
        e.tokens += t;
        e.cost_usd += c;
    }
    let needs = exec.needs.lock().unwrap().clone();
    if !needs.is_empty() {
        stop_sessions(deps, &sessions).await;
        return needs_access(deps, team, &s, e, needs).await;
    }
    let (step, _, _) = match result {
        Ok(r) => r,
        Err(why) => {
            stop_sessions(deps, &sessions).await;
            e.status = "failed".into();
            let why = if why.contains("model spend") {
                format!(
                    "The run used its model budget (${:.2}) before it finished.",
                    s.budget.max_usd
                )
            } else {
                format!("The agent stopped: {}", first_line(&why))
            };
            e.summary = why.clone();
            finish(deps, team, &s, &mut e, &why).await;
            return e;
        }
    };
    let text = match step {
        Step::Done(text) => text,
        Step::More => {
            // Save where the run is, and go on in the next worker step.
            let out = exec.out.lock().unwrap().clone();
            let athena_bytes = *exec.athena_bytes.lock().unwrap();
            let scripts = exec.scripts.load(Ordering::SeqCst);
            drop(exec);
            let next = Saved {
                entry: e.clone(),
                sessions: sessions.into_inner(),
                convo,
                out,
                athena_bytes,
                scripts,
                steps: saved.steps + 1,
                ..saved
            };
            let key = state_key(team, &run_id);
            let sent = match serde_json::to_string(&next) {
                Ok(body) => match deps.files.put(&key, &body).await {
                    Ok(()) => {
                        deps.runs
                            .continue_later(&SkillRunMessage {
                                team_id: team.to_string(),
                                schedule_id: s.id.clone(),
                                run_id: run_id.clone(),
                                trigger: e.trigger.clone(),
                                requested_by: e.requested_by.clone(),
                                resume: key,
                                mode: String::new(),
                            })
                            .await
                    }
                    Err(why) => Err(why),
                },
                Err(why) => Err(why.to_string()),
            };
            if let Err(why) = sent {
                let sessions: Sessions = tokio::sync::Mutex::new(next.sessions);
                stop_sessions(deps, &sessions).await;
                e.status = "failed".into();
                let why = format!("The run couldn't go on in its next step: {why}");
                e.summary = why.clone();
                finish(deps, team, &s, &mut e, &why).await;
                return e;
            }
            deps.runs
                .note(
                    team,
                    &run_id,
                    &format!("Going on in step {}", next.steps + 1),
                )
                .await;
            let _ = super::put_run(deps.kv, team, &e).await;
            deps.runs.write(team, &row(&s, &e, "")).await;
            return e;
        }
    };
    stop_sessions(deps, &sessions).await;
    if Instant::now() >= deadline {
        e.status = "failed".into();
        let why = format!(
            "The run used its {} minutes before it finished.",
            s.budget.max_minutes
        );
        e.summary = why.clone();
        finish(deps, team, &s, &mut e, &why).await;
        return e;
    }

    // Keep the result.
    let out = exec.out.lock().unwrap().clone();
    let restored = &saved.restored;
    let made = made_by_run(&out, restored);
    let mut kept = Vec::new();
    let mut not_kept = Vec::new();
    for name in &made {
        if kept.len() >= crate::outputs::MAX_FILES {
            not_kept.push(format!(
                "{name} (more than {} outputs)",
                crate::outputs::MAX_FILES
            ));
            continue;
        }
        let body = &out[name];
        if body.len() > crate::outputs::MAX_BYTES {
            not_kept.push(format!("{name} (over 5 MB)"));
            continue;
        }
        match deps
            .files
            .put(&crate::outputs::key(team, &run_id, name), body)
            .await
        {
            Ok(()) => kept.push(name.clone()),
            Err(why) => not_kept.push(format!("{name} ({why})")),
        }
    }
    let prefix = super::state_prefix(team, &s.id);
    let mut state_saved = 0;
    for d in &s.state_dirs {
        let pre = format!("{d}/");
        for (p, body) in out.iter().filter(|(p, _)| p.starts_with(&pre)) {
            if restored.get(p) == Some(body) {
                continue;
            }
            if deps.files.put(&format!("{prefix}{p}"), body).await.is_ok() {
                state_saved += 1;
            }
        }
    }
    if state_saved > 0 {
        let all: Vec<String> = deps
            .files
            .list(&prefix)
            .await
            .unwrap_or_default()
            .iter()
            .filter_map(|k| rel(&prefix, k))
            .collect();
        let keep = keep_newest(&all, &s.state_dirs);
        for p in all.iter().filter(|p| !keep.contains(p)) {
            let _ = deps.files.delete(&format!("{prefix}{p}")).await;
        }
    }
    e.status = "completed".into();
    e.report = pick_report(&kept);
    e.outputs = kept;
    e.summary = crate::truncate_str(text.trim(), 4_000).to_string();
    if !not_kept.is_empty() {
        e.summary
            .push_str(&format!("\n\nNot kept: {}", not_kept.join(", ")));
    }
    deps.runs
        .note(
            team,
            &run_id,
            &format!(
                "Kept {} outputs and {state_saved} memory files for the next run",
                e.outputs.len()
            ),
        )
        .await;
    finish(deps, team, &s, &mut e, "").await;
    e
}

/// Ends a run that lacks access: one output listing what it needs, nothing
/// else (no partial report, no memory kept).
async fn needs_access(
    deps: &Deps<'_>,
    team: &str,
    s: &Schedule,
    mut e: RunEntry,
    needs: Vec<String>,
) -> RunEntry {
    let body = format!(
        "# Needs access\n\nThe run of the skill `{}` stopped before it read anything it would \
         report on. Nothing else of the run was kept.\n\n{}\n",
        s.skill_name,
        needs
            .iter()
            .map(|n| format!("- {n}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    if deps
        .files
        .put(
            &crate::outputs::key(team, &e.run_id, "needs-access.md"),
            &body,
        )
        .await
        .is_ok()
    {
        e.outputs = vec!["needs-access.md".into()];
    }
    e.status = "needs_access".into();
    e.summary = format!("Needs access: {}", needs.join("; "));
    e.needs_access = needs;
    let why = e.summary.clone();
    finish(deps, team, s, &mut e, &why).await;
    e
}

#[cfg(test)]
pub(crate) mod tests;
