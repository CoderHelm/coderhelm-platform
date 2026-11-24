//! One skill run, start to end, behind traits so it runs in tests against
//! fakes and in the worker against AWS.
//!
//! 1. **Claim** the schedule (one run at a time) and list the run with the
//!    team's runs (`ticket_source` `skill_run`).
//! 2. **Read the skill's folder** at the synced commit: its `SKILL.md` and
//!    every file next to it (the scripts too, whatever their size).
//! 3. **Check access** before the agent starts: every granted tool must be
//!    offered by its connected server, and the AWS role must be taken on.
//!    Anything missing ends the run as `needs_access`, naming each one, with
//!    nothing else written: never a partial result.
//! 4. **Restore the memory folder** (`snapshots/`) kept by the last run into
//!    the run's output folder.
//! 5. **Run the agent** with the skill as its instructions and only these
//!    tools: the granted ones (writes refused unless the schedule allows
//!    them), Athena through the reader role, the skill's scripts in the
//!    sandbox (`CLAUDE_SKILL_DIR` is the skill's folder, `{out}` the output
//!    folder, `{work}` its scratch folder), and the output folder's files.
//!    A tool refused for access stops the run the same way as step 3.
//! 6. **Keep the result**: the files the run made in the output folder are
//!    the run's outputs (the report, the snapshot), the memory folder is
//!    kept for the next run, the scratch folder is dropped.
//! 7. **Tell** the run page, the schedule's history and, as chosen, the
//!    team's chat channels and an issue on a failed scheduled run.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::{AwsGrant, Budget, LastRun, RunEntry, Schedule, SkillRunMessage, ATHENA_TOOL};
use crate::plugins::connect::{Connections, ToolDef};
use crate::plugins::kv::Kv;
use crate::plugins::run::ToolExec;
use crate::tool_registry::Risk;

/// The output folder's scratch folder (`mktemp -d` on a laptop): dropped
/// when the run ends, never an output.
pub const WORK_DIR: &str = "work";
/// Script runs one skill run may start.
pub const MAX_SCRIPTS: usize = 20;
/// Files the output folder holds at most, and their size together.
pub const MAX_FILES: usize = 400;
pub const MAX_BYTES: usize = 20 * 1024 * 1024;
/// Memory files kept per memory folder (a year of weekly snapshots).
pub const MAX_STATE_FILES: usize = 60;
/// Characters of a tool's result handed to the model.
const MAX_BACK: usize = 20_000;
/// Characters shown when a result went to a file instead.
const PREVIEW: usize = 1_500;

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

/// Files in the data bucket: the memory folder and the run's outputs.
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

/// The team's runs table and a run's progress notes.
#[async_trait::async_trait]
pub trait RunLog: Send + Sync {
    async fn write(&self, team: &str, row: &RunRow);
    async fn note(&self, team: &str, run: &str, text: &str);
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
}

/// The agent loop, held to `budget` (model spend and time).
#[async_trait::async_trait]
pub trait Agent: Send + Sync {
    async fn run(
        &self,
        team: &str,
        system: &str,
        user: &str,
        tools: &[ToolDef],
        exec: &dyn ToolExec,
        budget: &Budget,
    ) -> Result<(String, u64, f64), String>;
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
    /// The dashboard, for links (`https://app.example.com`).
    pub app_url: &'a str,
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

// ── The tools ───────────────────────────────────────────────────────────────

fn tool(name: &str, description: &str, schema: Value) -> ToolDef {
    ToolDef {
        name: name.into(),
        description: description.into(),
        input_schema: schema,
    }
}

/// The tools of a run of `s`: CoderHelm's own and the granted ones (with
/// the input schemas their servers gave).
pub fn tools(s: &Schedule, schemas: &BTreeMap<String, ToolDef>) -> Vec<ToolDef> {
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
    for g in &s.tools {
        let d = schemas.get(&g.name);
        v.push(ToolDef {
            name: g.name.clone(),
            description: d
                .map(|d| d.description.clone())
                .unwrap_or_else(|| format!("{} of {}", g.server_tool(), g.server)),
            input_schema: d
                .map(|d| d.input_schema.clone())
                .filter(|s| s.is_object())
                .unwrap_or_else(|| json!({"type":"object"})),
        });
    }
    v
}

/// What the run tells the agent: the rules of an unattended run on
/// CoderHelm, how a laptop's words map here, then the skill.
pub fn system_prompt(s: &Schedule, skill_md: &str, files: &[String], today: &str) -> String {
    let dirs = if s.state_dirs.is_empty() {
        "none".to_string()
    } else {
        s.state_dirs
            .iter()
            .map(|d| format!("`{d}/`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let tools = s
        .tools
        .iter()
        .map(|t| {
            format!(
                "- `{}` (also written `mcp__{}__{}`): {}",
                t.name,
                t.server,
                t.server_tool(),
                if t.risk() == Risk::Read {
                    "reads"
                } else if s.allow_writes {
                    "may change things; allowed by this schedule"
                } else {
                    "refused: this run is read-only"
                }
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
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
         - Call tools by the names the skill uses. This run's tools:\n{tools}\n{athena}\
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

// ── One run's tool calls ────────────────────────────────────────────────────

struct Exec<'a> {
    deps: &'a Deps<'a>,
    team: &'a str,
    run_id: &'a str,
    s: &'a Schedule,
    skill: &'a [(String, Vec<u8>)],
    out: Mutex<BTreeMap<String, String>>,
    scripts: AtomicUsize,
    athena_bytes: Mutex<u64>,
    athena_lock: tokio::sync::Mutex<()>,
    needs: Mutex<Vec<String>>,
    stopped: AtomicBool,
    deadline: Instant,
}

impl Exec<'_> {
    fn grant(&self, name: &str) -> Option<&super::ToolGrant> {
        self.s
            .tools
            .iter()
            .find(|g| g.name == name || format!("mcp__{}__{}", g.server, g.server_tool()) == name)
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

    /// A granted tool or Athena, through the gate. `Err` is the refusal or
    /// the error as the agent sees it.
    async fn gated(&self, name: &str, args: &Value) -> Result<String, String> {
        if name == ATHENA_TOOL {
            return self.athena(args).await;
        }
        let Some(g) = self.grant(name) else {
            return Err(format!("'{name}' isn't one of this run's tools."));
        };
        match g.risk() {
            Risk::Read => {}
            Risk::Write if self.s.allow_writes => {}
            Risk::Write => {
                return Err(format!(
                    "Refused: {} may change something, and this run is read-only. The \
                     schedule's settings allow writes; this one doesn't.",
                    g.name
                ))
            }
            Risk::Consequential => {
                return Err(format!("Refused: {} sends, publishes or deletes.", g.name))
            }
        }
        let args = if args.is_object() {
            args.clone()
        } else {
            json!({})
        };
        match self
            .deps
            .conns
            .call(self.team, &g.server, g.server_tool(), &args)
            .await
        {
            Ok(t) => {
                if t.len() < 400 && is_access_error(&t) {
                    self.need(format!("{}: {}", g.name, first_line(&t)));
                }
                Ok(t)
            }
            Err(e) => {
                if is_access_error(&e) {
                    self.need(format!("{}: {}", g.name, first_line(&e)));
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
                self.need(format!("{ATHENA_TOOL}: {}", first_line(&e)));
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
            .run(&key, self.skill, &before, &cmd, self.deadline)
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
    let failed = e.status != "completed";
    let link = run_link(deps.app_url, &e.run_id);
    if s.notify.chat == "always" || (s.notify.chat == "failure" && failed) {
        let mut lines = vec![match e.status.as_str() {
            "completed" => format!("Finished: {}", crate::truncate_str(&e.summary, 300)),
            "needs_access" => format!("Needs access: {}", e.needs_access.join("; ")),
            _ => format!("Failed: {error}"),
        }];
        if !e.report.is_empty() {
            lines.push(format!("Report: {}", e.report));
        }
        deps.notify
            .chat(team, &format!("Skill run: {}", s.skill_name), &lines, &link)
            .await;
    }
    if failed && s.notify.issue_on_failure && e.trigger == "schedule" {
        let body = format!(
            "The scheduled run of the skill `{}` didn't finish.\n\n{}\n\nRun: {link}",
            s.skill_name,
            if e.status == "needs_access" {
                format!("Needs access: {}", e.needs_access.join("; "))
            } else {
                error.to_string()
            }
        );
        let title = format!(
            "{} failed {}",
            s.skill_name,
            e.started_at.get(..10).unwrap_or("")
        );
        let _ = deps.notify.issue(team, &s.skill_repo, &title, &body).await;
    }
}

/// One run of a schedule. Returns its history entry (`None` when nothing ran:
/// a paused schedule's fire, a deleted schedule, or a schedule's fire while
/// a run is still working).
pub async fn execute(deps: &Deps<'_>, msg: &SkillRunMessage) -> Option<RunEntry> {
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
    deps.runs
        .note(
            team,
            &run_id,
            &format!("Read the skill's folder: {} files", skill.len()),
        )
        .await;

    // Access first: everything the run is granted must be there.
    let mut needs = Vec::new();
    let mut schemas = BTreeMap::new();
    let mut listed: BTreeMap<String, Result<Vec<ToolDef>, String>> = BTreeMap::new();
    for g in &s.tools {
        if !listed.contains_key(&g.server) {
            listed.insert(g.server.clone(), deps.conns.tools(team, &g.server).await);
        }
        match &listed[&g.server] {
            Err(why) => needs.push(format!(
                "{}: the MCP server '{}' isn't usable ({})",
                g.name,
                g.server,
                first_line(why)
            )),
            Ok(ts) => match ts.iter().find(|t| t.name == g.server_tool()) {
                Some(t) => {
                    schemas.insert(g.name.clone(), t.clone());
                }
                None => needs.push(format!(
                    "{}: the MCP server '{}' offers no tool '{}'",
                    g.name,
                    g.server,
                    g.server_tool()
                )),
            },
        }
    }
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
        return Some(needs_access(deps, team, &s, e, needs).await);
    }
    deps.runs
        .note(team, &run_id, "Checked access: every granted tool is there")
        .await;

    // The memory folder, as the last run left it.
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

    let deadline = Instant::now() + Duration::from_secs(s.budget.max_minutes as u64 * 60);
    let exec = Exec {
        deps,
        team,
        run_id: &run_id,
        s: &s,
        skill: &skill,
        out: Mutex::new(restored.clone()),
        scripts: AtomicUsize::new(0),
        athena_bytes: Mutex::new(0),
        athena_lock: tokio::sync::Mutex::new(()),
        needs: Mutex::new(Vec::new()),
        stopped: AtomicBool::new(false),
        deadline,
    };
    let skill_md = skill
        .iter()
        .find(|(p, _)| p == "SKILL.md")
        .map(|(_, b)| String::from_utf8_lossy(b).to_string())
        .unwrap_or_default();
    let names: Vec<String> = skill.iter().map(|(p, _)| p.clone()).collect();
    let today = match super::cron::time_zone(&s.timezone) {
        Ok(tz) => now
            .with_timezone(&tz)
            .format("%Y-%m-%d (%A) %Z")
            .to_string(),
        Err(_) => now.format("%Y-%m-%d UTC").to_string(),
    };
    let system = system_prompt(&s, &skill_md, &names, &today);
    let user = if s.instructions.trim().is_empty() {
        format!("Run the skill {} now, end to end.", s.skill_name)
    } else {
        s.instructions.clone()
    };
    let result = deps
        .agent
        .run(team, &system, &user, &tools(&s, &schemas), &exec, &s.budget)
        .await;
    let needs = exec.needs.lock().unwrap().clone();
    if !needs.is_empty() {
        if let Ok((_, t, c)) = &result {
            e.tokens = *t;
            e.cost_usd = *c;
        }
        return Some(needs_access(deps, team, &s, e, needs).await);
    }
    let (text, tokens, cost) = match result {
        Ok(r) => r,
        Err(why) => {
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
            return Some(e);
        }
    };
    e.tokens = tokens;
    e.cost_usd = cost;
    if Instant::now() >= deadline {
        e.status = "failed".into();
        let why = format!(
            "The run used its {} minutes before it finished.",
            s.budget.max_minutes
        );
        e.summary = why.clone();
        finish(deps, team, &s, &mut e, &why).await;
        return Some(e);
    }

    // Keep the result.
    let out = exec.out.lock().unwrap().clone();
    let made = made_by_run(&out, &restored);
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
        // The memory folder stays bounded: the newest of each folder.
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
    Some(e)
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
