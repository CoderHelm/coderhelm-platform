//! The team's app map, made and kept current by Floorplan (vendored at
//! `vendor/floorplan`, MIT) run the way its own workflow says to run it
//! without an agent session (`workflow.json`, `docs/unattended.md`):
//!
//! - its scripts (scan, refresh, build, describe) run in the run sandbox,
//!   where Node already is, through CoderHelm's driver
//!   (`services/worker/floorplan/driver.mjs`) on a snapshot of every
//!   repository the team has, side by side, so requests are matched to
//!   endpoints across repositories as Floorplan does it;
//! - its model jobs (architecture, update_architecture, descriptions) run
//!   here, on the team's own model, metered under feature `app_map`, with
//!   the read-only tools the workflow names (`read_file`, `list_files`,
//!   `grep`) over the same snapshot and `submit`, whose answer is checked
//!   against the job's schema before it is used. The descriptions use the
//!   light model;
//! - updates are Floorplan's own refresh: it compares each file's
//!   fingerprints with the last map, carries descriptions over, and lists
//!   only what is new or changed to describe. A map is redone only when a
//!   repository's default branch moved.
//!
//! One step per message (`AppMapMessage.step`): `start` reads the heads and
//! runs the scan or the refresh; `build` writes the model jobs' answers and
//! builds. The map is kept in the bucket (`common::app_map::graph_key`), with
//! Floorplan's own files for the next refresh (`state_key`) and the step's
//! job (`job_key`). Next to the map go the page's copy (`view_key`, which
//! the browser reads from the bucket itself) and the map's small index
//! (`index_key`), so reading the page never reads the whole map. Recent work
//! and Health are placed with the index when the page is read
//! (`common::app_map::overlays`), so they are always current.

use std::collections::BTreeMap;
use std::io::Read;

use common::app_map as am;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::agent::llm::{ConverseOptions, ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;
use crate::models::{AppMapMessage, TokenUsage};
use crate::WorkerState;

include!(concat!(env!("OUT_DIR"), "/floorplan_files.rs"));

/// CoderHelm's driver, run in the sandbox.
const DRIVER: &str = include_str!("../../floorplan/driver.mjs");
/// The folder the sandbox runs in.
const TOP: &str = "app-map";
/// Attempts at an architecture the build accepts (the workflow's own limit).
pub const MAX_ARCH_ATTEMPTS: u32 = 4;
/// Attempts at complete descriptions (the workflow's own limit).
pub const MAX_DESC_ATTEMPTS: u32 = 3;
/// A file larger than this is left out of the snapshot (generated or data).
const MAX_FILE_BYTES: u64 = 1024 * 1024;
/// The snapshot of every repository together, at most.
const MAX_SNAPSHOT_BYTES: u64 = 400 * 1024 * 1024;
/// Description jobs run at the same time.
const PARALLEL_JOBS: usize = 4;

/// The run in progress, kept between steps.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Job {
    /// `owner/name` to the commit the map is being made from.
    pub heads: BTreeMap<String, String>,
    /// `owner/name` to its default branch.
    #[serde(default)]
    pub branches: BTreeMap<String, String>,
    /// Answers waiting for the next build (map-relative path to JSON).
    #[serde(default)]
    pub answers: BTreeMap<String, Value>,
    #[serde(default)]
    pub arch_attempts: u32,
    #[serde(default)]
    pub desc_attempts: u32,
    /// The heads the last finished map was made from.
    #[serde(default)]
    pub built: BTreeMap<String, String>,
}

/// What to do after a sandbox step, from what Floorplan said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Run this model job (`architecture` or `update_architecture`, with the
    /// refresh item's text when it is one), then build.
    Architecture { job: &'static str, item: String },
    /// Describe these worksheets (by key), then build.
    Describe(Vec<String>),
    /// The map is done.
    Done,
    /// It can't go further; why, in plain words.
    Stop(String),
}

/// The next move from a step's result (the driver's `result`). Pure.
pub fn decide(result: &Value, job: &Job) -> Next {
    let build = &result["build"];
    // A first map: the scan is in and there is no architecture yet.
    if result["architecture"].is_null() {
        if result["scan"]["ok"] == false {
            return Next::Stop(first_error(
                &result["scan"],
                "The repositories couldn't be read.",
            ));
        }
        return if job.arch_attempts < MAX_ARCH_ATTEMPTS {
            Next::Architecture {
                job: "architecture",
                item: String::new(),
            }
        } else {
            Next::Stop("The map's parts couldn't be worked out.".into())
        };
    }
    // A refresh's NEXT list: structure work first.
    for it in result["refresh"]["next"].as_array().into_iter().flatten() {
        let code = it["code"].as_str().unwrap_or("");
        if matches!(code, "fix_build" | "assign_new_files" | "review_structure")
            && job.arch_attempts < MAX_ARCH_ATTEMPTS
        {
            return Next::Architecture {
                job: "update_architecture",
                item: it["text"].as_str().unwrap_or(code).to_string(),
            };
        }
    }
    if build["ok"] != true {
        return if job.arch_attempts < MAX_ARCH_ATTEMPTS {
            Next::Architecture {
                job: "architecture",
                item: String::new(),
            }
        } else {
            Next::Stop(first_error(build, "The map didn't build."))
        };
    }
    let unassigned = build["files"]["unassignedPercent"].as_f64().unwrap_or(0.0);
    if unassigned >= 10.0 && job.arch_attempts < MAX_ARCH_ATTEMPTS {
        return Next::Architecture {
            job: "architecture",
            item: String::new(),
        };
    }
    let complete = build["descriptions"]["complete"] == true;
    let keys: Vec<String> = result["worksheets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|w| w["key"].as_str().map(str::to_string))
        .collect();
    if !complete && !keys.is_empty() && job.desc_attempts < MAX_DESC_ATTEMPTS {
        return Next::Describe(keys);
    }
    Next::Done
}

fn first_error(v: &Value, fallback: &str) -> String {
    v["errors"]
        .as_array()
        .and_then(|e| e.first())
        .and_then(|e| e.as_str())
        .map(|e| common::truncate_str(e, 300).to_string())
        .unwrap_or_else(|| fallback.to_string())
}

// ── Snapshot ───────────────────────────────────────────────────────────────

/// A repository's files from GitHub's tarball (its one top folder removed),
/// under `root/<folder>/`, leaving out large and binary files.
pub fn repo_entries(tarball: &[u8], folder: &str, budget: &mut u64) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(tarball));
    let Ok(entries) = ar.entries() else {
        return out;
    };
    for mut e in entries.flatten() {
        if e.header().entry_type() != tar::EntryType::Regular || e.size() > MAX_FILE_BYTES {
            continue;
        }
        let Ok(path) = e.path().map(|p| p.to_string_lossy().to_string()) else {
            continue;
        };
        let Some((_, rel)) = path.split_once('/') else {
            continue;
        };
        if rel.is_empty() || rel.split('/').any(|s| s == ".." || s == ".git") {
            continue;
        }
        let mut buf = Vec::new();
        if e.read_to_end(&mut buf).is_err() || buf.iter().take(8000).any(|b| *b == 0) {
            continue;
        }
        if *budget < buf.len() as u64 {
            break;
        }
        *budget -= buf.len() as u64;
        out.push((format!("root/{folder}/{rel}"), buf));
    }
    out
}

/// Floorplan's own files from the last run, under `root/`.
pub fn state_entries(tgz: &[u8]) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(tgz));
    let Ok(entries) = ar.entries() else {
        return out;
    };
    for mut e in entries.flatten() {
        let Ok(path) = e.path().map(|p| p.to_string_lossy().to_string()) else {
            continue;
        };
        if e.header().entry_type() != tar::EntryType::Regular
            || !path.starts_with("floorplan/")
            || path.contains("..")
        {
            continue;
        }
        let mut buf = Vec::new();
        if e.read_to_end(&mut buf).is_ok() {
            out.push((format!("root/{path}"), buf));
        }
    }
    out
}

/// The sandbox's folder: the driver, the job, the vendored release and the
/// snapshot.
pub fn pack(job: &Value, files: &[(String, Vec<u8>)]) -> Result<Vec<u8>, String> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut tar = tar::Builder::new(gz);
    let mut add = |path: String, body: &[u8]| -> Result<(), String> {
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tar.append_data(&mut h, format!("{TOP}/{path}"), body)
            .map_err(|e| e.to_string())
    };
    add("driver.mjs".into(), DRIVER.as_bytes())?;
    add("job.json".into(), job.to_string().as_bytes())?;
    for (p, b) in FLOORPLAN_FILES {
        add(format!("fp/{p}"), b)?;
    }
    for (p, b) in files {
        add(p.clone(), b)?;
    }
    tar.into_inner()
        .and_then(|g| g.finish())
        .map_err(|e| e.to_string())
}

fn vendored(path: &str) -> &'static str {
    FLOORPLAN_FILES
        .iter()
        .find(|(p, _)| *p == path)
        .and_then(|(_, b)| std::str::from_utf8(b).ok())
        .unwrap_or("")
}

// ── Model jobs ─────────────────────────────────────────────────────────────

/// The files the model's tools read: the snapshot, by map path
/// (`<folder>/<path>`).
pub struct Snapshot {
    pub files: BTreeMap<String, Vec<u8>>,
}

fn glob_re(glob: &str) -> Option<regex::Regex> {
    let mut re = String::from("^");
    let mut chars = glob.trim_start_matches("./").chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' if chars.peek() == Some(&'*') => {
                chars.next();
                if chars.peek() == Some(&'/') {
                    chars.next();
                    re.push_str("(?:.*/)?");
                } else {
                    re.push_str(".*");
                }
            }
            '*' => re.push_str("[^/]*"),
            '?' => re.push_str("[^/]"),
            c => re.push_str(&regex::escape(&c.to_string())),
        }
    }
    re.push('$');
    regex::Regex::new(&re).ok()
}

impl Snapshot {
    pub fn from_entries(entries: &[(String, Vec<u8>)]) -> Self {
        Self {
            files: entries
                .iter()
                .filter_map(|(p, b)| Some((p.strip_prefix("root/")?.to_string(), b.clone())))
                .filter(|(p, _)| !p.starts_with("floorplan/.cache"))
                .collect(),
        }
    }

    pub fn read_file(&self, input: &Value) -> String {
        let path = input["path"]
            .as_str()
            .unwrap_or("")
            .trim_start_matches("./");
        let Some(b) = self.files.get(path) else {
            return format!("No file {path} under the codebase root.");
        };
        let text = String::from_utf8_lossy(b);
        let start = input["start_line"]
            .as_u64()
            .or(input["offset"].as_u64())
            .unwrap_or(1)
            .max(1) as usize;
        let end = input["end_line"]
            .as_u64()
            .map(|e| e as usize)
            .unwrap_or(start + 399);
        let lines: Vec<String> = text
            .lines()
            .enumerate()
            .skip(start - 1)
            .take(end.saturating_sub(start) + 1)
            .map(|(i, l)| format!("{}\t{}", i + 1, common::truncate_str(l, 400)))
            .collect();
        common::truncate_str(&lines.join("\n"), 40_000).to_string()
    }

    pub fn list_files(&self, input: &Value) -> String {
        let glob = input["glob"]
            .as_str()
            .or(input["pattern"].as_str())
            .unwrap_or("**");
        let Some(re) = glob_re(glob) else {
            return format!("Not a glob: {glob}");
        };
        let hits: Vec<&String> = self
            .files
            .keys()
            .filter(|p| re.is_match(p))
            .take(500)
            .collect();
        if hits.is_empty() {
            return "No files match.".into();
        }
        hits.iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn grep(&self, input: &Value) -> String {
        let pattern = input["pattern"]
            .as_str()
            .or(input["regex"].as_str())
            .unwrap_or("");
        let Ok(re) = regex::RegexBuilder::new(pattern)
            .size_limit(1 << 20)
            .build()
        else {
            return format!("Not a regular expression: {pattern}");
        };
        let only = input["glob"].as_str().and_then(glob_re);
        let mut out = Vec::new();
        for (p, b) in &self.files {
            if only.as_ref().is_some_and(|g| !g.is_match(p)) {
                continue;
            }
            for (i, l) in String::from_utf8_lossy(b).lines().enumerate() {
                if re.is_match(l) {
                    out.push(format!(
                        "{p}:{}: {}",
                        i + 1,
                        common::truncate_str(l.trim(), 200)
                    ));
                    if out.len() >= 200 {
                        return out.join("\n");
                    }
                }
            }
        }
        if out.is_empty() {
            "No matches.".into()
        } else {
            out.join("\n")
        }
    }
}

/// The four tools of Floorplan's workflow.
pub fn tools() -> Vec<ToolDefinition> {
    let wf: Value = serde_json::from_str(am::WORKFLOW_JSON).unwrap_or_default();
    let about = |k: &str| {
        wf["conventions"]["tools"][k]
            .as_str()
            .unwrap_or("")
            .to_string()
    };
    vec![
        ToolDefinition {
            name: "read_file".into(),
            description: about("read_file"),
            input_schema: json!({"type": "object", "properties": {"path": {"type": "string"}, "start_line": {"type": "integer"}, "end_line": {"type": "integer"}}, "required": ["path"]}),
        },
        ToolDefinition {
            name: "list_files".into(),
            description: about("list_files"),
            input_schema: json!({"type": "object", "properties": {"glob": {"type": "string"}}, "required": ["glob"]}),
        },
        ToolDefinition {
            name: "grep".into(),
            description: about("grep"),
            input_schema: json!({"type": "object", "properties": {"pattern": {"type": "string"}, "glob": {"type": "string"}}, "required": ["pattern"]}),
        },
        ToolDefinition {
            name: "submit".into(),
            description: about("submit"),
            input_schema: json!({"type": "object", "properties": {"answer": {"type": "object", "description": "The answer: one JSON object matching the job's schema."}}, "required": ["answer"]}),
        },
    ]
}

/// The tools over one snapshot, keeping the accepted answer.
struct JobTools<'a> {
    snapshot: &'a Snapshot,
    schema: Value,
    answer: std::sync::Mutex<Option<Value>>,
}

#[async_trait::async_trait]
impl ToolExecutor for JobTools<'_> {
    async fn execute(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let text = match name {
            "read_file" => self.snapshot.read_file(input),
            "list_files" => self.snapshot.list_files(input),
            "grep" => self.snapshot.grep(input),
            "submit" => {
                let answer = if input["answer"].is_object() {
                    input["answer"].clone()
                } else {
                    input.clone()
                };
                let problems = am::check_answer(&self.schema, &answer);
                if problems.is_empty() {
                    *self.answer.lock().unwrap_or_else(|e| e.into_inner()) = Some(answer);
                    "Accepted. The job is done; reply with a short sentence and stop.".to_string()
                } else {
                    format!(
                        "Not accepted. Fix these and submit again:\n- {}",
                        problems.join("\n- ")
                    )
                }
            }
            _ => format!("No tool {name}."),
        };
        Ok(json!(text))
    }
}

/// One job's prompt: its references and instructions (system), its inputs
/// (user), as `workflow.json` lists them.
pub fn job_prompt(job: &str, inputs: &[(&str, String)]) -> Option<(String, String, Value)> {
    let wf: Value = serde_json::from_str(am::WORKFLOW_JSON).ok()?;
    let j = &wf["jobs"][job];
    if j.is_null() {
        return None;
    }
    let mut system = String::from("You do one job in making a map of a codebase, for Floorplan. Work with the tools you have: read_file, list_files and grep (read-only, inside the codebase root) and submit. Give your answer only through submit, as one JSON object matching the schema below; if submit says it is not accepted, fix it and submit again.\n\n");
    for line in j["instructions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| l.as_str())
    {
        system.push_str(line);
        system.push('\n');
    }
    let schema = wf["schemas"][j["schema"].as_str().unwrap_or(job)].clone();
    system.push_str(&format!("\nThe answer's schema:\n{schema}\n"));
    for r in j["read"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| r.as_str())
    {
        system.push_str(&format!(
            "\n--- {r} ---\n{}\n",
            vendored(&format!("skills/floorplan/{r}"))
        ));
    }
    let user = inputs
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| format!("## {k}\n{v}"))
        .collect::<Vec<_>>()
        .join("\n\n");
    Some((system, user, schema))
}

#[allow(clippy::too_many_arguments)]
async fn run_job(
    state: &WorkerState,
    provider: &crate::agent::provider::ModelProvider,
    model_id: &str,
    job: &str,
    inputs: &[(&str, String)],
    snapshot: &Snapshot,
    usage: &mut TokenUsage,
) -> Option<Value> {
    let (system, user, schema) = job_prompt(job, inputs)?;
    let wf: Value = serde_json::from_str(am::WORKFLOW_JSON).ok()?;
    let calls = wf["jobs"][job]["toolCalls"].as_u64().unwrap_or(30) as usize;
    let exec = JobTools {
        snapshot,
        schema,
        answer: std::sync::Mutex::new(None),
    };
    let mut messages = vec![(
        "user".to_string(),
        vec![json!({"type": "text", "text": user})],
    )];
    let r = crate::agent::provider::converse(
        state,
        provider,
        common::agent_policy::Agent::Planner,
        model_id,
        &system,
        &mut messages,
        &tools(),
        &exec,
        usage,
        ConverseOptions {
            max_turns: calls + 6,
            max_tokens: 16_000,
            deadline: None,
            effort: Some("low"),
        },
        None,
        None,
    )
    .await;
    if let Err(e) = r {
        warn!(job, error = %e, "App map: a model job failed");
    }
    exec.answer.into_inner().unwrap_or_else(|e| e.into_inner())
}

// ── Steps ──────────────────────────────────────────────────────────────────

async fn s3_get(state: &WorkerState, key: &str) -> Option<Vec<u8>> {
    let o = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(key)
        .send()
        .await
        .ok()?;
    Some(o.body.collect().await.ok()?.into_bytes().to_vec())
}

async fn s3_put(state: &WorkerState, key: &str, body: Vec<u8>) -> Result<(), String> {
    state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(body))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("app map: could not keep {key}: {e}"))
}

/// Changes the running build's status; `Err` when `build` is no longer the
/// build running (a newer one replaced it), and this one stops.
async fn change(
    state: &WorkerState,
    team_id: &str,
    f: impl Fn(Option<&am::Status>, i64) -> Option<am::Status>,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    match am::update_status(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        |cur| f(cur, now),
    )
    .await?
    {
        am::Updated::Written(_) => Ok(()),
        am::Updated::Kept(_) => Err("this build is no longer the one running".into()),
    }
}

/// The build's step, in plain words (and its stage, one of `am::STAGES`).
async fn progress(
    state: &WorkerState,
    team_id: &str,
    build: &str,
    stage: Option<&str>,
    step: &str,
) -> Result<(), String> {
    change(state, team_id, |cur, now| {
        am::progress(cur, build, stage, step, now)
    })
    .await
}

/// The message's turn of the build, if it is the build's next one: `None`
/// for a second message about a build already running, a message delivered
/// twice, or a build a newer one replaced, which the worker skips.
pub fn turn_to_take(msg: &AppMapMessage, cur: Option<&am::Status>, now: i64) -> Option<am::Status> {
    am::take_turn(cur, &msg.build, msg.turn, now)
}

/// Queues the next step of a build (or a whole new build with `step` empty
/// and `turn` 0).
pub async fn queue(
    state: &WorkerState,
    team_id: &str,
    step: &str,
    build: &str,
    turn: u32,
    delay_secs: i32,
) -> bool {
    let msg = crate::models::WorkerMessage::AppMap(AppMapMessage {
        team_id: team_id.to_string(),
        repo: String::new(),
        step: step.to_string(),
        build: build.to_string(),
        turn,
    });
    let Ok(body) = serde_json::to_string(&msg) else {
        return false;
    };
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(delay_secs)
        .send()
        .await
        .is_ok()
}

/// After a push to a default branch: a team that has a map gets it brought
/// up to date (once, a little later, so a burst of pushes is one update).
pub async fn after_push(state: &WorkerState, team_id: &str) {
    if s3_get(state, &am::state_key(team_id)).await.is_none() {
        return;
    }
    // A build already queued or running covers this push.
    if let Ok(Ok(st)) =
        am::queue_build(&state.dynamo, &state.config.settings_table_name, team_id).await
    {
        queue(state, team_id, "", &st.build, 0, 120).await;
    }
}

/// One step of a team's map: the message's turn of its build, taken once.
pub async fn run(state: &WorkerState, msg: AppMapMessage) {
    let team_id = msg.team_id.clone();
    let now = chrono::Utc::now().timestamp();
    let taken = am::update_status(
        &state.dynamo,
        &state.config.settings_table_name,
        &team_id,
        |cur| turn_to_take(&msg, cur, now),
    )
    .await;
    let st = match taken {
        Ok(am::Updated::Written(st)) => st,
        Ok(am::Updated::Kept(cur)) => {
            info!(
                team_id,
                build = %msg.build,
                turn = msg.turn,
                running = %cur.map(|c| c.build).unwrap_or_default(),
                "App map: not the build's next turn (a duplicate, or a build already running), skipped"
            );
            return;
        }
        Err(e) => {
            warn!(team_id, error = %e, "App map: the build status couldn't be read");
            return;
        }
    };
    let build = st.build.clone();
    let call = step(state, msg, &build, st.turn);
    if let Err(e) = common::usage::with_feature(common::usage::feature::APP_MAP, call).await {
        warn!(team_id, error = %e, "App map: the step failed");
        let _ = change(state, &team_id, |cur, now| am::failed(cur, &build, &e, now)).await;
    }
}

async fn step(
    state: &WorkerState,
    msg: AppMapMessage,
    build: &str,
    turn: u32,
) -> Result<(), String> {
    let team_id = msg.team_id.as_str();
    let repos = super::plan::fetch_team_repos(state, team_id).await;
    if repos.is_empty() {
        return change(state, team_id, |cur, now| {
            am::finished(
                cur,
                build,
                0,
                "No repositories are turned on for this team.",
                now,
            )
        })
        .await;
    }
    let installation = super::code_graph::team_installation_id(state, team_id)
        .await
        .ok_or("no GitHub installation for the team")?;
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation,
        &state.http,
    )
    .map_err(|e| e.to_string())?;
    let mut job: Job = s3_get(state, &am::job_key(team_id))
        .await
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    let folders = am::folders(&repos);
    if msg.step.is_empty() {
        // Start: where each default branch is now.
        let mut heads = BTreeMap::new();
        let mut branches = BTreeMap::new();
        for r in &repos {
            let Some((owner, name)) = r.split_once('/') else {
                continue;
            };
            let Ok(branch) = github.get_default_branch(owner, name).await else {
                continue;
            };
            if let Ok(sha) = github.get_ref(owner, name, &branch).await {
                heads.insert(r.clone(), sha);
                branches.insert(r.clone(), branch);
            }
        }
        let have_map = s3_get(state, &am::graph_key(team_id)).await.is_some();
        if have_map && heads == job.built {
            info!(team_id, "App map: no default branch moved, nothing to redo");
            return change(state, team_id, |cur, now| {
                am::finished(cur, build, 0, "", now)
            })
            .await;
        }
        let built = std::mem::take(&mut job.built);
        job = Job {
            heads,
            branches,
            built,
            ..Default::default()
        };
    }
    if msg.step.is_empty() {
        let n = job.heads.len() as u32;
        change(state, team_id, |cur, now| {
            am::progress(
                cur,
                build,
                Some(am::STAGES[0]),
                "Reading the repositories",
                now,
            )
            .map(|mut s| {
                s.repos = n;
                s
            })
        })
        .await?;
    } else {
        // The stage stays: putting the map together is part of the one
        // before it.
        progress(state, team_id, build, None, "Putting the map together").await?;
    }

    // The snapshot: every repository at its head, and the last map's files.
    let mut budget = MAX_SNAPSHOT_BYTES;
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    for (repo, sha) in &job.heads {
        let Some((owner, name)) = repo.split_once('/') else {
            continue;
        };
        let folder = folders
            .get(repo)
            .cloned()
            .unwrap_or_else(|| name.to_string());
        let tarball = github
            .download_tarball(owner, name, sha)
            .await
            .map_err(|e| format!("{repo} couldn't be read: {e}"))?;
        entries.extend(repo_entries(&tarball, &folder, &mut budget));
    }
    if let Some(prev) = s3_get(state, &am::state_key(team_id)).await {
        entries.extend(state_entries(&prev));
    }
    let snapshot = Snapshot::from_entries(&entries);
    let mode = if msg.step.is_empty() {
        if entries
            .iter()
            .any(|(p, _)| p == "root/floorplan/graph.json")
        {
            "refresh"
        } else {
            "start"
        }
    } else {
        "build"
    };
    let branches: BTreeMap<&String, &String> = job
        .branches
        .iter()
        .filter_map(|(r, b)| Some((folders.get(r)?, b)))
        .collect();
    let heads: BTreeMap<&String, &String> = job
        .heads
        .iter()
        .filter_map(|(r, h)| Some((folders.get(r)?, h)))
        .collect();
    let spec = json!({"mode": mode, "answers": job.answers, "branches": branches, "heads": heads});
    let source = pack(&spec, &entries)?;

    let sandbox = crate::clients::sandbox::SandboxClient::new(
        &state.codebuild,
        &state.s3,
        &state.logs,
        &state.config.sandbox_bucket_name,
        &state.config.sandbox_project_name,
    );
    let key = format!("app-map-{}", uuid::Uuid::new_v4().simple());
    let out = sandbox
        .run_script(
            crate::clients::sandbox::ScriptRun {
                key: &key,
                source,
                cmd: "node driver.mjs",
                secret: None,
                hosts: Vec::new(),
            },
            None,
        )
        .await
        .map_err(|e| format!("the sandbox couldn't run Floorplan ({e})"))?;
    let file = |n: &str| {
        out.files
            .iter()
            .find(|(f, _)| f.ends_with(n))
            .map(|(_, b)| b.clone())
    };
    let b64 = |s: String| -> Option<Vec<u8>> {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD
            .decode(s.trim())
            .ok()
    };
    let result: Value = file("result.b64")
        .and_then(b64)
        .and_then(|b| am::gunzip(&b, 64 * 1024 * 1024))
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or_else(|| {
            format!(
                "Floorplan gave no result: {}",
                common::truncate_str(out.output.lines().last().unwrap_or(""), 300)
            )
        })?;
    if let Some(st) = file("state.b64").and_then(b64) {
        s3_put(state, &am::state_key(team_id), st).await?;
    }
    job.answers.clear();

    let provider = crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await
    .map_err(|e| format!("no model for this team: {e}"))?;
    let mut usage = TokenUsage::default();
    match decide(&result, &job) {
        Next::Architecture { job: name, item } => {
            job.arch_attempts += 1;
            progress(
                state,
                team_id,
                build,
                Some(am::STAGES[1]),
                "Finding the parts and how they connect",
            )
            .await?;
            // What CoderHelm already knows about each repository (its code
            // map) helps where Floorplan's scan summary is thin.
            let mut known = String::new();
            for r in job.heads.keys() {
                if let Some(m) = common::code_map::load(
                    &state.dynamo,
                    &state.config.settings_table_name,
                    team_id,
                    r,
                )
                .await
                {
                    if let Some(s) = m.section("architecture") {
                        known.push_str(&format!(
                            "### {} (folder {})\n{}\n\n",
                            r,
                            folders.get(r).map(String::as_str).unwrap_or(""),
                            common::truncate_str(&s.text, 2_000)
                        ));
                    }
                }
            }
            let inputs = vec![
                ("What to do (from the refresh's NEXT list)", item),
                (
                    "Scan summary",
                    result["scan"]["summary"].as_str().unwrap_or("").to_string(),
                ),
                (
                    "What the refresh found",
                    result["refresh"]["report"]
                        .as_str()
                        .unwrap_or("")
                        .to_string(),
                ),
                (
                    "What the last build said",
                    result["build"]["report"].as_str().unwrap_or("").to_string(),
                ),
                (
                    "The current architecture.json",
                    if result["architecture"].is_null() {
                        String::new()
                    } else {
                        result["architecture"].to_string()
                    },
                ),
                ("What Coderhelm already knows about each repository", known),
            ];
            let answer = run_job(
                state,
                &provider,
                provider.primary_model_id(),
                name,
                &inputs,
                &snapshot,
                &mut usage,
            )
            .await
            .ok_or("the map's parts couldn't be worked out this time")?;
            job.answers
                .insert("floorplan/architecture.json".into(), answer);
            next_step(state, team_id, &job, build, turn).await
        }
        Next::Describe(keys) => {
            job.desc_attempts += 1;
            progress(
                state,
                team_id,
                build,
                Some(am::STAGES[2]),
                "Describing each part, file and endpoint",
            )
            .await?;
            let style = result["describe"]["style"]
                .as_str()
                .unwrap_or("")
                .to_string();
            let sheets: Vec<&Value> = result["worksheets"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|w| {
                    w["key"]
                        .as_str()
                        .is_some_and(|k| keys.iter().any(|x| x == k))
                })
                .collect();
            let light = state.config.light_model_id.clone();
            for chunk in sheets.chunks(PARALLEL_JOBS) {
                let runs = chunk.iter().map(|w| {
                    let inputs = vec![
                        (
                            "Worksheet",
                            w["worksheet"].as_str().unwrap_or("").to_string(),
                        ),
                        (
                            "The file to fill in, as it stands",
                            w["descriptions"].to_string(),
                        ),
                        ("Who you're writing for", style.clone()),
                    ];
                    let provider = &provider;
                    let snapshot = &snapshot;
                    let light = light.clone();
                    async move {
                        let mut u = TokenUsage::default();
                        let a = run_job(
                            state,
                            provider,
                            &light,
                            "descriptions",
                            &inputs,
                            snapshot,
                            &mut u,
                        )
                        .await;
                        (w["descriptionsPath"].as_str().unwrap_or("").to_string(), a)
                    }
                });
                for (path, answer) in futures::future::join_all(runs).await {
                    if let Some(a) = answer {
                        job.answers.insert(path, a);
                    }
                }
            }
            next_step(state, team_id, &job, build, turn).await
        }
        Next::Stop(why) => {
            s3_put(
                state,
                &am::job_key(team_id),
                serde_json::to_vec(&job).unwrap_or_default(),
            )
            .await?;
            Err(why)
        }
        Next::Done => {
            let graph = file("graph.b64")
                .and_then(b64)
                .ok_or("the build wrote no map")?;
            let json = am::gunzip(&graph, am::MAX_GRAPH_BYTES).ok_or("the map is too large")?;
            let mut g: Value = serde_json::from_slice(&json).map_err(|e| e.to_string())?;
            am::stamp_repos(
                &mut g,
                &folders
                    .iter()
                    .filter(|(r, _)| job.heads.contains_key(*r))
                    .map(|(r, f)| (r.clone(), f.clone()))
                    .collect(),
            );
            s3_put(
                state,
                &am::graph_key(team_id),
                am::gzip(g.to_string().as_bytes()),
            )
            .await?;
            // The page's copy and the index the read API places overlays
            // with, so a page view never reads the whole map.
            am::store_derived(&state.s3, &state.config.bucket_name, team_id, &g).await?;
            job.built = job.heads.clone();
            job.arch_attempts = 0;
            job.desc_attempts = 0;
            s3_put(
                state,
                &am::job_key(team_id),
                serde_json::to_vec(&job).unwrap_or_default(),
            )
            .await?;
            let n = job.heads.len() as u32;
            change(state, team_id, |cur, now| {
                am::finished(cur, build, n, "", now)
            })
            .await?;
            info!(
                team_id,
                files = g["files"].as_array().map(|a| a.len()).unwrap_or(0),
                "App map: built"
            );
            Ok(())
        }
    }
}

async fn next_step(
    state: &WorkerState,
    team_id: &str,
    job: &Job,
    build: &str,
    turn: u32,
) -> Result<(), String> {
    s3_put(
        state,
        &am::job_key(team_id),
        serde_json::to_vec(job).unwrap_or_default(),
    )
    .await?;
    if queue(state, team_id, "build", build, turn, 0).await {
        Ok(())
    } else {
        Err("the next step couldn't be queued".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_duplicate_message_for_a_build_already_running_is_skipped() {
        let now = 10_000;
        let msg = |build: &str, step: &str, turn: u32| AppMapMessage {
            team_id: "TEAM#harbor".into(),
            step: step.into(),
            build: build.into(),
            turn,
            ..Default::default()
        };
        let queued = am::queue_next(None, now, "b1").unwrap();
        // The build's start message runs once.
        let running = turn_to_take(&msg("b1", "", 0), Some(&queued), now).unwrap();
        assert_eq!((running.state.as_str(), running.turn), ("building", 1));
        // The same start message again, or a start for another build while
        // this one runs: skipped.
        assert_eq!(
            turn_to_take(&msg("b1", "", 0), Some(&running), now + 3),
            None
        );
        assert_eq!(
            turn_to_take(&msg("b2", "", 0), Some(&running), now + 3),
            None
        );
        // The build's next step runs once, too.
        let next = turn_to_take(&msg("b1", "build", 1), Some(&running), now + 60).unwrap();
        assert_eq!(next.turn, 2);
        assert_eq!(
            turn_to_take(&msg("b1", "build", 1), Some(&next), now + 61),
            None
        );
        // No status at all (nothing asked for a build): nothing runs.
        assert_eq!(turn_to_take(&msg("b1", "", 0), None, now), None);
    }

    #[test]
    fn the_steps_follow_floorplans_workflow() {
        let job = Job::default();
        // A first map: the scan is in, no architecture yet.
        let first = json!({"scan": {"ok": true, "summary": "REPOS (2)"}});
        assert_eq!(
            decide(&first, &job),
            Next::Architecture {
                job: "architecture",
                item: String::new()
            }
        );
        // The build failed: the architecture again, until the limit.
        let failed =
            json!({"architecture": {}, "build": {"ok": false, "errors": ["nodes[0] is wrong"]}});
        assert!(matches!(decide(&failed, &job), Next::Architecture { .. }));
        let spent = Job {
            arch_attempts: MAX_ARCH_ATTEMPTS,
            ..Default::default()
        };
        assert_eq!(
            decide(&failed, &spent),
            Next::Stop("nodes[0] is wrong".into())
        );
        // Too much left unassigned: the architecture again.
        let loose = json!({"architecture": {}, "build": {"ok": true, "files": {"unassignedPercent": 25.0}, "descriptions": {"complete": true}}});
        assert!(matches!(decide(&loose, &job), Next::Architecture { .. }));
        // Descriptions missing: describe only the worksheets listed.
        let todo = json!({"architecture": {}, "build": {"ok": true, "files": {"unassignedPercent": 0.0}, "descriptions": {"complete": false}}, "worksheets": [{"key": "api"}]});
        assert_eq!(decide(&todo, &job), Next::Describe(vec!["api".into()]));
        // A refresh: new folders first, then nothing left when Floorplan
        // carried every description over (no model call at all).
        let moved = json!({"architecture": {}, "refresh": {"next": [{"code": "assign_new_files", "text": "Give harbor-api/src/fuel a home"}, {"code": "recent_work"}]}, "build": {"ok": true, "files": {"unassignedPercent": 4.0}, "descriptions": {"complete": true}}});
        assert_eq!(
            decide(&moved, &job),
            Next::Architecture {
                job: "update_architecture",
                item: "Give harbor-api/src/fuel a home".into()
            }
        );
        let same = json!({"architecture": {}, "refresh": {"next": [{"code": "recent_work"}]}, "build": {"ok": true, "files": {"unassignedPercent": 0.0}, "descriptions": {"complete": true}}});
        assert_eq!(decide(&same, &job), Next::Done);
    }

    #[test]
    fn snapshots_keep_text_files_under_their_folder() {
        let mut tar = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        for (p, b) in [
            (
                "harbor-api-abc123/src/server.js",
                &b"app.listen(4000);\n"[..],
            ),
            ("harbor-api-abc123/logo.png", &b"\x89PNG\0\0"[..]),
            ("harbor-api-abc123/.git/config", &b"x"[..]),
        ] {
            let mut h = tar::Header::new_gnu();
            h.set_size(b.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            tar.append_data(&mut h, p, b).unwrap();
        }
        let bytes = tar.into_inner().unwrap().finish().unwrap();
        let mut budget = MAX_SNAPSHOT_BYTES;
        let e = repo_entries(&bytes, "harbor-api", &mut budget);
        assert_eq!(
            e.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>(),
            vec!["root/harbor-api/src/server.js"]
        );
        let packed = pack(&json!({"mode": "start"}), &e).unwrap();
        let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(&packed[..]));
        let names: Vec<String> = ar
            .entries()
            .unwrap()
            .flatten()
            .map(|x| x.path().unwrap().to_string_lossy().to_string())
            .collect();
        assert!(names.contains(&"app-map/driver.mjs".to_string()));
        assert!(names.contains(&"app-map/fp/skills/floorplan/scripts/scan.mjs".to_string()));
        assert!(names.contains(&"app-map/fp/LICENSE".to_string()));
        assert!(names.contains(&"app-map/root/harbor-api/src/server.js".to_string()));
    }

    #[test]
    fn the_model_tools_read_only_the_snapshot() {
        let s = Snapshot::from_entries(&[
            (
                "root/harbor-api/src/server.js".into(),
                b"const app = express();\napp.listen(4000);\n".to_vec(),
            ),
            (
                "root/harbor-web/app/page.tsx".into(),
                b"export default () => null;\n".to_vec(),
            ),
            ("root/floorplan/.cache/scan.json".into(), b"{}".to_vec()),
        ]);
        assert_eq!(
            s.list_files(&json!({"glob": "harbor-api/**"})),
            "harbor-api/src/server.js"
        );
        assert_eq!(
            s.list_files(&json!({"glob": "**/*.tsx"})),
            "harbor-web/app/page.tsx"
        );
        assert!(s
            .read_file(&json!({"path": "harbor-api/src/server.js", "start_line": 2}))
            .starts_with("2\tapp.listen"));
        assert!(s
            .read_file(&json!({"path": "../etc/passwd"}))
            .starts_with("No file"));
        assert_eq!(
            s.grep(&json!({"pattern": "listen\\("})),
            "harbor-api/src/server.js:2: app.listen(4000);"
        );
        assert!(s
            .list_files(&json!({"glob": "floorplan/**"}))
            .starts_with("No files"));
    }

    #[tokio::test]
    async fn submit_accepts_only_an_answer_that_fits_the_schema() {
        let snapshot = Snapshot::from_entries(&[]);
        let wf: Value = serde_json::from_str(am::WORKFLOW_JSON).unwrap();
        let t = JobTools {
            snapshot: &snapshot,
            schema: wf["schemas"]["descriptions"].clone(),
            answer: std::sync::Mutex::new(None),
        };
        let bad = t
            .execute(
                "submit",
                &json!({"answer": {"files": {"a.js": {"name": ""}}}}),
            )
            .await
            .unwrap();
        assert!(bad.as_str().unwrap().starts_with("Not accepted"));
        assert!(t.answer.lock().unwrap().is_none());
        let good =
            json!({"files": {"a.js": {"name": "Start", "description": "Starts the service."}}});
        let ok = t
            .execute("submit", &json!({"answer": good.clone()}))
            .await
            .unwrap();
        assert!(ok.as_str().unwrap().starts_with("Accepted"));
        assert_eq!(t.answer.lock().unwrap().clone(), Some(good));
    }

    #[test]
    fn each_job_gets_floorplans_own_instructions_references_and_schema() {
        let (system, user, schema) = job_prompt(
            "descriptions",
            &[
                ("Worksheet", "## harbor-api/src/server.js".into()),
                ("Empty", String::new()),
            ],
        )
        .unwrap();
        assert!(system.contains("--- references/writing-descriptions.md ---"));
        assert!(system.contains("You name and describe the files and endpoints"));
        assert!(user.contains("## Worksheet") && !user.contains("## Empty"));
        assert_eq!(schema["required"], json!(["files"]));
        assert!(job_prompt("architecture", &[])
            .unwrap()
            .0
            .contains("--- references/schema.md ---"));
        assert!(job_prompt("nope", &[]).is_none());
        let names: Vec<String> = tools().into_iter().map(|t| t.name).collect();
        assert_eq!(names, vec!["read_file", "list_files", "grep", "submit"]);
    }
}
