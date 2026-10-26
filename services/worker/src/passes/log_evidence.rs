//! The evidence step every fix run started from a log finding takes before
//! it plans: what the team's read-only AWS connection says about the error.
//!
//! - which log group and which tasks emit the error, with counts and the
//!   first and last time (CloudWatch Logs Insights);
//! - the ECS service behind the log group and its task definition: the
//!   `command` and `entryPoint` production starts with, ephemeral storage
//!   (none set means 20 GiB on Fargate), cpu and memory;
//! - each running task's start time against its first error, which gives a
//!   fill or growth rate;
//! - which tasks are failing right now.
//!
//! Every lookup records what it returned or why it couldn't, so a later
//! claim that something is "unconfirmed" can be checked against the access
//! the run had (see `log_fix_checks`). Only reads: list and describe calls
//! and Logs Insights queries. The facts are stored on the run and given to
//! every agent that plans, writes or describes the change.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

/// Fargate's ephemeral storage when a task definition sets none.
pub const FARGATE_DEFAULT_GIB: i32 = 20;

/// How far back the error is counted.
pub const WINDOW_HOURS: i64 = 7 * 24;

/// A task whose last error is this recent is failing now.
pub const FAILING_NOW_MINUTES: i64 = 15;

/// Most services described while looking for the one behind the log group.
const MAX_SERVICES: usize = 60;

/// One log stream's matching lines.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamCount {
    pub stream: String,
    pub count: u64,
    /// Milliseconds since the epoch.
    pub first_ms: i64,
    pub last_ms: i64,
}

/// One ECS service.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceFact {
    pub cluster: String,
    pub name: String,
    pub task_definition: String,
    pub launch_type: String,
    pub running: i32,
}

/// One container in a task definition.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerFact {
    pub name: String,
    pub image: String,
    /// Overrides the image's Docker CMD when set.
    pub command: Vec<String>,
    /// Overrides the image's Docker ENTRYPOINT when set.
    pub entry_point: Vec<String>,
    pub working_directory: String,
    pub log_group: String,
    pub stream_prefix: String,
}

/// A task definition, as described.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskDefFact {
    pub arn: String,
    pub family: String,
    pub revision: i32,
    pub cpu: String,
    pub memory: String,
    /// `ephemeralStorage.sizeInGiB` as set; `None` when the task definition
    /// sets none.
    pub ephemeral_gib: Option<i32>,
    pub requires_fargate: bool,
    pub containers: Vec<ContainerFact>,
}

impl TaskDefFact {
    /// The disk a task gets: what the definition sets, else Fargate's
    /// default. `None` on EC2 without a setting (the host's disk).
    pub fn disk_gib(&self, launch_type: &str) -> Option<i32> {
        self.ephemeral_gib.or_else(|| {
            (self.requires_fargate || launch_type.eq_ignore_ascii_case("FARGATE"))
                .then_some(FARGATE_DEFAULT_GIB)
        })
    }

    /// The container that writes to `group`.
    pub fn container_for(&self, group: &str) -> Option<&ContainerFact> {
        self.containers.iter().find(|c| c.log_group == group)
    }
}

/// One task of the service, as described.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskFact {
    pub id: String,
    pub last_status: String,
    pub started_ms: Option<i64>,
    pub task_definition: String,
}

/// One task joined with its errors.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskErrors {
    pub task_id: String,
    pub running: bool,
    pub started_at: String,
    pub errors: u64,
    pub first_error_at: String,
    pub last_error_at: String,
    /// Hours from the task's start to its first error.
    pub hours_to_first_error: Option<f64>,
    /// The disk divided by those hours, in MB per hour.
    pub fill_mb_per_hour: Option<f64>,
    pub failing_now: bool,
}

/// One lookup the step made and what came of it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceTry {
    /// The read it made: `logs:StartQuery`, `ecs:DescribeTaskDefinition`.
    pub source: String,
    /// What it was for, in plain words.
    pub question: String,
    pub ok: bool,
    /// A short summary of what came back, or the error.
    pub outcome: String,
}

/// What the step found.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LogEvidence {
    pub log_group: String,
    pub needle: String,
    pub window_start: String,
    pub window_end: String,
    pub total_errors: u64,
    pub streams: Vec<StreamCount>,
    pub service: Option<ServiceFact>,
    pub task_def: Option<TaskDefFact>,
    /// The disk each task gets, in GiB, and where that number comes from.
    pub disk_gib: Option<i32>,
    pub disk_basis: String,
    pub tasks: Vec<TaskErrors>,
    /// Median fill rate over the tasks that have one, MB per hour.
    pub fill_mb_per_hour: Option<f64>,
    pub tried: Vec<SourceTry>,
    /// How the container starts, resolved from the repository (set before
    /// the PR, see [`resolve_start`]).
    #[serde(default)]
    pub start: Option<StartPath>,
}

impl LogEvidence {
    /// Every lookup that failed, by source.
    pub fn failed_sources(&self) -> Vec<&SourceTry> {
        self.tried.iter().filter(|t| !t.ok).collect()
    }

    /// The tasks failing now.
    pub fn failing_now(&self) -> Vec<&TaskErrors> {
        self.tasks.iter().filter(|t| t.failing_now).collect()
    }
}

/// The read-only lookups the step makes. The worker's is the team's AWS
/// connection; tests use a fake.
#[async_trait::async_trait]
pub trait Reader: Send + Sync {
    /// Per log stream: lines in `group` containing `needle`, with the first
    /// and last time, between `start` and `end` (seconds since the epoch).
    async fn error_streams(
        &self,
        group: &str,
        needle: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<StreamCount>, String>;
    async fn clusters(&self) -> Result<Vec<String>, String>;
    /// The services of one cluster, described.
    async fn services(&self, cluster: &str) -> Result<Vec<ServiceFact>, String>;
    async fn task_definition(&self, arn: &str) -> Result<TaskDefFact, String>;
    /// The service's tasks, described.
    async fn tasks(&self, cluster: &str, service: &str) -> Result<Vec<TaskFact>, String>;
}

/// What the step looks for.
#[derive(Debug, Clone, Default)]
pub struct Target {
    pub log_group: String,
    /// A literal piece of the error line.
    pub needle: String,
}

fn ms_text(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_default()
}

/// The task id an awslogs stream name ends in (`prefix/container/task-id`).
pub fn task_id_of_stream(stream: &str) -> &str {
    stream.rsplit('/').next().unwrap_or(stream)
}

fn try_ok(tried: &mut Vec<SourceTry>, source: &str, question: &str, outcome: String) {
    tried.push(SourceTry {
        source: source.into(),
        question: question.into(),
        ok: true,
        outcome,
    });
}

fn try_err(tried: &mut Vec<SourceTry>, source: &str, question: &str, e: String) {
    tried.push(SourceTry {
        source: source.into(),
        question: question.into(),
        ok: false,
        outcome: common::truncate_str(&e, 300).to_string(),
    });
}

/// Runs the lookups and works out the numbers. Never fails: what a lookup
/// couldn't answer is recorded with the source tried.
pub async fn gather(reader: &dyn Reader, target: &Target, now: DateTime<Utc>) -> LogEvidence {
    let end = now.timestamp();
    let start = end - WINDOW_HOURS * 3600;
    let mut ev = LogEvidence {
        log_group: target.log_group.clone(),
        needle: target.needle.clone(),
        window_start: ms_text(start * 1000),
        window_end: ms_text(end * 1000),
        ..Default::default()
    };

    // 1. Which streams (tasks) emit the error.
    let q = "which tasks write the error, how often, first and last time";
    match reader
        .error_streams(&target.log_group, &target.needle, start, end)
        .await
    {
        Ok(mut s) => {
            s.sort_by_key(|x| std::cmp::Reverse(x.count));
            ev.total_errors = s.iter().map(|x| x.count).sum();
            try_ok(
                &mut ev.tried,
                "logs:StartQuery",
                q,
                format!(
                    "{} matching lines in {} streams of {}",
                    ev.total_errors,
                    s.len(),
                    target.log_group
                ),
            );
            ev.streams = s;
        }
        Err(e) => try_err(&mut ev.tried, "logs:StartQuery", q, e),
    }

    // 2. The service whose task definition logs to this group.
    let q = "the ECS service and task definition that write to the log group";
    let clusters = match reader.clusters().await {
        Ok(c) => {
            try_ok(
                &mut ev.tried,
                "ecs:ListClusters",
                q,
                format!("{} clusters", c.len()),
            );
            c
        }
        Err(e) => {
            try_err(&mut ev.tried, "ecs:ListClusters", q, e);
            Vec::new()
        }
    };
    let mut described: BTreeMap<String, TaskDefFact> = BTreeMap::new();
    let mut seen = 0usize;
    'clusters: for cluster in &clusters {
        let services = match reader.services(cluster).await {
            Ok(s) => s,
            Err(e) => {
                try_err(&mut ev.tried, "ecs:DescribeServices", q, e);
                continue;
            }
        };
        for svc in services {
            seen += 1;
            if seen > MAX_SERVICES {
                break 'clusters;
            }
            let td = match described.get(&svc.task_definition) {
                Some(td) => td.clone(),
                None => match reader.task_definition(&svc.task_definition).await {
                    Ok(td) => {
                        described.insert(svc.task_definition.clone(), td.clone());
                        td
                    }
                    Err(e) => {
                        try_err(&mut ev.tried, "ecs:DescribeTaskDefinition", q, e);
                        continue;
                    }
                },
            };
            if td.container_for(&target.log_group).is_some() {
                try_ok(
                    &mut ev.tried,
                    "ecs:DescribeTaskDefinition",
                    q,
                    format!(
                        "service {} in {} runs {}:{}",
                        svc.name, svc.cluster, td.family, td.revision
                    ),
                );
                ev.disk_gib = td.disk_gib(&svc.launch_type);
                ev.disk_basis = match (td.ephemeral_gib, ev.disk_gib) {
                    (Some(g), _) => format!("ephemeralStorage.sizeInGiB = {g}"),
                    (None, Some(g)) => {
                        format!("ephemeralStorage not set, so Fargate's default of {g} GiB")
                    }
                    (None, None) => {
                        "ephemeralStorage not set and not on Fargate: the host's disk".into()
                    }
                };
                ev.task_def = Some(td);
                ev.service = Some(svc);
                break 'clusters;
            }
        }
    }
    if ev.service.is_none() && !clusters.is_empty() {
        try_err(
            &mut ev.tried,
            "ecs:DescribeTaskDefinition",
            q,
            format!(
                "no service's task definition logs to {} ({} services looked at)",
                target.log_group,
                seen.min(MAX_SERVICES)
            ),
        );
    }

    // 3. The service's tasks, joined with their errors.
    if let Some(svc) = ev.service.clone() {
        let q = "each task's start time against its first error";
        match reader.tasks(&svc.cluster, &svc.name).await {
            Ok(tasks) => {
                try_ok(
                    &mut ev.tried,
                    "ecs:DescribeTasks",
                    q,
                    format!("{} tasks", tasks.len()),
                );
                ev.tasks = join_tasks(&tasks, &ev.streams, ev.disk_gib, now);
            }
            Err(e) => try_err(&mut ev.tried, "ecs:DescribeTasks", q, e),
        }
    }
    // Streams of tasks that are gone still show who failed and when.
    for s in &ev.streams {
        let id = task_id_of_stream(&s.stream);
        if !ev.tasks.iter().any(|t| t.task_id == id) {
            ev.tasks.push(TaskErrors {
                task_id: id.to_string(),
                running: false,
                errors: s.count,
                first_error_at: ms_text(s.first_ms),
                last_error_at: ms_text(s.last_ms),
                ..Default::default()
            });
        }
    }
    ev.fill_mb_per_hour = median(ev.tasks.iter().filter_map(|t| t.fill_mb_per_hour).collect());
    ev
}

fn median(mut v: Vec<f64>) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(v[v.len() / 2])
}

/// Pure: each task with its errors, the hours from its start to its first
/// error, the fill rate those hours give, and whether it is failing now.
pub fn join_tasks(
    tasks: &[TaskFact],
    streams: &[StreamCount],
    disk_gib: Option<i32>,
    now: DateTime<Utc>,
) -> Vec<TaskErrors> {
    let now_ms = now.timestamp_millis();
    tasks
        .iter()
        .map(|t| {
            let s = streams
                .iter()
                .find(|s| task_id_of_stream(&s.stream) == t.id);
            let running = t.last_status.eq_ignore_ascii_case("RUNNING");
            let hours = match (t.started_ms, s) {
                (Some(st), Some(s)) if s.first_ms > st => {
                    Some((s.first_ms - st) as f64 / 3_600_000.0)
                }
                _ => None,
            };
            let fill = match (hours, disk_gib) {
                (Some(h), Some(g)) if h >= 0.1 => Some(g as f64 * 1024.0 / h),
                _ => None,
            };
            TaskErrors {
                task_id: t.id.clone(),
                running,
                started_at: t.started_ms.map(ms_text).unwrap_or_default(),
                errors: s.map_or(0, |s| s.count),
                first_error_at: s.map(|s| ms_text(s.first_ms)).unwrap_or_default(),
                last_error_at: s.map(|s| ms_text(s.last_ms)).unwrap_or_default(),
                hours_to_first_error: hours.map(|h| (h * 10.0).round() / 10.0),
                fill_mb_per_hour: fill.map(f64::round),
                failing_now: running
                    && s.is_some_and(|s| now_ms - s.last_ms <= FAILING_NOW_MINUTES * 60_000),
            }
        })
        .collect()
}

/// How production starts the container, resolved from the repository.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartPath {
    /// What runs: the task definition's entryPoint and command, or the
    /// Dockerfile's when the task definition sets none.
    pub command: String,
    /// Where the command came from (`task definition fam:3`, `Dockerfile`).
    pub from: String,
    /// The package script the command runs, and what it is.
    pub script: String,
    pub working_directory: String,
    /// The repository files read to resolve it.
    pub files_read: Vec<String>,
    /// The command is known and the files that say what it runs were read.
    pub verified: bool,
    /// What is missing when not verified.
    pub missing: String,
}

impl StartPath {
    /// One line for the PR description.
    pub fn line(&self) -> String {
        let mut s = format!(
            "Production starts the container with `{}` ({})",
            self.command, self.from
        );
        match self.script.split_once(": ") {
            Some((name, run)) => s.push_str(&format!(", which runs the `{name}` script (`{run}`)")),
            None if !self.script.is_empty() => {
                s.push_str(&format!(", which runs `{}`", self.script))
            }
            None => {}
        }
        if !self.working_directory.is_empty() {
            s.push_str(&format!(", working directory `{}`", self.working_directory));
        }
        s.push('.');
        s
    }
}

/// Pure: the package script a start command runs (`npm start`,
/// `npm run serve`, `yarn start`, `pnpm run x`), by name.
pub fn script_name(command: &str) -> Option<String> {
    let words: Vec<&str> = command.split_whitespace().collect();
    let pm = words
        .iter()
        .position(|w| matches!(*w, "npm" | "yarn" | "pnpm" | "bun"))?;
    let rest = &words[pm + 1..];
    match rest {
        ["start", ..] => Some("start".into()),
        ["run", name, ..] | ["run-script", name, ..] => Some(name.to_string()),
        [name, ..] if words[pm] != "npm" && !name.starts_with('-') => Some(name.to_string()),
        _ => None,
    }
}

/// Pure: the CMD and ENTRYPOINT and WORKDIR a Dockerfile ends with.
pub fn dockerfile_start(dockerfile: &str) -> (String, String) {
    let mut cmd = String::new();
    let mut entry = String::new();
    let mut workdir = String::new();
    for line in dockerfile.lines() {
        let t = line.trim();
        let upper = t.to_ascii_uppercase();
        if let Some(rest) = upper.strip_prefix("CMD ").map(|_| &t[4..]) {
            cmd = exec_form(rest);
        } else if upper.starts_with("ENTRYPOINT ") {
            entry = exec_form(&t[11..]);
        } else if upper.starts_with("WORKDIR ") {
            workdir = t[8..].trim().to_string();
        }
    }
    let command = [entry, cmd]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    (command, workdir)
}

fn exec_form(s: &str) -> String {
    let s = s.trim();
    match serde_json::from_str::<Vec<String>>(s) {
        Ok(v) => v.join(" "),
        Err(_) => s.to_string(),
    }
}

/// What [`resolve_start`] reads from the repository.
#[async_trait::async_trait]
pub trait RepoFiles: Send + Sync {
    async fn read(&self, path: &str) -> Option<String>;
}

/// Resolves how production starts the container behind the log group, from
/// the task definition and the repository's start files.
pub async fn resolve_start(files: &dyn RepoFiles, ev: &LogEvidence) -> StartPath {
    let mut sp = StartPath::default();
    let container = ev
        .task_def
        .as_ref()
        .and_then(|td| td.container_for(&ev.log_group).map(|c| (td, c)));
    let dockerfile = files.read("Dockerfile").await;
    if dockerfile.is_some() {
        sp.files_read.push("Dockerfile".into());
    }
    let (docker_cmd, docker_wd) = dockerfile
        .as_deref()
        .map(dockerfile_start)
        .unwrap_or_default();
    match container {
        Some((td, c)) if !c.command.is_empty() || !c.entry_point.is_empty() => {
            sp.command = c
                .entry_point
                .iter()
                .chain(c.command.iter())
                .cloned()
                .collect::<Vec<_>>()
                .join(" ");
            sp.from = format!(
                "task definition {}:{} `command`, which overrides the image's CMD",
                td.family, td.revision
            );
            sp.working_directory = if c.working_directory.is_empty() {
                docker_wd.clone()
            } else {
                c.working_directory.clone()
            };
        }
        Some((td, _)) if !docker_cmd.is_empty() => {
            sp.command = docker_cmd.clone();
            sp.from = format!(
                "the Dockerfile's CMD; task definition {}:{} sets no command",
                td.family, td.revision
            );
            sp.working_directory = docker_wd.clone();
        }
        None if !docker_cmd.is_empty() => {
            sp.command = docker_cmd.clone();
            sp.from = "the Dockerfile's CMD; the task definition could not be read".into();
            sp.working_directory = docker_wd.clone();
        }
        _ => {}
    }
    if sp.command.is_empty() {
        sp.missing =
            "neither the task definition nor a root Dockerfile names a start command".into();
        return sp;
    }
    if let Some(name) = script_name(&sp.command) {
        let pkg = files.read("package.json").await;
        if pkg.is_some() {
            sp.files_read.push("package.json".into());
        }
        let script = pkg
            .as_deref()
            .and_then(|p| serde_json::from_str::<serde_json::Value>(p).ok())
            .and_then(|v| v["scripts"][&name].as_str().map(String::from));
        match script {
            Some(s) => sp.script = format!("{name}: {s}"),
            None => {
                sp.missing = format!("package.json has no `{name}` script");
                return sp;
            }
        }
    }
    // Verified: the command is known from the task definition (or the
    // Dockerfile it falls back to), and the files it names were read.
    sp.verified = container.is_some() || !docker_cmd.is_empty();
    if !sp.verified {
        sp.missing = "the task definition could not be read".into();
    }
    sp
}

fn fmt_f(v: f64) -> String {
    if v >= 100.0 {
        format!("{v:.0}")
    } else {
        format!("{v:.1}")
    }
}

/// The facts as the agents read them.
pub fn facts_markdown(ev: &LogEvidence) -> String {
    let mut out =
        String::from("## What the logs and the AWS account show (read before planning)\n");
    out.push_str(&format!(
        "Window: {} to {}. Log group: `{}`. Error text searched: `{}`.\n",
        ev.window_start, ev.window_end, ev.log_group, ev.needle
    ));
    if !ev.streams.is_empty() {
        out.push_str(&format!(
            "* {} matching lines from {} log streams.\n",
            ev.total_errors,
            ev.streams.len()
        ));
    }
    if let (Some(svc), Some(td)) = (&ev.service, &ev.task_def) {
        out.push_str(&format!(
            "* Service `{}` in cluster `{}` ({} running, launch type {}), task definition `{}:{}`: cpu {}, memory {}.\n",
            svc.name,
            svc.cluster,
            svc.running,
            if svc.launch_type.is_empty() { "not set" } else { &svc.launch_type },
            td.family,
            td.revision,
            if td.cpu.is_empty() { "not set" } else { &td.cpu },
            if td.memory.is_empty() { "not set" } else { &td.memory },
        ));
        out.push_str(&format!(
            "* Disk per task: {} ({}).\n",
            ev.disk_gib
                .map(|g| format!("{g} GiB"))
                .unwrap_or_else(|| "unknown".into()),
            ev.disk_basis
        ));
        if let Some(c) = td.container_for(&ev.log_group) {
            out.push_str(&format!(
                "* Container `{}`: entryPoint {}, command {}{}.\n",
                c.name,
                if c.entry_point.is_empty() {
                    "not set (the image's ENTRYPOINT)".to_string()
                } else {
                    format!("`{}`", c.entry_point.join(" "))
                },
                if c.command.is_empty() {
                    "not set (the image's CMD)".to_string()
                } else {
                    format!("`{}` (overrides the image's CMD)", c.command.join(" "))
                },
                if c.working_directory.is_empty() {
                    String::new()
                } else {
                    format!(", working directory `{}`", c.working_directory)
                }
            ));
        }
    }
    if !ev.tasks.is_empty() {
        out.push_str("* Tasks (id, running, started, errors, first error, last error, hours from start to first error, MB/h that gives):\n");
        for t in ev.tasks.iter().take(12) {
            out.push_str(&format!(
                "  * `{}`: {}, started {}, {} errors, first {}, last {}, {}, {}{}\n",
                t.task_id,
                if t.running { "running" } else { "not running" },
                if t.started_at.is_empty() {
                    "unknown"
                } else {
                    &t.started_at
                },
                t.errors,
                if t.first_error_at.is_empty() {
                    "none"
                } else {
                    &t.first_error_at
                },
                if t.last_error_at.is_empty() {
                    "none"
                } else {
                    &t.last_error_at
                },
                t.hours_to_first_error
                    .map(|h| format!("{} h", fmt_f(h)))
                    .unwrap_or_else(|| "n/a".into()),
                t.fill_mb_per_hour
                    .map(|r| format!("{} MB/h", fmt_f(r)))
                    .unwrap_or_else(|| "n/a".into()),
                if t.failing_now {
                    " — FAILING NOW"
                } else {
                    ""
                },
            ));
        }
    }
    if let Some(r) = ev.fill_mb_per_hour {
        out.push_str(&format!(
            "* Median fill rate: about {} MB/h (disk divided by the hours from a task's start to its first error).\n",
            fmt_f(r)
        ));
    }
    let failing = ev.failing_now();
    if !failing.is_empty() {
        out.push_str(&format!(
            "* Failing right now: {}. A person can relieve it (for example by stopping that task so the service starts a fresh one); agents never take that action.\n",
            failing
                .iter()
                .map(|t| format!("`{}` (errors since {})", t.task_id, t.first_error_at))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(sp) = &ev.start {
        if sp.verified {
            out.push_str(&format!("* {}\n", sp.line()));
        } else {
            out.push_str(&format!("* Start path not resolved: {}.\n", sp.missing));
        }
    }
    out.push_str("\nSources tried:\n");
    for t in &ev.tried {
        out.push_str(&format!(
            "* `{}` ({}): {}{}\n",
            t.source,
            t.question,
            if t.ok { "" } else { "FAILED: " },
            t.outcome
        ));
    }
    out
}

/// The rules for a fix that starts from a log finding, given to every agent
/// of the run with the facts.
pub const LOG_RUN_RULES: &str = r#"## Rules for a fix that starts from a log finding
- Settle facts with the access this run has. The facts above came from the team's AWS account. Never write "unconfirmed", "could not confirm" or "inferred" about anything they answer. Call something open only when no source above can answer it, and name the source that was tried (for example `ecs:DescribeTasks`).
- Check how the container actually starts. ECS `command` overrides the image's Docker CMD and `entryPoint` its ENTRYPOINT. Read the task definition's command above and the repository's start scripts (package.json scripts, Dockerfile, entrypoint scripts). Show that the code you change is on the path production runs, and that paths built from the working directory resolve under that start command.
- Size limits come from measured data. Put the arithmetic in the PR description, for example "disk 20 GiB, fills in about 59 h, so about 350 MB/h; a budget that keeps N hours is X".
- Fix the symptom and name the cause. Put the probable cause of the growth under "Not in this PR" as a follow-up.
- When a task is failing right now, say so with its id and since when, and say what relieves it (for example stopping that task). Leave that action to a person; never take it.
- When the facts contradict a ticket detail, correct it in one line with the numbers, then move on.
- Code comments say what the code does, not the story of the bug.
- The PR description is short: the problem with numbers, the changes, why the limits were chosen, risk, verification. No lists of unconfirmed or open items.
- When the fix also needs a change in another connected repository (the infrastructure code that defines the task, its storage or its alarms), plan it as its own `## owner/repo` group in tasks.md. CoderHelm opens a linked pull request there."#;

/// The facts and the rules, as one block for the agents' instructions.
pub fn instructions_block(ev: &LogEvidence) -> String {
    format!("{}\n\n{LOG_RUN_RULES}", facts_markdown(ev))
}

/// Appends the block to a run's instructions (once).
pub fn with_instructions(instructions: &str, ev: &LogEvidence) -> String {
    let block = instructions_block(ev);
    let head = "## What the logs and the AWS account show";
    let base = match instructions.find(head) {
        Some(i) => instructions[..i].trim_end(),
        None => instructions.trim_end(),
    };
    if base.is_empty() {
        block
    } else {
        format!("{base}\n\n{block}")
    }
}

/// Parses a Logs Insights timestamp (`2026-10-24 03:12:45.123`, UTC).
pub fn parse_insights_time(s: &str) -> Option<i64> {
    NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S%.f")
        .ok()
        .map(|t| t.and_utc().timestamp_millis())
}

/// The team's AWS connection as a [`Reader`]: Logs Insights and ECS list and
/// describe calls through the log reader role. Nothing is written.
pub struct AwsReader {
    logs: aws_sdk_cloudwatchlogs::Client,
    ecs: aws_sdk_ecs::Client,
}

impl AwsReader {
    pub fn new(session: &common::live::aws::Session) -> Self {
        use aws_sdk_ecs::config::{BehaviorVersion, Credentials, Region};
        let creds = Credentials::new(
            &session.access_key_id,
            &session.secret_access_key,
            Some(session.session_token.clone()),
            None,
            "coderhelm-team-role",
        );
        let ecs = aws_sdk_ecs::Client::from_conf(
            aws_sdk_ecs::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new(session.region.clone()))
                .credentials_provider(creds)
                .build(),
        );
        Self {
            logs: common::live::aws::logs_client(session),
            ecs,
        }
    }
}

fn ecs_err<E: std::error::Error + 'static>(e: aws_sdk_ecs::error::SdkError<E>) -> String {
    aws_sdk_ecs::error::DisplayErrorContext(e).to_string()
}

#[async_trait::async_trait]
impl Reader for AwsReader {
    async fn error_streams(
        &self,
        group: &str,
        needle: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<StreamCount>, String> {
        use aws_sdk_cloudwatchlogs::types::QueryStatus;
        let filter = if needle.is_empty() {
            "filter @message like /(?i)(error|exception|fatal|panic)/".to_string()
        } else {
            format!(
                "filter @message like \"{}\"",
                needle.replace(['"', '\\'], "")
            )
        };
        let query = format!(
            "fields @logStream | {filter} | stats count(*) as n, min(@timestamp) as first, max(@timestamp) as last by @logStream | sort n desc | limit 50"
        );
        let q = self
            .logs
            .start_query()
            .log_group_names(group)
            .query_string(query)
            .start_time(start)
            .end_time(end)
            .send()
            .await
            .map_err(|e| aws_sdk_cloudwatchlogs::error::DisplayErrorContext(e).to_string())?;
        let id = q.query_id().unwrap_or_default().to_string();
        let started = std::time::Instant::now();
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
            let r = self
                .logs
                .get_query_results()
                .query_id(&id)
                .send()
                .await
                .map_err(|e| aws_sdk_cloudwatchlogs::error::DisplayErrorContext(e).to_string())?;
            match r.status() {
                Some(QueryStatus::Complete) => {
                    return Ok(r
                        .results()
                        .iter()
                        .map(|row| {
                            let get = |name: &str| {
                                row.iter()
                                    .find(|f| f.field() == Some(name))
                                    .and_then(|f| f.value())
                                    .unwrap_or("")
                                    .to_string()
                            };
                            StreamCount {
                                stream: get("@logStream"),
                                count: get("n").parse().unwrap_or(0),
                                first_ms: parse_insights_time(&get("first")).unwrap_or(0),
                                last_ms: parse_insights_time(&get("last")).unwrap_or(0),
                            }
                        })
                        .collect())
                }
                Some(QueryStatus::Failed | QueryStatus::Cancelled | QueryStatus::Timeout) => {
                    return Err("the Logs Insights query didn't finish".into())
                }
                _ if started.elapsed() > std::time::Duration::from_secs(40) => {
                    let _ = self.logs.stop_query().query_id(&id).send().await;
                    return Err("the Logs Insights query took longer than 40 s".into());
                }
                _ => {}
            }
        }
    }

    async fn clusters(&self) -> Result<Vec<String>, String> {
        let out = self.ecs.list_clusters().send().await.map_err(ecs_err)?;
        Ok(out.cluster_arns().to_vec())
    }

    async fn services(&self, cluster: &str) -> Result<Vec<ServiceFact>, String> {
        let arns = self
            .ecs
            .list_services()
            .cluster(cluster)
            .max_results(100)
            .send()
            .await
            .map_err(ecs_err)?
            .service_arns()
            .to_vec();
        let mut out = Vec::new();
        for chunk in arns.chunks(10) {
            let d = self
                .ecs
                .describe_services()
                .cluster(cluster)
                .set_services(Some(chunk.to_vec()))
                .send()
                .await
                .map_err(ecs_err)?;
            for s in d.services() {
                out.push(ServiceFact {
                    cluster: cluster.rsplit('/').next().unwrap_or(cluster).to_string(),
                    name: s.service_name().unwrap_or_default().to_string(),
                    task_definition: s.task_definition().unwrap_or_default().to_string(),
                    launch_type: s
                        .launch_type()
                        .map(|l| l.as_str().to_string())
                        .unwrap_or_default(),
                    running: s.running_count(),
                });
            }
        }
        Ok(out)
    }

    async fn task_definition(&self, arn: &str) -> Result<TaskDefFact, String> {
        let out = self
            .ecs
            .describe_task_definition()
            .task_definition(arn)
            .send()
            .await
            .map_err(ecs_err)?;
        let td = out
            .task_definition()
            .ok_or("no task definition came back")?;
        Ok(TaskDefFact {
            arn: td.task_definition_arn().unwrap_or_default().to_string(),
            family: td.family().unwrap_or_default().to_string(),
            revision: td.revision(),
            cpu: td.cpu().unwrap_or_default().to_string(),
            memory: td.memory().unwrap_or_default().to_string(),
            ephemeral_gib: td.ephemeral_storage().map(|e| e.size_in_gib()),
            requires_fargate: td
                .requires_compatibilities()
                .iter()
                .any(|c| c.as_str() == "FARGATE"),
            containers: td
                .container_definitions()
                .iter()
                .map(|c| {
                    let opts = c.log_configuration().and_then(|l| l.options());
                    let opt = |k: &str| opts.and_then(|o| o.get(k)).cloned().unwrap_or_default();
                    ContainerFact {
                        name: c.name().unwrap_or_default().to_string(),
                        image: c.image().unwrap_or_default().to_string(),
                        command: c.command().to_vec(),
                        entry_point: c.entry_point().to_vec(),
                        working_directory: c.working_directory().unwrap_or_default().to_string(),
                        log_group: opt("awslogs-group"),
                        stream_prefix: opt("awslogs-stream-prefix"),
                    }
                })
                .collect(),
        })
    }

    async fn tasks(&self, cluster: &str, service: &str) -> Result<Vec<TaskFact>, String> {
        let arns = self
            .ecs
            .list_tasks()
            .cluster(cluster)
            .service_name(service)
            .max_results(100)
            .send()
            .await
            .map_err(ecs_err)?
            .task_arns()
            .to_vec();
        let mut out = Vec::new();
        for chunk in arns.chunks(100) {
            let d = self
                .ecs
                .describe_tasks()
                .cluster(cluster)
                .set_tasks(Some(chunk.to_vec()))
                .send()
                .await
                .map_err(ecs_err)?;
            for t in d.tasks() {
                let arn = t.task_arn().unwrap_or_default();
                out.push(TaskFact {
                    id: arn.rsplit('/').next().unwrap_or(arn).to_string(),
                    last_status: t.last_status().unwrap_or_default().to_string(),
                    started_ms: t.started_at().and_then(|d| d.to_millis().ok()),
                    task_definition: t.task_definition_arn().unwrap_or_default().to_string(),
                });
            }
        }
        Ok(out)
    }
}

/// A run's repository files at a ref, through GitHub.
pub struct GithubFiles<'a> {
    pub github: &'a crate::clients::github::GitHubClient,
    pub owner: &'a str,
    pub repo: &'a str,
    pub git_ref: &'a str,
}

#[async_trait::async_trait]
impl RepoFiles for GithubFiles<'_> {
    async fn read(&self, path: &str) -> Option<String> {
        self.github
            .read_file(self.owner, self.repo, path, self.git_ref)
            .await
            .ok()
            .filter(|s| !s.trim().is_empty())
    }
}

/// The evidence step for a run started from a log finding: `None` for any
/// other run. When the finding's AWS account isn't connected, the evidence
/// says so (the source tried and why it couldn't answer).
pub async fn gather_for_run(
    state: &crate::WorkerState,
    msg: &crate::models::TicketMessage,
) -> Option<LogEvidence> {
    use common::log_findings as lf;
    if !matches!(msg.source, crate::models::TicketSource::Alert) {
        return None;
    }
    let ticket = lf::load_ticket(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
        &msg.ticket_id,
    )
    .await?;
    let finding = lf::load_finding(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        &msg.team_id,
        &ticket.rec_id,
    )
    .await
    .ok()
    .flatten();
    let target = Target {
        log_group: finding
            .as_ref()
            .map(|f| f.source_log_group.clone())
            .filter(|g| !g.is_empty())
            .unwrap_or_else(|| ticket.log_group.clone()),
        needle: finding
            .as_ref()
            .and_then(|f| lf::pattern_literal(&f.error_pattern))
            .unwrap_or_default(),
    };
    let account = finding
        .as_ref()
        .map(|f| f.source_account_id.clone())
        .unwrap_or_default();
    let session = lf::session_for(
        &state.dynamo,
        &state.sts,
        &state.config.aws_insights_table_name,
        &msg.team_id,
        &account,
    )
    .await;
    let now = Utc::now();
    match session {
        Ok(Some(s)) => Some(gather(&AwsReader::new(&s), &target, now).await),
        other => {
            let why = match other {
                Err(e) => format!("the log reader role couldn't be taken on: {e}"),
                _ => format!("no AWS connection for account `{account}`"),
            };
            Some(LogEvidence {
                log_group: target.log_group,
                needle: target.needle,
                window_start: ms_text((now.timestamp() - WINDOW_HOURS * 3600) * 1000),
                window_end: ms_text(now.timestamp_millis()),
                tried: vec![SourceTry {
                    source: "sts:AssumeRole".into(),
                    question: "the team's read-only AWS connection".into(),
                    ok: false,
                    outcome: why,
                }],
                ..Default::default()
            })
        }
    }
}

/// Stores the evidence on the run (`log_evidence`, JSON).
pub async fn save(state: &crate::WorkerState, team_id: &str, run_id: &str, ev: &LogEvidence) {
    let json = serde_json::to_string(ev).unwrap_or_default();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(run_id.to_string()),
        )
        .update_expression("SET log_evidence = :e")
        .expression_attribute_values(
            ":e",
            aws_sdk_dynamodb::types::AttributeValue::S(
                common::truncate_str(&json, 60_000).to_string(),
            ),
        )
        .send()
        .await;
}

/// One line for the run's progress notes.
pub fn summary_note(ev: &LogEvidence) -> String {
    let ok = ev.tried.iter().filter(|t| t.ok).count();
    let mut s = format!(
        "Evidence from the logs and AWS: {} lookups answered, {} couldn't",
        ok,
        ev.tried.len() - ok
    );
    if ev.total_errors > 0 {
        s.push_str(&format!(
            "; {} errors from {} tasks",
            ev.total_errors,
            ev.streams.len()
        ));
    }
    if let Some(g) = ev.disk_gib {
        s.push_str(&format!("; disk {g} GiB"));
    }
    if let Some(r) = ev.fill_mb_per_hour {
        s.push_str(&format!("; fills at about {} MB/h", fmt_f(r)));
    }
    let failing = ev.failing_now();
    if !failing.is_empty() {
        s.push_str(&format!(
            "; failing now: {}",
            failing
                .iter()
                .map(|t| t.task_id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    s
}

#[cfg(test)]
pub(crate) mod fake {
    use super::*;

    /// A fake AWS account for tests and the eval.
    #[derive(Default, Clone, Deserialize)]
    pub struct FakeAws {
        #[serde(default)]
        pub streams: Vec<StreamCount>,
        #[serde(default)]
        pub clusters: Vec<String>,
        #[serde(default)]
        pub services: Vec<ServiceFact>,
        #[serde(default)]
        pub task_defs: Vec<TaskDefFact>,
        #[serde(default)]
        pub tasks: Vec<TaskFact>,
        /// Sources that refuse (`ecs:DescribeTasks`), with the error.
        #[serde(default)]
        pub refuse: BTreeMap<String, String>,
    }

    impl FakeAws {
        fn refused(&self, source: &str) -> Result<(), String> {
            match self.refuse.get(source) {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            }
        }
    }

    #[async_trait::async_trait]
    impl Reader for FakeAws {
        async fn error_streams(
            &self,
            _g: &str,
            _n: &str,
            _s: i64,
            _e: i64,
        ) -> Result<Vec<StreamCount>, String> {
            self.refused("logs:StartQuery")?;
            Ok(self.streams.clone())
        }
        async fn clusters(&self) -> Result<Vec<String>, String> {
            self.refused("ecs:ListClusters")?;
            Ok(self.clusters.clone())
        }
        async fn services(&self, cluster: &str) -> Result<Vec<ServiceFact>, String> {
            self.refused("ecs:DescribeServices")?;
            Ok(self
                .services
                .iter()
                .filter(|s| s.cluster == cluster)
                .cloned()
                .collect())
        }
        async fn task_definition(&self, arn: &str) -> Result<TaskDefFact, String> {
            self.refused("ecs:DescribeTaskDefinition")?;
            self.task_defs
                .iter()
                .find(|t| t.arn == arn)
                .cloned()
                .ok_or_else(|| "not found".to_string())
        }
        async fn tasks(&self, _c: &str, _s: &str) -> Result<Vec<TaskFact>, String> {
            self.refused("ecs:DescribeTasks")?;
            Ok(self.tasks.clone())
        }
    }

    /// Repository files for tests.
    pub struct FakeFiles(pub BTreeMap<String, String>);

    #[async_trait::async_trait]
    impl RepoFiles for FakeFiles {
        async fn read(&self, path: &str) -> Option<String> {
            self.0.get(path).cloned()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::*;
    use super::*;

    fn t(s: &str) -> i64 {
        parse_insights_time(s).unwrap()
    }

    fn now() -> DateTime<Utc> {
        Utc.timestamp_millis_opt(t("2026-10-25 12:00:00.000"))
            .unwrap()
    }

    fn account() -> FakeAws {
        FakeAws {
            streams: vec![
                StreamCount {
                    stream: "web/site/aaa111".into(),
                    count: 900,
                    first_ms: t("2026-10-24 23:00:00.000"),
                    last_ms: t("2026-10-25 11:58:00.000"),
                },
                StreamCount {
                    stream: "web/site/bbb222".into(),
                    count: 40,
                    first_ms: t("2026-10-25 09:00:00.000"),
                    last_ms: t("2026-10-25 09:20:00.000"),
                },
            ],
            clusters: vec!["main".into()],
            services: vec![
                ServiceFact {
                    cluster: "main".into(),
                    name: "api".into(),
                    task_definition: "td-api".into(),
                    launch_type: "FARGATE".into(),
                    running: 2,
                },
                ServiceFact {
                    cluster: "main".into(),
                    name: "site".into(),
                    task_definition: "td-site".into(),
                    launch_type: "FARGATE".into(),
                    running: 2,
                },
            ],
            task_defs: vec![
                TaskDefFact {
                    arn: "td-api".into(),
                    family: "api".into(),
                    revision: 4,
                    containers: vec![ContainerFact {
                        name: "api".into(),
                        log_group: "/ecs/api".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                TaskDefFact {
                    arn: "td-site".into(),
                    family: "site".into(),
                    revision: 9,
                    cpu: "1024".into(),
                    memory: "2048".into(),
                    ephemeral_gib: None,
                    requires_fargate: true,
                    containers: vec![ContainerFact {
                        name: "site".into(),
                        command: vec!["npm".into(), "run".into(), "serve".into()],
                        log_group: "/ecs/site".into(),
                        ..Default::default()
                    }],
                },
            ],
            tasks: vec![
                TaskFact {
                    id: "aaa111".into(),
                    last_status: "RUNNING".into(),
                    started_ms: Some(t("2026-10-22 12:00:00.000")),
                    task_definition: "td-site".into(),
                },
                TaskFact {
                    id: "ccc333".into(),
                    last_status: "RUNNING".into(),
                    started_ms: Some(t("2026-10-25 06:00:00.000")),
                    task_definition: "td-site".into(),
                },
            ],
            refuse: BTreeMap::new(),
        }
    }

    fn target() -> Target {
        Target {
            log_group: "/ecs/site".into(),
            needle: "no space left on device".into(),
        }
    }

    #[tokio::test]
    async fn gathers_the_service_disk_tasks_and_fill_rate() {
        let ev = gather(&account(), &target(), now()).await;
        assert_eq!(ev.total_errors, 940);
        assert_eq!(ev.service.as_ref().unwrap().name, "site");
        assert_eq!(ev.disk_gib, Some(20));
        assert!(ev.disk_basis.contains("default"));
        let a = ev.tasks.iter().find(|t| t.task_id == "aaa111").unwrap();
        // Started 10-22 12:00, first error 10-24 23:00: 59 h, 20480 MB / 59 h.
        assert_eq!(a.hours_to_first_error, Some(59.0));
        assert_eq!(a.fill_mb_per_hour, Some(347.0));
        assert!(a.failing_now);
        // A task that stopped erroring two and a half hours ago isn't failing now.
        let b = ev.tasks.iter().find(|t| t.task_id == "bbb222").unwrap();
        assert!(!b.failing_now && !b.running);
        let c = ev.tasks.iter().find(|t| t.task_id == "ccc333").unwrap();
        assert_eq!(c.errors, 0);
        assert!(ev.failed_sources().is_empty());
        let md = facts_markdown(&ev);
        assert!(md.contains("FAILING NOW"));
        assert!(md.contains("`npm run serve` (overrides the image's CMD)"));
        assert!(md.contains("20 GiB"));
    }

    #[tokio::test]
    async fn a_refused_read_is_recorded_with_its_source() {
        let mut a = account();
        a.refuse
            .insert("ecs:DescribeTasks".into(), "AccessDeniedException".into());
        let ev = gather(&a, &target(), now()).await;
        let failed = ev.failed_sources();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].source, "ecs:DescribeTasks");
        // The streams still name the tasks that failed.
        assert!(ev.tasks.iter().any(|t| t.task_id == "aaa111"));
        assert!(facts_markdown(&ev).contains("FAILED: AccessDeniedException"));
    }

    #[tokio::test]
    async fn no_service_logging_to_the_group_is_said_out_loud() {
        let ev = gather(
            &account(),
            &Target {
                log_group: "/ecs/other".into(),
                needle: "x".into(),
            },
            now(),
        )
        .await;
        assert!(ev.service.is_none());
        assert!(ev.failed_sources().iter().any(|t| t
            .outcome
            .contains("no service's task definition logs to /ecs/other")));
    }

    #[test]
    fn script_names_and_dockerfiles() {
        assert_eq!(script_name("npm run serve").as_deref(), Some("serve"));
        assert_eq!(script_name("npm start").as_deref(), Some("start"));
        assert_eq!(script_name("yarn start").as_deref(), Some("start"));
        assert_eq!(script_name("node server.js"), None);
        let (cmd, wd) = dockerfile_start(
            "FROM node:20\nWORKDIR /app\nENTRYPOINT [\"docker-entrypoint.sh\"]\nCMD [\"npm\", \"start\"]\n",
        );
        assert_eq!(cmd, "docker-entrypoint.sh npm start");
        assert_eq!(wd, "/app");
    }

    #[tokio::test]
    async fn the_task_definitions_command_wins_over_the_dockerfile() {
        let ev = gather(&account(), &target(), now()).await;
        let files = FakeFiles(
            [
                (
                    "Dockerfile".to_string(),
                    "FROM node\nWORKDIR /app\nCMD [\"npm\",\"start\"]".to_string(),
                ),
                (
                    "package.json".to_string(),
                    r#"{"scripts":{"start":"next start","serve":"node server.js"}}"#.to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let sp = resolve_start(&files, &ev).await;
        assert!(sp.verified, "{sp:?}");
        assert_eq!(sp.command, "npm run serve");
        assert_eq!(sp.script, "serve: node server.js");
        assert_eq!(sp.working_directory, "/app");
        assert!(sp.from.contains("overrides the image's CMD"));
    }

    #[tokio::test]
    async fn a_missing_script_is_not_verified() {
        let ev = gather(&account(), &target(), now()).await;
        let files = FakeFiles(
            [(
                "package.json".to_string(),
                r#"{"scripts":{"start":"next start"}}"#.to_string(),
            )]
            .into_iter()
            .collect(),
        );
        let sp = resolve_start(&files, &ev).await;
        assert!(!sp.verified);
        assert!(sp.missing.contains("`serve`"));
    }

    #[test]
    fn the_block_replaces_an_earlier_one() {
        let ev = LogEvidence {
            log_group: "/ecs/site".into(),
            ..Default::default()
        };
        let once = with_instructions("Team rules.", &ev);
        let twice = with_instructions(&once, &ev);
        assert_eq!(once, twice);
        assert!(once.starts_with("Team rules."));
        assert!(once.contains("Rules for a fix that starts from a log finding"));
    }
}
