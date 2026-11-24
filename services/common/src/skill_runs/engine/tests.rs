//! A whole skill run against fakes: a synthetic skill (testdata/visit-count:
//! a script, a baseline file and a `snapshots/` memory folder) whose script
//! really runs, in a local stand-in for the sandbox.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::{json, Value};

use super::*;
use crate::plugins::kv::{Expect, Memory};
use crate::skill_runs::{self as sr, Destinations, ToolGrant};

const TEAM: &str = "TEAM#t1";
const SKILL_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/skill_runs/testdata/visit-count"
);

pub fn skill_files() -> Vec<(String, Vec<u8>)> {
    ["SKILL.md", "count_visits.py", "baseline.json"]
        .iter()
        .map(|f| {
            (
                f.to_string(),
                std::fs::read(Path::new(SKILL_DIR).join(f)).unwrap(),
            )
        })
        .collect()
}

pub struct Source;

#[async_trait::async_trait]
impl SkillSource for Source {
    async fn files(&self, _: &str, _: &str, _: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
        Ok(skill_files())
    }
}

/// The sandbox, here: the skill's files in a fresh folder, the output folder
/// at `.coderhelm-out` inside it, `bash -c` from the skill's folder.
pub struct LocalSandbox;

fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(root, &p, out);
        } else if let Ok(t) = std::fs::read_to_string(&p) {
            out.insert(
                p.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/"),
                t,
            );
        }
    }
}

#[async_trait::async_trait]
impl Sandbox for LocalSandbox {
    async fn run(
        &self,
        key: &str,
        skill: &[(String, Vec<u8>)],
        out: &BTreeMap<String, String>,
        cmd: &str,
        _: Instant,
    ) -> Result<ScriptResult, String> {
        let base: PathBuf =
            std::env::temp_dir().join(format!("skillrun-{key}-{}", ulid::Ulid::new()));
        let dir = base.join("skill");
        let o = dir.join(".coderhelm-out");
        for (p, b) in skill {
            let f = dir.join(p);
            std::fs::create_dir_all(f.parent().unwrap()).unwrap();
            std::fs::write(f, b).unwrap();
        }
        for (p, b) in out {
            let f = o.join(p);
            std::fs::create_dir_all(f.parent().unwrap()).unwrap();
            std::fs::write(f, b).unwrap();
        }
        std::fs::create_dir_all(&o).unwrap();
        let r = std::process::Command::new("bash")
            .arg("-c")
            .arg(cmd)
            .current_dir(&dir)
            .env("CODERHELM_OUT", &o)
            .env_remove("CLAUDE_SKILL_DIR")
            .output()
            .map_err(|e| e.to_string())?;
        let mut files = BTreeMap::new();
        walk(&o, &o, &mut files);
        let _ = std::fs::remove_dir_all(&base);
        Ok(ScriptResult {
            ran: true,
            exit_code: r.status.code(),
            output: format!(
                "{}{}",
                String::from_utf8_lossy(&r.stdout),
                String::from_utf8_lossy(&r.stderr)
            ),
            files,
            complete: true,
        })
    }
}

/// The team's connections: `acme-traffic` offers `traffic_counts` (and a
/// tool that writes); anything else isn't connected.
#[derive(Default)]
pub struct Conns {
    pub week: Mutex<u32>,
    pub deny: Mutex<bool>,
    pub calls: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl Connections for Conns {
    async fn tools(&self, _: &str, server: &str) -> Result<Vec<ToolDef>, String> {
        if server != "acme-traffic" {
            return Err(format!("{server} isn't connected on the Connections page."));
        }
        Ok(vec![
            ToolDef {
                name: "traffic_counts".into(),
                description: "Visits per page".into(),
                input_schema: json!({"type":"object","properties":{"days":{"type":"integer"}}}),
            },
            ToolDef {
                name: "set_banner".into(),
                description: "Changes the site's banner".into(),
                input_schema: json!({"type":"object"}),
            },
        ])
    }
    async fn call(&self, _: &str, _: &str, tool: &str, args: &Value) -> Result<String, String> {
        self.calls.lock().unwrap().push(format!("{tool} {args}"));
        if *self.deny.lock().unwrap() {
            return Err("AccessDenied: the key was revoked".into());
        }
        let w = *self.week.lock().unwrap();
        Ok(if w == 0 {
            "/home 900\n/classes 120\n/pricing 300\n".into()
        } else {
            "/home 950\n/classes 400\n/pricing 290\n/events 150\n".into()
        })
    }
}

pub struct NoAthena;

#[async_trait::async_trait]
impl Athena for NoAthena {
    async fn check(&self, _: &str, _: &AwsGrant) -> Result<(), AwsError> {
        Err(AwsError::Access(
            "AccessDenied: not authorized to perform sts:AssumeRole".into(),
        ))
    }
    async fn query(
        &self,
        _: &str,
        _: &AwsGrant,
        _: &str,
        _: &str,
    ) -> Result<AthenaResult, AwsError> {
        Err(AwsError::Other("no".into()))
    }
}

#[derive(Default)]
pub struct MemFiles(pub Mutex<BTreeMap<String, String>>);

#[async_trait::async_trait]
impl Files for MemFiles {
    async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
    async fn get(&self, key: &str) -> Result<Option<String>, String> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }
    async fn put(&self, key: &str, body: &str) -> Result<(), String> {
        self.0.lock().unwrap().insert(key.into(), body.into());
        Ok(())
    }
    async fn delete(&self, key: &str) -> Result<(), String> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

#[derive(Default)]
pub struct Runs {
    pub rows: Mutex<Vec<RunRow>>,
    pub notes: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl RunLog for Runs {
    async fn write(&self, _: &str, row: &RunRow) {
        self.rows.lock().unwrap().push(row.clone());
    }
    async fn note(&self, _: &str, _: &str, text: &str) {
        self.notes.lock().unwrap().push(text.into());
    }
}

#[derive(Default)]
pub struct Told {
    pub chat: Mutex<Vec<String>>,
    pub issues: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl Notify for Told {
    async fn chat(&self, _: &str, title: &str, lines: &[String], url: &str) -> bool {
        self.chat
            .lock()
            .unwrap()
            .push(format!("{title} | {} | {url}", lines.join(" | ")));
        true
    }
    async fn issue(&self, _: &str, repo: &str, title: &str, _: &str) -> Result<String, String> {
        self.issues.lock().unwrap().push(format!("{repo}: {title}"));
        Ok("https://github.com/acme/skills/issues/1".into())
    }
}

/// An agent that does what the synthetic skill says: plan, call each tool
/// the plan lists into the work folder, build. It reads the plan's JSON out
/// of what the script printed, as a model would.
pub struct SkillFollower {
    pub date: String,
    /// Also tries the tool that writes, to see the gate refuse it.
    pub try_write: bool,
    pub refusals: Mutex<Vec<String>>,
    pub asked: Mutex<Option<(String, Vec<String>)>>,
}

impl SkillFollower {
    pub fn new(date: &str) -> Self {
        Self {
            date: date.into(),
            try_write: false,
            refusals: Mutex::new(Vec::new()),
            asked: Mutex::new(None),
        }
    }
}

fn printed(result: &str) -> &str {
    result
        .split("What it printed:\n")
        .nth(1)
        .unwrap_or("")
        .trim()
}

#[async_trait::async_trait]
impl Agent for SkillFollower {
    async fn run(
        &self,
        _: &str,
        system: &str,
        _: &str,
        tools: &[ToolDef],
        exec: &dyn ToolExec,
        _: &Budget,
    ) -> Result<(String, u64, f64), String> {
        *self.asked.lock().unwrap() = Some((
            system.to_string(),
            tools.iter().map(|t| t.name.clone()).collect(),
        ));
        let plan = exec
            .call(
                RUN_SCRIPT,
                &json!({"script": "${CLAUDE_SKILL_DIR}/count_visits.py", "args": ["plan", "--out", "{out}"]}),
            )
            .await;
        let plan: Value =
            serde_json::from_str(printed(&plan)).map_err(|e| format!("{e}: {plan}"))?;
        for c in plan["calls"].as_array().unwrap() {
            let r = exec
                .call(
                    CALL_TO_FILE,
                    &json!({"tool": c["tool"], "args": c["args"], "file": format!("work/{}", c["file"].as_str().unwrap())}),
                )
                .await;
            if exec.stop() {
                exec.call(
                    NEEDS_ACCESS,
                    &json!({"missing": [format!("traffic_counts: {r}")]}),
                )
                .await;
                return Ok(("Stopped: needs access.".into(), 1_000, 0.01));
            }
        }
        if self.try_write {
            let r = exec.call("set_banner", &json!({"text": "hi"})).await;
            self.refusals.lock().unwrap().push(r);
        }
        let built = exec
            .call(
                RUN_SCRIPT,
                &json!({"script": "count_visits.py", "args": ["build", "--raw", "{work}", "--out", "{out}", "--date", self.date]}),
            )
            .await;
        let r: Value =
            serde_json::from_str(printed(&built)).map_err(|e| format!("{e}: {built}"))?;
        Ok((
            format!(
                "Report: {}. Findings: {}",
                r["report"].as_str().unwrap_or(""),
                r["findings"]
            ),
            12_000,
            0.12,
        ))
    }
}

pub struct Rig {
    pub kv: Memory,
    pub conns: Conns,
    pub files: MemFiles,
    pub runs: Runs,
    pub told: Told,
}

impl Rig {
    pub fn new() -> Self {
        Self {
            kv: Memory::default(),
            conns: Conns::default(),
            files: MemFiles::default(),
            runs: Runs::default(),
            told: Told::default(),
        }
    }

    pub async fn schedule(&self, change: impl FnOnce(&mut Schedule)) -> Schedule {
        let mut s = Schedule {
            id: "01SCHED".into(),
            skill_repo: "acme/skills".into(),
            skill_path: "skills/visit-count/SKILL.md".into(),
            skill_name: "visit-count".into(),
            cron: "23 7 * * MON".into(),
            timezone: "America/New_York".into(),
            tools: vec![
                ToolGrant {
                    name: "traffic_counts".into(),
                    server: "acme-traffic".into(),
                    reads_only: true,
                    ..Default::default()
                },
                ToolGrant {
                    name: "set_banner".into(),
                    server: "acme-traffic".into(),
                    ..Default::default()
                },
            ],
            state_dirs: vec!["snapshots".into()],
            notify: Destinations {
                chat: "always".into(),
                issue_on_failure: true,
            },
            ..Default::default()
        };
        change(&mut s);
        sr::validate(&mut s).unwrap();
        sr::put(&self.kv, TEAM, &s, Expect::Any).await.unwrap();
        s
    }

    pub async fn run(
        &self,
        agent: &dyn Agent,
        athena: &dyn Athena,
        trigger: &str,
    ) -> Option<RunEntry> {
        let deps = Deps {
            kv: &self.kv,
            source: &Source,
            conns: &self.conns,
            athena,
            sandbox: &LocalSandbox,
            files: &self.files,
            runs: &self.runs,
            notify: &self.told,
            agent,
            app_url: "https://app.example.com",
        };
        execute(
            &deps,
            &SkillRunMessage {
                team_id: TEAM.into(),
                schedule_id: "01SCHED".into(),
                run_id: String::new(),
                trigger: trigger.into(),
                requested_by: "admin@example.com".into(),
            },
        )
        .await
    }

    pub fn file(&self, key_end: &str) -> Option<String> {
        self.files
            .0
            .lock()
            .unwrap()
            .iter()
            .find(|(k, _)| k.ends_with(key_end))
            .map(|(_, v)| v.clone())
    }
}

#[tokio::test]
async fn a_scheduled_skill_runs_end_to_end_and_remembers_last_week() {
    let rig = Rig::new();
    rig.schedule(|_| {}).await;

    // Week one: no memory yet, so the plan asks for 30 days too.
    let a = SkillFollower::new("2026-11-23");
    let e = rig.run(&a, &NoAthena, "manual").await.unwrap();
    assert_eq!(e.status, "completed", "{e:?}");
    assert_eq!(
        e.outputs,
        ["reports/2026-11-23.html", "snapshots/2026-11-23.json"]
    );
    assert_eq!(e.report, "reports/2026-11-23.html");
    let calls = rig.conns.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        [
            r#"traffic_counts {"days":7}"#,
            r#"traffic_counts {"days":30}"#
        ]
    );
    // The outputs are the run's; the snapshot is also kept as memory, and
    // the work folder is gone.
    let first = rig
        .file(&format!("outputs/{}/reports/2026-11-23.html", e.run_id))
        .unwrap();
    assert!(first.contains("first run: no earlier week"), "{first}");
    assert!(rig
        .file("skill-state/01SCHED/snapshots/2026-11-23.json")
        .is_some());
    assert!(!rig
        .files
        .0
        .lock()
        .unwrap()
        .keys()
        .any(|k| k.contains("/work/")));

    // Week two: the memory comes back, and the report compares with it.
    *rig.conns.week.lock().unwrap() = 1;
    let b = SkillFollower::new("2026-11-30");
    let e2 = rig.run(&b, &NoAthena, "schedule").await.unwrap();
    assert_eq!(e2.status, "completed", "{e2:?}");
    // Only what this run made: not last week's restored snapshot.
    assert_eq!(
        e2.outputs,
        ["reports/2026-11-30.html", "snapshots/2026-11-30.json"]
    );
    assert_eq!(
        rig.conns.calls.lock().unwrap().len(),
        3,
        "a weekly run asks for 7 days only"
    );
    let report = rig
        .file(&format!("outputs/{}/reports/2026-11-30.html", e2.run_id))
        .unwrap();
    assert!(report.contains("compared with 2026-11-23"), "{report}");
    assert!(
        report.contains("<b>/classes</b> grew from 120 to 400"),
        "{report}"
    );
    assert!(report.contains("<b>/events</b> new"), "{report}");
    assert!(e2.summary.contains("grew"), "{}", e2.summary);
    assert_eq!(e2.tokens, 12_000);
    if let Ok(p) = std::env::var("SKILL_RUN_REPORT_OUT") {
        std::fs::write(p, &report).unwrap();
    }

    // The run list, the history and the schedule say how it went.
    let rows = rig.runs.rows.lock().unwrap().clone();
    assert!(rows
        .iter()
        .any(|r| r.run_id == e2.run_id && r.status == "completed"));
    let hist = sr::runs(&rig.kv, TEAM, "01SCHED").await.unwrap();
    assert_eq!(hist.len(), 2);
    assert_eq!(hist[0].run_id, e2.run_id, "newest first");
    let (s, _) = sr::get(&rig.kv, TEAM, "01SCHED").await.unwrap().unwrap();
    assert!(s.running.is_none());
    assert_eq!(s.last_run.unwrap().run_id, e2.run_id);
    let notes = rig.runs.notes.lock().unwrap().join("\n");
    assert!(notes.contains("Restored 1 memory files"), "{notes}");
    assert!(
        notes.contains("Ran count_visits.py build: exit 0"),
        "{notes}"
    );
    assert_eq!(rig.told.chat.lock().unwrap().len(), 2);
    assert!(rig.told.issues.lock().unwrap().is_empty());

    // The agent was told how the skill's laptop words map, and given only
    // this run's tools.
    let (system, tools) = b.asked.lock().unwrap().clone().unwrap();
    assert!(system.contains("`snapshots/`"), "{system}");
    assert!(system.contains("name: visit-count"));
    assert!(system.contains("refused: this run is read-only"));
    assert!(
        system.contains(crate::host_steps::PURPOSE),
        "laptop steps are read by their purpose"
    );
    assert!(tools.contains(&"traffic_counts".to_string()));
    assert!(!tools.contains(&ATHENA_TOOL.to_string()));
}

#[tokio::test]
async fn a_tool_that_writes_is_refused_unless_the_schedule_allows_writes() {
    let rig = Rig::new();
    rig.schedule(|_| {}).await;
    let mut a = SkillFollower::new("2026-11-23");
    a.try_write = true;
    let e = rig.run(&a, &NoAthena, "manual").await.unwrap();
    assert_eq!(e.status, "completed");
    let refused = a.refusals.lock().unwrap()[0].clone();
    assert!(refused.contains("read-only"), "{refused}");
    assert!(!rig
        .conns
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|c| c.starts_with("set_banner")));

    let rig = Rig::new();
    rig.schedule(|s| s.allow_writes = true).await;
    let mut a = SkillFollower::new("2026-11-23");
    a.try_write = true;
    rig.run(&a, &NoAthena, "manual").await.unwrap();
    assert!(rig
        .conns
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|c| c.starts_with("set_banner")));
}

#[tokio::test]
async fn missing_access_stops_before_the_agent_with_what_it_needs() {
    let rig = Rig::new();
    rig.schedule(|s| {
        s.tools.push(ToolGrant {
            name: "error_rates".into(),
            server: "observability".into(),
            reads_only: true,
            ..Default::default()
        });
        s.aws = Some(AwsGrant {
            account: "111122223333".into(),
            region: "us-east-1".into(),
            workgroup: "logs".into(),
            max_gb: 10.0,
        });
    })
    .await;
    let a = SkillFollower::new("2026-11-23");
    let e = rig.run(&a, &NoAthena, "schedule").await.unwrap();
    assert_eq!(e.status, "needs_access");
    assert_eq!(e.needs_access.len(), 2, "{:?}", e.needs_access);
    assert!(e.needs_access[0].starts_with("error_rates: the MCP server 'observability'"));
    assert!(e.needs_access[1].starts_with("aws_athena_query: the AWS reader role in 111122223333"));
    assert!(a.asked.lock().unwrap().is_none(), "the agent never started");
    assert_eq!(e.outputs, ["needs-access.md"]);
    assert!(rig.conns.calls.lock().unwrap().is_empty());
    assert!(!rig
        .files
        .0
        .lock()
        .unwrap()
        .keys()
        .any(|k| k.contains("skill-state")));
    // A failed scheduled run tells the channel and opens the issue.
    assert!(rig.told.chat.lock().unwrap()[0].contains("Needs access: error_rates"));
    assert_eq!(rig.told.issues.lock().unwrap().len(), 1);
    assert_eq!(
        rig.runs.rows.lock().unwrap().last().unwrap().status,
        "failed"
    );
}

#[tokio::test]
async fn a_tool_refused_for_access_mid_run_keeps_no_partial_report_or_memory() {
    let rig = Rig::new();
    rig.schedule(|_| {}).await;
    *rig.conns.deny.lock().unwrap() = true;
    let a = SkillFollower::new("2026-11-23");
    let e = rig.run(&a, &NoAthena, "manual").await.unwrap();
    assert_eq!(e.status, "needs_access", "{e:?}");
    assert!(
        e.needs_access[0].contains("AccessDenied"),
        "{:?}",
        e.needs_access
    );
    assert_eq!(e.outputs, ["needs-access.md"]);
    let keys: Vec<String> = rig.files.0.lock().unwrap().keys().cloned().collect();
    assert!(
        !keys
            .iter()
            .any(|k| k.contains(".html") || k.contains("skill-state")),
        "{keys:?}"
    );
    // A manual run (a person testing) opens no issue.
    assert!(rig.told.issues.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_paused_schedule_doesnt_fire_and_two_runs_never_overlap() {
    let rig = Rig::new();
    rig.schedule(|s| s.paused = true).await;
    let a = SkillFollower::new("2026-11-23");
    assert!(rig.run(&a, &NoAthena, "schedule").await.is_none());
    // Run now still runs a paused schedule: it is how people test it.
    assert_eq!(
        rig.run(&a, &NoAthena, "manual").await.unwrap().status,
        "completed"
    );

    let now = chrono::Utc::now();
    sr::claim(&rig.kv, TEAM, "01SCHED", "other-run", now)
        .await
        .unwrap();
    let e = rig.run(&a, &NoAthena, "manual").await.unwrap();
    assert_eq!(e.status, "failed");
    assert!(e.summary.contains("one at a time"), "{}", e.summary);
    assert!(rig.run(&a, &NoAthena, "schedule").await.is_none());
}

#[test]
fn scripts_get_their_folders_and_quoted_arguments() {
    let c = script_command(
        "scan.py",
        &[
            "build".into(),
            "--raw".into(),
            "{work}".into(),
            "--out={out}".into(),
            "it's".into(),
        ],
    )
    .unwrap();
    assert_eq!(
        c,
        "export CLAUDE_SKILL_DIR=\"$PWD\"; mkdir -p \"$CODERHELM_OUT/work\"; python3 'scan.py' \
         'build' '--raw' \"$CODERHELM_OUT/work\" '--out='\"$CODERHELM_OUT\" 'it'\\''s'"
    );
    assert!(script_command("x.exe", &[]).is_err());
    assert!(script_command("a.sh", &["$(rm -rf /)".into()])
        .unwrap()
        .ends_with("'$(rm -rf /)'"));
}

#[test]
fn only_reading_sql_reaches_athena() {
    assert!(read_only_sql("SELECT count(*) FROM waf_logs.cloudfront"));
    assert!(read_only_sql("  with a as (select 1) select * from a;"));
    for bad in [
        "DROP TABLE x",
        "INSERT INTO x VALUES (1)",
        "select 1; drop table x",
        "CREATE TABLE x AS SELECT 1",
        "",
    ] {
        assert!(!read_only_sql(bad), "{bad}");
    }
}

#[test]
fn access_errors_are_told_from_passing_failures() {
    assert!(is_access_error(
        "An error occurred (AccessDeniedException) when calling"
    ));
    assert!(is_access_error("User: arn:aws:sts::1:assumed-role/R/s is not authorized to perform: athena:StartQueryExecution"));
    assert!(is_access_error("HTTP status 401: invalid API key"));
    assert!(!is_access_error(
        "HIVE_S3_THROTTLING: Please reduce your request rate"
    ));
    assert!(!is_access_error("timed out"));
}

#[test]
fn memory_keeps_the_newest_snapshots_of_each_folder() {
    let paths: Vec<String> = (0..70)
        .map(|i| format!("snapshots/2026-{:02}-{:02}.json", 1 + i / 28, 1 + i % 28))
        .chain(["other/x.json".to_string()])
        .collect();
    let keep = keep_newest(&paths, &["snapshots".into()]);
    assert_eq!(keep.len(), MAX_STATE_FILES);
    assert!(keep.contains(&"snapshots/2026-03-14.json".to_string()));
    assert!(!keep.contains(&"snapshots/2026-01-01.json".to_string()));
    assert!(!keep.iter().any(|k| k.starts_with("other/")));
    assert_eq!(
        pick_report(&[
            "snapshots/a.json".into(),
            "x.html".into(),
            "reports/b.html".into()
        ]),
        "reports/b.html"
    );
}
