//! A whole skill run against fakes: a synthetic skill (testdata/visit-count:
//! a script, a baseline file and a `snapshots/` memory folder) whose script
//! really runs, and whose tools come from a stdio MCP server
//! (testdata/traffic-mcp) the run finds in the team's repositories and
//! starts behind the real relay (`mcp_relay.py`), in a local stand-in for
//! the sandbox.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::{json, Value};

use super::*;
use crate::plugins::kv::{Expect, Memory};
use crate::skill_runs::stdio::{self, StdioSource};
use crate::skill_runs::{self as sr, Destinations};

const TEAM: &str = "TEAM#t1";
const DATA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/skill_runs/testdata");
const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

fn data(p: &str) -> PathBuf {
    Path::new(DATA).join(p)
}

pub fn skill_files() -> Vec<(String, Vec<u8>)> {
    ["SKILL.md", "count_visits.py", "baseline.json"]
        .iter()
        .map(|f| {
            (
                f.to_string(),
                std::fs::read(data("visit-count").join(f)).unwrap(),
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

fn scratch(what: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("skillrun-{what}-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&d).unwrap();
    d
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
        let base = scratch(key);
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

/// The sandbox's stdio sessions, here: the server's files and the relay in a
/// fresh folder, request slots as files, the relay as a local process.
#[derive(Default)]
pub struct LocalStdio {
    pub procs: Mutex<BTreeMap<String, std::process::Child>>,
    pub started: Mutex<Vec<(String, bool)>>,
    pub stopped: Mutex<usize>,
}

#[async_trait::async_trait]
impl stdio::StdioHost for LocalStdio {
    async fn start(
        &self,
        _: &str,
        key: &str,
        s: &stdio::StdioServer,
        aws: Option<&BTreeMap<String, String>>,
        _: Instant,
    ) -> Result<stdio::Session, String> {
        let StdioSource::Git { repo, .. } = &s.source else {
            return Err("only repositories here".into());
        };
        let dir = scratch(key);
        let server = dir.join("server");
        std::fs::create_dir_all(&server).unwrap();
        let name = repo.rsplit('/').next().unwrap();
        for e in std::fs::read_dir(data(name))
            .map_err(|e| e.to_string())?
            .flatten()
        {
            std::fs::copy(e.path(), server.join(e.file_name())).unwrap();
        }
        std::fs::write(dir.join("relay.py"), stdio::RELAY).unwrap();
        let slots: Vec<(String, String)> = (0..stdio::SLOTS)
            .map(|i| {
                (
                    dir.join(format!("req-{i}.json")).display().to_string(),
                    dir.join(format!("resp-{i}.json")).display().to_string(),
                )
            })
            .collect();
        let secret = aws.map(|k| {
            let p = dir.join("secret.json");
            std::fs::write(&p, serde_json::to_string(k).unwrap()).unwrap();
            p.display().to_string()
        });
        let m = stdio::manifest(s, &s.install(&[]), &slots, secret.as_deref());
        std::fs::write(dir.join("manifest.json"), m.to_string()).unwrap();
        let child = std::process::Command::new("python3")
            .arg(dir.join("relay.py"))
            .arg(dir.join("manifest.json"))
            .current_dir(&dir)
            .stdout(std::process::Stdio::null())
            .spawn()
            .map_err(|e| e.to_string())?;
        self.procs
            .lock()
            .unwrap()
            .insert(dir.display().to_string(), child);
        self.started
            .lock()
            .unwrap()
            .push((s.id.clone(), aws.is_some()));
        Ok(stdio::Session {
            key: dir.display().to_string(),
            build: "local".into(),
            next: 0,
        })
    }

    async fn request(
        &self,
        session: &mut stdio::Session,
        req: &Value,
        _: Instant,
    ) -> Result<Value, String> {
        let i = session.next;
        session.next += 1;
        let dir = Path::new(&session.key);
        std::fs::write(dir.join(format!("req-{i}.json")), req.to_string()).unwrap();
        let resp = dir.join(format!("resp-{i}.json"));
        for _ in 0..600 {
            if let Ok(t) = std::fs::read_to_string(&resp) {
                let v: Value = serde_json::from_str(&t).map_err(|e| e.to_string())?;
                return if v["ok"] == json!(true) {
                    Ok(v["result"].clone())
                } else {
                    Err(v["error"].as_str().unwrap_or("error").to_string())
                };
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Err("no answer".into())
    }

    async fn stop(&self, session: &stdio::Session) {
        let mut s = session.clone();
        let _ = self
            .request(&mut s, &json!({"op": "stop"}), Instant::now())
            .await;
        if let Some(mut c) = self.procs.lock().unwrap().remove(&session.key) {
            let _ = c.wait();
        }
        *self.stopped.lock().unwrap() += 1;
    }
}

/// The team's read-only role: keys for its one connected account.
pub struct Keys {
    pub ok: bool,
}

#[async_trait::async_trait]
impl stdio::RoleKeys for Keys {
    async fn keys(&self, _: &str, account: &str) -> Result<BTreeMap<String, String>, String> {
        if !self.ok {
            return Err(format!(
                "AccessDenied: not authorized to perform sts:AssumeRole in {account}"
            ));
        }
        Ok(BTreeMap::from([
            ("AWS_ACCESS_KEY_ID".into(), "ASIAEXAMPLEKEYID".into()),
            (
                "AWS_SECRET_ACCESS_KEY".into(),
                "example-secret-key-material".into(),
            ),
            (
                "AWS_SESSION_TOKEN".into(),
                "example-session-token-material".into(),
            ),
            ("AWS_REGION".into(), "us-east-1".into()),
        ]))
    }
}

/// The team: its repositories declare the traffic server; one connected
/// AWS account; no connected MCP server.
pub struct Team {
    pub accounts: Vec<String>,
    pub week_file: String,
    pub deny_file: String,
}

#[async_trait::async_trait]
impl Servers for Team {
    async fn connected(&self, _: &str) -> Vec<(String, String)> {
        Vec::new()
    }
    async fn aws_accounts(&self, _: &str) -> Vec<String> {
        self.accounts.clone()
    }
    async fn find(
        &self,
        _: &str,
        families: &[String],
    ) -> (Vec<(stdio::StdioServer, bool)>, Vec<String>) {
        if !families.iter().any(|f| f == "traffic") {
            return (Vec::new(), Vec::new());
        }
        let mut s = stdio::StdioServer {
            id: "traffic-mcp".into(),
            source: StdioSource::Git {
                repo: "acme/traffic-mcp".into(),
                commit: SHA.into(),
            },
            command: vec!["python3".into(), "server.py".into()],
            aws_account: String::new(),
            env: BTreeMap::new(),
        };
        s.env
            .insert("FAKE_WEEK_FILE".into(), self.week_file.clone());
        s.env
            .insert("FAKE_DENY_FILE".into(), self.deny_file.clone());
        (vec![(s, true)], Vec::new())
    }
}

/// A model that reads `traffic_counts` as a read.
#[derive(Default)]
pub struct Judge {
    pub asked: Mutex<Vec<(String, Vec<String>)>>,
}

#[async_trait::async_trait]
impl risk::RiskJudge for Judge {
    async fn judge(&self, _: &str, version: &str, tools: &[ToolDef]) -> BTreeMap<String, Verdict> {
        self.asked.lock().unwrap().push((
            version.into(),
            tools.iter().map(|t| t.name.clone()).collect(),
        ));
        tools
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    Verdict {
                        risk: if t.name.contains("count") {
                            Risk::Read
                        } else {
                            Risk::Write
                        },
                        why: "looks up visit counts".into(),
                    },
                )
            })
            .collect()
    }
}

pub struct NoBuiltins;

#[async_trait::async_trait]
impl Builtins for NoBuiltins {
    async fn offered(&self, _: &str) -> Vec<Offered> {
        Vec::new()
    }
    async fn call(&self, _: &str, _: &str, _: &str, _: &Value) -> Result<String, String> {
        Err("none".into())
    }
}

#[derive(Default)]
pub struct Gaps(pub Mutex<Vec<(String, Vec<String>)>>);

#[async_trait::async_trait]
impl AccessGaps for Gaps {
    async fn record(&self, _: &str, account: &str, actions: &[String], _: &str) {
        self.0
            .lock()
            .unwrap()
            .push((account.into(), actions.to_vec()));
    }
}

/// No MCP servers are connected here.
pub struct NoConns;

#[async_trait::async_trait]
impl Connections for NoConns {
    async fn tools(&self, _: &str, server: &str) -> Result<Vec<ToolDef>, String> {
        Err(format!("{server} isn't connected on the Connections page."))
    }
    async fn call(&self, _: &str, _: &str, _: &str, _: &Value) -> Result<String, String> {
        Err("not connected".into())
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
    pub queued: Mutex<Vec<SkillRunMessage>>,
}

#[async_trait::async_trait]
impl RunLog for Runs {
    async fn write(&self, _: &str, row: &RunRow) {
        self.rows.lock().unwrap().push(row.clone());
    }
    async fn note(&self, _: &str, _: &str, text: &str) {
        self.notes.lock().unwrap().push(text.into());
    }
    async fn continue_later(&self, msg: &SkillRunMessage) -> Result<(), String> {
        self.queued.lock().unwrap().push(msg.clone());
        Ok(())
    }
}

#[derive(Default)]
pub struct Told {
    pub chat: Mutex<Vec<String>>,
    pub issues: Mutex<Vec<String>>,
    pub emails: Mutex<Vec<String>>,
    pub jira: Mutex<Vec<String>>,
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
    async fn email(&self, _: &str, to: &[String], title: &str, lines: &[String], _: &str) {
        self.emails
            .lock()
            .unwrap()
            .push(format!("{} | {title} | {}", to.join(","), lines[0]));
    }
    async fn jira_comment(&self, _: &str, ticket: &str, body: &str) -> Result<(), String> {
        self.jira.lock().unwrap().push(format!(
            "comment {ticket}: {}",
            body.lines().nth(2).unwrap_or("")
        ));
        Ok(())
    }
    async fn jira_ticket(
        &self,
        _: &str,
        project: &str,
        title: &str,
        _: &str,
    ) -> Result<String, String> {
        self.jira
            .lock()
            .unwrap()
            .push(format!("ticket {project}: {title}"));
        Ok(format!("{project}-7"))
    }
}

/// An agent that does what the synthetic skill says: plan, call each tool
/// the plan lists into the work folder, build. It keeps its place in the
/// run's conversation, so a run split over worker steps goes on where it
/// was, and it ends a step when the step's time is up (after one call at
/// least), as the worker's loop does.
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

fn done_steps(convo: &[Value]) -> Vec<String> {
    convo
        .iter()
        .filter_map(|m| m["content"][0]["text"].as_str())
        .filter_map(|t| t.strip_prefix("did:"))
        .map(str::to_string)
        .collect()
}

#[async_trait::async_trait]
impl Agent for SkillFollower {
    async fn run(
        &self,
        _: &str,
        system: &str,
        tools: &[ToolDef],
        exec: &dyn ToolExec,
        _: f64,
        _: Instant,
        convo: &mut Vec<Value>,
    ) -> Result<(Step, u64, f64), String> {
        *self.asked.lock().unwrap() = Some((
            system.to_string(),
            tools.iter().map(|t| t.name.clone()).collect(),
        ));
        let mut did_one = false;
        let mark = |convo: &mut Vec<Value>, what: String| {
            convo.push(json!({"role": "assistant", "content": [{"type": "text", "text": format!("did:{what}")}]}));
        };
        loop {
            if did_one && exec.step_over() {
                return Ok((Step::More, 400, 0.01));
            }
            let done = done_steps(convo);
            match done.len() {
                0 => {
                    let plan = exec
                        .call(
                            RUN_SCRIPT,
                            &json!({"script": "${CLAUDE_SKILL_DIR}/count_visits.py", "args": ["plan", "--out", "{out}"]}),
                        )
                        .await;
                    let p = printed(&plan).to_string();
                    let _: Value = serde_json::from_str(&p).map_err(|e| format!("{e}: {plan}"))?;
                    mark(convo, p);
                }
                n => {
                    let plan: Value = serde_json::from_str(&done[0]).unwrap();
                    let calls = plan["calls"].as_array().unwrap();
                    if n <= calls.len() {
                        let c = &calls[n - 1];
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
                            return Ok((Step::Done("Stopped: needs access.".into()), 1_000, 0.01));
                        }
                        mark(convo, format!("call {n}"));
                    } else {
                        if self.try_write {
                            let r = exec.call("tag_page", &json!({"page": "/home"})).await;
                            self.refusals.lock().unwrap().push(r);
                            let b = exec.call("set_banner", &json!({"text": "hi"})).await;
                            self.refusals.lock().unwrap().push(b);
                        }
                        let built = exec
                            .call(
                                RUN_SCRIPT,
                                &json!({"script": "count_visits.py", "args": ["build", "--raw", "{work}", "--out", "{out}", "--date", self.date]}),
                            )
                            .await;
                        let r: Value = serde_json::from_str(printed(&built))
                            .map_err(|e| format!("{e}: {built}"))?;
                        return Ok((
                            Step::Done(format!(
                                "Report: {}. Findings: {}",
                                r["report"].as_str().unwrap_or(""),
                                r["findings"]
                            )),
                            12_000,
                            0.12,
                        ));
                    }
                }
            }
            did_one = true;
        }
    }
}

pub struct Rig {
    pub kv: Memory,
    pub files: MemFiles,
    pub runs: Runs,
    pub told: Told,
    pub stdio: LocalStdio,
    pub judge: Judge,
    pub gaps: Gaps,
    pub team: Team,
    pub keys: Keys,
    pub step: std::time::Duration,
}

impl Rig {
    pub fn new() -> Self {
        let dir = scratch("rig");
        Self {
            kv: Memory::default(),
            files: MemFiles::default(),
            runs: Runs::default(),
            told: Told::default(),
            stdio: LocalStdio::default(),
            judge: Judge::default(),
            gaps: Gaps::default(),
            team: Team {
                accounts: vec!["111122223333".into()],
                week_file: dir.join("week").display().to_string(),
                deny_file: dir.join("deny").display().to_string(),
            },
            keys: Keys { ok: true },
            step: std::time::Duration::from_secs(600),
        }
    }

    pub fn week(&self, n: u32) {
        std::fs::write(&self.team.week_file, n.to_string()).unwrap();
    }

    pub async fn schedule(&self, change: impl FnOnce(&mut Schedule)) -> Schedule {
        let mut s = Schedule {
            id: "01SCHED".into(),
            skill_repo: "acme/skills".into(),
            skill_path: "skills/visit-count/SKILL.md".into(),
            skill_name: "visit-count".into(),
            cron: "23 7 * * MON".into(),
            timezone: "America/New_York".into(),
            auto_tools: true,
            notify: Destinations {
                chat: "always".into(),
                issue_on_failure: true,
                emails: vec!["ops@example.com".into()],
                email_on: "always".into(),
                jira_ticket: "OPS-1".into(),
                jira_comment_on: "always".into(),
                jira_project: "OPS".into(),
            },
            ..Default::default()
        };
        change(&mut s);
        sr::validate(&mut s).unwrap();
        sr::put(&self.kv, TEAM, &s, Expect::Any).await.unwrap();
        s
    }

    fn deps<'a>(&'a self, agent: &'a dyn Agent) -> Deps<'a> {
        Deps {
            kv: &self.kv,
            source: &Source,
            conns: &NoConns,
            athena: &NoAthena,
            sandbox: &LocalSandbox,
            files: &self.files,
            runs: &self.runs,
            notify: &self.told,
            agent,
            stdio: &self.stdio,
            keys: &self.keys,
            judge: &self.judge,
            builtins: &NoBuiltins,
            servers: &self.team,
            gaps: &self.gaps,
            app_url: "https://app.example.com",
            step: self.step,
        }
    }

    /// Runs the schedule now, and every step a long run goes on in.
    pub async fn run(&self, agent: &dyn Agent, trigger: &str) -> Option<RunEntry> {
        let deps = self.deps(agent);
        let mut e = execute(
            &deps,
            &SkillRunMessage {
                team_id: TEAM.into(),
                schedule_id: "01SCHED".into(),
                trigger: trigger.into(),
                requested_by: "admin@example.com".into(),
                ..Default::default()
            },
        )
        .await;
        loop {
            let next = self.runs.queued.lock().unwrap().pop();
            let Some(msg) = next else { return e };
            e = execute(&deps, &msg).await;
        }
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
async fn a_scheduled_skill_runs_end_to_end_through_a_stdio_server_in_its_sandbox() {
    let rig = Rig::new();
    rig.schedule(|_| {}).await;

    // Week one: no memory yet, so the plan asks for 30 days too.
    rig.week(0);
    let a = SkillFollower::new("2026-11-23");
    let e = rig.run(&a, "manual").await.unwrap();
    assert_eq!(e.status, "completed", "{e:?}");
    assert_eq!(
        e.outputs,
        ["reports/2026-11-23.html", "snapshots/2026-11-23.json"]
    );
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

    // The server was found in the team's repositories, started with the
    // role's keys, and stopped at the end.
    assert_eq!(
        *rig.stdio.started.lock().unwrap(),
        [("traffic-mcp".to_string(), true)]
    );
    assert_eq!(*rig.stdio.stopped.lock().unwrap(), 1);

    // Its tools, as the schedule now shows them: nobody ticked anything.
    let (s, _) = sr::get(&rig.kv, TEAM, "01SCHED").await.unwrap().unwrap();
    let res = s.resolution.clone().unwrap();
    let by: BTreeMap<&str, (Risk, risk::Basis)> = res
        .tools
        .iter()
        .map(|t| (t.name.as_str(), (t.risk, t.basis)))
        .collect();
    assert_eq!(by["traffic_counts"], (Risk::Read, risk::Basis::Model));
    assert_eq!(by["tag_page"], (Risk::Write, risk::Basis::Annotation));
    assert_eq!(
        by["set_banner"],
        (Risk::Consequential, risk::Basis::Annotation)
    );
    assert_eq!(s.stdio.len(), 1, "the server is pinned on the schedule");
    assert!(matches!(&s.stdio[0].source, StdioSource::Git { commit, .. } if commit == SHA));
    assert_eq!(s.stdio[0].aws_account, "111122223333");
    // One model check, for the one tool its server says nothing about.
    let asked = rig.judge.asked.lock().unwrap().clone();
    assert_eq!(
        asked,
        [(
            format!("git:acme/traffic-mcp@{SHA}"),
            vec!["traffic_counts".to_string()]
        )]
    );

    // Week two: the memory comes back, and the report compares with it.
    rig.week(1);
    let b = SkillFollower::new("2026-11-30");
    let e2 = rig.run(&b, "schedule").await.unwrap();
    assert_eq!(e2.status, "completed", "{e2:?}");
    assert_eq!(
        e2.outputs,
        ["reports/2026-11-30.html", "snapshots/2026-11-30.json"]
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
    if let Ok(p) = std::env::var("SKILL_RUN_REPORT_OUT") {
        std::fs::write(p, &report).unwrap();
    }

    // The agent saw only the tools it may call: never the one that deletes.
    let (system, tools) = b.asked.lock().unwrap().clone().unwrap();
    assert!(tools.contains(&"traffic_counts".to_string()));
    assert!(tools.contains(&"tag_page".to_string()));
    assert!(!tools.contains(&"set_banner".to_string()));
    assert!(system.contains("`traffic_counts`: reads"), "{system}");
    assert!(system.contains("`tag_page`: refused: this run is read-only"));

    // Every destination heard about both runs.
    assert_eq!(rig.told.chat.lock().unwrap().len(), 2);
    assert_eq!(rig.told.emails.lock().unwrap().len(), 2);
    assert!(rig.told.emails.lock().unwrap()[1]
        .starts_with("ops@example.com | Skill run: visit-count | Finished"));
    let jira = rig.told.jira.lock().unwrap().clone();
    assert_eq!(jira.len(), 2);
    assert!(jira[0].starts_with("comment OPS-1: Finished"), "{jira:?}");
    assert!(rig.told.issues.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_run_longer_than_a_worker_step_goes_on_in_the_next_with_its_server_still_running() {
    let mut rig = Rig::new();
    rig.step = std::time::Duration::from_secs(0);
    rig.schedule(|_| {}).await;
    rig.week(0);
    let a = SkillFollower::new("2026-11-23");
    let e = rig.run(&a, "manual").await.unwrap();
    assert_eq!(e.status, "completed", "{e:?}");
    let notes = rig.runs.notes.lock().unwrap().join("\n");
    assert!(notes.contains("Going on in step 2"), "{notes}");
    assert!(notes.contains("Going on in step 3"), "{notes}");
    // One server for the whole run, across its steps.
    assert_eq!(rig.stdio.started.lock().unwrap().len(), 1);
    assert_eq!(*rig.stdio.stopped.lock().unwrap(), 1);
    assert_eq!(
        e.outputs,
        ["reports/2026-11-23.html", "snapshots/2026-11-23.json"]
    );
    assert!(
        !rig.files
            .0
            .lock()
            .unwrap()
            .keys()
            .any(|k| k.ends_with("state.json")),
        "the saved state is gone"
    );
}

#[tokio::test]
async fn a_tool_that_writes_is_refused_unless_allowed_and_one_that_deletes_never_runs() {
    let rig = Rig::new();
    rig.schedule(|_| {}).await;
    let mut a = SkillFollower::new("2026-11-23");
    a.try_write = true;
    let e = rig.run(&a, "manual").await.unwrap();
    assert_eq!(e.status, "completed");
    let r = a.refusals.lock().unwrap().clone();
    assert!(r[0].contains("read-only"), "{r:?}");
    assert!(r[1].contains("sends, publishes or deletes"), "{r:?}");

    let rig = Rig::new();
    rig.schedule(|s| s.allow_writes = true).await;
    let mut a = SkillFollower::new("2026-11-23");
    a.try_write = true;
    rig.run(&a, "manual").await.unwrap();
    let r = a.refusals.lock().unwrap().clone();
    assert!(r[0].starts_with("changed:"), "{r:?}");
    assert!(r[1].contains("sends, publishes or deletes"), "{r:?}");
}

#[tokio::test]
async fn an_aws_refusal_names_the_missing_permission_and_records_it_for_the_role_update() {
    let rig = Rig::new();
    rig.schedule(|_| {}).await;
    std::fs::write(&rig.team.deny_file, "1").unwrap();
    let a = SkillFollower::new("2026-11-23");
    let e = rig.run(&a, "schedule").await.unwrap();
    assert_eq!(e.status, "needs_access", "{e:?}");
    assert!(
        e.needs_access[0].contains("cloudfront:ListDistributions"),
        "{:?}",
        e.needs_access
    );
    assert!(e.needs_access[0].contains("111122223333"));
    assert_eq!(
        *rig.gaps.0.lock().unwrap(),
        [(
            "111122223333".to_string(),
            vec!["cloudfront:ListDistributions".to_string()]
        )]
    );
    assert_eq!(e.outputs, ["needs-access.md"]);
    let keys: Vec<String> = rig.files.0.lock().unwrap().keys().cloned().collect();
    assert!(
        !keys
            .iter()
            .any(|k| k.contains(".html") || k.contains("skill-state")),
        "{keys:?}"
    );
    // A failed scheduled run files its Jira ticket and its issue.
    assert!(rig
        .told
        .jira
        .lock()
        .unwrap()
        .iter()
        .any(|j| j.starts_with("ticket OPS: visit-count failed")));
    assert_eq!(rig.told.issues.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn missing_access_stops_before_the_agent_with_what_it_needs() {
    let mut rig = Rig::new();
    rig.team.accounts.clear();
    rig.schedule(|s| {
        s.aws = Some(AwsGrant {
            account: "111122223333".into(),
            region: "us-east-1".into(),
            workgroup: "logs".into(),
            max_gb: 10.0,
        });
    })
    .await;
    rig.keys = Keys { ok: false };
    let a = SkillFollower::new("2026-11-23");
    let e = rig.run(&a, "schedule").await.unwrap();
    assert_eq!(e.status, "needs_access");
    assert!(
        e.needs_access
            .iter()
            .any(|n| n.starts_with("traffic-mcp: CoderHelm's read-only role")),
        "{:?}",
        e.needs_access
    );
    assert!(
        e.needs_access
            .iter()
            .any(|n| n.starts_with("aws_athena_query: the AWS reader role")),
        "{:?}",
        e.needs_access
    );
    assert!(a.asked.lock().unwrap().is_none(), "the agent never started");
    assert_eq!(e.outputs, ["needs-access.md"]);

    // With no connected account at all, the server that reads AWS says so.
    let mut rig = Rig::new();
    rig.team.accounts.clear();
    rig.schedule(|_| {}).await;
    let e = rig
        .run(&SkillFollower::new("2026-11-23"), "manual")
        .await
        .unwrap();
    assert!(
        e.needs_access[0].contains("no AWS account is connected"),
        "{:?}",
        e.needs_access
    );
}

#[tokio::test]
async fn reading_a_drafts_tools_finds_them_without_running() {
    let rig = Rig::new();
    let d = sr::Draft {
        id: "01DRAFT".into(),
        skill_repo: "acme/skills".into(),
        skill_path: "skills/visit-count/SKILL.md".into(),
        skill_name: "visit-count".into(),
        ..Default::default()
    };
    crate::plugins::kv::put_as(&rig.kv, TEAM, &sr::resolve_sk("01DRAFT"), &d, Expect::Any)
        .await
        .unwrap();
    let a = SkillFollower::new("2026-11-23");
    let deps = rig.deps(&a);
    let none = execute(
        &deps,
        &SkillRunMessage {
            team_id: TEAM.into(),
            schedule_id: "01DRAFT".into(),
            trigger: "manual".into(),
            mode: "resolve".into(),
            ..Default::default()
        },
    )
    .await;
    assert!(none.is_none());
    let (d, _) = crate::plugins::kv::get_as::<sr::Draft>(&rig.kv, TEAM, &sr::resolve_sk("01DRAFT"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.done);
    let res = d.resolution.unwrap();
    let names: Vec<&str> = res.tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["traffic_counts", "tag_page", "set_banner"]);
    assert_eq!(res.servers[0].id, "traffic-mcp");
    assert!(a.asked.lock().unwrap().is_none());
    assert_eq!(*rig.stdio.stopped.lock().unwrap(), 1);
}

#[tokio::test]
async fn a_paused_schedule_doesnt_fire_and_two_runs_never_overlap() {
    let rig = Rig::new();
    rig.schedule(|s| s.paused = true).await;
    let a = SkillFollower::new("2026-11-23");
    assert!(rig.run(&a, "schedule").await.is_none());
    // Run now still runs a paused schedule: it is how people test it.
    assert_eq!(rig.run(&a, "manual").await.unwrap().status, "completed");

    let now = chrono::Utc::now();
    sr::claim(&rig.kv, TEAM, "01SCHED", "other-run", now)
        .await
        .unwrap();
    let e = rig.run(&a, "manual").await.unwrap();
    assert_eq!(e.status, "failed");
    assert!(e.summary.contains("one at a time"), "{}", e.summary);
    assert!(rig.run(&a, "schedule").await.is_none());
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
fn access_errors_are_told_from_passing_failures_and_name_their_actions() {
    assert!(is_access_error(
        "An error occurred (AccessDeniedException) when calling"
    ));
    let refusal = "User: arn:aws:sts::1:assumed-role/R/s is not authorized to perform: athena:StartQueryExecution on resource: x";
    assert!(is_access_error(refusal));
    assert_eq!(refused_actions(refusal), ["athena:StartQueryExecution"]);
    assert!(refused_actions("timed out").is_empty());
    assert!(is_access_error("HTTP status 401: invalid API key"));
    assert!(!is_access_error(
        "HIVE_S3_THROTTLING: Please reduce your request rate"
    ));
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
    assert_eq!(
        pick_report(&[
            "snapshots/a.json".into(),
            "x.html".into(),
            "reports/b.html".into()
        ]),
        "reports/b.html"
    );
}
