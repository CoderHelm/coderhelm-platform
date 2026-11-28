//! A scheduled skill run in the worker: `common::skill_runs::engine` with
//! AWS behind its seams.
//!
//! - the skill's folder: read from its repository at the commit the team's
//!   skill sync read last, through the team's GitHub App installation (the
//!   whole folder, scripts of any size, not the 32 KB text the skill store
//!   keeps for agents to read);
//! - the granted tools: the team's connected MCP servers, remote first, the
//!   same connections plugin runs use;
//! - Athena: the team's AWS reader role (`skill_runs::aws`);
//! - scripts: the code sandbox, the skill's files under `skill/` and the
//!   output folder at `skill/.coderhelm-out` (`$CODERHELM_OUT`), which comes
//!   back whole after each script;
//! - files: the data bucket (outputs and the memory folder);
//! - the run: the runs table (`ticket_source` `skill_run`) and its notes;
//! - the agent: the tool loop as `Agent::SkillRun`, every model call held to
//!   the schedule's model budget and attributed to `skill_run`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use aws_sdk_dynamodb::types::AttributeValue as A;
use common::plugins::connect::ToolDef;
use common::plugins::run::ToolExec;
use common::skill_runs::engine::{self, Deps, RunRow, ScriptResult, Step};
use common::skill_runs::resolve::Offered;
use common::skill_runs::{risk, stdio, SkillRunMessage};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::agent::llm::{ConverseOptions, ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;
use crate::clients::sandbox::{SandboxClient, ScriptRun};
use crate::WorkerState;

type Res = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Files of one skill's folder brought into a run at most, and their size.
const MAX_SKILL_FILES: usize = 200;
const MAX_SKILL_BYTES: usize = 8 * 1024 * 1024;

fn github(state: &WorkerState, installation: u64) -> Result<GitHubClient, String> {
    GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation,
        &state.http,
    )
    .map_err(|e| format!("GitHub sign in failed: {e}"))
}

// ── The skill's folder ──────────────────────────────────────────────────────

struct RepoSource<'a> {
    state: &'a WorkerState,
}

/// Pure: the files of the folder `dir` in a tree listing, by their path
/// inside it, within the caps. `Err` names what went over.
pub fn folder_files<'t>(
    tree: impl Iterator<Item = (&'t String, &'t str, &'t str)>,
    dir: &str,
    max: usize,
) -> Result<Vec<(String, String)>, String> {
    let prefix = if dir.is_empty() {
        String::new()
    } else {
        format!("{dir}/")
    };
    let mut out: Vec<(String, String)> = tree
        .filter(|(_, kind, _)| *kind == "blob")
        .filter_map(|(p, _, sha)| {
            p.strip_prefix(&prefix)
                .filter(|r| !r.starts_with(".coderhelm-out"))
                .map(|r| (r.to_string(), sha.to_string()))
        })
        .collect();
    out.sort();
    if out.len() > max {
        return Err(format!(
            "the folder holds {} files; a run brings at most {max}",
            out.len()
        ));
    }
    Ok(out)
}

#[async_trait::async_trait]
impl engine::SkillSource for RepoSource<'_> {
    async fn files(
        &self,
        team: &str,
        repo: &str,
        skill_path: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        let st = common::skills::SkillStore {
            dynamo: &self.state.dynamo,
            table: &self.state.config.settings_table_name,
            team,
        };
        let r = st
            .repo(repo)
            .await?
            .ok_or_else(|| format!("{repo} isn't connected as a skill repository any more"))?;
        let (owner, name) = repo.split_once('/').ok_or("not owner/name")?;
        let gh = github(self.state, r.installation_id)?;
        let sha = if r.sha.is_empty() {
            let branch = gh
                .get_default_branch(owner, name)
                .await
                .map_err(|e| e.to_string())?;
            gh.get_ref(owner, name, &branch)
                .await
                .map_err(|e| e.to_string())?
        } else {
            r.sha.clone()
        };
        let tree = gh
            .tree_all(owner, name, &sha)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("the repository's tree is too large to read in one go")?;
        let dir = skill_path
            .strip_suffix("SKILL.md")
            .unwrap_or("")
            .trim_end_matches('/');
        let listed = folder_files(
            tree.iter()
                .map(|(p, e)| (p, e.kind.as_str(), e.sha.as_str())),
            dir,
            MAX_SKILL_FILES,
        )?;
        let mut files = Vec::new();
        let mut total = 0;
        for (p, blob) in listed {
            let bytes = gh
                .blob_bytes(owner, name, &blob)
                .await
                .map_err(|e| format!("{p}: {e}"))?;
            total += bytes.len();
            if total > MAX_SKILL_BYTES {
                return Err(format!(
                    "the skill's folder is over {} MB",
                    MAX_SKILL_BYTES / (1024 * 1024)
                ));
            }
            files.push((p, bytes));
        }
        Ok(files)
    }
}

// ── The sandbox ─────────────────────────────────────────────────────────────

/// The skill's files under `skill/`, the output folder under
/// `skill/.coderhelm-out/`.
pub fn pack(
    skill: &[(String, Vec<u8>)],
    out: &BTreeMap<String, String>,
) -> Result<Vec<u8>, String> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut tar = tar::Builder::new(gz);
    let mut add = |path: String, body: &[u8]| {
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(0o755);
        h.set_cksum();
        tar.append_data(&mut h, path, body)
            .map_err(|e| e.to_string())
    };
    for (p, b) in skill {
        add(format!("skill/{p}"), b)?;
    }
    for (p, b) in out {
        add(format!("skill/.coderhelm-out/{p}"), b.as_bytes())?;
    }
    // The folder itself, so a run with nothing in it yet still has it.
    let mut h = tar::Header::new_gnu();
    h.set_entry_type(tar::EntryType::Directory);
    h.set_size(0);
    h.set_mode(0o755);
    h.set_cksum();
    tar.append_data(&mut h, "skill/.coderhelm-out/", &[][..])
        .map_err(|e| e.to_string())?;
    tar.into_inner()
        .and_then(|g| g.finish())
        .map_err(|e| e.to_string())
}

/// Pure: whether what came back is the whole output folder. A build that
/// didn't run, dropped files, or brought back nothing where there were
/// files (an upload that failed) is merged, never taken as the folder.
pub fn whole(ran: bool, dropped: usize, came_back: usize, had: usize) -> bool {
    ran && dropped == 0 && !(came_back == 0 && had > 0)
}

struct CodeSandbox<'a> {
    client: SandboxClient<'a>,
}

#[async_trait::async_trait]
impl engine::Sandbox for CodeSandbox<'_> {
    async fn run(
        &self,
        key: &str,
        skill: &[(String, Vec<u8>)],
        out: &BTreeMap<String, String>,
        cmd: &str,
        deadline: Instant,
    ) -> Result<ScriptResult, String> {
        if !self.client.is_enabled() {
            return Err("the code sandbox isn't set up for this install".into());
        }
        let source = pack(skill, out)?;
        let r = self
            .client
            .run_script(
                ScriptRun {
                    key,
                    source,
                    cmd,
                    secret: None,
                    hosts: Vec::new(),
                    max_files: engine::MAX_FILES,
                },
                Some(deadline),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(ScriptResult {
            complete: whole(r.ran, r.dropped.len(), r.files.len(), out.len()),
            ran: r.ran,
            exit_code: r.exit_code,
            output: r.output,
            files: r.files.into_iter().collect(),
        })
    }
}

// ── Files in the data bucket ────────────────────────────────────────────────

struct Bucket<'a> {
    state: &'a WorkerState,
}

#[async_trait::async_trait]
impl engine::Files for Bucket<'_> {
    async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let page = self
                .state
                .s3
                .list_objects_v2()
                .bucket(&self.state.config.bucket_name)
                .prefix(prefix)
                .set_continuation_token(token.clone())
                .send()
                .await
                .map_err(|e| e.to_string())?;
            keys.extend(
                page.contents()
                    .iter()
                    .filter_map(|o| o.key().map(str::to_string)),
            );
            token = page.next_continuation_token().map(str::to_string);
            if token.is_none() || keys.len() > 2_000 {
                return Ok(keys);
            }
        }
    }
    async fn get(&self, key: &str) -> Result<Option<String>, String> {
        let Ok(o) = self
            .state
            .s3
            .get_object()
            .bucket(&self.state.config.bucket_name)
            .key(key)
            .send()
            .await
        else {
            return Ok(None);
        };
        let b = o.body.collect().await.map_err(|e| e.to_string())?;
        Ok(Some(String::from_utf8_lossy(&b.into_bytes()).to_string()))
    }
    async fn put(&self, key: &str, body: &str) -> Result<(), String> {
        let name = key.rsplit('/').next().unwrap_or(key);
        self.state
            .s3
            .put_object()
            .bucket(&self.state.config.bucket_name)
            .key(key)
            .body(body.as_bytes().to_vec().into())
            .content_type(common::outputs::content_type(name))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    async fn delete(&self, key: &str) -> Result<(), String> {
        self.state
            .s3
            .delete_object()
            .bucket(&self.state.config.bucket_name)
            .key(key)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

// ── The run in the team's runs ──────────────────────────────────────────────

struct Runs<'a> {
    state: &'a WorkerState,
}

#[async_trait::async_trait]
impl engine::RunLog for Runs<'_> {
    async fn write(&self, team: &str, r: &RunRow) {
        let table = &self.state.config.runs_table_name;
        if table.is_empty() {
            return;
        }
        let now = chrono::Utc::now().to_rfc3339();
        // Through the one gate (`common::run_ending`): finished with its
        // result on the run page, stopped with its error, or working.
        let ending = common::run_ending::for_job(
            &r.status,
            &r.run_id,
            "the skill's result on the run page",
            &r.error,
        );
        let mut set = common::run_ending::item_fields(&ending);
        set.extend([
            ("ticket_source", A::S("skill_run".into())),
            ("ticket_id", A::S(format!("skill:{}", r.schedule_id))),
            ("title", A::S(format!("Skill run: {}", r.skill_name))),
            ("current_pass", A::S("skill_run".into())),
            ("skill_schedule_id", A::S(r.schedule_id.clone())),
            ("skill_repo", A::S(r.repo.clone())),
            ("sender", A::S(r.requested_by.clone())),
            ("tokens_in", A::N(r.tokens.to_string())),
            ("tokens_out", A::N("0".into())),
            ("cost_usd", A::N(format!("{:.6}", r.cost_usd))),
            ("updated_at", A::S(now)),
        ]);
        if !r.error.is_empty() {
            set.push((
                "error",
                A::S(common::truncate_str(&r.error, 1_000).to_string()),
            ));
        }
        let mut expr = "SET created_at = if_not_exists(created_at, :created_at)".to_string();
        let mut req = self
            .state
            .dynamo
            .update_item()
            .table_name(table)
            .key("team_id", A::S(team.to_string()))
            .key("run_id", A::S(r.run_id.clone()))
            .expression_attribute_values(":created_at", A::S(r.created_at.clone()));
        for (k, v) in set {
            expr.push_str(&format!(", #{k} = :{k}"));
            req = req
                .expression_attribute_names(format!("#{k}"), k)
                .expression_attribute_values(format!(":{k}"), v);
        }
        if let Err(e) = req.update_expression(expr).send().await {
            warn!(team, run = %r.run_id, error = %e, "Couldn't list the skill run with the team's runs");
            return;
        }
        // Counted once each, however often the record is written.
        if self.state.config.analytics_table_name.is_empty() {
            return;
        }
        let counts = common::team_counts::DynamoCounts {
            dynamo: self.state.dynamo.clone(),
            table: self.state.config.analytics_table_name.clone(),
        };
        let end = match r.status.as_str() {
            "completed" => Some(common::team_counts::RunEnd::Completed),
            "failed" => Some(common::team_counts::RunEnd::Failed),
            _ => None,
        };
        let counted = match end {
            None => {
                common::team_counts::count_run_started(&counts, team, &r.run_id, &r.created_at)
                    .await
            }
            Some(end) => {
                let _ =
                    common::team_counts::count_run_started(&counts, team, &r.run_id, &r.created_at)
                        .await;
                common::team_counts::count_run_ended(
                    &counts,
                    team,
                    &r.run_id,
                    end,
                    &chrono::Utc::now().to_rfc3339(),
                )
                .await
            }
        };
        if let Err(e) = counted {
            warn!(team, run = %r.run_id, error = %e, "Couldn't count the skill run");
        }
    }

    async fn continue_later(&self, msg: &SkillRunMessage) -> Result<(), String> {
        send(self.state, msg).await
    }

    async fn note(&self, team: &str, run: &str, text: &str) {
        super::run_lists::append_note(
            &super::run_lists::Target {
                dynamo: &self.state.dynamo,
                table: &self.state.config.runs_table_name,
                team_id: team,
                run_id: run,
                archive: Some((&self.state.s3, &self.state.config.bucket_name)),
            },
            text,
        )
        .await;
    }
}

// ── Where the result goes ───────────────────────────────────────────────────

struct Tell<'a> {
    state: &'a WorkerState,
}

/// The chat message of a run that ended.
pub fn chat_message(title: &str, lines: &[String], url: &str) -> common::notify::Message {
    use common::notify::{Block, Button, Message, Size, Text, Tone};
    let failed = lines.first().is_some_and(|l| !l.starts_with("Finished"));
    let mut blocks = vec![Block::Banner {
        text: title.to_string(),
        tone: if failed { Tone::Attention } else { Tone::Good },
        size: Size::Medium,
    }];
    blocks.extend(lines.iter().map(|l| Block::Text(Text::new(l.clone()))));
    Message {
        summary: format!("{title}: {}", lines.first().cloned().unwrap_or_default()),
        blocks,
        buttons: vec![Button::new("Open the run", url)],
        full_width: false,
    }
}

#[async_trait::async_trait]
impl engine::Notify for Tell<'_> {
    async fn chat(&self, team: &str, title: &str, lines: &[String], url: &str) -> bool {
        let cipher = common::mcp_credentials::Kms {
            client: &self.state.kms,
            key_id: "",
        };
        let channels = common::log_findings::channels(
            &self.state.dynamo,
            &cipher,
            &self.state.config.settings_table_name,
            team,
            // A run that didn't finish is an alert; a report is an update.
            if lines.first().is_some_and(|l| l.starts_with("Finished")) {
                common::notify::route::Kind::Update
            } else {
                common::notify::route::Kind::Alert
            },
        )
        .await;
        if channels.is_empty() {
            return false;
        }
        common::notify::deliver_recorded(
            &common::notify::Http(&self.state.http),
            &channels,
            &chat_message(title, lines, url),
            "skill_run",
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            team,
        )
        .await
        .any_ok()
    }

    async fn email(&self, team: &str, to: &[String], title: &str, lines: &[String], url: &str) {
        use common::mail::templates::fit;
        for addr in to {
            let mail = common::mail::Mail {
                kind: "skill_run_result".into(),
                to: addr.clone(),
                template: "skill_run_result",
                params: json!({
                    "skill": fit(title.trim_start_matches("Skill run: "), 120),
                    "outcome": fit(&lines.join("\n"), 1500),
                    "run_link": url,
                })
                .as_object()
                .cloned()
                .unwrap_or_default(),
                ses: common::mail::SesBody::Simple {
                    subject: title.to_string(),
                    text: format!("{}\n\nOpen the run: {url}", lines.join("\n")),
                    html: None,
                    headers: vec![],
                },
            };
            let clock = common::deliveries::Clock::start();
            let sent = common::mail::send(&crate::clients::email::sender(self.state), &mail).await;
            common::deliveries::record(
                &self.state.dynamo,
                &self.state.config.settings_table_name,
                team,
                &common::mail::delivery("skill_run_result", addr, &sent, clock.ms()),
            )
            .await;
            if let Err(e) = sent {
                warn!(team, code = %e.code, error = %e.message, "Couldn't email a skill run's result");
            }
        }
    }

    async fn jira_comment(&self, team: &str, ticket: &str, body: &str) -> Result<(), String> {
        let say = common::conversation::Say::note("skill_run", body.to_string());
        super::conversation::jira(self.state, team, ticket, say, "note", "Skill run")
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn jira_ticket(
        &self,
        team: &str,
        project: &str,
        title: &str,
        body: &str,
    ) -> Result<String, String> {
        let j = super::followups::jira(self.state, team).await;
        if !j.connected || j.create_url.is_empty() {
            return Err("the team's Jira app can't file tickets".into());
        }
        let r = self
            .state
            .http
            .post(&j.create_url)
            .header("Content-Type", "application/json")
            .body(
                json!({
                    "projectKey": project,
                    "summary": title,
                    "description": body,
                    "labels": ["coderhelm-skill-run"],
                    "forge_secret": j.secret,
                })
                .to_string(),
            )
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = r.status().as_u16();
        let v: Value = r.json().await.unwrap_or_default();
        match v["key"].as_str() {
            Some(k) if (200..300).contains(&status) => Ok(k.to_string()),
            _ => Err(format!("the Jira app answered {status}")),
        }
    }

    async fn issue(
        &self,
        team: &str,
        repo: &str,
        title: &str,
        body: &str,
    ) -> Result<String, String> {
        let st = common::skills::SkillStore {
            dynamo: &self.state.dynamo,
            table: &self.state.config.settings_table_name,
            team,
        };
        let r = st.repo(repo).await?.ok_or("not connected")?;
        let (owner, name) = repo.split_once('/').ok_or("not owner/name")?;
        github(self.state, r.installation_id)?
            .create_issue(owner, name, title, body)
            .await
            .map(|(_, url)| url)
            .map_err(|e| e.to_string())
    }
}

// ── The agent ───────────────────────────────────────────────────────────────

struct WorkerAgent<'a> {
    state: &'a WorkerState,
}

struct Adapter<'a> {
    exec: &'a dyn ToolExec,
}

#[async_trait::async_trait]
impl ToolExecutor for Adapter<'_> {
    async fn execute(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        // The worker step is over: the call waits for the run's next step.
        if self.exec.step_over() {
            return Err(Box::new(crate::agent::llm::StepEnds));
        }
        if self.exec.stop() {
            return Ok(json!(
                "Stop now: the run needs access or its time is used. End without a report."
            ));
        }
        Ok(Value::String(self.exec.call(name, input).await))
    }
}

/// The run's conversation as the tool loop keeps it, and back.
pub fn convo_in(convo: &[Value]) -> Vec<(String, Vec<Value>)> {
    convo
        .iter()
        .map(|m| {
            (
                m["role"].as_str().unwrap_or("user").to_string(),
                m["content"].as_array().cloned().unwrap_or_default(),
            )
        })
        .collect()
}

pub fn convo_out(messages: &[(String, Vec<Value>)]) -> Vec<Value> {
    messages
        .iter()
        .map(|(role, content)| json!({"role": role, "content": content}))
        .collect()
}

#[async_trait::async_trait]
impl engine::Agent for WorkerAgent<'_> {
    async fn run(
        &self,
        team: &str,
        system: &str,
        tools: &[ToolDef],
        exec: &dyn ToolExec,
        budget_usd: f64,
        deadline: Instant,
        convo: &mut Vec<Value>,
    ) -> Result<(Step, u64, f64), String> {
        let provider = crate::agent::provider::ModelProvider::load_for_team(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            team,
        )
        .await
        .map_err(|e| e.to_string())?;
        let defs: Vec<ToolDefinition> = tools
            .iter()
            .map(|t| ToolDefinition {
                name: t.name.clone(),
                description: t.description.clone(),
                input_schema: t.input_schema.clone(),
            })
            .collect();
        let mut messages = convo_in(convo);
        let mut usage = crate::models::TokenUsage::default();
        let model = provider.primary_model_id().to_string();
        let allowance = Arc::new(common::usage::RunAllowance::new(budget_usd));
        let r = common::usage::with_run_allowance(
            allowance,
            common::usage::with_feature(
                common::usage::feature::SKILL_RUN,
                crate::agent::provider::converse(
                    self.state,
                    &provider,
                    common::agent_policy::Agent::SkillRun,
                    &model,
                    system,
                    &mut messages,
                    &defs,
                    &Adapter { exec },
                    &mut usage,
                    ConverseOptions {
                        max_turns: 150,
                        max_tokens: 16_384,
                        deadline: Some(deadline),
                        effort: Some("medium"),
                    },
                    None,
                    None,
                ),
            ),
        )
        .await;
        *convo = convo_out(&messages);
        match r {
            Ok(text) => Ok((Step::Done(text), usage.total_tokens(), usage.cost_usd)),
            Err(e) if e.downcast_ref::<crate::agent::llm::StepEnds>().is_some() => {
                Ok((Step::More, usage.total_tokens(), usage.cost_usd))
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

// ── Stdio MCP servers in the sandbox ────────────────────────────────────────

/// Files of one server's repository brought into its session at most.
const MAX_SERVER_FILES: usize = 3_000;
const MAX_SERVER_BYTES: usize = 40 * 1024 * 1024;
/// Hosts a session needs besides the team's allowlist: npm's registry for
/// its install and AWS's APIs for its reads.
const SESSION_HOSTS: &[&str] = &["registry.npmjs.org", "amazonaws.com"];

/// The session's folder: the relay, its manifest, the server's files.
pub fn pack_session(
    relay: &str,
    manifest: &Value,
    server: &[(String, Vec<u8>)],
) -> Result<Vec<u8>, String> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut tar = tar::Builder::new(gz);
    let mut add = |path: String, body: &[u8], mode: u32| {
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(mode);
        h.set_cksum();
        tar.append_data(&mut h, path, body)
            .map_err(|e| e.to_string())
    };
    add("session/relay.py".into(), relay.as_bytes(), 0o755)?;
    add(
        "session/manifest.json".into(),
        manifest.to_string().as_bytes(),
        0o644,
    )?;
    for (p, b) in server {
        add(format!("session/server/{p}"), b, 0o755)?;
    }
    let mut h = tar::Header::new_gnu();
    h.set_entry_type(tar::EntryType::Directory);
    h.set_size(0);
    h.set_mode(0o755);
    h.set_cksum();
    tar.append_data(&mut h, "session/server/", &[][..])
        .map_err(|e| e.to_string())?;
    tar.into_inner()
        .and_then(|g| g.finish())
        .map_err(|e| e.to_string())
}

struct SandboxStdio<'a> {
    state: &'a WorkerState,
    client: SandboxClient<'a>,
}

impl SandboxStdio<'_> {
    /// A repository's files at a commit, through the team's installation.
    async fn repo_files(
        &self,
        team: &str,
        repo: &str,
        commit: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        let inst = super::plugins::team_installation(self.state, team)
            .await
            .ok_or("the team's GitHub App installation isn't known")?;
        let (owner, name) = repo.split_once('/').ok_or("not owner/name")?;
        let gh = github(self.state, inst)?;
        let tree = gh
            .tree_all(owner, name, commit)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("the repository's tree is too large to read in one go")?;
        let listed = folder_files(
            tree.iter()
                .map(|(p, e)| (p, e.kind.as_str(), e.sha.as_str())),
            "",
            MAX_SERVER_FILES,
        )?;
        let mut files = Vec::new();
        let mut total = 0;
        for (p, blob) in listed {
            if p.starts_with("node_modules/") || p.starts_with(".git/") {
                continue;
            }
            let bytes = gh
                .blob_bytes(owner, name, &blob)
                .await
                .map_err(|e| format!("{p}: {e}"))?;
            total += bytes.len();
            if total > MAX_SERVER_BYTES {
                return Err(format!(
                    "{repo} is over {} MB",
                    MAX_SERVER_BYTES / (1024 * 1024)
                ));
            }
            files.push((p, bytes));
        }
        Ok(files)
    }
}

#[async_trait::async_trait]
impl stdio::StdioHost for SandboxStdio<'_> {
    async fn start(
        &self,
        team: &str,
        key: &str,
        s: &stdio::StdioServer,
        aws: Option<&BTreeMap<String, String>>,
        deadline: Instant,
    ) -> Result<stdio::Session, String> {
        if !self.client.is_enabled() {
            return Err("the code sandbox isn't set up for this install".into());
        }
        let repos: Vec<String> = super::infra_repo::team_repos(self.state, team)
            .await
            .into_iter()
            .map(|r| r.full_name)
            .collect();
        stdio::validate(s, &repos, &stdio::catalog_packages())?;
        let (files, lock) = match &s.source {
            stdio::StdioSource::Git { repo, commit } => {
                let f = self.repo_files(team, repo, commit).await?;
                let root: Vec<String> = f
                    .iter()
                    .filter(|(p, _)| !p.contains('/'))
                    .map(|(p, _)| p.clone())
                    .collect();
                (f, root)
            }
            stdio::StdioSource::Npm { .. } => (Vec::new(), Vec::new()),
        };
        let lock: Vec<&str> = lock.iter().map(String::as_str).collect();
        let key = common::content_hash(key)[..24].to_string();
        let slots = self
            .client
            .session_slots(&key, stdio::SLOTS, std::time::Duration::from_secs(4 * 3600))
            .await
            .map_err(|e| e.to_string())?;
        let manifest = stdio::manifest(s, &s.install(&lock), &slots, None);
        let source = pack_session(stdio::RELAY, &manifest, &files)?;
        let secret = aws.map(|k| serde_json::to_string(k).unwrap_or_default());
        let hosts: Vec<String> = SESSION_HOSTS.iter().map(|h| h.to_string()).collect();
        let left = deadline.saturating_duration_since(Instant::now()).as_secs() / 60;
        let build = self
            .client
            .start_session(
                &key,
                source,
                secret.as_deref(),
                &hosts,
                (left as i32 + 140).min(300),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(stdio::Session {
            key,
            build,
            next: 0,
        })
    }

    async fn request(
        &self,
        session: &mut stdio::Session,
        req: &Value,
        deadline: Instant,
    ) -> Result<Value, String> {
        let i = session.next;
        session.next += 1;
        // The first request also waits for the build to start and the server
        // to install: give it at least five minutes.
        let deadline = if i == 0 {
            deadline.max(Instant::now() + std::time::Duration::from_secs(300))
        } else {
            deadline
        };
        let body = self
            .client
            .session_request(&session.key, &session.build, i, &req.to_string(), deadline)
            .await
            .map_err(|e| e.to_string())?;
        let v: Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
        if v["ok"].as_bool() == Some(true) {
            Ok(v["result"].clone())
        } else {
            Err(v["error"]
                .as_str()
                .unwrap_or("the server failed")
                .to_string())
        }
    }

    async fn stop(&self, session: &stdio::Session) {
        let deadline = Instant::now() + std::time::Duration::from_secs(20);
        let _ = self
            .client
            .session_request(
                &session.key,
                &session.build,
                session.next,
                &json!({"op": "stop"}).to_string(),
                deadline,
            )
            .await;
        self.client.stop_session(&session.key, &session.build).await;
    }
}

/// The team's read-only role's keys, short-lived, for one session.
struct Keys<'a> {
    state: &'a WorkerState,
}

#[async_trait::async_trait]
impl stdio::RoleKeys for Keys<'_> {
    async fn keys(&self, team: &str, account: &str) -> Result<BTreeMap<String, String>, String> {
        let s = common::log_findings::session_for(
            &self.state.dynamo,
            &self.state.sts,
            &self.state.config.aws_insights_table_name,
            team,
            account,
        )
        .await?
        .ok_or_else(|| format!("AWS account {account} isn't connected with a reader role"))?
        .into_session();
        Ok(BTreeMap::from([
            ("AWS_ACCESS_KEY_ID".to_string(), s.access_key_id),
            ("AWS_SECRET_ACCESS_KEY".to_string(), s.secret_access_key),
            ("AWS_SESSION_TOKEN".to_string(), s.session_token),
            ("AWS_REGION".to_string(), s.region.clone()),
            ("AWS_DEFAULT_REGION".to_string(), s.region),
        ]))
    }
}

// ── Tool risk, read by a model when a server says nothing ───────────────────

struct Judge<'a> {
    state: &'a WorkerState,
}

const VERDICT_PREFIX: &str = "TOOLRISK#";

#[async_trait::async_trait]
impl risk::RiskJudge for Judge<'_> {
    async fn judge(
        &self,
        team: &str,
        version: &str,
        tools: &[ToolDef],
    ) -> BTreeMap<String, risk::Verdict> {
        let kv = super::plugins::kv(self.state);
        let sk = format!("{VERDICT_PREFIX}{}", common::content_hash(version));
        let mut cached: BTreeMap<String, risk::Verdict> =
            common::plugins::kv::get_as(&kv, team, &sk)
                .await
                .ok()
                .flatten()
                .map(|(v, _)| v)
                .unwrap_or_default();
        let ask: Vec<ToolDef> = tools
            .iter()
            .filter(|t| !cached.contains_key(&t.name))
            .cloned()
            .collect();
        if ask.is_empty() {
            return cached;
        }
        let Ok(provider) = crate::agent::provider::ModelProvider::load_for_team(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            team,
        )
        .await
        else {
            return cached;
        };
        let mut usage = crate::models::TokenUsage::default();
        let answer = common::usage::with_feature(
            common::usage::feature::SKILL_RUN,
            crate::agent::provider::converse_simple(
                self.state,
                &provider,
                provider.judgment_model_id(),
                risk::JUDGE_SYSTEM,
                &risk::judge_prompt(&ask),
                &mut usage,
            ),
        )
        .await;
        if let Ok(text) = answer {
            cached.extend(risk::read_verdicts(&text, &ask));
            let _ = common::plugins::kv::put_as(
                &kv,
                team,
                &sk,
                &cached,
                common::plugins::kv::Expect::Any,
            )
            .await;
        }
        cached
    }
}

// ── CoderHelm's own connections, in a well-known server's shape ─────────────

/// New Relic's `execute_nrql_query` (its MCP server's tool: `account_id`,
/// `nrql_query`, results as `{"results": [...]}`) through the team's New
/// Relic connection.
pub const NRQL_TOOL: &str = "execute_nrql_query";
pub const NEWRELIC_SOURCE: &str = "coderhelm:newrelic";

struct Own<'a> {
    state: &'a WorkerState,
}

#[async_trait::async_trait]
impl engine::Builtins for Own<'_> {
    async fn offered(&self, team: &str) -> Vec<Offered> {
        let Some(ops) =
            super::ops_tools::OpsTools::load(self.state, team, "", "skill_run", 0).await
        else {
            return Vec::new();
        };
        if !ops.has_newrelic() {
            return Vec::new();
        }
        vec![Offered {
            source: NEWRELIC_SOURCE.into(),
            alias: "newrelic".into(),
            tool: ToolDef {
                name: NRQL_TOOL.into(),
                description: "Run one read-only NRQL query in a New Relic account; returns its results as JSON {\"results\": [...]}.".into(),
                input_schema: json!({"type": "object", "properties": {
                    "account_id": {"type": "integer"},
                    "nrql_query": {"type": "string"}}, "required": ["nrql_query"]}),
                read_only: Some(true),
                destructive: Some(false),
            },
        }]
    }

    async fn call(
        &self,
        team: &str,
        source: &str,
        tool: &str,
        args: &Value,
    ) -> Result<String, String> {
        if source != NEWRELIC_SOURCE || tool != NRQL_TOOL {
            return Err(format!("{source} offers no tool '{tool}'"));
        }
        let ops = super::ops_tools::OpsTools::load(self.state, team, "", "skill_run", 0)
            .await
            .ok_or("New Relic isn't connected")?;
        let account = match &args["account_id"] {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => String::new(),
        };
        let nrql = args["nrql_query"].as_str().unwrap_or("");
        let results = ops
            .newrelic_results(&account, nrql)
            .await
            .map_err(|e| format!("{} {}", e.message, e.fix))?;
        Ok(serde_json::to_string_pretty(&json!({"results": results})).unwrap_or_default())
    }
}

// ── The team's servers and accounts ─────────────────────────────────────────

struct TeamServers<'a> {
    state: &'a WorkerState,
}

#[async_trait::async_trait]
impl engine::Servers for TeamServers<'_> {
    async fn connected(&self, team: &str) -> Vec<(String, String)> {
        let table = if self.state.config.mcp_configs_table_name.is_empty() {
            &self.state.config.settings_table_name
        } else {
            &self.state.config.mcp_configs_table_name
        };
        let mut out: Vec<(String, String)> = crate::agent::mcp::load_team_plugins(
            &self.state.dynamo,
            &self.state.kms,
            table,
            &self.state.config.secrets_key_id,
            team,
        )
        .await
        .into_iter()
        .map(|p| (p.server_id.clone(), p.server_id))
        .collect();
        let rt = common::mcp_remote::runtime::Runtime::aws(
            self.state.dynamo.clone(),
            table,
            self.state.kms.clone(),
            &self.state.config.secrets_key_id,
            team,
        );
        if let Ok(list) = rt.store.list_servers(&rt.team).await {
            for s in list {
                if !out.iter().any(|(id, _)| *id == s.id) {
                    let alias = if s.catalog_id.is_empty() {
                        s.name.to_ascii_lowercase().replace(' ', "-")
                    } else {
                        s.catalog_id.clone()
                    };
                    out.push((s.id, alias));
                }
            }
        }
        out
    }

    async fn aws_accounts(&self, team: &str) -> Vec<String> {
        let t = &self.state.config.aws_insights_table_name;
        if t.is_empty() {
            return Vec::new();
        }
        let Ok(out) = self
            .state
            .dynamo
            .query()
            .table_name(t)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", A::S(team.to_string()))
            .expression_attribute_values(":p", A::S("AWS_CONN#".into()))
            .send()
            .await
        else {
            return Vec::new();
        };
        out.items()
            .iter()
            .filter_map(|i| {
                i.get("sk")?
                    .as_s()
                    .ok()?
                    .strip_prefix("AWS_CONN#")
                    .map(str::to_string)
            })
            .collect()
    }

    async fn find(
        &self,
        team: &str,
        families: &[String],
    ) -> (Vec<(stdio::StdioServer, bool)>, Vec<String>) {
        let mut found = Vec::new();
        let mut problems = Vec::new();
        let repos = super::infra_repo::team_repos(self.state, team).await;
        let candidates: Vec<String> = repos
            .into_iter()
            .map(|r| r.full_name)
            .filter(|full| {
                let name = full.rsplit('/').next().unwrap_or("").to_ascii_lowercase();
                families
                    .iter()
                    .any(|f| name.starts_with(&f.to_ascii_lowercase()))
            })
            .take(5)
            .collect();
        if candidates.is_empty() {
            return (found, problems);
        }
        let Some(inst) = super::plugins::team_installation(self.state, team).await else {
            return (
                found,
                vec!["the team's GitHub App installation isn't known".into()],
            );
        };
        let Ok(gh) = github(self.state, inst) else {
            return (found, vec!["GitHub sign in failed".into()]);
        };
        for repo in candidates {
            let Some((owner, name)) = repo.split_once('/') else {
                continue;
            };
            let head = match gh.get_default_branch(owner, name).await {
                Ok(b) => gh.get_ref(owner, name, &b).await.ok(),
                Err(_) => None,
            };
            let Some(head) = head else {
                problems.push(format!("{repo}: its default branch couldn't be read"));
                continue;
            };
            match gh
                .read_file_if_exists(owner, name, "package.json", &head)
                .await
            {
                Ok(Some(text)) => {
                    if let Some(found_one) = stdio::server_from_package(&repo, &head, &text) {
                        found.push(found_one);
                    }
                }
                Ok(None) => {}
                Err(e) => problems.push(format!("{repo}: {e}")),
            }
        }
        (found, problems)
    }
}

/// Reads CoderHelm's role was refused, for the install's role update.
struct Gaps<'a> {
    state: &'a WorkerState,
}

#[async_trait::async_trait]
impl engine::AccessGaps for Gaps<'_> {
    async fn record(&self, team: &str, account: &str, actions: &[String], needed_for: &str) {
        common::aws_role_reads::record(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            team,
            &common::aws_role_reads::Gap {
                account: account.to_string(),
                actions: actions.to_vec(),
                needed_for: needed_for.to_string(),
                seen_at: chrono::Utc::now().to_rfc3339(),
                ..Default::default()
            },
        )
        .await;
    }
}

// ── The pass ────────────────────────────────────────────────────────────────

/// Sends a skill run message to the worker's queue.
pub async fn send(state: &WorkerState, msg: &SkillRunMessage) -> Result<(), String> {
    let body = serde_json::to_string(&common::messages::WorkerMessage::SkillRun(msg.clone()))
        .map_err(|e| e.to_string())?;
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// How long one worker step runs the agent before the run goes on in the
/// next: the worker's own time limit is 15 minutes.
pub const STEP: std::time::Duration = std::time::Duration::from_secs(9 * 60);

/// One skill run: a schedule's fire, Run now, a long run's next step, or a
/// reading of a skill's tools for the schedule's form.
pub async fn run(state: &WorkerState, msg: SkillRunMessage) -> Res {
    let kv = super::plugins::kv(state);
    let local = super::plugins::CatalogConnections { state };
    let remote = super::plugins::remote(
        state,
        &msg.team_id,
        Some(&msg.requested_by),
        format!("skill run, schedule {}", msg.schedule_id),
    );
    let conns = common::plugins::remote::Layered {
        remote: &remote,
        local: &local,
    };
    let athena = common::skill_runs::aws::RoleAthena {
        dynamo: state.dynamo.clone(),
        sts: state.sts.clone(),
        insights_table: state.config.aws_insights_table_name.clone(),
    };
    let egress = crate::agent::policy::sandbox_egress(state, &msg.team_id).await;
    let sandbox = CodeSandbox {
        client: SandboxClient::new(
            &state.codebuild,
            &state.s3,
            &state.logs,
            &state.config.sandbox_bucket_name,
            &state.config.sandbox_project_name,
        )
        .with_egress(egress.clone()),
    };
    let stdio_host = SandboxStdio {
        state,
        client: SandboxClient::new(
            &state.codebuild,
            &state.s3,
            &state.logs,
            &state.config.sandbox_bucket_name,
            &state.config.sandbox_project_name,
        )
        .with_egress(egress),
    };
    let deps = Deps {
        kv: &kv,
        source: &RepoSource { state },
        conns: &conns,
        athena: &athena,
        sandbox: &sandbox,
        files: &Bucket { state },
        runs: &Runs { state },
        notify: &Tell { state },
        agent: &WorkerAgent { state },
        stdio: &stdio_host,
        keys: &Keys { state },
        judge: &Judge { state },
        builtins: &Own { state },
        servers: &TeamServers { state },
        gaps: &Gaps { state },
        app_url: common::public_urls::app_url(&state.config.stage),
        step: STEP,
    };
    match engine::execute(&deps, &msg).await {
        Some(e) => info!(
            team = %msg.team_id,
            schedule = %msg.schedule_id,
            run = %e.run_id,
            status = %e.status,
            outputs = e.outputs.len(),
            "Skill run step finished"
        ),
        None => {
            info!(team = %msg.team_id, schedule = %msg.schedule_id, trigger = %msg.trigger, mode = %msg.mode, "Skill run: nothing to run")
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_skills_folder_is_its_files_by_their_path_inside_it() {
        let tree = [
            ("skills/scan/SKILL.md".to_string(), "blob", "a"),
            ("skills/scan/scan.py".to_string(), "blob", "b"),
            ("skills/scan/lib".to_string(), "tree", "c"),
            ("skills/scan/lib/util.py".to_string(), "blob", "d"),
            ("skills/scan-other/SKILL.md".to_string(), "blob", "e"),
            ("skills/scan/.coderhelm-out/x".to_string(), "blob", "f"),
        ];
        let got = folder_files(
            tree.iter().map(|(p, k, s)| (p, *k, *s)),
            "skills/scan",
            MAX_SKILL_FILES,
        )
        .unwrap();
        assert_eq!(
            got,
            [
                ("SKILL.md".to_string(), "a".to_string()),
                ("lib/util.py".into(), "d".into()),
                ("scan.py".into(), "b".into())
            ]
        );
        let many: Vec<(String, &str, &str)> = (0..MAX_SKILL_FILES + 1)
            .map(|i| (format!("s/f{i}"), "blob", "x"))
            .collect();
        assert!(folder_files(
            many.iter().map(|(p, k, s)| (p, *k, *s)),
            "s",
            MAX_SKILL_FILES
        )
        .is_err());
    }

    #[test]
    fn the_output_folder_rides_inside_the_skills_folder() {
        let out = BTreeMap::from([("snapshots/2026-11-23.json".to_string(), "{}".to_string())]);
        let bytes = pack(&[("scan.py".into(), b"print(1)".to_vec())], &out).unwrap();
        let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(&bytes[..]));
        let paths: Vec<String> = ar
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(
            paths,
            [
                "skill/scan.py",
                "skill/.coderhelm-out/snapshots/2026-11-23.json",
                "skill/.coderhelm-out/"
            ]
        );
    }

    #[test]
    fn an_empty_answer_never_wipes_the_memory_folder() {
        assert!(whole(true, 0, 3, 1));
        assert!(whole(true, 0, 0, 0));
        assert!(!whole(true, 0, 0, 2), "an upload that failed");
        assert!(!whole(false, 0, 3, 1));
        assert!(!whole(true, 1, 3, 1));
    }

    #[test]
    fn the_chat_card_says_how_it_ended_and_links_the_run() {
        let m = chat_message(
            "Skill run: visit-count",
            &["Needs access: traffic_counts: AccessDenied".into()],
            "https://app.example.com/runs/detail?id=r1",
        );
        assert!(m.summary.contains("Needs access"));
        assert_eq!(
            m.buttons[0].url,
            "https://app.example.com/runs/detail?id=r1"
        );
    }
}
