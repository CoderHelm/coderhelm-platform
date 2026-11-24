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
use common::skill_runs::engine::{self, Deps, RunRow, ScriptResult};
use common::skill_runs::Budget;
use common::skill_runs::SkillRunMessage;
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
    if out.len() > MAX_SKILL_FILES {
        return Err(format!(
            "the skill's folder holds {} files; a run brings at most {MAX_SKILL_FILES}",
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
        let mut set = vec![
            ("status", A::S(r.status.clone())),
            ("status_run_id", A::S(format!("{}#{}", r.status, r.run_id))),
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
        ];
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
        if self.exec.stop() {
            return Ok(json!(
                "Stop now: the run needs access or its time is used. End without a report."
            ));
        }
        Ok(Value::String(self.exec.call(name, input).await))
    }
}

#[async_trait::async_trait]
impl engine::Agent for WorkerAgent<'_> {
    async fn run(
        &self,
        team: &str,
        system: &str,
        user: &str,
        tools: &[ToolDef],
        exec: &dyn ToolExec,
        budget: &Budget,
    ) -> Result<(String, u64, f64), String> {
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
        let mut messages = vec![(
            "user".to_string(),
            vec![json!({"type": "text", "text": user})],
        )];
        let mut usage = crate::models::TokenUsage::default();
        let model = provider.primary_model_id().to_string();
        let deadline =
            Instant::now() + std::time::Duration::from_secs(budget.max_minutes as u64 * 60);
        let allowance = Arc::new(common::usage::RunAllowance::new(budget.max_usd));
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
                        max_turns: 120,
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
        match r {
            Ok(text) => Ok((text, usage.total_tokens(), usage.cost_usd)),
            Err(e) => Err(e.to_string()),
        }
    }
}

// ── The pass ────────────────────────────────────────────────────────────────

/// One skill run: a schedule's fire or Run now.
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
    let sandbox = CodeSandbox {
        client: SandboxClient::new(
            &state.codebuild,
            &state.s3,
            &state.logs,
            &state.config.sandbox_bucket_name,
            &state.config.sandbox_project_name,
        )
        .with_egress(crate::agent::policy::sandbox_egress(state, &msg.team_id).await),
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
        app_url: common::public_urls::app_url(&state.config.stage),
    };
    match engine::execute(&deps, &msg).await {
        Some(e) => info!(
            team = %msg.team_id,
            schedule = %msg.schedule_id,
            run = %e.run_id,
            status = %e.status,
            outputs = e.outputs.len(),
            "Skill run finished"
        ),
        None => {
            info!(team = %msg.team_id, schedule = %msg.schedule_id, trigger = %msg.trigger, "Skill run skipped")
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
        let got = folder_files(tree.iter().map(|(p, k, s)| (p, *k, *s)), "skills/scan").unwrap();
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
        assert!(folder_files(many.iter().map(|(p, k, s)| (p, *k, *s)), "s").is_err());
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
