//! The worker side of plugins: runs, publishing, source sync and its poll,
//! recurring triggers, results, and the review notifications.
//!
//! All the rules live in `common::plugins`; this file gives them the real
//! world: DynamoDB, the agent loop, the team's MCP servers, GitHub (through
//! the team's installation, or with no credentials for a public
//! repository), EventBridge Scheduler, SES and the team's chat channels.

use common::messages::{
    PluginPublishMessage, PluginResultsMessage, PluginRunMessage, PluginSyncMessage,
    PluginTriggerMessage,
};
use common::plugins::connect::{self, Connections, ToolDef};
use common::plugins::package::RepoRead;
use common::plugins::run::{Agent, ToolExec};
use common::plugins::source::{self, Access, Source};
use common::plugins::{drafts, flag, install, kv, limits, publish, run, schedule, signed};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::agent::llm::{ConverseOptions, ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;
use crate::WorkerState;

type Res = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub fn kv(state: &WorkerState) -> kv::Dynamo {
    kv::Dynamo {
        client: state.dynamo.clone(),
        table: state.config.settings_table_name.clone(),
    }
}

fn usage(state: &WorkerState) -> limits::Dynamo {
    limits::Dynamo {
        client: state.dynamo.clone(),
        analytics_table: state.config.analytics_table_name.clone(),
        settings_table: state.config.settings_table_name.clone(),
    }
}

fn scheduler(state: &WorkerState) -> schedule::EventBridge {
    schedule::EventBridge {
        client: state.scheduler.clone(),
        group: state.config.plugin_schedule_group.clone(),
        role_arn: state.config.plugin_scheduler_role_arn.clone(),
        queue_arn: state.config.ticket_queue_arn.clone(),
    }
}

fn queue(state: &WorkerState) -> schedule::Sqs {
    schedule::Sqs {
        client: state.sqs.clone(),
        url: state.config.ticket_queue_url.clone(),
    }
}

fn cipher(state: &WorkerState) -> common::mcp_credentials::Kms<'_> {
    // The worker only opens sealed values.
    common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: "",
    }
}

fn runs_table(state: &WorkerState) -> run::RunsTable {
    run::RunsTable {
        client: state.dynamo.clone(),
        table: state.config.runs_table_name.clone(),
    }
}

/// The team's own GitHub App installation.
pub async fn team_installation(state: &WorkerState, team: &str) -> Option<u64> {
    let out = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("META".into()),
        )
        .send()
        .await
        .ok()?;
    let item = out.item()?;
    item.get("github_install_id")
        .or_else(|| item.get("installation_id"))
        .and_then(|v| v.as_n().ok().or_else(|| v.as_s().ok()))
        .and_then(|n| n.parse().ok())
        .filter(|n: &u64| *n > 0)
}

// ── Reading sources ─────────────────────────────────────────────────────────

/// A private source, read through the team's installation.
struct InstallationRead {
    gh: GitHubClient,
    owner: String,
    repo: String,
}

#[async_trait::async_trait]
impl RepoRead for InstallationRead {
    async fn resolve(&self, git_ref: &str) -> Result<String, String> {
        self.gh
            .resolve_ref(&self.owner, &self.repo, git_ref)
            .await
            .map_err(|e| e.to_string())
    }
    async fn tree(&self, sha: &str) -> Result<Vec<(String, u64)>, String> {
        self.gh
            .list_files_with_sizes(&self.owner, &self.repo, sha)
            .await
            .map_err(|e| e.to_string())
    }
    async fn read(&self, sha: &str, path: &str) -> Result<String, String> {
        self.gh
            .read_file(&self.owner, &self.repo, path, sha)
            .await
            .map_err(|e| e.to_string())
    }
}

/// A public source, read with no credentials at all.
struct PublicRead {
    http: reqwest::Client,
    owner: String,
    repo: String,
}

impl PublicRead {
    async fn get(&self, path: &str) -> Result<Value, String> {
        let url = format!(
            "https://api.github.com/repos/{}/{}{path}",
            self.owner, self.repo
        );
        let r = self
            .http
            .get(&url)
            .header("Accept", "application/vnd.github+json")
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !r.status().is_success() {
            return Err(format!("GitHub answered {}", r.status()));
        }
        r.json().await.map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl RepoRead for PublicRead {
    async fn resolve(&self, git_ref: &str) -> Result<String, String> {
        let git_ref = git_ref.trim();
        let sha = |v: &Value, p: &str| {
            v.pointer(p)
                .and_then(|x| x.as_str())
                .map(str::to_string)
                .ok_or_else(|| "no sha".to_string())
        };
        if git_ref.is_empty() {
            let repo = self.get("").await?;
            let branch = repo["default_branch"]
                .as_str()
                .unwrap_or("main")
                .to_string();
            return sha(
                &self.get(&format!("/git/ref/heads/{branch}")).await?,
                "/object/sha",
            );
        }
        if let Some(tag) = git_ref.strip_prefix("tags/") {
            let r = self.get(&format!("/git/ref/tags/{tag}")).await?;
            let s = sha(&r, "/object/sha")?;
            if r.pointer("/object/type").and_then(|x| x.as_str()) == Some("tag") {
                return sha(&self.get(&format!("/git/tags/{s}")).await?, "/object/sha");
            }
            return Ok(s);
        }
        if (7..=40).contains(&git_ref.len()) && git_ref.chars().all(|c| c.is_ascii_hexdigit()) {
            return sha(&self.get(&format!("/commits/{git_ref}")).await?, "/sha");
        }
        sha(
            &self.get(&format!("/git/ref/heads/{git_ref}")).await?,
            "/object/sha",
        )
    }
    async fn tree(&self, sha: &str) -> Result<Vec<(String, u64)>, String> {
        let t = self.get(&format!("/git/trees/{sha}?recursive=1")).await?;
        Ok(t["tree"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|e| e["type"] == "blob")
                    .filter_map(|e| {
                        Some((
                            e["path"].as_str()?.to_string(),
                            e["size"].as_u64().unwrap_or(0),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default())
    }
    async fn read(&self, sha: &str, path: &str) -> Result<String, String> {
        let url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}?ref={sha}",
            self.owner,
            self.repo,
            path.split('/')
                .map(|p| urlencoding::encode(p).into_owned())
                .collect::<Vec<_>>()
                .join("/")
        );
        let r = self
            .http
            .get(&url)
            .header("Accept", "application/vnd.github.raw")
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !r.status().is_success() {
            return Err(format!("GitHub answered {}", r.status()));
        }
        r.text().await.map_err(|e| e.to_string())
    }
}

/// The reader for a source: public with no credentials, private through
/// the installation the source was added with (always the team's own; the
/// engine checks it against the team's installation before reading).
fn reader_for(state: &WorkerState, src: &Source) -> Result<Box<dyn RepoRead>, String> {
    let (owner, repo) = src.repo.split_once('/').ok_or("bad repository")?;
    Ok(match src.access {
        Access::Public => Box::new(PublicRead {
            http: state.http.clone(),
            owner: owner.into(),
            repo: repo.into(),
        }),
        Access::Installation => Box::new(InstallationRead {
            gh: GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                src.installation_id,
                &state.http,
            )
            .map_err(|e| e.to_string())?,
            owner: owner.into(),
            repo: repo.into(),
        }),
    })
}

/// Syncs a source and offers its new versions to the plugins installed
/// from it (or one plugin when `plugin_id` is set).
pub async fn sync(state: &WorkerState, msg: PluginSyncMessage) -> Res {
    let kv = kv(state);
    let team = msg.team_id.as_str();
    if !flag::enabled(&kv, team).await {
        return Ok(());
    }
    let inst = team_installation(state, team).await;
    let manual = msg.reason == "admin";
    let by = if manual { "admin" } else { "system" };
    let plugins = install::list(&kv, team).await.map_err(|e| e.error)?;
    let targets: Vec<&install::Installed> = plugins
        .iter()
        .filter(|p| {
            (msg.plugin_id.is_empty() || p.id == msg.plugin_id)
                && (msg.source_id.is_empty() || p.source_id == msg.source_id)
        })
        .collect();
    if !msg.source_id.is_empty() {
        let (src, _) = source::get(&kv, team, &msg.source_id)
            .await
            .map_err(|e| e.error)?;
        let reader = reader_for(state, &src)?;
        match source::sync(&kv, reader.as_ref(), team, inst, &src.id, msg.force).await {
            Ok(s) => info!(team, source = %s.id, plugins = s.plugins.len(), "Plugin source synced"),
            Err(e) => warn!(team, source = %src.id, error = %e.error, "Plugin source sync refused"),
        }
    }
    for p in targets {
        let Ok((src, _)) = source::get(&kv, team, &p.source_id).await else {
            continue;
        };
        let reader = reader_for(state, &src)?;
        match install::sync_plugin(&kv, reader.as_ref(), team, inst, &p.id, by, manual).await {
            Ok(offer) => {
                info!(team, plugin = %p.id, ?offer, "Plugin version offered");
                if offer == install::Offer::Applied {
                    refresh_crons(state, team, &p.id).await;
                }
            }
            Err(e) => warn!(team, plugin = %p.id, error = %e.error, "Plugin sync refused"),
        }
    }
    Ok(())
}

/// Makes a plugin's recurring schedules match its pinned version.
pub async fn refresh_crons(state: &WorkerState, team: &str, plugin: &str) {
    let kv = kv(state);
    let Ok((p, _)) = install::get(&kv, team, plugin).await else {
        return;
    };
    let m = install::current_snapshot(&kv, team, &p)
        .await
        .ok()
        .map(|s| s.manifest);
    install::sync_crons(&kv, &scheduler(state), team, &p, m.as_ref()).await;
}

/// The 15 minute poll: public sources every time, installation sources as
/// a backstop to their webhooks.
pub async fn sweep(state: &WorkerState) -> Res {
    let kv = kv(state);
    let now = chrono::Utc::now();
    for (team, id) in source::all(&kv).await? {
        if !flag::enabled(&kv, &team).await {
            continue;
        }
        let Ok((src, _)) = source::get(&kv, &team, &id).await else {
            continue;
        };
        if !source::due_for_poll(&src, now) {
            continue;
        }
        let msg = PluginSyncMessage {
            team_id: team.clone(),
            source_id: id.clone(),
            reason: "poll".into(),
            ..Default::default()
        };
        if let Err(e) = sync(state, msg).await {
            warn!(team, source = %id, error = %e, "Plugin poll: one source failed");
        }
    }
    Ok(())
}

// ── Connections ─────────────────────────────────────────────────────────────

/// The team's catalog MCP servers (the Connections page), through the MCP
/// proxy. Remote OAuth servers join through `common::mcp_remote` once it is
/// deployed.
struct CatalogConnections<'a> {
    state: &'a WorkerState,
}

impl CatalogConnections<'_> {
    async fn plugin(
        &self,
        team: &str,
        server: &str,
    ) -> Result<crate::agent::mcp::McpPlugin, String> {
        let table = if self.state.config.mcp_configs_table_name.is_empty() {
            &self.state.config.settings_table_name
        } else {
            &self.state.config.mcp_configs_table_name
        };
        crate::agent::mcp::load_team_plugins(&self.state.dynamo, &self.state.kms, table, team)
            .await
            .into_iter()
            .find(|p| p.server_id == server)
            .ok_or_else(|| format!("{server} isn't connected on the Connections page."))
    }
}

#[async_trait::async_trait]
impl Connections for CatalogConnections<'_> {
    async fn tools(&self, team: &str, server: &str) -> Result<Vec<ToolDef>, String> {
        let p = self.plugin(team, server).await?;
        let schemas = match crate::agent::mcp::load_tool_cache(
            &self.state.s3,
            &self.state.config.bucket_name,
            server,
        )
        .await
        {
            Some(c) => c.tools,
            None => crate::agent::mcp::list_tools(
                &self.state.lambda,
                &self.state.config.mcp_proxy_function_name,
                &p,
            )
            .await
            .map_err(|e| e.to_string())?,
        };
        Ok(schemas
            .into_iter()
            .map(|t| ToolDef {
                name: t.name,
                description: t.description,
                input_schema: t.input_schema,
            })
            .collect())
    }
    async fn call(
        &self,
        team: &str,
        server: &str,
        tool: &str,
        args: &Value,
    ) -> Result<String, String> {
        let p = self.plugin(team, server).await?;
        let v = crate::agent::mcp::call_tool(
            &self.state.lambda,
            &self.state.config.mcp_proxy_function_name,
            &p,
            tool,
            args,
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(match v {
            Value::String(s) => s,
            other => other.to_string(),
        })
    }
}

// ── The agent loop ──────────────────────────────────────────────────────────

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
            return Ok(json!("Stop now: a monthly token limit was reached."));
        }
        Ok(Value::String(self.exec.call(name, input).await))
    }
}

#[async_trait::async_trait]
impl Agent for WorkerAgent<'_> {
    async fn run(
        &self,
        team: &str,
        system: &str,
        user: &str,
        tools: &[ToolDef],
        exec: &dyn ToolExec,
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
                input_schema: if t.input_schema.is_object() {
                    t.input_schema.clone()
                } else {
                    json!({"type": "object"})
                },
            })
            .collect();
        let mut messages = vec![(
            "user".to_string(),
            vec![json!({"type": "text", "text": user})],
        )];
        let mut usage = crate::models::TokenUsage::default();
        let model = provider.primary_model_id().to_string();
        let text = crate::agent::provider::converse(
            self.state,
            &provider,
            common::agent_policy::Agent::Plugin,
            &model,
            system,
            &mut messages,
            &defs,
            &Adapter { exec },
            &mut usage,
            ConverseOptions {
                max_turns: run::MAX_CALLS + 2,
                max_tokens: 16_384,
                deadline: Some(std::time::Instant::now() + std::time::Duration::from_secs(12 * 60)),
                effort: Some("medium"),
            },
            None,
            None,
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok((text, usage.total_tokens(), usage.cost_usd))
    }
}

/// No knowledge bases until the knowledge work is deployed: plugins attach
/// bases as the agent `plugin:<id>` (`run::knowledge_agent`).
fn knowledge() -> run::NoKnowledge {
    run::NoKnowledge
}

/// One plugin run.
pub async fn run_plugin(state: &WorkerState, msg: PluginRunMessage) -> Res {
    let kv = kv(state);
    let usage = usage(state);
    let conns = CatalogConnections { state };
    let remote = connect::NoRemote;
    let http = connect::Https {
        http: &state.http,
        remote: &remote,
    };
    let cipher = cipher(state);
    let know = knowledge();
    let runs = runs_table(state);
    let agent = WorkerAgent { state };
    let deps = run::Deps {
        kv: &kv,
        usage: &usage,
        agent: &agent,
        conns: &conns,
        http: &http,
        cipher: &cipher,
        knowledge: &know,
        runs: &runs,
    };
    let rec = run::execute(&deps, &msg).await;
    info!(team = %msg.team_id, plugin = %msg.plugin_id, run = %msg.run_id, status = ?rec.status, drafts = rec.artifacts.len(), "Plugin run finished");
    for id in &rec.artifacts {
        notify_review(state, &msg.team_id, id).await;
    }
    Ok(())
}

/// A recurring schedule fired.
pub async fn trigger(state: &WorkerState, msg: PluginTriggerMessage) -> Res {
    let kv = kv(state);
    if !flag::enabled(&kv, &msg.team_id).await {
        return Ok(());
    }
    if let Err(e) = run::on_schedule(
        &kv,
        &queue(state),
        Some(&runs_table(state)),
        &usage(state),
        &msg.team_id,
        &msg.plugin_id,
        msg.trigger,
    )
    .await
    {
        warn!(team = %msg.team_id, plugin = %msg.plugin_id, error = %e.error, "Scheduled plugin run refused");
    }
    Ok(())
}

/// A release was written: start the plugins with a release trigger.
pub async fn on_release(state: &WorkerState, team: &str, repo: &str, tag: &str, notes: &str) {
    let kv = kv(state);
    for r in run::on_release(
        &kv,
        &queue(state),
        Some(&runs_table(state)),
        &usage(state),
        team,
        repo,
        tag,
        notes,
    )
    .await
    {
        if let Err(e) = r {
            warn!(team, repo, tag, error = %e.error, "Release plugin run refused");
        }
    }
}

fn publish_deps<'a>(
    kv: &'a kv::Dynamo,
    sched: &'a schedule::EventBridge,
    http: &'a connect::Https<'a>,
    conns: &'a CatalogConnections<'a>,
    cipher: &'a common::mcp_credentials::Kms<'a>,
) -> publish::Deps<'a> {
    publish::Deps {
        kv,
        sched,
        http,
        conns,
        cipher,
    }
}

/// Publishes one approved draft version.
pub async fn publish(state: &WorkerState, msg: PluginPublishMessage) -> Res {
    let kv = kv(state);
    let sched = scheduler(state);
    let remote = connect::NoRemote;
    let http = connect::Https {
        http: &state.http,
        remote: &remote,
    };
    let conns = CatalogConnections { state };
    let cipher = cipher(state);
    let out = publish::run(
        &publish_deps(&kv, &sched, &http, &conns, &cipher),
        &msg,
        chrono::Utc::now(),
    )
    .await;
    info!(team = %msg.team_id, artifact = %msg.artifact_id, outcome = ?out, "Plugin publish");
    Ok(())
}

/// Reads a published draft's results back as a lesson.
pub async fn results(state: &WorkerState, msg: PluginResultsMessage) -> Res {
    let kv = kv(state);
    let sched = scheduler(state);
    let remote = connect::NoRemote;
    let http = connect::Https {
        http: &state.http,
        remote: &remote,
    };
    let conns = CatalogConnections { state };
    let cipher = cipher(state);
    match publish::results(&publish_deps(&kv, &sched, &http, &conns, &cipher), &msg).await {
        Ok(l) => {
            info!(team = %msg.team_id, artifact = %msg.artifact_id, kept = l.is_some(), "Plugin results read")
        }
        Err(e) => {
            warn!(team = %msg.team_id, artifact = %msg.artifact_id, error = %e.error, "Plugin results refused")
        }
    }
    Ok(())
}

// ── Review notifications ────────────────────────────────────────────────────

/// A draft went to review: a card to the plugin's chat channels and a
/// signed approval email to each approver.
pub async fn notify_review(state: &WorkerState, team: &str, artifact: &str) {
    let kv = kv(state);
    let Ok((a, _)) = drafts::get(&kv, team, artifact).await else {
        return;
    };
    if a.status != drafts::Status::InReview {
        return;
    }
    let Ok((p, _)) = install::get(&kv, team, &a.plugin_id).await else {
        return;
    };
    let excerpt = drafts::version(&kv, team, &a.id, a.version)
        .await
        .map(|v| v.content)
        .unwrap_or_default();
    let base = common::plugins::chat::dashboard(&state.config.stage);
    if !p.notify.is_empty() {
        let item = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team.to_string()),
            )
            .key(
                "sk",
                aws_sdk_dynamodb::types::AttributeValue::S(
                    common::alert_notify::TEAM_SETTINGS_SK.into(),
                ),
            )
            .send()
            .await
            .ok()
            .and_then(|o| o.item().cloned())
            .unwrap_or_default();
        let mut channels = common::notify::store::open_all(&cipher(state), team, &item).await;
        if p.notify == "teams" {
            channels.slack = None;
        } else if p.notify == "slack" {
            channels.teams = None;
        }
        if !channels.is_empty() {
            let card = common::plugins::chat::review_card(&a, &excerpt, base);
            common::notify::deliver(
                &common::notify::Http(&state.http),
                &channels,
                &card,
                "plugin review",
            )
            .await;
        }
    }
    // A signed link for each named approver (never the requester).
    if state.secrets.jwt_secret.is_empty() || state.config.ses_from_address.is_empty() {
        return;
    }
    let Some(ap) = &a.approval else { return };
    let exp = chrono::Utc::now().timestamp() + signed::VALID_SECS;
    for to in p
        .approvers
        .members
        .iter()
        .filter(|m| !m.eq_ignore_ascii_case(&a.requested_by))
    {
        let token = signed::sign(
            &state.secrets.jwt_secret,
            &signed::ApprovalLink {
                team_id: team.to_string(),
                approval_id: ap.id.clone(),
                version: a.version,
                email: to.clone(),
            },
            exp,
        );
        let link = signed::url(&token);
        let body = format!(
            "A draft from the {} plugin is waiting for your approval.\n\n{}\n\n{}\n\nApprove or reject this exact version (version {}): {link}\nReview it in CoderHelm: {base}/plugins/draft/?id={}\n\nNothing is published until someone other than the requester approves it. This link works for three days and only for you.",
            a.plugin_name,
            a.title,
            common::truncate_str(&excerpt, 1200),
            a.version,
            a.id
        );
        let sent = state
            .ses
            .send_email()
            .from_email_address(&state.config.ses_from_address)
            .destination(
                aws_sdk_sesv2::types::Destination::builder()
                    .to_addresses(to)
                    .build(),
            )
            .content(
                aws_sdk_sesv2::types::EmailContent::builder()
                    .simple(
                        aws_sdk_sesv2::types::Message::builder()
                            .subject(
                                aws_sdk_sesv2::types::Content::builder()
                                    .data(format!("Approve: {}", a.title))
                                    .build()
                                    .unwrap_or_else(|_| unreachable_content()),
                            )
                            .body(
                                aws_sdk_sesv2::types::Body::builder()
                                    .text(
                                        aws_sdk_sesv2::types::Content::builder()
                                            .data(body)
                                            .build()
                                            .unwrap_or_else(|_| unreachable_content()),
                                    )
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        if let Err(e) = sent {
            warn!(team, to = %to, error = %e, "Couldn't send a plugin approval email");
        }
    }
}

fn unreachable_content() -> aws_sdk_sesv2::types::Content {
    aws_sdk_sesv2::types::Content::builder()
        .data(" ")
        .build()
        .expect("content with data")
}
