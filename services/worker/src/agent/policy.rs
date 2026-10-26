//! The worker's side of agent permissions: the gate every tool loop asks,
//! where approval cards are announced, and how a loop that has to stop
//! (agents paused, or an approval nobody answered) ends its run.
//!
//! Every model loop runs through [`super::anthropic::converse_tool_loop`],
//! which takes a [`Gate`] and asks it before each call; [`super::provider::converse`]
//! builds that gate from the provider's [`PolicyScope`] and the agent named
//! at the call site. A test in this module reads the worker's sources and
//! fails when a tool executor is called anywhere else.

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::agent_policy::approvals::Approval;
use common::agent_policy::gate::{Notifier, Wait};
use common::agent_policy::links::Channel;
use common::agent_policy::store::{DynamoStore, PolicyStore};
use common::agent_policy::{Agent, Gate, GateCtx, Stop};

use crate::WorkerState;

/// What a model provider is working on: the team, and the run, repository
/// and person when known. Set where the run starts; every loop on that
/// provider is gated with it.
#[derive(Debug, Clone, Default)]
pub struct PolicyScope {
    pub team: String,
    pub run_id: String,
    pub repo: String,
    pub person: String,
    pub task: String,
    /// Work that isn't a run (a pull request review): what its approvals
    /// and decisions are kept under.
    pub thread: String,
    /// The agent a run as a whole belongs to when it isn't the default for
    /// each pass: a fix run started by an alert is the alert fixer's, one
    /// started by a log finding is log auto-fix's.
    pub origin: Option<Agent>,
}

impl PolicyScope {
    /// The agent a call site names, or the run's own when it has one.
    pub fn agent(&self, at_call_site: Agent) -> Agent {
        match (self.origin, at_call_site) {
            // The reviewer stays the reviewer whoever started the run.
            (_, Agent::Reviewer) => Agent::Reviewer,
            (Some(o), Agent::Planner | Agent::Coder) => o,
            _ => at_call_site,
        }
    }

    pub fn ctx(&self, agent: Agent) -> GateCtx {
        GateCtx::new(&self.team, self.agent(agent))
            .repo(&self.repo)
            .run(&self.run_id)
            .person(&self.person)
            .task(&self.task)
            .thread(&self.thread)
    }
}

/// The settings-table store.
pub fn store(state: &WorkerState) -> Arc<dyn PolicyStore> {
    Arc::new(DynamoStore::new(
        state.dynamo.clone(),
        state.config.settings_table_name.clone(),
    ))
}

/// The sandbox's network allowlist for a team: the list when its network is
/// limited, None when the team opened it. When the policy can't be read the
/// default list applies (limited).
pub async fn sandbox_egress(state: &WorkerState, team: &str) -> Option<Vec<String>> {
    match store(state).load(team).await {
        Ok(p) => p.sandbox.limit_egress.then(|| p.sandbox.egress.clone()),
        Err(_) => Some(common::agent_policy::model::default_egress()),
    }
}

/// How long a run waits for an answer: until two minutes before its
/// deadline (enough to wrap up), at least 30 seconds, at most 12 minutes.
pub fn wait_budget(deadline: Option<Instant>) -> Duration {
    let max = Duration::from_secs(12 * 60);
    match deadline {
        Some(d) => d
            .saturating_duration_since(Instant::now())
            .saturating_sub(Duration::from_secs(120))
            .clamp(Duration::from_secs(30), max),
        None => max,
    }
}

/// The gate for one loop.
pub fn gate(state: &WorkerState, ctx: GateCtx, deadline: Option<Instant>) -> Gate {
    let notifier = ChannelNotifier {
        dynamo: state.dynamo.clone(),
        kms: state.kms.clone(),
        http: state.http.clone(),
        table: state.config.settings_table_name.clone(),
        secret: state.secrets.jwt_secret.clone(),
    };
    Gate::new(store(state), ctx)
        .with_notifier(Arc::new(notifier))
        .with_wait(Wait::Block {
            max: wait_budget(deadline),
            poll: Duration::from_secs(4),
        })
}

/// Announces approval cards on the team's notification channels (the same
/// Slack and Teams channels its alerts use), with signed Approve and Deny
/// links. Nothing is sent when the team has none set up; the card is still
/// on the run page and in the approvals queue.
pub struct ChannelNotifier {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub kms: aws_sdk_kms::Client,
    pub http: reqwest::Client,
    pub table: String,
    pub secret: String,
}

impl ChannelNotifier {
    /// The team's notification channels, opened, when notifications are on.
    async fn channels(&self, team: &str) -> Option<common::notify::Channels> {
        let item = match self
            .dynamo
            .get_item()
            .table_name(&self.table)
            .key(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team.to_string()),
            )
            .key(
                "sk",
                aws_sdk_dynamodb::types::AttributeValue::S(
                    common::alert_notify::TEAM_SETTINGS_SK.to_string(),
                ),
            )
            .send()
            .await
        {
            Ok(r) => r.item().cloned().unwrap_or_default(),
            Err(e) => {
                tracing::warn!(team, error = %e, "Couldn't read notification channels");
                return None;
            }
        };
        let enabled = item
            .get("enabled")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false);
        if !enabled {
            return None;
        }
        let cipher = common::mcp_credentials::Kms {
            client: &self.kms,
            key_id: "",
        };
        let channels = common::notify::store::open_all(&cipher, team, &item)
            .await
            .usable();
        (!channels.is_empty()).then_some(channels)
    }

    /// Posts one message to the team's channels.
    pub async fn post(&self, team: &str, msg: &common::notify::Message) {
        if let Some(channels) = self.channels(team).await {
            let poster = common::notify::Http(&self.http);
            common::notify::deliver(&poster, &channels, msg, "agent notice").await;
        }
    }
}

#[async_trait::async_trait]
impl Notifier for ChannelNotifier {
    async fn approval_needed(&self, team: &str, a: &Approval) {
        let Some(channels) = self.channels(team).await else {
            return;
        };
        let poster = common::notify::Http(&self.http);
        // Each channel's buttons are signed for that channel.
        for kind in common::notify::ChannelKind::ALL {
            let Some(url) = channels.get(kind) else {
                continue;
            };
            let channel = match kind {
                common::notify::ChannelKind::Slack => Channel::Slack,
                common::notify::ChannelKind::Teams => Channel::Teams,
            };
            let msg =
                common::agent_policy::notify::approval_message(&self.secret, team, a, channel);
            let one = common::notify::Channels {
                teams: (kind == common::notify::ChannelKind::Teams).then(|| url.to_string()),
                slack: (kind == common::notify::ChannelKind::Slack).then(|| url.to_string()),
            };
            common::notify::deliver(&poster, &one, &msg, "agent approval").await;
        }
    }
}

/// What happened to one tool call.
pub enum Gated {
    /// The gate allowed it (or a person approved it) and it ran.
    Ran(Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>>),
    /// Not run; the model reads this instead of a result.
    Refused(String),
    /// The loop must end now.
    Stop(Stop),
}

/// Asks the gate, then runs the tool only if it may. The one place a tool
/// executor is called from a model loop.
pub async fn gated_execute(
    gate: &Gate,
    executor: &dyn super::llm::ToolExecutor,
    name: &str,
    input: &serde_json::Value,
    why: &str,
) -> Gated {
    use common::agent_policy::{Outcome, Request};
    match gate
        .check(Request {
            tool: name,
            args: input,
            why,
        })
        .await
    {
        Outcome::Run { .. } => Gated::Ran(executor.execute(name, input).await),
        Outcome::Refused { message, .. } | Outcome::Pending { message, .. } => {
            Gated::Refused(message)
        }
        Outcome::Stop(stop) => {
            record_stop(&stop);
            Gated::Stop(stop)
        }
    }
}

/// Sign-in prompts for a run: each connection with tools the run could use
/// that the person it works for hasn't connected (or the team hasn't set
/// up), once per connection per run. Each is kept on the run (the run page
/// shows it) and posted to the team's Slack and Teams channels with a link.
/// The run carries on without it.
pub async fn sign_in_prompts_for_run(state: &WorkerState, team: &str, run_id: &str, person: &str) {
    use common::agent_policy::signin::{self, Need};
    use common::mcp_remote::store::{AuthKind, Sharing, Status};
    if run_id.is_empty() {
        return;
    }
    let table = if state.config.mcp_configs_table_name.is_empty() {
        &state.config.settings_table_name
    } else {
        &state.config.mcp_configs_table_name
    };
    let rt = common::mcp_remote::runtime::Runtime::aws(
        state.dynamo.clone(),
        table,
        state.kms.clone(),
        &state.config.secrets_key_id,
        team,
    );
    let user = (!person.is_empty()).then_some(person);
    let usable: Vec<String> = rt
        .usable(user)
        .await
        .into_iter()
        .map(|(s, _)| s.id)
        .collect();
    let servers = rt.store.list_servers(team).await.unwrap_or_default();
    let st = store(state);
    let thread = format!("run:{run_id}");
    let mut prompts = Vec::new();
    for s in servers {
        if common::mcp_remote::tools::effective(&s).is_empty() || usable.contains(&s.id) {
            continue;
        }
        let need = match (s.sharing, s.auth, s.status) {
            // Nobody known to sign in for: a personal server can't be asked about.
            (Sharing::Personal, _, _) if user.is_none() => continue,
            (Sharing::Personal, AuthKind::OAuth, _) => Need::Personal,
            (Sharing::Shared, _, Status::Connected) => continue,
            (Sharing::Shared, _, _) => Need::TeamMissing,
            _ => continue,
        };
        if !signin::should_prompt(&*st, team, &thread, &s.id).await {
            continue;
        }
        prompts.push(signin::prompt(
            &s.id,
            &s.name,
            need,
            false,
            "use it in this run",
        ));
    }
    if prompts.is_empty() {
        return;
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team.to_string()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(run_id.to_string()),
        )
        .update_expression("SET sign_in_prompts = :p")
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_values(
            ":p",
            aws_sdk_dynamodb::types::AttributeValue::S(
                serde_json::to_string(&prompts).unwrap_or_default(),
            ),
        )
        .send()
        .await;
    let notifier = ChannelNotifier {
        dynamo: state.dynamo.clone(),
        kms: state.kms.clone(),
        http: state.http.clone(),
        table: state.config.settings_table_name.clone(),
        secret: state.secrets.jwt_secret.clone(),
    };
    for p in &prompts {
        notifier
            .post(
                team,
                &common::agent_policy::notify::sign_in_message(p, run_id),
            )
            .await;
    }
}

/// What a loop left for its message handler.
#[derive(Debug, Default)]
struct StopSlot {
    stop: Option<Stop>,
    /// The run the stop ended, once its failure path claimed it.
    run_id: String,
}

tokio::task_local! {
    /// Set while one worker message is handled: the stop a loop hit, if any,
    /// so the run ends with its note instead of as a failure.
    static STOPPED: Arc<std::sync::Mutex<StopSlot>>;
}

/// Runs `fut` with a place for a loop to leave its stop. Returns the stop
/// and the run it ended (empty when no run claimed it).
pub async fn watch_stops<F: std::future::Future>(fut: F) -> (F::Output, Option<(Stop, String)>) {
    let slot = Arc::new(std::sync::Mutex::new(StopSlot::default()));
    let out = STOPPED.scope(slot.clone(), fut).await;
    let left = slot
        .lock()
        .ok()
        .and_then(|mut s| s.stop.take().map(|st| (st, std::mem::take(&mut s.run_id))));
    (out, left)
}

/// A loop stopped: leave the stop for the message handler.
pub fn record_stop(stop: &Stop) {
    let _ = STOPPED.try_with(|s| {
        if let Ok(mut s) = s.lock() {
            s.stop.get_or_insert_with(|| stop.clone());
        }
    });
}

/// The stop a loop left in this message, if any.
#[cfg(test)]
pub fn current_stop() -> Option<Stop> {
    STOPPED
        .try_with(|s| s.lock().ok().and_then(|s| s.stop.clone()))
        .ok()
        .flatten()
}

/// Called where a run would be marked failed: when a loop stopped instead,
/// returns the stop (and remembers the run) so the run ends with its note.
pub fn claim_stop(run_id: &str) -> Option<Stop> {
    STOPPED
        .try_with(|s| {
            let mut s = s.lock().ok()?;
            let stop = s.stop.clone()?;
            if s.run_id.is_empty() {
                s.run_id = run_id.to_string();
            }
            Some(stop)
        })
        .ok()
        .flatten()
}

/// Ends a run a loop stopped: queued (agents paused; it resumes when the
/// pause ends) or waiting for input (an approval nobody answered), with the
/// note on the run. Never overwrites a run a person cancelled or closed.
pub async fn end_run_for_stop(state: &WorkerState, team_id: &str, run_id: &str, stop: &Stop) {
    use aws_sdk_dynamodb::types::AttributeValue;
    use common::run_status::EndedValues;
    let status = match stop {
        Stop::Paused { .. } => common::run_status::QUEUED,
        Stop::TimedOut { .. } => common::run_status::NEEDS_INPUT,
    };
    let approval_id = match stop {
        Stop::TimedOut { approval } => approval.id.clone(),
        Stop::Paused { .. } => String::new(),
    };
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, policy_note = :n, policy_approval = :a, updated_at = :t",
        )
        .condition_expression(format!("attribute_exists(run_id) AND {}", common::run_status::not_ended("#s")))
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", AttributeValue::S(status.to_string()))
        .expression_attribute_values(":sri", AttributeValue::S(format!("{status}#{run_id}")))
        .expression_attribute_values(":n", AttributeValue::S(stop.note()))
        .expression_attribute_values(":a", AttributeValue::S(approval_id))
        .expression_attribute_values(":t", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
        .ended_values()
        .send()
        .await;
    match r {
        Ok(_) => {
            crate::passes::webhooks::emit_run(state, team_id, run_id, status).await;
        }
        Err(e) => tracing::warn!(run_id, error = %e, "Couldn't record why the run stopped"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_agents_take_over_planning_and_coding_but_not_review() {
        let s = PolicyScope {
            team: "TEAM#a".into(),
            origin: Some(Agent::AlertFixer),
            ..Default::default()
        };
        assert_eq!(s.agent(Agent::Coder), Agent::AlertFixer);
        assert_eq!(s.agent(Agent::Planner), Agent::AlertFixer);
        assert_eq!(s.agent(Agent::Reviewer), Agent::Reviewer);
        let plain = PolicyScope::default();
        assert_eq!(plain.agent(Agent::Coder), Agent::Coder);
    }

    #[test]
    fn the_wait_leaves_time_to_wrap_up() {
        let soon = Instant::now() + Duration::from_secs(200);
        let b = wait_budget(Some(soon));
        assert!(b <= Duration::from_secs(80) && b >= Duration::from_secs(30));
        assert_eq!(wait_budget(None), Duration::from_secs(720));
        let past = Instant::now();
        assert_eq!(wait_budget(Some(past)), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn a_stop_is_seen_by_the_message_handler() {
        let stop = Stop::Paused {
            reason: "freeze".into(),
        };
        let s2 = stop.clone();
        let ((), got) = watch_stops(async move {
            record_stop(&s2);
            assert!(current_stop().unwrap().note().contains("freeze"));
            assert!(claim_stop("run-9").is_some());
        })
        .await;
        assert_eq!(got, Some((stop, "run-9".to_string())));
        // Outside a watched message nothing is kept and nothing panics.
        record_stop(&Stop::Paused {
            reason: String::new(),
        });
        assert!(current_stop().is_none());
    }

    /// The bypass test: every call to a tool executor goes through the gated
    /// loop. A `.execute(` anywhere else in the worker fails this test unless
    /// it is one executor handing a call to another inside its own
    /// `execute` (already gated by the loop that called it), a test, or one
    /// of the reviewed exceptions below.
    #[test]
    fn every_tool_call_goes_through_the_gate() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        collect(&root, &mut files);
        // file, and why its call is gated elsewhere.
        const REVIEWED: &[(&str, &str)] = &[
            (
                "passes/plan_chat.rs",
                "the plan chat's tool calls are gated in the gateway before it invokes the worker",
            ),
            (
                "passes/agent_tools.rs",
                "an asked team agent's executor hands its calls to the parent executor; the asked \
                 agent's own loop is gated, and so was the ask_team_agent call",
            ),
            (
                "agent/policy.rs",
                "gated_execute: the executor runs only on Outcome::Run",
            ),
        ];
        let mut bad = Vec::new();
        for f in &files {
            let rel = f
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let src = std::fs::read_to_string(f).unwrap();
            let code = src.split("#[cfg(test)]").next().unwrap_or(&src);
            for (i, line) in code.lines().enumerate() {
                let t = line.trim();
                if t.starts_with("//") {
                    continue;
                }
                let calls_executor = t.contains(".execute(name")
                    || t.contains(".execute(tool_name")
                    || t.contains("executor.execute(")
                    || (t.starts_with(".execute(") && !t.contains("sql"));
                if !calls_executor {
                    continue;
                }
                if rel == "agent/anthropic.rs" {
                    // The loop itself: must be the gated call.
                    if !code.contains("gated_execute(")
                        || t.contains("tool_executor.execute(tool_name")
                    {
                        bad.push(format!(
                            "{rel}:{} calls the executor without the gate",
                            i + 1
                        ));
                    }
                    continue;
                }
                if REVIEWED.iter().any(|(r, _)| *r == rel) {
                    continue;
                }
                // Inside an `impl ToolExecutor`'s own execute: a delegate.
                let before = &code[..code.lines().take(i).map(|l| l.len() + 1).sum::<usize>()];
                let last_impl = before.rfind("impl");
                let in_executor = last_impl
                    .map(|at| {
                        before[at..]
                            .lines()
                            .next()
                            .unwrap_or("")
                            .contains("ToolExecutor for")
                    })
                    .unwrap_or(false);
                if !in_executor {
                    bad.push(format!("{rel}:{}: {t}", i + 1));
                }
            }
        }
        assert!(
            bad.is_empty(),
            "tool calls that skip the policy gate:\n{}",
            bad.join("\n")
        );

        // And the loop asks the gate before it runs a tool.
        let anthropic = std::fs::read_to_string(root.join("agent/anthropic.rs")).unwrap();
        let body = anthropic
            .split("pub async fn converse_tool_loop(")
            .nth(1)
            .unwrap();
        let check = body
            .find("gated_execute(")
            .expect("the loop calls gated_execute");
        assert!(!body[..check].contains("tool_executor.execute("));
    }

    struct Counting(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl super::super::llm::ToolExecutor for Counting {
        async fn execute(
            &self,
            _name: &str,
            _input: &serde_json::Value,
        ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(serde_json::json!("done"))
        }
    }

    /// A denied call never reaches the executor; an allowed one does; a
    /// pause stops the loop.
    #[tokio::test]
    async fn the_executor_runs_only_when_the_gate_allows() {
        use common::agent_policy::model::{Decision, Rule, RuleSource, Scope, TeamPolicy};
        use common::agent_policy::store::MemStore;
        let store = Arc::new(MemStore::new());
        let mut p = TeamPolicy::starting();
        p.version = 1;
        p.rules.push(Rule {
            id: "no-writes-here".into(),
            scope: Scope::Repo,
            repo: Some("acme/web".into()),
            agent: None,
            kind: None,
            server: None,
            tool: Some("write_file".into()),
            risk: None,
            command: None,
            decision: Decision::Deny,
            locked: false,
            note: String::new(),
            source: RuleSource::Manual,
            created_by: String::new(),
            created_at: String::new(),
        });
        store.save("TEAM#a", &p, 0).await.unwrap();
        let ctx = PolicyScope {
            team: "TEAM#a".into(),
            repo: "acme/web".into(),
            run_id: "r1".into(),
            ..Default::default()
        }
        .ctx(Agent::Coder);
        let gate = Gate::new(store.clone(), ctx).offering(["write_file", "read_file"]);
        let ex = Counting(Default::default());
        let input = serde_json::json!({"path": "a"});
        match gated_execute(&gate, &ex, "write_file", &input, "").await {
            Gated::Refused(m) => assert!(m.starts_with("Not allowed")),
            _ => panic!("expected refused"),
        }
        assert_eq!(ex.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(matches!(
            gated_execute(&gate, &ex, "read_file", &input, "").await,
            Gated::Ran(Ok(_))
        ));
        assert_eq!(ex.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        // A tool the model wasn't offered is unknown: denied.
        assert!(matches!(
            gated_execute(&gate, &ex, "made_up", &input, "").await,
            Gated::Refused(_)
        ));

        common::agent_policy::pause::pause(&*store, "TEAM#a", "a@x.dev", "freeze", None)
            .await
            .unwrap();
        let gate = Gate::new(
            store.clone(),
            PolicyScope {
                team: "TEAM#a".into(),
                ..Default::default()
            }
            .ctx(Agent::Planner),
        );
        let (out, stop) = watch_stops(async {
            matches!(
                gated_execute(&gate, &ex, "read_file", &input, "").await,
                Gated::Stop(_)
            )
        })
        .await;
        assert!(out);
        assert!(stop.is_some());
        assert_eq!(ex.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    fn collect(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                collect(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
}
