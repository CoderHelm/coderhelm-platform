//! Does the outside steps CoderHelm owns, after the pull request opens, and
//! carries them across invocations until each is done.
//!
//! One generic executor for every step kind (see `outside_work::StepKind`):
//!
//! 1. the step's **action** comes from its kind and target
//!    ([`outside_work::action_for`]): start a workflow (dispatch, or the
//!    label it runs on), write content through a write connection, run the
//!    confirm query;
//! 2. every action that changes something goes through the team's **agent
//!    permissions** ([`gate`]): allowed calls run, an ask raises the
//!    approval card and the step waits on it (`waiting_approval`), a denial
//!    is reported as not done with the rule to change. Production steps take
//!    the same path; needing approval never hands a step to a person;
//! 3. a started workflow is **waited for and verified** (`running`): its
//!    conclusion and failing tests are read back and reported. A failed
//!    end-to-end run goes to the run's CI fix round ([`FailedRun`]);
//! 4. a step that outlasts this invocation stays `running` or
//!    `waiting_approval` on the run's decision, and the CI resume carries it
//!    on ([`on_resume`]) while the run waits in `awaiting_ci`. Later steps
//!    wait for it: steps run in their order (the deploy before the content
//!    its fields need).
//!
//! A step reaches a person only when no connection or permission exists to
//! do it at all (a production CMS write, for example: CoderHelm's CMS
//! connection is staging only). It is reported as not done, with the access
//! to grant, at the top of the pull request and the ticket note.

use std::time::{Duration, Instant};

use common::agent_policy::{Agent, GateCtx, Outcome, Request, Wait};
use serde_json::{json, Value};
use tracing::{info, warn};

use super::fix_location::{FixDecision, OutsideChange};
use super::outside_work::{
    self, Abilities, Action, Doer, NoteStep, Placement, Route, StepKind, StepRun, Target,
};
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// The agent-permissions tool for starting a workflow (a dispatch, or the
/// label a workflow runs on).
pub const START_WORKFLOW: &str = "start_workflow";
/// The agent-permissions tool for writing CMS content or running a
/// migration's mutations.
pub const WRITE_CONTENT: &str = "write_content";

/// How often a started workflow is looked at.
const POLL: Duration = Duration::from_secs(15);
/// Tries to find the run a dispatch or label started.
const FIND_TRIES: u32 = 6;
/// Failing tests named in a report.
const MAX_FAILING: usize = 20;

pub const RUNNING: &str = "running";
pub const WAITING_APPROVAL: &str = "waiting_approval";
pub const DONE: &str = "done";
pub const FAILED: &str = "failed";

/// Where the steps run.
pub struct Ctx<'a> {
    pub state: &'a WorkerState,
    pub github: &'a GitHubClient,
    pub team_id: &'a str,
    pub run_id: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    pub branch: &'a str,
    pub pr_number: u64,
}

impl Ctx<'_> {
    fn full(&self) -> String {
        format!("{}/{}", self.owner, self.repo)
    }
}

/// A started end-to-end run that failed: the run's CI fix round takes it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FailedRun {
    pub workflow_run_id: u64,
    pub workflow: String,
    pub url: String,
    pub head_sha: String,
    pub failing: Vec<String>,
}

/// What one pass of the executor did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    /// A step's state changed (save and report it).
    pub changed: bool,
    /// A step waits on an approval or a workflow run.
    pub waiting: bool,
    /// A started end-to-end run failed.
    pub failed_e2e: Option<FailedRun>,
}

// ── The gate ────────────────────────────────────────────────────────────────

/// What the team's agent permissions said about one action.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Gated {
    Run,
    /// An approval card is waiting (its id), or agents are paused.
    Waiting(String, String),
    Refused(String),
}

/// The arguments the gate sees (and an approval card shows) for a step's
/// action. Stable across invocations, so a person's "approve once" is found
/// again when the resume asks the same thing.
pub fn gate_args(
    s: &NoteStep,
    action: &Action,
    repo: &str,
    branch: &str,
) -> Option<(&'static str, Value)> {
    let base = |extra: Value| {
        let mut v = json!({
            "repo": repo,
            "ref": branch,
            "target": s.target().as_str(),
            "step": one_line(&s.step),
        });
        if let (Some(o), Some(e)) = (v.as_object_mut(), extra.as_object()) {
            for (k, x) in e {
                o.insert(k.clone(), x.clone());
            }
        }
        v
    };
    match action {
        Action::Dispatch(r) => Some((START_WORKFLOW, base(json!({ "workflow": r.file })))),
        Action::Label(l, r) => Some((
            START_WORKFLOW,
            base(json!({ "workflow": r.file, "label": l })),
        )),
        Action::Write(_) => Some((
            WRITE_CONTENT,
            base(json!({
                "mutations": common::content_hash(&serde_json::to_string(&s.mutations).unwrap_or_default()),
                "migration": s.migration,
            })),
        )),
        Action::Query | Action::Cannot(_) => None,
    }
}

async fn gate(cx: &Ctx<'_>, tool: &str, args: &Value, why: &str) -> Gated {
    let ctx = GateCtx::new(cx.team_id, Agent::Coder)
        .repo(&cx.full())
        .run(cx.run_id);
    // Never block the invocation on a card: the step waits on it, and the
    // resume asks again (an approval is found as a grant then).
    let g = crate::agent::policy::gate(cx.state, ctx, None).with_wait(Wait::Return);
    match g.check(Request { tool, args, why }).await {
        Outcome::Run { .. } => Gated::Run,
        Outcome::Pending { approval, .. } => {
            Gated::Waiting(approval.id.clone(), approval.headline())
        }
        Outcome::Refused { message, .. } => Gated::Refused(message),
        Outcome::Stop(s) => Gated::Waiting(String::new(), s.note()),
    }
}

// ── The executor ────────────────────────────────────────────────────────────

/// Does the CoderHelm steps in order, until one waits (an approval, a
/// workflow still running) or `until`. Records each step's state and result
/// on it. Steps that ended earlier (done, or reported not done) are left.
pub async fn execute(
    cx: &Ctx<'_>,
    a: &Abilities,
    c: &mut OutsideChange,
    until: Instant,
) -> Progress {
    let mut p = Progress::default();
    c.placed = Some(Placement {
        repo: cx.full(),
        branch: cx.branch.to_string(),
        pr_number: cx.pr_number,
    });
    let snapshot = c.clone();
    for i in 0..c.steps.len() {
        let s = &mut c.steps[i];
        if s.doer() != Doer::CoderHelm || s.done {
            continue;
        }
        if !s.result.trim().is_empty() && !outside_work::under_way(s) {
            // Ended without being done: reported; a later step that needs
            // it doesn't run (below).
            if blocks_later(s) {
                break;
            }
            continue;
        }
        p.changed = true;
        let action = outside_work::action_for(s, a, &snapshot);
        let running = s.run.as_ref().is_some_and(|r| r.state == RUNNING);
        if running {
            if let Some(Action::Dispatch(r) | Action::Label(_, r)) = Some(action.clone()) {
                poll(cx, s, &r, until, &mut p).await;
            } else {
                poll(cx, s, &unknown_route(s), until, &mut p).await;
            }
        } else {
            act(cx, a, &snapshot, s, action, until, &mut p).await;
        }
        if outside_work::under_way(s) {
            p.waiting = true;
            break;
        }
        if !s.done && blocks_later(s) {
            break;
        }
    }
    if !p.waiting && confirm(cx, a, c).await {
        p.changed = true;
    }
    p
}

/// Whether later steps wait for this one when it isn't done: a deploy (the
/// content after it needs its fields) or a content change (the confirm and
/// the tests read it).
fn blocks_later(s: &NoteStep) -> bool {
    matches!(
        s.step_kind(),
        StepKind::Deploy | StepKind::Content | StepKind::Migration
    )
}

fn unknown_route(s: &NoteStep) -> Route {
    let file = s
        .run
        .as_ref()
        .map(|r| r.workflow.clone())
        .unwrap_or_default();
    Route {
        name: file.rsplit('/').next().unwrap_or(&file).to_string(),
        file,
        command: String::new(),
        trigger: outside_work::Trigger::Dispatch,
    }
}

async fn act(
    cx: &Ctx<'_>,
    a: &Abilities,
    c: &OutsideChange,
    s: &mut NoteStep,
    action: Action,
    until: Instant,
    p: &mut Progress,
) {
    match &action {
        Action::Cannot(why) => {
            s.run = None;
            s.result = format!("not done: {why}");
            return;
        }
        Action::Query => {
            query_step(cx, a, c, s).await;
            return;
        }
        Action::Write(Target::Production) => {
            s.run = None;
            s.result = format!("not done: {}", production_write_gap());
            return;
        }
        _ => {}
    }
    // What a write runs is read before asking, so a card shows real work
    // and a step with nothing to run is reported, not approved.
    let mut mutations = Vec::new();
    let mut conn = None;
    if let Action::Write(Target::Staging) = &action {
        match staging_connection(cx).await {
            Ok(x) => conn = Some(x),
            Err(why) => {
                s.run = None;
                s.result = format!("not done: {why}");
                return;
            }
        }
        let file = if s.mutations.is_empty() && outside_work::is_mutations_file(&s.migration) {
            cx.github
                .read_file(cx.owner, cx.repo, s.migration.trim(), cx.branch)
                .await
                .ok()
        } else {
            None
        };
        match outside_work::mutations_of(s, file.as_deref()) {
            Ok(m) => mutations = m,
            Err(why) => {
                s.run = None;
                s.result = format!("not done: {why}");
                return;
            }
        }
    }
    let Some((tool, args)) = gate_args(s, &action, &cx.full(), cx.branch) else {
        return;
    };
    match gate(cx, tool, &args, &one_line(&s.step)).await {
        Gated::Run => {}
        Gated::Waiting(id, headline) => {
            s.run = Some(StepRun {
                state: WAITING_APPROVAL.to_string(),
                approval_id: id,
                ..Default::default()
            });
            s.result = format!(
                "waiting for approval on the Agent permissions page ({}); CoderHelm does it once it is approved",
                headline.trim_end_matches('.')
            );
            return;
        }
        Gated::Refused(m) => {
            s.run = None;
            s.result = format!(
                "not done: the team's agent permissions refuse `{tool}` here ({}); allow `{tool}` for this repository on the Agent permissions page",
                one_line(&m)
            );
            return;
        }
    }
    match action {
        Action::Dispatch(r) | Action::Label(_, r) => {
            start(cx, s, &action_label(&args), &r, until, p).await;
        }
        Action::Write(Target::Staging) => {
            if let Some((conn, token)) = conn {
                write(cx, s, &conn, &token, &mutations).await;
            }
        }
        _ => {}
    }
}

fn action_label(args: &Value) -> Option<String> {
    args["label"].as_str().map(String::from)
}

/// What a production CMS write lacks, and the access to grant.
pub fn production_write_gap() -> String {
    "no production CMS write connection exists for this repository (CoderHelm's CMS write \
     connection is staging only and refuses production datasets); grant: a person with \
     production CMS write access applies the mutations listed in this step, until a \
     production write connection can be added in Settings"
        .to_string()
}

async fn staging_connection(
    cx: &Ctx<'_>,
) -> Result<(common::staging_cms::StagingCms, String), String> {
    let Some((c, token)) = super::feature::load_connection(cx.state, cx.team_id, &cx.full()).await
    else {
        return Err(format!(
            "{} has no staging CMS write connection; grant: add one in Settings, repository, \
             Staging CMS (a staging-only write token)",
            cx.full()
        ));
    };
    c.check_writable().map_err(|e| {
        format!("{e}; grant: point the repository's staging CMS connection at a staging dataset")
    })?;
    Ok((c, token))
}

async fn write(
    cx: &Ctx<'_>,
    s: &mut NoteStep,
    c: &common::staging_cms::StagingCms,
    token: &str,
    mutations: &[Value],
) {
    let sent = cx
        .state
        .http
        .post(c.mutate_url())
        .bearer_auth(token)
        .json(&json!({ "mutations": mutations }))
        .timeout(Duration::from_secs(30))
        .send()
        .await;
    match sent {
        Ok(r) if r.status().is_success() => {
            let body: Value = r.json().await.unwrap_or_default();
            s.done = true;
            s.run = Some(StepRun {
                state: DONE.to_string(),
                ..Default::default()
            });
            s.result = format!(
                "ran {} mutation(s) on the `{}` dataset (staging){}",
                mutations.len(),
                c.dataset,
                body["transactionId"]
                    .as_str()
                    .map(|t| format!(", transaction `{t}`"))
                    .unwrap_or_default()
            );
        }
        Ok(r) => {
            s.result = format!(
                "not done: staging refused the mutations ({}); grant: give the staging CMS connection's token write access",
                r.status()
            );
        }
        Err(e) => {
            s.result = format!("not done: the staging write failed: {e}");
        }
    }
}

/// Starts the route (dispatch, or its label), finds the run it started and
/// waits for it.
async fn start(
    cx: &Ctx<'_>,
    s: &mut NoteStep,
    label: &Option<String>,
    r: &Route,
    until: Instant,
    p: &mut Progress,
) {
    let since = chrono::Utc::now() - chrono::Duration::seconds(5);
    let file = r.file.rsplit('/').next().unwrap_or(&r.file).to_string();
    let started = match label {
        Some(l) => cx
            .github
            .add_label(cx.owner, cx.repo, cx.pr_number, l)
            .await
            .map(|_| {
                format!(
                    "added the `{l}` label to #{}, which starts {}",
                    cx.pr_number,
                    r.line()
                )
            }),
        None => cx
            .github
            .dispatch_workflow(cx.owner, cx.repo, &file, cx.branch, &json!({}))
            .await
            .map(|_| format!("started {} on `{}`", r.line(), cx.branch)),
    };
    let what = match started {
        Ok(w) => w,
        Err(e) => {
            s.run = None;
            s.result = format!("not done: starting {} failed: {e}", r.line());
            return;
        }
    };
    s.run = Some(StepRun {
        state: RUNNING.to_string(),
        workflow: r.file.clone(),
        started_at: since.to_rfc3339(),
        ..Default::default()
    });
    s.result = what;
    for i in 0..FIND_TRIES {
        if find_run(cx, s, &file).await || Instant::now() + POLL >= until {
            break;
        }
        if i + 1 < FIND_TRIES {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    poll(cx, s, r, until, p).await;
}

/// Looks for the run a step started (the newest run of its workflow on the
/// branch, created after it started). True when found.
async fn find_run(cx: &Ctx<'_>, s: &mut NoteStep, file: &str) -> bool {
    let Some(run) = s.run.as_mut() else {
        return false;
    };
    if run.workflow_run_id != 0 {
        return true;
    }
    let since = chrono::DateTime::parse_from_rfc3339(&run.started_at)
        .map(|t| t.with_timezone(&chrono::Utc))
        .ok();
    let Ok(runs) = cx
        .github
        .list_workflow_runs(cx.owner, cx.repo, file, cx.branch)
        .await
    else {
        return false;
    };
    match pick_run(&runs, since) {
        Some((id, url)) => {
            run.workflow_run_id = id;
            run.url = url;
            true
        }
        None => false,
    }
}

/// The run a start made: the newest of `runs` (newest first, as GitHub
/// lists them) created at or after `since`.
pub fn pick_run(
    runs: &[Value],
    since: Option<chrono::DateTime<chrono::Utc>>,
) -> Option<(u64, String)> {
    runs.iter()
        .find(|r| {
            let created = r["created_at"]
                .as_str()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok());
            match (since, created) {
                (Some(s), Some(c)) => c.with_timezone(&chrono::Utc) >= s,
                (None, _) => true,
                (Some(_), None) => false,
            }
        })
        .and_then(|r| {
            Some((
                r["id"].as_u64()?,
                r["html_url"].as_str().unwrap_or_default().to_string(),
            ))
        })
}

/// Waits for a started run until it completes or `until`; then records its
/// conclusion and failing tests.
async fn poll(cx: &Ctx<'_>, s: &mut NoteStep, r: &Route, until: Instant, p: &mut Progress) {
    let file = r.file.rsplit('/').next().unwrap_or(&r.file).to_string();
    loop {
        if !find_run(cx, s, &file).await {
            if Instant::now() + POLL >= until {
                return;
            }
            tokio::time::sleep(POLL).await;
            continue;
        }
        let id = s.run.as_ref().map(|x| x.workflow_run_id).unwrap_or(0);
        if let Ok(run) = cx.github.get_workflow_run(cx.owner, cx.repo, id).await {
            if run["status"].as_str() == Some("completed") {
                let conclusion = run["conclusion"].as_str().unwrap_or("unknown").to_string();
                finish(cx, s, r, &run, &conclusion, p).await;
                return;
            }
        }
        if Instant::now() + POLL >= until {
            let url = s.run.as_ref().map(|x| x.url.clone()).unwrap_or_default();
            s.result = format!(
                "started {}: {url}; still running, CoderHelm reads the result when it ends",
                r.line()
            );
            return;
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn finish(
    cx: &Ctx<'_>,
    s: &mut NoteStep,
    r: &Route,
    run: &Value,
    conclusion: &str,
    p: &mut Progress,
) {
    let id = run["id"].as_u64().unwrap_or(0);
    let url = run["html_url"].as_str().unwrap_or_default().to_string();
    let ok = conclusion == "success";
    let failing = if ok {
        Vec::new()
    } else {
        cx.github
            .get_workflow_run_logs(cx.owner, cx.repo, id)
            .await
            .map(|l| failing_tests(&l))
            .unwrap_or_default()
    };
    s.done = ok;
    s.run = Some(StepRun {
        state: if ok { DONE } else { FAILED }.to_string(),
        workflow_run_id: id,
        url: url.clone(),
        workflow: r.file.clone(),
        conclusion: conclusion.to_string(),
        failing: failing.clone(),
        ..s.run.clone().unwrap_or_default()
    });
    s.result = report_line(r, &url, conclusion, &failing);
    if !ok && s.step_kind() == StepKind::E2e {
        p.failed_e2e = Some(FailedRun {
            workflow_run_id: id,
            workflow: r.name.clone(),
            url,
            head_sha: run["head_sha"].as_str().unwrap_or_default().to_string(),
            failing,
        });
    }
    info!(run_id = cx.run_id, workflow = %r.file, conclusion, "Outside step's workflow ended");
}

/// The step's result line for a finished run.
pub fn report_line(r: &Route, url: &str, conclusion: &str, failing: &[String]) -> String {
    let mut t = format!("ran {}: {url}, concluded `{conclusion}`", r.line());
    if !failing.is_empty() {
        t.push_str(&format!(
            "; failing: {}",
            failing
                .iter()
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if conclusion != "success" && r.trigger != outside_work::Trigger::Script {
        t.push_str("; the failure goes to the run's CI fix round");
    }
    t
}

/// The failing tests a workflow log names (Playwright, Cypress, Jest and
/// Vitest summaries), without timestamps, at most a few.
pub fn failing_tests(log: &str) -> Vec<String> {
    let ts = regex::Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s*").ok();
    let numbered = regex::Regex::new(r"^\d+\)\s+(.+)$").ok();
    let mut out: Vec<String> = Vec::new();
    for raw in log.lines() {
        let line = match &ts {
            Some(re) => re.replace(raw, "").to_string(),
            None => raw.to_string(),
        };
        let l = line.trim();
        let name = if let Some(rest) = l
            .strip_prefix("✘")
            .or_else(|| l.strip_prefix("✕"))
            .or_else(|| l.strip_prefix("×"))
        {
            Some(rest.trim().to_string())
        } else if let Some(rest) = l.strip_prefix("FAIL ") {
            Some(rest.trim().to_string())
        } else if l.contains(" › ") || l.contains("] › ") {
            numbered
                .as_ref()
                .and_then(|re| re.captures(l))
                .map(|c| c[1].trim().to_string())
        } else {
            None
        };
        if let Some(n) = name {
            // Playwright's retry lines repeat the title with "(retry #1)".
            let n = n
                .split(" (retry #")
                .next()
                .unwrap_or(&n)
                .trim_start_matches(|c: char| c.is_ascii_digit() || c == ' ')
                .trim()
                .to_string();
            if !n.is_empty() && !out.contains(&n) {
                out.push(n);
            }
        }
        if out.len() >= MAX_FAILING {
            break;
        }
    }
    out
}

/// Runs a verify step's query and records it.
async fn query_step(cx: &Ctx<'_>, a: &Abilities, c: &OutsideChange, s: &mut NoteStep) {
    let Some(q) = c
        .confirm
        .as_ref()
        .map(|f| f.query.clone())
        .filter(|q| !q.trim().is_empty())
    else {
        s.result = "not done: the note gives no confirm query to run".to_string();
        return;
    };
    match read(cx, a, &q).await {
        Ok((v, on)) => {
            s.result = format!("ran `{}` on {on}: {}", one_line(&q), quote(&v.to_string()));
            s.done = true;
        }
        Err(e) => s.result = format!("not done: ran `{}`: {e}", one_line(&q)),
    }
}

/// Runs the confirm query once the steps ended, and quotes its result on
/// `confirm`. True when it ran.
async fn confirm(cx: &Ctx<'_>, a: &Abilities, c: &mut OutsideChange) -> bool {
    let Some(f) = c.confirm.as_mut() else {
        return false;
    };
    if f.query.trim().is_empty() || !f.result.is_empty() {
        return false;
    }
    let q = f.query.clone();
    f.result = match read(cx, a, &q).await {
        Ok((v, on)) => format!("{} (on {on})", v),
        Err(e) => format!("the query failed: {e}"),
    };
    true
}

/// A read query: through the staging write connection when there is one
/// (it can read a private dataset), else the public read API.
async fn read(cx: &Ctx<'_>, a: &Abilities, q: &str) -> Result<(Value, String), String> {
    if let Ok((c, token)) = staging_connection(cx).await {
        let r = cx
            .state
            .http
            .get(c.query_url())
            .bearer_auth(&token)
            .query(&[("query", q)])
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("the staging query API returned {}", r.status()));
        }
        let v: Value = r
            .json()
            .await
            .map_err(|e| format!("unreadable result: {e}"))?;
        return Ok((
            v["result"].clone(),
            format!("the `{}` dataset (staging)", c.dataset),
        ));
    }
    let Some(src) = a.source.as_ref() else {
        return Err("no CMS project id and dataset are named in the repository".into());
    };
    outside_work::public_query(&cx.state.http, src, q)
        .await
        .map(|v| (v, src.labels()))
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn quote(s: &str) -> String {
    let s = one_line(s);
    if s.len() <= 600 {
        return s;
    }
    let mut end = 600;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

// ── Keeping it across invocations ───────────────────────────────────────────

/// Saves the decision (with its steps' state) on the run record, where the
/// resume reads it.
pub async fn save(state: &WorkerState, team_id: &str, run_id: &str, d: &FixDecision) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression("SET fix_decision = :fd")
        .expression_attribute_values(
            ":fd",
            super::attr_s(&serde_json::to_string(d).unwrap_or_default()),
        )
        .send()
        .await;
}

/// The decision a run record holds.
pub fn decision_of(
    record: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Option<FixDecision> {
    let raw = record.get("fix_decision")?.as_s().ok()?;
    serde_json::from_str(raw).ok()
}

/// The requirements a run leaves not done (a person's steps, CoderHelm
/// steps that didn't work), from its record. Empty when every one is done.
pub async fn left_undone(state: &WorkerState, team_id: &str, run_id: &str) -> Vec<String> {
    let Ok(out) = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .send()
        .await
    else {
        return Vec::new();
    };
    out.item
        .as_ref()
        .and_then(decision_of)
        .and_then(|d| d.outside_change)
        .map(|c| outside_work::not_done_lines(&c))
        .unwrap_or_default()
}

/// Writes a `ci_failed` event for a started end-to-end run that failed, with
/// its workflow run id, so the resume's CI fix round reads its logs.
pub async fn record_failed(state: &WorkerState, run_id: &str, f: &FailedRun) {
    if state.config.events_table_name.is_empty() {
        return;
    }
    let now = chrono::Utc::now();
    let sk = format!("EVENT#{}#ci_failed", now.format("%Y%m%dT%H%M%S%.3fZ"));
    let payload = json!({
        "head_sha": f.head_sha,
        "workflow_run_id": f.workflow_run_id,
        "workflow_name": f.workflow,
        "conclusion": "failure",
        "failing_tests": f.failing,
    });
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.events_table_name)
        .item("pk", super::attr_s(&format!("RUN#{run_id}")))
        .item("sk", super::attr_s(&sk))
        .item("event_type", super::attr_s("ci_failed"))
        .item("payload", super::attr_s(&payload.to_string()))
        .item(
            "processed",
            aws_sdk_dynamodb::types::AttributeValue::Bool(false),
        )
        .item("created_at", super::attr_s(&now.to_rfc3339()))
        .item(
            "expires_at",
            super::attr_n((now.timestamp() as u64 + 30 * 86400).to_string()),
        )
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not record the failed end-to-end run");
    }
}

/// Rewrites the PR's Not in this PR section from the steps (with their
/// results) and puts the requirements not done at the top of its body.
pub async fn refresh_pr(github: &GitHubClient, owner: &str, repo: &str, pr: u64, d: &FixDecision) {
    let Some(c) = d.outside_change.as_ref() else {
        return;
    };
    let Ok(pr_json) = github.get_pull_request(owner, repo, pr).await else {
        return;
    };
    let body = pr_json["body"].as_str().unwrap_or("");
    let (mut owned, rest) = super::completeness::take_owned(body);
    owned.not_in_pr = super::completeness::for_people_section(
        &super::fix_location::not_in_pr_steps(Some(d), &format!("{owner}/{repo}")),
        &d.questions,
    );
    let updated = outside_work::with_not_done_top(&super::completeness::assemble(&rest, &owned), c);
    if updated != body {
        if let Err(e) = github.update_pr_body(owner, repo, pr, &updated).await {
            warn!(pr, error = %e, "Could not update the PR's outside steps");
        }
    }
}

/// What the resume makes of the outside steps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnResume {
    /// Nothing under way.
    Idle,
    /// A step waits on an approval or a workflow: the run isn't finished.
    Waiting,
    /// A started end-to-end run failed: it goes to the CI fix round.
    Failed(FailedRun),
}

/// Carries the outside steps on from the CI resume: asks the gate again for
/// a step waiting on approval, reads a running workflow, does the steps
/// after it. Short: the resume comes back every couple of minutes.
pub async fn on_resume(
    state: &WorkerState,
    msg: &crate::models::ResumeMessage,
    record: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> OnResume {
    let Some(mut d) = decision_of(record) else {
        return OnResume::Idle;
    };
    let Some(c) = d.outside_change.as_mut() else {
        return OnResume::Idle;
    };
    if !outside_work::unfinished(c) {
        return OnResume::Idle;
    }
    let get = |k: &str| {
        record
            .get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let place = c.placed.clone().unwrap_or_else(|| Placement {
        repo: get("repo"),
        branch: get("branch"),
        pr_number: record
            .get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0),
    });
    let Some((owner, repo)) = place.repo.split_once('/') else {
        return OnResume::Idle;
    };
    if place.branch.is_empty() || place.pr_number == 0 {
        return OnResume::Idle;
    }
    let Ok(github) = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    ) else {
        return OnResume::Waiting;
    };
    let env = super::load_codegen_env(state, &msg.team_id, owner, repo).await;
    let staging_write = super::feature::load_connection(state, &msg.team_id, &place.repo)
        .await
        .is_some();
    let abilities =
        outside_work::load(&github, owner, repo, &place.branch, &env, staging_write).await;
    let cx = Ctx {
        state,
        github: &github,
        team_id: &msg.team_id,
        run_id: &msg.run_id,
        owner,
        repo,
        branch: &place.branch,
        pr_number: place.pr_number,
    };
    let before = c.clone();
    let p = execute(&cx, &abilities, c, Instant::now() + Duration::from_secs(45)).await;
    if *c != before {
        let lines: Vec<String> = c
            .steps
            .iter()
            .zip(before.steps.iter())
            .enumerate()
            .filter(|(_, (now, was))| now != was && now.doer() == Doer::CoderHelm)
            .map(|(i, (s, _))| outside_work::step_line(i + 1, s).0)
            .collect();
        for l in &lines {
            super::add_progress_note(
                state,
                &msg.team_id,
                &msg.run_id,
                &format!("Outside step: {l}"),
            )
            .await;
        }
        save(state, &msg.team_id, &msg.run_id, &d).await;
        refresh_pr(&github, owner, repo, place.pr_number, &d).await;
    }
    if let Some(f) = p.failed_e2e {
        return OnResume::Failed(f);
    }
    if p.waiting {
        OnResume::Waiting
    } else {
        OnResume::Idle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::outside_work::Trigger;

    fn step(kind: &str, target: &str, text: &str) -> NoteStep {
        NoteStep {
            step: text.into(),
            by: "coderhelm".into(),
            owner: "CoderHelm".into(),
            kind: kind.into(),
            target: target.into(),
            ..Default::default()
        }
    }

    fn route(file: &str, trigger: Trigger) -> Route {
        Route {
            name: "Deploy".into(),
            file: file.into(),
            command: "npx sanity deploy".into(),
            trigger,
        }
    }

    #[test]
    fn every_change_goes_through_the_gate_with_stable_args() {
        let s = step("deploy", "production", "Deploy the app to production");
        let r = route(".github/workflows/deploy.yml", Trigger::Dispatch);
        let (tool, args) = gate_args(&s, &Action::Dispatch(r.clone()), "acme/web", "feat").unwrap();
        assert_eq!(tool, START_WORKFLOW);
        assert_eq!(args["target"], "production");
        assert_eq!(args["workflow"], ".github/workflows/deploy.yml");
        // The same step asks the same thing on every invocation, so an
        // approval given once is found again.
        assert_eq!(
            gate_args(&s, &Action::Dispatch(r.clone()), "acme/web", "feat")
                .unwrap()
                .1,
            args
        );
        let (tool, args) =
            gate_args(&s, &Action::Label("deploy".into(), r), "acme/web", "feat").unwrap();
        assert_eq!(tool, START_WORKFLOW);
        assert_eq!(args["label"], "deploy");
        let mut w = step("migration", "staging", "Run the hero migration");
        w.mutations = vec![json!({"patch": {"id": "home", "set": {"title": "x"}}})];
        let (tool, a1) =
            gate_args(&w, &Action::Write(Target::Staging), "acme/web", "feat").unwrap();
        assert_eq!(tool, WRITE_CONTENT);
        w.mutations = vec![json!({"patch": {"id": "home", "set": {"title": "y"}}})];
        let (_, a2) = gate_args(&w, &Action::Write(Target::Staging), "acme/web", "feat").unwrap();
        assert_ne!(
            a1["mutations"], a2["mutations"],
            "other mutations ask again"
        );
        assert!(gate_args(&w, &Action::Query, "acme/web", "feat").is_none());
        // Both tools are known to the agent-permissions registry as writes:
        // the team's policy decides, and with no rule they ask.
        let reg = common::agent_policy::registry::default_registry();
        for t in [START_WORKFLOW, WRITE_CONTENT] {
            assert_eq!(reg.risk(t), Some(common::agent_policy::Risk::Write), "{t}");
        }
    }

    #[test]
    fn finds_the_run_a_start_made() {
        let since = chrono::DateTime::parse_from_rfc3339("2026-10-28T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let runs = vec![
            json!({"id": 3, "created_at": "2026-10-28T10:00:04Z", "html_url": "https://x/3"}),
            json!({"id": 2, "created_at": "2026-10-28T09:00:00Z", "html_url": "https://x/2"}),
        ];
        assert_eq!(
            pick_run(&runs, Some(since)),
            Some((3, "https://x/3".into()))
        );
        assert_eq!(
            pick_run(&runs[1..], Some(since)),
            None,
            "an older run is not ours"
        );
    }

    #[test]
    fn reads_the_failing_tests_from_the_log() {
        let log = "2026-10-28T10:00:01.000Z Running 12 tests using 2 workers\n\
                   2026-10-28T10:00:09.000Z   ✘  3 [chromium] › e2e/home.spec.ts:12:5 › Home › shows the tabs (5.1s)\n\
                   2026-10-28T10:00:20.000Z   1) [chromium] › e2e/home.spec.ts:12:5 › Home › shows the tabs \n\
                   2026-10-28T10:00:21.000Z   2) [chromium] › e2e/cart.spec.ts:8:3 › Cart › adds an item (retry #1)\n\
                   FAIL src/cart.test.ts\n\
                   2026-10-28T10:00:22.000Z   12 passed\n";
        let f = failing_tests(log);
        assert!(
            f.iter()
                .any(|t| t.contains("e2e/home.spec.ts:12:5 › Home › shows the tabs")),
            "{f:?}"
        );
        assert!(
            f.iter()
                .any(|t| t.contains("Cart › adds an item") && !t.contains("retry")),
            "{f:?}"
        );
        assert!(f.contains(&"src/cart.test.ts".to_string()), "{f:?}");
        assert!(!f.iter().any(|t| t.contains("passed")));
        assert!(failing_tests("all good\n12 passed").is_empty());
    }

    #[test]
    fn a_finished_run_reports_its_conclusion_and_failures() {
        let r = route(
            ".github/workflows/e2e.yml",
            Trigger::Label("run-e2e".into()),
        );
        let ok = report_line(&r, "https://x/1", "success", &[]);
        assert!(ok.contains("concluded `success`") && !ok.contains("fix round"));
        let bad = report_line(
            &r,
            "https://x/1",
            "failure",
            &["Home › shows the tabs".into()],
        );
        assert!(bad.contains("concluded `failure`"));
        assert!(bad.contains("failing: `Home › shows the tabs`"));
        assert!(bad.contains("CI fix round"));
    }

    #[test]
    fn a_production_content_write_names_the_missing_connection() {
        let g = production_write_gap();
        assert!(g.contains("no production CMS write connection"));
        assert!(g.contains("grant:"));
    }

    #[test]
    fn a_failed_deploy_or_write_holds_the_steps_after_it() {
        assert!(blocks_later(&step("deploy", "staging", "Deploy")));
        assert!(blocks_later(&step("migration", "staging", "Run it")));
        assert!(!blocks_later(&step("e2e", "staging", "Run the e2e")));
        assert!(!blocks_later(&step("verify", "staging", "Confirm")));
    }
}
