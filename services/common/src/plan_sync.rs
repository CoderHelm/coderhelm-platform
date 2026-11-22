//! Keeps a plan's tasks linked to their runs and their derived state
//! written down.
//!
//! Each task's runs are the runs of the ticket it filed: its Jira key, or
//! its GitHub issue (`GH-<n>` in the issue's repository). A refresh
//! ([`refresh`]) reads them, stamps each run that does not say so yet with
//! its plan and task (`plan_id`, `plan_task_id`), writes the ticket's plan
//! link when it is missing (so later runs of the ticket carry the plan from
//! their first write), derives every task's state and the plan's
//! ([`crate::plan_state`]) and writes them on the task and plan rows when
//! they changed.
//!
//! It runs on the events that move a task: a run's status change (the
//! worker), a pull request opened, reviewed, checked, merged or closed (the
//! GitHub webhook), and an approval. The plan page refreshes the plan it
//! shows, and the plans list refreshes plans written before this existed
//! ([`DERIVED_VERSION`]), a bounded number per page: that is the backfill.
//! A refresh is idempotent: it only adds a missing link or stamp and
//! rewrites derived fields.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

use crate::plan_state::{
    self, Criterion, Drift, PlanState, PlanView, PrFacts, RunFacts, TaskFacts, TaskState, TaskView,
};

pub type Item = HashMap<String, AttributeValue>;

/// Bumped when the derived fields change shape: plans written with an older
/// version are refreshed by the list.
pub const DERIVED_VERSION: u64 = 1;
/// Run ids kept on a task row.
const MAX_RUN_IDS: usize = 25;

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn n_f64(item: &Item, k: &str) -> f64 {
    item.get(k)
        .and_then(|v| v.as_n().ok().or_else(|| v.as_s().ok()))
        .and_then(|n| n.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn n_u64(item: &Item, k: &str) -> Option<u64> {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<f64>().ok())
        .map(|f| f as u64)
}

fn strings(item: &Item, k: &str) -> Vec<String> {
    item.get(k)
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default()
}

/// A task row as the derivation reads it.
pub fn task_facts(item: &Item) -> TaskFacts {
    TaskFacts {
        task_id: s(item, "task_id"),
        title: s(item, "title"),
        description: s(item, "description"),
        acceptance_criteria: s(item, "acceptance_criteria"),
        status: s(item, "status"),
        depends_on: s(item, "depends_on"),
        failure_reason: s(item, "failure_reason"),
        created_at: s(item, "created_at"),
        approved_at: s(item, "approved_at"),
        rejected_at: s(item, "rejected_at"),
        rejected_by: s(item, "rejected_by"),
        edited_at: s(item, "edited_at"),
        edited_by_kind: s(item, "edited_by_kind"),
        edited_by: s(item, "edited_by"),
        added_by_kind: s(item, "added_by_kind"),
        covers: strings(item, "covers"),
    }
}

/// A run record as the derivation reads it.
pub fn run_facts(item: &Item) -> RunFacts {
    let prs = crate::run_prs::from_item(item)
        .into_iter()
        .map(|p| PrFacts {
            repo: p.repo,
            number: p.pr_number,
            url: p.url,
            outcome: p.outcome,
        })
        .collect();
    let ledger = crate::scope_ledger::Ledger::from_json(&s(item, crate::scope_ledger::FIELD));
    RunFacts {
        run_id: s(item, "run_id"),
        status: s(item, "status"),
        created_at: s(item, "created_at"),
        updated_at: s(item, "updated_at"),
        cost_usd: n_f64(item, "cost_usd"),
        current_pass: s(item, "current_pass"),
        error: {
            let e = s(item, "error_message");
            if e.is_empty() {
                s(item, "error")
            } else {
                e
            }
        },
        close_reason: s(item, "close_reason"),
        waiting_on: crate::run_wait::WaitingOn::of_item(item),
        prs,
        pr_outcome: s(item, "pr_outcome"),
        ci_last: s(item, "ci_last"),
        standing: crate::merge_standing::Standing::from_json(&s(
            item,
            crate::merge_standing::FIELD,
        )),
        review: s(item, "pr_review"),
        additions: n_u64(item, "pr_additions"),
        deletions: n_u64(item, "pr_deletions"),
        trace: crate::trace::Trace::from_json(&s(item, crate::trace::FIELD)),
        files: strings(item, "files_modified"),
        scope_moved_in: ledger
            .entries
            .iter()
            .filter(|e| !e.is_out())
            .map(|e| (e.item.clone(), e.moved_in.clone()))
            .collect(),
    }
}

/// The ticket a task filed, as runs record it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTicket {
    /// The runs' `ticket_id`: the Jira key, or `GH-<n>`.
    pub ticket_id: String,
    /// For a GitHub issue, the repository its runs are in (lowercase).
    pub repo: Option<String>,
    /// The settings-table link from the ticket to the task.
    pub link_sk: String,
}

/// The ticket a task row records, if it filed one.
pub fn task_ticket(item: &Item) -> Option<TaskTicket> {
    let key = s(item, "jira_ticket_key");
    let key = key.trim();
    if !key.is_empty() && !key.eq_ignore_ascii_case("UNKNOWN") {
        // A task linked to a live run's ticket records `GH-<n>` here.
        if let Some(n) = key
            .to_ascii_uppercase()
            .strip_prefix("GH-")
            .and_then(|n| n.parse::<u64>().ok())
        {
            let repo = s(item, "repo").to_lowercase();
            return Some(TaskTicket {
                ticket_id: format!("GH-{n}"),
                link_sk: crate::plan_dependents::github_link_sk(&repo, n),
                repo: (!repo.is_empty()).then_some(repo),
            });
        }
        return Some(TaskTicket {
            ticket_id: key.to_ascii_uppercase(),
            repo: None,
            link_sk: crate::plan_dependents::jira_link_sk(key),
        });
    }
    let url = s(item, "issue_url");
    let path = url.trim().split_once("://")?.1;
    let parts: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    if let [_host, owner, repo, "issues", n] = parts.as_slice() {
        let n: u64 = n.parse().ok().filter(|n| *n > 0)?;
        let repo = format!("{owner}/{repo}").to_lowercase();
        return Some(TaskTicket {
            ticket_id: format!("GH-{n}"),
            link_sk: crate::plan_dependents::github_link_sk(&repo, n),
            repo: Some(repo),
        });
    }
    None
}

/// Where the plan rows, the runs and the links are read and written.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// The plan row and its task rows; `None` when there is no plan.
    async fn plan_rows(
        &self,
        team: &str,
        plan_id: &str,
    ) -> Result<Option<(Item, Vec<Item>)>, String>;
    /// Every run whose `ticket_id` is this one.
    async fn runs_of_ticket(&self, team: &str, ticket_id: &str) -> Result<Vec<Item>, String>;
    /// Sets the derived fields on a task row.
    async fn write_task(
        &self,
        team: &str,
        plan_id: &str,
        task_id: &str,
        fields: Vec<(String, AttributeValue)>,
    ) -> Result<(), String>;
    /// Sets the derived fields on the plan row.
    async fn write_plan(
        &self,
        team: &str,
        plan_id: &str,
        fields: Vec<(String, AttributeValue)>,
    ) -> Result<(), String>;
    /// Marks a run as the task's, unless it already names a plan.
    async fn stamp_run(
        &self,
        team: &str,
        run_id: &str,
        plan_id: &str,
        task_id: &str,
        plan_title: &str,
    ) -> Result<(), String>;
    /// Writes the ticket's plan link unless one exists.
    async fn ensure_link(
        &self,
        team: &str,
        link_sk: &str,
        plan_id: &str,
        task_id: &str,
        plan_title: &str,
    ) -> Result<(), String>;
}

/// A task with everything the plan page shows for it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskDetail {
    pub task_id: String,
    #[serde(flatten)]
    pub view: TaskView,
    /// The criteria ids it covers.
    pub covers: Vec<String>,
}

/// A refreshed plan: its view, its tasks' views and its criteria.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Refreshed {
    pub plan: PlanView,
    pub tasks: Vec<TaskDetail>,
    pub criteria: Vec<Criterion>,
    /// Runs this refresh stamped with their plan and task.
    pub runs_linked: usize,
    /// Ticket links it wrote.
    pub links_written: usize,
}

/// Derives the plan's state and its tasks', links the runs, and writes what
/// changed. `None` when the plan does not exist.
pub async fn refresh(
    store: &dyn Store,
    team: &str,
    plan_id: &str,
    now: &str,
) -> Result<Option<Refreshed>, String> {
    let Some((plan, mut tasks)) = store.plan_rows(team, plan_id).await? else {
        return Ok(None);
    };
    tasks.sort_by_key(|t| n_u64(t, "task_order").unwrap_or(0));
    let plan_title = crate::truncate_str(&s(&plan, "title"), 300).to_string();
    let plan_repo = s(&plan, "repo").to_lowercase();
    let started_at = s(&plan, "executed_at");
    let mut out = Refreshed::default();

    let facts: Vec<TaskFacts> = tasks.iter().map(task_facts).collect();
    let titles: HashMap<String, String> = facts
        .iter()
        .map(|t| (t.task_id.clone(), t.title.clone()))
        .collect();
    let mut all_runs: Vec<Vec<RunFacts>> = Vec::with_capacity(tasks.len());
    for (item, f) in tasks.iter().zip(&facts) {
        let mut runs = Vec::new();
        if let Some(mut ticket) = task_ticket(item) {
            if ticket.repo.is_none() && ticket.ticket_id.starts_with("GH-") && !plan_repo.is_empty()
            {
                ticket.repo = Some(plan_repo.clone());
            }
            // Once per task: the worker writes the link for every ticket it
            // files, so only tasks from before that need one.
            let linked = item
                .get("ticket_linked")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            if !linked
                && store
                    .ensure_link(team, &ticket.link_sk, plan_id, &f.task_id, &plan_title)
                    .await
                    .is_ok()
            {
                out.links_written += 1;
            }
            for r in store.runs_of_ticket(team, &ticket.ticket_id).await? {
                if let Some(repo) = &ticket.repo {
                    if !s(&r, "repo").eq_ignore_ascii_case(repo) {
                        continue;
                    }
                }
                let named = s(&r, "plan_id");
                if !named.is_empty() && named != plan_id {
                    // Another plan's run of the same ticket.
                    continue;
                }
                if named.is_empty() || s(&r, "plan_task_id").is_empty() {
                    let run_id = s(&r, "run_id");
                    if !run_id.is_empty()
                        && store
                            .stamp_run(team, &run_id, plan_id, &f.task_id, &plan_title)
                            .await
                            .is_ok()
                    {
                        out.runs_linked += 1;
                    }
                }
                runs.push(run_facts(&r));
            }
        }
        all_runs.push(runs);
    }

    let mut views: Vec<TaskView> = Vec::with_capacity(tasks.len());
    for (f, runs) in facts.iter().zip(&all_runs) {
        let waits = (!f.depends_on.is_empty())
            .then(|| titles.get(&f.depends_on).map(String::as_str))
            .flatten();
        let mut v = plan_state::task_view(f, runs, waits);
        v.drift = plan_state::task_drift(f, &v, runs, &started_at);
        views.push(v);
    }
    let pairs: Vec<(&TaskFacts, &TaskView)> = facts.iter().zip(views.iter()).collect();
    out.plan = plan_state::plan_view(&pairs, now);

    let traces: Vec<Option<plan_state::RunTrace>> = all_runs
        .iter()
        .map(|r| plan_state::newest_trace(r))
        .collect();
    let with_traces: Vec<(&TaskFacts, Option<&plan_state::RunTrace>)> = facts
        .iter()
        .zip(traces.iter().map(Option::as_ref))
        .collect();
    let plan_criteria = strings(&plan, "criteria");
    out.criteria = plan_state::criteria(&plan_criteria, &with_traces);

    // Write what changed.
    for ((item, f), v) in tasks.iter().zip(&facts).zip(&views) {
        let mut fields = task_fields(v, now);
        if task_ticket(item).is_some() {
            fields.push(("ticket_linked".to_string(), AttributeValue::Bool(true)));
        }
        if fingerprint(&fields) != s(item, "derived_fp") {
            let fp = fingerprint(&fields);
            fields.push(("derived_fp".into(), AttributeValue::S(fp)));
            store.write_task(team, plan_id, &f.task_id, fields).await?;
        }
        out.tasks.push(TaskDetail {
            task_id: f.task_id.clone(),
            view: v.clone(),
            covers: covers_of(&f.task_id, &out.criteria),
        });
    }
    let fields = plan_fields(&out.plan, &out.criteria);
    if fingerprint(&fields) != s(&plan, "derived_fp")
        || n_u64(&plan, "derived_v") != Some(DERIVED_VERSION)
    {
        let mut fields = fields;
        let fp = fingerprint(&fields);
        fields.push(("derived_fp".into(), AttributeValue::S(fp)));
        fields.push(("derived_at".into(), AttributeValue::S(now.to_string())));
        fields.push((
            "derived_v".into(),
            AttributeValue::N(DERIVED_VERSION.to_string()),
        ));
        store.write_plan(team, plan_id, fields).await?;
    }
    Ok(Some(out))
}

fn covers_of(task_id: &str, criteria: &[Criterion]) -> Vec<String> {
    criteria
        .iter()
        .filter(|c| c.tasks.iter().any(|t| t == task_id))
        .map(|c| c.id.clone())
        .collect()
}

fn json<T: Serialize>(v: &T) -> AttributeValue {
    AttributeValue::S(serde_json::to_string(v).unwrap_or_default())
}

/// The derived fields of a task row.
fn task_fields(v: &TaskView, _now: &str) -> Vec<(String, AttributeValue)> {
    let ids: Vec<AttributeValue> = v
        .runs
        .iter()
        .rev()
        .take(MAX_RUN_IDS)
        .map(|r| AttributeValue::S(r.run_id.clone()))
        .collect();
    let mut f = vec![
        (
            "state".to_string(),
            AttributeValue::S(v.state.code().into()),
        ),
        (
            "state_detail".to_string(),
            AttributeValue::S(v.detail.clone()),
        ),
        (
            "state_reason".to_string(),
            AttributeValue::S(v.reason.clone()),
        ),
        (
            "attempts".to_string(),
            AttributeValue::N(v.attempts.to_string()),
        ),
        (
            "cost_usd".to_string(),
            AttributeValue::N(format!("{:.4}", v.cost_usd)),
        ),
        ("prs".to_string(), json(&v.prs)),
        (
            "ticket_filed".to_string(),
            AttributeValue::Bool(v.ticket_filed),
        ),
    ];
    if !ids.is_empty() {
        f.push(("run_ids".to_string(), AttributeValue::L(ids)));
        // The newest attempt and its pull request, for older readers.
        if let Some(last) = v.runs.last() {
            f.push(("run_id".to_string(), AttributeValue::S(last.run_id.clone())));
        }
    }
    if let Some(pr) = v.prs.first() {
        f.push(("pr_url".to_string(), AttributeValue::S(pr.url.clone())));
    }
    f
}

/// The derived fields of a plan row: what the list shows without reading
/// the tasks.
fn plan_fields(p: &PlanView, criteria: &[Criterion]) -> Vec<(String, AttributeValue)> {
    vec![
        (
            "state".to_string(),
            AttributeValue::S(p.state.code().into()),
        ),
        // The older word, so a client that reads `status` is not told Done
        // before the work merged.
        (
            "status".to_string(),
            AttributeValue::S(p.state.legacy_status().into()),
        ),
        ("summary".to_string(), AttributeValue::S(p.summary.clone())),
        ("counts".to_string(), json(&p.counts)),
        (
            "needs_count".to_string(),
            AttributeValue::N(p.needs.len().to_string()),
        ),
        (
            "cost_usd".to_string(),
            AttributeValue::N(format!("{:.4}", p.cost_usd)),
        ),
        (
            "attempts".to_string(),
            AttributeValue::N(p.attempts.to_string()),
        ),
        (
            "last_activity_at".to_string(),
            AttributeValue::S(p.last_activity.clone()),
        ),
        (
            "criteria_met".to_string(),
            AttributeValue::N(criteria.iter().filter(|c| c.met).count().to_string()),
        ),
        (
            "criteria_count".to_string(),
            AttributeValue::N(criteria.len().to_string()),
        ),
    ]
}

fn fingerprint(fields: &[(String, AttributeValue)]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for (k, v) in fields {
        h.update(k.as_bytes());
        h.update(format!("{v:?}").as_bytes());
    }
    hex::encode(&h.finalize()[..12])
}

/// The list's numbers for a plan row, from its derived fields.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Stored {
    pub state: Option<PlanState>,
    pub summary: String,
    pub counts: plan_state::Counts,
    pub needs_count: u64,
    pub cost_usd: f64,
    pub attempts: u64,
    pub last_activity_at: String,
    pub criteria_met: u64,
    pub criteria_count: u64,
    /// The derived fields are current ([`DERIVED_VERSION`]).
    pub current: bool,
}

/// The derived fields a plan row holds.
pub fn stored(item: &Item) -> Stored {
    Stored {
        state: PlanState::from_code(&s(item, "state")),
        summary: s(item, "summary"),
        counts: serde_json::from_str(&s(item, "counts")).unwrap_or_default(),
        needs_count: n_u64(item, "needs_count").unwrap_or(0),
        cost_usd: n_f64(item, "cost_usd"),
        attempts: n_u64(item, "attempts").unwrap_or(0),
        last_activity_at: s(item, "last_activity_at"),
        criteria_met: n_u64(item, "criteria_met").unwrap_or(0),
        criteria_count: n_u64(item, "criteria_count").unwrap_or(0),
        current: n_u64(item, "derived_v") == Some(DERIVED_VERSION),
    }
}

/// Whether the list should refresh a plan row before showing it: written
/// before the derived fields existed, or open work last derived more than
/// `stale_minutes` ago (a missed event never leaves it wrong for long).
pub fn needs_refresh(item: &Item, now: &str, stale_minutes: i64) -> bool {
    let st = stored(item);
    if !st.current {
        return true;
    }
    let open = !matches!(st.state, Some(PlanState::Done) | Some(PlanState::Drafting));
    if !open {
        return false;
    }
    let (Ok(at), Ok(now)) = (
        chrono::DateTime::parse_from_rfc3339(&s(item, "derived_at")),
        chrono::DateTime::parse_from_rfc3339(now),
    ) else {
        return true;
    };
    (now - at).num_minutes() >= stale_minutes
}

/// The plan and task a run record names, when it is a plan's.
pub fn plan_of_run(item: &Item) -> Option<(String, String)> {
    let plan = s(item, "plan_id");
    (!plan.is_empty()).then(|| (plan, s(item, "plan_task_id")))
}

/// What starting tasks is expected to take, from the team's recent runs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Estimate {
    /// Tasks it would start.
    pub tasks: usize,
    /// Runs expected: one per task, up to the attempts a ticket took lately.
    pub runs_low: usize,
    pub runs_high: usize,
    pub cost_low: f64,
    pub cost_high: f64,
    /// One run's typical cost (a Retry or Force run).
    pub per_run_low: f64,
    pub per_run_high: f64,
    /// Finished runs the numbers come from; 0 means no history yet.
    pub sample: usize,
}

/// The estimate for `tasks` tasks from recent finished runs: `(ticket_id,
/// cost)`. Per run: the middle half of the costs (25th to 75th
/// percentile); runs: one per task up to the average attempts per ticket.
pub fn estimate(tasks: usize, recent: &[(String, f64)]) -> Estimate {
    let mut costs: Vec<f64> = recent
        .iter()
        .map(|(_, c)| *c)
        .filter(|c| *c > 0.0)
        .collect();
    costs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut e = Estimate {
        tasks,
        runs_low: tasks,
        runs_high: tasks,
        sample: costs.len(),
        ..Default::default()
    };
    if costs.is_empty() {
        return e;
    }
    let at = |q: f64| costs[((costs.len() - 1) as f64 * q).round() as usize];
    e.per_run_low = at(0.25);
    e.per_run_high = at(0.75);
    let tickets: std::collections::HashSet<&str> = recent.iter().map(|(t, _)| t.as_str()).collect();
    let per_ticket = if tickets.is_empty() {
        1.0
    } else {
        (recent.len() as f64 / tickets.len() as f64).max(1.0)
    };
    e.runs_high = ((tasks as f64) * per_ticket).ceil() as usize;
    e.cost_low = e.per_run_low * tasks as f64;
    e.cost_high = e.per_run_high * e.runs_high as f64;
    e
}

/// The tables in DynamoDB.
pub struct Dynamo {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub plans_table: String,
    pub runs_table: String,
    pub settings_table: String,
}

fn err<E: std::error::Error + 'static>(
    e: aws_sdk_dynamodb::error::SdkError<E, aws_sdk_dynamodb::config::http::HttpResponse>,
) -> String {
    aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string()
}

#[async_trait::async_trait]
impl Store for Dynamo {
    async fn plan_rows(
        &self,
        team: &str,
        plan_id: &str,
    ) -> Result<Option<(Item, Vec<Item>)>, String> {
        let prefix = format!("PLAN#{plan_id}");
        let (items, _) = crate::paged::all_pages(
            |start| {
                let q = self
                    .dynamo
                    .query()
                    .table_name(&self.plans_table)
                    .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                    .expression_attribute_values(":pk", AttributeValue::S(team.to_string()))
                    .expression_attribute_values(":p", AttributeValue::S(prefix.clone()))
                    .set_exclusive_start_key(start);
                async move {
                    let out = q.send().await.map_err(err)?;
                    Ok::<_, String>((out.items.unwrap_or_default(), out.last_evaluated_key))
                }
            },
            20,
        )
        .await?;
        let mut plan = None;
        let mut tasks = Vec::new();
        for item in items {
            let sk = s(&item, "sk");
            if sk == prefix {
                plan = Some(item);
            } else if sk.starts_with(&format!("{prefix}#TASK#")) {
                tasks.push(item);
            }
        }
        Ok(plan.map(|p| (p, tasks)))
    }

    async fn runs_of_ticket(&self, team: &str, ticket_id: &str) -> Result<Vec<Item>, String> {
        let (items, _) = crate::paged::all_pages(
            |start| {
                let q = self
                    .dynamo
                    .query()
                    .table_name(&self.runs_table)
                    .index_name("ticket-index")
                    .key_condition_expression("team_id = :t AND ticket_id = :k")
                    .expression_attribute_values(":t", AttributeValue::S(team.to_string()))
                    .expression_attribute_values(":k", AttributeValue::S(ticket_id.to_string()))
                    .set_exclusive_start_key(start);
                async move {
                    let out = q.send().await.map_err(err)?;
                    Ok::<_, String>((out.items.unwrap_or_default(), out.last_evaluated_key))
                }
            },
            10,
        )
        .await?;
        // The index may project only some attributes: read each run whole.
        let mut out = Vec::with_capacity(items.len());
        for i in items {
            let run_id = s(&i, "run_id");
            if run_id.is_empty() {
                continue;
            }
            let full = self
                .dynamo
                .get_item()
                .table_name(&self.runs_table)
                .key("team_id", AttributeValue::S(team.to_string()))
                .key("run_id", AttributeValue::S(run_id))
                .send()
                .await
                .map_err(err)?
                .item;
            out.push(full.unwrap_or(i));
        }
        Ok(out)
    }

    async fn write_task(
        &self,
        team: &str,
        plan_id: &str,
        task_id: &str,
        fields: Vec<(String, AttributeValue)>,
    ) -> Result<(), String> {
        self.set(&format!("PLAN#{plan_id}#TASK#{task_id}"), team, fields)
            .await
    }

    async fn write_plan(
        &self,
        team: &str,
        plan_id: &str,
        fields: Vec<(String, AttributeValue)>,
    ) -> Result<(), String> {
        self.set(&format!("PLAN#{plan_id}"), team, fields).await
    }

    async fn stamp_run(
        &self,
        team: &str,
        run_id: &str,
        plan_id: &str,
        task_id: &str,
        plan_title: &str,
    ) -> Result<(), String> {
        let mut u = self
            .dynamo
            .update_item()
            .table_name(&self.runs_table)
            .key("team_id", AttributeValue::S(team.to_string()))
            .key("run_id", AttributeValue::S(run_id.to_string()))
            .condition_expression(
                "attribute_exists(run_id) AND (attribute_not_exists(plan_id) OR plan_id = :p)",
            )
            .expression_attribute_values(":p", AttributeValue::S(plan_id.to_string()))
            .expression_attribute_values(":t", AttributeValue::S(task_id.to_string()));
        let mut set = vec!["plan_id = :p", "plan_task_id = :t"];
        if !plan_title.is_empty() {
            set.push("plan_title = if_not_exists(plan_title, :pt)");
            u = u.expression_attribute_values(":pt", AttributeValue::S(plan_title.to_string()));
        }
        u.update_expression(format!("SET {}", set.join(", ")))
            .send()
            .await
            .map(|_| ())
            .map_err(err)
    }

    async fn ensure_link(
        &self,
        team: &str,
        link_sk: &str,
        plan_id: &str,
        task_id: &str,
        plan_title: &str,
    ) -> Result<(), String> {
        let mut put = self
            .dynamo
            .put_item()
            .table_name(&self.settings_table)
            .item("pk", AttributeValue::S(team.to_string()))
            .item("sk", AttributeValue::S(link_sk.to_string()))
            .item("plan_id", AttributeValue::S(plan_id.to_string()))
            .item("task_id", AttributeValue::S(task_id.to_string()))
            .condition_expression("attribute_not_exists(sk)");
        if !plan_title.is_empty() {
            put = put.item("plan_title", AttributeValue::S(plan_title.to_string()));
        }
        put.send().await.map(|_| ()).map_err(err)
    }
}

impl Dynamo {
    async fn set(
        &self,
        sk: &str,
        team: &str,
        fields: Vec<(String, AttributeValue)>,
    ) -> Result<(), String> {
        if fields.is_empty() {
            return Ok(());
        }
        let mut u = self
            .dynamo
            .update_item()
            .table_name(&self.plans_table)
            .key("pk", AttributeValue::S(team.to_string()))
            .key("sk", AttributeValue::S(sk.to_string()))
            .condition_expression("attribute_exists(sk)");
        let mut parts = Vec::new();
        for (i, (k, v)) in fields.into_iter().enumerate() {
            parts.push(format!("#f{i} = :v{i}"));
            u = u
                .expression_attribute_names(format!("#f{i}"), k)
                .expression_attribute_values(format!(":v{i}"), v);
        }
        u.update_expression(format!("SET {}", parts.join(", ")))
            .send()
            .await
            .map(|_| ())
            .map_err(err)
    }
}

/// The recent finished runs of a team, `(ticket_id, cost)`, newest first.
pub async fn recent_costs(
    dynamo: &aws_sdk_dynamodb::Client,
    runs_table: &str,
    team: &str,
) -> Vec<(String, f64)> {
    let out = dynamo
        .query()
        .table_name(runs_table)
        .key_condition_expression("team_id = :t")
        .expression_attribute_values(":t", AttributeValue::S(team.to_string()))
        .projection_expression("ticket_id, cost_usd, #s")
        .expression_attribute_names("#s", "status")
        .scan_index_forward(false)
        .limit(200)
        .send()
        .await;
    let Ok(out) = out else { return Vec::new() };
    out.items()
        .iter()
        .filter(|i| {
            matches!(
                s(i, "status").as_str(),
                "completed" | "success" | "merged" | "failed" | "closed" | "retried"
            )
        })
        .map(|i| (s(i, "ticket_id"), n_f64(i, "cost_usd")))
        .collect()
}

/// Plan rows and runs in memory (tests and the incident cases).
#[derive(Default)]
pub struct Memory {
    pub plans: std::sync::Mutex<HashMap<(String, String), Item>>,
    pub runs: std::sync::Mutex<Vec<(String, Item)>>,
    pub links: std::sync::Mutex<HashMap<(String, String), (String, String)>>,
}

fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Memory {
    pub fn put_row(&self, team: &str, item: Item) {
        let sk = s(&item, "sk");
        lock(&self.plans).insert((team.to_string(), sk), item);
    }

    pub fn put_run(&self, team: &str, item: Item) {
        let id = s(&item, "run_id");
        let mut runs = lock(&self.runs);
        runs.retain(|(t, r)| !(t == team && s(r, "run_id") == id));
        runs.push((team.to_string(), item));
    }

    pub fn run(&self, team: &str, run_id: &str) -> Option<Item> {
        lock(&self.runs)
            .iter()
            .find(|(t, r)| t == team && s(r, "run_id") == run_id)
            .map(|(_, r)| r.clone())
    }

    pub fn row(&self, team: &str, sk: &str) -> Option<Item> {
        lock(&self.plans)
            .get(&(team.to_string(), sk.to_string()))
            .cloned()
    }

    pub fn link(&self, team: &str, link_sk: &str) -> Option<(String, String)> {
        lock(&self.links)
            .get(&(team.to_string(), link_sk.to_string()))
            .cloned()
    }
}

#[async_trait::async_trait]
impl Store for Memory {
    async fn plan_rows(
        &self,
        team: &str,
        plan_id: &str,
    ) -> Result<Option<(Item, Vec<Item>)>, String> {
        let rows = lock(&self.plans);
        let prefix = format!("PLAN#{plan_id}");
        let plan = rows.get(&(team.to_string(), prefix.clone())).cloned();
        let tasks = rows
            .iter()
            .filter(|((t, sk), _)| t == team && sk.starts_with(&format!("{prefix}#TASK#")))
            .map(|(_, v)| v.clone())
            .collect();
        Ok(plan.map(|p| (p, tasks)))
    }

    async fn runs_of_ticket(&self, team: &str, ticket_id: &str) -> Result<Vec<Item>, String> {
        Ok(lock(&self.runs)
            .iter()
            .filter(|(t, r)| t == team && s(r, "ticket_id").eq_ignore_ascii_case(ticket_id))
            .map(|(_, r)| r.clone())
            .collect())
    }

    async fn write_task(
        &self,
        team: &str,
        plan_id: &str,
        task_id: &str,
        fields: Vec<(String, AttributeValue)>,
    ) -> Result<(), String> {
        let mut rows = lock(&self.plans);
        let row = rows
            .get_mut(&(team.to_string(), format!("PLAN#{plan_id}#TASK#{task_id}")))
            .ok_or("no task")?;
        row.extend(fields);
        Ok(())
    }

    async fn write_plan(
        &self,
        team: &str,
        plan_id: &str,
        fields: Vec<(String, AttributeValue)>,
    ) -> Result<(), String> {
        let mut rows = lock(&self.plans);
        let row = rows
            .get_mut(&(team.to_string(), format!("PLAN#{plan_id}")))
            .ok_or("no plan")?;
        row.extend(fields);
        Ok(())
    }

    async fn stamp_run(
        &self,
        team: &str,
        run_id: &str,
        plan_id: &str,
        task_id: &str,
        plan_title: &str,
    ) -> Result<(), String> {
        let mut runs = lock(&self.runs);
        let (_, r) = runs
            .iter_mut()
            .find(|(t, r)| t == team && s(r, "run_id") == run_id)
            .ok_or("no run")?;
        let named = s(r, "plan_id");
        if !named.is_empty() && named != plan_id {
            return Err("another plan's run".into());
        }
        r.insert("plan_id".into(), AttributeValue::S(plan_id.into()));
        r.insert("plan_task_id".into(), AttributeValue::S(task_id.into()));
        if !plan_title.is_empty() {
            r.entry("plan_title".into())
                .or_insert_with(|| AttributeValue::S(plan_title.into()));
        }
        Ok(())
    }

    async fn ensure_link(
        &self,
        team: &str,
        link_sk: &str,
        plan_id: &str,
        task_id: &str,
        _plan_title: &str,
    ) -> Result<(), String> {
        let mut links = lock(&self.links);
        let key = (team.to_string(), link_sk.to_string());
        if links.contains_key(&key) {
            return Err("exists".into());
        }
        links.insert(key, (plan_id.to_string(), task_id.to_string()));
        Ok(())
    }
}

/// A task's state as stored, for readers that do not refresh.
pub fn stored_task_state(item: &Item) -> Option<TaskState> {
    TaskState::from_code(&s(item, "state"))
}

/// A task's drift marks as derived, for tests and readers.
pub fn drift_of(detail: &TaskDetail) -> &[Drift] {
    &detail.view.drift
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEAM: &str = "TEAM#1";

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| {
                let v = match *k {
                    "task_order" | "cost_usd" | "issue_number" => AttributeValue::N(v.to_string()),
                    _ => AttributeValue::S(v.to_string()),
                };
                (k.to_string(), v)
            })
            .collect()
    }

    fn store() -> Memory {
        let m = Memory::default();
        m.put_row(
            TEAM,
            item(&[
                ("sk", "PLAN#P1"),
                ("plan_id", "P1"),
                ("title", "Rate-limit the login form"),
                ("repo", "example-org/web"),
                ("status", "done"),
                ("executed_at", "2026-11-01T00:00:00Z"),
            ]),
        );
        // Filed and marked done the moment its ticket was filed, the old way.
        m.put_row(
            TEAM,
            item(&[
                ("sk", "PLAN#P1#TASK#T1"),
                ("task_id", "T1"),
                ("title", "Limit attempts"),
                ("status", "done"),
                ("task_order", "0"),
                ("jira_ticket_key", "ENG-1"),
            ]),
        );
        m.put_row(
            TEAM,
            item(&[
                ("sk", "PLAN#P1#TASK#T2"),
                ("task_id", "T2"),
                ("title", "Show when to retry"),
                ("status", "done"),
                ("task_order", "1"),
                ("issue_url", "https://github.com/example-org/web/issues/7"),
                ("issue_number", "7"),
            ]),
        );
        m
    }

    #[tokio::test]
    async fn backfill_links_the_runs_and_recomputes_the_states() {
        let m = store();
        m.put_run(
            TEAM,
            item(&[
                ("run_id", "R1"),
                ("ticket_id", "ENG-1"),
                ("repo", "example-org/web"),
                ("status", "completed"),
                ("created_at", "2026-11-01T01:00:00Z"),
                ("pr_url", "https://github.com/example-org/web/pull/31"),
                ("cost_usd", "3.5"),
            ]),
        );
        // The same issue number in another repository is not this task's.
        m.put_run(
            TEAM,
            item(&[
                ("run_id", "RX"),
                ("ticket_id", "GH-7"),
                ("repo", "example-org/other"),
                ("status", "merged"),
                ("created_at", "2026-11-01T01:00:00Z"),
            ]),
        );
        let out = refresh(&m, TEAM, "P1", "2026-11-02T00:00:00Z")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.runs_linked, 1);
        assert_eq!(out.links_written, 2);
        assert_eq!(out.tasks[0].view.state, TaskState::InReview);
        assert_eq!(out.tasks[1].view.state, TaskState::Queued);
        assert_eq!(
            out.plan.state,
            PlanState::Running,
            "never Done with nothing merged"
        );
        let run = m.run(TEAM, "R1").unwrap();
        assert_eq!(s(&run, "plan_id"), "P1");
        assert_eq!(s(&run, "plan_task_id"), "T1");
        assert!(s(&m.run(TEAM, "RX").unwrap(), "plan_id").is_empty());
        let plan = m.row(TEAM, "PLAN#P1").unwrap();
        assert_eq!(s(&plan, "state"), "running");
        assert_eq!(s(&plan, "status"), "executing");
        let st = stored(&plan);
        assert!(st.current && st.counts.in_review == 1 && st.counts.queued == 1);
        assert!((st.cost_usd - 3.5).abs() < 1e-9);
        assert_eq!(
            s(&m.row(TEAM, "PLAN#P1#TASK#T1").unwrap(), "state"),
            "in_review"
        );
        assert_eq!(
            m.link(TEAM, &crate::plan_dependents::jira_link_sk("ENG-1")),
            Some(("P1".into(), "T1".into()))
        );

        // Idempotent: a second refresh links nothing new and changes nothing.
        let again = refresh(&m, TEAM, "P1", "2026-11-02T00:00:00Z")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((again.runs_linked, again.links_written), (0, 0));
        assert_eq!(
            s(&m.row(TEAM, "PLAN#P1").unwrap(), "derived_fp"),
            s(&plan, "derived_fp")
        );

        // The PR merges and the other task's run appears: the plan moves.
        let mut r1 = m.run(TEAM, "R1").unwrap();
        r1.insert("pr_outcome".into(), AttributeValue::S("merged".into()));
        r1.insert("status".into(), AttributeValue::S("merged".into()));
        m.put_run(TEAM, r1);
        m.put_run(
            TEAM,
            item(&[
                ("run_id", "R2"),
                ("ticket_id", "GH-7"),
                ("repo", "example-org/web"),
                ("status", "failed"),
                ("error_message", "The tests time out"),
                ("created_at", "2026-11-01T02:00:00Z"),
            ]),
        );
        let out = refresh(&m, TEAM, "P1", "2026-11-02T00:00:00Z")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.tasks[0].view.state, TaskState::Merged);
        assert_eq!(out.tasks[1].view.state, TaskState::Failed);
        assert_eq!(out.tasks[1].view.reason, "The tests time out");
        assert_eq!(out.plan.state, PlanState::NeedsYou);
        assert_eq!(out.plan.needs.len(), 1);
        assert_eq!(stored(&m.row(TEAM, "PLAN#P1").unwrap()).needs_count, 1);
    }

    #[test]
    fn the_list_refreshes_rows_written_before_and_stale_open_ones() {
        let old = item(&[("sk", "PLAN#P1"), ("status", "done")]);
        assert!(needs_refresh(&old, "2026-11-02T00:00:00Z", 10));
        let mut cur = old.clone();
        cur.insert(
            "derived_v".into(),
            AttributeValue::N(DERIVED_VERSION.to_string()),
        );
        cur.insert("state".into(), AttributeValue::S("done".into()));
        cur.insert(
            "derived_at".into(),
            AttributeValue::S("2026-11-01T00:00:00Z".into()),
        );
        assert!(!needs_refresh(&cur, "2026-11-02T00:00:00Z", 10));
        cur.insert("state".into(), AttributeValue::S("running".into()));
        assert!(needs_refresh(&cur, "2026-11-02T00:00:00Z", 10));
        assert!(!needs_refresh(&cur, "2026-11-01T00:05:00Z", 10));
    }

    #[test]
    fn the_estimate_reads_recent_costs_and_attempts() {
        let none = estimate(3, &[]);
        assert_eq!((none.runs_low, none.sample), (3, 0));
        let recent: Vec<(String, f64)> = vec![
            ("A".into(), 2.0),
            ("A".into(), 4.0),
            ("B".into(), 6.0),
            ("C".into(), 8.0),
        ];
        let e = estimate(2, &recent);
        assert_eq!(e.runs_low, 2);
        assert_eq!(e.runs_high, 3, "4 runs over 3 tickets");
        assert!(e.per_run_low > 0.0 && e.per_run_high >= e.per_run_low);
        assert!(e.cost_high >= e.cost_low);
    }

    #[test]
    fn a_task_names_its_ticket_by_key_or_by_issue() {
        let t = task_ticket(&item(&[("jira_ticket_key", "eng-4")])).unwrap();
        assert_eq!(t.ticket_id, "ENG-4");
        let t = task_ticket(&item(&[(
            "issue_url",
            "https://github.com/Example-Org/Web/issues/12",
        )]))
        .unwrap();
        assert_eq!(
            (t.ticket_id.as_str(), t.repo.as_deref()),
            ("GH-12", Some("example-org/web"))
        );
        assert!(task_ticket(&item(&[("jira_ticket_key", "UNKNOWN")])).is_none());
        let t = task_ticket(&item(&[("jira_ticket_key", "GH-3"), ("repo", "o/r")])).unwrap();
        assert_eq!(t.repo.as_deref(), Some("o/r"));
    }
}
