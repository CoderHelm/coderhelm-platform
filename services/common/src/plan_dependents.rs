//! Which waiting plan tasks a merged run releases.
//!
//! A plan's task can wait on another task. When the run of the task it waits
//! on merges its pull request, the waiting task starts. The merged run's
//! task is found through its ticket's plan link ([`link_sk`]), the
//! settings-table row written when the plan opened the ticket, which names
//! the plan and the task.
//!
//! The task was once found by issue number alone. A Jira ticket's run has
//! no issue number (it is recorded as 0), so a merged Jira task never
//! released its own dependents and could release those of the first task
//! with no issue number, in any plan. GitHub issue numbers repeat across
//! repositories, so a merge in one repository could release the dependents
//! of a task in another.

use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
use std::sync::Mutex;

/// Settings-table sort key prefix of the link from a ticket to its plan task.
pub const LINK_SK_PREFIX: &str = "PLAN_TICKET#";
/// Status of a task that waits on another task's merge.
pub const WAITING: &str = "waiting";

/// Settings-table sort key of the link from a GitHub issue to its plan task.
pub fn github_link_sk(repo: &str, issue_number: u64) -> String {
    format!(
        "{LINK_SK_PREFIX}github#{}#{issue_number}",
        repo.to_lowercase()
    )
}

/// Settings-table sort key of the link from a Jira ticket to its plan task.
pub fn jira_link_sk(key: &str) -> String {
    format!("{LINK_SK_PREFIX}jira#{}", key.to_uppercase())
}

/// The ticket a merged run worked, as its run record holds it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergedRun {
    /// `github`, `jira` or `alert`.
    pub source: String,
    /// The Jira key, or `GH-<n>` for a GitHub issue.
    pub ticket_id: String,
    /// `owner/name` the run worked in (a GitHub issue's repository).
    pub repo: String,
    /// The GitHub issue number; 0 for a ticket that is not a GitHub issue.
    pub issue_number: u64,
}

impl MergedRun {
    /// The ticket fields of a run record.
    pub fn from_item(item: &HashMap<String, AttributeValue>) -> Self {
        let s = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        MergedRun {
            source: s("ticket_source"),
            ticket_id: s("ticket_id"),
            repo: s("repo"),
            issue_number: item
                .get("issue_number")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0),
        }
    }
}

/// Settings-table sort key of the plan link of the run's ticket, the same
/// key the plan wrote when it opened the ticket. None when the run's ticket
/// cannot have one: issue number 0 names no GitHub issue.
pub fn link_sk(run: &MergedRun) -> Option<String> {
    match run.source.as_str() {
        "jira" => {
            let key = run.ticket_id.trim();
            (!key.is_empty() && key != "UNKNOWN").then(|| jira_link_sk(key))
        }
        "github" => (run.issue_number > 0 && !run.repo.trim().is_empty())
            .then(|| github_link_sk(run.repo.trim(), run.issue_number)),
        _ => None,
    }
}

/// A task of a plan, as far as releasing it goes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Task {
    pub task_id: String,
    pub status: String,
    /// The task this one waits on ("" for none).
    pub depends_on: String,
}

/// The waiting tasks a merge releases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    pub plan_id: String,
    /// The merged run's task.
    pub task_id: String,
    /// The tasks that waited on it.
    pub tasks: Vec<String>,
}

/// Where plan links and plan tasks are read.
#[async_trait::async_trait]
pub trait PlanStore: Send + Sync {
    /// `(plan_id, task_id)` the link at `link_sk` names, if it exists.
    async fn linked_task(
        &self,
        team: &str,
        link_sk: &str,
    ) -> Result<Option<(String, String)>, String>;
    /// Every task of a plan.
    async fn plan_tasks(&self, team: &str, plan_id: &str) -> Result<Vec<Task>, String>;
}

/// The waiting tasks a merged run releases: those that wait on the task its
/// ticket's plan link names. None when the ticket has no plan link or no
/// task waits on it.
pub async fn dependents_to_release(
    store: &dyn PlanStore,
    team: &str,
    run: &MergedRun,
) -> Result<Option<Release>, String> {
    let Some(sk) = link_sk(run) else {
        return Ok(None);
    };
    let Some((plan_id, task_id)) = store.linked_task(team, &sk).await? else {
        return Ok(None);
    };
    if plan_id.is_empty() || task_id.is_empty() {
        return Ok(None);
    }
    let tasks: Vec<String> = store
        .plan_tasks(team, &plan_id)
        .await?
        .into_iter()
        .filter(|t| t.status == WAITING && t.depends_on == task_id)
        .map(|t| t.task_id)
        .collect();
    Ok((!tasks.is_empty()).then_some(Release {
        plan_id,
        task_id,
        tasks,
    }))
}

/// The settings table (plan links) and the plans table (tasks).
pub struct DynamoPlans {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub settings_table: String,
    pub plans_table: String,
}

#[async_trait::async_trait]
impl PlanStore for DynamoPlans {
    async fn linked_task(
        &self,
        team: &str,
        link_sk: &str,
    ) -> Result<Option<(String, String)>, String> {
        let item = self
            .dynamo
            .get_item()
            .table_name(&self.settings_table)
            .key("pk", AttributeValue::S(team.to_string()))
            .key("sk", AttributeValue::S(link_sk.to_string()))
            .projection_expression("plan_id, task_id")
            .send()
            .await
            .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string())?
            .item;
        Ok(item.map(|i| (s_of(&i, "plan_id"), s_of(&i, "task_id"))))
    }

    async fn plan_tasks(&self, team: &str, plan_id: &str) -> Result<Vec<Task>, String> {
        let prefix = format!("PLAN#{plan_id}#TASK#");
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
                    let out = q.send().await.map_err(|e| {
                        aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string()
                    })?;
                    Ok::<_, String>((out.items.unwrap_or_default(), out.last_evaluated_key))
                }
            },
            100,
        )
        .await?;
        Ok(items
            .iter()
            .filter_map(|i| {
                let sk = s_of(i, "sk");
                let task_id = sk.strip_prefix(&prefix)?;
                Some(Task {
                    task_id: task_id.to_string(),
                    status: s_of(i, "status"),
                    depends_on: s_of(i, "depends_on"),
                })
            })
            .collect())
    }
}

fn s_of(item: &HashMap<String, AttributeValue>, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// Plan links and tasks in memory (tests).
#[derive(Default)]
pub struct MemoryPlans {
    /// `(team, link_sk)` → `(plan_id, task_id)`.
    pub links: Mutex<HashMap<(String, String), (String, String)>>,
    /// `(team, plan_id)` → tasks.
    pub tasks: Mutex<HashMap<(String, String), Vec<Task>>>,
}

impl MemoryPlans {
    /// Links a ticket to its plan task.
    pub fn link(&self, team: &str, link_sk: &str, plan_id: &str, task_id: &str) {
        self.links.lock().unwrap_or_else(|e| e.into_inner()).insert(
            (team.to_string(), link_sk.to_string()),
            (plan_id.to_string(), task_id.to_string()),
        );
    }

    /// Adds a task to a plan.
    pub fn task(&self, team: &str, plan_id: &str, task_id: &str, status: &str, depends_on: &str) {
        self.tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry((team.to_string(), plan_id.to_string()))
            .or_default()
            .push(Task {
                task_id: task_id.to_string(),
                status: status.to_string(),
                depends_on: depends_on.to_string(),
            });
    }
}

#[async_trait::async_trait]
impl PlanStore for MemoryPlans {
    async fn linked_task(
        &self,
        team: &str,
        link_sk: &str,
    ) -> Result<Option<(String, String)>, String> {
        Ok(self
            .links
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(team.to_string(), link_sk.to_string()))
            .cloned())
    }

    async fn plan_tasks(&self, team: &str, plan_id: &str) -> Result<Vec<Task>, String> {
        Ok(self
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(team.to_string(), plan_id.to_string()))
            .cloned()
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: &str = "TEAM#acme";

    fn jira_run(key: &str) -> MergedRun {
        MergedRun {
            source: "jira".into(),
            ticket_id: key.into(),
            repo: "acme/web".into(),
            issue_number: 0,
        }
    }

    fn github_run(repo: &str, n: u64) -> MergedRun {
        MergedRun {
            source: "github".into(),
            ticket_id: format!("GH-{n}"),
            repo: repo.into(),
            issue_number: n,
        }
    }

    #[tokio::test]
    async fn a_merged_jira_task_releases_its_own_dependents() {
        let s = MemoryPlans::default();
        // Another plan's GitHub task that has no issue yet (issue number 0),
        // with a task waiting on it.
        s.task(T, "p-other", "t-draft", "draft", "");
        s.task(T, "p-other", "t-other-next", WAITING, "t-draft");
        // The Jira plan: t2 waits on t1, whose ticket is PROJ-7.
        s.task(T, "p-jira", "t1", "running", "");
        s.task(T, "p-jira", "t2", WAITING, "t1");
        s.link(T, &jira_link_sk("PROJ-7"), "p-jira", "t1");

        let got = dependents_to_release(&s, T, &jira_run("PROJ-7"))
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(Release {
                plan_id: "p-jira".into(),
                task_id: "t1".into(),
                tasks: vec!["t2".into()],
            })
        );
    }

    #[tokio::test]
    async fn the_same_issue_number_in_another_repository_releases_nothing() {
        let s = MemoryPlans::default();
        // One plan spans two repositories; both tasks' issues are #12.
        s.task(T, "p1", "t-web", "running", "");
        s.task(T, "p1", "t-web-next", WAITING, "t-web");
        s.task(T, "p1", "t-api", "running", "");
        s.task(T, "p1", "t-api-next", WAITING, "t-api");
        s.link(T, &github_link_sk("acme/web", 12), "p1", "t-web");
        s.link(T, &github_link_sk("acme/api", 12), "p1", "t-api");

        let got = dependents_to_release(&s, T, &github_run("Acme/API", 12))
            .await
            .unwrap()
            .expect("the api task's dependents are released");
        assert_eq!(got.task_id, "t-api");
        assert_eq!(got.tasks, vec!["t-api-next".to_string()]);
    }

    #[tokio::test]
    async fn a_merge_whose_ticket_has_no_plan_link_releases_nothing() {
        let s = MemoryPlans::default();
        // A plan task whose issue is #5 in acme/web, linked there.
        s.task(T, "p1", "t1", "running", "");
        s.task(T, "p1", "t2", WAITING, "t1");
        s.link(T, &github_link_sk("acme/web", 5), "p1", "t1");

        // Issue #5 in another repository, and a Jira ticket, neither from a plan.
        for run in [github_run("acme/api", 5), jira_run("OPS-1")] {
            assert_eq!(dependents_to_release(&s, T, &run).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn issue_number_zero_never_matches() {
        let s = MemoryPlans::default();
        s.task(T, "p1", "t1", "draft", "");
        s.task(T, "p1", "t2", WAITING, "t1");
        s.link(T, &github_link_sk("acme/web", 0), "p1", "t1");
        for run in [
            github_run("acme/web", 0),
            MergedRun::default(),
            jira_run(""),
            MergedRun {
                source: "alert".into(),
                ..github_run("acme/web", 0)
            },
        ] {
            assert_eq!(dependents_to_release(&s, T, &run).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn only_waiting_tasks_on_the_merged_task_are_released() {
        let s = MemoryPlans::default();
        s.task(T, "p1", "t1", "running", "");
        s.task(T, "p1", "t2", WAITING, "t1");
        s.task(T, "p1", "t3", "running", "t1");
        s.task(T, "p1", "t4", WAITING, "t2");
        s.task(T, "p1", "t5", WAITING, "t1");
        s.link(T, &github_link_sk("acme/web", 3), "p1", "t1");
        let got = dependents_to_release(&s, T, &github_run("acme/web", 3))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.tasks, vec!["t2".to_string(), "t5".to_string()]);
    }

    #[test]
    fn a_run_record_gives_the_link_the_plan_wrote() {
        let item: HashMap<String, AttributeValue> = [
            ("ticket_source", AttributeValue::S("github".into())),
            ("ticket_id", AttributeValue::S("GH-9".into())),
            ("repo", AttributeValue::S("Acme/Web".into())),
            ("issue_number", AttributeValue::N("9".into())),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let run = MergedRun::from_item(&item);
        assert_eq!(
            link_sk(&run).as_deref(),
            Some("PLAN_TICKET#github#acme/web#9")
        );
        assert_eq!(
            link_sk(&jira_run("proj-7")).as_deref(),
            Some("PLAN_TICKET#jira#PROJ-7")
        );
    }
}
