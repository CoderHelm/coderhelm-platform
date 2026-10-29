//! When a fix started from a log finding also needs a change outside the
//! repository it runs in (the infrastructure code that defines the task, its
//! storage or its alarms), the run opens that pull request too, in the
//! team's repository that defines the resource, linked to the first one.
//!
//! The repository is found from data: the team's connected repositories
//! (their roles and descriptions decide only the order they are searched
//! in), and a code search of each for the names the evidence step read
//! (task family, service, cluster, log group) in Terraform, CDK and
//! CloudFormation files. A repository is picked only when its
//! infrastructure code names the resource. When none does, the "also needed
//! outside the code" note stays and names what was searched.
//!
//! Opening a pull request in a second repository asks the team's agent
//! permissions first (`open_pull_request`, a write), scoped to that
//! repository.

use super::fix_location::{EndToEndStep, FixDecision, FixLocation};
use super::log_evidence::LogEvidence;
use super::plan::{PlanResult, RepoTasks};

/// One connected repository.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
pub struct RepoInfo {
    pub full_name: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub description: String,
}

/// A search hit: a file of `repo` that names `term`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct CodeHit {
    pub repo: String,
    pub path: String,
    pub term: String,
}

/// What the search found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Found {
    Repo {
        repo: String,
        /// The infrastructure files that name the resource.
        files: Vec<String>,
        terms: Vec<String>,
    },
    None {
        searched: Vec<String>,
        terms: Vec<String>,
    },
}

/// Most repositories searched, and names searched for.
const MAX_REPOS: usize = 8;
const MAX_TERMS: usize = 4;

/// Pure: whether a file is infrastructure code: Terraform, CloudFormation
/// or a CDK stack.
pub fn is_iac_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    let file = p.rsplit('/').next().unwrap_or(&p);
    p.ends_with(".tf")
        || p.ends_with(".tf.json")
        || p.ends_with(".tfvars")
        || p.ends_with(".hcl")
        || ((p.ends_with(".yaml") || p.ends_with(".yml") || p.ends_with(".json"))
            && (p.contains("cloudformation") || p.contains("cfn") || file.contains("template")))
        || ((p.ends_with(".ts")
            || p.ends_with(".py")
            || p.ends_with(".go")
            || p.ends_with(".java"))
            && (file.contains("stack") || p.contains("/cdk/") || p.starts_with("cdk/")))
}

/// Pure: the names to search for, from the evidence: the task family, the
/// service, the log group, the cluster; distinct, at least 4 characters.
pub fn search_terms(ev: &LogEvidence) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut add = |s: &str| {
        let s = s.trim().to_string();
        if s.len() >= 4 && !out.contains(&s) {
            out.push(s);
        }
    };
    if let Some(td) = &ev.task_def {
        add(&td.family);
    }
    if let Some(svc) = &ev.service {
        add(&svc.name);
    }
    add(&ev.log_group);
    if let Some(svc) = &ev.service {
        add(&svc.cluster);
    }
    out.truncate(MAX_TERMS);
    out
}

fn infra_word(r: &RepoInfo) -> bool {
    let t = format!("{} {}", r.role, r.description).to_ascii_lowercase();
    [
        "infra",
        "terraform",
        "iac",
        "cloudformation",
        "cdk",
        "platform",
        "devops",
        "ops",
    ]
    .iter()
    .any(|w| t.contains(w))
}

/// Pure: the repositories to search, in order: enabled, not the run's own,
/// those whose role or description speaks of infrastructure first.
pub fn search_order(primary: &str, repos: &[RepoInfo]) -> Vec<String> {
    let mut v: Vec<&RepoInfo> = repos
        .iter()
        .filter(|r| r.enabled && !r.full_name.eq_ignore_ascii_case(primary))
        .collect();
    v.sort_by_key(|r| !infra_word(r));
    v.into_iter()
        .take(MAX_REPOS)
        .map(|r| r.full_name.clone())
        .collect()
}

/// Pure: the repository whose infrastructure code names the most of the
/// terms (ties go to the one whose role speaks of infrastructure, then the
/// search order). `None` when no infrastructure file names any.
pub fn pick(primary: &str, repos: &[RepoInfo], hits: &[CodeHit], terms: &[String]) -> Found {
    let order = search_order(primary, repos);
    let mut best: Option<(usize, bool, usize, String)> = None;
    for (i, repo) in order.iter().enumerate() {
        let mut named: Vec<&str> = hits
            .iter()
            .filter(|h| &h.repo == repo && is_iac_path(&h.path))
            .map(|h| h.term.as_str())
            .collect();
        named.sort();
        named.dedup();
        if named.is_empty() {
            continue;
        }
        let infra = repos
            .iter()
            .find(|r| &r.full_name == repo)
            .is_some_and(infra_word);
        let better = match &best {
            None => true,
            Some((n, inf, _, _)) => named.len() > *n || (named.len() == *n && infra && !inf),
        };
        if better {
            best = Some((named.len(), infra, i, repo.clone()));
        }
    }
    match best {
        Some((_, _, _, repo)) => {
            let mut files: Vec<String> = hits
                .iter()
                .filter(|h| h.repo == repo && is_iac_path(&h.path))
                .map(|h| h.path.clone())
                .collect();
            files.sort();
            files.dedup();
            let mut found_terms: Vec<String> = hits
                .iter()
                .filter(|h| h.repo == repo && is_iac_path(&h.path))
                .map(|h| h.term.clone())
                .collect();
            found_terms.sort();
            found_terms.dedup();
            Found::Repo {
                repo,
                files,
                terms: found_terms,
            }
        }
        None => Found::None {
            searched: order,
            terms: terms.to_vec(),
        },
    }
}

/// Pure: the sentence the outside change keeps when no repository defines
/// the resource.
pub fn not_found_note(searched: &[String], terms: &[String]) -> String {
    let names = terms
        .iter()
        .map(|t| format!("`{t}`"))
        .collect::<Vec<_>>()
        .join(", ");
    if searched.is_empty() {
        format!(
            "No other repository is connected to search for {names}; the change has to be made where the resource is defined."
        )
    } else {
        format!(
            "No connected repository defines it: searched {} for {names} in Terraform, CDK and CloudFormation files.",
            searched
                .iter()
                .map(|r| format!("`{r}`"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// Pure: the tasks for the infrastructure repository's pull request.
pub fn infra_tasks(decision: &FixDecision, files: &[String], terms: &[String]) -> String {
    let oc = decision.outside_change.clone().unwrap_or_default();
    let mut t = format!("- [ ] {}\n", oc.change.trim());
    if !oc.where_.trim().is_empty() {
        t.push_str(&format!("- [ ] Where: {}\n", oc.where_.trim()));
    }
    t.push_str(&format!(
        "- [ ] The resource is defined in {} (they name {}). Change it there, following this repository's AGENTS.md or CLAUDE.md, its module layout and naming.\n",
        files
            .iter()
            .take(6)
            .map(|f| format!("`{f}`"))
            .collect::<Vec<_>>()
            .join(", "),
        terms
            .iter()
            .map(|t| format!("`{t}`"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    t.push_str("- [ ] Size every new value (storage, alarm threshold, period) from the measured numbers in the evidence, and put the arithmetic in the PR description.\n");
    if !oc.verify.trim().is_empty() {
        t.push_str(&format!("- [ ] Verify: {}\n", oc.verify.trim()));
    }
    t
}

/// Pure: the plan made into two repositories' work: the run's own tasks,
/// then the infrastructure repository's. The outside change becomes that
/// pull request's step, so neither PR says "it has to be made there".
pub fn link_into_plan(
    plan: &mut PlanResult,
    primary: (&str, &str),
    infra: &str,
    files: &[String],
    terms: &[String],
) -> bool {
    let Some(decision) = plan.fix.clone() else {
        return false;
    };
    let Some((owner, name)) = infra.split_once('/') else {
        return false;
    };
    let change = decision
        .outside_change
        .as_ref()
        .map(|o| o.change.trim().to_string())
        .unwrap_or_default();
    if change.is_empty() {
        return false;
    }
    plan.repo_tasks = vec![
        RepoTasks {
            owner: primary.0.to_string(),
            name: primary.1.to_string(),
            tasks: plan.tasks.clone(),
        },
        RepoTasks {
            owner: owner.to_string(),
            name: name.to_string(),
            tasks: infra_tasks(&decision, files, terms),
        },
    ];
    let mut d = decision;
    d.fix_location = FixLocation::Code;
    d.outside_change = None;
    d.end_to_end.push(EndToEndStep {
        step: change,
        by: "other_repo".into(),
        owner: String::new(),
        repo: infra.to_string(),
    });
    plan.fix = Some(d);
    true
}

/// The team's connected repositories with their roles and descriptions.
pub async fn team_repos(state: &crate::WorkerState, team_id: &str) -> Vec<RepoInfo> {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let out = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", A::S(team_id.to_string()))
        .expression_attribute_values(":prefix", A::S("REPO#".to_string()))
        .send()
        .await;
    let s = |i: &std::collections::HashMap<String, A>, k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    match out {
        Ok(o) => o
            .items()
            .iter()
            .map(|i| RepoInfo {
                full_name: s(i, "repo_name"),
                enabled: i
                    .get("enabled")
                    .and_then(|v| v.as_bool().ok())
                    .copied()
                    .unwrap_or(false),
                role: s(i, "role"),
                description: s(i, "description"),
            })
            .filter(|r| !r.full_name.is_empty())
            .collect(),
        Err(e) => {
            tracing::warn!(error = %e, "Couldn't read the team's repositories");
            Vec::new()
        }
    }
}

/// Searches the team's other repositories for the evidence's names.
pub async fn locate(
    github: &crate::clients::github::GitHubClient,
    primary: &str,
    repos: &[RepoInfo],
    terms: &[String],
) -> Found {
    let mut hits = Vec::new();
    for repo in search_order(primary, repos) {
        let Some((owner, name)) = repo.split_once('/') else {
            continue;
        };
        for term in terms {
            if let Ok(found) = github.search_code(owner, name, "HEAD", term).await {
                hits.extend(found.into_iter().map(|r| CodeHit {
                    repo: repo.clone(),
                    path: r.path,
                    term: term.clone(),
                }));
            }
        }
    }
    pick(primary, repos, &hits, terms)
}

/// Whether the team's agent permissions let this run open a pull request in
/// `repo`. Asks (and waits for an answer within the run's budget) when the
/// policy says ask. `Err` carries why not.
pub async fn may_open_pr(
    state: &crate::WorkerState,
    provider: &crate::agent::provider::ModelProvider,
    repo: &str,
    deadline: Option<std::time::Instant>,
) -> Result<(), String> {
    use common::agent_policy::{Agent, Outcome, Request};
    let ctx = provider.scope.ctx(Agent::Coder).repo(repo);
    let gate = crate::agent::policy::gate(state, ctx, deadline);
    let args = serde_json::json!({ "repo": repo });
    match gate
        .check(Request {
            tool: "open_pull_request",
            args: &args,
            why: "the fix also needs a change in this repository",
        })
        .await
    {
        Outcome::Run { .. } => Ok(()),
        Outcome::Refused { message, .. } | Outcome::Pending { message, .. } => Err(message),
        Outcome::Stop(stop) => {
            crate::agent::policy::record_stop(&stop);
            Err("agents are stopped".into())
        }
    }
}

/// For a run started from a log finding whose fix needs a change outside
/// its repository: finds the repository that defines the resource and, when
/// the team's permissions allow it, plans that pull request alongside the
/// run's own. Otherwise the outside change keeps its note, naming what was
/// searched (or why the PR wasn't opened). Returns whether the plan now
/// covers both repositories.
#[allow(clippy::too_many_arguments)]
pub async fn link_outside_change(
    state: &crate::WorkerState,
    github: &crate::clients::github::GitHubClient,
    provider: &crate::agent::provider::ModelProvider,
    msg: &crate::models::TicketMessage,
    run_id: &str,
    plan: &mut PlanResult,
    ev: &LogEvidence,
    deadline: Option<std::time::Instant>,
) -> bool {
    let primary = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let terms = search_terms(ev);
    let repos = team_repos(state, &msg.team_id).await;
    let found = if terms.is_empty() {
        Found::None {
            searched: Vec::new(),
            terms: vec![ev.log_group.clone()],
        }
    } else {
        locate(github, &primary, &repos, &terms).await
    };
    let note = match found {
        Found::Repo { repo, files, terms } => match may_open_pr(state, provider, &repo, deadline).await {
            Ok(()) => {
                if link_into_plan(
                    plan,
                    (&msg.repo_owner, &msg.repo_name),
                    &repo,
                    &files,
                    &terms,
                ) {
                    super::add_progress_note(
                        state,
                        &msg.team_id,
                        run_id,
                        &format!(
                            "The change outside this repository goes to a linked pull request in {repo}: its infrastructure code ({}) defines the resource",
                            files.iter().take(3).cloned().collect::<Vec<_>>().join(", ")
                        ),
                    )
                    .await;
                    return true;
                }
                return false;
            }
            Err(why) => format!(
                "{repo} defines it ({}), but the team's agent permissions didn't allow a pull request there: {why}",
                files.iter().take(3).cloned().collect::<Vec<_>>().join(", ")
            ),
        },
        Found::None { searched, terms } => not_found_note(&searched, &terms),
    };
    super::add_progress_note(state, &msg.team_id, run_id, &note).await;
    if let Some(oc) = plan.fix.as_mut().and_then(|d| d.outside_change.as_mut()) {
        oc.where_ = format!("{} {note}", oc.where_.trim()).trim().to_string();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::fix_location::OutsideChange;

    fn repos() -> Vec<RepoInfo> {
        vec![
            RepoInfo {
                full_name: "acme/site".into(),
                enabled: true,
                role: "web".into(),
                ..Default::default()
            },
            RepoInfo {
                full_name: "acme/api".into(),
                enabled: true,
                role: "backend".into(),
                ..Default::default()
            },
            RepoInfo {
                full_name: "acme/deploy".into(),
                enabled: true,
                description: "Terraform for the ECS services and alarms".into(),
                ..Default::default()
            },
        ]
    }

    fn hit(repo: &str, path: &str, term: &str) -> CodeHit {
        CodeHit {
            repo: repo.into(),
            path: path.into(),
            term: term.into(),
        }
    }

    #[test]
    fn iac_paths() {
        assert!(is_iac_path("modules/ecs/main.tf"));
        assert!(is_iac_path("cfn/web.yaml"));
        assert!(is_iac_path("lib/web-stack.ts"));
        assert!(!is_iac_path("src/app/page.tsx"));
        assert!(!is_iac_path("docker-compose.yml"));
    }

    #[test]
    fn picks_the_repo_whose_infrastructure_code_names_the_resource() {
        let terms = vec!["site".to_string(), "/ecs/site".to_string()];
        let hits = vec![
            // Application code naming the log group doesn't count.
            hit("acme/api", "src/logger.ts", "/ecs/site"),
            hit("acme/deploy", "services/site/main.tf", "site"),
            hit("acme/deploy", "services/site/alarms.tf", "/ecs/site"),
        ];
        assert_eq!(
            search_order("acme/site", &repos())[0],
            "acme/deploy",
            "infrastructure repos are searched first"
        );
        match pick("acme/site", &repos(), &hits, &terms) {
            Found::Repo { repo, files, .. } => {
                assert_eq!(repo, "acme/deploy");
                assert_eq!(files.len(), 2);
            }
            f => panic!("{f:?}"),
        }
    }

    #[test]
    fn nothing_defining_it_keeps_the_note_with_what_was_searched() {
        let terms = vec!["site".to_string()];
        let f = pick(
            "acme/site",
            &repos(),
            &[hit("acme/api", "src/a.ts", "site")],
            &terms,
        );
        let Found::None { searched, terms } = f else {
            panic!()
        };
        let note = not_found_note(&searched, &terms);
        assert!(note.contains("`acme/deploy`") && note.contains("`acme/api`"));
        assert!(note.contains("`site`"));
        assert!(!note.contains("acme/site`,"));
    }

    #[test]
    fn linking_makes_two_repos_and_drops_the_outside_note() {
        let mut plan = PlanResult {
            proposal: String::new(),
            design: String::new(),
            tasks: "- [ ] Bound the cache".into(),
            spec: String::new(),
            repo_tasks: vec![],
            fix: Some(FixDecision {
                fix_location: FixLocation::Both,
                outside_change: Some(OutsideChange {
                    system: "infrastructure".into(),
                    change: "Raise the task's ephemeral storage and alarm on its use".into(),
                    where_: "the task definition".into(),
                    verify: "the alarm shows in CloudWatch".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        assert!(link_into_plan(
            &mut plan,
            ("acme", "site"),
            "acme/deploy",
            &["services/site/main.tf".into()],
            &["site".into()],
        ));
        assert_eq!(plan.repo_tasks.len(), 2);
        assert_eq!(plan.repo_tasks[1].name, "deploy");
        assert!(plan.repo_tasks[1].tasks.contains("`services/site/main.tf`"));
        let d = plan.fix.unwrap();
        assert_eq!(d.fix_location, FixLocation::Code);
        assert!(d.outside_change.is_none());
        assert!(d.end_to_end.iter().any(|s| s.in_pr("acme/deploy")));
    }
}
