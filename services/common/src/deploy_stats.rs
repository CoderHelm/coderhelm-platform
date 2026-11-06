//! Deploys CoderHelm started, read from what really ran on GitHub.
//!
//! A deploy is a GitHub Actions run of a deploy workflow. CoderHelm started
//! it when its own app is the run's triggering actor: a deploy label it
//! added, a push to its own PR, a merge it made (merge gate, auto-merge), a
//! tag or release it created. A run a person starts or re-runs is theirs and
//! is never counted, even on a commit CoderHelm made.
//!
//! The environment comes from the run, not from any label: the `environment:`
//! of the workflow's jobs that ran, else the workflow's own name when it says
//! it deploys, else the repository's deploy workflows setting.
//!
//! Each deploy is one record per run and environment, keyed by the run's
//! start time, so a period is one range query and writing it twice (webhook
//! and backfill) is the same record.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value;

pub const PREFIX: &str = "DRUN#";
/// Set on a team once every repository's earlier deploys were read.
pub const BACKFILL_MARKER_SK: &str = "ANALYTICS#deploy-runs-backfilled#v1";
/// Set per repository once its earlier deploys were read, so a backfill cut
/// short continues where it stopped (`{BACKFILL_REPO_PREFIX}{owner/name}`).
pub const BACKFILL_REPO_PREFIX: &str = "ANALYTICS#deploy-runs-backfilled#v1#repo#";
/// Set when the backfill was last asked for, so a page load asks at most hourly.
pub const BACKFILL_QUEUED_SK: &str = "ANALYTICS#deploy-runs-backfill-queued";
/// While the backfill runs: how many repositories it has read of how many
/// (`repos_read`, `repos_total`), and the lease of the one pass reading
/// (`lease`, `lease_until`), so a page load never starts a second chain.
pub const BACKFILL_PROGRESS_SK: &str = "ANALYTICS#deploy-runs-backfill-progress";
/// How far back the backfill reads: the longest period the page shows (90
/// days) and the one before it.
pub const BACKFILL_DAYS: i64 = 182;
/// Records outlive the analytics window.
const KEEP_DAYS: i64 = 400;

/// Where a deploy went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Env {
    Preview,
    Staging,
    Production,
    Other,
}

impl Env {
    pub fn as_str(self) -> &'static str {
        match self {
            Env::Preview => "preview",
            Env::Staging => "staging",
            Env::Production => "production",
            Env::Other => "other",
        }
    }
}

fn words(s: &str) -> Vec<String> {
    s.to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_string)
        .collect()
}

/// Which environment a name (an environment, a workflow, a job) names, when
/// it names one. Whole words, so "product-api" is not production.
pub fn env_of(name: &str) -> Option<Env> {
    let w = words(name);
    let has = |list: &[&str]| w.iter().any(|x| list.contains(&x.as_str()));
    let joined = w.join("-");
    if has(&["preprod", "staging", "stage", "stg", "uat"]) || joined.contains("pre-prod") {
        Some(Env::Staging)
    } else if has(&["prod", "production", "prd", "live"]) {
        Some(Env::Production)
    } else if has(&["preview", "previews", "review", "ephemeral", "pr"])
        || w.iter().any(|x| {
            x.strip_prefix("pr")
                .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
        })
    {
        Some(Env::Preview)
    } else {
        None
    }
}

/// An environment name's environment, `Other` when it names none of the three.
pub fn env_or_other(name: &str) -> Env {
    env_of(name).unwrap_or(Env::Other)
}

/// Whether a name says it deploys.
pub fn says_deploy(name: &str) -> bool {
    words(name)
        .iter()
        .any(|w| w.starts_with("deploy") || w == "cd")
}

/// How CoderHelm caused a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// A push to its own PR or a label it added (`pull_request` events).
    PullRequest,
    /// A merge it made into the default branch.
    Merge,
    /// A tag it created.
    Tag,
    /// A release it published.
    Release,
    /// A push to another branch, or a run chained from one of its runs.
    Other,
}

impl Cause {
    pub fn as_str(self) -> &'static str {
        match self {
            Cause::PullRequest => "pull_request",
            Cause::Merge => "merge",
            Cause::Tag => "tag",
            Cause::Release => "release",
            Cause::Other => "other",
        }
    }
}

/// The login that started a run: whoever triggered this attempt (a re-run's
/// person), else the actor of the event.
pub fn trigger(run: &Value) -> (&str, &str) {
    let t = &run["triggering_actor"];
    let a = if t["login"].is_string() {
        t
    } else {
        &run["actor"]
    };
    (
        a["login"].as_str().unwrap_or(""),
        a["type"].as_str().unwrap_or(""),
    )
}

/// What a run's event says CoderHelm did. `is_tag`: the pushed ref is a tag.
pub fn cause_of(run: &Value, default_branch: &str, is_tag: bool) -> Cause {
    let branch = run["head_branch"].as_str().unwrap_or("");
    match run["event"].as_str().unwrap_or("") {
        "pull_request" | "pull_request_target" => Cause::PullRequest,
        "push" if is_tag => Cause::Tag,
        "push" if !default_branch.is_empty() && branch == default_branch => Cause::Merge,
        "create" => Cause::Tag,
        "release" => Cause::Release,
        _ => Cause::Other,
    }
}

/// Whether CoderHelm started a run: its app triggered it. A person who
/// triggers or re-runs is never CoderHelm, whatever the commit.
pub fn started_by_coderhelm(run: &Value, bot_login: &str) -> bool {
    let (login, kind) = trigger(run);
    !bot_login.is_empty() && kind != "User" && login.eq_ignore_ascii_case(bot_login)
}

/// A run pushed to the default branch by another bot (a merge queue): it is
/// CoderHelm's when CoderHelm merged the PR the commit came from. `pulls` is
/// GitHub's list of PRs for the commit.
pub fn merged_by_coderhelm(pulls: &[Value], bot_login: &str) -> bool {
    !bot_login.is_empty()
        && pulls.iter().any(|p| {
            p["merged_by"]["login"]
                .as_str()
                .is_some_and(|l| l.eq_ignore_ascii_case(bot_login))
        })
}

/// One job of a workflow file: its key, display name, and `environment:` as
/// written (a name, or an expression).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JobDef {
    pub key: String,
    pub name: String,
    pub environment: String,
}

/// The jobs of a workflow file. `None` when it isn't YAML.
pub fn parse_workflow(yaml: &str) -> Option<Vec<JobDef>> {
    let doc: serde_yaml::Value = serde_yaml::from_str(yaml).ok()?;
    let jobs = doc.get("jobs")?.as_mapping()?;
    Some(
        jobs.iter()
            .filter_map(|(k, v)| {
                let key = k.as_str()?.to_string();
                let name = v
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or(&key)
                    .to_string();
                let env = v.get("environment");
                let environment = env
                    .and_then(|e| e.as_str())
                    .or_else(|| env.and_then(|e| e.get("name")).and_then(|n| n.as_str()))
                    .unwrap_or("")
                    .trim()
                    .to_string();
                Some(JobDef {
                    key,
                    name,
                    environment,
                })
            })
            .collect(),
    )
}

/// An `environment:` written as an expression names its choices in quotes
/// (`${{ github.event_name == 'push' && 'production' || 'preview' }}`): the
/// one this run used, by its event.
fn resolve_expression(raw: &str, run: &Value) -> Option<String> {
    if !raw.contains("${{") {
        return Some(raw.to_string());
    }
    let literals: Vec<&str> = raw
        .split('\'')
        .enumerate()
        .filter(|(i, _)| i % 2 == 1)
        .map(|(_, s)| s)
        .filter(|s| env_of(s).is_some())
        .collect();
    let on_pr = matches!(
        run["event"].as_str(),
        Some("pull_request" | "pull_request_target")
    );
    match literals.as_slice() {
        [] => None,
        [one] => Some(one.to_string()),
        many => many
            .iter()
            .find(|s| (env_of(s) == Some(Env::Preview)) == on_pr)
            .map(|s| s.to_string()),
    }
}

/// A job from GitHub's list of a run's jobs matches a workflow job by its
/// name (a matrix job is "name (value)").
fn job_ran<'a>(def: &JobDef, ran: &'a [Value]) -> Option<&'a Value> {
    ran.iter().find(|j| {
        let n = j["name"].as_str().unwrap_or("");
        n == def.name
            || n == def.key
            || n.starts_with(&format!("{} (", def.name))
            || n.starts_with(&format!("{} / ", def.name))
    })
}

fn outcome(conclusion: &str) -> Option<&'static str> {
    match conclusion {
        "success" => Some("succeeded"),
        "failure" | "timed_out" | "startup_failure" | "action_required" => Some("failed"),
        _ => None,
    }
}

/// One deploy a run made: the environment as named, where it went, and
/// whether it worked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub name: String,
    pub env: Env,
    pub outcome: &'static str,
}

/// The repository's deploy workflows setting: `file.yml=environment`, comma
/// separated; a file without `=` is a deploy whose environment comes from the
/// run. Keys are file names without folders, lowercased.
pub fn parse_deploy_workflows(setting: &str) -> HashMap<String, String> {
    setting
        .split(',')
        .filter_map(|p| {
            let (file, env) = p.split_once('=').unwrap_or((p, ""));
            let file = file.trim().rsplit('/').next()?.trim().to_ascii_lowercase();
            (!file.is_empty()).then(|| (file, env.trim().to_string()))
        })
        .collect()
}

/// What one completed run deployed, if it is a deploy. `jobs`: the workflow
/// file's jobs (None when it couldn't be read). `ran`: GitHub's jobs of this
/// run (None when not read: every job counts as run). `setting`: the
/// repository's deploy workflows.
pub fn run_targets(
    run: &Value,
    jobs: Option<&[JobDef]>,
    ran: Option<&[Value]>,
    setting: &HashMap<String, String>,
) -> Vec<Target> {
    let run_outcome = outcome(run["conclusion"].as_str().unwrap_or(""));
    let workflow = run["name"].as_str().unwrap_or("");
    let path = run["path"].as_str().unwrap_or("");
    let file = path.rsplit('/').next().unwrap_or("").to_ascii_lowercase();
    let mut out: Vec<Target> = Vec::new();
    fn push(out: &mut Vec<Target>, name: String, env: Env, outcome: &'static str) {
        if !out.iter().any(|t| t.name.eq_ignore_ascii_case(&name)) {
            out.push(Target { name, env, outcome });
        }
    }
    let set = setting.get(&file);
    if let Some(env) = set.filter(|e| !e.is_empty()) {
        if let Some(o) = run_outcome {
            push(&mut out, env.clone(), env_or_other(env), o);
        }
        return out;
    }
    let named_deploy = set.is_some() || says_deploy(workflow) || says_deploy(&file);
    // Jobs that declare an environment and ran.
    let mut declared = false;
    for def in jobs.unwrap_or(&[]) {
        if def.environment.is_empty() {
            continue;
        }
        let Some(name) = resolve_expression(&def.environment, run) else {
            continue;
        };
        // An environment used for secrets by a test job isn't a deploy.
        if env_of(&name).is_none() && !(named_deploy || says_deploy(&def.name)) {
            continue;
        }
        declared = true;
        let job = ran.and_then(|r| job_ran(def, r));
        if ran.is_some() && job.is_none() {
            continue;
        }
        let job_outcome = match job {
            Some(j) => outcome(j["conclusion"].as_str().unwrap_or("")),
            None => run_outcome,
        };
        let Some(o) = job_outcome else { continue };
        push(&mut out, name.clone(), env_or_other(&name), o);
    }
    // Deploy jobs that declare where they go decide: none ran, none deployed.
    if declared {
        return out;
    }
    // No environment: the workflow or a job that ran says it deploys.
    let ran_names: Vec<&str> = ran
        .unwrap_or(&[])
        .iter()
        .filter(|j| outcome(j["conclusion"].as_str().unwrap_or("")).is_some())
        .filter_map(|j| j["name"].as_str())
        .collect();
    let deploy_job = ran_names.iter().find(|n| says_deploy(n));
    if !named_deploy && deploy_job.is_none() {
        return out;
    }
    let Some(o) = run_outcome else { return out };
    let env = [workflow, file.as_str()]
        .into_iter()
        .chain(deploy_job.copied())
        .find_map(env_of)
        .unwrap_or(Env::Other);
    push(&mut out, env.as_str().to_string(), env, o);
    out
}

pub fn stat_sk(created_at: &str, repo: &str, run_id: u64, env_name: &str) -> String {
    format!(
        "{PREFIX}{created_at}#{}#{run_id}#{}",
        repo.to_ascii_lowercase(),
        env_name.to_ascii_lowercase()
    )
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// The record of one deploy. Written again (webhook, then backfill) it is
/// the same item.
pub fn stat_item(
    team_id: &str,
    repo: &str,
    run: &Value,
    cause: Cause,
    target: &Target,
) -> HashMap<String, AttributeValue> {
    let created_at = run["created_at"].as_str().unwrap_or("").to_string();
    let run_id = run["id"].as_u64().unwrap_or(0);
    let ttl = chrono::DateTime::parse_from_rfc3339(&created_at)
        .map(|d| d.timestamp())
        .unwrap_or_else(|_| chrono::Utc::now().timestamp())
        + KEEP_DAYS * 86_400;
    HashMap::from([
        ("pk".to_string(), s(team_id)),
        (
            "sk".to_string(),
            s(&stat_sk(&created_at, repo, run_id, &target.name)),
        ),
        ("record_type".to_string(), s("deploy_run")),
        ("created_at".to_string(), s(&created_at)),
        ("repo".to_string(), s(repo)),
        ("run_id".to_string(), AttributeValue::N(run_id.to_string())),
        (
            "workflow".to_string(),
            s(run["name"].as_str().unwrap_or("")),
        ),
        ("event".to_string(), s(run["event"].as_str().unwrap_or(""))),
        (
            "head_sha".to_string(),
            s(run["head_sha"].as_str().unwrap_or("")),
        ),
        ("cause".to_string(), s(cause.as_str())),
        ("environment".to_string(), s(target.env.as_str())),
        ("environment_name".to_string(), s(&target.name)),
        ("outcome".to_string(), s(target.outcome)),
        ("ttl".to_string(), AttributeValue::N(ttl.to_string())),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const BOT: &str = "coderhelm[bot]";

    fn run(event: &str, branch: &str, by: (&str, &str), name: &str, path: &str) -> Value {
        json!({
            "id": 77, "event": event, "head_branch": branch, "name": name,
            "path": path, "conclusion": "success", "head_sha": "abc",
            "created_at": "2026-10-30T01:02:03Z",
            "actor": {"login": BOT, "type": "Bot"},
            "triggering_actor": {"login": by.0, "type": by.1},
        })
    }

    fn jobs(names: &[(&str, &str)]) -> Vec<Value> {
        names
            .iter()
            .map(|(n, c)| json!({"name": n, "conclusion": c}))
            .collect()
    }

    #[test]
    fn environments_from_names() {
        assert_eq!(env_of("Production"), Some(Env::Production));
        assert_eq!(env_of("prod-us-east"), Some(Env::Production));
        assert_eq!(env_of("pre-prod"), Some(Env::Staging));
        assert_eq!(env_of("staging"), Some(Env::Staging));
        assert_eq!(env_of("pr-1234"), Some(Env::Preview));
        assert_eq!(env_of("pr1234"), Some(Env::Preview));
        assert_eq!(env_of("Preview"), Some(Env::Preview));
        assert_eq!(env_of("product-api"), None);
        assert_eq!(env_of("e2e"), None);
        assert_eq!(env_or_other("sandbox"), Env::Other);
        assert!(says_deploy("Deploy web"));
        assert!(says_deploy("deploy-infra.yml"));
        assert!(!says_deploy("CI"));
    }

    #[test]
    fn only_coderhelm_triggered_runs_are_its_own() {
        let ours = run("push", "main", (BOT, "Bot"), "Deploy", "d.yml");
        assert!(started_by_coderhelm(&ours, BOT));
        // A person re-running CoderHelm's run: theirs.
        let rerun = run("push", "main", ("someone", "User"), "Deploy", "d.yml");
        assert!(!started_by_coderhelm(&rerun, BOT));
        // Another app.
        let other = run(
            "push",
            "main",
            ("dependabot[bot]", "Bot"),
            "Deploy",
            "d.yml",
        );
        assert!(!started_by_coderhelm(&other, BOT));
        // Older payloads have no triggering actor: the actor decides.
        let mut old = ours.clone();
        old.as_object_mut().unwrap().remove("triggering_actor");
        assert!(started_by_coderhelm(&old, BOT));
        assert!(!started_by_coderhelm(&ours, ""));
        let pulls = [json!({"merged_by": {"login": "Coderhelm[bot]"}})];
        assert!(merged_by_coderhelm(&pulls, BOT));
        assert!(!merged_by_coderhelm(
            &[json!({"merged_by": {"login": "someone"}})],
            BOT
        ));
    }

    #[test]
    fn causes_from_the_event() {
        let c = |e: &str, b: &str, tag: bool| {
            cause_of(&run(e, b, (BOT, "Bot"), "x", "x.yml"), "main", tag)
        };
        assert_eq!(c("pull_request", "fix", false), Cause::PullRequest);
        assert_eq!(c("push", "main", false), Cause::Merge);
        assert_eq!(c("push", "v1.2.3", true), Cause::Tag);
        assert_eq!(c("release", "v1.2.3", false), Cause::Release);
        assert_eq!(c("push", "feature", false), Cause::Other);
        assert_eq!(c("workflow_run", "main", false), Cause::Other);
    }

    const ENVIRONMENTS: &str = r#"
name: Deploy
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    environment: e2e
  preview:
    name: Deploy preview
    environment:
      name: preview
      url: https://x
  release:
    environment: ${{ github.event_name == 'push' && 'production' || 'preview' }}
"#;

    #[test]
    fn workflow_jobs_and_environments_are_read() {
        let j = parse_workflow(ENVIRONMENTS).unwrap();
        assert_eq!(j.len(), 3);
        assert_eq!(j[0].environment, "e2e");
        assert_eq!(j[1].name, "Deploy preview");
        assert_eq!(j[1].environment, "preview");
        assert!(j[2].environment.contains("${{"));
        assert!(parse_workflow("::: not yaml").is_none());
    }

    #[test]
    fn preview_on_every_push_to_its_pr() {
        let defs = parse_workflow(ENVIRONMENTS).unwrap();
        let r = run("pull_request", "fix", (BOT, "Bot"), "CI", "ci.yml");
        let ran = jobs(&[
            ("test", "success"),
            ("Deploy preview", "success"),
            ("release", "success"),
        ]);
        let t = run_targets(&r, Some(&defs), Some(&ran), &HashMap::new());
        // The test job's environment isn't a deploy; the expression picks
        // preview on a PR; one preview is one deploy.
        assert_eq!(
            t,
            vec![Target {
                name: "preview".into(),
                env: Env::Preview,
                outcome: "succeeded"
            }]
        );
    }

    #[test]
    fn production_on_merge_from_an_expression() {
        let defs = parse_workflow(ENVIRONMENTS).unwrap();
        let r = run("push", "main", (BOT, "Bot"), "CI", "ci.yml");
        let ran = jobs(&[
            ("test", "success"),
            ("Deploy preview", "skipped"),
            ("release", "failure"),
        ]);
        let t = run_targets(&r, Some(&defs), Some(&ran), &HashMap::new());
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].env, Env::Production);
        assert_eq!(t[0].outcome, "failed");
    }

    #[test]
    fn production_on_tag_from_the_workflow_name() {
        // No environments: the workflow's name says deploy and production.
        let r = run(
            "push",
            "v2.4.0",
            (BOT, "Bot"),
            "Deploy production",
            ".github/workflows/release.yml",
        );
        let t = run_targets(&r, Some(&[]), None, &HashMap::new());
        assert_eq!(t[0].env, Env::Production);
        assert_eq!(
            cause_of(&r, "main", true),
            Cause::Tag,
            "a tag CoderHelm pushed"
        );
    }

    #[test]
    fn staging_by_label_from_the_job_environment() {
        let yaml = "jobs:\n  ship:\n    if: contains(github.event.pull_request.labels.*.name, 'deploy-staging')\n    environment: Staging\n";
        let defs = parse_workflow(yaml).unwrap();
        let r = run("pull_request", "fix", (BOT, "Bot"), "Label deploy", "l.yml");
        let ran = jobs(&[("ship", "success")]);
        let t = run_targets(&r, Some(&defs), Some(&ran), &HashMap::new());
        assert_eq!(t[0].env, Env::Staging);
        assert_eq!(t[0].name, "Staging");
        // The label's push without the label: the job is skipped, no deploy.
        let skipped = jobs(&[("ship", "skipped")]);
        assert!(run_targets(&r, Some(&defs), Some(&skipped), &HashMap::new()).is_empty());
    }

    #[test]
    fn a_workflow_that_does_not_deploy_is_not_counted() {
        let r = run("pull_request", "fix", (BOT, "Bot"), "CI", "ci.yml");
        let defs = parse_workflow("jobs:\n  test:\n    runs-on: x\n").unwrap();
        let ran = jobs(&[("test", "success")]);
        assert!(run_targets(&r, Some(&defs), Some(&ran), &HashMap::new()).is_empty());
        // Cancelled: nothing deployed.
        let mut c = run("push", "main", (BOT, "Bot"), "Deploy", "deploy.yml");
        c["conclusion"] = json!("cancelled");
        assert!(run_targets(&c, None, None, &HashMap::new()).is_empty());
    }

    #[test]
    fn the_setting_names_workflows_the_names_do_not() {
        let set = parse_deploy_workflows(" .github/workflows/Apply.yml = production, plan.yml ");
        assert_eq!(set.get("apply.yml").unwrap(), "production");
        assert_eq!(set.get("plan.yml").unwrap(), "");
        let r = run(
            "push",
            "main",
            (BOT, "Bot"),
            "Terraform",
            ".github/workflows/apply.yml",
        );
        assert!(run_targets(&r, None, None, &HashMap::new()).is_empty());
        let t = run_targets(&r, None, None, &set);
        assert_eq!(t[0].env, Env::Production);
        // Listed without an environment: a deploy, environment from the run.
        let p = run(
            "push",
            "main",
            (BOT, "Bot"),
            "Plan",
            ".github/workflows/plan.yml",
        );
        assert_eq!(run_targets(&p, None, None, &set)[0].env, Env::Other);
    }

    #[test]
    fn the_record_is_the_same_item_written_twice() {
        let r = run("push", "main", (BOT, "Bot"), "Deploy", "deploy.yml");
        let t = Target {
            name: "Production".into(),
            env: Env::Production,
            outcome: "succeeded",
        };
        let a = stat_item("TEAM#t", "Acme/Web", &r, Cause::Merge, &t);
        let b = stat_item("TEAM#t", "acme/web", &r, Cause::Merge, &t);
        assert_eq!(a["sk"], b["sk"]);
        assert_eq!(
            a["sk"].as_s().unwrap(),
            "DRUN#2026-10-30T01:02:03Z#acme/web#77#production"
        );
        assert_eq!(a["environment"].as_s().unwrap(), "production");
        assert_eq!(a["cause"].as_s().unwrap(), "merge");
        assert_eq!(a["outcome"].as_s().unwrap(), "succeeded");
    }
}
