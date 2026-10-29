//! Releasing a change that migrates data or content: expand, deploy,
//! contract, against how each environment really deploys.
//!
//! A run once wrote one content migration that set a new field and removed
//! the two old ones, and a note saying to run it when the pull request
//! merges, before the web deploy. Production web deployed only on a version
//! tag, so running it at merge left production's old code reading removed
//! fields until someone tagged a release; the new code had no fallback to the
//! old fields either, so the other order broke too.
//!
//! The rules here, all in code:
//!
//! - [`mechanics`] reads how each environment deploys from the repository's
//!   workflows (on merge, on a `v*` tag, on dispatch), which workflow runs
//!   migrations (and its dataset and dry-run inputs), the cache purge the
//!   repository has, and which CMS dataset each environment reads.
//! - [`plan_gaps`] sends a plan back when it migrates a field that deployed
//!   code reads and doesn't split it: an additive (expand) migration that
//!   runs any time, the new code reading the new field with a fallback to the
//!   old one, a test proving the fallback, a separate contract migration
//!   that removes the old fields only after the production deploy, and the
//!   affected counts per environment it could read (with the live items whose
//!   behaviour changes), or the quoted error of a failed lookup.
//! - [`diff_gaps`] checks the same on the code the run wrote: one migration
//!   that both sets and removes, code that stops reading a removed field,
//!   no test with the old field set and the new one missing.
//! - [`steps`] derives the ordered release steps (dry runs, the expand
//!   migration per environment, the deploys with their real triggers, the
//!   production tag, the contract migration after the production deploy,
//!   the cache purge), each with the agent-permissions tool it is gated by,
//!   the steps it waits for and the count that verifies it. CoderHelm does
//!   them ([`super::release_run`]); a step goes to a person only when
//!   CoderHelm has no way to do it, with the access to grant.
//! - [`note_lines`] renders the steps once, for the ticket note and the
//!   pull request.

use std::collections::BTreeSet;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::outside_work::ContentSource;

// ── Environments and how they deploy ────────────────────────────────────────

/// An environment a release step runs in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Env {
    Staging,
    Production,
}

impl Env {
    pub fn word(self) -> &'static str {
        match self {
            Env::Staging => "staging",
            Env::Production => "production",
        }
    }

    /// The environment a name points at: `prod`, `production`, `live` for
    /// production; `staging`, `stage`, `preview`, `dev`, `qa`, `uat`, `test`
    /// for staging.
    pub fn of(name: &str) -> Option<Env> {
        let l = name.to_ascii_lowercase();
        let words: Vec<&str> = l
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|w| !w.is_empty())
            .collect();
        if words
            .iter()
            .any(|w| w.starts_with("prod") || *w == "live" || *w == "prd")
        {
            return Some(Env::Production);
        }
        if words.iter().any(|w| {
            w.starts_with("stag")
                || ["stage", "preview", "dev", "develop", "qa", "uat", "test"].contains(w)
        }) {
            return Some(Env::Staging);
        }
        None
    }
}

/// What starts a deploy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "on", content = "value", rename_all = "snake_case")]
pub enum On {
    /// A push to this branch: the pull request merging.
    Merge(String),
    /// A pushed tag matching this pattern (`v*`), or a published release.
    Tag(String),
    /// `workflow_dispatch`: CoderHelm can start it.
    Dispatch,
}

impl On {
    pub fn words(&self) -> String {
        match self {
            On::Merge(b) => format!("on merge to `{b}`"),
            On::Tag(p) => format!("on a `{p}` tag"),
            On::Dispatch => "when started (workflow_dispatch)".to_string(),
        }
    }
}

/// One environment's deploy: the workflow and what starts it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deploy {
    pub env: Env,
    pub name: String,
    pub file: String,
    pub on: On,
}

impl Deploy {
    pub fn line(&self) -> String {
        format!(
            "{} deploys {} (`{}`, {})",
            capital(self.env.word()),
            self.on.words(),
            self.name,
            self.file
        )
    }
}

/// A workflow CoderHelm can start (migrations, a cache purge), with the
/// inputs it takes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Runner {
    pub name: String,
    pub file: String,
    /// The input that picks the environment or dataset, and the values it
    /// allows (empty when free text).
    #[serde(default)]
    pub env_input: String,
    #[serde(default)]
    pub env_options: Vec<String>,
    /// The input that makes it a dry run.
    #[serde(default)]
    pub dry_input: String,
    /// The input that names the migration to run.
    #[serde(default)]
    pub file_input: String,
}

impl Runner {
    /// The value of the environment input for `env`: the option named for
    /// it, else the environment's dataset, else its word.
    pub fn env_value(&self, env: Env, dataset: Option<&str>) -> String {
        if let Some(o) = self.env_options.iter().find(|o| Env::of(o) == Some(env)) {
            return o.clone();
        }
        dataset.unwrap_or(env.word()).to_string()
    }
}

/// A CMS dataset an environment reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvSource {
    pub env: Env,
    pub source: ContentSource,
}

/// How the repository releases.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Mechanics {
    pub deploys: Vec<Deploy>,
    pub migrations: Option<Runner>,
    pub purge: Option<Runner>,
    pub sources: Vec<EnvSource>,
}

impl Mechanics {
    /// The deploy of `env`: one started by merge, else tag, else dispatch.
    pub fn deploy(&self, env: Env) -> Option<&Deploy> {
        let of = |f: fn(&On) -> bool| self.deploys.iter().find(|d| d.env == env && f(&d.on));
        of(|o| matches!(o, On::Merge(_)))
            .or_else(|| of(|o| matches!(o, On::Tag(_))))
            .or_else(|| of(|o| matches!(o, On::Dispatch)))
    }

    pub fn source(&self, env: Env) -> Option<&ContentSource> {
        self.sources
            .iter()
            .find(|s| s.env == env)
            .map(|s| &s.source)
    }

    /// The datasets the planner may query, by environment.
    pub fn datasets(&self) -> Vec<String> {
        self.sources
            .iter()
            .map(|s| s.source.dataset.clone())
            .collect()
    }

    /// What the planner is told about releasing in this repository.
    pub fn prompt_section(&self) -> String {
        let mut out =
            String::from("\n\n## How this repository releases (read from its workflows)\n");
        if self.deploys.is_empty() {
            out.push_str("- No deploy workflow names an environment.\n");
        }
        for d in &self.deploys {
            out.push_str(&format!("- {}\n", d.line()));
        }
        match &self.migrations {
            Some(r) => out.push_str(&format!(
                "- Migrations run with `{}` ({}, workflow_dispatch{}{})\n",
                r.name,
                r.file,
                if r.env_input.is_empty() {
                    String::new()
                } else {
                    format!(", environment input `{}`", r.env_input)
                },
                if r.dry_input.is_empty() {
                    String::new()
                } else {
                    format!(", dry-run input `{}`", r.dry_input)
                }
            )),
            None => out.push_str("- No workflow runs migrations on dispatch.\n"),
        }
        if let Some(p) = &self.purge {
            out.push_str(&format!("- Cache purge: `{}` ({})\n", p.name, p.file));
        }
        for s in &self.sources {
            out.push_str(&format!(
                "- {} content: dataset `{}` (query it with `query_content` and `dataset: \"{}\"`)\n",
                capital(s.env.word()),
                s.source.dataset,
                s.source.dataset
            ));
        }
        out.push_str(RELEASE_GUIDANCE);
        out
    }
}

/// What every plan that migrates data is told.
pub const RELEASE_GUIDANCE: &str = "\
When the change migrates data or content and removes, renames or changes the meaning of a field \
that deployed code reads, split it (expand, then contract), and write `release` in the \
fix_location.json block. A migration that only writes values (a data fix such as `UPDATE ... SET \
start_time = ..., updated_at = now()`) or only adds a field runs any time and needs no `release`; \
never list a column it writes as an old field:
- `fields`: each change, `{\"old\": [\"oldA\", \"oldB\"], \"new\": \"newField\", \"types\": [...]}`.
- `migrations`: an `expand` migration that only sets the new field where it is missing (old code \
ignores it; it can run any time), and a separate `contract` migration that removes the old fields. \
Each `{\"file\": \"path\", \"phase\": \"expand\" | \"contract\"}`, each a task.
- The new code reads the new field with a fallback to the old one while both exist, and keeps the \
old field in its queries and projections: say where in `fallback`.
- `fallback_test`: the test (a task) proving it: old field set, new missing, renders as before.
- `impact`: for every environment whose data you can read (staging and production, read only, \
with `query_content`), the count of affected items, `{\"env\", \"dataset\", \"query\", \
\"result\"}`, or the quoted `error` of a failed attempt. Never write that you could not look \
without that error.
- `live_items`: the production items whose behaviour changes during the transition (for example \
the ones with an old flag set), by id and title.
- `count_new_missing` and `count_old_present`: GROQ (or SQL) count queries that return 0 once the \
expand and the contract have run.
CoderHelm runs the release itself after the pull request: dry runs, the expand migration on each \
environment, the deploys with the triggers listed above, the production tag, the contract \
migration only after it has seen the production deploy succeed, and the cache purge. Don't write \
those as steps for a person.";

fn capital(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

// ── Reading the workflows ───────────────────────────────────────────────────

fn yaml_str(v: &serde_yaml::Value) -> String {
    match v {
        serde_yaml::Value::String(s) => s.clone(),
        serde_yaml::Value::Mapping(m) => m
            .get(serde_yaml::Value::String("name".into()))
            .map(yaml_str)
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn yaml_list(v: Option<&serde_yaml::Value>) -> Vec<String> {
    match v {
        Some(serde_yaml::Value::Sequence(s)) => s.iter().map(yaml_str).collect(),
        Some(serde_yaml::Value::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

/// The triggers of a workflow's `on:`.
fn triggers(doc: &serde_yaml::Value) -> Vec<On> {
    let on = doc
        .get("on")
        .or_else(|| doc.get(serde_yaml::Value::Bool(true)));
    let mut out = Vec::new();
    let names: Vec<String> = match on {
        Some(serde_yaml::Value::String(s)) => vec![s.clone()],
        Some(serde_yaml::Value::Sequence(s)) => s.iter().map(yaml_str).collect(),
        Some(serde_yaml::Value::Mapping(m)) => m.keys().map(yaml_str).collect(),
        _ => Vec::new(),
    };
    for n in names {
        let body = on.and_then(|o| o.get(n.as_str()));
        match n.as_str() {
            "push" => {
                let branches = yaml_list(body.and_then(|b| b.get("branches")));
                let tags = yaml_list(body.and_then(|b| b.get("tags")));
                for t in &tags {
                    out.push(On::Tag(t.clone()));
                }
                for b in &branches {
                    out.push(On::Merge(b.clone()));
                }
                if tags.is_empty() && branches.is_empty() {
                    out.push(On::Merge("main".to_string()));
                }
            }
            "release" => out.push(On::Tag("a published release".to_string())),
            "workflow_dispatch" => out.push(On::Dispatch),
            _ => {}
        }
    }
    out
}

/// Which of a workflow's triggers a job's `if:` lets through.
fn job_allows(cond: &str, on: &On) -> bool {
    let c = cond.to_ascii_lowercase();
    if c.is_empty() {
        return true;
    }
    let tag_only = c.contains("refs/tags") && !c.contains("!startswith(github.ref, 'refs/tags");
    let branch_only = c.contains("refs/heads/") && !c.contains("refs/tags");
    let dispatch_only = c.contains("== 'workflow_dispatch'");
    let push_only = c.contains("== 'push'");
    match on {
        On::Tag(_) => !branch_only && !dispatch_only,
        On::Merge(b) => {
            !tag_only
                && !dispatch_only
                && (!branch_only || c.contains(&format!("refs/heads/{}", b.to_ascii_lowercase())))
        }
        On::Dispatch => !tag_only && !branch_only && !push_only,
    }
}

fn is_deploy_text(t: &str) -> bool {
    let l = t.to_ascii_lowercase();
    l.contains("deploy") || l.contains("release") || l.contains("publish")
}

/// The deploys one workflow makes: each job that deploys to an environment
/// it names (its `environment:`, its key or name, else the workflow's name
/// or file), once per trigger its `if:` lets through.
pub fn workflow_deploys(path: &str, text: &str) -> Vec<Deploy> {
    let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(text) else {
        return Vec::new();
    };
    let name = doc
        .get("name")
        .map(yaml_str)
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| path.rsplit('/').next().unwrap_or(path).to_string());
    let ons = triggers(&doc);
    if ons.is_empty() {
        return Vec::new();
    }
    let file_env = Env::of(&name).or_else(|| Env::of(path.rsplit('/').next().unwrap_or(path)));
    let workflow_deploys = is_deploy_text(&name) || is_deploy_text(path);
    let mut out: Vec<Deploy> = Vec::new();
    let Some(serde_yaml::Value::Mapping(jobs)) = doc.get("jobs") else {
        return Vec::new();
    };
    for (key, job) in jobs {
        let key = yaml_str(key);
        let job_text = serde_yaml::to_string(job).unwrap_or_default();
        if !(workflow_deploys || is_deploy_text(&key) || is_deploy_text(&job_text)) {
            continue;
        }
        let env_name = job.get("environment").map(yaml_str).unwrap_or_default();
        let job_name = job.get("name").map(yaml_str).unwrap_or_default();
        let env = Env::of(&env_name)
            .or_else(|| Env::of(&key))
            .or_else(|| Env::of(&job_name))
            .or(file_env);
        let Some(env) = env else {
            continue;
        };
        let cond = job.get("if").map(yaml_str).unwrap_or_default();
        for on in &ons {
            if job_allows(&cond, on) {
                let d = Deploy {
                    env,
                    name: name.clone(),
                    file: path.to_string(),
                    on: on.clone(),
                };
                if !out.contains(&d) {
                    out.push(d);
                }
            }
        }
    }
    out
}

/// A dispatchable workflow whose text says it does `what` (migrations, a
/// cache purge), with its inputs read.
fn runner(path: &str, text: &str, what: &[&str]) -> Option<Runner> {
    let l = text.to_ascii_lowercase();
    if !what.iter().any(|w| l.contains(w)) {
        return None;
    }
    let doc = serde_yaml::from_str::<serde_yaml::Value>(text).ok()?;
    if !triggers(&doc).contains(&On::Dispatch) {
        return None;
    }
    let on = doc
        .get("on")
        .or_else(|| doc.get(serde_yaml::Value::Bool(true)));
    let inputs = on
        .and_then(|o| o.get("workflow_dispatch"))
        .and_then(|w| w.get("inputs"));
    let mut r = Runner {
        name: doc
            .get("name")
            .map(yaml_str)
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| path.rsplit('/').next().unwrap_or(path).to_string()),
        file: path.to_string(),
        ..Default::default()
    };
    if let Some(serde_yaml::Value::Mapping(m)) = inputs {
        for (k, v) in m {
            let k = yaml_str(k);
            let lk = k.to_ascii_lowercase();
            if lk.contains("dry") {
                r.dry_input = k;
            } else if ["dataset", "environment", "env", "target", "stage"]
                .iter()
                .any(|w| lk == *w || lk.ends_with(&format!("_{w}")))
            {
                r.env_options = yaml_list(v.get("options"));
                r.env_input = k;
            } else if ["migration", "script", "file", "name", "id"]
                .iter()
                .any(|w| lk.contains(w))
            {
                r.file_input = k;
            }
        }
    }
    Some(r)
}

/// Dataset names a file gives (`dataset: 'production'`,
/// `SANITY_STUDIO_DATASET=staging`, a workflow's env).
fn datasets_in(text: &str) -> Vec<String> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r#"(?i)\b[a-z_]*dataset\b["']?\s*[:=]\s*["'`]?([a-z][a-z0-9_-]{1,63})\b"#)
            .unwrap()
    });
    re.captures_iter(text)
        .map(|c| c[1].to_string())
        .filter(|d| !d.eq_ignore_ascii_case("process") && !d.eq_ignore_ascii_case("import"))
        .collect()
}

/// How the repository releases, from its files (workflows, package files,
/// CMS config) and the content source #356's reader found.
pub fn mechanics(files: &[(String, String)], base: Option<&ContentSource>) -> Mechanics {
    let mut m = Mechanics::default();
    let mut names: Vec<(String, String)> = Vec::new();
    for (path, text) in files {
        let workflow = path.starts_with(".github/workflows/")
            && (path.ends_with(".yml") || path.ends_with(".yaml"));
        if workflow {
            for d in workflow_deploys(path, text) {
                if !m.deploys.contains(&d) {
                    m.deploys.push(d);
                }
            }
            if m.migrations.is_none() {
                m.migrations = runner(path, text, &["migrat"]);
            }
            if m.purge.is_none() {
                m.purge = runner(path, text, &["purge", "revalidate", "invalidat"]);
            }
        }
        for d in datasets_in(text) {
            names.push((d, path.clone()));
        }
    }
    if let Some(b) = base {
        names.insert(0, (b.dataset.clone(), b.origin.clone()));
        for env in [Env::Staging, Env::Production] {
            let pick = names
                .iter()
                .find(|(d, _)| Env::of(d) == Some(env))
                .or_else(|| {
                    // A dataset named for neither is production's when it
                    // is the only one left (the CMS default, `production`).
                    (env == Env::Production)
                        .then(|| names.iter().find(|(d, _)| Env::of(d).is_none()))
                        .flatten()
                });
            if let Some((d, from)) = pick {
                if !m.sources.iter().any(|s| s.source.dataset == *d) {
                    m.sources.push(EnvSource {
                        env,
                        source: ContentSource {
                            project_id: b.project_id.clone(),
                            dataset: d.clone(),
                            origin: from.clone(),
                        },
                    });
                }
            }
        }
    }
    m.deploys.sort_by_key(|d| d.env);
    m
}

/// A run's release: the plan's, with how the repository releases and the
/// steps derived from both.
#[derive(Debug, Clone, Default)]
pub struct Planned {
    pub release: Release,
    pub mechanics: Mechanics,
    pub steps: Vec<Step>,
}

impl Planned {
    pub fn lines(&self) -> Vec<String> {
        note_lines(&self.release, &self.steps, &self.mechanics)
    }
}

/// The run's release, read against the repository at `git_ref` the same
/// way the planner saw it. `None` when the plan migrates nothing.
pub async fn for_run(
    state: &crate::WorkerState,
    msg: &crate::models::TicketMessage,
    github: &crate::clients::github::GitHubClient,
    plan: &super::plan::PlanResult,
    git_ref: &str,
) -> Option<Planned> {
    let release = plan
        .fix
        .as_ref()?
        .release
        .clone()
        .filter(|r| !r.is_empty())?;
    let env = super::load_codegen_env(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    let staging_write = super::feature::load_connection(
        state,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
    )
    .await
    .is_some();
    let a = super::outside_work::load(
        github,
        &msg.repo_owner,
        &msg.repo_name,
        git_ref,
        &env,
        staging_write,
    )
    .await;
    let steps = steps(&release, &a.release);
    Some(Planned {
        release,
        mechanics: a.release,
        steps,
    })
}

// ── What the planner writes ─────────────────────────────────────────────────

/// One field change.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldChange {
    #[serde(default, deserialize_with = "super::fix_location::string_or_list")]
    pub old: Vec<String>,
    #[serde(default)]
    pub new: String,
    #[serde(default, deserialize_with = "super::fix_location::string_or_list")]
    pub types: Vec<String>,
}

/// A migration file and when it runs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedMigration {
    #[serde(default)]
    pub file: String,
    /// `expand` (additive, any time) or `contract` (removes, after the
    /// production deploy).
    #[serde(default)]
    pub phase: String,
}

impl PlannedMigration {
    pub fn is_expand(&self) -> bool {
        self.phase.trim().eq_ignore_ascii_case("expand")
    }
    pub fn is_contract(&self) -> bool {
        self.phase.trim().eq_ignore_ascii_case("contract")
    }
}

/// The affected count in one environment, or the error of the attempt.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Impact {
    #[serde(default)]
    pub env: String,
    #[serde(default)]
    pub dataset: String,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub result: String,
    #[serde(default)]
    pub error: String,
}

impl Impact {
    pub fn env(&self) -> Option<Env> {
        Env::of(&self.env).or_else(|| Env::of(&self.dataset))
    }
    /// The count the result gives, when it is one.
    pub fn count(&self) -> Option<u64> {
        count_of(&self.result)
    }
}

/// A number from a query result: a bare count, `{"result": n}`, or the
/// length of a returned list.
pub fn count_of(result: &str) -> Option<u64> {
    let r = result.trim();
    let v: Value = serde_json::from_str(r).ok().or_else(|| {
        r.split(|c: char| !c.is_ascii_digit())
            .find(|w| !w.is_empty())
            .and_then(|w| w.parse::<u64>().ok())
            .map(Value::from)
    })?;
    let v = v.get("result").cloned().unwrap_or(v);
    match v {
        Value::Number(n) => n.as_u64(),
        Value::Array(a) => Some(a.len() as u64),
        _ => None,
    }
}

/// The release a plan that migrates data writes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Release {
    #[serde(default)]
    pub fields: Vec<FieldChange>,
    #[serde(default)]
    pub migrations: Vec<PlannedMigration>,
    /// Where the new code falls back to the old field.
    #[serde(default)]
    pub fallback: String,
    /// The test proving the fallback.
    #[serde(default)]
    pub fallback_test: String,
    #[serde(default)]
    pub impact: Vec<Impact>,
    /// Production items whose behaviour changes during the transition.
    #[serde(default, deserialize_with = "super::fix_location::string_or_list")]
    pub live_items: Vec<String>,
    /// A count query that returns 0 once the expand migration has run.
    #[serde(default)]
    pub count_new_missing: String,
    /// A count query that returns 0 once the contract migration has run.
    #[serde(default)]
    pub count_old_present: String,
}

impl Release {
    pub fn removed_fields(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for f in &self.fields {
            for o in &f.old {
                if !o.trim().is_empty() && !out.contains(o) {
                    out.push(o.trim().to_string());
                }
            }
        }
        out
    }
    pub fn expand(&self) -> Option<&PlannedMigration> {
        self.migrations.iter().find(|m| m.is_expand())
    }
    pub fn contract(&self) -> Option<&PlannedMigration> {
        self.migrations.iter().find(|m| m.is_contract())
    }
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty() && self.migrations.is_empty()
    }
}

// ── Reading a migration ─────────────────────────────────────────────────────

/// Whether a path is a data or content migration.
pub fn is_migration(path: &str) -> bool {
    let l = path.to_ascii_lowercase();
    let base = l.rsplit('/').next().unwrap_or(&l);
    (l.contains("migrations/") || l.contains("migration/") || base.contains("migrat"))
        && !is_test_path(&l)
        && !base.ends_with(".md")
        && !base.ends_with(".json")
}

fn is_test_path(l: &str) -> bool {
    l.contains(".test.")
        || l.contains(".spec.")
        || l.contains("__tests__")
        || l.contains("/tests/")
        || l.contains("/test/")
        || l.contains("/e2e/")
        || l.starts_with("e2e/")
        || l.starts_with("test/")
        || l.starts_with("tests/")
}

fn is_code_path(l: &str) -> bool {
    [
        ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".vue", ".svelte", ".astro", ".py", ".rb",
        ".go", ".rs", ".php", ".groq", ".graphql", ".gql",
    ]
    .iter()
    .any(|e| l.ends_with(e))
}

/// The fields a migration sets and removes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Touches {
    pub sets: BTreeSet<String>,
    pub removes: BTreeSet<String>,
}

fn leaf(path: &str) -> String {
    path.rsplit('.')
        .next()
        .unwrap_or(path)
        .split('[')
        .next()
        .unwrap_or("")
        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .to_string()
}

fn quoted(s: &str) -> Vec<String> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r#"["'`]([A-Za-z_][\w.\[\]]*)["'`]"#).unwrap());
    re.captures_iter(s).map(|c| leaf(&c[1])).collect()
}

/// The keys of the object literal that starts right after `start` (the
/// index just past a `{`), at its top level.
fn object_keys(text: &str, start: usize) -> Vec<String> {
    let mut depth = 1i32;
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = start;
    let mut token_start = start;
    while i < bytes.len() && depth > 0 {
        match bytes[i] {
            b'{' | b'[' | b'(' => depth += 1,
            b'}' | b']' | b')' => depth -= 1,
            b',' if depth == 1 => token_start = i + 1,
            b':' if depth == 1 => {
                let k = text[token_start..i]
                    .trim()
                    .trim_matches(|c| c == '"' || c == '\'' || c == '`');
                if !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    out.push(k.to_string());
                }
                token_start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out
}

/// Reads the fields a migration's text sets and removes: content
/// migrations (`at('x', unset())`, `unset(['a'])`, `set({x: ...})`,
/// `setIfMissing`, `delete doc.x`, `$set`/`$unset`) and SQL or schema
/// migrations (`ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN a TO b`,
/// `add_column`, `remove_column`, `rename_column`).
pub fn touches(text: &str) -> Touches {
    static RES: std::sync::OnceLock<Vec<(Regex, u8)>> = std::sync::OnceLock::new();
    // 0 = removes group 1, 1 = sets group 1, 2 = removes 1 and sets 2,
    // 3 = removes every quoted name in group 1.
    let res = RES.get_or_init(|| {
        [
            (r#"at\(\s*["'`]([\w.\[\]]+)["'`]\s*,\s*unset\("#, 0),
            (r#"at\(\s*["'`]([\w.\[\]]+)["'`]\s*,\s*(?:set|setIfMissing|insert|replace|inc)\w*\("#, 1),
            (r#"\bunset\(\s*\[([^\]]*)\]"#, 3),
            (r#"\bunset\(\s*["'`]([\w.\[\]]+)["'`]"#, 0),
            (r#"\bdelete\s+[\w$]+(?:\.[\w$]+|\[[^\]]+\])*\.(\w+)"#, 0),
            (r#"\bdelete\s+[\w$.]+\[\s*["'`](\w+)["'`]\s*\]"#, 0),
            (r#"(?i)\bdrop\s+column\s+(?:if\s+exists\s+)?["`]?(\w+)"#, 0),
            (r#"(?i)\badd\s+column\s+(?:if\s+not\s+exists\s+)?["`]?(\w+)"#, 1),
            (r#"(?i)\brename\s+column\s+["`]?(\w+)["`]?\s+to\s+["`]?(\w+)"#, 2),
            (r#"\bremove_column\s*\(?\s*:\w+\s*,\s*:(\w+)"#, 0),
            (r#"\badd_column\s*\(?\s*:\w+\s*,\s*:(\w+)"#, 1),
            (r#"\brename_column\s*\(?\s*:\w+\s*,\s*:(\w+)\s*,\s*:(\w+)"#, 2),
            // Query-builder schema calls: `dropColumn("table", "col")`,
            // `table.dropColumn("col")`, `renameColumn("table", "a", "b")`,
            // `table.renameColumn("a", "b")`.
            (r#"\bdropColumns?\(\s*["'`]?[\w.]+["'`]?\s*,\s*["'`](\w+)["'`]"#, 0),
            (r#"\bdropColumns?\(\s*["'`](\w+)["'`]\s*\)"#, 0),
            (r#"\brenameColumn\(\s*["'`]?[\w.]+["'`]?\s*,\s*["'`](\w+)["'`]\s*,\s*["'`](\w+)["'`]"#, 2),
            (r#"\brenameColumn\(\s*["'`](\w+)["'`]\s*,\s*["'`](\w+)["'`]\s*\)"#, 2),
        ]
        .into_iter()
        .map(|(p, k)| (Regex::new(p).unwrap(), k))
        .collect()
    });
    let mut t = Touches::default();
    for (re, kind) in res {
        for c in re.captures_iter(text) {
            match kind {
                0 => {
                    t.removes.insert(leaf(&c[1]));
                }
                1 => {
                    t.sets.insert(leaf(&c[1]));
                }
                2 => {
                    t.removes.insert(leaf(&c[1]));
                    t.sets.insert(leaf(&c[2]));
                }
                _ => t.removes.extend(quoted(&c[1])),
            }
        }
    }
    // Object-literal sets and unsets: `set({a: 1})`, `$set: {a: 1}`,
    // `$unset: {a: ''}`.
    static OBJ: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let obj = OBJ.get_or_init(|| {
        Regex::new(r#"(\$unset|\$set|\bset|\bsetIfMissing)\s*(?:\(|:)\s*\{"#).unwrap()
    });
    for c in obj.captures_iter(text) {
        let end = c.get(0).map(|m| m.end()).unwrap_or(0);
        let keys = object_keys(text, end);
        if &c[1] == "$unset" {
            t.removes.extend(keys);
        } else {
            t.sets.extend(keys);
        }
    }
    t.removes.retain(|f| !f.is_empty());
    t.sets.retain(|f| !f.is_empty() && !f.starts_with('_'));
    t
}

/// The lines a patch adds (or, with `added == false`, removes).
fn patch_lines(patch: &str, added: bool) -> String {
    let mark = if added { '+' } else { '-' };
    patch
        .lines()
        .filter(|l| l.starts_with(mark) && !l.starts_with("+++") && !l.starts_with("---"))
        .map(|l| &l[1..])
        .collect::<Vec<_>>()
        .join("\n")
}

fn has_word(text: &str, word: &str) -> bool {
    if word.is_empty() {
        return false;
    }
    text.match_indices(word).any(|(i, _)| {
        let before = text[..i].chars().next_back();
        let after = text[i + word.len()..].chars().next();
        let ident = |c: Option<char>| c.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
        !ident(before) && !ident(after)
    })
}

/// What the change's migrations do, from its diff.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Migrated {
    /// Each migration file, with what it touches.
    pub files: Vec<(String, Touches)>,
}

impl Migrated {
    pub fn removes(&self) -> BTreeSet<String> {
        self.files
            .iter()
            .flat_map(|(_, t)| t.removes.iter().cloned())
            .collect()
    }
    pub fn sets(&self) -> BTreeSet<String> {
        let removed = self.removes();
        self.files
            .iter()
            .flat_map(|(_, t)| t.sets.iter().cloned())
            .filter(|s| !removed.contains(s))
            .collect()
    }
}

/// The change's migrations, from a diff's (path, status, patch).
pub fn migrated(files: &[(String, String, String)]) -> Migrated {
    let mut m = Migrated::default();
    for (path, status, patch) in files {
        if status == "removed" || !is_migration(path) {
            continue;
        }
        let t = touches(&patch_lines(patch, true));
        if !t.sets.is_empty() || !t.removes.is_empty() {
            m.files.push((path.clone(), t));
        }
    }
    m
}

fn fields_list(f: &BTreeSet<String>) -> String {
    f.iter()
        .map(|x| format!("`{x}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// What the code the run wrote gets wrong about releasing its migration, as
/// instructions for the fix round (empty when it is right).
pub fn diff_gaps(files: &[(String, String, String)]) -> Vec<String> {
    let m = migrated(files);
    let mut out = Vec::new();
    for (path, t) in &m.files {
        let new: BTreeSet<String> = t.sets.difference(&t.removes).cloned().collect();
        if !t.removes.is_empty() && !new.is_empty() {
            out.push(format!(
                "`{path}` sets {} and removes {} in one migration. Production's deployed code \
                 still reads the removed fields until the deploy that ships this change, so \
                 split it: an expand migration that only sets {} where it is missing (old code \
                 ignores it; it can run any time), and a separate contract migration that \
                 removes {} and runs only after the production deploy.",
                fields_list(&new),
                fields_list(&t.removes),
                fields_list(&new),
                fields_list(&t.removes)
            ));
        }
    }
    let removed = m.removes();
    if removed.is_empty() {
        return out;
    }
    let new = m.sets();
    let code: Vec<&(String, String, String)> = files
        .iter()
        .filter(|(p, s, _)| {
            let l = p.to_ascii_lowercase();
            s != "removed" && is_code_path(&l) && !is_migration(p) && !is_test_path(&l)
        })
        .collect();
    for r in &removed {
        let dropped = code
            .iter()
            .any(|(_, _, patch)| has_word(&patch_lines(patch, false), r));
        let kept = code
            .iter()
            .any(|(_, _, patch)| has_word(&patch_lines(patch, true), r));
        if dropped && !kept {
            out.push(format!(
                "The code stops reading `{r}`, which a migration removes. While both fields \
                 exist, read the new field with a fallback to `{r}` and keep `{r}` in the \
                 queries and projections, so the code works before and after the migrations run."
            ));
        }
    }
    let tested = files.iter().any(|(p, s, patch)| {
        let added = patch_lines(patch, true);
        s != "removed"
            && is_test_path(&p.to_ascii_lowercase())
            && removed.iter().any(|r| has_word(&added, r))
            && (new.is_empty() || new.iter().any(|n| has_word(&added, n)))
    });
    if !tested {
        out.push(format!(
            "Add a test proving the fallback: an item with {} set and {} missing renders as it \
             did before the change.",
            fields_list(&removed),
            if new.is_empty() {
                "the new field".to_string()
            } else {
                fields_list(&new)
            }
        ));
    }
    out
}

// ── Checking the plan ───────────────────────────────────────────────────────

/// Phrases a release uses when it didn't look.
const NO_LOOK: &[&str] = &[
    "not possible",
    "could not look",
    "couldn't look",
    "could not query",
    "couldn't query",
    "cannot query",
    "can't query",
    "unable to query",
    "not queried",
    "no access",
];

fn says_no_look(t: &str) -> bool {
    let l = t.to_ascii_lowercase();
    NO_LOOK.iter().any(|p| l.contains(p))
}

/// What a plan's release lacks, as instructions to the planner (empty when
/// it is complete or the plan migrates nothing). `task_paths`: the files
/// the plan's tasks change.
///
/// Only a real removal or rename is split into expand and contract: a
/// migration that writes values (`UPDATE classes SET start_time = '08:00',
/// updated_at = now()`) or only adds a field runs any time and needs no
/// release. A field counts as removed when the plan's own words remove,
/// drop, rename or unset it (`text`: the plan's text, see
/// [`shows_removal`]) or the plan already planned the contract migration
/// that removes it; a field the planner lists as both old and new is
/// written, not removed.
pub fn gaps(
    release: Option<&Release>,
    task_paths: &[String],
    text: &str,
    m: &Mechanics,
) -> Vec<String> {
    let migrates = task_paths.iter().any(|p| is_migration(p));
    let r = match release {
        Some(r) if !r.is_empty() => r,
        _ if migrates && shows_removal(text, None) => {
            return vec![format!(
                "The plan migrates data ({}). Write `release` in the fix_location.json block: \
                 the fields it changes, an expand and a separate contract migration, the \
                 fallback and its test, and the affected counts per environment. {}",
                task_paths
                    .iter()
                    .filter(|p| is_migration(p))
                    .map(|p| format!("`{p}`"))
                    .collect::<Vec<_>>()
                    .join(", "),
                deploy_summary(m)
            )]
        }
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    let removed = real_removals(r, text);
    let in_tasks = |f: &str| {
        let f = f.trim().trim_start_matches("./");
        !f.is_empty() && task_paths.iter().any(|p| p == f || p.ends_with(f))
    };
    if !removed.is_empty() {
        let list = removed
            .iter()
            .map(|f| format!("`{f}`"))
            .collect::<Vec<_>>()
            .join(", ");
        match (r.expand(), r.contract()) {
            (Some(e), Some(c)) if e.file.trim() == c.file.trim() => out.push(format!(
                "`{}` is both the expand and the contract migration. Split it into two files: \
                 one that only sets the new field (runs any time), one that removes {list} \
                 (runs after the production deploy). {}",
                e.file,
                deploy_summary(m)
            )),
            (Some(e), Some(c)) => {
                for f in [&e.file, &c.file] {
                    if !in_tasks(f) {
                        out.push(format!(
                            "The migration `{f}` is not a task: add it to tasks.md."
                        ));
                    }
                }
            }
            _ => out.push(format!(
                "The change removes {list}, which deployed code reads. Plan two migrations in \
                 `release.migrations`: an `expand` one that only sets the new field where it is \
                 missing (old code ignores it; runs any time) and a separate `contract` one that \
                 removes {list}, which CoderHelm runs only after it has seen the production \
                 deploy succeed. {}",
                deploy_summary(m)
            )),
        }
        if r.fallback.trim().is_empty() {
            out.push(format!(
                "Say in `release.fallback` where the new code reads the new field with a \
                 fallback to {list} while both exist (and keeps {list} in its queries and \
                 projections). Without it the code breaks on items the expand migration hasn't \
                 reached."
            ));
        }
        if r.fallback_test.trim().is_empty() || !in_tasks(&r.fallback_test) {
            out.push(format!(
                "Plan a test proving the fallback (an item with {list} set and the new field \
                 missing renders as before), as a task, and name it in `release.fallback_test`."
            ));
        }
    }
    // Counts in every environment the run can read.
    for s in &m.sources {
        let i = r.impact.iter().find(|i| {
            i.env() == Some(s.env) || i.dataset.trim().eq_ignore_ascii_case(&s.source.dataset)
        });
        match i {
            None => out.push(format!(
                "Count the affected items in {} (dataset `{}`, readable with `query_content` and \
                 `dataset: \"{}\"`) and put the query and its result in `release.impact`.",
                s.env.word(),
                s.source.dataset,
                s.source.dataset
            )),
            Some(i) if i.query.trim().is_empty() => out.push(format!(
                "The {} impact quotes no query: put the exact count query and its result in \
                 `release.impact`.",
                s.env.word()
            )),
            Some(i) if i.error.trim().is_empty() && says_no_look(&i.result) => out.push(format!(
                "The {} impact says it could not look, but dataset `{}` is readable: run the \
                 query with `query_content` and quote the result, or quote the error a failed \
                 attempt gave.",
                s.env.word(),
                s.source.dataset
            )),
            Some(i) if i.error.trim().is_empty() && i.result.trim().is_empty() => {
                out.push(format!(
                    "The {} impact has a query but no result: run it and quote what came back.",
                    s.env.word()
                ))
            }
            _ => {}
        }
    }
    let prod_affected = r
        .impact
        .iter()
        .any(|i| i.env() == Some(Env::Production) && i.count().is_some_and(|n| n > 0));
    if prod_affected && !removed.is_empty() && r.live_items.is_empty() {
        out.push(
            "Production has affected items: name in `release.live_items` the live ones whose \
             behaviour changes during the transition (for example those with an old flag set), \
             by id and title, from a query."
                .to_string(),
        );
    }
    if !m.sources.is_empty() && !removed.is_empty() {
        if r.count_new_missing.trim().is_empty() {
            out.push("Write `release.count_new_missing`: a count query that returns 0 once the expand migration has run (items of the changed types without the new field).".to_string());
        }
        if r.count_old_present.trim().is_empty() {
            out.push("Write `release.count_old_present`: a count query that returns 0 once the contract migration has run (items still holding an old field).".to_string());
        }
    }
    out
}

fn deploy_summary(m: &Mechanics) -> String {
    if m.deploys.is_empty() {
        return String::new();
    }
    format!(
        "How this repository deploys: {}.",
        m.deploys
            .iter()
            .map(Deploy::line)
            .collect::<Vec<_>>()
            .join("; ")
    )
}

/// The release gaps of a plan, from its decision, tasks and words.
pub fn plan_gaps(plan: &super::plan::PlanResult, m: &Mechanics) -> Vec<String> {
    let paths: Vec<String> = super::plan_coverage::plan_tasks(&plan.tasks)
        .into_iter()
        .map(|t| t.path)
        .collect();
    gaps(
        plan.fix.as_ref().and_then(|d| d.release.as_ref()),
        &paths,
        &super::plan_judge::plan_text(plan),
        m,
    )
}

/// Verbs that take a field away. `replace` is not one: a data fix replaces
/// values.
const REMOVAL_VERBS: &[&str] = &[
    "remov", "drop", "renam", "unset", "deprecat", "retir", "delet",
];

/// Words that say the object of a removal is a field.
const FIELD_NOUNS: &[&str] = &["column", "field", "property", "attribute"];

/// Whether `text` takes a field away: a migration statement that removes or
/// renames one (see [`touches`]), or words that remove, drop, rename or
/// unset it. With `field`, that field must be the one named next to the
/// verb (within the same clause); without, any column or field. Writing a
/// value is never a removal: `UPDATE t SET updated_at = now()`, `set({...})`
/// and `DELETE FROM t WHERE ...` (rows, not a field) all say no.
pub fn shows_removal(text: &str, field: Option<&str>) -> bool {
    let t = touches(text);
    match field {
        Some(f) if t.removes.contains(f) => return true,
        None if !t.removes.is_empty() => return true,
        _ => {}
    }
    for clause in text.split(['.', '\n', ';', '!', '?']) {
        let lower = clause.to_ascii_lowercase();
        let words: Vec<(usize, &str)> = word_spans(&lower);
        let verbs: Vec<usize> = words
            .iter()
            .enumerate()
            .filter(|(i, (_, w))| {
                REMOVAL_VERBS.iter().any(|v| w.starts_with(v))
                    // `delete from t` deletes rows; `deleted_at` is a column.
                    && !(w.starts_with("delet")
                        && (words.get(i + 1).is_some_and(|(_, n)| *n == "from")
                            || w.contains('_')))
            })
            .map(|(i, _)| i)
            .collect();
        if verbs.is_empty() {
            continue;
        }
        let near = |pred: &dyn Fn(&str) -> bool| {
            verbs.iter().any(|&v| {
                let lo = v.saturating_sub(6);
                let hi = (v + 7).min(words.len());
                words[lo..hi].iter().any(|(_, w)| pred(w))
            })
        };
        let hit = match field {
            Some(f) => {
                let f = f.to_ascii_lowercase();
                near(&|w: &str| w == f)
            }
            None => near(&|w: &str| {
                FIELD_NOUNS
                    .iter()
                    .any(|n| w == *n || w.strip_suffix('s') == Some(n))
            }),
        };
        if hit {
            return true;
        }
    }
    false
}

/// The identifier-like words of `text` (letters, digits, `_`), with where
/// each starts.
fn word_spans(text: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    for (i, c) in text.char_indices() {
        let ident = c.is_ascii_alphanumeric() || c == '_';
        match (ident, start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                out.push((s, &text[s..i]));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s) = start {
        out.push((s, &text[s..]));
    }
    out
}

/// The fields a plan's release really removes: each old field that is not
/// also the change's new field (written, not removed), and that the plan's
/// words take away or its planned contract migration removes.
pub fn real_removals(r: &Release, text: &str) -> Vec<String> {
    let contract = r.contract().is_some();
    let mut out: Vec<String> = Vec::new();
    for f in &r.fields {
        for o in &f.old {
            let o = o.trim();
            if o.is_empty() || o == f.new.trim() || out.iter().any(|x| x == o) {
                continue;
            }
            if contract || shows_removal(text, Some(o)) {
                out.push(o.to_string());
            }
        }
    }
    out
}

// ── The release steps ───────────────────────────────────────────────────────

/// What a release step does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    DryRun,
    Expand,
    Deploy,
    Contract,
    Purge,
}

/// Where a step stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    #[default]
    Pending,
    /// Waiting on a person's approval card.
    Approval,
    /// Started; waiting for its workflow run.
    Running,
    Done,
    Failed,
    /// A person declined the approval.
    Declined,
    /// CoderHelm has no way to do it: `detail` says what access to grant.
    NoAccess,
}

impl Status {
    pub fn finished(self) -> bool {
        matches!(
            self,
            Status::Done | Status::Failed | Status::Declined | Status::NoAccess
        )
    }
}

/// One step of the release, in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub id: String,
    pub kind: Kind,
    pub env: Env,
    pub what: String,
    /// The workflow and inputs, or the trigger it waits for.
    pub how: String,
    /// The agent-permissions tool the step is checked as (empty when it
    /// only waits, for a deploy that runs on merge).
    pub tool: String,
    /// Steps that must be done first.
    pub after: Vec<String>,
    /// It runs only once the pull request has merged.
    pub after_merge: bool,
    /// A count query that must return 0 once it has run.
    #[serde(default)]
    pub verify: String,
    #[serde(default)]
    pub status: Status,
    /// What happened: the run link, the counts, why it can't be done.
    #[serde(default)]
    pub detail: String,
    #[serde(default)]
    pub before: String,
    #[serde(default)]
    pub after_count: String,
    #[serde(default)]
    pub approval_id: String,
    /// The workflow run it started or waits on.
    #[serde(default)]
    pub run_id: u64,
    /// The tag the production release was cut as.
    #[serde(default)]
    pub tag: String,
    /// The workflow file it starts or waits on.
    #[serde(default)]
    pub workflow: String,
    /// The inputs it starts the workflow with.
    #[serde(default)]
    pub inputs: Vec<(String, String)>,
    /// What starts the deploy it waits on.
    #[serde(default)]
    pub trigger: Option<On>,
    /// The CMS project and dataset its count query reads.
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub dataset: String,
    /// When it was started (unix seconds), to find its workflow run.
    #[serde(default)]
    pub started_at: i64,
}

/// Why CoderHelm can't run migrations itself, and the access to grant.
pub const NO_MIGRATION_RUNNER: &str = "the repository has no workflow that runs migrations on \
     dispatch. Grant it: add a workflow with `workflow_dispatch` (inputs `dataset`, `migration`, \
     `dry_run`) that runs the repository's migrations with the dataset's write token in the \
     repository's secrets. CoderHelm then starts it; production runs still ask for approval \
     unless the team allows them";

/// The gate tool for a step: staging writes and deploys are writes the
/// team can allow; production writes, the production deploy or tag and a
/// cache purge are consequential and ask unless the team allowed them.
pub fn tool_for(kind: Kind, env: Env, on: Option<&On>) -> &'static str {
    match (kind, env) {
        (Kind::DryRun, _) => "release_dry_run",
        (Kind::Expand | Kind::Contract, Env::Staging) => "release_staging_migration",
        (Kind::Expand | Kind::Contract, Env::Production) => "release_production_migration",
        (Kind::Deploy, _) if matches!(on, Some(On::Merge(_)) | None) => "",
        (Kind::Deploy, Env::Staging) => "release_staging_deploy",
        (Kind::Deploy, Env::Production) => match on {
            Some(On::Tag(_)) => "release_production_tag",
            _ => "release_production_deploy",
        },
        (Kind::Purge, _) => "release_cache_purge",
    }
}

fn migration_inputs(m: &Mechanics, file: &str, env: Env, dry: bool) -> Vec<(String, String)> {
    let Some(r) = &m.migrations else {
        return Vec::new();
    };
    let mut inputs = Vec::new();
    if !r.env_input.is_empty() {
        inputs.push((
            r.env_input.clone(),
            r.env_value(env, m.source(env).map(|s| s.dataset.as_str())),
        ));
    }
    if !r.file_input.is_empty() {
        inputs.push((r.file_input.clone(), file.to_string()));
    }
    if !r.dry_input.is_empty() {
        inputs.push((r.dry_input.clone(), dry.to_string()));
    }
    inputs
}

fn migration_how(m: &Mechanics, file: &str, env: Env, dry: bool) -> String {
    match &m.migrations {
        Some(r) => {
            let mut inputs = Vec::new();
            if !r.env_input.is_empty() {
                inputs.push(format!(
                    "{}={}",
                    r.env_input,
                    r.env_value(env, m.source(env).map(|s| s.dataset.as_str()))
                ));
            }
            if !r.file_input.is_empty() {
                inputs.push(format!("{}={}", r.file_input, file));
            }
            if dry && !r.dry_input.is_empty() {
                inputs.push(format!("{}=true", r.dry_input));
            }
            format!(
                "start `{}` ({}){}",
                r.name,
                r.file,
                if inputs.is_empty() {
                    String::new()
                } else {
                    format!(" with {}", inputs.join(", "))
                }
            )
        }
        None => format!("run `{file}` on {}", env.word()),
    }
}

/// The ordered release steps for a plan's release, against how the
/// repository deploys. Empty when the release migrates nothing.
pub fn steps(r: &Release, m: &Mechanics) -> Vec<Step> {
    let mut out: Vec<Step> = Vec::new();
    let Some(expand) = r.expand() else {
        return out;
    };
    let contract = r.contract();
    let envs: Vec<Env> = {
        let mut e: Vec<Env> = m.deploys.iter().map(|d| d.env).collect();
        e.extend(m.sources.iter().map(|s| s.env));
        e.sort();
        e.dedup();
        if e.is_empty() {
            vec![Env::Staging, Env::Production]
        } else {
            e
        }
    };
    let step = |id: String, kind: Kind, env: Env, what: String, how: String| Step {
        workflow: String::new(),
        inputs: Vec::new(),
        trigger: None,
        project_id: m
            .source(env)
            .map(|s| s.project_id.clone())
            .unwrap_or_default(),
        dataset: m.source(env).map(|s| s.dataset.clone()).unwrap_or_default(),
        started_at: 0,
        tool: String::new(),
        id,
        kind,
        env,
        what,
        how,
        after: Vec::new(),
        after_merge: false,
        verify: String::new(),
        status: Status::Pending,
        detail: String::new(),
        before: String::new(),
        after_count: String::new(),
        approval_id: String::new(),
        run_id: 0,
        tag: String::new(),
    };
    let dry = m
        .migrations
        .as_ref()
        .is_some_and(|x| !x.dry_input.is_empty());
    let mut prev: Option<String> = None;
    // Expand: dry run then the migration, staging first. It is additive, so
    // it runs before the merge: the new code finds the field set.
    for env in &envs {
        if dry {
            let mut s = step(
                format!("dry_run_expand_{}", env.word()),
                Kind::DryRun,
                *env,
                format!(
                    "Dry run the expand migration `{}` on {}",
                    expand.file,
                    env.word()
                ),
                migration_how(m, &expand.file, *env, true),
            );
            s.tool = tool_for(Kind::DryRun, *env, None).into();
            s.after.extend(prev.clone());
            prev = Some(s.id.clone());
            out.push(s);
        }
        let mut s = step(
            format!("expand_{}", env.word()),
            Kind::Expand,
            *env,
            format!(
                "Run the expand migration `{}` on {} (sets the new field where it is missing; \
                 old code ignores it)",
                expand.file,
                env.word()
            ),
            migration_how(m, &expand.file, *env, false),
        );
        s.tool = tool_for(Kind::Expand, *env, None).into();
        s.verify = r.count_new_missing.clone();
        s.after.extend(prev.clone());
        prev = Some(s.id.clone());
        out.push(s);
    }
    // The deploys that ship the code, with their real triggers.
    let last_expand = prev.clone();
    let mut deploy_ids: Vec<(Env, String)> = Vec::new();
    for env in &envs {
        let d = m.deploy(*env);
        let how = match d {
            Some(d) => match &d.on {
                On::Merge(_) => format!(
                    "wait for `{}` ({}), which runs {}",
                    d.name,
                    d.file,
                    d.on.words()
                ),
                On::Tag(p) => format!(
                    "cut the next `{p}` tag at the merge commit, then wait for `{}` ({})",
                    d.name, d.file
                ),
                On::Dispatch => format!(
                    "start `{}` ({}) on the base branch, then wait for it",
                    d.name, d.file
                ),
            },
            None => format!("no workflow deploys {}", env.word()),
        };
        let what = match d.map(|d| &d.on) {
            Some(On::Tag(_)) => format!(
                "Tag the production release so {} deploys the change",
                env.word()
            ),
            _ => format!("Deploy the change to {}", env.word()),
        };
        let mut s = step(
            format!("deploy_{}", env.word()),
            Kind::Deploy,
            *env,
            what,
            how,
        );
        s.tool = tool_for(Kind::Deploy, *env, d.map(|d| &d.on)).into();
        match d {
            Some(d) => {
                s.workflow = d.file.clone();
                s.trigger = Some(d.on.clone());
            }
            None => {
                s.status = Status::NoAccess;
                s.detail = format!(
                    "no workflow in the repository deploys {}. Grant it: a deploy workflow \
                     whose job names the `{}` environment (on merge, on a tag or on \
                     workflow_dispatch)",
                    env.word(),
                    env.word()
                );
            }
        }
        s.after_merge = true;
        s.after.extend(last_expand.clone());
        if let Some((_, id)) = deploy_ids.last() {
            s.after.push(id.clone());
        }
        deploy_ids.push((*env, s.id.clone()));
        out.push(s);
    }
    // Contract: only after that environment's deploy was seen succeeding
    // (and production's after staging's contract).
    if let Some(c) = contract {
        let mut prev_contract: Option<String> = None;
        for env in &envs {
            let deploy_id = deploy_ids
                .iter()
                .find(|(e, _)| e == env)
                .map(|(_, id)| id.clone());
            // Old code runs in every environment until production deploys:
            // the contract waits for the production deploy everywhere.
            let prod_deploy = deploy_ids
                .iter()
                .find(|(e, _)| *e == Env::Production)
                .map(|(_, id)| id.clone());
            let mut after: Vec<String> = deploy_id.into_iter().collect();
            if *env == Env::Production {
                after.extend(prev_contract.clone());
            }
            if let Some(p) = prod_deploy {
                if !after.contains(&p) {
                    after.push(p);
                }
            }
            if dry {
                let mut s = step(
                    format!("dry_run_contract_{}", env.word()),
                    Kind::DryRun,
                    *env,
                    format!(
                        "Dry run the contract migration `{}` on {}",
                        c.file,
                        env.word()
                    ),
                    migration_how(m, &c.file, *env, true),
                );
                s.tool = tool_for(Kind::DryRun, *env, None).into();
                s.after = after.clone();
                s.after_merge = true;
                after = vec![s.id.clone()];
                out.push(s);
            }
            let mut s = step(
                format!("contract_{}", env.word()),
                Kind::Contract,
                *env,
                format!(
                    "Run the contract migration `{}` on {} (removes {}), only after the \
                     production deploy that ships the new code succeeded",
                    c.file,
                    env.word(),
                    r.removed_fields()
                        .iter()
                        .map(|f| format!("`{f}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                migration_how(m, &c.file, *env, false),
            );
            s.tool = tool_for(Kind::Contract, *env, None).into();
            s.verify = r.count_old_present.clone();
            s.after = after;
            s.after_merge = true;
            prev_contract = Some(s.id.clone());
            out.push(s);
        }
    }
    for s in out.iter_mut() {
        if !matches!(s.kind, Kind::DryRun | Kind::Expand | Kind::Contract) {
            continue;
        }
        let file = if s.id.contains("contract") {
            contract.map(|c| c.file.clone()).unwrap_or_default()
        } else {
            expand.file.clone()
        };
        match &m.migrations {
            Some(r) => {
                s.workflow = r.file.clone();
                s.inputs = migration_inputs(m, &file, s.env, s.kind == Kind::DryRun);
            }
            None => {
                s.status = Status::NoAccess;
                s.detail = NO_MIGRATION_RUNNER.to_string();
            }
        }
    }
    if let Some(p) = &m.purge {
        let last = out.last().map(|s| s.id.clone());
        let mut s = step(
            "purge_production".to_string(),
            Kind::Purge,
            Env::Production,
            "Purge the production cache so pages render the new field".to_string(),
            format!("start `{}` ({})", p.name, p.file),
        );
        s.tool = tool_for(Kind::Purge, Env::Production, None).into();
        s.workflow = p.file.clone();
        s.after.extend(last);
        s.after_merge = true;
        out.push(s);
    }
    out
}

/// Whether migrations run in more than one environment: the PR's risk is
/// then at least Medium.
pub fn runs_in_many_envs(steps: &[Step]) -> bool {
    let envs: BTreeSet<Env> = steps
        .iter()
        .filter(|s| matches!(s.kind, Kind::Expand | Kind::Contract))
        .map(|s| s.env)
        .collect();
    envs.len() > 1
}

/// Raises the PR's risk floor for a release with migrations in several
/// environments.
pub fn raise_floor(floor: &mut super::pr_risk::Floor, steps: &[Step]) {
    if !runs_in_many_envs(steps) {
        return;
    }
    let envs: BTreeSet<Env> = steps
        .iter()
        .filter(|s| matches!(s.kind, Kind::Expand | Kind::Contract))
        .map(|s| s.env)
        .collect();
    floor.reasons.push(format!(
        "migrations run in {} ({} steps, ordered expand, deploy, contract)",
        envs.into_iter()
            .map(Env::word)
            .collect::<Vec<_>>()
            .join(" and "),
        steps
            .iter()
            .filter(|s| matches!(s.kind, Kind::Expand | Kind::Contract))
            .count()
    ));
    if floor.level < super::pr_risk::Level::Medium {
        floor.level = super::pr_risk::Level::Medium;
    }
}

// ── Rendering: one source for the ticket note and the PR ────────────────────

fn status_words(s: &Step) -> String {
    let d = s.detail.split_whitespace().collect::<Vec<_>>().join(" ");
    let d = d.trim_end_matches('.');
    match s.status {
        Status::Pending if s.after_merge => "CoderHelm does this after the merge.".to_string(),
        Status::Pending => "CoderHelm does this after the pull request opens.".to_string(),
        Status::Approval => format!("Waiting for approval ({d})."),
        Status::Running => format!("Running: {d}."),
        Status::Done => format!("Done by CoderHelm: {d}."),
        Status::Failed => format!("Failed: {d}. Later steps wait."),
        Status::Declined => format!("Declined: {d}. Later steps wait."),
        Status::NoAccess => format!("Not done: CoderHelm can't do this yet. {d}."),
    }
}

fn gate_words(tool: &str) -> &'static str {
    match tool {
        "" => "",
        "release_production_migration"
        | "release_production_tag"
        | "release_production_deploy"
        | "release_cache_purge" => {
            " Gate: consequential (an approval card, unless the team allowed it)."
        }
        _ => " Gate: write (runs when the team allows it, else an approval card).",
    }
}

/// The release lines, numbered, for the ticket note and the PR.
pub fn note_lines(r: &Release, steps: &[Step], m: &Mechanics) -> Vec<String> {
    let mut out = Vec::new();
    if !m.deploys.is_empty() {
        out.push(format!(
            "How it deploys: {}.",
            m.deploys
                .iter()
                .map(Deploy::line)
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    for i in &r.impact {
        let env = i
            .env()
            .map(|e| capital(e.word()))
            .unwrap_or_else(|| i.env.clone());
        if !i.error.trim().is_empty() {
            out.push(format!(
                "{env} (dataset `{}`): the count query `{}` failed: {}",
                i.dataset,
                one_line(&i.query),
                one_line(&i.error)
            ));
        } else {
            out.push(format!(
                "{env} (dataset `{}`): `{}` returned {}",
                i.dataset,
                one_line(&i.query),
                one_line(&i.result)
            ));
        }
    }
    if !r.live_items.is_empty() {
        out.push(format!(
            "Live items whose behaviour changes during the transition: {}",
            r.live_items
                .iter()
                .map(|x| one_line(x))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    if !r.fallback.trim().is_empty() {
        out.push(format!(
            "Fallback while both fields exist: {} (tested in `{}`)",
            one_line(&r.fallback),
            one_line(&r.fallback_test)
        ));
    }
    for (n, s) in steps.iter().enumerate() {
        let mut t = format!(
            "{}. {}. How: {}.",
            n + 1,
            one_line(&s.what),
            one_line(&s.how)
        );
        if !s.after.is_empty() {
            let names: Vec<String> = s
                .after
                .iter()
                .filter_map(|id| steps.iter().position(|x| &x.id == id))
                .map(|i| (i + 1).to_string())
                .collect();
            if !names.is_empty() {
                t.push_str(&format!(" After step {}.", names.join(", ")));
            }
        }
        t.push_str(gate_words(&s.tool));
        if !s.verify.trim().is_empty() {
            t.push_str(&format!(
                " Verified by `{}` returning 0",
                one_line(&s.verify)
            ));
            match (s.before.trim(), s.after_count.trim()) {
                ("", "") => t.push('.'),
                (b, "") => t.push_str(&format!(" (before: {b}).")),
                (b, a) => t.push_str(&format!(" (before: {b}, after: {a}).")),
            }
        }
        t.push(' ');
        t.push_str(&status_words(s));
        out.push(t);
    }
    out
}

/// The PR section's markers: CoderHelm replaces what is between them.
pub const SECTION_START: &str = "<!-- coderhelm:release -->";
pub const SECTION_END: &str = "<!-- /coderhelm:release -->";

/// The PR's release section.
pub fn section(lines: &[String]) -> String {
    if lines.is_empty() {
        return String::new();
    }
    let mut out = format!("{SECTION_START}\n## Release steps (done by CoderHelm)\n\n");
    for l in lines {
        if l.starts_with(|c: char| c.is_ascii_digit()) {
            out.push_str(&format!("{l}\n"));
        } else {
            out.push_str(&format!("- {l}\n"));
        }
    }
    out.push_str(SECTION_END);
    out
}

/// The release section a body has now (empty when none).
pub fn current_section(body: &str) -> String {
    match (body.find(SECTION_START), body.find(SECTION_END)) {
        (Some(a), Some(b)) if b > a => body[a..b + SECTION_END.len()].to_string(),
        _ => String::new(),
    }
}

/// The body with its release section replaced (or added at the end).
pub fn put_section(body: &str, section: &str) -> String {
    if let (Some(a), Some(b)) = (body.find(SECTION_START), body.find(SECTION_END)) {
        if b > a {
            let mut out = body[..a].trim_end().to_string();
            if !section.is_empty() {
                out.push_str("\n\n");
                out.push_str(section);
            }
            let rest = body[b + SECTION_END.len()..].trim_start();
            if !rest.is_empty() {
                out.push_str("\n\n");
                out.push_str(rest);
            }
            return out;
        }
    }
    if section.is_empty() {
        return body.to_string();
    }
    format!("{}\n\n{section}", body.trim_end())
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Adds a `dataset` choice to the `query_content` tool, so the planner can
/// count in every environment it can read.
pub fn add_dataset_param(t: &mut crate::agent::llm::ToolDefinition, datasets: &[String]) {
    if datasets.len() < 2 {
        return;
    }
    if let Some(p) = t.input_schema["properties"].as_object_mut() {
        p.insert(
            "dataset".to_string(),
            serde_json::json!({
                "type": "string",
                "enum": datasets,
                "description": "The dataset to read (default: the staging one). Production is read only, like every query here."
            }),
        );
    }
    t.description.push_str(&format!(
        " Datasets: {}; pass `dataset` to pick one.",
        datasets.join(", ")
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const TAG_PROD: &str = r#"
name: Deploy web
on:
  push:
    branches: [main]
    tags: ['v*']
jobs:
  staging:
    if: github.ref == 'refs/heads/main'
    environment: staging
    runs-on: ubuntu-latest
    steps:
      - run: npm run deploy
  production:
    if: startsWith(github.ref, 'refs/tags/')
    environment: production
    runs-on: ubuntu-latest
    steps:
      - run: npm run deploy
"#;

    const MIGRATE: &str = r#"
name: Run content migration
on:
  workflow_dispatch:
    inputs:
      dataset:
        type: choice
        options: [staging, production]
      migration:
        type: string
      dry_run:
        type: boolean
jobs:
  migrate:
    runs-on: ubuntu-latest
    steps:
      - run: npx sanity migration run ${{ inputs.migration }} --dataset ${{ inputs.dataset }}
"#;

    fn source(dataset: &str) -> ContentSource {
        ContentSource {
            project_id: "abc123".into(),
            dataset: dataset.into(),
            origin: "sanity.config.ts".into(),
        }
    }

    fn repo() -> Mechanics {
        mechanics(
            &[
                (".github/workflows/deploy.yml".into(), TAG_PROD.into()),
                (".github/workflows/migrate.yml".into(), MIGRATE.into()),
                (
                    "studio/sanity.config.ts".into(),
                    "export default [{dataset: 'staging'}, {dataset: 'production'}]".into(),
                ),
            ],
            Some(&source("staging")),
        )
    }

    #[test]
    fn deploys_are_read_from_the_workflows() {
        let m = repo();
        assert_eq!(
            m.deploy(Env::Staging).map(|d| &d.on),
            Some(&On::Merge("main".into()))
        );
        assert_eq!(
            m.deploy(Env::Production).map(|d| &d.on),
            Some(&On::Tag("v*".into()))
        );
        // The production job never runs on merge.
        assert!(!m
            .deploys
            .iter()
            .any(|d| d.env == Env::Production && matches!(d.on, On::Merge(_))));
        let r = m.migrations.as_ref().unwrap();
        assert_eq!(r.env_input, "dataset");
        assert_eq!(r.dry_input, "dry_run");
        assert_eq!(r.file_input, "migration");
        assert_eq!(r.env_value(Env::Production, None), "production");
        assert_eq!(m.source(Env::Production).unwrap().dataset, "production");
        assert_eq!(m.source(Env::Staging).unwrap().dataset, "staging");
        assert!(m
            .prompt_section()
            .contains("Production deploys on a `v*` tag"));
    }

    #[test]
    fn one_migration_that_sets_and_removes_is_sent_back() {
        let patch = "+export default defineMigration({\n+  migrate: { object(node) {\n+    return [at('newFlag', setIfMissing(node.oldA || node.oldB)), at('oldA', unset()), at('oldB', unset())]\n+  }}\n+})";
        let files = vec![(
            "studio/migrations/flag/index.ts".to_string(),
            "added".to_string(),
            patch.to_string(),
        )];
        let g = diff_gaps(&files);
        assert!(
            g[0].contains("sets `newFlag` and removes `oldA`, `oldB` in one migration"),
            "{g:?}"
        );
        assert!(g
            .iter()
            .any(|x| x.starts_with("Add a test proving the fallback")));
    }

    #[test]
    fn split_migrations_with_fallback_and_test_pass() {
        let files = vec![
            (
                "studio/migrations/flag-expand/index.ts".to_string(),
                "added".to_string(),
                "+return [at('newFlag', setIfMissing(node.oldA ?? false))]".to_string(),
            ),
            (
                "studio/migrations/flag-contract/index.ts".to_string(),
                "added".to_string(),
                "+return [at('oldA', unset())]".to_string(),
            ),
            (
                "web/components/Block.tsx".to_string(),
                "modified".to_string(),
                "-const big = block.oldA\n+const big = block.newFlag ?? block.oldA".to_string(),
            ),
            (
                "web/components/Block.test.tsx".to_string(),
                "added".to_string(),
                "+render({oldA: true}) // newFlag missing\n+expect(h1).toBeTruthy()".to_string(),
            ),
        ];
        assert!(diff_gaps(&files).is_empty(), "{:?}", diff_gaps(&files));
    }

    #[test]
    fn code_without_a_fallback_is_sent_back() {
        let files = vec![
            (
                "migrations/2-contract.ts".to_string(),
                "added".to_string(),
                "+patch.unset(['oldA'])".to_string(),
            ),
            (
                "web/Block.tsx".to_string(),
                "modified".to_string(),
                "-const big = block.oldA\n+const big = block.newFlag".to_string(),
            ),
        ];
        let g = diff_gaps(&files);
        assert!(
            g.iter()
                .any(|x| x.contains("The code stops reading `oldA`")),
            "{g:?}"
        );
    }

    /// A schedule data fix: one migration writes the right class time and
    /// bumps `updated_at`. Writing a column is not removing it, so the plan
    /// needs no expand and contract (a run once was sent back to split a
    /// data fix because its release listed `updated_at`).
    #[test]
    fn writing_a_column_is_not_a_removal() {
        let sql = "UPDATE class_schedule SET start_time = '08:00', updated_at = now() \
                   WHERE class_id = 42;";
        let t = touches(sql);
        assert!(t.removes.is_empty(), "{t:?}");
        assert!(!shows_removal(sql, None));
        assert!(!shows_removal(sql, Some("updated_at")));
        let rows = "DELETE FROM class_schedule WHERE updated_at < now() - interval '1 year';";
        assert!(
            !shows_removal(rows, Some("updated_at")),
            "rows, not a field"
        );
        let soft = "Set deleted_at on the duplicate row and update updated_at.";
        assert!(!shows_removal(soft, Some("updated_at")));

        let files = vec![(
            "src/database/migrations/1783400000000-FixClassTime.ts".to_string(),
            "added".to_string(),
            format!("+await queryRunner.query(`{sql}`);"),
        )];
        assert!(diff_gaps(&files).is_empty(), "{:?}", diff_gaps(&files));

        let plan_text = "Correct the Saturday class time from 9am to 8am with a data \
                         migration that runs UPDATE class_schedule SET start_time = '08:00', \
                         updated_at = now() for the one class.";
        let paths = vec!["src/database/migrations/1783400000000-FixClassTime.ts".to_string()];
        assert!(gaps(None, &paths, plan_text, &repo()).is_empty());
        // The planner listed the column it writes as a changed field: still
        // no removal.
        let written = Release {
            fields: vec![FieldChange {
                old: vec!["updated_at".into()],
                new: String::new(),
                types: vec![],
            }],
            ..Default::default()
        };
        let g = gaps(Some(&written), &paths, plan_text, &repo()).join("\n");
        assert!(!g.contains("removes `updated_at`"), "{g}");
        assert!(!g.contains("Plan two migrations"), "{g}");
        let same = Release {
            fields: vec![FieldChange {
                old: vec!["start_time".into()],
                new: "start_time".into(),
                types: vec![],
            }],
            ..Default::default()
        };
        assert!(real_removals(&same, "Drop the start_time column").is_empty());
    }

    #[test]
    fn a_real_removal_or_rename_is_still_split() {
        let paths = vec!["db/migrations/0042_drop_legacy.sql".to_string()];
        // No release yet: words or a statement that drop a column ask for one.
        for text in [
            "Drop the legacy_flag column once the new code reads is_featured.",
            "ALTER TABLE pages DROP COLUMN legacy_flag;",
            "Rename the `title` field to `headline`.",
            "await queryRunner.dropColumn(\"pages\", \"legacy_flag\");",
            "table.renameColumn('title', 'headline')",
        ] {
            let g = gaps(None, &paths, text, &repo());
            assert_eq!(g.len(), 1, "{text}: {g:?}");
            assert!(g[0].contains("Write `release`"), "{g:?}");
        }
        // A release whose old field the words remove: split it.
        let r = Release {
            fields: vec![FieldChange {
                old: vec!["legacy_flag".into()],
                new: "is_featured".into(),
                types: vec![],
            }],
            ..Default::default()
        };
        let text = "Replace legacy_flag with is_featured, then remove legacy_flag.";
        let g = gaps(Some(&r), &paths, text, &repo()).join("\n");
        assert!(g.contains("removes `legacy_flag`"), "{g}");
        assert!(g.contains("Plan two migrations"), "{g}");
        // Query-builder schema calls are read as what they are.
        let t = touches("await queryRunner.renameColumn(\"pages\", \"title\", \"headline\")");
        assert!(
            t.removes.contains("title") && t.sets.contains("headline"),
            "{t:?}"
        );
        let t = touches("table.dropColumn('legacy_flag')");
        assert!(t.removes.contains("legacy_flag"), "{t:?}");
    }

    #[test]
    fn sql_rename_is_expand_and_contract_at_once() {
        let t = touches("ALTER TABLE t RENAME COLUMN old_name TO new_name;");
        assert!(t.removes.contains("old_name") && t.sets.contains("new_name"));
        let t = touches("db.coll.updateMany({}, {$set: {newF: 1}, $unset: {oldF: ''}})");
        assert!(
            t.sets.contains("newF") && t.removes.contains("oldF"),
            "{t:?}"
        );
    }

    fn release() -> Release {
        Release {
            fields: vec![FieldChange {
                old: vec!["oldA".into(), "oldB".into()],
                new: "newFlag".into(),
                types: vec!["hero".into()],
            }],
            migrations: vec![
                PlannedMigration {
                    file: "studio/migrations/expand.ts".into(),
                    phase: "expand".into(),
                },
                PlannedMigration {
                    file: "studio/migrations/contract.ts".into(),
                    phase: "contract".into(),
                },
            ],
            fallback: "Block.tsx reads newFlag ?? oldA".into(),
            fallback_test: "web/Block.test.tsx".into(),
            impact: vec![
                Impact {
                    env: "staging".into(),
                    dataset: "staging".into(),
                    query: "count(*[_type == 'page'])".into(),
                    result: "160".into(),
                    error: String::new(),
                },
                Impact {
                    env: "production".into(),
                    dataset: "production".into(),
                    query: "count(*[_type == 'page'])".into(),
                    result: "285".into(),
                    error: String::new(),
                },
            ],
            live_items: vec!["36 pages with oldA set: home, about".into()],
            count_new_missing: "count(*[!defined(newFlag)])".into(),
            count_old_present: "count(*[defined(oldA)])".into(),
        }
    }

    /// The words of a plan that replaces two fields with a new one.
    const PLAN_TEXT: &str = "Replace the hero's oldA and oldB flags with newFlag. The contract \
                             migration removes the oldA and oldB fields after the production deploy.";

    fn tasks() -> Vec<String> {
        vec![
            "studio/migrations/expand.ts".into(),
            "studio/migrations/contract.ts".into(),
            "web/Block.tsx".into(),
            "web/Block.test.tsx".into(),
        ]
    }

    #[test]
    fn a_complete_release_passes_the_plan_check() {
        assert!(
            gaps(Some(&release()), &tasks(), PLAN_TEXT, &repo()).is_empty(),
            "{:?}",
            gaps(Some(&release()), &tasks(), PLAN_TEXT, &repo())
        );
    }

    #[test]
    fn a_plan_that_migrates_without_a_release_is_sent_back() {
        let g = gaps(None, &tasks(), PLAN_TEXT, &repo());
        assert_eq!(g.len(), 1);
        assert!(g[0].contains("Production deploys on a `v*` tag"), "{g:?}");
    }

    #[test]
    fn one_migration_or_no_fallback_or_no_counts_is_sent_back() {
        let mut r = release();
        r.migrations.retain(|m| m.is_expand());
        r.fallback.clear();
        r.impact.truncate(1);
        r.impact[0].result = "Direct dataset queries were not possible".into();
        let g = gaps(Some(&r), &tasks(), PLAN_TEXT, &repo()).join("\n");
        assert!(g.contains("Plan two migrations"), "{g}");
        assert!(g.contains("release.fallback"), "{g}");
        assert!(g.contains("Count the affected items in production"), "{g}");
        assert!(
            g.contains("The staging impact says it could not look"),
            "{g}"
        );
    }

    #[test]
    fn a_failed_lookup_with_its_error_is_accepted() {
        let mut r = release();
        r.impact[1].result.clear();
        r.impact[1].error = "the read API returned 401 Unauthorized".into();
        r.live_items.clear();
        assert!(gaps(Some(&r), &tasks(), PLAN_TEXT, &repo()).is_empty());
    }

    #[test]
    fn production_items_must_be_named() {
        let mut r = release();
        r.live_items.clear();
        let g = gaps(Some(&r), &tasks(), PLAN_TEXT, &repo());
        assert!(g.iter().any(|x| x.contains("release.live_items")), "{g:?}");
    }

    #[test]
    fn steps_follow_the_real_deploy_triggers() {
        let m = repo();
        let s = steps(&release(), &m);
        let ids: Vec<&str> = s.iter().map(|x| x.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "dry_run_expand_staging",
                "expand_staging",
                "dry_run_expand_production",
                "expand_production",
                "deploy_staging",
                "deploy_production",
                "dry_run_contract_staging",
                "contract_staging",
                "dry_run_contract_production",
                "contract_production",
            ]
        );
        let get = |id: &str| s.iter().find(|x| x.id == id).unwrap();
        // Expand runs before the merge; contract only after the production
        // deploy, which is a tag.
        assert!(!get("expand_production").after_merge);
        assert_eq!(get("deploy_production").tool, "release_production_tag");
        assert_eq!(get("deploy_staging").tool, "");
        assert!(get("dry_run_contract_staging")
            .after
            .contains(&"deploy_production".to_string()));
        assert_eq!(
            get("contract_production").tool,
            "release_production_migration"
        );
        assert_eq!(get("expand_staging").tool, "release_staging_migration");
        assert!(get("expand_production").how.contains("dataset=production"));
        assert!(get("dry_run_expand_staging").how.contains("dry_run=true"));
        assert!(runs_in_many_envs(&s));
        let lines = note_lines(&release(), &s, &m);
        let text = lines.join("\n");
        assert!(text.contains(
            "Production (dataset `production`): `count(*[_type == 'page'])` returned 285"
        ));
        assert!(text.contains("Production deploys on a `v*` tag"));
        assert!(text.contains("Gate: consequential"));
    }

    #[test]
    fn release_risk_is_at_least_medium() {
        let mut f = super::super::pr_risk::Floor {
            level: super::super::pr_risk::Level::Low,
            reasons: Vec::new(),
        };
        raise_floor(&mut f, &steps(&release(), &repo()));
        assert_eq!(f.level, super::super::pr_risk::Level::Medium);
        assert!(f.reasons[0].contains("staging and production"));
    }

    #[test]
    fn the_section_is_replaced_in_place() {
        let s1 = section(&["1. a".to_string()]);
        let body = put_section("Summary\n\nVerification", &s1);
        assert!(body.ends_with(SECTION_END));
        let s2 = section(&["1. b".to_string()]);
        let again = put_section(&body, &s2);
        assert!(again.contains("1. b") && !again.contains("1. a"));
        assert_eq!(again.matches(SECTION_START).count(), 1);
    }

    #[test]
    fn counts_are_read_from_results() {
        assert_eq!(count_of("160"), Some(160));
        assert_eq!(count_of(r#"{"result": 36}"#), Some(36));
        assert_eq!(count_of("[1,2]"), Some(2));
        assert_eq!(count_of("none"), None);
    }
}
