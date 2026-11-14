//! Work a ticket needs beyond the code: done by the run, not handed out.
//!
//! A run should do everything the ticket needs. Before a step goes to a
//! person, it is decided in this order, in code, with the reason recorded
//! on the step ([`Doer`]):
//!
//! 1. the change itself does it (`this_pr`): a content or data migration in
//!    the repository's migrations folder that replaces the existing item by
//!    key, test fixtures or end-to-end setup that create their own content,
//!    config or registration files;
//! 2. CoderHelm does it after the pull request opens, with access it already
//!    has (`coderhelm`): dispatching the repository's studio or staging
//!    deploy workflow (or adding the label its CI deploys on), re-running the
//!    end-to-end workflow, running the confirm query ([`perform`]). The
//!    results (the run link, the query output) go into the note;
//! 3. a person (`person`) only for what is left: production content, an
//!    action that needs a person's approval, access CoderHelm doesn't have.
//!    Such a step names why CoderHelm couldn't (`why_person`) and how to
//!    grant it so it can next time (`grant`).
//!
//! What the repository offers is read from its files ([`Abilities`]): the
//! CMS project and dataset its config or codegen environment names (so the
//! public read endpoint is known and the note's lookups can be made and
//! quoted), its deploy workflows and scripts, its migrations and fixtures
//! folders. [`gaps`] checks a planner's note against them: a note that says
//! it couldn't look while a readable source exists, a step without an owner
//! or an order that puts content before the deploy that makes its fields
//! exist, a person step the change or CoderHelm could do, a person step with
//! no reason, content without exact values, a confirm without a query.
//! Those go back to the planner. The ticket note and the pull request's
//! "Not in this PR" section are both rendered from the same steps
//! ([`note_markdown`], [`pr_lines`]).

use common::pr_steps::{self, Checked, State, Step, When};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::fix_location::OutsideChange;

/// The read API version the derived query endpoint uses.
pub const CMS_API_VERSION: &str = "v2021-10-21";
/// Bytes of a query result quoted in a note.
const RESULT_QUOTE: usize = 600;

// ── What the repository lets a run do itself ────────────────────────────────

/// A CMS whose read API the run can query: the project and dataset the
/// repository's config or codegen environment names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentSource {
    pub project_id: String,
    pub dataset: String,
    /// Where the values came from (a file path, or "codegen environment").
    pub origin: String,
}

impl ContentSource {
    /// The project's public query endpoint.
    pub fn query_url(&self) -> String {
        format!(
            "https://{}.api.sanity.io/{CMS_API_VERSION}/data/query/{}",
            self.project_id, self.dataset
        )
    }

    /// The labelled names a note uses: never one value under another's name.
    pub fn labels(&self) -> String {
        format!(
            "CMS project id `{}`, dataset `{}` (from {})",
            self.project_id, self.dataset, self.origin
        )
    }
}

/// How a deploy (or an end-to-end run) is started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// A workflow with `workflow_dispatch`: CoderHelm can start it.
    Dispatch,
    /// A workflow that runs when this label is added to a pull request:
    /// CoderHelm can add it.
    Label(String),
    /// A workflow that runs on a push or merge: it runs when the PR merges.
    OnMerge,
    /// A package script a person runs (`npm run deploy`).
    Script,
}

/// A workflow or script that deploys the CMS studio or schema, or runs the
/// end-to-end tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// The workflow's `name:` or the script's name.
    pub name: String,
    /// The workflow file (`.github/workflows/x.yml`) or the package file.
    pub file: String,
    /// The command it runs (`sanity deploy`, `npm run deploy:studio`).
    pub command: String,
    pub trigger: Trigger,
}

impl Route {
    /// The route in a note: its name, file and how it starts.
    pub fn line(&self) -> String {
        let how = match &self.trigger {
            Trigger::Dispatch => "started with workflow_dispatch".to_string(),
            Trigger::Label(l) => format!("runs when the `{l}` label is added to the PR"),
            Trigger::OnMerge => "runs on merge".to_string(),
            Trigger::Script => format!("run `{}`", self.command),
        };
        format!("`{}` ({}, {how})", self.name, self.file)
    }

    /// Whether CoderHelm can start it itself.
    pub fn startable(&self) -> bool {
        matches!(self.trigger, Trigger::Dispatch | Trigger::Label(_))
    }

    /// Whether a step's text names this route.
    fn named_in(&self, text: &str) -> bool {
        let t = text.to_lowercase();
        let file = self.file.rsplit('/').next().unwrap_or(&self.file);
        [self.name.as_str(), file, self.command.as_str()]
            .iter()
            .filter(|s| s.len() >= 3)
            .any(|s| t.contains(&s.to_lowercase()))
    }
}

/// What the repository lets a run do itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Abilities {
    pub source: Option<ContentSource>,
    /// Studio or schema deploys, staging first.
    pub deploys: Vec<Route>,
    /// Workflows that run the end-to-end tests.
    pub e2e: Vec<Route>,
    /// The folder content or data migrations live in.
    pub migrations_dir: Option<String>,
    /// The folder end-to-end fixtures or setup live in.
    pub fixtures_dir: Option<String>,
    /// The repository has a staging CMS write connection CoderHelm may use.
    pub staging_write: bool,
    /// Every dataset the repository names for an environment (staging and
    /// production), each readable with `query_content`'s `dataset`.
    pub datasets: Vec<String>,
    /// How the repository releases: each environment's deploy trigger, its
    /// migration and cache-purge workflows, each environment's dataset.
    pub release: super::release_plan::Mechanics,
}

fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn unquote(s: &str) -> &str {
    s.trim()
        .trim_end_matches([',', ';'])
        .trim()
        .trim_matches(|c| c == '\'' || c == '"' || c == '`')
}

/// The value of `key: value` or `key=value` on a line, when the line sets
/// that key (case-insensitive key match on the given names).
fn value_of<'a>(line: &'a str, keys: &[&str]) -> Option<&'a str> {
    let l = line.trim().trim_start_matches("export ");
    let (k, v) = l.split_once(['=', ':'])?;
    let k = k.trim().trim_matches(|c| c == '\'' || c == '"');
    keys.iter()
        .any(|want| k.eq_ignore_ascii_case(want))
        .then(|| unquote(v))
}

fn is_project_key(k: &str) -> bool {
    let u = k.to_ascii_uppercase();
    u.contains("SANITY") && u.contains("PROJECT") && u.ends_with("ID")
}

fn is_dataset_key(k: &str) -> bool {
    let u = k.to_ascii_uppercase();
    u.contains("SANITY") && u.ends_with("DATASET")
}

/// The CMS the repository reads, from its codegen environment (`env`) and
/// its files (`.env*` other than production ones, `sanity.config.*`,
/// `sanity.cli.*`). A dataset named for staging is preferred, since the
/// end-to-end tests read staging. `None` when no project id is named.
pub fn content_source(
    files: &[(String, String)],
    env: &[(String, String)],
) -> Option<ContentSource> {
    let mut project: Option<(String, String)> = None;
    let mut datasets: Vec<(String, String)> = Vec::new();
    let mut env_map: Vec<(String, String)> = env.to_vec();
    for (k, v) in env {
        if is_project_key(k) && valid_name(v) && project.is_none() {
            project = Some((v.clone(), "the codegen environment".into()));
        }
        if is_dataset_key(k) && valid_name(v) {
            datasets.push((v.clone(), format!("the codegen environment ({k})")));
        }
    }
    for (path, text) in files {
        let base = path.rsplit('/').next().unwrap_or(path);
        let is_env = base.starts_with(".env") && !base.contains("prod");
        let is_cfg = base.starts_with("sanity.config.") || base.starts_with("sanity.cli.");
        if !is_env && !is_cfg {
            continue;
        }
        for line in text.lines() {
            if is_env {
                if let Some((k, v)) = line.trim().split_once('=') {
                    let (k, v) = (k.trim().trim_start_matches("export ").trim(), unquote(v));
                    env_map.push((k.to_string(), v.to_string()));
                    if is_project_key(k) && valid_name(v) && project.is_none() {
                        project = Some((v.to_string(), path.clone()));
                    }
                    if is_dataset_key(k) && valid_name(v) {
                        let from = if base.contains("staging") {
                            format!("{path} (staging)")
                        } else {
                            path.clone()
                        };
                        datasets.push((v.to_string(), from));
                    }
                }
                continue;
            }
            for (keys, is_project) in [(&["projectId"][..], true), (&["dataset"][..], false)] {
                let Some(raw) = value_of(line, keys) else {
                    continue;
                };
                // `process.env.X` (or `import.meta.env.X`) resolves through
                // the environment values read so far.
                let v = match raw.rsplit_once("env.") {
                    Some((_, var)) => {
                        let var =
                            var.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_');
                        let var = var
                            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                            .next()
                            .unwrap_or("");
                        match env_map.iter().find(|(k, _)| k == var) {
                            Some((_, v)) => v.clone(),
                            None => continue,
                        }
                    }
                    None => raw.to_string(),
                };
                if !valid_name(&v) {
                    continue;
                }
                if is_project && project.is_none() {
                    project = Some((v, path.clone()));
                } else if !is_project {
                    datasets.push((v, path.clone()));
                }
            }
        }
    }
    let (project_id, origin) = project?;
    let (dataset, dataset_from) = datasets
        .iter()
        .find(|(d, from)| d.contains("staging") || from.contains("staging"))
        .or_else(|| datasets.first())
        .cloned()?;
    Some(ContentSource {
        project_id,
        dataset,
        origin: if origin == dataset_from {
            origin
        } else {
            format!("{origin}; dataset from {dataset_from}")
        },
    })
}

/// The top-level `name:` of a workflow file.
fn workflow_name(text: &str) -> Option<String> {
    text.lines()
        .find(|l| l.starts_with("name:"))
        .map(|l| unquote(&l[5..]).to_string())
        .filter(|n| !n.is_empty())
}

/// The label a workflow runs on, when it runs on a labelled pull request.
fn workflow_label(text: &str) -> Option<String> {
    if !text.contains("labeled") {
        return None;
    }
    for pat in ["github.event.label.name == ", "labels.*.name, "] {
        if let Some(i) = text.find(pat) {
            let rest = &text[i + pat.len()..];
            let q = rest.chars().next()?;
            if q == '\'' || q == '"' {
                let inner = &rest[1..];
                let end = inner.find(q)?;
                return Some(inner[..end].to_string());
            }
        }
    }
    None
}

/// The command a workflow runs that deploys the studio or schema: a `run`
/// line that deploys, never the words "deploy" and "studio" somewhere in
/// the file. A test workflow that only mentions both once started for a
/// step that said "deploy the staging Studio".
fn deploy_command(text: &str) -> Option<String> {
    let about_cms = {
        let l = text.to_lowercase();
        l.contains("studio") || l.contains("sanity") || l.contains("schema")
    };
    text.lines()
        .map(str::trim)
        .filter(|l| !l.starts_with('#'))
        .map(|l| l.trim_start_matches("- ").trim())
        .filter_map(|l| l.strip_prefix("run:").map(str::trim))
        .find(|l| {
            let c = l.to_lowercase();
            if c.contains("sanity deploy") || c.contains("sanity schema deploy") {
                return true;
            }
            let runner = ["npm ", "pnpm ", "yarn ", "npx ", "make ", "sanity "]
                .iter()
                .any(|r| c.starts_with(r) || c.contains(&format!("&& {r}")));
            runner && c.contains("deploy") && about_cms
        })
        .map(str::to_string)
}

fn deploys_studio(text: &str) -> bool {
    deploy_command(text).is_some()
}

fn runs_e2e(text: &str) -> bool {
    let l = text.to_lowercase();
    l.contains("playwright test") || l.contains("cypress run") || l.contains("test:e2e")
}

fn route_for(path: &str, text: &str, command: &str) -> Route {
    let trigger = if text.contains("workflow_dispatch") {
        Trigger::Dispatch
    } else if let Some(l) = workflow_label(text) {
        Trigger::Label(l)
    } else {
        Trigger::OnMerge
    };
    Route {
        name: workflow_name(text)
            .unwrap_or_else(|| path.rsplit('/').next().unwrap_or(path).to_string()),
        file: path.to_string(),
        command: command.to_string(),
        trigger,
    }
}

/// The repository's deploy and end-to-end routes, migrations and fixtures
/// folders, from its tree and files (workflows, package files).
pub fn routes(tree: &[String], files: &[(String, String)]) -> (Vec<Route>, Vec<Route>) {
    let mut deploys = Vec::new();
    let mut e2e = Vec::new();
    for (path, text) in files {
        let workflow = path.starts_with(".github/workflows/")
            && (path.ends_with(".yml") || path.ends_with(".yaml"));
        if workflow {
            if deploys_studio(text) {
                let cmd = deploy_command(text).unwrap_or_default();
                deploys.push(route_for(path, text, &cmd));
            }
            if runs_e2e(text) {
                e2e.push(route_for(path, text, ""));
            }
            continue;
        }
        if path.rsplit('/').next() == Some("package.json") {
            let Ok(v) = serde_json::from_str::<Value>(text) else {
                continue;
            };
            let dir = path.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
            for (name, cmd) in v["scripts"].as_object().into_iter().flatten() {
                let cmd = cmd.as_str().unwrap_or("");
                if cmd.contains("sanity deploy") || cmd.contains("sanity schema deploy") {
                    let run = if dir.is_empty() {
                        format!("npm run {name}")
                    } else {
                        format!("cd {dir} && npm run {name}")
                    };
                    deploys.push(Route {
                        name: name.clone(),
                        file: path.clone(),
                        command: run,
                        trigger: Trigger::Script,
                    });
                }
            }
        }
    }
    let _ = tree;
    // Staging first, then what CoderHelm can start, then the rest.
    let rank = |r: &Route| {
        let staging = format!("{} {}", r.name, r.file)
            .to_lowercase()
            .contains("staging");
        (!staging, !r.startable())
    };
    deploys.sort_by_key(rank);
    e2e.sort_by_key(rank);
    (deploys, e2e)
}

/// The folder that holds `marker` (`migrations`, `fixtures`), shallowest
/// first, among the tree's files (not under node_modules).
fn folder_with(tree: &[String], markers: &[&str]) -> Option<String> {
    let mut found: Vec<String> = tree
        .iter()
        .filter(|p| !p.contains("node_modules/"))
        .filter_map(|p| {
            let parts: Vec<&str> = p.split('/').collect();
            let at = parts[..parts.len().saturating_sub(1)]
                .iter()
                .position(|s| markers.contains(&s.to_lowercase().as_str()))?;
            Some(parts[..=at].join("/"))
        })
        .collect();
    found.sort_by_key(|d| (d.matches('/').count(), d.clone()));
    found.into_iter().next()
}

/// What the repository lets a run do itself.
pub fn abilities(
    tree: &[String],
    files: &[(String, String)],
    env: &[(String, String)],
    staging_write: bool,
) -> Abilities {
    let (deploys, e2e) = routes(tree, files);
    let source = content_source(files, env);
    let release = super::release_plan::mechanics(files, source.as_ref());
    Abilities {
        datasets: release.datasets(),
        source,
        deploys,
        e2e,
        migrations_dir: folder_with(tree, &["migrations"]),
        fixtures_dir: folder_with(tree, &["fixtures", "seed", "seeds"]),
        staging_write,
        release,
    }
}

/// The files [`abilities`] reads from a tree.
pub fn files_to_read(tree: &[String]) -> Vec<String> {
    tree.iter()
        .filter(|p| !p.contains("node_modules/") && p.matches('/').count() <= 3)
        .filter(|p| {
            let b = p.rsplit('/').next().unwrap_or(p);
            (p.starts_with(".github/workflows/") && (b.ends_with(".yml") || b.ends_with(".yaml")))
                || b == "package.json"
                || b.starts_with("sanity.config.")
                || b.starts_with("sanity.cli.")
                || (b.starts_with(".env") && !b.contains("prod"))
        })
        .take(40)
        .cloned()
        .collect()
}

/// Reads what the repository lets a run do itself at `git_ref`.
pub async fn load(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
    env: &[(String, String)],
    staging_write: bool,
) -> Abilities {
    let tree: Vec<String> = match github.get_tree(owner, repo, git_ref).await {
        Ok(t) => t
            .into_iter()
            .filter(|e| e.entry_type == "blob")
            .map(|e| e.path)
            .collect(),
        Err(_) => return Abilities::default(),
    };
    let mut files = Vec::new();
    for p in files_to_read(&tree) {
        if let Ok(t) = github.read_file(owner, repo, &p, git_ref).await {
            files.push((p, t));
        }
    }
    abilities(&tree, &files, env, staging_write)
}

impl Abilities {
    /// What the planner is told: what it can query, what the change and
    /// CoderHelm can do, in the order steps are decided.
    pub fn prompt_section(&self) -> String {
        let mut out = String::from(
            "\n\n## What this repository lets the run do itself (decide every step in this order)\n\
             1. The change does it (`by: this_pr`, a task in tasks.md): a content or data migration \
             that replaces the existing item by key, test fixtures or end-to-end setup that create \
             their own content (the test then never depends on editors), config or registration.\n\
             2. CoderHelm does it after the PR opens (`by: coderhelm`), production included: \
             start a deploy or end-to-end workflow listed below, run a migration or content \
             mutations through a write connection, run the confirm query. When the team's agent \
             permissions say to ask, CoderHelm raises an approval card and waits for it; needing \
             approval is never a reason to hand a step to a person.\n\
             3. Only what no connection or permission lets CoderHelm do at all goes to a person \
             (`by: person`), each with `why_person` (the exact connection or permission missing) \
             and `grant` (how to give CoderHelm that access). Such a requirement is reported as \
             not done.\n\
             Give every step `kind` (deploy, e2e, content, migration, verify, other) and `target` \
             (staging, production): the run decides each step's action from them.\n",
        );
        match &self.source {
            Some(s) => out.push_str(&format!(
                "- Content: {}. Its read API is public: query it with `query_content` (a GROQ query; endpoint `{}`) \
                 before you write anything about existing content, and quote the query and its \
                 result in `outside_change.lookup`. Never write that you could not look up or \
                 that none exists without a failed attempt's error.\n",
                s.labels(),
                s.query_url()
            )),
            None => out.push_str("- Content: no CMS project id and dataset found in the repository's config or codegen environment.\n"),
        }
        for r in &self.deploys {
            out.push_str(&format!("- Studio/schema deploy: {}\n", r.line()));
        }
        for r in &self.e2e {
            out.push_str(&format!("- End-to-end workflow: {}\n", r.line()));
        }
        if let Some(d) = &self.migrations_dir {
            out.push_str(&format!(
                "- Migrations folder: `{d}` (content changes go here as a task)\n"
            ));
        }
        if let Some(d) = &self.fixtures_dir {
            out.push_str(&format!("- Fixtures folder: `{d}`\n"));
        }
        out.push_str(if self.staging_write {
            "- Staging CMS write connection: yes (CoderHelm can change staging content, never production)\n"
        } else {
            "- Staging CMS write connection: none for this repository\n"
        });
        out
    }

    fn startable_deploy(&self) -> Option<&Route> {
        self.deploys.iter().find(|r| r.startable())
    }

    fn startable_e2e(&self) -> Option<&Route> {
        self.e2e.iter().find(|r| r.startable())
    }
}

// ── The note ────────────────────────────────────────────────────────────────

/// Who does a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Doer {
    ThisPr,
    CoderHelm,
    Person,
}

impl Doer {
    pub fn parse(s: &str) -> Doer {
        match s
            .trim()
            .to_ascii_lowercase()
            .replace(['-', ' '], "_")
            .as_str()
        {
            "this_pr" | "pr" | "change" => Doer::ThisPr,
            "coderhelm" | "run" | "agent" => Doer::CoderHelm,
            _ => Doer::Person,
        }
    }
}

/// One step of the work outside the code, in the order it must happen.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteStep {
    /// What has to happen.
    #[serde(default)]
    pub step: String,
    /// `this_pr`, `coderhelm` or `person`.
    #[serde(default)]
    pub by: String,
    /// Who does it ("the engineer merging", "a content editor").
    #[serde(default)]
    pub owner: String,
    /// The workflow, command or screen it is done with.
    #[serde(default)]
    pub how: String,
    /// For a person: why CoderHelm couldn't (the missing permission or
    /// connection, by name, or why a person must decide).
    #[serde(default)]
    pub why_person: String,
    /// For a person: how to grant CoderHelm that access next time.
    #[serde(default)]
    pub grant: String,
    /// Exact values to set (field = value, reference targets, settings).
    #[serde(default, deserialize_with = "super::fix_location::string_or_list")]
    pub values: Vec<String>,
    /// Fields to remove.
    #[serde(default, deserialize_with = "super::fix_location::string_or_list")]
    pub remove: Vec<String>,
    /// The minimum the feature and its tests need to be meaningful.
    #[serde(default)]
    pub minimum: String,
    /// What happened when CoderHelm did it (a run link, a query result).
    #[serde(default)]
    pub result: String,
    /// CoderHelm did it and it worked.
    #[serde(default)]
    pub done: bool,
    /// The planner's step kind: `deploy`, `e2e`, `content`, `migration`,
    /// `verify` or `other`. Decides the action; the step's words are read
    /// only when it is missing ([`NoteStep::step_kind`]).
    #[serde(default)]
    pub kind: String,
    /// Where the step acts: `staging` or `production`.
    #[serde(default)]
    pub target: String,
    /// For content or a migration CoderHelm runs: the CMS mutations, as the
    /// CMS's mutate API takes them.
    #[serde(default)]
    pub mutations: Vec<Value>,
    /// For a migration CoderHelm runs: the change's file holding its
    /// mutations (`.json` array or `.ndjson`), read from the PR branch.
    #[serde(default)]
    pub migration: String,
    /// What CoderHelm's executor is doing with the step (see
    /// `outside_exec`): waiting on an approval card or a workflow run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<StepRun>,
}

/// What a step is, from the planner's structured `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    /// Deploys the studio, schema or app.
    Deploy,
    /// Runs the end-to-end tests.
    E2e,
    /// Changes content.
    Content,
    /// Runs a content or data migration.
    Migration,
    /// A read query that confirms.
    Verify,
    Other,
}

impl StepKind {
    pub fn parse(s: &str) -> Option<StepKind> {
        Some(
            match s
                .trim()
                .to_ascii_lowercase()
                .replace(['-', ' '], "_")
                .as_str()
            {
                "deploy" | "release" => StepKind::Deploy,
                "e2e" | "end_to_end" | "test" | "tests" => StepKind::E2e,
                "content" | "data" => StepKind::Content,
                "migration" | "migrate" => StepKind::Migration,
                "verify" | "confirm" | "query" => StepKind::Verify,
                "other" => StepKind::Other,
                _ => return None,
            },
        )
    }

    /// Whether it changes stored content (by mutations or a migration).
    pub fn writes(self) -> bool {
        matches!(self, StepKind::Content | StepKind::Migration)
    }
}

/// Where a step acts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Staging,
    Production,
}

impl Target {
    pub fn parse(s: &str) -> Option<Target> {
        match s.trim().to_ascii_lowercase().as_str() {
            "staging" | "stage" | "preview" | "test" => Some(Target::Staging),
            "production" | "prod" | "live" => Some(Target::Production),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Target::Staging => "staging",
            Target::Production => "production",
        }
    }
}

/// The pull request CoderHelm's outside steps run against.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    /// `owner/name`.
    pub repo: String,
    pub branch: String,
    pub pr_number: u64,
}

/// In a run across several repositories, the one whose pull request carries
/// the outside steps: the first the steps name (`owner/name`, or the name
/// alone), else the first in the plan's order. `None` with no repositories.
pub fn outside_repo<'a>(c: &OutsideChange, repos: &'a [String]) -> Option<&'a String> {
    let text = c
        .steps
        .iter()
        .map(|s| format!("{} {} {}", s.step, s.how, s.owner))
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    let named = |r: &&String| {
        let full = r.to_lowercase();
        let name = full.rsplit('/').next().unwrap_or(&full).to_string();
        text.contains(&full)
            || (name.len() >= 3
                && (words(&text).any(|w| w == name)
                    || (name.contains(['-', '_', '.']) && text.contains(&name))))
    };
    repos.iter().find(named).or_else(|| repos.first())
}

/// The executor's record of a step it started (see `outside_exec`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepRun {
    /// `waiting_approval`, `running`, `done` or `failed`.
    #[serde(default)]
    pub state: String,
    /// The approval card the step waits on.
    #[serde(default)]
    pub approval_id: String,
    /// The workflow run it started.
    #[serde(default)]
    pub workflow_run_id: u64,
    #[serde(default)]
    pub url: String,
    /// The workflow file it started.
    #[serde(default)]
    pub workflow: String,
    /// The run's conclusion once completed.
    #[serde(default)]
    pub conclusion: String,
    /// The tests that failed, read from the run.
    #[serde(default)]
    pub failing: Vec<String>,
    /// When it was started (RFC 3339).
    #[serde(default)]
    pub started_at: String,
}

impl NoteStep {
    pub fn doer(&self) -> Doer {
        Doer::parse(&self.by)
    }

    fn text(&self) -> String {
        format!("{} {}", self.step, self.how).to_lowercase()
    }

    /// What the step is: the planner's `kind` when it gave one, else read
    /// from the step's words.
    pub fn step_kind(&self) -> StepKind {
        if let Some(k) = StepKind::parse(&self.kind) {
            return k;
        }
        if self.words_deploy() {
            StepKind::Deploy
        } else if self.words_content() {
            StepKind::Content
        } else if self.words_e2e() {
            StepKind::E2e
        } else if self.words_verify() {
            StepKind::Verify
        } else {
            StepKind::Other
        }
    }

    /// Where the step acts: the planner's `target`, else its words.
    pub fn target(&self) -> Target {
        if let Some(t) = Target::parse(&self.target) {
            return t;
        }
        let t = self.text();
        if t.contains("production") || words(&t).any(|w| w == "prod") {
            Target::Production
        } else {
            Target::Staging
        }
    }

    fn is_deploy(&self) -> bool {
        self.step_kind() == StepKind::Deploy
    }

    fn is_content(&self) -> bool {
        self.step_kind().writes()
    }

    fn is_production(&self) -> bool {
        self.target() == Target::Production
    }

    fn is_e2e_rerun(&self) -> bool {
        self.step_kind() == StepKind::E2e
    }

    fn is_verify(&self) -> bool {
        self.step_kind() == StepKind::Verify
    }

    // Keyword fallbacks, for a step without a `kind`.

    fn words_deploy(&self) -> bool {
        let t = self.text();
        t.contains("deploy") && (t.contains("studio") || t.contains("schema") || t.contains("cms"))
    }

    fn words_content(&self) -> bool {
        let t = self.text();
        let verb = [
            "add", "create", "replace", "update", "set", "fill", "enter", "populate",
        ]
        .iter()
        .any(|v| words(&t).any(|w| w == *v));
        let noun = [
            "content", "block", "document", "entry", "item", "tab", "card", "record", "page",
        ]
        .iter()
        .any(|n| words(&t).any(|w| w.trim_end_matches('s') == *n));
        verb && noun && !self.words_deploy()
    }

    fn words_e2e(&self) -> bool {
        let t = self.text();
        (t.contains("e2e") || t.contains("end-to-end") || t.contains("end to end"))
            && (t.contains("run") || t.contains("rerun"))
    }

    fn words_verify(&self) -> bool {
        let t = self.text();
        t.contains("confirm") || t.contains("verify") || t.contains("query")
    }
}

fn words(t: &str) -> impl Iterator<Item = &str> {
    t.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
}

/// The lookup behind the note: the query, where it ran and what came back
/// (or the error of a failed attempt).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentLookup {
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub result: String,
    #[serde(default)]
    pub error: String,
}

/// An item already there that the work replaces.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExistingItem {
    /// Its document id.
    #[serde(default)]
    pub id: String,
    /// Its array key, for a block inside a document.
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub title: String,
    /// Fields it has that the new schema doesn't know.
    #[serde(default, deserialize_with = "super::fix_location::string_or_list")]
    pub old_fields: Vec<String>,
    /// Fields the new schema requires that it lacks.
    #[serde(default, deserialize_with = "super::fix_location::string_or_list")]
    pub missing_fields: Vec<String>,
    /// The earlier pull request or branch that left it, or where that was
    /// searched for.
    #[serde(default)]
    pub left_by: String,
}

/// How to confirm the work is done.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Confirm {
    /// The exact read query.
    #[serde(default)]
    pub query: String,
    /// The result shape it must return.
    #[serde(default)]
    pub expect: String,
    /// The page to look at.
    #[serde(default)]
    pub page: String,
    /// The end-to-end test that must run and pass, not skip.
    #[serde(default)]
    pub e2e: String,
    /// What the query returned when CoderHelm ran it.
    #[serde(default)]
    pub result: String,
}

/// A labelled identifier (project id, dataset, environment).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Labelled {
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub value: String,
}

/// Phrases a note uses when it didn't look.
const NO_LOOK: &[&str] = &[
    "could not look",
    "couldn't look",
    "cannot look",
    "can't look",
    "unable to look",
    "did not look",
    "didn't look",
    "not looked up",
    "assumes none",
    "assume none",
    "assuming none",
    "assumes no ",
    "assuming no ",
    "not checked",
];

fn says_no_look(text: &str) -> bool {
    let t = text.to_lowercase();
    NO_LOOK.iter().any(|p| t.contains(p))
}

/// Whether a reason for a person is only that the step needs approval
/// (which CoderHelm asks for itself), not a missing connection.
fn says_approval(why: &str) -> bool {
    let t = why.to_lowercase();
    (t.contains("approval") || t.contains("approve") || t.contains("sign-off"))
        && !t.contains("connection")
        && !t.contains("permission")
        && !t.contains("token")
        && !t.contains("access")
}

/// Whether a path names a mutations file CoderHelm can read and run.
pub fn is_mutations_file(path: &str) -> bool {
    let p = path.trim().to_ascii_lowercase();
    p.ends_with(".json") || p.ends_with(".ndjson")
}

/// Whether a lookup's result lists at least one item.
fn result_has_items(result: &str) -> bool {
    let r = result.trim();
    if let Ok(v) = serde_json::from_str::<Value>(r) {
        let v = v.get("result").cloned().unwrap_or(v);
        return match v {
            Value::Array(a) => !a.is_empty(),
            Value::Object(o) => !o.is_empty(),
            Value::Null => false,
            _ => true,
        };
    }
    r.contains("\"_id\"") || r.contains("_id:")
}

/// What the plan says about the work, for [`gaps`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PlanFacts {
    /// The plan changes the CMS schema.
    pub schema_changed: bool,
    /// The plan adds or changes an end-to-end spec.
    pub e2e: bool,
}

/// What a planner's outside-change note lacks, as instructions to the
/// planner (empty when it is complete). See the module docs.
pub fn gaps(c: &OutsideChange, a: &Abilities, plan: PlanFacts) -> Vec<String> {
    let mut out = Vec::new();
    let all_text = format!(
        "{} {} {} {}",
        c.change,
        c.existing,
        c.verify,
        c.steps
            .iter()
            .map(|s| s.step.as_str())
            .collect::<Vec<_>>()
            .join(" ")
    );
    let content = c.adds_content() || c.steps.iter().any(NoteStep::is_content);
    let lookup = c.lookup.as_ref();
    let attempted = lookup.is_some_and(|l| !l.query.trim().is_empty());
    let failed = lookup.is_some_and(|l| !l.error.trim().is_empty());
    // Query before claiming.
    if let Some(s) = &a.source {
        if says_no_look(&all_text) && !failed {
            out.push(format!(
                "The note says it could not look up the content (or assumes none exists), but \
                 the CMS read API is public: {}. Query it with `query_content` ({}), quote \
                 the query and its result in `outside_change.lookup`, and write the note from \
                 what came back. Only a failed attempt may say it couldn't look, quoting the error.",
                s.labels(),
                s.query_url()
            ));
        } else if content && !attempted {
            out.push(format!(
                "The note changes content but quotes no lookup. Query it with `query_content` ({}) for \
                 items of that kind first and put the query, endpoint and result in \
                 `outside_change.lookup` ({}).",
                s.query_url(),
                s.labels()
            ));
        }
    }
    // What is already there.
    if let Some(l) = lookup.filter(|l| result_has_items(&l.result)) {
        if c.existing_items.is_empty() {
            out.push(format!(
                "Your lookup `{}` returned items, so the note must describe each in \
                 `outside_change.existing_items`: id (and key), title, fields the new schema \
                 doesn't know (`old_fields`), new required fields it lacks (`missing_fields`), \
                 and the earlier pull request or branch that left it (`left_by`, from the \
                 ticket's earlier PRs or git history).",
                one_line(&l.query)
            ));
        }
        let adds = c.steps.iter().any(|s| {
            let t = s.step.to_lowercase();
            (words(&t).any(|w| w == "add" || w == "create"))
                && !t.contains("replace")
                && !t.contains("update")
        });
        if adds {
            out.push("Content of that kind is already there: the step replaces that item's content (by its id and key), never adds a second one.".to_string());
        }
    }
    for e in &c.existing_items {
        if e.id.trim().is_empty() || e.title.trim().is_empty() || e.left_by.trim().is_empty() {
            out.push(format!(
                "Describe the existing item `{}` fully: id, title and `left_by` (which earlier PR or branch left it, or where you searched).",
                if e.id.is_empty() { "?" } else { &e.id }
            ));
        }
    }
    // Ordered steps with owners.
    if c.steps.is_empty() {
        out.push("Write the outside work as `outside_change.steps`, in the order it must happen, each with `by` (this_pr, coderhelm or person) and `owner`.".to_string());
    }
    for (i, s) in c.steps.iter().enumerate() {
        let n = i + 1;
        if s.doer() != Doer::ThisPr && s.owner.trim().is_empty() {
            out.push(format!(
                "Step {n} (`{}`) names no owner.",
                one_line(&s.step)
            ));
        }
        if s.doer() == Doer::Person {
            if s.why_person.trim().is_empty() || s.grant.trim().is_empty() {
                out.push(format!(
                    "Step {n} (`{}`) goes to a person without saying why CoderHelm can't do it \
                     (`why_person`: the exact permission or connection it lacks) and how to grant \
                     it (`grant`). Do it in the change or as a CoderHelm step instead when you can.",
                    one_line(&s.step)
                ));
            }
            // Approval is not a reason: CoderHelm asks for it itself.
            if says_approval(&s.why_person) {
                out.push(format!(
                    "Step {n} goes to a person because it needs approval. CoderHelm asks for \
                     approval itself (an agent-permissions card a person approves in one click) \
                     and then does the step: make it `by: coderhelm`. A step goes to a person \
                     only when no connection or permission exists to do it at all."
                ));
            }
            // A person step the change or CoderHelm could do.
            if s.is_deploy() {
                if let Some(r) = a.startable_deploy() {
                    out.push(format!(
                        "Step {n} deploys the studio/schema, and CoderHelm can start {}: make it \
                         `by: coderhelm` with that workflow in `how`.",
                        r.line()
                    ));
                }
            } else if s.is_content() && !s.is_production() {
                if a.staging_write {
                    out.push(format!(
                        "Step {n} changes staging content by hand, and CoderHelm has this \
                         repository's staging CMS write connection: make it `by: coderhelm` \
                         (`kind: migration` or `content`, `target: staging`) with the exact \
                         `mutations`{}; CoderHelm runs it after the staging deploy and then \
                         runs the confirm query.",
                        a.migrations_dir
                            .as_ref()
                            .map(|d| format!(
                                " (write the migration in `{d}` as a task too, and name its file in `migration`)"
                            ))
                            .unwrap_or_default()
                    ));
                } else if let Some(d) = &a.migrations_dir {
                    out.push(format!(
                        "Step {n} changes staging content by hand, but the repository keeps \
                         migrations in `{d}`: write a migration there as a task (it replaces the \
                         existing item by key) and make the step `by: this_pr`."
                    ));
                } else if plan.e2e {
                    out.push(format!(
                        "Step {n} has an editor create content an end-to-end test reads: have \
                         the test's own setup or fixtures create it{} (a task, `by: this_pr`), \
                         so the test never depends on editors; keep a person step only for \
                         production content.",
                        a.fixtures_dir
                            .as_ref()
                            .map(|d| format!(" in `{d}`"))
                            .unwrap_or_default()
                    ));
                }
            } else if s.is_e2e_rerun() {
                if let Some(r) = a.startable_e2e() {
                    out.push(format!(
                        "Step {n} re-runs the end-to-end tests, and CoderHelm can start {}: make it `by: coderhelm`.",
                        r.line()
                    ));
                }
            } else if s.is_verify() && a.source.is_some() {
                out.push(format!(
                    "Step {n} is a read query CoderHelm can run itself: make it `by: coderhelm`."
                ));
            }
        }
        if s.doer() == Doer::CoderHelm
            && s.is_content()
            && s.mutations.is_empty()
            && !is_mutations_file(&s.migration)
        {
            out.push(format!(
                "Step {n} (`{}`) has CoderHelm change content but gives nothing to run: put the \
                 exact CMS mutations in `mutations` (createOrReplace, or patch by id with \
                 set/unset), or name the change's `.json`/`.ndjson` migration file in `migration`.",
                one_line(&s.step)
            ));
        }
        if s.is_content() && s.values.is_empty() && s.mutations.is_empty() {
            out.push(format!(
                "Step {n} (`{}`) changes content without exact values: list each field and value \
                 (reference targets, tab definitions with their settings) in `values` and fields \
                 to remove in `remove`.",
                one_line(&s.step)
            ));
        }
        if s.is_content() && plan.e2e && s.minimum.trim().is_empty() {
            out.push(format!(
                "Step {n} feeds an end-to-end test: say in `minimum` the least content the \
                 feature and its test need to be meaningful (for example how many cards a tab \
                 needs before the carousel can scroll), from the test and component code."
            ));
        }
    }
    // A schema change deploys before content.
    if plan.schema_changed && !a.deploys.is_empty() {
        let first_outside = c.steps.iter().find(|s| s.doer() != Doer::ThisPr);
        let names_route = |s: &NoteStep| {
            a.deploys
                .iter()
                .any(|r| r.named_in(&format!("{} {}", s.step, s.how)))
        };
        if first_outside.is_some_and(|s| !(s.is_deploy() && names_route(s)))
            || (c.steps.iter().any(NoteStep::is_content) && first_outside.is_none())
        {
            out.push(format!(
                "The change edits the CMS schema, and its fields reach the editor only after the \
                 studio/schema is deployed. Make step 1 that deploy, naming {} and its owner \
                 (the engineer merging, or CoderHelm when it can start it); content steps come after.",
                a.deploys[0].line()
            ));
        }
    }
    // How to confirm.
    if content && a.source.is_some() {
        let ok = c
            .confirm
            .as_ref()
            .is_some_and(|f| !f.query.trim().is_empty() && !f.expect.trim().is_empty());
        if !ok {
            out.push("Say how to confirm in `outside_change.confirm`: the exact read query and the result shape it must return (for example exactly one item, count of tabs >= 2, the reference set, old fields not defined).".to_string());
        }
        if plan.e2e && c.confirm.as_ref().is_none_or(|f| f.e2e.trim().is_empty()) {
            out.push("Name in `confirm.e2e` the end-to-end test that must run and pass (not skip) once the content is in.".to_string());
        }
    }
    // Labelled names.
    if let Some(s) = &a.source {
        for n in &c.names {
            let l = n.label.to_lowercase();
            if n.value == s.project_id && !l.contains("project") {
                out.push(format!(
                    "`{}` is the CMS project id, labelled `{}`.",
                    n.value, n.label
                ));
            }
            if n.value == s.dataset && !l.contains("dataset") {
                out.push(format!(
                    "`{}` is the CMS dataset, labelled `{}`.",
                    n.value, n.label
                ));
            }
        }
    }
    out
}

/// The outside-step gaps of a plan: its decision's note checked against
/// what the repository offers, with what the plan's tasks change.
pub fn plan_gaps(plan: &super::plan::PlanResult, a: &Abilities) -> Vec<String> {
    let Some(c) = plan.fix.as_ref().and_then(|d| d.outside_change.as_ref()) else {
        return Vec::new();
    };
    if c.change.trim().is_empty() && c.steps.is_empty() {
        return Vec::new();
    }
    let tasks = super::plan_coverage::plan_tasks(&plan.tasks);
    let facts = PlanFacts {
        schema_changed: tasks.iter().any(|t| {
            let p = t.path.to_lowercase();
            p.contains("schema")
                && (p.contains("studio") || p.contains("sanity") || p.contains("cms"))
        }),
        e2e: tasks
            .iter()
            .any(|t| super::feature::content::is_e2e_spec(&t.path)),
    };
    gaps(c, a, facts)
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn quote_result(s: &str) -> String {
    let s = one_line(s);
    if s.len() <= RESULT_QUOTE {
        return s;
    }
    let mut end = RESULT_QUOTE;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

// ── Rendering: one source for the ticket note and the PR ────────────────────

/// One step in the one state it is in now (`common::pr_steps`). `n` is its
/// number among the steps; `pr_open`: the pull request exists, so nothing
/// waits for it to open. `None` for a step the pull request itself does.
pub fn step_now(c: &OutsideChange, i: usize, pr_open: bool) -> Option<Step> {
    let s = c.steps.get(i)?;
    let text = s.text();
    let when = if text.contains("after merg") || text.contains("once merged") {
        When::AfterMerge
    } else if text.contains("before merg") {
        When::BeforeMerge
    } else {
        When::Any
    };
    let (owner, state) = match s.doer() {
        Doer::ThisPr => return None,
        Doer::Person => (
            match one_line(&s.owner) {
                o if o.is_empty() => "a person on the team".to_string(),
                o => o,
            },
            State::Person {
                why: one_line(&s.why_person),
                unblock: one_line(&s.grant),
            },
        ),
        Doer::CoderHelm => {
            let run_state = s.run.as_ref().map(|r| r.state.as_str()).unwrap_or("");
            if s.done {
                (
                    "CoderHelm".to_string(),
                    State::Done {
                        evidence: pr_steps::strip_machinery(&s.result),
                    },
                )
            } else if run_state == "waiting_approval" {
                (
                    "a team admin".to_string(),
                    State::Approval {
                        who: "a team admin".to_string(),
                        place: "on the Agent permissions page".to_string(),
                    },
                )
            } else if run_state == "running" {
                (
                    "CoderHelm".to_string(),
                    State::Waiting {
                        on: "when the workflow it started ends".to_string(),
                    },
                )
            } else if !s.result.trim().is_empty() {
                let why = s.result.trim().trim_start_matches("not done:").trim();
                // What the executor wrote: the reason, then the access to grant.
                let (cause, unblock) = match why.split_once("; grant:") {
                    Some((cause, grant)) => {
                        (pr_steps::strip_machinery(cause), pr_steps::plain(grant))
                    }
                    None => pr_steps::plain_error(why),
                };
                let owner = if unblock.contains("GitHub") || unblock.contains("Settings") {
                    "a repository admin"
                } else {
                    "a person on the team"
                };
                (owner.to_string(), State::Failed { cause, unblock })
            } else {
                // Not started: what it waits for, as things stand now.
                let blocked_by = c.steps[..i].iter().position(|b| {
                    b.doer() != Doer::ThisPr
                        && !b.done
                        && matches!(
                            b.step_kind(),
                            StepKind::Deploy | StepKind::Content | StepKind::Migration
                        )
                });
                let mut on = match (pr_open, blocked_by) {
                    (false, _) => "when the pull request opens".to_string(),
                    (true, Some(b)) => format!("once step {} is done", b + 1),
                    (true, None) => "on its next step".to_string(),
                };
                // A production step is asked for first: who approves, where.
                if s.target() == Target::Production
                    && matches!(
                        s.step_kind(),
                        StepKind::Deploy | StepKind::Content | StepKind::Migration
                    )
                {
                    on.push_str(" and a team admin has approved it on the Agent permissions page");
                }
                ("CoderHelm".to_string(), State::Waiting { on })
            }
        }
    };
    // What the step needs to be done right is part of what it is.
    let mut what = one_line(&s.step);
    let mut extra: Vec<String> = Vec::new();
    if !s.values.is_empty() {
        extra.push(format!(
            "values: {}",
            s.values
                .iter()
                .map(|v| one_line(v))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    if !s.remove.is_empty() {
        extra.push(format!(
            "remove: {}",
            s.remove
                .iter()
                .map(|v| format!("`{}`", one_line(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !s.minimum.trim().is_empty() {
        extra.push(format!(
            "at least: {}",
            one_line(&s.minimum).trim_end_matches('.')
        ));
    }
    if !extra.is_empty() {
        what = format!("{} ({})", what.trim_end_matches('.'), extra.join("; "));
    }
    // "After merging: ... after merge" says it twice.
    if when == When::AfterMerge {
        for tail in [
            " after merge",
            " after merging",
            " once merged",
            " after the merge",
        ] {
            if what.to_lowercase().ends_with(tail) {
                what.truncate(what.len() - tail.len());
            }
        }
    }
    Some(Step {
        what,
        how: one_line(&s.how),
        when,
        owner,
        state,
    })
}

/// Every step outside the pull request, each in its one state. A check
/// CoderHelm made (a verify step it ran) is a result, not a step: see
/// [`checked_now`].
pub fn steps_now(c: &OutsideChange, pr_open: bool) -> Vec<Step> {
    // The confirm query CoderHelm already ran answers a step that only
    // asks for it to be run.
    let confirmed = c
        .confirm
        .as_ref()
        .is_some_and(|f| !f.result.trim().is_empty() && !f.result.starts_with("the query failed"));
    (0..c.steps.len())
        .filter(|i| {
            let s = &c.steps[*i];
            !(s.step_kind() == StepKind::Verify
                && s.doer() == Doer::CoderHelm
                && (s.done || confirmed))
        })
        .filter_map(|i| step_now(c, i, pr_open))
        .collect()
}

/// The checks CoderHelm made itself, each with its result: a verify step
/// it ran, and the confirm query.
pub fn checked_now(c: &OutsideChange) -> Vec<Checked> {
    let mut out: Vec<Checked> = c
        .steps
        .iter()
        .filter(|s| s.done && s.doer() == Doer::CoderHelm && s.step_kind() == StepKind::Verify)
        .map(|s| Checked {
            what: one_line(&s.step),
            result: "the query returned what was expected".to_string(),
            when: String::new(),
        })
        .collect();
    if let Some(f) = c.confirm.as_ref().filter(|f| !f.result.trim().is_empty()) {
        let failed = f.result.starts_with("the query failed");
        out.push(Checked {
            what: if f.expect.trim().is_empty() {
                "the content the change needs".to_string()
            } else {
                format!("the content query returns {}", one_line(&f.expect))
            },
            result: if failed {
                let (cause, _) = pr_steps::plain_error(&f.result);
                format!("could not be read ({cause})")
            } else {
                "it does".to_string()
            },
            when: String::new(),
        });
    }
    out
}

/// What is left to confirm that CoderHelm cannot: a page only a person can
/// judge. A query it can run and a test CI runs are never given to a person.
pub fn person_checks(c: &OutsideChange) -> Vec<Step> {
    let Some(f) = c.confirm.as_ref() else {
        return Vec::new();
    };
    if f.page.trim().is_empty() || says_no_look(&f.page) {
        return Vec::new();
    }
    vec![Step {
        what: format!("Look at {}", one_line(&f.page).trim_end_matches('.')),
        how: String::new(),
        when: When::Any,
        owner: "the reviewer".to_string(),
        state: State::Person {
            why: "how a page looks is a judgement only a person can make".to_string(),
            unblock: String::new(),
        },
    }]
}

/// The raw evidence behind the note: the names, the lookup with its result,
/// what was already there, the confirm query. For the collapsed part only.
pub fn raw_evidence(c: &OutsideChange) -> Vec<String> {
    let mut out = context_lines(c);
    if let Some(f) = c.confirm.as_ref().filter(|f| !f.query.trim().is_empty()) {
        let mut t = format!("Confirm query `{}`", one_line(&f.query));
        if !f.result.trim().is_empty() {
            t.push_str(&format!(" returned {}", quote_result(&f.result)));
        }
        out.push(t);
    }
    for s in c.steps.iter().filter(|s| !s.result.trim().is_empty()) {
        out.push(format!(
            "\"{}\": {}",
            one_line(&s.step),
            quote_result(&s.result)
        ));
    }
    out
}

/// One step as a line with its owner, for a progress note and the plan
/// check: generated from its state (see [`step_now`]).
pub fn step_line(n: usize, s: &NoteStep) -> (String, String) {
    let only = OutsideChange {
        steps: vec![s.clone()],
        ..Default::default()
    };
    match step_now(&only, 0, true) {
        Some(step) => (format!("{n}. {}", step.line()), step.owner),
        None => (
            format!("{n}. {}", one_line(&s.step)),
            "this pull request".to_string(),
        ),
    }
}

/// The lines before the steps: the names, the lookup and what is there.
pub fn context_lines(c: &OutsideChange) -> Vec<String> {
    let mut out = Vec::new();
    if !c.names.is_empty() {
        out.push(format!(
            "Names: {}",
            c.names
                .iter()
                .map(|n| format!("{} `{}`", one_line(&n.label), one_line(&n.value)))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(l) = &c.lookup {
        if !l.error.trim().is_empty() {
            out.push(format!(
                "Looked up `{}` at {}: the lookup failed ({})",
                one_line(&l.query),
                one_line(&l.endpoint),
                one_line(&l.error)
            ));
        } else if !l.query.trim().is_empty() {
            out.push(format!(
                "Looked up `{}` at {}: {}",
                one_line(&l.query),
                one_line(&l.endpoint),
                quote_result(&l.result)
            ));
        }
    }
    for e in &c.existing_items {
        let mut t = format!("Already there: `{}`", one_line(&e.id));
        if !e.key.trim().is_empty() {
            t.push_str(&format!(" (key `{}`)", one_line(&e.key)));
        }
        if !e.title.trim().is_empty() {
            t.push_str(&format!(", \"{}\"", one_line(&e.title)));
        }
        if !e.old_fields.is_empty() {
            t.push_str(&format!(
                "; fields the new schema doesn't know: {}",
                e.old_fields
                    .iter()
                    .map(|f| format!("`{f}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !e.missing_fields.is_empty() {
            t.push_str(&format!(
                "; new required fields it lacks: {}",
                e.missing_fields
                    .iter()
                    .map(|f| format!("`{f}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !e.left_by.trim().is_empty() {
            t.push_str(&format!("; left by {}", one_line(&e.left_by)));
        }
        t.push_str(". Its content is replaced, not added to.");
        out.push(t);
    }
    out
}

/// The work outside this pull request that is still to come and needs
/// nobody now (a step CoderHelm does at a named point), each with its one
/// owner: what the PR's "Not in this PR" lists. What a person has to do is
/// in the lead; what is done, checked or found is evidence.
pub fn pr_lines(c: &OutsideChange) -> Vec<(String, String)> {
    steps_now(c, true)
        .into_iter()
        .filter(|s| matches!(s.state, State::Waiting { .. }))
        .map(|s| (s.line(), s.owner))
        .collect()
}

/// The ticket note's body: what a person has to do first (or that nothing
/// is needed), then what CoderHelm did, checked and will do. The raw
/// evidence stays on the pull request and the run page.
pub fn note_markdown(c: &OutsideChange) -> String {
    let mut steps = steps_now(c, c.placed.is_some());
    steps.extend(person_checks(c));
    let needed = pr_steps::needed(&steps);
    let mut out = String::new();
    if needed.is_empty() {
        out.push_str(&format!(
            "**Needed from you:** {}\n",
            pr_steps::NOTHING_NEEDED
        ));
    } else {
        out.push_str("**Needed from you:**\n");
        for (i, l) in needed.iter().enumerate() {
            out.push_str(&format!("{}. {l}\n", i + 1));
        }
    }
    let rest = pr_steps::others(&steps);
    let checked = checked_now(c);
    if !rest.is_empty() || !checked.is_empty() {
        out.push_str("\n**What CoderHelm does and checked**\n");
        for l in rest {
            out.push_str(&format!("- {l}\n"));
        }
        for k in checked {
            out.push_str(&format!("- {}\n", k.line()));
        }
    }
    out
}

// ── Requirements a person holds, for the plan check ─────────────────────────

/// What the plan check counts for the outside steps.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Held {
    /// Steps a person does, with the recorded reason: not done by the run.
    pub by_person: Vec<String>,
    /// Steps given to a person with no reason: not covered.
    pub unjustified: Vec<String>,
    /// CoderHelm steps that did not work: not done.
    pub not_done: Vec<String>,
}

/// The outside steps the run did not do, sorted by what the plan check
/// makes of them.
pub fn held(c: &OutsideChange) -> Held {
    let mut h = Held::default();
    for (i, s) in c.steps.iter().enumerate() {
        let (line, owner) = step_line(i + 1, s);
        match s.doer() {
            Doer::ThisPr => {}
            Doer::CoderHelm => {
                if !s.done && !s.result.trim().is_empty() && !under_way(s) {
                    h.not_done.push(line);
                }
            }
            Doer::Person => {
                if s.why_person.trim().is_empty() || s.grant.trim().is_empty() {
                    h.unjustified.push(format!("{line} (owner: {owner})"));
                } else {
                    h.by_person.push(format!("{line} (owner: {owner})"));
                }
            }
        }
    }
    h
}

/// The questions a person is asked for each step handed out with no
/// reason, once the plan was sent back and still gives none: the run stops
/// on them instead of opening a pull request that silently leaves the step.
pub fn unjustified_questions(c: &OutsideChange) -> Vec<common::run_questions::Question> {
    c.steps
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            s.doer() == Doer::Person
                && (s.why_person.trim().is_empty() || s.grant.trim().is_empty())
        })
        .enumerate()
        .map(|(q, (i, s))| common::run_questions::Question {
            id: format!("q{}", q + 1),
            question: format!(
                "Step {} of the work outside the code (\"{}\") is planned for a person, but \
                 nothing says why CoderHelm can't do it. Which connection or permission does \
                 CoderHelm lack for it, or may CoderHelm do it itself?",
                i + 1,
                one_line(&s.step)
            ),
            why: "A requirement is never handed out silently: CoderHelm either does it, or \
                  reports it as not done with the exact access to grant."
                .to_string(),
            suggestions: vec![
                "CoderHelm may do it (it asks for approval on the Agent permissions page)"
                    .to_string(),
                "A person must do it, because CoderHelm has no connection for it (name it)"
                    .to_string(),
            ],
            decision: String::new(),
            evidence: Vec::new(),
            ..Default::default()
        })
        .collect()
}

/// The requirements the run leaves not done, each with why and the access
/// to grant: steps a person holds and CoderHelm steps that didn't work.
pub fn not_done_lines(c: &OutsideChange) -> Vec<String> {
    let h = held(c);
    h.by_person.into_iter().chain(h.not_done).collect()
}

const NOT_DONE_START: &str = "<!-- coderhelm:not-done:start -->";
const NOT_DONE_END: &str = "<!-- coderhelm:not-done:end -->";

/// What a person has to do for the steps not done, numbered, each with
/// why CoderHelm did not and what lets it. Empty when nothing is needed.
#[cfg(test)]
pub fn not_done_block(c: &OutsideChange) -> String {
    let needed = pr_steps::needed(&steps_now(c, c.placed.is_some()));
    if needed.is_empty() {
        return String::new();
    }
    let mut out = String::from("**Needed from you:**\n");
    for (i, l) in needed.iter().enumerate() {
        out.push_str(&format!("{}. {l}\n", i + 1));
    }
    out
}

/// The PR body without the not-done block an earlier shape of the
/// description put at its top: what a person has to do is in the lead now
/// (`description`), rendered from the same steps.
pub fn with_not_done_top(body: &str, _c: &OutsideChange) -> String {
    match (body.find(NOT_DONE_START), body.find(NOT_DONE_END)) {
        (Some(a), Some(b)) if b > a => format!("{}{}", &body[..a], &body[b + NOT_DONE_END.len()..])
            .trim_start()
            .to_string(),
        _ => body.to_string(),
    }
}

/// Whether a CoderHelm step is still under way (an approval card or a
/// workflow run it waits on), so the run is not finished.
pub fn under_way(s: &NoteStep) -> bool {
    s.doer() == Doer::CoderHelm
        && !s.done
        && s.run
            .as_ref()
            .is_some_and(|r| r.state == "waiting_approval" || r.state == "running")
}

/// Whether the note has CoderHelm steps not finished yet: never started, or
/// under way.
pub fn unfinished(c: &OutsideChange) -> bool {
    c.steps.iter().any(|s| {
        s.doer() == Doer::CoderHelm && !s.done && (s.result.trim().is_empty() || under_way(s))
    })
}

// ── Doing the CoderHelm steps ───────────────────────────────────────────────

/// Runs a read query on the public endpoint (no token). Returns the
/// `result`, or the error a failed attempt gave.
pub async fn public_query(
    http: &reqwest::Client,
    s: &ContentSource,
    query: &str,
) -> Result<Value, String> {
    let r = http
        .get(s.query_url())
        .query(&[("query", query)])
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;
    let status = r.status();
    if !status.is_success() {
        return Err(format!("the read API returned {status}"));
    }
    let v: Value = r
        .json()
        .await
        .map_err(|e| format!("unreadable result: {e}"))?;
    Ok(v["result"].clone())
}

/// The read-only content query tool the planner gets when the repository
/// names its CMS project and dataset.
pub const TOOL: &str = "query_content";

pub fn tool(s: &ContentSource) -> crate::agent::llm::ToolDefinition {
    crate::agent::llm::ToolDefinition {
        name: TOOL.to_string(),
        description: format!(
            "Run a read-only GROQ query on the repository's CMS ({}) through its public read \
             API, without a token, and get the result. Use it before writing anything about \
             existing content (what items of a type exist, their ids, keys, titles and fields), \
             and quote the query and result in the note.",
            s.labels()
        ),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "A GROQ query, for example *[_type == \"page\" && count(sections[_type == \"hero\"]) > 0][0...5]{_id, title, \"items\": sections[_type == \"hero\"]}"}
            },
            "required": ["query"]
        }),
    }
}

/// Answers a [`TOOL`] call: the query, the endpoint and the result (or the
/// error, quoted). `None` for another tool.
pub async fn handle(
    http: &reqwest::Client,
    a: &Abilities,
    name: &str,
    input: &Value,
) -> Option<String> {
    if name != TOOL {
        return None;
    }
    let Some(base) = a.source.as_ref() else {
        return Some("No CMS project id and dataset are named in this repository.".into());
    };
    // Another environment's dataset (production, read only), when named.
    let picked;
    let s = match input["dataset"].as_str().map(str::trim) {
        Some(d) if !d.is_empty() && d != base.dataset => {
            if !a.datasets.iter().any(|x| x == d) {
                return Some(format!(
                    "Unknown dataset `{d}`. Datasets: {}.",
                    a.datasets.join(", ")
                ));
            }
            picked = ContentSource {
                dataset: d.to_string(),
                ..base.clone()
            };
            &picked
        }
        _ => base,
    };
    let q = input["query"].as_str().unwrap_or("").trim();
    if q.is_empty() || q.len() > 2_000 {
        return Some("Give a GROQ query (up to 2000 characters).".into());
    }
    Some(match public_query(http, s, q).await {
        Ok(v) => {
            let mut out = v.to_string();
            if out.len() > 8_000 {
                let mut end = 8_000;
                while !out.is_char_boundary(end) {
                    end -= 1;
                }
                out.truncate(end);
                out.push_str("… (cut)");
            }
            format!("endpoint: {}\nquery: {q}\nresult: {out}", s.query_url())
        }
        Err(e) => format!("endpoint: {}\nquery: {q}\nerror: {e}", s.query_url()),
    })
}

/// What one CoderHelm step will do, from its kind, target and the
/// repository. The executor (`outside_exec`) runs it through the team's
/// agent permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Start this workflow on the PR branch.
    Dispatch(Route),
    /// Add this label to the PR, which starts the route.
    Label(String, Route),
    /// Run the confirm query.
    Query,
    /// Write content (mutations or a migration's file) to the target's CMS
    /// through a write connection.
    Write(Target),
    /// CoderHelm can't do it: why, and how to grant it.
    Cannot(String),
}

/// Whether a route deploys staging.
fn is_staging_route(r: &Route) -> bool {
    format!("{} {} {}", r.name, r.file, r.command)
        .to_lowercase()
        .contains("staging")
}

/// The action for a CoderHelm step.
pub fn action_for(s: &NoteStep, a: &Abilities, c: &OutsideChange) -> Action {
    let target = s.target();
    let pick = |routes: &[Route], fit: &dyn Fn(&Route) -> bool| -> Option<Action> {
        let ok = |r: &&Route| r.startable() && fit(r);
        let r = routes
            .iter()
            .filter(ok)
            .find(|r| r.named_in(&format!("{} {}", s.step, s.how)))
            .or_else(|| routes.iter().find(ok))?
            .clone();
        Some(match &r.trigger {
            Trigger::Label(l) => Action::Label(l.clone(), r.clone()),
            _ => Action::Dispatch(r),
        })
    };
    match s.step_kind() {
        StepKind::Deploy => {
            let fit = |r: &Route| match target {
                Target::Production => !is_staging_route(r),
                Target::Staging => true,
            };
            pick(&a.deploys, &fit).unwrap_or_else(|| {
                // Said plainly, with the commands a person runs instead.
                let scripts: Vec<String> = a
                    .deploys
                    .iter()
                    .filter(|r| r.trigger == Trigger::Script && fit(r))
                    .map(|r| format!("`{}`", r.command))
                    .collect();
                Action::Cannot(format!(
                    "the repository has no {}deploy workflow CoderHelm can start{}; grant: add a \
                     deploy workflow with a manual start (`workflow_dispatch:` in its triggers)",
                    if target == Target::Production { "production " } else { "" },
                    if scripts.is_empty() {
                        String::new()
                    } else {
                        format!(" (it deploys with {})", scripts.join(" or "))
                    }
                ))
            })
        }
        StepKind::E2e => pick(&a.e2e, &|_| true).unwrap_or_else(|| {
            Action::Cannot("the end-to-end workflow has no workflow_dispatch trigger; add `workflow_dispatch:` to it so CoderHelm can start it".to_string())
        }),
        StepKind::Content | StepKind::Migration => Action::Write(target),
        StepKind::Verify => {
            if a.source.is_some() {
                Action::Query
            } else {
                Action::Cannot("no CMS project id and dataset are named in the repository's config or codegen environment; add them to the codegen environment (Settings, repository, codegen environment)".to_string())
            }
        }
        StepKind::Other => {
            if a.source.is_some() && c.confirm.as_ref().is_some_and(|f| !f.query.is_empty()) {
                Action::Query
            } else {
                Action::Cannot("CoderHelm has no action for this step".to_string())
            }
        }
    }
}

/// The mutations a step runs: its own, else the ones in its migration file
/// (`text`, a JSON array, an object with `mutations`, or NDJSON).
pub fn mutations_of(s: &NoteStep, text: Option<&str>) -> Result<Vec<Value>, String> {
    if !s.mutations.is_empty() {
        return Ok(s.mutations.clone());
    }
    let Some(text) = text else {
        return Err("the step gives no mutations and no migration file".to_string());
    };
    let t = text.trim();
    if let Ok(v) = serde_json::from_str::<Value>(t) {
        let list = v.get("mutations").cloned().unwrap_or(v);
        return match list {
            Value::Array(a) if !a.is_empty() => Ok(a),
            _ => Err(format!("`{}` holds no mutations", s.migration)),
        };
    }
    let mut out = Vec::new();
    for (i, line) in t.lines().filter(|l| !l.trim().is_empty()).enumerate() {
        out.push(
            serde_json::from_str::<Value>(line)
                .map_err(|e| format!("`{}` line {}: {e}", s.migration, i + 1))?,
        );
    }
    if out.is_empty() {
        return Err(format!("`{}` holds no mutations", s.migration));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(p: &str, t: &str) -> (String, String) {
        (p.to_string(), t.to_string())
    }

    fn repo() -> (Vec<String>, Vec<(String, String)>) {
        let files = vec![
            f(".env.staging", "NEXT_PUBLIC_SANITY_PROJECT_ID=abc123\nNEXT_PUBLIC_SANITY_DATASET=staging\n"),
            f(".env.production", "NEXT_PUBLIC_SANITY_DATASET=production\n"),
            f(
                ".github/workflows/studio-staging.yml",
                "name: Deploy studio (staging)\non:\n  workflow_dispatch:\n  push:\n    branches: [main]\njobs:\n  d:\n    steps:\n      - run: npx sanity deploy\n",
            ),
            f(
                ".github/workflows/e2e.yml",
                "name: E2E\non:\n  pull_request:\n    types: [labeled]\njobs:\n  e:\n    if: contains(github.event.pull_request.labels.*.name, 'run-e2e')\n    steps:\n      - run: npx playwright test\n",
            ),
            f("studio/package.json", r#"{"scripts":{"deploy":"sanity deploy"}}"#),
        ];
        let tree = vec![
            "studio/migrations/001-hero/index.ts".to_string(),
            "e2e/fixtures/page.json".to_string(),
            "src/app/page.tsx".to_string(),
        ];
        (tree, files)
    }

    #[test]
    fn reads_the_cms_deploys_and_folders_from_the_repository() {
        let (tree, files) = repo();
        let a = abilities(&tree, &files, &[], false);
        let s = a.source.clone().unwrap();
        assert_eq!(s.project_id, "abc123");
        assert_eq!(s.dataset, "staging");
        assert_eq!(
            s.query_url(),
            "https://abc123.api.sanity.io/v2021-10-21/data/query/staging"
        );
        assert!(s
            .labels()
            .contains("CMS project id `abc123`, dataset `staging`"));
        assert_eq!(a.deploys[0].file, ".github/workflows/studio-staging.yml");
        assert_eq!(a.deploys[0].trigger, Trigger::Dispatch);
        assert_eq!(a.deploys[0].command, "npx sanity deploy");
        assert!(a
            .deploys
            .iter()
            .any(|r| r.command == "cd studio && npm run deploy"));
        assert_eq!(a.e2e[0].trigger, Trigger::Label("run-e2e".into()));
        assert_eq!(a.migrations_dir.as_deref(), Some("studio/migrations"));
        assert_eq!(a.fixtures_dir.as_deref(), Some("e2e/fixtures"));
        // The codegen environment and a config that reads it.
        let s = content_source(
            &[f("studio/sanity.config.ts", "export default defineConfig({\n  projectId: process.env.SANITY_STUDIO_PROJECT_ID!,\n  dataset: 'staging',\n})")],
            &[("SANITY_STUDIO_PROJECT_ID".into(), "zz9".into())],
        )
        .unwrap();
        assert_eq!(
            (s.project_id.as_str(), s.dataset.as_str()),
            ("zz9", "staging")
        );
        assert!(content_source(&[f("README.md", "projectId: x")], &[]).is_none());
        assert!(a.prompt_section().contains("query_content"));
    }

    fn step(by: &str, step: &str) -> NoteStep {
        NoteStep {
            step: step.into(),
            by: by.into(),
            owner: "a content editor".into(),
            ..Default::default()
        }
    }

    /// The reviewed case: a note that said it couldn't look up the content
    /// while the read API was public, added a block that was already there,
    /// put content before the studio deploy and handed everything to people.
    #[test]
    fn the_reviewed_note_goes_back_with_every_rule() {
        let (tree, files) = repo();
        let a = abilities(&tree, &[files[0].clone(), files[2].clone()], &[], false);
        let c = OutsideChange {
            system: "the CMS".into(),
            change: "Add a carousel block to the events page".into(),
            existing: "I could not look up existing content; this assumes none exists.".into(),
            steps: vec![
                step("person", "Add a carousel block to the events page"),
                NoteStep {
                    owner: "the engineer merging".into(),
                    ..step("person", "Deploy the studio")
                },
            ],
            names: vec![Labelled {
                label: "dataset".into(),
                value: "abc123".into(),
            }],
            ..Default::default()
        };
        let g = gaps(
            &c,
            &a,
            PlanFacts {
                schema_changed: true,
                e2e: true,
            },
        )
        .join("\n");
        assert!(g.contains("the CMS read API is public"), "{g}");
        assert!(g.contains("https://abc123.api.sanity.io/v2021-10-21/data/query/staging"));
        assert!(g.contains("without saying why CoderHelm can't do it"));
        assert!(
            g.contains("CoderHelm can start `Deploy studio (staging)`"),
            "{g}"
        );
        assert!(g.contains("migrations in `studio/migrations`"), "{g}");
        assert!(g.contains("Make step 1 that deploy"), "{g}");
        assert!(g.contains("without exact values"));
        assert!(g.contains("`minimum`"));
        assert!(g.contains("outside_change.confirm"));
        assert!(g.contains("`abc123` is the CMS project id, labelled `dataset`"));
    }

    #[test]
    fn a_lookup_that_found_items_needs_them_described_and_replaced() {
        let a = Abilities {
            source: Some(ContentSource {
                project_id: "p1".into(),
                dataset: "staging".into(),
                origin: ".env".into(),
            }),
            ..Default::default()
        };
        let mut c = OutsideChange {
            system: "the CMS".into(),
            change: "Add a carousel".into(),
            lookup: Some(ContentLookup {
                endpoint: "x".into(),
                query: "*[_type == \"carousel\"]{_id}".into(),
                result: r#"[{"_id":"page-events"}]"#.into(),
                error: String::new(),
            }),
            steps: vec![NoteStep {
                values: vec!["title = Classes".into()],
                why_person: "only an editor picks the classes shown".into(),
                grant: "none: a content decision".into(),
                ..step("person", "Add a carousel block on production")
            }],
            confirm: Some(Confirm {
                query: "count(*[_type == \"carousel\"])".into(),
                expect: "exactly 1".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let g = gaps(&c, &a, PlanFacts::default()).join("\n");
        assert!(g.contains("existing_items"), "{g}");
        assert!(g.contains("never adds a second"), "{g}");
        c.existing_items = vec![ExistingItem {
            id: "page-events".into(),
            key: "k1".into(),
            title: "Events".into(),
            old_fields: vec!["image".into()],
            missing_fields: vec!["tabs".into()],
            left_by: "#41 (branch feature/carousel)".into(),
        }];
        c.steps[0].step = "Replace the carousel block's content on production".into();
        let g = gaps(&c, &a, PlanFacts::default());
        assert!(g.is_empty(), "{g:?}");
        let note = note_markdown(&c);
        // The person's step comes first, with what it needs and why.
        assert!(
            note.starts_with("**Needed from you:**\n1. Replace the carousel block's content on production (values: title = Classes). CoderHelm cannot: only an editor picks the classes shown. (a content editor)\n"),
            "{note}"
        );
        // What was found and how it is confirmed are evidence, not items.
        assert!(
            !note.contains("Already there") && !note.contains("How to confirm"),
            "{note}"
        );
        let raw = raw_evidence(&c).join("\n");
        assert!(raw.contains("Already there: `page-events` (key `k1`), \"Events\"; fields the new schema doesn't know: `image`; new required fields it lacks: `tabs`; left by #41"), "{raw}");
        assert!(
            raw.contains("Confirm query `count(*[_type == \"carousel\"])`"),
            "{raw}"
        );
        // Nothing waits on CoderHelm here, so the PR lists no step of its own.
        assert!(pr_lines(&c).is_empty());
        // A failed attempt may say so, quoting the error.
        let failed = OutsideChange {
            existing: "could not look up: the lookup failed".into(),
            lookup: Some(ContentLookup {
                query: "*[_type == \"carousel\"]".into(),
                error: "the read API returned 401 Unauthorized".into(),
                ..Default::default()
            }),
            ..c.clone()
        };
        assert!(!gaps(&failed, &a, PlanFacts::default())
            .iter()
            .any(|g| g.contains("read API is public")));
        assert!(context_lines(&failed)[0]
            .contains("the lookup failed (the read API returned 401 Unauthorized)"));
    }

    #[test]
    fn the_plan_check_counts_a_person_step_without_a_reason_as_not_covered() {
        let c = OutsideChange {
            steps: vec![
                step("this_pr", "Write the migration"),
                step("person", "Publish on production"),
                NoteStep {
                    why_person: "production content needs an editor's approval".into(),
                    grant: "none: CoderHelm never writes production".into(),
                    ..step("person", "Approve the production content")
                },
                NoteStep {
                    result: "not done: the end-to-end workflow has no workflow_dispatch trigger"
                        .into(),
                    ..step("coderhelm", "Re-run the e2e tests")
                },
            ],
            ..Default::default()
        };
        let h = held(&c);
        assert_eq!(h.unjustified.len(), 1);
        assert!(h.unjustified[0].starts_with("2. Publish on production"));
        assert_eq!(h.by_person.len(), 1);
        assert_eq!(h.not_done.len(), 1);
        assert!(
            h.not_done[0].contains("CoderHelm could not: the end-to-end workflow")
                && !h.not_done[0].contains("not done"),
            "{}",
            h.not_done[0]
        );
    }

    #[test]
    fn each_coderhelm_step_gets_its_action() {
        let (tree, files) = repo();
        let a = abilities(&tree, &files, &[], false);
        let c = OutsideChange::default();
        assert!(matches!(
            action_for(&step("coderhelm", "Deploy the studio schema to staging"), &a, &c),
            Action::Dispatch(r) if r.file.ends_with("studio-staging.yml")
        ));
        assert!(matches!(
            action_for(&step("coderhelm", "Re-run the e2e tests on staging"), &a, &c),
            Action::Label(l, _) if l == "run-e2e"
        ));
        assert_eq!(
            action_for(&step("coderhelm", "Confirm with the query"), &a, &c),
            Action::Query
        );
        assert!(matches!(
            action_for(
                &step("coderhelm", "Replace the block content on production"),
                &a,
                &c
            ),
            Action::Write(Target::Production)
        ));
        let none = Abilities::default();
        assert!(matches!(
            action_for(&step("coderhelm", "Deploy the studio"), &none, &c),
            Action::Cannot(w) if w.contains("workflow_dispatch")
        ));
    }

    #[test]
    fn the_step_kind_and_target_decide_the_action_not_the_wording() {
        let (tree, files) = repo();
        let a = abilities(&tree, &files, &[], true);
        let c = OutsideChange::default();
        // Words say "content", the structured kind says deploy: deploy wins.
        let mut s = step("coderhelm", "Publish the new content fields");
        s.kind = "deploy".into();
        assert_eq!(s.step_kind(), StepKind::Deploy);
        assert!(
            matches!(action_for(&s, &a, &c), Action::Dispatch(r) if r.file.ends_with("studio-staging.yml"))
        );
        // Words say deploy, the kind says verify.
        let mut v = step(
            "coderhelm",
            "Deploy check: confirm the studio has the field",
        );
        v.kind = "verify".into();
        assert_eq!(action_for(&v, &a, &c), Action::Query);
        // The target is structured too: no word "production" needed.
        let mut w = step("coderhelm", "Replace the hero block");
        w.kind = "migration".into();
        w.target = "production".into();
        assert_eq!(action_for(&w, &a, &c), Action::Write(Target::Production));
        w.target = "staging".into();
        assert_eq!(action_for(&w, &a, &c), Action::Write(Target::Staging));
        // A production deploy never starts the staging workflow.
        let mut d = step("coderhelm", "Deploy");
        d.kind = "deploy".into();
        d.target = "production".into();
        assert!(
            matches!(action_for(&d, &a, &c), Action::Cannot(w) if w.contains("production deploy"))
        );
        // Without a kind, the words still decide (the fallback).
        assert_eq!(
            step("coderhelm", "Deploy the studio schema").step_kind(),
            StepKind::Deploy
        );
        assert_eq!(
            step("coderhelm", "Re-run the e2e tests").step_kind(),
            StepKind::E2e
        );
        assert_eq!(
            step("coderhelm", "Replace the block content").step_kind(),
            StepKind::Content
        );
        assert_eq!(
            step("coderhelm", "Replace the block on prod").target(),
            Target::Production
        );
    }

    #[test]
    fn approval_is_never_a_reason_to_give_a_step_to_a_person() {
        let (tree, files) = repo();
        let a = abilities(&tree, &files, &[], true);
        let mut s = step("person", "Deploy the app to production");
        s.kind = "deploy".into();
        s.target = "production".into();
        s.why_person = "production deploys need a person's approval".into();
        s.grant = "none".into();
        let c = OutsideChange {
            system: "the CMS".into(),
            change: "Release".into(),
            steps: vec![s.clone()],
            ..Default::default()
        };
        let g = gaps(&c, &a, PlanFacts::default());
        assert!(
            g.iter()
                .any(|x| x.contains("CoderHelm asks for approval itself")),
            "{g:?}"
        );
        // A missing connection is a reason.
        let mut ok = s;
        ok.why_person = "no production deploy permission for the CoderHelm app".into();
        let c = OutsideChange {
            steps: vec![ok],
            ..c
        };
        let g = gaps(&c, &a, PlanFacts::default());
        assert!(!g.iter().any(|x| x.contains("asks for approval")), "{g:?}");
    }

    #[test]
    fn staging_content_with_a_write_connection_is_coderhelms_with_mutations() {
        let (tree, files) = repo();
        let a = abilities(&tree, &files, &[], true);
        let mut by_hand = step("person", "Replace the hero block content on staging");
        by_hand.kind = "content".into();
        by_hand.target = "staging".into();
        by_hand.values = vec!["title = New".into()];
        by_hand.why_person = "an editor".into();
        by_hand.grant = "none".into();
        let c = OutsideChange {
            system: "the CMS".into(),
            change: "Replace the hero".into(),
            steps: vec![by_hand],
            ..Default::default()
        };
        let g = gaps(&c, &a, PlanFacts::default());
        assert!(
            g.iter()
                .any(|x| x.contains("staging CMS write connection") && x.contains("`mutations`")),
            "{g:?}"
        );
        // A CoderHelm content step with nothing to run goes back.
        let mut ours = step("coderhelm", "Run the hero migration on staging");
        ours.kind = "migration".into();
        ours.values = vec!["title = New".into()];
        let c2 = OutsideChange {
            steps: vec![ours.clone()],
            ..c.clone()
        };
        assert!(gaps(&c2, &a, PlanFacts::default())
            .iter()
            .any(|x| x.contains("gives nothing to run")));
        ours.migration = "studio/migrations/002-hero.ndjson".into();
        let c3 = OutsideChange {
            steps: vec![ours],
            ..c
        };
        assert!(!gaps(&c3, &a, PlanFacts::default())
            .iter()
            .any(|x| x.contains("gives nothing to run")));
    }

    #[test]
    fn a_migration_runs_its_own_mutations_or_its_file() {
        let mut s = step("coderhelm", "Run it");
        s.migration = "m.ndjson".into();
        let nd = "{\"patch\":{\"id\":\"a\",\"set\":{\"x\":1}}}\n\n{\"delete\":{\"id\":\"b\"}}\n";
        assert_eq!(mutations_of(&s, Some(nd)).unwrap().len(), 2);
        let arr = r#"{"mutations":[{"createOrReplace":{"_id":"c","_type":"page"}}]}"#;
        assert_eq!(mutations_of(&s, Some(arr)).unwrap().len(), 1);
        assert!(mutations_of(&s, Some("not json")).is_err());
        assert!(mutations_of(&s, None).is_err());
        s.mutations = vec![serde_json::json!({"delete": {"id": "z"}})];
        assert_eq!(mutations_of(&s, None).unwrap().len(), 1);
    }

    #[test]
    fn a_person_step_without_a_reason_becomes_a_question() {
        let c = OutsideChange {
            system: "the CMS".into(),
            change: "Publish".into(),
            steps: vec![
                step("person", "Publish the page"),
                NoteStep {
                    why_person: "no production CMS write connection".into(),
                    grant: "add one".into(),
                    ..step("person", "Approve the copy")
                },
            ],
            ..Default::default()
        };
        let q = unjustified_questions(&c);
        assert_eq!(q.len(), 1);
        assert!(q[0].question.contains("Step 1") && q[0].question.contains("Publish the page"));
        assert!(q[0].question.contains("connection or permission"));
    }

    #[test]
    fn what_is_not_done_leads_the_pr_and_the_ticket_with_the_access_to_grant() {
        let held_step = NoteStep {
            why_person: "no production CMS write connection exists".into(),
            grant: "add a production write token".into(),
            ..step("person", "Replace the hero on production")
        };
        let failed = NoteStep {
            result: "not done: the end-to-end workflow has no workflow_dispatch trigger".into(),
            ..step("coderhelm", "Re-run the e2e tests")
        };
        let waiting = NoteStep {
            result: "waiting for approval".into(),
            run: Some(StepRun {
                state: "waiting_approval".into(),
                ..Default::default()
            }),
            ..step("coderhelm", "Deploy the studio")
        };
        let c = OutsideChange {
            system: "the CMS".into(),
            change: "Release".into(),
            steps: vec![held_step, failed, waiting],
            ..Default::default()
        };
        let block = not_done_block(&c);
        assert_eq!(
            block,
            "**Needed from you:**\n\
             1. Replace the hero on production. CoderHelm cannot: no production CMS write connection exists. To let it: add a production write token. (a content editor)\n\
             2. Approve \"Deploy the studio\": a team admin approves it on the Agent permissions page, and CoderHelm then does it. (a team admin)\n"
        );
        // A failure with no listed reason is CoderHelm's to retry, never a
        // person's chore (`person_gate`).
        assert!(!block.contains("Re-run the e2e tests"), "{block}");
        // One state each: nothing says both "CoderHelm" and "needs access".
        assert!(!block.contains("owner: CoderHelm"), "{block}");
        // The ticket note starts with it.
        assert!(note_markdown(&c).starts_with("**Needed from you:**\n1. Replace the hero"));
        // The block an earlier shape put at the top of the PR is taken out:
        // the lead says it now.
        let old = format!("{NOT_DONE_START}\n**Not done**\n- x\n{NOT_DONE_END}\n\n## Summary\nx");
        assert_eq!(with_not_done_top(&old, &c), "## Summary\nx");
        assert_eq!(with_not_done_top("## Summary\nx", &c), "## Summary\nx");
        assert!(unfinished(&c), "the waiting step keeps the run going");
    }

    #[test]
    fn the_multi_repo_run_puts_the_outside_steps_on_the_repo_they_name() {
        let repos = vec!["acme/web".to_string(), "acme/content-studio".to_string()];
        let c = OutsideChange {
            steps: vec![NoteStep {
                how: "the deploy workflow in content-studio".into(),
                ..step("coderhelm", "Deploy the studio")
            }],
            ..Default::default()
        };
        assert_eq!(outside_repo(&c, &repos), Some(&repos[1]));
        let none = OutsideChange {
            steps: vec![step("coderhelm", "Run the confirm query")],
            ..Default::default()
        };
        assert_eq!(outside_repo(&none, &repos), Some(&repos[0]));
        assert_eq!(outside_repo(&none, &[]), None);
    }
}
