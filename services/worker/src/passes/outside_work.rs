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

fn deploys_studio(text: &str) -> bool {
    let l = text.to_lowercase();
    l.contains("sanity deploy")
        || l.contains("sanity schema deploy")
        || (l.contains("deploy") && l.contains("studio"))
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
                let cmd = text
                    .lines()
                    .map(str::trim)
                    .map(|l| l.trim_start_matches("- ").trim_start_matches("run:").trim())
                    .find(|l| l.contains("deploy") && !l.starts_with('#') && !l.starts_with("name"))
                    .unwrap_or("")
                    .to_string();
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
    Abilities {
        source: content_source(files, env),
        deploys,
        e2e,
        migrations_dir: folder_with(tree, &["migrations"]),
        fixtures_dir: folder_with(tree, &["fixtures", "seed", "seeds"]),
        staging_write,
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
             2. CoderHelm does it after the PR opens (`by: coderhelm`): start a deploy or end-to-end \
             workflow listed below, run the confirm query.\n\
             3. Only what is left goes to a person (`by: person`), each with `why_person` (the \
             exact permission or connection CoderHelm lacks, or why a person must decide) and \
             `grant` (how to give CoderHelm that access so it can do it next time).\n",
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
}

impl NoteStep {
    pub fn doer(&self) -> Doer {
        Doer::parse(&self.by)
    }

    fn text(&self) -> String {
        format!("{} {}", self.step, self.how).to_lowercase()
    }

    fn is_deploy(&self) -> bool {
        let t = self.text();
        t.contains("deploy") && (t.contains("studio") || t.contains("schema") || t.contains("cms"))
    }

    fn is_content(&self) -> bool {
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
        verb && noun && !self.is_deploy()
    }

    fn is_production(&self) -> bool {
        let t = self.text();
        t.contains("production") || words(&t).any(|w| w == "prod")
    }

    fn is_e2e_rerun(&self) -> bool {
        let t = self.text();
        (t.contains("e2e") || t.contains("end-to-end") || t.contains("end to end"))
            && (t.contains("run") || t.contains("rerun"))
            && !self.is_content()
    }

    fn is_verify(&self) -> bool {
        let t = self.text();
        (t.contains("confirm") || t.contains("verify") || t.contains("query"))
            && !self.is_content()
            && !self.is_deploy()
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
                if let Some(d) = &a.migrations_dir {
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
                } else if a.staging_write {
                    out.push(format!(
                        "Step {n} changes staging content, and CoderHelm has this repository's \
                         staging CMS write connection: make it `by: coderhelm`."
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
        if s.is_content() && s.values.is_empty() {
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

/// One step as both the ticket note and the PR list it, with its owner.
pub fn step_line(n: usize, s: &NoteStep) -> (String, String) {
    let mut t = format!("{n}. {}", one_line(&s.step));
    if !s.how.trim().is_empty() {
        t.push_str(&format!(
            " How: {}.",
            one_line(&s.how).trim_end_matches('.')
        ));
    }
    if !s.values.is_empty() {
        t.push_str(&format!(
            " Values: {}.",
            s.values
                .iter()
                .map(|v| one_line(v))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    if !s.remove.is_empty() {
        t.push_str(&format!(
            " Remove: {}.",
            s.remove
                .iter()
                .map(|v| format!("`{}`", one_line(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !s.minimum.trim().is_empty() {
        t.push_str(&format!(
            " At least: {}.",
            one_line(&s.minimum).trim_end_matches('.')
        ));
    }
    match s.doer() {
        Doer::CoderHelm if !s.result.trim().is_empty() => {
            let state = if s.done {
                "Done by CoderHelm"
            } else {
                "CoderHelm could not do it"
            };
            t.push_str(&format!(
                " {state}: {}.",
                one_line(&s.result).trim_end_matches('.')
            ));
        }
        Doer::CoderHelm => t.push_str(" CoderHelm does this after the pull request opens."),
        Doer::Person => {
            if !s.why_person.trim().is_empty() {
                t.push_str(&format!(
                    " Why a person: {}.",
                    one_line(&s.why_person).trim_end_matches('.')
                ));
            }
            if !s.grant.trim().is_empty() {
                t.push_str(&format!(
                    " So CoderHelm can do it next time: {}.",
                    one_line(&s.grant).trim_end_matches('.')
                ));
            }
        }
        Doer::ThisPr => {}
    }
    let owner = match s.doer() {
        Doer::ThisPr => "this pull request".to_string(),
        Doer::CoderHelm => "CoderHelm".to_string(),
        Doer::Person => match one_line(&s.owner) {
            o if o.is_empty() => "not named yet".to_string(),
            o => o,
        },
    };
    (t, owner)
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

/// The confirm line.
pub fn confirm_line(c: &OutsideChange) -> Option<String> {
    let f = c.confirm.as_ref()?;
    if f.query.trim().is_empty() && f.page.trim().is_empty() && f.e2e.trim().is_empty() {
        return None;
    }
    let mut t = String::from("How to confirm:");
    if !f.query.trim().is_empty() {
        t.push_str(&format!(" query `{}`", one_line(&f.query)));
        if !f.expect.trim().is_empty() {
            t.push_str(&format!(
                ", expect {}",
                one_line(&f.expect).trim_end_matches('.')
            ));
        }
        t.push(';');
    }
    if !f.page.trim().is_empty() {
        t.push_str(&format!(
            " look at {};",
            one_line(&f.page).trim_end_matches('.')
        ));
    }
    if !f.e2e.trim().is_empty() {
        t.push_str(&format!(
            " `{}` must run and pass, not skip;",
            one_line(&f.e2e)
        ));
    }
    let mut t = t.trim_end_matches(';').to_string();
    t.push('.');
    if !f.result.trim().is_empty() {
        t.push_str(&format!(
            " CoderHelm ran the query: {}",
            quote_result(&f.result)
        ));
    }
    Some(t)
}

/// The steps outside this pull request, in order, each with its owner: what
/// the PR's "Not in this PR" lists, from the same steps as the ticket note.
pub fn pr_lines(c: &OutsideChange) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = c
        .steps
        .iter()
        .enumerate()
        .filter(|(_, s)| s.doer() != Doer::ThisPr)
        .map(|(i, s)| step_line(i + 1, s))
        .collect();
    if let Some(l) = confirm_line(c) {
        out.push((l, "the owner of the last step".to_string()));
    }
    out
}

/// The ticket note's body: the names and lookup, then every step in order
/// with its owner, then how to confirm.
pub fn note_markdown(c: &OutsideChange) -> String {
    let mut out = String::new();
    for l in context_lines(c) {
        out.push_str(&format!("- {l}\n"));
    }
    if !c.steps.is_empty() {
        out.push_str("\n**Steps, in order**\n");
        for (i, s) in c.steps.iter().enumerate() {
            let (t, owner) = step_line(i + 1, s);
            out.push_str(&format!("{t} (owner: {owner})\n"));
        }
    }
    if let Some(l) = confirm_line(c) {
        out.push_str(&format!("\n{l}\n"));
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
                if !s.done && !s.result.trim().is_empty() {
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
    let Some(s) = a.source.as_ref() else {
        return Some("No CMS project id and dataset are named in this repository.".into());
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

/// What one CoderHelm step will do, from its text and the repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Start this workflow on the PR branch.
    Dispatch(Route),
    /// Add this label to the PR.
    Label(String, Route),
    /// Run the confirm query.
    Query,
    /// CoderHelm can't do it: why, and how to grant it.
    Cannot(String),
}

/// The action for a CoderHelm step.
pub fn action_for(s: &NoteStep, a: &Abilities, c: &OutsideChange) -> Action {
    let pick = |routes: &[Route]| -> Option<Action> {
        let r = routes
            .iter()
            .filter(|r| r.startable())
            .find(|r| r.named_in(&format!("{} {}", s.step, s.how)))
            .or_else(|| routes.iter().find(|r| r.startable()))?
            .clone();
        Some(match &r.trigger {
            Trigger::Label(l) => Action::Label(l.clone(), r.clone()),
            _ => Action::Dispatch(r),
        })
    };
    if s.is_deploy() {
        return pick(&a.deploys).unwrap_or_else(|| {
            Action::Cannot("the repository has no studio/schema deploy workflow CoderHelm can start (one with workflow_dispatch, or one that runs on a pull request label); add `workflow_dispatch:` to the deploy workflow's triggers".to_string())
        });
    }
    if s.is_e2e_rerun() {
        return pick(&a.e2e).unwrap_or_else(|| {
            Action::Cannot("the end-to-end workflow has no workflow_dispatch trigger; add `workflow_dispatch:` to it so CoderHelm can start it".to_string())
        });
    }
    if s.is_verify() || (!s.is_content() && c.confirm.as_ref().is_some_and(|f| !f.query.is_empty()))
    {
        return if a.source.is_some() {
            Action::Query
        } else {
            Action::Cannot("no CMS project id and dataset are named in the repository's config or codegen environment; add them to the codegen environment (Settings, repository, codegen environment)".to_string())
        };
    }
    if s.is_content() {
        return Action::Cannot(if s.is_production() {
            "CoderHelm never changes production content".to_string()
        } else if a.staging_write {
            "content beyond removing dropped fields and setting schema defaults is not written by CoderHelm on staging; write it as a migration in the change".to_string()
        } else {
            "this repository has no staging CMS write connection; add one in Settings, repository, Staging CMS (a staging-only write token)".to_string()
        });
    }
    Action::Cannot("CoderHelm has no action for this step".to_string())
}

/// Does the CoderHelm steps after the pull request opens, recording each
/// result on its step (and the confirm query's on `confirm`). Returns
/// whether anything was done.
#[allow(clippy::too_many_arguments)]
pub async fn perform(
    github: &crate::clients::github::GitHubClient,
    http: &reqwest::Client,
    owner: &str,
    repo: &str,
    branch: &str,
    pr_number: u64,
    a: &Abilities,
    c: &mut OutsideChange,
) -> bool {
    let mut any = false;
    let snapshot = c.clone();
    for s in c
        .steps
        .iter_mut()
        .filter(|s| s.doer() == Doer::CoderHelm && !s.done)
    {
        any = true;
        match action_for(s, a, &snapshot) {
            Action::Dispatch(r) => {
                match github
                    .dispatch_workflow(
                        owner,
                        repo,
                        r.file.rsplit('/').next().unwrap_or(&r.file),
                        branch,
                        &serde_json::json!({}),
                    )
                    .await
                {
                    Ok(()) => {
                        let link = github
                            .list_workflow_runs(
                                owner,
                                repo,
                                r.file.rsplit('/').next().unwrap_or(&r.file),
                                branch,
                            )
                            .await
                            .ok()
                            .and_then(|runs| {
                                runs.first()
                                    .and_then(|x| x["html_url"].as_str().map(String::from))
                            })
                            .unwrap_or_else(|| {
                                format!("https://github.com/{owner}/{repo}/actions")
                            });
                        s.result = format!("started {} on `{branch}`: {link}", r.line());
                        s.done = true;
                    }
                    Err(e) => s.result = format!("starting {} failed: {e}", r.line()),
                }
            }
            Action::Label(l, r) => match github.add_label(owner, repo, pr_number, &l).await {
                Ok(()) => {
                    s.result = format!(
                        "added the `{l}` label to #{pr_number}, which starts {}",
                        r.line()
                    );
                    s.done = true;
                }
                Err(e) => s.result = format!("adding the `{l}` label failed: {e}"),
            },
            Action::Query => {
                let (Some(src), Some(q)) = (
                    a.source.as_ref(),
                    snapshot
                        .confirm
                        .as_ref()
                        .map(|f| f.query.clone())
                        .filter(|q| !q.trim().is_empty()),
                ) else {
                    s.result = "no confirm query to run".to_string();
                    continue;
                };
                match public_query(http, src, &q).await {
                    Ok(v) => {
                        let out = v.to_string();
                        s.result = format!(
                            "ran `{}` on {}: {}",
                            one_line(&q),
                            src.labels(),
                            quote_result(&out)
                        );
                        s.done = true;
                    }
                    Err(e) => s.result = format!("ran `{}` on {}: {e}", one_line(&q), src.labels()),
                }
            }
            Action::Cannot(why) => s.result = format!("not done: {why}"),
        }
    }
    // The confirm query's own result, whenever it can be read.
    if let (Some(src), Some(f)) = (a.source.as_ref(), c.confirm.as_mut()) {
        if !f.query.trim().is_empty() && f.result.is_empty() {
            f.result = match public_query(http, src, &f.query).await {
                Ok(v) => v.to_string(),
                Err(e) => format!("the query failed: {e}"),
            };
            any = true;
        }
    }
    any
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
        assert!(note.contains("Already there: `page-events` (key `k1`), \"Events\"; fields the new schema doesn't know: `image`; new required fields it lacks: `tabs`; left by #41"), "{note}");
        assert!(note.contains("1. Replace the carousel block's content on production Values: title = Classes. Why a person: only an editor picks the classes shown."));
        assert!(note.contains(
            "How to confirm: query `count(*[_type == \"carousel\"])`, expect exactly 1."
        ));
        // The PR lists the same steps.
        let pr = pr_lines(&c);
        assert!(note.contains(&pr[0].0));
        assert_eq!(pr[0].1, "a content editor");
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
            h.not_done[0].contains("CoderHelm could not do it: not done: the end-to-end workflow")
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
            action_for(&step("coderhelm", "Replace the block content on production"), &a, &c),
            Action::Cannot(w) if w.contains("never changes production")
        ));
        let none = Abilities::default();
        assert!(matches!(
            action_for(&step("coderhelm", "Deploy the studio"), &none, &c),
            Action::Cannot(w) if w.contains("workflow_dispatch")
        ));
    }
}
