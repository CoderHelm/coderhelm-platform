//! New-feature tickets, handled the way a careful person would, for every
//! team. Each rule is enforced in code where it can be:
//!
//! 1. **New files are expected** ([`plan_files`]): every plan task says
//!    whether it creates, modifies, deletes or replaces its path. Only
//!    modify, delete and replace paths must exist; a create path must not.
//! 2. **Earlier attempts first** ([`prior`]): earlier pull requests and
//!    branches for the feature (the ticket, the tickets it names, similar
//!    titles) are read with what each did and why it ended; unrelated
//!    commits are never carried over and the branch starts from the default
//!    branch. The plan lists where an attempt and the ticket differ.
//! 3. **Reuse before writing** and 4. **mirror the closest sibling**
//!    ([`reuse`]): the plan lists what it reuses and the closest existing
//!    sibling; before the pull request, new exports that duplicate an
//!    existing helper are flagged (code search for candidates, a model
//!    judges).
//! 5. **Registration completeness** ([`registration`]): a new item appears
//!    everywhere its siblings are registered, or the pull request is blocked
//!    with the list.
//! 6. **Content-dependent tests** ([`content`]): a schema change that makes
//!    the staging content the end-to-end tests read out of date is settled
//!    through the repository's staging-only CMS write connection, or the run
//!    stops naming exactly what must change.
//! 7. **Verification checklist** ([`checklist`]): type-check, lint, unit
//!    tests per acceptance case, the spec and its test map entry, test
//!    packages outside the workspace, repeat runs and responsive checks,
//!    with counts in the pull request; a failing item keeps it a draft.
//! 8. **Open product questions** ([`questions`]): raised in the pull request,
//!    never decided.
//!
//! The planner reads [`planner_block`]; the plan's own check adds
//! [`plan_gaps`]; [`gate`] runs before the pull request; [`shape_body`]
//! writes the description's sections; [`ready_hold`] keeps the draft.

pub mod checklist;
pub mod content;
pub mod plan_files;
pub mod prior;
pub mod questions;
pub mod registration;
pub mod reuse;

use common::e2e::test_package::{self, Repeat, Step};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::passes::triage::TicketIntent;
use crate::WorkerState;

/// Fix rounds the gate gives before it blocks or reports.
const MAX_FIX_ROUNDS: u32 = 1;

/// What the planner is told, by the ticket's kind.
pub fn planner_block(intent: TicketIntent) -> String {
    let mut s = String::new();
    if intent == TicketIntent::Feature {
        s.push_str(&format!(
            "\n\n## New-feature rules (CoderHelm checks these)\n\
             - Reuse before writing: search the code for existing utilities, components, \
             constants and data-fetch functions with similar names and behaviour. List the ones \
             you will reuse, each with its path, in design.md under `{}` (or what you searched \
             when none fits). New code that duplicates an existing exported helper is flagged.\n\
             - Mirror the closest sibling: when adding a component, block, route, handler or any \
             file of a kind that already exists, name the closest existing one by path in \
             design.md under `{}` and follow its structure and usage exactly (props, layout \
             wrappers, overflow and scroll handling, accessibility labels).\n\
             - Register it everywhere: find every place a comparable sibling is referenced \
             (registries, index files, query fragments, maps, test maps, docs) and add a task \
             for each. Regenerate generated files. A missing registration blocks the pull \
             request.\n\
             - Tests: one unit test per option or case the acceptance criteria list; for UI, an \
             end-to-end spec (added to any test map the repository keeps) that also checks there \
             is no horizontal scroll at a narrow ({}px or less) and a wide ({}px or more) \
             viewport.\n\
             - {}\n",
            reuse::REUSE_HEADING,
            reuse::SIBLING_HEADING,
            checklist::NARROW_MAX,
            checklist::WIDE_MIN,
            questions::PLANNER,
        ));
    } else {
        s.push_str(&format!("\n\n{}\n", questions::PLANNER));
    }
    s
}

/// What the plan leaves out, for the plan's own check to send back once.
pub fn plan_gaps(files: &PlanResult, intent: TicketIntent, found: &prior::Found) -> Vec<String> {
    let text = format!(
        "{}\n{}\n{}\n{}",
        files.proposal, files.design, files.tasks, files.spec
    );
    let codes = !files.tasks.trim().is_empty()
        && !files.tasks.starts_with("NO_CHANGES_NEEDED:")
        && !files.tasks.starts_with("CLARIFICATION_NEEDED:");
    if !codes {
        return Vec::new();
    }
    let mut out = prior::plan_gaps(&files.design, &text, &found.attempts);
    if intent == TicketIntent::Feature {
        let creates = plan_files::planned_files(&files.tasks)
            .iter()
            .any(plan_files::PlannedFile::is_new);
        out.extend(reuse::plan_gaps(&files.design, creates));
    }
    out
}

/// Finds the earlier attempts for the run's repository (see [`prior`]).
pub async fn find_attempts(
    github: &GitHubClient,
    msg: &TicketMessage,
    own_branch: &str,
    known: &[crate::passes::ticket_history::EarlierPr],
) -> prior::Found {
    let default_branch = github
        .get_default_branch(&msg.repo_owner, &msg.repo_name)
        .await
        .unwrap_or_else(|_| msg.base_branch.clone());
    let s = prior::Search {
        owner: &msg.repo_owner,
        name: &msg.repo_name,
        default_branch: &default_branch,
        ticket_id: &msg.ticket_id,
        title: &msg.title,
        body: &msg.body,
        own_branch,
    };
    prior::Found {
        attempts: prior::gather(github, &s, known).await,
        keys: s.keys(),
        words: s.ticket_words(),
    }
}

/// What the gate found, for the description and the ready check.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrFacts {
    pub attempts: Vec<String>,
    pub sibling: Option<String>,
    /// New code still flagged as duplicating an existing helper.
    pub duplicates: Vec<String>,
    /// Registration places checked for the new items.
    pub registrations_checked: usize,
    /// Places a new item still isn't registered in (runs other than
    /// features; a feature is blocked instead).
    pub unregistered: Vec<String>,
    /// Set when the change touches a CMS schema the end-to-end tests read.
    pub content: Option<content::Outcome>,
    pub checklist: Vec<checklist::Item>,
    pub facts: checklist::Facts,
    pub maps: Vec<String>,
    pub questions: Vec<String>,
    /// The sandbox steps that failed, and the head they ran on.
    pub sandbox_failed: Vec<String>,
    pub head: String,
}

impl PrFacts {
    fn is_empty(&self) -> bool {
        self.attempts.is_empty()
            && self.sibling.is_none()
            && self.duplicates.is_empty()
            && self.registrations_checked == 0
            && self.unregistered.is_empty()
            && self.content.is_none()
            && self.checklist.is_empty()
            && self.questions.is_empty()
    }
}

/// The description's sections, after what the writer wrote.
pub fn sections(f: &PrFacts) -> String {
    let mut s = String::new();
    if !f.attempts.is_empty() {
        s.push_str("## Earlier attempts\nThis branch starts from the default branch; only what fits was reused.\n");
        for a in &f.attempts {
            s.push_str(&format!("- {a}\n"));
        }
        s.push('\n');
    }
    if f.sibling.is_some()
        || !f.duplicates.is_empty()
        || !f.unregistered.is_empty()
        || f.registrations_checked > 0
    {
        s.push_str("## Reuse and registration\n");
        if let Some(sib) = &f.sibling {
            s.push_str(&format!("- Follows the closest sibling: `{sib}`\n"));
        }
        if f.registrations_checked > 0 && f.unregistered.is_empty() {
            s.push_str(&format!(
                "- Registered in every place its siblings are ({} checked)\n",
                f.registrations_checked
            ));
        }
        for d in f.duplicates.iter().chain(&f.unregistered) {
            s.push_str(&format!("- Flagged for review: {d}\n"));
        }
        s.push('\n');
    }
    if let Some(c) = &f.content {
        s.push_str("## Staging content\n");
        if c.changed.is_empty() {
            s.push_str("- No staging content was changed.\n");
        }
        for l in &c.changed {
            s.push_str(&format!("- Changed on staging: {l}\n"));
        }
        s.push('\n');
    }
    let cl = checklist::markdown(&f.checklist);
    if !cl.is_empty() {
        s.push_str(&cl);
        s.push('\n');
    }
    let qs = questions::markdown(&f.questions);
    if !qs.is_empty() {
        s.push_str(&qs);
    }
    s.trim_end().to_string()
}

/// The description with the sections added once.
pub fn shape_body(body: String, f: &PrFacts) -> String {
    let add = sections(f);
    if add.is_empty() || body.contains(add.as_str()) {
        return body;
    }
    format!("{}\n\n{add}\n", body.trim_end())
}

/// The run's diff as (path, status, patch).
async fn diff(w: &super::proof::Work<'_>) -> Result<Vec<(String, String, String)>, String> {
    w.github
        .compare_files_with_patches(
            &w.msg.repo_owner,
            &w.msg.repo_name,
            &w.msg.base_branch,
            w.branch,
        )
        .await
        .map_err(|e| format!("the diff couldn't be read ({e})"))
}

/// The added lines of a patch from the line exporting `name`, for the judge.
fn new_body(patch: &str, name: &str) -> String {
    let lines: Vec<&str> = patch
        .lines()
        .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
        .map(|l| &l[1..])
        .collect();
    let start = lines.iter().position(|l| l.contains(name)).unwrap_or(0);
    lines[start..]
        .iter()
        .take(40)
        .copied()
        .collect::<Vec<_>>()
        .join("\n")
}

/// Duplicate candidates for the new exports, judged.
async fn duplicates(
    w: &super::proof::Work<'_>,
    files: &[(String, String, String)],
    usage: &mut TokenUsage,
) -> Vec<reuse::Duplicate> {
    let (owner, repo, base) = (&w.msg.repo_owner, &w.msg.repo_name, &w.msg.base_branch);
    let changed: Vec<String> = files.iter().map(|(p, _, _)| p.clone()).collect();
    let mut pairs = Vec::new();
    for (path, name) in reuse::new_exports(files).into_iter().take(6) {
        let Some(term) = reuse::search_term(&name) else {
            continue;
        };
        let hits: Vec<(String, Vec<String>)> =
            match w.github.search_code(owner, repo, base, &term).await {
                Ok(h) => h.into_iter().map(|r| (r.path, r.matches)).collect(),
                Err(_) => continue,
            };
        let patch = files
            .iter()
            .find(|(p, _, _)| p == &path)
            .map(|(_, _, x)| x.as_str())
            .unwrap_or("");
        for c in reuse::similar_existing(&path, &name, &hits, &changed)
            .into_iter()
            .take(2)
        {
            let old = w
                .github
                .read_file_lines(owner, repo, &c.path, base, c.line, c.line + 40)
                .await
                .unwrap_or_default();
            pairs.push((c, new_body(patch, &name), old));
        }
    }
    reuse::judge(w.state, w.provider, usage, &pairs).await
}

/// The static checklist facts, read from the diff and the branch.
async fn checklist_facts(
    w: &super::proof::Work<'_>,
    files: &[(String, String, String)],
    tree: &[String],
) -> (checklist::Facts, Vec<String>) {
    let (owner, repo) = (&w.msg.repo_owner, &w.msg.repo_name);
    let paths: Vec<String> = files
        .iter()
        .filter(|(_, s, _)| s != "removed")
        .map(|(p, _, _)| p.clone())
        .collect();
    let specs: Vec<String> = paths
        .iter()
        .filter(|p| content::is_e2e_spec(p))
        .cloned()
        .collect();
    let mut responsive = Vec::new();
    for s in &specs {
        if let Ok(t) = w.github.read_file(owner, repo, s, w.branch).await {
            if checklist::responsive_checked(&t) {
                responsive.push(s.clone());
            }
        }
    }
    let maps: Vec<String> = tree
        .iter()
        .filter(|p| checklist::is_test_map(p))
        .take(5)
        .cloned()
        .collect();
    let mut map_texts = Vec::new();
    for m in &maps {
        if let Ok(t) = w.github.read_file(owner, repo, m, w.branch).await {
            map_texts.push((m.clone(), t));
        }
    }
    let new_specs: Vec<String> = files
        .iter()
        .filter(|(p, s, _)| s == "added" && content::is_e2e_spec(p))
        .map(|(p, _, _)| p.clone())
        .collect();
    // Every value of each new option the change adds, against the tests the
    // change adds or changes (read whole: a large file's patch is cut).
    let options = checklist::new_options(files);
    let mut tests_text = String::new();
    if !options.is_empty() {
        for (p, _, patch) in files
            .iter()
            .filter(|(p, s, _)| s != "removed" && checklist::is_test_path(p))
            .take(15)
        {
            match w.github.read_file(owner, repo, p, w.branch).await {
                Ok(t) => tests_text.push_str(&t),
                Err(_) => tests_text.push_str(patch),
            }
            tests_text.push('\n');
        }
    }
    let facts = checklist::Facts {
        options_found: options.len(),
        options_untested: checklist::untested_options(&options, &tests_text),
        cases_required: checklist::acceptance_cases(&w.msg.body, &w.plan.spec),
        cases_added: checklist::unit_cases_added(files),
        ui: checklist::is_ui(&paths),
        specs,
        responsive_specs: responsive,
        map_missing: checklist::missing_from_test_maps(&map_texts, &new_specs)
            .into_iter()
            .map(|(m, s)| format!("{m}: {s}"))
            .collect(),
        has_e2e: tree.iter().any(|p| content::is_e2e_spec(p)),
        data_skips: files
            .iter()
            .filter(|(p, s, _)| s != "removed" && content::is_e2e_spec(p))
            .flat_map(|(p, _, patch)| checklist::data_skips(p, patch))
            .collect(),
    };
    (facts, maps)
}

/// The team's test address for the repository (staging or a preview with
/// `{pr}`), as Tests settings keep it.
async fn test_target(state: &WorkerState, team_id: &str, repo: &str) -> Option<String> {
    let st = common::e2e::store::Store::new(&state.dynamo, &state.config.tests_table_name);
    if !st.enabled() {
        return None;
    }
    st.settings(team_id)
        .await
        .ok()?
        .report_source(repo)?
        .target_for(None)
}

/// The sandbox part of the checklist: the root's type-check, lint and unit
/// tests, each test package outside the workspace, and the new specs run
/// [`checklist::REPEATS`] times against the team's test address. Returns
/// the items and the failures' lines.
async fn sandbox_items(
    w: &super::proof::Work<'_>,
    files: &[(String, String, String)],
    tree: &[String],
    specs: &[String],
    head: &str,
) -> (Vec<checklist::Item>, Vec<String>) {
    use checklist::{Item, Status};
    let (owner, repo, branch) = (&w.msg.repo_owner, &w.msg.repo_name, w.branch);
    let state = w.state;
    let mut items = Vec::new();
    let mut failed = Vec::new();
    let root_pj = w
        .github
        .read_file(owner, repo, "package.json", branch)
        .await
        .ok();
    let mut steps: Vec<Step> = root_pj
        .as_deref()
        .map(|pj| checklist::root_steps(pj, tree))
        .unwrap_or_default();
    // Test packages outside the main workspace, checked on their own.
    let changed: Vec<String> = files
        .iter()
        .filter(|(_, s, _)| s != "removed")
        .map(|(p, _, _)| p.clone())
        .collect();
    let pnpm = w
        .github
        .read_file(owner, repo, "pnpm-workspace.yaml", branch)
        .await
        .ok();
    let globs = test_package::workspace_globs(root_pj.as_deref(), pnpm.as_deref());
    let mut texts: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for f in &changed {
        if let Some(dir) = test_package::package_dir(tree, f).filter(|d| !d.is_empty()) {
            let pj = format!("{dir}/package.json");
            if let std::collections::hash_map::Entry::Vacant(slot) = texts.entry(pj) {
                if let Ok(t) = w.github.read_file(owner, repo, slot.key(), branch).await {
                    slot.insert(t);
                }
            }
        }
    }
    let pkgs = test_package::packages_outside(tree, &changed, &globs, &|p| texts.get(p).cloned());
    for p in &pkgs {
        if let Some(s) = test_package::check_steps(p) {
            steps.extend(s);
        }
    }
    // The new spec, repeated against a fixed test address. A preview
    // address (`{pr}`) exists only once the pull request does.
    let target = test_target(state, &w.msg.team_id, &format!("{owner}/{repo}")).await;
    let spec_pkg = pkgs
        .iter()
        .find(|p| p.runner.is_some())
        .cloned()
        .or_else(|| {
            specs.first().and_then(|s| {
                let dir = test_package::package_dir(tree, s)?;
                let pj = texts
                    .get(&format!("{dir}/package.json"))
                    .cloned()
                    .or(root_pj.clone())?;
                let mut v =
                    test_package::packages_outside(tree, std::slice::from_ref(s), &[], &|_| {
                        Some(pj.clone())
                    });
                v.pop()
            })
        });
    let repeat = match (&target, &spec_pkg) {
        (Some(t), Some(p)) if !t.contains("{pr}") && !specs.is_empty() => {
            let rel: Vec<String> = specs
                .iter()
                .map(|s| {
                    s.strip_prefix(&format!("{}/", p.dir))
                        .unwrap_or(s)
                        .to_string()
                })
                .collect();
            test_package::repeat_cmd(p, &rel, checklist::REPEATS, Some(t))
        }
        _ => None,
    };
    if !specs.is_empty() && repeat.is_none() {
        items.push(Item::new(
            "new spec repeated",
            Status::NotRun,
            format!(
                "run `{}` with `--repeat-each={}` against the preview once it is up",
                specs.join(" "),
                checklist::REPEATS
            ),
        ));
    }
    if steps.is_empty() && repeat.is_none() {
        return (items, failed);
    }
    if state.config.sandbox_bucket_name.is_empty() || state.config.sandbox_project_name.is_empty() {
        for s in &steps {
            items.push(Item::new(
                s.kind,
                Status::NotRun,
                format!("`{}`; CI runs it on the pull request", s.cmd),
            ));
        }
        return (items, failed);
    }
    // The team's agent permissions decide which commands may run.
    let gate = crate::agent::policy::gate(
        state,
        w.provider.scope.ctx(common::agent_policy::Agent::Coder),
        None,
    );
    let mut kept: Vec<Step> = Vec::new();
    for s in steps {
        match gate
            .check_command(&s.cmd, "Run the feature's verification checklist")
            .await
        {
            common::agent_policy::Outcome::Run { .. } => kept.push(s),
            common::agent_policy::Outcome::Refused { message, .. }
            | common::agent_policy::Outcome::Pending { message, .. } => items.push(Item::new(
                s.kind,
                Status::NotRun,
                format!("`{}` ({message})", s.cmd),
            )),
            common::agent_policy::Outcome::Stop(_) => return (items, failed),
        }
    }
    let repeat = match repeat {
        Some(cmd) => match gate.check_command(&cmd, "Repeat the new spec").await {
            common::agent_policy::Outcome::Run { .. } => Some(cmd),
            _ => None,
        },
        None => None,
    };
    let sandbox = crate::clients::sandbox::SandboxClient::new(
        &state.codebuild,
        &state.s3,
        &state.logs,
        &state.config.sandbox_bucket_name,
        &state.config.sandbox_project_name,
    )
    .with_egress(crate::agent::policy::sandbox_egress(state, &w.msg.team_id).await);
    let source = match sandbox
        .ensure_source(w.run_id, head, || {
            w.github.download_tarball(owner, repo, head)
        })
        .await
    {
        Ok(k) => k,
        Err(e) => {
            items.push(Item::new(
                "sandbox",
                Status::NotRun,
                format!("the branch couldn't be prepared ({e})"),
            ));
            return (items, failed);
        }
    };
    let script = test_package::sandbox_script(&kept, repeat.as_deref());
    let node = super::detect_node_version(w.github, owner, repo, branch).await;
    match sandbox
        .run_checks(&source, &script, node.as_deref(), None, None)
        .await
    {
        Ok(o) if o.ran => {
            let n = kept.len() + usize::from(repeat.is_some());
            let codes = test_package::step_codes(&o.output, n);
            for (s, c) in kept.iter().zip(&codes) {
                let (status, verdict) = match c {
                    Some(0) => (Status::Pass, "passed".to_string()),
                    Some(c) => (Status::Fail, format!("failed (exit {c})")),
                    None => (Status::NotRun, "no result".to_string()),
                };
                if status == Status::Fail {
                    failed.push(format!("{} failed: `{}`", s.kind, s.cmd));
                }
                items.push(Item::new(s.kind, status, format!("`{}` {verdict}", s.cmd)));
            }
            if repeat.is_some() {
                let r: Option<Repeat> = test_package::parse_repeat(&o.output);
                let status = if r.is_some_and(|r| r.green()) {
                    Status::Pass
                } else {
                    Status::Fail
                };
                let line = r.map(|r| r.line()).unwrap_or_else(|| "no count".into());
                if status == Status::Fail {
                    failed.push(format!(
                        "the new spec repeated {} times: {line}",
                        checklist::REPEATS
                    ));
                }
                items.push(Item::new(
                    "new spec repeated",
                    status,
                    format!(
                        "{} x{} against {}: {line}",
                        specs.join(", "),
                        checklist::REPEATS,
                        target.as_deref().unwrap_or("")
                    ),
                ));
            }
            if !failed.is_empty() {
                failed.push(format!(
                    "Output:\n```\n{}\n```",
                    common::tail_str(&o.output, 3_000)
                ));
            }
        }
        Ok(o) => items.push(Item::new(
            "sandbox",
            Status::NotRun,
            format!("no result ({})", o.reason),
        )),
        Err(e) => items.push(Item::new(
            "sandbox",
            Status::NotRun,
            format!("couldn't run ({e})"),
        )),
    }
    (items, failed)
}

/// Rule 6: settles the staging content a schema change needs. `None` when
/// the change touches no CMS schema the end-to-end tests read.
async fn content_outcome(
    w: &super::proof::Work<'_>,
    files: &[(String, String, String)],
    tree: &[String],
) -> Option<content::Outcome> {
    let (owner, repo, base) = (&w.msg.repo_owner, &w.msg.repo_name, &w.msg.base_branch);
    let mut changes = Vec::new();
    let mut specs = Vec::new();
    let mut schemas: Vec<(String, String)> = Vec::new();
    for (p, s, patch) in files {
        if s == "removed" {
            continue;
        }
        if content::is_e2e_spec(p) {
            if let Ok(t) = w.github.read_file(owner, repo, p, w.branch).await {
                specs.push((p.clone(), t));
            }
            continue;
        }
        let lower = p.to_lowercase();
        if !(lower.contains("schema") || lower.contains("studio") || lower.contains("cms")) {
            continue;
        }
        let Ok(text) = w.github.read_file(owner, repo, p, w.branch).await else {
            continue;
        };
        if content::is_schema_file(p, &text) {
            changes.extend(content::schema_changes(
                s,
                patch,
                &content::type_name(&text),
            ));
            schemas.push((p.clone(), text));
        }
    }
    if changes.is_empty() || !tree.iter().any(|p| content::is_e2e_spec(p)) {
        return None;
    }
    let mut cms = false;
    for r in tree
        .iter()
        .filter(|p| content::is_catch_all_path(p))
        .take(5)
    {
        if let Ok(t) = w.github.read_file(owner, repo, r, base).await {
            if content::is_cms_catch_all(r, &t) {
                cms = true;
                break;
            }
        }
    }
    if !cms {
        return None;
    }
    let needs = content::needs(&changes, &specs);
    if needs.is_empty() {
        return Some(content::Outcome::default());
    }
    let full = format!("{owner}/{repo}");
    let conn = load_connection(w.state, &w.msg.team_id, &full).await;
    let mut prod_files = Vec::new();
    for p in tree.iter().filter(|p| {
        let n = p.rsplit('/').next().unwrap_or(p);
        n.starts_with(".env") && n.contains("prod")
    }) {
        if let Ok(t) = w.github.read_file(owner, repo, p, base).await {
            prod_files.push((p.clone(), t));
        }
    }
    let prod = common::staging_cms::production_datasets_in_code(&prod_files);
    // Each new block's fields and where documents hold it, so a block of
    // that type already on staging is named for replacement.
    let shapes: Vec<(String, content::BlockShape)> = needs
        .iter()
        .filter_map(|n| match n {
            content::Need::BlockOnPage { block, .. } => {
                let (fields, required) = schemas
                    .iter()
                    .find(|(_, t)| content::type_name(t) == *block)
                    .map(|(_, t)| content::fields_of(t))
                    .unwrap_or_default();
                Some((
                    block.clone(),
                    content::BlockShape {
                        fields,
                        required,
                        held_in: content::held_in(block, &schemas),
                        min_items: specs
                            .iter()
                            .filter(|(p, _)| {
                                matches!(n, content::Need::BlockOnPage { spec, .. } if spec == p)
                            })
                            .find_map(|(_, t)| content::min_items(t)),
                    },
                ))
            }
            _ => None,
        })
        .collect();
    // Without a write connection, existing content is still looked up
    // through the CMS's public read API when the repository names it.
    let env = super::load_codegen_env(w.state, &w.msg.team_id, owner, repo).await;
    let mut cfg_files = Vec::new();
    for p in super::outside_work::files_to_read(tree)
        .into_iter()
        .filter(|p| !p.starts_with(".github/") && !p.ends_with("package.json"))
    {
        if let Ok(t) = w.github.read_file(owner, repo, &p, base).await {
            cfg_files.push((p, t));
        }
    }
    let public = super::outside_work::content_source(&cfg_files, &env);
    let out = content::settle(
        &w.state.http,
        content::Access {
            repo: &full,
            conn: conn.as_ref().map(|(c, t)| (c, t.as_str())),
            public: public.as_ref(),
        },
        &prod,
        &needs,
        &shapes,
    )
    .await;
    info!(
        run_id = w.run_id,
        changed = out.changed.len(),
        blocking = out.blocking.len(),
        "Staging content checked for the schema change"
    );
    Some(out)
}

/// The repository's staging CMS connection with its opened token.
pub(crate) async fn load_connection(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
) -> Option<(common::staging_cms::StagingCms, String)> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(&common::staging_cms::sk(repo)))
        .send()
        .await
        .ok()?
        .item?;
    let c = common::staging_cms::StagingCms::from_item(&item)?;
    let sealed = item.get(common::staging_cms::TOKEN_ATTR)?.as_s().ok()?;
    let kms = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: &state.config.secrets_key_id,
    };
    match common::staging_cms::open(&kms, team_id, repo, sealed).await {
        Ok(t) => Some((c, t)),
        Err(e) => {
            warn!(repo, error = %e, "Could not open the staging CMS token");
            None
        }
    }
}

/// The check before the pull request opens. Problems get one fix round:
/// carried-over commits and missing registrations that survive it block the
/// pull request, and so does staging content that must change and can't be
/// changed here; duplicates and checklist failures go in the description
/// (the checklist keeps the pull request a draft). `Ok(None)` when nothing
/// here applies to the run.
pub async fn gate(
    w: &super::proof::Work<'_>,
    evidence: &mut super::proof::RunEvidence,
    usage: &mut TokenUsage,
    intent: TicketIntent,
    found: &prior::Found,
    has_time: impl Fn() -> bool,
) -> Result<Option<PrFacts>, String> {
    let feature = intent == TicketIntent::Feature;
    let (owner, repo, base) = (&w.msg.repo_owner, &w.msg.repo_name, &w.msg.base_branch);
    let tree: Vec<String> = w
        .github
        .get_tree(owner, repo, base)
        .await
        .map(|t| {
            t.into_iter()
                .filter(|e| e.entry_type == "blob")
                .map(|e| e.path)
                .collect()
        })
        .unwrap_or_default();
    let planned: Vec<String> = plan_files::planned_files(&w.plan.tasks)
        .into_iter()
        .map(|f| f.path)
        .collect();
    let sibling = reuse::recorded_sibling(&w.plan.design);
    let mut rounds = 0;
    let mut facts = PrFacts {
        attempts: found.pr_lines(),
        sibling: sibling.clone().filter(|_| feature),
        ..Default::default()
    };
    loop {
        let files = diff(w).await?;
        let changed: Vec<String> = files
            .iter()
            .filter(|(_, s, _)| s != "removed")
            .map(|(p, _, _)| p.clone())
            .collect();
        let added: Vec<String> = files
            .iter()
            .filter(|(_, s, _)| s == "added")
            .map(|(p, _, _)| p.clone())
            .collect();
        let head = w
            .github
            .get_ref(owner, repo, w.branch)
            .await
            .unwrap_or_default();
        let carried = prior::carried_unrelated(
            &changed,
            &planned,
            &found.attempts,
            &found.keys,
            &found.words,
        );
        let (missing, places) = if added.is_empty() {
            (Vec::new(), 0)
        } else {
            registration::check(
                w.github,
                owner,
                repo,
                base,
                w.branch,
                &added,
                &tree,
                sibling.as_deref(),
            )
            .await
        };
        let dups = if feature {
            duplicates(w, &files, usage).await
        } else {
            Vec::new()
        };
        let (cf, maps) = if feature {
            checklist_facts(w, &files, &tree).await
        } else {
            (checklist::Facts::default(), Vec::new())
        };
        let static_items = if feature {
            checklist::static_items(&cf)
        } else {
            Vec::new()
        };
        let static_fails = checklist::failing(&static_items);
        // The sandbox runs once the cheap problems are settled.
        let (sandbox, sandbox_failed) = if feature
            && carried.is_empty()
            && missing.is_empty()
            && static_fails.is_empty()
            && !changed.is_empty()
        {
            sandbox_items(w, &files, &tree, &cf.specs, &head).await
        } else {
            (Vec::new(), Vec::new())
        };
        info!(
            run_id = w.run_id,
            carried = carried.len(),
            missing_registrations = missing.len(),
            duplicates = dups.len(),
            checklist_fails = static_fails.len(),
            sandbox_fails = sandbox_failed.len(),
            rounds,
            "New-feature rules checked before the pull request"
        );
        // A data migration released expand then contract, with a fallback
        // and its test (every ticket kind).
        let release_gaps = crate::passes::release_plan::diff_gaps(&files);
        let clean = release_gaps.is_empty()
            && carried.is_empty()
            && missing.is_empty()
            && dups.is_empty()
            && static_fails.is_empty()
            && sandbox_failed.is_empty();
        if clean || rounds >= MAX_FIX_ROUNDS || !has_time() {
            if !release_gaps.is_empty() {
                return Err(format!(
                    "the migration can't be released safely yet: {}",
                    release_gaps.join(" ")
                ));
            }
            if !carried.is_empty() {
                return Err(format!(
                    "the branch carries over changes from an earlier attempt that this ticket doesn't ask for: {}",
                    carried
                        .iter()
                        .map(|(p, from)| format!("`{p}` (from {from})"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            // A feature's new item must be registered; for other runs the
            // places are flagged in the description instead.
            if !missing.is_empty() && feature {
                return Err(format!(
                    "new items aren't registered everywhere their siblings are: {}",
                    missing
                        .iter()
                        .map(registration::Missing::line)
                        .collect::<Vec<_>>()
                        .join("; ")
                ));
            }
            let content = content_outcome(w, &files, &tree).await;
            if let Some(c) = content.as_ref().filter(|c| !c.blocking.is_empty()) {
                return Err(format!(
                    "the change makes the staging content its end-to-end tests read out of date. \
                     Change this content on staging{}: {}",
                    if c.changed.is_empty() {
                        String::new()
                    } else {
                        format!(" (already changed: {})", c.changed.join("; "))
                    },
                    c.blocking_lines().join("; ")
                ));
            }
            facts.registrations_checked = places;
            facts.unregistered = missing.iter().map(registration::Missing::line).collect();
            facts.duplicates = dups.iter().map(reuse::Duplicate::line).collect();
            facts.content = content;
            facts.checklist = static_items.into_iter().chain(sandbox).collect();
            facts.facts = cf;
            facts.maps = maps;
            facts.questions = open_questions(
                w,
                usage,
                questions::collect(&w.plan.design, &evidence.notes),
            )
            .await;
            facts.sandbox_failed = sandbox_failed;
            facts.head = head;
            super::add_progress_note(
                w.state,
                &w.msg.team_id,
                w.run_id,
                &format!(
                    "New-feature rules checked: {} checklist item(s) failing, {} flagged duplicate(s)",
                    checklist::failing(&facts.checklist).len(),
                    facts.duplicates.len()
                ),
            )
            .await;
            return Ok((!facts.is_empty()).then_some(facts));
        }
        rounds += 1;
        let mut feedback = String::new();
        if !release_gaps.is_empty() {
            feedback.push_str("\nThe migration must be released expand then contract:\n");
            for g in &release_gaps {
                feedback.push_str(&format!("- {g}\n"));
            }
        }
        if !carried.is_empty() {
            feedback.push_str("\nThe branch changes files only an earlier attempt's unrelated commits touched. Undo these changes (restore each file to the default branch's version) unless this ticket needs them:\n");
            for (p, from) in &carried {
                feedback.push_str(&format!("- `{p}` (from {from})\n"));
            }
        }
        if !missing.is_empty() {
            feedback.push_str(&registration::feedback(&missing));
        }
        if !dups.is_empty() {
            feedback.push_str("\nNew code duplicates existing exported helpers. Call the existing helper instead and remove the new one, unless it is really different (then say why in your notes):\n");
            for d in &dups {
                feedback.push_str(&format!("- {}\n", d.line()));
            }
        }
        feedback.push_str(&checklist::feedback(&static_items));
        if !sandbox_failed.is_empty() {
            feedback.push_str("\nThe verification checks fail:\n");
            for f in &sandbox_failed {
                feedback.push_str(&format!("{f}\n"));
            }
        }
        super::add_progress_note(
            w.state,
            &w.msg.team_id,
            w.run_id,
            "New-feature rules found problems, fixing",
        )
        .await;
        let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "implement").await;
        match super::implement::run(
            w.state,
            w.msg,
            w.github,
            w.plan,
            w.branch,
            w.rules,
            w.repo_instructions,
            Some(&feedback),
            "medium",
            w.provider,
            usage,
            w.file_cache,
            Some(w.run_id),
            None,
            // Registrations and test maps live outside the diff.
            false,
        )
        .await
        {
            Ok(r) => evidence.merge(r.evidence),
            Err(e) => return Err(format!("the fix round failed: {e}")),
        }
        let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "review").await;
    }
}

/// The product questions nothing on the ticket settles yet: the ticket's own
/// text, people's comments on it and the answers given in CoderHelm are read
/// first (see `resolved::settled_on_ticket`), so an answered question leaves
/// the description instead of being asked on every update.
async fn open_questions(
    w: &super::proof::Work<'_>,
    usage: &mut TokenUsage,
    qs: Vec<String>,
) -> Vec<String> {
    if qs.is_empty() {
        return qs;
    }
    let notes = super::open_pr_choice::load_record(w.state, &w.msg.team_id, &w.msg.ticket_id)
        .await
        .notes;
    let settled = super::resolved::settled_on_ticket(
        &w.msg.answers,
        &super::plan::ticket_text(&w.msg.title, &w.msg.body),
        &notes,
    );
    let asked = common::run_questions::from_lines(&qs);
    let verdicts = super::resolved::judge(w.state, w.provider, &asked, &settled, usage).await;
    super::resolved::open_only(asked, &verdicts)
        .into_iter()
        .map(|q| q.question)
        .collect()
}

// ── the ready check ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ReadyRecord {
    run_id: String,
    facts: checklist::Facts,
    maps: Vec<String>,
    sandbox_failed: Vec<String>,
    sha: String,
}

fn ready_sk(repo: &str, pr: u64) -> String {
    format!("FEATURE_READY#{repo}#{pr}")
}

/// Records a feature pull request, so leaving draft waits for its
/// checklist.
pub async fn register_pr(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    pr: u64,
    f: &PrFacts,
) {
    if f.checklist.is_empty() {
        return;
    }
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let r = ReadyRecord {
        run_id: run_id.to_string(),
        facts: f.facts.clone(),
        maps: f.maps.clone(),
        sandbox_failed: f
            .sandbox_failed
            .iter()
            .filter(|l| !l.starts_with("Output:"))
            .cloned()
            .collect(),
        sha: f.head.clone(),
    };
    let ttl = (chrono::Utc::now() + chrono::Duration::days(60)).timestamp();
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", super::attr_s(&msg.team_id))
        .item("sk", super::attr_s(&ready_sk(&repo, pr)))
        .item(
            "data",
            super::attr_s(&serde_json::to_string(&r).unwrap_or_default()),
        )
        .item(
            "ttl",
            aws_sdk_dynamodb::types::AttributeValue::N(ttl.to_string()),
        )
        .send()
        .await
    {
        warn!(error = %e, "Could not save the feature's ready check");
    }
}

/// Why a feature pull request stays a draft, read from its head now: the
/// checklist's diff items fail, or the sandbox checks failed on this same
/// head. `None` when it may leave draft (or isn't a feature pull request).
pub async fn ready_hold(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
) -> Option<String> {
    let full = format!("{owner}/{repo}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(&ready_sk(&full, pr)))
        .send()
        .await
        .ok()?
        .item?;
    let r: ReadyRecord = serde_json::from_str(item.get("data")?.as_s().ok()?).ok()?;
    let p = github.get_pull_request(owner, repo, pr).await.ok()?;
    let sha = p["head"]["sha"].as_str().unwrap_or("").to_string();
    let base = p["base"]["ref"].as_str().unwrap_or("main").to_string();
    if sha == r.sha && !r.sandbox_failed.is_empty() {
        return Some(r.sandbox_failed.join("; "));
    }
    let files = github
        .compare_files_with_patches(owner, repo, &base, &sha)
        .await
        .ok()?;
    let mut facts = r.facts.clone();
    facts.cases_added = checklist::unit_cases_added(&files);
    facts.responsive_specs.clear();
    for s in &facts.specs {
        if let Ok(t) = github.read_file(owner, repo, s, &sha).await {
            if checklist::responsive_checked(&t) {
                facts.responsive_specs.push(s.clone());
            }
        }
    }
    let mut maps = Vec::new();
    for m in &r.maps {
        if let Ok(t) = github.read_file(owner, repo, m, &sha).await {
            maps.push((m.clone(), t));
        }
    }
    let new_specs: Vec<String> = files
        .iter()
        .filter(|(p, s, _)| s == "added" && content::is_e2e_spec(p))
        .map(|(p, _, _)| p.clone())
        .collect();
    facts.map_missing = checklist::missing_from_test_maps(&maps, &new_specs)
        .into_iter()
        .map(|(m, s)| format!("{m}: {s}"))
        .collect();
    let fails = checklist::failing(&checklist::static_items(&facts));
    if fails.is_empty() {
        None
    } else {
        info!(pr, fails = ?fails, "Feature checklist fails; the PR stays a draft");
        Some(fails.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(design: &str, tasks: &str) -> PlanResult {
        PlanResult {
            proposal: String::new(),
            design: design.into(),
            tasks: tasks.into(),
            spec: String::new(),
            repo_tasks: vec![],
            fix: None,
        }
    }

    #[test]
    fn a_feature_plan_is_sent_back_for_reuse_and_sibling() {
        let p = plan(
            "## Approach\nx",
            "- [ ] src/blocks/Quote.tsx: [create] The block\n",
        );
        let gaps = plan_gaps(&p, TicketIntent::Feature, &prior::Found::default());
        assert_eq!(gaps.len(), 2);
        // A bug plan isn't held to the feature sections.
        assert!(plan_gaps(&p, TicketIntent::Bug, &prior::Found::default()).is_empty());
        // Nor is a plan that changes no code.
        let p = plan("", "NO_CHANGES_NEEDED: already there");
        assert!(plan_gaps(&p, TicketIntent::Feature, &prior::Found::default()).is_empty());
    }

    #[test]
    fn the_planner_is_told_the_feature_rules_only_for_features() {
        let f = planner_block(TicketIntent::Feature);
        assert!(f.contains(reuse::REUSE_HEADING));
        assert!(f.contains(reuse::SIBLING_HEADING));
        assert!(f.contains("Open product questions"));
        let b = planner_block(TicketIntent::Bug);
        assert!(!b.contains(reuse::REUSE_HEADING));
        assert!(b.contains("Open product questions"));
    }

    #[test]
    fn the_description_states_staging_content_and_the_checklist() {
        let f = PrFacts {
            attempts: vec!["#41 (o/r) \"Quote block\": closed".into()],
            sibling: Some("src/blocks/Cards.tsx".into()),
            registrations_checked: 2,
            content: Some(content::Outcome::default()),
            checklist: vec![checklist::Item::new(
                "lint",
                checklist::Status::Pass,
                "`npm run lint` passed",
            )],
            questions: vec!["Who sees the block?".into()],
            ..Default::default()
        };
        let body = shape_body("## Summary\nAdds the block.".into(), &f);
        assert!(body.contains("## Earlier attempts"));
        assert!(body.contains("Follows the closest sibling: `src/blocks/Cards.tsx`"));
        assert!(body.contains("## Staging content\n- No staging content was changed."));
        assert!(body.contains("## Verification checklist\n- [PASS] lint"));
        assert!(body.contains("## Open product questions"));
        // Added once.
        assert_eq!(shape_body(body.clone(), &f), body);
    }

    #[test]
    fn a_missing_registration_outside_a_feature_is_flagged_not_claimed() {
        let f = PrFacts {
            registrations_checked: 2,
            unregistered: vec!["`Quote` isn't registered in `e2e/test-map.json`".into()],
            ..Default::default()
        };
        let s = sections(&f);
        assert!(s.contains("Flagged for review: `Quote` isn't registered"));
        assert!(!s.contains("Registered in every place"));
    }

    #[test]
    fn new_body_starts_at_the_export() {
        let patch = "+import x from 'y'\n+export function formatEventDate(d) {\n+  return d\n+}\n";
        assert!(new_body(patch, "formatEventDate").starts_with("export function formatEventDate"));
    }
}
