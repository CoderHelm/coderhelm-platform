//! How a review fix round goes out, the same for every team.
//!
//! The agent writes to the round's WIP branch (see `super::round`), never
//! the pull request. Before the round lands, what it changed is loaded
//! ([`load_wip`]) into a [`staged::Staged`] view and checked; what the
//! checks take out is written back to the WIP branch ([`write_back`]):
//!
//! 1. Scope ([`scope`]): a judge reads each hunk against the review threads;
//!    hunks no thread asked for are taken back out.
//! 2. Logs ([`logs`]): INFO logs added to scheduled or repeated jobs that
//!    fire on every run are flagged; a log message whose old wording other
//!    code matches (metric filters, alarms, alert rules, scripts, runbooks,
//!    tests) keeps its wording; a changed log level or message needs a test
//!    that asserts it.
//! 3. Generated files ([`generated_step`]): a round that edits what a code
//!    generator reads (a CMS schema, a GraphQL schema) carries the
//!    regenerated output, the new types are in it, and no file declares its
//!    own copy of a generated type: the same check the pull request had
//!    before it opened (`super::generators`), so a later round can't skip
//!    it.
//! 4. The full test suite ([`suite`]), found the way the repository runs it
//!    and run in the sandbox against the staged files.
//!
//! What a check finds goes back to the agent for one fix-up turn, then the
//! round is checked again: hunks still out of scope and log wording still
//! changed despite its consumers are taken out; a failing suite or a
//! changed log with no test holds the push. What passes lands through the
//! round's own landing (one commit, one push) with a message in the
//! repository's commit style ([`commit_style`]).
//! After the push, approvals the push dismissed are named to the person who
//! asked ([`approvals`]). Requester constraints ([`constraints`]) decide
//! whether anything is posted on the PR and whether it may ever be merged.

pub mod approvals;
pub mod commit_style;
pub mod constraints;
pub mod evidence;
pub mod hunks;
pub mod logs;
pub mod scope;
pub mod staged;
pub mod suite;

use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::TokenUsage;
use crate::WorkerState;

pub use staged::Staged;

/// The WIP checkpoint the checks' reverts are written with.
const WRITE_BACK_MESSAGE: &str = "Take out what the review comments didn't ask for";

/// Most characters of hunks a judge reads.
const MAX_HUNK_TEXT: usize = 60_000;

/// Most of the team's other repositories searched for a log's wording.
const MAX_INFRA_REPOS: usize = 4;

/// The round as it stands on its WIP branch (see `super::round`): every
/// file it changed since `base_sha`, with its content then and now. The
/// checks read and revert this; [`write_back`] puts reverts on the WIP
/// branch, and the round lands from there as one commit.
pub async fn load_wip(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base_sha: &str,
    wip: &str,
) -> Staged {
    let staged = Staged::new();
    if base_sha.is_empty() || wip.is_empty() {
        return staged;
    }
    let Ok(compare) = github.get_diff(owner, repo, base_sha, wip).await else {
        return staged;
    };
    // The agent's checkpoint messages, for the one commit's message.
    for m in compare["commits"].as_array().into_iter().flatten() {
        let text = m["commit"]["message"].as_str().unwrap_or("");
        if !text.starts_with(WRITE_BACK_MESSAGE) {
            staged.note_message(text.lines().next().unwrap_or(""));
        }
    }
    for c in super::round::changed_from_compare(&compare) {
        if let Some(prev) = &c.previous {
            let old = github
                .read_file_live(owner, repo, prev, base_sha)
                .await
                .ok();
            staged.write(prev, old, None);
        }
        let original = if c.previous.is_some() {
            None
        } else {
            github
                .read_file_live(owner, repo, &c.path, base_sha)
                .await
                .ok()
        };
        let current = if c.removed {
            None
        } else {
            github.read_file_live(owner, repo, &c.path, wip).await.ok()
        };
        staged.write(&c.path, original, current);
    }
    staged
}

/// Writes what the checks changed in the round (reverted hunks, files
/// put back) onto its WIP branch, as one checkpoint commit there. The pull
/// request is not touched: the round still lands as one commit.
pub async fn write_back(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    wip: &str,
    loaded: &[(String, staged::Change)],
    staged: &Staged,
) -> bool {
    let ops = wip_ops(loaded, staged);
    if ops.is_empty() {
        return true;
    }
    match github
        .batch_write(owner, repo, wip, WRITE_BACK_MESSAGE, &ops)
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(wip, error = %e, "review fix: could not write the checks' reverts to the WIP branch");
            false
        }
    }
}

/// Pure: the writes that bring the WIP branch from `loaded` (what it held)
/// to `staged` (what the checks left).
pub fn wip_ops(
    loaded: &[(String, staged::Change)],
    staged: &Staged,
) -> Vec<crate::clients::github::FileOp> {
    let mut ops = Vec::new();
    for (path, was) in loaded {
        let now = staged.read(path).unwrap_or_else(|| was.original.clone());
        if now == was.current {
            continue;
        }
        ops.push(match now {
            Some(content) => crate::clients::github::FileOp::Write {
                path: path.clone(),
                content,
            },
            None => crate::clients::github::FileOp::Delete { path: path.clone() },
        });
    }
    ops
}

/// The agent's replies (split by `---SPLIT---`), each opened with why the
/// round was held, when it was. Unchanged when nothing holds it.
pub fn held_replies(response: &str, held: &[String]) -> String {
    if held.is_empty() {
        return response.to_string();
    }
    let note = format!(
        "**Not pushed yet.** Before pushing, CoderHelm checks a review fix; this one is held: {}.\n\n",
        held.join("; ")
    );
    response
        .split("---SPLIT---")
        .map(|s| format!("{note}{}", s.trim()))
        .collect::<Vec<_>>()
        .join("\n---SPLIT---\n")
}

/// One fix round's context.
pub struct Round<'a> {
    pub state: &'a WorkerState,
    pub github: &'a GitHubClient,
    pub provider: &'a ModelProvider,
    pub team_id: &'a str,
    pub run_id: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    pub branch: &'a str,
    pub pr_number: u64,
    /// What the review threads ask (empty for a CI-only round).
    pub requests: Vec<scope::Request>,
    pub review_body: String,
    pub ci_failures: bool,
    pub deadline: Option<Instant>,
    suite: tokio::sync::OnceCell<Result<suite::Suite, String>>,
    has_tests: tokio::sync::OnceCell<bool>,
}

impl<'a> Round<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        state: &'a WorkerState,
        github: &'a GitHubClient,
        provider: &'a ModelProvider,
        team_id: &'a str,
        run_id: &'a str,
        owner: &'a str,
        repo: &'a str,
        branch: &'a str,
        pr_number: u64,
    ) -> Self {
        Self {
            state,
            github,
            provider,
            team_id,
            run_id,
            owner,
            repo,
            branch,
            pr_number,
            requests: Vec::new(),
            review_body: String::new(),
            ci_failures: false,
            deadline: None,
            suite: tokio::sync::OnceCell::new(),
            has_tests: tokio::sync::OnceCell::new(),
        }
    }

    /// Whether a person asked for this round (scope is judged against them).
    pub fn human(&self) -> bool {
        !self.requests.is_empty() || !self.review_body.trim().is_empty()
    }

    /// The PR's address, for notices.
    pub fn pr_url(&self) -> String {
        format!(
            "https://github.com/{}/{}/pull/{}",
            self.owner, self.repo, self.pr_number
        )
    }

    /// A file as the round sees it: staged, else the branch.
    async fn text(&self, staged: &Staged, path: &str) -> String {
        match staged.read(path) {
            Some(t) => t.unwrap_or_default(),
            None => self
                .github
                .read_file(self.owner, self.repo, path, self.branch)
                .await
                .unwrap_or_default(),
        }
    }
}

/// What one check of the staged round found.
#[derive(Debug, Default)]
pub struct Check {
    /// What goes back to the agent for its fix-up turn.
    pub findings: Vec<String>,
    /// Evidence lines for the PR.
    pub evidence: Vec<String>,
    /// Why the round must not be pushed (only from the last check).
    pub hold: Vec<String>,
    pub suite: Option<suite::Outcome>,
}

impl Check {
    /// The message for the agent's one fix-up turn, when there is anything.
    pub fn retry_prompt(&self) -> Option<String> {
        if self.findings.is_empty() {
            return None;
        }
        Some(format!(
            "Your changes are staged, not pushed yet. Checks before the push found:\n\n{}\n\n\
             Fix these now with your tools (your staged files are what read_file returns). \
             Change nothing the review comments didn't ask for. Then write your replies to the \
             comments again, in the same order, separated by `---SPLIT---`, saying what you \
             changed.",
            self.findings
                .iter()
                .map(|f| format!("- {f}"))
                .collect::<Vec<_>>()
                .join("\n")
        ))
    }
}

/// Checks the staged round. `last` is the check after the fix-up turn: it
/// takes out what is still out of scope and decides whether the push is
/// held.
pub async fn check(
    round: &Round<'_>,
    staged: &Staged,
    usage: &mut TokenUsage,
    last: bool,
) -> Check {
    let mut out = Check::default();
    if staged.is_empty() {
        return out;
    }

    // 1. Scope.
    if round.human() {
        scope_step(round, staged, usage, last, &mut out).await;
    }
    if staged.is_empty() {
        out.evidence
            .push("Nothing the review comments asked for was left to push".to_string());
        return out;
    }

    // 2. Logs.
    logs_step(round, staged, usage, last, &mut out).await;

    // 3. Generated files.
    generated_step(round, staged, last, &mut out).await;

    // 3. The full suite: on the first check only when nothing else goes back
    //    to the agent (the fix-up turn changes the files anyway), always on
    //    the last.
    if last || out.findings.is_empty() {
        let outcome = run_suite(round, staged).await;
        match &outcome {
            suite::Outcome::Failed { tail, .. } => {
                if last {
                    out.hold.push(outcome.line());
                } else {
                    out.findings.push(format!(
                        "{}. Make every test pass (fix the code, or a test that asserts the old \
                         behaviour the comment asked to change; never skip or delete a test). \
                         Output:\n```\n{}\n```",
                        outcome.line(),
                        common::head_tail_str(tail, 4_000)
                    ));
                }
            }
            suite::Outcome::Passed { .. } | suite::Outcome::NotRun(_) => {}
        }
        out.suite = Some(outcome);
    }
    if let Some(s) = &out.suite {
        out.evidence.push(s.line());
    }
    out
}

/// A staged change as (path, status, patch), the patch holding the lines
/// the file gains (enough for the generator checks, which read added lines).
pub fn staged_patch(path: &str, c: &staged::Change) -> (String, String, String) {
    let status = match (&c.original, &c.current) {
        (None, _) => "added",
        (_, None) => "removed",
        _ => "modified",
    };
    let before: std::collections::HashSet<&str> = c
        .original
        .as_deref()
        .map(|t| t.lines().collect())
        .unwrap_or_default();
    let patch: String = c
        .current
        .as_deref()
        .unwrap_or("")
        .lines()
        .filter(|l| !before.contains(l))
        .map(|l| format!("+{l}\n"))
        .collect();
    (path.to_string(), status.to_string(), format!("@@\n{patch}"))
}

/// What the generator checks find in a round's staged files: for each
/// generator whose inputs the round edits, its outputs not regenerated, a
/// new type missing from them, or a hand-written copy of a generated type.
pub fn generated_problems(
    gens: &[super::generators::Generator],
    files: &[(String, String, String)],
    outputs_of: impl Fn(&super::generators::Generator) -> Vec<(String, String)>,
) -> Vec<String> {
    let paths: Vec<String> = files.iter().map(|(p, _, _)| p.clone()).collect();
    let mut out = Vec::new();
    for (g, inputs) in super::generators::affected(gens, &paths) {
        let mut why: Vec<String> = Vec::new();
        let outputs = outputs_of(g);
        let owned = super::generators::ownership_problems(g, files, &outputs);
        if super::generators::outputs_missing(g, &paths)
            && !super::generators::owned_types(g, files).is_empty()
        {
            why.push(format!(
                "its generated files ({}) are not in this round.",
                g.outputs
                    .iter()
                    .take(3)
                    .map(|o| format!("`{o}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        why.extend(owned);
        if !why.is_empty() {
            out.push(super::generators::block_reason(g, &inputs, &why.join(" ")));
        }
    }
    out
}

async fn generated_step(round: &Round<'_>, staged: &Staged, last: bool, out: &mut Check) {
    let files: Vec<(String, String, String)> = staged
        .changes()
        .iter()
        .map(|(p, c)| staged_patch(p, c))
        .collect();
    if files.is_empty() {
        return;
    }
    let repo = super::generators::load(round.github, round.owner, round.repo, round.branch).await;
    let gens = super::generators::detect(&repo);
    if gens.is_empty() {
        return;
    }
    // Each generator's output as the round leaves it: staged, else the
    // branch.
    let mut texts: Vec<(String, String)> = Vec::new();
    for g in &gens {
        for p in repo
            .tree
            .iter()
            .filter(|p| {
                g.outputs
                    .iter()
                    .any(|o| super::generators::glob_match(o, p))
            })
            .take(3)
        {
            if !texts.iter().any(|(q, _)| q == p) {
                texts.push((p.clone(), round.text(staged, p).await));
            }
        }
    }
    let problems = generated_problems(&gens, &files, |g| {
        texts
            .iter()
            .filter(|(p, _)| {
                g.outputs
                    .iter()
                    .any(|o| super::generators::glob_match(o, p))
            })
            .cloned()
            .collect()
    });
    if problems.is_empty() {
        return;
    }
    if last {
        out.hold.extend(problems);
    } else {
        out.findings.push(format!(
            "{} Regenerate the generated output (with run_codegen when it is offered, else \
             exactly what the generator writes) in this round, and type new things from the \
             generated types, never with a hand-written copy.",
            problems.join(" ")
        ));
    }
}

async fn scope_step(
    round: &Round<'_>,
    staged: &Staged,
    usage: &mut TokenUsage,
    last: bool,
    out: &mut Check,
) {
    let hunks = staged.hunks();
    let total = hunks.len();
    let Some(verdicts) = judge_scope(round, &hunks, usage).await else {
        out.evidence.push(format!(
            "Scope: the check could not read the {total} hunk(s) against the comments; nothing taken out"
        ));
        return;
    };
    let ids = scope::out_of_scope(&verdicts);
    if ids.is_empty() {
        out.evidence.push(format!(
            "Scope: all {total} hunk(s) are what the review comments asked for"
        ));
        return;
    }
    let reverted = scope::reverted_lines(&verdicts, &hunks);
    // The originals, to check each touched file still parses.
    let before: Vec<(String, staged::Change)> = staged.changes();
    let touched = staged.revert(&ids);
    info!(
        run_id = round.run_id,
        reverted = ids.len(),
        "review fix: out-of-scope hunks taken out"
    );
    out.evidence.push(format!(
        "Scope: took out {} of {total} hunk(s) no review comment asked for: {}",
        ids.len(),
        reverted.join("; ")
    ));
    for path in touched {
        let Some(Some(now)) = staged.read(&path) else {
            continue;
        };
        let original = before
            .iter()
            .find(|(p, _)| *p == path)
            .and_then(|(_, c)| c.original.clone());
        if let Err(problem) = super::syntax_check::validate_change(&path, original.as_deref(), &now)
        {
            if last {
                staged.restore(&path);
                out.evidence.push(format!(
                    "Scope: `{path}` no longer parsed without the out-of-scope hunks ({problem}); its change was left out"
                ));
            } else {
                out.findings.push(format!(
                    "These hunks were taken out because no review comment asked for them: {}. \
                     Without them `{path}` no longer parses ({problem}). Make `{path}` correct \
                     again with only what the comments asked for.",
                    reverted.join("; ")
                ));
            }
        }
    }
}

/// The scope judge, asked again once when its answer can't be read or
/// leaves hunks out. None when there is still no answer.
async fn judge_scope(
    round: &Round<'_>,
    hunks: &[staged::NumberedHunk],
    usage: &mut TokenUsage,
) -> Option<Vec<scope::Verdict>> {
    if hunks.is_empty() {
        return Some(Vec::new());
    }
    let ids: Vec<usize> = hunks.iter().map(|h| h.id).collect();
    let text = common::head_tail_str(
        &hunks
            .iter()
            .map(|h| format!("[hunk {}]\n{}", h.id, h.hunk.render()))
            .collect::<Vec<_>>()
            .join("\n"),
        MAX_HUNK_TEXT,
    );
    let prompt = scope::prompt(
        &round.review_body,
        &round.requests,
        round.ci_failures,
        &text,
    );
    let mut best: Option<Vec<scope::Verdict>> = None;
    for attempt in 0..2 {
        match crate::agent::anthropic::converse_json(
            &round.provider.client(),
            round.provider.primary_model_id(),
            scope::SYSTEM,
            &prompt,
            &scope::schema(),
            true,
            "medium",
            4096,
            usage,
        )
        .await
        {
            Ok(answer) => match scope::parse(&answer, &ids) {
                Some((verdicts, missing)) if missing.is_empty() => return Some(verdicts),
                Some((verdicts, missing)) => {
                    warn!(run_id = round.run_id, attempt, missing = ?missing, "review fix: scope judge left hunks out");
                    best = Some(verdicts);
                }
                None => {
                    warn!(
                        run_id = round.run_id,
                        attempt, "review fix: scope judge answer unreadable"
                    )
                }
            },
            Err(e) => {
                warn!(run_id = round.run_id, attempt, error = %e, "review fix: scope judge failed")
            }
        }
    }
    best
}

async fn logs_step(
    round: &Round<'_>,
    staged: &Staged,
    usage: &mut TokenUsage,
    last: bool,
    out: &mut Check,
) {
    let hunks = staged.hunks();
    let found = logs::changes(&hunks);
    if found.is_empty() {
        return;
    }

    // Log wording other things read.
    let mut kept_text: Vec<usize> = Vec::new();
    for f in &found {
        let Some(old) = f.change.old_text() else {
            continue;
        };
        let Some(term) = logs::search_term(old) else {
            out.evidence.push(format!(
                "Log text: \"{old}\" in `{}` is too short to search for consumers",
                f.path
            ));
            continue;
        };
        let (consumers, searched) = log_consumers(round, &f.path, &term).await;
        if consumers.is_empty() {
            out.evidence.push(format!(
                "Log text: searched {} for \"{term}\": no metric filter, alarm, alert rule, script, runbook, doc or test uses it",
                searched.join(", ")
            ));
            continue;
        }
        let named = consumers
            .iter()
            .map(|(repo, path, kind)| format!("`{path}` in {repo} ({kind})"))
            .collect::<Vec<_>>()
            .join(", ");
        out.evidence.push(format!(
            "Log text: \"{term}\" is used by {named}; the wording is kept"
        ));
        if last {
            kept_text.push(f.hunk);
        } else {
            out.findings.push(format!(
                "`{}` {}: the old wording is used by {named}. Keep the old message text exactly; \
                 change only what the comment asked (the level, for example).",
                f.path,
                f.change.describe()
            ));
        }
    }
    if !kept_text.is_empty() {
        kept_text.sort_unstable();
        kept_text.dedup();
        staged.revert(&kept_text);
        out.evidence.push(format!(
            "Log text: {} hunk(s) that still changed used wording were taken out",
            kept_text.len()
        ));
    }

    // Tests that pin a changed level or wording.
    let tests = logs::test_additions(&staged.hunks());
    let needing: Vec<&logs::Found> = found
        .iter()
        .filter(|f| f.change.needs_test() && !kept_text.contains(&f.hunk))
        .collect();
    if !needing.is_empty() {
        let has_tests = *round
            .has_tests
            .get_or_init(|| async {
                round
                    .github
                    .get_tree(round.owner, round.repo, round.branch)
                    .await
                    .map(|t| {
                        t.iter()
                            .any(|e| e.entry_type == "blob" && logs::is_test_path(&e.path))
                    })
                    .unwrap_or(false)
            })
            .await;
        for f in needing {
            if logs::asserted_by(&f.change, &tests) {
                out.evidence.push(format!(
                    "Log test: `{}` {} is asserted by the round's test changes",
                    f.path,
                    f.change.describe()
                ));
            } else if !has_tests {
                out.evidence.push(format!(
                    "Log test: `{}` {}; the repository has no tests to add one to",
                    f.path,
                    f.change.describe()
                ));
            } else if last {
                out.hold.push(format!(
                    "`{}` {} with no test asserting the new level and text",
                    f.path,
                    f.change.describe()
                ));
            } else {
                let (level, text) = f.change.asserted();
                out.findings.push(format!(
                    "`{}` {}: add or update a test that asserts the log at {level} with \
                     \"{text}\" (pytest `caplog`, `assertLogs`, a Jest/Vitest spy on the logger, \
                     a captured `tracing` subscriber), written like the repository's existing \
                     tests for this code, with its fakes.",
                    f.path,
                    f.change.describe()
                ));
            }
        }
    }

    // INFO on every run of a repeated job.
    let mut texts: std::collections::HashMap<String, String> = Default::default();
    for f in &found {
        if !texts.contains_key(&f.path) {
            texts.insert(f.path.clone(), round.text(staged, &f.path).await);
        }
    }
    let candidates =
        logs::info_in_repeated_jobs(&found, |p| texts.get(p).cloned().unwrap_or_default());
    if candidates.is_empty() {
        return;
    }
    let mut prompt = String::new();
    for (i, c) in candidates.iter().enumerate() {
        let (_, msg) = c.change.asserted();
        let needle = if msg.is_empty() { "info" } else { msg };
        let text = texts.get(&c.path).map(|t| t.as_str()).unwrap_or("");
        prompt.push_str(&format!(
            "## Log {} in `{}`: {}\n```\n{}\n```\n\n",
            i + 1,
            c.path,
            c.change.describe(),
            logs::around(text, needle, 25)
        ));
    }
    let every = match crate::agent::anthropic::converse_json(
        &round.provider.client(),
        round.provider.primary_model_id(),
        logs::RUNS_SYSTEM,
        &prompt,
        &logs::runs_schema(),
        true,
        "low",
        2048,
        usage,
    )
    .await
    {
        Ok(a) => logs::every_run(&a),
        Err(e) => {
            warn!(run_id = round.run_id, error = %e, "review fix: log level check failed");
            return;
        }
    };
    for (id, reason) in every {
        let Some(c) = candidates.get(id.saturating_sub(1)) else {
            continue;
        };
        let line = format!(
            "`{}` {} fires on every run of a repeated job ({reason}): DEBUG, or INFO only when something changed",
            c.path,
            c.change.describe()
        );
        if last {
            out.evidence.push(format!("Log level flagged: {line}"));
        } else {
            out.findings.push(line);
        }
    }
}

/// Who reads a log's wording: hits for `term` in this repository (other
/// than the file that logs it) and in the team's other repositories,
/// infrastructure first. Returns (repo, path, kind) for each consumer and
/// the repositories searched.
async fn log_consumers(
    round: &Round<'_>,
    log_path: &str,
    term: &str,
) -> (Vec<(String, String, &'static str)>, Vec<String>) {
    let own = format!("{}/{}", round.owner, round.repo);
    let mut searched = vec![own.clone()];
    let mut found = Vec::new();
    let mut take = |repo: &str, hits: Vec<crate::clients::github::SearchResult>| {
        for h in hits {
            if repo == own && h.path == log_path {
                continue;
            }
            let kind = logs::consumer_kind(&h.path, &h.matches.join("\n"));
            if kind == "code" {
                continue;
            }
            if !found
                .iter()
                .any(|(r, p, _): &(String, String, &str)| r == repo && *p == h.path)
            {
                found.push((repo.to_string(), h.path, kind));
            }
        }
    };
    if let Ok(hits) = round
        .github
        .search_code(round.owner, round.repo, round.branch, term)
        .await
    {
        take(&own, hits);
    }
    let repos = super::infra_repo::team_repos(round.state, round.team_id).await;
    for full in super::infra_repo::search_order(&own, &repos)
        .into_iter()
        .take(MAX_INFRA_REPOS)
    {
        let Some((o, n)) = full.split_once('/') else {
            continue;
        };
        if let Ok(hits) = round.github.search_code(o, n, "HEAD", term).await {
            take(&full, hits);
        }
        searched.push(full);
    }
    (found, searched)
}

/// Finds how the repository runs its full suite (once per round).
async fn discover_suite(round: &Round<'_>, staged: &Staged) -> Result<suite::Suite, String> {
    let mut files: Vec<(String, String)> = Vec::new();
    if let Ok(entries) = round
        .github
        .list_directory(round.owner, round.repo, ".github/workflows", round.branch)
        .await
    {
        for e in entries
            .iter()
            .filter(|e| e.name.ends_with(".yml") || e.name.ends_with(".yaml"))
            .take(12)
        {
            let t = round.text(staged, &e.path).await;
            if !t.is_empty() {
                files.push((e.path.clone(), t));
            }
        }
    }
    for path in suite::SOURCES {
        let t = round.text(staged, path).await;
        if !t.is_empty() {
            files.push((path.to_string(), t));
        }
    }
    for dir in ["scripts", "bin", "ci"] {
        if let Ok(entries) = round
            .github
            .list_directory(round.owner, round.repo, dir, round.branch)
            .await
        {
            for e in entries
                .iter()
                .filter(|e| e.name.to_ascii_lowercase().contains("test"))
                .take(4)
            {
                let t = round.text(staged, &e.path).await;
                if !t.is_empty() {
                    files.push((e.path.clone(), t));
                }
            }
        }
    }
    if files.is_empty() {
        return Err(
            "the repository has no CI workflow, contributing guide or build file to read".into(),
        );
    }
    let mut usage = TokenUsage::default();
    let answer = crate::agent::anthropic::converse_json(
        &round.provider.client(),
        round.provider.primary_model_id(),
        suite::SYSTEM,
        &suite::prompt(&files),
        &suite::schema(),
        true,
        "low",
        2048,
        &mut usage,
    )
    .await
    .map_err(|e| format!("could not read how the tests run: {e}"))?;
    suite::parse(&answer)
}

/// Runs the full suite in the sandbox against the staged files.
async fn run_suite(round: &Round<'_>, staged: &Staged) -> suite::Outcome {
    let state = round.state;
    let sandbox = crate::clients::sandbox::SandboxClient::new(
        &state.codebuild,
        &state.s3,
        &state.logs,
        &state.config.sandbox_bucket_name,
        &state.config.sandbox_project_name,
    )
    .with_egress(crate::agent::policy::sandbox_egress(state, round.team_id).await);
    if !sandbox.is_enabled() {
        return suite::Outcome::NotRun("the sandbox is not set up for this install".into());
    }
    let found = round
        .suite
        .get_or_init(|| discover_suite(round, staged))
        .await
        .clone();
    let s = match found {
        Ok(s) => s,
        Err(why) => return suite::Outcome::NotRun(why),
    };
    let script = s.script();
    // The team's agent permissions decide whether the command may run.
    let gate = crate::agent::policy::gate(
        state,
        round.provider.scope.ctx(common::agent_policy::Agent::Coder),
        round.deadline,
    );
    match gate
        .check_command(
            &script,
            "Run the repository's full test suite before pushing a review fix",
        )
        .await
    {
        common::agent_policy::Outcome::Run { .. } => {}
        common::agent_policy::Outcome::Stop(stop) => {
            crate::agent::policy::record_stop(&stop);
            return suite::Outcome::NotRun("the team's agent permissions stopped the run".into());
        }
        common::agent_policy::Outcome::Refused { message, .. }
        | common::agent_policy::Outcome::Pending { message, .. } => {
            return suite::Outcome::NotRun(format!(
                "the team's agent permissions don't allow `{}` ({message})",
                common::truncate_str(&script, 200)
            ));
        }
    }
    let head = match round
        .github
        .get_ref(round.owner, round.repo, round.branch)
        .await
    {
        Ok(h) => h,
        Err(e) => return suite::Outcome::NotRun(format!("could not read the branch: {e}")),
    };
    let writes: Vec<(String, Option<String>)> = staged
        .changes()
        .into_iter()
        .map(|(p, c)| (p, c.current))
        .collect();
    let tag = suite::state_tag(&writes);
    let source = match sandbox
        .ensure_source(round.run_id, &format!("{head}-{tag}"), || async {
            let tarball = round
                .github
                .download_tarball(round.owner, round.repo, &head)
                .await?;
            suite::overlay_tarball(&tarball, &writes)
        })
        .await
    {
        Ok(k) => k,
        Err(e) => {
            return suite::Outcome::NotRun(format!("could not prepare the staged files: {e}"))
        }
    };
    let node = super::detect_node_version(round.github, round.owner, round.repo, &head).await;
    match sandbox
        .run_checks(&source, &script, node.as_deref(), None, round.deadline)
        .await
    {
        // A failing suite that couldn't connect to a host the sandbox's
        // network allowlist refused didn't test the change: it says so.
        Ok(o) if !o.passed && blocked_suite(&o.output).is_some() => {
            suite::Outcome::NotRun(blocked_suite(&o.output).unwrap_or_default())
        }
        Ok(o) if o.ran => {
            let counts = suite::counts(&o.output);
            if o.passed {
                suite::Outcome::Passed {
                    command: script,
                    source: s.source.clone(),
                    counts,
                }
            } else {
                suite::Outcome::Failed {
                    command: script,
                    source: s.source.clone(),
                    counts,
                    exit: o.exit_code,
                    tail: o.output,
                }
            }
        }
        Ok(o) => suite::Outcome::NotRun(if o.reason.is_empty() {
            common::truncate_str(&o.output, 300).to_string()
        } else {
            o.reason
        }),
        Err(e) => suite::Outcome::NotRun(format!("the sandbox failed: {e}")),
    }
}

/// Why a failing suite run says nothing about the change: it couldn't
/// reach a host the sandbox's network allowlist refused. None when no host
/// was refused or nothing failed to connect.
fn blocked_suite(output: &str) -> Option<String> {
    use common::stop_rules::tooling;
    let hosts = tooling::blocked_hosts(output);
    let err = tooling::network_error(output)?;
    (!hosts.is_empty()).then(|| tooling::blocked_message("the test suite", &hosts, Some(&err)))
}

/// The round's one commit message: the team's commit template when it set
/// one, else the repository's own style learned from the base branch.
pub async fn commit_message(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    conventions: &common::git_conventions::GitConventions,
    tokens: &common::git_conventions::Tokens,
    agent_messages: &[String],
) -> (String, String) {
    let history: Vec<String> = github
        .recent_commits(owner, repo, base, 30)
        .await
        .unwrap_or_default()
        .iter()
        .filter_map(|c| c["commit"]["message"].as_str().map(String::from))
        .collect();
    let style = commit_style::learn(&history);
    let message = if conventions.commit.is_some() || style.samples < commit_style::MIN_SAMPLES {
        let first = agent_messages
            .first()
            .cloned()
            .unwrap_or_else(|| "Address review comments".to_string());
        conventions.commit_message(tokens, &first)
    } else {
        commit_style::compose(&style, tokens.key(), agent_messages)
    };
    let how = if conventions.commit.is_some() {
        "the team's commit template".to_string()
    } else {
        commit_style::describe(&style)
    };
    (commit_style::without_co_authors(&style, &message), how)
}

/// The run's ticket and how its requester hears back.
#[derive(Debug, Clone, Default)]
pub struct Ticket {
    pub source: String,
    pub id: String,
    pub title: String,
    pub body: String,
    pub issue_number: u64,
    /// `owner/name` the issue lives in.
    pub repo: String,
    pub constraints: constraints::Constraints,
}

impl Ticket {
    pub async fn load(state: &WorkerState, team_id: &str, run_id: &str) -> Ticket {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let item = state
            .dynamo
            .get_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", A::S(team_id.to_string()))
            .key("run_id", A::S(run_id.to_string()))
            .send()
            .await
            .ok()
            .and_then(|r| r.item);
        let s = |k: &str| {
            item.as_ref()
                .and_then(|i| i.get(k))
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        Ticket {
            source: s("ticket_source"),
            id: s("ticket_id"),
            title: s("title"),
            body: s("ticket_body"),
            issue_number: item
                .as_ref()
                .and_then(|i| i.get("issue_number"))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0),
            repo: s("repo"),
            constraints: constraints::from_json(&s(constraints::RUN_ATTR)),
        }
    }
}

/// Reads the requester's constraints from the ticket and the review
/// thread, adds them to what earlier rounds found, and keeps them on the
/// run and for the PR's merge gate.
#[allow(clippy::too_many_arguments)]
pub async fn read_constraints(
    state: &WorkerState,
    github: &GitHubClient,
    provider: &ModelProvider,
    team_id: &str,
    run_id: &str,
    owner: &str,
    repo: &str,
    pr_number: u64,
    ticket: &Ticket,
    thread: &str,
    usage: &mut TokenUsage,
) -> constraints::Constraints {
    let mut body = ticket.body.clone();
    if body.trim().is_empty() && ticket.issue_number > 0 {
        let (o, r) = ticket.repo.split_once('/').unwrap_or((owner, repo));
        if let Ok(issue) = github.get_issue(o, r, ticket.issue_number).await {
            body = issue["body"].as_str().unwrap_or("").to_string();
        }
    }
    let sources = vec![
        (
            "The ticket".to_string(),
            format!("{}\n\n{body}", ticket.title),
        ),
        (
            "The review thread on the pull request".to_string(),
            thread.to_string(),
        ),
    ];
    let found = match crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        constraints::SYSTEM,
        &constraints::prompt(&sources),
        &constraints::schema(),
        true,
        "low",
        1024,
        usage,
    )
    .await
    {
        Ok(a) => constraints::parse(&a, &sources),
        Err(e) => {
            warn!(run_id, error = %e, "review fix: could not read requester constraints");
            constraints::Constraints::default()
        }
    };
    let all = ticket.constraints.merged(&found);
    if all != ticket.constraints {
        save_constraints(state, team_id, run_id, owner, repo, pr_number, &all).await;
        info!(run_id, constraints = ?all.lines(), "review fix: requester constraints recorded");
    }
    all
}

async fn save_constraints(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    owner: &str,
    repo: &str,
    pr_number: u64,
    c: &constraints::Constraints,
) {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let json = serde_json::to_string(c).unwrap_or_default();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", A::S(team_id.to_string()))
        .key("run_id", A::S(run_id.to_string()))
        .update_expression("SET #c = :c")
        .expression_attribute_names("#c", constraints::RUN_ATTR)
        .expression_attribute_values(":c", A::S(json.clone()))
        .send()
        .await;
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", A::S(team_id.to_string()))
        .item("sk", A::S(constraints::pr_sk(owner, repo, pr_number)))
        .item("constraints", A::S(json))
        .item("run_id", A::S(run_id.to_string()))
        .send()
        .await;
}

/// The constraints kept for a pull request (what the merge gate reads).
pub async fn pr_constraints(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr_number: u64,
) -> constraints::Constraints {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", A::S(team_id.to_string()))
        .key("sk", A::S(constraints::pr_sk(owner, repo, pr_number)))
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|i| i.get("constraints").and_then(|v| v.as_s().ok()).cloned())
        .map(|s| constraints::from_json(&s))
        .unwrap_or_default()
}

/// Tells the person who asked: on the run page (a progress note), and on
/// the ticket they used (the Jira ticket or the GitHub issue). Never a PR
/// comment.
pub async fn tell_requester(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    run_id: &str,
    ticket: &Ticket,
    title: &str,
    text: &str,
) {
    super::add_progress_note(state, team_id, run_id, text).await;
    match ticket.source.as_str() {
        "jira" if !ticket.id.is_empty() => {
            if let Err(e) = super::post_jira_comment(
                state,
                team_id,
                &ticket.id,
                &super::fix_location::jira_text(text),
                "info",
                title,
            )
            .await
            {
                warn!(run_id, error = %e, "review fix: could not tell the requester on the ticket");
            }
        }
        "github" if ticket.issue_number > 0 => {
            if let Some((o, r)) = ticket.repo.split_once('/') {
                let _ = github
                    .create_issue_comment(
                        o,
                        r,
                        ticket.issue_number,
                        &format!("**{title}**\n\n{text}"),
                    )
                    .await;
            }
        }
        _ => {}
    }
}

/// What a push did to the PR's approvals: checks the reviews after the
/// push (dismissal is applied by GitHub just after it) against the
/// approvers from before.
pub async fn dismissed_after_push(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr_number: u64,
    before: &[String],
) -> Vec<String> {
    if before.is_empty() {
        return Vec::new();
    }
    let mut gone = Vec::new();
    for attempt in 0..3u64 {
        tokio::time::sleep(Duration::from_secs(2 + attempt * 3)).await;
        if let Ok(after) = github.list_pr_reviews(owner, repo, pr_number).await {
            gone = approvals::dismissed(before, &after);
            if !gone.is_empty() {
                break;
            }
        }
    }
    gone
}

/// Records the round on the PR description's Review fixes section.
pub async fn record_round(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr_number: u64,
    round: &evidence::Round,
) {
    let Ok(pr) = github.get_pull_request(owner, repo, pr_number).await else {
        return;
    };
    let body = pr["body"].as_str().unwrap_or("").to_string();
    let section = evidence::section(&evidence::current(&body), round);
    let updated = super::completeness::with_section(
        &body,
        super::completeness::Section::ReviewFixes,
        section,
    );
    if updated != body {
        if let Err(e) = github
            .update_pr_body(owner, repo, pr_number, &updated)
            .await
        {
            warn!(pr_number, error = %e, "review fix: could not record the round in the description");
        }
    }
}

/// Marks the run as waiting on approvals again (the run page shows it).
pub async fn mark_reapproval(state: &WorkerState, team_id: &str, run_id: &str, who: &[String]) {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", A::S(team_id.to_string()))
        .key("run_id", A::S(run_id.to_string()))
        .update_expression("SET reapproval_needed = :w")
        .expression_attribute_values(":w", A::L(who.iter().map(|w| A::S(w.clone())).collect()))
        .send()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A later round on an open PR adds a CMS block and types it by hand:
    /// the round is held until the output is regenerated and imported.
    #[test]
    fn a_later_round_that_edits_a_schema_needs_the_generated_types() {
        use super::super::generators::{GenKind, Generator};
        let gens = vec![Generator {
            kind: GenKind::Sanity,
            name: "npm run typegen".into(),
            command: "npm run typegen".into(),
            inputs: vec!["**/schemaTypes/**".into()],
            outputs: vec!["src/sanity.types.ts".into()],
        }];
        let schema = staged::Change {
            original: None,
            current: Some(
                "export default defineType({\n  name: 'gallery',\n  type: 'object',\n})\n".into(),
            ),
        };
        let component = staged::Change {
            original: Some("export function Page() {}\n".into()),
            current: Some(
                "export function Page() {}\ntype GalleryBlock = { title: string }\n".into(),
            ),
        };
        let files = vec![
            staged_patch("studio/schemaTypes/gallery.ts", &schema),
            staged_patch("src/components/Page.tsx", &component),
        ];
        assert_eq!(files[0].1, "added");
        assert_eq!(files[1].2, "@@\n+type GalleryBlock = { title: string }\n");
        let p = generated_problems(&gens, &files, |_| {
            vec![("src/sanity.types.ts".into(), "export type Hero = {}".into())]
        });
        assert_eq!(p.len(), 1, "{p:?}");
        assert!(p[0].contains("are not in this round"));
        assert!(p[0].contains("has no `Gallery`"));
        assert!(p[0].contains("declares `GalleryBlock` by hand"));
        // Regenerated in the round and used from there: nothing.
        let mut ok = files.clone();
        ok[1].2 = "@@\n+import type { Gallery } from '../sanity.types'\n".into();
        ok.push((
            "src/sanity.types.ts".into(),
            "modified".into(),
            "@@\n+export type Gallery = {}\n".into(),
        ));
        let p = generated_problems(&gens, &ok, |_| {
            vec![(
                "src/sanity.types.ts".into(),
                "export type Gallery = {}".into(),
            )]
        });
        assert!(p.is_empty(), "{p:?}");
    }

    #[test]
    fn only_files_the_checks_changed_are_written_back_to_the_wip_branch() {
        let s = Staged::new();
        s.write("a.py", Some("a\nb\n".into()), Some("A\nb\n".into()));
        s.write("b.py", Some("x\n".into()), Some("y\n".into()));
        s.write("new.py", None, Some("n\n".into()));
        let loaded = s.changes();
        assert!(wip_ops(&loaded, &s).is_empty());
        // The checks put a.py back and took the new file out.
        s.restore("a.py");
        let ids: Vec<usize> = s
            .hunks()
            .iter()
            .filter(|h| h.hunk.path == "new.py")
            .map(|h| h.id)
            .collect();
        s.revert(&ids);
        let ops = wip_ops(&loaded, &s);
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().any(|o| matches!(o,
            crate::clients::github::FileOp::Write { path, content } if path == "a.py" && content == "a\nb\n")));
        assert!(ops.iter().any(|o| matches!(o,
            crate::clients::github::FileOp::Delete { path } if path == "new.py")));
    }

    #[test]
    fn a_held_round_opens_every_reply_with_why() {
        assert_eq!(held_replies("Done.", &[]), "Done.");
        let r = held_replies(
            "Lowered it.\n---SPLIT---\nRenamed it.",
            &["Full test suite FAILED (exit 1): `make test`".into()],
        );
        let parts: Vec<&str> = r.split("---SPLIT---").collect();
        assert_eq!(parts.len(), 2);
        for p in parts {
            assert!(p.trim().starts_with("**Not pushed yet.**"), "{p}");
            assert!(p.contains("`make test`"));
        }
    }

    #[test]
    fn the_retry_prompt_lists_findings_and_asks_for_the_replies_again() {
        let mut c = Check::default();
        assert!(c.retry_prompt().is_none());
        c.findings
            .push("`jobs/a.py` adds info log \"x\": DEBUG".into());
        let p = c.retry_prompt().unwrap();
        assert!(p.contains("- `jobs/a.py` adds info log"));
        assert!(p.contains("---SPLIT---"));
        assert!(p.contains("not pushed yet"));
    }

    #[test]
    fn the_review_fix_guidance_names_levels_consumers_and_tests() {
        assert!(logs::GUIDANCE.contains("DEBUG"));
        assert!(logs::GUIDANCE.contains("metric filters"));
        assert!(logs::GUIDANCE.contains("caplog"));
        assert!(logs::CHECKLIST_ITEM.contains("INFO"));
    }
}
