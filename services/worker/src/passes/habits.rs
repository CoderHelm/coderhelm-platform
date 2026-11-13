//! What a careful engineer settles before writing code, as part of the plan.
//!
//! A plan once named the files to change and nothing about them: whether an
//! earlier attempt at the same change had been reverted, which other code
//! (in this repository or another of the team's) used what it changed, how
//! a bug would be shown to exist before its fix, and which of its doubts
//! changed what gets built. Each is a field of the plan's decision here
//! ([`Habits`], the `habits` object of the `fix_location.json` block), and
//! each is held to what code can check:
//!
//! - **Reproduce first** ([`Repro`]): a bug's plan names the test that fails
//!   on the code as it is, and that test's task is moved to the top of the
//!   checklist ([`put_repro_first`]) so it is written before the fix. When
//!   no failing test is feasible the plan says why and names the next best
//!   proof.
//! - **Read the history** ([`AreaHistory`]): the worker itself reads the
//!   commits of the files the plan modifies ([`read_history`]) and hands the
//!   planner what it found. A revert on one of them must be answered by
//!   name.
//! - **Know who it affects** ([`Affected`]): the worker asks the code graph
//!   what the planned files reach and searches the team's other
//!   repositories for each name the plan says it changes
//!   ([`find_dependents`]). Every other repository with a hit, and the
//!   callers in this one, are accounted for: updated by a task, or safe with
//!   the reason.
//! - **Ask early** ([`EarlyQuestion`]): a doubt that changes what gets built
//!   is asked at planning time with a proposed default, and the default is
//!   what the run builds meanwhile ([`fold_questions`]).
//!
//! What a draft lacks goes back to the planner in the plan's one send-back
//! ([`plan_gaps`]); what is still open after it stays on the decision's open
//! gaps. The coder reads the result in design.md ([`design_section`]), and
//! the check after the build (`habits_check`) holds the change to it.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};

use super::cross_repo::RepoSearch;
use super::feature::plan_files::{planned_files, FileAction};
use super::fix_location::{FixDecision, FixLocation};
use super::plan::PlanResult;
use crate::clients::github::GitHubClient;

/// Files whose history the worker reads for one plan.
pub const MAX_HISTORY_FILES: usize = 8;
/// Commits read per file.
const HISTORY_COMMITS: u32 = 30;
/// Names searched for in the team's other repositories for one plan.
pub const MAX_SURFACES: usize = 6;
/// Other repositories searched.
const MAX_OTHER_REPOS: usize = 8;
/// Hits kept per repository and name.
const MAX_HITS: usize = 5;
/// In-repository dependents named to the planner.
const MAX_IN_REPO: usize = 25;

/// A field that reads loosely: a value of the wrong shape is left empty, so
/// one malformed habit never costs the plan its whole decision.
fn lenient<'de, D: Deserializer<'de>, T: DeserializeOwned + Default>(d: D) -> Result<T, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    Ok(serde_json::from_value(v).unwrap_or_default())
}

/// The `habits` object of the planner's decision.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Habits {
    #[serde(default, deserialize_with = "lenient")]
    pub repro: Repro,
    #[serde(default, deserialize_with = "lenient")]
    pub area_history: Vec<AreaHistory>,
    /// Each name other code may depend on that the change alters: an
    /// exported function or type, an API route, a response or event field,
    /// a table column, a config key.
    #[serde(default, deserialize_with = "lenient")]
    pub surfaces: Vec<String>,
    /// When the change alters nothing other code can depend on: why.
    #[serde(default, deserialize_with = "lenient")]
    pub no_outside_use: String,
    #[serde(default, deserialize_with = "lenient")]
    pub affected: Vec<Affected>,
    #[serde(default, deserialize_with = "lenient")]
    pub ask_early: Vec<EarlyQuestion>,
}

/// How a bug is shown to exist before it is fixed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repro {
    /// The test file that fails on the code as it is (a task of the plan).
    #[serde(default)]
    pub test: String,
    /// Why it fails there: the reported failure, as the test will show it.
    #[serde(default)]
    pub fails_with: String,
    /// When no failing test can be written: why (no test setup, a pure
    /// configuration change).
    #[serde(default)]
    pub not_feasible: String,
    /// The next best proof, when there is no failing test.
    #[serde(default)]
    pub next_best: String,
}

/// What the history of one touched file shows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AreaHistory {
    #[serde(default)]
    pub path: String,
    /// What past pull requests, reverts and blame show.
    #[serde(default)]
    pub found: String,
    /// The pull requests and commits it rests on (`#123`, a short sha).
    #[serde(default, deserialize_with = "lenient")]
    pub refs: Vec<String>,
    /// What it changes about this plan.
    #[serde(default)]
    pub bearing: String,
}

/// One caller or dependent of what the change touches.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Affected {
    /// Where it is: `owner/repo:path`, or a path of this repository.
    #[serde(default, rename = "where")]
    pub where_: String,
    /// What it uses of the change.
    #[serde(default)]
    pub uses: String,
    /// `update` (a task changes it) or `safe`.
    #[serde(default)]
    pub action: String,
    /// Why it is safe, or what the update is.
    #[serde(default)]
    pub why: String,
}

impl Affected {
    /// The repository it names (`owner/repo`), when it names one.
    pub fn repo(&self) -> Option<&str> {
        let (repo, _) = self.where_.trim().split_once(':')?;
        (repo.matches('/').count() == 1 && !repo.contains(' ')).then_some(repo)
    }

    /// Its path, without the repository.
    pub fn path(&self) -> &str {
        match self.repo() {
            Some(_) => self.where_.trim().split_once(':').map_or("", |(_, p)| p),
            None => self.where_.trim(),
        }
        .trim()
    }

    pub fn updates(&self) -> bool {
        self.action.trim().eq_ignore_ascii_case("update")
    }

    fn is_safe(&self) -> bool {
        self.action.trim().eq_ignore_ascii_case("safe")
    }
}

/// A doubt that changes what gets built, asked while the plan is made.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EarlyQuestion {
    #[serde(default)]
    pub question: String,
    /// What the run builds until someone answers.
    #[serde(default)]
    pub default: String,
    /// The plan's files that would change with another answer.
    #[serde(default, deserialize_with = "lenient")]
    pub depends_on: Vec<String>,
}

// ── What the worker reads itself ───────────────────────────────────────────

/// A revert among a file's commits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Revert {
    pub sha: String,
    pub date: String,
    pub subject: String,
}

/// The recent history of one file the plan modifies.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileHistory {
    pub path: String,
    pub commits: usize,
    pub reverts: Vec<Revert>,
    /// Pull requests its commits name (`(#123)`), newest first.
    pub prs: Vec<u64>,
    /// Its newest commits, one line each.
    pub recent: Vec<String>,
}

fn pr_numbers(subject: &str) -> Vec<u64> {
    let mut out = Vec::new();
    let mut rest = subject;
    while let Some(i) = rest.find('#') {
        rest = &rest[i + 1..];
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if let Ok(n) = digits.parse::<u64>() {
            if !out.contains(&n) {
                out.push(n);
            }
        }
    }
    out
}

/// Whether a commit subject takes an earlier change back out.
pub fn is_revert(subject: &str) -> bool {
    let low = subject.trim().to_ascii_lowercase();
    low.starts_with("revert")
        || low.contains("reverts commit")
        || low.contains("revert \"")
        || low.starts_with("back out")
        || low.starts_with("backout")
        || low.starts_with("roll back")
        || low.starts_with("rollback")
}

/// One file's history from GitHub's commit list for its path.
pub fn file_history(path: &str, commits: &[serde_json::Value]) -> FileHistory {
    let mut h = FileHistory {
        path: path.to_string(),
        commits: commits.len(),
        ..Default::default()
    };
    for c in commits {
        let sha: String = c["sha"].as_str().unwrap_or("").chars().take(7).collect();
        let message = c["commit"]["message"].as_str().unwrap_or("");
        let subject = message.lines().next().unwrap_or("").trim();
        let date: String = c["commit"]["committer"]["date"]
            .as_str()
            .or_else(|| c["commit"]["author"]["date"].as_str())
            .unwrap_or("")
            .chars()
            .take(10)
            .collect();
        for n in pr_numbers(subject) {
            if !h.prs.contains(&n) {
                h.prs.push(n);
            }
        }
        if h.recent.len() < 5 {
            h.recent.push(format!(
                "{sha} {date} {}",
                common::truncate_str(subject, 120)
            ));
        }
        if is_revert(subject) || message.contains("This reverts commit") {
            h.reverts.push(Revert {
                sha,
                date,
                subject: common::truncate_str(subject, 160).to_string(),
            });
        }
    }
    h
}

/// The tasks of the run's own repository: its group in a plan that spans
/// several, else the whole checklist.
fn own_tasks<'a>(files: &'a PlanResult, own_repo: &str) -> &'a str {
    files
        .repo_tasks
        .iter()
        .find(|r| format!("{}/{}", r.owner, r.name).eq_ignore_ascii_case(own_repo))
        .map_or(files.tasks.as_str(), |r| r.tasks.as_str())
}

/// The existing files of this repository the plan changes (never the ones
/// it creates, never its tests).
pub fn modified_paths(files: &PlanResult, own_repo: &str) -> Vec<String> {
    planned_files(own_tasks(files, own_repo))
        .into_iter()
        .filter(|f| !f.is_new() && f.action != Some(FileAction::Create))
        .map(|f| f.path)
        .filter(|p| !is_test_path(p))
        .collect()
}

/// Whether the plan writes code at all.
fn plans_code(files: &PlanResult) -> bool {
    let t = files.tasks.trim_start();
    let stops = t.is_empty()
        || t.starts_with("NO_CHANGES_NEEDED:")
        || t.starts_with("CLARIFICATION_NEEDED:")
        || t.starts_with(super::fix_location::NO_CODE_CHANGE);
    !stops
        && files.fix.as_ref().is_some_and(|d| {
            matches!(
                d.effective_location(),
                FixLocation::Code | FixLocation::Both
            )
        })
}

/// Reads the commits of the files the plan modifies. A file GitHub can't
/// list is left out: the check never fails a plan on a read error.
pub async fn read_history(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    files: &PlanResult,
) -> Vec<FileHistory> {
    if !plans_code(files) {
        return Vec::new();
    }
    let mut out = Vec::new();
    for path in modified_paths(files, &format!("{owner}/{repo}"))
        .into_iter()
        .take(MAX_HISTORY_FILES)
    {
        match github
            .commits_for_path(owner, repo, &path, None, HISTORY_COMMITS)
            .await
        {
            Ok(commits) if !commits.is_empty() => out.push(file_history(&path, &commits)),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "Could not read a planned file's history")
            }
        }
    }
    out
}

/// What depends on the planned change, as the worker found it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dependents {
    /// Files of this repository that import a planned file or reference a
    /// name it defines, with why.
    pub in_repo: Vec<(String, String)>,
    /// Hits in the team's other repositories: repository, the name
    /// searched, the matching lines.
    pub other_repos: Vec<(String, String, Vec<String>)>,
    /// The team has other repositories connected.
    pub has_other_repos: bool,
}

/// A name worth a code search: long enough and specific enough that a hit
/// means a use.
pub fn searchable(surface: &str) -> Option<String> {
    let s = surface.trim().trim_matches('`').trim();
    let ok = s.len() >= 4 && s.len() <= 80 && !s.contains('\n');
    ok.then(|| s.to_string())
}

/// Asks the code graph what the planned files reach, and searches the
/// team's other repositories for each name the plan says it changes.
pub async fn find_dependents(
    state: &crate::WorkerState,
    graph: Option<&super::code_graph::Graph>,
    search: &dyn RepoSearch,
    own_repo: &str,
    team_repos: &[String],
    files: &PlanResult,
) -> Dependents {
    let others: Vec<&String> = team_repos
        .iter()
        .filter(|r| !r.eq_ignore_ascii_case(own_repo))
        .take(MAX_OTHER_REPOS)
        .collect();
    let mut out = Dependents {
        has_other_repos: !others.is_empty(),
        ..Default::default()
    };
    if !plans_code(files) {
        return out;
    }
    let paths = modified_paths(files, own_repo);
    if let Some(g) = graph {
        let planned: Vec<String> = planned_files(own_tasks(files, own_repo))
            .into_iter()
            .map(|f| f.path)
            .collect();
        out.in_repo = g
            .impacted_by(state, &paths)
            .await
            .into_iter()
            .filter(|(f, _)| !planned.contains(f) && !is_test_path(f))
            .collect();
    }
    let surfaces: Vec<String> = files
        .fix
        .as_ref()
        .map(|d| {
            d.habits
                .surfaces
                .iter()
                .filter_map(|s| searchable(s))
                .collect()
        })
        .unwrap_or_default();
    for surface in surfaces.iter().take(MAX_SURFACES) {
        for repo in &others {
            match search.search(repo, surface).await {
                Ok(hits) if !hits.is_empty() => out.other_repos.push((
                    repo.to_string(),
                    surface.clone(),
                    hits.into_iter().take(MAX_HITS).collect(),
                )),
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(repo = %repo, error = %e, "Could not search a repository for dependents")
                }
            }
        }
    }
    out
}

/// What the worker read for one draft.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Facts {
    pub own_repo: String,
    pub history: Vec<FileHistory>,
    pub dependents: Dependents,
}

/// Where the worker reads a draft's facts.
pub struct Reader<'a> {
    pub state: &'a crate::WorkerState,
    pub github: &'a GitHubClient,
    pub graph: Option<&'a super::code_graph::Graph>,
    pub search: &'a dyn RepoSearch,
    pub owner: &'a str,
    pub repo: &'a str,
    pub team_repos: &'a [String],
}

impl Reader<'_> {
    /// The history of the files a draft modifies and what depends on them.
    pub async fn facts(&self, files: &PlanResult) -> Facts {
        let own_repo = format!("{}/{}", self.owner, self.repo);
        Facts {
            history: read_history(self.github, self.owner, self.repo, files).await,
            dependents: find_dependents(
                self.state,
                self.graph,
                self.search,
                &own_repo,
                self.team_repos,
                files,
            )
            .await,
            own_repo,
        }
    }
}

/// What the check still finds after the plan's one send-back, kept on the
/// decision: each gap as an open gap, and each use in another repository
/// the plan never accounted for listed among those it affects, marked as
/// not checked, so the pull request names it either way.
pub fn keep_open(d: &mut FixDecision, facts: &Facts, left: &[String]) {
    for (repo, surface, hits) in &facts.dependents.other_repos {
        let known = d
            .habits
            .affected
            .iter()
            .any(|a| a.repo().is_some_and(|r| r.eq_ignore_ascii_case(repo)));
        if known {
            continue;
        }
        let path = hits
            .first()
            .and_then(|h| h.split(':').next())
            .map(str::trim)
            .unwrap_or("");
        d.habits.affected.push(Affected {
            where_: format!("{repo}:{path}"),
            uses: format!("`{surface}`"),
            action: UNCHECKED.to_string(),
            why: String::new(),
        });
    }
    d.open_gaps.extend(
        left.iter()
            .map(|g| common::truncate_str(g.lines().next().unwrap_or(g), 400).to_string()),
    );
}

/// The action of a dependent the worker found and the plan never settled.
pub const UNCHECKED: &str = "unchecked";

// ── The plan's check ───────────────────────────────────────────────────────

fn mentions(text: &str, r: &Revert) -> bool {
    let low = text.to_ascii_lowercase();
    (!r.sha.is_empty() && low.contains(&r.sha.to_ascii_lowercase()))
        || pr_numbers(&r.subject)
            .iter()
            .any(|n| super::ticket_history::names_pr(text, *n))
}

fn history_text(h: &AreaHistory) -> String {
    format!("{} {} {}", h.found, h.refs.join(" "), h.bearing)
}

fn history_lines(history: &[FileHistory]) -> String {
    history
        .iter()
        .map(|h| {
            let mut s = format!("  - `{}`: {} recent commit(s)", h.path, h.commits);
            if !h.prs.is_empty() {
                let prs: Vec<String> = h.prs.iter().take(6).map(|n| format!("#{n}")).collect();
                s.push_str(&format!(", pull requests {}", prs.join(", ")));
            }
            for r in &h.reverts {
                s.push_str(&format!(
                    "\n    - REVERT {} {}: {}",
                    r.sha, r.date, r.subject
                ));
            }
            for l in h.recent.iter().take(3) {
                s.push_str(&format!("\n    - {l}"));
            }
            s
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn same_path(a: &str, b: &str) -> bool {
    let n = |p: &str| {
        p.trim()
            .trim_matches('`')
            .trim_start_matches("./")
            .to_string()
    };
    let (a, b) = (n(a), n(b));
    !a.is_empty() && (a == b || a.ends_with(&format!("/{b}")) || b.ends_with(&format!("/{a}")))
}

fn is_task(files: &PlanResult, own_repo: &str, path: &str) -> bool {
    planned_files(own_tasks(files, own_repo))
        .iter()
        .any(|f| same_path(&f.path, path))
}

/// Whether the plan gives `repo` work of its own: a task group or an
/// end-to-end step.
fn plans_repo(files: &PlanResult, repo: &str) -> bool {
    files
        .repo_tasks
        .iter()
        .any(|r| format!("{}/{}", r.owner, r.name).eq_ignore_ascii_case(repo))
        || files.fix.as_ref().is_some_and(|d| {
            d.end_to_end
                .iter()
                .any(|s| s.repo.trim().eq_ignore_ascii_case(repo))
        })
}

/// What a draft still lacks, as instructions for the planner (empty when
/// nothing is missing). Each carries what the worker read, so the planner
/// answers facts and never has to be trusted to have looked.
pub fn plan_gaps(files: &PlanResult, facts: &Facts) -> Vec<String> {
    let Some(d) = files.fix.as_ref().filter(|_| plans_code(files)) else {
        return Vec::new();
    };
    let h = &d.habits;
    let mut out = Vec::new();

    // Reproduce first.
    if d.intent.is_bug() {
        let r = &h.repro;
        let test = r.test.trim();
        if test.is_empty() && (r.not_feasible.trim().is_empty() || r.next_best.trim().is_empty()) {
            out.push("This is a bug: say how it is shown to exist before the fix (`habits.repro`). Name the test file that fails on the code as it is for the reported reason (`test`, with `fails_with`), and give that file a task in tasks.md. Only when no failing test can be written (the repository has no test setup for this code, the change is pure configuration), say why in `not_feasible` and name the next best proof in `next_best` (the exact command, query or page that shows the failure before and its absence after).".to_string());
        } else if !test.is_empty() && !is_task(files, &facts.own_repo, test) {
            out.push(format!(
                "`habits.repro.test` names `{test}`, but tasks.md has no task for that file. Add the task that writes the failing test (it is written before the fix)."
            ));
        }
    }

    // Read the history.
    let unanswered: Vec<&FileHistory> = facts
        .history
        .iter()
        .filter(|f| {
            f.reverts.iter().any(|r| {
                !h.area_history
                    .iter()
                    .any(|a| same_path(&a.path, &f.path) && mentions(&history_text(a), r))
            })
        })
        .collect();
    if !unanswered.is_empty() {
        out.push(format!(
            "A file you plan to change had a change taken back out. Read each revert below (`github_history` with `kind: pull` or `commits`, and the pull request's discussion) and answer it in `habits.area_history`: an entry for that path whose `refs` name the revert's commit or pull request, `found` saying what was tried and why it was reverted, and `bearing` saying how this plan differs. A plan that repeats a reverted approach is wrong.\n{}",
            history_lines(&unanswered.into_iter().cloned().collect::<Vec<_>>())
        ));
    } else if !facts.history.is_empty() && h.area_history.is_empty() {
        out.push(format!(
            "Record what the history of the files you change shows (`habits.area_history`: one entry per file that has history worth knowing, with the pull requests or commits in `refs`, what they show in `found` and what it changes about this plan in `bearing`). This is their recent history; read further with `github_history` (`commits` with `path`, `blame`, `pull`) where a commit bears on the change:\n{}",
            history_lines(&facts.history)
        ));
    }
    for a in &h.area_history {
        if !a.path.trim().is_empty() && a.found.trim().is_empty() {
            out.push(format!(
                "`habits.area_history` lists `{}` without `found`: say what its history shows.",
                a.path.trim()
            ));
        }
    }

    // Know who it affects.
    let dep = &facts.dependents;
    let missing: Vec<&(String, String, Vec<String>)> = dep
        .other_repos
        .iter()
        .filter(|(repo, _, _)| {
            !h.affected
                .iter()
                .any(|a| a.repo().is_some_and(|r| r.eq_ignore_ascii_case(repo)))
        })
        .collect();
    if !missing.is_empty() {
        let lines: Vec<String> = missing
            .iter()
            .map(|(repo, surface, hits)| {
                format!(
                    "  - {repo} uses `{surface}`:\n{}",
                    hits.iter()
                        .map(|l| format!("    {}", common::truncate_str(l.trim(), 200)))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            })
            .collect();
        out.push(format!(
            "Another of the team's repositories uses what this plan changes. Read each use below (pass `repo` to `read_file_lines`) and account for it in `habits.affected` (`where`: `owner/repo:path`, `uses`, `action`: `update` or `safe`, `why`). `update` needs that repository's tasks (a `## owner/repo` group in tasks.md); `safe` needs the reason it keeps working.\n{}",
            lines.join("\n")
        ));
    }
    let in_repo_named = h
        .affected
        .iter()
        .any(|a| a.repo().is_none() && !a.path().is_empty());
    if !dep.in_repo.is_empty() && !in_repo_named {
        let lines: Vec<String> = dep
            .in_repo
            .iter()
            .take(MAX_IN_REPO)
            .map(|(f, why)| format!("  - `{f}` ({why})"))
            .collect();
        let more = dep.in_repo.len().saturating_sub(MAX_IN_REPO);
        out.push(format!(
            "The code graph shows these files of this repository use what the plan changes{}. Account for them in `habits.affected`: one entry per file that must change (`action`: `update`, with a task in tasks.md) and one per group that keeps working (`action`: `safe`, `why`: for example the signature and return shape are unchanged). Read a caller before calling it safe.\n{}",
            if more > 0 { format!(" ({more} more not listed; `graph_impact` lists all)") } else { String::new() },
            lines.join("\n")
        ));
    }
    if dep.has_other_repos
        && !modified_paths(files, &facts.own_repo).is_empty()
        && h.surfaces.iter().filter_map(|s| searchable(s)).count() == 0
        && h.no_outside_use.trim().is_empty()
    {
        out.push("The team has other repositories connected and this plan changes existing code. List in `habits.surfaces` each name other code may depend on that the change alters (an exported function or type, an API route, a response or event field, a table column, a config key), so each is searched for across those repositories; or say in `habits.no_outside_use` why nothing outside this repository can depend on the change.".to_string());
    }
    for a in &h.affected {
        let w = a.where_.trim();
        if w.is_empty() {
            continue;
        }
        if a.action.trim() == UNCHECKED {
            continue;
        }
        if !a.updates() && !a.is_safe() {
            out.push(format!(
                "`habits.affected` lists `{w}` without a decision: set `action` to `update` or `safe`."
            ));
        } else if a.is_safe() && a.why.trim().is_empty() {
            out.push(format!(
                "`habits.affected` calls `{w}` safe without `why`: say what keeps it working."
            ));
        } else if a.updates() {
            let planned = match a.repo() {
                Some(r) if !r.eq_ignore_ascii_case(&facts.own_repo) => plans_repo(files, r),
                _ => is_task(files, &facts.own_repo, a.path()),
            };
            if !planned {
                out.push(format!(
                    "`habits.affected` says `{w}` is updated, but tasks.md has no task for it. Add the task (under its `## owner/repo` group when it is another repository), or make it `safe` with the reason."
                ));
            }
        }
    }

    // Ask early.
    for q in &h.ask_early {
        if !q.question.trim().is_empty() && q.default.trim().is_empty() {
            out.push(format!(
                "`habits.ask_early` asks \"{}\" without a `default`: say what you build until someone answers, and plan that.",
                common::truncate_str(q.question.trim(), 160)
            ));
        }
    }
    out
}

/// The line an early question becomes in the decision's questions.
pub fn question_line(q: &EarlyQuestion) -> String {
    let question = q.question.trim();
    let mark = if question.ends_with('?') { "" } else { "?" };
    let mut s = format!(
        "{question}{mark} Proposed default: {}. I am building the default now; a different answer changes it",
        q.default.trim().trim_end_matches('.')
    );
    let deps: Vec<&str> = q
        .depends_on
        .iter()
        .map(|d| d.trim())
        .filter(|d| !d.is_empty())
        .take(6)
        .collect();
    if deps.is_empty() {
        s.push('.');
    } else {
        s.push_str(&format!(" in {}.", deps.join(", ")));
    }
    s
}

/// Puts every early question that has a default among the decision's
/// questions, so it is asked on the ticket while the run builds (the run
/// never waits on those) and listed in the pull request. A question already
/// there in the same words is replaced by the line with its default.
pub fn fold_questions(d: &mut FixDecision) {
    for q in d.habits.ask_early.clone() {
        let question = q.question.trim();
        if question.is_empty() || q.default.trim().is_empty() {
            continue;
        }
        let line = question_line(&q);
        if d.questions.iter().any(|x| x.trim() == line) {
            continue;
        }
        let stem = question.trim_end_matches('?').to_ascii_lowercase();
        d.questions
            .retain(|x| !x.to_ascii_lowercase().contains(&stem));
        d.questions.push(line);
    }
}

/// Moves the reproducing test's task to the top of the checklist (of its
/// repository's group, in a plan that spans several), so it is written and
/// committed before the fix. `None` when nothing moves.
pub fn put_repro_first(tasks: &str, test: &str) -> Option<String> {
    let test = test.trim();
    if test.is_empty() {
        return None;
    }
    let lines: Vec<&str> = tasks.lines().collect();
    let is_task_line = |l: &str| l.trim_start().starts_with("- [");
    let at = lines.iter().position(|l| {
        is_task_line(l)
            && planned_files(l)
                .first()
                .is_some_and(|f| same_path(&f.path, test))
    })?;
    // The first task of the same group: back to the group's heading.
    let group_start = lines[..at]
        .iter()
        .rposition(|l| l.trim_start().starts_with("## "))
        .map_or(0, |i| i + 1);
    let first = (group_start..at).find(|i| is_task_line(lines[*i]))?;
    let mut out: Vec<&str> = lines.clone();
    let line = out.remove(at);
    out.insert(first, line);
    let mut s = out.join("\n");
    if tasks.ends_with('\n') {
        s.push('\n');
    }
    Some(s)
}

/// The heading of the section design.md gains.
pub const DESIGN_HEADING: &str = "## How this change is built";

/// What the coder must hold to, from the plan's habits, as a section of
/// design.md: always the plan's own files (the check after the build holds
/// every change to them), and what the plan settled before code.
pub fn design_section(d: &FixDecision) -> String {
    let h = &d.habits;
    let mut out = String::new();
    let r = &h.repro;
    if !r.test.trim().is_empty() {
        out.push_str(&format!(
            "- **Reproduce first.** Write `{}` before any other task and commit it on its own. It must fail on the code as it is{}. Test through code that already exists (the entry point the bug is reported at), so the test builds and fails on its assertion without the fix; never weaken it to make it pass. After the build, every new or changed test is run without the fix: one that passes either way comes back to you.\n",
            r.test.trim(),
            if r.fails_with.trim().is_empty() { String::new() } else { format!(": {}", r.fails_with.trim().trim_end_matches('.')) }
        ));
    } else if !r.not_feasible.trim().is_empty() {
        out.push_str(&format!(
            "- **No failing test for this one:** {}. Proof instead: {}. Record that proof's result before and after the change in your Evidence.\n",
            r.not_feasible.trim().trim_end_matches('.'),
            r.next_best.trim().trim_end_matches('.')
        ));
    }
    for a in h.area_history.iter().filter(|a| !a.found.trim().is_empty()) {
        out.push_str(&format!(
            "- **History of `{}`:** {}{}\n",
            a.path.trim(),
            a.found.trim(),
            if a.bearing.trim().is_empty() {
                String::new()
            } else {
                format!(" For this change: {}", a.bearing.trim())
            }
        ));
    }
    for a in h.affected.iter().filter(|a| a.updates()) {
        out.push_str(&format!(
            "- **Update with it:** `{}`{}.\n",
            a.where_.trim(),
            if a.why.trim().is_empty() {
                String::new()
            } else {
                format!(" ({})", a.why.trim().trim_end_matches('.'))
            }
        ));
    }
    // Only a doubt still among the decision's questions: one the ticket
    // already settles was dropped from them, and is not built as a default.
    let asked = |q: &EarlyQuestion| {
        let stem = q.question.trim().trim_end_matches('?').to_ascii_lowercase();
        !stem.is_empty()
            && !q.default.trim().is_empty()
            && d.questions
                .iter()
                .any(|x| x.to_ascii_lowercase().contains(&stem))
    };
    for q in h.ask_early.iter().filter(|q| asked(q)) {
        out.push_str(&format!(
            "- **Asked on the ticket, build the default:** {} Build: {}. Keep that choice in one place so another answer is a small change.\n",
            q.question.trim(),
            q.default.trim().trim_end_matches('.')
        ));
    }
    out.push_str("- **Stay inside the plan.** Change only the files the tasks name, their tests, and what those need to build. When the change truly needs another file, say so in your Evidence on a line `Outside the plan: <path>: <why the change needs it>`; a changed file with no such line is put back.\n");
    format!("{DESIGN_HEADING}\n{out}")
}

/// design.md with the section, once, for a plan whose tasks name files.
pub fn with_design_section(design: &str, d: &FixDecision, tasks: &str) -> String {
    if planned_files(tasks).is_empty() || design.contains(DESIGN_HEADING) {
        return design.to_string();
    }
    let section = design_section(d);
    format!("{}\n\n{section}", design.trim_end())
}

/// The planner's instructions for the `habits` object, by what the ticket
/// asks for.
pub fn planner_guidance(is_bug: bool) -> String {
    let repro = if is_bug {
        "- **Reproduce first (`repro`).** Before the fix, a test must fail on the code as it is, for the reason the ticket reports. Name that test file in `repro.test` (find the nearest existing test for the code and follow it), say in `fails_with` what it will show, and give the file its own task. Only when no failing test can be written (no test setup reaches this code, a pure configuration change) leave `test` empty, say why in `not_feasible` and name the next best proof in `next_best`: the exact command, query or page that shows the failure before the change and its absence after.\n"
    } else {
        ""
    };
    format!(
        "## Before any code (the `habits` object of the block, every plan that changes code)\n\n{repro}\
- **Read the history (`area_history`).** Before you plan a change to an existing file, read what was done to it: `github_history` with `kind: commits` and `path`, `kind: blame` on the lines you change, and `kind: pull` for a pull request a commit names. Look for the same change tried before, a revert, and the reason a line is the way it is. Record what bears on this plan, with the pull requests or commits in `refs`. A reverted approach is never planned again without saying what differs.\n\
- **Know who it affects (`surfaces`, `affected`).** List in `surfaces` each name other code may depend on that the change alters (an exported function or type, an API route, a response or event field, a table column, a config key). Find its users: `graph_callers` and `graph_impact` in this repository, and `search_code` with `repo` in each of the team's other repositories. Record each user that must change (`action: update`, with a task for it; another repository's under its `## owner/repo` group) and each group that keeps working (`action: safe`, with the reason). When nothing outside the repository can depend on the change, say why in `no_outside_use`.\n\
- **Ask early (`ask_early`).** When the ticket reads two ways that lead to different code, or leaves a product decision open, don't build one reading and list the doubt at the end: write the question with the `default` you propose, plan the default, and list in `depends_on` the files that would change with another answer. The question is asked on the ticket now and the run keeps building. Never ask what the code, the data, the history or a lookup can answer: find it out.\n\n\
Add this to the block:\n\
```\n\
\"habits\": {{\n\
  \"repro\": {{\"test\": \"path of the test file that fails before the fix (bugs)\", \"fails_with\": \"what it shows on the code as it is\", \"not_feasible\": \"why no failing test can be written; empty when test is set\", \"next_best\": \"the next best proof, when there is no test\"}},\n\
  \"area_history\": [{{\"path\": \"a file the plan changes\", \"found\": \"what its pull requests, reverts and blame show\", \"refs\": [\"#123\", \"a1b2c3d\"], \"bearing\": \"what it changes about this plan\"}}],\n\
  \"surfaces\": [\"each name other code may depend on that the change alters\"],\n\
  \"no_outside_use\": \"why nothing outside this repository can depend on the change; empty when surfaces lists any\",\n\
  \"affected\": [{{\"where\": \"owner/repo:path, or a path of this repository\", \"uses\": \"what it uses of the change\", \"action\": \"update | safe\", \"why\": \"the update, or why it keeps working\"}}],\n\
  \"ask_early\": [{{\"question\": \"one decision only a person can make\", \"default\": \"what you build until someone answers\", \"depends_on\": [\"files that would change with another answer\"]}}]\n\
}}\n\
```"
    )
}

/// Whether a path is a test, by the conventions of the common ecosystems.
pub fn is_test_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    let base = p.rsplit('/').next().unwrap_or(&p);
    base.contains(".test.")
        || base.contains(".spec.")
        || base.contains("_test.")
        || base.contains("_spec.")
        || base.starts_with("test_")
        || base.ends_with("test.java")
        || base.ends_with("tests.java")
        || base.ends_with("test.kt")
        || base.ends_with("tests.cs")
        || ["__tests__/", "tests/", "test/", "spec/", "__snapshots__/"]
            .iter()
            .any(|d| p.starts_with(d) || p.contains(&format!("/{d}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::fix_location::{self, EndToEndStep};
    use crate::passes::plan::RepoTasks;
    use crate::passes::triage::TicketIntent;
    use serde_json::json;

    const OWN: &str = "acme/shop-api";

    fn plan(tasks: &str, intent: TicketIntent, habits: Habits) -> PlanResult {
        PlanResult {
            proposal: String::new(),
            design: "## Design\nChange the total.".into(),
            tasks: tasks.into(),
            spec: String::new(),
            repo_tasks: Vec::new(),
            fix: Some(FixDecision {
                intent,
                habits,
                ..Default::default()
            }),
        }
    }

    fn facts() -> Facts {
        Facts {
            own_repo: OWN.into(),
            ..Default::default()
        }
    }

    const TASKS: &str = "- [ ] src/cart/total.ts: [modify] Round per line, not per cart\n- [ ] src/cart/total.test.ts: [modify] A cart with three lines of 0.335\n";

    fn commit(sha: &str, date: &str, message: &str) -> serde_json::Value {
        json!({"sha": sha, "commit": {"message": message, "committer": {"date": date}}})
    }

    #[test]
    fn the_block_reads_habits_and_a_malformed_one_never_costs_the_decision() {
        let reply = r#"```fix_location.json
{"fix_location": "code", "cause": "c", "habits": {
  "repro": {"test": "src/cart/total.test.ts", "fails_with": "total is 1.00, expected 1.01"},
  "area_history": "nothing found",
  "surfaces": ["cartTotal"],
  "affected": [{"where": "acme/storefront:src/checkout.ts", "uses": "total", "action": "safe", "why": "reads the same field"}],
  "ask_early": [{"question": "Round half up or half even?", "default": "half up", "depends_on": ["src/cart/total.ts"]}]
}}
```"#;
        let d = fix_location::parse(reply).expect("a decision");
        assert_eq!(d.habits.repro.test, "src/cart/total.test.ts");
        assert!(
            d.habits.area_history.is_empty(),
            "the wrong shape is left empty"
        );
        assert_eq!(d.habits.surfaces, vec!["cartTotal"]);
        assert_eq!(d.habits.affected[0].repo(), Some("acme/storefront"));
        assert_eq!(d.habits.affected[0].path(), "src/checkout.ts");
        assert_eq!(d.habits.ask_early[0].default, "half up");
        // A decision written before the field existed still reads.
        let old = "```fix_location.json\n{\"fix_location\": \"code\", \"cause\": \"c\"}\n```";
        assert_eq!(fix_location::parse(old).unwrap().habits, Habits::default());
    }

    #[test]
    fn a_bug_plan_names_its_failing_test_or_says_why_there_is_none() {
        let none = plan(TASKS, TicketIntent::Bug, Habits::default());
        let gaps = plan_gaps(&none, &facts());
        assert!(gaps.iter().any(|g| g.contains("habits.repro")), "{gaps:?}");

        let named = plan(
            TASKS,
            TicketIntent::Bug,
            Habits {
                repro: Repro {
                    test: "src/cart/total.test.ts".into(),
                    fails_with: "1.00 instead of 1.01".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        assert!(plan_gaps(&named, &facts()).is_empty());

        let untasked = plan(
            "- [ ] src/cart/total.ts: [modify] Round per line\n",
            TicketIntent::Bug,
            named.fix.as_ref().unwrap().habits.clone(),
        );
        let gaps = plan_gaps(&untasked, &facts());
        assert!(gaps[0].contains("no task for that file"), "{gaps:?}");

        let config = plan(
            "- [ ] config/limits.yml: [modify] Raise the limit\n",
            TicketIntent::Bug,
            Habits {
                repro: Repro {
                    not_feasible: "a value in a YAML file no test loads".into(),
                    next_best: "GET /limits returns 50 before and 100 after".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        assert!(plan_gaps(&config, &facts()).is_empty());
        // Why without a next best proof is half an answer.
        let mut half = config;
        half.fix.as_mut().unwrap().habits.repro.next_best.clear();
        assert_eq!(plan_gaps(&half, &facts()).len(), 1);
    }

    #[test]
    fn new_work_and_plans_without_code_need_no_reproduction() {
        let feature = plan(TASKS, TicketIntent::Feature, Habits::default());
        assert!(plan_gaps(&feature, &facts()).is_empty());
        let stop = plan(
            "CLARIFICATION_NEEDED:\n- question: Which one?",
            TicketIntent::Bug,
            Habits::default(),
        );
        assert!(plan_gaps(&stop, &facts()).is_empty());
        let mut outside = plan("NO_CODE_CHANGE: data", TicketIntent::Bug, Habits::default());
        outside.fix.as_mut().unwrap().fix_location = FixLocation::Unknown;
        assert!(plan_gaps(&outside, &facts()).is_empty());
    }

    #[test]
    fn the_reproducing_test_is_written_first() {
        let moved = put_repro_first(TASKS, "src/cart/total.test.ts").expect("it moves");
        assert!(
            moved.starts_with("- [ ] src/cart/total.test.ts:"),
            "{moved}"
        );
        assert!(moved.ends_with('\n'));
        assert_eq!(moved.lines().count(), 2);
        // Already first, or not a task: nothing moves.
        assert_eq!(put_repro_first(&moved, "src/cart/total.test.ts"), None);
        assert_eq!(put_repro_first(TASKS, "src/other.test.ts"), None);
        // In a plan that spans repositories it stays in its own group.
        let multi = "## acme/shop-api\n- [ ] a.ts: [modify] x\n- [ ] a.test.ts: [create] y\n## acme/storefront\n- [ ] b.ts: [modify] z\n- [ ] b.test.ts: [create] w\n";
        let moved = put_repro_first(multi, "b.test.ts").unwrap();
        let lines: Vec<&str> = moved.lines().collect();
        assert_eq!(lines[1], "- [ ] a.ts: [modify] x");
        assert_eq!(lines[4], "- [ ] b.test.ts: [create] w");
    }

    #[test]
    fn a_files_history_reads_its_reverts_and_pull_requests() {
        let commits = vec![
            commit(
                "aaaaaaa1111",
                "2026-03-04T10:00:00Z",
                "Cache totals per cart (#88)",
            ),
            commit(
                "bbbbbbb2222",
                "2026-02-11T09:00:00Z",
                "Revert \"Round per line (#71)\" (#74)\n\nThis reverts commit ccccccc.",
            ),
            commit(
                "ccccccc3333",
                "2026-02-10T09:00:00Z",
                "Round per line (#71)",
            ),
        ];
        let h = file_history("src/cart/total.ts", &commits);
        assert_eq!(h.commits, 3);
        assert_eq!(h.prs, vec![88, 71, 74]);
        assert_eq!(h.reverts.len(), 1);
        assert_eq!(h.reverts[0].sha, "bbbbbbb");
        assert_eq!(h.reverts[0].date, "2026-02-11");
        assert!(is_revert("Back out the retry change"));
        assert!(!is_revert("Add a revert button to the editor"));
    }

    fn with_history() -> Facts {
        Facts {
            own_repo: OWN.into(),
            history: vec![file_history(
                "src/cart/total.ts",
                &[
                    commit(
                        "bbbbbbb2222",
                        "2026-02-11T09:00:00Z",
                        "Revert \"Round per line (#71)\" (#74)",
                    ),
                    commit(
                        "ccccccc3333",
                        "2026-02-10T09:00:00Z",
                        "Round per line (#71)",
                    ),
                ],
            )],
            ..Default::default()
        }
    }

    #[test]
    fn a_revert_on_a_planned_file_must_be_answered_by_name() {
        let silent = plan(TASKS, TicketIntent::Feature, Habits::default());
        let gaps = plan_gaps(&silent, &with_history());
        assert_eq!(gaps.len(), 1);
        assert!(gaps[0].contains("REVERT bbbbbbb 2026-02-11"), "{}", gaps[0]);
        assert!(gaps[0].contains("taken back out"));

        // An entry for the file that doesn't name the revert doesn't answer it.
        let vague = plan(
            TASKS,
            TicketIntent::Feature,
            Habits {
                area_history: vec![AreaHistory {
                    path: "src/cart/total.ts".into(),
                    found: "Touched a few times this year.".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        assert_eq!(plan_gaps(&vague, &with_history()).len(), 1);

        let answered = plan(
            TASKS,
            TicketIntent::Feature,
            Habits {
                area_history: vec![AreaHistory {
                    path: "src/cart/total.ts".into(),
                    found: "Per-line rounding shipped in #71 and was reverted in #74: invoices no longer matched the ledger export.".into(),
                    refs: vec!["#71".into(), "#74".into()],
                    bearing: "This plan rounds per line and updates the export in the same change.".into(),
                }],
                ..Default::default()
            },
        );
        assert!(plan_gaps(&answered, &with_history()).is_empty());
    }

    #[test]
    fn history_without_a_revert_is_handed_to_the_planner_once() {
        let mut f = with_history();
        f.history[0].reverts.clear();
        let silent = plan(TASKS, TicketIntent::Feature, Habits::default());
        let gaps = plan_gaps(&silent, &f);
        assert_eq!(gaps.len(), 1);
        assert!(gaps[0].contains("`src/cart/total.ts`: 2 recent commit(s), pull requests #71, #74"));
    }

    fn uses_elsewhere() -> Facts {
        Facts {
            own_repo: OWN.into(),
            dependents: Dependents {
                in_repo: vec![("src/invoice/pdf.ts".into(), "references `cartTotal`".into())],
                other_repos: vec![(
                    "acme/storefront".into(),
                    "cartTotal".into(),
                    vec!["src/checkout.ts:41: const total = cart.cartTotal.amount".into()],
                )],
                has_other_repos: true,
            },
            ..Default::default()
        }
    }

    #[test]
    fn every_dependent_the_worker_found_is_accounted_for() {
        let h = Habits {
            surfaces: vec!["cartTotal".into()],
            ..Default::default()
        };
        let silent = plan(TASKS, TicketIntent::Feature, h.clone());
        let gaps = plan_gaps(&silent, &uses_elsewhere());
        assert_eq!(gaps.len(), 2, "{gaps:?}");
        assert!(gaps[0].contains("acme/storefront uses `cartTotal`"));
        assert!(gaps[0].contains("src/checkout.ts:41"));
        assert!(gaps[1].contains("`src/invoice/pdf.ts` (references `cartTotal`)"));

        // Safe needs its reason; update needs its task.
        let mut loose = h.clone();
        loose.affected = vec![
            Affected {
                where_: "acme/storefront:src/checkout.ts".into(),
                action: "update".into(),
                ..Default::default()
            },
            Affected {
                where_: "src/invoice/pdf.ts".into(),
                action: "safe".into(),
                ..Default::default()
            },
        ];
        let gaps = plan_gaps(
            &plan(TASKS, TicketIntent::Feature, loose.clone()),
            &uses_elsewhere(),
        );
        assert_eq!(gaps.len(), 2, "{gaps:?}");
        assert!(gaps[0].contains("no task for it"));
        assert!(gaps[1].contains("safe without `why`"));

        loose.affected[1].why = "reads `amount`, which keeps its type".into();
        let mut settled = plan(TASKS, TicketIntent::Feature, loose);
        settled.repo_tasks = vec![
            RepoTasks {
                owner: "acme".into(),
                name: "shop-api".into(),
                tasks: TASKS.into(),
            },
            RepoTasks {
                owner: "acme".into(),
                name: "storefront".into(),
                tasks: "- [ ] src/checkout.ts: [modify] Read the per-line total".into(),
            },
        ];
        assert!(plan_gaps(&settled, &uses_elsewhere()).is_empty());
        // An end-to-end step in that repository plans it too.
        settled.repo_tasks.clear();
        settled.fix.as_mut().unwrap().end_to_end = vec![EndToEndStep {
            step: "Storefront reads the new total".into(),
            by: "other_repo".into(),
            repo: "acme/storefront".into(),
            ..Default::default()
        }];
        assert!(plan_gaps(&settled, &uses_elsewhere()).is_empty());
    }

    #[test]
    fn a_team_with_other_repositories_gets_the_changed_names_or_the_reason_there_are_none() {
        let f = Facts {
            own_repo: OWN.into(),
            dependents: Dependents {
                has_other_repos: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let silent = plan(TASKS, TicketIntent::Feature, Habits::default());
        assert!(plan_gaps(&silent, &f)[0].contains("habits.surfaces"));
        let internal = plan(
            TASKS,
            TicketIntent::Feature,
            Habits {
                no_outside_use: "a private helper of one module".into(),
                ..Default::default()
            },
        );
        assert!(plan_gaps(&internal, &f).is_empty());
        // A plan that only adds files changes nothing others use.
        let adds = plan(
            "- [ ] src/new.ts: [create] A new helper\n",
            TicketIntent::Feature,
            Habits::default(),
        );
        assert!(plan_gaps(&adds, &f).is_empty());
        assert_eq!(searchable(" `id` "), None, "too short to mean a use");
    }

    #[test]
    fn an_early_question_carries_its_default_and_is_asked_once() {
        let q = EarlyQuestion {
            question: "Round half up or half even".into(),
            default: "half up, as the invoice module does.".into(),
            depends_on: vec!["src/cart/total.ts".into()],
        };
        let mut d = FixDecision {
            questions: vec![
                "Round half up or half even?".into(),
                "Which currency?".into(),
            ],
            habits: Habits {
                ask_early: vec![q.clone()],
                ..Default::default()
            },
            ..Default::default()
        };
        fold_questions(&mut d);
        fold_questions(&mut d);
        assert_eq!(d.questions.len(), 2, "{:?}", d.questions);
        assert_eq!(d.questions[0], "Which currency?");
        assert_eq!(
            d.questions[1],
            "Round half up or half even? Proposed default: half up, as the invoice module does. I am building the default now; a different answer changes it in src/cart/total.ts."
        );
        // The line is fit to ask: it names what was considered and recommended.
        let asked = common::run_questions::from_lines(&d.questions);
        assert!(common::run_questions::too_open(&asked[1]).is_none());

        let bare = plan(
            TASKS,
            TicketIntent::Feature,
            Habits {
                ask_early: vec![EarlyQuestion {
                    question: "Which rounding?".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        assert!(plan_gaps(&bare, &facts())[0].contains("without a `default`"));
        let mut d = bare.fix.unwrap();
        fold_questions(&mut d);
        assert!(
            d.questions.is_empty(),
            "a question without a default isn't asked as one"
        );
    }

    #[test]
    fn the_coder_reads_the_habits_in_the_design() {
        let d = FixDecision {
            habits: Habits {
                repro: Repro {
                    test: "src/cart/total.test.ts".into(),
                    fails_with: "1.00, expected 1.01".into(),
                    ..Default::default()
                },
                affected: vec![Affected {
                    where_: "src/invoice/pdf.ts".into(),
                    action: "update".into(),
                    why: "prints the total".into(),
                    ..Default::default()
                }],
                ask_early: vec![EarlyQuestion {
                    question: "Half up or half even?".into(),
                    default: "half up".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        let mut d = d;
        fold_questions(&mut d);
        let design = with_design_section("## Design\nx\n", &d, TASKS);
        assert!(design.contains(DESIGN_HEADING));
        assert!(design.contains("Write `src/cart/total.test.ts` before any other task"));
        assert!(design.contains("**Update with it:** `src/invoice/pdf.ts` (prints the total)"));
        assert!(design.contains("build the default"));
        assert!(design.contains("Outside the plan: <path>"));
        assert_eq!(
            with_design_section(&design, &d, TASKS),
            design,
            "added once"
        );
        // A plan with no habits still tells the coder the scope it is held to;
        // a plan that names no files has none.
        let bare = with_design_section("d", &FixDecision::default(), TASKS);
        assert!(bare.contains("Outside the plan: <path>"), "{bare}");
        assert_eq!(
            with_design_section("d", &FixDecision::default(), "NO_CODE_CHANGE: data"),
            "d"
        );
    }

    #[test]
    fn the_planner_is_told_each_habit_and_only_a_bug_reproduces() {
        let bug = planner_guidance(true);
        for field in [
            "repro",
            "area_history",
            "surfaces",
            "affected",
            "ask_early",
            "no_outside_use",
        ] {
            assert!(bug.contains(&format!("\"{field}\"")), "{field}");
        }
        assert!(bug.contains("Reproduce first"));
        assert!(!planner_guidance(false).contains("Reproduce first"));
        // The example in the guidance is the shape the block reads.
        assert!(fix_location::planner_guidance(TicketIntent::Bug).contains("\"habits\""));
    }

    #[test]
    fn tests_are_told_from_code_across_ecosystems() {
        for p in [
            "src/cart/total.test.ts",
            "web/__tests__/cart.tsx",
            "pkg/cart/total_test.go",
            "tests/test_total.py",
            "app/test_total.py",
            "spec/models/cart_spec.rb",
            "src/test/java/CartTest.java",
        ] {
            assert!(is_test_path(p), "{p}");
        }
        for p in [
            "src/cart/total.ts",
            "src/contest/latest.ts",
            "pkg/attest/sign.go",
        ] {
            assert!(!is_test_path(p), "{p}");
        }
    }
}
