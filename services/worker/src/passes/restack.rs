//! A branch's own change rebuilt on another branch, file by file.
//!
//! A run stacked on an open pull request has to move when that pull
//! request's branch is rebased: its branch still holds the old commits,
//! which now read as copies under other hashes. Replaying the branch's own
//! commits one by one stopped at a commit count, and merging the other
//! branch in first needed a merge that could fail; both stopped the run.
//!
//! Here no commit is replayed. The change is what the branch's own commits
//! (not the copies, not merge commits) touched, as files: each path with its
//! final content on the branch, or its removal (a rename is both). One new
//! commit on the other branch's head holds that branch's tree with those
//! paths laid over it, so the commit count never matters and nothing has to
//! be merged first.
//!
//! A path the other branch also changed since the point the run copied
//! from is a conflict: the model merges the two versions (both sides' work
//! kept). When it can't, the commit keeps the other branch's version and a
//! labelled follow-up commit puts this change's back, so the pull request
//! still opens, as a draft that names the path.
//!
//! Before the branch moves, the result is checked: every path of the change
//! holds the change's (or the merged) content and every other path equals
//! the other branch's head. It moves with a lease (only if nobody pushed
//! since; when someone did, it is rebuilt from the new head). GitHub errors
//! are retried with backoff; only a missing permission stops, naming it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::time::Duration;

use serde_json::Value;
use tracing::{info, warn};

use crate::clients::github::{CommitFile, GitEntry, GitHubClient};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A path's entry in a tree; `None` when the path isn't there.
pub type Slot = Option<GitEntry>;

/// A commit of a compare's list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub sha: String,
    pub parents: Vec<String>,
    pub message: String,
}

impl Listed {
    pub fn from_json(c: &Value) -> Option<Listed> {
        Some(Listed {
            sha: c["sha"].as_str()?.to_string(),
            parents: c["parents"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|p| p["sha"].as_str().map(String::from))
                .collect(),
            message: c
                .pointer("/commit/message")
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_string(),
        })
    }
}

/// What the rebuild reads and writes on the repository.
#[async_trait::async_trait]
pub trait Repo: Sync {
    async fn head(&self, branch: &str) -> Result<String, Error>;
    /// A commit's tree and parents.
    async fn commit(&self, sha: &str) -> Result<(String, Vec<String>), Error>;
    /// Every commit in `base...head`, oldest first.
    async fn between(&self, base: &str, head: &str) -> Result<Vec<Listed>, Error>;
    /// The files a (non-merge) commit changed.
    async fn files(&self, sha: &str) -> Result<Vec<CommitFile>, Error>;
    /// Each path's entry at a commit.
    async fn slots(
        &self,
        commit: &str,
        paths: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, Slot>, Error>;
    async fn blob(&self, sha: &str) -> Result<Vec<u8>, Error>;
    async fn put_blob(&self, bytes: &[u8]) -> Result<String, Error>;
    async fn put_tree(&self, base_tree: &str, changes: &[(String, Slot)]) -> Result<String, Error>;
    async fn put_commit(
        &self,
        message: &str,
        tree: &str,
        parents: &[String],
    ) -> Result<String, Error>;
    /// Moves the branch to `to` only if it still points at `lease`.
    async fn move_branch(&self, branch: &str, lease: &str, to: &str) -> Result<bool, Error>;
    /// The paths that differ between two commits; `None` when there are
    /// more than GitHub lists.
    async fn changed(&self, from: &str, to: &str) -> Result<Option<Vec<String>>, Error>;
}

/// Merges two versions of a file, keeping both sides' work.
#[async_trait::async_trait]
pub trait Merger: Send {
    /// `theirs` is the other branch's version, `ours` the change's; `None`
    /// when they can't be merged safely.
    async fn merge(
        &mut self,
        path: &str,
        ancestor: Option<&str>,
        theirs: &str,
        ours: &str,
    ) -> Option<String>;
}

/// No model at hand: every conflict is kept as a follow-up commit.
pub struct NoMerger;

#[async_trait::async_trait]
impl Merger for NoMerger {
    async fn merge(&mut self, _: &str, _: Option<&str>, _: &str, _: &str) -> Option<String> {
        None
    }
}

/// The model merges a conflicting path (`pr::merge_file`: validated, an
/// invalid merge is `None`).
pub struct ModelMerger<'a> {
    pub state: &'a crate::WorkerState,
    pub provider: &'a crate::agent::provider::ModelProvider,
    pub usage: &'a mut crate::models::TokenUsage,
    /// What the other branch is, for the prompt ("pull request #42's branch").
    pub theirs: String,
}

#[async_trait::async_trait]
impl Merger for ModelMerger<'_> {
    async fn merge(
        &mut self,
        path: &str,
        ancestor: Option<&str>,
        theirs: &str,
        ours: &str,
    ) -> Option<String> {
        match super::pr::merge_file(
            self.state,
            self.provider,
            self.usage,
            path,
            ancestor,
            &self.theirs,
            theirs,
            ours,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                warn!(path, error = %e, "The model couldn't merge the two versions");
                None
            }
        }
    }
}

/// Which of the branch's commits are its own change.
#[derive(Debug, Clone, Copy)]
pub enum Own<'a> {
    /// Its commits past `base`, the copies of the other branch's commits
    /// left out.
    NotCopies { base: &'a str, copies: &'a [String] },
    /// Its commits past `from`, the other branch's old head it was built on
    /// (that branch was force-pushed since).
    After { from: &'a str },
}

/// One rebuild: `branch`'s own change onto `onto`'s head.
pub struct Job<'a> {
    pub branch: &'a str,
    pub onto: &'a str,
    pub own: Own<'a>,
    /// The new commit's message.
    pub message: &'a str,
    /// The other branch's pull request, for the follow-up commit's message.
    pub onto_pr: u64,
}

/// What a rebuild did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Rebuilt {
    /// The branch's new head.
    pub tip: String,
    /// The branch's own commits the change came from.
    pub own_commits: usize,
    /// The paths of the change.
    pub paths: usize,
    /// Paths both changed, merged by the model.
    pub merged: Vec<String>,
    /// Paths both changed that couldn't be merged: the other branch's
    /// version, then this change's in a labelled follow-up commit.
    pub unresolved: Vec<String>,
    /// How many times it was built (more than one when the branch moved).
    pub tries: u32,
}

/// Why a rebuild didn't move the branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Halt {
    /// The GitHub App lacks a permission: the only reason a run stops.
    Permission { permission: String, doing: String },
    /// Anything else (after retries); the pull request still opens.
    Failed(String),
}

impl Halt {
    /// In plain words, with what to do for a missing permission.
    pub fn say(&self, repo: &str) -> String {
        match self {
            Halt::Permission { permission, doing } => format!(
                "CoderHelm's GitHub App needs the \"{permission}\" permission on {repo} for \
                 {doing}. An organization owner can grant it in the GitHub App's settings \
                 (Permissions); run the ticket again after that."
            ),
            Halt::Failed(why) => why.clone(),
        }
    }
}

const READ: &str = "Contents: read";
const WRITE: &str = "Contents: read and write";

/// Tries per GitHub call; the waits between double from [`BACKOFF_MS`].
const TRIES: u32 = 4;
#[cfg(not(test))]
const BACKOFF_MS: u64 = 1_000;
#[cfg(test)]
const BACKOFF_MS: u64 = 0;
/// Builds of one rebuild when the branch keeps moving under it.
pub const LEASE_TRIES: u32 = 3;

/// The permission a failed call lacked, from GitHub's answer: a workflow
/// file needs "Workflows: read and write"; any other refusal for a missing
/// permission needs `step`'s. A rate limit is not a permission.
pub fn permission_needed(err: &str, step: &str) -> Option<String> {
    let l = err.to_ascii_lowercase();
    if l.contains("rate limit") {
        return None;
    }
    if l.contains("workflow") && (l.contains("permission") || l.contains("scope")) {
        return Some("Workflows: read and write".to_string());
    }
    let forbidden = l.contains("(403") || l.contains(" 403 ");
    if forbidden && (l.contains("resource not accessible") || l.contains("permission")) {
        return Some(step.to_string());
    }
    None
}

/// One GitHub call, retried with backoff; a missing permission stops at
/// once.
pub async fn retry<T, F, Fut>(permission: &str, doing: &str, mut call: F) -> Result<T, Halt>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut last = String::new();
    for i in 0..TRIES {
        match call().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                last = e.to_string();
                if let Some(p) = permission_needed(&last, permission) {
                    return Err(Halt::Permission {
                        permission: p,
                        doing: doing.to_string(),
                    });
                }
                if i + 1 < TRIES {
                    tokio::time::sleep(Duration::from_millis(BACKOFF_MS << i)).await;
                }
            }
        }
    }
    Err(Halt::Failed(format!(
        "{doing} kept failing on GitHub ({})",
        common::truncate_str(&last, 300)
    )))
}

/// The branch's own commits: neither merge commits nor copies.
pub fn own_commits<'a>(listed: &'a [Listed], copies: &[String]) -> Vec<&'a Listed> {
    listed
        .iter()
        .filter(|c| c.parents.len() <= 1 && !copies.contains(&c.sha))
        .collect()
}

/// The paths a commit's files touch: a rename is its new path (added) and
/// its old one (removed).
pub fn touched(files: &[CommitFile]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for f in files {
        if !f.path.is_empty() {
            out.insert(f.path.clone());
        }
        if let Some(p) = f.previous.as_ref().filter(|p| !p.is_empty()) {
            out.insert(p.clone());
        }
    }
    out
}

/// What each path of the change becomes on the other branch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// Paths whose version is the change's (`None` removes the path).
    pub take: Vec<(String, Slot)>,
    /// Paths both changed since the point the change was copied from.
    pub conflicts: Vec<String>,
}

fn at(m: &BTreeMap<String, Slot>, p: &str) -> Slot {
    m.get(p).cloned().flatten()
}

/// Per path: the same on both is nothing to do; the other branch left it
/// as it was at `base` (the point the run copied from), so the change's
/// version wins; the change's net result is `base`'s, so the other
/// branch's stays; both changed it: a conflict.
pub fn plan(
    paths: &BTreeSet<String>,
    ours: &BTreeMap<String, Slot>,
    theirs: &BTreeMap<String, Slot>,
    base: &BTreeMap<String, Slot>,
) -> Plan {
    let mut out = Plan::default();
    for p in paths {
        let (o, t, b) = (at(ours, p), at(theirs, p), at(base, p));
        if o == t || o == b {
            continue;
        }
        if t == b {
            out.take.push((p.clone(), o));
        } else {
            out.conflicts.push(p.clone());
        }
    }
    out
}

/// The check before the branch moves: each path of the change holds what
/// it should, and nothing else differs from the other branch's head
/// (`changed`, when GitHub could list it all).
pub fn holds(
    expect: &BTreeMap<String, Slot>,
    got: &BTreeMap<String, Slot>,
    changed: Option<&[String]>,
    paths: &BTreeSet<String>,
) -> Result<(), String> {
    for (p, e) in expect {
        if at(got, p) != *e {
            return Err(format!("`{p}` doesn't hold the expected version"));
        }
    }
    if let Some(x) = changed.and_then(|c| c.iter().find(|f| !paths.contains(*f))) {
        return Err(format!(
            "`{x}` differs from the other branch though this change never touched it"
        ));
    }
    Ok(())
}

/// The follow-up commit's message: the paths this change's version comes
/// back to, for review.
pub fn follow_up_message(paths: &[String], onto_pr: u64) -> String {
    let mut m = format!(
        "Follow-up: this change's version of {} file(s) #{onto_pr} also changed\n\n\
         These couldn't be merged with #{onto_pr}'s version: the commit before this one keeps \
         #{onto_pr}'s, this one puts this change's back. Review them before merging:\n",
        paths.len()
    );
    for p in paths {
        m.push_str(&format!("- {p}\n"));
    }
    m.trim_end().to_string()
}

enum Attempt {
    Done(Rebuilt),
    Moved,
}

/// Rebuilds `job.branch`'s own change on `job.onto`'s head and moves the
/// branch there; built again from the new head when the branch moves
/// meanwhile, at most [`LEASE_TRIES`] times.
pub async fn rebuild<R: Repo, M: Merger>(
    repo: &R,
    merger: &mut M,
    job: &Job<'_>,
) -> Result<Rebuilt, Halt> {
    for n in 1..=LEASE_TRIES {
        match attempt(repo, merger, job).await? {
            Attempt::Done(mut r) => {
                r.tries = n;
                return Ok(r);
            }
            Attempt::Moved => {
                info!(
                    branch = job.branch,
                    n, "The branch moved while it was rebuilt; building again"
                );
            }
        }
    }
    Err(Halt::Failed(format!(
        "`{}` kept moving while it was being rebuilt ({LEASE_TRIES} tries)",
        job.branch
    )))
}

async fn attempt<R: Repo, M: Merger>(
    repo: &R,
    merger: &mut M,
    job: &Job<'_>,
) -> Result<Attempt, Halt> {
    let lease = retry(READ, "reading the branch's head", || repo.head(job.branch)).await?;
    let dep = retry(READ, "reading the other branch's head", || {
        repo.head(job.onto)
    })
    .await?;
    let (dep_tree, _) = retry(READ, "reading the other branch's head", || {
        repo.commit(&dep)
    })
    .await?;
    let (from, copies): (&str, &[String]) = match job.own {
        Own::NotCopies { base, copies } => (base, copies),
        Own::After { from } => (from, &[]),
    };
    let listed = retry(READ, "listing the branch's commits", || {
        repo.between(from, &lease)
    })
    .await?;
    let own = own_commits(&listed, copies);
    let Some(first) = own.first() else {
        return Err(Halt::Failed(
            "the branch has no commits of its own".to_string(),
        ));
    };
    // The point the change was copied from: the other branch's old head,
    // or the parent of the branch's first own commit.
    let start = match job.own {
        Own::After { from } => from.to_string(),
        Own::NotCopies { .. } => match first.parents.first() {
            Some(p) => p.clone(),
            None => from.to_string(),
        },
    };
    let mut paths = BTreeSet::new();
    for c in &own {
        let files = retry(READ, "reading a commit's files", || repo.files(&c.sha)).await?;
        paths.extend(touched(&files));
    }
    let ours = retry(READ, "reading the branch's files", || {
        repo.slots(&lease, &paths)
    })
    .await?;
    let theirs = retry(READ, "reading the other branch's files", || {
        repo.slots(&dep, &paths)
    })
    .await?;
    let base = retry(READ, "reading the files the change started from", || {
        repo.slots(&start, &paths)
    })
    .await?;
    let planned = plan(&paths, &ours, &theirs, &base);

    let mut overlay = planned.take.clone();
    let mut merged = Vec::new();
    let mut unresolved = Vec::new();
    for p in &planned.conflicts {
        let entry = resolve(
            repo,
            merger,
            p,
            &at(&base, p),
            &at(&theirs, p),
            &at(&ours, p),
        )
        .await?;
        match entry {
            Some(e) => {
                overlay.push((p.clone(), Some(e)));
                merged.push(p.clone());
            }
            None => unresolved.push(p.clone()),
        }
    }

    let tree = if overlay.is_empty() {
        dep_tree.clone()
    } else {
        retry(WRITE, "writing the rebuilt files", || {
            repo.put_tree(&dep_tree, &overlay)
        })
        .await?
    };
    let parents = vec![dep.clone()];
    let mut tip = retry(WRITE, "writing the rebuilt commit", || {
        repo.put_commit(job.message, &tree, &parents)
    })
    .await?;
    if !unresolved.is_empty() {
        let back: Vec<(String, Slot)> = unresolved
            .iter()
            .map(|p| (p.clone(), at(&ours, p)))
            .collect();
        let tree2 = retry(WRITE, "writing the follow-up files", || {
            repo.put_tree(&tree, &back)
        })
        .await?;
        let message = follow_up_message(&unresolved, job.onto_pr);
        let parents = vec![tip.clone()];
        tip = retry(WRITE, "writing the follow-up commit", || {
            repo.put_commit(&message, &tree2, &parents)
        })
        .await?;
    }

    // The check: what each path should hold now.
    let set: BTreeMap<String, Slot> = overlay.iter().cloned().collect();
    let expect: BTreeMap<String, Slot> = paths
        .iter()
        .map(|p| {
            let e = if unresolved.contains(p) {
                at(&ours, p)
            } else if let Some(e) = set.get(p) {
                e.clone()
            } else {
                at(&theirs, p)
            };
            (p.clone(), e)
        })
        .collect();
    let got = retry(READ, "reading the rebuilt files", || {
        repo.slots(&tip, &paths)
    })
    .await?;
    let changed = retry(READ, "comparing the rebuilt branch", || {
        repo.changed(&dep, &tip)
    })
    .await?;
    if let Err(why) = holds(&expect, &got, changed.as_deref(), &paths) {
        return Err(Halt::Failed(format!(
            "the rebuilt branch failed its check ({why}); the branch wasn't moved"
        )));
    }
    let moved = retry(WRITE, "moving the branch", || {
        repo.move_branch(job.branch, &lease, &tip)
    })
    .await?;
    if !moved {
        return Ok(Attempt::Moved);
    }
    Ok(Attempt::Done(Rebuilt {
        tip,
        own_commits: own.len(),
        paths: paths.len(),
        merged,
        unresolved,
        tries: 0,
    }))
}

/// A conflicting path merged by the model: both versions must be text
/// files (a removal, a binary file or a submodule can't be merged).
async fn resolve<R: Repo, M: Merger>(
    repo: &R,
    merger: &mut M,
    path: &str,
    base: &Slot,
    theirs: &Slot,
    ours: &Slot,
) -> Result<Option<GitEntry>, Halt> {
    let (Some(t), Some(o)) = (theirs, ours) else {
        return Ok(None);
    };
    if t.kind != "blob" || o.kind != "blob" {
        return Ok(None);
    }
    let text = |b: Vec<u8>| String::from_utf8(b).ok();
    let Some(their_text) = text(retry(READ, "reading a file", || repo.blob(&t.sha)).await?) else {
        return Ok(None);
    };
    let Some(our_text) = text(retry(READ, "reading a file", || repo.blob(&o.sha)).await?) else {
        return Ok(None);
    };
    let ancestor = match base {
        Some(b) if b.kind == "blob" => {
            text(retry(READ, "reading a file", || repo.blob(&b.sha)).await?)
        }
        _ => None,
    };
    let Some(m) = merger
        .merge(path, ancestor.as_deref(), &their_text, &our_text)
        .await
    else {
        return Ok(None);
    };
    let sha = retry(WRITE, "writing a merged file", || {
        repo.put_blob(m.as_bytes())
    })
    .await?;
    Ok(Some(GitEntry {
        mode: o.mode.clone(),
        kind: "blob".to_string(),
        sha,
    }))
}

// ── Pull requests stacked on a force-pushed branch ─────────────────────────

/// Most stacked pull requests one force push rebuilds (one at a time).
pub const MAX_DEPENDENTS: usize = 5;

fn same_repo(p: &Value, owner: &str, repo: &str) -> bool {
    p["head"]["repo"]["full_name"]
        .as_str()
        .is_some_and(|f| f.eq_ignore_ascii_case(&format!("{owner}/{repo}")))
}

/// CoderHelm's open pull requests (from this repository) whose base is
/// `branch`, at most [`MAX_DEPENDENTS`].
pub fn dependents<'a>(open: &'a [Value], owner: &str, repo: &str, branch: &str) -> Vec<&'a Value> {
    open.iter()
        .filter(|p| p["base"]["ref"].as_str() == Some(branch) && same_repo(p, owner, repo))
        .filter(|p| {
            p["user"]["login"]
                .as_str()
                .is_some_and(|l| l.to_ascii_lowercase().contains("coderhelm"))
                || p["head"]["ref"]
                    .as_str()
                    .is_some_and(|b| b.starts_with("coderhelm/"))
        })
        .take(MAX_DEPENDENTS)
        .collect()
}

/// The rebuilt commit's message: the stacked pull request's first own
/// commit's subject (already in the team's convention), else its title.
pub fn restacked_message(subject: &str, dep: u64, branch: &str) -> String {
    format!(
        "{}\n\nRebuilt on #{dep} (`{branch}`) file by file after it was force-pushed.",
        subject.trim()
    )
}

/// A force push to `msg.branch`: each of CoderHelm's open pull requests
/// stacked on it gets its own change (its commits past `msg.before`, the
/// old head) rebuilt on the new head, one at a time, and its stack section
/// says what happened. One that can't be rebuilt now says so and is
/// rebuilt when its ticket runs again (`stack::at_start`).
pub async fn after_forced_push(
    state: &crate::WorkerState,
    msg: crate::models::RestackMessage,
) -> Result<(), Error> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let (owner, repo) = (msg.repo_owner.as_str(), msg.repo_name.as_str());
    let open = github.list_open_pulls(owner, repo).await?;
    let open = open.as_array().cloned().unwrap_or_default();
    let stacked = dependents(&open, owner, repo, &msg.branch);
    // Only an open pull request's branch is something to stack on.
    let Some(dep) = open.iter().find(|p| {
        p["head"]["ref"].as_str() == Some(msg.branch.as_str()) && same_repo(p, owner, repo)
    }) else {
        return Ok(());
    };
    if stacked.is_empty() {
        return Ok(());
    }
    let dep_number = dep["number"].as_u64().unwrap_or(0);
    let dep_title = dep["title"].as_str().unwrap_or("").to_string();
    let provider = crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await
    .ok();
    let mut usage = crate::models::TokenUsage::default();
    let known = BTreeMap::new();
    let gh = Gh {
        github: &github,
        owner,
        repo,
        known: &known,
    };
    for p in stacked {
        let (Some(number), Some(head)) = (p["number"].as_u64(), p["head"]["ref"].as_str()) else {
            continue;
        };
        // Already built on the new head.
        if matches!(
            github
                .compare_status(owner, repo, &msg.branch, head)
                .await
                .as_deref(),
            Ok("ahead") | Ok("identical")
        ) {
            continue;
        }
        let subject = github
            .compare_all_commits(owner, repo, &msg.before, head)
            .await
            .ok()
            .and_then(|c| {
                c.iter()
                    .filter_map(Listed::from_json)
                    .find(|l| l.parents.len() <= 1)
                    .and_then(|l| l.message.lines().next().map(String::from))
            })
            .unwrap_or_else(|| {
                p["title"]
                    .as_str()
                    .unwrap_or("Rebuild this change")
                    .to_string()
            });
        let message = restacked_message(&subject, dep_number, &msg.branch);
        let job = Job {
            branch: head,
            onto: &msg.branch,
            own: Own::After { from: &msg.before },
            message: &message,
            onto_pr: dep_number,
        };
        let result = match &provider {
            Some(pv) => {
                let mut m = ModelMerger {
                    state,
                    provider: pv,
                    usage: &mut usage,
                    theirs: format!("pull request #{dep_number}'s branch"),
                };
                rebuild(&gh, &mut m, &job).await
            }
            None => rebuild(&gh, &mut NoMerger, &job).await,
        };
        let base = super::stack::Dependency {
            number: dep_number,
            branch: msg.branch.clone(),
            title: dep_title.clone(),
            ..Default::default()
        };
        let stated = match result {
            Ok(r) => {
                info!(
                    pr = number,
                    dep = dep_number,
                    tries = r.tries,
                    "Rebuilt a stacked pull request on its base's new head"
                );
                let mut why = format!(
                    "#{dep_number} was force-pushed; this pull request's own change ({} commit(s), \
                     {} file(s)) was rebuilt on its new head file by file.",
                    r.own_commits, r.paths
                );
                if !r.merged.is_empty() {
                    let list: Vec<String> = r.merged.iter().map(|p| format!("`{p}`")).collect();
                    why.push_str(&format!(
                        " #{dep_number} had also changed {}; the two versions were merged, keeping both.",
                        list.join(", ")
                    ));
                }
                super::stack::Dependency {
                    why,
                    unresolved: r.unresolved,
                    ..base
                }
            }
            Err(h) => {
                warn!(pr = number, dep = dep_number, why = %h.say(&format!("{owner}/{repo}")), "Couldn't rebuild a stacked pull request");
                super::stack::Dependency {
                    why: format!(
                        "#{dep_number} was force-pushed and this pull request's change couldn't be \
                         rebuilt on its new head yet ({}).",
                        h.say(&format!("{owner}/{repo}"))
                    ),
                    not_stacked: true,
                    ..base
                }
            }
        };
        super::stack::note_pr(&github, owner, repo, number, &stated).await;
    }
    Ok(())
}

// ── The repository on GitHub ────────────────────────────────────────────────

/// [`Repo`] on GitHub. `known` holds commits' files already read (the
/// copy check reads every commit's).
pub struct Gh<'a> {
    pub github: &'a GitHubClient,
    pub owner: &'a str,
    pub repo: &'a str,
    pub known: &'a BTreeMap<String, Vec<CommitFile>>,
}

#[async_trait::async_trait]
impl Repo for Gh<'_> {
    async fn head(&self, branch: &str) -> Result<String, Error> {
        self.github.get_ref(self.owner, self.repo, branch).await
    }

    async fn commit(&self, sha: &str) -> Result<(String, Vec<String>), Error> {
        self.github
            .commit_tree_parents(self.owner, self.repo, sha)
            .await
    }

    async fn between(&self, base: &str, head: &str) -> Result<Vec<Listed>, Error> {
        Ok(self
            .github
            .compare_all_commits(self.owner, self.repo, base, head)
            .await?
            .iter()
            .filter_map(Listed::from_json)
            .collect())
    }

    async fn files(&self, sha: &str) -> Result<Vec<CommitFile>, Error> {
        if let Some(f) = self.known.get(sha) {
            return Ok(f.clone());
        }
        self.github.commit_changes(self.owner, self.repo, sha).await
    }

    async fn slots(
        &self,
        commit: &str,
        paths: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, Slot>, Error> {
        let (root, _) = self.commit(commit).await?;
        if let Some(all) = self.github.tree_all(self.owner, self.repo, &root).await? {
            return Ok(paths
                .iter()
                .map(|p| (p.clone(), all.get(p).cloned()))
                .collect());
        }
        // A tree too big to list at once: walk it, one directory a read.
        let mut levels: HashMap<String, Vec<(String, GitEntry)>> = HashMap::new();
        let mut dirs: HashMap<String, Option<String>> = HashMap::new();
        dirs.insert(String::new(), Some(root));
        let mut out = BTreeMap::new();
        for p in paths {
            let (dir, name) = p.rsplit_once('/').unwrap_or(("", p.as_str()));
            let tree = self.dir_tree(dir, &mut dirs, &mut levels).await?;
            let slot = match tree {
                Some(t) => self
                    .level(&t, &mut levels)
                    .await?
                    .iter()
                    .find(|(n, e)| n == name && e.kind != "tree")
                    .map(|(_, e)| e.clone()),
                None => None,
            };
            out.insert(p.clone(), slot);
        }
        Ok(out)
    }

    async fn blob(&self, sha: &str) -> Result<Vec<u8>, Error> {
        self.github.blob_bytes(self.owner, self.repo, sha).await
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<String, Error> {
        self.github.put_blob(self.owner, self.repo, bytes).await
    }

    async fn put_tree(&self, base_tree: &str, changes: &[(String, Slot)]) -> Result<String, Error> {
        self.github
            .put_tree(self.owner, self.repo, base_tree, changes)
            .await
    }

    async fn put_commit(
        &self,
        message: &str,
        tree: &str,
        parents: &[String],
    ) -> Result<String, Error> {
        self.github
            .put_commit(self.owner, self.repo, message, tree, parents)
            .await
    }

    async fn move_branch(&self, branch: &str, lease: &str, to: &str) -> Result<bool, Error> {
        self.github
            .move_with_lease(self.owner, self.repo, branch, lease, to)
            .await
    }

    async fn changed(&self, from: &str, to: &str) -> Result<Option<Vec<String>>, Error> {
        let files = self
            .github
            .compare_changed_files(self.owner, self.repo, from, to)
            .await?;
        // The compare lists at most 300 files: past that it isn't the list.
        Ok((files.len() < 300).then_some(files))
    }
}

impl Gh<'_> {
    async fn level<'m>(
        &self,
        tree: &str,
        levels: &'m mut HashMap<String, Vec<(String, GitEntry)>>,
    ) -> Result<&'m Vec<(String, GitEntry)>, Error> {
        if !levels.contains_key(tree) {
            let l = self.github.tree_level(self.owner, self.repo, tree).await?;
            levels.insert(tree.to_string(), l);
        }
        Ok(&levels[tree])
    }

    /// The tree sha of directory `dir` (`""` is the root), `None` when it
    /// isn't there.
    async fn dir_tree(
        &self,
        dir: &str,
        dirs: &mut HashMap<String, Option<String>>,
        levels: &mut HashMap<String, Vec<(String, GitEntry)>>,
    ) -> Result<Option<String>, Error> {
        if let Some(t) = dirs.get(dir) {
            return Ok(t.clone());
        }
        let mut cur = dirs.get("").cloned().flatten();
        let mut walked = String::new();
        for part in dir.split('/') {
            if !walked.is_empty() {
                walked.push('/');
            }
            walked.push_str(part);
            if let Some(t) = dirs.get(&walked) {
                cur = t.clone();
                continue;
            }
            cur = match cur {
                Some(t) => self
                    .level(&t, levels)
                    .await?
                    .iter()
                    .find(|(n, e)| n == part && e.kind == "tree")
                    .map(|(_, e)| e.sha.clone()),
                None => None,
            };
            dirs.insert(walked.clone(), cur.clone());
        }
        Ok(cur)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn blob(sha: &str) -> GitEntry {
        GitEntry {
            mode: "100644".into(),
            kind: "blob".into(),
            sha: sha.into(),
        }
    }

    /// A repository in memory: commits hold whole trees (path to entry),
    /// blobs are their text, trees are named by their content.
    #[derive(Default)]
    struct Fake {
        inner: Mutex<FakeInner>,
    }

    /// A commit's whole tree, parents, message and order.
    type FakeCommit = (BTreeMap<String, GitEntry>, Vec<String>, String, usize);

    #[derive(Default)]
    struct FakeInner {
        commits: BTreeMap<String, FakeCommit>,
        trees: BTreeMap<String, BTreeMap<String, GitEntry>>,
        blobs: BTreeMap<String, Vec<u8>>,
        branches: BTreeMap<String, String>,
        seq: usize,
        /// Someone pushes this file to the branch the next time it moves.
        push_on_move: Option<(String, String, String)>,
        /// The next calls to read a head fail with a server error.
        head_failures: u32,
        /// Writing a tree is refused for a missing permission.
        forbid_trees: bool,
        moves: u32,
    }

    fn text_sha(t: &[u8]) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        t.hash(&mut h);
        format!("b{:015x}", h.finish())
    }

    fn tree_sha(t: &BTreeMap<String, GitEntry>) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for (p, e) in t {
            (p, &e.mode, &e.kind, &e.sha).hash(&mut h);
        }
        format!("t{:015x}", h.finish())
    }

    impl Fake {
        fn put_text(&self, text: &str) -> GitEntry {
            let mut i = self.inner.lock().unwrap();
            let sha = text_sha(text.as_bytes());
            i.blobs.insert(sha.clone(), text.as_bytes().to_vec());
            blob(&sha)
        }

        /// A commit on `parents` whose tree is the first parent's with
        /// `files` laid over it (`None` removes).
        fn commit_on(&self, parents: &[&str], files: &[(&str, Option<&str>)], msg: &str) -> String {
            let entries: Vec<(String, Slot)> = files
                .iter()
                .map(|(p, t)| (p.to_string(), t.map(|t| self.put_text(t))))
                .collect();
            let mut i = self.inner.lock().unwrap();
            let mut tree = parents
                .first()
                .map(|p| i.commits[*p].0.clone())
                .unwrap_or_default();
            for (p, e) in entries {
                match e {
                    Some(e) => tree.insert(p, e),
                    None => tree.remove(&p),
                };
            }
            i.seq += 1;
            let sha = format!("c{:04}", i.seq);
            let seq = i.seq;
            i.commits.insert(
                sha.clone(),
                (
                    tree,
                    parents.iter().map(|p| p.to_string()).collect(),
                    msg.to_string(),
                    seq,
                ),
            );
            sha
        }

        /// A merge commit of two parents holding `files` over the first's.
        fn merge_of(&self, a: &str, b: &str, files: &[(&str, Option<&str>)]) -> String {
            self.commit_on(&[a, b], files, "Merge")
        }

        fn set(&self, branch: &str, sha: &str) {
            self.inner
                .lock()
                .unwrap()
                .branches
                .insert(branch.into(), sha.into());
        }

        fn file(&self, branch: &str, path: &str) -> Option<String> {
            let i = self.inner.lock().unwrap();
            let c = &i.branches[branch];
            i.commits[c]
                .0
                .get(path)
                .map(|e| String::from_utf8(i.blobs[&e.sha].clone()).unwrap())
        }

        fn tree_of(&self, sha: &str) -> BTreeMap<String, GitEntry> {
            self.inner.lock().unwrap().commits[sha].0.clone()
        }

        /// A branch name or a commit.
        fn sha(i: &FakeInner, name: &str) -> String {
            i.branches
                .get(name)
                .cloned()
                .unwrap_or_else(|| name.to_string())
        }

        fn ancestors(i: &FakeInner, sha: &str) -> BTreeSet<String> {
            let mut out = BTreeSet::new();
            let mut todo = vec![Fake::sha(i, sha)];
            while let Some(s) = todo.pop() {
                if out.insert(s.clone()) {
                    todo.extend(i.commits[&s].1.iter().cloned());
                }
            }
            out
        }
    }

    fn diff(a: &BTreeMap<String, GitEntry>, b: &BTreeMap<String, GitEntry>) -> Vec<String> {
        let keys: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
        keys.into_iter()
            .filter(|k| a.get(*k) != b.get(*k))
            .cloned()
            .collect()
    }

    #[async_trait::async_trait]
    impl Repo for Fake {
        async fn head(&self, branch: &str) -> Result<String, Error> {
            let mut i = self.inner.lock().unwrap();
            if i.head_failures > 0 {
                i.head_failures -= 1;
                return Err("HTTP status server error (502 Bad Gateway) for url (x)".into());
            }
            i.branches
                .get(branch)
                .cloned()
                .ok_or_else(|| "HTTP status client error (404 Not Found) for url (x)".into())
        }

        async fn commit(&self, sha: &str) -> Result<(String, Vec<String>), Error> {
            let mut i = self.inner.lock().unwrap();
            let (tree, parents, _, _) = i.commits.get(sha).cloned().ok_or("no commit")?;
            let t = tree_sha(&tree);
            i.trees.insert(t.clone(), tree);
            Ok((t, parents))
        }

        async fn between(&self, base: &str, head: &str) -> Result<Vec<Listed>, Error> {
            let i = self.inner.lock().unwrap();
            let skip = Fake::ancestors(&i, base);
            let mut out: Vec<(usize, Listed)> = Fake::ancestors(&i, head)
                .into_iter()
                .filter(|s| !skip.contains(s))
                .map(|s| {
                    let (_, parents, message, seq) = i.commits[&s].clone();
                    (
                        seq,
                        Listed {
                            sha: s,
                            parents,
                            message,
                        },
                    )
                })
                .collect();
            out.sort_by_key(|(n, _)| *n);
            Ok(out.into_iter().map(|(_, l)| l).collect())
        }

        async fn files(&self, sha: &str) -> Result<Vec<CommitFile>, Error> {
            let i = self.inner.lock().unwrap();
            let (tree, parents, _, _) = &i.commits[sha];
            let before = parents
                .first()
                .map(|p| i.commits[p].0.clone())
                .unwrap_or_default();
            let changed = diff(&before, tree);
            let added: Vec<&String> = changed
                .iter()
                .filter(|p| !before.contains_key(*p))
                .collect();
            let removed: Vec<&String> = changed.iter().filter(|p| !tree.contains_key(*p)).collect();
            let mut out = Vec::new();
            let mut renamed_from = BTreeSet::new();
            for p in &changed {
                let mut previous = None;
                if added.contains(&p) {
                    // GitHub reports a moved file as one renamed file.
                    previous = removed
                        .iter()
                        .find(|r| before[**r].sha == tree[p].sha && !renamed_from.contains(**r))
                        .map(|r| r.to_string());
                    if let Some(r) = &previous {
                        renamed_from.insert(r.clone());
                    }
                }
                out.push(CommitFile {
                    path: p.clone(),
                    previous,
                    patch: String::new(),
                });
            }
            out.retain(|f| !renamed_from.contains(&f.path));
            Ok(out)
        }

        async fn slots(
            &self,
            commit: &str,
            paths: &BTreeSet<String>,
        ) -> Result<BTreeMap<String, Slot>, Error> {
            let i = self.inner.lock().unwrap();
            let tree = &i.commits.get(&Fake::sha(&i, commit)).ok_or("no commit")?.0;
            Ok(paths
                .iter()
                .map(|p| (p.clone(), tree.get(p).cloned()))
                .collect())
        }

        async fn blob(&self, sha: &str) -> Result<Vec<u8>, Error> {
            Ok(self.inner.lock().unwrap().blobs[sha].clone())
        }

        async fn put_blob(&self, bytes: &[u8]) -> Result<String, Error> {
            let sha = text_sha(bytes);
            self.inner
                .lock()
                .unwrap()
                .blobs
                .insert(sha.clone(), bytes.to_vec());
            Ok(sha)
        }

        async fn put_tree(
            &self,
            base_tree: &str,
            changes: &[(String, Slot)],
        ) -> Result<String, Error> {
            let mut i = self.inner.lock().unwrap();
            if i.forbid_trees {
                return Err("HTTP status client error (403 Forbidden) for url (x): \
                            {\"message\":\"Resource not accessible by integration\"}"
                    .into());
            }
            let mut tree = i.trees[base_tree].clone();
            for (p, e) in changes {
                match e {
                    Some(e) => {
                        tree.insert(p.clone(), e.clone());
                    }
                    None => {
                        // GitHub refuses to remove a path the tree lacks.
                        tree.remove(p)
                            .ok_or("HTTP status client error (422) no such path")?;
                    }
                }
            }
            let t = tree_sha(&tree);
            i.trees.insert(t.clone(), tree);
            Ok(t)
        }

        async fn put_commit(
            &self,
            message: &str,
            tree: &str,
            parents: &[String],
        ) -> Result<String, Error> {
            let mut i = self.inner.lock().unwrap();
            let t = i.trees[tree].clone();
            i.seq += 1;
            let sha = format!("n{:04}", i.seq);
            let seq = i.seq;
            i.commits
                .insert(sha.clone(), (t, parents.to_vec(), message.to_string(), seq));
            Ok(sha)
        }

        async fn move_branch(&self, branch: &str, lease: &str, to: &str) -> Result<bool, Error> {
            let pushed = {
                let mut i = self.inner.lock().unwrap();
                i.moves += 1;
                i.push_on_move.take()
            };
            if let Some((b, path, text)) = pushed {
                let head = self.inner.lock().unwrap().branches[&b].clone();
                let c = self.commit_on(&[&head], &[(&path, Some(&text))], "A person's push");
                self.set(&b, &c);
            }
            let mut i = self.inner.lock().unwrap();
            if i.branches[branch] != lease {
                return Ok(false);
            }
            i.branches.insert(branch.into(), to.into());
            Ok(true)
        }

        async fn changed(&self, from: &str, to: &str) -> Result<Option<Vec<String>>, Error> {
            let i = self.inner.lock().unwrap();
            Ok(Some(diff(&i.commits[from].0, &i.commits[to].0)))
        }
    }

    /// Merges by keeping both versions' lines (theirs, then ours' new ones).
    #[derive(Default)]
    struct Both {
        calls: Vec<(String, Option<String>)>,
    }

    #[async_trait::async_trait]
    impl Merger for Both {
        async fn merge(
            &mut self,
            path: &str,
            ancestor: Option<&str>,
            theirs: &str,
            ours: &str,
        ) -> Option<String> {
            self.calls
                .push((path.to_string(), ancestor.map(String::from)));
            let mut out: Vec<&str> = theirs.lines().collect();
            for l in ours.lines() {
                if !out.contains(&l) {
                    out.push(l);
                }
            }
            Some(out.join("\n"))
        }
    }

    /// acme/web: `main` at m0; the dependency `coderhelm/feature-a` (#42)
    /// had commits a1, a2 on it; the run's branch `coderhelm/feature-b`
    /// started from a2 and made its own commits; then #42's branch was
    /// rebased on a newer main (same changes, new hashes a1', a2').
    struct Stack {
        repo: Fake,
        copies: Vec<String>,
    }

    fn stack(own: usize) -> Stack {
        let repo = Fake::default();
        let m0 = repo.commit_on(
            &[],
            &[
                ("web/app.ts", Some("app 1")),
                ("web/old.ts", Some("old")),
                ("README.md", Some("readme")),
            ],
            "main",
        );
        let a1 = repo.commit_on(
            &[&m0],
            &[("web/schema.ts", Some("schema 1"))],
            "ABC-12: schema",
        );
        let a2 = repo.commit_on(&[&a1], &[("web/rows.ts", Some("rows 1"))], "ABC-12: rows");
        let mut tip = a2.clone();
        for n in 0..own {
            tip = repo.commit_on(
                &[&tip],
                &[(&format!("web/b/f{}.ts", n % 7), Some(&format!("b {n}")))],
                &format!("ABC-99: step {n}"),
            );
        }
        repo.set("coderhelm/feature-b", &tip);
        // #42 rebased onto a newer main.
        let m1 = repo.commit_on(&[&m0], &[("README.md", Some("readme 2"))], "main moved");
        let n1 = repo.commit_on(
            &[&m1],
            &[("web/schema.ts", Some("schema 1"))],
            "ABC-12: schema",
        );
        let n2 = repo.commit_on(&[&n1], &[("web/rows.ts", Some("rows 1"))], "ABC-12: rows");
        repo.set("coderhelm/feature-a", &n2);
        repo.set("main", &m1);
        Stack {
            repo,
            copies: vec![a1, a2],
        }
    }

    fn job<'a>(own: Own<'a>) -> Job<'a> {
        Job {
            branch: "coderhelm/feature-b",
            onto: "coderhelm/feature-a",
            own,
            message: "ABC-99: one field for the two flags",
            onto_pr: 42,
        }
    }

    /// The branch now: one commit (or two) on the dependency's head, and
    /// every path is the dependency's except the change's.
    fn assert_stacked(s: &Stack, r: &Rebuilt, commits: usize) {
        let i = s.repo.inner.lock().unwrap();
        let dep = i.branches["coderhelm/feature-a"].clone();
        let mut c = i.branches["coderhelm/feature-b"].clone();
        assert_eq!(c, r.tip);
        for _ in 0..commits {
            c = i.commits[&c].1[0].clone();
        }
        assert_eq!(c, dep, "the change sits right on the dependency's head");
    }

    #[tokio::test]
    async fn a_branch_with_more_commits_than_any_replay_allowed_stacks() {
        // 150 own commits: past the old 100-commit replay limit.
        let s = stack(150);
        let r = rebuild(
            &s.repo,
            &mut NoMerger,
            &job(Own::NotCopies {
                base: "main",
                copies: &s.copies,
            }),
        )
        .await
        .unwrap();
        assert_eq!(r.own_commits, 150);
        assert_eq!(r.paths, 7);
        assert!(r.unresolved.is_empty() && r.merged.is_empty());
        assert_stacked(&s, &r, 1);
        // The change's files, and the dependency's (and main's) everything else.
        assert_eq!(
            s.repo.file("coderhelm/feature-b", "web/b/f3.ts").as_deref(),
            Some("b 143")
        );
        assert_eq!(
            s.repo.file("coderhelm/feature-b", "README.md").as_deref(),
            Some("readme 2")
        );
        assert_eq!(
            s.repo.file("coderhelm/feature-b", "web/rows.ts").as_deref(),
            Some("rows 1")
        );
    }

    #[tokio::test]
    async fn a_dependency_never_merged_into_the_branch_stacks() {
        // The dependency moved on after the rebase and the branch never had
        // it merged in: nothing has to be merged first.
        let s = stack(3);
        let dep = s.repo.inner.lock().unwrap().branches["coderhelm/feature-a"].clone();
        let d3 = s
            .repo
            .commit_on(&[&dep], &[("web/extra.ts", Some("more"))], "ABC-12: more");
        s.repo.set("coderhelm/feature-a", &d3);
        // A merge of main into the branch is no part of the change.
        let tip = s.repo.inner.lock().unwrap().branches["coderhelm/feature-b"].clone();
        let main = s.repo.inner.lock().unwrap().branches["main"].clone();
        let mg = s
            .repo
            .merge_of(&tip, &main, &[("README.md", Some("readme 2"))]);
        s.repo.set("coderhelm/feature-b", &mg);
        let r = rebuild(
            &s.repo,
            &mut NoMerger,
            &job(Own::NotCopies {
                base: "main",
                copies: &s.copies,
            }),
        )
        .await
        .unwrap();
        assert_eq!((r.own_commits, r.paths), (3, 3));
        assert_stacked(&s, &r, 1);
        assert_eq!(
            s.repo
                .file("coderhelm/feature-b", "web/extra.ts")
                .as_deref(),
            Some("more")
        );
        let i = s.repo.inner.lock().unwrap();
        assert_eq!(i.commits[&r.tip].2, "ABC-99: one field for the two flags");
    }

    #[tokio::test]
    async fn a_path_both_changed_is_merged_keeping_both_sides() {
        let s = stack(0);
        let tip = s.repo.inner.lock().unwrap().branches["coderhelm/feature-b"].clone();
        let o = s.repo.commit_on(
            &[&tip],
            &[("web/rows.ts", Some("rows 1\nours"))],
            "ABC-99: rows",
        );
        s.repo.set("coderhelm/feature-b", &o);
        let dep = s.repo.inner.lock().unwrap().branches["coderhelm/feature-a"].clone();
        let d = s.repo.commit_on(
            &[&dep],
            &[("web/rows.ts", Some("rows 1\ntheirs"))],
            "ABC-12: fix",
        );
        s.repo.set("coderhelm/feature-a", &d);
        let mut m = Both::default();
        let r = rebuild(
            &s.repo,
            &mut m,
            &job(Own::NotCopies {
                base: "main",
                copies: &s.copies,
            }),
        )
        .await
        .unwrap();
        assert_eq!(r.merged, vec!["web/rows.ts".to_string()]);
        assert!(r.unresolved.is_empty());
        // The model saw the version the change was copied from.
        assert_eq!(
            m.calls,
            vec![("web/rows.ts".to_string(), Some("rows 1".to_string()))]
        );
        assert_eq!(
            s.repo.file("coderhelm/feature-b", "web/rows.ts").as_deref(),
            Some("rows 1\ntheirs\nours")
        );
        assert_stacked(&s, &r, 1);
    }

    #[tokio::test]
    async fn a_conflict_that_can_t_be_merged_becomes_a_labelled_follow_up() {
        let s = stack(1);
        let tip = s.repo.inner.lock().unwrap().branches["coderhelm/feature-b"].clone();
        let o = s
            .repo
            .commit_on(&[&tip], &[("web/rows.ts", Some("ours"))], "ABC-99: rows");
        s.repo.set("coderhelm/feature-b", &o);
        let dep = s.repo.inner.lock().unwrap().branches["coderhelm/feature-a"].clone();
        let d = s
            .repo
            .commit_on(&[&dep], &[("web/rows.ts", Some("theirs"))], "ABC-12: fix");
        s.repo.set("coderhelm/feature-a", &d);
        let r = rebuild(
            &s.repo,
            &mut NoMerger,
            &job(Own::NotCopies {
                base: "main",
                copies: &s.copies,
            }),
        )
        .await
        .unwrap();
        assert_eq!(r.unresolved, vec!["web/rows.ts".to_string()]);
        assert_stacked(&s, &r, 2);
        // The first commit keeps #42's version; the follow-up puts ours back
        // and says so.
        let (first, msg) = {
            let i = s.repo.inner.lock().unwrap();
            (i.commits[&r.tip].1[0].clone(), i.commits[&r.tip].2.clone())
        };
        let first_tree = s.repo.tree_of(&first);
        let i = s.repo.inner.lock().unwrap();
        assert_eq!(i.blobs[&first_tree["web/rows.ts"].sha], b"theirs");
        drop(i);
        assert_eq!(
            s.repo.file("coderhelm/feature-b", "web/rows.ts").as_deref(),
            Some("ours")
        );
        assert!(msg.starts_with("Follow-up: this change's version of 1 file(s) #42 also changed"));
        assert!(msg.contains("- web/rows.ts"));
    }

    #[tokio::test]
    async fn removed_and_renamed_files_come_along() {
        let s = stack(0);
        let tip = s.repo.inner.lock().unwrap().branches["coderhelm/feature-b"].clone();
        // A rename (old.ts to new.ts, same content) and a removal.
        let o = s.repo.commit_on(
            &[&tip],
            &[("web/old.ts", None), ("web/new.ts", Some("old"))],
            "ABC-99: rename",
        );
        let o2 = s
            .repo
            .commit_on(&[&o], &[("web/app.ts", None)], "ABC-99: remove app");
        s.repo.set("coderhelm/feature-b", &o2);
        let renamed = s.repo.files(&o).await.unwrap();
        assert_eq!(
            renamed,
            vec![CommitFile {
                path: "web/new.ts".into(),
                previous: Some("web/old.ts".into()),
                patch: String::new()
            }]
        );
        let r = rebuild(
            &s.repo,
            &mut NoMerger,
            &job(Own::NotCopies {
                base: "main",
                copies: &s.copies,
            }),
        )
        .await
        .unwrap();
        assert_eq!(r.paths, 3);
        assert_stacked(&s, &r, 1);
        let b = "coderhelm/feature-b";
        assert_eq!(s.repo.file(b, "web/new.ts").as_deref(), Some("old"));
        assert_eq!(s.repo.file(b, "web/old.ts"), None);
        assert_eq!(s.repo.file(b, "web/app.ts"), None);
        assert_eq!(s.repo.file(b, "README.md").as_deref(), Some("readme 2"));
    }

    #[tokio::test]
    async fn a_kept_branch_rebuilt_at_start_carries_no_copies_into_the_work() {
        // A retry starts on the branch an earlier attempt left: it is rebuilt
        // before any implementation, so the new work lands on #42's head and
        // the pull request step finds nothing left to drop.
        let s = stack(4);
        let r = rebuild(
            &s.repo,
            &mut NoMerger,
            &job(Own::NotCopies {
                base: "main",
                copies: &s.copies,
            }),
        )
        .await
        .unwrap();
        let w = s.repo.commit_on(
            &[&r.tip],
            &[("web/b/f1.ts", Some("implemented"))],
            "ABC-99: work",
        );
        s.repo.set("coderhelm/feature-b", &w);
        let ahead = s
            .repo
            .between("coderhelm/feature-a", "coderhelm/feature-b")
            .await
            .unwrap();
        assert_eq!(ahead.len(), 2);
        assert!(ahead.iter().all(|c| !s.copies.contains(&c.sha)));
        // Built again (a second retry): one commit again, same files.
        let again = rebuild(
            &s.repo,
            &mut NoMerger,
            &job(Own::NotCopies {
                base: "coderhelm/feature-a",
                copies: &[],
            }),
        )
        .await
        .unwrap();
        assert_stacked(&s, &again, 1);
        assert_eq!(
            s.repo.file("coderhelm/feature-b", "web/b/f1.ts").as_deref(),
            Some("implemented")
        );
    }

    #[tokio::test]
    async fn a_force_pushed_dependency_rebuilds_from_its_old_head() {
        // The dependency's commits were reworded and squashed (no patch id
        // matches): the old head says where the branch's own work starts.
        let s = stack(2);
        let old = s.copies[1].clone();
        let r = rebuild(&s.repo, &mut NoMerger, &job(Own::After { from: &old }))
            .await
            .unwrap();
        assert_eq!(r.own_commits, 2);
        assert_stacked(&s, &r, 1);
    }

    #[tokio::test]
    async fn the_branch_moving_meanwhile_is_built_again_with_the_push() {
        let s = stack(2);
        s.repo.inner.lock().unwrap().push_on_move = Some((
            "coderhelm/feature-b".into(),
            "web/late.ts".into(),
            "pushed meanwhile".into(),
        ));
        let r = rebuild(
            &s.repo,
            &mut NoMerger,
            &job(Own::NotCopies {
                base: "main",
                copies: &s.copies,
            }),
        )
        .await
        .unwrap();
        assert_eq!(r.tries, 2);
        assert_eq!(r.own_commits, 3);
        assert_stacked(&s, &r, 1);
        assert_eq!(
            s.repo.file("coderhelm/feature-b", "web/late.ts").as_deref(),
            Some("pushed meanwhile")
        );
    }

    #[tokio::test]
    async fn github_errors_are_retried_and_only_a_permission_stops() {
        let s = stack(1);
        s.repo.inner.lock().unwrap().head_failures = 3;
        let own = Own::NotCopies {
            base: "main",
            copies: &s.copies,
        };
        assert!(rebuild(&s.repo, &mut NoMerger, &job(own)).await.is_ok());

        let s = stack(1);
        s.repo.inner.lock().unwrap().forbid_trees = true;
        let own = Own::NotCopies {
            base: "main",
            copies: &s.copies,
        };
        let h = rebuild(&s.repo, &mut NoMerger, &job(own))
            .await
            .unwrap_err();
        assert_eq!(
            h,
            Halt::Permission {
                permission: "Contents: read and write".into(),
                doing: "writing the rebuilt files".into()
            }
        );
        assert!(h.say("acme/web").starts_with(
            "CoderHelm's GitHub App needs the \"Contents: read and write\" permission on acme/web"
        ));
        // Nothing moved.
        assert_eq!(s.repo.inner.lock().unwrap().moves, 0);
    }

    #[test]
    fn a_force_push_rebuilds_only_coderhelm_s_pull_requests_stacked_on_it() {
        let pr = |n: u64, head: &str, base: &str, user: &str, repo: &str| {
            serde_json::json!({
                "number": n,
                "head": {"ref": head, "repo": {"full_name": repo}},
                "base": {"ref": base},
                "user": {"login": user},
            })
        };
        let open = vec![
            pr(
                42,
                "coderhelm/feature-a",
                "main",
                "acme-coderhelm[bot]",
                "acme/web",
            ),
            pr(
                43,
                "coderhelm/feature-b",
                "coderhelm/feature-a",
                "acme-coderhelm[bot]",
                "acme/web",
            ),
            pr(44, "pat/tweak", "coderhelm/feature-a", "pat", "acme/web"),
            pr(
                45,
                "coderhelm/feature-c",
                "coderhelm/feature-a",
                "acme-coderhelm[bot]",
                "someone/web",
            ),
            pr(
                46,
                "coderhelm/feature-d",
                "main",
                "acme-coderhelm[bot]",
                "acme/web",
            ),
        ];
        let d: Vec<u64> = dependents(&open, "acme", "web", "coderhelm/feature-a")
            .iter()
            .map(|p| p["number"].as_u64().unwrap())
            .collect();
        assert_eq!(d, vec![43]);
        let many: Vec<Value> = (0..9)
            .map(|n| {
                pr(
                    n,
                    &format!("coderhelm/x-{n}"),
                    "b",
                    "coderhelm[bot]",
                    "acme/web",
                )
            })
            .collect();
        assert_eq!(dependents(&many, "acme", "web", "b").len(), MAX_DEPENDENTS);
        let m = restacked_message("ABC-99: one field ", 42, "coderhelm/feature-a");
        assert!(m.starts_with("ABC-99: one field\n\nRebuilt on #42"));
    }

    #[test]
    fn which_permission_a_refusal_names() {
        let e = "HTTP status client error (403 Forbidden) for url (u): Resource not accessible by integration";
        assert_eq!(permission_needed(e, WRITE).as_deref(), Some(WRITE));
        let w =
            "HTTP status client error (422 Unprocessable Entity) for url (u): refusing to allow \
                 a GitHub App to create or update workflow `.github/workflows/ci.yml` without \
                 `workflows` permission";
        assert_eq!(
            permission_needed(w, WRITE).as_deref(),
            Some("Workflows: read and write")
        );
        let rl = "HTTP status client error (403 Forbidden) for url (u): API rate limit exceeded";
        assert_eq!(permission_needed(rl, WRITE), None);
        assert_eq!(
            permission_needed("HTTP status server error (502)", WRITE),
            None
        );
    }

    #[test]
    fn the_plan_takes_the_change_keeps_theirs_or_names_a_conflict() {
        let paths: BTreeSet<String> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let m = |v: &[(&str, Option<&str>)]| -> BTreeMap<String, Slot> {
            v.iter()
                .map(|(p, s)| (p.to_string(), s.map(blob)))
                .collect()
        };
        let base = m(&[
            ("a", Some("1")),
            ("b", Some("1")),
            ("c", Some("1")),
            ("d", Some("1")),
            ("e", None),
        ]);
        let ours = m(&[
            ("a", Some("2")),
            ("b", Some("1")),
            ("c", Some("2")),
            ("d", None),
            ("e", Some("9")),
        ]);
        let theirs = m(&[
            ("a", Some("1")),
            ("b", Some("3")),
            ("c", Some("3")),
            ("d", Some("1")),
            ("e", Some("9")),
        ]);
        let p = plan(&paths, &ours, &theirs, &base);
        // a: only ours changed; d: ours removed it; b: ours net nothing,
        // theirs stays; c: both changed; e: the same on both.
        assert_eq!(
            p.take,
            vec![("a".to_string(), Some(blob("2"))), ("d".to_string(), None)]
        );
        assert_eq!(p.conflicts, vec!["c".to_string()]);
    }

    #[test]
    fn the_check_refuses_a_wrong_path_or_a_stray_change() {
        let paths: BTreeSet<String> = ["a".to_string()].into();
        let e: BTreeMap<String, Slot> = [("a".to_string(), Some(blob("2")))].into();
        assert!(holds(&e, &e, Some(&["a".to_string()]), &paths).is_ok());
        let wrong: BTreeMap<String, Slot> = [("a".to_string(), Some(blob("1")))].into();
        assert!(holds(&e, &wrong, None, &paths).is_err());
        assert!(
            holds(&e, &e, Some(&["a".to_string(), "z".to_string()]), &paths)
                .unwrap_err()
                .contains("`z`")
        );
    }
}
