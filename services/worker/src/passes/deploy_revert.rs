//! Staying through deploy: a revert pull request when a merged change of
//! CoderHelm's breaks its deploy.
//!
//! After a merge the post-merge check (`health_check`) watches the merge
//! commit's own deploy. When that deploy fails on checks that were not
//! failing before the merge, CoderHelm opens a pull request that reverts
//! the change, with the evidence, ready for the team's approval. It never merges it: a revert of
//! a shared branch is the team's decision, made the way the team approves
//! any pull request.
//!
//! The revert is built from trees, with no sandbox: every path the merge
//! changed goes back to what it was before the merge, on top of the base
//! branch as it stands now. A path a later commit changed again is not
//! touched: then there is no clean revert, and CoderHelm says which files
//! moved instead of opening a pull request that would undo someone else's
//! work. One revert pull request per merge, however many times the deploy
//! is looked at.
//!
//! GitHub failing for a moment (a 5xx, a secondary limit, no answer) is
//! never the end of the revert. Every call on the path goes through
//! [`step`], which asks the worker's one classifier
//! (`common::run_recovery::classify`) whether the error passes by itself and
//! tries again if it does. A path still failing that way after its tries is
//! [`Outcome::Retry`], and the deploy watch looks again later
//! (`health_check`), so the revert is built on that look. The calls that
//! write are safe to repeat: the branch is reset only while it has no open
//! pull request, and a pull request GitHub opened before its answer failed
//! is found before another is asked for.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;

use tracing::{info, warn};

use crate::clients::github::{GitEntry, GitHubClient};

/// The branch a merge's revert lives on: one per merged pull request and
/// merge commit, so a second look finds the first one's pull request.
pub fn branch(pr_number: u64, merge_sha: &str) -> String {
    format!("coderhelm/revert-{pr_number}-{}", short(merge_sha))
}

fn short(sha: &str) -> &str {
    sha.get(..7).unwrap_or(sha)
}

/// What reverting a merge on top of the current base comes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Lay these over the base's tree (`None` removes the path).
    Clean(Vec<(String, Option<GitEntry>)>),
    /// Later commits changed these files the merge changed: no clean revert.
    Moved(Vec<String>),
    /// The base already has every path as it was before the merge.
    AlreadyReverted,
}

/// The paths a merge changed: every path whose entry differs between the
/// tree before it and the tree it made (a rename is both its paths).
pub fn changed_paths(
    before: &HashMap<String, GitEntry>,
    merged: &HashMap<String, GitEntry>,
) -> Vec<String> {
    let mut paths: BTreeSet<&String> = BTreeSet::new();
    for (p, e) in merged {
        if before.get(p) != Some(e) {
            paths.insert(p);
        }
    }
    for p in before.keys() {
        if !merged.contains_key(p) {
            paths.insert(p);
        }
    }
    paths.into_iter().cloned().collect()
}

/// Plans the revert of a merge on the base as it stands: `before` is the
/// tree the merge started from, `merged` the tree it made, `head` the base
/// branch's tree now. A path still as the merge left it goes back to what
/// it was; a path already back is left; a path a later commit changed is
/// reported and nothing is planned.
pub fn plan(
    before: &HashMap<String, GitEntry>,
    merged: &HashMap<String, GitEntry>,
    head: &HashMap<String, GitEntry>,
) -> Plan {
    let mut changes: Vec<(String, Option<GitEntry>)> = Vec::new();
    let mut moved: Vec<String> = Vec::new();
    for path in changed_paths(before, merged) {
        let (was, made, now) = (before.get(&path), merged.get(&path), head.get(&path));
        if now == was {
            continue;
        }
        if now == made {
            changes.push((path, was.cloned()));
        } else {
            moved.push(path);
        }
    }
    if !moved.is_empty() {
        Plan::Moved(moved)
    } else if changes.is_empty() {
        Plan::AlreadyReverted
    } else {
        Plan::Clean(changes)
    }
}

/// Why CoderHelm proposes the revert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cause {
    /// The merge commit's deploy failed on checks that passed before it.
    DeployFailed,
}

/// The revert pull request's title, in GitHub's own shape so tools that
/// read reverts recognise it.
pub fn title(original_title: &str) -> String {
    format!(
        "Revert \"{}\"",
        common::truncate_str(original_title.trim(), 200)
    )
}

/// The revert's commit message.
pub fn commit_message(original_title: &str, pr_number: u64, merge_sha: &str) -> String {
    format!(
        "{}\n\nThis reverts commit {merge_sha} (#{pr_number}).",
        title(original_title)
    )
}

/// The revert pull request's description: what it reverts, the evidence
/// (one line each, as recorded), what the revert does and does not undo,
/// and that it waits for the team's approval. `rollback` is the reverted
/// change's own account of what a revert leaves in place
/// (`reviewer_guide::rollback` of its files).
pub fn body(
    pr_number: u64,
    merge_sha: &str,
    cause: &Cause,
    evidence: &[String],
    files: &[String],
    not_undone: &str,
) -> String {
    let why = match cause {
        Cause::DeployFailed => format!(
            "The deploy of `{}` (the merge of #{pr_number}) failed on checks that were not \
             failing before the merge.",
            short(merge_sha)
        ),
    };
    let evidence: String = evidence
        .iter()
        .map(|e| format!("* {}\n", e.trim().trim_start_matches(['-', '*']).trim()))
        .collect();
    let shown: Vec<String> = files.iter().take(20).map(|f| format!("`{f}`")).collect();
    let more = files.len().saturating_sub(shown.len());
    let mut out = format!(
        "Reverts #{pr_number}\n\n**Summary** {why} This pull request puts every file that merge \
         changed back to what it was before it.\n\n**Evidence**\n{evidence}\n**Changes**\n* \
         {}{}\n\n",
        shown.join(", "),
        if more > 0 {
            format!(" and {more} more")
        } else {
            String::new()
        },
    );
    if !not_undone.trim().is_empty() {
        out.push_str(&format!("**Not undone by this revert** {not_undone}\n\n"));
    }
    out.push_str(
        "**Approval** CoderHelm opened this and does not merge it: it merges the way the team \
         approves any pull request. Closing it keeps the original change.",
    );
    out
}

/// What became of the proposal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Opened {
        number: u64,
        url: String,
    },
    /// A revert pull request for this merge is open already.
    Exists {
        number: u64,
        url: String,
    },
    /// Later commits changed files the merge changed.
    Moved(Vec<String>),
    AlreadyReverted,
    /// GitHub failed for a moment on every try (the classifier's
    /// `Transient`): nothing is final, the deploy watch builds it again on
    /// its next look. The error, in plain words.
    Retry(String),
    /// Something couldn't be read or written, in plain words.
    Failed(String),
}

/// One line for the report on the merged pull request and the run: what
/// CoderHelm did about the failed deploy, never a request to do its work.
pub fn report_line(o: &Outcome) -> String {
    match o {
        Outcome::Opened { number, .. } => format!(
            "CoderHelm opened #{number} to revert this change, with the evidence. It is not \
             merged without the team's approval."
        ),
        Outcome::Exists { number, .. } => {
            format!("The revert of this change is #{number}, waiting for the team's approval.")
        }
        Outcome::Moved(files) => format!(
            "CoderHelm did not open a revert: later commits changed {} since this merge, and a \
             revert would undo their work too.",
            files
                .iter()
                .take(5)
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Outcome::AlreadyReverted => {
            "The base branch already has every file as it was before this merge: nothing to \
             revert."
                .to_string()
        }
        Outcome::Retry(why) => format!(
            "GitHub failed for a moment while CoderHelm built the revert ({}); it builds it again \
             on its next look at the deploy.",
            common::truncate_str(why, 200)
        ),
        Outcome::Failed(why) => format!(
            "CoderHelm could not build the revert ({}); the failing checks are listed above.",
            common::truncate_str(why, 200)
        ),
    }
}

/// What to revert and why.
pub struct Request<'a> {
    pub owner: &'a str,
    pub repo: &'a str,
    pub pr_number: u64,
    pub merge_sha: &'a str,
    pub base_branch: &'a str,
    pub cause: Cause,
    pub evidence: &'a [String],
}

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// Every GitHub call the revert path makes, so a test can make any one of
/// them fail.
#[allow(async_fn_in_trait)]
pub trait RevertGitHub {
    async fn find_open_pr_for_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<Option<serde_json::Value>, BoxErr>;
    async fn get_pull_request(
        &self,
        owner: &str,
        repo: &str,
        pr: u64,
    ) -> Result<serde_json::Value, BoxErr>;
    async fn commit_tree_parents(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<(String, Vec<String>), BoxErr>;
    async fn tree_all(
        &self,
        owner: &str,
        repo: &str,
        tree: &str,
    ) -> Result<Option<HashMap<String, GitEntry>>, BoxErr>;
    async fn get_ref(&self, owner: &str, repo: &str, branch: &str) -> Result<String, BoxErr>;
    async fn put_tree(
        &self,
        owner: &str,
        repo: &str,
        base_tree: &str,
        changes: &[(String, Option<GitEntry>)],
    ) -> Result<String, BoxErr>;
    async fn put_commit(
        &self,
        owner: &str,
        repo: &str,
        message: &str,
        tree: &str,
        parents: &[String],
    ) -> Result<String, BoxErr>;
    async fn create_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        from: &str,
    ) -> Result<String, BoxErr>;
    #[allow(clippy::too_many_arguments)]
    async fn create_pull_request(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: &str,
        head: &str,
        base: &str,
        draft: bool,
    ) -> Result<serde_json::Value, BoxErr>;
}

impl RevertGitHub for GitHubClient {
    async fn find_open_pr_for_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<Option<serde_json::Value>, BoxErr> {
        GitHubClient::find_open_pr_for_branch(self, owner, repo, branch).await
    }
    async fn get_pull_request(
        &self,
        owner: &str,
        repo: &str,
        pr: u64,
    ) -> Result<serde_json::Value, BoxErr> {
        GitHubClient::get_pull_request(self, owner, repo, pr).await
    }
    async fn commit_tree_parents(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<(String, Vec<String>), BoxErr> {
        GitHubClient::commit_tree_parents(self, owner, repo, sha).await
    }
    async fn tree_all(
        &self,
        owner: &str,
        repo: &str,
        tree: &str,
    ) -> Result<Option<HashMap<String, GitEntry>>, BoxErr> {
        GitHubClient::tree_all(self, owner, repo, tree).await
    }
    async fn get_ref(&self, owner: &str, repo: &str, branch: &str) -> Result<String, BoxErr> {
        GitHubClient::get_ref(self, owner, repo, branch).await
    }
    async fn put_tree(
        &self,
        owner: &str,
        repo: &str,
        base_tree: &str,
        changes: &[(String, Option<GitEntry>)],
    ) -> Result<String, BoxErr> {
        GitHubClient::put_tree(self, owner, repo, base_tree, changes).await
    }
    async fn put_commit(
        &self,
        owner: &str,
        repo: &str,
        message: &str,
        tree: &str,
        parents: &[String],
    ) -> Result<String, BoxErr> {
        GitHubClient::put_commit(self, owner, repo, message, tree, parents).await
    }
    async fn create_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        from: &str,
    ) -> Result<String, BoxErr> {
        GitHubClient::create_branch(self, owner, repo, branch, from).await
    }
    async fn create_pull_request(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: &str,
        head: &str,
        base: &str,
        draft: bool,
    ) -> Result<serde_json::Value, BoxErr> {
        GitHubClient::create_pull_request(self, owner, repo, title, body, head, base, draft).await
    }
}

/// Tries each GitHub call on the revert path gets.
pub const TRIES: u32 = 3;

/// Whether an error passes by itself, in the words of the worker's one
/// classifier: a 5xx, a 429, a secondary limit, a dropped connection.
pub fn transient(e: &(dyn std::error::Error + 'static)) -> bool {
    common::run_recovery::classify(&common::run_fail::error_text(e)).kind
        == common::run_recovery::Kind::Transient
}

/// One call on the revert path: tried again, with a growing pause from
/// `pause_ms`, while the classifier says its error passes by itself. The
/// call gets the try it is on (0 first), so a write can look for what an
/// earlier try did before it asks again.
async fn step<T, F, Fut>(pause_ms: u64, mut call: F) -> Result<T, BoxErr>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, BoxErr>>,
{
    let mut attempt = 0;
    loop {
        match call(attempt).await {
            Ok(v) => return Ok(v),
            Err(e) if attempt + 1 < TRIES && transient(&*e) => {
                warn!(attempt, error = %e, "Deploy watch: GitHub failed for a moment on the revert, trying again");
                if pause_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(pause_ms << attempt)).await;
                }
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn tree_of<G: RevertGitHub>(
    github: &G,
    pause_ms: u64,
    owner: &str,
    repo: &str,
    commit: &str,
) -> Result<(String, Vec<String>, HashMap<String, GitEntry>), BoxErr> {
    let (tree, parents) = step(pause_ms, |_| {
        github.commit_tree_parents(owner, repo, commit)
    })
    .await?;
    let entries = step(pause_ms, |_| github.tree_all(owner, repo, &tree))
        .await?
        .ok_or("the repository's tree is too large to read in one request")?;
    Ok((tree, parents, entries))
}

/// The commit the merged pull request started from. A merge commit and a
/// squash start from the merge commit's first parent. A rebase merge laid
/// the pull request's commits one by one: it started that many first
/// parents back, which shows as the merge commit alone changing fewer files
/// than the pull request did.
async fn before_merge<G: RevertGitHub>(
    github: &G,
    pause_ms: u64,
    r: &Request<'_>,
    pr: &serde_json::Value,
    merged: &HashMap<String, GitEntry>,
    parents: &[String],
) -> Result<HashMap<String, GitEntry>, BoxErr> {
    let first = parents.first().ok_or("the merge commit has no parent")?;
    let (_, mut up, mut before) = tree_of(github, pause_ms, r.owner, r.repo, first).await?;
    let commits = pr["commits"].as_u64().unwrap_or(1);
    let files = pr["changed_files"].as_u64().unwrap_or(0) as usize;
    if parents.len() == 1 && commits > 1 && changed_paths(&before, merged).len() < files {
        for _ in 1..commits.min(100) {
            let Some(next) = up.first().cloned() else {
                break;
            };
            let (_, p, t) = tree_of(github, pause_ms, r.owner, r.repo, &next).await?;
            up = p;
            before = t;
        }
    }
    Ok(before)
}

fn opened(pr: &serde_json::Value) -> (u64, String) {
    (
        pr["number"].as_u64().unwrap_or(0),
        pr["html_url"].as_str().unwrap_or("").to_string(),
    )
}

async fn build<G: RevertGitHub>(
    github: &G,
    r: &Request<'_>,
    pause_ms: u64,
) -> Result<Outcome, BoxErr> {
    let name = branch(r.pr_number, r.merge_sha);
    if let Some(open) = step(pause_ms, |_| {
        github.find_open_pr_for_branch(r.owner, r.repo, &name)
    })
    .await?
    {
        let (number, url) = opened(&open);
        return Ok(Outcome::Exists { number, url });
    }
    let pr = step(pause_ms, |_| {
        github.get_pull_request(r.owner, r.repo, r.pr_number)
    })
    .await?;
    let (_, parents, merged) = tree_of(github, pause_ms, r.owner, r.repo, r.merge_sha).await?;
    let before = before_merge(github, pause_ms, r, &pr, &merged, &parents).await?;
    let head_sha = step(pause_ms, |_| github.get_ref(r.owner, r.repo, r.base_branch)).await?;
    let (head_tree, _, head) = tree_of(github, pause_ms, r.owner, r.repo, &head_sha).await?;
    let changes = match plan(&before, &merged, &head) {
        Plan::Clean(c) => c,
        Plan::Moved(files) => return Ok(Outcome::Moved(files)),
        Plan::AlreadyReverted => return Ok(Outcome::AlreadyReverted),
    };
    let original = pr["title"].as_str().unwrap_or("");
    let tree = step(pause_ms, |_| {
        github.put_tree(r.owner, r.repo, &head_tree, &changes)
    })
    .await?;
    let message = commit_message(original, r.pr_number, r.merge_sha);
    let parents = [head_sha.clone()];
    let commit = step(pause_ms, |_| {
        github.put_commit(r.owner, r.repo, &message, &tree, &parents)
    })
    .await?;
    // A branch left by an earlier try with no pull request is reset to this
    // commit (`create_branch`), so the step repeats safely.
    step(pause_ms, |_| {
        github.create_branch(r.owner, r.repo, &name, &commit)
    })
    .await?;
    let files: Vec<String> = changes.iter().map(|(p, _)| p.clone()).collect();
    let as_files: Vec<(String, String, String)> = files
        .iter()
        .map(|p| (p.clone(), "modified".to_string(), String::new()))
        .collect();
    let rollback = super::reviewer_guide::rollback(&as_files);
    let not_undone = rollback
        .split_once("The revert does not undo everything: ")
        .map(|(_, rest)| rest.to_string())
        .unwrap_or_default();
    let title = title(original);
    let body = body(
        r.pr_number,
        r.merge_sha,
        &r.cause,
        r.evidence,
        &files,
        &not_undone,
    );
    let pr = step(pause_ms, |attempt| {
        let (title, body, name) = (&title, &body, &name);
        async move {
            // GitHub may have opened it before its answer failed: one pull
            // request per merge, never a second.
            if attempt > 0 {
                if let Some(open) = github
                    .find_open_pr_for_branch(r.owner, r.repo, name)
                    .await?
                {
                    return Ok(open);
                }
            }
            github
                .create_pull_request(
                    r.owner,
                    r.repo,
                    title,
                    body,
                    name,
                    r.base_branch,
                    // Ready for approval, never a draft: it is complete as it is.
                    false,
                )
                .await
        }
    })
    .await?;
    let (number, url) = opened(&pr);
    Ok(Outcome::Opened { number, url })
}

/// Pause before the second try of a call on the revert path (doubled for
/// each try after it).
const PAUSE_MS: u64 = 2_000;

/// Opens the revert pull request for a merge (or finds the one already
/// open). Never merges anything.
pub async fn propose(github: &GitHubClient, r: &Request<'_>) -> Outcome {
    propose_with(github, r, PAUSE_MS).await
}

/// [`propose`] on any GitHub, with its pause between tries.
pub async fn propose_with<G: RevertGitHub>(github: &G, r: &Request<'_>, pause_ms: u64) -> Outcome {
    match build(github, r, pause_ms).await {
        Ok(o) => {
            info!(pr = r.pr_number, sha = %r.merge_sha, outcome = ?o, "Deploy watch: revert proposal");
            o
        }
        Err(e) => outcome_of_error(r, &*e),
    }
}

/// What an error on the revert path comes to: [`Outcome::Retry`] while the
/// classifier says it passes by itself, [`Outcome::Failed`] for a real
/// answer.
fn outcome_of_error(
    r: &Request<'_>,
    e: &(dyn std::error::Error + Send + Sync + 'static),
) -> Outcome {
    let text = common::run_fail::error_text(e);
    if transient(e) {
        warn!(pr = r.pr_number, sha = %r.merge_sha, error = %text, "Deploy watch: GitHub kept failing for a moment on the revert; the next look builds it again");
        Outcome::Retry(text)
    } else {
        warn!(pr = r.pr_number, sha = %r.merge_sha, error = %text, "Deploy watch: could not build the revert");
        Outcome::Failed(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(sha: &str) -> GitEntry {
        GitEntry {
            mode: "100644".to_string(),
            kind: "blob".to_string(),
            sha: sha.to_string(),
        }
    }

    fn tree(files: &[(&str, &str)]) -> HashMap<String, GitEntry> {
        files.iter().map(|(p, s)| (p.to_string(), e(s))).collect()
    }

    #[test]
    fn a_revert_right_after_the_merge_restores_every_path() {
        let before = tree(&[
            ("src/a.ts", "a1"),
            ("src/old.ts", "o1"),
            ("README.md", "r1"),
        ]);
        // Changed a file, added one, renamed one.
        let merged = tree(&[
            ("src/a.ts", "a2"),
            ("src/new.ts", "n1"),
            ("src/renamed.ts", "o1"),
            ("README.md", "r1"),
        ]);
        assert_eq!(
            changed_paths(&before, &merged),
            vec!["src/a.ts", "src/new.ts", "src/old.ts", "src/renamed.ts"]
        );
        let Plan::Clean(changes) = plan(&before, &merged, &merged) else {
            panic!("clean revert expected");
        };
        assert_eq!(
            changes,
            vec![
                ("src/a.ts".to_string(), Some(e("a1"))),
                ("src/new.ts".to_string(), None),
                ("src/old.ts".to_string(), Some(e("o1"))),
                ("src/renamed.ts".to_string(), None),
            ]
        );
    }

    #[test]
    fn later_commits_to_other_files_stay_and_the_revert_is_still_clean() {
        let before = tree(&[("src/a.ts", "a1"), ("src/b.ts", "b1")]);
        let merged = tree(&[("src/a.ts", "a2"), ("src/b.ts", "b1")]);
        let head = tree(&[("src/a.ts", "a2"), ("src/b.ts", "b9"), ("src/c.ts", "c1")]);
        assert_eq!(
            plan(&before, &merged, &head),
            Plan::Clean(vec![("src/a.ts".to_string(), Some(e("a1")))])
        );
    }

    #[test]
    fn a_file_someone_changed_since_is_never_overwritten() {
        let before = tree(&[("src/a.ts", "a1"), ("src/b.ts", "b1")]);
        let merged = tree(&[("src/a.ts", "a2"), ("src/b.ts", "b2")]);
        let head = tree(&[("src/a.ts", "a3"), ("src/b.ts", "b2")]);
        assert_eq!(
            plan(&before, &merged, &head),
            Plan::Moved(vec!["src/a.ts".to_string()])
        );
    }

    #[test]
    fn a_change_already_reverted_plans_nothing() {
        let before = tree(&[("src/a.ts", "a1")]);
        let merged = tree(&[("src/a.ts", "a2"), ("src/n.ts", "n1")]);
        assert_eq!(plan(&before, &merged, &before), Plan::AlreadyReverted);
        // Half reverted by hand: the rest is still a clean revert.
        let half = tree(&[("src/a.ts", "a1"), ("src/n.ts", "n1")]);
        assert_eq!(
            plan(&before, &merged, &half),
            Plan::Clean(vec![("src/n.ts".to_string(), None)])
        );
    }

    #[test]
    fn the_pull_request_carries_the_evidence_and_waits_for_approval() {
        let sha = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";
        assert_eq!(branch(41, sha), "coderhelm/revert-41-a1b2c3d");
        assert_eq!(title("Add order totals"), "Revert \"Add order totals\"");
        let b = body(
            41,
            sha,
            &Cause::DeployFailed,
            &["- [**deploy-production**](https://github.com/acme/shop/actions/runs/9/job/1) (failure)".to_string()],
            &["src/a.ts".to_string(), "db/migrate/0042.sql".to_string()],
            "a migration that already ran stays applied (`db/migrate/0042.sql`).",
        );
        // GitHub and the reward records recognise a revert by this line.
        assert!(b.starts_with("Reverts #41\n"));
        assert!(b.contains("The deploy of `a1b2c3d` (the merge of #41) failed"));
        assert!(b.contains("* [**deploy-production**](https://github.com/acme/shop/actions/runs/9/job/1) (failure)"));
        assert!(b.contains("`src/a.ts`, `db/migrate/0042.sql`"));
        assert!(b.contains("**Not undone by this revert** a migration that already ran"));
        assert!(b.contains("does not merge it"));
        let low = b.to_lowercase();
        for phrase in crate::passes::ownership::HAND_OFF_PHRASES {
            assert!(!low.contains(phrase), "{phrase}");
        }
    }

    /// A repository where the merge `m` (from `p`) is the base's head, so
    /// its revert is clean; `fail` says which call fails, how, and how often.
    #[derive(Default)]
    pub(crate) struct Fake {
        pub fail: std::sync::Mutex<HashMap<&'static str, (u32, &'static str)>>,
        pub calls: std::sync::Mutex<Vec<&'static str>>,
        /// A pull request GitHub opened although its answer failed.
        pub opened_anyway: std::sync::Mutex<bool>,
        /// Whether a failed `create_pull_request` opens it anyway.
        pub opens_on_failure: std::sync::Mutex<bool>,
    }

    impl Fake {
        pub(crate) fn failing(call: &'static str, times: u32, how: &'static str) -> Self {
            let f = Fake::default();
            f.fail.lock().unwrap().insert(call, (times, how));
            f
        }
        fn hit(&self, call: &'static str) -> Result<(), BoxErr> {
            self.calls.lock().unwrap().push(call);
            let mut fail = self.fail.lock().unwrap();
            if let Some((left, how)) = fail.get_mut(call) {
                if *left > 0 {
                    *left -= 1;
                    return Err((*how).to_string().into());
                }
            }
            Ok(())
        }
        pub(crate) fn count(&self, call: &str) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|c| **c == call)
                .count()
        }
    }

    impl RevertGitHub for Fake {
        async fn find_open_pr_for_branch(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<Option<serde_json::Value>, BoxErr> {
            self.hit("find_open_pr_for_branch")?;
            Ok((*self.opened_anyway.lock().unwrap())
                .then(|| serde_json::json!({"number": 58, "html_url": "https://github.com/acme/shop/pull/58"})))
        }
        async fn get_pull_request(
            &self,
            _: &str,
            _: &str,
            _: u64,
        ) -> Result<serde_json::Value, BoxErr> {
            self.hit("get_pull_request")?;
            Ok(serde_json::json!({"title": "Add order totals", "commits": 1, "changed_files": 1}))
        }
        async fn commit_tree_parents(
            &self,
            _: &str,
            _: &str,
            sha: &str,
        ) -> Result<(String, Vec<String>), BoxErr> {
            self.hit("commit_tree_parents")?;
            Ok(match sha {
                "p" => ("tp".into(), vec![]),
                _ => ("tm".into(), vec!["p".into()]),
            })
        }
        async fn tree_all(
            &self,
            _: &str,
            _: &str,
            t: &str,
        ) -> Result<Option<HashMap<String, GitEntry>>, BoxErr> {
            self.hit("tree_all")?;
            Ok(Some(match t {
                "tp" => tree(&[("src/a.ts", "a1")]),
                _ => tree(&[("src/a.ts", "a2")]),
            }))
        }
        async fn get_ref(&self, _: &str, _: &str, _: &str) -> Result<String, BoxErr> {
            self.hit("get_ref")?;
            Ok("m".into())
        }
        async fn put_tree(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: &[(String, Option<GitEntry>)],
        ) -> Result<String, BoxErr> {
            self.hit("put_tree")?;
            Ok("tr".into())
        }
        async fn put_commit(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
            _: &[String],
        ) -> Result<String, BoxErr> {
            self.hit("put_commit")?;
            Ok("c".into())
        }
        async fn create_branch(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<String, BoxErr> {
            self.hit("create_branch")?;
            Ok("c".into())
        }
        async fn create_pull_request(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
            _: bool,
        ) -> Result<serde_json::Value, BoxErr> {
            if let Err(e) = self.hit("create_pull_request") {
                if *self.opens_on_failure.lock().unwrap() {
                    *self.opened_anyway.lock().unwrap() = true;
                }
                return Err(e);
            }
            Ok(
                serde_json::json!({"number": 57, "html_url": "https://github.com/acme/shop/pull/57"}),
            )
        }
    }

    pub(crate) fn request() -> Request<'static> {
        Request {
            owner: "acme",
            repo: "shop",
            pr_number: 41,
            merge_sha: "m",
            base_branch: "main",
            cause: Cause::DeployFailed,
            evidence: &[],
        }
    }

    pub(crate) const PULLS_500: &str =
        "HTTP status server error (500 Internal Server Error) for url \
         (https://api.github.com/repos/acme/shop/pulls)";

    #[tokio::test]
    async fn a_500_on_any_step_of_the_revert_is_tried_again_and_the_revert_opens() {
        for call in [
            "find_open_pr_for_branch",
            "get_pull_request",
            "commit_tree_parents",
            "tree_all",
            "get_ref",
            "put_tree",
            "put_commit",
            "create_branch",
            "create_pull_request",
        ] {
            let gh = Fake::failing(call, 1, PULLS_500);
            let out = propose_with(&gh, &request(), 0).await;
            assert!(
                matches!(out, Outcome::Opened { number: 57, .. }),
                "{call}: {out:?}"
            );
        }
        // A secondary limit is GitHub's too, and passes.
        let gh = Fake::failing(
            "create_pull_request",
            1,
            "HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/acme/shop/pulls): You have exceeded a secondary rate limit",
        );
        assert!(matches!(
            propose_with(&gh, &request(), 0).await,
            Outcome::Opened { .. }
        ));
    }

    #[tokio::test]
    async fn a_pull_request_github_opened_before_its_answer_failed_is_never_opened_twice() {
        let gh = Fake::failing("create_pull_request", 1, PULLS_500);
        *gh.opens_on_failure.lock().unwrap() = true;
        let out = propose_with(&gh, &request(), 0).await;
        // The retry looked first and found the one GitHub opened.
        assert!(matches!(out, Outcome::Opened { number: 58, .. }), "{out:?}");
        assert_eq!(gh.count("create_pull_request"), 1);
    }

    #[tokio::test]
    async fn github_failing_on_every_try_is_a_retry_on_the_next_look_never_final() {
        let gh = Fake::failing("create_pull_request", TRIES, PULLS_500);
        let out = propose_with(&gh, &request(), 0).await;
        assert!(matches!(out, Outcome::Retry(_)), "{out:?}");
        assert_eq!(gh.count("create_pull_request") as u32, TRIES);
        assert!(report_line(&out).contains("builds it again"));
        // The next look builds it.
        let out = propose_with(&gh, &request(), 0).await;
        assert!(matches!(out, Outcome::Opened { number: 57, .. }), "{out:?}");
        // A real answer (a 422, a missing permission) is final and tried once.
        let gh = Fake::failing(
            "create_pull_request",
            1,
            "HTTP status client error (422 Unprocessable Entity) for url (https://api.github.com/repos/acme/shop/pulls): Validation Failed",
        );
        assert!(matches!(
            propose_with(&gh, &request(), 0).await,
            Outcome::Failed(_)
        ));
        assert_eq!(gh.count("create_pull_request"), 1);
    }

    #[test]
    fn every_outcome_is_reported_in_words() {
        let opened = report_line(&Outcome::Opened {
            number: 57,
            url: String::new(),
        });
        assert!(
            opened.contains("#57") && opened.contains("not merged without the team's approval")
        );
        let moved = report_line(&Outcome::Moved(vec!["src/a.ts".to_string()]));
        assert!(moved.contains("did not open a revert") && moved.contains("`src/a.ts`"));
        assert!(report_line(&Outcome::AlreadyReverted).contains("nothing to revert"));
        assert!(report_line(&Outcome::Failed("GitHub API 502".into())).contains("GitHub API 502"));
    }
}
