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

use std::collections::{BTreeSet, HashMap};

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

async fn tree_of(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    commit: &str,
) -> Result<(String, Vec<String>, HashMap<String, GitEntry>), BoxErr> {
    let (tree, parents) = github.commit_tree_parents(owner, repo, commit).await?;
    let entries = github
        .tree_all(owner, repo, &tree)
        .await?
        .ok_or("the repository's tree is too large to read in one request")?;
    Ok((tree, parents, entries))
}

/// The commit the merged pull request started from. A merge commit and a
/// squash start from the merge commit's first parent. A rebase merge laid
/// the pull request's commits one by one: it started that many first
/// parents back, which shows as the merge commit alone changing fewer files
/// than the pull request did.
async fn before_merge(
    github: &GitHubClient,
    r: &Request<'_>,
    pr: &serde_json::Value,
    merged: &HashMap<String, GitEntry>,
    parents: &[String],
) -> Result<HashMap<String, GitEntry>, BoxErr> {
    let first = parents.first().ok_or("the merge commit has no parent")?;
    let (_, mut up, mut before) = tree_of(github, r.owner, r.repo, first).await?;
    let commits = pr["commits"].as_u64().unwrap_or(1);
    let files = pr["changed_files"].as_u64().unwrap_or(0) as usize;
    if parents.len() == 1 && commits > 1 && changed_paths(&before, merged).len() < files {
        for _ in 1..commits.min(100) {
            let Some(next) = up.first().cloned() else {
                break;
            };
            let (_, p, t) = tree_of(github, r.owner, r.repo, &next).await?;
            up = p;
            before = t;
        }
    }
    Ok(before)
}

async fn build(github: &GitHubClient, r: &Request<'_>) -> Result<Outcome, BoxErr> {
    let name = branch(r.pr_number, r.merge_sha);
    if let Some(open) = github
        .find_open_pr_for_branch(r.owner, r.repo, &name)
        .await?
    {
        return Ok(Outcome::Exists {
            number: open["number"].as_u64().unwrap_or(0),
            url: open["html_url"].as_str().unwrap_or("").to_string(),
        });
    }
    let pr = github
        .get_pull_request(r.owner, r.repo, r.pr_number)
        .await?;
    let (_, parents, merged) = tree_of(github, r.owner, r.repo, r.merge_sha).await?;
    let before = before_merge(github, r, &pr, &merged, &parents).await?;
    let head_sha = github.get_ref(r.owner, r.repo, r.base_branch).await?;
    let (head_tree, _, head) = tree_of(github, r.owner, r.repo, &head_sha).await?;
    let changes = match plan(&before, &merged, &head) {
        Plan::Clean(c) => c,
        Plan::Moved(files) => return Ok(Outcome::Moved(files)),
        Plan::AlreadyReverted => return Ok(Outcome::AlreadyReverted),
    };
    let original = pr["title"].as_str().unwrap_or("");
    let tree = github
        .put_tree(r.owner, r.repo, &head_tree, &changes)
        .await?;
    let commit = github
        .put_commit(
            r.owner,
            r.repo,
            &commit_message(original, r.pr_number, r.merge_sha),
            &tree,
            &[head_sha],
        )
        .await?;
    github
        .create_branch(r.owner, r.repo, &name, &commit)
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
    let opened = github
        .create_pull_request(
            r.owner,
            r.repo,
            &title(original),
            &body(
                r.pr_number,
                r.merge_sha,
                &r.cause,
                r.evidence,
                &files,
                &not_undone,
            ),
            &name,
            r.base_branch,
            // Ready for approval, never a draft: it is complete as it is.
            false,
        )
        .await?;
    Ok(Outcome::Opened {
        number: opened["number"].as_u64().unwrap_or(0),
        url: opened["html_url"].as_str().unwrap_or("").to_string(),
    })
}

/// Opens the revert pull request for a merge (or finds the one already
/// open). Never merges anything.
pub async fn propose(github: &GitHubClient, r: &Request<'_>) -> Outcome {
    match build(github, r).await {
        Ok(o) => {
            info!(pr = r.pr_number, sha = %r.merge_sha, outcome = ?o, "Deploy watch: revert proposal");
            o
        }
        Err(e) => {
            warn!(pr = r.pr_number, sha = %r.merge_sha, error = %e, "Deploy watch: could not build the revert");
            Outcome::Failed(common::run_fail::error_text(&*e))
        }
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
