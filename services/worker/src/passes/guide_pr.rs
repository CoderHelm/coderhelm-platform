//! Proposing lessons for a repository's guide file as a pull request, and
//! settling them when it closes (`common::guide` has the rules).
//!
//! [`propose`] runs after an outcome taught or reinforced a lesson. It does
//! nothing unless the repository has lessons that reached the confidence
//! for the guide and no guide pull request is open or was closed too
//! recently. It claims the repository's record first, then makes one branch
//! from the default branch's head, one commit that adds the lessons to the
//! guide file on that branch, and one pull request. It has no way to write
//! to the default branch: every write names the new branch
//! ([`GuideRepo::write`]), and a branch that turns out to be the default
//! branch is refused before anything is written.
//!
//! [`on_closed`] runs when the pull request closes: lessons still in the
//! merged file are marked as in the guide, the ones people took out (or
//! all of them, when it closed without merging) are declined.

use crate::clients::github::GitHubClient;
use crate::WorkerState;
use common::guide::{self, Hold, Record};
use common::rewards::lessons::Lesson;
use common::rewards::store::Store;
use tracing::{info, warn};

/// The repository, as the proposal needs it.
#[async_trait::async_trait]
pub trait GuideRepo: Send + Sync {
    async fn default_branch(&self) -> Result<String, String>;
    /// A file's text at a ref; None when it is not there.
    async fn read(&self, path: &str, git_ref: &str) -> Result<Option<String>, String>;
    async fn create_branch(&self, branch: &str, from: &str) -> Result<(), String>;
    /// Writes one file in one commit on `branch`.
    async fn write(
        &self,
        path: &str,
        content: &str,
        branch: &str,
        message: &str,
    ) -> Result<(), String>;
    /// Opens the pull request; its number.
    async fn open_pr(&self, title: &str, body: &str, head: &str, base: &str)
        -> Result<u64, String>;
    /// Puts the lessons label on the pull request.
    async fn label(&self, pr: u64, label: &str) -> Result<(), String>;
}

/// The team's records, as the proposal needs them.
#[async_trait::async_trait]
pub trait GuideStore: Send + Sync {
    /// Whether the team has guide pull requests on for this repository.
    /// An error counts as off.
    async fn enabled(&self) -> Result<bool, String>;
    async fn lessons(&self) -> Result<Vec<Lesson>, String>;
    async fn record(&self) -> Result<Option<Record>, String>;
    /// Writes `next` only when the stored record is still `prev`.
    async fn swap(&self, prev: Option<&Record>, next: &Record) -> Result<bool, String>;
    async fn settle(&self, kept: &[String], declined: &[String], pr: u64) -> Result<(), String>;
}

/// What a call to [`propose`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Proposal {
    /// The team turned guide pull requests off (for this repository or for
    /// all), or the switch could not be read: nothing is written.
    Off,
    /// No lesson has reached the confidence for the guide (or the guide
    /// already says them all).
    NothingReady,
    /// Rate limited.
    Held(Hold),
    /// Another worker claimed the repository first.
    Raced,
    /// The pull request was opened with these lessons.
    Opened { pr: u64, lessons: Vec<String> },
    /// GitHub refused a step; the record says so and it is tried again
    /// later.
    Failed(String),
}

/// The guide file to add to: the first of the known ones the repository
/// has, with its text; else the default one, empty.
async fn guide_file(repo: &dyn GuideRepo, base: &str) -> Result<(String, String), String> {
    for path in guide::GUIDE_FILES {
        if let Some(text) = repo.read(path, base).await? {
            return Ok((path.to_string(), text));
        }
    }
    Ok((guide::DEFAULT_FILE.to_string(), String::new()))
}

/// Proposes the repository's ready lessons for its guide file as one pull
/// request, when the rate limit allows one.
pub async fn propose(
    repo: &dyn GuideRepo,
    store: &dyn GuideStore,
    repo_name: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Proposal, String> {
    let kept = store.lessons().await?;
    // The cheap check first: most outcomes leave nothing ready.
    if guide::pick(&kept, "", now).is_empty() {
        return Ok(Proposal::NothingReady);
    }
    // The team's switch, before anything is claimed, read or created. A
    // switch that cannot be read is off: nothing goes to a repository on a
    // guess.
    if !store.enabled().await.unwrap_or(false) {
        return Ok(Proposal::Off);
    }
    let prev = store.record().await?;
    if let Err(hold) = guide::may_propose(prev.as_ref(), now) {
        return Ok(Proposal::Held(hold));
    }
    let base = repo.default_branch().await?;
    let (path, text) = guide_file(repo, &base).await?;
    let picked = guide::pick(&kept, &text, now);
    let Some(content) = guide::with_lessons(&text, &picked) else {
        return Ok(Proposal::NothingReady);
    };
    let branch = guide::branch(now);
    // Never the default branch, whatever it is called.
    if branch == base || !guide::is_guide_branch(&branch) {
        return Err(format!("refusing to write the guide on `{base}`"));
    }
    let ids: Vec<String> = picked.iter().map(|l| l.id.clone()).collect();
    let claim = Record {
        repo: repo_name.to_string(),
        state: guide::STATE_OPENING.into(),
        pr: 0,
        branch: branch.clone(),
        path: path.clone(),
        lesson_ids: ids.clone(),
        at: now.to_rfc3339(),
        declined_in_a_row: prev.as_ref().map(|p| p.declined_in_a_row).unwrap_or(0),
    };
    if !store.swap(prev.as_ref(), &claim).await? {
        return Ok(Proposal::Raced);
    }
    let opened = async {
        repo.create_branch(&branch, &base).await?;
        repo.write(
            &path,
            &content,
            &branch,
            &format!(
                "Guide: add {} from earlier pull requests",
                if picked.len() == 1 {
                    "a lesson".to_string()
                } else {
                    format!("{} lessons", picked.len())
                }
            ),
        )
        .await?;
        repo.open_pr(
            &guide::pr_title(picked.len()),
            &guide::pr_body(&path, repo_name, &picked),
            &branch,
            &base,
        )
        .await
    }
    .await;
    // The label says where it comes from in every list of pull requests.
    // Without it the title and the description still do.
    if let Ok(pr) = &opened {
        if let Err(e) = repo.label(*pr, guide::LABEL).await {
            warn!(repo = repo_name, pr, error = %e, "Could not label the guide pull request");
        }
    }
    let next = match &opened {
        Ok(pr) => Record {
            state: guide::STATE_OPEN.into(),
            pr: *pr,
            ..claim.clone()
        },
        Err(_) => Record {
            state: guide::STATE_FAILED.into(),
            ..claim.clone()
        },
    };
    if !store.swap(Some(&claim), &next).await? {
        warn!(
            repo = repo_name,
            "The guide record changed under the proposal"
        );
    }
    Ok(match opened {
        Ok(pr) => Proposal::Opened { pr, lessons: ids },
        Err(e) => Proposal::Failed(e),
    })
}

/// What [`on_closed`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Closed {
    /// Not the repository's recorded guide pull request: nothing touched.
    NotOurs,
    Settled {
        merged: bool,
        kept: Vec<String>,
        declined: Vec<String>,
    },
}

/// Settles the repository's guide pull request `pr` that closed (`merged`
/// or not): marks each proposed lesson as in the guide or declined, and
/// records the outcome for the rate limit.
pub async fn on_closed(
    repo: &dyn GuideRepo,
    store: &dyn GuideStore,
    pr: u64,
    merged: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Closed, String> {
    let Some(rec) = store.record().await? else {
        return Ok(Closed::NotOurs);
    };
    if rec.pr != pr || pr == 0 || rec.state != guide::STATE_OPEN {
        return Ok(Closed::NotOurs);
    }
    let kept_lessons = store.lessons().await?;
    let proposed: Vec<Lesson> = kept_lessons
        .into_iter()
        .filter(|l| rec.lesson_ids.contains(&l.id))
        .collect();
    let merged_guide = if merged {
        let base = repo.default_branch().await?;
        Some(repo.read(&rec.path, &base).await?.unwrap_or_default())
    } else {
        None
    };
    let settled = guide::settle(&proposed, merged_guide.as_deref());
    let next = Record {
        state: if merged {
            guide::STATE_MERGED
        } else {
            guide::STATE_DECLINED
        }
        .into(),
        at: now.to_rfc3339(),
        declined_in_a_row: if merged { 0 } else { rec.declined_in_a_row + 1 },
        ..rec.clone()
    };
    // The record first: a redelivered close finds it settled and stops.
    if !store.swap(Some(&rec), &next).await? {
        return Ok(Closed::NotOurs);
    }
    store.settle(&settled.kept, &settled.declined, pr).await?;
    Ok(Closed::Settled {
        merged,
        kept: settled.kept,
        declined: settled.declined,
    })
}

// ── The real edges ───────────────────────────────────────────────────

pub struct GitHubRepo<'a> {
    pub github: &'a GitHubClient,
    pub owner: &'a str,
    pub name: &'a str,
}

#[async_trait::async_trait]
impl GuideRepo for GitHubRepo<'_> {
    async fn default_branch(&self) -> Result<String, String> {
        self.github
            .get_default_branch(self.owner, self.name)
            .await
            .map_err(|e| e.to_string())
    }

    async fn read(&self, path: &str, git_ref: &str) -> Result<Option<String>, String> {
        self.github
            .read_file_if_exists(self.owner, self.name, path, git_ref)
            .await
            .map_err(|e| e.to_string())
    }

    async fn create_branch(&self, branch: &str, from: &str) -> Result<(), String> {
        self.github
            .create_branch(self.owner, self.name, branch, from)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn write(
        &self,
        path: &str,
        content: &str,
        branch: &str,
        message: &str,
    ) -> Result<(), String> {
        let sha = self
            .github
            .get_file_sha(self.owner, self.name, path, branch)
            .await
            .ok();
        self.github
            .write_file(
                self.owner,
                self.name,
                path,
                content,
                branch,
                message,
                sha.as_deref(),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn open_pr(
        &self,
        title: &str,
        body: &str,
        head: &str,
        base: &str,
    ) -> Result<u64, String> {
        let pr = self
            .github
            .create_pull_request(self.owner, self.name, title, body, head, base, false)
            .await
            .map_err(|e| e.to_string())?;
        pr["number"]
            .as_u64()
            .filter(|n| *n > 0)
            .ok_or_else(|| "GitHub returned no pull request number".to_string())
    }

    async fn label(&self, pr: u64, label: &str) -> Result<(), String> {
        self.github
            .ensure_label(self.owner, self.name, label)
            .await
            .map_err(|e| e.to_string())?;
        self.github
            .add_labels(self.owner, self.name, pr, &[label.to_string()])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

pub struct DynamoStore<'a> {
    pub state: &'a WorkerState,
    pub team_id: &'a str,
    pub repo: &'a str,
}

#[async_trait::async_trait]
impl GuideStore for DynamoStore<'_> {
    async fn enabled(&self) -> Result<bool, String> {
        guide::load_switch(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            self.team_id,
        )
        .await
        .map(|s| s.allows(self.repo))
    }

    async fn lessons(&self) -> Result<Vec<Lesson>, String> {
        Store::new(&self.state.dynamo, &self.state.config.settings_table_name)
            .lessons(self.team_id, Some(self.repo))
            .await
    }

    async fn record(&self) -> Result<Option<Record>, String> {
        guide::load(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            self.team_id,
            self.repo,
        )
        .await
    }

    async fn swap(&self, prev: Option<&Record>, next: &Record) -> Result<bool, String> {
        guide::swap(
            &self.state.dynamo,
            &self.state.config.settings_table_name,
            self.team_id,
            prev,
            next,
        )
        .await
    }

    async fn settle(&self, kept: &[String], declined: &[String], pr: u64) -> Result<(), String> {
        Store::new(&self.state.dynamo, &self.state.config.settings_table_name)
            .settle_guide_lessons(self.team_id, self.repo, kept, declined, pr)
            .await
    }
}

/// Proposes the guide pull request for `owner/name` when its lessons and
/// the rate limit allow. Best effort: a failure is logged.
pub async fn maybe_propose(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    name: &str,
) {
    let repo = format!("{owner}/{name}");
    let edge = GitHubRepo {
        github,
        owner,
        name,
    };
    let store = DynamoStore {
        state,
        team_id,
        repo: &repo,
    };
    match propose(&edge, &store, &repo, chrono::Utc::now()).await {
        Ok(Proposal::Opened { pr, lessons }) => {
            info!(team_id, repo = %repo, pr, lessons = lessons.len(), "Guide pull request opened")
        }
        Ok(Proposal::Failed(e)) => {
            warn!(team_id, repo = %repo, error = %e, "The guide pull request could not be opened")
        }
        Ok(other) => info!(team_id, repo = %repo, outcome = ?other, "No guide pull request now"),
        Err(e) => warn!(team_id, repo = %repo, error = %e, "Guide proposal failed"),
    }
}

/// Settles a closed guide pull request (the `guide_closed` outcome).
pub async fn closed(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    name: &str,
    pr: u64,
) -> Result<(), String> {
    let merged = github
        .get_pull_request(owner, name, pr)
        .await
        .map_err(|e| e.to_string())?["merged_at"]
        .as_str()
        .is_some_and(|m| !m.is_empty());
    let repo = format!("{owner}/{name}");
    let edge = GitHubRepo {
        github,
        owner,
        name,
    };
    let store = DynamoStore {
        state,
        team_id,
        repo: &repo,
    };
    let done = on_closed(&edge, &store, pr, merged, chrono::Utc::now()).await?;
    info!(team_id, repo = %repo, pr, outcome = ?done, "Guide pull request closed");
    Ok(())
}

#[cfg(test)]
pub mod fakes {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// A pull request the fake opened: number, title, body, head, base.
    pub type OpenedPr = (u64, String, String, String, String);

    /// A repository in memory: branches of files, and every write made.
    #[derive(Default)]
    pub struct FakeRepo {
        pub default: String,
        pub branches: Mutex<BTreeMap<String, BTreeMap<String, String>>>,
        /// (branch, path) of each write, in order.
        pub writes: Mutex<Vec<(String, String)>>,
        pub prs: Mutex<Vec<OpenedPr>>,
        pub fail_pr: bool,
        /// (pull request, label) of each label put on.
        pub labels: Mutex<Vec<(u64, String)>>,
    }

    impl FakeRepo {
        pub fn new(default: &str, files: &[(&str, &str)]) -> Self {
            let tree: BTreeMap<String, String> = files
                .iter()
                .map(|(p, c)| (p.to_string(), c.to_string()))
                .collect();
            FakeRepo {
                default: default.into(),
                branches: Mutex::new(BTreeMap::from([(default.to_string(), tree)])),
                ..Default::default()
            }
        }

        pub fn file(&self, branch: &str, path: &str) -> Option<String> {
            self.branches
                .lock()
                .unwrap()
                .get(branch)?
                .get(path)
                .cloned()
        }

        /// Merges `branch` into the default branch (what a person's merge
        /// does), optionally with the file as they edited it.
        pub fn merge(&self, branch: &str, path: &str, edited: Option<&str>) {
            let mut b = self.branches.lock().unwrap();
            let text = edited
                .map(str::to_string)
                .or_else(|| b.get(branch).and_then(|t| t.get(path).cloned()))
                .unwrap_or_default();
            b.get_mut(&self.default)
                .unwrap()
                .insert(path.to_string(), text);
        }
    }

    #[async_trait::async_trait]
    impl GuideRepo for FakeRepo {
        async fn default_branch(&self) -> Result<String, String> {
            Ok(self.default.clone())
        }

        async fn read(&self, path: &str, git_ref: &str) -> Result<Option<String>, String> {
            Ok(self.file(git_ref, path))
        }

        async fn create_branch(&self, branch: &str, from: &str) -> Result<(), String> {
            let mut b = self.branches.lock().unwrap();
            let tree = b.get(from).cloned().ok_or("no such base")?;
            b.insert(branch.to_string(), tree);
            Ok(())
        }

        async fn write(
            &self,
            path: &str,
            content: &str,
            branch: &str,
            _message: &str,
        ) -> Result<(), String> {
            self.writes
                .lock()
                .unwrap()
                .push((branch.to_string(), path.to_string()));
            self.branches
                .lock()
                .unwrap()
                .get_mut(branch)
                .ok_or("no such branch")?
                .insert(path.to_string(), content.to_string());
            Ok(())
        }

        async fn open_pr(
            &self,
            title: &str,
            body: &str,
            head: &str,
            base: &str,
        ) -> Result<u64, String> {
            if self.fail_pr {
                return Err("GitHub said 422".into());
            }
            let mut prs = self.prs.lock().unwrap();
            let n = 40 + prs.len() as u64;
            prs.push((n, title.into(), body.into(), head.into(), base.into()));
            Ok(n)
        }

        async fn label(&self, pr: u64, label: &str) -> Result<(), String> {
            self.labels.lock().unwrap().push((pr, label.to_string()));
            Ok(())
        }
    }

    /// The team's records in memory, with the same compare-and-swap.
    pub struct FakeStore {
        pub lessons: Mutex<Vec<Lesson>>,
        pub record: Mutex<Option<Record>>,
        /// The next swap loses to another worker.
        pub lose_next_swap: Mutex<bool>,
        /// The team's switch; None stands for one that cannot be read.
        pub switch: Mutex<Option<guide::Switch>>,
        pub repo: String,
    }

    impl Default for FakeStore {
        /// Guide pull requests on, as for a team that saved no setting.
        fn default() -> Self {
            FakeStore {
                lessons: Default::default(),
                record: Default::default(),
                lose_next_swap: Default::default(),
                switch: Mutex::new(Some(guide::Switch::default())),
                repo: "acme/web".into(),
            }
        }
    }

    #[async_trait::async_trait]
    impl GuideStore for FakeStore {
        async fn enabled(&self) -> Result<bool, String> {
            match &*self.switch.lock().unwrap() {
                Some(s) => Ok(s.allows(&self.repo)),
                None => Err("the settings table did not answer".into()),
            }
        }

        async fn lessons(&self) -> Result<Vec<Lesson>, String> {
            Ok(self.lessons.lock().unwrap().clone())
        }

        async fn record(&self) -> Result<Option<Record>, String> {
            Ok(self.record.lock().unwrap().clone())
        }

        async fn swap(&self, prev: Option<&Record>, next: &Record) -> Result<bool, String> {
            let mut lose = self.lose_next_swap.lock().unwrap();
            if *lose {
                *lose = false;
                return Ok(false);
            }
            let mut r = self.record.lock().unwrap();
            if r.as_ref() != prev {
                return Ok(false);
            }
            *r = Some(next.clone());
            Ok(true)
        }

        async fn settle(
            &self,
            kept: &[String],
            declined: &[String],
            _pr: u64,
        ) -> Result<(), String> {
            for l in self.lessons.lock().unwrap().iter_mut() {
                if kept.contains(&l.id) {
                    l.in_guide = true;
                }
                if declined.contains(&l.id) && !l.guide_declined {
                    l.guide_declined = true;
                    l.contradicted += 1;
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;
    use chrono::TimeZone;

    const REPO: &str = "acme/web";

    fn at(day: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc
            .with_ymd_and_hms(2026, 10, day, 9, 30, 0)
            .unwrap()
    }

    fn lesson(id: &str, text: &str, prs: &[&str]) -> Lesson {
        Lesson {
            id: id.into(),
            repo: REPO.into(),
            agent: "coder".into(),
            text: text.into(),
            check: "Search the diff for the old call: there should be none.".into(),
            seen: prs.len() as u64,
            prs: prs.iter().map(|p| p.to_string()).collect(),
            last_seen_at: "2026-10-19T00:00:00Z".into(),
            evidence: vec!["PR #12 in people's words: \"Use the shared client\"".into()],
            ..Default::default()
        }
    }

    fn store(lessons: Vec<Lesson>) -> FakeStore {
        FakeStore {
            lessons: std::sync::Mutex::new(lessons),
            ..Default::default()
        }
    }

    fn hook() -> Lesson {
        lesson(
            "L1",
            "Fetch through the shared `useApi` hook, never a bare `fetch` call.",
            &["acme/web#12", "acme/web#15"],
        )
    }

    #[tokio::test]
    async fn a_confident_lesson_becomes_one_pull_request_and_never_a_push_to_the_default_branch() {
        let repo = FakeRepo::new("trunk", &[("AGENTS.md", "# Guide\n\nRun `make test`.\n")]);
        let st = store(vec![hook()]);
        let got = propose(&repo, &st, REPO, at(20)).await.unwrap();
        assert_eq!(
            got,
            Proposal::Opened {
                pr: 40,
                lessons: vec!["L1".into()]
            }
        );
        // One write, on the new branch; the default branch is as it was.
        let writes = repo.writes.lock().unwrap().clone();
        assert_eq!(
            writes,
            vec![(
                "coderhelm/guide-lessons-20261020-0930".to_string(),
                "AGENTS.md".to_string()
            )]
        );
        assert_eq!(
            repo.file("trunk", "AGENTS.md").unwrap(),
            "# Guide\n\nRun `make test`.\n"
        );
        let on_branch = repo
            .file("coderhelm/guide-lessons-20261020-0930", "AGENTS.md")
            .unwrap();
        assert!(on_branch.starts_with("# Guide\n\nRun `make test`."));
        assert!(on_branch.contains("- Fetch through the shared `useApi` hook"));
        // The pull request targets the default branch and names the evidence.
        let prs = repo.prs.lock().unwrap().clone();
        assert_eq!(prs.len(), 1);
        let (_, title, body, head, base) = &prs[0];
        assert_eq!(title, "CoderHelm lessons: one for the guide file");
        assert_eq!(
            (head.as_str(), base.as_str()),
            ("coderhelm/guide-lessons-20261020-0930", "trunk")
        );
        assert!(body.contains("From #12, #15.") && body.contains("Use the shared client"));
        let rec = st.record.lock().unwrap().clone().unwrap();
        assert_eq!((rec.state.as_str(), rec.pr), (guide::STATE_OPEN, 40));
        // It says where it comes from: in the title, the first words of the
        // description, and a label.
        assert!(title.starts_with("CoderHelm lessons:"));
        assert!(body.starts_with("**This pull request comes from CoderHelm's lessons"));
        assert_eq!(
            repo.labels.lock().unwrap().clone(),
            vec![(40, "coderhelm-lessons".to_string())]
        );
    }

    fn switch(enabled: bool, repos: &[(&str, bool)]) -> Option<guide::Switch> {
        Some(guide::Switch {
            enabled,
            repos: repos.iter().map(|(r, on)| (r.to_string(), *on)).collect(),
        })
    }

    /// Nothing reached the repository: no branch, no commit, no pull
    /// request, no label, and no claim on its record.
    fn untouched(repo: &FakeRepo, st: &FakeStore) {
        assert!(repo.writes.lock().unwrap().is_empty());
        assert!(repo.prs.lock().unwrap().is_empty());
        assert!(repo.labels.lock().unwrap().is_empty());
        assert_eq!(
            repo.branches.lock().unwrap().len(),
            1,
            "only the default branch"
        );
        assert!(st.record.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn turned_off_for_the_team_nothing_is_written_and_the_lesson_still_reaches_the_prompt() {
        let repo = FakeRepo::new("main", &[("AGENTS.md", "# Guide\n")]);
        let st = store(vec![hook()]);
        *st.switch.lock().unwrap() = switch(false, &[]);
        for day in [20, 21, 28] {
            assert_eq!(
                propose(&repo, &st, REPO, at(day)).await.unwrap(),
                Proposal::Off
            );
        }
        untouched(&repo, &st);
        // The lesson is as it was, and is still given to the next run.
        let kept = st.lessons.lock().unwrap().clone();
        assert!(!kept[0].in_guide && !kept[0].guide_declined && kept[0].contradicted == 0);
        let recalled = common::rewards::lessons::recall(&kept, "coder", at(20));
        assert_eq!(recalled.len(), 1);
        assert!(common::rewards::lessons::prompt_block(&recalled).contains("`useApi`"));
        // Turned back on: the same lesson is proposed.
        *st.switch.lock().unwrap() = switch(true, &[]);
        assert!(matches!(
            propose(&repo, &st, REPO, at(29)).await.unwrap(),
            Proposal::Opened { .. }
        ));
    }

    #[tokio::test]
    async fn a_repository_can_be_set_apart_from_the_team() {
        // On for the team, off for this repository.
        let repo = FakeRepo::new("main", &[]);
        let st = store(vec![hook()]);
        *st.switch.lock().unwrap() = switch(true, &[(REPO, false)]);
        assert_eq!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::Off
        );
        untouched(&repo, &st);
        // Another repository's override does not turn this one off.
        *st.switch.lock().unwrap() = switch(true, &[("acme/api", false)]);
        assert!(matches!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::Opened { .. }
        ));
        // Off for the team, on for this repository.
        let repo = FakeRepo::new("main", &[]);
        let st = store(vec![hook()]);
        *st.switch.lock().unwrap() = switch(false, &[(REPO, true)]);
        assert!(matches!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::Opened { .. }
        ));
    }

    #[tokio::test]
    async fn a_switch_that_cannot_be_read_counts_as_off() {
        let repo = FakeRepo::new("main", &[]);
        let st = store(vec![hook()]);
        *st.switch.lock().unwrap() = None;
        assert_eq!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::Off
        );
        untouched(&repo, &st);
    }

    #[tokio::test]
    async fn a_guide_pull_request_open_when_the_switch_goes_off_is_still_settled() {
        let repo = FakeRepo::new("main", &[("AGENTS.md", "# Guide\n")]);
        let st = store(vec![hook()]);
        let Proposal::Opened { pr, .. } = propose(&repo, &st, REPO, at(20)).await.unwrap() else {
            panic!("opened");
        };
        *st.switch.lock().unwrap() = switch(false, &[]);
        // The team closes it: its lesson is declined as usual, and no new
        // one is proposed while the switch is off.
        let done = on_closed(&repo, &st, pr, false, at(21)).await.unwrap();
        assert!(matches!(done, Closed::Settled { merged: false, .. }));
        st.lessons.lock().unwrap().push(lesson(
            "L2",
            "Name migration files `NNNN_what.sql` in `db/migrations`.",
            &["acme/web#3", "acme/web#5"],
        ));
        assert_eq!(
            propose(&repo, &st, REPO, at(30)).await.unwrap(),
            Proposal::Off
        );
        assert_eq!(repo.prs.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn at_most_one_guide_pull_request_is_open_per_repository() {
        let repo = FakeRepo::new("main", &[]);
        let st = store(vec![hook()]);
        assert!(matches!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::Opened { .. }
        ));
        // No guide file yet: one was started on the branch only.
        assert!(repo.file("main", "AGENTS.md").is_none());
        // Another lesson becomes ready while the first is open: held.
        st.lessons.lock().unwrap().push(lesson(
            "L2",
            "Name migration files `NNNN_what.sql` in `db/migrations`.",
            &["acme/web#3", "acme/web#5"],
        ));
        for day in [20, 21, 28] {
            assert_eq!(
                propose(&repo, &st, REPO, at(day)).await.unwrap(),
                Proposal::Held(Hold::Open(40))
            );
        }
        assert_eq!(repo.prs.lock().unwrap().len(), 1);
        assert_eq!(repo.writes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn nothing_is_proposed_without_a_confident_lesson_or_when_the_guide_says_it() {
        let repo = FakeRepo::new(
            "main",
            &[(
                "CLAUDE.md",
                "- Always fetch through the shared `useApi` hook; never a bare `fetch` call.\n",
            )],
        );
        // Seen once: not yet.
        let st = store(vec![lesson(
            "L0",
            "Use `zod` for request bodies.",
            &["acme/web#9"],
        )]);
        assert_eq!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::NothingReady
        );
        // Ready, but people already wrote it in the guide themselves.
        let st = store(vec![hook()]);
        assert_eq!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::NothingReady
        );
        assert!(repo.writes.lock().unwrap().is_empty());
        assert!(st.record.lock().unwrap().is_none(), "nothing claimed");
    }

    #[tokio::test]
    async fn two_workers_open_one_pull_request() {
        let repo = FakeRepo::new("main", &[]);
        let st = store(vec![hook()]);
        *st.lose_next_swap.lock().unwrap() = true;
        assert_eq!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::Raced
        );
        assert!(repo.writes.lock().unwrap().is_empty());
        assert!(repo.prs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_failure_at_github_is_recorded_and_tried_again_the_next_day() {
        let mut repo = FakeRepo::new("main", &[]);
        repo.fail_pr = true;
        let st = store(vec![hook()]);
        assert!(matches!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::Failed(_)
        ));
        assert_eq!(
            st.record.lock().unwrap().as_ref().unwrap().state,
            guide::STATE_FAILED
        );
        assert!(matches!(
            propose(&repo, &st, REPO, at(20)).await.unwrap(),
            Proposal::Held(Hold::Until(_))
        ));
        repo.fail_pr = false;
        assert!(matches!(
            propose(&repo, &st, REPO, at(22)).await.unwrap(),
            Proposal::Opened { .. }
        ));
    }

    #[tokio::test]
    async fn a_merged_guide_pull_request_puts_the_kept_lessons_in_the_guide() {
        let repo = FakeRepo::new("main", &[("AGENTS.md", "# Guide\n")]);
        let migrations = lesson(
            "L2",
            "Name migration files `NNNN_what.sql` in `db/migrations`.",
            &["acme/web#3", "acme/web#5"],
        );
        let st = store(vec![hook(), migrations]);
        let Proposal::Opened { pr, .. } = propose(&repo, &st, REPO, at(20)).await.unwrap() else {
            panic!("opened");
        };
        // The team removes the migrations line, then merges.
        let branch = "coderhelm/guide-lessons-20261020-0930";
        let edited: String = repo
            .file(branch, "AGENTS.md")
            .unwrap()
            .lines()
            .filter(|l| !l.contains("migration"))
            .collect::<Vec<_>>()
            .join("\n");
        repo.merge(branch, "AGENTS.md", Some(&edited));
        // Some other pull request closing is not the guide's.
        assert_eq!(
            on_closed(&repo, &st, 999, true, at(21)).await.unwrap(),
            Closed::NotOurs
        );
        let done = on_closed(&repo, &st, pr, true, at(21)).await.unwrap();
        assert_eq!(
            done,
            Closed::Settled {
                merged: true,
                kept: vec!["L1".into()],
                declined: vec!["L2".into()]
            }
        );
        let lessons = st.lessons.lock().unwrap().clone();
        assert!(lessons[0].in_guide && !lessons[0].guide_declined);
        assert!(lessons[1].guide_declined && lessons[1].contradicted == 1);
        // A redelivered close settles nothing twice.
        assert_eq!(
            on_closed(&repo, &st, pr, true, at(21)).await.unwrap(),
            Closed::NotOurs
        );
        assert_eq!(st.lessons.lock().unwrap()[1].contradicted, 1);
        // Neither lesson is proposed again, and the pause holds anyway.
        assert_eq!(
            propose(&repo, &st, REPO, at(22)).await.unwrap(),
            Proposal::NothingReady
        );
    }

    #[tokio::test]
    async fn a_declined_guide_pull_request_pauses_proposals_and_drops_its_lessons() {
        let repo = FakeRepo::new("main", &[]);
        let st = store(vec![hook()]);
        let Proposal::Opened { pr, .. } = propose(&repo, &st, REPO, at(1)).await.unwrap() else {
            panic!("opened");
        };
        let done = on_closed(&repo, &st, pr, false, at(2)).await.unwrap();
        assert_eq!(
            done,
            Closed::Settled {
                merged: false,
                kept: vec![],
                declined: vec!["L1".into()]
            }
        );
        assert!(repo.file("main", "AGENTS.md").is_none());
        // A new lesson becomes ready the next week: the pause still holds.
        st.lessons.lock().unwrap().push(lesson(
            "L2",
            "Name migration files `NNNN_what.sql` in `db/migrations`.",
            &["acme/web#3", "acme/web#5"],
        ));
        assert!(matches!(
            propose(&repo, &st, REPO, at(9)).await.unwrap(),
            Proposal::Held(Hold::Until(_))
        ));
        assert_eq!(repo.prs.lock().unwrap().len(), 1);
    }
}
