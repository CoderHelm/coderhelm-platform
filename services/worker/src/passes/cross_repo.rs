//! Reading the team's other repositories while a run investigates.
//!
//! A ticket filed against one repository is often about data another one
//! produces (a sync, an indexer, a pipeline job in a data repository). The
//! planner reads them all, read-only:
//!
//! - a code search that finds nothing in the run's repository searches the
//!   team's other connected repositories before it answers "nothing"
//!   ([`search_others`]), so the trace doesn't stop at the repository
//!   boundary;
//! - the first time the planner reads another repository, it gets that
//!   repository's own instruction files (AGENTS.md, CLAUDE.md, ...) and the
//!   team's instructions for it ([`instructions_note`]), the same as the
//!   run's own repository.
//!
//! Writes never come from here: the fix goes where the plan's decision puts
//! it (see `fix_location`).

use std::collections::HashSet;
use std::sync::Mutex;

/// The most other repositories a fallback search reads.
pub const MAX_FALLBACK_REPOS: usize = 8;
/// The most hits shown per repository in a fallback search.
const MAX_HITS_PER_REPO: usize = 10;
/// A consulted repository's instructions, at most.
pub const MAX_INSTRUCTION_CHARS: usize = 6_000;

/// The planner's line about the team's other repositories.
pub const PLANNER_LINE: &str = "\n\nThe team's other repositories are part of the investigation, \
     read-only. When the code that produces the data a ticket is about (a sync, an indexer, a \
     pipeline job) isn't in this repository, it is usually in one of them: search them by the \
     same names before you call that step unknown. A search here that finds nothing also \
     searches them. The first read of another repository shows its own instructions \
     (AGENTS.md, CLAUDE.md and the team's instructions for it): follow them for that \
     repository. In your plan, say which repository holds which layer of the data.";

/// A code search over one repository, as the fallback needs it: each hit
/// as text (`path` and its matching lines).
#[async_trait::async_trait]
pub trait RepoSearch: Send + Sync {
    async fn search(&self, repo: &str, query: &str) -> Result<Vec<String>, String>;
}

/// What a search in the run's repository that found nothing finds in the
/// team's other repositories: each hit under its repository, and which were
/// searched. `None` when there are no others or none has a hit.
pub async fn search_others(
    s: &dyn RepoSearch,
    own: &str,
    repos: &[String],
    query: &str,
) -> Option<(String, Vec<String>)> {
    let others: Vec<&String> = repos
        .iter()
        .filter(|r| !r.eq_ignore_ascii_case(own))
        .take(MAX_FALLBACK_REPOS)
        .collect();
    if others.is_empty() {
        return None;
    }
    let mut found: Vec<(String, Vec<String>)> = Vec::new();
    for r in &others {
        match s.search(r, query).await {
            Ok(hits) if !hits.is_empty() => found.push(((*r).clone(), hits)),
            Ok(_) => {}
            Err(e) => tracing::warn!(repo = %r, error = %e, "fallback code search failed"),
        }
    }
    if found.is_empty() {
        return None;
    }
    let searched: Vec<String> = others.iter().map(|r| r.to_string()).collect();
    let mut out = format!(
        "No match in {own}. Found in the team's other repositories (read-only; pass `repo` to read \
         these files):\n"
    );
    let mut hit_repos = Vec::new();
    for (repo, hits) in found {
        out.push_str(&format!("\n## {repo}\n"));
        for h in hits.iter().take(MAX_HITS_PER_REPO) {
            out.push_str(h);
            out.push_str("\n---\n");
        }
        hit_repos.push(repo);
    }
    out.push_str(&format!("\nSearched: {}", searched.join(", ")));
    Some((out, hit_repos))
}

/// The note shown with the first read of another repository: its own
/// instruction files and the team's instructions for it. Empty when it has
/// neither.
pub fn instructions_note(repo: &str, files: &str, team: &str) -> String {
    let files = files.trim();
    let team = team.trim();
    if files.is_empty() && team.is_empty() {
        return String::new();
    }
    let mut out = format!(
        "## Instructions for {repo} (first read of this repository; follow them for its code)\n"
    );
    if !files.is_empty() {
        out.push_str(&format!(
            "\n### Its instruction files\n{}\n",
            common::truncate_str(files, MAX_INSTRUCTION_CHARS)
        ));
    }
    if !team.is_empty() {
        out.push_str(&format!(
            "\n### Your team's instructions for it\n{}\n",
            common::truncate_str(team, MAX_INSTRUCTION_CHARS / 2)
        ));
    }
    out.push_str("\n---\n\n");
    out
}

/// The repositories the planner has read in this run, beyond its own.
#[derive(Debug, Default)]
pub struct Consulted {
    seen: Mutex<HashSet<String>>,
}

impl Consulted {
    /// True the first time `repo` is read (case-insensitive).
    pub fn first(&self, repo: &str) -> bool {
        self.seen
            .lock()
            .map(|mut s| s.insert(repo.to_ascii_lowercase()))
            .unwrap_or(false)
    }

    /// Every repository read so far, sorted.
    pub fn repos(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .seen
            .lock()
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        v.sort();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Fake {
        hits: HashMap<&'static str, Vec<&'static str>>,
        calls: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl RepoSearch for Fake {
        async fn search(&self, repo: &str, query: &str) -> Result<Vec<String>, String> {
            self.calls.lock().unwrap().push(format!("{repo} {query}"));
            if repo == "acme/broken" {
                return Err("rate limited".into());
            }
            Ok(self
                .hits
                .get(repo)
                .map(|h| h.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default())
        }
    }

    fn repos() -> Vec<String> {
        ["acme/web", "acme/broken", "acme/api", "acme/pipelines"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[tokio::test]
    async fn a_search_that_finds_nothing_here_searches_the_teams_other_repos() {
        let fake = Fake {
            hits: HashMap::from([(
                "acme/pipelines",
                vec!["indexer/build.py\n42: def build_search_index(rows):"],
            )]),
            calls: Mutex::new(Vec::new()),
        };
        let (text, hit) = search_others(&fake, "acme/web", &repos(), "build_search_index")
            .await
            .expect("found in the pipelines repo");
        assert_eq!(hit, vec!["acme/pipelines"]);
        assert!(text.contains("No match in acme/web"), "{text}");
        assert!(
            text.contains("## acme/pipelines\nindexer/build.py"),
            "{text}"
        );
        assert!(text.contains("Searched: acme/broken, acme/api, acme/pipelines"));
        // Its own repository is never searched again; a failing one doesn't
        // stop the others.
        let calls = fake.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert!(!calls.iter().any(|c| c.starts_with("acme/web ")));
    }

    #[tokio::test]
    async fn nothing_anywhere_or_no_other_repos_is_none() {
        let fake = Fake {
            hits: HashMap::new(),
            calls: Mutex::new(Vec::new()),
        };
        assert!(search_others(&fake, "acme/web", &repos(), "x")
            .await
            .is_none());
        assert!(
            search_others(&fake, "acme/web", &["acme/web".to_string()], "x")
                .await
                .is_none()
        );
        assert!(search_others(&fake, "acme/web", &[], "x").await.is_none());
    }

    #[test]
    fn a_consulted_repo_brings_its_own_instructions_once() {
        let c = Consulted::default();
        assert!(c.first("acme/pipelines"));
        assert!(!c.first("Acme/Pipelines"));
        assert!(c.first("acme/api"));
        assert_eq!(c.repos(), vec!["acme/api", "acme/pipelines"]);

        let note = instructions_note(
            "acme/pipelines",
            "### AGENTS.md\n\nThe indexer copies start dates from the schedule.",
            "Pipeline changes need a backfill note.",
        );
        assert!(
            note.contains("## Instructions for acme/pipelines"),
            "{note}"
        );
        assert!(note.contains("The indexer copies start dates"), "{note}");
        assert!(note.contains("Your team's instructions for it\nPipeline changes"));
        assert_eq!(instructions_note("acme/api", " ", ""), "");
        let only_team = instructions_note("acme/api", "", "Owns bookings.");
        assert!(only_team.contains("Owns bookings.") && !only_team.contains("instruction files"));
        assert!(PLANNER_LINE.contains("say which repository holds which layer"));
    }
}
