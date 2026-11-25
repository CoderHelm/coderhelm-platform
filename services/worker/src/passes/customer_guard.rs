//! The customer details guard on one pull request of a public repository
//! (`common::customer_guard`): reads its title, body, commit messages and
//! the lines it adds under `docs/` and in test and incident fixtures, and
//! sets the "Customer details guard" check run on its head.
//!
//! The terms are read from the install's own records and kept in this
//! process's memory only, for a few minutes. Nothing here logs a term or
//! the text around one: a hit is logged by its kind and place.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::customer_guard::{self as guard, records, InstallRecords, Source, Verdict};
use common::messages::{CustomerGuardMessage, WorkerMessage};
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// How long the records read for one check serve the next ones.
const RECORDS_FOR: Duration = Duration::from_secs(300);

/// Reads of a pull request whose text changed while it was being checked:
/// it is read again, up to this many times, so the check always speaks for
/// the text it ends on.
const MAX_READS: usize = 3;

static RECORDS: Mutex<Option<(Instant, Arc<InstallRecords>)>> = Mutex::new(None);

fn tables(state: &WorkerState) -> records::Tables {
    let c = &state.config;
    records::Tables {
        main: c.table_name.clone(),
        teams: c.teams_table_name.clone(),
        users: c.users_table_name.clone(),
        repos: c.repos_table_name.clone(),
        jira_config: c.jira_config_table_name.clone(),
        settings: c.settings_table_name.clone(),
        aws_insights: c.aws_insights_table_name.clone(),
    }
}

/// The install's records, read at most every few minutes, held in memory.
async fn install_records(state: &WorkerState) -> Result<Arc<InstallRecords>, Error> {
    if let Some((at, r)) = RECORDS.lock().ok().and_then(|g| g.clone()) {
        if at.elapsed() < RECORDS_FOR {
            return Ok(r);
        }
    }
    let r = Arc::new(records::load(&state.dynamo, &tables(state)).await?);
    if let Ok(mut g) = RECORDS.lock() {
        *g = Some((Instant::now(), r.clone()));
    }
    Ok(r)
}

/// Pure: the texts of a pull request the guard reads, from GitHub's pull
/// request, its commits and its files. `full` gives the content of a file
/// GitHub sent no patch for (too large): every line of it is read.
pub fn sources(
    pr: &serde_json::Value,
    commits: &[serde_json::Value],
    files: &[serde_json::Value],
    full: &dyn Fn(&str) -> Option<String>,
) -> Vec<Source> {
    let mut out = vec![
        Source::Title(pr["title"].as_str().unwrap_or("").to_string()),
        Source::Body(pr["body"].as_str().unwrap_or("").to_string()),
    ];
    for c in commits {
        out.push(Source::Commit {
            sha: c["sha"].as_str().unwrap_or("").to_string(),
            message: c["commit"]["message"].as_str().unwrap_or("").to_string(),
        });
    }
    for f in files {
        let path = f["filename"].as_str().unwrap_or("");
        if path.is_empty() || !guard::reads_file(path) || f["status"].as_str() == Some("removed") {
            continue;
        }
        let lines = match f["patch"].as_str() {
            Some(p) => guard::added_lines(p),
            None if f["changes"].as_u64().unwrap_or(0) > 0 => full(path)
                .map(|t| {
                    t.lines()
                        .enumerate()
                        .map(|(i, l)| (i + 1, l.to_string()))
                        .collect()
                })
                .unwrap_or_default(),
            None => Vec::new(),
        };
        if !lines.is_empty() {
            out.push(Source::Added {
                path: path.to_string(),
                lines,
            });
        }
    }
    out
}

/// Pure: the verdict for a pull request from where it came from: skipped
/// (without reading a word of it) unless [`guard::trusted`], else from its
/// hits.
pub fn decide(
    origin: &guard::Origin,
    read: impl FnOnce() -> Vec<Source>,
    terms: &guard::TermSet,
) -> (Verdict, Vec<guard::Hit>) {
    if !guard::trusted(origin) {
        return (guard::skipped(), Vec::new());
    }
    let hits = guard::scan(&read(), terms);
    (guard::verdict(&hits), hits)
}

/// Where a pull request came from, with the author's write access read from
/// GitHub (only for a branch of the repository itself: a fork is never
/// trusted, so it costs no request).
async fn origin(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr: &serde_json::Value,
) -> guard::Origin {
    let mut o = guard::Origin::from_pr(pr);
    if guard::same_repo(&o) {
        o.author_can_write = github.has_write_access(owner, repo, &o.author_login).await;
    }
    o
}

/// Pure: has the pull request's text or head changed between two reads.
fn moved(before: &serde_json::Value, after: &serde_json::Value) -> bool {
    before["head"]["sha"] != after["head"]["sha"]
        || before["title"] != after["title"]
        || before["body"] != after["body"]
}

/// Tries of one event before it is left to the next event on the pull
/// request (an edit or a push); the merge stays held meanwhile.
const MAX_ATTEMPTS: u32 = 8;

/// Pure: the wait before the next try.
pub fn retry_delay(attempt: u32) -> i32 {
    (30i32 << attempt.min(5)).min(900)
}

/// Runs the guard; a failure is sent again after a delay.
pub async fn run_or_retry(state: &WorkerState, msg: &CustomerGuardMessage) {
    let Err(e) = run(state, msg).await else {
        return;
    };
    let attempt = msg.attempt + 1;
    warn!(pr = msg.pr_number, attempt, error = %e, "Customer details guard: could not set the check; trying again");
    if attempt >= MAX_ATTEMPTS {
        return;
    }
    let next = WorkerMessage::CustomerGuard(CustomerGuardMessage {
        attempt,
        ..msg.clone()
    });
    let Ok(body) = serde_json::to_string(&next) else {
        return;
    };
    if let Err(e) = state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(retry_delay(msg.attempt))
        .send()
        .await
    {
        warn!(pr = msg.pr_number, error = %e, "Customer details guard: could not queue the next try");
    }
}

pub async fn run(state: &WorkerState, msg: &CustomerGuardMessage) -> Result<(), Error> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let (owner, repo, n) = (&msg.repo_owner, &msg.repo_name, msg.pr_number);
    let mut pr = github.get_pull_request(owner, repo, n).await?;
    // Private repositories publish nothing, and the hosted side checks only
    // CoderHelm's own repositories: no check anywhere else.
    if pr["base"]["repo"]["private"].as_bool() != Some(false) || pr["state"] == "closed" {
        return Ok(());
    }
    if !github.customer_guard_checks(owner).await? {
        return Ok(());
    }
    for _ in 0..MAX_READS {
        let head = pr["head"]["sha"].as_str().unwrap_or("").to_string();
        if head.is_empty() {
            return Ok(());
        }
        let from = origin(&github, owner, repo, &pr).await;
        let (verdict, hits) = if guard::trusted(&from) {
            let records = install_records(state).await?;
            let author = pr["user"]["login"].as_str().unwrap_or("");
            let terms = guard::derive(&records, &[owner.as_str(), author]);
            let commits = github.list_pr_commits_all(owner, repo, n).await?;
            let files = github.list_pull_files_all(owner, repo, n).await?;
            let mut large = std::collections::HashMap::new();
            for f in &files {
                let path = f["filename"].as_str().unwrap_or("");
                // GitHub sends no patch for a large file: it is read whole,
                // and a failed read fails the check (tried again), never
                // skips the file.
                if f["patch"].is_null()
                    && guard::reads_file(path)
                    && f["status"] != "removed"
                    && f["changes"].as_u64().unwrap_or(0) > 0
                {
                    let t = github.read_file_live(owner, repo, path, &head).await?;
                    large.insert(path.to_string(), t);
                }
            }
            let full = |p: &str| large.get(p).cloned();
            decide(&from, || sources(&pr, &commits, &files, &full), &terms)
        } else {
            decide(&from, Vec::new, &guard::TermSet::default())
        };
        let again = github.get_pull_request(owner, repo, n).await?;
        if moved(&pr, &again) {
            pr = again;
            continue;
        }
        for h in &hits {
            // The kind and the place, never the text.
            info!(pr = n, kind = h.kind.as_str(), place = %h.place.describe(), "Customer details guard: a line carries a customer detail");
        }
        github
            .create_check_run(owner, repo, &guard::check_body(&head, &verdict))
            .await?;
        info!(
            pr = n,
            conclusion = verdict.conclusion,
            lines = hits.len(),
            "Customer details guard: check set"
        );
        return Ok(());
    }
    warn!(
        pr = n,
        "Customer details guard: the pull request kept changing; the next event checks it"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::customer_guard::{InstallRecords, TeamRecords};
    use serde_json::json;

    /// A synthetic install: one customer team with an invented organization,
    /// and the repository's own team.
    fn install() -> InstallRecords {
        InstallRecords {
            teams: vec![
                TeamRecords {
                    orgs: vec!["Quorvex-Labs".into()],
                    repos: vec!["quorvex-ledger".into()],
                    ..Default::default()
                },
                TeamRecords {
                    orgs: vec!["example-oss".into()],
                    ..Default::default()
                },
            ],
            install: TeamRecords::default(),
        }
    }

    fn pr(body: &str, head_repo: &str, assoc: &str) -> serde_json::Value {
        json!({
            "title": "Retry the sync",
            "body": body,
            "state": "open",
            "author_association": assoc,
            "user": {"login": "someone", "type": "User"},
            "head": {"sha": "abc1234def", "repo": {"full_name": head_repo}},
            "base": {"repo": {"full_name": "example-oss/tool", "private": false}},
        })
    }

    #[test]
    fn a_fork_gets_the_same_output_whatever_its_text_says() {
        let terms = guard::derive(&install(), &["example-oss"]);
        let with = pr("Fixes the sync for Quorvex Labs", "stranger/tool", "NONE");
        let without = pr("Fixes the sync", "stranger/tool", "NONE");
        let a = decide(
            &guard::Origin::from_pr(&with),
            || panic!("a fork's text is never read"),
            &terms,
        )
        .0;
        let b = decide(
            &guard::Origin::from_pr(&without),
            || panic!("a fork's text is never read"),
            &terms,
        )
        .0;
        assert_eq!(a, b);
        assert_eq!(
            guard::check_body("abc1234def", &a),
            guard::check_body("abc1234def", &b)
        );
        assert_eq!(a.conclusion, "neutral");
        // A member's branch of the repository itself is checked.
        let member = pr(
            "Fixes the sync for Quorvex Labs",
            "example-oss/tool",
            "MEMBER",
        );
        let read = || sources(&member, &[], &[], &|_| None);
        assert_eq!(
            decide(&guard::Origin::from_pr(&member), read, &terms)
                .0
                .conclusion,
            "failure"
        );
    }

    #[test]
    fn an_outside_author_on_a_branch_of_the_repo_is_skipped() {
        let terms = guard::derive(&install(), &["example-oss"]);
        let p = pr("For Quorvex Labs", "example-oss/tool", "CONTRIBUTOR");
        assert_eq!(
            decide(&guard::Origin::from_pr(&p), Vec::new, &terms).0,
            guard::skipped()
        );
    }

    #[test]
    fn the_sources_are_the_title_body_commits_and_docs_and_fixture_additions() {
        let p = pr("body", "example-oss/tool", "MEMBER");
        let commits = vec![json!({"sha": "c0ffee1234", "commit": {"message": "one\ntwo"}})];
        let files = vec![
            json!({"filename": "docs/a.md", "status": "modified", "patch": "@@ -1,0 +4,1 @@\n+added", "changes": 1}),
            json!({"filename": "src/main.rs", "status": "modified", "patch": "@@ -1,0 +1,1 @@\n+code", "changes": 1}),
            json!({"filename": "services/worker/src/passes/incidents/zz_case.rs", "status": "added", "changes": 9}),
            json!({"filename": "docs/gone.md", "status": "removed", "patch": "@@ -1,1 +0,0 @@\n-x", "changes": 1}),
        ];
        let s = sources(&p, &commits, &files, &|path| {
            (path == "services/worker/src/passes/incidents/zz_case.rs").then(|| "x\ny".to_string())
        });
        assert_eq!(s.len(), 5, "{s:?}");
        assert!(s.contains(&Source::Added {
            path: "docs/a.md".into(),
            lines: vec![(4, "added".into())]
        }));
        assert!(s.contains(&Source::Added {
            path: "services/worker/src/passes/incidents/zz_case.rs".into(),
            lines: vec![(1, "x".into()), (2, "y".into())]
        }));
        assert!(!s
            .iter()
            .any(|x| matches!(x, Source::Added { path, .. } if path == "src/main.rs")));
    }

    #[test]
    fn an_edit_between_reads_is_read_again() {
        let a = pr("one", "example-oss/tool", "MEMBER");
        let mut b = a.clone();
        assert!(!moved(&a, &b));
        b["body"] = json!("two");
        assert!(moved(&a, &b));
    }

    #[test]
    fn the_message_is_handled_by_the_worker() {
        let main = include_str!("../main.rs");
        assert!(main.contains("WorkerMessage::CustomerGuard(msg) =>"));
        assert!(main.contains("passes::customer_guard::run_or_retry(&state, &msg)"));
    }

    /// Nothing in this pass logs a term or the text: every log line here
    /// names only the pull request number, a kind, a place or a count.
    #[test]
    fn the_pass_logs_no_text() {
        let src = include_str!("customer_guard.rs");
        let code = src.split("#[cfg(test)]").next().unwrap();
        for line in code
            .lines()
            .filter(|l| l.contains("info!(") || l.contains("warn!("))
        {
            for banned in ["title", "body", "message", "joined", "term =", "text"] {
                assert!(
                    !line.contains(&format!("{banned} =")) && !line.contains(&format!("%{banned}")),
                    "{line}"
                );
            }
        }
    }
}
