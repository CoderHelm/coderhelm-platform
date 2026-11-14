//! The repository's own form for pull request titles and commit subjects,
//! read once per run and applied to both (`common::pr_conventions`).
//!
//! A run with a ticket has always written `{ticket}: {title}` or the team's
//! template. A run without one (a security update) wrote `fix(deps): ...`
//! whatever the repository did, and its commits could carry a sample key.
//! Here the form is detected: the team's template, the repository's guide,
//! its lint and title-check configuration, then what people merged. The
//! evidence is kept on the run and shown in the description's collapsed
//! part.
//!
//! When the form needs a ticket key and the run has none, the key is a real
//! one or there is none: one already made for this fix, or a new ticket in
//! the connected tracker when the team allows that ([`may_create_ticket`]).
//! Otherwise the closest valid form is used and the pull request says so
//! once. A key is never invented.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use common::git_conventions::GitConventions;
use common::pr_conventions::{self as pc, Convention, Form, Inputs, Key, Kind, Sample, Written};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// How long a repository's detected form is kept in this process.
const KEEP: Duration = Duration::from_secs(3600);

/// The setting (on the team's git conventions record) that lets CoderHelm
/// create a tracker ticket for a run that has none, when the repository's
/// titles need a key. Off unless a team turns it on.
pub const CREATE_TICKET_ATTR: &str = "create_ticket";
/// On a security ticket's record: the tracker ticket made for it.
pub const TRACKER_KEY_ATTR: &str = "tracker_key";
pub const TRACKER_URL_ATTR: &str = "tracker_url";

type Kept = HashMap<String, (Instant, Convention, Convention)>;
static DETECTED: Mutex<Option<Kept>> = Mutex::new(None);

/// A run's title and commit form, and the key they may carry.
#[derive(Debug, Clone)]
pub struct ForRun {
    pub title: Convention,
    pub commit: Convention,
    pub key: Key,
}

impl ForRun {
    /// `was` (a title in whatever form it was first written) in the
    /// repository's form.
    pub fn title(&self, was: &str) -> Written {
        pc::subject(&self.title, &self.key, was)
    }

    /// `was` (a commit message) with its subject in the repository's form
    /// and its body kept.
    pub fn commit(&self, was: &str) -> String {
        match was.split_once('\n') {
            Some((first, body)) => {
                format!(
                    "{}\n{body}",
                    pc::subject(&self.commit, &self.key, first).text
                )
            }
            None => pc::subject(&self.commit, &self.key, was).text,
        }
    }

    /// What the form was read from, in words.
    pub fn evidence(&self) -> String {
        if self.title.evidence == self.commit.evidence {
            self.title.evidence.clone()
        } else {
            format!(
                "{} (titles) and {} (commits)",
                self.title.evidence, self.commit.evidence
            )
        }
    }
}

fn is_bot(login: &str, kind: &str) -> bool {
    kind.eq_ignore_ascii_case("bot") || login.ends_with("[bot]") || super::is_own_login(login)
}

/// Reads what a detection needs from the repository: the lint and title
/// check configuration, the recent merged pull requests and base commits.
async fn read(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    guide: &str,
    team: &GitConventions,
) -> (Convention, Convention) {
    let mut commitlint: Option<(String, String)> = None;
    if let Ok(root) = github.list_directory(owner, repo, "", base).await {
        if let Some(f) = root.iter().find(|e| {
            e.name.starts_with(".commitlintrc") || e.name.starts_with("commitlint.config")
        }) {
            if let Ok(text) = github.read_file(owner, repo, &f.path, base).await {
                commitlint = Some((f.path.clone(), text));
            }
        }
    }
    let mut title_checks: Vec<(String, String)> = Vec::new();
    if let Ok(flows) = github
        .list_directory(owner, repo, ".github/workflows", base)
        .await
    {
        for f in flows
            .iter()
            .filter(|e| {
                let n = e.name.to_lowercase();
                ["title", "semantic", "lint", "commit", "pr-"]
                    .iter()
                    .any(|w| n.contains(w))
            })
            .take(4)
        {
            if let Ok(text) = github.read_file(owner, repo, &f.path, base).await {
                if text.contains("pull_request") && text.to_lowercase().contains("title") {
                    title_checks.push((f.path.clone(), text));
                }
            }
        }
    }
    let merged: Vec<Sample> = github
        .list_pull_requests(owner, repo, "closed", 60)
        .await
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter(|p| !p["merged_at"].is_null())
        .map(|p| Sample {
            text: p["title"].as_str().unwrap_or("").to_string(),
            bot: is_bot(
                p["user"]["login"].as_str().unwrap_or(""),
                p["user"]["type"].as_str().unwrap_or(""),
            ),
        })
        .collect();
    let commits: Vec<Sample> = github
        .recent_commits(owner, repo, base, 60)
        .await
        .unwrap_or_default()
        .iter()
        .map(|c| Sample {
            text: c["commit"]["message"]
                .as_str()
                .unwrap_or("")
                .lines()
                .next()
                .unwrap_or("")
                .to_string(),
            bot: is_bot(
                c["author"]["login"].as_str().unwrap_or(""),
                c["author"]["type"].as_str().unwrap_or(""),
            ),
        })
        .collect();
    let checks: Vec<(&str, &str)> = title_checks
        .iter()
        .map(|(p, t)| (p.as_str(), t.as_str()))
        .collect();
    let inputs = |template: bool| Inputs {
        team_template: template,
        guide,
        commitlint: commitlint.as_ref().map(|(p, t)| (p.as_str(), t.as_str())),
        title_checks: checks.clone(),
        merged: &merged,
        commits: &commits,
    };
    (
        pc::detect(Kind::Title, &inputs(team.pr_title.is_some())),
        pc::detect(Kind::Commit, &inputs(team.commit.is_some())),
    )
}

/// The repository's title and commit form (kept for an hour per process).
pub async fn detect(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    team: &GitConventions,
) -> (Convention, Convention) {
    let key = format!(
        "{owner}/{repo}@{base}#{}{}",
        team.pr_title.is_some(),
        team.commit.is_some()
    );
    if let Ok(g) = DETECTED.lock() {
        if let Some((at, t, c)) = g.as_ref().and_then(|m| m.get(&key)) {
            if at.elapsed() < KEEP {
                return (t.clone(), c.clone());
            }
        }
    }
    let guide = super::load_repo_instructions_at_ref(github, owner, repo, base).await;
    let found = read(github, owner, repo, base, &guide, team).await;
    info!(
        repo = %format!("{owner}/{repo}"),
        title = ?found.0.form,
        title_from = %found.0.evidence,
        commit = ?found.1.form,
        commit_from = %found.1.evidence,
        "Read the repository's title and commit form"
    );
    if let Ok(mut g) = DETECTED.lock() {
        g.get_or_insert_with(HashMap::new)
            .insert(key, (Instant::now(), found.0.clone(), found.1.clone()));
    }
    found
}

async fn settings_item(
    state: &WorkerState,
    team_id: &str,
    sk: &str,
) -> Option<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(sk))
        .send()
        .await
        .ok()?
        .item
}

/// Whether the team lets CoderHelm create a tracker ticket for a run that
/// has none. Off unless it is set.
pub async fn may_create_ticket(state: &WorkerState, team_id: &str) -> bool {
    settings_item(state, team_id, common::git_conventions::SETTINGS_SK)
        .await
        .and_then(|i| {
            i.get(CREATE_TICKET_ATTR)
                .and_then(|v| v.as_bool().ok())
                .copied()
        })
        .unwrap_or(false)
}

async fn jira_config(state: &WorkerState, team_id: &str) -> HashMap<String, String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s("JIRA#config"))
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .map(|i| {
            i.into_iter()
                .filter_map(|(k, v)| v.as_s().ok().map(|s| (k, s.clone())))
                .collect()
        })
        .unwrap_or_default()
}

/// Whether `key` exists in the connected tracker: `None` when no tracker
/// could be asked.
pub async fn in_tracker(state: &WorkerState, team_id: &str, key: &str) -> Option<bool> {
    let cfg = jira_config(state, team_id).await;
    let (url, secret) = (cfg.get("get_issues_url")?, cfg.get("forge_secret")?);
    let resp = state
        .http
        .post(url)
        .json(&json!({ "forge_secret": secret, "keys": [key] }))
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .ok()?;
    let body: Value = resp.json().await.ok()?;
    let found = body["issues"].as_array()?.iter().any(|i| {
        i["key"]
            .as_str()
            .is_some_and(|k| k.eq_ignore_ascii_case(key))
    });
    Some(found)
}

/// Creates a ticket for a run that has none: `(key, url)`.
async fn create_ticket(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    summary: &str,
    description: &str,
) -> Result<(String, String), String> {
    let cfg = jira_config(state, team_id).await;
    let url = cfg
        .get("create_ticket_url")
        .ok_or("no tracker that takes new tickets is connected")?;
    let project = cfg
        .get("default_project")
        .filter(|p| !p.is_empty())
        .ok_or("the connected tracker has no default project for new tickets")?;
    let resp = state
        .http
        .post(url)
        .header("Content-Type", "application/json")
        .body(
            json!({
                "projectKey": project,
                "summary": summary,
                "description": description,
                "labels": ["coderhelm", format!("coderhelm:{repo}")],
            })
            .to_string(),
        )
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .map_err(|_| "the tracker did not answer".to_string())?;
    if !resp.status().is_success() {
        return Err(format!(
            "the tracker refused the new ticket ({})",
            resp.status()
        ));
    }
    let body: Value = resp.json().await.unwrap_or_default();
    let key = body["key"]
        .as_str()
        .filter(|k| common::git_conventions::is_ticket_key(k))
        .ok_or("the tracker returned no ticket key")?
        .to_string();
    let link = cfg
        .get("site_url")
        .map(|s| format!("{}/browse/{key}", s.trim_end_matches('/')))
        .unwrap_or_default();
    Ok((key, link))
}

/// The key a run without a ticket may write, when the form needs one: the
/// ticket already made for this fix (checked in the tracker), a new one
/// when the team allows it, or none with why.
async fn key_for_keyless(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    ticket_id: &str,
    summary: &str,
) -> Key {
    let sk = common::security_fixes::ticket_sk(ticket_id);
    let item = settings_item(state, team_id, &sk).await.unwrap_or_default();
    let kept = item
        .get(TRACKER_KEY_ATTR)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    if !kept.is_empty() {
        let exists = in_tracker(state, team_id, &kept).await;
        if pc::real_key(&kept, None, exists) {
            return Key::Real(kept);
        }
    }
    if !may_create_ticket(state, team_id).await {
        return Key::None(
            "creating a ticket for a run that has none is off in the team's settings".into(),
        );
    }
    let description = format!(
        "CoderHelm opened a pull request in {repo} that has no ticket: {summary}. This ticket exists so its title and commits can carry a key, as the repository's do."
    );
    match create_ticket(state, team_id, repo, summary, &description).await {
        Ok((key, url)) => {
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.settings_table_name)
                .key("pk", super::attr_s(team_id))
                .key("sk", super::attr_s(&sk))
                .update_expression("SET #k = :k, #u = :u")
                .expression_attribute_names("#k", TRACKER_KEY_ATTR)
                .expression_attribute_names("#u", TRACKER_URL_ATTR)
                .expression_attribute_values(":k", super::attr_s(&key))
                .expression_attribute_values(":u", super::attr_s(&url))
                .send()
                .await;
            info!(ticket_id, key = %key, "Created a tracker ticket for a run that had none");
            Key::Real(key)
        }
        Err(why) => {
            warn!(
                ticket_id,
                why, "Could not create a tracker ticket; the title carries no key"
            );
            Key::None(why)
        }
    }
}

/// Everything a run needs to write its title and commits in the
/// repository's form. `run_key`: the run's own tracker key, when its ticket
/// has one.
#[allow(clippy::too_many_arguments)]
pub async fn for_run(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    base: &str,
    ticket_id: &str,
    run_key: Option<&str>,
    summary: &str,
    team: &GitConventions,
) -> ForRun {
    let (title, commit) = detect(github, owner, repo, base, team).await;
    let needs_key =
        matches!(title.form, Form::TicketKey(_)) || matches!(commit.form, Form::TicketKey(_));
    let key = match run_key {
        Some(k) => Key::Real(k.to_string()),
        None if needs_key && common::security_fixes::is_ticket(ticket_id) => {
            key_for_keyless(
                state,
                team_id,
                &format!("{owner}/{repo}"),
                ticket_id,
                summary,
            )
            .await
        }
        None => Key::None("this run has no ticket in a tracker".into()),
    };
    ForRun { title, commit, key }
}

/// A commit message a run writes, in the repository's form: the team's
/// template when it set one, the run's own ticket key as always when it has
/// a ticket, and for a run without one (a security update) the form the
/// repository's history and configuration show.
pub async fn commit_for(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &crate::models::TicketMessage,
    was: &str,
) -> String {
    let team = super::load_git_conventions(state, &msg.team_id, &msg.ticket_id).await;
    let tokens = super::ticket_tokens(msg);
    if team.commit.is_some() || !common::security_fixes::is_ticket(&msg.ticket_id) {
        return team.commit_message(&tokens, was);
    }
    for_run(
        state,
        github,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        &msg.base_branch,
        &msg.ticket_id,
        None,
        &msg.title,
        &team,
    )
    .await
    .commit(was)
}

/// Keeps what the form was read from, and what the pull request says once
/// about it, on the run (`description` shows both).
pub async fn record(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    c: &ForRun,
    note: Option<&str>,
) {
    if run_id.is_empty() {
        return;
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression("SET #e = :e, #n = :n")
        .expression_attribute_names("#e", super::description::CONVENTION_EVIDENCE)
        .expression_attribute_names("#n", super::description::CONVENTION_NOTE)
        .expression_attribute_values(":e", super::attr_s(&c.evidence()))
        .expression_attribute_values(":n", super::attr_s(note.unwrap_or("")))
        .send()
        .await;
}

/// Whether a run with its own ticket keeps the title it always had: yes,
/// unless the repository states or enforces Conventional Commits.
pub fn keeps_ticket_form(c: &Convention) -> bool {
    !(c.form == Form::Conventional && c.source <= pc::Source::LintConfig)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::pr_conventions::{Sep, Source};

    fn keyed() -> Convention {
        Convention {
            form: Form::TicketKey(Sep::Colon),
            source: Source::MergedPullRequests,
            evidence: "24 of the last 30 merged pull requests written by people".into(),
        }
    }

    #[test]
    fn a_keyless_run_writes_the_closest_valid_form_and_says_so_once() {
        let run = ForRun {
            title: keyed(),
            commit: keyed(),
            key: Key::None(
                "creating a ticket for a run that has none is off in the team's settings".into(),
            ),
        };
        let w = run.title("fix(deps): security updates for web (14 packages, 31 advisories)");
        assert_eq!(
            w.text,
            "Security updates for web (14 packages, 31 advisories)"
        );
        assert!(w.note.unwrap().contains("is off in the team's settings"));
        assert_eq!(
            run.commit("fix(deps): glob-walk 2.1.0 to 2.1.4\n\nbody stays"),
            "Glob-walk 2.1.0 to 2.1.4\n\nbody stays"
        );
        // With a real key, both carry it.
        let run = ForRun {
            key: Key::Real("OPS-431".into()),
            ..run
        };
        assert_eq!(
            run.title("fix(deps): security updates for web").text,
            "OPS-431: Security updates for web"
        );
        assert_eq!(
            run.commit("KEY-1: fix(deps): glob-walk 2.1.0 to 2.1.4"),
            "OPS-431: Glob-walk 2.1.0 to 2.1.4"
        );
        assert_eq!(run.evidence(), keyed().evidence);
    }

    #[test]
    fn a_run_with_a_ticket_keeps_its_form_unless_the_repository_says_otherwise() {
        assert!(keeps_ticket_form(&keyed()));
        let inferred = Convention {
            form: Form::Conventional,
            source: Source::BaseCommits,
            evidence: "x".into(),
        };
        assert!(keeps_ticket_form(&inferred));
        let enforced = Convention {
            source: Source::LintConfig,
            ..inferred
        };
        assert!(!keeps_ticket_form(&enforced));
    }
}
