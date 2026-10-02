//! Release notes for a release tag (opt-in per repo).
//!
//! When CoderHelm cuts a release tag on the repo's release branch, this pass:
//!   1. gathers everything since the previous release tag — the merged PRs
//!      (title, body, labels, branch) and the Jira tickets they reference;
//!   2. asks the heavy model for two write-ups, following the repo's own release
//!      notes guide when it has one (e.g. a `changelog-deploy` skill):
//!        - technical GitHub Release notes (engineers), and
//!        - a business-facing changelog entry (ops / IT / leadership);
//!   3. publishes: the GitHub Release for the tag, the entry prepended to the
//!      repo's Confluence changelog (through the CoderHelm Jira app), and the
//!      entry POSTed to the team's email webhook (e.g. an Atlassian Automation
//!      rule that emails the IT distro).
//!
//! The release tag is the approval: CoderHelm only tags after an approved,
//! green merge. Writing rules carried over from the team's skill: never invent a
//! description for a change the sources don't explain — such changes are listed
//! under "Other changes" with their title as written.
//!
//! Every step's result is recorded per tag (RELEASE#{owner}/{repo}#{tag}), so a
//! retry or a dashboard re-send only redoes what is missing. A failed step posts
//! to the repo's Teams webhook when one is configured.

use aws_sdk_dynamodb::types::AttributeValue;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use tracing::{info, warn};

use super::{attr_n, attr_s};
use crate::agent::provider::{self, ModelProvider};
use crate::clients::github::GitHubClient;
use crate::models::{ReleaseNotesMessage, TokenUsage};
use crate::WorkerState;

const MAX_PRS: usize = 80;
const MAX_COMMITS: usize = 500;

/// Files a repo may keep its release-notes writing guide in, checked in order.
/// `release_notes_guide` in the repo config overrides this list.
const GUIDE_CANDIDATES: &[&str] = &[
    ".claude/skills/changelog-deploy/SKILL.md",
    ".claude/skills/release-notes/SKILL.md",
    ".claude/skills/changelog/SKILL.md",
    "RELEASE_NOTES.md",
    "docs/release-notes.md",
    ".github/release-notes.md",
];

/// Per-repo release-notes config (REVIEW_CONFIG#REPO item).
#[derive(Debug, Clone, Default)]
pub struct ReleaseNotesConfig {
    pub enabled: bool,
    /// Only tags cut on merges into this branch produce notes. Empty = the
    /// repo's default branch.
    pub branch: String,
    pub confluence_space: String,
    /// Page the changelog lives under (its id). Empty = no Confluence step.
    pub confluence_parent_id: String,
    /// Container page title under the parent (one child page per year).
    pub confluence_container: String,
    /// HTTPS webhook that sends the email (e.g. Atlassian Automation). Empty =
    /// no email step.
    pub email_webhook_url: String,
    /// Repo path of the writing guide. Empty = auto-detect (GUIDE_CANDIDATES).
    pub guide_path: String,
    /// Extra free-text instructions (audience, tone, product name…).
    pub instructions: String,
    pub teams_webhook_url: String,
}

impl ReleaseNotesConfig {
    pub async fn load(state: &WorkerState, team_id: &str, owner: &str, repo: &str) -> Self {
        let sk = format!("REVIEW_CONFIG#REPO#{owner}/{repo}");
        let Some(item) = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(team_id))
            .key("sk", attr_s(&sk))
            .send()
            .await
            .ok()
            .and_then(|o| o.item().cloned())
        else {
            return Self::default();
        };
        let s = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        let b = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false)
        };
        let killed = b("killed");
        let container = s("release_notes_confluence_container");
        Self {
            enabled: b("release_notes") && !killed,
            branch: s("release_notes_branch"),
            confluence_space: s("release_notes_confluence_space"),
            confluence_parent_id: s("release_notes_confluence_parent_id"),
            confluence_container: if container.trim().is_empty() {
                "Changelog".to_string()
            } else {
                container
            },
            email_webhook_url: s("release_notes_email_webhook_url"),
            guide_path: s("release_notes_guide"),
            instructions: s("release_notes_instructions"),
            teams_webhook_url: s("teams_webhook_url"),
        }
    }
}

/// Called right after CoderHelm cuts a release tag. Enqueues the notes job when
/// the repo has release notes on and the tag is on its release branch.
#[allow(clippy::too_many_arguments)]
pub async fn after_tag(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    base_branch: &str,
    tag: &str,
    sha: &str,
) {
    let cfg = ReleaseNotesConfig::load(state, team_id, owner, repo).await;
    if !cfg.enabled {
        return;
    }
    let release_branch = if cfg.branch.trim().is_empty() {
        match github.get_default_branch(owner, repo).await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "Release notes: could not read default branch — skipping");
                return;
            }
        }
    } else {
        cfg.branch.trim().to_string()
    };
    if base_branch != release_branch {
        info!(%tag, base_branch, %release_branch, "Release notes: tag not on the release branch — skipped");
        return;
    }
    enqueue(
        state,
        &ReleaseNotesMessage {
            team_id: team_id.to_string(),
            installation_id,
            repo_owner: owner.to_string(),
            repo_name: repo.to_string(),
            tag: tag.to_string(),
            sha: sha.to_string(),
            resend: String::new(),
            check_branch: false,
        },
    )
    .await;
}

/// The branch whose tags get notes: the configured one, else the default branch.
async fn release_branch(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    cfg: &ReleaseNotesConfig,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if cfg.branch.trim().is_empty() {
        github.get_default_branch(owner, repo).await
    } else {
        Ok(cfg.branch.trim().to_string())
    }
}

/// A tag is a release of `branch` when the branch contains the tag's commit.
pub fn tag_on_branch(compare_status_branch_vs_tag: &str) -> bool {
    matches!(compare_status_branch_vs_tag, "ahead" | "identical")
}

pub async fn enqueue(state: &WorkerState, msg: &ReleaseNotesMessage) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let body = match serde_json::to_value(msg) {
        Ok(mut v) => {
            if let Some(o) = v.as_object_mut() {
                o.insert("type".to_string(), json!("release_notes"));
            }
            v.to_string()
        }
        Err(_) => return false,
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %e, "Release notes: enqueue failed");
            false
        }
    }
}

// ── Gathering ────────────────────────────────────────────────────────────────

/// One merged PR in the release.
#[derive(Debug, Clone)]
pub struct ReleasePr {
    pub number: u64,
    pub title: String,
    pub body: String,
    pub branch: String,
    pub labels: Vec<String>,
    pub author: String,
}

/// The previous release tag: the highest version below `tag` sharing its
/// prefix (semver-aware; falls back to name order for non-version tags).
pub fn previous_tag(tag: &str, all: &[String]) -> Option<String> {
    /// (prefix, numeric parts). `v1.2.3` → [1,2,3]; date tags `v20260930-101500`
    /// → [20260930, 101500]. Pre-releases (`v1.2.4-rc1`) have a non-numeric part
    /// and are not versions here, so they're never picked as "previous".
    fn version(t: &str) -> Option<(String, Vec<u64>)> {
        let i = t.find(|c: char| c.is_ascii_digit())?;
        let (prefix, rest) = t.split_at(i);
        let parts: Option<Vec<u64>> = rest
            .split(['.', '-', '+'])
            .map(|p| p.parse().ok())
            .collect();
        Some((prefix.to_string(), parts?))
    }
    match version(tag) {
        Some((prefix, cur)) => all
            .iter()
            .filter(|t| t.as_str() != tag)
            .filter_map(|t| version(t).map(|(p, v)| (t, p, v)))
            .filter(|(_, p, v)| *p == prefix && *v < cur)
            .max_by(|a, b| a.2.cmp(&b.2))
            .map(|(t, _, _)| t.clone()),
        None => all.iter().filter(|t| t.as_str() < tag).max().cloned(),
    }
}

/// Jira-style keys (`CPM-123`) in the given texts, deduplicated, in order.
pub fn jira_keys(texts: &[&str]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for text in texts {
        let bytes = text.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            // project key: an uppercase letter, then uppercase letters/digits
            if bytes[i].is_ascii_uppercase() && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric()) {
                let start = i;
                let mut j = i + 1;
                while j < bytes.len()
                    && (bytes[j].is_ascii_uppercase() || bytes[j].is_ascii_digit())
                {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b'-' && j - start >= 2 {
                    let mut k = j + 1;
                    while k < bytes.len() && bytes[k].is_ascii_digit() {
                        k += 1;
                    }
                    let followed_ok = k >= bytes.len() || !bytes[k].is_ascii_alphanumeric();
                    if k > j + 1 && followed_ok {
                        let key = &text[start..k];
                        if seen.insert(key.to_string()) {
                            out.push(key.to_string());
                        }
                        i = k;
                        continue;
                    }
                }
                i = j;
                continue;
            }
            i += 1;
        }
    }
    out
}

async fn gather_prs(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    prev: Option<&str>,
    tag: &str,
    sha: &str,
) -> Vec<ReleasePr> {
    let commits = match prev {
        Some(p) => github
            .compare_commits(owner, repo, p, tag, MAX_COMMITS)
            .await
            .unwrap_or_default(),
        // First release: just the recent history at the tag.
        None => github
            .recent_commits(owner, repo, sha, 50)
            .await
            .unwrap_or_default(),
    };
    let mut prs: Vec<ReleasePr> = Vec::new();
    let mut seen = BTreeSet::new();
    // Newest first, so the cap keeps the most recent work.
    for c in commits.iter().rev() {
        if prs.len() >= MAX_PRS {
            break;
        }
        let Some(csha) = c["sha"].as_str() else {
            continue;
        };
        let Ok(pulls) = github.pulls_for_commit(owner, repo, csha).await else {
            continue;
        };
        for p in pulls {
            let Some(n) = p["number"].as_u64() else {
                continue;
            };
            if p["merged_at"].is_null() || !seen.insert(n) {
                continue;
            }
            prs.push(ReleasePr {
                number: n,
                title: p["title"].as_str().unwrap_or("").to_string(),
                body: common::truncate_str(p["body"].as_str().unwrap_or(""), 3_000).to_string(),
                branch: p["head"]["ref"].as_str().unwrap_or("").to_string(),
                labels: p["labels"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|l| l["name"].as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default(),
                author: p["user"]["login"].as_str().unwrap_or("").to_string(),
            });
        }
    }
    prs
}

/// Jira summaries through the CoderHelm Jira app (best-effort; empty when the
/// app isn't connected or the lookup trigger isn't registered).
async fn jira_summaries(
    state: &WorkerState,
    team_id: &str,
    keys: &[String],
) -> Vec<(String, String)> {
    if keys.is_empty() {
        return vec![];
    }
    let Some((url, secret)) = forge_trigger(state, team_id, "get_issues_url").await else {
        return vec![];
    };
    let resp = state
        .http
        .post(&url)
        .json(&json!({ "forge_secret": secret, "keys": keys }))
        .send()
        .await;
    let Ok(resp) = resp else { return vec![] };
    let Ok(body) = resp.json::<Value>().await else {
        return vec![];
    };
    body["issues"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|i| {
                    Some((
                        i["key"].as_str()?.to_string(),
                        format!(
                            "{} [{}]",
                            i["summary"].as_str().unwrap_or(""),
                            i["type"].as_str().unwrap_or("")
                        ),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A registered Forge web trigger URL (by field) + the shared secret.
async fn forge_trigger(
    state: &WorkerState,
    team_id: &str,
    field: &str,
) -> Option<(String, String)> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .ok()?
        .item?;
    let get = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
            .cloned()
    };
    Some((get(field)?, get("forge_secret")?))
}

async fn load_guide(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    sha: &str,
    cfg: &ReleaseNotesConfig,
) -> Option<(String, String)> {
    let candidates: Vec<&str> = if cfg.guide_path.trim().is_empty() {
        GUIDE_CANDIDATES.to_vec()
    } else {
        vec![cfg.guide_path.trim()]
    };
    for path in candidates {
        if let Ok(text) = github.read_file(owner, repo, path, sha).await {
            if !text.trim().is_empty() {
                return Some((
                    path.to_string(),
                    common::truncate_str(&text, 24_000).to_string(),
                ));
            }
        }
    }
    None
}

// ── Writing ──────────────────────────────────────────────────────────────────

/// What the model returns.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Notes {
    /// Engineering-facing GitHub Release body (markdown).
    #[serde(default)]
    pub github_release_md: String,
    /// One-line headline for the entry / email subject.
    #[serde(default)]
    pub headline: String,
    /// Business-facing changelog entry body (markdown, no heading).
    #[serde(default)]
    pub entry_md: String,
    /// PRs it could not describe from the sources (listed as written).
    #[serde(default)]
    pub unclear: Vec<String>,
}

const WRITER_SYSTEM: &str = "You write release notes for a software team. You produce two \
different write-ups of the SAME release from the sources given:\n\
1. `github_release_md` — for engineers: a terse \"What's changed\" list grouped by theme, \
PR-linked (#123), Jira keys linked when given. Standard GitHub release-notes tone.\n\
2. `entry_md` — for business readers (operations, IT/help desk, leadership): what changed for \
users and why it matters. Rules:\n\
- No technical implementation details: no file, function, library or class names, no commit \
SHAs. PR numbers are not needed in this entry.\n\
- Group into 2-4 themes with a bold theme heading (`**Theme**`), lead with the user-facing \
improvement or motivation, then short bullets if useful. Link a Jira ticket on the theme \
heading when there is one obvious anchor. Aim for about 150-250 words.\n\
- Internal-only work (tests, CI, dependency bumps, refactors with no behavior change, tooling) \
is dropped, or — when substantial — mentioned briefly with the prefix \"Internal change:\".\n\
- NEVER invent a user-facing description. If a PR's title, body and Jira ticket don't make \
clear what it does for users, do not guess: put it under a final \"**Other changes**\" heading \
using its title as written, and list it in `unclear`.\n\
- If the repo provides a release-notes guide, follow its WRITING rules (audience, tone, \
format, entry shape). Ignore any tool / command / publishing steps it describes — publishing \
is handled for you.\n\
`headline` is one short sentence framing the release (used as the email subject line).\n\
Output ONLY a fenced ```json block: {\"github_release_md\": \"...\", \"headline\": \"...\", \
\"entry_md\": \"...\", \"unclear\": [\"#123 title\", ...]}";

#[allow(clippy::too_many_arguments)]
fn writer_prompt(
    owner: &str,
    repo: &str,
    tag: &str,
    prev: Option<&str>,
    prs: &[ReleasePr],
    jira: &[(String, String)],
    existing_release: Option<&str>,
    guide: Option<&(String, String)>,
    instructions: &str,
) -> String {
    let mut p = format!(
        "Repository: {owner}/{repo}\nRelease: {tag}{}\n\n",
        prev.map(|p| format!(" (changes since {p})"))
            .unwrap_or_default()
    );
    if let Some(body) = existing_release.filter(|b| !b.trim().is_empty()) {
        p.push_str(&format!(
            "## Existing GitHub Release notes for this tag (highest-signal source)\n{}\n\n",
            common::truncate_str(body, 8_000)
        ));
    }
    p.push_str("## Merged pull requests\n");
    if prs.is_empty() {
        p.push_str("(none found)\n");
    }
    for pr in prs {
        p.push_str(&format!(
            "### #{} {}\nbranch: {} · labels: {} · author: {}\n{}\n\n",
            pr.number,
            pr.title,
            pr.branch,
            if pr.labels.is_empty() {
                "-".to_string()
            } else {
                pr.labels.join(", ")
            },
            pr.author,
            if pr.body.trim().is_empty() {
                "(no description)"
            } else {
                pr.body.trim()
            }
        ));
    }
    if !jira.is_empty() {
        p.push_str("## Jira tickets referenced\n");
        for (k, s) in jira {
            p.push_str(&format!("- {k}: {s}\n"));
        }
        p.push('\n');
    }
    if let Some((path, text)) = guide {
        p.push_str(&format!(
            "## The repo's release-notes guide ({path})\n{text}\n\n"
        ));
    }
    if !instructions.trim().is_empty() {
        p.push_str(&format!(
            "## Team instructions\n{}\n\n",
            instructions.trim()
        ));
    }
    p.push_str("Write both release notes now.");
    p
}

pub fn parse_notes(reply: &str) -> Option<Notes> {
    let block = match reply.rfind("```json") {
        Some(s) => {
            let after = &reply[s + 7..];
            after.find("```").map(|e| after[..e].trim().to_string())?
        }
        None => {
            let s = reply.find('{')?;
            let e = reply.rfind('}')?;
            reply[s..=e].to_string()
        }
    };
    let notes: Notes = serde_json::from_str(&block).ok()?;
    (!notes.entry_md.trim().is_empty() || !notes.github_release_md.trim().is_empty())
        .then_some(notes)
}

/// Markdown → Confluence storage (XHTML).
pub fn to_storage(md: &str) -> String {
    let parser = pulldown_cmark::Parser::new_ext(md, pulldown_cmark::Options::ENABLE_STRIKETHROUGH);
    let mut html = String::new();
    pulldown_cmark::html::push_html(&mut html, parser);
    html
}

/// The entry as published: dated heading, metadata line, body.
pub fn entry_markdown(tag: &str, date: &str, release_url: &str, notes: &Notes) -> String {
    let mut s = format!("### {date} — {tag}\n\n");
    if !release_url.is_empty() {
        s.push_str(&format!("*[Release notes]({release_url})*\n\n"));
    }
    if !notes.headline.trim().is_empty() {
        s.push_str(notes.headline.trim());
        s.push_str("\n\n");
    }
    s.push_str(notes.entry_md.trim());
    s.push('\n');
    s
}

// ── Run ──────────────────────────────────────────────────────────────────────

pub async fn run(
    state: &WorkerState,
    msg: ReleaseNotesMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let owner = msg.repo_owner.as_str();
    let repo = msg.repo_name.as_str();
    let cfg = ReleaseNotesConfig::load(state, &msg.team_id, owner, repo).await;
    if !cfg.enabled {
        info!(%msg.tag, "Release notes: off for this repo");
        return Ok(());
    }
    // One run per tag at a time (a duplicate delivery or double re-send).
    let lock = format!("RELEASELOCK#{owner}/{repo}#{}", msg.tag);
    if !claim(state, &msg.team_id, &lock, 900).await {
        info!(%msg.tag, "Release notes: already running for this tag");
        return Ok(());
    }
    let result = run_locked(state, &msg, &cfg).await;
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&msg.team_id))
        .key("sk", attr_s(&lock))
        .send()
        .await;
    if let Err(e) = &result {
        warn!(%msg.tag, error = %e, "Release notes failed");
        alert_teams(state, &cfg, owner, repo, &msg.tag, &e.to_string()).await;
    }
    result
}

async fn run_locked(
    state: &WorkerState,
    msg: &ReleaseNotesMessage,
    cfg: &ReleaseNotesConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let owner = msg.repo_owner.as_str();
    let repo = msg.repo_name.as_str();
    let tag = msg.tag.as_str();
    let record_sk = format!("RELEASE#{owner}/{repo}#{tag}");
    let mut rec = load_record(state, &msg.team_id, &record_sk).await;
    let regenerate = msg.resend == "all";

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // Tags CoderHelm didn't cut (pushed by a person or CI) only get notes when
    // they're on the release branch — not tags on feature or hotfix branches.
    if msg.check_branch {
        let branch = release_branch(&github, owner, repo, cfg).await?;
        let status = github.compare_status(owner, repo, tag, &branch).await?;
        if !tag_on_branch(&status) {
            info!(%tag, %branch, %status, "Release notes: tag isn't on the release branch — skipped");
            return Ok(());
        }
    }

    // 1) Notes — reuse the stored ones unless regenerating.
    let notes = if !regenerate && !rec.get("entry_md").is_none_or(|s| s.is_empty()) {
        Notes {
            github_release_md: rec.get("github_release_md").cloned().unwrap_or_default(),
            headline: rec.get("headline").cloned().unwrap_or_default(),
            entry_md: rec.get("entry_md").cloned().unwrap_or_default(),
            unclear: vec![],
        }
    } else {
        let tags: Vec<String> = github
            .list_tags(owner, repo)
            .await
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
            .iter()
            .filter_map(|t| t["name"].as_str().map(String::from))
            .collect();
        let prev = previous_tag(tag, &tags);
        let prs = gather_prs(&github, owner, repo, prev.as_deref(), tag, &msg.sha).await;
        let texts: Vec<&str> = prs
            .iter()
            .flat_map(|p| [p.title.as_str(), p.body.as_str(), p.branch.as_str()])
            .collect();
        let keys = jira_keys(&texts);
        let jira = jira_summaries(state, &msg.team_id, &keys).await;
        let existing = github.get_release_by_tag(owner, repo, tag).await;
        let existing_body = existing
            .as_ref()
            .and_then(|r| r["body"].as_str())
            .map(String::from);
        let guide = load_guide(&github, owner, repo, &msg.sha, cfg).await;
        let prompt = writer_prompt(
            owner,
            repo,
            tag,
            prev.as_deref(),
            &prs,
            &jira,
            existing_body.as_deref(),
            guide.as_ref(),
            &cfg.instructions,
        );
        let provider = ModelProvider::load_for_team(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
        )
        .await?;
        let mut usage = TokenUsage::default();
        let reply = provider::converse_simple(
            state,
            &provider,
            provider.heavy_model_id(),
            WRITER_SYSTEM,
            &prompt,
            &mut usage,
        )
        .await?;
        let notes = parse_notes(&reply).ok_or("The model's release notes could not be parsed")?;
        if let Some(r) = existing.as_ref().and_then(|r| r["html_url"].as_str()) {
            rec.insert("github_release_url".into(), r.to_string());
        }
        rec.insert("prev_tag".into(), prev.unwrap_or_default());
        rec.insert("pr_count".into(), prs.len().to_string());
        rec.insert("github_release_md".into(), notes.github_release_md.clone());
        rec.insert("headline".into(), notes.headline.clone());
        rec.insert("entry_md".into(), notes.entry_md.clone());
        rec.insert("unclear".into(), notes.unclear.join("\n"));
        if regenerate {
            rec.remove("confluence_url");
            rec.remove("email_sent_at");
        }
        save_record(state, &msg.team_id, &record_sk, tag, &rec).await;
        notes
    };

    // 2) GitHub Release (skipped when one already exists for the tag).
    if rec.get("github_release_url").is_none_or(|s| s.is_empty()) {
        let body = if notes.github_release_md.trim().is_empty() {
            notes.entry_md.clone()
        } else {
            notes.github_release_md.clone()
        };
        let created = github.create_release(owner, repo, tag, tag, &body).await?;
        let url = created["html_url"].as_str().unwrap_or("").to_string();
        rec.insert("github_release_url".into(), url);
        save_record(state, &msg.team_id, &record_sk, tag, &rec).await;
    }
    let release_url = rec.get("github_release_url").cloned().unwrap_or_default();
    let date = chrono::Utc::now()
        .with_timezone(&chrono_tz_ny())
        .format("%Y-%m-%d")
        .to_string();
    let entry = entry_markdown(tag, &date, &release_url, &notes);

    // 3) Confluence changelog entry.
    if !cfg.confluence_parent_id.trim().is_empty()
        && (regenerate || rec.get("confluence_url").is_none_or(|s| s.is_empty()))
    {
        let (url, secret) = forge_trigger(state, &msg.team_id, "publish_release_url")
            .await
            .ok_or("Confluence publishing isn't connected — update the CoderHelm app in Jira and reconnect it in Settings → Integrations")?;
        let resp = state
            .http
            .post(&url)
            .json(&json!({
                "forge_secret": secret,
                "spaceKey": cfg.confluence_space.trim(),
                "parentId": cfg.confluence_parent_id.trim(),
                "containerTitle": cfg.confluence_container.trim(),
                "year": &date[..4],
                "entryKey": format!("{owner}/{repo}@{tag}"),
                "entryHtml": to_storage(&entry),
            }))
            .send()
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(format!("Confluence publish failed ({status}): {body}").into());
        }
        rec.insert(
            "confluence_url".into(),
            body["url"].as_str().unwrap_or("published").to_string(),
        );
        save_record(state, &msg.team_id, &record_sk, tag, &rec).await;
    }

    // 4) Email (after Confluence, so the email can link the page).
    let resend_email = msg.resend == "email" || regenerate;
    if !cfg.email_webhook_url.trim().is_empty()
        && (resend_email || rec.get("email_sent_at").is_none_or(|s| s.is_empty()))
    {
        let payload = json!({
            "repo": format!("{owner}/{repo}"),
            "tag": tag,
            "date": date,
            "subject": if notes.headline.trim().is_empty() { format!("{repo} {tag} released") } else { format!("{repo} {tag}: {}", notes.headline.trim()) },
            "summary_markdown": entry,
            "summary_html": to_storage(&entry),
            "confluence_url": rec.get("confluence_url").cloned().unwrap_or_default(),
            "release_url": release_url,
        });
        let resp = state
            .http
            .post(cfg.email_webhook_url.trim())
            .json(&payload)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(format!("Email webhook returned {}", resp.status()).into());
        }
        rec.insert("email_sent_at".into(), chrono::Utc::now().to_rfc3339());
        save_record(state, &msg.team_id, &record_sk, tag, &rec).await;
    }
    info!(%tag, repo = %format!("{owner}/{repo}"), "Release notes published");
    Ok(())
}

fn chrono_tz_ny() -> chrono::FixedOffset {
    // America/New_York without a tz database: EDT (UTC-4) from the second Sunday
    // of March to the first Sunday of November, else EST (UTC-5). Only the date
    // of the entry depends on it.
    use chrono::Datelike;
    let now = chrono::Utc::now();
    let y = now.year();
    let nth_sunday = |month: u32, n: u32| {
        let first = chrono::NaiveDate::from_ymd_opt(y, month, 1).unwrap_or_default();
        let offset = (7 - first.weekday().num_days_from_sunday()) % 7;
        first + chrono::Days::new((offset + 7 * (n - 1)) as u64)
    };
    let today = now.date_naive();
    let dst = today >= nth_sunday(3, 2) && today < nth_sunday(11, 1);
    chrono::FixedOffset::west_opt(if dst { 4 * 3600 } else { 5 * 3600 })
        .unwrap_or_else(|| chrono::FixedOffset::east_opt(0).unwrap())
}

// ── Record + helpers ─────────────────────────────────────────────────────────

async fn load_record(state: &WorkerState, team_id: &str, sk: &str) -> HashMap<String, String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .map(|item| {
            item.into_iter()
                .filter_map(|(k, v)| v.as_s().ok().map(|s| (k, s.clone())))
                .collect()
        })
        .unwrap_or_default()
}

async fn save_record(
    state: &WorkerState,
    team_id: &str,
    sk: &str,
    tag: &str,
    rec: &HashMap<String, String>,
) {
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(sk))
        .item("tag", attr_s(tag))
        .item("updated_at", attr_s(&chrono::Utc::now().to_rfc3339()));
    for (k, v) in rec {
        if matches!(k.as_str(), "pk" | "sk" | "tag" | "updated_at") {
            continue;
        }
        put = put.item(k, AttributeValue::S(v.clone()));
    }
    if let Err(e) = put.send().await {
        warn!(error = %e, "Release notes: could not save record");
    }
}

async fn claim(state: &WorkerState, team_id: &str, sk: &str, ttl_secs: u64) -> bool {
    let ttl = chrono::Utc::now().timestamp() as u64 + ttl_secs;
    match state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(sk))
        .item("ttl", attr_n(ttl))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => !e
            .as_service_error()
            .map(|se| se.is_conditional_check_failed_exception())
            .unwrap_or(false),
    }
}

async fn alert_teams(
    state: &WorkerState,
    cfg: &ReleaseNotesConfig,
    owner: &str,
    repo: &str,
    tag: &str,
    error: &str,
) {
    if cfg.teams_webhook_url.trim().is_empty() {
        return;
    }
    let card = json!({
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.5",
                "body": [
                    {"type": "TextBlock", "text": format!("⚠️ Release notes failed — {owner}/{repo} {tag}"), "weight": "Bolder", "wrap": true, "color": "Attention"},
                    {"type": "TextBlock", "text": common::truncate_str(error, 500), "wrap": true},
                    {"type": "TextBlock", "text": "Send them again from CoderHelm → Releases once fixed.", "wrap": true, "isSubtle": true}
                ]
            }
        }]
    });
    if let Err(e) = state
        .http
        .post(cfg.teams_webhook_url.trim())
        .json(&card)
        .send()
        .await
    {
        warn!(error = %e, "Release notes: Teams alert failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn previous_tag_is_the_highest_lower_version_with_same_prefix() {
        let all = tags(&[
            "v1.2.10",
            "v1.2.9",
            "v1.3.0",
            "v1.2.2",
            "rel-9.0.0",
            "v1.2.11-rc1",
        ]);
        assert_eq!(previous_tag("v1.3.0", &all).as_deref(), Some("v1.2.10"));
        assert_eq!(previous_tag("v1.2.10", &all).as_deref(), Some("v1.2.9"));
        assert_eq!(previous_tag("v1.2.2", &all), None);
    }

    #[test]
    fn previous_tag_for_date_tags_uses_name_order() {
        let all = tags(&["v20260930-101500", "v20261001-090000", "v20260929-120000"]);
        // date tags parse as one big "version" component — still ordered right
        assert_eq!(
            previous_tag("v20261001-090000", &all).as_deref(),
            Some("v20260930-101500")
        );
    }

    #[test]
    fn jira_keys_found_once_in_order() {
        let k = jira_keys(&[
            "CPM-8344: browse tests",
            "fixes CPM-12 and cpm-99 (lowercase ignored), see CPM-8344",
            "branch feat/CPM-7001-join",
            "not a key: UTF-8, X-1, ABCD-",
        ]);
        assert_eq!(k, vec!["CPM-8344", "CPM-12", "CPM-7001", "UTF-8"]);
    }

    #[test]
    fn tag_counts_as_release_only_when_branch_contains_it() {
        // compare(tag...branch): branch "ahead" of / "identical" to the tag
        assert!(tag_on_branch("ahead"));
        assert!(tag_on_branch("identical"));
        // tag on another branch, or newer than the branch
        assert!(!tag_on_branch("diverged"));
        assert!(!tag_on_branch("behind"));
        assert!(!tag_on_branch(""));
    }

    #[test]
    fn parses_fenced_json() {
        let reply = "Here you go\n```json\n{\"github_release_md\":\"## What's changed\\n- #1\",\"headline\":\"Faster join\",\"entry_md\":\"**Join**\\nquicker\",\"unclear\":[\"#7 misc\"]}\n```";
        let n = parse_notes(reply).unwrap();
        assert_eq!(n.headline, "Faster join");
        assert_eq!(n.unclear, vec!["#7 misc"]);
        assert!(parse_notes("no json here").is_none());
        assert!(parse_notes("```json\n{\"headline\":\"x\"}\n```").is_none());
    }

    #[test]
    fn entry_has_date_tag_link_and_body_and_renders_to_storage() {
        let n = Notes {
            headline: "Members can join faster.".into(),
            entry_md: "**Online join**\n\n- Start date picker".into(),
            ..Default::default()
        };
        let md = entry_markdown("v2.3.0", "2026-10-01", "https://gh/r", &n);
        assert!(md.starts_with("### 2026-10-01 — v2.3.0\n"));
        assert!(md.contains("[Release notes](https://gh/r)"));
        let html = to_storage(&md);
        assert!(html.contains("<h3>2026-10-01 — v2.3.0</h3>"));
        assert!(html.contains("<strong>Online join</strong>"));
        assert!(html.contains("<li>Start date picker</li>"));
    }

    #[test]
    fn prompt_carries_sources_and_guide() {
        let prs = vec![ReleasePr {
            number: 12,
            title: "CPM-1: start date".into(),
            body: String::new(),
            branch: "feat/x".into(),
            labels: vec!["Deploy".into()],
            author: "a".into(),
        }];
        let guide = (
            "SKILL.md".to_string(),
            "Write for business readers.".to_string(),
        );
        let p = writer_prompt(
            "o",
            "r",
            "v2",
            Some("v1"),
            &prs,
            &[("CPM-1".into(), "Start date [Story]".into())],
            Some("hand notes"),
            Some(&guide),
            "Product: Fitness app",
        );
        assert!(p.contains("changes since v1"));
        assert!(p.contains("### #12 CPM-1: start date"));
        assert!(p.contains("(no description)"));
        assert!(p.contains("- CPM-1: Start date [Story]"));
        assert!(p.contains("hand notes"));
        assert!(p.contains("Write for business readers."));
        assert!(p.contains("Product: Fitness app"));
    }
}
