//! The worker's side of `common::conversation`: every comment the worker
//! writes on a GitHub issue or pull request, or on a Jira ticket, goes
//! through here, so it reads the thread first and speaks with one voice.
//!
//! - [`github`]: an issue or pull request conversation, read through the
//!   installation's client (every request counted against its budget; a
//!   thread read in the last [`CACHE_SECS`] seconds isn't asked again).
//! - [`jira`]: a Jira ticket, through the team's Jira app, with a ledger of
//!   what CoderHelm said there.
//! - [`edit_own`]: a status comment a pass keeps by its own id.
//! - [`review_reply`]: a reply in a pull request's review thread.
//!
//! [`ModelJudge`] lets a run's model read the thread before it speaks; the
//! guards in `common::conversation` hold whatever it says.
//!
//! The source scan at the bottom fails on any direct call to the trackers'
//! comment endpoints outside this file and `common::conversation_io`.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use common::conversation::{
    self, Channel, Heard, How, Judge, Judged, Meta, Place, Say, Spoke, View,
};
use serde_json::{json, Value};
use tracing::warn;

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::TokenUsage;
use crate::WorkerState;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// How long a thread read is reused.
pub const CACHE_SECS: u64 = 90;

static CACHE: LazyLock<Mutex<HashMap<String, (Instant, View)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn cached(key: &str) -> Option<View> {
    let c = CACHE.lock().ok()?;
    c.get(key)
        .filter(|(at, _)| at.elapsed().as_secs() < CACHE_SECS)
        .map(|(_, v)| v.clone())
}

fn cache(key: &str, view: View) {
    if let Ok(mut c) = CACHE.lock() {
        if c.len() > 500 {
            c.retain(|_, (at, _)| at.elapsed().as_secs() < CACHE_SECS);
        }
        c.insert(key.to_string(), (Instant::now(), view));
    }
}

/// What was just written, in the cached view.
fn remember(key: &str, id: &str, meta: &Meta) {
    if let Ok(mut c) = CACHE.lock() {
        if let Some((_, v)) = c.get_mut(key) {
            v.remember(id, chrono::Utc::now().timestamp_millis(), meta);
        }
    }
}

/// An issue or pull request conversation, through the installation's
/// client.
pub struct GithubThread<'a> {
    pub gh: &'a GitHubClient,
    pub owner: &'a str,
    pub repo: &'a str,
    pub number: u64,
}

#[async_trait::async_trait]
impl Channel for GithubThread<'_> {
    fn place(&self) -> Place {
        Place::github(self.owner, self.repo, self.number)
    }

    async fn read(&self) -> Result<View, String> {
        let key = self.place().key();
        if let Some(v) = cached(&key) {
            return Ok(v);
        }
        let comments = self
            .gh
            .list_issue_comments(self.owner, self.repo, self.number)
            .await
            .map_err(|e| e.to_string())?;
        let view = common::conversation_io::view_from_github(&comments);
        cache(&key, view.clone());
        Ok(view)
    }

    async fn create(&self, body: &str, meta: &Meta) -> Result<String, String> {
        let v = self
            .gh
            .create_issue_comment(
                self.owner,
                self.repo,
                self.number,
                &conversation::with_marker(body, meta),
            )
            .await
            .map_err(|e| e.to_string())?;
        let id = v["id"].as_u64().map(|n| n.to_string()).unwrap_or_default();
        remember(&self.place().key(), &id, meta);
        Ok(id)
    }

    async fn react(&self, to: &Heard, content: &str) -> Result<(), String> {
        let n: u64 = to
            .id
            .parse()
            .map_err(|_| format!("not a comment id: {}", to.id))?;
        self.gh
            .add_reaction(self.owner, self.repo, n, to.line, content)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn reply(&self, to: &Heard, how: How, body: &str, meta: &Meta) -> Result<String, String> {
        if how == How::Thread {
            let n: u64 = to
                .id
                .parse()
                .map_err(|_| format!("not a comment id: {}", to.id))?;
            let v = self
                .gh
                .reply_to_review_comment(self.owner, self.repo, self.number, n, body)
                .await
                .map_err(|e| e.to_string())?;
            let id = v["id"].as_u64().map(|n| n.to_string()).unwrap_or_default();
            remember(&self.place().key(), &id, meta);
            return Ok(id);
        }
        self.create(body, meta).await
    }

    async fn edit(&self, id: &str, body: &str, meta: &Meta) -> Result<String, String> {
        let n: u64 = id.parse().map_err(|_| format!("not a comment id: {id}"))?;
        self.gh
            .edit_issue_comment(
                self.owner,
                self.repo,
                n,
                &conversation::with_marker(body, meta),
            )
            .await
            .map_err(|e| e.to_string())?;
        remember(&self.place().key(), id, meta);
        Ok(id.to_string())
    }
}

/// GitHub's shape of a written comment, for callers that read `id` and
/// `html_url`: `id` is null when nothing new was said (`spoke` says why).
fn answer(owner: &str, repo: &str, number: u64, spoke: &Spoke) -> Value {
    match spoke {
        Spoke::Posted(id) | Spoke::Edited(id) => {
            let n = id.parse::<u64>().ok();
            json!({
                "id": n,
                "spoke": if matches!(spoke, Spoke::Posted(_)) { "posted" } else { "edited" },
                "html_url": n.map(|n| format!("https://github.com/{owner}/{repo}/issues/{number}#issuecomment-{n}")),
            })
        }
        Spoke::Quiet(why) => json!({ "id": null, "spoke": "quiet", "why": why }),
    }
}

/// Says `say` on an issue or pull request conversation.
pub async fn github(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    say: Say,
) -> Result<Value, Error> {
    github_judged(gh, owner, repo, number, say, None).await
}

/// [`github`], with a model reading the thread first.
pub async fn github_judged(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    say: Say,
    judge: Option<&dyn Judge>,
) -> Result<Value, Error> {
    let ch = GithubThread {
        gh,
        owner,
        repo,
        number,
    };
    let now = chrono::Utc::now().timestamp_millis();
    let spoke = Box::pin(conversation::speak(&ch, judge, &say, now)).await?;
    Ok(answer(owner, repo, number, &spoke))
}

/// Rewrites a status comment a pass keeps by its own id (`slot` names it):
/// laid out like every other comment, and marked as CoderHelm's.
pub async fn edit_own(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    comment_id: u64,
    slot: &str,
    body: &str,
) -> Result<Value, Error> {
    edit_own_asking(gh, owner, repo, comment_id, slot, body, &[]).await
}

/// [`edit_own`] for a comment that asks `asks`.
pub async fn edit_own_asking(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    comment_id: u64,
    slot: &str,
    body: &str,
    asks: &[String],
) -> Result<Value, Error> {
    let place = Place::github(owner, repo, 0);
    let shaped = conversation::shape(body, place.budget(), true);
    let meta = Meta {
        slot: slot.to_string(),
        fp: conversation::fingerprint(body),
        gist: conversation::cut_words(&conversation::visible(body), 160),
        first_ms: chrono::Utc::now().timestamp_millis(),
        asks: conversation::merge_questions(asks)
            .iter()
            .map(|q| conversation::cut_words(q, 200))
            .collect(),
        ..Default::default()
    };
    let v = gh
        .edit_issue_comment(
            owner,
            repo,
            comment_id,
            &conversation::with_marker(&shaped, &meta),
        )
        .await?;
    Ok(v)
}

/// What CoderHelm last replied in each review thread (this process), so a
/// retried round doesn't say the same thing in it twice.
static REPLIED: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A reply in a pull request's review thread (to comment `comment_id`).
/// The same words in the same thread again are not posted.
pub async fn review_reply(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    pr: u64,
    comment_id: u64,
    body: &str,
) -> Result<Value, Error> {
    let key = format!("{owner}/{repo}#{pr}/{comment_id}");
    let fp = conversation::fingerprint(body);
    if REPLIED
        .lock()
        .ok()
        .and_then(|r| r.get(&key).cloned())
        .is_some_and(|f| f == fp)
    {
        return Ok(
            json!({ "id": null, "spoke": "quiet", "why": "the same reply is already in the thread" }),
        );
    }
    let shaped = conversation::shape(body, Place::github(owner, repo, pr).budget(), true);
    let v = gh
        .reply_to_review_comment(owner, repo, pr, comment_id, &shaped)
        .await?;
    if let Ok(mut r) = REPLIED.lock() {
        if r.len() > 2_000 {
            r.clear();
        }
        r.insert(key, fp);
    }
    Ok(v)
}

/// The team's Jira app address and site.
async fn jira_config(
    state: &WorkerState,
    team_id: &str,
) -> Result<(String, Option<String>), Error> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("JIRA#config".to_string()),
        )
        .send()
        .await?
        .item;
    let field = |k: &str| {
        item.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
            .cloned()
    };
    let url = field("add_comment_url").ok_or("No Jira add_comment_url configured")?;
    Ok((url, field("site_url")))
}

/// The address of a comment on a Jira ticket.
pub fn jira_link(site: Option<&str>, issue_key: &str, id: Option<&str>) -> Option<String> {
    let site = site?.trim().trim_end_matches('/');
    if site.is_empty() || issue_key.is_empty() {
        return None;
    }
    Some(match id.filter(|i| !i.is_empty()) {
        Some(id) => format!("{site}/browse/{issue_key}?focusedCommentId={id}"),
        None => format!("{site}/browse/{issue_key}"),
    })
}

/// Says `say` on a Jira ticket, in the Jira app's `comment_type` panel
/// under `title`. Returns what was done and the comment's address when
/// the site is known.
pub async fn jira(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
    say: Say,
    comment_type: &str,
    title: &str,
) -> Result<(Spoke, Option<String>), Error> {
    jira_judged(state, team_id, ticket, say, comment_type, title, None).await
}

/// [`jira`], with a model reading the thread first.
pub async fn jira_judged(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
    say: Say,
    comment_type: &str,
    title: &str,
    judge: Option<&dyn Judge>,
) -> Result<(Spoke, Option<String>), Error> {
    let (url, site) = jira_config(state, team_id).await?;
    let ch = common::conversation_io::JiraChannel {
        http: &state.http,
        dynamo: &state.dynamo,
        settings_table: &state.config.settings_table_name,
        team_id,
        url,
        ticket: ticket.to_string(),
        comment_type: comment_type.to_string(),
        title: conversation::no_arrows(title),
    };
    let now = chrono::Utc::now().timestamp_millis();
    let spoke = Box::pin(conversation::speak(&ch, judge, &say, now)).await?;
    let link = jira_link(site.as_deref(), ticket, spoke.id());
    Ok((spoke, link))
}

/// Marks a question CoderHelm asked on an issue or pull request answered.
pub async fn mark_answered_github(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    question: &str,
    who: &str,
    answer: &str,
) -> Result<(), Error> {
    let ch = GithubThread {
        gh,
        owner,
        repo,
        number,
    };
    Box::pin(conversation::mark_answered(&ch, question, who, answer)).await?;
    Ok(())
}

/// Marks a question CoderHelm asked on a Jira ticket answered.
pub async fn mark_answered_jira(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
    question: &str,
    who: &str,
    answer: &str,
) -> Result<(), Error> {
    let (url, _) = jira_config(state, team_id).await?;
    let ch = common::conversation_io::JiraChannel {
        http: &state.http,
        dynamo: &state.dynamo,
        settings_table: &state.config.settings_table_name,
        team_id,
        url,
        ticket: ticket.to_string(),
        comment_type: "info".into(),
        title: String::new(),
    };
    Box::pin(conversation::mark_answered(&ch, question, who, answer)).await?;
    Ok(())
}

/// A run's model, reading the thread before CoderHelm speaks.
pub struct ModelJudge<'a> {
    pub provider: &'a ModelProvider,
    pub usage: tokio::sync::Mutex<&'a mut TokenUsage>,
}

impl<'a> ModelJudge<'a> {
    pub fn new(provider: &'a ModelProvider, usage: &'a mut TokenUsage) -> Self {
        ModelJudge {
            provider,
            usage: tokio::sync::Mutex::new(usage),
        }
    }
}

#[async_trait::async_trait]
impl Judge for ModelJudge<'_> {
    async fn judge(&self, prompt: &str) -> Option<Judged> {
        let mut usage = self.usage.lock().await;
        match Box::pin(crate::agent::anthropic::converse_json(
            &self.provider.client(),
            self.provider.judgment_model_id(),
            conversation::JUDGE_SYSTEM,
            prompt,
            &conversation::judge_schema(),
            true,
            common::stop_judge::JUDGMENT_EFFORT,
            400,
            &mut usage,
        ))
        .await
        {
            Ok(v) => Some(conversation::parse_judged(&v)),
            Err(e) => {
                warn!(error = %e, "Could not read the thread with the model; the guards decide");
                None
            }
        }
    }
}

#[cfg(test)]
mod scan {
    /// The lines of `src` outside its `#[cfg(test)]` items (numbered from
    /// 1): the scans read only what the services run.
    pub(super) fn non_test_lines(src: &str) -> Vec<(usize, &str)> {
        let lines: Vec<&str> = src.lines().collect();
        let mut out = Vec::new();
        let mut i = 0;
        while i < lines.len() {
            let l = lines[i];
            if l.trim() == "#[cfg(test)]" {
                let indent = &l[..l.len() - l.trim_start().len()];
                let close = format!("{indent}}}");
                i += 1;
                if i < lines.len() && lines[i].trim_end().ends_with(';') {
                    i += 1;
                    continue;
                }
                while i < lines.len() && lines[i].trim_end() != close {
                    i += 1;
                }
                i += 1;
                continue;
            }
            out.push((i + 1, l));
            i += 1;
        }
        out
    }

    /// Lines the source scan lets through: the GitHub client's own definitions
    /// and the conversation layer's.
    pub(super) fn allowed_line(rel: &str, line: &str) -> bool {
        let _ = rel;
        line.contains("pub async fn ")
    }

    /// Whether a source line builds text CoderHelm posts (a comment, a reply,
    /// a ticket note): the scan of arrow glyphs reads those.
    pub(super) fn posted_template(line: &str) -> bool {
        let l = line.to_ascii_lowercase();
        !(l.contains("info!(")
            || l.contains("warn!(")
            || l.contains("error!(")
            || l.contains("debug!(")
            || l.contains("tracing::"))
    }
}

#[cfg(test)]
mod tests {
    /// Every place the services write a comment on a ticket or a pull
    /// request goes through this layer: a direct call to the trackers'
    /// comment endpoints anywhere else fails here.
    #[test]
    fn every_comment_goes_through_the_conversation_layer() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let allowed = [
            "worker/src/passes/conversation.rs",
            "worker/src/clients/github.rs",
            "common/src/conversation_io.rs",
            "common/src/conversation.rs",
        ];
        let needles = [
            ".create_issue_comment(",
            ".edit_issue_comment(",
            ".reply_to_review_comment(",
            "post_jira_comment(",
            "post_jira_comment_link(",
            "\"add_comment_url\")",
            "/comments\", ",
            "}/comments\")",
            "/comments\"),",
        ];
        let mut found = Vec::new();
        let mut stack = vec![
            root.join("worker/src"),
            root.join("gateway/src"),
            root.join("common/src"),
        ];
        while let Some(dir) = stack.pop() {
            for e in std::fs::read_dir(&dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                    continue;
                }
                if p.extension().and_then(|x| x.to_str()) != Some("rs") {
                    continue;
                }
                let rel = p
                    .strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                if allowed.contains(&rel.as_str()) {
                    continue;
                }
                let src = std::fs::read_to_string(&p).unwrap();
                // Tests may name the calls in strings and fakes.
                for (n, line) in super::scan::non_test_lines(&src) {
                    let l = line.trim_start();
                    if l.starts_with("//") {
                        continue;
                    }
                    for k in &needles {
                        if line.contains(k)
                            && !line.contains("/reactions")
                            && !super::scan::allowed_line(&rel, line)
                        {
                            found.push(format!("{rel}:{n}: {}", line.trim()));
                        }
                    }
                }
            }
        }
        assert!(
            found.is_empty(),
            "comments written outside the conversation layer:\n{}",
            found.join("\n")
        );
    }

    /// No template CoderHelm posts carries an arrow glyph.
    #[test]
    fn no_posted_template_has_an_arrow() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut found = Vec::new();
        let mut stack = vec![
            root.join("worker/src/passes"),
            root.join("common/src"),
            root.join("gateway/src/routes"),
        ];
        let arrows = common::conversation::is_arrow;
        while let Some(dir) = stack.pop() {
            for e in std::fs::read_dir(&dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                    continue;
                }
                if p.extension().and_then(|x| x.to_str()) != Some("rs") {
                    continue;
                }
                let src = std::fs::read_to_string(&p).unwrap();
                for (n, line) in super::scan::non_test_lines(&src) {
                    let l = line.trim_start();
                    if l.starts_with("//") || !line.contains('"') {
                        continue;
                    }
                    if !super::scan::posted_template(line) {
                        continue;
                    }
                    if line.chars().any(arrows) {
                        found.push(format!("{}:{n}: {}", p.display(), l));
                    }
                }
            }
        }
        assert!(
            found.is_empty(),
            "arrow glyphs in posted text:\n{}",
            found.join("\n")
        );
    }

    /// Jira comments are plain text: a red line only for a run that failed,
    /// never a warning panel. The scan reads every Jira call's comment type
    /// and title.
    #[test]
    fn only_a_failed_run_asks_jira_for_a_red_line() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let call =
            regex::Regex::new(r#"(?s)(?:conversation::jira(?:_judged|_at)?)\((.{0,600}?)\.await"#)
                .unwrap();
        let typed = regex::Regex::new(r#""([a-z_]+)",\s*"([^"]*)""#).unwrap();
        let mut found = Vec::new();
        let mut stack = vec![root.join("worker/src"), root.join("gateway/src")];
        while let Some(dir) = stack.pop() {
            for e in std::fs::read_dir(&dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                    continue;
                }
                if p.extension().and_then(|x| x.to_str()) != Some("rs") {
                    continue;
                }
                let src = std::fs::read_to_string(&p).unwrap();
                let code: String = super::scan::non_test_lines(&src)
                    .into_iter()
                    .map(|(_, l)| l)
                    .collect::<Vec<_>>()
                    .join("\n");
                for c in call.captures_iter(&code) {
                    for t in typed.captures_iter(&c[1]) {
                        if common::conversation::jira_panel(&t[1]) == "error"
                            && &t[2] != "Run failed"
                        {
                            found.push(format!("{}: {} {}", p.display(), &t[1], &t[2]));
                        }
                    }
                }
            }
        }
        assert!(
            found.is_empty(),
            "a red panel outside a failed run:\n{}",
            found.join("\n")
        );
    }

    #[test]
    fn a_failed_runs_comment_fits_its_cause() {
        use common::conversation::failure_comment;
        let dup = "Stopped before opening a pull request: the branch repeats 4 commit(s) of open pull request #41 (`coderhelm/abc-1`) under other hashes (c1aaaaa, c2bbbbb). Dropping them didn't work (too many commits to replay), and neither did merging #41's branch in and rebuilding this change";
        let c = failure_comment(dup, "https://app.example/runs/detail?id=r1", false);
        assert!(!c.contains("Add more detail"), "{c}");
        assert!(
            c.contains("the open pull request named above carries this work"),
            "{c}"
        );
    }
}
