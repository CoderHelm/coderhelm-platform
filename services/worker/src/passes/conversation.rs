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
//! - [`reply_github`], [`reply_jira`], [`reply_review`]: a reply to a
//!   person (their comment, their review comment, their question, their
//!   instruction, a refusal). The words are written by the composer
//!   (`common::reply_compose`) from the thread and the run's records, on
//!   the team's light model ([`Replier`]); the path's own text is the draft
//!   and the fallback. Every person-facing reply goes through these (a
//!   source guard below holds it to that).
//!
//! [`ModelJudge`] lets a run's model read the thread before it speaks; the
//! guards in `common::conversation` hold whatever it or the composer says.
//!
//! The source scan at the bottom fails on any direct call to the trackers'
//! comment endpoints outside this file and `common::conversation_io`.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use common::conversation::{
    self, Channel, Decision, Heard, How, Judge, Judged, Kind, Meta, Place, Say, Spoke, View,
};
use common::reply_compose::{self, Compose, Composer, Facts};
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

    async fn hide(&self, said: &common::conversation::Said, hidden: bool) -> Result<(), String> {
        if said.node_id.is_empty() {
            return Err("no node id for the comment".into());
        }
        self.gh
            .hide_own_comment(&said.node_id, hidden)
            .await
            .map_err(|e| e.to_string())
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

/// A reply to people on an issue or pull request conversation: `say` is
/// the path's draft; the composer writes it from the thread and `facts`.
#[allow(clippy::too_many_arguments)]
pub async fn reply_github(
    r: &Replier<'_>,
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    say: Say,
    facts: &Facts,
) -> Result<Value, Error> {
    let ch = GithubThread {
        gh,
        owner,
        repo,
        number,
    };
    let now = chrono::Utc::now().timestamp_millis();
    let c = Compose { composer: r, facts };
    let spoke = Box::pin(conversation::speak_with(&ch, None, Some(c), &say, now)).await?;
    Ok(answer(owner, repo, number, &spoke))
}

/// The composer of replies to people, on the team's light model, loaded
/// the first time a reply needs it (most say calls answer nobody).
pub struct Replier<'a> {
    pub state: &'a WorkerState,
    pub team_id: &'a str,
    provider: tokio::sync::OnceCell<Option<ModelProvider>>,
    usage: tokio::sync::Mutex<TokenUsage>,
}

impl<'a> Replier<'a> {
    pub fn new(state: &'a WorkerState, team_id: &'a str) -> Self {
        Replier {
            state,
            team_id,
            provider: tokio::sync::OnceCell::new(),
            usage: tokio::sync::Mutex::new(TokenUsage::default()),
        }
    }

    /// With the provider the pass already loaded.
    pub fn with_provider(
        state: &'a WorkerState,
        team_id: &'a str,
        provider: &ModelProvider,
    ) -> Self {
        Replier {
            provider: tokio::sync::OnceCell::new_with(Some(Some(provider.clone()))),
            ..Replier::new(state, team_id)
        }
    }
}

#[async_trait::async_trait]
impl Composer for Replier<'_> {
    async fn compose(&self, system: &str, prompt: &str, schema: &Value) -> Option<Value> {
        let provider = self
            .provider
            .get_or_init(|| async {
                ModelProvider::load_for_team(
                    &self.state.dynamo,
                    &self.state.config.settings_table_name,
                    self.team_id,
                )
                .await
                .map_err(|e| warn!(team_id = self.team_id, error = %e, "No model to write the reply; the draft is posted"))
                .ok()
            })
            .await
            .as_ref()?;
        let mut usage = self.usage.lock().await;
        match Box::pin(crate::agent::anthropic::converse_json(
            &provider.client(),
            provider.primary_model_id(),
            system,
            prompt,
            schema,
            true,
            "low",
            reply_compose::MAX_TOKENS,
            &mut usage,
        ))
        .await
        {
            Ok(v) => Some(v),
            Err(e) => {
                warn!(error = %e, "Could not write the reply with the model; the draft is posted");
                None
            }
        }
    }
}

/// Updates a status a pass keeps (`slot` names it) on issue or pull request
/// `number`. It goes through the layer like every other status: into the
/// thread's one status comment, edited in place and held to the comment
/// style; a pass's own comment id is never written to directly, so no pass
/// can keep a second status comment beside it.
pub async fn edit_own(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    slot: &str,
    body: &str,
) -> Result<Value, Error> {
    edit_own_asking(gh, owner, repo, number, slot, body, &[]).await
}

/// [`edit_own`] for a status that asks `asks`.
pub async fn edit_own_asking(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    slot: &str,
    body: &str,
    asks: &[String],
) -> Result<Value, Error> {
    github(
        gh,
        owner,
        repo,
        number,
        Say::status(slot, body).asks(asks.to_vec()),
    )
    .await
}

/// Whether a login is CoderHelm's app (`coderhelm[bot]`, an install's own
/// app name containing it).
fn own_login(login: &str) -> bool {
    login.to_ascii_lowercase().contains("coderhelm")
}

/// The reviews on a PR as `common::review_supersede` reads them.
pub fn reviews_of(v: &Value) -> Vec<common::review_supersede::Review> {
    v.as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
        .iter()
        .map(|r| common::review_supersede::Review {
            id: r["id"].as_u64().unwrap_or(0),
            own: r["user"]["type"].as_str() == Some("Bot")
                && own_login(r["user"]["login"].as_str().unwrap_or("")),
            state: r["state"].as_str().unwrap_or("").to_string(),
            body: r["body"].as_str().unwrap_or("").to_string(),
            commit_id: r["commit_id"].as_str().unwrap_or("").to_string(),
        })
        .collect()
}

/// The review threads as `common::review_supersede` reads them.
pub fn threads_of(nodes: &[Value]) -> Vec<common::review_supersede::Thread> {
    nodes
        .iter()
        .map(|t| {
            let comments: Vec<&Value> = t["comments"]["nodes"]
                .as_array()
                .map(|a| a.iter().collect())
                .unwrap_or_default();
            common::review_supersede::Thread {
                id: t["id"].as_str().unwrap_or("").to_string(),
                resolved: t["isResolved"].as_bool().unwrap_or(false),
                path: t["path"].as_str().unwrap_or("").to_string(),
                first_body: comments
                    .first()
                    .and_then(|c| c["body"].as_str())
                    .unwrap_or("")
                    .to_string(),
                comments: comments
                    .iter()
                    .map(|c| common::review_supersede::ThreadComment {
                        node_id: c["id"].as_str().unwrap_or("").to_string(),
                        own: own_login(c["author"]["login"].as_str().unwrap_or("")),
                        review_id: c["pullRequestReview"]["databaseId"].as_u64().unwrap_or(0),
                        minimized: c["isMinimized"].as_bool().unwrap_or(false),
                    })
                    .collect(),
                outdated: t["isOutdated"].as_bool().unwrap_or(false),
                line_gone: t["line"].is_null(),
                holds_at_head: None,
            }
        })
        .collect()
}

/// After CoderHelm posted review `new_id` on a pull request, its earlier
/// reviews get out of the way (`common::review_supersede`): bodies cut to
/// one line, earlier inline comments hidden as outdated, threads the new
/// review no longer raises resolved, its own earlier request for changes
/// dismissed. Best effort: a step GitHub refuses (a repository that limits
/// dismissals, a missing permission) is skipped with a log line; the review
/// already posted stands.
pub async fn supersede_reviews(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    new_id: u64,
    raised: &[String],
) {
    use common::review_supersede::{plan, DISMISS_MESSAGE, OUTDATED_BODY};
    let reviews = match gh.list_pr_reviews(owner, repo, number).await {
        Ok(v) => reviews_of(&v),
        Err(e) => {
            warn!(pr = number, error = %e, "Could not list reviews to supersede CoderHelm's earlier ones");
            return;
        }
    };
    let threads = match gh.list_review_threads_detail(owner, repo, number).await {
        Ok(v) => threads_of(&v),
        Err(e) => {
            tracing::info!(pr = number, error = %e, "Could not read review threads; earlier reviews are only rewritten");
            Vec::new()
        }
    };
    let p = plan(&reviews, &threads, new_id, raised);
    for id in &p.edit {
        if let Err(e) = gh
            .update_review_body(owner, repo, number, *id, OUTDATED_BODY)
            .await
        {
            tracing::info!(pr = number, review_id = id, error = %e, "Skipped rewriting an earlier review");
        }
    }
    for node in &p.minimize {
        if let Err(e) = gh.hide_own_comment(node, true).await {
            tracing::info!(pr = number, comment = %node, error = %e, "Skipped hiding an earlier review comment");
        }
    }
    for t in &p.resolve {
        if let Err(e) = gh.resolve_review_thread(t).await {
            tracing::info!(pr = number, thread = %t, error = %e, "Skipped resolving a settled review thread");
        }
    }
    for id in &p.dismiss {
        if let Err(e) = gh
            .dismiss_review(owner, repo, number, *id, DISMISS_MESSAGE)
            .await
        {
            tracing::info!(pr = number, review_id = id, error = %e, "Skipped dismissing an earlier request for changes (the repository may limit dismissals)");
        }
    }
}

/// Cuts CoderHelm's own review bodies to the one line
/// `common::review_supersede::tidy` decided (`review_tidy`), here with the
/// other rewrites of its earlier reviews. Best effort, logged.
pub async fn collapse_own_reviews(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    collapse: &[(u64, String)],
) {
    for (id, body) in collapse {
        if let Err(e) = gh.update_review_body(owner, repo, number, *id, body).await {
            tracing::info!(pr = number, review_id = id, error = %e, "Skipped cutting a review of CoderHelm's to one line");
        }
    }
}

/// What CoderHelm last replied in each review thread (this process), so a
/// retried round doesn't say the same thing in it twice.
static REPLIED: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The slot of a reply in a review thread.
const REVIEW_SLOT: &str = "review-thread";

/// A pull request's review comments read in the last [`CACHE_SECS`]
/// seconds: one feedback round answers several threads.
type ReadAt = (Instant, Vec<Value>);

static REVIEW_COMMENTS: LazyLock<Mutex<HashMap<String, ReadAt>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

async fn review_comments(gh: &GitHubClient, owner: &str, repo: &str, pr: u64) -> Vec<Value> {
    let key = format!("{owner}/{repo}#{pr}").to_ascii_lowercase();
    if let Some(v) = REVIEW_COMMENTS.lock().ok().and_then(|c| {
        c.get(&key)
            .filter(|(at, _)| at.elapsed().as_secs() < CACHE_SECS)
            .map(|(_, v)| v.clone())
    }) {
        return v;
    }
    let all = match gh.get_review_comments(owner, repo, pr).await {
        Ok(v) => v,
        Err(e) => {
            warn!(pr, error = %e, "Could not read the review threads before replying; replying on what is known");
            return Vec::new();
        }
    };
    if let Ok(mut c) = REVIEW_COMMENTS.lock() {
        if c.len() > 200 {
            c.clear();
        }
        c.insert(key, (Instant::now(), all.clone()));
    }
    all
}

/// One review thread as the composer reads it: the comments in the thread
/// of `comment_id`, the comment itself, and the diff it sits on.
pub fn review_thread(
    all: &[Value],
    comment_id: u64,
) -> (View, Option<Heard>, Option<(String, u64, String)>) {
    let id_of = |c: &Value| c["id"].as_u64().unwrap_or(0);
    let target = all.iter().find(|c| id_of(c) == comment_id);
    let root = target
        .and_then(|t| t["in_reply_to_id"].as_u64())
        .unwrap_or(comment_id);
    let thread: Vec<Value> = all
        .iter()
        .filter(|c| id_of(c) == root || c["in_reply_to_id"].as_u64() == Some(root))
        .cloned()
        .collect();
    let mut view = common::conversation_io::view_from_github(&thread);
    // CoderHelm's replies from before they carried a marker are context too.
    for c in &thread {
        let bot = c["user"]["type"].as_str() == Some("Bot")
            || c["user"]["login"]
                .as_str()
                .is_some_and(|l| l.ends_with("[bot]"));
        let body = c["body"].as_str().unwrap_or("");
        if bot && conversation::read_marker(body).is_none() {
            let at = c["created_at"]
                .as_str()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|t| t.timestamp_millis())
                .unwrap_or(0);
            view.said.push(common::conversation::Said {
                id: id_of(c).to_string(),
                at_ms: at,
                meta: Meta::default(),
                body: body.to_string(),
                node_id: String::new(),
            });
        }
    }
    let to = view
        .heard
        .iter()
        .find(|h| h.id == comment_id.to_string())
        .cloned()
        .map(|h| Heard { line: true, ..h });
    let code = target.map(|t| {
        (
            t["path"].as_str().unwrap_or("").to_string(),
            t["line"]
                .as_u64()
                .or(t["original_line"].as_u64())
                .unwrap_or(0),
            t["diff_hunk"].as_str().unwrap_or("").to_string(),
        )
    });
    (view, to, code)
}

/// A reply in a pull request's review thread, to a person's comment
/// `comment_id`: `body` is the path's draft; the composer writes the reply
/// from the thread, the diff under the comment and `facts`. The same reply
/// in the same thread again is not posted.
#[allow(clippy::too_many_arguments)]
pub async fn reply_review(
    r: &Replier<'_>,
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    pr: u64,
    comment_id: u64,
    body: &str,
    facts: &Facts,
) -> Result<Value, Error> {
    let key = format!("{owner}/{repo}#{pr}/{comment_id}");
    let fp = conversation::fingerprint(body);
    let quiet = || {
        Ok(
            json!({ "id": null, "spoke": "quiet", "why": "the same reply is already in the thread" }),
        )
    };
    if REPLIED
        .lock()
        .ok()
        .and_then(|r| r.get(&key).cloned())
        .is_some_and(|f| f == fp)
    {
        return quiet();
    }
    let all = Box::pin(review_comments(gh, owner, repo, pr)).await;
    let (view, to, code) = review_thread(&all, comment_id);
    // A retry in another process: the thread already holds this reply.
    if view
        .said
        .iter()
        .any(|s| s.meta.slot == REVIEW_SLOT && s.meta.fp == fp)
    {
        return quiet();
    }
    let mut facts = facts.clone();
    if let Some((path, line, hunk)) = code.filter(|(p, _, _)| !p.is_empty()) {
        facts = facts.code(&path, line, &hunk);
    }
    let to = to.unwrap_or(Heard {
        id: comment_id.to_string(),
        line: true,
        ..Default::default()
    });
    let place = Place::github(owner, repo, pr);
    let now = chrono::Utc::now().timestamp_millis();
    let say = Say::reply(REVIEW_SLOT, body, vec![to.id.clone()]);
    let meta = Meta {
        slot: REVIEW_SLOT.to_string(),
        kind: Kind::Reply,
        fp: fp.clone(),
        gist: conversation::cut_words(&conversation::visible(body), 160),
        first_ms: now,
        ..Default::default()
    };
    let decision = Decision::Reply {
        to: to.clone(),
        others: vec![to],
        how: How::Thread,
        ack: String::new(),
        text: conversation::shape(body, place.budget(), true),
        meta,
    };
    let decision = Box::pin(reply_compose::apply(
        Compose {
            composer: r,
            facts: &facts,
        },
        &view,
        &say,
        &place,
        decision,
    ))
    .await;
    let v = match decision {
        Decision::Reply { text, meta, .. } => {
            // The same house style and thread rules as every other comment.
            let text = match common::comment_style::for_thread(
                &text,
                &place,
                common::comment_style::REPLY_LINES,
            ) {
                Ok(t) if !t.trim().is_empty() => t,
                Ok(_) => return quiet(),
                Err(problems) => {
                    let why = problems
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; ");
                    tracing::error!(pr, problems = %why, "comment style defect: a review thread reply broke the style; not posted");
                    return Ok(json!({ "id": null, "spoke": "quiet", "why": why }));
                }
            };
            let shaped = conversation::shape(&text, place.budget(), true);
            gh.reply_to_review_comment(
                owner,
                repo,
                pr,
                comment_id,
                &conversation::with_marker(&shaped, &meta),
            )
            .await?
        }
        Decision::React { content, .. } => {
            if let Err(e) = gh
                .add_reaction(owner, repo, comment_id, true, &content)
                .await
            {
                warn!(pr, error = %e, "Could not react on the review comment");
            }
            json!({ "id": null, "spoke": "quiet", "why": "reacted on the comment" })
        }
        _ => return quiet(),
    };
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
    Box::pin(jira_with(
        state,
        team_id,
        ticket,
        say,
        comment_type,
        title,
        judge,
        None,
    ))
    .await
}

/// A reply to people on a Jira ticket: `say` is the path's draft; the
/// composer writes it from the ticket's thread and `facts`.
pub async fn reply_jira(
    r: &Replier<'_>,
    ticket: &str,
    say: Say,
    facts: &Facts,
    comment_type: &str,
    title: &str,
) -> Result<(Spoke, Option<String>), Error> {
    Box::pin(jira_with(
        r.state,
        r.team_id,
        ticket,
        say,
        comment_type,
        title,
        None,
        Some(Compose { composer: r, facts }),
    ))
    .await
}

#[allow(clippy::too_many_arguments)]
async fn jira_with(
    state: &WorkerState,
    team_id: &str,
    ticket: &str,
    say: Say,
    comment_type: &str,
    title: &str,
    judge: Option<&dyn Judge>,
    compose: Option<Compose<'_>>,
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
    let spoke = Box::pin(conversation::speak_with(&ch, judge, compose, &say, now)).await?;
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
    /// GitHub's shapes, read for superseding: a person's review and a
    /// person's thread are never CoderHelm's, whatever they say.
    #[test]
    fn supersede_reads_who_wrote_what() {
        let reviews = serde_json::json!([
            {"id": 1, "state": "CHANGES_REQUESTED", "body": "Fix the null check.", "user": {"login": "coderhelm[bot]", "type": "Bot"}},
            {"id": 2, "state": "CHANGES_REQUESTED", "body": "Please rename this.", "user": {"login": "reviewer-one", "type": "User"}},
            {"id": 3, "state": "COMMENTED", "body": "coderhelm said this", "user": {"login": "coderhelm-fan", "type": "User"}},
            {"id": 4, "state": "APPROVED", "body": "All good.", "user": {"login": "coderhelm[bot]", "type": "Bot"}}
        ]);
        let r = super::reviews_of(&reviews);
        assert_eq!(
            r.iter().filter(|x| x.own).map(|x| x.id).collect::<Vec<_>>(),
            vec![1, 4]
        );
        let threads = vec![serde_json::json!({
            "id": "T1", "isResolved": false, "path": "src/a.ts",
            "comments": {"nodes": [
                {"id": "C1", "isMinimized": false, "body": "Null check missing.", "author": {"login": "coderhelm"}, "pullRequestReview": {"databaseId": 1}},
                {"id": "P1", "isMinimized": false, "body": "Why?", "author": {"login": "reviewer-one"}, "pullRequestReview": {"databaseId": 2}}
            ]}
        })];
        let t = super::threads_of(&threads);
        assert!(t[0].own() && t[0].person_waiting());
        let p = common::review_supersede::plan(&r, &t, 4, &[]);
        assert_eq!(p.edit, vec![1]);
        assert_eq!(p.dismiss, vec![1]);
        // The person's question is unanswered: the thread stays in sight.
        assert!(p.minimize.is_empty() && p.resolve.is_empty(), "{p:?}");
    }

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

    /// Every reply to a person, in the worker and the gateway, goes through
    /// the composer: a `Say::reply_all`, `Say::reply`, `Say::ack` (or a
    /// person-facing status such as the comment acknowledgement) handed to
    /// the plain `github`, `jira` or `jira_at`, or a reply in a review
    /// thread written any other way than `reply_review`, fails here.
    #[test]
    fn every_reply_to_a_person_goes_through_the_composer() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let says = regex::Regex::new(
            r#"Say::(?:reply_all|reply|ack)\(|Say::status\(\s*"comment-received""#,
        )
        .unwrap();
        let calls = regex::Regex::new(
            r"(?:\breply_github|\breply_jira_at|\breply_jira|\breply_review|conversation::github|conversation::jira_at|conversation::jira|github_judged|jira_judged|\bspeak)\(|let say =",
        )
        .unwrap();
        let composed = |c: &str| {
            [
                "reply_github(",
                "reply_jira_at(",
                "reply_jira(",
                "reply_review(",
            ]
            .iter()
            .any(|k| c.ends_with(k))
        };
        let mut found = Vec::new();
        let mut seen = 0;
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
                let rel = p
                    .strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                // This layer, and the eval (a test module) that feeds it.
                if rel == "worker/src/passes/conversation.rs"
                    || rel == "worker/src/passes/reply_eval.rs"
                {
                    continue;
                }
                let src = std::fs::read_to_string(&p).unwrap();
                let code: String = super::scan::non_test_lines(&src)
                    .into_iter()
                    .filter(|(_, l)| !l.trim_start().starts_with("//"))
                    .map(|(_, l)| l)
                    .collect::<Vec<_>>()
                    .join("\n");
                // The plain reply in a review thread is gone; only
                // `reply_review` writes there.
                if code.contains("review_reply(") {
                    found.push(format!("{rel}: a review thread reply outside reply_review"));
                }
                for m in says.find_iter(&code) {
                    seen += 1;
                    let before: Vec<regex::Match> = calls.find_iter(&code[..m.start()]).collect();
                    let Some(open) = before.last() else {
                        found.push(format!("{rel}: {} with no call", m.as_str()));
                        continue;
                    };
                    let call = if open.as_str() == "let say =" {
                        // The say goes to the next call.
                        calls
                            .find_iter(&code[m.end()..])
                            .find(|c| c.as_str() != "let say =")
                            .map(|c| c.as_str().to_string())
                            .unwrap_or_default()
                    } else {
                        open.as_str().to_string()
                    };
                    if !composed(&call) {
                        let line = code[..m.start()].matches('\n').count() + 1;
                        found.push(format!("{rel}:~{line}: {} goes to {call}", m.as_str()));
                    }
                }
            }
        }
        assert!(
            seen >= 15,
            "the scan found only {seen} replies: is it reading the sources?"
        );
        assert!(
            found.is_empty(),
            "replies to people that skip the composer:\n{}",
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
        let call = regex::Regex::new(
            r#"(?s)(?:conversation::(?:reply_)?jira(?:_judged|_at)?)\((.{0,600}?)\.await"#,
        )
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
