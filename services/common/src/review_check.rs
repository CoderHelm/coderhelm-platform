//! The pull request's "CoderHelm review" check run, from the moment a review
//! is scheduled to the moment it ends.
//!
//! GitHub shows checks per head commit. A push review waits a few minutes for
//! the PR to go quiet, so the gateway puts the check on the new head as soon
//! as it schedules the review, `queued`, saying when the review starts. The
//! worker takes that check over when the job starts (`in_progress`) and
//! completes it however the job ends; a job that hands its review to a later
//! one (a retry, a full review) hands the check over with it, queued again.
//!
//! Every way CoderHelm accepts work on a PR shows here within seconds: a push,
//! a label, a reply, a re-review, an edited description. A job on a commit
//! whose check already finished opens a new run of the same name, which GitHub
//! shows in place of the finished one, so the commit keeps one check. A job
//! that ends without a new verdict (a reply that was only answered, a re-review
//! with nothing changed) completes its run with the conclusion and title of
//! the run before it ([`close_restoring`]), so the commit always shows the
//! verdict that stands.
//!
//! Each check carries an `external_id` naming the job it waits for and when
//! that job is due ([`external_id`]). A job finds its own check by that key,
//! and a check still open long after its job was due is completed by the
//! next review of the commit or by the stuck-review sweep, so a stale check
//! never sits on a PR.
//!
//! A check run needs the app's checks:write permission, which an installation
//! may not have granted. GitHub then refuses (403) and the review goes on
//! exactly as without it: the refusal is logged once per installation, and
//! nothing else changes.

use std::collections::HashSet;
use std::sync::Mutex;

use serde_json::{json, Value};
use tracing::{info, warn};

/// The check's name on the PR.
pub const CHECK_NAME: &str = "CoderHelm review";

/// What every `external_id` this code writes starts with.
const ID_PREFIX: &str = "coderhelm-review";

/// How long after its job was due an open check is given up on: a job's
/// lease (it stops by then, finished or not) and a margin for the queue.
pub const STALE_AFTER_SECS: i64 = 17 * 60 + 5 * 60;

/// The `external_id` of a check waiting for the job with dedup key `key`,
/// due to start at `due` (unix seconds).
pub fn external_id(key: &str, due: i64) -> String {
    format!("{ID_PREFIX}|{due}|{key}")
}

/// The job key and due time an `external_id` names; `None` for one this code
/// didn't write (an older check carried the job's own id).
pub fn parse_external_id(id: &str) -> Option<(i64, &str)> {
    let rest = id.strip_prefix(ID_PREFIX)?.strip_prefix('|')?;
    let (due, key) = rest.split_once('|')?;
    Some((due.parse().ok()?, key))
}

/// The PR's reviews page on the dashboard at `app`.
pub fn pr_page(app: &str, owner: &str, repo: &str, pr: u64) -> String {
    format!("{app}/reviewer/pr?repo={owner}/{repo}&pr={pr}")
}

/// The first seven characters of a commit.
pub fn short(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

fn clock(at: i64) -> String {
    chrono::DateTime::from_timestamp(at, 0)
        .map(|t| t.format("%H:%M UTC").to_string())
        .unwrap_or_default()
}

/// Pure: the title of a check waiting for its review, due at `due`.
pub fn queued_title(head: &str, due: i64, now: i64) -> String {
    let short = short(head);
    if due - now >= 30 {
        format!(
            "Waiting for more pushes, then reviewing {short} at {}",
            clock(due)
        )
    } else {
        format!("Reviewing {short} shortly")
    }
}

/// Whether the gateway may show a job on the commit when it sends it. A reply
/// that didn't address CoderHelm may not be for it at all: only the worker can
/// tell, by reading whose thread or text it answers. It leaves the check alone
/// until then and opens it if the reply turns out to answer CoderHelm.
pub fn shows_when_sent(trigger: &str, mentioned: bool) -> bool {
    match trigger {
        "thread_reply" => mentioned,
        "quote_reply" => false,
        _ => true,
    }
}

/// What a job does, in the words of a check's title.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Work {
    /// A person's comment to read and answer.
    Reply,
    /// A person asked for another look.
    ReReview,
    /// The description was edited to answer the review.
    Description,
    /// A push, a label, the PR opening or leaving draft, a run's own review.
    Review,
}

impl Work {
    /// The work of a job from its trigger and whether it carries a comment.
    pub fn of(trigger: &str, has_question: bool) -> Self {
        if has_question {
            return Work::Reply;
        }
        match trigger {
            "reply" | "rerequest" => Work::ReReview,
            crate::review_description::TRIGGER => Work::Description,
            _ => Work::Review,
        }
    }
}

/// Pure: the title of a check waiting for job `work`, due at `due`.
pub fn waiting_title(work: Work, head: &str, due: i64, now: i64) -> String {
    let short = short(head);
    match work {
        Work::Reply => format!("Reading your reply on {short}"),
        Work::ReReview => format!("Re-reviewing {short}"),
        Work::Description => format!("Re-reviewing {short} with the edited description"),
        Work::Review => queued_title(head, due, now),
    }
}

/// Pure: the title of a check whose job has started.
pub fn started_title(work: Work, head: &str) -> String {
    let short = short(head);
    match work {
        Work::Reply => format!("Reading your reply on {short}"),
        Work::ReReview => format!("Re-reviewing {short}"),
        Work::Description => format!("Re-reviewing {short} with the edited description"),
        Work::Review => format!("Reviewing {short}"),
    }
}

/// Pure: the summary of a check waiting for job `work`.
pub fn waiting_summary(work: Work, due: i64, now: i64, details_url: &str) -> String {
    match work {
        Work::Review => queued_summary(due, now, details_url),
        Work::Reply => format!(
            "CoderHelm got your reply and is reading it. The verdict on this commit stays as it \
             is unless something it rests on changed.\n\n[See the pull request's \
             reviews]({details_url}) on CoderHelm."
        ),
        Work::ReReview | Work::Description => format!(
            "CoderHelm got the request and starts on it shortly.\n\n[See the pull request's \
             reviews]({details_url}) on CoderHelm."
        ),
    }
}

/// Pure: the summary of a check waiting for its review.
pub fn queued_summary(due: i64, now: i64, details_url: &str) -> String {
    let wait = if due - now >= 30 {
        format!(
            "CoderHelm reviews this commit at {} if no newer commit is pushed by then; a \
             newer push replaces this review with the review of that commit.",
            clock(due)
        )
    } else {
        "CoderHelm starts reviewing this commit shortly.".to_string()
    };
    format!("{wait}\n\n[See the pull request's reviews]({details_url}) on CoderHelm.")
}

/// One CoderHelm check on a commit that hasn't completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCheck {
    pub id: u64,
    /// The job it waits for; `None` for a check this code didn't key.
    pub key: Option<String>,
    /// When that job was due, or when the check started.
    pub since: Option<i64>,
    /// Waiting for its job (not yet taken over).
    pub queued: bool,
}

impl OpenCheck {
    /// Open long after its job was due: that job stopped without ending it.
    pub fn stale(&self, now: i64) -> bool {
        match self.since {
            Some(t) => now - t > STALE_AFTER_SECS,
            None => true,
        }
    }
}

/// Pure: the CoderHelm checks in a `check-runs` listing that haven't
/// completed, oldest first.
pub fn open_checks(list: &Value) -> Vec<OpenCheck> {
    let mut out: Vec<OpenCheck> = list["check_runs"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[])
        .iter()
        .filter(|c| c["name"].as_str() == Some(CHECK_NAME))
        .filter(|c| c["status"].as_str() != Some("completed"))
        .filter_map(|c| {
            let id = c["id"].as_u64()?;
            let parsed = c["external_id"].as_str().and_then(parse_external_id);
            let started = c["started_at"]
                .as_str()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|t| t.timestamp());
            Some(OpenCheck {
                id,
                key: parsed.map(|(_, k)| k.to_string()),
                since: parsed.map(|(d, _)| d).or(started),
                queued: c["status"].as_str() == Some("queued"),
            })
        })
        .collect();
    out.sort_by_key(|c| c.id);
    out
}

/// The finished result a commit's check shows: the newest completed CoderHelm
/// run that isn't `except`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Standing {
    pub conclusion: &'static str,
    pub title: String,
    pub summary: String,
}

/// A conclusion this code writes, or neutral.
fn known_conclusion(c: &str) -> &'static str {
    match c {
        "success" => "success",
        "failure" => "failure",
        "skipped" => "skipped",
        _ => "neutral",
    }
}

/// Pure: [`Standing`] from a `check-runs` listing.
pub fn standing_in(list: &Value, except: u64) -> Option<Standing> {
    list["check_runs"]
        .as_array()?
        .iter()
        .filter(|c| c["name"].as_str() == Some(CHECK_NAME))
        .filter(|c| c["status"].as_str() == Some("completed"))
        .filter(|c| c["id"].as_u64() != Some(except))
        .max_by_key(|c| c["id"].as_u64().unwrap_or(0))
        .map(|c| Standing {
            conclusion: known_conclusion(c["conclusion"].as_str().unwrap_or("")),
            title: c["output"]["title"].as_str().unwrap_or("").to_string(),
            summary: c["output"]["summary"].as_str().unwrap_or("").to_string(),
        })
}

/// The three check-run calls, so every path can be tested without GitHub.
#[async_trait::async_trait]
pub trait CheckCalls: Sync {
    /// Create a check run; its id.
    async fn create(&self, owner: &str, repo: &str, body: &Value) -> Result<u64, String>;
    /// Update check run `id`.
    async fn update(&self, owner: &str, repo: &str, id: u64, body: &Value) -> Result<(), String>;
    /// The check runs named [`CHECK_NAME`] on commit `head`.
    async fn list(&self, owner: &str, repo: &str, head: &str) -> Result<Value, String>;
}

/// The calls made with an installation token (the gateway's; a hybrid
/// install's token comes from CoderHelm's broker, which allows checks:write).
pub struct TokenCalls<'a> {
    pub http: &'a reqwest::Client,
    pub token: String,
}

impl TokenCalls<'_> {
    async fn send(&self, req: reqwest::RequestBuilder) -> Result<Value, String> {
        let resp = req
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Coderhelm-bot")
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!(
                "GitHub answered {status}: {}",
                crate::truncate_str(&body, 300)
            ));
        }
        Ok(serde_json::from_str(&body).unwrap_or(Value::Null))
    }
}

const API: &str = "https://api.github.com";

#[async_trait::async_trait]
impl CheckCalls for TokenCalls<'_> {
    async fn create(&self, owner: &str, repo: &str, body: &Value) -> Result<u64, String> {
        let url = format!("{API}/repos/{owner}/{repo}/check-runs");
        let out = self.send(self.http.post(url).json(body)).await?;
        out["id"]
            .as_u64()
            .ok_or_else(|| "check run created without an id".to_string())
    }
    async fn update(&self, owner: &str, repo: &str, id: u64, body: &Value) -> Result<(), String> {
        let url = format!("{API}/repos/{owner}/{repo}/check-runs/{id}");
        self.send(self.http.patch(url).json(body)).await.map(|_| ())
    }
    async fn list(&self, owner: &str, repo: &str, head: &str) -> Result<Value, String> {
        let url = list_url(owner, repo, head);
        self.send(self.http.get(url)).await
    }
}

/// The listing of CoderHelm's checks on `head`: every run, not only the newest
/// of the name, so a job can read what the commit showed before it.
pub fn list_url(owner: &str, repo: &str, head: &str) -> String {
    format!(
        "{API}/repos/{owner}/{repo}/commits/{head}/check-runs?check_name={}&filter=all&per_page=100",
        urlencoding::encode(CHECK_NAME)
    )
}

/// True when GitHub refused for want of permission (the installation has
/// no checks:write).
pub fn is_refusal(e: &str) -> bool {
    e.contains("403 Forbidden") || e.contains("Resource not accessible by integration")
}

/// Installations that refused a check run in this process: their refusal
/// was logged once.
static REFUSED: Mutex<Option<HashSet<u64>>> = Mutex::new(None);

/// True the first time `installation` refuses in this process.
pub fn first_refusal(installation: u64) -> bool {
    let mut set = REFUSED.lock().unwrap_or_else(|p| p.into_inner());
    set.get_or_insert_with(HashSet::new).insert(installation)
}

/// Log a failed check-run call. A refusal for want of permission is the
/// expected state of an installation that hasn't granted checks:write: one
/// info line, then silence.
pub fn log_failure(installation: u64, pr: u64, what: &str, e: &str) {
    if is_refusal(e) {
        if first_refusal(installation) {
            info!(
                installation,
                pr,
                "GitHub refused the CoderHelm review check (the app has no checks:write \
                 permission on this installation); reviews go on without it"
            );
        }
        return;
    }
    warn!(installation, pr, what, error = %e, "CoderHelm review check: GitHub call failed");
}

/// Where a review is: the check's place on the PR.
#[derive(Debug, Clone, Copy)]
pub struct Target<'a> {
    pub installation: u64,
    pub owner: &'a str,
    pub repo: &'a str,
    pub pr: u64,
    pub head: &'a str,
}

/// The open CoderHelm checks on the target's head; `None` when GitHub
/// refused or failed (logged).
async fn open_on_head<C: CheckCalls + ?Sized>(api: &C, t: &Target<'_>) -> Option<Vec<OpenCheck>> {
    match api.list(t.owner, t.repo, t.head).await {
        Ok(list) => Some(open_checks(&list)),
        Err(e) => {
            log_failure(t.installation, t.pr, "list", &e);
            None
        }
    }
}

/// What [`queue`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Queued {
    /// A new queued check, this id.
    Created(u64),
    /// The head already shows an open CoderHelm check (this one): the review
    /// takes it over when it starts, so no second check is added.
    Exists(u64),
    /// GitHub refused or failed: the review goes on without a check.
    Nothing,
}

/// A review of `t.head` is scheduled for job `key`, due at `due`: show it
/// on the commit right away as a queued check, its Details linking
/// `details_url`. One open CoderHelm check per commit.
pub async fn queue<C: CheckCalls + ?Sized>(
    api: &C,
    t: &Target<'_>,
    key: &str,
    due: i64,
    details_url: &str,
) -> Queued {
    queue_work(api, t, Work::Review, key, due, details_url).await
}

/// CoderHelm accepted `work` on `t.head` for job `key`, due at `due`: the
/// commit shows it right away. A commit already showing an open check keeps
/// it (the job running there speaks for the commit); a finished one is shown
/// over by this new run until the job ends.
pub async fn queue_work<C: CheckCalls + ?Sized>(
    api: &C,
    t: &Target<'_>,
    work: Work,
    key: &str,
    due: i64,
    details_url: &str,
) -> Queued {
    if t.head.is_empty() {
        return Queued::Nothing;
    }
    let Some(open) = open_on_head(api, t).await else {
        return Queued::Nothing;
    };
    let now = chrono::Utc::now().timestamp();
    if let Some(c) = open.iter().find(|c| !c.stale(now)) {
        return Queued::Exists(c.id);
    }
    let body = json!({
        "name": CHECK_NAME,
        "head_sha": t.head,
        "status": "queued",
        "external_id": external_id(key, due),
        "details_url": details_url,
        "output": {
            "title": crate::truncate_str(&waiting_title(work, t.head, due, now), 250),
            "summary": waiting_summary(work, due, now, details_url),
        },
    });
    let id = match api.create(t.owner, t.repo, &body).await {
        Ok(id) => id,
        Err(e) => {
            log_failure(t.installation, t.pr, "queue", &e);
            return Queued::Nothing;
        }
    };
    // A stale one left open on the same commit is given up on now.
    for c in open {
        close_restoring(api, t, c.id, &stopped_close(details_url)).await;
    }
    Queued::Created(id)
}

/// The body that moves a check to in progress for job `key`.
fn in_progress_body(key: &str, title: &str, details_url: &str) -> Value {
    let now = chrono::Utc::now();
    json!({
        "status": "in_progress",
        "started_at": now.to_rfc3339(),
        "external_id": external_id(key, now.timestamp()),
        "details_url": details_url,
        "output": {
            "title": crate::truncate_str(title, 250),
            "summary": format!(
                "CoderHelm is reviewing this pull request. [Follow the review]({details_url})."
            ),
        },
    })
}

/// Job `key` starts: take over the check queued for it on `t.head` and move
/// it to in progress. `None` when no check waits for this job (a job nobody
/// queued a check for opens one when its formal review starts, see
/// [`take_over_or_open`]).
pub async fn start<C: CheckCalls + ?Sized>(
    api: &C,
    t: &Target<'_>,
    key: &str,
    title: &str,
    details_url: &str,
) -> Option<u64> {
    if t.head.is_empty() || key.is_empty() {
        return None;
    }
    let open = open_on_head(api, t).await?;
    let mine = open.iter().find(|c| c.key.as_deref() == Some(key))?;
    match api
        .update(
            t.owner,
            t.repo,
            mine.id,
            &in_progress_body(key, title, details_url),
        )
        .await
    {
        Ok(()) => Some(mine.id),
        Err(e) => {
            log_failure(t.installation, t.pr, "start", &e);
            None
        }
    }
}

/// Check `id`, held by job `key`, says what the review does now and links
/// `details_url`.
pub async fn progress<C: CheckCalls + ?Sized>(
    api: &C,
    t: &Target<'_>,
    id: u64,
    key: &str,
    title: &str,
    details_url: &str,
) {
    if let Err(e) = api
        .update(
            t.owner,
            t.repo,
            id,
            &in_progress_body(key, title, details_url),
        )
        .await
    {
        log_failure(t.installation, t.pr, "progress", &e);
    }
}

/// A formal review of `t.head` starts for job `key`: take over the open
/// CoderHelm check on the commit (one queued for a review this one now
/// does, or one left by a job that stopped), or open a new one in progress.
pub async fn take_over_or_open<C: CheckCalls + ?Sized>(
    api: &C,
    t: &Target<'_>,
    key: &str,
    title: &str,
    details_url: &str,
) -> Option<u64> {
    if t.head.is_empty() {
        return None;
    }
    let open = open_on_head(api, t).await?;
    let body = in_progress_body(key, title, details_url);
    if let Some(c) = open.first() {
        return match api.update(t.owner, t.repo, c.id, &body).await {
            Ok(()) => Some(c.id),
            Err(e) => {
                log_failure(t.installation, t.pr, "take over", &e);
                None
            }
        };
    }
    let mut body = body;
    body["name"] = json!(CHECK_NAME);
    body["head_sha"] = json!(t.head);
    match api.create(t.owner, t.repo, &body).await {
        Ok(id) => Some(id),
        Err(e) => {
            log_failure(t.installation, t.pr, "open", &e);
            None
        }
    }
}

/// Hand check `id` to job `key`, due at `due`: queued again, saying why.
/// That job takes it over when it starts.
pub async fn hand_off<C: CheckCalls + ?Sized>(
    api: &C,
    t: &Target<'_>,
    id: u64,
    key: &str,
    due: i64,
    title: &str,
    summary: &str,
) -> bool {
    let body = json!({
        "status": "queued",
        "external_id": external_id(key, due),
        "output": {
            "title": crate::truncate_str(title, 250),
            "summary": crate::truncate_str(summary, 60_000),
        },
    });
    match api.update(t.owner, t.repo, id, &body).await {
        Ok(()) => true,
        Err(e) => {
            log_failure(t.installation, t.pr, "hand off", &e);
            false
        }
    }
}

/// A completed check: its conclusion and output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    pub conclusion: &'static str,
    pub title: String,
    pub summary: String,
    /// A new Details link, when the old one no longer fits.
    pub details_url: Option<String>,
}

/// The ending of a check whose job stopped without ending it.
pub fn stopped_close(details_url: &str) -> Close {
    Close {
        conclusion: "neutral",
        title: "Review didn't finish: it stopped without a result".into(),
        summary: "The review this check waited for stopped without a result, so there is no \
                  new verdict for this commit; any earlier verdict stands. It runs again on \
                  the next push, or comment **@coderhelm re-review** to run it now."
            .into(),
        details_url: Some(details_url.to_string()),
    }
}

/// Complete check `id`. Never fails the review: an error is logged.
pub async fn close<C: CheckCalls + ?Sized>(api: &C, t: &Target<'_>, id: u64, c: &Close) {
    let mut body = json!({
        "status": "completed",
        "conclusion": c.conclusion,
        "completed_at": chrono::Utc::now().to_rfc3339(),
        "output": {
            "title": crate::truncate_str(&c.title, 250),
            "summary": crate::truncate_str(&c.summary, 60_000),
        },
    });
    if let Some(d) = &c.details_url {
        body["details_url"] = json!(d);
    }
    if let Err(e) = api.update(t.owner, t.repo, id, &body).await {
        log_failure(t.installation, t.pr, "close", &e);
    }
}

/// Complete check `id` for a job that ended without a new verdict: the commit
/// goes back to showing the run before it, the same conclusion and title, with
/// what this job did added to the summary. With no finished run before it,
/// `c` as it is.
pub async fn close_restoring<C: CheckCalls + ?Sized>(api: &C, t: &Target<'_>, id: u64, c: &Close) {
    let standing = match api.list(t.owner, t.repo, t.head).await {
        Ok(list) => standing_in(&list, id),
        Err(e) => {
            log_failure(t.installation, t.pr, "read the standing check", &e);
            None
        }
    };
    match standing {
        Some(s) => {
            let restored = Close {
                conclusion: s.conclusion,
                title: s.title,
                summary: format!("{}\n\n{}", s.summary.trim_end(), c.summary),
                details_url: c.details_url.clone(),
            };
            close(api, t, id, &restored).await;
        }
        None => close(api, t, id, c).await,
    }
}

/// Complete the check waiting for job `key` on `t.head`, if there is one:
/// for a job that ends before it takes its check over.
pub async fn close_waiting<C: CheckCalls + ?Sized>(
    api: &C,
    t: &Target<'_>,
    key: &str,
    c: &Close,
) -> bool {
    if t.head.is_empty() || key.is_empty() {
        return false;
    }
    let Some(open) = open_on_head(api, t).await else {
        return false;
    };
    // Still waiting: a job already running under this key (a redelivered copy
    // is what ends early) holds its check itself.
    let Some(mine) = open
        .iter()
        .find(|o| o.queued && o.key.as_deref() == Some(key))
    else {
        return false;
    };
    close_restoring(api, t, mine.id, c).await;
    true
}

/// Complete every CoderHelm check on `t.head` left open long after its job
/// was due (the stuck-review sweep). How many it completed.
pub async fn close_stale<C: CheckCalls + ?Sized>(
    api: &C,
    t: &Target<'_>,
    now: i64,
    details_url: &str,
) -> usize {
    let Some(open) = open_on_head(api, t).await else {
        return 0;
    };
    let mut n = 0;
    for c in open.iter().filter(|c| c.stale(now)) {
        close_restoring(api, t, c.id, &stopped_close(details_url)).await;
        n += 1;
    }
    n
}

/// Pure: a check's conclusion for a verdict that stands without a new
/// review: an approval passes, anything else is neutral (the review on the
/// PR says what holds it; the check never blocks a merge by itself).
pub fn standing_conclusion(verdict: &str) -> &'static str {
    if verdict.eq_ignore_ascii_case("APPROVE") {
        "success"
    } else {
        "neutral"
    }
}

/// Pure: a verdict in words.
pub fn verdict_words(verdict: &str) -> String {
    match verdict.to_ascii_uppercase().as_str() {
        "APPROVE" => "approved".into(),
        "REQUEST_CHANGES" => "changes requested".into(),
        "COMMENT" => "comments, nothing holding the merge".into(),
        other => other.to_lowercase().replace('_', " "),
    }
}

/// GitHub's check runs in memory, for the gateway's and the worker's tests.
#[doc(hidden)]
pub mod fake {
    use super::*;

    /// Check runs kept in memory; `refuse` answers every call with a 403.
    #[derive(Default)]
    pub struct FakeChecks {
        pub refuse: bool,
        pub runs: Mutex<Vec<Value>>,
        pub calls: Mutex<Vec<String>>,
    }

    impl FakeChecks {
        pub fn refusing() -> Self {
            Self {
                refuse: true,
                ..Default::default()
            }
        }
        pub fn run(&self, id: u64) -> Value {
            self.runs
                .lock()
                .unwrap()
                .iter()
                .find(|r| r["id"].as_u64() == Some(id))
                .cloned()
                .unwrap_or(Value::Null)
        }
        pub fn count(&self) -> usize {
            self.runs.lock().unwrap().len()
        }
        /// What GitHub shows for the commit: its newest run of the name.
        pub fn shown(&self, head: &str) -> Value {
            self.runs
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["head_sha"].as_str() == Some(head))
                .max_by_key(|r| r["id"].as_u64().unwrap_or(0))
                .cloned()
                .unwrap_or(Value::Null)
        }
        fn refused(&self, what: &str) -> Result<(), String> {
            self.calls.lock().unwrap().push(what.to_string());
            if self.refuse {
                Err(
                    "HTTP status client error (403 Forbidden): Resource not accessible by \
                     integration"
                        .into(),
                )
            } else {
                Ok(())
            }
        }
    }

    #[async_trait::async_trait]
    impl CheckCalls for FakeChecks {
        async fn create(&self, _o: &str, _r: &str, body: &Value) -> Result<u64, String> {
            self.refused("create")?;
            let mut runs = self.runs.lock().unwrap();
            let id = runs.len() as u64 + 1;
            let mut run = body.clone();
            run["id"] = json!(id);
            runs.push(run);
            Ok(id)
        }
        async fn update(&self, _o: &str, _r: &str, id: u64, body: &Value) -> Result<(), String> {
            self.refused("update")?;
            let mut runs = self.runs.lock().unwrap();
            let run = runs
                .iter_mut()
                .find(|r| r["id"].as_u64() == Some(id))
                .ok_or("no such check run")?;
            for (k, v) in body.as_object().unwrap() {
                run[k] = v.clone();
            }
            Ok(())
        }
        async fn list(&self, _o: &str, _r: &str, head: &str) -> Result<Value, String> {
            self.refused("list")?;
            let runs: Vec<Value> = self
                .runs
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["head_sha"].as_str() == Some(head))
                .cloned()
                .collect();
            Ok(json!({ "check_runs": runs }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::FakeChecks;
    use super::*;

    const HEAD: &str = "9fceb02d0ae598e95dc970b74767f19372d61af8";

    fn target(installation: u64) -> Target<'static> {
        Target {
            installation,
            owner: "acme",
            repo: "web",
            pr: 7,
            head: HEAD,
        }
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    #[test]
    fn the_external_id_names_the_job_and_when_it_is_due() {
        let id = external_id("head#abc", 1_700_000_000);
        assert_eq!(parse_external_id(&id), Some((1_700_000_000, "head#abc")));
        assert_eq!(
            parse_external_id("coderhelm-review|1|retry#1#head#abc"),
            Some((1, "retry#1#head#abc"))
        );
        // An older check carried the job's own id.
        assert_eq!(
            parse_external_id("1b4e28ba-2fa1-11d2-883f-0016d3cca427"),
            None
        );
    }

    #[test]
    fn the_queued_title_says_when_the_review_starts() {
        let t0 = 1_793_896_153; // 16:29:13 UTC
        assert_eq!(
            queued_title(HEAD, t0 + 180, t0),
            "Waiting for more pushes, then reviewing 9fceb02 at 16:32 UTC"
        );
        assert_eq!(queued_title(HEAD, t0, t0), "Reviewing 9fceb02 shortly");
        assert!(queued_summary(
            t0 + 180,
            t0,
            "https://app.example/reviewer/pr?repo=a/b&pr=7"
        )
        .contains("(https://app.example/reviewer/pr?repo=a/b&pr=7)"));
        assert_eq!(
            pr_page("https://app.example", "acme", "web", 7),
            "https://app.example/reviewer/pr?repo=acme/web&pr=7"
        );
    }

    #[test]
    fn only_coderhelm_checks_that_are_still_open_are_listed() {
        let list = json!({"check_runs": [
            {"id": 3, "name": CHECK_NAME, "status": "queued", "external_id": external_id("head#a", 100)},
            {"id": 1, "name": CHECK_NAME, "status": "in_progress", "external_id": "job-uuid", "started_at": "2026-11-05T16:00:00Z"},
            {"id": 2, "name": CHECK_NAME, "status": "completed"},
            {"id": 4, "name": "build", "status": "queued"},
        ]});
        let open = open_checks(&list);
        assert_eq!(open.len(), 2);
        assert_eq!(open[0].id, 1);
        assert_eq!(open[0].key, None);
        assert_eq!(open[1].key.as_deref(), Some("head#a"));
        assert_eq!(open[1].since, Some(100));
        // Long past its due time plus a lease: stale.
        assert!(open[1].stale(100 + STALE_AFTER_SECS + 1));
        assert!(!open[1].stale(100 + STALE_AFTER_SECS));
    }

    #[tokio::test]
    async fn a_scheduled_review_shows_a_queued_check_at_once() {
        let gh = FakeChecks::default();
        let t = target(1);
        let due = now() + 180;
        let q = queue(&gh, &t, "head#x", due, "https://app.example/reviewer/pr").await;
        assert_eq!(q, Queued::Created(1));
        let run = gh.run(1);
        assert_eq!(run["status"], "queued");
        assert_eq!(run["name"], CHECK_NAME);
        assert_eq!(run["head_sha"], HEAD);
        assert_eq!(run["details_url"], "https://app.example/reviewer/pr");
        assert_eq!(run["external_id"], external_id("head#x", due).as_str());
        assert!(run["output"]["title"]
            .as_str()
            .unwrap()
            .starts_with("Waiting for more pushes, then reviewing 9fceb02 at "));
        // A second scheduling for the same commit adds no second check.
        let again = queue(&gh, &t, "head#x", due, "u").await;
        assert_eq!(again, Queued::Exists(1));
        assert_eq!(gh.count(), 1);
    }

    #[tokio::test]
    async fn the_job_takes_over_its_queued_check() {
        let gh = FakeChecks::default();
        let t = target(1);
        queue(&gh, &t, "head#x", now() + 180, "u").await;
        // Another job's key doesn't take it.
        assert_eq!(start(&gh, &t, "desc#x", "Reviewing", "s").await, None);
        let id = start(&gh, &t, "head#x", "Reviewing 9fceb02", "s").await;
        assert_eq!(id, Some(1));
        let run = gh.run(1);
        assert_eq!(run["status"], "in_progress");
        assert_eq!(run["output"]["title"], "Reviewing 9fceb02");
        assert_eq!(gh.count(), 1);
        // The formal review then takes the same check, never a second one.
        let again = take_over_or_open(&gh, &t, "head#x", "Reading", "s").await;
        assert_eq!(again, Some(1));
        assert_eq!(gh.count(), 1);
        close(
            &gh,
            &t,
            1,
            &Close {
                conclusion: "success",
                title: "Approved, no findings".into(),
                summary: "s".into(),
                details_url: None,
            },
        )
        .await;
        assert_eq!(gh.run(1)["conclusion"], "success");
        // A review with nothing open on the commit opens its own.
        let fresh = take_over_or_open(&gh, &t, "label#x", "Reading", "s").await;
        assert_eq!(fresh, Some(2));
        assert_eq!(gh.run(2)["status"], "in_progress");
    }

    #[tokio::test]
    async fn a_handed_off_check_waits_for_the_next_job() {
        let gh = FakeChecks::default();
        let t = target(1);
        let id = take_over_or_open(&gh, &t, "head#x", "Reading", "s")
            .await
            .unwrap();
        assert!(
            hand_off(
                &gh,
                &t,
                id,
                "full#x",
                now(),
                "Reviewing the whole PR",
                "why"
            )
            .await
        );
        assert_eq!(gh.run(id)["status"], "queued");
        assert_eq!(start(&gh, &t, "full#x", "Reviewing", "s").await, Some(id));
        assert_eq!(gh.count(), 1);
    }

    #[tokio::test]
    async fn a_job_that_ends_before_starting_completes_its_waiting_check() {
        let gh = FakeChecks::default();
        let t = target(1);
        queue(&gh, &t, "head#x", now() + 180, "u").await;
        let c = Close {
            conclusion: "neutral",
            title: "Replaced by the review of abc1234".into(),
            summary: "s".into(),
            details_url: None,
        };
        assert!(!close_waiting(&gh, &t, "head#other", &c).await);
        assert!(close_waiting(&gh, &t, "head#x", &c).await);
        assert_eq!(gh.run(1)["status"], "completed");
        assert_eq!(gh.run(1)["conclusion"], "neutral");
    }

    #[tokio::test]
    async fn a_stale_check_never_sits_on_the_commit() {
        let gh = FakeChecks::default();
        let t = target(1);
        let long_ago = now() - STALE_AFTER_SECS - 600;
        queue(&gh, &t, "head#old", long_ago, "u").await;
        // A new scheduling replaces it and completes the stale one.
        let q = queue(&gh, &t, "head#new", now(), "u").await;
        assert_eq!(q, Queued::Created(2));
        assert_eq!(gh.run(1)["conclusion"], "neutral");
        assert_eq!(
            gh.run(1)["output"]["title"],
            "Review didn't finish: it stopped without a result"
        );
        // The sweep completes one no review came back for.
        let gh = FakeChecks::default();
        queue(&gh, &t, "head#old", long_ago, "u").await;
        assert_eq!(close_stale(&gh, &t, now(), "u").await, 1);
        assert_eq!(gh.run(1)["status"], "completed");
        // One still within its time is left alone.
        let gh = FakeChecks::default();
        queue(&gh, &t, "head#x", now() + 180, "u").await;
        assert_eq!(close_stale(&gh, &t, now(), "u").await, 0);
    }

    #[tokio::test]
    async fn a_refusal_is_silent_and_logged_once() {
        let gh = FakeChecks::refusing();
        let t = target(77_001);
        assert_eq!(queue(&gh, &t, "head#x", now(), "u").await, Queued::Nothing);
        assert_eq!(start(&gh, &t, "head#x", "R", "s").await, None);
        assert_eq!(take_over_or_open(&gh, &t, "head#x", "R", "s").await, None);
        assert!(!close_waiting(&gh, &t, "head#x", &stopped_close("u")).await);
        assert_eq!(close_stale(&gh, &t, now(), "u").await, 0);
        // Nothing but the refused listings was tried.
        assert!(gh.calls.lock().unwrap().iter().all(|c| c == "list"));
        assert!(!first_refusal(77_001));
        assert!(first_refusal(77_002));
        assert!(!first_refusal(77_002));
        // A server error is not a refusal.
        assert!(!is_refusal("GitHub answered 502 Bad Gateway: x"));
        assert!(is_refusal("GitHub answered 403 Forbidden: x"));
    }

    #[tokio::test]
    async fn a_reply_answered_only_leaves_the_standing_verdict_showing() {
        let gh = FakeChecks::default();
        let t = target(1);
        let v = take_over_or_open(&gh, &t, "head#x", "Reading", "s")
            .await
            .unwrap();
        close(
            &gh,
            &t,
            v,
            &Close {
                conclusion: "neutral",
                title: "Changes requested: 2 findings".into(),
                summary: "[Read the review](https://x/r) on the pull request.".into(),
                details_url: None,
            },
        )
        .await;
        // The reply shows at once, in plain words, over the finished check.
        let q = queue_work(&gh, &t, Work::Reply, "reply#9", now(), "https://app/pr").await;
        let id = match q {
            Queued::Created(id) => id,
            other => panic!("{other:?}"),
        };
        assert_eq!(gh.shown(HEAD)["status"], "queued");
        assert_eq!(
            gh.shown(HEAD)["output"]["title"],
            "Reading your reply on 9fceb02"
        );
        // Then in progress with the session link.
        let title = started_title(Work::Reply, HEAD);
        assert_eq!(
            start(&gh, &t, "reply#9", &title, "https://app/session?id=j").await,
            Some(id)
        );
        assert_eq!(gh.shown(HEAD)["status"], "in_progress");
        assert_eq!(gh.shown(HEAD)["details_url"], "https://app/session?id=j");
        // Answered, nothing changed: the commit shows the verdict again.
        close_restoring(
            &gh,
            &t,
            id,
            &Close {
                conclusion: "neutral",
                title: "Answered".into(),
                summary: "Answered on the pull request.".into(),
                details_url: Some("https://app/session?id=j".into()),
            },
        )
        .await;
        let shown = gh.shown(HEAD);
        assert_eq!(shown["conclusion"], "neutral");
        assert_eq!(shown["output"]["title"], "Changes requested: 2 findings");
        let summary = shown["output"]["summary"].as_str().unwrap();
        assert!(summary.contains("[Read the review](https://x/r)"));
        assert!(summary.contains("Answered on the pull request."));
        assert_eq!(shown["details_url"], "https://app/session?id=j");
        // An approval stays a pass.
        let gh = FakeChecks::default();
        let v = take_over_or_open(&gh, &t, "head#x", "Reading", "s")
            .await
            .unwrap();
        close(
            &gh,
            &t,
            v,
            &Close {
                conclusion: "success",
                title: "Approved, no findings".into(),
                summary: "s".into(),
                details_url: None,
            },
        )
        .await;
        let id = take_over_or_open(&gh, &t, "reply#1", "Reading", "s")
            .await
            .unwrap();
        close_restoring(&gh, &t, id, &stopped_close("u")).await;
        assert_eq!(gh.shown(HEAD)["conclusion"], "success");
        assert_eq!(gh.shown(HEAD)["output"]["title"], "Approved, no findings");
    }

    #[tokio::test]
    async fn a_stale_reply_check_goes_back_to_the_verdict_and_a_running_job_keeps_its_own() {
        let gh = FakeChecks::default();
        let t = target(1);
        let v = take_over_or_open(&gh, &t, "head#x", "Reading", "s")
            .await
            .unwrap();
        close(
            &gh,
            &t,
            v,
            &Close {
                conclusion: "success",
                title: "Approved, no findings".into(),
                summary: "s".into(),
                details_url: None,
            },
        )
        .await;
        queue_work(
            &gh,
            &t,
            Work::ReReview,
            "reply#2",
            now() - STALE_AFTER_SECS - 60,
            "u",
        )
        .await;
        assert_eq!(close_stale(&gh, &t, now(), "u").await, 1);
        assert_eq!(gh.shown(HEAD)["output"]["title"], "Approved, no findings");
        assert_eq!(gh.shown(HEAD)["conclusion"], "success");
        // A redelivered copy of a job that is running doesn't end its check.
        let gh = FakeChecks::default();
        queue_work(&gh, &t, Work::Reply, "reply#3", now(), "u").await;
        start(&gh, &t, "reply#3", "R", "s").await;
        assert!(!close_waiting(&gh, &t, "reply#3", &stopped_close("u")).await);
        assert_eq!(gh.run(1)["status"], "in_progress");
    }

    #[test]
    fn titles_say_what_is_happening_and_ignored_replies_show_nothing() {
        let t0 = 1_793_896_153;
        for (trigger, question, title) in [
            ("reply", true, "Reading your reply on 9fceb02"),
            ("thread_reply", true, "Reading your reply on 9fceb02"),
            ("reply", false, "Re-reviewing 9fceb02"),
            ("rerequest", false, "Re-reviewing 9fceb02"),
            (
                "description",
                false,
                "Re-reviewing 9fceb02 with the edited description",
            ),
            ("labeled", false, "Reviewing 9fceb02 shortly"),
        ] {
            assert_eq!(
                waiting_title(Work::of(trigger, question), HEAD, t0, t0),
                title
            );
        }
        assert!(shows_when_sent("reply", true));
        assert!(shows_when_sent("rerequest", false));
        assert!(shows_when_sent("thread_reply", true));
        assert!(!shows_when_sent("thread_reply", false));
        assert!(!shows_when_sent("quote_reply", false));
    }

    #[test]
    fn a_standing_verdict_keeps_its_conclusion() {
        assert_eq!(standing_conclusion("APPROVE"), "success");
        assert_eq!(standing_conclusion("REQUEST_CHANGES"), "neutral");
        assert_eq!(standing_conclusion("COMMENT"), "neutral");
        assert_eq!(verdict_words("REQUEST_CHANGES"), "changes requested");
        assert_eq!(verdict_words("APPROVE"), "approved");
    }
}
