//! Syncing one source: list, compare, fetch what changed, index only what
//! changed, remove what is gone, and say how it went.
//!
//! The same for every connector. One sync holds the source ([`super::LEASE_SECS`])
//! so a webhook, the schedule and "Sync now" never run two at once; a change
//! that arrives meanwhile sets `again` and the sync goes round once more
//! when it is done.
//!
//! A sync is done in steps. One worker run lists the selection, plans the
//! sync and keeps the plan ([`super::pending`]), then fetches and indexes
//! until its time budget ([`Ctx::budget_secs`]) is spent; what is left is
//! saved and the next run continues it (the worker queues that run at
//! once; the scheduled sweep is the backstop). The source shows how far it
//! is ("265 of 4,812") and how many items are searchable already, the whole
//! way through. No listing is cut at a fixed number: a source keeps up to
//! its limit ([`Source::limit`]) and says when it holds less than its
//! selection covers.
//!
//! Re-indexing is as small as the change:
//!
//! - an item whose version marker is unchanged is not fetched (and a
//!   connector with a cursor lists only what changed since it);
//! - a fetched item whose text hashes the same as before is not embedded
//!   (only its title, link or place are updated);
//! - a changed item re-embeds only the passages whose text changed; the
//!   others keep their vectors from the previous index.
//!
//! Items are fetched a few at a time ([`super::Kind::fetch_concurrency`]).
//! A refusal that is passing ([`ConnectorError::is_busy`]: a rate limit, an
//! overloaded server, a timeout, a dropped connection) never makes an item
//! "couldn't read": the item shows "waiting to retry" with when, and goes
//! back on the list. A rate limit pauses the whole sync for at least a
//! minute (or the wait the provider named) and it continues where it
//! stopped; an item an overloaded server didn't answer goes to the end of
//! the list, and the run waits [`RETRY_WAIT_SECS`] before asking again.
//! After [`MAX_TRIES`] in one sync, the item waits [`WAITING_SECS`] for a
//! later sync, which reads it whether or not the provider lists it as
//! changed ([`retry_due`]). Only permanent refusals (deleted, no access,
//! a bad item) are "couldn't read". A large first sync waits for an admin to
//! see what it will cost ([`super::LARGE_SYNC_ITEMS`]), and every embedding
//! counts against the team's monthly token limit ([`Ctx::token_room`]).
//!
//! Every embedding goes through the [`Embedder`] given (Titan, metered on
//! the team's usage in production). A source big enough keeps a search pack
//! ([`crate::knowledge::pack`]), updated at the end of each run.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;

use super::super::chunk::chunk;
use super::super::pack;
use super::super::search::{Embedder, Index, IndexedChunk};
use super::super::{self as kb, FileMeta, FileStatus};
use super::connector::{Connector, ConnectorError, Document, Listed};
use super::pending::{self, Pending};
use super::store::SyncStore;
use super::{
    chunk_key, content_hash, item_id, limits, next_sync, plan_sync, Kind, Progress, Source,
    SourceError, Status, LARGE_SYNC_ITEMS, LEASE_SECS,
};

/// Embedding calls in flight at once for one item.
const EMBED_CONCURRENCY: usize = 6;
/// Progress is saved at most this often while items are fetched (seconds).
const PROGRESS_SECS: i64 = 5;
/// The list left is saved after this many items, so a run that stops
/// without saving repeats little.
const SAVE_LIST_EVERY: u32 = 250;
/// Rounds one sync runs at most when changes keep arriving.
const MAX_ROUNDS: usize = 3;
/// Items fetched to estimate a large first sync.
const SAMPLE: usize = 8;
/// How long a sync pauses after a provider asks us to slow down.
pub const RATE_WAIT_SECS: i64 = 60;
/// Tries one sync gives an item refused for a passing reason before it
/// waits for a later sync.
pub const MAX_TRIES: u32 = 3;
/// How long a run waits before asking again for items an overloaded or
/// silent server didn't answer.
pub const RETRY_WAIT_SECS: i64 = 90;
/// How long an item refused through a whole sync waits for the next one.
pub const WAITING_SECS: i64 = 15 * 60;
/// Items in a row refused for a passing reason that pause the whole run:
/// the server is down, not one document.
const BUSY_IN_A_ROW: usize = 6;
/// How long a large first sync waits for an admin before it looks again.
const APPROVAL_RECHECK_SECS: i64 = 24 * 60 * 60;
/// Older items' indexes read at most in one run to add them to the pack.
const PACK_BACKFILL: usize = 2_000;
/// One worker run's time budget in production: the Lambda stops at 15
/// minutes and the hold lasts 14, which leaves room to save the list and
/// the pack after the last item.
pub const RUN_BUDGET_SECS: i64 = 11 * 60;

/// What a sync did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Outcome {
    /// Items indexed for the first time.
    pub added: usize,
    /// Items indexed again because their text changed.
    pub updated: usize,
    /// Items left out (no text, a file it doesn't read, too large).
    pub skipped: usize,
    /// The items it couldn't read or left out, with why (the first few).
    pub problems: Vec<super::runs::ItemProblem>,
    /// It ran (another sync didn't hold the source).
    pub ran: bool,
    pub listed: usize,
    pub fetched: usize,
    /// Fetched, but the text was the same: not embedded.
    pub same_text: usize,
    /// Items indexed (new or changed text).
    pub indexed: usize,
    pub embedded_chunks: usize,
    /// Passages whose vectors were kept from before.
    pub reused_chunks: usize,
    pub removed: usize,
    pub failed: usize,
    /// Items refused for a passing reason, waiting to be read again.
    pub waiting: usize,
    /// Every problem item counted by kind and reason (`waiting`,
    /// `rate_limited`: 165), beyond the first few kept in full.
    pub reasons: Vec<super::runs::ReasonCount>,
    /// Why the sync stopped, when it did.
    pub error: Option<ConnectorError>,
    /// The team's monthly token limit stopped it.
    pub over_budget: bool,
    /// More is left: the next run continues after [`Self::wait_secs`].
    pub more: bool,
    pub wait_secs: i64,
    /// A large first sync waits for an admin to start it.
    pub awaiting: bool,
}

/// What the sync needs.
pub struct Ctx<'a> {
    pub store: &'a dyn SyncStore,
    pub connector: &'a dyn Connector,
    pub embedder: &'a dyn Embedder,
    pub team: &'a str,
    /// Unix seconds now.
    pub now: &'a (dyn Fn() -> i64 + Send + Sync),
    /// The connection this sync opened, saved on the source when it ends.
    pub used: Option<super::UsedConnection>,
    /// What started it ([`super::trigger`]), for the history.
    pub trigger: &'a str,
    /// Seconds one run spends before it hands over to the next (0: no
    /// limit). [`RUN_BUDGET_SECS`] in production.
    pub budget_secs: i64,
    /// Embedding tokens the team may still use this month (`None`: no
    /// limit).
    pub token_room: Option<u64>,
    /// The most items the source keeps now and which limit that is
    /// (`source` or `base`); `None` keeps what the source has.
    pub limit: Option<(u32, &'a str)>,
}

fn rfc3339(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .unwrap_or_default()
        .to_rfc3339()
}

/// What a source shows when the team's monthly token limit stopped it.
pub fn budget_error() -> SourceError {
    SourceError {
        message: "Your team's monthly token limit is reached, so nothing more can be embedded this month.".into(),
        fix: "Raise the monthly token limit in Settings, or wait for next month. What is already brought in stays searchable, and the sync continues from where it stopped.".into(),
        action: "budget".into(),
        at: chrono::Utc::now().to_rfc3339(),
    }
}

/// Syncs the source `id` of base `kb_id`, unless another sync holds it (then
/// that sync is asked to go round again).
pub async fn run(ctx: &Ctx<'_>, kb_id: &str, id: &str) -> Outcome {
    let mut out = Outcome::default();
    let Ok(Some(src)) = ctx.store.source(ctx.team, kb_id, id).await else {
        return out;
    };
    let now = (ctx.now)();
    match ctx
        .store
        .take_lease(ctx.team, &src, now, now + LEASE_SECS)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            // Someone is syncing it: ask them to go round once more.
            let mut again = src;
            if !again.again {
                again.again = true;
                let _ = ctx.store.put_source(ctx.team, &again).await;
            }
            return out;
        }
        Err(e) => {
            tracing::warn!(error = %e, "Knowledge source hold not taken");
            return out;
        }
    }
    out.ran = true;
    let started = now;
    for _ in 0..MAX_ROUNDS {
        let Ok(Some(mut src)) = ctx.store.source(ctx.team, kb_id, id).await else {
            return out;
        };
        if let Some((limit, why)) = ctx.limit {
            src.item_limit = Some(limit);
            src.limit_reason = why.to_string();
        }
        src.again = false;
        src.status = Status::Syncing;
        if !src.continuing {
            src.progress = Some(Progress {
                phase: "listing".into(),
                done: 0,
                total: 0,
            });
        }
        src.lease_until = (ctx.now)() + LEASE_SECS;
        let _ = ctx.store.put_source(ctx.team, &src).await;
        let round = one_round(ctx, &mut src, started).await;
        out.listed = round.listed.max(out.listed);
        out.fetched += round.fetched;
        out.added += round.added;
        out.updated += round.updated;
        out.skipped += round.skipped;
        out.problems.extend(round.problems.iter().cloned());
        out.problems.truncate(super::runs::MAX_RUN_PROBLEMS);
        for r in &round.reasons {
            for _ in 0..r.count {
                super::runs::count_reason(&mut out.reasons, &r.kind, &r.reason);
            }
        }
        out.same_text += round.same_text;
        out.indexed += round.indexed;
        out.embedded_chunks += round.embedded_chunks;
        out.reused_chunks += round.reused_chunks;
        out.removed += round.removed;
        out.failed = round.failed;
        out.waiting = round.waiting;
        out.error = round.error.clone();
        out.over_budget = round.over_budget;
        out.more = round.more;
        out.wait_secs = round.wait_secs;
        out.awaiting = round.awaiting;
        let again = finish(ctx, kb_id, id, &src, &round).await;
        if !again || round.error.is_some() {
            break;
        }
    }
    // Every run of a sync is in the history, a sync done in steps too.
    if let Ok(Some(src)) = ctx.store.source(ctx.team, kb_id, id).await {
        let via = ctx.used.as_ref().map(|u| u.via.as_str()).unwrap_or("");
        let run = super::runs::from_outcome(&src, ctx.trigger, started, (ctx.now)(), &out, via);
        if let Err(e) = ctx.store.put_run(ctx.team, &run).await {
            tracing::warn!(error = %e, "Knowledge sync run not recorded");
        }
    }
    out
}

/// Saves the outcome onto the freshest copy of the source (someone may have
/// edited its name or selection meanwhile), releases the hold, and says
/// whether to go round again (a change arrived during a sync that is done).
async fn finish(ctx: &Ctx<'_>, kb_id: &str, id: &str, ran: &Source, r: &Outcome) -> bool {
    let now = (ctx.now)();
    let mut s = match ctx.store.source(ctx.team, kb_id, id).await {
        Ok(Some(s)) => s,
        // Removed while syncing: nothing to save.
        _ => return false,
    };
    let again = s.again;
    let items = ctx
        .store
        .items(ctx.team, kb_id, id)
        .await
        .unwrap_or_default();
    s.items = items.iter().filter(|f| f.ready_version > 0).count() as u32;
    s.failed_items = items
        .iter()
        .filter(|f| f.status == FileStatus::Failed && f.skipped.is_empty() && !f.is_waiting())
        .count() as u32;
    s.waiting_items = items.iter().filter(|f| f.is_waiting()).count() as u32;
    s.next_retry_at = items
        .iter()
        .filter(|f| f.is_waiting())
        .filter_map(|f| secs_of(&f.retry_at))
        .min()
        .unwrap_or(0);
    s.skipped_items = items.iter().filter(|f| !f.skipped.is_empty()).count() as u32;
    s.truncated = ran.truncated;
    s.cursor = ran.cursor.clone();
    s.item_limit = ran.item_limit;
    s.limit_reason = ran.limit_reason.clone();
    s.total = ran.total;
    s.listed = ran.listed;
    s.estimate = ran.estimate.clone();
    s.pack = ran.pack.clone();
    s.continuing = r.more;
    s.last_sync_at = rfc3339(now);
    s.lease_until = 0;
    if let Some(used) = &ctx.used {
        s.used = Some(used.clone());
    }
    if r.awaiting {
        s.status = Status::Approval;
        s.error = None;
        s.progress = None;
        s.next_sync_at = now + APPROVAL_RECHECK_SECS;
    } else if r.more {
        // Handed over: the next run continues (the worker queues it now;
        // the sweep finds it due as a backstop).
        s.status = Status::Syncing;
        s.error = None;
        s.progress = ran.progress.clone();
        s.next_sync_at = now + r.wait_secs;
    } else if r.over_budget {
        s.status = Status::Error;
        s.error = Some(budget_error());
        s.progress = None;
        s.next_sync_at = next_sync(&s, now, true);
    } else {
        s.progress = None;
        match &r.error {
            None => {
                s.status = Status::Ready;
                s.error = None;
                s.last_success_at = rfc3339(now);
                s.next_sync_at = next_sync(&s, now, false);
            }
            Some(e) => {
                s.status = Status::Error;
                s.error = Some(super::source_error(&s, e));
                s.next_sync_at = next_sync(&s, now, true);
            }
        }
        // Items waiting to be read again bring the next sync forward.
        if s.next_retry_at > 0 && !s.paused {
            s.next_sync_at = s.next_sync_at.min(s.next_retry_at.max(now + 1));
        }
    }
    // A change that arrived while a sync is still working through its
    // list is kept for when the list is done.
    let done = !r.more && !r.awaiting;
    if again && done {
        s.again = false;
    }
    let _ = ctx.store.put_source(ctx.team, &s).await;
    let _ = ctx.store.put_registry(ctx.team, &s).await;
    again && done && r.error.is_none() && !r.over_budget
}

/// Records that a source couldn't be synced at all (its connection
/// couldn't be opened), with the fix, and schedules a retry.
pub async fn fail(
    store: &dyn SyncStore,
    team: &str,
    kb_id: &str,
    id: &str,
    err: &ConnectorError,
    now: i64,
    trigger: &str,
) {
    let Ok(Some(s)) = store.source(team, kb_id, id).await else {
        return;
    };
    let error = super::source_error(&s, err);
    fail_with(store, team, kb_id, id, error, now, trigger).await;
}

/// [`fail`] with the error already worded.
pub async fn fail_with(
    store: &dyn SyncStore,
    team: &str,
    kb_id: &str,
    id: &str,
    error: super::SourceError,
    now: i64,
    trigger: &str,
) {
    let Ok(Some(mut s)) = store.source(team, kb_id, id).await else {
        return;
    };
    let run = super::runs::stopped(&s, trigger, now, error.clone());
    if let Err(e) = store.put_run(team, &run).await {
        tracing::warn!(error = %e, "Knowledge sync run not recorded");
    }
    s.status = Status::Error;
    s.error = Some(error);
    s.progress = None;
    s.continuing = false;
    s.last_sync_at = rfc3339(now);
    s.next_sync_at = next_sync(&s, now, true);
    let _ = store.put_source(team, &s).await;
    let _ = store.put_registry(team, &s).await;
}

/// What the page sees while a run works: progress, and how many items are
/// searchable already.
async fn save_progress(ctx: &Ctx<'_>, src: &mut Source, progress: Progress, live: u32) {
    let lease = (ctx.now)() + LEASE_SECS;
    src.progress = Some(progress.clone());
    src.lease_until = lease;
    src.items = live;
    // Onto the freshest copy, so an edit made meanwhile (a new name, a
    // pause) isn't overwritten by this sync's older copy.
    let mut fresh = match ctx.store.source(ctx.team, &src.kb_id, &src.id).await {
        Ok(Some(f)) => f,
        _ => src.clone(),
    };
    fresh.status = Status::Syncing;
    fresh.progress = Some(progress);
    fresh.lease_until = lease;
    fresh.items = live;
    fresh.total = src.total;
    fresh.listed = src.listed;
    fresh.truncated = src.truncated;
    fresh.item_limit = src.item_limit;
    fresh.limit_reason = src.limit_reason.clone();
    let _ = ctx.store.put_source(ctx.team, &fresh).await;
}

/// Embedding tokens left under the team's monthly limit, shared by the
/// items fetched at once.
struct Room(Option<AtomicU64>);

impl Room {
    /// Takes `n` tokens; false (and nothing taken) when they don't fit.
    fn take(&self, n: u64) -> bool {
        match &self.0 {
            None => true,
            Some(left) => left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |l| l.checked_sub(n))
                .is_ok(),
        }
    }
}

/// How one item went.
enum Step {
    Indexed {
        embedded: usize,
        reused: usize,
        sketch: pack::Item,
        new: bool,
    },
    SameText,
    Failed {
        fetched: bool,
        why: String,
    },
    /// Left out, saying why (`empty`, `unsupported`, `too_large`).
    Skipped {
        fetched: bool,
        why: String,
        code: &'static str,
    },
    Removed {
        was_ready: bool,
    },
    /// Nothing more can be read now (a refused sign-in): the item goes back
    /// on the list.
    Stop(ConnectorError),
    /// Refused for a passing reason (a rate limit, an overloaded or silent
    /// server): the item waits and is read again.
    Busy(ConnectorError),
    /// The team's token limit: the item goes back on the list.
    OverBudget,
}

async fn one_item(
    ctx: &Ctx<'_>,
    src: &Source,
    item: &Listed,
    prev: Option<&FileMeta>,
    room: &Room,
) -> Step {
    match ctx.connector.fetch(src, item).await {
        Ok(doc) if doc.is_empty() => {
            let why = "It has no text to read.";
            mark_failed(ctx, src, item, prev, why, "empty").await;
            Step::Skipped {
                fetched: true,
                why: why.into(),
                code: "empty",
            }
        }
        Ok(doc) => match index_item(ctx, src, item, prev, &doc, room).await {
            Ok(Indexed::SameText) => Step::SameText,
            Ok(Indexed::New {
                embedded,
                reused,
                sketch,
            }) => Step::Indexed {
                embedded,
                reused,
                sketch,
                new: !prev.is_some_and(|p| p.ready_version > 0),
            },
            Err(IndexError::OverBudget) => Step::OverBudget,
            Err(IndexError::Failed(why)) => {
                mark_failed(ctx, src, item, prev, &why, "").await;
                Step::Failed { fetched: true, why }
            }
        },
        Err(ConnectorError::Config(why)) => {
            // Settings that are wrong for this one item: its size or kind.
            // It is left out, and says why.
            let reason = if why.to_lowercase().contains("large") {
                "too_large"
            } else {
                "unsupported"
            };
            mark_failed(ctx, src, item, prev, &why, reason).await;
            Step::Skipped {
                fetched: false,
                why,
                code: reason,
            }
        }
        Err(e) if e.item_gone() => {
            // Deleted, or the connection may no longer see it: its
            // content goes too.
            let was_ready = prev.is_some_and(|p| p.ready_version > 0);
            match prev {
                Some(p) if ctx.store.delete_item(ctx.team, p).await.is_ok() => {
                    Step::Removed { was_ready }
                }
                Some(_) => Step::Failed {
                    fetched: false,
                    why: "The removed item's content couldn't be deleted.".into(),
                },
                None => Step::Removed { was_ready: false },
            }
        }
        Err(e @ ConnectorError::Reconnect(_)) => Step::Stop(e),
        Err(e) if e.is_busy() => Step::Busy(e),
        Err(e) => {
            let why = e.to_string();
            mark_failed(ctx, src, item, prev, &why, "").await;
            Step::Failed {
                fetched: false,
                why,
            }
        }
    }
}

/// Lists and plans a sync, or picks up the plan an earlier run left.
async fn plan(
    ctx: &Ctx<'_>,
    src: &mut Source,
    stored: &[FileMeta],
    live: u32,
) -> Result<Pending, ConnectorError> {
    let key = pending::key(ctx.team, &src.kb_id, &src.id);
    let plan_key = pending::plan_key(src);
    if let Ok(Some(bytes)) = ctx.store.read_bytes(ctx.team, &key).await {
        if let Some(p) = Pending::decode(&bytes).filter(|p| p.plan == plan_key) {
            return Ok(p);
        }
    }
    save_progress(
        ctx,
        src,
        Progress {
            phase: "listing".into(),
            done: 0,
            total: 0,
        },
        live,
    )
    .await;
    let listing = ctx.connector.list(src).await?;
    let mut plan = plan_sync(stored, &listing);
    // Items waiting to be read again (and items an older version gave up
    // on for a passing reason) are read now, even when the listing names
    // only what changed.
    let now = (ctx.now)();
    let planned: HashSet<String> = plan
        .fetch
        .iter()
        .map(|i| i.external_id.clone())
        .chain(plan.remove.iter().map(|f| f.external_id.clone()))
        .collect();
    for f in stored {
        if !planned.contains(&f.external_id) && retry_due(f, now) {
            plan.fetch.push(Listed {
                external_id: f.external_id.clone(),
                title: f.name.clone(),
                url: f.url.clone(),
                path: f.path.clone(),
                hint: f.hint.clone(),
                ..Default::default()
            });
        }
    }
    let n = listing.items.len() as u32;
    // A listing of only what changed says nothing of the whole: the totals
    // of the last complete one stay.
    let whole = listing.complete || listing.truncated;
    let total = match listing.total {
        Some(t) => Some(t),
        None if listing.complete && !listing.truncated => Some(n as u64),
        None if whole => None,
        None => src.total,
    };
    let listed = if whole { n } else { src.listed.max(n) };
    Ok(Pending {
        plan: plan_key,
        planned: plan.fetch.len() as u32,
        done: 0,
        fetch: plan.fetch,
        remove: plan.remove.into_iter().map(|f| f.external_id).collect(),
        cursor: listing.cursor,
        truncated: listing.truncated,
        total,
        listed,
        problem: listing.problem.as_ref().map(pending::keep_error),
        at: (ctx.now)(),
        tries: Default::default(),
    })
}

/// Unix seconds of an RFC 3339 time.
fn secs_of(t: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(t)
        .ok()
        .map(|d| d.timestamp())
}

/// Is a stored item due to be read again now: it waits and its time has
/// come, or an older version marked it "couldn't read" for what is
/// passing (a `429`, a server that stopped answering, an answer too large
/// for the server's limit, which is now read in pieces)?
pub fn retry_due(f: &FileMeta, now: i64) -> bool {
    if !f.is_synced() || f.status != FileStatus::Failed || !f.skipped.is_empty() {
        return false;
    }
    if !f.retry_at.is_empty() {
        return secs_of(&f.retry_at).is_none_or(|t| t <= now);
    }
    f.reason.is_empty()
        && matches!(
            kb::reason_of(&f.error),
            "rate_limited" | "unavailable" | "too_large"
        )
}

/// Embedding tokens of a few items spread over the list, for a large
/// first sync's estimate.
async fn sample_tokens(ctx: &Ctx<'_>, src: &Source, list: &[Listed]) -> Vec<u64> {
    let step = (list.len() / SAMPLE).max(1);
    let picks: Vec<&Listed> = list.iter().step_by(step).take(SAMPLE).collect();
    let docs: Vec<Result<Document, ConnectorError>> = futures::stream::iter(picks)
        .map(|item| ctx.connector.fetch(src, item))
        .buffered(4)
        .collect()
        .await;
    docs.into_iter()
        .filter_map(Result::ok)
        .map(|d| {
            chunk(&d.sections)
                .iter()
                .take(kb::MAX_CHUNKS_PER_FILE)
                .map(|c| limits::tokens_of(c.loc.len() + 1 + c.text.len()))
                .sum()
        })
        .collect()
}

async fn one_round(ctx: &Ctx<'_>, src: &mut Source, started: i64) -> Outcome {
    let mut out = Outcome::default();
    let deadline = if ctx.budget_secs > 0 {
        started + ctx.budget_secs
    } else {
        i64::MAX
    };
    let mut stored = match ctx.store.items(ctx.team, &src.kb_id, &src.id).await {
        Ok(s) => s,
        Err(e) => {
            out.error = Some(ConnectorError::Temporary(format!(
                "Stored items unreadable: {e}"
            )));
            return out;
        }
    };
    rename_stored(ctx, src, &mut stored).await;
    let mut live = stored.iter().filter(|f| f.ready_version > 0).count() as u32;
    let first = live == 0;
    let mut pending = match plan(ctx, src, &stored, live).await {
        Ok(p) => p,
        Err(e) => {
            out.error = Some(e);
            return out;
        }
    };
    let by_ext: HashMap<String, FileMeta> = stored
        .iter()
        .map(|f| (f.external_id.clone(), f.clone()))
        .collect();
    // Items a run that stopped without saving already brought in.
    pending.fetch.retain(|item| {
        !by_ext.get(&item.external_id).is_some_and(|f| {
            !item.version.is_empty()
                && f.remote_version == item.version
                && f.ready_version > 0
                && f.status == FileStatus::Ready
        })
    });
    pending.done = pending.planned.saturating_sub(pending.fetch.len() as u32);
    out.listed = pending.listed as usize;
    src.truncated = pending.truncated;
    src.total = pending.total;
    src.listed = pending.listed;
    let list_key = pending::key(ctx.team, &src.kb_id, &src.id);

    // A large first sync shows what it will cost before anything is
    // embedded, and waits for an admin.
    if first && src.approved_at == 0 && pending.fetch.len() > LARGE_SYNC_ITEMS {
        let sample = sample_tokens(ctx, src, &pending.fetch).await;
        src.estimate = Some(limits::estimate(
            pending.fetch.len() as u64,
            &sample,
            ctx.token_room,
            &rfc3339((ctx.now)()),
        ));
        let _ = ctx
            .store
            .write_bytes(ctx.team, &list_key, pending.encode())
            .await;
        out.awaiting = true;
        return out;
    }

    let room = Room(ctx.token_room.map(AtomicU64::new));
    let conc = src.kind.map(Kind::fetch_concurrency).unwrap_or(4);
    let mut queue: std::collections::VecDeque<Listed> = pending.fetch.drain(..).collect();
    let mut back: Vec<Listed> = Vec::new();
    let mut stop: Option<ConnectorError> = None;
    // Items refused for a passing reason (with why), and the longest wait
    // a provider named.
    let mut parked: Vec<(Listed, ConnectorError)> = Vec::new();
    let mut retry: Vec<Listed> = Vec::new();
    let (mut busy_run, mut named_wait, mut timed_out) = (0usize, 0i64, false);
    let mut sketches: HashMap<String, pack::Item> = HashMap::new();
    let (mut last_progress, mut saved_at) = (0i64, pending.done);
    let progress = |done: u32, planned: u32| Progress {
        phase: "fetching".into(),
        done,
        total: planned,
    };
    save_progress(ctx, src, progress(pending.done, pending.planned), live).await;
    while !queue.is_empty() && stop.is_none() && !out.over_budget {
        if (ctx.now)() >= deadline {
            timed_out = true;
            break;
        }
        let batch: Vec<Listed> = (0..conc * 2).map_while(|_| queue.pop_front()).collect();
        let shared: &Source = src;
        let steps: Vec<(Listed, Step)> = futures::stream::iter(batch)
            .map(|item| {
                let prev = by_ext.get(&item.external_id);
                let room = &room;
                async move {
                    let step = one_item(ctx, shared, &item, prev, room).await;
                    (item, step)
                }
            })
            .buffered(conc)
            .collect()
            .await;
        for (item, step) in steps {
            if !matches!(step, Step::Busy(_)) {
                busy_run = 0;
            }
            match step {
                Step::Stop(e) => {
                    back.push(item);
                    stop.get_or_insert(e);
                    continue;
                }
                Step::Busy(e) => {
                    busy_run += 1;
                    if let ConnectorError::Busy(b) = &e {
                        named_wait = named_wait.max(b.retry_after.unwrap_or(0) as i64);
                    }
                    let rate = e.busy_reason() == Some("rate_limited");
                    if rate || busy_run >= BUSY_IN_A_ROW {
                        // The provider's limit, or a server that is down:
                        // the whole run waits.
                        stop.get_or_insert(e.clone());
                    }
                    retry.push(item.clone());
                    parked.push((item, e));
                    continue;
                }
                Step::OverBudget => {
                    back.push(item);
                    out.over_budget = true;
                    continue;
                }
                Step::Indexed {
                    embedded,
                    reused,
                    sketch,
                    new,
                } => {
                    out.fetched += 1;
                    out.indexed += 1;
                    out.embedded_chunks += embedded;
                    out.reused_chunks += reused;
                    if new {
                        live += 1;
                        out.added += 1;
                    } else {
                        out.updated += 1;
                    }
                    sketches.insert(sketch.file_id.clone(), sketch);
                }
                Step::SameText => {
                    out.fetched += 1;
                    out.same_text += 1;
                }
                Step::Failed { fetched, why } => {
                    out.failed += 1;
                    out.fetched += usize::from(fetched);
                    let reason = match kb::reason_of(&why) {
                        "rate_limited" | "unavailable" => "failed",
                        r => r,
                    };
                    problem(&mut out, &item, "failed", &why, reason);
                }
                Step::Skipped { fetched, why, code } => {
                    out.skipped += 1;
                    out.fetched += usize::from(fetched);
                    problem(&mut out, &item, "skipped", &why, code);
                }
                Step::Removed { was_ready } => {
                    out.removed += 1;
                    live -= u32::from(was_ready && live > 0);
                }
            }
            pending.done += 1;
        }
        let now = (ctx.now)();
        if now - last_progress >= PROGRESS_SECS {
            last_progress = now;
            save_progress(ctx, src, progress(pending.done, pending.planned), live).await;
        }
        if pending.done - saved_at >= SAVE_LIST_EVERY {
            saved_at = pending.done;
            let mut left = pending.clone();
            left.fetch = back
                .iter()
                .chain(retry.iter())
                .chain(queue.iter())
                .cloned()
                .collect();
            let _ = ctx
                .store
                .write_bytes(ctx.team, &list_key, left.encode())
                .await;
        }
    }
    // Refused items go back on the list (a rate-limited one first, where
    // it was; one an overloaded server didn't answer at the end) until
    // they've had their tries this sync; then they wait for a later one.
    let mut given_up: HashSet<String> = HashSet::new();
    let mut front: Vec<Listed> = Vec::new();
    let mut end: Vec<Listed> = Vec::new();
    let stopped_busy = stop.as_ref().is_some_and(ConnectorError::is_busy);
    for item in retry {
        let n = pending.tries.entry(item.external_id.clone()).or_default();
        *n += 1;
        if *n >= MAX_TRIES {
            given_up.insert(item.external_id.clone());
            pending.done += 1;
        } else if stopped_busy {
            front.push(item);
        } else {
            end.push(item);
        }
    }
    let mut left: Vec<Listed> = front;
    left.extend(back);
    left.extend(queue);
    left.extend(end);
    pending.fetch = left;
    // When the list goes on: after the provider's wait, a minute for a rate
    // limit, a little longer for a server that didn't answer.
    let run_wait = if stopped_busy {
        RATE_WAIT_SECS.max(named_wait)
    } else if !timed_out && stop.is_none() && !out.over_budget {
        RETRY_WAIT_SECS.max(named_wait)
    } else {
        0
    };
    let now = (ctx.now)();
    for (item, e) in &parked {
        let later = given_up.contains(&item.external_id);
        let at = if later {
            now + WAITING_SECS.max(named_wait)
        } else {
            now + run_wait.max(1)
        };
        mark_waiting(ctx, src, item, by_ext.get(&item.external_id), e, at).await;
        out.waiting += 1;
        problem(
            &mut out,
            item,
            "waiting",
            &e.to_string(),
            e.busy_reason().unwrap_or("unavailable"),
        );
    }

    if !pending.fetch.is_empty() {
        // Not done: keep the list for the next run.
        let _ = ctx
            .store
            .write_bytes(ctx.team, &list_key, pending.encode())
            .await;
        update_pack(ctx, src, &sketches, deadline).await;
        let mut p = progress(pending.done, pending.planned);
        match stop {
            Some(e) if e.is_busy() => {
                p.phase = "waiting".into();
                out.more = true;
                out.wait_secs = run_wait;
            }
            Some(e) => out.error = Some(e),
            None if out.over_budget => {}
            None => {
                out.more = true;
                if run_wait > 0 {
                    // Only items that weren't answered are left: wait first.
                    p.phase = "waiting".into();
                    out.wait_secs = run_wait;
                }
            }
        }
        src.progress = Some(p);
        src.items = live;
        return out;
    }

    // Everything is fetched: what is gone goes, and the listing's cursor
    // is committed.
    if !pending.remove.is_empty() {
        save_progress(
            ctx,
            src,
            Progress {
                phase: "removing".into(),
                done: 0,
                total: pending.remove.len() as u32,
            },
            live,
        )
        .await;
    }
    for ext in &pending.remove {
        if let Some(f) = by_ext.get(ext) {
            if ctx.store.delete_item(ctx.team, f).await.is_ok() {
                out.removed += 1;
            }
        }
    }
    src.cursor = std::mem::take(&mut pending.cursor);
    src.estimate = None;
    ctx.store.delete_object(ctx.team, &list_key).await;
    update_pack(ctx, src, &sketches, deadline).await;
    if out.error.is_none() {
        out.error = pending.problem.as_ref().map(pending::read_error);
    }
    out
}

/// Brings the source's search pack up to date: this run's new sketches go
/// in, items that changed or went come out, and items searchable now but
/// missing from it (an older source, a pack shard lost) are read from their
/// indexes, a bounded number a run. Only shards that changed are written.
async fn update_pack(
    ctx: &Ctx<'_>,
    src: &mut Source,
    fresh: &HashMap<String, pack::Item>,
    deadline: i64,
) {
    let Ok(items) = ctx.store.items(ctx.team, &src.kb_id, &src.id).await else {
        return;
    };
    let ready: Vec<&FileMeta> = items.iter().filter(|f| f.ready_version > 0).collect();
    if src.pack.is_empty() && ready.len() < pack::PACK_FROM {
        return;
    }
    let current: HashMap<&str, u32> = ready
        .iter()
        .map(|f| (f.id.as_str(), f.ready_version))
        .collect();
    if src.pack.len() != pack::SHARDS {
        for k in pack::keys(ctx.team, &src.kb_id, &src.id, &src.pack) {
            ctx.store.delete_object(ctx.team, &k).await;
        }
        src.pack = vec![0; pack::SHARDS];
    }
    let model = ctx.embedder.model().to_string();
    let mut backfill = PACK_BACKFILL;
    for no in 0..pack::SHARDS {
        let v = src.pack[no];
        let old_key = pack::shard_key(ctx.team, &src.kb_id, &src.id, no, v);
        let old = if v > 0 {
            match ctx.store.read_bytes(ctx.team, &old_key).await {
                Ok(Some(b)) => pack::decode(&b).ok(),
                _ => None,
            }
        } else {
            None
        };
        let mut shard = match old {
            Some(s) if s.model == model => s,
            _ => pack::Shard {
                model: model.clone(),
                ..Default::default()
            },
        };
        let mut changed = v == 0 || shard.model != model;
        changed |= pack::apply(&mut shard, fresh, &current, no);
        // Items searchable now but not in the pack.
        let have: HashSet<String> = shard.items.iter().map(|i| i.file_id.clone()).collect();
        let missing: Vec<&FileMeta> = ready
            .iter()
            .copied()
            .filter(|f| pack::shard_of(&f.id) == no && !have.contains(&f.id))
            .take(backfill)
            .collect();
        if !missing.is_empty() && (ctx.now)() < deadline.saturating_add(LEASE_SECS / 4) {
            let read: Vec<Option<pack::Item>> = futures::stream::iter(missing)
                .map(|f| async move {
                    let key = f.searchable_index(ctx.team)?;
                    let idx = ctx.store.read_index(ctx.team, &key).await.ok()??;
                    (idx.model == ctx.embedder.model())
                        .then(|| pack::sketch(&f.id, f.ready_version, &idx))
                })
                .buffered(16)
                .collect()
                .await;
            backfill = backfill.saturating_sub(read.len());
            let found: HashMap<String, pack::Item> = read
                .into_iter()
                .flatten()
                .map(|i| (i.file_id.clone(), i))
                .collect();
            changed |= pack::apply(&mut shard, &found, &current, no);
        }
        if !changed {
            continue;
        }
        let key = pack::shard_key(ctx.team, &src.kb_id, &src.id, no, v + 1);
        if ctx
            .store
            .write_bytes(ctx.team, &key, pack::encode(&shard))
            .await
            .is_ok()
        {
            if v > 0 {
                ctx.store.delete_object(ctx.team, &old_key).await;
            }
            src.pack[no] = v + 1;
        }
    }
}

/// Stored items saved under an id the connector names differently now are
/// renamed in place (an id that is already stored under its new name is
/// a duplicate, and goes).
async fn rename_stored(ctx: &Ctx<'_>, src: &Source, stored: &mut Vec<FileMeta>) {
    let mut taken: std::collections::HashSet<String> =
        stored.iter().map(|f| f.external_id.clone()).collect();
    let mut keep = Vec::with_capacity(stored.len());
    for mut f in std::mem::take(stored) {
        let Some(now) = ctx
            .connector
            .current_id(src, &f.external_id)
            .filter(|n| *n != f.external_id)
        else {
            keep.push(f);
            continue;
        };
        if taken.contains(&now) {
            if ctx.store.delete_item(ctx.team, &f).await.is_err() {
                keep.push(f);
            }
            continue;
        }
        taken.insert(now.clone());
        f.external_id = now;
        if let Err(e) = ctx.store.put_item(ctx.team, &f).await {
            tracing::warn!(error = %e, "A stored knowledge item wasn't renamed; the next sync tries again");
        }
        keep.push(f);
    }
    *stored = keep;
}

enum Indexed {
    SameText,
    New {
        embedded: usize,
        reused: usize,
        sketch: pack::Item,
    },
}

enum IndexError {
    Failed(String),
    OverBudget,
}

impl From<String> for IndexError {
    fn from(s: String) -> Self {
        IndexError::Failed(s)
    }
}

fn as_of(updated: &str, now: i64) -> String {
    let d = updated.get(..10).unwrap_or("");
    if chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").is_ok() {
        d.to_string()
    } else {
        rfc3339(now).get(..10).unwrap_or("").to_string()
    }
}

/// The item record as it stands before indexing (new, or the stored one).
fn base_item(src: &Source, item: &Listed, prev: Option<&FileMeta>, now: i64) -> FileMeta {
    prev.cloned().unwrap_or_else(|| FileMeta {
        id: item_id(&src.id, &item.external_id),
        kb_id: src.kb_id.clone(),
        kind: src.kind_key().to_string(),
        version: 0,
        uploaded_at: rfc3339(now),
        uploaded_by: format!(
            "{} sync",
            src.kind.map(super::Kind::label).unwrap_or("Source")
        ),
        source_id: src.id.clone(),
        external_id: item.external_id.clone(),
        ..Default::default()
    })
}

async fn index_item(
    ctx: &Ctx<'_>,
    src: &Source,
    item: &Listed,
    prev: Option<&FileMeta>,
    doc: &Document,
    room: &Room,
) -> Result<Indexed, IndexError> {
    let now = (ctx.now)();
    let hash = content_hash(doc);
    let mut f = base_item(src, item, prev, now);
    f.name = crate::truncate_str(
        if doc.title.trim().is_empty() {
            &item.title
        } else {
            &doc.title
        },
        300,
    )
    .to_string();
    f.url = if doc.url.is_empty() {
        item.url.clone()
    } else {
        doc.url.clone()
    };
    f.path = item.path.clone();
    f.remote_version = item.version.clone();
    f.as_of = as_of(
        if doc.updated_at.is_empty() {
            &item.updated_at
        } else {
            &doc.updated_at
        },
        now,
    );
    // The same text as what is searchable now: nothing to embed.
    f.skipped.clear();
    f.retry_at.clear();
    f.reason.clear();
    f.hint = serde_json::Value::Null;
    if prev.is_some_and(|p| p.content_hash == hash && p.ready_version > 0) {
        f.status = FileStatus::Ready;
        f.error.clear();
        ctx.store.put_item(ctx.team, &f).await?;
        return Ok(Indexed::SameText);
    }
    let mut chunks = chunk(&doc.sections);
    f.truncated = chunks.len() > kb::MAX_CHUNKS_PER_FILE;
    chunks.truncate(kb::MAX_CHUNKS_PER_FILE);
    // Vectors of passages whose text didn't change are kept.
    let mut reuse: HashMap<String, Vec<f32>> = HashMap::new();
    if let Some(key) = prev.and_then(|p| p.searchable_index(ctx.team)) {
        if let Ok(Some(old)) = ctx.store.read_index(ctx.team, &key).await {
            if old.model == ctx.embedder.model() {
                for c in &old.chunks {
                    reuse.insert(chunk_key(&c.loc, &c.text), c.vector.clone());
                }
            }
        }
    }
    let mut reused = 0usize;
    let mut todo: Vec<(usize, String)> = Vec::new();
    let mut vectors: Vec<Option<Vec<f32>>> = vec![None; chunks.len()];
    for (i, c) in chunks.iter().enumerate() {
        match reuse.get(&chunk_key(&c.loc, &c.text)) {
            Some(v) => {
                vectors[i] = Some(v.clone());
                reused += 1;
            }
            None => todo.push((
                i,
                if c.loc.is_empty() {
                    c.text.clone()
                } else {
                    format!("{}\n{}", c.loc, c.text)
                },
            )),
        }
    }
    // What this item embeds counts against the team's monthly limit.
    let tokens: u64 = todo
        .iter()
        .map(|(_, t)| limits::tokens_of(crate::truncate_str(t, 8_000).len()))
        .sum();
    if !room.take(tokens) {
        return Err(IndexError::OverBudget);
    }
    let embedded = todo.len();
    let results: Vec<(usize, Result<Vec<f32>, String>)> = futures::stream::iter(todo)
        .map(|(i, input)| async move {
            (
                i,
                ctx.embedder.embed(crate::truncate_str(&input, 8_000)).await,
            )
        })
        .buffered(EMBED_CONCURRENCY)
        .collect()
        .await;
    for (i, r) in results {
        vectors[i] = Some(r.map_err(|e| {
            tracing::warn!(error = %e, "Knowledge embedding failed");
            "The text couldn't be indexed right now. It's tried again on the next sync.".to_string()
        })?);
    }
    let indexed: Vec<IndexedChunk> = chunks
        .into_iter()
        .zip(vectors)
        .map(|(c, v)| IndexedChunk {
            ord: c.ord,
            loc: c.loc,
            text: c.text,
            vector: v.unwrap_or_default(),
        })
        .collect();
    let dims = indexed.first().map_or(0, |c| c.vector.len());
    let index = Index::build(ctx.embedder.model(), dims, indexed)
        .map_err(|_| "The text couldn't be indexed right now.".to_string())?;
    let old_ready = f.ready_version;
    f.version += 1;
    let key = kb::index_key(ctx.team, &f.kb_id, &f.id, f.version);
    ctx.store.write_index(ctx.team, &key, &index).await?;
    f.ready_version = f.version;
    f.status = FileStatus::Ready;
    f.error.clear();
    f.chunks = index.chunks.len() as u32;
    f.size = doc.sections.iter().map(|s| s.text.len() as u64).sum();
    f.content_hash = hash;
    f.indexed_at = rfc3339(now);
    ctx.store.put_item(ctx.team, &f).await?;
    if old_ready > 0 && old_ready != f.ready_version {
        ctx.store
            .delete_object(
                ctx.team,
                &kb::index_key(ctx.team, &f.kb_id, &f.id, old_ready),
            )
            .await;
    }
    Ok(Indexed::New {
        embedded,
        reused,
        sketch: pack::sketch(&f.id, f.ready_version, &index),
    })
}

/// Notes an item the run couldn't read or left out, for the history.
fn problem(out: &mut Outcome, item: &Listed, kind: &str, why: &str, reason: &str) {
    super::runs::count_reason(&mut out.reasons, kind, reason);
    if out.problems.len() < super::runs::MAX_RUN_PROBLEMS {
        out.problems.push(super::runs::ItemProblem {
            name: item.title.clone(),
            url: item.url.clone(),
            kind: kind.into(),
            error: crate::truncate_str(why, 300).to_string(),
            fix: super::runs::item_fix(why, kind),
            reason: reason.to_string(),
        });
    }
}

/// Records why an item couldn't be read. A version that was searchable
/// stays searchable.
async fn mark_failed(
    ctx: &Ctx<'_>,
    src: &Source,
    item: &Listed,
    prev: Option<&FileMeta>,
    why: &str,
    skipped: &str,
) {
    let mut f = base_item(src, item, prev, (ctx.now)());
    f.skipped = skipped.to_string();
    if f.name.is_empty() {
        f.name = item.title.clone();
    }
    f.url = item.url.clone();
    f.path = item.path.clone();
    f.status = FileStatus::Failed;
    f.error = crate::truncate_str(why, 300).to_string();
    f.retry_at.clear();
    f.hint = serde_json::Value::Null;
    f.reason = if skipped.is_empty() {
        kb::reason_of(why).to_string()
    } else {
        skipped.to_string()
    };
    if f.reason == "rate_limited" || f.reason == "unavailable" {
        // Permanent here: the words only sound passing.
        f.reason = "failed".into();
    }
    // Fetch it again next time even though its marker is unchanged.
    f.remote_version.clear();
    let _ = ctx.store.put_item(ctx.team, &f).await;
}

/// Records that an item was refused for a passing reason and is read again
/// at `at` (unix seconds). A version that was searchable stays searchable;
/// it never counts as "couldn't read".
async fn mark_waiting(
    ctx: &Ctx<'_>,
    src: &Source,
    item: &Listed,
    prev: Option<&FileMeta>,
    e: &ConnectorError,
    at: i64,
) {
    let mut f = base_item(src, item, prev, (ctx.now)());
    if f.name.is_empty() {
        f.name = item.title.clone();
    }
    f.url = item.url.clone();
    f.path = item.path.clone();
    f.skipped.clear();
    f.status = FileStatus::Failed;
    f.error = crate::truncate_str(&e.to_string(), 300).to_string();
    f.reason = e.busy_reason().unwrap_or("unavailable").to_string();
    f.retry_at = rfc3339(at);
    f.hint = item.hint.clone();
    f.remote_version.clear();
    let _ = ctx.store.put_item(ctx.team, &f).await;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::super::connector::{Listing, Node};
    use super::super::store::memory::MemSyncStore;
    use super::super::{shown_status, Kind, Selected};
    use super::*;
    use crate::knowledge::chunk::Section;
    use crate::knowledge::search::testing::WordEmbedder;

    /// A provider whose pages tests change between syncs.
    #[derive(Default)]
    struct FakeProvider {
        /// id -> (version, title, text)
        pages: Mutex<Vec<(String, String, String, String)>>,
        gone: Mutex<Vec<String>>,
        too_big: Mutex<Vec<String>>,
        fail_list: Mutex<Option<ConnectorError>>,
        fetches: AtomicUsize,
        /// Each fetch takes this many seconds on the rig's clock.
        clock: Mutex<Option<(Arc<AtomicI64>, i64)>>,
        /// These ids answer "slow down" once each.
        limited: Mutex<Vec<String>>,
    }

    impl FakeProvider {
        fn set(&self, id: &str, version: &str, text: &str) {
            let mut p = self.pages.lock().unwrap();
            p.retain(|x| x.0 != id);
            p.push((id.into(), version.into(), format!("Page {id}"), text.into()));
            p.sort();
        }
        fn drop_page(&self, id: &str) {
            self.pages.lock().unwrap().retain(|x| x.0 != id);
        }
    }

    #[async_trait::async_trait]
    impl Connector for FakeProvider {
        fn kind(&self) -> Kind {
            Kind::Notion
        }
        async fn browse(&self, _: Option<&str>, _: &str) -> Result<Vec<Node>, ConnectorError> {
            Ok(Vec::new())
        }
        async fn list(&self, _: &Source) -> Result<Listing, ConnectorError> {
            if let Some(e) = self.fail_list.lock().unwrap().clone() {
                return Err(e);
            }
            Ok(Listing {
                items: self
                    .pages
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(id, v, t, _)| Listed {
                        external_id: id.clone(),
                        title: t.clone(),
                        url: format!("https://www.notion.so/{id}"),
                        version: v.clone(),
                        updated_at: "2026-10-20T10:00:00Z".into(),
                        ..Default::default()
                    })
                    .collect(),
                complete: true,
                ..Default::default()
            })
        }
        async fn fetch(&self, _: &Source, item: &Listed) -> Result<Document, ConnectorError> {
            if let Some((c, secs)) = self.clock.lock().unwrap().as_ref() {
                c.fetch_add(*secs, Ordering::SeqCst);
            }
            {
                let mut l = self.limited.lock().unwrap();
                if let Some(i) = l.iter().position(|x| *x == item.external_id) {
                    l.remove(i);
                    return Err(ConnectorError::RateLimited("slow down".into()));
                }
            }
            self.fetches.fetch_add(1, Ordering::SeqCst);
            if self.gone.lock().unwrap().contains(&item.external_id) {
                return Err(ConnectorError::Forbidden("no access".into()));
            }
            if self.too_big.lock().unwrap().contains(&item.external_id) {
                return Err(ConnectorError::Config(
                    "The file is larger than 25 MB.".into(),
                ));
            }
            let p = self.pages.lock().unwrap();
            let (_, _, title, text) = p.iter().find(|x| x.0 == item.external_id).cloned().unwrap();
            Ok(Document {
                title,
                url: item.url.clone(),
                updated_at: item.updated_at.clone(),
                sections: text
                    .split("\n\n")
                    .enumerate()
                    .map(|(i, t)| Section {
                        loc: format!("Part {i}"),
                        text: t.into(),
                        header: None,
                    })
                    .collect(),
            })
        }
    }

    /// Counts embeddings.
    struct Counting(AtomicUsize);
    #[async_trait::async_trait]
    impl Embedder for Counting {
        fn model(&self) -> &str {
            WordEmbedder.model()
        }
        async fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            WordEmbedder.embed(text).await
        }
    }

    struct Rig {
        store: Arc<MemSyncStore>,
        provider: Arc<FakeProvider>,
        embedder: Arc<Counting>,
        clock: Arc<AtomicI64>,
        budget: Mutex<i64>,
        room: Mutex<Option<u64>>,
    }

    fn rig() -> Rig {
        let store = Arc::new(MemSyncStore::default());
        store.sources.lock().unwrap().insert(
            "s1".into(),
            Source {
                id: "s1".into(),
                kb_id: "kb1".into(),
                kind: Some(Kind::Notion),
                name: "Wiki".into(),
                selection: vec![Selected {
                    id: "page:root".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        Rig {
            store,
            provider: Arc::new(FakeProvider::default()),
            embedder: Arc::new(Counting(AtomicUsize::new(0))),
            clock: Arc::new(AtomicI64::new(1_800_000_000)),
            budget: Mutex::new(0),
            room: Mutex::new(None),
        }
    }

    fn source(r: &Rig) -> Source {
        r.store.sources.lock().unwrap()["s1"].clone()
    }

    async fn sync(r: &Rig) -> Outcome {
        let clock = r.clock.clone();
        let now = move || clock.load(Ordering::SeqCst);
        let ctx = Ctx {
            store: r.store.as_ref(),
            connector: r.provider.as_ref(),
            embedder: r.embedder.as_ref(),
            team: "TEAM#t",
            now: &now,
            used: None,
            trigger: "manual",
            budget_secs: *r.budget.lock().unwrap(),
            token_room: *r.room.lock().unwrap(),
            limit: None,
        };
        run(&ctx, "kb1", "s1").await
    }

    /// Runs until the sync has nothing more to do (as the worker queues the
    /// next run), and says how many runs it took.
    async fn sync_all(r: &Rig) -> (usize, Outcome) {
        let mut runs = 0;
        loop {
            runs += 1;
            let o = sync(r).await;
            if !o.more || runs > 500 {
                return (runs, o);
            }
            r.clock.fetch_add(o.wait_secs, Ordering::SeqCst);
        }
    }

    fn items(r: &Rig) -> Vec<FileMeta> {
        let mut v: Vec<FileMeta> = r.store.items.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.external_id.cmp(&b.external_id));
        v
    }

    fn embeds(r: &Rig) -> usize {
        r.embedder.0.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn a_first_sync_indexes_every_item_with_its_original_link() {
        let r = rig();
        r.provider.set(
            "a",
            "1",
            "Deploys run on Fridays.\n\nRollbacks need a ticket.",
        );
        r.provider.set("b", "1", "Onboarding checklist.");
        let o = sync(&r).await;
        assert!(o.ran);
        assert_eq!((o.indexed, o.removed, o.failed), (2, 0, 0));
        let it = items(&r);
        assert_eq!(it.len(), 2);
        assert_eq!(it[0].url, "https://www.notion.so/a");
        assert_eq!(it[0].kind, "notion");
        assert_eq!(it[0].as_of, "2026-10-20");
        assert!(it
            .iter()
            .all(|f| f.status == FileStatus::Ready && f.ready_version == 1));
        let s = r.store.sources.lock().unwrap()["s1"].clone();
        assert_eq!(s.status, Status::Ready);
        assert_eq!(s.items, 2);
        assert_eq!(s.lease_until, 0, "the hold is released");
        assert!(s.next_sync_at > 1_800_000_000);
        assert_eq!(r.store.registry.lock().unwrap()["s1"].0, s.next_sync_at);
        // Progress was shown while it ran.
        assert!(r
            .store
            .source_writes
            .lock()
            .unwrap()
            .iter()
            .any(|w| w.status == Status::Syncing
                && w.progress.as_ref().is_some_and(|p| p.phase == "fetching")));
    }

    #[tokio::test]
    async fn every_sync_is_recorded_with_what_it_did_and_what_it_left_out() {
        let r = rig();
        r.provider.set("a", "1", "Deploys run on Fridays.");
        r.provider.set("b", "1", "Onboarding checklist.");
        r.provider.set("c", "1", "  ");
        r.provider.set("d", "1", "A huge export.");
        r.provider.too_big.lock().unwrap().push("d".into());
        let o = sync(&r).await;
        assert_eq!((o.added, o.updated, o.skipped, o.failed), (2, 0, 2, 0));
        let run = r.store.runs.lock().unwrap().last().cloned().unwrap();
        assert_eq!(run.trigger, "manual");
        assert_eq!(
            run.result, "done",
            "left-out items aren't a problem with the sync"
        );
        assert_eq!(
            (run.listed, run.added, run.skipped, run.unchanged),
            (4, 2, 2, 0)
        );
        assert!(run.embedded > 0);
        let kinds: Vec<&str> = run.problems.iter().map(|p| p.kind.as_str()).collect();
        assert_eq!(kinds, ["skipped", "skipped"]);
        assert!(run.problems.iter().any(|p| p.fix.contains("Split it")));
        // Left-out items say why, and aren't counted as failed.
        let it = items(&r);
        let c = it.iter().find(|f| f.external_id == "c").unwrap();
        assert_eq!(
            (c.skipped.as_str(), c.status),
            ("empty", FileStatus::Failed)
        );
        assert_eq!(
            it.iter().find(|f| f.external_id == "d").unwrap().skipped,
            "too_large"
        );
        assert!(it
            .iter()
            .find(|f| f.external_id == "a")
            .unwrap()
            .indexed_at
            .starts_with("20"));
        let s = r.store.sources.lock().unwrap()["s1"].clone();
        assert_eq!((s.failed_items, s.skipped_items), (0, 2));

        // A changed page is an update; a page with text now is read again.
        r.provider.set("a", "2", "Deploys run on Mondays.");
        r.provider.set("c", "2", "Now it has text.");
        let o = sync(&r).await;
        assert_eq!((o.added, o.updated), (1, 1));
        let run = r.store.runs.lock().unwrap().last().cloned().unwrap();
        assert_eq!((run.added, run.updated, run.unchanged), (1, 1, 1));
        assert!(items(&r)
            .iter()
            .find(|f| f.external_id == "c")
            .unwrap()
            .skipped
            .is_empty());
        assert_eq!(r.store.runs.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_sync_that_cant_start_is_recorded_as_stopped_with_its_fix() {
        let r = rig();
        let err = ConnectorError::Reconnect("Notion refused the sign-in.".into());
        fail(
            r.store.as_ref(),
            "TEAM#t",
            "kb1",
            "s1",
            &err,
            1_800_000_000,
            "schedule",
        )
        .await;
        let run = r.store.runs.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            (run.result.as_str(), run.trigger.as_str()),
            ("stopped", "schedule")
        );
        assert!(run.error.unwrap().fix.contains("Reconnect"));
    }

    #[tokio::test]
    async fn a_source_keeps_its_newest_runs_only() {
        let store = MemSyncStore::default();
        for i in 0..(super::super::runs::MAX_RUNS_PER_SOURCE + 5) {
            let run = super::super::runs::SyncRun {
                source_id: "s1".into(),
                started_at: format!("{i}"),
                ..Default::default()
            };
            store.put_run("t", &run).await.unwrap();
        }
        let runs = store.runs.lock().unwrap();
        assert_eq!(runs.len(), super::super::runs::MAX_RUNS_PER_SOURCE);
        assert_eq!(runs[0].started_at, "5", "the oldest went first");
    }

    #[tokio::test]
    async fn a_sync_saves_the_connection_it_used() {
        let r = rig();
        r.provider.set("a", "1", "Deploys run on Fridays.");
        sync(&r).await;
        assert!(r.store.sources.lock().unwrap()["s1"].used.is_none());
        let clock = r.clock.clone();
        let now = move || clock.load(Ordering::SeqCst);
        let used = super::super::UsedConnection {
            via: "oauth".into(),
            owner: "team".into(),
            by: "ada@example.com".into(),
            used_at: "2026-11-11T10:00:00Z".into(),
            ..Default::default()
        };
        let ctx = Ctx {
            store: r.store.as_ref(),
            connector: r.provider.as_ref(),
            embedder: r.embedder.as_ref(),
            team: "TEAM#t",
            now: &now,
            used: Some(used.clone()),
            trigger: "manual",
            budget_secs: 0,
            token_room: None,
            limit: None,
        };
        run(&ctx, "kb1", "s1").await;
        assert_eq!(r.store.sources.lock().unwrap()["s1"].used, Some(used));
    }

    #[tokio::test]
    async fn unchanged_markers_skip_fetching_and_unchanged_text_skips_embedding() {
        let r = rig();
        r.provider.set("a", "1", "Deploys run on Fridays.");
        sync(&r).await;
        let (fetches, embedded) = (r.provider.fetches.load(Ordering::SeqCst), embeds(&r));
        // Nothing changed: nothing fetched, nothing embedded.
        let o = sync(&r).await;
        assert_eq!(o.fetched, 0);
        assert_eq!(r.provider.fetches.load(Ordering::SeqCst), fetches);
        // A new marker with the same text: fetched, not embedded.
        r.provider.set("a", "2", "Deploys run on Fridays.");
        let o = sync(&r).await;
        assert_eq!((o.fetched, o.same_text, o.indexed), (1, 1, 0));
        assert_eq!(embeds(&r), embedded, "the content hash skipped it");
        assert_eq!(items(&r)[0].remote_version, "2");
        assert_eq!(items(&r)[0].ready_version, 1, "the same index stays");
    }

    #[tokio::test]
    async fn an_edit_re_embeds_only_the_changed_passages() {
        let r = rig();
        r.provider.set(
            "a",
            "1",
            "Alpha part stays.\n\nBeta part changes.\n\nGamma part stays.",
        );
        sync(&r).await;
        let before = embeds(&r);
        assert_eq!(before, 3);
        r.provider.set(
            "a",
            "2",
            "Alpha part stays.\n\nBeta part was edited.\n\nGamma part stays.",
        );
        let o = sync(&r).await;
        assert_eq!((o.embedded_chunks, o.reused_chunks), (1, 2));
        assert_eq!(embeds(&r), before + 1);
        let f = &items(&r)[0];
        assert_eq!(f.ready_version, 2);
        // The new index is stored and the old one is gone.
        let objects = r.store.objects.lock().unwrap();
        assert!(objects.contains_key(&kb::index_key("TEAM#t", "kb1", &f.id, 2)));
        assert!(!objects.contains_key(&kb::index_key("TEAM#t", "kb1", &f.id, 1)));
        let idx = &objects[&kb::index_key("TEAM#t", "kb1", &f.id, 2)];
        assert!(idx.chunks.iter().any(|c| c.text.contains("was edited")));
    }

    #[tokio::test]
    async fn deletions_and_lost_access_remove_content() {
        let r = rig();
        r.provider.set("a", "1", "Kept.");
        r.provider.set("b", "1", "Deleted later.");
        r.provider.set("c", "1", "Access removed later.");
        sync(&r).await;
        assert_eq!(items(&r).len(), 3);
        let c_id = items(&r)[2].id.clone();
        r.provider.drop_page("b");
        // c is still listed with a new marker, but the connection can't read it.
        r.provider.set("c", "2", "Access removed later.");
        r.provider.gone.lock().unwrap().push("c".into());
        let o = sync(&r).await;
        assert_eq!(o.removed, 2);
        let left: Vec<String> = items(&r).into_iter().map(|f| f.external_id).collect();
        assert_eq!(left, ["a"]);
        assert!(
            !r.store
                .objects
                .lock()
                .unwrap()
                .keys()
                .any(|k| k.contains(&c_id)),
            "the removed item's index is deleted"
        );
        assert_eq!(r.store.sources.lock().unwrap()["s1"].items, 1);
    }

    #[tokio::test]
    async fn a_revoked_sign_in_stops_the_sync_with_a_reconnect_fix() {
        let r = rig();
        r.provider.set("a", "1", "Text.");
        sync(&r).await;
        *r.provider.fail_list.lock().unwrap() = Some(ConnectorError::Reconnect("gone".into()));
        let o = sync(&r).await;
        assert!(o.error.is_some());
        let s = r.store.sources.lock().unwrap()["s1"].clone();
        assert_eq!(s.status, Status::Error);
        let e = s.error.unwrap();
        assert_eq!(e.action, "reconnect");
        assert_eq!(e.fix, "Reconnect Notion to sign in again.");
        assert_eq!(items(&r).len(), 1, "a failed listing removes nothing");
        // Retried sooner than a normal interval.
        assert!(
            s.next_sync_at - r.clock.load(Ordering::SeqCst)
                < super::super::DEFAULT_INTERVAL_MINS * 60
        );
    }

    #[tokio::test]
    async fn a_held_source_is_asked_to_go_again_instead_of_syncing_twice() {
        let r = rig();
        r.provider.set("a", "1", "Text.");
        {
            let mut all = r.store.sources.lock().unwrap();
            all.get_mut("s1").unwrap().lease_until = 1_800_000_000 + 100;
        }
        let o = sync(&r).await;
        assert!(!o.ran);
        assert!(r.store.sources.lock().unwrap()["s1"].again);
        assert!(items(&r).is_empty());
        // Once the hold runs out (a crashed sync), the next one takes over.
        r.clock.fetch_add(200, Ordering::SeqCst);
        let o = sync(&r).await;
        assert!(o.ran);
        assert_eq!(items(&r).len(), 1);
        assert!(!r.store.sources.lock().unwrap()["s1"].again);
    }

    #[tokio::test]
    async fn a_big_sync_is_done_in_runs_that_continue_where_the_last_stopped() {
        let r = rig();
        for i in 0..1_500 {
            r.provider
                .set(&format!("p{i:05}"), "1", &format!("Page number {i} text."));
        }
        // Approved up front: this test is about the steps, not the estimate.
        r.store
            .sources
            .lock()
            .unwrap()
            .get_mut("s1")
            .unwrap()
            .approved_at = 1;
        *r.provider.clock.lock().unwrap() = Some((r.clock.clone(), 1));
        *r.budget.lock().unwrap() = 200;
        let first = sync(&r).await;
        assert!(first.more, "one run doesn't finish 1,500 items in 200 s");
        let s = source(&r);
        assert_eq!(s.status, Status::Syncing);
        assert!(s.continuing);
        assert_eq!(s.lease_until, 0, "the hold is released for the next run");
        assert_eq!(s.total, Some(1_500), "the real total, not a cap");
        assert!(
            s.items > 0 && s.items < 1_500,
            "searchable so far: {}",
            s.items
        );
        let p = s.progress.clone().unwrap();
        assert_eq!((p.phase.as_str(), p.total), ("fetching", 1_500));
        assert_eq!(p.done, s.items);
        assert_eq!(
            shown_status(&s, r.clock.load(Ordering::SeqCst)),
            Status::Syncing
        );
        // The page saw searchable items climb while the run worked.
        assert!(r
            .store
            .source_writes
            .lock()
            .unwrap()
            .iter()
            .any(|w| w.status == Status::Syncing && w.items > 0 && w.items < s.items));
        let (runs, last) = sync_all(&r).await;
        assert!(runs >= 3, "{runs} more runs");
        assert!(!last.more && last.error.is_none());
        let s = source(&r);
        assert_eq!(
            (s.status, s.items, s.continuing),
            (Status::Ready, 1_500, false)
        );
        assert!(!s.truncated);
        // Nothing was fetched twice, and the list is gone.
        assert_eq!(r.provider.fetches.load(Ordering::SeqCst), 1_500);
        assert!(r
            .store
            .bytes
            .lock()
            .unwrap()
            .keys()
            .all(|k| !k.ends_with("pending.json")));
        // A re-sync with two edits fetches those two only.
        r.provider.set("p00007", "2", "Edited.");
        r.provider.set("p01234", "2", "Edited too.");
        let o = sync(&r).await;
        assert_eq!((o.fetched, o.indexed, o.more), (2, 2, false));
        assert_eq!(r.provider.fetches.load(Ordering::SeqCst), 1_502);
    }

    #[tokio::test]
    async fn a_large_first_sync_waits_for_an_admin_with_its_estimate() {
        let r = rig();
        for i in 0..(LARGE_SYNC_ITEMS + 5) {
            r.provider
                .set(&format!("p{i:05}"), "1", "Some words to embed here.");
        }
        let o = sync(&r).await;
        assert!(o.awaiting && !o.more);
        assert_eq!(embeds(&r), 0, "nothing embedded before it is started");
        let s = source(&r);
        assert_eq!(s.status, Status::Approval);
        let e = s.estimate.clone().unwrap();
        assert_eq!(e.items, (LARGE_SYNC_ITEMS + 5) as u64);
        assert!(e.tokens >= e.items, "{e:?}");
        assert!(r.provider.fetches.load(Ordering::SeqCst) <= SAMPLE);
        // Started: it picks up the kept list without listing again.
        r.store
            .sources
            .lock()
            .unwrap()
            .get_mut("s1")
            .unwrap()
            .approved_at = 1;
        let (_, last) = sync_all(&r).await;
        assert!(last.error.is_none());
        let s = source(&r);
        assert_eq!(
            (s.status, s.items as usize),
            (Status::Ready, LARGE_SYNC_ITEMS + 5)
        );
        assert!(s.estimate.is_none());
    }

    #[tokio::test]
    async fn the_token_limit_stops_a_sync_and_it_continues_once_there_is_room() {
        let r = rig();
        for i in 0..20 {
            r.provider.set(&format!("p{i:02}"), "1", "Twelve chars");
        }
        // Room for about five items.
        *r.room.lock().unwrap() = Some(5 * 8);
        let o = sync(&r).await;
        assert!(o.over_budget && !o.more);
        let s = source(&r);
        assert_eq!(s.status, Status::Error);
        assert_eq!(s.error.as_ref().unwrap().action, "budget");
        assert!(s.items > 0 && s.items < 20, "{}", s.items);
        *r.room.lock().unwrap() = None;
        let o = sync(&r).await;
        assert!(o.error.is_none() && !o.over_budget);
        assert_eq!(source(&r).items, 20);
    }

    #[tokio::test]
    async fn a_provider_asking_to_slow_down_pauses_and_the_sync_continues() {
        let r = rig();
        for i in 0..10 {
            r.provider.set(&format!("p{i}"), "1", "Text.");
        }
        r.provider.limited.lock().unwrap().push("p5".into());
        let o = sync(&r).await;
        assert!(o.more && o.error.is_none());
        assert_eq!(o.wait_secs, RATE_WAIT_SECS);
        let s = source(&r);
        assert_eq!(s.progress.as_ref().unwrap().phase, "waiting");
        assert!(s.error.is_none());
        assert!(s.next_sync_at >= r.clock.load(Ordering::SeqCst) + RATE_WAIT_SECS);
        let (_, last) = sync_all(&r).await;
        assert!(last.error.is_none());
        assert_eq!(source(&r).items, 10);
    }

    #[tokio::test]
    async fn a_big_source_keeps_a_search_pack_and_rewrites_only_touched_shards() {
        let r = rig();
        for i in 0..(pack::PACK_FROM + 20) {
            r.provider
                .set(&format!("p{i:04}"), "1", &format!("Runbook {i} words"));
        }
        r.store
            .sources
            .lock()
            .unwrap()
            .get_mut("s1")
            .unwrap()
            .approved_at = 1;
        sync_all(&r).await;
        let s = source(&r);
        assert_eq!(s.pack.len(), pack::SHARDS);
        assert!(s.pack.iter().all(|v| *v == 1), "{:?}", s.pack);
        let mut packed = 0;
        for k in pack::keys("TEAM#t", "kb1", "s1", &s.pack) {
            let b = r.store.bytes.lock().unwrap()[&k].clone();
            packed += pack::decode(&b).unwrap().items.len();
        }
        assert_eq!(packed, pack::PACK_FROM + 20, "every item is in the pack");
        // One edit rewrites one shard.
        r.provider.set("p0003", "2", "Edited runbook words");
        sync(&r).await;
        let after = source(&r).pack;
        assert_eq!(after.iter().filter(|v| **v == 2).count(), 1, "{after:?}");
        // A lost shard is rebuilt from the items' own indexes.
        let lost = pack::keys("TEAM#t", "kb1", "s1", &after)[0].clone();
        r.store.bytes.lock().unwrap().remove(&lost);
        r.provider.set("p0004", "2", "Another edit");
        sync(&r).await;
        let mut packed = 0;
        for k in pack::keys("TEAM#t", "kb1", "s1", &source(&r).pack) {
            let b = r.store.bytes.lock().unwrap()[&k].clone();
            packed += pack::decode(&b).unwrap().items.len();
        }
        assert_eq!(packed, pack::PACK_FROM + 20);
    }

    #[tokio::test]
    async fn the_limit_given_is_kept_on_the_source() {
        let r = rig();
        r.provider.set("a", "1", "Text.");
        let clock = r.clock.clone();
        let now = move || clock.load(Ordering::SeqCst);
        let ctx = Ctx {
            store: r.store.as_ref(),
            connector: r.provider.as_ref(),
            embedder: r.embedder.as_ref(),
            team: "TEAM#t",
            now: &now,
            budget_secs: 0,
            token_room: None,
            limit: Some((30_000, "base")),
            used: None,
            trigger: "manual",
        };
        run(&ctx, "kb1", "s1").await;
        let s = source(&r);
        assert_eq!(
            (s.item_limit, s.limit_reason.as_str()),
            (Some(30_000), "base")
        );
    }
}
