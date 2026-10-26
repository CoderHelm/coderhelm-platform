//! Syncing one source: list, compare, fetch what changed, index only what
//! changed, remove what is gone, and say how it went.
//!
//! The same for every connector. One sync holds the source ([`super::LEASE_SECS`])
//! so a webhook, the schedule and "Sync now" never run two at once; a change
//! that arrives meanwhile sets `again` and the running sync goes round once
//! more. Progress is saved as it goes, so the page shows "12 of 40".
//!
//! Re-indexing is as small as the change:
//!
//! - an item whose version marker is unchanged is not fetched;
//! - a fetched item whose text hashes the same as before is not embedded
//!   (only its title, link or place are updated);
//! - a changed item re-embeds only the passages whose text changed; the
//!   others keep their vectors from the previous index.
//!
//! Every embedding goes through the [`Embedder`] given (Titan, metered on
//! the team's usage in production).

use std::collections::HashMap;

use futures::StreamExt;

use super::super::chunk::chunk;
use super::super::search::{Embedder, Index, IndexedChunk};
use super::super::{self as kb, FileMeta, FileStatus};
use super::connector::{Connector, ConnectorError, Document, Listed};
use super::store::SyncStore;
use super::{
    chunk_key, content_hash, item_id, next_sync, plan_sync, Progress, Source, Status, LEASE_SECS,
};

/// Embedding calls in flight at once for one item.
const EMBED_CONCURRENCY: usize = 6;
/// Progress is saved after this many items.
const PROGRESS_EVERY: u32 = 5;
/// Rounds one sync runs at most when changes keep arriving.
const MAX_ROUNDS: usize = 3;

/// What a sync did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Outcome {
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
    /// Why the sync stopped, when it did.
    pub error: Option<ConnectorError>,
}

/// What the sync needs.
pub struct Ctx<'a> {
    pub store: &'a dyn SyncStore,
    pub connector: &'a dyn Connector,
    pub embedder: &'a dyn Embedder,
    pub team: &'a str,
    /// Unix seconds now.
    pub now: &'a (dyn Fn() -> i64 + Send + Sync),
}

fn rfc3339(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .unwrap_or_default()
        .to_rfc3339()
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
    for _ in 0..MAX_ROUNDS {
        let Ok(Some(mut src)) = ctx.store.source(ctx.team, kb_id, id).await else {
            return out;
        };
        src.again = false;
        src.status = Status::Syncing;
        src.progress = Some(Progress {
            phase: "listing".into(),
            done: 0,
            total: 0,
        });
        src.lease_until = (ctx.now)() + LEASE_SECS;
        let _ = ctx.store.put_source(ctx.team, &src).await;
        let round = one_round(ctx, &mut src).await;
        out.listed = round.listed;
        out.fetched += round.fetched;
        out.same_text += round.same_text;
        out.indexed += round.indexed;
        out.embedded_chunks += round.embedded_chunks;
        out.reused_chunks += round.reused_chunks;
        out.removed += round.removed;
        out.failed = round.failed;
        out.error = round.error.clone();
        let again = finish(ctx, kb_id, id, &src, &round).await;
        if !again || round.error.is_some() {
            break;
        }
    }
    out
}

/// Saves the outcome onto the freshest copy of the source (someone may have
/// edited its name or selection meanwhile), releases the hold, and says
/// whether a change arrived during the sync.
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
        .filter(|f| f.status == FileStatus::Failed)
        .count() as u32;
    s.truncated = ran.truncated;
    s.cursor = ran.cursor.clone();
    s.last_sync_at = rfc3339(now);
    s.progress = None;
    s.lease_until = 0;
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
    if again {
        s.again = false;
    }
    let _ = ctx.store.put_source(ctx.team, &s).await;
    let _ = ctx.store.put_registry(ctx.team, &s).await;
    again && r.error.is_none()
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
) {
    let Ok(Some(s)) = store.source(team, kb_id, id).await else {
        return;
    };
    let error = super::source_error(&s, err);
    fail_with(store, team, kb_id, id, error, now).await;
}

/// [`fail`] with the error already worded.
pub async fn fail_with(
    store: &dyn SyncStore,
    team: &str,
    kb_id: &str,
    id: &str,
    error: super::SourceError,
    now: i64,
) {
    let Ok(Some(mut s)) = store.source(team, kb_id, id).await else {
        return;
    };
    s.status = Status::Error;
    s.error = Some(error);
    s.progress = None;
    s.last_sync_at = rfc3339(now);
    s.next_sync_at = next_sync(&s, now, true);
    let _ = store.put_source(team, &s).await;
    let _ = store.put_registry(team, &s).await;
}

async fn save_progress(ctx: &Ctx<'_>, src: &mut Source, phase: &str, done: u32, total: u32) {
    let progress = Progress {
        phase: phase.into(),
        done,
        total,
    };
    let lease = (ctx.now)() + LEASE_SECS;
    src.progress = Some(progress.clone());
    src.lease_until = lease;
    // Onto the freshest copy, so an edit made meanwhile (a new name, a
    // pause) isn't overwritten by this sync's older copy.
    let mut fresh = match ctx.store.source(ctx.team, &src.kb_id, &src.id).await {
        Ok(Some(f)) => f,
        _ => src.clone(),
    };
    fresh.status = Status::Syncing;
    fresh.progress = Some(progress);
    fresh.lease_until = lease;
    let _ = ctx.store.put_source(ctx.team, &fresh).await;
}

async fn one_round(ctx: &Ctx<'_>, src: &mut Source) -> Outcome {
    let mut out = Outcome::default();
    let listing = match ctx.connector.list(src).await {
        Ok(l) => l,
        Err(e) => {
            out.error = Some(e);
            return out;
        }
    };
    out.listed = listing.items.len();
    src.truncated = listing.truncated;
    let stored = match ctx.store.items(ctx.team, &src.kb_id, &src.id).await {
        Ok(s) => s,
        Err(e) => {
            out.error = Some(ConnectorError::Temporary(format!(
                "Stored items unreadable: {e}"
            )));
            return out;
        }
    };
    let by_ext: HashMap<String, FileMeta> = stored
        .iter()
        .map(|f| (f.external_id.clone(), f.clone()))
        .collect();
    let plan = plan_sync(&stored, &listing);
    let total = plan.fetch.len() as u32;
    save_progress(ctx, src, "fetching", 0, total).await;
    for (i, item) in plan.fetch.iter().enumerate() {
        let prev = by_ext.get(&item.external_id);
        match ctx.connector.fetch(src, item).await {
            Ok(doc) => {
                out.fetched += 1;
                match index_item(ctx, src, item, prev, &doc).await {
                    Ok(Indexed::SameText) => out.same_text += 1,
                    Ok(Indexed::New { embedded, reused }) => {
                        out.indexed += 1;
                        out.embedded_chunks += embedded;
                        out.reused_chunks += reused;
                    }
                    Err(why) => {
                        out.failed += 1;
                        mark_failed(ctx, src, item, prev, &why).await;
                    }
                }
            }
            Err(e) if e.item_gone() => {
                // Deleted, or the connection may no longer see it: its
                // content goes too.
                if let Some(p) = prev {
                    if ctx.store.delete_item(ctx.team, p).await.is_ok() {
                        out.removed += 1;
                    }
                }
            }
            Err(e @ ConnectorError::Reconnect(_)) | Err(e @ ConnectorError::RateLimited(_)) => {
                // Nothing more can be read now; what was done is kept.
                out.error = Some(e);
                return out;
            }
            Err(e) => {
                out.failed += 1;
                mark_failed(ctx, src, item, prev, &e.to_string()).await;
            }
        }
        let done = i as u32 + 1;
        if done.is_multiple_of(PROGRESS_EVERY) && done < total {
            save_progress(ctx, src, "fetching", done, total).await;
        }
    }
    if !plan.remove.is_empty() {
        save_progress(ctx, src, "removing", 0, plan.remove.len() as u32).await;
    }
    for f in &plan.remove {
        if ctx.store.delete_item(ctx.team, f).await.is_ok() {
            out.removed += 1;
        }
    }
    src.cursor = listing.cursor;
    out
}

enum Indexed {
    SameText,
    New { embedded: usize, reused: usize },
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
) -> Result<Indexed, String> {
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
    if prev.is_some_and(|p| p.content_hash == hash && p.ready_version > 0) {
        f.status = FileStatus::Ready;
        f.error.clear();
        ctx.store.put_item(ctx.team, &f).await?;
        return Ok(Indexed::SameText);
    }
    if doc.is_empty() {
        return Err("This page has no text to index.".into());
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
    ctx.store.put_item(ctx.team, &f).await?;
    if old_ready > 0 && old_ready != f.ready_version {
        ctx.store
            .delete_object(
                ctx.team,
                &kb::index_key(ctx.team, &f.kb_id, &f.id, old_ready),
            )
            .await;
    }
    Ok(Indexed::New { embedded, reused })
}

/// Records why an item couldn't be read. A version that was searchable
/// stays searchable.
async fn mark_failed(
    ctx: &Ctx<'_>,
    src: &Source,
    item: &Listed,
    prev: Option<&FileMeta>,
    why: &str,
) {
    let mut f = base_item(src, item, prev, (ctx.now)());
    if f.name.is_empty() {
        f.name = item.title.clone();
    }
    f.url = item.url.clone();
    f.path = item.path.clone();
    f.status = FileStatus::Failed;
    f.error = crate::truncate_str(why, 300).to_string();
    // Fetch it again next time even though its marker is unchanged.
    f.remote_version.clear();
    let _ = ctx.store.put_item(ctx.team, &f).await;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::super::connector::{Listing, Node};
    use super::super::store::memory::MemSyncStore;
    use super::super::{Kind, Selected};
    use super::*;
    use crate::knowledge::chunk::Section;
    use crate::knowledge::search::testing::WordEmbedder;

    /// A provider whose pages tests change between syncs.
    #[derive(Default)]
    struct FakeProvider {
        /// id -> (version, title, text)
        pages: Mutex<Vec<(String, String, String, String)>>,
        gone: Mutex<Vec<String>>,
        fail_list: Mutex<Option<ConnectorError>>,
        fetches: AtomicUsize,
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
            self.fetches.fetch_add(1, Ordering::SeqCst);
            if self.gone.lock().unwrap().contains(&item.external_id) {
                return Err(ConnectorError::Forbidden("no access".into()));
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
        }
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
        };
        run(&ctx, "kb1", "s1").await
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
}
