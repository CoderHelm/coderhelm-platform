//! Connected sources: a knowledge base kept in sync with pages that live
//! somewhere else (Notion, Confluence, Sanity, Google Drive, GitHub, public
//! web pages), next to the files people upload.
//!
//! A source is one connector plus a selection ("these Notion pages and
//! everything under them"). Syncing it works the same for every connector
//! ([`sync`]):
//!
//! 1. the connector lists every item the selection covers now, each with the
//!    provider's own version marker, without content ([`Connector::list`]);
//! 2. that listing is compared with the items stored last time
//!    ([`plan_sync`]): new and changed items are fetched, items no longer
//!    listed are deleted, the rest are left alone;
//! 3. a fetched item whose text hashes to what was indexed before is not
//!    embedded again; a changed item re-embeds only the passages whose text
//!    changed (the rest keep their vectors).
//!
//! Each synced item is a [`FileMeta`](super::FileMeta) of its base with its
//! `source_id`, `external_id` and original `url`, and its search index is
//! stored exactly like an upload's. Search, citations and the agents' tool
//! therefore need nothing new: a passage from Notion is found and cited like
//! one from a PDF, and its citation links to the Notion page.
//!
//! Records, all on the settings table:
//!
//! - a source: `pk` = team, `sk` = [`source_sk`];
//! - the sync registry, one row per source across teams so the scheduled
//!   sweep finds what is due without scanning: `pk` = [`REGISTRY_PK`],
//!   `sk` = [`registry_sk`];
//! - a webhook address: `pk` = [`hook_pk`] (the hash of its token), naming
//!   the team, base and source.
//!
//! Tokens never live here: a source names its connection (an OAuth sign-in
//! kept by `mcp_remote` and sealed by KMS, a team's saved integration key,
//! the GitHub App) and the worker opens it for each sync.

pub mod alert;
pub mod confluence;
pub mod connect;
pub mod connector;
pub mod gdrive;
pub mod github;
pub mod mcp;
pub mod notion;
pub mod oauth;
pub mod sanity;
pub mod sanity_mcp;
pub mod store;
pub mod sync;
pub mod text;
pub mod token;
pub mod web;
pub mod webhook;

use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use connector::{Connector, ConnectorError, Document, Listed, Listing, Node};

use super::FileMeta;

/// Asks the worker to sync one source (from the gateway's "Sync now" and
/// webhooks, and from the scheduled sweep).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncMessage {
    pub team_id: String,
    pub kb_id: String,
    pub source_id: String,
}

/// Sources one knowledge base holds.
pub const MAX_SOURCES_PER_BASE: usize = 10;
/// Items one source keeps; a bigger selection syncs its first ones and says
/// so.
pub const MAX_ITEMS_PER_SOURCE: usize = 1_000;
/// Entries one selection holds (each may cover a whole tree).
pub const MAX_SELECTED: usize = 100;
/// How often a source is synced on schedule without webhooks.
pub const DEFAULT_INTERVAL_MINS: i64 = 60;
/// How often a source whose provider sends webhooks is still synced on
/// schedule, as a backstop for a missed event.
pub const WEBHOOK_INTERVAL_MINS: i64 = 6 * 60;
/// How often a source read through a person's sign-in is synced: no
/// webhook reaches it, and each sync asks only for what changed, so it can
/// look often.
pub const SIGNIN_INTERVAL_MINS: i64 = 15;
/// A sync holds its source this long at most; a crashed sync's hold runs out
/// and the next one takes over.
pub const LEASE_SECS: i64 = 14 * 60;

/// The kinds of connector. A plugin adds a kind by implementing
/// [`Connector`] behind a new [`Kind`] (see `connector.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Notion,
    Confluence,
    Sanity,
    GoogleDrive,
    Github,
    Web,
}

impl Kind {
    pub const ALL: [Kind; 6] = [
        Kind::Notion,
        Kind::Confluence,
        Kind::Sanity,
        Kind::GoogleDrive,
        Kind::Github,
        Kind::Web,
    ];

    pub fn key(self) -> &'static str {
        match self {
            Kind::Notion => "notion",
            Kind::Confluence => "confluence",
            Kind::Sanity => "sanity",
            Kind::GoogleDrive => "google_drive",
            Kind::Github => "github",
            Kind::Web => "web",
        }
    }

    pub fn parse(s: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|k| k.key() == s)
    }

    pub fn label(self) -> &'static str {
        match self {
            Kind::Notion => "Notion",
            Kind::Confluence => "Confluence",
            Kind::Sanity => "Sanity",
            Kind::GoogleDrive => "Google Drive",
            Kind::Github => "GitHub",
            Kind::Web => "Website",
        }
    }

    /// One line under the connector's name in "Add source".
    pub fn blurb(self) -> &'static str {
        match self {
            Kind::Notion => "Pages and databases, with the pages under them",
            Kind::Confluence => "Spaces or page trees",
            Kind::Sanity => "Documents of the types you pick",
            Kind::GoogleDrive => "Folders, Docs, Sheets, Slides and PDFs",
            Kind::Github => "Docs folders and Markdown in your repositories",
            Kind::Web => "Public pages or a whole sitemap",
        }
    }

    /// Does the provider tell Coderhelm about changes as they happen?
    pub fn has_webhooks(self) -> bool {
        matches!(self, Kind::Sanity | Kind::Notion | Kind::Github)
    }
}

/// One entry of a selection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selected {
    /// The provider's id (a page id, a space key, a folder id, a URL).
    pub id: String,
    pub title: String,
    /// What it is (`page`, `database`, `space`, `folder`, `file`, `repo`,
    /// `path`, `type`, `url`, `sitemap`).
    pub kind: String,
    /// The titles above it, for showing where it is.
    #[serde(default)]
    pub path: Vec<String>,
    /// Include what is under it, now and later. On by default: "everything I
    /// select, including new children".
    #[serde(default = "yes")]
    pub include_children: bool,
}

fn yes() -> bool {
    true
}

/// Where a source is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Waiting for its first sync.
    #[default]
    Queued,
    Syncing,
    Ready,
    /// The last sync stopped; [`Source::error`] says why and how to fix it.
    Error,
}

/// Why a sync stopped, in plain words, and the one thing that fixes it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceError {
    pub message: String,
    /// The fix, as a sentence ("Reconnect Notion to sign in again.").
    pub fix: String,
    /// The button that fixes it: `reconnect`, `edit`, `retry`, or empty.
    #[serde(default)]
    pub action: String,
    pub at: String,
}

/// Whose sign-in a source reads with (a source synced through a provider's
/// remote MCP server). Set by the server only, never from what a page sends:
/// it names the person whose connection every sync uses.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignIn {
    /// The MCP server record that holds the sign-in (the team's Connected
    /// apps entry for the provider, or Knowledge's own `ks-<kind>` one).
    pub server_id: String,
    /// The connection's owner on that record (`user:<email>` or `team`).
    pub owner: String,
    /// The member who connected it.
    pub email: String,
    /// Their name, for "Synced with Ada's Sanity sign-in".
    #[serde(default)]
    pub name: String,
    /// The provider account the sign-in belongs to ("Connected as").
    #[serde(default)]
    pub account: String,
    /// When the team's admins were told it stopped working (cleared once
    /// it works again), so they are told once.
    #[serde(default)]
    pub alerted_at: String,
}

impl SignIn {
    /// The name a page shows for the member: their name, else their email.
    pub fn who(&self) -> &str {
        if self.name.trim().is_empty() {
            &self.email
        } else {
            self.name.trim()
        }
    }
}

/// The error a source shows. A source read through someone's sign-in says
/// whose sign-in stopped working, and any admin can sign in again.
pub fn source_error(s: &Source, e: &ConnectorError) -> SourceError {
    let kind = s.kind.unwrap_or(Kind::Web);
    match (&s.signin, e) {
        (Some(si), ConnectorError::Reconnect(_)) => signin_error(si, kind, e.to_string(), false),
        _ => e.for_source(kind),
    }
}

/// The one sentence a sign-in source shows when its sign-in can't be used,
/// with the "Sign in again" button (`sign_in`) any admin can press.
pub fn signin_error(si: &SignIn, kind: Kind, message: String, member_gone: bool) -> SourceError {
    let who = si.who();
    let label = kind.label();
    let fix = if member_gone {
        format!("{who} is no longer on the team, so their {label} sign-in can't be used. Sign in again to keep this up to date.")
    } else {
        format!("{who}'s {label} sign-in stopped working. Sign in again to keep this up to date.")
    };
    SourceError {
        message,
        fix,
        action: "sign_in".into(),
        at: chrono::Utc::now().to_rfc3339(),
    }
}

/// Progress of the sync running now.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// `listing`, `fetching`, `removing`.
    pub phase: String,
    pub done: u32,
    pub total: u32,
}

/// A knowledge base's connected source.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub id: String,
    pub kb_id: String,
    pub kind: Option<Kind>,
    /// What the page calls it ("Engineering wiki").
    pub name: String,
    pub selection: Vec<Selected>,
    /// Connector settings (Sanity: dataset, types and how each maps to text;
    /// GitHub: the repository and branch).
    #[serde(default)]
    pub options: serde_json::Value,
    /// Stops scheduled and webhook syncs; "Sync now" still works.
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub status: Status,
    #[serde(default)]
    pub error: Option<SourceError>,
    #[serde(default)]
    pub progress: Option<Progress>,
    /// Items synced and searchable.
    #[serde(default)]
    pub items: u32,
    /// Items that couldn't be read last time (each says why).
    #[serde(default)]
    pub failed_items: u32,
    /// The selection covers more than [`MAX_ITEMS_PER_SOURCE`] items.
    #[serde(default)]
    pub truncated: bool,
    /// The connector's own position for the next sync (opaque).
    #[serde(default)]
    pub cursor: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub created_by: String,
    #[serde(default)]
    pub last_sync_at: String,
    #[serde(default)]
    pub last_success_at: String,
    /// Unix seconds; the sweep starts a sync at or after this.
    #[serde(default)]
    pub next_sync_at: i64,
    /// Unix seconds a running sync holds the source until.
    #[serde(default)]
    pub lease_until: i64,
    /// A change arrived while a sync ran: run once more after it.
    #[serde(default)]
    pub again: bool,
    /// The provider sends change events for this source.
    #[serde(default)]
    pub webhook: bool,
    #[serde(default)]
    pub webhook_seen_at: String,
    /// The provider's id of the account it reads (a Notion workspace, a
    /// Sanity project), to route that provider's webhooks.
    #[serde(default)]
    pub account: String,
    /// The key of this source's webhook address record ([`hook_pk`]), when
    /// it has one.
    #[serde(default)]
    pub hook: String,
    /// Read through a person's sign-in (not an API key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signin: Option<SignIn>,
}

impl Source {
    /// Minutes between scheduled syncs.
    pub fn interval_mins(&self) -> i64 {
        if self.signin.is_some() && !self.webhook {
            SIGNIN_INTERVAL_MINS
        } else if self.webhook {
            WEBHOOK_INTERVAL_MINS
        } else {
            DEFAULT_INTERVAL_MINS
        }
    }

    /// Is a scheduled sync due at `now` (unix seconds)?
    pub fn due(&self, now: i64) -> bool {
        !self.paused && self.next_sync_at <= now && self.lease_until <= now
    }

    pub fn kind_key(&self) -> &'static str {
        self.kind.map(Kind::key).unwrap_or("")
    }
}

/// The status a source shows: a sync whose hold ran out is not syncing any
/// more, whatever its record says.
pub fn shown_status(s: &Source, now: i64) -> Status {
    if s.status == Status::Syncing && s.lease_until < now {
        if s.error.is_some() {
            Status::Error
        } else if s.last_success_at.is_empty() {
            Status::Queued
        } else {
            Status::Ready
        }
    } else {
        s.status
    }
}

// ── Keys ────────────────────────────────────────────────────────────────────

pub fn source_sk(kb_id: &str, source_id: &str) -> String {
    format!("KBSRC#{kb_id}#{source_id}")
}

pub fn source_prefix(kb_id: &str) -> String {
    format!("KBSRC#{kb_id}#")
}

/// Every team's sources are listed under this one key for the sweep.
pub const REGISTRY_PK: &str = "KBSRC_REGISTRY";

pub fn registry_sk(team_id: &str, kb_id: &str, source_id: &str) -> String {
    format!("{team_id}|{kb_id}|{source_id}")
}

/// `(team, base, source)` of a registry sort key.
pub fn parse_registry_sk(sk: &str) -> Option<(String, String, String)> {
    let mut it = sk.rsplitn(3, '|');
    let source = it.next()?.to_string();
    let kb = it.next()?.to_string();
    let team = it.next()?.to_string();
    (!team.is_empty() && super::valid_id(&kb) && super::valid_id(&source))
        .then_some((team, kb, source))
}

/// A webhook token's record key: its hash, so a stored record never yields
/// a working address.
pub fn hook_pk(token: &str) -> String {
    format!("KBHOOK#{}", hex(&Sha256::digest(token.as_bytes())))
}

/// The id of a synced item: the same for the same source and provider id, so
/// a sync that runs twice updates one record.
pub fn item_id(source_id: &str, external_id: &str) -> String {
    let h = Sha256::digest(format!("{source_id}\n{external_id}").as_bytes());
    format!("i{}", &hex(&h)[..31])
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The settings-table record of a source.
pub fn source_record(team_id: &str, s: &Source) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".into(), AttributeValue::S(team_id.to_string())),
        ("sk".into(), AttributeValue::S(source_sk(&s.kb_id, &s.id))),
        (
            "data".into(),
            AttributeValue::S(serde_json::to_string(s).unwrap_or_default()),
        ),
    ])
}

// ── Content hashes ──────────────────────────────────────────────────────────

/// SHA-256 of a document as it would be indexed: its title, address and
/// every section's place and text. Equal hashes mean nothing to re-index.
pub fn content_hash(doc: &Document) -> String {
    let mut h = Sha256::new();
    h.update(doc.title.as_bytes());
    h.update([0]);
    h.update(doc.url.as_bytes());
    for s in &doc.sections {
        h.update([1]);
        h.update(s.loc.as_bytes());
        h.update([0]);
        h.update(s.text.as_bytes());
        if let Some(head) = &s.header {
            h.update([2]);
            h.update(head.as_bytes());
        }
    }
    hex(&h.finalize())
}

/// The key a passage's vector is reused under: its place and text.
pub fn chunk_key(loc: &str, text: &str) -> String {
    hex(&Sha256::digest(format!("{loc}\n{text}").as_bytes()))
}

// ── Planning a sync ─────────────────────────────────────────────────────────

/// What one sync does.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Plan {
    /// New items, and items whose version marker changed: fetched.
    pub fetch: Vec<Listed>,
    /// Stored items no longer covered (deleted, moved out of the selection,
    /// or no longer visible to the connection): removed.
    pub remove: Vec<FileMeta>,
    /// Listed with the same marker (or failed last time, then fetched):
    /// left alone.
    pub unchanged: usize,
}

/// Compares a listing with the items stored for the source.
///
/// A complete listing names every item the selection covers, so a stored
/// item missing from it is removed. An incremental listing (`complete` is
/// false) names only what changed since the cursor, and its `deleted` ids
/// are the only removals. An item that failed last time is fetched again
/// even when its marker is unchanged.
pub fn plan_sync(stored: &[FileMeta], listing: &Listing) -> Plan {
    let by_ext: HashMap<&str, &FileMeta> =
        stored.iter().map(|f| (f.external_id.as_str(), f)).collect();
    let mut plan = Plan::default();
    let mut seen: HashSet<&str> = HashSet::new();
    for item in &listing.items {
        if !seen.insert(item.external_id.as_str()) {
            continue;
        }
        match by_ext.get(item.external_id.as_str()) {
            Some(f)
                if !item.version.is_empty()
                    && f.remote_version == item.version
                    && f.ready_version > 0
                    && f.status == super::FileStatus::Ready =>
            {
                plan.unchanged += 1
            }
            _ => plan.fetch.push(item.clone()),
        }
    }
    let deleted: HashSet<&str> = listing.deleted.iter().map(String::as_str).collect();
    for f in stored {
        let gone = if listing.complete {
            !seen.contains(f.external_id.as_str())
        } else {
            deleted.contains(f.external_id.as_str())
        };
        if gone {
            plan.remove.push(f.clone());
        }
    }
    plan
}

/// The next scheduled sync after one that ended at `now`: sooner after a
/// temporary failure, never sooner than five minutes.
pub fn next_sync(s: &Source, now: i64, failed: bool) -> i64 {
    let mins = if failed {
        (s.interval_mins() / 4).max(5)
    } else {
        s.interval_mins()
    };
    now + mins * 60
}

/// The selection, checked: at most [`MAX_SELECTED`], ids and titles kept
/// short, duplicates dropped.
pub fn clean_selection(raw: &[Selected]) -> Result<Vec<Selected>, String> {
    if raw.is_empty() {
        return Err("Pick at least one thing to sync.".into());
    }
    if raw.len() > MAX_SELECTED {
        return Err(format!("Pick up to {MAX_SELECTED} things per source."));
    }
    let mut out: Vec<Selected> = Vec::new();
    for s in raw {
        let id = s.id.trim();
        if id.is_empty() || id.len() > 2_000 || id.chars().any(char::is_control) {
            return Err("One of the picked items has no usable id.".into());
        }
        if out.iter().any(|o| o.id == id) {
            continue;
        }
        out.push(Selected {
            id: id.to_string(),
            title: crate::truncate_str(s.title.trim(), 300).to_string(),
            kind: crate::truncate_str(s.kind.trim(), 40).to_string(),
            path: s
                .path
                .iter()
                .take(10)
                .map(|p| crate::truncate_str(p, 200).to_string())
                .collect(),
            include_children: s.include_children,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::super::FileStatus;
    use super::*;

    fn stored(ext: &str, version: &str) -> FileMeta {
        FileMeta {
            id: item_id("s", ext),
            kb_id: "kb".into(),
            source_id: "s".into(),
            external_id: ext.into(),
            remote_version: version.into(),
            status: FileStatus::Ready,
            version: 1,
            ready_version: 1,
            ..Default::default()
        }
    }

    fn listed(ext: &str, version: &str) -> Listed {
        Listed {
            external_id: ext.into(),
            title: ext.into(),
            version: version.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_complete_listing_fetches_new_and_changed_and_removes_missing() {
        let have = vec![stored("a", "1"), stored("b", "1"), stored("c", "1")];
        let listing = Listing {
            items: vec![listed("a", "1"), listed("b", "2"), listed("d", "1")],
            complete: true,
            ..Default::default()
        };
        let p = plan_sync(&have, &listing);
        let fetched: Vec<&str> = p.fetch.iter().map(|l| l.external_id.as_str()).collect();
        assert_eq!(fetched, ["b", "d"]);
        let removed: Vec<&str> = p.remove.iter().map(|f| f.external_id.as_str()).collect();
        assert_eq!(removed, ["c"]);
        assert_eq!(p.unchanged, 1);
    }

    #[test]
    fn an_incremental_listing_removes_only_what_it_says_was_deleted() {
        let have = vec![stored("a", "1"), stored("b", "1"), stored("c", "1")];
        let listing = Listing {
            items: vec![listed("b", "2")],
            deleted: vec!["c".into()],
            complete: false,
            ..Default::default()
        };
        let p = plan_sync(&have, &listing);
        assert_eq!(p.fetch.len(), 1);
        let removed: Vec<&str> = p.remove.iter().map(|f| f.external_id.as_str()).collect();
        assert_eq!(removed, ["c"]);
    }

    #[test]
    fn failed_or_unversioned_items_are_fetched_again() {
        let mut failed = stored("a", "1");
        failed.status = FileStatus::Failed;
        failed.ready_version = 0;
        let p = plan_sync(
            &[failed, stored("b", "")],
            &Listing {
                items: vec![listed("a", "1"), listed("b", "")],
                complete: true,
                ..Default::default()
            },
        );
        assert_eq!(p.fetch.len(), 2);
        assert!(p.remove.is_empty());
    }

    #[test]
    fn duplicates_in_a_listing_are_fetched_once() {
        let p = plan_sync(
            &[],
            &Listing {
                items: vec![listed("a", "1"), listed("a", "1")],
                complete: true,
                ..Default::default()
            },
        );
        assert_eq!(p.fetch.len(), 1);
    }

    #[test]
    fn hashes_change_with_the_text_only() {
        let doc = |t: &str| Document {
            title: "Runbook".into(),
            url: "https://x.example/p".into(),
            sections: vec![super::super::chunk::Section {
                loc: "Deploy".into(),
                text: t.into(),
                header: None,
            }],
            ..Default::default()
        };
        assert_eq!(content_hash(&doc("a")), content_hash(&doc("a")));
        assert_ne!(content_hash(&doc("a")), content_hash(&doc("b")));
        assert_ne!(chunk_key("A", "x"), chunk_key("B", "x"));
    }

    #[test]
    fn ids_and_keys_are_stable_and_safe() {
        let a = item_id("src1", "page-1");
        assert_eq!(a, item_id("src1", "page-1"));
        assert_ne!(a, item_id("src2", "page-1"));
        assert!(super::super::valid_id(&a));
        assert_eq!(a.len(), 32);
        let sk = registry_sk("TEAM#g_1", "kb1", "src1");
        assert_eq!(
            parse_registry_sk(&sk),
            Some(("TEAM#g_1".into(), "kb1".into(), "src1".into()))
        );
        assert_eq!(parse_registry_sk("x|../y|z"), None);
        assert!(hook_pk("tok").starts_with("KBHOOK#"));
        assert_ne!(hook_pk("tok"), hook_pk("tok2"));
        assert!(!hook_pk("tok").contains("tok"));
    }

    #[test]
    fn selections_are_checked() {
        let one = Selected {
            id: "p1".into(),
            title: "Runbooks".into(),
            kind: "page".into(),
            include_children: true,
            ..Default::default()
        };
        assert!(clean_selection(&[]).is_err());
        assert_eq!(
            clean_selection(&[one.clone(), one.clone()]).unwrap().len(),
            1
        );
        let many: Vec<Selected> = (0..=MAX_SELECTED)
            .map(|i| Selected {
                id: format!("p{i}"),
                ..one.clone()
            })
            .collect();
        assert!(clean_selection(&many).is_err());
        let bad = Selected {
            id: "a\u{0}b".into(),
            ..one
        };
        assert!(clean_selection(&[bad]).is_err());
        // "Include what is under it" is on unless turned off.
        let s: Selected = serde_json::from_str(r#"{"id":"x","title":"t","kind":"page"}"#).unwrap();
        assert!(s.include_children);
    }

    #[test]
    fn schedules_follow_webhooks_pauses_and_failures() {
        let mut s = Source {
            next_sync_at: 100,
            ..Default::default()
        };
        assert!(s.due(100));
        assert!(!s.due(99));
        s.lease_until = 200;
        assert!(!s.due(150), "a running sync holds it");
        s.lease_until = 0;
        s.paused = true;
        assert!(!s.due(1_000));
        s.paused = false;
        assert_eq!(next_sync(&s, 0, false), DEFAULT_INTERVAL_MINS * 60);
        assert_eq!(next_sync(&s, 0, true), 15 * 60);
        s.webhook = true;
        assert_eq!(next_sync(&s, 0, false), WEBHOOK_INTERVAL_MINS * 60);
        s.webhook = false;
        s.signin = Some(SignIn::default());
        assert_eq!(next_sync(&s, 0, false), SIGNIN_INTERVAL_MINS * 60);
    }

    #[test]
    fn a_sign_in_source_says_whose_sign_in_stopped_working() {
        let mut s = Source {
            kind: Some(Kind::Sanity),
            ..Default::default()
        };
        let gone = ConnectorError::Reconnect("expired".into());
        assert_eq!(source_error(&s, &gone).action, "reconnect");
        s.signin = Some(SignIn {
            email: "ada@example.com".into(),
            name: "Ada".into(),
            ..Default::default()
        });
        let e = source_error(&s, &gone);
        assert_eq!(e.action, "sign_in");
        assert_eq!(
            e.fix,
            "Ada's Sanity sign-in stopped working. Sign in again to keep this up to date."
        );
        let e = signin_error(
            s.signin.as_ref().unwrap(),
            Kind::Sanity,
            String::new(),
            true,
        );
        assert!(e.fix.starts_with("Ada is no longer on the team"));
        // Other errors read as before.
        assert_eq!(
            source_error(&s, &ConnectorError::Temporary("x".into())).action,
            "retry"
        );
    }

    #[test]
    fn a_sync_whose_hold_ran_out_is_not_shown_as_syncing() {
        let s = Source {
            status: Status::Syncing,
            lease_until: 10,
            last_success_at: "2026-10-01T00:00:00Z".into(),
            ..Default::default()
        };
        assert_eq!(shown_status(&s, 5), Status::Syncing);
        assert_eq!(shown_status(&s, 11), Status::Ready);
    }

    #[test]
    fn kinds_round_trip() {
        for k in Kind::ALL {
            assert_eq!(Kind::parse(k.key()), Some(k));
            assert_eq!(serde_json::to_value(k).unwrap(), serde_json::json!(k.key()));
        }
        assert_eq!(Kind::parse("dropbox"), None);
    }
}
