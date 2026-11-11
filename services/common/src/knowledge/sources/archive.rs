//! Importing an archive: a `.zip` of a Notion export (Markdown and CSV), a
//! Confluence HTML export, or a zipped docs folder, uploaded once and
//! unpacked on the server (Notion's paths are often too long to unpack on
//! Windows).
//!
//! An archive is a source like any other ([`Kind::Archive`]), so the sync
//! engine does the rest: every supported entry becomes an item of the base
//! (its path in the archive is its id), and uploading a newer export to the
//! same source re-reads only the entries whose content changed (by their
//! CRC-32 and size), keeps the vectors of passages whose text didn't change,
//! and removes the entries the newer export no longer has.
//!
//! The archive stays in the team's own folder of the data bucket
//! ([`archive_key`]), KMS encrypted, and goes with its source or its base.
//! Every entry becomes readable by everyone who can read the base, which the
//! page says before the upload ([`NOTE`]).

use std::io::Read;
use std::sync::Arc;

use tokio::sync::OnceCell;

use super::super::extract::{extract, Kind as FileKind};
use super::connector::{Connector, ConnectorError, Document, Listed, Listing, Node};
use super::{Kind, Source};

/// The largest archive taken.
pub const MAX_ARCHIVE_BYTES: u64 = 200 * 1024 * 1024;
/// Entries read from one archive at most (supported or not).
const MAX_ENTRIES: usize = 50_000;
/// An entry that unpacks to more than this many times its packed size is
/// not read (a zip bomb).
const MAX_RATIO: u64 = 200;

/// Shown before an archive is uploaded.
pub const NOTE: &str = "Every page in the archive becomes readable by everyone who can read this knowledge base. A Confluence Full Export also includes pages that are restricted in Confluence.";

/// Where an archive's upload is kept: the team's folder, by base, source
/// and upload.
pub fn archive_key(team_id: &str, kb_id: &str, source_id: &str, version: u32) -> String {
    format!("{team_id}/knowledge/{kb_id}/archives/{source_id}/v{version}/archive.zip")
}

/// An archive's file name, checked.
pub fn check_archive(name: &str, size: u64) -> Result<String, String> {
    let name = super::super::clean_file_name(name);
    if !name.to_ascii_lowercase().ends_with(".zip") {
        return Err(
            "Upload a .zip file: a Notion or Confluence export, or a zipped docs folder.".into(),
        );
    }
    if size == 0 {
        return Err("The archive is empty.".into());
    }
    if size > MAX_ARCHIVE_BYTES {
        return Err(format!(
            "The archive is larger than {} MB. Split it, or connect the source in place instead.",
            MAX_ARCHIVE_BYTES / (1024 * 1024)
        ));
    }
    Ok(name)
}

/// An exported name made readable: Notion's ` <32 hex id>` and
/// Confluence's `_<page id>` endings dropped, dashes in Confluence's names
/// read as spaces.
pub fn clean_title(segment: &str) -> String {
    let (stem, ext) = match segment.rsplit_once('.') {
        Some((s, e)) if FileKind::of(segment).is_some() => (s, Some(e)),
        _ => (segment, None),
    };
    let mut t = stem.to_string();
    if let Some((head, id)) = t.rsplit_once(' ') {
        if id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit()) {
            t = head.to_string();
        }
    }
    if ext.is_some_and(|e| e.eq_ignore_ascii_case("html") || e.eq_ignore_ascii_case("htm")) {
        if let Some((head, id)) = t.rsplit_once('_') {
            if !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()) {
                t = head.replace('-', " ");
            }
        }
    }
    let t = t.trim();
    if t.is_empty() {
        segment.to_string()
    } else {
        t.to_string()
    }
}

/// Is this entry a document (not a folder, a hidden file or a Mac resource
/// fork)?
fn readable(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    !path.ends_with('/')
        && !path.starts_with("__MACOSX/")
        && !path.split('/').any(|p| p.starts_with('.'))
        && FileKind::of(name).is_some()
}

/// The archive's bytes, as the connector reads them. The seam tests fake.
#[async_trait::async_trait]
pub trait ArchiveBytes: Send + Sync {
    async fn bytes(&self) -> Result<Vec<u8>, ConnectorError>;
}

/// The archive connector, reading one uploaded archive.
pub struct Archive<'a> {
    pub bytes: Box<dyn ArchiveBytes + 'a>,
    /// A ceiling on top of the source's own limit ([`Source::limit`]);
    /// `usize::MAX` in production.
    pub max_items: usize,
    loaded: OnceCell<Arc<Vec<u8>>>,
}

impl<'a> Archive<'a> {
    pub fn new(bytes: Box<dyn ArchiveBytes + 'a>, max_items: usize) -> Self {
        Archive {
            bytes,
            max_items,
            loaded: OnceCell::new(),
        }
    }

    /// The archive, read once per sync.
    async fn data(&self) -> Result<Arc<Vec<u8>>, ConnectorError> {
        self.loaded
            .get_or_try_init(|| async { self.bytes.bytes().await.map(Arc::new) })
            .await
            .cloned()
    }
}

fn open(bytes: &[u8]) -> Result<zip::ZipArchive<std::io::Cursor<&[u8]>>, ConnectorError> {
    zip::ZipArchive::new(std::io::Cursor::new(bytes)).map_err(|_| {
        ConnectorError::Config(
            "The upload isn't a zip archive Coderhelm can read. Export again and upload the .zip as it was downloaded.".into(),
        )
    })
}

/// A zip entry's time as `YYYY-MM-DD`.
fn entry_date(f: &zip::read::ZipFile<'_, std::io::Cursor<&[u8]>>) -> String {
    f.last_modified()
        .filter(|d| d.year() >= 1990)
        .map(|d| format!("{:04}-{:02}-{:02}", d.year(), d.month(), d.day()))
        .unwrap_or_default()
}

/// Every document entry of the archive, with its version marker.
pub fn entries(bytes: &[u8], max_items: usize) -> Result<Listing, ConnectorError> {
    let mut zip = open(bytes)?;
    let mut items: Vec<Listed> = Vec::new();
    for i in 0..zip.len().min(MAX_ENTRIES) {
        let Ok(f) = zip.by_index_raw(i) else { continue };
        // zip 9 checks the raw name: an entry whose name is not valid text is skipped.
        let Ok(name) = f.name() else { continue };
        let path = name.replace('\\', "/");
        if !readable(&path) {
            continue;
        }
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let Some((last, folders)) = segments.split_last() else {
            continue;
        };
        items.push(Listed {
            external_id: path.clone(),
            title: clean_title(last),
            updated_at: entry_date(&f),
            version: format!("{:08x}-{}", f.crc32(), f.size()),
            path: folders
                .iter()
                .map(|s| clean_title(s))
                .collect::<Vec<_>>()
                .join(" / "),
            hint: serde_json::json!({ "size": f.size(), "packed": f.compressed_size() }),
            ..Default::default()
        });
    }
    let (truncated, found) = super::keep_within(&mut items, max_items);
    Ok(Listing {
        items,
        complete: true,
        truncated,
        total: Some(found),
        ..Default::default()
    })
}

/// One entry's content, bounded.
pub fn entry(bytes: &[u8], path: &str) -> Result<Vec<u8>, ConnectorError> {
    let mut zip = open(bytes)?;
    let max = super::super::MAX_FILE_BYTES;
    let f = zip.by_name(path).map_err(|_| {
        ConnectorError::NotFound("The newer upload no longer has this page.".into())
    })?;
    if f.size() > max {
        return Err(ConnectorError::Config(format!(
            "The file is larger than {} MB.",
            max / (1024 * 1024)
        )));
    }
    if f.compressed_size() > 0 && f.size() / f.compressed_size().max(1) > MAX_RATIO {
        return Err(ConnectorError::Config(
            "This file unpacks to far more than its size in the archive, so it isn't read.".into(),
        ));
    }
    let mut out = Vec::with_capacity(f.size() as usize);
    f.take(max + 1)
        .read_to_end(&mut out)
        .map_err(|_| ConnectorError::Config("This file in the archive is damaged.".into()))?;
    if out.len() as u64 > max {
        return Err(ConnectorError::Config("The file is too large.".into()));
    }
    Ok(out)
}

#[async_trait::async_trait]
impl Connector for Archive<'_> {
    fn kind(&self) -> Kind {
        Kind::Archive
    }

    async fn browse(
        &self,
        _parent: Option<&str>,
        _query: &str,
    ) -> Result<Vec<Node>, ConnectorError> {
        Ok(Vec::new())
    }

    async fn list(&self, source: &Source) -> Result<Listing, ConnectorError> {
        let data = self.data().await?;
        let max = source.limit().min(self.max_items);
        tokio::task::spawn_blocking(move || entries(&data, max))
            .await
            .map_err(|_| ConnectorError::Temporary("The archive couldn't be read.".into()))?
    }

    async fn fetch(&self, _source: &Source, item: &Listed) -> Result<Document, ConnectorError> {
        let name = item.external_id.rsplit('/').next().unwrap_or("");
        let kind = FileKind::of(name)
            .ok_or_else(|| ConnectorError::Config("This kind of file isn't read.".into()))?;
        let data = self.data().await?;
        let path = item.external_id.clone();
        let extracted = tokio::task::spawn_blocking(move || {
            entry(&data, &path).and_then(|b| extract(kind, &b).map_err(ConnectorError::Config))
        })
        .await
        .map_err(|_| ConnectorError::Temporary("The archive couldn't be read.".into()))??;
        let mut sections = extracted.sections;
        for s in sections.iter_mut() {
            if s.loc.is_empty() {
                s.loc = item.title.clone();
            }
        }
        Ok(Document {
            title: item.title.clone(),
            url: String::new(),
            updated_at: extracted.as_of.unwrap_or_else(|| item.updated_at.clone()),
            sections,
        })
    }
}

/// The archive in the data bucket.
pub struct StoredArchive {
    pub s3: aws_sdk_s3::Client,
    pub bucket: String,
    pub key: String,
}

#[async_trait::async_trait]
impl ArchiveBytes for StoredArchive {
    async fn bytes(&self) -> Result<Vec<u8>, ConnectorError> {
        let out = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .send()
            .await
            .map_err(|_| {
                ConnectorError::Config("The uploaded archive wasn't found. Upload it again.".into())
            })?;
        if out.content_length().unwrap_or(0).max(0) as u64 > MAX_ARCHIVE_BYTES {
            return Err(ConnectorError::Config("The archive is too large.".into()));
        }
        Ok(out
            .body
            .collect()
            .await
            .map_err(|_| ConnectorError::Temporary("The archive couldn't be read.".into()))?
            .into_bytes()
            .to_vec())
    }
}

/// The key of a source's current upload, from its options.
pub fn current_key(team_id: &str, src: &Source) -> Option<String> {
    let v = src.options["version"].as_u64().filter(|v| *v > 0)? as u32;
    Some(archive_key(team_id, &src.kb_id, &src.id, v))
}

#[cfg(test)]
pub mod testing {
    use std::io::Write;

    use super::*;

    /// A zip of `(path, content)` entries.
    pub fn zip_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut w = zip::ZipWriter::new(&mut buf);
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            for (p, c) in entries {
                if p.ends_with('/') {
                    w.add_directory(*p, opts).unwrap();
                } else {
                    w.start_file(*p, opts).unwrap();
                    w.write_all(c).unwrap();
                }
            }
            w.finish().unwrap();
        }
        buf.into_inner()
    }

    /// An archive held in memory, swappable for a newer upload.
    pub struct InMemory(pub Arc<std::sync::Mutex<Vec<u8>>>);

    #[async_trait::async_trait]
    impl ArchiveBytes for InMemory {
        async fn bytes(&self) -> Result<Vec<u8>, ConnectorError> {
            Ok(self.0.lock().unwrap().clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::testing::{zip_of, InMemory};
    use super::*;
    use crate::knowledge::search::testing::WordEmbedder;
    use crate::knowledge::sources::store::memory::MemSyncStore;
    use crate::knowledge::sources::store::SyncStore;
    use crate::knowledge::sources::{sync, Selected, Status};

    const NOTION_ID: &str = "0123456789abcdef0123456789abcdef";

    fn notion_export() -> Vec<u8> {
        let page = format!("Engineering {NOTION_ID}/Runbooks {NOTION_ID}/Deploy {NOTION_ID}.md");
        let table = format!("Engineering {NOTION_ID}/Services {NOTION_ID}.csv");
        zip_of(&[
            (&format!("Engineering {NOTION_ID}/"), b""),
            (&page, b"# Deploy\nRoll out region by region." as &[u8]),
            (&table, b"Service,Owner\nbilling,payments\n"),
            ("__MACOSX/._x.md", b"junk"),
            (".DS_Store", b"junk"),
            ("images/logo.png", b"png"),
        ])
    }

    #[test]
    fn exported_names_read_as_their_titles() {
        assert_eq!(clean_title(&format!("Deploy {NOTION_ID}.md")), "Deploy");
        assert_eq!(
            clean_title(&format!("Engineering {NOTION_ID}")),
            "Engineering"
        );
        assert_eq!(
            clean_title("Release-Checklist_123456789.html"),
            "Release Checklist"
        );
        assert_eq!(clean_title("guide.md"), "guide");
        assert_eq!(
            clean_title("v2_2026.md"),
            "v2_2026",
            "only Confluence's HTML names lose their ids"
        );
    }

    #[test]
    fn archives_are_checked_before_upload() {
        assert_eq!(check_archive("Export.zip", 10).unwrap(), "Export.zip");
        assert!(check_archive("export.tar.gz", 10)
            .unwrap_err()
            .contains(".zip"));
        assert!(check_archive("a.zip", 0).unwrap_err().contains("empty"));
        assert!(check_archive("a.zip", MAX_ARCHIVE_BYTES + 1)
            .unwrap_err()
            .contains("200 MB"));
        assert_eq!(
            archive_key("TEAM#a", "kb", "s", 2),
            "TEAM#a/knowledge/kb/archives/s/v2/archive.zip"
        );
        assert!(crate::knowledge::owns_key(
            "TEAM#a",
            &archive_key("TEAM#a", "kb", "s", 2)
        ));
        assert!(NOTE.contains("Full Export"));
    }

    #[test]
    fn a_notion_export_lists_its_pages_and_tables_only() {
        let l = entries(&notion_export(), 100).unwrap();
        let titles: Vec<&str> = l.items.iter().map(|i| i.title.as_str()).collect();
        assert_eq!(titles, ["Deploy", "Services"]);
        assert_eq!(l.items[0].path, "Engineering / Runbooks");
        assert!(l.items[0].version.contains('-'));
        assert!(l.complete && !l.truncated);
        let few = entries(&notion_export(), 1).unwrap();
        assert!(few.truncated);
        assert!(matches!(
            entries(b"not a zip", 10),
            Err(ConnectorError::Config(_))
        ));
    }

    #[test]
    fn an_entry_is_read_bounded_and_a_zip_bomb_is_refused() {
        let big = vec![b'a'; 1_000_000];
        let z = zip_of(&[("bomb.txt", &big), ("ok.txt", b"fine")]);
        assert_eq!(entry(&z, "ok.txt").unwrap(), b"fine");
        assert!(entry(&z, "bomb.txt")
            .unwrap_err()
            .to_string()
            .contains("far more"));
        assert!(matches!(
            entry(&z, "missing.txt"),
            Err(ConnectorError::NotFound(_))
        ));
    }

    fn source() -> Source {
        Source {
            id: "arch".into(),
            kb_id: "kb".into(),
            kind: Some(Kind::Archive),
            name: "Notion export".into(),
            selection: vec![Selected {
                id: "archive".into(),
                title: "Export.zip".into(),
                kind: "file".into(),
                ..Default::default()
            }],
            options: serde_json::json!({ "file_name": "Export.zip", "version": 1 }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_newer_export_keeps_what_is_unchanged_rereads_changes_and_removes_the_rest() {
        let held = Arc::new(Mutex::new(notion_export()));
        let c = Archive::new(Box::new(InMemory(held.clone())), 100);
        let store = MemSyncStore::default();
        store.put_source("t", &source()).await.unwrap();
        let ctx = sync::Ctx {
            store: &store,
            connector: &c,
            embedder: &WordEmbedder,
            team: "t",
            now: &|| 1_000,
            used: None,
            trigger: "manual",
            budget_secs: 0,
            token_room: None,
            limit: None,
        };
        let first = sync::run(&ctx, "kb", "arch").await;
        assert_eq!((first.fetched, first.indexed, first.failed), (2, 2, 0));
        let items = store.items("t", "kb", "arch").await.unwrap();
        assert!(items
            .iter()
            .any(|f| f.name == "Deploy" && f.path == "Engineering / Runbooks"));
        // A newer export: Deploy edited, Services gone, a new page, and a
        // copy of a page with the same text under another name.
        let page = format!("Engineering {NOTION_ID}/Runbooks {NOTION_ID}/Deploy {NOTION_ID}.md");
        *held.lock().unwrap() = zip_of(&[
            (&page, b"# Deploy\nRoll out one region at a time." as &[u8]),
            ("Onboarding.md", b"# Onboarding\nRead the handbook."),
        ]);
        // The next sync reads the new upload (a new connector per sync).
        let c = Archive::new(Box::new(InMemory(held.clone())), 100);
        let ctx = sync::Ctx {
            connector: &c,
            ..ctx
        };
        let second = sync::run(&ctx, "kb", "arch").await;
        assert_eq!(second.fetched, 2, "the edited page and the new one");
        assert_eq!(second.removed, 1, "the page the newer export no longer has");
        let items = store.items("t", "kb", "arch").await.unwrap();
        let names: std::collections::BTreeSet<&str> =
            items.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["Deploy", "Onboarding"].into_iter().collect());
        // The same export again: nothing fetched.
        let third = sync::run(&ctx, "kb", "arch").await;
        assert_eq!(third.fetched, 0);
        let s = store.source("t", "kb", "arch").await.unwrap().unwrap();
        assert_eq!(s.status, Status::Ready);
        assert_eq!(s.items, 2);
    }

    #[test]
    fn the_current_upload_is_found_from_the_source() {
        let s = source();
        assert_eq!(
            current_key("t", &s).unwrap(),
            "t/knowledge/kb/archives/arch/v1/archive.zip"
        );
        let none = Source {
            options: serde_json::json!({}),
            ..s
        };
        assert!(current_key("t", &none).is_none());
    }
}
