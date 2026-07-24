//! In-memory repository snapshot built from a GitHub tarball download.
//!
//! One tarball request (a core-API call, not code-search quota) replaces the
//! GitHub Code Search API for `search_code` and serves tree/list/read tools
//! locally. This fixes three problems with API-backed search:
//! - code search is limited to 10 requests/min, which agents burn in seconds
//! - code search only indexes the DEFAULT branch, so results never reflect
//!   the working branch or the agent's own commits
//! - the index lags pushes, so fresh code is invisible
//!
//! Writes made through the GitHub API are mirrored into the snapshot via
//! `apply_write`/`apply_delete` so subsequent searches and reads stay coherent.

use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::io::Read;
use tar::Archive;
use tokio::sync::RwLock;

/// Per-file size cap — files larger than this keep their path in the tree but
/// content is not stored (reads fall back to the GitHub API).
const MAX_FILE_BYTES: usize = 512 * 1024;
/// Total stored-content cap; beyond this, remaining files are path-only.
const MAX_TOTAL_BYTES: usize = 256 * 1024 * 1024;
/// Search result caps to keep tool output token-bounded.
const MAX_RESULT_FILES: usize = 30;
const MAX_MATCHES_PER_FILE: usize = 5;
const MAX_FRAGMENT_LEN: usize = 300;

/// A single search hit within a file.
pub struct SnapshotMatch {
    pub path: String,
    /// "line_no: line text" fragments.
    pub fragments: Vec<String>,
}

enum FileEntry {
    /// UTF-8 text content.
    Text(String),
    /// Binary or oversized — path is listed but content is not held.
    Unindexed,
}

/// Extensions worth flagging when oversized: a search miss on one of these is
/// probably "the file wasn't indexed", not "the symbol doesn't exist". The
/// motivating case is a 4MB generated `sanity.types.ts` (> MAX_FILE_BYTES) that
/// search silently skips, so the agent searches a generated type in vain.
const SEARCHABLE_EXTS: &[&str] = &[
    "ts", "tsx", "js", "jsx", "mjs", "cjs", "json", "rs", "go", "py", "java", "kt", "rb", "php",
    "cs", "swift", "css", "scss", "less", "html", "vue", "svelte", "graphql", "gql", "sql", "yaml",
    "yml", "toml", "md",
];

fn is_searchable_ext(path: &str) -> bool {
    path.rsplit_once('.')
        .map(|(_, ext)| SEARCHABLE_EXTS.contains(&ext.to_ascii_lowercase().as_str()))
        .unwrap_or(false)
}

/// Immutable-ish snapshot of a repo at a ref, with write mirroring.
pub struct RepoSnapshot {
    files: RwLock<HashMap<String, FileEntry>>,
    /// Paths of code/data files too large to index (content-searching skipped).
    /// Surfaced by `search` so the agent reads them directly instead of
    /// concluding the symbol is absent.
    oversized: RwLock<Vec<String>>,
}

impl RepoSnapshot {
    /// Build a snapshot from raw `.tar.gz` bytes as returned by
    /// `GET /repos/{owner}/{repo}/tarball/{ref}`. The archive's single
    /// top-level directory prefix is stripped.
    pub fn from_tarball(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let decoder = GzDecoder::new(bytes);
        let mut archive = Archive::new(decoder);
        let mut files = HashMap::new();
        let mut oversized = Vec::new();
        let mut total_bytes = 0usize;

        for entry in archive.entries()? {
            let mut entry = entry?;
            if !entry.header().entry_type().is_file() {
                continue;
            }
            let path = entry.path()?;
            // Strip the "{owner}-{repo}-{sha}/" prefix GitHub adds.
            let rel: String = {
                let mut comps = path.components();
                comps.next();
                comps.as_path().to_string_lossy().to_string()
            };
            if rel.is_empty() {
                continue;
            }
            let size = entry.header().size()? as usize;
            if size > MAX_FILE_BYTES || total_bytes + size > MAX_TOTAL_BYTES {
                // A large code/data file (e.g. generated types) isn't searched —
                // remember it so `search` can point the agent at read_file.
                if size > MAX_FILE_BYTES && is_searchable_ext(&rel) {
                    oversized.push(rel.clone());
                }
                files.insert(rel, FileEntry::Unindexed);
                continue;
            }
            let mut buf = Vec::with_capacity(size);
            entry.read_to_end(&mut buf)?;
            match String::from_utf8(buf) {
                Ok(text) => {
                    total_bytes += text.len();
                    files.insert(rel, FileEntry::Text(text));
                }
                Err(_) => {
                    files.insert(rel, FileEntry::Unindexed);
                }
            }
        }

        oversized.sort();
        Ok(Self {
            files: RwLock::new(files),
            oversized: RwLock::new(oversized),
        })
    }

    /// All file paths (text and unindexed), sorted.
    pub async fn tree(&self) -> Vec<String> {
        let files = self.files.read().await;
        let mut paths: Vec<String> = files.keys().cloned().collect();
        paths.sort();
        paths
    }

    /// Immediate children of a directory, formatted as "dir: name" / "file: name".
    /// Returns None if no entry lives under the path.
    pub async fn list_directory(&self, dir: &str) -> Option<Vec<String>> {
        let prefix = if dir.is_empty() || dir == "." || dir == "/" {
            String::new()
        } else {
            format!("{}/", dir.trim_matches('/'))
        };
        let files = self.files.read().await;
        let mut dirs = std::collections::BTreeSet::new();
        let mut leaf_files = std::collections::BTreeSet::new();
        for path in files.keys() {
            let Some(rest) = path.strip_prefix(&prefix) else {
                continue;
            };
            match rest.split_once('/') {
                Some((child_dir, _)) => {
                    dirs.insert(child_dir.to_string());
                }
                None => {
                    leaf_files.insert(rest.to_string());
                }
            }
        }
        if dirs.is_empty() && leaf_files.is_empty() {
            return None;
        }
        let mut out: Vec<String> = dirs.into_iter().map(|d| format!("dir: {d}")).collect();
        out.extend(leaf_files.into_iter().map(|f| format!("file: {f}")));
        Some(out)
    }

    /// Full text content, if the file is indexed.
    pub async fn read_file(&self, path: &str) -> Option<String> {
        let files = self.files.read().await;
        match files.get(path.trim_start_matches('/')) {
            Some(FileEntry::Text(content)) => Some(content.clone()),
            _ => None,
        }
    }

    /// Case-insensitive multi-term search. A line matches when it contains
    /// every whitespace-separated term (mirrors GitHub code search AND
    /// semantics that the pass prompts were written against).
    pub async fn search(&self, query: &str) -> Vec<SnapshotMatch> {
        let terms: Vec<String> = query.split_whitespace().map(|t| t.to_lowercase()).collect();
        if terms.is_empty() {
            return Vec::new();
        }
        let files = self.files.read().await;
        let mut results = Vec::new();
        // Deterministic order so repeated searches are stable.
        let mut paths: Vec<&String> = files.keys().collect();
        paths.sort();
        for path in paths {
            let Some(FileEntry::Text(content)) = files.get(path) else {
                continue;
            };
            let mut fragments = Vec::new();
            for (idx, line) in content.lines().enumerate() {
                let lower = line.to_lowercase();
                if terms.iter().all(|t| lower.contains(t)) {
                    let trimmed = line.trim();
                    let fragment = if trimmed.len() > MAX_FRAGMENT_LEN {
                        &trimmed[..trimmed
                            .char_indices()
                            .take_while(|(i, _)| *i < MAX_FRAGMENT_LEN)
                            .last()
                            .map(|(i, c)| i + c.len_utf8())
                            .unwrap_or(0)]
                    } else {
                        trimmed
                    };
                    fragments.push(format!("{}: {}", idx + 1, fragment));
                    if fragments.len() >= MAX_MATCHES_PER_FILE {
                        break;
                    }
                }
            }
            if !fragments.is_empty() {
                results.push(SnapshotMatch {
                    path: path.clone(),
                    fragments,
                });
                if results.len() >= MAX_RESULT_FILES {
                    break;
                }
            }
        }

        // Surface large code/data files that weren't content-searched, so a miss
        // reads as "not indexed" (read_file it) rather than "symbol absent".
        let oversized = self.oversized.read().await;
        if !oversized.is_empty() {
            let listed: Vec<String> = oversized.iter().take(10).cloned().collect();
            let more = oversized.len().saturating_sub(listed.len());
            let mut note = format!(
                "NOTE: {} large file(s) were NOT content-searched (too big to index). If you \
                 expected a match in one, use read_file / read_file_lines to read it directly: {}",
                oversized.len(),
                listed.join(", ")
            );
            if more > 0 {
                note.push_str(&format!(" (+{more} more)"));
            }
            results.push(SnapshotMatch {
                path: "(unsearched large files)".to_string(),
                fragments: vec![note],
            });
        }
        results
    }

    /// Mirror a write that was committed through the GitHub API.
    pub async fn apply_write(&self, path: &str, content: &str) {
        let key = path.trim_start_matches('/').to_string();
        // A file rewritten through the API is now held as text and searchable,
        // so drop any stale "oversized/unsearched" flag for it.
        self.oversized.write().await.retain(|p| p != &key);
        self.files
            .write()
            .await
            .insert(key, FileEntry::Text(content.to_string()));
    }

    /// Mirror a delete that was committed through the GitHub API.
    pub async fn apply_delete(&self, path: &str) {
        let key = path.trim_start_matches('/').to_string();
        self.oversized.write().await.retain(|p| p != &key);
        self.files.write().await.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;

    fn make_tarball(files: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::fast()));
        for (path, content) in files {
            let full = format!("owner-repo-abc123/{path}");
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, &full, content.as_bytes())
                .unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    #[tokio::test]
    async fn search_and_reads() {
        let tarball = make_tarball(&[
            (
                "src/filter.ts",
                "const ageRangeMap = {\n  ZERO_TO_THREE: [0, 3],\n};\n",
            ),
            ("src/other.ts", "export const unrelated = 1;\n"),
            ("README.md", "Age filter docs\n"),
        ]);
        let snap = RepoSnapshot::from_tarball(&tarball).unwrap();

        // Tree + prefix stripping
        assert_eq!(
            snap.tree().await,
            vec!["README.md", "src/filter.ts", "src/other.ts"]
        );

        // Single-term search with line numbers
        let hits = snap.search("ageRangeMap").await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, "src/filter.ts");
        assert_eq!(hits[0].fragments, vec!["1: const ageRangeMap = {"]);

        // Multi-term AND, case-insensitive
        let hits = snap.search("age FILTER").await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, "README.md");

        // list_directory
        let root = snap.list_directory("").await.unwrap();
        assert_eq!(root, vec!["dir: src", "file: README.md"]);
        let src = snap.list_directory("src").await.unwrap();
        assert_eq!(src, vec!["file: filter.ts", "file: other.ts"]);
        assert!(snap.list_directory("nope").await.is_none());

        // Write coherence
        snap.apply_write("src/filter.ts", "const renamedMap = {};\n")
            .await;
        assert!(snap.search("ageRangeMap").await.is_empty());
        assert_eq!(snap.search("renamedMap").await.len(), 1);
        snap.apply_delete("src/other.ts").await;
        assert!(snap.read_file("src/other.ts").await.is_none());
        assert_eq!(snap.tree().await, vec!["README.md", "src/filter.ts"]);
    }

    #[tokio::test]
    async fn oversized_text_file_is_surfaced_in_search() {
        // A >512KB .ts file (like a generated sanity.types.ts) is not indexed;
        // search must still tell the agent it exists so it reads it directly.
        let big = format!("export type Generated = {};\n", "x".repeat(600 * 1024));
        let tarball = make_tarball(&[
            ("src/app.ts", "import { thing } from './x';\n"),
            ("src/lib/sanity.types.ts", &big),
            ("assets/logo.bin", &"0".repeat(600 * 1024)), // oversized but not searchable ext
        ]);
        let snap = RepoSnapshot::from_tarball(&tarball).unwrap();

        // Searching a generated type returns the "unsearched large files" note
        // (which names the .ts, not the .bin).
        let hits = snap.search("Generated").await;
        let note = hits.iter().find(|m| m.path == "(unsearched large files)");
        assert!(note.is_some(), "expected an unsearched-large-files note");
        let text = &note.unwrap().fragments[0];
        assert!(text.contains("src/lib/sanity.types.ts"));
        assert!(!text.contains("logo.bin")); // non-code ext not flagged

        // After the file is rewritten via the API it's indexed — no stale note.
        snap.apply_write(
            "src/lib/sanity.types.ts",
            "export type Generated = number;\n",
        )
        .await;
        let hits = snap.search("Generated").await;
        assert!(hits.iter().all(|m| m.path != "(unsearched large files)"));
        assert!(hits.iter().any(|m| m.path == "src/lib/sanity.types.ts"));
    }
}
