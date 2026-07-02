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

/// Immutable-ish snapshot of a repo at a ref, with write mirroring.
pub struct RepoSnapshot {
    files: RwLock<HashMap<String, FileEntry>>,
}

impl RepoSnapshot {
    /// Build a snapshot from raw `.tar.gz` bytes as returned by
    /// `GET /repos/{owner}/{repo}/tarball/{ref}`. The archive's single
    /// top-level directory prefix is stripped.
    pub fn from_tarball(
        bytes: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let decoder = GzDecoder::new(bytes);
        let mut archive = Archive::new(decoder);
        let mut files = HashMap::new();
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

        Ok(Self {
            files: RwLock::new(files),
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
        let terms: Vec<String> = query
            .split_whitespace()
            .map(|t| t.to_lowercase())
            .collect();
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
        results
    }

    /// Mirror a write that was committed through the GitHub API.
    pub async fn apply_write(&self, path: &str, content: &str) {
        self.files.write().await.insert(
            path.trim_start_matches('/').to_string(),
            FileEntry::Text(content.to_string()),
        );
    }

    /// Mirror a delete that was committed through the GitHub API.
    pub async fn apply_delete(&self, path: &str) {
        self.files.write().await.remove(path.trim_start_matches('/'));
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
            ("src/filter.ts", "const ageRangeMap = {\n  ZERO_TO_THREE: [0, 3],\n};\n"),
            ("src/other.ts", "export const unrelated = 1;\n"),
            ("README.md", "Age filter docs\n"),
        ]);
        let snap = RepoSnapshot::from_tarball(&tarball).unwrap();

        // Tree + prefix stripping
        assert_eq!(snap.tree().await, vec!["README.md", "src/filter.ts", "src/other.ts"]);

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
        snap.apply_write("src/filter.ts", "const renamedMap = {};\n").await;
        assert!(snap.search("ageRangeMap").await.is_empty());
        assert_eq!(snap.search("renamedMap").await.len(), 1);
        snap.apply_delete("src/other.ts").await;
        assert!(snap.read_file("src/other.ts").await.is_none());
        assert_eq!(snap.tree().await, vec!["README.md", "src/filter.ts"]);
    }
}
