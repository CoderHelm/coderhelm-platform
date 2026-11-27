//! The reads of GitHub this worker already made, kept so the same read costs
//! nothing the next time.
//!
//! A run's step reads the same things many times: its pull request, its
//! branch's ref, its CI, the files of a commit, from code that doesn't know
//! the others asked a moment ago (78 places read a pull request, 74 a ref).
//! A busy afternoon of runs on one installation spent GitHub's whole hourly
//! budget that way, twice in two hours, and every run waited for the reset.
//!
//! So every read the client sends (`github::send_tracked`) goes through here:
//!
//! - **A read asked before is asked conditionally**: with the validator
//!   (`ETag`) GitHub gave it, as `If-None-Match`. GitHub answers 304 when
//!   nothing changed, and a 304 isn't counted against the hourly budget; the
//!   caller gets the body kept from the first answer, exactly as if GitHub
//!   had sent it again. A read that changed comes back whole and replaces
//!   the one kept. Nothing is ever served without asking GitHub, so a read
//!   is never stale.
//! - **A read that can't change is not asked again at all**: a git object,
//!   a commit, a comparison or a file named by its full commit SHA
//!   ([`immutable`]). These are kept for as long as the process lives (and
//!   room allows).
//!
//! Reads are kept per installation, URL and media type, in this process, up
//! to [`MAX_BYTES`] in all and [`MAX_ENTRY_BYTES`] each (a tarball or a log
//! is never kept). The oldest go first.

use std::collections::{HashMap, VecDeque};
use std::sync::{LazyLock, Mutex};

/// The most a process keeps, in bytes of bodies.
pub const MAX_BYTES: usize = 48 * 1024 * 1024;
/// The largest body kept.
pub const MAX_ENTRY_BYTES: usize = 1024 * 1024;

/// One read as GitHub answered it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kept {
    /// The validator GitHub gave it (empty for an immutable read kept
    /// without one).
    pub etag: String,
    /// The headers a caller reads from a response: type, paging, validator.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Kept {
    /// The read as a response, for the caller that asked for it.
    pub fn response(&self, url: &str) -> Option<reqwest::Response> {
        use reqwest::ResponseBuilderExt;
        let mut b = http::Response::builder()
            .status(200)
            .url(reqwest::Url::parse(url).ok()?);
        for (k, v) in &self.headers {
            b = b.header(k.as_str(), v.as_str());
        }
        Some(reqwest::Response::from(b.body(self.body.clone()).ok()?))
    }
}

/// The headers kept with a read.
pub const KEPT_HEADERS: &[&str] = &["content-type", "etag", "link"];

#[derive(Default)]
struct Store {
    reads: HashMap<String, Kept>,
    order: VecDeque<String>,
    bytes: usize,
}

static STORE: LazyLock<Mutex<Store>> = LazyLock::new(|| Mutex::new(Store::default()));

/// The key a read is kept under.
pub fn key(installation_id: u64, url: &str, accept: &str) -> String {
    format!("{installation_id} {accept} {url}")
}

/// The read kept under `key`.
pub fn get(key: &str) -> Option<Kept> {
    STORE.lock().ok()?.reads.get(key).cloned()
}

impl Store {
    fn put(&mut self, key: &str, kept: Kept, max_bytes: usize) {
        if kept.body.len() > MAX_ENTRY_BYTES {
            return;
        }
        let size = kept.body.len();
        if let Some(old) = self.reads.insert(key.to_string(), kept) {
            self.bytes -= old.body.len();
            self.order.retain(|k| k != key);
        }
        self.bytes += size;
        self.order.push_back(key.to_string());
        while self.bytes > max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(old) = self.reads.remove(&oldest) {
                self.bytes -= old.body.len();
            }
        }
    }
}

/// Keeps a read (replacing the one kept under `key`), dropping the oldest
/// while the store is over [`MAX_BYTES`]. A body over [`MAX_ENTRY_BYTES`]
/// isn't kept.
pub fn put(key: &str, kept: Kept) {
    if let Ok(mut s) = STORE.lock() {
        s.put(key, kept, MAX_BYTES);
    }
}

fn is_sha(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Whether a read of `url` answers the same forever: a git blob, tree or
/// commit, a commit, a comparison of two commits, or a file's contents, each
/// named by full commit SHA. Anything named by a branch, a tag, a number or
/// a short SHA can change, and is asked again conditionally instead.
pub fn immutable(url: &str) -> bool {
    let Ok(u) = reqwest::Url::parse(url) else {
        return false;
    };
    let segs: Vec<&str> = u.path_segments().map(|s| s.collect()).unwrap_or_default();
    // /repos/{owner}/{repo}/...
    let [repos, _, _, rest @ ..] = segs.as_slice() else {
        return false;
    };
    if *repos != "repos" {
        return false;
    }
    let query_ref = || {
        u.query_pairs()
            .find(|(k, _)| k == "ref")
            .is_some_and(|(_, v)| is_sha(&v))
    };
    match rest {
        ["git", "blobs" | "trees" | "commits", sha] => is_sha(sha),
        ["commits", sha] => is_sha(sha),
        ["compare", range] => range
            .split_once("...")
            .or_else(|| range.split_once(".."))
            .is_some_and(|(a, b)| is_sha(a) && is_sha(b)),
        ["contents", ..] => query_ref(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kept(n: usize) -> Kept {
        Kept {
            etag: "\"e\"".into(),
            headers: vec![("content-type".into(), "application/json".into())],
            body: vec![b'x'; n],
        }
    }

    #[test]
    fn only_reads_named_by_a_full_sha_never_change() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let other = "89abcdef0123456789abcdef0123456789abcdef";
        let base = "https://api.github.com/repos/o/r";
        for url in [
            format!("{base}/git/blobs/{sha}"),
            format!("{base}/git/trees/{sha}?recursive=1"),
            format!("{base}/git/commits/{sha}"),
            format!("{base}/commits/{sha}"),
            format!("{base}/compare/{sha}...{other}"),
            format!("{base}/compare/{sha}..{other}?per_page=100"),
            format!("{base}/contents/apps/web/a.ts?ref={sha}"),
        ] {
            assert!(immutable(&url), "{url}");
        }
        for url in [
            format!("{base}/git/ref/heads/main"),
            format!("{base}/git/trees/main"),
            format!("{base}/commits/main"),
            format!("{base}/commits/{sha}/check-runs"),
            format!("{base}/commits/{sha}/status"),
            format!("{base}/commits/{sha}/pulls"),
            format!("{base}/compare/main...{sha}"),
            format!("{base}/compare/{sha}...feature"),
            format!("{base}/contents/a.ts?ref=main"),
            format!("{base}/contents/a.ts"),
            format!("{base}/pulls/7"),
            format!("{base}/commits/0123456"),
            "https://api.github.com/graphql".to_string(),
            "not a url".to_string(),
        ] {
            assert!(!immutable(&url), "{url}");
        }
    }

    #[test]
    fn reads_are_kept_by_installation_url_and_media_type() {
        let a = key(1, "https://api.github.com/repos/o/r/pulls/7", "json");
        let b = key(2, "https://api.github.com/repos/o/r/pulls/7", "json");
        let c = key(1, "https://api.github.com/repos/o/r/pulls/7", "diff");
        assert_ne!(a, b);
        assert_ne!(a, c);
        put(&a, kept(3));
        assert_eq!(get(&a).unwrap().body.len(), 3);
        assert!(get(&b).is_none());
        // A newer answer replaces the one kept.
        put(&a, kept(5));
        assert_eq!(get(&a).unwrap().body.len(), 5);
    }

    #[test]
    fn a_large_body_is_never_kept_and_the_store_stays_bounded() {
        let big = key(3, "https://api.github.com/repos/o/r/tarball/x", "json");
        put(&big, kept(MAX_ENTRY_BYTES + 1));
        assert!(get(&big).is_none());
        // The bound, on a store of its own (tests share the process's).
        let mut s = Store::default();
        let max = 4 * MAX_ENTRY_BYTES;
        for i in 0..10 {
            s.put(&format!("k{i}"), kept(MAX_ENTRY_BYTES), max);
            assert!(s.bytes <= max);
        }
        assert!(!s.reads.contains_key("k0"), "the oldest went first");
        assert!(s.reads.contains_key("k9"));
        assert_eq!(s.reads.len(), 4);
        assert_eq!(s.order.len(), 4);
    }

    #[tokio::test]
    async fn a_kept_read_answers_as_the_response_it_was() {
        let k = Kept {
            etag: "\"v1\"".into(),
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("link".into(), "<https://x/?page=2>; rel=\"next\"".into()),
            ],
            body: br#"{"state":"open"}"#.to_vec(),
        };
        let url = "https://api.github.com/repos/o/r/pulls/7";
        let r = k.response(url).unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.url().as_str(), url);
        assert!(r.headers().get("link").is_some());
        let v: serde_json::Value = r.json().await.unwrap();
        assert_eq!(v["state"], "open");
    }
}
