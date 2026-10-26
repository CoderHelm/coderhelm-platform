//! No hardcoded hosted addresses in the product.
//!
//! A self-hosted install links to itself: its dashboard, API, stream and
//! sign-in come from `public_urls.rs` (APP_URL, API_URL or the SSM parameter).
//! This scans the code that ships (services, lambdas, infra, installer) for
//! the hosted service's own hosts (`app.`, `api.`, `stream.`, `auth.`
//! coderhelm.com) and its marketing site, and fails on any outside the files
//! below, each kept for a stated reason. Comments, Rust test modules and test
//! files are not scanned. Public docs (docs.coderhelm.com) are always fine.

use std::path::{Path, PathBuf};

/// Files that may name a hosted address, and why.
const ALLOWED: &[(&str, &str)] = &[
    ("services/common/src/public_urls.rs", "the one place the hosted defaults live, used only when the deployment is not self-hosted"),
    ("services/common/src/hybrid/mod.rs", "hybrid: the hosted API an install talks to once an admin turns hybrid on"),
    ("services/gateway/src/routes/hybrid.rs", "hybrid: the hosted side's fallback page for {name}.coderhelm.com"),
    ("services/gateway/src/routes/hybrid_install.rs", "hybrid: tells an install it can't reach the hosted API"),
    ("services/gateway/src/openapi.rs", "the committed API document names the hosted server; a self-hosted install serves its own (served_document)"),
    ("services/common/src/mcp_remote/http.rs", "the User-Agent names the product; not a link anyone opens"),
    ("infra/lib/config.ts", "hybrid: the hosted API's default URL"),
    ("infra/lib/service-env.ts", "hybrid: the hosted API's default URL"),
    ("installer/lib/hybrid.mjs", "hybrid: the hosted API's default URL"),
    ("installer/lib/util.mjs", "telemetry endpoint, off unless the admin turns it on"),
];

/// Where shipped code lives (relative to the repository root).
const ROOTS: &[&str] = &[
    "services",
    "lambda",
    "infra/lib",
    "infra/bin",
    "installer/lib",
    "installer/bin",
    "selfhost",
];

const EXTS: &[&str] = &["rs", "ts", "mjs", "js", "py", "sh", "yaml", "yml", "html"];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        let name = e.file_name().to_string_lossy().to_string();
        if p.is_dir() {
            if matches!(
                name.as_str(),
                "target" | "node_modules" | "test" | "tests" | "__pycache__" | "cdk.out" | "golden"
            ) {
                continue;
            }
            files(&p, out);
        } else if p
            .extension()
            .is_some_and(|x| EXTS.contains(&x.to_string_lossy().as_ref()))
            && !name.contains(".test.")
            && !name.starts_with("test_")
        {
            out.push(p);
        }
    }
}

/// A hosted address in this line, if any.
fn hosted(line: &str) -> Option<&'static str> {
    let l = line.to_ascii_lowercase();
    [
        "app.coderhelm.com",
        "api.coderhelm.com",
        "stream.coderhelm.com",
        "auth.coderhelm.com",
        "://coderhelm.com",
        "://www.coderhelm.com",
    ]
    .into_iter()
    .find(|h| {
        l.match_indices(h).any(|(i, _)| {
            // `x.app.coderhelm.com` is someone's own name, not the hosted app.
            h.starts_with("://")
                || i == 0
                || !l.as_bytes()[i - 1].is_ascii_alphanumeric()
                    && l.as_bytes()[i - 1] != b'.'
                    && l.as_bytes()[i - 1] != b'-'
        })
    })
}

fn is_comment(t: &str) -> bool {
    t.starts_with("//")
        || t.starts_with('#') && !t.starts_with("#[")
        || t.starts_with("* ")
        || t == "*"
        || t.starts_with("/*")
}

#[test]
fn no_hardcoded_hosted_addresses_outside_the_allowed_files() {
    let root = repo_root();
    let mut all = Vec::new();
    for r in ROOTS {
        files(&root.join(r), &mut all);
    }
    assert!(
        all.len() > 50,
        "the scan found the code ({} files)",
        all.len()
    );
    let mut found = Vec::new();
    for p in &all {
        let rel = p
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if ALLOWED.iter().any(|(f, _)| *f == rel) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(p) else {
            continue;
        };
        for (n, line) in text.lines().enumerate() {
            if rel.ends_with(".rs") && line.starts_with("#[cfg(test)]") {
                break;
            }
            let t = line.trim();
            if is_comment(t) {
                continue;
            }
            if let Some(h) = hosted(line) {
                found.push(format!("{rel}:{}: {h}: {}", n + 1, t));
            }
        }
    }
    assert!(
        found.is_empty(),
        "Hardcoded hosted addresses. Build the install's links from common::public_urls (app_url, api_url), or add the file to ALLOWED with the reason it must name the hosted service:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_scan_catches_what_it_should() {
    assert_eq!(
        hosted(r#"let u = "https://app.coderhelm.com/runs";"#),
        Some("app.coderhelm.com")
    );
    assert_eq!(
        hosted("API_URL=https://api.coderhelm.com"),
        Some("api.coderhelm.com")
    );
    assert_eq!(
        hosted("see https://coderhelm.com/privacy"),
        Some("://coderhelm.com")
    );
    assert_eq!(hosted("https://docs.coderhelm.com/self-host/"), None);
    assert_eq!(hosted("https://acme.app.coderhelm.com"), None);
    assert!(is_comment("// https://app.coderhelm.com"));
    assert!(!is_comment("#[derive(Debug)]"));
}
