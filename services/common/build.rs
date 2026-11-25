//! The version of what extracts the app map, for `app_map::EXTRACTOR_VERSION`.
//!
//! A map is kept until a repository's default branch moves, so a release
//! that changed what the extractor finds (a new detector, a new vendored
//! Floorplan, a new graph contract) left every map built before it showing
//! the old content. Each map now carries the version of the extractor that
//! made it, and a map made by another one is built again.
//!
//! The version is a hash of the sandbox's driver
//! (`worker/floorplan/driver.mjs`), CoderHelm's detectors
//! (`worker/floorplan/coderhelm/`, their tests left out) and the vendored
//! Floorplan's `VERSION`, so the gateway and the worker built from the same
//! code agree on it, and it changes exactly when one of those does.

use std::path::{Path, PathBuf};

/// Raised by hand when the map's contract changes in a way none of the
/// hashed files show (the worker's own handling of the graph). 2: screens a
/// route dispatches to by type, and journey steps that name their screen.
const CONTRACT: &str = "2";

fn files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            files(&p, out);
        } else {
            out.push(p);
        }
    }
}

/// FNV-1a, 64 bits: stable across toolchains, unlike the standard hasher.
fn fnv(hash: &mut u64, bytes: &[u8]) {
    for b in bytes {
        *hash ^= u64::from(*b);
        *hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
}

fn main() {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()));
    let floorplan = root.join("../worker/floorplan");
    let driver = floorplan.join("driver.mjs");
    let detectors = floorplan.join("coderhelm");
    let version = root.join("../../vendor/floorplan/VERSION");
    for p in [&driver, &detectors, &version] {
        println!("cargo:rerun-if-changed={}", p.display());
    }
    let mut all = vec![driver, version];
    let mut found = Vec::new();
    files(&detectors, &mut found);
    all.extend(found.into_iter().filter(|p| {
        let name = p
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();
        !name.ends_with(".test.mjs")
    }));
    // The same order everywhere: by path relative to the crate.
    let mut named: Vec<(String, PathBuf)> = all
        .into_iter()
        .map(|p| {
            let rel = p
                .strip_prefix(&root)
                .unwrap_or(&p)
                .to_string_lossy()
                .replace('\\', "/");
            (rel, p)
        })
        .collect();
    named.sort();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    fnv(&mut hash, CONTRACT.as_bytes());
    fnv(&mut hash, &[0]);
    for (rel, p) in &named {
        fnv(&mut hash, rel.as_bytes());
        fnv(&mut hash, &[0]);
        if let Ok(bytes) = std::fs::read(p) {
            // The same bytes on every checkout (no CRLF difference).
            let text: Vec<u8> = bytes.into_iter().filter(|b| *b != b'\r').collect();
            fnv(&mut hash, &text);
        }
        fnv(&mut hash, &[0]);
    }
    println!("cargo:rustc-env=CODERHELM_APP_MAP_EXTRACTOR={hash:016x}");
}
