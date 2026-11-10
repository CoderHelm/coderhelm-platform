//! The version of the code that checks a change, for `passes::checks_version`.
//!
//! A hold CoderHelm's own checks write into a pull request's description is
//! text: nothing ran the check again when the code that wrote it changed, so
//! a pull request held by a rule that was later fixed stayed held. Each such
//! line now carries the version of the code that wrote it, and the ready
//! sweep checks the pull request again when the running code differs.
//!
//! The version is a hash of the worker's and the common crate's sources, so
//! it is the same wherever the same code runs (hosted or a self-hosted
//! install) and changes whenever that code does, with no build step to
//! remember.

use std::path::{Path, PathBuf};

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
    let dirs = [root.join("src"), root.join("../common/src")];
    let mut all = Vec::new();
    for d in &dirs {
        println!("cargo:rerun-if-changed={}", d.display());
        files(d, &mut all);
    }
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
    for (rel, p) in &named {
        fnv(&mut hash, rel.as_bytes());
        fnv(&mut hash, &[0]);
        if let Ok(bytes) = std::fs::read(p) {
            fnv(&mut hash, &bytes);
        }
        fnv(&mut hash, &[0]);
    }
    println!("cargo:rustc-env=CODERHELM_CHECKS_VERSION={hash:016x}");
}
