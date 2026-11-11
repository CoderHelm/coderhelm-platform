//! Reads a security fix's upgrade notes before its run starts (the rules
//! are `common::upgrade_notes`): the package's registry for its source
//! repository, that repository's GitHub releases or changelog for the
//! versions between the current and the fixed one, and this repository's
//! uses of the APIs the breaking changes name. Bounded: at most
//! `MAX_PACKAGES` packages, a short timeout per read, and a read that fails
//! is said in the notes, never a reason the run doesn't start.

use std::time::{Duration, Instant};

use common::security_fixes::FixUnit;
use common::upgrade_notes::{self as un, PackageNotes};
use serde_json::Value;
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// Time all of one unit's reads may take.
const BUDGET: Duration = Duration::from_secs(60);
/// One registry read.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

async fn registry(state: &WorkerState, url: &str) -> Result<Value, String> {
    let r = state
        .http
        .get(url)
        .header("Accept", "application/json")
        .timeout(READ_TIMEOUT)
        .send()
        .await
        .map_err(|e| format!("the registry couldn't be read ({e})"))?;
    if !r.status().is_success() {
        return Err(format!("the registry answered {}", r.status().as_u16()));
    }
    r.json::<Value>()
        .await
        .map_err(|e| format!("the registry's answer couldn't be read ({e})"))
}

/// The notes for one package: releases first, then a changelog file.
async fn read_one(state: &WorkerState, gh: &GitHubClient, n: &mut PackageNotes) {
    let meta = match un::registry_url(&n.ecosystem, &n.package, &n.to) {
        Some(url) => match registry(state, &url).await {
            Ok(v) => v,
            Err(why) => {
                n.missing = why;
                return;
            }
        },
        None => Value::Null,
    };
    let Some(src) = un::source_repo(&n.ecosystem, &n.package, &meta) else {
        n.missing = if meta.is_null() && n.ecosystem != "Go" {
            format!("CoderHelm doesn't read {} release notes", n.ecosystem)
        } else {
            "the registry names no GitHub repository".to_string()
        };
        return;
    };
    let full = format!("{}/{}", src.owner, src.name);
    match gh
        .get_api(&format!("/repos/{full}/releases?per_page=100"))
        .await
    {
        Ok(v) => {
            let releases = v.as_array().cloned().unwrap_or_default();
            let read = un::releases_between(&releases, &n.package, &n.from, &n.to);
            if !read.is_empty() {
                un::fill(
                    n,
                    &format!("the GitHub releases of {full}"),
                    &format!("https://github.com/{full}/releases"),
                    &read,
                );
                return;
            }
        }
        Err(e) => warn!(repo = %full, error = %e, "upgrade notes: releases couldn't be read"),
    }
    let branch = gh
        .get_default_branch(&src.owner, &src.name)
        .await
        .unwrap_or_else(|_| "HEAD".into());
    let mut dirs: Vec<String> = vec![];
    if let Some(d) = &src.dir {
        dirs.push(format!("{d}/"));
    }
    dirs.push(String::new());
    for dir in &dirs {
        for name in un::CHANGELOG_FILES {
            let path = format!("{dir}{name}");
            let Ok(Some(text)) = gh
                .read_file_if_exists(&src.owner, &src.name, &path, &branch)
                .await
            else {
                continue;
            };
            let read = un::changelog_between(&text, &n.from, &n.to);
            un::fill(
                n,
                &format!("`{path}` of {full}"),
                &format!("https://github.com/{full}/blob/{branch}/{path}"),
                &read,
            );
            return;
        }
    }
    n.missing = format!("{full} has no GitHub releases or changelog file for these versions");
}

/// Where this repository uses the APIs the breaking changes name.
async fn find_call_sites(gh: &GitHubClient, repo: &str, base: &str, n: &mut PackageNotes) {
    let Some((owner, name)) = repo.split_once('/') else {
        return;
    };
    let mut sites = vec![];
    for api in n.apis.clone() {
        let term = un::search_term(&api);
        let Ok(results) = gh.search_code(owner, name, base, &term).await else {
            continue;
        };
        let pairs: Vec<(String, Vec<String>)> =
            results.into_iter().map(|r| (r.path, r.matches)).collect();
        for c in un::call_sites(&api, &pairs) {
            if !sites.contains(&c) {
                sites.push(c);
            }
        }
        if sites.len() >= un::MAX_CALL_SITES {
            break;
        }
    }
    sites.truncate(un::MAX_CALL_SITES);
    n.call_sites = sites;
}

/// The notes for a unit's bumps, read now.
pub async fn read(
    state: &WorkerState,
    gh: &GitHubClient,
    unit: &FixUnit,
    base: &str,
) -> Vec<PackageNotes> {
    let started = Instant::now();
    let Some((owner, name)) = unit.repo.split_once('/') else {
        return vec![];
    };
    let tree: Vec<String> = gh
        .get_tree(owner, name, base)
        .await
        .map(|t| {
            t.into_iter()
                .filter(|e| e.entry_type == "blob")
                .map(|e| e.path)
                .collect()
        })
        .unwrap_or_default();
    let tools = un::lock_tools(&tree);
    let mut out = vec![];
    for b in unit.bumps.iter().take(un::MAX_PACKAGES) {
        let mut n = PackageNotes::for_bump(b, &unit.ecosystem);
        if let Some(t) = un::lock_tool_for(&tools, &b.manifest, &unit.ecosystem) {
            n.lockfile = t.lockfile.clone();
            n.lock_command = t.command.clone();
        }
        if started.elapsed() >= BUDGET {
            n.missing = "not read: the time for reading notes ran out".into();
        } else {
            read_one(state, gh, &mut n).await;
            if !n.apis.is_empty() && started.elapsed() < BUDGET {
                find_call_sites(gh, &unit.repo, base, &mut n).await;
            }
        }
        out.push(n);
    }
    for b in unit.bumps.iter().skip(un::MAX_PACKAGES) {
        let mut n = PackageNotes::for_bump(b, &unit.ecosystem);
        n.missing = format!(
            "not read: notes are read for the first {} packages of a pull request",
            un::MAX_PACKAGES
        );
        out.push(n);
    }
    info!(
        repo = %unit.repo,
        packages = out.len(),
        read = out.iter().filter(|n| n.read()).count(),
        breaking = out.iter().map(|n| n.breaking.len()).sum::<usize>(),
        call_sites = out.iter().map(|n| n.call_sites.len()).sum::<usize>(),
        secs = started.elapsed().as_secs(),
        "security: upgrade notes read"
    );
    out
}

/// The notes kept on a ticket record (`notes`), with `more` added (a
/// bundle's joined bumps replace their package's earlier notes).
pub fn merged(kept: &str, more: &[PackageNotes]) -> Vec<PackageNotes> {
    let mut all: Vec<PackageNotes> = serde_json::from_str(kept).unwrap_or_default();
    for n in more {
        all.retain(|k| !k.package.eq_ignore_ascii_case(&n.package));
        all.push(n.clone());
    }
    all
}
