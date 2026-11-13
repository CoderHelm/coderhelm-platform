//! Reads a security fix's upgrade notes before its run starts (the rules
//! are `common::upgrade_notes`): the package's registry for its source
//! repository, that repository's GitHub releases or changelog for the
//! versions between the current and the fixed one, and this repository's
//! uses of the APIs the breaking changes name. Bounded: at most
//! `MAX_PACKAGES` packages, a short timeout per read, and a read that fails
//! is said in the notes, never a reason the run doesn't start.
//!
//! Bumps that cross a major version are read first, so a large bundle never
//! spends its reads on patch bumps and leaves a major unread. For a major of
//! a package that is only pulled in by others, the parents' registry records
//! are read too: the range each declares for the package, installed and
//! newest (`common::upgrade_notes::ParentSupport`).

use std::time::{Duration, Instant};

use common::security_fixes::{self as sec, Dep, Finding, FindingState, FixUnit};
use common::upgrade_notes::{self as un, PackageNotes, ParentSupport};
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

/// The parents of a bump's package among its findings (the package before
/// it on each dependency path), and whether any finding has it as a direct
/// dependency. Each parent with its installed version when the dependency
/// list holds exactly one.
pub fn parents_of(
    b: &sec::Bump,
    members: &[(&Finding, &FindingState)],
    deps: &[Dep],
) -> (bool, Vec<(String, String)>) {
    let mine: Vec<&Finding> = members
        .iter()
        .map(|(f, _)| *f)
        .filter(|f| sec::in_bump(b, f))
        .collect();
    let direct = mine
        .iter()
        .any(|f| f.relation == sec::Relation::Direct || f.path.len() == 1);
    let mut parents: Vec<(String, String)> = vec![];
    for f in &mine {
        let [.., parent, _] = f.path.as_slice() else {
            continue;
        };
        if parents.iter().any(|(p, _)| p == parent) {
            continue;
        }
        let mut versions: Vec<&str> = deps
            .iter()
            .filter(|d| d.name == *parent && d.ecosystem.eq_ignore_ascii_case(&f.ecosystem))
            .map(|d| d.version.as_str())
            .collect();
        versions.sort();
        versions.dedup();
        let version = match versions.as_slice() {
            [one] => one.to_string(),
            _ => String::new(),
        };
        parents.push((parent.clone(), version));
        if parents.len() >= un::MAX_PARENTS {
            break;
        }
    }
    (direct, parents)
}

/// What one parent declares for the package: its installed version's
/// range and its newest release's.
async fn read_parent(
    state: &WorkerState,
    ecosystem: &str,
    package: &str,
    to: &str,
    parent: &str,
    installed: &str,
) -> ParentSupport {
    let mut p = ParentSupport {
        parent: parent.to_string(),
        version: installed.to_string(),
        ..Default::default()
    };
    if un::deps_url(ecosystem, parent, None).is_none()
        && un::deps_url(ecosystem, parent, Some("0")).is_none()
    {
        p.missing = format!("CoderHelm doesn't read {ecosystem} dependency ranges");
        return p;
    }
    if !installed.is_empty() {
        if let Some(url) = un::deps_url(ecosystem, parent, Some(installed)) {
            match registry(state, &url).await {
                Ok(meta) => {
                    if let Some(range) = un::declared_range(ecosystem, &meta, package) {
                        p.admits = un::range_admits(ecosystem, &range, to);
                        p.declares = range;
                    }
                }
                Err(why) => p.missing = why,
            }
        }
    } else {
        p.missing = "the dependency list doesn't name one installed version of it".into();
    }
    if let Some(url) = un::deps_url(ecosystem, parent, None) {
        match registry(state, &url).await {
            Ok(meta) => {
                p.latest = un::record_version(ecosystem, &meta).unwrap_or_default();
                if let Some(range) = un::declared_range(ecosystem, &meta, package) {
                    p.latest_admits = un::range_admits(ecosystem, &range, to);
                    p.latest_declares = range;
                }
            }
            Err(why) if p.missing.is_empty() => p.missing = why,
            Err(_) => {}
        }
    }
    p
}

/// The order a unit's bumps are read in: the ones that cross a major
/// version first, then the rest as the unit lists them.
pub fn read_order(bumps: &[sec::Bump]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..bumps.len()).filter(|&i| bumps[i].major).collect();
    order.extend((0..bumps.len()).filter(|&i| !bumps[i].major));
    order
}

/// The notes for a unit's bumps, read now. `members`: the unit's findings;
/// `deps`: the repository's dependency list (for a parent's installed
/// version).
pub async fn read(
    state: &WorkerState,
    gh: &GitHubClient,
    unit: &FixUnit,
    base: &str,
    members: &[(&Finding, &FindingState)],
    deps: &[Dep],
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
    let order = read_order(&unit.bumps);
    for &i in order.iter().take(un::MAX_PACKAGES) {
        let b = &unit.bumps[i];
        let ecosystem = members
            .iter()
            .find(|(f, _)| sec::in_bump(b, f))
            .map(|(f, _)| f.ecosystem.as_str())
            .filter(|e| !e.is_empty())
            .unwrap_or(&unit.ecosystem);
        let mut n = PackageNotes::for_bump(b, ecosystem);
        if let Some(t) = un::lock_tool_for(&tools, &b.manifest, ecosystem) {
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
        if b.major {
            // Why it leaves its line, and who uses it: this repository, or
            // the parents that pull it in.
            n.major_reason = members
                .iter()
                .filter(|(f, _)| sec::in_bump(b, f))
                .find(|(f, _)| f.fixed_version.as_deref() == Some(b.to.as_str()))
                .map(|(f, st)| common::dep_fix::major_reason(f, st))
                .unwrap_or_default();
            let (direct, parents) = parents_of(b, members, deps);
            n.direct = direct;
            for (parent, installed) in parents {
                if started.elapsed() >= BUDGET {
                    n.parents.push(ParentSupport {
                        parent,
                        version: installed,
                        missing: "the time for reading notes ran out".into(),
                        ..Default::default()
                    });
                    continue;
                }
                n.parents.push(
                    read_parent(state, ecosystem, &b.package, &b.to, &parent, &installed).await,
                );
            }
        }
        out.push(n);
    }
    for &i in order.iter().skip(un::MAX_PACKAGES) {
        let mut n = PackageNotes::for_bump(&unit.bumps[i], &unit.ecosystem);
        n.missing = format!(
            "not read: notes are read for the first {} packages of a pull request, major versions first",
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
        // A package bumped on two version lines keeps notes for each.
        all.retain(|k| {
            !(k.package.eq_ignore_ascii_case(&n.package) && sec::same_line(&k.from, &n.from))
        });
        all.push(n.clone());
    }
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::security_fixes::{Bump, Relation};

    fn finding(id: &str, pkg: &str, version: &str, path: &[&str]) -> (Finding, FindingState) {
        (
            Finding {
                id: id.into(),
                ecosystem: "npm".into(),
                package: pkg.into(),
                version: version.into(),
                relation: if path.len() > 1 {
                    Relation::Transitive
                } else {
                    Relation::Direct
                },
                path: path.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            },
            FindingState::default(),
        )
    }

    fn bump(pkg: &str, from: &str, to: &str, major: bool) -> Bump {
        Bump {
            package: pkg.into(),
            from: from.into(),
            to: to.into(),
            major,
            manifest: "pnpm-lock.yaml".into(),
        }
    }

    fn dep(name: &str, version: &str) -> Dep {
        Dep {
            ecosystem: "npm".into(),
            name: name.into(),
            version: version.into(),
            manifest: "pnpm-lock.yaml".into(),
            ..Default::default()
        }
    }

    #[test]
    fn major_versions_are_read_before_any_other_bump() {
        let bumps: Vec<Bump> = (0..12)
            .map(|i| bump(&format!("lib-{i:02}"), "2.1.0", "2.1.5", i == 9 || i == 11))
            .collect();
        let order = read_order(&bumps);
        assert_eq!(&order[..3], &[9, 11, 0]);
        assert_eq!(order.len(), 12);
        // Inside the first packages read, whatever the bundle's size.
        assert!(order
            .iter()
            .take(common::upgrade_notes::MAX_PACKAGES)
            .any(|&i| i == 11));
    }

    #[test]
    fn a_transitive_packages_parents_come_from_its_dependency_paths() {
        let fs = [
            finding("a", "glob-walk", "7.2.0", &["build-kit", "glob-walk"]),
            finding(
                "b",
                "glob-walk",
                "7.2.0",
                &["app-shell", "file-sync", "glob-walk"],
            ),
            // Another version line of the package: not this bump's.
            finding("c", "glob-walk", "8.0.3", &["other-parent", "glob-walk"]),
        ];
        let members: Vec<(&Finding, &FindingState)> = fs.iter().map(|(f, s)| (f, s)).collect();
        let deps = [
            dep("build-kit", "3.2.1"),
            dep("file-sync", "1.0.0"),
            dep("file-sync", "2.0.0"),
        ];
        let b = bump("glob-walk", "7.2.0", "8.1.1", true);
        assert_eq!(
            parents_of(&b, &members, &deps),
            (
                false,
                vec![
                    ("build-kit".to_string(), "3.2.1".to_string()),
                    // Two versions installed: which one pulls it in isn't known.
                    ("file-sync".to_string(), String::new()),
                ]
            )
        );
        // A direct dependency has no parent to ask.
        let own = [finding("d", "tmpl-engine", "2.3.0", &["tmpl-engine"])];
        let members: Vec<(&Finding, &FindingState)> = own.iter().map(|(f, s)| (f, s)).collect();
        assert_eq!(
            parents_of(
                &bump("tmpl-engine", "2.3.0", "3.9.0", true),
                &members,
                &deps
            ),
            (true, vec![])
        );
    }

    #[test]
    fn notes_joined_later_replace_only_their_own_version_line() {
        let note = |from: &str, to: &str| PackageNotes {
            package: "glob-walk".into(),
            from: from.into(),
            to: to.into(),
            ..Default::default()
        };
        let kept =
            serde_json::to_string(&vec![note("7.2.0", "7.5.4"), note("8.0.3", "8.1.1")]).unwrap();
        let all = merged(&kept, &[note("7.1.0", "7.5.9")]);
        let lines: Vec<(&str, &str)> = all
            .iter()
            .map(|n| (n.from.as_str(), n.to.as_str()))
            .collect();
        assert_eq!(lines, vec![("8.0.3", "8.1.1"), ("7.1.0", "7.5.9")]);
    }
}
