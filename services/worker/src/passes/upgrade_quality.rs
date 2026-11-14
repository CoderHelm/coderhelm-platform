//! The reads `common::upgrade_quality` decides from, and the record it
//! leaves for the pull request.
//!
//! Everything that decides is pure and lives in `common::upgrade_quality`:
//! families, the verification set, the advisory count, why a major is
//! safe, and the behaviour probe. This module reads what those need (the
//! registry's record of a package, the base branch's and the head's
//! lockfiles, the project's own files, the repository's findings and
//! notes), and keeps the result on the security ticket as the run's
//! upgrade evidence, stamped with the head commit it is of.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use common::dep_fix::{self as df, DepRun};
use common::security_fixes::{self as sec, Finding, FindingState, Scope};
use common::upgrade_notes as un;
use common::upgrade_quality::{
    advisories as adv, behaviour as bh, family as fam, safety, verify as vq, UpgradeFacts,
};
use serde_json::Value;
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::models::TicketMessage;
use crate::WorkerState;

/// Time the family search of one plan may take.
const FAMILY_BUDGET: Duration = Duration::from_secs(45);
/// Bumps of one plan whose families are looked for.
const MAX_FAMILY_BUMPS: usize = 40;
/// Call-site files one behaviour plan reads.
const MAX_CALL_SITE_FILES: usize = 12;
/// Majors one run gathers safety evidence for.
const MAX_MAJORS: usize = 8;
/// Dependency paths read per major.
const MAX_PATHS: usize = 4;

/// The registry ecosystem a lockfile's packages come from.
pub fn ecosystem_of(lockfile: &str) -> &'static str {
    match common::lockfile_sync::lock_manager(lockfile) {
        Some("npm" | "pnpm" | "yarn" | "bun") => "npm",
        Some("cargo") => "crates.io",
        Some("poetry" | "uv" | "pipenv") => "PyPI",
        Some("bundler") => "RubyGems",
        Some("go") => "Go",
        _ => "",
    }
}

/// The registry's record of one version of a package. `Ok(None)`: the
/// registry has no such version.
async fn registry_record(
    state: &WorkerState,
    ecosystem: &str,
    package: &str,
    version: &str,
) -> Result<Option<Value>, String> {
    let Some(url) = un::registry_url(ecosystem, package, version) else {
        return Err(format!("CoderHelm reads no registry for {ecosystem}"));
    };
    match super::upgrade_notes::registry(state, &url).await {
        Ok(v) => Ok(Some(v)),
        Err(why) if why.ends_with(" 404") => Ok(None),
        Err(why) => Err(why),
    }
}

fn source_key(ecosystem: &str, package: &str, meta: &Value) -> Option<String> {
    un::source_repo(ecosystem, package, meta)
        .map(|r| format!("{}/{}", r.owner, r.name).to_ascii_lowercase())
}

/// The peer ranges a registry record declares (npm's `peerDependencies`;
/// other registries have no such field).
fn peers_of(ecosystem: &str, meta: &Value) -> Vec<(String, String)> {
    if ecosystem != "npm" {
        return vec![];
    }
    meta["peerDependencies"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(k, v)| v.as_str().map(|r| (k.clone(), r.to_string())))
        .collect()
}

async fn copies_at(
    gh: &GitHubClient,
    msg: &TicketMessage,
    lockfile: &str,
    git_ref: &str,
) -> Option<Vec<(String, String)>> {
    let text = gh
        .read_file_if_exists(&msg.repo_owner, &msg.repo_name, lockfile, git_ref)
        .await
        .ok()??;
    common::reachability::lock_copies(lockfile, &text)
}

/// The families of a plan's packages, found from the registry and the base
/// branch's lockfile, and brought into the plan (`family::attach`): each
/// bump of a member then carries the others. Returns the families and the
/// peer ranges the plan's new releases declare.
pub async fn families(
    state: &WorkerState,
    gh: &GitHubClient,
    msg: &TicketMessage,
    plan: &mut df::DepPlan,
) -> (Vec<fam::Family>, Vec<fam::PeerRange>) {
    let started = Instant::now();
    let mut base: BTreeMap<String, Option<Vec<(String, String)>>> = BTreeMap::new();
    let mut sources: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut families: Vec<fam::Family> = vec![];
    let mut peers: Vec<fam::PeerRange> = vec![];
    for b in plan.bumps.iter().take(MAX_FAMILY_BUMPS) {
        if started.elapsed() >= FAMILY_BUDGET {
            break;
        }
        let ecosystem = ecosystem_of(&b.manifest);
        if ecosystem.is_empty() {
            continue;
        }
        // Already a member of a family found for an earlier bump.
        if families.iter().any(|f| {
            f.lockfile == b.manifest
                && f.base_version == b.from
                && f.members.iter().any(|m| m.eq_ignore_ascii_case(&b.package))
        }) {
            continue;
        }
        if !base.contains_key(&b.manifest) {
            let read = copies_at(gh, msg, &b.manifest, &msg.base_branch).await;
            base.insert(b.manifest.clone(), read);
        }
        let Some(Some(copies)) = base.get(&b.manifest) else {
            continue;
        };
        // The release it moves to: its source repository and its peers.
        let to = registry_record(state, ecosystem, &b.package, &b.to)
            .await
            .ok()
            .flatten();
        let peer_ranges = to
            .as_ref()
            .map(|m| peers_of(ecosystem, m))
            .unwrap_or_default();
        for (on, range) in &peer_ranges {
            peers.push(fam::PeerRange {
                from: b.package.clone(),
                on: on.clone(),
                range: range.clone(),
            });
        }
        let candidates = fam::candidates(&b.package, &b.from, copies);
        if candidates.is_empty() && !peer_ranges.iter().any(|(_, r)| fam::pins(r)) {
            continue;
        }
        let mut known = vec![fam::Known {
            name: b.package.clone(),
            source: to
                .as_ref()
                .and_then(|m| source_key(ecosystem, &b.package, m)),
        }];
        for c in &candidates {
            let key = format!("{ecosystem}:{c}");
            if !sources.contains_key(&key) {
                let version = copies
                    .iter()
                    .find(|(n, _)| n == c)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default();
                let source = registry_record(state, ecosystem, c, &version)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|m| source_key(ecosystem, c, &m));
                sources.insert(key.clone(), source);
            }
            known.push(fam::Known {
                name: c.clone(),
                source: sources.get(&key).cloned().flatten(),
            });
        }
        let Some(mut f) = fam::detect(
            &b.package,
            &b.from,
            &b.manifest,
            copies,
            &known,
            &peer_ranges,
        ) else {
            continue;
        };
        if f.members.len() >= 2 {
            // Which members publish the version that fixes the advisory.
            let wanted = vec![b.to.clone()];
            let mut published: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for m in &f.members {
                let has = if m.eq_ignore_ascii_case(&b.package) {
                    to.as_ref().map(|_| true)
                } else {
                    registry_record(state, ecosystem, m, &b.to)
                        .await
                        .ok()
                        .map(|r| r.is_some())
                };
                if let Some(has) = has {
                    published.insert(m.clone(), if has { wanted.clone() } else { vec![] });
                }
            }
            match fam::target(&f, &wanted, &published) {
                Ok(v) => f.target = v,
                Err(why) => f.held = why,
            }
        }
        families.push(f);
    }
    fam::attach(plan, &mut families);
    info!(
        families = families.len(),
        moving = families.iter().filter(|f| !f.target.is_empty()).count(),
        secs = started.elapsed().as_secs(),
        "dep fix: families read"
    );
    (families, peers)
}

/// What the verification set of a run runs: the set of the project most
/// of its applied bumps are in, found in that project's own files.
pub async fn verify_plan(
    gh: &GitHubClient,
    msg: &TicketMessage,
    run: &DepRun,
    tree: &[String],
) -> vq::Plan {
    let Some(lockfile) = main_lockfile(run) else {
        return vq::Plan {
            none: "no bump was applied".into(),
            ..Default::default()
        };
    };
    let mut files = vec![];
    for path in vq::files_to_read(&lockfile, tree) {
        if let Ok(Some(text)) = gh
            .read_file_if_exists(&msg.repo_owner, &msg.repo_name, &path, &msg.base_branch)
            .await
        {
            files.push((path, text));
        }
    }
    let manager = common::lockfile_sync::lock_manager(&lockfile).unwrap_or("");
    let audit = common::dep_audit::production(manager);
    vq::discover(
        &lockfile,
        tree,
        &files,
        audit.as_ref().map(|(shown, cmd)| (*shown, cmd.as_str())),
    )
}

/// The lockfile most of a run's applied bumps go through.
pub fn main_lockfile(run: &DepRun) -> Option<String> {
    let mut by: BTreeMap<&str, usize> = BTreeMap::new();
    for b in run.applied_bumps() {
        *by.entry(b.manifest.as_str()).or_default() += 1;
    }
    by.into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(a.0)))
        .map(|(l, _)| l.to_string())
}

/// The requests the runtime proof sends for the run's applied majors: the
/// routes declared in the files that call each (`behaviour::plan`).
pub async fn behaviour_plan(
    state: &WorkerState,
    gh: &GitHubClient,
    msg: &TicketMessage,
    run: &DepRun,
) -> bh::Planned {
    let applied = run.applied_bumps();
    let notes: Vec<un::PackageNotes> =
        super::security_fixes::dep_notes(state, &msg.team_id, &msg.ticket_id)
            .await
            .into_iter()
            .filter(|n| {
                n.major
                    && applied
                        .iter()
                        .any(|b| b.major && b.package.eq_ignore_ascii_case(&n.package))
            })
            .collect();
    let mut paths: Vec<String> = notes
        .iter()
        .flat_map(|n| n.call_sites.iter().map(|c| c.file.clone()))
        .collect();
    paths.sort();
    paths.dedup();
    let mut files = vec![];
    for path in paths.into_iter().take(MAX_CALL_SITE_FILES) {
        if let Ok(Some(text)) = gh
            .read_file_if_exists(&msg.repo_owner, &msg.repo_name, &path, &msg.base_branch)
            .await
        {
            files.push((path, text));
        }
    }
    bh::plan(&notes, &files)
}

/// The team's other repositories, searched through the cross-repository
/// search (`cross_repo::search_others`).
struct OtherRepos<'a> {
    gh: &'a GitHubClient,
}

#[async_trait::async_trait]
impl super::cross_repo::RepoSearch for OtherRepos<'_> {
    async fn search(&self, repo: &str, query: &str) -> Result<Vec<String>, String> {
        let (owner, name) = repo.split_once('/').ok_or("not owner/name")?;
        let branch = self
            .gh
            .get_default_branch(owner, name)
            .await
            .map_err(|e| e.to_string())?;
        self.gh
            .search_code(owner, name, &branch, query)
            .await
            .map(|r| r.into_iter().map(|h| h.path).collect())
            .map_err(|e| e.to_string())
    }
}

/// The team's other repositories that name an endpoint whose answer
/// changed, and how many were searched, in words.
async fn consumers(
    state: &WorkerState,
    gh: &GitHubClient,
    msg: &TicketMessage,
    paths: &[String],
) -> (Vec<String>, String) {
    let own = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let repos = super::plan::fetch_team_repos(state, &msg.team_id).await;
    let others = repos
        .iter()
        .filter(|r| !r.eq_ignore_ascii_case(&own))
        .take(super::cross_repo::MAX_FALLBACK_REPOS)
        .count();
    if others == 0 || paths.is_empty() {
        return (vec![], String::new());
    }
    let search = OtherRepos { gh };
    let mut found: Vec<String> = vec![];
    for path in paths.iter().take(4) {
        if let Some((_, hit)) = super::cross_repo::search_others(&search, &own, &repos, path).await
        {
            for r in hit {
                if !found.contains(&r) {
                    found.push(r);
                }
            }
        }
    }
    (
        found,
        format!(
            "{others} other repositor{} of the team, for {}",
            if others == 1 { "y" } else { "ies" },
            paths
                .iter()
                .take(4)
                .map(|p| format!("`{p}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )
}

/// The manifest scripts whose command runs `package`, by name.
pub fn scripts_running(manifest: &str, package: &str) -> Vec<String> {
    let Ok(v) = serde_json::from_str::<Value>(manifest) else {
        return vec![];
    };
    // A scoped package's command is its last part (`@scope/cli` runs `cli`).
    let word = package.rsplit('/').next().unwrap_or(package);
    let mut out: Vec<String> = v["scripts"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, cmd)| {
            cmd.as_str().is_some_and(|c| {
                c.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_')
                    .any(|w| w == word)
            })
        })
        .map(|(name, _)| name.clone())
        .collect();
    out.sort();
    out
}

/// The packages an npm lockfile marks as running a script at install.
pub fn install_script_packages(lockfile: &str, text: &str) -> Vec<String> {
    let name = lockfile.rsplit('/').next().unwrap_or(lockfile);
    if !matches!(name, "package-lock.json" | "npm-shrinkwrap.json") {
        return vec![];
    }
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    let mut out: Vec<String> = v["packages"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, p)| p["hasInstallScript"].as_bool() == Some(true))
        .filter_map(|(loc, _)| loc.rsplit("node_modules/").next().map(String::from))
        .filter(|n| !n.is_empty())
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Where the run's work is: what every read here needs.
pub struct At<'a> {
    pub state: &'a WorkerState,
    pub gh: &'a GitHubClient,
    pub msg: &'a TicketMessage,
    pub branch: &'a str,
}

/// The safety evidence of each applied major: its dependency paths with
/// who imports each root, whether the repository uses it, its verdict and
/// how it is fixed.
async fn majors(
    at: &At<'_>,
    run: &DepRun,
    findings: &[(Finding, FindingState)],
    notes: &[un::PackageNotes],
) -> Vec<safety::MajorSafety> {
    let (o, n, base) = (&at.msg.repo_owner, &at.msg.repo_name, &at.msg.base_branch);
    let mut out = vec![];
    let mut manifests: BTreeMap<String, String> = BTreeMap::new();
    let mut install: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for b in run
        .applied_bumps()
        .into_iter()
        .filter(|b| b.major)
        .take(MAX_MAJORS)
    {
        let mine: Vec<&(Finding, FindingState)> = findings
            .iter()
            .filter(|(f, _)| b.finding_ids.contains(&f.id))
            .collect();
        let note = notes
            .iter()
            .find(|x| x.package.eq_ignore_ascii_case(&b.package));
        let dir = b.manifest.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
        let manifest_path = if dir.is_empty() {
            "package.json".to_string()
        } else {
            format!("{dir}/package.json")
        };
        if !manifests.contains_key(&manifest_path) {
            let text = at
                .gh
                .read_file_if_exists(o, n, &manifest_path, base)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            manifests.insert(manifest_path.clone(), text);
        }
        if !install.contains_key(&b.manifest) {
            let text = at
                .gh
                .read_file_if_exists(o, n, &b.manifest, base)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            install.insert(
                b.manifest.clone(),
                install_script_packages(&b.manifest, &text),
            );
        }
        let manifest = manifests.get(&manifest_path).cloned().unwrap_or_default();
        let scripted = install.get(&b.manifest).cloned().unwrap_or_default();
        // One path per direct dependency that brings it in.
        let mut roots: Vec<(String, safety::Declared)> = vec![];
        for (f, _) in &mine {
            let root = f.path.first().cloned().unwrap_or_else(|| f.package.clone());
            let declared = match f.scope {
                Scope::Runtime => safety::Declared::Production,
                Scope::Dev => safety::Declared::Development,
                Scope::Unknown => safety::Declared::NotKnown,
            };
            if !roots.iter().any(|(r, _)| *r == root) {
                roots.push((root, declared));
            }
        }
        let mut paths = vec![];
        for (root, declared) in roots.into_iter().take(MAX_PATHS) {
            let importers = match at.gh.search_code(o, n, base, &root).await {
                Ok(hits) => hits
                    .into_iter()
                    .map(|h| h.path)
                    .filter(|p| {
                        !df::commit_path(p) && !df::is_doc_path(p) && un::is_call_site_file(p)
                    })
                    .take(12)
                    .collect(),
                Err(_) => vec![],
            };
            paths.push(safety::PathIn {
                scripts: scripts_running(&manifest, &root),
                install_script: scripted.contains(&root) || scripted.contains(&b.package),
                root,
                declared,
                importers,
            });
        }
        let mut used: Vec<String> = note
            .map(|x| {
                x.call_sites
                    .iter()
                    .map(|c| format!("{}:{}", c.file, c.line))
                    .collect()
            })
            .unwrap_or_default();
        for (f, _) in &mine {
            for l in f.evidence.iter().flat_map(|e| e.lines.iter()) {
                let at_line = format!("{}:{}", l.file, l.line);
                if !used.contains(&at_line) {
                    used.push(at_line);
                }
            }
        }
        used.truncate(12);
        let apis = note.map(|x| x.apis.clone()).unwrap_or_default();
        let searched = if apis.is_empty() {
            format!("`{}` in the repository's code", b.package)
        } else {
            format!(
                "`{}` and the APIs its notes name ({}) in the repository's code",
                b.package,
                apis.iter()
                    .take(4)
                    .map(|a| format!("`{a}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let direct = note.is_some_and(|x| x.direct)
            || mine
                .iter()
                .any(|(f, _)| f.relation == sec::Relation::Direct || f.path.len() == 1);
        // Yarn's resolutions have no version line in their key.
        let selector = match df::Manager::of_lockfile(&b.manifest) {
            Some(df::Manager::Yarn) => b.package.clone(),
            _ => format!("{}@{}", b.package, b.line),
        };
        let ecosystem = ecosystem_of(&b.manifest);
        out.push(safety::evidence(&safety::MajorIn {
            package: &b.package,
            from: &b.from,
            to: &b.to,
            ecosystem,
            why_major: &b.major_reason,
            paths: &paths,
            used_in: &used,
            searched: &searched,
            verdict: sec::worst_verdict(mine.iter().map(|(f, _)| f.verdict)),
            direct,
            parents: note.map(|x| x.parents.as_slice()).unwrap_or(&[]),
            via_parent: b.via_parent.as_ref(),
            selector: &selector,
        }));
    }
    out
}

/// What the runtime proof's requests showed for each applied major.
async fn behaviour(at: &At<'_>, run: &DepRun, notes: &[un::PackageNotes]) -> Vec<bh::Behaviour> {
    let proof = &run.proof;
    let compared: Vec<bh::Compared> = match (&proof.plan, &proof.base, &proof.head) {
        (Some(plan), Some(base), Some(head)) if proof.proved == run.head => {
            bh::compare(&plan.probes, &base.probes, &head.probes)
        }
        _ => vec![],
    };
    let why_none = if !proof.skipped.is_empty() {
        format!("the runtime proof did not run ({})", proof.skipped)
    } else if proof.head.is_some() && proof.proved != run.head {
        "the runtime proof is of an earlier commit of this branch".to_string()
    } else {
        match &proof.plan {
            Some(p) if p.start.is_none() => format!(
                "the application could not be started to send a request ({})",
                p.no_start
            ),
            _ => "the application was not started in this run".to_string(),
        }
    };
    let mut out = vec![];
    for b in run.applied_bumps().into_iter().filter(|b| b.major) {
        let note = notes
            .iter()
            .find(|x| x.package.eq_ignore_ascii_case(&b.package));
        let mine: Vec<bh::Compared> = run
            .probed
            .iter()
            .find(|(p, _)| p.eq_ignore_ascii_case(&b.package))
            .map(|(_, idx)| {
                idx.iter()
                    .filter_map(|i| compared.get(*i).cloned())
                    .collect()
            })
            .unwrap_or_default();
        let not_probed = if !mine.is_empty() {
            String::new()
        } else {
            run.not_probed
                .iter()
                .find(|(p, _)| p.eq_ignore_ascii_case(&b.package))
                .map(|(_, why)| why.clone())
                .unwrap_or_else(|| why_none.clone())
        };
        let mut one = bh::Behaviour {
            package: b.package.clone(),
            from: b.from.clone(),
            to: b.to.clone(),
            breaking_used: note.map(bh::used_breaking).unwrap_or_default(),
            compared: mine,
            not_probed,
            ..Default::default()
        };
        let changed = one.changed_paths();
        if !changed.is_empty() {
            let (found, searched) = consumers(at.state, at.gh, at.msg, &changed).await;
            one.consumers = found;
            one.consumers_searched = searched;
        }
        out.push(one);
    }
    out
}

/// The advisories of the head, counted once: every advisory CoderHelm
/// holds for the lockfile's packages against the base branch's copies and
/// the head's, laid beside the package manager's audit of the head.
async fn advisories(
    at: &At<'_>,
    run: &DepRun,
    lockfile: &str,
    of_repo: &[(Finding, FindingState)],
    base: &[(String, String)],
    head: &[(String, String)],
) -> adv::Reconciled {
    let (o, n) = (&at.msg.repo_owner, &at.msg.repo_name);
    let lockfiles = vec![lockfile.to_string()];
    let held: Vec<adv::Held> = of_repo
        .iter()
        .filter(|(f, _)| {
            f.manifest == lockfile
                || df::lockfile_for(&f.manifest, &lockfiles).as_deref() == Some(lockfile)
        })
        .filter(|(f, _)| f.status.is_open() || f.status == sec::Status::Withdrawn)
        .map(|(f, st)| adv::Held::of(f, st))
        .collect();
    let dir = lockfile.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
    let under = |name: &str| {
        if dir.is_empty() {
            name.to_string()
        } else {
            format!("{dir}/{name}")
        }
    };
    let read = |path: String| async move {
        at.gh
            .read_file_if_exists(o, n, &path, at.branch)
            .await
            .ok()
            .flatten()
            .unwrap_or_default()
    };
    let manifest = read(under("package.json")).await;
    let workspace = read(under("pnpm-workspace.yaml")).await;
    let overrides = adv::overrides_of(&manifest);
    let ignored = adv::ignored_of(&manifest, &workspace);
    let stays: Vec<(String, String)> = run.deferred_ids();
    // The audit's listing is of the commit the set last ran on.
    let audited = run
        .verify
        .head
        .as_ref()
        .is_some_and(|h| h.sha == run.head && h.ran);
    let audit = run.verify.audit.as_ref().filter(|_| audited);
    let no_audit = match (&run.verify.plan, audited) {
        (_, true) => String::new(),
        (Some(p), false) => p
            .not_applicable
            .iter()
            .find(|(k, _)| *k == vq::Kind::Audit)
            .map(|(_, why)| why.clone())
            .unwrap_or_else(|| {
                if run.verify.skipped.is_empty() {
                    "the verification set gave no result on this commit".to_string()
                } else {
                    run.verify.skipped.clone()
                }
            }),
        (None, false) => "the verification set did not run".to_string(),
    };
    adv::reconcile(&adv::Input {
        sha: &run.head,
        lockfile,
        held: &held,
        base,
        head,
        audit,
        no_audit: &no_audit,
        ignored: &ignored,
        overrides: &overrides,
        stays: &stays,
    })
}

/// The largest record kept on a ticket: well under the store's item size.
const MAX_FACTS_CHARS: usize = 150_000;

/// Keeps the run's upgrade evidence on the security ticket, where the
/// pull request's description reads it.
async fn save(state: &WorkerState, team_id: &str, ticket: &str, facts: &UpgradeFacts) {
    let Ok(json) = serde_json::to_string(facts) else {
        return;
    };
    if json.len() > MAX_FACTS_CHARS {
        warn!(
            ticket,
            chars = json.len(),
            "dep fix: the upgrade evidence is too large to keep"
        );
        return;
    }
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(&sec::ticket_sk(ticket)))
        .update_expression("SET dep_facts = :f")
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_values(":f", super::attr_s(&json))
        .send()
        .await
    {
        warn!(ticket, error = %e, "dep fix: the upgrade evidence could not be kept");
    }
}

/// Records the run's upgrade evidence for its head commit: families and
/// their coherence on the head, the verification set's results, the
/// advisory count laid beside the audit, why each major is safe, and what
/// the behaviour probe showed. Nothing here is a claim beyond what ran:
/// what did not run is listed with why.
pub async fn record(at: &At<'_>, run: &DepRun) -> UpgradeFacts {
    let mut facts = UpgradeFacts {
        sha: run.head.clone(),
        families: run.families.clone(),
        ..Default::default()
    };
    if run.applied.is_empty() {
        return facts;
    }
    let Some(lockfile) = main_lockfile(run) else {
        return facts;
    };
    let base = copies_at(at.gh, at.msg, &lockfile, &at.msg.base_branch).await;
    let head = copies_at(at.gh, at.msg, &lockfile, at.branch).await;
    // Every lockfile the run bumped: the families the head installs at
    // more than one version, and the peer ranges it leaves unmet.
    let mut lockfiles: Vec<String> = run
        .applied_bumps()
        .iter()
        .map(|b| b.manifest.clone())
        .collect();
    lockfiles.sort();
    lockfiles.dedup();
    for l in &lockfiles {
        let copies = if *l == lockfile {
            head.clone()
        } else {
            copies_at(at.gh, at.msg, l, at.branch).await
        };
        let Some(copies) = copies else {
            facts.not_run.push(format!(
                "the family check of `{l}`: its lockfile on the head could not be read"
            ));
            continue;
        };
        facts.splits.extend(fam::splits(&run.families, l, &copies));
        let peers: Vec<fam::PeerRange> = run
            .peers
            .iter()
            .filter(|p| {
                run.applied_bumps()
                    .iter()
                    .any(|b| b.manifest == *l && b.package == p.from)
            })
            .cloned()
            .collect();
        facts
            .peers_unmet
            .extend(fam::peers_unmet(ecosystem_of(l), &peers, &copies));
        if *l != lockfile {
            facts.not_run.push(format!(
                "the verification set for the project of `{l}`: it runs for the project most of the bumps are in (`{lockfile}`); this project's bumps were probed with the repository's checks and its lockfile was written by its own package manager"
            ));
        }
    }
    // The verification set, of this commit only.
    match &run.verify.head {
        Some(v) if v.sha == run.head => {
            facts.verified = Some(v.clone());
            facts.verified_base = run.verify.base.clone();
        }
        _ => facts.not_run.push(format!(
            "the verification set (the project's own install, type check, build, lint, tests and production audit): {}",
            if run.verify.skipped.is_empty() {
                "no result on this commit"
            } else {
                &run.verify.skipped
            }
        )),
    }
    let findings = super::security_fixes::load_findings(at.state, &at.msg.team_id).await;
    let repo = format!("{}/{}", at.msg.repo_owner, at.msg.repo_name);
    let of_repo: Vec<(Finding, FindingState)> = findings
        .into_iter()
        .filter(|(f, _)| f.repo.eq_ignore_ascii_case(&repo))
        .collect();
    match (&base, &head) {
        (Some(b), Some(h)) => {
            facts.advisories = Some(advisories(at, run, &lockfile, &of_repo, b, h).await)
        }
        _ => facts.not_run.push(format!(
            "the advisory count: `{lockfile}` could not be read on the base branch and on the head"
        )),
    }
    let notes =
        super::security_fixes::dep_notes(at.state, &at.msg.team_id, &at.msg.ticket_id).await;
    facts.majors = majors(at, run, &of_repo, &notes).await;
    facts.behaviour = behaviour(at, run, &notes).await;
    save(at.state, &at.msg.team_id, &at.msg.ticket_id, &facts).await;
    info!(
        sha = %facts.sha,
        families = facts.families.len(),
        splits = facts.splits.len(),
        majors = facts.majors.len(),
        probed = facts.behaviour.iter().filter(|b| !b.compared.is_empty()).count(),
        "dep fix: upgrade evidence recorded"
    );
    facts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_lockfile_names_its_registry() {
        assert_eq!(ecosystem_of("pnpm-lock.yaml"), "npm");
        assert_eq!(ecosystem_of("apps/api/yarn.lock"), "npm");
        assert_eq!(ecosystem_of("Cargo.lock"), "crates.io");
        assert_eq!(ecosystem_of("svc/uv.lock"), "PyPI");
        assert_eq!(ecosystem_of("Gemfile.lock"), "RubyGems");
        assert_eq!(ecosystem_of("go.sum"), "Go");
        assert_eq!(ecosystem_of("README.md"), "");
    }

    #[test]
    fn a_script_that_runs_a_package_and_an_install_script_are_read() {
        let manifest = r#"{"scripts":{"build":"bundle-kit --mode production","lint":"lint-kit .","start":"node dist/main.js"}}"#;
        assert_eq!(scripts_running(manifest, "bundle-kit"), vec!["build"]);
        assert_eq!(scripts_running(manifest, "@tools/lint-kit"), vec!["lint"]);
        assert!(scripts_running(manifest, "web-kit").is_empty());
        let lock = r#"{"packages":{"":{},"node_modules/native-kit":{"version":"1.0.0","hasInstallScript":true},"node_modules/a/node_modules/@s/gyp-kit":{"hasInstallScript":true},"node_modules/plain":{"version":"2.0.0"}}}"#;
        assert_eq!(
            install_script_packages("package-lock.json", lock),
            vec!["@s/gyp-kit", "native-kit"]
        );
        // Another manager's lockfile does not say: nothing is assumed.
        assert!(install_script_packages("pnpm-lock.yaml", lock).is_empty());
    }

    #[test]
    fn the_main_lockfile_is_where_most_applied_bumps_are() {
        let bump = |m: &str| df::PlanBump {
            package: "x".into(),
            manifest: m.into(),
            ..Default::default()
        };
        let mut run = DepRun {
            plan: df::DepPlan {
                bumps: vec![
                    bump("apps/a/pnpm-lock.yaml"),
                    bump("pnpm-lock.yaml"),
                    bump("pnpm-lock.yaml"),
                ],
                deferred: vec![],
            },
            ..Default::default()
        };
        assert_eq!(main_lockfile(&run), None);
        run.applied = vec![0, 1, 2];
        assert_eq!(main_lockfile(&run).as_deref(), Some("pnpm-lock.yaml"));
    }
}
