//! A repository's code map: what CoderHelm knows about the code, kept
//! current with the default branch, so the planner and the coder start
//! from it instead of exploring the layout again on every run.
//!
//! It lives on the repository's agent file record (`common::agent_file`,
//! `pk = <team>`, `sk = AGENTS#REPO#<owner/repo>`, attribute [`FIELD`]) and
//! is made of sections, each stamped with the commit it describes:
//!
//! - five sections for the whole repository: its architecture, its stack and
//!   versions, how to build, test and run it, its conventions, and the
//!   dependencies between its packages and services ([`GLOBAL`]);
//! - one section per top-level area (`services/`, `apps/`, at most
//!   [`MAX_AREAS`]): its structure and entry points ([`AREA_PREFIX`]).
//!
//! The map as a whole is stamped with the commit and the tree it was last
//! brought to. On every run the base is compared with the stamp
//! ([`plan_refresh`]):
//!
//! - the same commit, or the same tree (byte-identical files): every section
//!   is reused;
//! - otherwise the comparison's changed paths decide ([`sections_touched`]):
//!   a manifest changes the stack, build and dependency sections; CI the
//!   build section; lint and format config the conventions; a file added,
//!   removed or renamed in an area, or an entry point or package manifest
//!   changed in it, that area's section; an area added or gone, the
//!   architecture. A plain edit inside a source file changes no section;
//! - a comparison that could not be read, or was cut by GitHub, rebuilds
//!   every section.
//!
//! Only the sections to rebuild are read again (their own files,
//! [`inputs_for`]) and written in one model call ([`prompt`],
//! [`parse_reply`]); the rest are reused as they are. A section the reply
//! left out keeps its older stamp and is rebuilt on the next run.
//!
//! What a run saves is recorded on it ([`Exploration`]): the coder's tool
//! calls before its first write, and whether it started from a map.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agent_file::{self, Kind};

/// The agent file record's attribute that holds the map, as JSON.
pub const FIELD: &str = "code_map";
/// The run record's attribute for what the planner's refresh did.
pub const REFRESH_FIELD: &str = "code_map_refresh";
/// The run record's attribute for the coder's exploration.
pub const EXPLORATION_FIELD: &str = "exploration";

/// Top-level areas with a section of their own; the rest are named in the
/// architecture section.
pub const MAX_AREAS: usize = 12;
/// Files read for one section.
pub const MAX_INPUT_FILES: usize = 10;
/// Characters of one file read for a section.
pub const MAX_FILE_CHARS: usize = 4_000;
/// Paths listed for one area.
pub const MAX_LISTING: usize = 100;
/// Characters of file content in one request, across every section: a
/// first build of a large repository reads less of each file.
pub const MAX_PROMPT_FILE_CHARS: usize = 150_000;
/// Characters of the map given to an agent.
pub const MAX_RENDER_CHARS: usize = 24_000;
/// Files GitHub lists for one comparison at most: a list this long may be
/// cut, so it is not trusted.
pub const COMPARE_FILE_LIMIT: usize = 300;

/// The id prefix of an area's section (`area:services`).
pub const AREA_PREFIX: &str = "area:";

/// The whole-repository sections: id, title, what it says.
pub const GLOBAL: &[(&str, &str, &str)] = &[
    (
        "architecture",
        "Architecture",
        "what the repository is for, its main parts and how a request or job flows through them, and where each part is deployed",
    ),
    (
        "stack",
        "Tech stack and versions",
        "languages, frameworks, runtimes and the versions the manifests and toolchain files pin",
    ),
    (
        "build",
        "Build, test and run",
        "the exact commands to install, build, type-check, lint, test (each suite: unit, integration, end-to-end) and run locally, and what CI runs",
    ),
    (
        "conventions",
        "Conventions",
        "code style, lint and format rules, naming, error handling and logging patterns, test layout, and what the repository's guidance and pull request template ask",
    ),
    (
        "dependencies",
        "Dependencies between packages and services",
        "which packages, services and infrastructure depend on which, shared libraries, and what changes together",
    ),
];

pub fn area_id(dir: &str) -> String {
    format!("{AREA_PREFIX}{dir}")
}

/// One section of the map.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Section {
    pub id: String,
    pub text: String,
    /// The commit this section describes.
    pub commit: String,
}

/// A repository's code map.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeMap {
    /// The commit the map was last brought to.
    pub commit: String,
    /// That commit's tree.
    #[serde(default)]
    pub tree: String,
    pub sections: Vec<Section>,
    /// When it was last written (RFC 3339).
    #[serde(default)]
    pub updated_at: String,
}

impl CodeMap {
    pub fn section(&self, id: &str) -> Option<&Section> {
        self.sections.iter().find(|s| s.id == id)
    }

    /// The areas it has a section for.
    pub fn areas(&self) -> Vec<String> {
        self.sections
            .iter()
            .filter_map(|s| s.id.strip_prefix(AREA_PREFIX).map(str::to_string))
            .collect()
    }

    /// Sections still describing an older commit than the map's.
    pub fn behind(&self) -> Vec<String> {
        self.sections
            .iter()
            .filter(|s| s.commit != self.commit)
            .map(|s| s.id.clone())
            .collect()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }
}

// ── What a change touches ──────────────────────────────────────────────────

/// One file of a comparison.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Changed {
    pub path: String,
    /// `added`, `removed`, `modified`, `renamed`, `copied` or `changed`.
    pub status: String,
    /// A renamed file's old path.
    pub previous: Option<String>,
}

impl Changed {
    pub fn new(path: &str, status: &str) -> Self {
        Self {
            path: path.to_string(),
            status: status.to_string(),
            previous: None,
        }
    }

    fn structural(&self) -> bool {
        matches!(
            self.status.as_str(),
            "added" | "removed" | "renamed" | "copied"
        )
    }
}

/// The files a GitHub comparison (`compare/base...head`) lists. `None` when
/// the list may be cut (GitHub lists at most [`COMPARE_FILE_LIMIT`]): the
/// caller then rebuilds every section.
pub fn changed_from_compare(v: &Value) -> Option<Vec<Changed>> {
    let files = v.get("files")?.as_array()?;
    if files.len() >= COMPARE_FILE_LIMIT {
        return None;
    }
    Some(
        files
            .iter()
            .filter_map(|f| {
                let path = f.get("filename")?.as_str()?;
                Some(Changed {
                    path: path.to_string(),
                    status: f
                        .get("status")
                        .and_then(|s| s.as_str())
                        .unwrap_or("modified")
                        .to_string(),
                    previous: f
                        .get("previous_filename")
                        .and_then(|s| s.as_str())
                        .map(str::to_string),
                })
            })
            .collect(),
    )
}

fn name_of(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase()
}

/// Package manifests, wherever they are (a monorepo keeps one per package).
const PACKAGE_MANIFESTS: &[&str] = &[
    "package.json",
    "cargo.toml",
    "go.mod",
    "pyproject.toml",
    "requirements.txt",
    "setup.py",
    "pom.xml",
    "build.gradle",
    "build.gradle.kts",
    "gemfile",
    "composer.json",
    "mix.exs",
    "pubspec.yaml",
    "package.swift",
    "deno.json",
];

/// Whether `path` is a package manifest.
pub fn is_package_manifest(path: &str) -> bool {
    PACKAGE_MANIFESTS.contains(&name_of(path).as_str())
}

const ENTRY_STEMS: &[&str] = &[
    "main", "index", "app", "lib", "server", "handler", "cli", "__main__", "manage", "program",
    "routes", "router", "urls", "wsgi", "asgi",
];

const CODE_EXTS: &[&str] = &[
    "rs", "ts", "tsx", "js", "jsx", "mjs", "cjs", "py", "go", "rb", "java", "kt", "cs", "php",
    "swift", "ex", "exs", "dart", "scala",
];

/// Whether `path` is an entry point: a `main`, `index`, `lib`, `app`,
/// `server`, `handler` or route table source file.
pub fn is_entry_point(path: &str) -> bool {
    let name = name_of(path);
    let Some((stem, ext)) = name.rsplit_once('.') else {
        return false;
    };
    CODE_EXTS.contains(&ext) && ENTRY_STEMS.contains(&stem)
}

/// Whether `path` describes infrastructure or how services are wired
/// (Terraform, CDK, CloudFormation, Kubernetes, compose files).
pub fn is_infra(path: &str) -> bool {
    let low = path.to_ascii_lowercase();
    let name = name_of(path);
    low.ends_with(".tf")
        || low.ends_with(".tfvars")
        || name == "cdk.json"
        || name.starts_with("docker-compose")
        || name.starts_with("compose.")
        || name == "serverless.yml"
        || name == "template.yaml"
        || name == "template.yml"
        || name == "chart.yaml"
        || low.split('/').any(|s| {
            matches!(
                s,
                "k8s" | "kubernetes" | "helm" | "charts" | "terraform" | "cloudformation"
            )
        })
}

/// The top-level areas of a tree that get a section: folders that hold
/// files, not hidden ones, the largest [`MAX_AREAS`], by name.
pub fn areas_of<S: AsRef<str>>(paths: &[S]) -> Vec<String> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for p in paths {
        if let Some(d) = agent_file::top_dir(p.as_ref()) {
            if !d.starts_with('.') {
                *counts.entry(d.to_string()).or_default() += 1;
            }
        }
    }
    let mut by_size: Vec<(String, usize)> = counts.into_iter().collect();
    by_size.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    by_size.truncate(MAX_AREAS);
    let mut out: Vec<String> = by_size.into_iter().map(|(d, _)| d).collect();
    out.sort();
    out
}

/// The sections a changed file makes stale. A plain edit inside a source
/// file touches none: the map describes structure, not every line.
pub fn sections_touched(c: &Changed, areas: &[String]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let paths: Vec<&str> = std::iter::once(c.path.as_str())
        .chain(c.previous.as_deref())
        .collect();
    for path in paths {
        match agent_file::kind_of(path) {
            Some(Kind::Guidance) | Some(Kind::PrTemplate) => {
                out.insert("conventions".to_string());
            }
            Some(Kind::Readme) => {
                out.insert("architecture".to_string());
            }
            Some(Kind::Manifest) => {
                for s in ["stack", "build", "dependencies"] {
                    out.insert(s.to_string());
                }
            }
            Some(Kind::Ci) => {
                out.insert("build".to_string());
            }
            Some(Kind::Tooling) => {
                out.insert("conventions".to_string());
                out.insert("build".to_string());
            }
            None => {}
        }
        let nested_manifest = path.contains('/') && is_package_manifest(path);
        if nested_manifest {
            out.insert("stack".to_string());
            out.insert("dependencies".to_string());
        }
        if is_infra(path) {
            out.insert("architecture".to_string());
            out.insert("dependencies".to_string());
        }
        if let Some(dir) = agent_file::top_dir(path) {
            let structural = c.structural() || is_entry_point(path) || nested_manifest;
            if structural && areas.iter().any(|a| a == dir) {
                out.insert(area_id(dir));
            }
        }
    }
    out
}

/// What a run does to the map before it starts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Refresh {
    /// Sections written again from the repository at the base.
    pub rebuild: Vec<String>,
    /// Sections kept as they are.
    pub reuse: Vec<String>,
    /// Sections of areas that are gone.
    pub drop: Vec<String>,
    /// Why, in a few words: `new`, `current`, `changed`, `unknown changes`.
    pub why: String,
}

impl Refresh {
    pub fn is_noop(&self) -> bool {
        self.rebuild.is_empty() && self.drop.is_empty()
    }
}

fn wanted(areas: &[String]) -> Vec<String> {
    GLOBAL
        .iter()
        .map(|(id, _, _)| id.to_string())
        .chain(areas.iter().map(|a| area_id(a)))
        .collect()
}

/// What to rebuild for a run whose base is `base` (tree `tree`), with the
/// areas the base has (`areas_now`) and the comparison's files since the
/// map's stamp (`changes`, `None` when it could not be read). Pure.
pub fn plan_refresh(
    map: Option<&CodeMap>,
    base: &str,
    tree: &str,
    areas_now: &[String],
    changes: Option<&[Changed]>,
) -> Refresh {
    let want = wanted(areas_now);
    let Some(map) = map.filter(|m| !m.sections.is_empty()) else {
        return Refresh {
            rebuild: want,
            why: "new".into(),
            ..Default::default()
        };
    };
    let have: BTreeSet<&str> = map.sections.iter().map(|s| s.id.as_str()).collect();
    let drop: Vec<String> = map
        .sections
        .iter()
        .filter(|s| !want.contains(&s.id))
        .map(|s| s.id.clone())
        .collect();
    let behind: BTreeSet<String> = map.behind().into_iter().collect();
    let same = map.commit == base || (!tree.is_empty() && map.tree == tree);
    let mut rebuild: BTreeSet<String> = want
        .iter()
        .filter(|id| !have.contains(id.as_str()) || behind.contains(*id))
        .cloned()
        .collect();
    let why = if same {
        "current"
    } else {
        match changes {
            None => {
                rebuild.extend(want.iter().cloned());
                "unknown changes"
            }
            Some(changes) => {
                for c in changes {
                    rebuild.extend(sections_touched(c, areas_now));
                }
                let before: BTreeSet<String> = map.areas().into_iter().collect();
                let now: BTreeSet<String> = areas_now.iter().cloned().collect();
                if before != now {
                    rebuild.insert("architecture".to_string());
                }
                "changed"
            }
        }
    };
    let rebuild: Vec<String> = want
        .iter()
        .filter(|id| rebuild.contains(*id))
        .cloned()
        .collect();
    let reuse = want
        .iter()
        .filter(|id| have.contains(id.as_str()) && !rebuild.contains(id))
        .cloned()
        .collect();
    Refresh {
        rebuild,
        reuse,
        drop,
        why: why.to_string(),
    }
}

// ── Reading a section's sources ────────────────────────────────────────────

/// The files read to write section `id`, from the tree's file paths: its
/// own sources only, at most [`MAX_INPUT_FILES`].
pub fn inputs_for<S: AsRef<str>>(id: &str, paths: &[S]) -> Vec<String> {
    let paths: Vec<&str> = paths.iter().map(|p| p.as_ref()).collect();
    let kinds = |want: &[Kind]| -> Vec<String> {
        agent_file::files_to_read(&paths)
            .into_iter()
            .filter(|p| agent_file::kind_of(p).is_some_and(|k| want.contains(&k)))
            .collect()
    };
    let mut out: Vec<String> = match id {
        "architecture" => {
            let mut v = kinds(&[Kind::Readme, Kind::Guidance]);
            v.extend(
                paths
                    .iter()
                    .filter(|p| is_infra(p) && p.matches('/').count() <= 1)
                    .map(|p| p.to_string()),
            );
            v
        }
        "stack" => kinds(&[Kind::Manifest]),
        "build" => kinds(&[Kind::Ci, Kind::Manifest, Kind::Tooling]),
        "conventions" => kinds(&[Kind::Guidance, Kind::Tooling, Kind::PrTemplate]),
        "dependencies" => {
            let mut v: Vec<String> = paths
                .iter()
                .filter(|p| is_package_manifest(p) && p.matches('/').count() <= 2)
                .map(|p| p.to_string())
                .collect();
            v.sort_by_key(|p| (p.matches('/').count(), p.clone()));
            v.extend(
                paths
                    .iter()
                    .filter(|p| is_infra(p) && name_of(p).contains("compose"))
                    .map(|p| p.to_string()),
            );
            v
        }
        _ => match id.strip_prefix(AREA_PREFIX) {
            Some(dir) => {
                let prefix = format!("{dir}/");
                let mut v: Vec<String> = paths
                    .iter()
                    .filter(|p| p.starts_with(&prefix))
                    .filter(|p| is_package_manifest(p) || is_entry_point(p))
                    .map(|p| p.to_string())
                    .collect();
                // Shallow first: a package's manifest and entry before deep ones.
                v.sort_by_key(|p| (p.matches('/').count(), !is_package_manifest(p), p.clone()));
                v
            }
            None => Vec::new(),
        },
    };
    let mut seen = BTreeSet::new();
    out.retain(|p| seen.insert(p.clone()));
    out.truncate(MAX_INPUT_FILES);
    out
}

/// The part of the tree a section is written from: for an area, its paths
/// (at most [`MAX_LISTING`]); for the rest, the top two levels of folders
/// with their file counts.
pub fn listing_for<S: AsRef<str>>(id: &str, paths: &[S]) -> String {
    if let Some(dir) = id.strip_prefix(AREA_PREFIX) {
        let prefix = format!("{dir}/");
        let under: Vec<&str> = paths
            .iter()
            .map(|p| p.as_ref())
            .filter(|p| p.starts_with(&prefix))
            .collect();
        let mut out: Vec<String> = under
            .iter()
            .take(MAX_LISTING)
            .map(|p| p.to_string())
            .collect();
        if under.len() > MAX_LISTING {
            out.push(format!("... and {} more files", under.len() - MAX_LISTING));
        }
        return out.join("\n");
    }
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut root: Vec<String> = Vec::new();
    for p in paths.iter().map(|p| p.as_ref()) {
        let parts: Vec<&str> = p.split('/').collect();
        match parts.len() {
            1 => root.push(p.to_string()),
            2 => *counts.entry(format!("{}/", parts[0])).or_default() += 1,
            _ => {
                *counts
                    .entry(format!("{}/{}/", parts[0], parts[1]))
                    .or_default() += 1
            }
        }
    }
    let mut out: Vec<String> = root;
    out.extend(counts.into_iter().map(|(d, n)| format!("{d} ({n} files)")));
    out.truncate(MAX_LISTING * 2);
    out.join("\n")
}

/// One section to write, with what it is written from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Source {
    pub id: String,
    pub listing: String,
    /// (path, content), each content cut at [`MAX_FILE_CHARS`].
    pub files: Vec<(String, String)>,
}

fn title_of(id: &str) -> String {
    match GLOBAL.iter().find(|(g, _, _)| *g == id) {
        Some((_, t, _)) => t.to_string(),
        None => format!(
            "Area `{}/`: structure and entry points",
            id.trim_start_matches(AREA_PREFIX)
        ),
    }
}

fn ask_of(id: &str) -> String {
    match GLOBAL.iter().find(|(g, _, _)| *g == id) {
        Some((_, _, a)) => a.to_string(),
        None => "what this folder holds, its packages or modules and their entry points (by path), where its code for each concern lives, and how it is tested".to_string(),
    }
}

/// The model's instructions for writing sections.
pub const SYSTEM: &str = "You write a repository's code map: short, factual sections an engineer \
reads before changing the code. Write only what the files and listings shown say; name paths, \
commands and versions exactly as they appear; never guess a command or a version. When the \
files don't say something, write that it isn't stated. Plain markdown, no code fences around a \
section, at most about 25 lines per section.";

/// The one request that writes every section in `sources`. Each section's
/// reply starts with a line `=== <id> ===`.
pub fn prompt(repo: &str, sources: &[Source]) -> String {
    let files: usize = sources.iter().map(|s| s.files.len()).sum();
    let per_file = (MAX_PROMPT_FILE_CHARS / files.max(1)).clamp(500, MAX_FILE_CHARS);
    let mut out = format!(
        "Repository: {repo}\n\nWrite these sections. Start each with its own line `=== <id> ===`, \
         exactly as given, then its text.\n"
    );
    for s in sources {
        out.push_str(&format!(
            "\n## Section `{}`: {}\nWhat it says: {}.\n\nPaths:\n{}\n",
            s.id,
            title_of(&s.id),
            ask_of(&s.id),
            s.listing
        ));
        for (path, content) in &s.files {
            out.push_str(&format!(
                "\n--- {path} ---\n{}\n",
                crate::truncate_str(content, per_file)
            ));
        }
    }
    out
}

/// The sections a reply wrote, among `wanted`: (id, text). A section the
/// reply left out, or left empty, is not in it.
pub fn parse_reply(reply: &str, wanted: &[String]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut cur: Option<(String, Vec<&str>)> = None;
    let flush = |cur: &mut Option<(String, Vec<&str>)>, out: &mut Vec<(String, String)>| {
        if let Some((id, lines)) = cur.take() {
            let text = lines.join("\n").trim().to_string();
            if !text.is_empty() && !out.iter().any(|(i, _)| *i == id) {
                out.push((id, text));
            }
        }
    };
    for line in reply.lines() {
        let t = line.trim();
        let marker = t
            .strip_prefix("===")
            .and_then(|r| r.strip_suffix("==="))
            .map(|r| r.trim().trim_matches('`').trim());
        if let Some(id) = marker {
            flush(&mut cur, &mut out);
            if wanted.iter().any(|w| w == id) {
                cur = Some((id.to_string(), Vec::new()));
            }
            continue;
        }
        if let Some((_, lines)) = cur.as_mut() {
            lines.push(line);
        }
    }
    flush(&mut cur, &mut out);
    out
}

/// The map after a refresh: kept sections as they were, rebuilt ones with
/// the new text stamped `base`, gone areas dropped, in a fixed order. A
/// section that was to be rebuilt and wasn't keeps its older stamp.
pub fn apply(
    map: Option<&CodeMap>,
    refresh: &Refresh,
    built: &[(String, String)],
    base: &str,
    tree: &str,
    now: &str,
) -> CodeMap {
    let mut by_id: BTreeMap<String, Section> = map
        .map(|m| {
            m.sections
                .iter()
                .filter(|s| !refresh.drop.contains(&s.id))
                .map(|s| (s.id.clone(), s.clone()))
                .collect()
        })
        .unwrap_or_default();
    for (id, text) in built {
        by_id.insert(
            id.clone(),
            Section {
                id: id.clone(),
                text: text.clone(),
                commit: base.to_string(),
            },
        );
    }
    // The map is at `base` now: sections the comparison didn't touch
    // describe it as well as they did the older commit.
    for id in &refresh.reuse {
        if let Some(s) = by_id.get_mut(id) {
            if !refresh.rebuild.contains(id) {
                s.commit = base.to_string();
            }
        }
    }
    let mut sections: Vec<Section> = GLOBAL
        .iter()
        .filter_map(|(id, _, _)| by_id.remove(*id))
        .collect();
    sections.extend(by_id.into_values());
    CodeMap {
        commit: base.to_string(),
        tree: tree.to_string(),
        sections,
        updated_at: now.to_string(),
    }
}

/// The map as an agent reads it, at most [`MAX_RENDER_CHARS`]. Empty for
/// no map.
pub fn render(map: &CodeMap) -> String {
    if map.sections.is_empty() {
        return String::new();
    }
    let short: String = map.commit.chars().take(7).collect();
    let behind = map.behind();
    let mut out = format!(
        "\n\n## What CoderHelm already knows about this repository\n\
         The repository's code map, kept current with its default branch at `{short}`. Start \
         from it instead of exploring the layout again: read only the files this change needs, \
         and check a detail in the code before you rely on it."
    );
    if !behind.is_empty() {
        out.push_str(&format!(
            " These sections describe an older commit and may be out of date: {}.",
            behind
                .iter()
                .map(|b| format!("`{b}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    out.push('\n');
    for s in &map.sections {
        let part = format!("\n### {}\n{}\n", title_of(&s.id), s.text.trim());
        if out.len() + part.len() > MAX_RENDER_CHARS {
            out.push_str("\n(Further sections left out for length.)\n");
            break;
        }
        out.push_str(&part);
    }
    out
}

// ── What a run saved ───────────────────────────────────────────────────────

/// What the planner's refresh did, kept on the run ([`REFRESH_FIELD`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshRecord {
    /// The commit the map was brought to.
    pub commit: String,
    pub why: String,
    pub reused: Vec<String>,
    pub rebuilt: Vec<String>,
    /// Sections to rebuild the reply left out (rebuilt next run).
    #[serde(default)]
    pub missed: Vec<String>,
}

/// How much the coder explored before its first write, kept on the run
/// ([`EXPLORATION_FIELD`]): with `map_commit` set it started from the map,
/// so runs with and without one can be compared.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exploration {
    /// Tool calls before the first file was written; `None` when it wrote
    /// nothing.
    pub calls_before_first_write: Option<u32>,
    /// All tool calls of the build.
    pub tool_calls: u32,
    /// The map's commit the coder was given, when it was given one.
    #[serde(default)]
    pub map_commit: Option<String>,
}

/// Counts the coder's tool calls until its first write. Shared by the
/// build's tool executor.
#[derive(Debug, Default)]
pub struct ExploreCounter {
    calls: std::sync::atomic::AtomicU32,
    first_write: std::sync::Mutex<Option<u32>>,
}

impl ExploreCounter {
    /// A tool call was made.
    pub fn call(&self) {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// A write went through: the calls up to it (itself not counted) are
    /// the exploration, kept once.
    pub fn wrote(&self) {
        let n = self.calls.load(std::sync::atomic::Ordering::Relaxed);
        let mut fw = self.first_write.lock().unwrap_or_else(|e| e.into_inner());
        if fw.is_none() {
            *fw = Some(n.saturating_sub(1));
        }
    }

    pub fn exploration(&self, map_commit: Option<String>) -> Exploration {
        Exploration {
            calls_before_first_write: *self.first_write.lock().unwrap_or_else(|e| e.into_inner()),
            tool_calls: self.calls.load(std::sync::atomic::Ordering::Relaxed),
            map_commit,
        }
    }
}

// ── Storage ────────────────────────────────────────────────────────────────

/// The map kept for `repo`, if any.
pub async fn load(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    repo: &str,
) -> Option<CodeMap> {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", A::S(team_id.to_string()))
        .key("sk", A::S(agent_file::sk(repo)))
        .projection_expression("#m")
        .expression_attribute_names("#m", FIELD)
        .send()
        .await
        .ok()?;
    out.item()?
        .get(FIELD)
        .and_then(|v| v.as_s().ok())
        .and_then(|s| CodeMap::from_json(s))
}

/// Keeps the map on the repository's agent file record (made when there is
/// none: a record without text is never regenerated, `agent_file::due`).
pub async fn save(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    repo: &str,
    map: &CodeMap,
) -> Result<(), String> {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    dynamo
        .update_item()
        .table_name(table)
        .key("pk", A::S(team_id.to_string()))
        .key("sk", A::S(agent_file::sk(repo)))
        .update_expression("SET #m = :m")
        .expression_attribute_names("#m", FIELD)
        .expression_attribute_values(":m", A::S(map.to_json()))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("code map write: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree() -> Vec<String> {
        [
            "README.md",
            ".github/pull_request_template.md",
            "package.json",
            "pnpm-workspace.yaml",
            ".github/workflows/ci.yml",
            "eslint.config.mjs",
            "apps/web/package.json",
            "apps/web/src/index.tsx",
            "apps/web/src/pages/home.tsx",
            "apps/web/src/pages/about.tsx",
            "apps/studio/package.json",
            "apps/studio/src/main.ts",
            "services/api/Cargo.toml",
            "services/api/src/main.rs",
            "services/api/src/routes/users.rs",
            "infra/main.tf",
            "infra/variables.tf",
        ]
        .map(String::from)
        .to_vec()
    }

    fn built_map() -> CodeMap {
        let areas = areas_of(&tree());
        let r = plan_refresh(None, "c1", "t1", &areas, None);
        let built: Vec<(String, String)> = r
            .rebuild
            .iter()
            .map(|id| (id.clone(), format!("text of {id}")))
            .collect();
        apply(None, &r, &built, "c1", "t1", "2026-11-18T00:00:00Z")
    }

    #[test]
    fn a_first_run_builds_every_section_stamped_with_its_commit() {
        let m = built_map();
        let ids: Vec<&str> = m.sections.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "architecture",
                "stack",
                "build",
                "conventions",
                "dependencies",
                "area:apps",
                "area:infra",
                "area:services"
            ]
        );
        assert!(m.sections.iter().all(|s| s.commit == "c1"));
        assert_eq!((m.commit.as_str(), m.tree.as_str()), ("c1", "t1"));
        assert!(m.behind().is_empty());
    }

    #[test]
    fn the_same_commit_or_tree_reuses_every_section() {
        let m = built_map();
        let areas = areas_of(&tree());
        let r = plan_refresh(Some(&m), "c1", "t1", &areas, None);
        assert!(r.is_noop(), "{r:?}");
        assert_eq!(r.reuse.len(), 8);
        // Another commit with byte-identical files (a revert, a merge of
        // nothing): the tree says so, no comparison needed.
        let r = plan_refresh(Some(&m), "c2", "t1", &areas, None);
        assert!(r.is_noop(), "{r:?}");
        assert_eq!(r.why, "current");
    }

    #[test]
    fn only_the_sections_a_change_touches_are_rebuilt() {
        let m = built_map();
        let areas = areas_of(&tree());
        // Plain edits inside source files: nothing to rebuild.
        let edits = vec![
            Changed::new("apps/web/src/pages/home.tsx", "modified"),
            Changed::new("services/api/src/routes/users.rs", "modified"),
        ];
        let r = plan_refresh(Some(&m), "c2", "t2", &areas, Some(&edits));
        assert!(r.rebuild.is_empty(), "{r:?}");
        // A new route file in the API and its entry point: that area only.
        let api = vec![
            Changed::new("services/api/src/routes/orders.rs", "added"),
            Changed::new("services/api/src/main.rs", "modified"),
        ];
        let r = plan_refresh(Some(&m), "c2", "t2", &areas, Some(&api));
        assert_eq!(r.rebuild, vec!["area:services".to_string()]);
        assert!(r.reuse.contains(&"area:apps".to_string()));
        // A bumped dependency in a package: stack and dependencies, and the
        // package's area.
        let dep = vec![Changed::new("apps/web/package.json", "modified")];
        let r = plan_refresh(Some(&m), "c2", "t2", &areas, Some(&dep));
        assert_eq!(
            r.rebuild,
            vec![
                "stack".to_string(),
                "dependencies".to_string(),
                "area:apps".to_string()
            ]
        );
        // CI and lint config.
        let ci = vec![
            Changed::new(".github/workflows/ci.yml", "modified"),
            Changed::new("eslint.config.mjs", "modified"),
        ];
        let r = plan_refresh(Some(&m), "c2", "t2", &areas, Some(&ci));
        assert_eq!(
            r.rebuild,
            vec!["build".to_string(), "conventions".to_string()]
        );
        // Infrastructure.
        let tf = vec![Changed::new("infra/queues.tf", "added")];
        let r = plan_refresh(Some(&m), "c2", "t2", &areas, Some(&tf));
        assert_eq!(
            r.rebuild,
            vec![
                "architecture".to_string(),
                "dependencies".to_string(),
                "area:infra".to_string()
            ]
        );
    }

    #[test]
    fn a_new_area_is_built_and_a_gone_one_dropped() {
        let m = built_map();
        let mut paths = tree();
        paths.retain(|p| !p.starts_with("infra/"));
        paths.push("e2e/home.spec.ts".into());
        let areas = areas_of(&paths);
        let changes = vec![
            Changed::new("infra/main.tf", "removed"),
            Changed::new("infra/variables.tf", "removed"),
            Changed::new("e2e/home.spec.ts", "added"),
        ];
        let r = plan_refresh(Some(&m), "c2", "t2", &areas, Some(&changes));
        assert!(r.rebuild.contains(&"area:e2e".to_string()), "{r:?}");
        assert!(r.rebuild.contains(&"architecture".to_string()));
        assert_eq!(r.drop, vec!["area:infra".to_string()]);
        let built = vec![
            ("architecture".to_string(), "new arch".to_string()),
            ("area:e2e".to_string(), "specs".to_string()),
            ("dependencies".to_string(), "deps".to_string()),
        ];
        let after = apply(Some(&m), &r, &built, "c2", "t2", "now");
        assert!(after.section("area:infra").is_none());
        assert_eq!(after.section("area:e2e").unwrap().commit, "c2");
        assert_eq!(
            after.section("area:apps").unwrap().text,
            "text of area:apps"
        );
        assert_eq!(after.section("area:apps").unwrap().commit, "c2");
        assert!(after.behind().is_empty());
    }

    #[test]
    fn unknown_changes_rebuild_everything_and_a_missed_section_is_retried() {
        let m = built_map();
        let areas = areas_of(&tree());
        let r = plan_refresh(Some(&m), "c2", "t2", &areas, None);
        assert_eq!(r.rebuild.len(), 8);
        assert_eq!(r.why, "unknown changes");
        // The reply wrote only two of them: the rest keep the old stamp and
        // the next run rebuilds them even with no change at all.
        let built = vec![
            ("architecture".to_string(), "a".to_string()),
            ("stack".to_string(), "s".to_string()),
        ];
        let after = apply(Some(&m), &r, &built, "c2", "t2", "now");
        assert_eq!(after.behind().len(), 6);
        let next = plan_refresh(Some(&after), "c2", "t2", &areas, None);
        assert_eq!(next.rebuild.len(), 6, "{next:?}");
        assert!(render(&after).contains("describe an older commit"));
    }

    #[test]
    fn a_cut_comparison_is_not_trusted() {
        let files: Vec<Value> = (0..COMPARE_FILE_LIMIT)
            .map(|i| serde_json::json!({"filename": format!("src/f{i}.rs"), "status": "modified"}))
            .collect();
        assert!(changed_from_compare(&serde_json::json!({ "files": files })).is_none());
        let small = serde_json::json!({"files": [
            {"filename": "b.rs", "status": "renamed", "previous_filename": "a.rs"}
        ]});
        let c = changed_from_compare(&small).unwrap();
        assert_eq!(c[0].previous.as_deref(), Some("a.rs"));
    }

    #[test]
    fn each_section_reads_its_own_sources() {
        let t = tree();
        assert_eq!(
            inputs_for("area:services", &t),
            vec!["services/api/Cargo.toml", "services/api/src/main.rs"]
        );
        assert!(inputs_for("build", &t).contains(&".github/workflows/ci.yml".to_string()));
        assert!(inputs_for("conventions", &t).contains(&"eslint.config.mjs".to_string()));
        assert!(inputs_for("dependencies", &t).contains(&"apps/web/package.json".to_string()));
        assert!(inputs_for("architecture", &t).contains(&"README.md".to_string()));
        assert!(listing_for("area:apps", &t).contains("apps/web/src/index.tsx"));
        assert!(!listing_for("area:apps", &t).contains("services/"));
    }

    #[test]
    fn replies_parse_by_marker_and_only_for_wanted_sections() {
        let wanted = vec!["stack".to_string(), "area:apps".to_string()];
        let reply = "Here you go.\n=== stack ===\nRust 1.86, Next.js 15.\n\n=== `area:apps` ===\n- `apps/web`: the site\n=== build ===\nnot asked\n=== stack ===\nagain";
        let got = parse_reply(reply, &wanted);
        assert_eq!(
            got,
            vec![
                ("stack".to_string(), "Rust 1.86, Next.js 15.".to_string()),
                (
                    "area:apps".to_string(),
                    "- `apps/web`: the site".to_string()
                ),
            ]
        );
        let p = prompt(
            "acme/shop",
            &[Source {
                id: "area:apps".into(),
                listing: "apps/web/src/index.tsx".into(),
                files: vec![("apps/web/package.json".into(), "{}".into())],
            }],
        );
        assert!(p.contains("=== <id> ===") && p.contains("--- apps/web/package.json ---"));
    }

    #[test]
    fn a_large_first_build_stays_within_its_budget() {
        let sources: Vec<Source> = (0..17)
            .map(|i| Source {
                id: format!("area:a{i}"),
                listing: String::new(),
                files: (0..10)
                    .map(|j| (format!("a{i}/f{j}.rs"), "x".repeat(10_000)))
                    .collect(),
            })
            .collect();
        let p = prompt("acme/big", &sources);
        assert!(p.len() < MAX_PROMPT_FILE_CHARS + 20_000, "{}", p.len());
    }

    #[test]
    fn the_counter_keeps_the_calls_before_the_first_write() {
        let c = ExploreCounter::default();
        for _ in 0..7 {
            c.call();
        }
        c.call(); // the write itself
        c.wrote();
        c.call();
        c.wrote();
        let e = c.exploration(Some("c1".into()));
        assert_eq!(e.calls_before_first_write, Some(7));
        assert_eq!(e.tool_calls, 9);
        assert_eq!(
            ExploreCounter::default()
                .exploration(None)
                .calls_before_first_write,
            None
        );
    }

    #[test]
    fn the_rendered_map_is_bounded_and_names_its_commit() {
        let m = built_map();
        let r = render(&m);
        assert!(r.contains("`c1`") && r.contains("### Tech stack and versions"));
        assert!(r.contains("### Area `services/`"));
        assert!(render(&CodeMap::default()).is_empty());
        let mut big = m.clone();
        for s in &mut big.sections {
            s.text = "x".repeat(6_000);
        }
        assert!(render(&big).len() <= MAX_RENDER_CHARS + 200);
        assert!(CodeMap::from_json(&m.to_json()) == Some(m));
    }
}
