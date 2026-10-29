//! Generated files are regenerated before the pull request opens.
//!
//! A pull request in a web repository changed its CMS schema files but not the
//! types generated from them; the repository has a type generation script,
//! and the reviewer had to point out the missing typegen. Generated output is
//! part of a complete change.
//!
//! This module finds the generators a repository uses ([`detect`]): package
//! scripts named like typegen/codegen/generate, Sanity typegen config, GraphQL
//! codegen config, OpenAPI client generators, Prisma, protobuf (buf), and any
//! rule the repository documents ("regenerate after changing X" in AGENTS.md
//! or CONTRIBUTING). Each comes with the inputs it reads and, where known, the
//! files it writes (globs). When the diff touches a generator's inputs, the
//! generator runs in the sandbox and its output is committed with the change
//! ([`gate`]). Before the pull request opens, a generator whose inputs changed
//! while its outputs did not (or whose run still changes tracked files) blocks
//! it with a plain reason, after one fix round.

use std::collections::HashMap;

use tracing::{info, warn};

use crate::clients::github::{FileOp, GitHubClient};
use crate::passes::plan_coverage::Work;

/// The tool a generator runs (a package script counts as the tool it runs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenKind {
    Sanity,
    GraphqlCodegen,
    Prisma,
    OpenApi,
    Protobuf,
    /// A rule the repository's docs state.
    Documented,
}

/// One generator, the files it reads and the files it writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generator {
    pub kind: GenKind,
    /// How a person reads it (`npm run typegen`, `graphql-codegen`).
    pub name: String,
    /// The command that regenerates, run from the repository root after the
    /// dependencies are installed.
    pub command: String,
    /// Globs of the files it reads.
    pub inputs: Vec<String>,
    /// Globs of the files it writes (empty when unknown).
    pub outputs: Vec<String>,
}

// ── Globs ─────────────────────────────────────────────────────────────────

fn expand_braces(p: &str) -> Vec<String> {
    let (Some(open), Some(close)) = (p.find('{'), p.find('}')) else {
        return vec![p.to_string()];
    };
    if close < open {
        return vec![p.to_string()];
    }
    let (head, rest) = (&p[..open], &p[close + 1..]);
    p[open + 1..close]
        .split(',')
        .flat_map(|alt| expand_braces(&format!("{head}{alt}{rest}")))
        .collect()
}

fn seg_match(pat: &[char], s: &[char]) -> bool {
    match (pat.first(), s.first()) {
        (None, None) => true,
        (Some('*'), _) => seg_match(&pat[1..], s) || (!s.is_empty() && seg_match(pat, &s[1..])),
        (Some('?'), Some(_)) => seg_match(&pat[1..], &s[1..]),
        (Some(a), Some(b)) if a == b => seg_match(&pat[1..], &s[1..]),
        _ => false,
    }
}

fn parts_match(pat: &[&str], path: &[&str]) -> bool {
    match pat.first() {
        None => path.is_empty(),
        Some(&"**") => (0..=path.len()).any(|i| parts_match(&pat[1..], &path[i..])),
        Some(p) => {
            !path.is_empty()
                && seg_match(
                    &p.chars().collect::<Vec<_>>(),
                    &path[0].chars().collect::<Vec<_>>(),
                )
                && parts_match(&pat[1..], &path[1..])
        }
    }
}

fn clean(p: &str) -> &str {
    p.trim().trim_start_matches("./").trim_start_matches('/')
}

/// Whether `path` matches the glob (`**`, `*`, `?`, `{a,b}`). A pattern that
/// names a directory (ends in `/`) matches everything under it.
pub fn glob_match(pattern: &str, path: &str) -> bool {
    let path = clean(path);
    expand_braces(clean(pattern)).iter().any(|p| {
        let p = if p.ends_with('/') {
            format!("{p}**")
        } else {
            p.clone()
        };
        let pat: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
        let parts: Vec<&str> = path.split('/').collect();
        parts_match(&pat, &parts)
    })
}

fn any_match(globs: &[String], path: &str) -> bool {
    globs.iter().any(|g| glob_match(g, path))
}

/// A glob relative to `dir` (a nested package's directory), as a path from
/// the repository root.
fn under(dir: &str, glob: &str) -> String {
    let g = clean(glob);
    if dir.is_empty() {
        g.to_string()
    } else {
        format!("{dir}/{g}")
    }
}

// ── What the repository holds ───────────────────────────────────────────────

/// The repository's file list and the text of the files detection reads.
#[derive(Debug, Clone, Default)]
pub struct RepoFiles {
    pub tree: Vec<String>,
    pub files: HashMap<String, String>,
}

const CODEGEN_CONFIGS: &[&str] = &[
    "codegen.yml",
    "codegen.yaml",
    "codegen.ts",
    "codegen.js",
    "codegen.json",
    ".graphqlrc.yml",
    ".graphqlrc.yaml",
];
const DOC_FILES: &[&str] = &[
    "AGENTS.md",
    "CLAUDE.md",
    "CONTRIBUTING.md",
    ".github/CONTRIBUTING.md",
    "docs/CONTRIBUTING.md",
];

fn base_name(p: &str) -> &str {
    p.rsplit('/').next().unwrap_or(p)
}

fn dir_of(p: &str) -> &str {
    p.rsplit_once('/').map(|(d, _)| d).unwrap_or("")
}

/// The files of `tree` detection reads: package manifests (not under
/// node_modules, at most three folders deep), generator configs and the
/// contribution docs.
pub fn candidate_files(tree: &[String]) -> Vec<String> {
    tree.iter()
        .filter(|p| !p.contains("node_modules/") && p.matches('/').count() <= 3)
        .filter(|p| {
            let b = base_name(p);
            b == "package.json"
                || b == "sanity-typegen.json"
                || b == "schema.prisma"
                || b == "buf.gen.yaml"
                || b == "openapitools.json"
                || b.starts_with("sanity.cli.")
                || b.starts_with("sanity.config.")
                || CODEGEN_CONFIGS.contains(&b)
                || DOC_FILES.contains(&p.as_str())
        })
        .cloned()
        .collect()
}

// ── Config readers (no YAML/TS parser: the few keys these configs use) ──────

fn indent(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

fn unquote(s: &str) -> String {
    s.trim()
        .trim_end_matches(',')
        .trim()
        .trim_matches(|c| c == '\'' || c == '"' || c == '`')
        .to_string()
}

/// The quoted strings in `text`.
fn quoted(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur: Option<(char, String)> = None;
    for c in text.chars() {
        match &mut cur {
            Some((q, s)) if c == *q => {
                out.push(std::mem::take(s));
                cur = None;
            }
            Some((_, s)) => s.push(c),
            None if matches!(c, '\'' | '"' | '`') => cur = Some((c, String::new())),
            None => {}
        }
    }
    out
}

/// The text of `key`'s value: what follows `key:` on its line and every more
/// indented line after it.
fn section<'a>(text: &'a str, key: &str) -> Option<Vec<&'a str>> {
    let lines: Vec<&str> = text.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let t = line.trim_start();
        let t = t.trim_start_matches(['\'', '"']);
        let Some(rest) = t.strip_prefix(key) else {
            continue;
        };
        let rest = rest.trim_start_matches(['\'', '"']);
        let Some(rest) = rest.strip_prefix(':') else {
            continue;
        };
        let ind = indent(line);
        let mut out = vec![rest];
        for l in &lines[i + 1..] {
            if l.trim().is_empty() {
                continue;
            }
            if indent(l) <= ind && !l.trim_start().starts_with(['-', ']', '}']) {
                break;
            }
            if indent(l) <= ind && l.trim_start().starts_with(['}', ']']) {
                break;
            }
            out.push(l);
        }
        return Some(out);
    }
    None
}

/// The values of a key that holds a path or a list of paths (YAML inline,
/// YAML list, or a JS/JSON string or array).
fn values(text: &str, key: &str) -> Vec<String> {
    let Some(sec) = section(text, key) else {
        return Vec::new();
    };
    let joined = sec.join("\n");
    let mut out = quoted(&joined);
    if out.is_empty() {
        let first = unquote(sec[0]);
        if !first.is_empty() && !first.starts_with(['[', '{']) {
            out.push(first);
        }
        for l in &sec[1..] {
            if let Some(v) = l.trim_start().strip_prefix("- ") {
                out.push(unquote(v));
            }
        }
    }
    out.into_iter()
        .filter(|v| !v.is_empty() && !v.contains("://") && !v.contains(' '))
        .collect()
}

/// The first-level keys under `key` (codegen's `generates:` map).
fn child_keys(text: &str, key: &str) -> Vec<String> {
    let Some(sec) = section(text, key) else {
        return Vec::new();
    };
    let child_indent = sec[1..].iter().map(|l| indent(l)).min();
    let mut out = Vec::new();
    for l in &sec[1..] {
        if Some(indent(l)) != child_indent {
            continue;
        }
        let t = l.trim();
        let k = if t.starts_with(['\'', '"', '`']) {
            quoted(t).into_iter().next()
        } else {
            t.split_once(':').map(|(k, _)| k.trim().to_string())
        };
        if let Some(k) = k.filter(|k| !k.is_empty() && (k.contains('/') || k.contains('.'))) {
            out.push(k);
        }
    }
    out
}

// ── Detection ─────────────────────────────────────────────────────────────

/// Script names that regenerate something.
fn is_generator_script(name: &str) -> bool {
    let n = name.to_lowercase();
    matches!(
        n.as_str(),
        "typegen"
            | "codegen"
            | "generate"
            | "gen"
            | "gen:types"
            | "generate:types"
            | "types:generate"
    ) || n.contains("typegen")
        || n.contains("codegen")
        || n.starts_with("generate:")
        || n.starts_with("gen:")
        || n.ends_with(":generate")
        || n.ends_with(":codegen")
}

fn sanity_globs(dir: &str, files: &HashMap<String, String>) -> (Vec<String>, Vec<String>) {
    let cfg = files.get(&under(dir, "sanity-typegen.json"));
    let mut inputs: Vec<String> = [
        "**/schemaTypes/**",
        "**/schemas/**",
        "**/schema/**",
        "**/sanity.config.*",
    ]
    .iter()
    .map(|g| under(dir, g))
    .collect();
    let mut outputs = vec![under(dir, "**/sanity.types.ts")];
    // Newer setups keep the typegen settings in the CLI config
    // (`typegen: { generates: './src/sanity.types.ts' }`).
    let cli = files
        .iter()
        .filter(|(p, _)| dir_of(p) == dir && base_name(p).starts_with("sanity.cli."))
        .map(|(_, t)| t);
    for text in cli {
        let gen = values(text, "generates");
        if !gen.is_empty() {
            outputs = gen.iter().map(|g| under(dir, g)).collect();
        }
        for p in values(text, "path") {
            inputs.push(under(dir, &p));
        }
    }
    if let Some(v) = cfg.and_then(|c| serde_json::from_str::<serde_json::Value>(c).ok()) {
        if let Some(p) = v["path"].as_str() {
            inputs.push(under(dir, p));
        }
        if let Some(g) = v["generates"].as_str() {
            outputs = vec![under(dir, g)];
        }
        if let Some(s) = v["schema"].as_str() {
            outputs.push(under(dir, s));
        }
    }
    (inputs, outputs)
}

fn codegen_globs(path: &str, text: &str) -> (Vec<String>, Vec<String>) {
    let dir = dir_of(path);
    let mut inputs: Vec<String> = values(text, "schema")
        .iter()
        .chain(values(text, "documents").iter())
        .map(|g| under(dir, g))
        .collect();
    inputs.push(path.to_string());
    let outputs = child_keys(text, "generates")
        .iter()
        .map(|g| under(dir, g))
        .collect();
    (inputs, outputs)
}

fn prisma_globs(dir: &str, tree: &[String]) -> (Vec<String>, Vec<String>) {
    let schemas: Vec<String> = tree
        .iter()
        .filter(|p| base_name(p) == "schema.prisma" && (dir.is_empty() || p.starts_with(dir)))
        .cloned()
        .collect();
    let inputs = if schemas.is_empty() {
        vec![under(dir, "**/schema.prisma")]
    } else {
        schemas
    };
    (inputs, Vec::new())
}

/// Paths an OpenAPI generator's command names: its spec (input) and the
/// file or folder after `-o`/`--output` (output).
fn openapi_globs(dir: &str, cmd: &str) -> (Vec<String>, Vec<String>) {
    let words: Vec<&str> = cmd.split_whitespace().collect();
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    for (i, w) in words.iter().enumerate() {
        let w = unquote(w);
        let prev = if i > 0 { words[i - 1] } else { "" };
        if matches!(prev, "-o" | "--output" | "--out" | "-O") {
            outputs.push(under(dir, &w));
        } else if (w.ends_with(".yaml") || w.ends_with(".yml") || w.ends_with(".json"))
            && !w.contains("://")
        {
            inputs.push(under(dir, &w));
        }
    }
    (inputs, outputs)
}

/// The inputs and outputs a script's command points to, by the tool it runs.
fn script_globs(
    dir: &str,
    cmd: &str,
    repo: &RepoFiles,
) -> Option<(GenKind, Vec<String>, Vec<String>)> {
    let c = cmd.to_lowercase();
    if c.contains("sanity") && (c.contains("typegen") || c.contains("schema extract")) {
        let (i, o) = sanity_globs(dir, &repo.files);
        return Some((GenKind::Sanity, i, o));
    }
    if c.contains("graphql-codegen") || c.contains("gql-gen") {
        let cfg = CODEGEN_CONFIGS
            .iter()
            .map(|n| under(dir, n))
            .find(|p| repo.files.contains_key(p))?;
        let (i, o) = codegen_globs(&cfg, &repo.files[&cfg]);
        return Some((GenKind::GraphqlCodegen, i, o));
    }
    if c.contains("prisma generate") {
        let (i, o) = prisma_globs(dir, &repo.tree);
        return Some((GenKind::Prisma, i, o));
    }
    if c.contains("openapi") || c.contains("orval") || c.contains("swagger") {
        let (i, o) = openapi_globs(dir, cmd);
        return (!i.is_empty()).then_some((GenKind::OpenApi, i, o));
    }
    if c.contains("buf generate") || c.contains("protoc") {
        return Some((
            GenKind::Protobuf,
            vec![under(dir, "**/*.proto")],
            Vec::new(),
        ));
    }
    None
}

/// A rule the docs state: a line that says to regenerate (or run a command)
/// after changing something, with the command and the paths in backticks.
fn documented(doc: &str) -> Vec<Generator> {
    let mut out = Vec::new();
    for line in doc.lines() {
        let l = line.to_lowercase();
        let says = l.contains("regenerat")
            || ((l.contains("run ") || l.contains("re-run"))
                && (l.contains("after")
                    || l.contains("whenever")
                    || l.contains("when you change")));
        if !says {
            continue;
        }
        let ticks: Vec<String> = line
            .split('`')
            .enumerate()
            .filter(|(i, _)| i % 2 == 1)
            .map(|(_, t)| t.trim().to_string())
            .collect();
        let starts = [
            "npm ",
            "pnpm ",
            "yarn ",
            "npx ",
            "bun ",
            "make ",
            "buf ",
            "go generate",
            "just ",
        ];
        let Some(command) = ticks
            .iter()
            .find(|t| starts.iter().any(|s| t.starts_with(s)))
            .cloned()
        else {
            continue;
        };
        let inputs: Vec<String> = ticks
            .iter()
            .filter(|t| **t != command && !t.contains(' ') && (t.contains('/') || t.contains('*')))
            .map(|t| clean(t).to_string())
            .collect();
        if !inputs.is_empty() {
            out.push(Generator {
                kind: GenKind::Documented,
                name: command.clone(),
                command,
                inputs,
                outputs: Vec::new(),
            });
        }
    }
    out
}

/// The package manager's install and run words for the repository.
fn package_manager(tree: &[String]) -> (&'static str, &'static str) {
    if tree.iter().any(|p| p == "pnpm-lock.yaml") {
        (
            "corepack enable && pnpm install --frozen-lockfile",
            "pnpm run",
        )
    } else if tree.iter().any(|p| p == "yarn.lock") {
        ("corepack enable && yarn install --frozen-lockfile", "yarn")
    } else if tree.iter().any(|p| p == "bun.lockb" || p == "bun.lock") {
        ("bun install --frozen-lockfile", "bun run")
    } else {
        ("npm ci --no-audit --no-fund", "npm run")
    }
}

/// The repository's generators, scripts first; a config a script already
/// runs is not counted twice.
pub fn detect(repo: &RepoFiles) -> Vec<Generator> {
    let (_, run) = package_manager(&repo.tree);
    let mut out: Vec<Generator> = Vec::new();
    let mut manifests: Vec<&String> = repo
        .files
        .keys()
        .filter(|p| base_name(p) == "package.json")
        .collect();
    manifests.sort();
    for path in manifests {
        let Ok(pkg) = serde_json::from_str::<serde_json::Value>(&repo.files[path]) else {
            continue;
        };
        let dir = dir_of(path);
        let Some(scripts) = pkg["scripts"].as_object() else {
            continue;
        };
        let mut names: Vec<&String> = scripts.keys().collect();
        names.sort();
        for name in names {
            if !is_generator_script(name) {
                continue;
            }
            let cmd = scripts[name].as_str().unwrap_or("");
            let Some((kind, inputs, outputs)) = script_globs(dir, cmd, repo) else {
                continue;
            };
            let command = if dir.is_empty() {
                format!("{run} {name}")
            } else {
                format!("cd {dir} && {run} {name}")
            };
            // The kind is the tool the script runs, so its config isn't
            // counted again below.
            out.push(Generator {
                kind,
                name: command.clone(),
                command,
                inputs,
                outputs,
            });
        }
    }
    let covered = |out: &[Generator], kind: GenKind| out.iter().any(|g| g.kind == kind);
    let mut configs: Vec<&String> = repo.files.keys().collect();
    configs.sort();
    for path in configs {
        let dir = dir_of(path);
        let text = &repo.files[path];
        let b = base_name(path);
        if CODEGEN_CONFIGS.contains(&b) && !covered(&out, GenKind::GraphqlCodegen) {
            let (inputs, outputs) = codegen_globs(path, text);
            out.push(Generator {
                kind: GenKind::GraphqlCodegen,
                name: "graphql-codegen".into(),
                command: format!("npx graphql-codegen --config {path}"),
                inputs,
                outputs,
            });
        } else if b == "sanity-typegen.json" && !covered(&out, GenKind::Sanity) {
            let (inputs, outputs) = sanity_globs(dir, &repo.files);
            out.push(Generator {
                kind: GenKind::Sanity,
                name: "sanity typegen".into(),
                command: if dir.is_empty() {
                    "npx sanity schema extract && npx sanity typegen generate".into()
                } else {
                    format!("cd {dir} && npx sanity schema extract && npx sanity typegen generate")
                },
                inputs,
                outputs,
            });
        } else if b == "schema.prisma" && !covered(&out, GenKind::Prisma) {
            out.push(Generator {
                kind: GenKind::Prisma,
                name: "prisma generate".into(),
                command: format!("npx prisma generate --schema {path}"),
                inputs: vec![path.clone()],
                outputs: Vec::new(),
            });
        } else if b == "buf.gen.yaml" && !covered(&out, GenKind::Protobuf) {
            out.push(Generator {
                kind: GenKind::Protobuf,
                name: "buf generate".into(),
                command: if dir.is_empty() {
                    "npx --yes @bufbuild/buf generate".into()
                } else {
                    format!("cd {dir} && npx --yes @bufbuild/buf generate")
                },
                inputs: vec![under(dir, "**/*.proto")],
                outputs: child_keys(text, "plugins")
                    .into_iter()
                    .chain(values(text, "out"))
                    .map(|o| under(dir, &format!("{}/", o.trim_end_matches('/'))))
                    .collect(),
            });
        } else if DOC_FILES.contains(&path.as_str()) {
            for g in documented(text) {
                if !out.iter().any(|o| o.command == g.command) {
                    out.push(g);
                }
            }
        }
    }
    out
}

/// The full command for the sandbox: install, then the generator.
pub fn sandbox_command(tree: &[String], g: &Generator) -> String {
    let (install, _) = package_manager(tree);
    let needs_node = g.command.starts_with("npm ")
        || g.command.starts_with("pnpm ")
        || g.command.starts_with("yarn")
        || g.command.starts_with("bun ")
        || g.command.starts_with("npx ")
        || g.command.contains("&& npx ")
        || g.command.contains("&& npm ")
        || g.command.contains("&& pnpm ")
        || g.command.contains("&& yarn");
    if needs_node && tree.iter().any(|p| p == "package.json") {
        format!("{install} && {}", g.command)
    } else {
        g.command.clone()
    }
}

/// One command that runs every generator the repository has, for the
/// coder's run_codegen tool: dependencies installed once, then each
/// generator in its own subshell from the repository root. `None` when it
/// has none.
pub fn all_command(repo: &RepoFiles) -> Option<String> {
    let gens = detect(repo);
    if gens.is_empty() {
        return None;
    }
    let (install, _) = package_manager(&repo.tree);
    let runs: Vec<String> = gens.iter().map(|g| format!("({})", g.command)).collect();
    let node = gens
        .iter()
        .any(|g| sandbox_command(&repo.tree, g) != g.command);
    Some(if node {
        format!("{install} && {}", runs.join(" && "))
    } else {
        runs.join(" && ")
    })
}

/// The generators whose inputs the change touches, with those files. A file
/// the generator itself writes is not an input change.
pub fn affected<'a>(
    gens: &'a [Generator],
    changed: &[String],
) -> Vec<(&'a Generator, Vec<String>)> {
    gens.iter()
        .filter_map(|g| {
            let hits: Vec<String> = changed
                .iter()
                .filter(|c| any_match(&g.inputs, c) && !any_match(&g.outputs, c))
                .cloned()
                .collect();
            (!hits.is_empty()).then_some((g, hits))
        })
        .collect()
}

/// The generator's known outputs are not in the change.
pub fn outputs_missing(g: &Generator, changed: &[String]) -> bool {
    !g.outputs.is_empty() && !changed.iter().any(|c| any_match(&g.outputs, c))
}

fn list(items: &[String], max: usize) -> String {
    let mut s: Vec<String> = items.iter().take(max).map(|i| format!("`{i}`")).collect();
    if items.len() > max {
        s.push(format!("{} more", items.len() - max));
    }
    s.join(", ")
}

/// Why the pull request doesn't open, in a sentence.
pub fn block_reason(g: &Generator, inputs: &[String], why: &str) -> String {
    format!(
        "The change edits {} (read by `{}`), but {why}",
        list(inputs, 3),
        g.name
    )
}

/// The one fix round's instructions.
pub fn fix_feedback(problems: &[(Generator, Vec<String>, String)]) -> String {
    let lines: Vec<String> = problems
        .iter()
        .map(|(g, inputs, why)| {
            format!(
                "- `{}` reads {} and {why} Regenerate its output{}: call run_codegen. Only when \
                 run_codegen is not offered, update the generated files ({}) exactly as the \
                 generator would. Never declare a type by hand for something the generator \
                 types: import the generated type.",
                g.name,
                list(inputs, 3),
                if g.outputs.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", list(&g.outputs, 3))
                },
                if g.outputs.is_empty() {
                    "the files it writes".to_string()
                } else {
                    list(&g.outputs, 3)
                }
            )
        })
        .collect();
    format!(
        "The change edits files a code generator reads, and the generated files are not up to \
         date. Generated output is part of the change:\n{}",
        lines.join("\n")
    )
}

// ── Generated types are used, never written in parallel ───────────────────

/// A type the change adds to a generator's inputs: the generator owns its
/// type, so code that uses it imports the generated one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedType {
    /// The name the input declares (`gallery`).
    pub name: String,
    /// The name the generated type has (`Gallery`).
    pub type_name: String,
    /// The input file that declares it.
    pub input: String,
}

/// The type name a generator gives a declared name: each word capitalised,
/// separators dropped (`gallery-block` and `galleryBlock` both read
/// `GalleryBlock`).
pub fn type_name_of(name: &str) -> String {
    name.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                Some(f) => f.to_ascii_uppercase().to_string() + c.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn added_lines(patch: &str) -> impl Iterator<Item = &str> {
    patch
        .lines()
        .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
        .map(|l| &l[1..])
}

/// The word after `kw` at the start of a trimmed line (`type Foo {`).
fn keyword_name(line: &str, kws: &[&str]) -> Option<String> {
    let t = line.trim_start();
    let t = t.strip_prefix("export ").unwrap_or(t).trim_start();
    for kw in kws {
        if let Some(rest) = t.strip_prefix(kw).and_then(|r| r.strip_prefix(' ')) {
            let n: String = rest
                .trim_start()
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !n.is_empty() {
                return Some(n);
            }
        }
    }
    None
}

/// The types an input file's diff adds, by the generator's kind.
pub fn added_types(kind: GenKind, status: &str, patch: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    match kind {
        GenKind::Sanity => {
            let lines: Vec<&str> = patch.lines().collect();
            for (i, l) in lines.iter().enumerate() {
                if !(l.starts_with('+') && l.contains("defineType(")) {
                    continue;
                }
                let name = lines[i..]
                    .iter()
                    .take(6)
                    .filter(|x| x.starts_with('+'))
                    .find_map(|x| super::feature::content::declared_name(x));
                if let Some(n) = name {
                    out.push(n);
                }
            }
            if out.is_empty() && status == "added" {
                if let Some(n) = added_lines(patch).find_map(super::feature::content::declared_name)
                {
                    out.push(n);
                }
            }
        }
        GenKind::GraphqlCodegen => out.extend(
            added_lines(patch)
                .filter_map(|l| keyword_name(l, &["type", "input", "enum", "interface", "union"])),
        ),
        GenKind::Prisma => {
            out.extend(added_lines(patch).filter_map(|l| keyword_name(l, &["model", "enum"])))
        }
        GenKind::Protobuf => {
            out.extend(added_lines(patch).filter_map(|l| keyword_name(l, &["message", "enum"])))
        }
        GenKind::OpenApi | GenKind::Documented => {}
    }
    out.dedup();
    out
}

/// The types the change adds to the inputs of `g`, from the diff as
/// (path, status, patch).
pub fn owned_types(g: &Generator, files: &[(String, String, String)]) -> Vec<OwnedType> {
    let mut out: Vec<OwnedType> = Vec::new();
    for (path, status, patch) in files {
        if status == "removed" || !any_match(&g.inputs, path) || any_match(&g.outputs, path) {
            continue;
        }
        for name in added_types(g.kind, status, patch) {
            let type_name = type_name_of(&name);
            if type_name.is_empty() || out.iter().any(|o| o.type_name == type_name) {
                continue;
            }
            out.push(OwnedType {
                name,
                type_name,
                input: path.clone(),
            });
        }
    }
    out
}

/// Whether the generated output has the type: its type name as a word, or
/// the declared name as a quoted literal (`_type: "gallery"`).
pub fn output_has(t: &OwnedType, output: &str) -> bool {
    let word = output
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .any(|w| w == t.type_name);
    word || output.contains(&format!("\"{}\"", t.name)) || output.contains(&format!("'{}'", t.name))
}

/// Suffixes a hand-written type for a thing carries (`GalleryProps`).
const TYPE_SUFFIXES: &[&str] = &[
    "Props", "Block", "Data", "Fields", "Type", "Schema", "Content", "Item", "Section", "Doc",
    "Document", "Value",
];

fn base_of(ident: &str) -> String {
    let mut s = ident.to_string();
    loop {
        let before = s.clone();
        for suf in TYPE_SUFFIXES {
            if s.len() > suf.len() && s.ends_with(suf) {
                s.truncate(s.len() - suf.len());
            }
        }
        if s == before {
            break;
        }
    }
    s.to_ascii_lowercase()
}

/// Whether a source file may declare types (not a test, not generated).
fn is_source(path: &str) -> bool {
    let l = path.to_ascii_lowercase();
    let code = [".ts", ".tsx", ".js", ".jsx", ".vue", ".svelte", ".astro"]
        .iter()
        .any(|e| l.ends_with(e));
    code && !l.ends_with(".d.ts")
        && !l.contains(".test.")
        && !l.contains(".spec.")
        && !l.contains("generated")
}

/// Types the change declares by hand for something a generator types:
/// `(file, declared name, owned type)`. A declaration built from the
/// generated type (`type GalleryProps = Gallery & {…}`) is not parallel.
pub fn hand_typed<'a>(
    owned: &'a [OwnedType],
    g: &Generator,
    files: &[(String, String, String)],
) -> Vec<(String, String, &'a OwnedType)> {
    let mut out = Vec::new();
    for (path, status, patch) in files {
        if status == "removed"
            || !is_source(path)
            || any_match(&g.outputs, path)
            || any_match(&g.inputs, path)
        {
            continue;
        }
        let added: Vec<&str> = added_lines(patch).collect();
        for (i, l) in added.iter().enumerate() {
            let Some(ident) = keyword_name(l, &["type", "interface"]) else {
                continue;
            };
            let Some(t) = owned
                .iter()
                .find(|t| base_of(&ident) == base_of(&t.type_name))
            else {
                continue;
            };
            // Built from the generated type: the declaration's own lines
            // (up to its closing brace) name it as a type.
            let mut decl: Vec<&str> = Vec::new();
            for (j, x) in added[i..].iter().take(8).enumerate() {
                decl.push(x);
                let t = x.trim();
                if (j > 0 && t.starts_with('}')) || (j == 0 && t.ends_with(';')) {
                    break;
                }
            }
            let body = decl.join(" ");
            let derived = names_type(&body.replacen(&ident, "", 1), &t.type_name);
            if !derived {
                out.push((path.clone(), ident, t));
            }
        }
    }
    out
}

/// Whether `text` uses `name` as a type: the whole word, not called
/// (`Gallery(` is a function).
fn names_type(text: &str, name: &str) -> bool {
    let b = text.as_bytes();
    let word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    text.match_indices(name).any(|(at, _)| {
        let before = at.checked_sub(1).map(|i| b[i]);
        let after = b.get(at + name.len()).copied();
        !before.is_some_and(word) && !after.is_some_and(|c| word(c) || c == b'(')
    })
}

/// What the generated output and the code that uses it must show for the
/// types the change adds: the output has each (else the generator didn't
/// pick it up), and no file declares its own copy. `outputs`: the text of
/// the generator's output files on the branch (empty when none could be
/// read, then only the hand-typed check runs).
pub fn ownership_problems(
    g: &Generator,
    files: &[(String, String, String)],
    outputs: &[(String, String)],
) -> Vec<String> {
    let owned = owned_types(g, files);
    if owned.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    if !outputs.is_empty() {
        for t in &owned {
            if !outputs.iter().any(|(_, text)| output_has(t, text)) {
                out.push(format!(
                    "its generated output ({}) has no `{}` for the `{}` that `{}` adds: the \
                     generator didn't pick it up.",
                    list(
                        &outputs.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(),
                        2
                    ),
                    t.type_name,
                    t.name,
                    t.input
                ));
            }
        }
    }
    let from = outputs
        .first()
        .map(|(p, _)| format!(" from `{p}`"))
        .unwrap_or_default();
    for (path, ident, t) in hand_typed(&owned, g, files) {
        out.push(format!(
            "`{path}` declares `{ident}` by hand for the `{}` the generator types: import the \
             generated `{}`{from} instead of a parallel type.",
            t.name, t.type_name
        ));
    }
    out
}

// ── Async: read the repo, run, check ────────────────────────────────────────

/// Reads what detection needs from the repository at `git_ref`.
pub async fn load(github: &GitHubClient, owner: &str, repo: &str, git_ref: &str) -> RepoFiles {
    let tree: Vec<String> = match github.get_tree(owner, repo, git_ref).await {
        Ok(t) => t
            .into_iter()
            .filter(|e| e.entry_type == "blob")
            .map(|e| e.path)
            .collect(),
        Err(e) => {
            warn!(error = %e, "Generator check could not read the tree");
            return RepoFiles::default();
        }
    };
    let mut files = HashMap::new();
    for p in candidate_files(&tree).into_iter().take(40) {
        if let Ok(text) = github.read_file(owner, repo, &p, git_ref).await {
            files.insert(p, text);
        }
    }
    RepoFiles { tree, files }
}

/// What a sandbox run of a generator came to.
enum Ran {
    /// It ran; these files changed (committed when there were any).
    Regenerated(Vec<String>),
    /// It ran and failed: why, and its output.
    Failed(String, String),
    /// The sandbox isn't set up, is out of time, or the team's policy holds
    /// the command.
    NotRun(String),
}

async fn run_in_sandbox(
    w: &Work<'_>,
    g: &Generator,
    cmd: &str,
    env: &[(String, String)],
    attempt: usize,
) -> Ran {
    let state = w.state;
    if state.config.sandbox_bucket_name.is_empty() || state.config.sandbox_project_name.is_empty() {
        return Ran::NotRun("no sandbox".into());
    }
    let gate = crate::agent::policy::gate(
        state,
        w.provider.scope.ctx(common::agent_policy::Agent::Coder),
        w.deadline,
    );
    match gate.check_command(cmd, "Regenerate generated files").await {
        common::agent_policy::Outcome::Run { .. } => {}
        _ => return Ran::NotRun("the team's agent permissions hold this command".into()),
    }
    let sandbox = crate::clients::sandbox::SandboxClient::new(
        &state.codebuild,
        &state.s3,
        &state.logs,
        &state.config.sandbox_bucket_name,
        &state.config.sandbox_project_name,
    )
    .with_egress(crate::agent::policy::sandbox_egress(state, &w.msg.team_id).await);
    if !sandbox.has_time(w.deadline) {
        return Ran::NotRun("not enough time left".into());
    }
    let (owner, name) = (&w.msg.repo_owner, &w.msg.repo_name);
    let head = match w.github.get_ref(owner, name, w.branch).await {
        Ok(h) => h,
        Err(e) => return Ran::NotRun(format!("branch head unreadable: {e}")),
    };
    let source = match sandbox
        .ensure_source(w.run_id, &head, || {
            w.github.download_tarball(owner, name, &head)
        })
        .await
    {
        Ok(s) => s,
        Err(e) => return Ran::NotRun(format!("snapshot failed: {e}")),
    };
    let node = super::detect_node_version(w.github, owner, name, w.branch).await;
    match sandbox
        .run_codegen(
            w.run_id,
            100 + attempt,
            &source,
            cmd,
            node.as_deref(),
            env,
            w.deadline,
        )
        .await
    {
        Ok(o) if o.ran && o.passed => {
            if o.changed.is_empty() {
                return Ran::Regenerated(Vec::new());
            }
            let paths: Vec<String> = o.changed.iter().map(|(p, _)| p.clone()).collect();
            let ops: Vec<FileOp> = o
                .changed
                .into_iter()
                .map(|(path, content)| FileOp::Write { path, content })
                .collect();
            let conventions =
                super::load_git_conventions(state, &w.msg.team_id, &w.msg.ticket_id).await;
            let message = conventions.commit_message(
                &super::ticket_tokens(w.msg),
                &format!("Regenerate generated files ({})", g.name),
            );
            match w
                .github
                .batch_write(owner, name, w.branch, &message, &ops)
                .await
            {
                Ok(_) => Ran::Regenerated(paths),
                Err(e) => Ran::Failed(
                    format!("its output couldn't be committed ({e})"),
                    String::new(),
                ),
            }
        }
        Ok(o) if o.ran => Ran::Failed(
            format!(
                "running it failed: {}",
                common::truncate_str(o.output.trim(), 300)
            ),
            o.output,
        ),
        Ok(o) => Ran::NotRun(o.output),
        Err(e) => Ran::NotRun(e.to_string()),
    }
}

/// What one pass over the affected generators found.
enum Pass {
    /// Generators still out of date (empty: all up to date).
    Problems(Vec<(Generator, Vec<String>, String)>),
    /// A generator failed for public values the codegen environment lacks:
    /// no fix round can settle that, a person sets them.
    MissingEnv(String),
}

/// Whether a generator runs through the repository's package scripts (the
/// worker's own codegen command already runs those).
fn is_script(g: &Generator) -> bool {
    let c = g.command.trim_start();
    let c = c.split("&& ").last().unwrap_or(c);
    ["npm run ", "pnpm run ", "yarn ", "bun run "]
        .iter()
        .any(|p| c.starts_with(p))
}

/// One pass over the affected generators: regenerate in the sandbox when it
/// can, and name each one still out of date. Package-script generators run
/// through the worker's codegen command (`detect_codegen_command`), once,
/// with the repository's public codegen environment (`load_codegen_env`).
async fn regenerate(w: &Work<'_>, attempt: usize, notes: &mut Vec<String>) -> Option<Pass> {
    let (owner, name) = (&w.msg.repo_owner, &w.msg.repo_name);
    let repo = load(w.github, owner, name, w.branch).await;
    let gens = detect(&repo);
    if gens.is_empty() {
        return None;
    }
    let mut changed =
        super::plan_coverage::changed_files(w.github, owner, name, &w.msg.base_branch, w.branch)
            .await?;
    let hits: Vec<(Generator, Vec<String>)> = affected(&gens, &changed)
        .into_iter()
        .map(|(g, i)| (g.clone(), i))
        .collect();
    if hits.is_empty() {
        return Some(Pass::Problems(Vec::new()));
    }
    let scripts_cmd = super::detect_codegen_command(w.github, owner, name, w.branch).await;
    let env = super::load_codegen_env(w.state, &w.msg.team_id, owner, name).await;
    let mut shared_run: Option<Ran> = None;
    let mut problems = Vec::new();
    for (i, (g, inputs)) in hits.into_iter().enumerate() {
        let ran = match (&scripts_cmd, is_script(&g)) {
            (Some(cmd), true) => match &shared_run {
                // The shared run already counted its files.
                Some(Ran::Regenerated(_)) => Ran::Regenerated(Vec::new()),
                Some(Ran::Failed(a, b)) => Ran::Failed(a.clone(), b.clone()),
                Some(Ran::NotRun(a)) => Ran::NotRun(a.clone()),
                None => {
                    let r = run_in_sandbox(w, &g, cmd, &env, attempt * 10 + i).await;
                    shared_run = Some(match &r {
                        Ran::Regenerated(p) => Ran::Regenerated(p.clone()),
                        Ran::Failed(a, b) => Ran::Failed(a.clone(), b.clone()),
                        Ran::NotRun(a) => Ran::NotRun(a.clone()),
                    });
                    r
                }
            },
            _ => {
                let cmd = sandbox_command(&repo.tree, &g);
                run_in_sandbox(w, &g, &cmd, &env, attempt * 10 + i).await
            }
        };
        match ran {
            Ran::Regenerated(paths) => {
                if paths.is_empty() {
                    notes.push(format!(
                        "`{}` ran; the generated files are up to date",
                        g.name
                    ));
                } else {
                    notes.push(format!(
                        "`{}` regenerated {} file(s) and they were committed",
                        g.name,
                        paths.len()
                    ));
                    changed.extend(paths);
                }
            }
            Ran::Failed(why, output) => {
                // Public values the generator reads that the repository's
                // codegen environment doesn't set: a person's to add.
                let refs: std::collections::BTreeSet<String> = repo
                    .files
                    .iter()
                    .filter(|(p, _)| {
                        let b = base_name(p);
                        b.starts_with("sanity.cli.")
                            || b.starts_with("sanity.config.")
                            || CODEGEN_CONFIGS.contains(&b)
                    })
                    .flat_map(|(_, t)| common::codegen_env::env_refs(t))
                    .collect();
                let missing = common::codegen_env::missing(&refs, &output, &env);
                if !missing.is_empty() {
                    let full = format!("{owner}/{name}");
                    record_missing_env(w, &full, &g.name, &missing).await;
                    return Some(Pass::MissingEnv(common::codegen_env::failure_reason(
                        &full, &g.name, &missing,
                    )));
                }
                problems.push((g, inputs, format!("{why}.")));
            }
            Ran::NotRun(why) => {
                info!(generator = %g.name, why, "Generator not run in the sandbox");
                if outputs_missing(&g, &changed) {
                    problems.push((
                        g.clone(),
                        inputs,
                        format!(
                            "its generated files ({}) were not regenerated.",
                            list(&g.outputs, 3)
                        ),
                    ));
                } else {
                    notes.push(format!(
                        "`{}` reads changed files and couldn't be run here ({why}); check its output",
                        g.name
                    ));
                }
            }
        }
    }
    // The new types are in the generated output and used from there:
    // a run that "passed" without picking up the schema, a hand-edited
    // output, or a parallel hand-written type are caught here.
    if let Ok(diff) = w
        .github
        .compare_files_with_patches(owner, name, &w.msg.base_branch, w.branch)
        .await
    {
        for g in &gens {
            if problems.iter().any(|(p, _, _)| p == g) {
                continue;
            }
            let mut outputs: Vec<(String, String)> = Vec::new();
            for p in repo
                .tree
                .iter()
                .filter(|p| any_match(&g.outputs, p))
                .take(3)
            {
                if let Ok(t) = w.github.read_file(owner, name, p, w.branch).await {
                    outputs.push((p.clone(), t));
                }
            }
            let found = ownership_problems(g, &diff, &outputs);
            if !found.is_empty() {
                let inputs = owned_types(g, &diff)
                    .into_iter()
                    .map(|t| t.input)
                    .collect::<Vec<_>>();
                problems.push((g.clone(), inputs, found.join(" ")));
            }
        }
    }
    Some(Pass::Problems(problems))
}

/// Keeps on the run which public values codegen was missing, for the run
/// page to show with a link to where they're set.
async fn record_missing_env(w: &Work<'_>, repo: &str, command: &str, missing: &[String]) {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let value = A::M(HashMap::from([
        ("repo".to_string(), A::S(repo.to_string())),
        ("command".to_string(), A::S(command.to_string())),
        (
            "vars".to_string(),
            A::L(missing.iter().map(|m| A::S(m.clone())).collect()),
        ),
    ]));
    if let Err(e) = w
        .state
        .dynamo
        .update_item()
        .table_name(&w.state.config.runs_table_name)
        .key("team_id", super::attr_s(&w.msg.team_id))
        .key("run_id", super::attr_s(w.run_id))
        .update_expression("SET codegen_env_missing = :m")
        .expression_attribute_values(":m", value)
        .send()
        .await
    {
        warn!(run_id = w.run_id, error = %e, "Could not record the missing codegen values");
    }
}

/// Before the pull request: generators whose inputs changed are run and
/// their output committed; one still out of date gets one fix round, then
/// blocks the pull request with a plain reason. Returns notes for the PR's
/// progress, or the reason.
pub async fn gate(
    w: &Work<'_>,
    usage: &mut crate::models::TokenUsage,
) -> Result<Vec<String>, String> {
    let mut notes = Vec::new();
    let problems = match regenerate(w, 0, &mut notes).await {
        None => return Ok(notes),
        Some(Pass::MissingEnv(why)) => return Err(why),
        Some(Pass::Problems(p)) if p.is_empty() => return Ok(notes),
        Some(Pass::Problems(p)) => p,
    };
    super::add_progress_note(
        w.state,
        &w.msg.team_id,
        w.run_id,
        &format!(
            "Generated files out of date for {}: one fix round",
            problems
                .iter()
                .map(|(g, _, _)| format!("`{}`", g.name))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )
    .await;
    let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "implement").await;
    if let Err(e) = super::implement::run(
        w.state,
        w.msg,
        w.github,
        w.plan,
        w.branch,
        w.rules,
        w.repo_instructions,
        Some(&fix_feedback(&problems)),
        "medium",
        w.provider,
        usage,
        w.file_cache,
        Some(w.run_id),
        w.deadline,
        // Generated files may not be in the PR yet.
        false,
    )
    .await
    {
        warn!(run_id = w.run_id, error = %e, "Generator fix round failed");
    }
    let mut notes2 = Vec::new();
    let still = match regenerate(w, 1, &mut notes2).await {
        Some(Pass::MissingEnv(why)) => return Err(why),
        Some(Pass::Problems(p)) => p,
        None => Vec::new(),
    };
    notes.extend(notes2);
    match still.first() {
        None => Ok(notes),
        Some((g, inputs, why)) => Err(format!(
            "Stopped before opening a pull request: {}",
            block_reason(g, inputs, why)
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo(files: &[(&str, &str)], extra_tree: &[&str]) -> RepoFiles {
        let mut tree: Vec<String> = files.iter().map(|(p, _)| p.to_string()).collect();
        tree.extend(extra_tree.iter().map(|p| p.to_string()));
        RepoFiles {
            tree,
            files: files
                .iter()
                .map(|(p, t)| (p.to_string(), t.to_string()))
                .collect(),
        }
    }

    fn changed(p: &[&str]) -> Vec<String> {
        p.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn globs() {
        assert!(glob_match(
            "**/schemaTypes/**",
            "studio/schemaTypes/blocks/hero.ts"
        ));
        assert!(glob_match("src/**/*.{ts,tsx}", "src/a/b/c.tsx"));
        assert!(!glob_match("src/**/*.{ts,tsx}", "src/a/b/c.js"));
        assert!(glob_match("./schema.graphql", "schema.graphql"));
        assert!(glob_match("src/gql/", "src/gql/graphql.ts"));
        assert!(glob_match("**/*.proto", "api/v1/user.proto"));
        assert!(!glob_match("*.ts", "src/a.ts"));
    }

    /// The observed case: a CMS schema changed, the typegen script exists,
    /// the generated types didn't change.
    #[test]
    fn npm_script_typegen_fixture() {
        let r = repo(
            &[
                (
                    "package.json",
                    r#"{"scripts":{"dev":"next dev","typegen":"sanity schema extract && sanity typegen generate"}}"#,
                ),
                (
                    "sanity-typegen.json",
                    r#"{"path":"./src/**/*.{ts,tsx}","schema":"schema.json","generates":"./src/sanity.types.ts"}"#,
                ),
            ],
            &[
                "pnpm-lock.yaml",
                "src/sanity.types.ts",
                "studio/schemaTypes/blocks/gallery.ts",
            ],
        );
        let gens = detect(&r);
        assert_eq!(gens.len(), 1, "{gens:?}");
        let g = &gens[0];
        assert_eq!(g.kind, GenKind::Sanity);
        assert_eq!(g.command, "pnpm run typegen");
        assert_eq!(
            sandbox_command(&r.tree, g),
            "corepack enable && pnpm install --frozen-lockfile && pnpm run typegen"
        );
        let diff = changed(&[
            "studio/schemaTypes/blocks/gallery.ts",
            "studio/schemaTypes/index.ts",
        ]);
        let hit = affected(&gens, &diff);
        assert_eq!(hit.len(), 1);
        assert_eq!(hit[0].1.len(), 2);
        assert!(outputs_missing(g, &diff));
        let why = block_reason(g, &hit[0].1, "its generated files were not regenerated.");
        assert!(why.contains("`pnpm run typegen`"));
        assert!(why.contains("gallery.ts"));
        // Regenerated: nothing missing.
        let mut with_types = diff.clone();
        with_types.push("src/sanity.types.ts".into());
        assert!(!outputs_missing(g, &with_types));
        // A change that touches no input affects nothing.
        assert!(affected(&gens, &changed(&["README.md"])).is_empty());
        // Editing only the generated file is not an input change.
        assert!(affected(&gens, &changed(&["src/sanity.types.ts"])).is_empty());
    }

    #[test]
    fn graphql_codegen_yaml_fixture() {
        let yml = "overwrite: true\nschema: \"schema.graphql\"\ndocuments:\n  - \"src/**/*.graphql\"\n  - \"src/queries/**/*.ts\"\ngenerates:\n  src/gql/graphql.ts:\n    plugins:\n      - typescript\n  ./src/gql/introspection.json:\n    plugins:\n      - introspection\n";
        let r = repo(
            &[("codegen.yml", yml)],
            &["schema.graphql", "package-lock.json"],
        );
        let gens = detect(&r);
        assert_eq!(gens.len(), 1);
        let g = &gens[0];
        assert_eq!(g.kind, GenKind::GraphqlCodegen);
        assert!(
            g.inputs.contains(&"schema.graphql".to_string()),
            "{:?}",
            g.inputs
        );
        assert!(g.inputs.contains(&"src/**/*.graphql".to_string()));
        assert_eq!(
            g.outputs,
            vec!["src/gql/graphql.ts", "src/gql/introspection.json"]
        );
        let diff = changed(&["src/queries/user.ts"]);
        assert_eq!(affected(&gens, &diff).len(), 1);
        assert!(outputs_missing(g, &diff));
        assert!(!outputs_missing(
            g,
            &changed(&["src/queries/user.ts", "src/gql/graphql.ts"])
        ));
        assert!(sandbox_command(&r.tree, g).contains("npx graphql-codegen --config codegen.yml"));
    }

    #[test]
    fn graphql_codegen_ts_fixture_through_a_script() {
        let ts = "import type { CodegenConfig } from '@graphql-codegen/cli';\n\nconst config: CodegenConfig = {\n  schema: 'https://api.example.com/graphql',\n  documents: ['app/**/*.tsx', 'lib/**/*.ts'],\n  generates: {\n    './lib/gql/': {\n      preset: 'client',\n    },\n  },\n};\nexport default config;\n";
        let r = repo(
            &[
                (
                    "web/package.json",
                    r#"{"scripts":{"codegen":"graphql-codegen --config codegen.ts"}}"#,
                ),
                ("web/codegen.ts", ts),
            ],
            &[],
        );
        let gens = detect(&r);
        assert_eq!(gens.len(), 1, "{gens:?}");
        let g = &gens[0];
        assert_eq!(g.command, "cd web && npm run codegen");
        // A remote schema is not a file input; the documents are.
        assert!(
            g.inputs.contains(&"web/app/**/*.tsx".to_string()),
            "{:?}",
            g.inputs
        );
        assert!(!g.inputs.iter().any(|i| i.contains("https")));
        assert_eq!(g.outputs, vec!["web/lib/gql/"]);
        assert_eq!(affected(&gens, &changed(&["web/app/page.tsx"])).len(), 1);
        assert!(!outputs_missing(
            g,
            &changed(&["web/app/page.tsx", "web/lib/gql/graphql.ts"])
        ));
    }

    #[test]
    fn prisma_openapi_protobuf_and_documented_rules() {
        let r = repo(
            &[
                ("package.json", r#"{"scripts":{"generate:api":"openapi-typescript spec/openapi.yaml -o src/api/schema.d.ts","db:generate":"prisma generate"}}"#),
                ("prisma/schema.prisma", "model User { id Int @id }"),
                ("proto/buf.gen.yaml", "version: v2\nplugins:\n  - remote: buf.build/protocolbuffers/go\n    out: gen/go\n"),
                ("AGENTS.md", "## Rules\n- Regenerate the client with `make client` after changing `api/spec/**`.\n- Use tabs.\n"),
            ],
            &[],
        );
        let gens = detect(&r);
        let by = |k: GenKind| gens.iter().find(|g| g.kind == k).cloned();
        let api = gens
            .iter()
            .find(|g| g.command.contains("generate:api"))
            .unwrap();
        assert_eq!(api.inputs, vec!["spec/openapi.yaml"]);
        assert_eq!(api.outputs, vec!["src/api/schema.d.ts"]);
        let prisma = gens
            .iter()
            .find(|g| g.command.contains("db:generate"))
            .unwrap();
        assert_eq!(prisma.inputs, vec!["prisma/schema.prisma"]);
        // The script already runs prisma: the schema file isn't a second one.
        assert_eq!(gens.iter().filter(|g| g.kind == GenKind::Prisma).count(), 1);
        let buf = by(GenKind::Protobuf).unwrap();
        assert_eq!(buf.inputs, vec!["proto/**/*.proto"]);
        assert_eq!(buf.outputs, vec!["proto/gen/go/"]);
        let doc = by(GenKind::Documented).unwrap();
        assert_eq!(doc.command, "make client");
        assert_eq!(doc.inputs, vec!["api/spec/**"]);
        assert_eq!(affected(&gens, &changed(&["api/spec/users.yaml"])).len(), 1);
        // Unknown outputs never block on their own; the sandbox run decides.
        assert!(!outputs_missing(&doc, &changed(&["api/spec/users.yaml"])));
        assert_eq!(sandbox_command(&r.tree, &doc), "make client");
    }

    #[test]
    fn scripts_that_generate_nothing_known_are_not_guessed() {
        let r = repo(
            &[(
                "package.json",
                r#"{"scripts":{"generate":"node scripts/sitemap.js","build":"next build"}}"#,
            )],
            &[],
        );
        assert!(detect(&r).is_empty());
        assert!(is_generator_script("gen:types"));
        assert!(is_generator_script("sanity:typegen"));
        assert!(!is_generator_script("build"));
    }

    #[test]
    fn only_manifests_configs_and_docs_are_read() {
        let tree = changed(&[
            "package.json",
            "node_modules/x/package.json",
            "apps/studio/package.json",
            "apps/studio/sanity-typegen.json",
            "codegen.yml",
            "AGENTS.md",
            "src/index.ts",
        ]);
        let c = candidate_files(&tree);
        assert!(c.contains(&"apps/studio/sanity-typegen.json".to_string()));
        assert!(!c.iter().any(|p| p.contains("node_modules")));
        assert!(!c.contains(&"src/index.ts".to_string()));
    }

    #[test]
    fn package_script_generators_run_through_the_workers_codegen_command() {
        let g = |c: &str| Generator {
            kind: GenKind::Sanity,
            name: c.into(),
            command: c.into(),
            inputs: vec![],
            outputs: vec![],
        };
        assert!(is_script(&g("pnpm run typegen")));
        assert!(is_script(&g("cd apps/studio && npm run codegen")));
        assert!(!is_script(&g("npx graphql-codegen --config codegen.yml")));
        assert!(!is_script(&g("make client")));
        // Sanity's own config files are read for the env values they need.
        assert_eq!(
            candidate_files(&["apps/studio/sanity.cli.ts".to_string()]).len(),
            1
        );
    }

    #[test]
    fn run_codegen_runs_every_generator_from_the_root() {
        let r = repo(
            &[
                (
                    "codegen.yml",
                    "schema: schema.graphql\ngenerates:\n  src/gql.ts:\n    plugins: []\n",
                ),
                ("prisma/schema.prisma", "model A { id Int @id }"),
            ],
            &["package.json", "package-lock.json"],
        );
        assert_eq!(
            all_command(&r).unwrap(),
            "npm ci --no-audit --no-fund && (npx graphql-codegen --config codegen.yml) && (npx prisma generate --schema prisma/schema.prisma)"
        );
        assert!(all_command(&RepoFiles::default()).is_none());
    }

    fn sanity_gen() -> Generator {
        Generator {
            kind: GenKind::Sanity,
            name: "npm run typegen".into(),
            command: "npm run typegen".into(),
            inputs: vec!["**/schemaTypes/**".into()],
            outputs: vec!["src/sanity.types.ts".into()],
        }
    }

    /// The observed case: a new block in the CMS schema, the generated
    /// union without it, and its props typed by hand in the component.
    #[test]
    fn a_new_schema_type_must_be_generated_and_imported() {
        let g = sanity_gen();
        let files = vec![
            (
                "studio/schemaTypes/blocks/gallery.ts".to_string(),
                "added".to_string(),
                "@@\n+import { defineType } from 'sanity'\n+export default defineType({\n+  name: 'gallery',\n+  type: 'object',\n+  fields: [{ name: 'title', type: 'string' }],\n+})\n".to_string(),
            ),
            (
                "src/components/blocks/Gallery.tsx".to_string(),
                "added".to_string(),
                "@@\n+export interface GalleryProps {\n+  title?: string\n+}\n+export function Gallery(p: GalleryProps) {}\n".to_string(),
            ),
        ];
        let owned = owned_types(&g, &files);
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].type_name, "Gallery");
        let stale = vec![(
            "src/sanity.types.ts".to_string(),
            "export type Hero = { _type: \"hero\" }\nexport type Block = Hero\n".to_string(),
        )];
        let p = ownership_problems(&g, &files, &stale);
        assert_eq!(p.len(), 2, "{p:?}");
        assert!(p[0].contains("has no `Gallery`"));
        assert!(p[1].contains("declares `GalleryProps` by hand"));
        assert!(p[1].contains("from `src/sanity.types.ts`"));

        // Regenerated, and the component builds on the generated type.
        let fresh = vec![(
            "src/sanity.types.ts".to_string(),
            "export type Gallery = { _type: \"gallery\"; title?: string }\n".to_string(),
        )];
        let mut good = files.clone();
        good[1].2 = "@@\n+import type { Gallery } from '@/sanity.types'\n+export type GalleryProps = Gallery & { className?: string }\n".into();
        assert!(ownership_problems(&g, &good, &fresh).is_empty());
        // An unrelated type is never flagged.
        let other = vec![(
            "src/components/Nav.tsx".to_string(),
            "modified".to_string(),
            "@@\n+type NavProps = { open: boolean }\n".to_string(),
        )];
        assert!(hand_typed(&owned, &g, &other).is_empty());
    }

    #[test]
    fn schema_languages_name_their_new_types() {
        assert_eq!(
            added_types(
                GenKind::GraphqlCodegen,
                "modified",
                "@@\n+type Gallery {\n+  id: ID!\n+}\n+extend type Query { x: Int }\n"
            ),
            vec!["Gallery".to_string()]
        );
        assert_eq!(
            added_types(GenKind::Prisma, "modified", "@@\n+model Booking {\n"),
            vec!["Booking".to_string()]
        );
        assert_eq!(
            added_types(
                GenKind::Sanity,
                "modified",
                "@@\n   fields: [\n+    defineField({ name: 'caption', type: 'string' }),\n"
            ),
            Vec::<String>::new()
        );
        assert_eq!(type_name_of("gallery-block"), "GalleryBlock");
        assert_eq!(type_name_of("galleryBlock"), "GalleryBlock");
    }

    #[test]
    fn typegen_settings_in_the_sanity_cli_config_are_read() {
        let r = repo(
            &[
                (
                    "package.json",
                    r#"{"scripts":{"typegen":"sanity schema extract && sanity typegen generate"}}"#,
                ),
                (
                    "sanity.cli.ts",
                    "export default defineCliConfig({\n  typegen: {\n    path: './app/**/*.{ts,tsx}',\n    generates: './app/types/cms.generated.ts',\n  },\n})\n",
                ),
            ],
            &["studio/schemaTypes/gallery.ts"],
        );
        let gens = detect(&r);
        assert_eq!(
            gens[0].outputs,
            vec!["app/types/cms.generated.ts".to_string()]
        );
        assert!(outputs_missing(
            &gens[0],
            &changed(&["studio/schemaTypes/gallery.ts"])
        ));
        assert!(!outputs_missing(
            &gens[0],
            &changed(&[
                "studio/schemaTypes/gallery.ts",
                "app/types/cms.generated.ts"
            ])
        ));
    }

    #[test]
    fn the_fix_round_names_the_generator_and_its_outputs() {
        let g = Generator {
            kind: GenKind::Documented,
            name: "npm run typegen".into(),
            command: "npm run typegen".into(),
            inputs: vec!["schemas/**".into()],
            outputs: vec!["sanity.types.ts".into()],
        };
        let fb = fix_feedback(&[(
            g,
            vec!["schemas/a.ts".into()],
            "its generated files were not regenerated.".into(),
        )]);
        assert!(fb.contains("`npm run typegen`"));
        assert!(fb.contains("run_codegen"));
        assert!(fb.contains("`sanity.types.ts`"));
    }
}
