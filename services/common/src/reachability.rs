//! Reachability without the model: whether a vulnerable package is loaded
//! by the repository's own code, read from the code itself.
//!
//! One pass over the repository's files answers every package of a tick
//! ([`Scanner`]): the import lines that load the package (or a subpath of
//! it) in source that runs (test, fixture and build-tool files apart), the
//! lines that use what was imported, the advisory's vulnerable functions in
//! those files, and the places that load a package by name without an
//! import (a config file, a `package.json` script, a quoted plugin name).
//! The lockfile says which direct dependencies bring a transitive package
//! in ([`lock_ancestry`]).
//!
//! [`decide`] then settles what the lines settle, with the files and lines
//! as its evidence:
//!
//! - imported, and the advisory's functions are called: reachable;
//! - imported, the advisory names functions, and none is called: present,
//!   not reachable;
//! - neither the package nor any direct dependency that brings it in is
//!   imported or named anywhere: present, not reachable;
//! - otherwise the lines go to the model ([`Decision::Ask`]), and only when
//!   there are lines to read: a package with nothing to show stays unknown
//!   with the reason, at no cost.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use regex::Regex;
use serde_json::Value;

use crate::security_fixes::{Evidence, EvidenceLine, Verdict};

/// Lines of one kind kept per package.
const MAX_LINES: usize = 24;
/// Usage lines kept per importing file.
const USES_PER_FILE: usize = 6;
/// Characters of one evidence line.
const LINE_CHARS: usize = 220;

// ── Files ───────────────────────────────────────────────────────────────

/// Folders whose files never count: installed packages, build output,
/// caches. `vendor/` is the dependency's own copy, not the repo's use of it.
fn skipped(path: &str) -> bool {
    let p = format!("/{}", path.to_ascii_lowercase());
    [
        "/node_modules/",
        "/vendor/",
        "/bower_components/",
        "/dist/",
        "/.next/",
        "/.nuxt/",
        "/.output/",
        "/.svelte-kit/",
        "/coverage/",
        "/__pycache__/",
        "/.venv/",
        "/venv/",
        "/site-packages/",
        "/target/",
        "/.git/",
        "/.github/",
        "/.yarn/",
    ]
    .iter()
    .any(|d| p.contains(d))
        || p.ends_with(".min.js")
        || p.ends_with(".map")
        || p.ends_with(".d.ts")
}

/// Test, fixture, story and end-to-end files: they import packages that
/// never load in production.
pub fn is_test_path(path: &str) -> bool {
    let p = format!("/{}", path.to_ascii_lowercase());
    let name = p.rsplit('/').next().unwrap_or("");
    [
        "/test/",
        "/tests/",
        "/__tests__/",
        "/__mocks__/",
        "/spec/",
        "/specs/",
        "/e2e/",
        "/cypress/",
        "/playwright/",
        "/fixtures/",
        "/testdata/",
        "/test-utils/",
        "/.storybook/",
        "/stories/",
    ]
    .iter()
    .any(|d| p.contains(d))
        || name.contains(".test.")
        || name.contains(".spec.")
        || name.contains(".stories.")
        || name.contains(".story.")
        || name.ends_with("_test.go")
        || name.ends_with("_test.py")
        || (name.starts_with("test_") && name.ends_with(".py"))
        || name == "conftest.py"
        || name.starts_with("jest.")
        || name.starts_with("vitest.")
        || name.starts_with("playwright.")
        || name.starts_with("cypress.")
}

/// Build and lint configuration (`vite.config.ts`, `.eslintrc.js`,
/// `babel.config.js`): what it imports runs at build time. Counted as a
/// mention, so the model reads it, never as a runtime import.
pub fn is_tooling_config(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    let stem = name.split('.').next().unwrap_or("");
    name.starts_with('.')
        || (name.contains(".config.")
            && !matches!(stem, "next" | "nuxt" | "app" | "server" | "astro"))
        || matches!(
            stem,
            "gulpfile" | "gruntfile" | "webpack" | "rollup" | "esbuild" | "tsup"
        )
}

/// The ecosystem a code file belongs to, by extension.
fn code_ecosystem(path: &str) -> Option<&'static str> {
    let ext = path.rsplit_once('.')?.1.to_ascii_lowercase();
    Some(match ext.as_str() {
        "js" | "jsx" | "ts" | "tsx" | "mjs" | "cjs" | "mts" | "cts" | "vue" | "svelte"
        | "astro" => "npm",
        "py" => "PyPI",
        "go" => "Go",
        "rs" => "crates.io",
        "java" | "kt" | "kts" | "scala" | "groovy" => "Maven",
        "cs" | "fs" | "vb" => "NuGet",
        "rb" => "RubyGems",
        "php" => "Packagist",
        _ => return None,
    })
}

/// Files where a package can be loaded by name without an import line.
fn is_config_text(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    if crate::security_fixes::is_dependency_file(path) && name != "package.json" {
        return false;
    }
    if name.starts_with(".env") {
        return false;
    }
    let ext = name.rsplit_once('.').map(|(_, e)| e).unwrap_or("");
    crate::security_fixes::is_dockerfile(path)
        || matches!(name.as_str(), "procfile" | "makefile" | "package.json")
        || matches!(
            ext,
            "json" | "yml" | "yaml" | "toml" | "cfg" | "ini" | "conf" | "sh"
        )
        || name.starts_with(".babelrc")
        || name.starts_with(".eslintrc")
        || name.starts_with(".postcssrc")
}

/// Ecosystems where "no file imports it" settles that the code never loads
/// it. Elsewhere packages load without an import naming them (Bundler,
/// Composer's autoload, Java packages that don't match the artifact).
pub fn absence_is_conclusive(ecosystem: &str) -> bool {
    matches!(ecosystem, "npm" | "PyPI" | "Go" | "crates.io")
}

// ── Names ───────────────────────────────────────────────────────────────

/// PyPI distributions whose import name isn't the distribution's name.
const PYPI_MODULES: &[(&str, &[&str])] = &[
    ("beautifulsoup4", &["bs4"]),
    ("pillow", &["PIL"]),
    ("pyyaml", &["yaml"]),
    ("python-dateutil", &["dateutil"]),
    ("scikit-learn", &["sklearn"]),
    ("opencv-python", &["cv2"]),
    ("opencv-python-headless", &["cv2"]),
    ("opencv-contrib-python", &["cv2"]),
    ("protobuf", &["google.protobuf"]),
    ("pyjwt", &["jwt"]),
    ("python-jose", &["jose"]),
    ("pycryptodome", &["Crypto"]),
    ("pycryptodomex", &["Cryptodome"]),
    ("python-multipart", &["multipart", "python_multipart"]),
    ("msgpack-python", &["msgpack"]),
    ("attrs", &["attr", "attrs"]),
    ("setuptools", &["setuptools", "pkg_resources"]),
    ("mysqlclient", &["MySQLdb"]),
    ("psycopg2-binary", &["psycopg2"]),
    ("grpcio", &["grpc"]),
    ("dnspython", &["dns"]),
    ("gitpython", &["git"]),
    ("pyopenssl", &["OpenSSL"]),
    ("python-ldap", &["ldap"]),
    ("djangorestframework", &["rest_framework"]),
    ("pysaml2", &["saml2"]),
    ("python-socketio", &["socketio"]),
    ("python-engineio", &["engineio"]),
    ("websocket-client", &["websocket"]),
    ("ruamel.yaml", &["ruamel"]),
    ("sentry-sdk", &["sentry_sdk"]),
    ("pywin32", &["win32api", "win32con", "pywintypes"]),
];

/// The names code imports a package by.
pub fn import_names(ecosystem: &str, package: &str) -> Vec<String> {
    match ecosystem {
        "PyPI" => {
            let lower = package.to_ascii_lowercase().replace('_', "-");
            if let Some((_, m)) = PYPI_MODULES.iter().find(|(p, _)| *p == lower) {
                return m.iter().map(|s| s.to_string()).collect();
            }
            if let Some(rest) = lower.strip_prefix("google-cloud-") {
                return vec![format!("google.cloud.{}", rest.replace('-', "_"))];
            }
            vec![lower.replace(['-', '.'], "_")]
        }
        "crates.io" => vec![package.replace('-', "_")],
        "Maven" => {
            // `group:artifact`: Java packages usually start with the group.
            let group = package.split(':').next().unwrap_or(package);
            vec![group.to_string()]
        }
        _ => vec![package.to_string()],
    }
}

/// Whether an import specifier loads `name` (the package, or a path in it).
fn spec_matches(ecosystem: &str, spec: &str, name: &str) -> bool {
    match ecosystem {
        "npm" | "Go" => spec == name || spec.strip_prefix(name).is_some_and(|r| r.starts_with('/')),
        "PyPI" | "Maven" | "NuGet" => {
            let (s, n) = if ecosystem == "PyPI" {
                (spec.to_string(), name.to_string())
            } else {
                (spec.to_ascii_lowercase(), name.to_ascii_lowercase())
            };
            s == n || s.strip_prefix(&n).is_some_and(|r| r.starts_with('.'))
        }
        "crates.io" => spec == name,
        "RubyGems" => {
            let n = name.to_ascii_lowercase();
            let s = spec.to_ascii_lowercase();
            s == n
                || s.strip_prefix(&n).is_some_and(|r| r.starts_with('/'))
                || s == n.replace('-', "/")
        }
        _ => false,
    }
}

/// The npm package an import specifier names (`@scope/pkg/sub` is
/// `@scope/pkg`); None for a relative path or a Node built-in.
fn npm_package_of(spec: &str) -> Option<&str> {
    if spec.starts_with('.') || spec.starts_with('/') || spec.starts_with("node:") {
        return None;
    }
    let mut parts = spec.splitn(3, '/');
    let first = parts.next()?;
    if first.starts_with('@') {
        let second = parts.next()?;
        Some(&spec[..first.len() + 1 + second.len()])
    } else {
        Some(first)
    }
}

/// An identifier the advisory names, as code calls it
/// (`Parser.onHeadersComplete` is `onHeadersComplete`).
pub fn symbol_name(sym: &str) -> Option<String> {
    let last = sym
        .rsplit(['.', ':', ')', '#', '/'])
        .find(|p| !p.trim().is_empty())
        .unwrap_or(sym)
        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '$')
        .to_string();
    (last.len() >= 3).then_some(last)
}

/// Identifiers an advisory's text names in backticks or as calls
/// (`` `parseUrl()` ``, `merge()`): hints for the model's evidence, never a
/// verdict on their own.
pub fn hints_from_text(text: &str, package: &str) -> Vec<String> {
    static STOP: &[&str] = &[
        "the",
        "and",
        "this",
        "that",
        "with",
        "from",
        "true",
        "false",
        "null",
        "undefined",
        "npm",
        "node",
        "http",
        "https",
        "json",
        "version",
        "versions",
        "package",
        "function",
        "object",
        "string",
        "require",
        "import",
        "export",
        "default",
        "const",
        "return",
    ];
    let re = Regex::new(
        r"`([A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*)(?:\(\))?`|\b([A-Za-z_$][\w$]{2,})\(\)",
    )
    .expect("valid");
    let mut out: Vec<String> = vec![];
    for c in re.captures_iter(text) {
        let raw = c
            .get(1)
            .or_else(|| c.get(2))
            .map(|m| m.as_str())
            .unwrap_or("");
        let Some(name) = symbol_name(raw) else {
            continue;
        };
        if name.eq_ignore_ascii_case(package)
            || STOP.contains(&name.to_ascii_lowercase().as_str())
            || name.chars().all(|c| c.is_ascii_digit() || c == '.')
        {
            continue;
        }
        if !out.contains(&name) {
            out.push(name);
        }
        if out.len() >= 8 {
            break;
        }
    }
    out
}

// ── Imports ─────────────────────────────────────────────────────────────

/// One import line: what it loads and the names it binds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Import {
    pub line: u64,
    pub spec: String,
    pub bindings: Vec<String>,
}

struct Patterns {
    js_from: Regex,
    js_as: Regex,
    js_require: Regex,
    js_bare: Regex,
    js_dynamic: Regex,
    py_import: Regex,
    py_from: Regex,
    py_dynamic: Regex,
    go_single: Regex,
    go_block_line: Regex,
    rs_path: Regex,
    java_import: Regex,
    cs_using: Regex,
    rb_require: Regex,
}

impl Patterns {
    fn new() -> Patterns {
        let r = |s: &str| Regex::new(s).expect("valid pattern");
        Patterns {
            js_as: r(r"([\w$]+)\s+as\s+([\w$]+)"),
            js_from: r(
                r#"(?s)\b(?:import|export)\s+(type\s+)?([\w$*\s{},]*?)\s*from\s*['"]([^'"\n]+)['"]"#,
            ),
            js_require: r(
                r#"(?:(?:const|let|var)\s+([\w${}\s,:]+?)\s*=\s*)?\brequire\s*\(\s*['"]([^'"\n]+)['"]\s*\)"#,
            ),
            js_bare: r(r#"(?m)^\s*import\s+['"]([^'"\n]+)['"]"#),
            js_dynamic: r(r#"\bimport\s*\(\s*['"]([^'"\n]+)['"]\s*\)"#),
            py_import: r(
                r"(?m)^\s*import\s+([\w\.]+(?:\s+as\s+\w+)?(?:\s*,\s*[\w\.]+(?:\s+as\s+\w+)?)*)",
            ),
            py_from: r(r"(?m)^\s*from\s+([\w\.]+)\s+import\s+\(?([\w\s,]+?)\)?\s*$"),
            py_dynamic: r(r#"\b(?:import_module|__import__)\s*\(\s*['"]([\w\.]+)['"]"#),
            go_single: r(r#"(?m)^\s*import\s+(?:([\w\.]+)\s+)?"([^"]+)""#),
            go_block_line: r(r#"^\s*(?:([\w\.]+)\s+)?"([^"]+)""#),
            rs_path: r(r"\b(?:use\s+|extern\s+crate\s+)?([a-z_][a-z0-9_]*)::"),
            java_import: r(r"(?m)^\s*import\s+(?:static\s+)?([\w\.]+)"),
            cs_using: r(r"(?m)^\s*(?:global\s+)?using\s+(?:static\s+)?([\w\.]+)\s*;"),
            rb_require: r(r#"(?m)^\s*require\s*\(?\s*['"]([^'"]+)['"]"#),
        }
    }
}

fn line_of(text: &str, byte: usize) -> u64 {
    text[..byte.min(text.len())]
        .bytes()
        .filter(|b| *b == b'\n')
        .count() as u64
        + 1
}

fn idents(s: &str) -> Vec<String> {
    s.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
        .filter(|w| !w.is_empty() && *w != "as" && *w != "type" && *w != "default")
        .map(String::from)
        .collect()
}

/// The imports of one code file.
fn imports_in(p: &Patterns, ecosystem: &str, text: &str) -> Vec<Import> {
    let mut out = vec![];
    match ecosystem {
        "npm" => {
            for c in p.js_from.captures_iter(text) {
                // `import type { X }` loads nothing at runtime.
                if c.get(1).is_some() {
                    continue;
                }
                let m = c.get(3).expect("group");
                let mut bindings: Vec<String> = vec![];
                let clause = c.get(2).map(|m| m.as_str()).unwrap_or("");
                // `{ a as b }` binds b; keep every identifier but the
                // renamed-from ones.
                let mut words = idents(clause);
                if clause.contains(" as ") {
                    let from: HashSet<String> = p
                        .js_as
                        .captures_iter(clause)
                        .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
                        .collect();
                    words.retain(|w| !from.contains(w));
                }
                bindings.append(&mut words);
                out.push(Import {
                    line: line_of(text, m.start()),
                    spec: m.as_str().to_string(),
                    bindings,
                });
            }
            for c in p.js_require.captures_iter(text) {
                let m = c.get(2).expect("group");
                out.push(Import {
                    line: line_of(text, m.start()),
                    spec: m.as_str().to_string(),
                    bindings: c.get(1).map(|b| idents(b.as_str())).unwrap_or_default(),
                });
            }
            for re in [&p.js_bare, &p.js_dynamic] {
                for c in re.captures_iter(text) {
                    let m = c.get(1).expect("group");
                    out.push(Import {
                        line: line_of(text, m.start()),
                        spec: m.as_str().to_string(),
                        bindings: vec![],
                    });
                }
            }
        }
        "PyPI" => {
            for c in p.py_import.captures_iter(text) {
                let m = c.get(1).expect("group");
                for part in m.as_str().split(',') {
                    let mut it = part.split_whitespace();
                    let module = it.next().unwrap_or("").to_string();
                    let alias = match (it.next(), it.next()) {
                        (Some("as"), Some(a)) => a.to_string(),
                        _ => module.split('.').next().unwrap_or("").to_string(),
                    };
                    out.push(Import {
                        line: line_of(text, m.start()),
                        spec: module,
                        bindings: vec![alias],
                    });
                }
            }
            for c in p.py_from.captures_iter(text) {
                let m = c.get(1).expect("group");
                if m.as_str().starts_with('.') {
                    continue;
                }
                out.push(Import {
                    line: line_of(text, m.start()),
                    spec: m.as_str().to_string(),
                    bindings: c.get(2).map(|b| idents(b.as_str())).unwrap_or_default(),
                });
            }
            for c in p.py_dynamic.captures_iter(text) {
                let m = c.get(1).expect("group");
                out.push(Import {
                    line: line_of(text, m.start()),
                    spec: m.as_str().to_string(),
                    bindings: vec![],
                });
            }
        }
        "Go" => {
            let mut in_block = false;
            for (i, line) in text.lines().enumerate() {
                let t = line.trim();
                if in_block {
                    if t.starts_with(')') {
                        in_block = false;
                        continue;
                    }
                    if let Some(c) = p.go_block_line.captures(line) {
                        out.push(go_import(&c, i));
                    }
                } else if t.starts_with("import (") || t == "import(" {
                    in_block = true;
                } else if let Some(c) = p.go_single.captures(line) {
                    out.push(go_import(&c, i));
                }
            }
        }
        "crates.io" => {
            for (i, line) in text.lines().enumerate() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                for c in p.rs_path.captures_iter(line) {
                    let m = c.get(1).expect("group");
                    // Only a path's first segment names a crate.
                    if line[..m.start()].ends_with("::") {
                        continue;
                    }
                    let name = m.as_str();
                    if matches!(name, "std" | "core" | "alloc" | "crate" | "self" | "super") {
                        continue;
                    }
                    out.push(Import {
                        line: i as u64 + 1,
                        spec: name.to_string(),
                        bindings: vec![name.to_string()],
                    });
                }
            }
        }
        "Maven" => {
            for c in p.java_import.captures_iter(text) {
                let m = c.get(1).expect("group");
                out.push(Import {
                    line: line_of(text, m.start()),
                    spec: m.as_str().to_string(),
                    bindings: vec![m.as_str().rsplit('.').next().unwrap_or("").to_string()],
                });
            }
        }
        "NuGet" => {
            for c in p.cs_using.captures_iter(text) {
                let m = c.get(1).expect("group");
                out.push(Import {
                    line: line_of(text, m.start()),
                    spec: m.as_str().to_string(),
                    bindings: vec![],
                });
            }
        }
        "RubyGems" => {
            for c in p.rb_require.captures_iter(text) {
                let m = c.get(1).expect("group");
                out.push(Import {
                    line: line_of(text, m.start()),
                    spec: m.as_str().to_string(),
                    bindings: vec![],
                });
            }
        }
        _ => {}
    }
    out.sort_by_key(|i| i.line);
    out.dedup_by(|a, b| a.line == b.line && a.spec == b.spec);
    out
}

fn go_import(c: &regex::Captures<'_>, i: usize) -> Import {
    let spec = c.get(2).map(|m| m.as_str()).unwrap_or("").to_string();
    let alias = c.get(1).map(|m| m.as_str().to_string()).unwrap_or_else(|| {
        spec.rsplit('/')
            .find(|s| !(s.starts_with('v') && s[1..].chars().all(|c| c.is_ascii_digit())))
            .unwrap_or("")
            .to_string()
    });
    Import {
        line: i as u64 + 1,
        spec,
        bindings: vec![alias],
    }
}

fn word_at(line: &str, word: &str) -> bool {
    if word.is_empty() {
        return false;
    }
    let is_id = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    let mut from = 0;
    while let Some(pos) = line[from..].find(word) {
        let s = from + pos;
        let e = s + word.len();
        let before = line[..s].chars().next_back();
        let after = line[e..].chars().next();
        if !before.is_some_and(is_id) && !after.is_some_and(is_id) {
            return true;
        }
        from = e;
    }
    false
}

/// Whether a line names a package the way config loads one: quoted
/// (`'css-loader'`, `"pg"`, `'pkg/sub'`), or, in a script or command
/// line, as a word.
fn mentions(line: &str, name: &str, loose: bool) -> bool {
    for q in ['\'', '"', '`'] {
        if line.contains(&format!("{q}{name}{q}")) || line.contains(&format!("{q}{name}/")) {
            return true;
        }
    }
    if line.trim_start().starts_with("- ") && line.trim_start()[2..].trim() == name {
        return true;
    }
    if loose {
        let is_name = |c: char| c.is_alphanumeric() || matches!(c, '_' | '-' | '@' | '/' | '.');
        let mut from = 0;
        while let Some(pos) = line[from..].find(name) {
            let s = from + pos;
            let e = s + name.len();
            let before = line[..s].chars().next_back();
            let after = line[e..].chars().next();
            if !before.is_some_and(is_name) && !after.is_some_and(|c| is_name(c) && c != '/') {
                return true;
            }
            from = e;
        }
    }
    false
}

// ── The scan ────────────────────────────────────────────────────────────

/// One package to read the code for.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Query {
    pub ecosystem: String,
    pub package: String,
    /// The folder whose code the manifest governs (`lambdas/resizer/`), or
    /// "" for the whole repository.
    pub dir: String,
    /// The direct dependencies that bring a transitive package in.
    pub parents: Vec<String>,
    /// The functions the advisory names (structured data: GHSA, OSV).
    pub symbols: Vec<String>,
    /// Identifiers the advisory's text names: evidence for the model only.
    pub hints: Vec<String>,
    /// Transitive, and the lockfile doesn't say which package brings it in.
    pub parents_unknown: bool,
}

impl Query {
    /// The folder a manifest governs.
    pub fn dir_of(manifest: &str) -> String {
        if manifest == crate::security_fixes::SBOM_MANIFEST || !manifest.contains('/') {
            return String::new();
        }
        match manifest.rsplit_once('/') {
            Some((d, _)) => format!("{d}/"),
            None => String::new(),
        }
    }
}

/// What the scan found for one query.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Facts {
    /// Import lines in code that runs.
    pub imports: Vec<EvidenceLine>,
    /// Files with such an import.
    pub importing_files: usize,
    /// Files that import it only in tests.
    pub test_files: usize,
    /// Lines using what was imported.
    pub uses: Vec<EvidenceLine>,
    /// Lines calling one of the advisory's functions in a file that imports
    /// the package (or a parent).
    pub symbol_uses: Vec<EvidenceLine>,
    /// Lines naming one of the advisory text's identifiers.
    pub hint_uses: Vec<EvidenceLine>,
    /// Per parent: its import lines.
    pub parent_imports: BTreeMap<String, Vec<EvidenceLine>>,
    /// Lines using what was imported from a parent.
    pub parent_uses: Vec<EvidenceLine>,
    /// Lines that load the package or a parent by name without an import
    /// (config, scripts, build-tool files, quoted plugin names).
    pub mentions: Vec<EvidenceLine>,
    /// Files read.
    pub files: usize,
}

fn ev(path: &str, line: u64, text: &str) -> EvidenceLine {
    EvidenceLine {
        file: path.to_string(),
        line,
        text: crate::truncate_str(text.trim(), LINE_CHARS).to_string(),
    }
}

fn push(v: &mut Vec<EvidenceLine>, l: EvidenceLine) {
    if v.len() < MAX_LINES && !v.iter().any(|x| x.file == l.file && x.line == l.line) {
        v.push(l);
    }
}

/// One pass over the repository's files for every query of a tick.
pub struct Scanner {
    queries: Vec<Query>,
    facts: Vec<Facts>,
    patterns: Patterns,
    /// Every name a query looks for, as one pattern: a file is read for a
    /// query only when it contains one of the query's names, found in one
    /// pass over the file whatever the number of queries.
    needles: Option<Regex>,
    by_needle: HashMap<String, Vec<usize>>,
}

impl Scanner {
    pub fn new(queries: Vec<Query>) -> Scanner {
        let facts = vec![Facts::default(); queries.len()];
        let mut by_needle: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, q) in queries.iter().enumerate() {
            for n in import_names(&q.ecosystem, &q.package)
                .into_iter()
                .chain(q.parents.iter().cloned())
            {
                if n.is_empty() {
                    continue;
                }
                let e = by_needle.entry(n).or_default();
                if !e.contains(&i) {
                    e.push(i);
                }
            }
        }
        // Longest first, so `lodash-es` isn't read as `lodash`.
        let mut names: Vec<&String> = by_needle.keys().collect();
        names.sort_by(|a, b| b.len().cmp(&a.len()).then(a.cmp(b)));
        let needles = (!names.is_empty())
            .then(|| {
                Regex::new(
                    &names
                        .iter()
                        .map(|n| regex::escape(n))
                        .collect::<Vec<_>>()
                        .join("|"),
                )
                .ok()
            })
            .flatten();
        Scanner {
            queries,
            facts,
            patterns: Patterns::new(),
            needles,
            by_needle,
        }
    }

    /// Reads one file (`text` None for a binary or oversized one).
    pub fn add(&mut self, path: &str, text: Option<&str>) {
        let Some(text) = text else { return };
        if skipped(path) {
            return;
        }
        let code_eco = code_ecosystem(path);
        let config = is_config_text(path);
        if code_eco.is_none() && !config {
            return;
        }
        let test = is_test_path(path);
        let tooling = is_tooling_config(path);
        let is_package_json = path.rsplit('/').next() == Some("package.json");
        // Which queries this file could matter to: a name the file contains
        // at all (one pass), and the query's folder.
        let Some(needles) = &self.needles else { return };
        let mut relevant: Vec<usize> = vec![];
        for m in needles.find_iter(text) {
            for i in self.by_needle.get(m.as_str()).into_iter().flatten() {
                if !relevant.contains(i) && path.starts_with(&self.queries[*i].dir) {
                    relevant.push(*i);
                }
            }
        }
        relevant.sort_unstable();
        if relevant.is_empty() {
            return;
        }
        let imports = match code_eco {
            Some(eco) => imports_in(&self.patterns, eco, text),
            None => vec![],
        };
        let lines: Vec<&str> = text.lines().collect();
        let dep_lines = if is_package_json {
            package_json_dependency_lines(&lines)
        } else {
            HashSet::new()
        };
        for i in relevant {
            let q = self.queries[i].clone();
            let names = import_names(&q.ecosystem, &q.package);
            let facts = &mut self.facts[i];
            facts.files += 1;
            let same_eco = code_eco == Some(q.ecosystem.as_str());
            let mine: Vec<&Import> = if same_eco {
                imports
                    .iter()
                    .filter(|im| {
                        names
                            .iter()
                            .any(|n| import_spec_matches(&q.ecosystem, &im.spec, n))
                    })
                    .collect()
            } else {
                vec![]
            };
            let parent_of = |im: &Import| -> Option<&String> {
                q.parents
                    .iter()
                    .find(|p| import_spec_matches(&q.ecosystem, &im.spec, p))
            };
            let theirs: Vec<(&Import, &String)> = if same_eco {
                imports
                    .iter()
                    .filter_map(|im| parent_of(im).map(|p| (im, p)))
                    .collect()
            } else {
                vec![]
            };
            let import_lines: HashSet<u64> = mine
                .iter()
                .chain(theirs.iter().map(|(im, _)| im))
                .map(|im| im.line)
                .collect();
            if test {
                if !mine.is_empty() {
                    facts.test_files += 1;
                }
                continue;
            }
            if tooling {
                for im in mine.iter().chain(theirs.iter().map(|(im, _)| im)) {
                    let t = lines.get(im.line as usize - 1).copied().unwrap_or("");
                    push(&mut facts.mentions, ev(path, im.line, t));
                }
                continue;
            }
            if !mine.is_empty() {
                facts.importing_files += 1;
                for im in &mine {
                    let t = lines.get(im.line as usize - 1).copied().unwrap_or("");
                    push(&mut facts.imports, ev(path, im.line, t));
                }
                let bindings: Vec<&String> =
                    mine.iter().flat_map(|im| im.bindings.iter()).collect();
                use_lines(&lines, path, &bindings, &import_lines, &mut facts.uses);
            }
            for (im, p) in &theirs {
                let t = lines.get(im.line as usize - 1).copied().unwrap_or("");
                let e = facts.parent_imports.entry((*p).clone()).or_default();
                push(e, ev(path, im.line, t));
            }
            if !theirs.is_empty() {
                let bindings: Vec<&String> = theirs
                    .iter()
                    .flat_map(|(im, _)| im.bindings.iter())
                    .collect();
                use_lines(
                    &lines,
                    path,
                    &bindings,
                    &import_lines,
                    &mut facts.parent_uses,
                );
            }
            // The advisory's functions, where the package (or a parent) is loaded.
            if !mine.is_empty() || !theirs.is_empty() {
                let syms: Vec<String> = q.symbols.iter().filter_map(|s| symbol_name(s)).collect();
                let hints = &q.hints;
                for (n, line) in lines.iter().enumerate() {
                    let ln = n as u64 + 1;
                    if import_lines.contains(&ln) {
                        continue;
                    }
                    if syms.iter().any(|s| word_at(line, s)) {
                        push(&mut facts.symbol_uses, ev(path, ln, line));
                    } else if hints.iter().any(|h| word_at(line, h)) {
                        push(&mut facts.hint_uses, ev(path, ln, line));
                    }
                }
            }
            // Loaded by name: quoted in code or config, or a word in a script.
            let loose = is_package_json
                || crate::security_fixes::is_dockerfile(path)
                || path.ends_with("Procfile")
                || (q.ecosystem == "PyPI" && config);
            if config || same_eco {
                for (n, line) in lines.iter().enumerate() {
                    let ln = n as u64 + 1;
                    if import_lines.contains(&ln) || dep_lines.contains(&ln) {
                        continue;
                    }
                    let named = names
                        .iter()
                        .chain(q.parents.iter())
                        .any(|nm| mentions(line, nm, loose));
                    if named {
                        push(&mut facts.mentions, ev(path, ln, line));
                    }
                }
            }
        }
    }

    /// What each query's scan found, in the order the queries came.
    pub fn finish(self) -> Vec<(Query, Facts)> {
        self.queries.into_iter().zip(self.facts).collect()
    }
}

fn import_spec_matches(ecosystem: &str, spec: &str, name: &str) -> bool {
    if ecosystem == "npm" {
        return npm_package_of(spec).is_some_and(|p| p == name);
    }
    spec_matches(ecosystem, spec, name)
}

fn use_lines(
    lines: &[&str],
    path: &str,
    bindings: &[&String],
    skip: &HashSet<u64>,
    out: &mut Vec<EvidenceLine>,
) {
    let bindings: Vec<&String> = bindings.iter().copied().filter(|b| b.len() >= 2).collect();
    if bindings.is_empty() {
        return;
    }
    let mut n_here = 0;
    for (n, line) in lines.iter().enumerate() {
        let ln = n as u64 + 1;
        if skip.contains(&ln) {
            continue;
        }
        let t = line.trim_start();
        if t.starts_with("//") || t.starts_with('#') || t.starts_with('*') {
            continue;
        }
        if bindings.iter().any(|b| word_at(line, b)) {
            push(out, ev(path, ln, line));
            n_here += 1;
            if n_here >= USES_PER_FILE {
                break;
            }
        }
    }
}

/// Lines of a `package.json` that only list dependencies (never a load).
fn package_json_dependency_lines(lines: &[&str]) -> HashSet<u64> {
    let re = Regex::new(
        r#""(dependencies|devDependencies|peerDependencies|optionalDependencies|bundledDependencies|bundleDependencies|resolutions|overrides|peerDependenciesMeta|dependenciesMeta|pnpm|workspaces)"\s*:"#,
    )
    .expect("valid");
    let mut out = HashSet::new();
    let mut depth: i64 = 0;
    let mut inside = false;
    for (n, line) in lines.iter().enumerate() {
        let ln = n as u64 + 1;
        if !inside && re.is_match(line) {
            inside = true;
            depth = 0;
        }
        if inside {
            out.insert(ln);
            depth += line.matches(['{', '[']).count() as i64;
            depth -= line.matches(['}', ']']).count() as i64;
            if depth <= 0 {
                inside = false;
            }
        }
    }
    out
}

// ── The decision ────────────────────────────────────────────────────────

/// What the code check concluded.
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    /// A verdict the lines settle.
    Settled(Verdict, Evidence),
    /// Lines to read, for the model: the evidence, and what the check found
    /// in plain words (for the prompt).
    Ask(Evidence, String),
}

fn files_of(lines: &[EvidenceLine]) -> usize {
    lines
        .iter()
        .map(|l| l.file.as_str())
        .collect::<HashSet<_>>()
        .len()
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

fn where_(q: &Query) -> String {
    if q.dir.is_empty() {
        "the repository".to_string()
    } else {
        format!("`{}`", q.dir.trim_end_matches('/'))
    }
}

/// The verdict from the scan, or the lines the model should read.
pub fn decide(q: &Query, facts: &Facts) -> Decision {
    let names = import_names(&q.ecosystem, &q.package);
    let where_ = where_(q);
    let checked = {
        let mut s = format!(
            "Read every source and config file under {where_} for imports of {}",
            names
                .iter()
                .map(|n| format!("`{n}`"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if !q.parents.is_empty() {
            s.push_str(&format!(
                " and of the packages that bring it in ({})",
                q.parents
                    .iter()
                    .take(8)
                    .map(|p| format!("`{p}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !q.symbols.is_empty() {
            s.push_str(&format!(
                ", and for calls to {}",
                q.symbols
                    .iter()
                    .take(6)
                    .map(|p| format!("`{p}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        s.push_str(". Tests, fixtures and build output don't count.");
        s
    };
    let tests_note = if facts.test_files > 0 {
        format!(
            " It is imported only by {}.",
            plural(facts.test_files, "test file", "test files")
        )
    } else {
        String::new()
    };
    let lines = |groups: &[&Vec<EvidenceLine>], cap: usize| -> Vec<EvidenceLine> {
        let mut out = vec![];
        for g in groups {
            for l in g.iter() {
                if out.len() < cap
                    && !out
                        .iter()
                        .any(|x: &EvidenceLine| x.file == l.file && x.line == l.line)
                {
                    out.push(l.clone());
                }
            }
        }
        out
    };
    // Imported where it runs.
    if !facts.imports.is_empty() {
        let n = facts.importing_files;
        if !q.symbols.is_empty() {
            if !facts.symbol_uses.is_empty() {
                let shown = lines(&[&facts.symbol_uses, &facts.imports], 12);
                return Decision::Settled(
                    Verdict::Reachable,
                    Evidence {
                        summary: format!(
                            "{} is imported in {} and the code calls the vulnerable function the advisory names ({}).",
                            q.package,
                            plural(n, "file", "files"),
                            q.symbols.iter().take(3).cloned().collect::<Vec<_>>().join(", ")
                        ),
                        lines: shown,
                        checked,
                        confidence: "high".into(),
                    },
                );
            }
            return Decision::Settled(
                Verdict::NotReachable,
                Evidence {
                    summary: format!(
                        "{} is imported in {}, but no line calls the functions the advisory names ({}).",
                        q.package,
                        plural(n, "file", "files"),
                        q.symbols.iter().take(3).cloned().collect::<Vec<_>>().join(", ")
                    ),
                    lines: lines(&[&facts.imports], 8),
                    checked,
                    confidence: "medium".into(),
                },
            );
        }
        let found = format!(
            "{} is imported in {} of code that runs; the advisory names no function, so whether the code uses the vulnerable feature is for you to judge from the lines.",
            q.package,
            plural(n, "file", "files"),
        );
        return Decision::Ask(
            Evidence {
                summary: found.clone(),
                lines: lines(
                    &[
                        &facts.imports,
                        &facts.hint_uses,
                        &facts.uses,
                        &facts.mentions,
                    ],
                    30,
                ),
                checked,
                confidence: "low".into(),
            },
            found,
        );
    }
    // Not imported. Loaded by name somewhere (config, a script, a plugin list).
    if !facts.mentions.is_empty() {
        let found = format!(
            "Nothing imports {} in code that runs, but {} name it or a package that brings it in, so it may be loaded by configuration or a tool.{tests_note}",
            q.package,
            plural(files_of(&facts.mentions), "file", "files"),
        );
        let parents: Vec<&Vec<EvidenceLine>> = facts.parent_imports.values().collect();
        let mut groups: Vec<&Vec<EvidenceLine>> = vec![&facts.mentions];
        groups.extend(parents);
        groups.push(&facts.parent_uses);
        return Decision::Ask(
            Evidence {
                summary: found.clone(),
                lines: lines(&groups, 30),
                checked,
                confidence: "low".into(),
            },
            found,
        );
    }
    // Comes in through a parent the code imports.
    if !facts.parent_imports.is_empty() {
        let via: Vec<&String> = facts.parent_imports.keys().collect();
        let found = format!(
            "The code never imports {} itself; it is loaded through {} (imported in {}). Judge whether the code's use of {} reaches the vulnerable part of {}.{tests_note}",
            q.package,
            via.iter().map(|p| p.as_str()).collect::<Vec<_>>().join(", "),
            plural(
                files_of(&facts.parent_imports.values().flatten().cloned().collect::<Vec<_>>()),
                "file",
                "files"
            ),
            via.iter().map(|p| p.as_str()).collect::<Vec<_>>().join(", "),
            q.package,
        );
        let mut groups: Vec<&Vec<EvidenceLine>> = facts.parent_imports.values().collect();
        groups.push(&facts.parent_uses);
        groups.push(&facts.hint_uses);
        return Decision::Ask(
            Evidence {
                summary: found.clone(),
                lines: lines(&groups, 30),
                checked,
                confidence: "low".into(),
            },
            found,
        );
    }
    // Neither it nor anything that brings it in is loaded by the code.
    if absence_is_conclusive(&q.ecosystem) && !q.parents_unknown {
        let summary = if q.parents.is_empty() {
            format!(
                "Nothing under {where_} imports {} or names it in a config file or script, so the code never loads it.{tests_note}",
                q.package
            )
        } else {
            format!(
                "Nothing under {where_} imports {} or the packages that bring it in ({}), and no config file or script names them, so the code never loads it.{tests_note}",
                q.package,
                q.parents.iter().take(8).cloned().collect::<Vec<_>>().join(", ")
            )
        };
        return Decision::Settled(
            Verdict::NotReachable,
            Evidence {
                summary,
                lines: vec![],
                checked,
                confidence: "medium".into(),
            },
        );
    }
    let summary = if q.parents_unknown {
        format!(
            "Nothing under {where_} imports {}, and the lockfile doesn't say which package brings it in, so whether that package calls it isn't known.{tests_note}",
            q.package
        )
    } else {
        format!(
            "Nothing under {where_} imports {}, but {} packages can load without an import line, so this isn't settled.{tests_note}",
            q.package, q.ecosystem
        )
    };
    Decision::Settled(
        Verdict::Unknown,
        Evidence {
            summary,
            lines: vec![],
            checked,
            confidence: "low".into(),
        },
    )
}

// ── Lockfiles: what brings a package in ─────────────────────────────────

/// How a lockfile pulls one package in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Ancestry {
    /// The lockfile has the package at that version.
    pub found: bool,
    /// A direct dependency of the project (or one of its workspaces).
    pub direct: bool,
    /// Every direct dependency whose tree holds it.
    pub parents: BTreeSet<String>,
}

impl Ancestry {
    pub fn merge(&mut self, other: Ancestry) {
        self.found |= other.found;
        self.direct |= other.direct;
        self.parents.extend(other.parents);
    }
}

/// Lockfile names [`lock_ancestry`] reads.
pub const LOCKFILES: &[&str] = &[
    "pnpm-lock.yaml",
    "package-lock.json",
    "npm-shrinkwrap.json",
    "yarn.lock",
];

pub fn is_lockfile(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    LOCKFILES.contains(&name)
}

/// The direct dependencies of a lockfile that bring `package@version` in.
/// `direct_names`: the names the project's `package.json` files list (only
/// `yarn.lock` needs them). None for a lockfile it can't read.
pub fn lock_ancestry(
    path: &str,
    text: &str,
    package: &str,
    version: &str,
    direct_names: &[String],
) -> Option<Ancestry> {
    let name = path.rsplit('/').next().unwrap_or(path);
    let graph = match name {
        "pnpm-lock.yaml" => pnpm_graph(text)?,
        "package-lock.json" | "npm-shrinkwrap.json" => npm_graph(text)?,
        "yarn.lock" => yarn_graph(text, direct_names)?,
        _ => return None,
    };
    Some(graph.ancestry(package, version))
}

/// A lockfile as a graph: nodes are installed copies (name, version), the
/// roots are the direct dependencies.
#[derive(Default)]
struct Graph {
    nodes: Vec<(String, String)>,
    edges: Vec<Vec<usize>>,
    /// (direct dependency name, its node)
    roots: Vec<(String, usize)>,
    /// The workspace projects the lockfile installs for: each one's folder
    /// from the lockfile's folder (`""` for the root) and the nodes it
    /// depends on directly.
    projects: Vec<(String, Vec<usize>)>,
    /// (project, a workspace project it depends on), by index in `projects`.
    project_links: Vec<(usize, usize)>,
}

/// `rel` from the folder `base`, with `.` and `..` resolved (`apps/web` and
/// `../studio` is `apps/studio`).
fn join_rel(base: &str, rel: &str) -> String {
    let mut parts: Vec<&str> = base.split('/').filter(|p| !p.is_empty()).collect();
    for seg in rel.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    parts.join("/")
}

/// One workspace project of a lockfile, and which of the packages asked
/// about its dependencies bring in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LockProject {
    /// Its folder, from the lockfile's folder (`""`: the lockfile's own).
    pub dir: String,
    /// The packages asked about that it installs, directly or through a
    /// dependency or another workspace project, as `name@version`.
    pub uses: Vec<String>,
}

fn graph_of(path: &str, text: &str, manifests: &[(String, String)]) -> Option<Graph> {
    let name = path.rsplit('/').next().unwrap_or(path);
    match name {
        "pnpm-lock.yaml" => pnpm_graph(text),
        "package-lock.json" | "npm-shrinkwrap.json" => npm_graph(text),
        "yarn.lock" => yarn_projects(text, manifests),
        _ => None,
    }
}

/// Every installed copy a lockfile resolves, as (name, version). `None` for
/// a lockfile this can't read: the caller then says the versions are not
/// known, never a guess.
pub fn lock_copies(path: &str, text: &str) -> Option<Vec<(String, String)>> {
    let name = path.rsplit('/').next().unwrap_or(path);
    let mut out: Vec<(String, String)> = match name {
        "pnpm-lock.yaml" | "package-lock.json" | "npm-shrinkwrap.json" | "yarn.lock" => {
            graph_of(path, text, &[])?.nodes
        }
        "Cargo.lock" | "poetry.lock" | "uv.lock" => toml_copies(text),
        "Gemfile.lock" => gem_copies(text),
        _ => return None,
    };
    out.retain(|(n, v)| !n.is_empty() && !v.is_empty());
    out.sort();
    out.dedup();
    (!out.is_empty()).then_some(out)
}

/// The `[[package]]` tables of a TOML lockfile (`Cargo.lock`, `poetry.lock`,
/// `uv.lock`): each one's `name` and `version`.
fn toml_copies(text: &str) -> Vec<(String, String)> {
    let mut out = vec![];
    let mut cur: Option<(String, String)> = None;
    let value = |l: &str, key: &str| -> Option<String> {
        let rest = l.strip_prefix(key)?.trim_start().strip_prefix('=')?;
        Some(rest.trim().trim_matches('"').to_string())
    };
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with('[') {
            if let Some(c) = cur.take() {
                out.push(c);
            }
            if t == "[[package]]" {
                cur = Some((String::new(), String::new()));
            }
            continue;
        }
        if let Some(c) = cur.as_mut() {
            if let Some(v) = value(t, "name") {
                c.0 = v;
            } else if let Some(v) = value(t, "version") {
                c.1 = v;
            }
        }
    }
    out.extend(cur);
    out
}

/// The gems a `Gemfile.lock` resolves: the `name (version)` lines of its
/// `specs:` lists (a dependency's own requirements sit deeper and carry a
/// range, never a single version).
fn gem_copies(text: &str) -> Vec<(String, String)> {
    let mut out = vec![];
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("    ") else {
            continue;
        };
        if rest.starts_with(' ') {
            continue;
        }
        let Some((name, ver)) = rest.split_once(" (") else {
            continue;
        };
        let ver = ver.trim_end_matches(')');
        if ver.chars().next().is_some_and(|c| c.is_ascii_digit()) && !ver.contains(' ') {
            out.push((name.to_string(), ver.to_string()));
        }
    }
    out
}

/// The workspace projects of a lockfile, each with the packages of `asked`
/// (name and a version on the line asked about) that its dependencies bring
/// in: the projects a change to those packages can break. `manifests`: each
/// workspace project's folder (from the lockfile's folder) and its
/// `package.json`; only `yarn.lock`, which does not list its projects,
/// needs them. `None` for a lockfile this can't read.
pub fn lock_projects(
    path: &str,
    text: &str,
    manifests: &[(String, String)],
    asked: &[(String, String)],
) -> Option<Vec<LockProject>> {
    let g = graph_of(path, text, manifests)?;
    if g.projects.is_empty() {
        return None;
    }
    let mut out = vec![];
    for (pi, (dir, _)) in g.projects.iter().enumerate() {
        // This project and every workspace project it depends on.
        let mut mine = vec![pi];
        let mut i = 0;
        while i < mine.len() {
            for (from, to) in &g.project_links {
                if *from == mine[i] && !mine.contains(to) {
                    mine.push(*to);
                }
            }
            i += 1;
        }
        let mut seen: HashSet<usize> = HashSet::new();
        let mut queue: VecDeque<usize> = VecDeque::new();
        for p in &mine {
            for n in &g.projects[*p].1 {
                if seen.insert(*n) {
                    queue.push_back(*n);
                }
            }
        }
        while let Some(n) = queue.pop_front() {
            for to in &g.edges[n] {
                if seen.insert(*to) {
                    queue.push_back(*to);
                }
            }
        }
        let mut uses: Vec<String> = seen
            .iter()
            .map(|n| &g.nodes[*n])
            .filter(|(name, ver)| {
                asked.iter().any(|(an, av)| {
                    an == name
                        && !ver.is_empty()
                        && crate::security_fixes::version_line(av)
                            == crate::security_fixes::version_line(ver)
                })
            })
            .map(|(name, ver)| format!("{name}@{ver}"))
            .collect();
        uses.sort();
        uses.dedup();
        out.push(LockProject {
            dir: dir.clone(),
            uses,
        });
    }
    out.sort_by(|a, b| a.dir.cmp(&b.dir));
    Some(out)
}

impl Graph {
    fn ancestry(&self, package: &str, version: &str) -> Ancestry {
        let targets: Vec<usize> = (0..self.nodes.len())
            .filter(|i| self.nodes[*i].0 == package && self.nodes[*i].1 == version)
            .collect();
        let mut out = Ancestry {
            found: !targets.is_empty(),
            ..Default::default()
        };
        if targets.is_empty() {
            return out;
        }
        let mut reverse: Vec<Vec<usize>> = vec![vec![]; self.nodes.len()];
        for (from, tos) in self.edges.iter().enumerate() {
            for to in tos {
                reverse[*to].push(from);
            }
        }
        let mut seen: HashSet<usize> = targets.iter().copied().collect();
        let mut queue: VecDeque<usize> = targets.iter().copied().collect();
        while let Some(n) = queue.pop_front() {
            for p in &reverse[n] {
                if seen.insert(*p) {
                    queue.push_back(*p);
                }
            }
        }
        for (name, node) in &self.roots {
            if targets.contains(node) {
                out.direct = true;
            } else if seen.contains(node) && name != package {
                out.parents.insert(name.clone());
            }
        }
        out
    }
}

fn yaml_map(v: &serde_yaml::Value) -> Vec<(String, &serde_yaml::Value)> {
    v.as_mapping()
        .into_iter()
        .flatten()
        .filter_map(|(k, v)| {
            let k = match k {
                serde_yaml::Value::String(s) => s.clone(),
                serde_yaml::Value::Number(n) => n.to_string(),
                _ => return None,
            };
            Some((k, v))
        })
        .collect()
}

fn yaml_str(v: &serde_yaml::Value) -> Option<String> {
    match v {
        serde_yaml::Value::String(s) => Some(s.clone()),
        serde_yaml::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// `name@version(peers)` from any pnpm key form: `/name/1.2.3_peer` (v5),
/// `/name@1.2.3(peer)` (v6), `name@1.2.3(peer)` (v9).
fn pnpm_canonical(key: &str, v5: bool) -> String {
    let k = key.strip_prefix('/').unwrap_or(key);
    if v5 {
        if let Some((name, ver)) = k.rsplit_once('/') {
            return format!("{name}@{ver}");
        }
    }
    k.to_string()
}

/// (name, plain version) of a canonical key.
fn pnpm_split(canonical: &str) -> Option<(String, String)> {
    let core = canonical.split('(').next().unwrap_or(canonical);
    let at = core[1..].find('@')? + 1;
    let name = &core[..at];
    let ver = core[at + 1..].split('_').next().unwrap_or("");
    Some((name.to_string(), ver.to_string()))
}

/// The node a dependency's value points at.
fn pnpm_target(name: &str, value: &str, v5: bool) -> Option<String> {
    if value.starts_with("link:") || value.starts_with("file:") || value.starts_with("workspace:") {
        return None;
    }
    if value.starts_with('/') {
        return Some(pnpm_canonical(value, v5));
    }
    if value.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return Some(format!("{name}@{value}"));
    }
    // An alias: the value is the real package's key.
    if value.contains('@') {
        return Some(pnpm_canonical(value, v5));
    }
    None
}

fn pnpm_graph(text: &str) -> Option<Graph> {
    let doc: serde_yaml::Value = serde_yaml::from_str(text).ok()?;
    let lockver = doc
        .get("lockfileVersion")
        .and_then(yaml_str)
        .unwrap_or_default();
    let v5 = lockver.starts_with('5') || lockver.starts_with('4');
    let mut g = Graph::default();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut node = |g: &mut Graph, canonical: &str| -> Option<usize> {
        if let Some(i) = index.get(canonical) {
            return Some(*i);
        }
        let (n, v) = pnpm_split(canonical)?;
        g.nodes.push((n, v));
        g.edges.push(vec![]);
        index.insert(canonical.to_string(), g.nodes.len() - 1);
        Some(g.nodes.len() - 1)
    };
    let deps_of = |entry: &serde_yaml::Value| -> Vec<(String, String)> {
        let mut out = vec![];
        for field in ["dependencies", "optionalDependencies"] {
            if let Some(m) = entry.get(field) {
                for (k, v) in yaml_map(m) {
                    if let Some(s) = yaml_str(v) {
                        out.push((k, s));
                    }
                }
            }
        }
        out
    };
    for section in ["packages", "snapshots"] {
        let Some(m) = doc.get(section) else { continue };
        for (key, entry) in yaml_map(m) {
            let canonical = pnpm_canonical(&key, v5);
            let Some(from) = node(&mut g, &canonical) else {
                continue;
            };
            for (dn, dv) in deps_of(entry) {
                if let Some(t) = pnpm_target(&dn, &dv, v5) {
                    if let Some(to) = node(&mut g, &t) {
                        g.edges[from].push(to);
                    }
                }
            }
        }
    }
    // The direct dependencies: each importer's, or the top level's.
    let mut importers: Vec<(String, &serde_yaml::Value)> = vec![];
    if let Some(m) = doc.get("importers") {
        importers.extend(yaml_map(m));
    } else {
        importers.push((".".to_string(), &doc));
    }
    // (project, the folder of the workspace project it links to)
    let mut links: Vec<(usize, String)> = vec![];
    for (key, imp) in importers {
        let dir = join_rel("", &key);
        let pi = g.projects.len();
        g.projects.push((dir.clone(), vec![]));
        for field in ["dependencies", "devDependencies", "optionalDependencies"] {
            let Some(m) = imp.get(field) else { continue };
            for (dn, dv) in yaml_map(m) {
                let value = match dv {
                    serde_yaml::Value::Mapping(_) => dv.get("version").and_then(yaml_str),
                    other => yaml_str(other),
                };
                let Some(value) = value else { continue };
                if let Some(to) = value.strip_prefix("link:") {
                    links.push((pi, join_rel(&dir, to)));
                    continue;
                }
                if let Some(t) = pnpm_target(&dn, &value, v5) {
                    if let Some(n) = node(&mut g, &t) {
                        g.roots.push((dn.clone(), n));
                        g.projects[pi].1.push(n);
                    }
                }
            }
        }
    }
    for (from, dir) in links {
        if let Some(to) = g.projects.iter().position(|(d, _)| *d == dir) {
            g.project_links.push((from, to));
        }
    }
    Some(g)
}

fn npm_graph(text: &str) -> Option<Graph> {
    let lock: Value = serde_json::from_str(text).ok()?;
    let packages = lock["packages"].as_object()?;
    let mut g = Graph::default();
    let mut index: HashMap<&str, usize> = HashMap::new();
    for (loc, p) in packages {
        let name = loc
            .rsplit_once("node_modules/")
            .map(|(_, n)| n)
            .unwrap_or("");
        index.insert(loc.as_str(), g.nodes.len());
        g.nodes.push((
            name.to_string(),
            p["version"].as_str().unwrap_or("").to_string(),
        ));
        g.edges.push(vec![]);
    }
    for (loc, p) in packages {
        let from = index[loc.as_str()];
        let is_project = !loc.contains("node_modules/");
        for field in [
            "dependencies",
            "optionalDependencies",
            "peerDependencies",
            "devDependencies",
        ] {
            if field == "devDependencies" && !is_project {
                continue;
            }
            for (dep, _) in p[field].as_object().into_iter().flatten() {
                let Some(target) = crate::security_fixes::npm_resolve(packages, loc, dep) else {
                    continue;
                };
                let to = index[target];
                g.edges[from].push(to);
                if is_project {
                    g.roots.push((dep.clone(), to));
                }
            }
        }
    }
    // The workspace projects: every location outside `node_modules`. A
    // dependency that resolves to a link is another workspace project.
    let project_locs: Vec<&str> = packages
        .keys()
        .map(String::as_str)
        .filter(|l| !l.contains("node_modules/"))
        .collect();
    let mut loc_of: Vec<&str> = vec![""; g.nodes.len()];
    for (loc, i) in &index {
        loc_of[*i] = loc;
    }
    for loc in &project_locs {
        let pi = g.projects.len();
        g.projects.push((loc.to_string(), vec![]));
        for to in g.edges[index[*loc]].clone() {
            match packages.get(loc_of[to]) {
                Some(p) if p["link"].as_bool() == Some(true) => {
                    let dir = p["resolved"].as_str().unwrap_or("").trim_matches('/');
                    if let Some(at) = project_locs.iter().position(|l| *l == dir) {
                        g.project_links.push((pi, at));
                    }
                }
                _ => g.projects[pi].1.push(to),
            }
        }
    }
    Some(g)
}

fn yarn_spec(name: &str, range: &str) -> String {
    let r = range.trim().trim_matches('"');
    let r = r.strip_prefix("npm:").unwrap_or(r);
    format!("{name}@{r}")
}

fn yarn_graph(text: &str, direct_names: &[String]) -> Option<Graph> {
    let (mut g, _) = yarn_parse(text)?;
    // The project's own dependencies: every copy of a name it lists.
    for d in direct_names {
        for (i, (n, _)) in g.nodes.iter().enumerate() {
            if n == d {
                g.roots.push((d.clone(), i));
            }
        }
    }
    Some(g)
}

/// A `yarn.lock` with its workspace projects, which the lockfile itself
/// does not list: each project's `package.json` names what it depends on,
/// and the lockfile says which copy each range resolves to.
fn yarn_projects(text: &str, manifests: &[(String, String)]) -> Option<Graph> {
    let (mut g, spec_to) = yarn_parse(text)?;
    let parsed: Vec<(String, Value)> = manifests
        .iter()
        .filter_map(|(dir, t)| Some((join_rel("", dir), serde_json::from_str::<Value>(t).ok()?)))
        .collect();
    for (dir, _) in &parsed {
        g.projects.push((dir.clone(), vec![]));
    }
    for (pi, (_, m)) in parsed.iter().enumerate() {
        for field in ["dependencies", "devDependencies", "optionalDependencies"] {
            for (dep, range) in m[field].as_object().into_iter().flatten() {
                let range = range.as_str().unwrap_or("");
                if let Some(n) = spec_to.get(&yarn_spec(dep, range)) {
                    g.roots.push((dep.clone(), *n));
                    g.projects[pi].1.push(*n);
                } else if let Some(to) = parsed
                    .iter()
                    .position(|(_, o)| o["name"].as_str() == Some(dep.as_str()))
                {
                    g.project_links.push((pi, to));
                }
            }
        }
    }
    Some(g)
}

/// The copies and edges of a `yarn.lock` (classic and berry), and which
/// copy each `name@range` resolves to.
fn yarn_parse(text: &str) -> Option<(Graph, HashMap<String, usize>)> {
    let mut g = Graph::default();
    let mut spec_to: HashMap<String, usize> = HashMap::new();
    let mut pending: Vec<(usize, String, String)> = vec![];
    let norm = yarn_spec;
    let split_spec = |spec: &str| -> Option<(String, String)> {
        let s = spec.trim().trim_matches('"');
        let at = s[1..].find('@')? + 1;
        Some((s[..at].to_string(), s[at + 1..].to_string()))
    };
    let mut current: Option<usize> = None;
    let mut in_deps = false;
    for line in text.lines() {
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        let t = line.trim();
        if indent == 0 && t.ends_with(':') {
            in_deps = false;
            let specs: Vec<(String, String)> = t
                .trim_end_matches(':')
                .split(", ")
                .filter_map(split_spec)
                .collect();
            let Some((name, _)) = specs.first().cloned() else {
                current = None;
                continue;
            };
            if name == "__metadata" {
                current = None;
                continue;
            }
            g.nodes.push((name, String::new()));
            g.edges.push(vec![]);
            let i = g.nodes.len() - 1;
            for (n, r) in specs {
                spec_to.insert(norm(&n, &r), i);
            }
            current = Some(i);
            continue;
        }
        let Some(i) = current else { continue };
        if indent == 2 {
            in_deps = matches!(t, "dependencies:" | "optionalDependencies:");
            if let Some(v) = t
                .strip_prefix("version ")
                .or_else(|| t.strip_prefix("version: "))
            {
                g.nodes[i].1 = v.trim().trim_matches('"').to_string();
            }
        } else if indent >= 4 && in_deps {
            let (n, r) = match t.split_once(": ") {
                Some((n, r)) => (n, r),
                None => match t.rsplit_once(' ') {
                    Some((n, r)) => (n, r),
                    None => continue,
                },
            };
            pending.push((i, n.trim().trim_matches('"').to_string(), r.to_string()));
        }
    }
    for (from, n, r) in pending {
        if let Some(to) = spec_to.get(&norm(&n, &r)) {
            g.edges[from].push(*to);
        }
    }
    if g.nodes.is_empty() {
        return None;
    }
    Some((g, spec_to))
}

/// The names a `package.json` lists as dependencies.
pub fn package_json_names(text: &str) -> Vec<String> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    let mut out = vec![];
    for field in [
        "dependencies",
        "devDependencies",
        "optionalDependencies",
        "peerDependencies",
    ] {
        for (k, _) in v[field].as_object().into_iter().flatten() {
            if !out.contains(k) {
                out.push(k.clone());
            }
        }
    }
    out
}

// ── Pace ────────────────────────────────────────────────────────────────

/// Model calls a tick may make: at most `per_tick`, and only while today's
/// tokens plus the calls' estimate stay under the day's cap.
pub fn model_calls_allowed(today: u64, cap: u64, estimate: u64, per_tick: usize) -> usize {
    if estimate == 0 {
        return per_tick;
    }
    let room = cap.saturating_sub(today) / estimate;
    (room as usize).min(per_tick)
}

/// A model call's tokens, guessed from its prompt (about four characters a
/// token, plus the instructions and the answer).
pub fn estimate_call_tokens(prompt_chars: usize) -> u64 {
    (prompt_chars as u64).div_ceil(4) + 900
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(queries: Vec<Query>, files: &[(&str, &str)]) -> Vec<(Query, Facts)> {
        let mut s = Scanner::new(queries);
        for (p, t) in files {
            s.add(p, Some(t));
        }
        s.finish()
    }

    fn q(pkg: &str) -> Query {
        Query {
            ecosystem: "npm".into(),
            package: pkg.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_package_nothing_imports_is_not_reachable_with_what_was_read() {
        let mut query = q("node-forge");
        query.parents = vec!["google-auth-library".into()];
        let out = scan(
            vec![query],
            &[
                ("src/server.ts", "import express from 'express';\nimport { forgeAhead } from './forge';\n"),
                ("src/forge.ts", "export const forgeAhead = 1; // not node-forge\n"),
                // Only a test imports it: tests don't load in production.
                ("src/__tests__/certs.test.ts", "import forge from 'node-forge';\n"),
                ("package.json", "{\n  \"scripts\": { \"start\": \"node dist/server.js\" },\n  \"dependencies\": {\n    \"google-auth-library\": \"^9.0.0\"\n  }\n}\n"),
            ],
        );
        let (query, facts) = &out[0];
        assert!(
            facts.imports.is_empty() && facts.mentions.is_empty(),
            "{facts:?}"
        );
        assert_eq!(facts.test_files, 1);
        match decide(query, facts) {
            Decision::Settled(Verdict::NotReachable, e) => {
                assert!(
                    e.summary.contains("node-forge") && e.summary.contains("google-auth-library"),
                    "{}",
                    e.summary
                );
                assert!(e.summary.contains("only by 1 test file"), "{}", e.summary);
                assert!(e.checked.contains("`node-forge`"), "{}", e.checked);
                assert_eq!(e.confidence, "medium");
            }
            other => panic!("expected not reachable, got {other:?}"),
        }
    }

    #[test]
    fn an_imported_package_that_calls_the_vulnerable_function_is_reachable() {
        let mut query = q("lodash");
        query.symbols = vec!["lodash.template".into()];
        let out = scan(
            vec![query.clone()],
            &[(
                "src/mail.ts",
                "import _ from 'lodash';\n\nexport function render(body: string, data: object) {\n  return _.template(body)(data);\n}\n",
            )],
        );
        match decide(&out[0].0, &out[0].1) {
            Decision::Settled(Verdict::Reachable, e) => {
                assert!(
                    e.lines
                        .iter()
                        .any(|l| l.file == "src/mail.ts" && l.line == 4),
                    "{:?}",
                    e.lines
                );
                assert!(e
                    .lines
                    .iter()
                    .any(|l| l.line == 1 && l.text.contains("from 'lodash'")));
                assert_eq!(e.confidence, "high");
            }
            other => panic!("expected reachable, got {other:?}"),
        }
        // Imported, but nothing calls the named function: present, not reachable.
        let out = scan(
            vec![query],
            &[(
                "src/a.js",
                "const { merge } = require('lodash/fp');\nmerge({}, {});\n",
            )],
        );
        match decide(&out[0].0, &out[0].1) {
            Decision::Settled(Verdict::NotReachable, e) => {
                assert_eq!(e.lines[0].file, "src/a.js");
                assert!(e.summary.contains("no line calls"), "{}", e.summary);
            }
            other => panic!("expected not reachable, got {other:?}"),
        }
    }

    #[test]
    fn imported_with_no_named_function_goes_to_the_model_with_the_usage() {
        let out = scan(
            vec![q("axios")],
            &[(
                "src/api.ts",
                "import axios, { AxiosError } from 'axios';\nimport type { AxiosResponse } from 'axios';\n\nconst client = axios.create({ baseURL: process.env.API });\nexport const get = (p: string) => client.get(p);\n",
            )],
        );
        let (query, facts) = &out[0];
        assert_eq!(
            facts.imports.len(),
            1,
            "a type-only import loads nothing: {:?}",
            facts.imports
        );
        match decide(query, facts) {
            Decision::Ask(e, found) => {
                assert!(found.contains("imported in 1 file"), "{found}");
                assert!(
                    e.lines.iter().any(|l| l.text.contains("axios.create")),
                    "{:?}",
                    e.lines
                );
            }
            other => panic!("expected the model, got {other:?}"),
        }
    }

    #[test]
    fn a_transitive_package_loaded_through_a_parent_goes_to_the_model() {
        let mut query = q("fast-xml-parser");
        query.parents = vec!["@aws-sdk/client-s3".into()];
        let out = scan(
            vec![query],
            &[(
                "lambdas/up/index.mjs",
                "import { S3Client, PutObjectCommand } from \"@aws-sdk/client-s3\";\nconst s3 = new S3Client({});\nawait s3.send(new PutObjectCommand({ Bucket: b }));\n",
            )],
        );
        match decide(&out[0].0, &out[0].1) {
            Decision::Ask(e, found) => {
                assert!(
                    found.contains("loaded through @aws-sdk/client-s3"),
                    "{found}"
                );
                assert!(
                    e.lines
                        .iter()
                        .any(|l| l.text.contains("s3.send") || l.text.contains("new S3Client")),
                    "{:?}",
                    e.lines
                );
            }
            other => panic!("expected the model, got {other:?}"),
        }
    }

    #[test]
    fn a_package_loaded_by_config_is_never_called_unloaded() {
        let mut query = q("postcss");
        query.parents = vec!["tailwindcss".into()];
        let out = scan(
            vec![query],
            &[("postcss.config.js", "module.exports = { plugins: { tailwindcss: {}, 'postcss-nested': {} } };\nrequire('tailwindcss');\n")],
        );
        assert!(matches!(decide(&out[0].0, &out[0].1), Decision::Ask(..)));
        // A runtime server started by a script.
        let out = scan(
            vec![q("next")],
            &[("package.json", "{\n  \"scripts\": {\n    \"start\": \"next start\"\n  },\n  \"dependencies\": {\n    \"next\": \"15.0.0\"\n  }\n}\n")],
        );
        let (_, facts) = &out[0];
        assert_eq!(facts.mentions.len(), 1, "{:?}", facts.mentions);
        assert_eq!(
            facts.mentions[0].line, 3,
            "the dependency list itself is no load"
        );
    }

    #[test]
    fn transitive_with_no_known_parent_stays_unknown_without_the_model() {
        let mut query = q("ip-address");
        query.parents_unknown = true;
        let out = scan(vec![query], &[("src/a.ts", "export {}\n")]);
        match decide(&out[0].0, &out[0].1) {
            Decision::Settled(Verdict::Unknown, e) => {
                assert!(e.summary.contains("lockfile doesn't say"))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn imports_are_read_per_language() {
        let p = Patterns::new();
        let py = imports_in(&p, "PyPI", "import yaml\nfrom jwt import decode as d\nimport os, requests.adapters\nfrom . import x\n");
        let specs: Vec<&str> = py.iter().map(|i| i.spec.as_str()).collect();
        assert_eq!(specs, ["yaml", "jwt", "os", "requests.adapters"]);
        assert!(spec_matches("PyPI", "requests.adapters", "requests"));
        assert_eq!(import_names("PyPI", "PyYAML"), ["yaml"]);
        let go = imports_in(
            &p,
            "Go",
            "package main\n\nimport (\n\t\"fmt\"\n\th2 \"golang.org/x/net/http2\"\n)\n",
        );
        assert_eq!(go[1].spec, "golang.org/x/net/http2");
        assert_eq!(go[1].bindings, ["h2"]);
        assert!(spec_matches(
            "Go",
            "golang.org/x/net/http2",
            "golang.org/x/net"
        ));
        assert!(!spec_matches(
            "Go",
            "golang.org/x/network",
            "golang.org/x/net"
        ));
        let rs = imports_in(
            &p,
            "crates.io",
            "use tokio::net::TcpListener;\nlet x = std::mem::take(&mut y);\n",
        );
        assert_eq!(
            rs.iter().map(|i| i.spec.as_str()).collect::<Vec<_>>(),
            ["tokio"]
        );
        let js = imports_in(
            &p,
            "npm",
            "import {\n  a,\n  b as c,\n} from '@scope/pkg/sub';\n",
        );
        assert_eq!(js[0].spec, "@scope/pkg/sub");
        assert_eq!(js[0].bindings, ["a", "c"]);
        assert!(import_spec_matches("npm", "@scope/pkg/sub", "@scope/pkg"));
        assert!(!import_spec_matches("npm", "lodash.merge", "lodash"));
    }

    #[test]
    fn hints_come_from_the_advisory_text() {
        assert_eq!(
            hints_from_text("Prototype pollution in `merge()` and `_.set` via `__proto__`; see parseQuery() in axios.", "axios"),
            ["merge", "set", "__proto__", "parseQuery"]
        );
    }

    #[test]
    fn pnpm_lockfiles_name_every_parent() {
        let v9 = r#"
lockfileVersion: '9.0'
importers:
  .:
    dependencies:
      google-auth-library:
        specifier: ^9.0.0
        version: 9.15.0
      axios:
        specifier: ^1.7.0
        version: 1.7.9(debug@4.3.7)
  apps/web:
    devDependencies:
      jsdom:
        specifier: 25.0.0
        version: 25.0.0
packages:
  node-forge@1.3.1:
    resolution: {integrity: sha512-x}
snapshots:
  google-auth-library@9.15.0:
    dependencies:
      gtoken: 7.1.0
  gtoken@7.1.0:
    dependencies:
      node-forge: 1.3.1
  axios@1.7.9(debug@4.3.7):
    dependencies:
      follow-redirects: 1.15.9(debug@4.3.7)
  follow-redirects@1.15.9(debug@4.3.7): {}
  jsdom@25.0.0:
    dependencies:
      node-forge: 1.3.1
  node-forge@1.3.1: {}
"#;
        let a = lock_ancestry("pnpm-lock.yaml", v9, "node-forge", "1.3.1", &[]).unwrap();
        assert!(a.found && !a.direct);
        assert_eq!(
            a.parents.iter().cloned().collect::<Vec<_>>(),
            ["google-auth-library", "jsdom"]
        );
        let a = lock_ancestry("pnpm-lock.yaml", v9, "axios", "1.7.9", &[]).unwrap();
        assert!(a.direct);
        let a = lock_ancestry("pnpm-lock.yaml", v9, "follow-redirects", "1.15.9", &[]).unwrap();
        assert_eq!(a.parents.iter().cloned().collect::<Vec<_>>(), ["axios"]);
        assert!(
            !lock_ancestry("pnpm-lock.yaml", v9, "node-forge", "1.3.2", &[])
                .unwrap()
                .found
        );
        let v6 = "lockfileVersion: '6.0'\ndependencies:\n  express:\n    specifier: ^4.0.0\n    version: 4.21.0\npackages:\n  /express@4.21.0:\n    dependencies:\n      qs: 6.13.0\n  /qs@6.13.0:\n    dev: false\n";
        let a = lock_ancestry("pnpm-lock.yaml", v6, "qs", "6.13.0", &[]).unwrap();
        assert_eq!(a.parents.iter().cloned().collect::<Vec<_>>(), ["express"]);
        let v5 = "lockfileVersion: 5.4\nspecifiers:\n  express: ^4.0.0\ndependencies:\n  express: 4.17.1\npackages:\n  /express/4.17.1:\n    dependencies:\n      qs: 6.7.0\n  /qs/6.7.0:\n    dev: false\n";
        let a = lock_ancestry("pnpm-lock.yaml", v5, "qs", "6.7.0", &[]).unwrap();
        assert_eq!(a.parents.iter().cloned().collect::<Vec<_>>(), ["express"]);
    }

    #[test]
    fn npm_and_yarn_lockfiles_name_every_parent() {
        let npm = r#"{"packages":{
            "":{"dependencies":{"a":"1","b":"1"}},
            "node_modules/a":{"version":"1.0.0","dependencies":{"x":"1"}},
            "node_modules/b":{"version":"1.0.0","dependencies":{"x":"1"}},
            "node_modules/x":{"version":"1.2.3"}}}"#;
        let a = lock_ancestry("package-lock.json", npm, "x", "1.2.3", &[]).unwrap();
        assert_eq!(a.parents.iter().cloned().collect::<Vec<_>>(), ["a", "b"]);
        let yarn = "# yarn lockfile v1\n\n\"a@^1.0.0\":\n  version \"1.0.0\"\n  dependencies:\n    x \"^1.2.0\"\n\nx@^1.2.0:\n  version \"1.2.3\"\n";
        let a = lock_ancestry("yarn.lock", yarn, "x", "1.2.3", &["a".into()]).unwrap();
        assert_eq!(a.parents.iter().cloned().collect::<Vec<_>>(), ["a"]);
    }

    // ── The workspace projects a changed package can break ─────────────

    fn asked() -> Vec<(String, String)> {
        vec![
            ("glob-walk".to_string(), "2.3.1".to_string()),
            ("path-match".to_string(), "3.1.2".to_string()),
        ]
    }

    fn uses(projects: &[LockProject]) -> Vec<(&str, Vec<&str>)> {
        projects
            .iter()
            .map(|p| (p.dir.as_str(), p.uses.iter().map(String::as_str).collect()))
            .collect()
    }

    /// A site, a studio no CI job builds, a shared package the site links,
    /// and a docs project that takes neither changed package.
    #[test]
    fn pnpm_workspace_projects_that_consume_a_changed_line_are_found() {
        let lock = "lockfileVersion: '9.0'\nimporters:\n  .:\n    devDependencies:\n      turbo:\n        specifier: ^2.0.0\n        version: 2.0.1\n  apps/web:\n    dependencies:\n      '@acme/ui':\n        specifier: workspace:*\n        version: link:../../packages/ui\n      next-ish:\n        specifier: ^14.0.0\n        version: 14.1.0\n  apps/studio:\n    dependencies:\n      studio-kit:\n        specifier: ^3.0.0\n        version: 3.2.0\n  apps/docs:\n    dependencies:\n      glob-walk:\n        specifier: ^4.0.0\n        version: 4.0.1\n  packages/ui:\n    dependencies:\n      path-match:\n        specifier: ^3.1.0\n        version: 3.1.2\npackages:\n  glob-walk@2.3.1: {}\n  glob-walk@4.0.1: {}\n  path-match@3.1.2: {}\n  next-ish@14.1.0: {}\n  studio-kit@3.2.0: {}\n  studio-cli@1.0.0: {}\n  turbo@2.0.1: {}\nsnapshots:\n  glob-walk@2.3.1: {}\n  glob-walk@4.0.1: {}\n  path-match@3.1.2: {}\n  next-ish@14.1.0: {}\n  studio-kit@3.2.0:\n    dependencies:\n      studio-cli: 1.0.0\n  studio-cli@1.0.0:\n    dependencies:\n      glob-walk: 2.3.1\n  turbo@2.0.1: {}\n";
        let projects = lock_projects("pnpm-lock.yaml", lock, &[], &asked()).unwrap();
        assert_eq!(
            uses(&projects),
            vec![
                ("", vec![]),
                // Two levels down, through the studio's tooling.
                ("apps/docs", vec![]),
                ("apps/studio", vec!["glob-walk@2.3.1"]),
                // Through the workspace package it links.
                ("apps/web", vec!["path-match@3.1.2"]),
                ("packages/ui", vec!["path-match@3.1.2"]),
            ],
            "the docs project has glob-walk on the 4 line only, which did not change"
        );
        let copies = lock_copies("pnpm-lock.yaml", lock).unwrap();
        assert!(copies.contains(&("glob-walk".into(), "2.3.1".into())));
        assert!(copies.contains(&("glob-walk".into(), "4.0.1".into())));
    }

    #[test]
    fn npm_workspace_projects_that_consume_a_changed_line_are_found() {
        let lock = r#"{"lockfileVersion":3,"packages":{
            "":{"name":"site","workspaces":["apps/*","packages/*"],"devDependencies":{"turbo":"^2.0.0"}},
            "apps/web":{"name":"web","dependencies":{"@acme/ui":"*","next-ish":"^14.0.0"}},
            "apps/studio":{"name":"studio","dependencies":{"studio-kit":"^3.0.0"}},
            "packages/ui":{"name":"@acme/ui","dependencies":{"path-match":"^3.1.0"}},
            "node_modules/@acme/ui":{"resolved":"packages/ui","link":true},
            "node_modules/web":{"resolved":"apps/web","link":true},
            "node_modules/studio":{"resolved":"apps/studio","link":true},
            "node_modules/turbo":{"version":"2.0.1","dev":true},
            "node_modules/next-ish":{"version":"14.1.0"},
            "node_modules/path-match":{"version":"3.1.2"},
            "node_modules/studio-kit":{"version":"3.2.0","dependencies":{"glob-walk":"^2.3.0"}},
            "node_modules/glob-walk":{"version":"4.0.1"},
            "node_modules/studio-kit/node_modules/glob-walk":{"version":"2.3.1"}}}"#;
        let projects = lock_projects("package-lock.json", lock, &[], &asked()).unwrap();
        assert_eq!(
            uses(&projects),
            vec![
                ("", vec![]),
                ("apps/studio", vec!["glob-walk@2.3.1"]),
                ("apps/web", vec!["path-match@3.1.2"]),
                ("packages/ui", vec!["path-match@3.1.2"]),
            ]
        );
        let copies = lock_copies("package-lock.json", lock).unwrap();
        assert!(copies.contains(&("glob-walk".into(), "2.3.1".into())));
        assert!(!copies.iter().any(|(n, _)| n == "web"), "a link is no copy");
    }

    #[test]
    fn yarn_workspace_projects_are_read_from_their_manifests() {
        let manifests = vec![
            (
                "".to_string(),
                r#"{"name":"site","private":true,"workspaces":["apps/*","packages/*"]}"#
                    .to_string(),
            ),
            (
                "apps/web".to_string(),
                r#"{"name":"web","dependencies":{"@acme/ui":"*","next-ish":"^14.0.0"}}"#
                    .to_string(),
            ),
            (
                "apps/studio".to_string(),
                r#"{"name":"studio","devDependencies":{"studio-kit":"^3.0.0"}}"#.to_string(),
            ),
            (
                "packages/ui".to_string(),
                r#"{"name":"@acme/ui","dependencies":{"path-match":"^3.1.0"}}"#.to_string(),
            ),
        ];
        let classic = "# yarn lockfile v1\n\nglob-walk@^2.3.0:\n  version \"2.3.1\"\n\nglob-walk@^4.0.0:\n  version \"4.0.1\"\n\nnext-ish@^14.0.0:\n  version \"14.1.0\"\n\npath-match@^3.1.0:\n  version \"3.1.2\"\n\nstudio-kit@^3.0.0:\n  version \"3.2.0\"\n  dependencies:\n    glob-walk \"^2.3.0\"\n";
        let berry = "__metadata:\n  version: 8\n\n\"glob-walk@npm:^2.3.0\":\n  version: 2.3.1\n  resolution: \"glob-walk@npm:2.3.1\"\n\n\"next-ish@npm:^14.0.0\":\n  version: 14.1.0\n  resolution: \"next-ish@npm:14.1.0\"\n\n\"path-match@npm:^3.1.0\":\n  version: 3.1.2\n  resolution: \"path-match@npm:3.1.2\"\n\n\"studio-kit@npm:^3.0.0\":\n  version: 3.2.0\n  resolution: \"studio-kit@npm:3.2.0\"\n  dependencies:\n    glob-walk: \"npm:^2.3.0\"\n";
        for lock in [classic, berry] {
            let projects = lock_projects("yarn.lock", lock, &manifests, &asked()).unwrap();
            assert_eq!(
                uses(&projects),
                vec![
                    ("", vec![]),
                    ("apps/studio", vec!["glob-walk@2.3.1"]),
                    ("apps/web", vec!["path-match@3.1.2"]),
                    ("packages/ui", vec!["path-match@3.1.2"]),
                ],
                "{lock}"
            );
        }
        // Without its projects' manifests a yarn.lock names no project.
        assert_eq!(lock_projects("yarn.lock", classic, &[], &asked()), None);
        assert_eq!(lock_projects("Cargo.lock", "", &[], &asked()), None);
    }

    #[test]
    fn the_copies_of_other_lockfiles_are_read_or_said_unreadable() {
        let cargo = "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.200\"\nsource = \"registry\"\n\n[[package]]\nname = \"time\"\nversion = \"0.3.36\"\n";
        assert_eq!(
            lock_copies("Cargo.lock", cargo).unwrap(),
            vec![
                ("serde".to_string(), "1.0.200".to_string()),
                ("time".to_string(), "0.3.36".to_string())
            ]
        );
        let gems = "GEM\n  remote: https://rubygems.org/\n  specs:\n    rack (2.2.8)\n    sinatra (3.1.0)\n      rack (~> 2.2, >= 2.2.4)\n\nDEPENDENCIES\n  sinatra\n";
        assert_eq!(
            lock_copies("Gemfile.lock", gems).unwrap(),
            vec![
                ("rack".to_string(), "2.2.8".to_string()),
                ("sinatra".to_string(), "3.1.0".to_string())
            ]
        );
        assert_eq!(
            lock_copies("go.sum", "example.com/x v1.0.0 h1:abc=\n"),
            None
        );
        assert_eq!(lock_copies("pnpm-lock.yaml", "not: [a lockfile"), None);
    }

    // ── Tiers move with what is learned ───────────────────────────────

    use crate::security_fixes::{
        self as sec, Advisory, AdvisoryRecord, Finding, FindingState, Observation, Scope,
        SecurityConfig, Stage, Tier,
    };

    fn critical(verdict: Verdict) -> (Finding, FindingState) {
        let f = Finding {
            id: "f1".into(),
            repo: "acme/web".into(),
            manifest: "pnpm-lock.yaml".into(),
            ecosystem: "npm".into(),
            package: "lodash".into(),
            version: "4.17.20".into(),
            fixed_version: Some("4.17.21".into()),
            advisory: Advisory {
                ids: vec!["CVE-2021-23337".into(), "GHSA-35jh-r3h4-6jhm".into()],
                primary: "CVE-2021-23337".into(),
                cvss: Some(9.8),
                ..Default::default()
            },
            scope: Scope::Runtime,
            verdict,
            first_seen_at: "2026-11-10T10:00:00Z".into(),
            status: sec::Status::Open,
            ..Default::default()
        };
        (f, FindingState::default())
    }

    /// A critical whose verdict was unknown sits at P1; the code check finds
    /// the vulnerable call, the verdict is reachable, and it becomes P0 with
    /// the P0 card due (its first card posted at P1).
    #[test]
    fn imported_and_called_turns_a_critical_into_p0_and_posts_again() {
        let cfg = SecurityConfig::default();
        let (mut f, mut st) = critical(Verdict::Unknown);
        sec::retier(&mut f, &st, &cfg);
        assert_eq!(f.tier, Some(Tier::P1));
        st.cards = vec!["new".into()];
        st.carded_tier = f.tier;
        let now = sec::parse_time("2026-11-10T12:00:00Z").unwrap();
        assert_eq!(sec::escalation(&f, &st, &cfg, now), None);
        let mut query = q("lodash");
        query.symbols = vec!["template".into()];
        let out = scan(
            vec![query],
            &[("src/mail.ts", "import { template } from 'lodash';\nexport const render = (b: string) => template(b)({});\n")],
        );
        let Decision::Settled(v, e) = decide(&out[0].0, &out[0].1) else {
            panic!("settled by the code");
        };
        f.verdict = v;
        f.evidence = Some(e);
        assert!(sec::retier(&mut f, &st, &cfg), "the tier rose");
        assert_eq!(f.tier, Some(Tier::P0));
        assert_eq!(sec::escalation(&f, &st, &cfg, now), Some(Stage::RaisedP0));
        st.cards.push("p0".into());
        assert_eq!(sec::escalation(&f, &st, &cfg, now), None, "once");
        // A card first posted at P0 never posts "now P0".
        let mut first_p0 = st.clone();
        first_p0.cards = vec!["new".into()];
        first_p0.carded_tier = Some(Tier::P0);
        assert_eq!(sec::escalation(&f, &first_p0, &cfg, now), None);
    }

    /// CISA lists an existing finding's CVE: the next tick's re-tier moves a
    /// runtime finding to P0, and the P0 card is due.
    #[test]
    fn a_kev_listing_on_an_existing_finding_retiers_it() {
        let cfg = SecurityConfig::default();
        let (mut f, mut st) = critical(Verdict::Unknown);
        f.advisory.cvss = Some(7.5);
        sec::retier(&mut f, &st, &cfg);
        assert_eq!(f.tier, Some(Tier::P1));
        st.cards = vec!["new".into()];
        st.carded_tier = Some(Tier::P1);
        let kev: BTreeMap<String, String> =
            [("CVE-2021-23337".to_string(), "2026-12-01".to_string())].into();
        assert!(sec::apply_kev(&mut f, &kev));
        assert!(sec::retier(&mut f, &st, &cfg));
        assert_eq!(f.tier, Some(Tier::P0));
        let now = sec::parse_time("2026-11-10T12:00:00Z").unwrap();
        assert_eq!(sec::escalation(&f, &st, &cfg, now), Some(Stage::RaisedP0));
    }

    /// An EPSS rise and a raised severity on an advisory update re-tier too.
    #[test]
    fn epss_and_a_raised_severity_retier() {
        let cfg = SecurityConfig::default();
        let (mut f, st) = critical(Verdict::Unknown);
        f.advisory.cvss = Some(5.0);
        sec::retier(&mut f, &st, &cfg);
        assert_eq!(f.tier, Some(Tier::P2));
        f.advisory.epss = Some(0.4);
        assert!(sec::retier(&mut f, &st, &cfg));
        assert_eq!(f.tier, Some(Tier::P1));
        // No score, GitHub's word only: moderate, then the advisory is
        // updated to critical. Reachable: P2, then P0.
        let (mut f, st) = critical(Verdict::Reachable);
        f.advisory.cvss = None;
        let (repo, manifest, version) = (f.repo.clone(), f.manifest.clone(), f.version.clone());
        let obs = |sev: &str| Observation {
            repo: repo.clone(),
            manifest: manifest.clone(),
            ecosystem: "npm".into(),
            package: "lodash".into(),
            version: version.clone(),
            advisory: AdvisoryRecord {
                ids: vec!["GHSA-35jh-r3h4-6jhm".into()],
                severity: Some(sev.into()),
                source: "github_advisory".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let now = sec::parse_time("2026-11-10T12:00:00Z").unwrap();
        let (mut f, st, _) = sec::apply(Some((f, st)), &obs("moderate"), now);
        sec::retier(&mut f, &st, &cfg);
        assert_eq!(f.tier, Some(Tier::P2));
        let (mut f, st, _) = sec::apply(Some((f, st)), &obs("critical"), now);
        assert_eq!(st.severity.as_deref(), Some("critical"));
        assert!(sec::retier(&mut f, &st, &cfg));
        assert_eq!(f.tier, Some(Tier::P0));
        // A lower word later never lowers it.
        let (_, st, _) = sec::apply(Some((f, st)), &obs("low"), now);
        assert_eq!(st.severity.as_deref(), Some("critical"));
    }

    /// 140 packages a day under the 400k cap: every one the code settles in
    /// the first tick, and the rest (the ones with lines for the model)
    /// within two days of ticks every 20 minutes, never over the cap.
    #[test]
    fn a_day_of_ticks_settles_140_packages_under_the_cap() {
        let cap = 400_000u64;
        let per_tick = 12;
        let estimate = estimate_call_tokens(8_000);
        // Measured on an install: 140 packages, about a third with lines
        // for the model; each call costs its estimate.
        let mut asks = 50usize;
        let (mut day_tokens, mut ticks) = (0u64, 0usize);
        while asks > 0 {
            ticks += 1;
            if ticks % 72 == 1 {
                day_tokens = 0;
            }
            let n = model_calls_allowed(day_tokens, cap, estimate, per_tick).min(asks);
            asks -= n;
            day_tokens += n as u64 * estimate;
            assert!(day_tokens <= cap, "over the cap at tick {ticks}");
            assert!(ticks <= 144, "two days of ticks and {asks} still waiting");
        }
        assert!(ticks <= 6, "{ticks} ticks for 50 model reads");
        assert_eq!(model_calls_allowed(cap - 100, cap, estimate, per_tick), 0);
    }
}
