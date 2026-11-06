//! Rule 5: a new thing of a kind that exists is registered everywhere its
//! siblings are.
//!
//! A new CMS block, page-builder block, route, job, migration or flag file
//! usually has to appear in more places than its own file: a registry, an
//! index file, a query fragment, a map, a test map, the docs. Before the pull
//! request, for each item the change adds:
//!
//! 1. its siblings are the existing items of the same kind: files with the
//!    same extension in the same folder, or, for an item that is a new
//!    folder (`blocks/Quote/index.tsx`), the other folders beside it;
//! 2. the places that register them are the files that name most of the
//!    siblings (a registry names all of them; a page that uses two of them
//!    is not a registration), found by code search on the default branch.
//!    Comments never count. Docs, migrations, fixtures and tests are never
//!    places. A manifest (`package.json` and other data files) counts only
//!    where it names the siblings as paths or keys (`"./accordion"`), never
//!    inside other names such as a dependency (`@radix-ui/react-accordion`);
//! 3. a place is a registry when it names the siblings by path or as a
//!    whole string (an import, an `exports` key, a list of ids); a code file
//!    that only mentions them as words is a maybe;
//! 4. a new file is an item of the siblings' kind only when its structure
//!    says so, never because of the folder it sits in ([`kind_of`]): it has
//!    the export shape the siblings share (a default export, a call to the
//!    same definer such as `defineType(...)`, the same base class or trait)
//!    and the shape the registries import them by (a registry that imports
//!    each sibling's default export needs one from the new file too). A
//!    helper, validator, utility, types or generated file next to them is
//!    another kind and has no places. When the structure can't tell (the
//!    file or too few siblings couldn't be read, or the siblings share no
//!    shape), its places are only noted, never a block;
//! 5. every place must name the new item on the run's branch.
//!
//! What a registry is missing goes back to the coder once with the list, and
//! blocks the pull request if still missing. A maybe never blocks: it is
//! noted in the pull request for a person to check. And a block never stops
//! the run: it is an Open item on the pull request (see `feature::gate`).

use std::collections::BTreeSet;

use crate::clients::github::GitHubClient;
use crate::passes::feature::reuse::name_words;

/// Most siblings sampled per item.
const MAX_SIBLINGS: usize = 8;
/// Most candidate files read per item.
const MAX_CANDIDATES: usize = 40;

/// A thing the change adds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewItem {
    /// Its name: the file stem, or the folder name for a folder item.
    pub id: String,
    /// The folder its siblings live in.
    pub parent: String,
    /// A folder item (`blocks/Quote/`), else a file item.
    pub folder: bool,
    /// File items: the extension siblings share.
    pub ext: String,
    /// The path that stands for it (the file, or the folder).
    pub path: String,
}

fn split_path(p: &str) -> (&str, &str) {
    match p.rsplit_once('/') {
        Some((d, f)) => (d, f),
        None => ("", p),
    }
}

fn stem_ext(file: &str) -> (&str, &str) {
    match file.split_once('.') {
        Some((s, e)) => (s, e),
        None => (file, ""),
    }
}

/// File names that stand for their folder.
const FOLDER_FILES: &[&str] = &[
    "index", "page", "route", "mod", "layout", "main", "__init__",
];

/// Paths that are never items or registration places.
fn skipped(p: &str) -> bool {
    let l = p.to_lowercase();
    let (_, f) = split_path(&l);
    l.ends_with(".lock")
        || f == "package-lock.json"
        || f == "pnpm-lock.yaml"
        || f.starts_with("changelog")
        || l.contains("node_modules/")
        || l.contains("/dist/")
        || l.contains("/build/")
        || f.ends_with(".snap")
        || f.ends_with(".map")
        || f.ends_with(".min.js")
}

fn is_test(p: &str) -> bool {
    let l = p.to_lowercase();
    l.contains(".test.")
        || l.contains(".spec.")
        || l.contains("/__tests__/")
        || l.contains(".stories.")
}

/// Type declarations and generated files: never items of their own (a
/// generated file is regenerated, it is not registered).
fn is_generated_or_types(p: &str) -> bool {
    let l = p.to_lowercase();
    l.ends_with(".d.ts")
        || l.contains(".generated.")
        || l.contains(".gen.")
        || l.contains("/generated/")
        || l.contains("/__generated__/")
}

/// The items the added files make. `added` are the paths the change adds;
/// `tree` is the default branch's file list.
pub fn new_items(added: &[String], tree: &[String]) -> Vec<NewItem> {
    let mut out: Vec<NewItem> = Vec::new();
    for p in added {
        if skipped(p) || is_test(p) || is_generated_or_types(p) {
            continue;
        }
        let (dir, file) = split_path(p);
        let (stem, ext) = stem_ext(file);
        // The folder is new when the default branch has nothing under it.
        let prefix = format!("{dir}/");
        let folder_is_new = !dir.is_empty() && !tree.iter().any(|t| t.starts_with(&prefix));
        let item = if folder_is_new || FOLDER_FILES.contains(&stem) {
            let (parent, name) = split_path(dir);
            if name.is_empty() || name.starts_with('[') || name.starts_with('(') {
                continue;
            }
            NewItem {
                id: name.to_string(),
                parent: parent.to_string(),
                folder: true,
                ext: String::new(),
                path: dir.to_string(),
            }
        } else {
            NewItem {
                id: stem.to_string(),
                parent: dir.to_string(),
                folder: false,
                ext: ext.to_string(),
                path: p.clone(),
            }
        };
        if !out.iter().any(|i| i.path == item.path) {
            out.push(item);
        }
    }
    out
}

/// The existing items of the same kind, from the default branch's tree. The
/// plan's recorded sibling, when it is one of them, comes first.
pub fn siblings(item: &NewItem, tree: &[String], recorded: Option<&str>) -> Vec<String> {
    let prefix = if item.parent.is_empty() {
        String::new()
    } else {
        format!("{}/", item.parent)
    };
    let mut out: BTreeSet<String> = BTreeSet::new();
    for t in tree {
        let Some(rest) = t.strip_prefix(&prefix) else {
            continue;
        };
        if skipped(t) || is_test(t) || is_generated_or_types(t) {
            continue;
        }
        if item.folder {
            if let Some((name, _)) = rest.split_once('/') {
                if name != item.id
                    && !name.starts_with('[')
                    && !name.starts_with('(')
                    && !name.starts_with('_')
                {
                    out.insert(name.to_string());
                }
            }
        } else if !rest.contains('/') {
            let (stem, ext) = stem_ext(rest);
            if ext == item.ext && stem != item.id && !FOLDER_FILES.contains(&stem) {
                out.insert(stem.to_string());
            }
        }
    }
    let mut list: Vec<String> = out.into_iter().collect();
    if let Some(r) = recorded {
        let (dir, file) = split_path(r.trim_end_matches('/'));
        let name = if item.folder {
            if FOLDER_FILES.contains(&stem_ext(file).0) {
                split_path(dir).1
            } else {
                file
            }
        } else {
            stem_ext(file).0
        };
        if let Some(i) = list.iter().position(|s| s == name) {
            let s = list.remove(i);
            list.insert(0, s);
        }
    }
    list.truncate(MAX_SIBLINGS);
    list
}

/// The ways code names an item: as written, PascalCase, camelCase,
/// kebab-case and snake_case.
pub fn variants(id: &str) -> Vec<String> {
    let w = name_words(id);
    let mut out = vec![id.to_string()];
    if !w.is_empty() {
        let cap = |s: &str| {
            let mut c = s.chars();
            c.next()
                .map(|f| f.to_ascii_uppercase().to_string() + c.as_str())
                .unwrap_or_default()
        };
        let pascal: String = w.iter().map(|x| cap(x)).collect();
        let camel = format!(
            "{}{}",
            w[0],
            w[1..].iter().map(|x| cap(x)).collect::<String>()
        );
        out.push(pascal);
        out.push(camel);
        out.push(w.join("-"));
        out.push(w.join("_"));
    }
    out.sort();
    out.dedup();
    out
}

/// Whether `text` names `word` on its own (not inside a longer name).
fn names_word(text: &str, word: &str) -> bool {
    if word.is_empty() {
        return false;
    }
    let ok = |c: Option<char>| c.is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
    let mut from = 0;
    while let Some(i) = text[from..].find(word) {
        let at = from + i;
        if ok(text[..at].chars().next_back()) && ok(text[at + word.len()..].chars().next()) {
            return true;
        }
        from = at + word.len();
    }
    false
}

/// Whether `text` names the item in any of its forms.
pub fn names_item(text: &str, id: &str) -> bool {
    variants(id).iter().any(|v| names_word(text, v))
}

/// Extensions of files that are data, not code: they register by path or
/// key, never by a word inside a longer name.
const DATA_EXTS: &[&str] = &["json", "jsonc", "yaml", "yml", "toml"];

/// Paths that are never registration places: docs, migrations, fixtures.
fn never_a_place(p: &str) -> bool {
    let l = p.to_lowercase();
    let (_, f) = split_path(&l);
    let ext = f.rsplit_once('.').map(|(_, e)| e).unwrap_or("");
    matches!(ext, "md" | "mdx" | "rst" | "txt")
        || l.starts_with("migrations/")
        || l.contains("/migrations/")
        || l.contains("/migration/")
        || l.contains("/fixtures/")
        || l.contains("/__fixtures__/")
        || l.contains("/__mocks__/")
        || l.starts_with("docs/")
        || l.contains("/docs/")
}

fn is_data_file(p: &str) -> bool {
    let (_, f) = split_path(p);
    f.rsplit_once('.')
        .is_some_and(|(_, e)| DATA_EXTS.contains(&e.to_lowercase().as_str()))
}

/// The text without comments: `//` and `/* */` in code, `#` lines in
/// scripts and YAML. Approximate (strings that look like comments are kept
/// when `//` follows a `:`, as in a URL).
pub fn without_comments(path: &str, text: &str) -> String {
    let (_, f) = split_path(path);
    let ext = f
        .rsplit_once('.')
        .map(|(_, e)| e.to_lowercase())
        .unwrap_or_default();
    let hash = matches!(ext.as_str(), "py" | "rb" | "sh" | "yaml" | "yml" | "toml");
    let mut out = String::with_capacity(text.len());
    let b = text.as_bytes();
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            out.push(c as char);
            if c == b'\\' && i + 1 < b.len() {
                out.push(b[i + 1] as char);
                i += 2;
                continue;
            }
            if c == q || c == b'\n' {
                quote = None;
            }
            i += 1;
            continue;
        }
        if !hash && c == b'/' && b.get(i + 1) == Some(&b'*') {
            match text[i + 2..].find("*/") {
                Some(end) => i += end + 4,
                None => break,
            }
            continue;
        }
        let line_comment = if hash {
            c == b'#' && (i == 0 || b[i - 1].is_ascii_whitespace())
        } else {
            c == b'/' && b.get(i + 1) == Some(&b'/') && (i == 0 || b[i - 1] != b':')
        };
        if line_comment {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'"' || c == b'\'' || c == b'`' {
            quote = Some(c);
        }
        out.push(c as char);
        i += 1;
    }
    out
}

/// The quoted strings in `text` (one line each).
fn quoted(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'"' || c == b'\'' || c == b'`' {
            let start = i + 1;
            let mut j = start;
            while j < b.len() && b[j] != c && b[j] != b'\n' {
                j += 1;
            }
            if j < b.len() && b[j] == c {
                out.push(&text[start..j]);
                i = j + 1;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// Whether `text` names the item as a path or a whole string: a quoted value
/// with a path segment that is the item (`"./accordion"`,
/// `"@/components/ui/accordion"`, `"accordion"`, `"./Hero.tsx"`), not a
/// longer name that contains it (`"@radix-ui/react-accordion"`).
pub fn names_as_path(text: &str, id: &str) -> bool {
    let forms = variants(id);
    quoted(text).into_iter().any(|q| {
        q.split('/').any(|seg| {
            let stem = seg.split('.').next().unwrap_or(seg);
            !stem.is_empty() && forms.iter().any(|f| f == stem)
        })
    })
}

/// The bare packages a file imports (`from 'sanity'`, `require("x")`), not
/// relative or aliased paths: what marks a kind of item.
pub fn imported_packages(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for key in ["from ", "require(", "import "] {
        let mut from = 0;
        while let Some(i) = text[from..].find(key) {
            let at = from + i + key.len();
            from = at;
            let rest = text[at..].trim_start();
            let Some(q) = rest.chars().next().filter(|c| *c == '"' || *c == '\'') else {
                continue;
            };
            let Some(end) = rest[1..].find(q) else {
                continue;
            };
            let spec = &rest[1..1 + end];
            if spec.is_empty()
                || spec.starts_with('.')
                || spec.starts_with("@/")
                || spec.starts_with("~/")
                || spec.starts_with('/')
            {
                continue;
            }
            // The package, without a subpath (`@scope/name`, or `name`).
            let mut parts = spec.split('/');
            let first = parts.next().unwrap_or("");
            let pkg = if first.starts_with('@') {
                format!("{first}/{}", parts.next().unwrap_or(""))
            } else {
                first.to_string()
            };
            out.insert(pkg);
        }
    }
    out
}

/// Whether the new item is of another kind than its siblings: every read
/// sibling (at least two) imports a package the new file imports none of.
pub fn other_kind(new_text: &str, sibling_texts: &[String]) -> bool {
    if sibling_texts.len() < 2 {
        return false;
    }
    let mut common: Option<BTreeSet<String>> = None;
    for t in sibling_texts {
        let pk = imported_packages(t);
        common = Some(match common {
            None => pk,
            Some(c) => c.intersection(&pk).cloned().collect(),
        });
    }
    let common = common.unwrap_or_default();
    if common.is_empty() {
        return false;
    }
    let mine = imported_packages(new_text);
    common.is_disjoint(&mine)
}

/// What a file exports, by structure: what makes two files the same kind of
/// item, whatever the framework.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Shape {
    /// A default export (`export default ...`, `module.exports = ...`).
    pub default_export: bool,
    /// What its exports are built with: the function a top-level export
    /// calls (`defineType` in `export default defineType({...})`), a base
    /// class (`class:Model`), a trait it implements (`impl:Block`) or a
    /// decorator (`@register`).
    pub definers: BTreeSet<String>,
}

const NOT_DEFINERS: &[&str] = &[
    "function", "class", "async", "new", "await", "typeof", "return", "if", "for", "while",
];

/// The dotted name at the start of `s` and what follows it.
fn lead_name(s: &str) -> (&str, &str) {
    let end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == '.'))
        .unwrap_or(s.len());
    (&s[..end], &s[end..])
}

/// The definer a value expression is built with: `defineType({...})` gives
/// `defineType`, `z.object<...>(...)` gives `z.object`; a literal gives none.
fn called(expr: &str) -> Option<String> {
    let (name, rest) = lead_name(expr.trim_start());
    let rest = rest.trim_start();
    (!name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && !NOT_DEFINERS.contains(&name)
        && (rest.starts_with('(') || rest.starts_with('<')))
    .then(|| name.to_string())
}

/// The base class after `extends`, if any.
fn extends(rest: &str) -> Option<String> {
    let at = rest.find(" extends ")?;
    let (name, _) = lead_name(rest[at + 9..].trim_start());
    (!name.is_empty()).then(|| format!("class:{name}"))
}

/// The file's [`Shape`], read from its top-level lines (comments left out).
pub fn shape(path: &str, text: &str) -> Shape {
    let text = without_comments(path, text);
    let mut out = Shape::default();
    // `const author = defineType(...)` then `export default author`.
    let mut locals: Vec<(String, String)> = Vec::new();
    let mut exported: Vec<String> = Vec::new();
    for line in text.lines() {
        if line.starts_with(char::is_whitespace) {
            continue;
        }
        let l = line.trim_end();
        if let Some(rest) = l
            .strip_prefix("export default ")
            .or_else(|| l.strip_prefix("module.exports ="))
            .or_else(|| l.strip_prefix("export ="))
        {
            out.default_export = true;
            let rest = rest.trim_start();
            if let Some(d) = called(rest) {
                out.definers.insert(d);
            } else if let Some(b) = extends(rest) {
                out.definers.insert(b);
            } else {
                let (name, _) = lead_name(rest);
                if !name.is_empty() {
                    exported.push(name.to_string());
                }
            }
            continue;
        }
        if let Some(list) = l.strip_prefix("export {") {
            for part in list.trim_end_matches(['}', ';', ' ']).split(',') {
                let mut words = part.split_whitespace();
                let name = words.next().unwrap_or("");
                if words.next() == Some("as") && words.next() == Some("default") {
                    out.default_export = true;
                }
                exported.push(name.to_string());
            }
            continue;
        }
        let (is_export, decl) = match l.strip_prefix("export ") {
            Some(d) => (true, d.trim_start()),
            None => (false, l),
        };
        let decl = decl.strip_prefix("declare ").unwrap_or(decl);
        if let Some(rest) = ["const ", "let ", "var "]
            .iter()
            .find_map(|k| decl.strip_prefix(k))
        {
            let (name, after) = lead_name(rest);
            if let Some((_, value)) = after.split_once('=') {
                if let Some(d) = called(value) {
                    if is_export {
                        out.definers.insert(d.clone());
                    }
                    locals.push((name.to_string(), d));
                }
            }
            continue;
        }
        if let Some(rest) = decl.strip_prefix("class ") {
            if let Some(b) = extends(&format!(" {rest}")) {
                if is_export {
                    out.definers.insert(b);
                }
            } else if let Some((_, bases)) = rest.split_once('(') {
                // Python: `class Article(Model):`.
                let (base, _) = lead_name(bases.trim_start());
                if !base.is_empty() && rest.trim_end().ends_with(':') {
                    out.definers.insert(format!("class:{base}"));
                }
            }
            continue;
        }
        if let Some(rest) = l.strip_prefix("impl") {
            // Rust: `impl Block for Quote`, `impl<T> Block for Quote<T>`.
            let rest = rest.trim_start();
            let rest = if rest.starts_with('<') {
                rest.split_once('>')
                    .map(|(_, r)| r)
                    .unwrap_or("")
                    .trim_start()
            } else {
                rest
            };
            if let Some((tr, _)) = rest.split_once(" for ") {
                let (name, _) = lead_name(tr.trim());
                if !name.is_empty() {
                    out.definers.insert(format!("impl:{name}"));
                }
            }
            continue;
        }
        if let Some(rest) = l.strip_prefix('@') {
            // A top-level decorator (`@register`, `@app.route(...)`).
            let (name, _) = lead_name(rest);
            if !name.is_empty() {
                out.definers.insert(format!("@{name}"));
            }
        }
    }
    for name in exported {
        if let Some((_, d)) = locals.iter().find(|(n, _)| *n == name) {
            out.definers.insert(d.clone());
        }
    }
    out
}

/// Whether `text` imports the sibling's default export: `import author from
/// './author'` (a registry that does needs one from every item it lists).
pub fn imports_default(text: &str, sib: &str) -> bool {
    let forms = variants(sib);
    let mut from = 0;
    while let Some(i) = text[from..].find("import ") {
        let at = from + i;
        from = at + 7;
        if at > 0 && !text[..at].ends_with(|c: char| c.is_whitespace() || c == ';') {
            continue;
        }
        let rest = text[from..].trim_start();
        let rest = rest.strip_prefix("type ").unwrap_or(rest).trim_start();
        let (name, after) = lead_name(rest);
        if name.is_empty() || name.contains('.') || name == "from" {
            continue;
        }
        let Some(spec_at) = after.find(" from ") else {
            continue;
        };
        // The import clause is the default binding, optionally `, { ... }`.
        let clause = &after[..spec_at];
        if !(clause.trim().is_empty() || clause.trim_start().starts_with(',')) {
            continue;
        }
        let spec = after[spec_at + 6..].trim_start();
        let Some(q) = spec.chars().next().filter(|c| *c == '"' || *c == '\'') else {
            continue;
        };
        let Some(end) = spec[1..].find(q) else {
            continue;
        };
        let last = spec[1..1 + end].rsplit('/').next().unwrap_or("");
        let stem = last.split('.').next().unwrap_or(last);
        if forms.iter().any(|f| f == stem) {
            return true;
        }
    }
    false
}

/// Whether a new file is the same kind of item as its siblings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// It has the shape the siblings share: its places must name it.
    Same,
    /// Another kind (a helper or validator beside them): it has no places.
    Other(String),
    /// Can't tell: its places are noted for a person, never a block.
    Unknown(String),
}

/// Whether the new file (`new`: path and text, `None` when it couldn't be
/// read) is the siblings' kind, judged by structure: the export shape the
/// read siblings all share, and the shape the registries import them by
/// (`registries`: their texts). Never by the folder.
pub fn kind_of(
    new: Option<(&str, &str)>,
    sibling_files: &[(String, String)],
    registries: &[&str],
    sibs: &[String],
) -> Kind {
    let Some((new_path, new_text)) = new else {
        return Kind::Unknown("its file couldn't be read to compare".to_string());
    };
    if sibling_files.len() < 2 {
        return Kind::Unknown("too few of its siblings could be read to compare".to_string());
    }
    let texts: Vec<String> = sibling_files.iter().map(|(_, t)| t.clone()).collect();
    if other_kind(new_text, &texts) {
        return Kind::Other("it imports none of the packages its siblings all import".to_string());
    }
    let shapes: Vec<Shape> = sibling_files.iter().map(|(p, t)| shape(p, t)).collect();
    let all_default = shapes.iter().all(|s| s.default_export);
    let common: BTreeSet<String> = shapes
        .iter()
        .skip(1)
        .fold(shapes[0].definers.clone(), |acc, s| {
            acc.intersection(&s.definers).cloned().collect()
        });
    let imported_default = registries
        .iter()
        .any(|r| sibs.iter().any(|s| imports_default(r, s)));
    if !all_default && !imported_default && common.is_empty() {
        return Kind::Unknown("its siblings share no export shape to compare it with".to_string());
    }
    let mine = shape(new_path, new_text);
    if (all_default || imported_default) && !mine.default_export {
        return Kind::Other(if imported_default {
            "it has no default export, and the registries import each sibling's default export"
                .to_string()
        } else {
            "it has no default export, which every sibling has".to_string()
        });
    }
    if !common.is_empty() && mine.definers.is_disjoint(&common) {
        let names = common
            .iter()
            .map(|d| format!("`{d}`"))
            .collect::<Vec<_>>()
            .join(", ");
        return Kind::Other(format!(
            "its exports aren't built with {names}, as every sibling's are"
        ));
    }
    Kind::Same
}

/// What the new item is missing, once its kind is known: nothing for
/// another kind, and only notes when the kind can't be told.
pub fn judge(
    item: &NewItem,
    sibs: &[String],
    kind: &Kind,
    places: &[(String, String, bool)],
    on_branch: &dyn Fn(&str) -> Option<String>,
) -> Vec<Missing> {
    match kind {
        Kind::Other(_) => Vec::new(),
        Kind::Same => missing(item, sibs, places, on_branch),
        Kind::Unknown(why) => missing(item, sibs, places, on_branch)
            .into_iter()
            .map(|mut m| {
                m.registry = false;
                m.unsure = Some(why.clone());
                m
            })
            .collect(),
    }
}

/// A sibling's own file on the default branch: `parent/<sib>.<ext>`, or its
/// folder's index-like file.
fn sibling_file(item: &NewItem, sib: &str, tree: &[String]) -> Option<String> {
    let base = if item.parent.is_empty() {
        sib.to_string()
    } else {
        format!("{}/{sib}", item.parent)
    };
    if item.folder {
        let prefix = format!("{base}/");
        tree.iter()
            .find(|t| {
                t.strip_prefix(&prefix).is_some_and(|rest| {
                    !rest.contains('/') && FOLDER_FILES.contains(&stem_ext(rest).0)
                })
            })
            .cloned()
    } else {
        let want = format!("{base}.{}", item.ext);
        tree.iter().find(|t| **t == want).cloned()
    }
}

/// Whether a file is one of a sibling's own files (its folder, or its file).
fn sibling_own(path: &str, item: &NewItem, sibs: &[String]) -> bool {
    let prefix = if item.parent.is_empty() {
        String::new()
    } else {
        format!("{}/", item.parent)
    };
    path.strip_prefix(&prefix).is_some_and(|rest| {
        let first = rest.split('/').next().unwrap_or("");
        let stem = stem_ext(first).0;
        sibs.iter().any(|s| s == first || s == stem)
    })
}

/// The files that register items of this kind: those naming most sampled
/// siblings (all of them when there are three or fewer; otherwise at least
/// three and 60% of them), with comments left out. A place is a registry
/// (`true`) when it names them as paths or whole strings, else a maybe
/// (`false`, code files only). `files` are (path, default-branch text).
pub fn registration_places(
    item: &NewItem,
    sibs: &[String],
    files: &[(String, String)],
) -> Vec<(String, bool)> {
    if sibs.is_empty() {
        return Vec::new();
    }
    let n = sibs.len();
    let need = if n <= 3 {
        n
    } else {
        3usize.max((n * 3).div_ceil(5))
    };
    files
        .iter()
        .filter(|(p, _)| {
            !skipped(p)
                && !is_test(p)
                && !never_a_place(p)
                && !sibling_own(p, item, sibs)
                && *p != item.path
        })
        .filter_map(|(p, raw)| {
            let text = without_comments(p, raw);
            let as_paths = sibs.iter().filter(|s| names_as_path(&text, s)).count();
            if as_paths >= need {
                return Some((p.clone(), true));
            }
            if is_data_file(p) {
                return None;
            }
            let as_words = sibs.iter().filter(|s| names_item(&text, s)).count();
            (as_words >= need).then(|| (p.clone(), false))
        })
        .collect()
}

/// A place a new item is missing from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Missing {
    pub item: String,
    pub path: String,
    /// The siblings that place names, for the message.
    pub siblings: Vec<String>,
    /// A registry (names the siblings as paths or whole strings): missing
    /// from it blocks. Else a maybe, only noted.
    pub registry: bool,
    /// Set when the structure couldn't tell whether the new file is the
    /// siblings' kind: why. Only noted.
    pub unsure: Option<String>,
}

impl Missing {
    pub fn line(&self) -> String {
        let names = self
            .siblings
            .iter()
            .take(3)
            .map(|s| format!("`{s}`"))
            .collect::<Vec<_>>()
            .join(", ");
        if let Some(why) = &self.unsure {
            return format!(
                "`{}` may need registering in `{}` (it names {names}); couldn't tell whether it is \
                 the same kind of item ({why}); check by hand",
                self.item, self.path
            );
        }
        if self.registry {
            format!(
                "`{}` isn't registered in `{}` (it names {names})",
                self.item, self.path
            )
        } else {
            format!(
                "`{}` may belong in `{}`: it mentions {names}, but not as a list of them; check by hand",
                self.item, self.path
            )
        }
    }
}

/// The places that don't name the item on the branch (comments left out).
/// `places` are (path, default-branch text, registry); `on_branch`: path to
/// the place's text on the run's branch (`None`: removed on the branch).
pub fn missing(
    item: &NewItem,
    sibs: &[String],
    places: &[(String, String, bool)],
    on_branch: &dyn Fn(&str) -> Option<String>,
) -> Vec<Missing> {
    let mut out = Vec::new();
    for (path, base_text, registry) in places {
        let Some(text) = on_branch(path) else {
            continue;
        };
        if !names_item(&without_comments(path, &text), &item.id) {
            let base = without_comments(path, base_text);
            out.push(Missing {
                item: item.id.clone(),
                path: path.clone(),
                siblings: sibs
                    .iter()
                    .filter(|s| names_item(&base, s))
                    .cloned()
                    .collect(),
                registry: *registry,
                unsure: None,
            });
        }
    }
    out
}

/// Registries the item is missing from: these go back to the coder and block.
pub fn blocking(missing: &[Missing]) -> Vec<Missing> {
    missing.iter().filter(|m| m.registry).cloned().collect()
}

/// What the coder is told.
pub fn feedback(missing: &[Missing]) -> String {
    let mut s = String::from(
        "\nThe change adds items of a kind that already exists, but doesn't register them \
         everywhere their siblings are registered. Add the entry yourself in each of these \
         places, following how the siblings appear there (don't stop to ask), then regenerate \
         any generated files that list them:\n",
    );
    for m in missing.iter().filter(|m| m.registry) {
        s.push_str(&format!("- {}\n", m.line()));
    }
    s
}

/// Checks every new item. `added` are the change's added paths; `tree` is
/// the default branch's file list. Returns what is missing and how many
/// registration places were checked.
#[allow(clippy::too_many_arguments)]
pub async fn check(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    branch: &str,
    added: &[String],
    tree: &[String],
    recorded_sibling: Option<&str>,
) -> (Vec<Missing>, usize) {
    let mut out: Vec<Missing> = Vec::new();
    let mut checked = 0usize;
    for item in new_items(added, tree).into_iter().take(6) {
        let sibs = siblings(&item, tree, recorded_sibling);
        if sibs.is_empty() {
            continue;
        }
        // Of the siblings' kind? Compare what the files import.
        let own_path = if item.folder {
            added
                .iter()
                .find(|p| {
                    p.strip_prefix(&format!("{}/", item.path))
                        .is_some_and(|rest| {
                            !rest.contains('/') && FOLDER_FILES.contains(&stem_ext(rest).0)
                        })
                })
                .cloned()
        } else {
            Some(item.path.clone())
        };
        let own = match own_path {
            Some(p) => github
                .read_file(owner, repo, &p, branch)
                .await
                .ok()
                .map(|t| (p, t)),
            None => None,
        };
        let mut sib_files: Vec<(String, String)> = Vec::new();
        for s in sibs.iter().take(3) {
            if let Some(f) = sibling_file(&item, s, tree) {
                if let Ok(t) = github.read_file(owner, repo, &f, base).await {
                    sib_files.push((f, t));
                }
            }
        }
        // Another kind by its imports alone: no need to search for places.
        if let Some((_, t)) = &own {
            let texts: Vec<String> = sib_files.iter().map(|(_, t)| t.clone()).collect();
            if other_kind(t, &texts) {
                continue;
            }
        }
        let mut candidates: Vec<String> = Vec::new();
        for s in sibs.iter().take(3) {
            if let Ok(hits) = github.search_code(owner, repo, base, s).await {
                for h in hits {
                    if !candidates.contains(&h.path) {
                        candidates.push(h.path);
                    }
                }
            }
        }
        candidates.retain(|p| !added.contains(p));
        candidates.truncate(MAX_CANDIDATES);
        let mut files: Vec<(String, String)> = Vec::new();
        for p in candidates {
            if let Ok(t) = github.read_file(owner, repo, &p, base).await {
                files.push((p, t));
            }
        }
        let keep = registration_places(&item, &sibs, &files);
        let places: Vec<(String, String, bool)> = files
            .into_iter()
            .filter_map(|(p, t)| {
                keep.iter()
                    .find(|(k, _)| *k == p)
                    .map(|(_, registry)| (p, t, *registry))
            })
            .collect();
        let registries: Vec<&str> = places
            .iter()
            .filter(|(_, _, registry)| *registry)
            .map(|(_, t, _)| t.as_str())
            .collect();
        let kind = kind_of(
            own.as_ref().map(|(p, t)| (p.as_str(), t.as_str())),
            &sib_files,
            &registries,
            &sibs,
        );
        if let Kind::Other(why) = &kind {
            tracing::info!(item = %item.path, why, "Not a new item of its siblings' kind");
            continue;
        }
        checked += places.len();
        let mut on_branch: Vec<(String, Option<String>)> = Vec::new();
        for (p, _, _) in &places {
            on_branch.push((
                p.clone(),
                github.read_file(owner, repo, p, branch).await.ok(),
            ));
        }
        let lookup = |p: &str| {
            on_branch
                .iter()
                .find(|(q, _)| q == p)
                .and_then(|(_, t)| t.clone())
        };
        for m in judge(&item, &sibs, &kind, &places, &lookup) {
            if !out.iter().any(|x| x.item == m.item && x.path == m.path) {
                out.push(m);
            }
        }
    }
    (out, checked)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree() -> Vec<String> {
        [
            "src/blocks/Hero.tsx",
            "src/blocks/Cards.tsx",
            "src/blocks/Banner.tsx",
            "src/blocks/Hero.test.tsx",
            "src/blocks/index.ts",
            "src/blocks/registry.ts",
            "src/pages/home.tsx",
            "src/queries/page.groq.ts",
            "cms/schemas/hero/index.ts",
            "cms/schemas/cards/index.ts",
            "cms/schemas/index.ts",
            "e2e/test-map.json",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn reads_file_items_and_folder_items() {
        let added = vec![
            "src/blocks/Quote.tsx".to_string(),
            "src/blocks/Quote.test.tsx".to_string(),
            "cms/schemas/quote/index.ts".to_string(),
            "package-lock.json".to_string(),
        ];
        let items = new_items(&added, &tree());
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].id, "Quote");
        assert!(!items[0].folder);
        assert_eq!(items[1].id, "quote");
        assert!(items[1].folder);
        assert_eq!(items[1].parent, "cms/schemas");
        assert_eq!(
            siblings(&items[0], &tree(), Some("src/blocks/Cards.tsx")),
            vec!["Cards", "Banner", "Hero"]
        );
        assert_eq!(siblings(&items[1], &tree(), None), vec!["cards", "hero"]);
    }

    #[test]
    fn names_items_in_any_case_style_but_not_inside_longer_names() {
        assert_eq!(
            variants("quoteBlock"),
            vec!["QuoteBlock", "quote-block", "quoteBlock", "quote_block"]
        );
        assert!(names_item("import QuoteBlock from './x'", "quote-block"));
        assert!(names_item("_type == \"quoteBlock\"", "QuoteBlock"));
        assert!(!names_item("HeroBanner", "Hero"));
    }

    #[test]
    fn a_registry_is_a_place_and_a_page_using_two_siblings_is_not() {
        let item = &new_items(&["src/blocks/Quote.tsx".to_string()], &tree())[0];
        let sibs = siblings(item, &tree(), None);
        let files = vec![
            (
                "src/blocks/index.ts".to_string(),
                "export { Hero } from './Hero'\nexport { Cards } from './Cards'\nexport { Banner } from './Banner'\n".to_string(),
            ),
            (
                "src/pages/home.tsx".to_string(),
                "<Hero /> <Cards />".to_string(),
            ),
            (
                "e2e/test-map.json".to_string(),
                "{\"hero\": \"hero.spec.ts\", \"cards\": \"cards.spec.ts\", \"banner\": \"b.spec.ts\"}".to_string(),
            ),
            (
                "src/blocks/Hero.tsx".to_string(),
                "Hero Cards Banner".to_string(),
            ),
        ];
        let places = registration_places(item, &sibs, &files);
        assert_eq!(
            places,
            vec![
                ("src/blocks/index.ts".to_string(), true),
                ("e2e/test-map.json".to_string(), true)
            ]
        );

        let placed: Vec<(String, String, bool)> = files
            .into_iter()
            .filter_map(|(p, t)| {
                places
                    .iter()
                    .find(|(k, _)| *k == p)
                    .map(|(_, r)| (p, t, *r))
            })
            .collect();
        // The branch registered it in the index but not in the test map.
        let branch = |p: &str| -> Option<String> {
            Some(match p {
                "src/blocks/index.ts" => {
                    "export { Quote } from './Quote'\nexport { Hero } from './Hero'".to_string()
                }
                _ => "{\"hero\": \"hero.spec.ts\"}".to_string(),
            })
        };
        let m = missing(item, &sibs, &placed, &branch);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].path, "e2e/test-map.json");
        assert!(m[0]
            .line()
            .contains("`Quote` isn't registered in `e2e/test-map.json`"));
        assert!(feedback(&m).contains("regenerate"));
    }

    fn ui_tree() -> Vec<String> {
        [
            "apps/web/src/components/ui/accordion.tsx",
            "apps/web/src/components/ui/alert.tsx",
            "apps/web/src/components/ui/card.tsx",
            "apps/web/package.json",
            "apps/web/src/components/ui/index.ts",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn dependency_names_in_a_manifest_are_not_a_registration() {
        let item = &new_items(
            &["apps/web/src/components/ui/heading.tsx".to_string()],
            &ui_tree(),
        )[0];
        let sibs = siblings(item, &ui_tree(), None);
        assert_eq!(sibs, vec!["accordion", "alert", "card"]);
        let pkg = r#"{"name": "web", "dependencies": {
            "@radix-ui/react-accordion": "1.2.0",
            "@radix-ui/react-alert-dialog": "1.1.0",
            "react-card-flip": "1.0.0"
        }}"#;
        let files = vec![("apps/web/package.json".to_string(), pkg.to_string())];
        assert!(registration_places(item, &sibs, &files).is_empty());
    }

    #[test]
    fn an_exports_map_and_a_barrel_are_registries() {
        let item = &new_items(
            &["apps/web/src/components/ui/heading.tsx".to_string()],
            &ui_tree(),
        )[0];
        let sibs = siblings(item, &ui_tree(), None);
        let pkg = r#"{"exports": {
            "./accordion": "./src/components/ui/accordion.tsx",
            "./alert": "./src/components/ui/alert.tsx",
            "./card": "./src/components/ui/card.tsx"
        }}"#;
        let barrel =
            "export * from './accordion'\nexport * from './alert'\nexport * from './card'\n";
        let files = vec![
            ("apps/web/package.json".to_string(), pkg.to_string()),
            (
                "apps/web/src/components/ui/index.ts".to_string(),
                barrel.to_string(),
            ),
        ];
        let places = registration_places(item, &sibs, &files);
        assert_eq!(places.len(), 2);
        assert!(places.iter().all(|(_, registry)| *registry));
    }

    #[test]
    fn comments_docs_and_migrations_never_make_a_place() {
        let item = &new_items(
            &["apps/web/src/components/ui/heading.tsx".to_string()],
            &ui_tree(),
        )[0];
        let sibs = siblings(item, &ui_tree(), None);
        let commented = "// see './accordion', './alert' and './card'\n/* './accordion' './alert' './card' */\nexport const x = 1\n";
        let files = vec![
            (
                "apps/web/src/lib/notes.ts".to_string(),
                commented.to_string(),
            ),
            (
                "docs/components.md".to_string(),
                "`./accordion` `./alert` `./card`".to_string(),
            ),
            (
                "apps/web/migrations/rename-cards/index.ts".to_string(),
                "const types = ['accordion', 'alert', 'card']".to_string(),
            ),
        ];
        assert!(registration_places(item, &sibs, &files).is_empty());
    }

    #[test]
    fn a_code_file_that_only_mentions_siblings_is_a_maybe_that_never_blocks() {
        let item = &new_items(&["src/blocks/Quote.tsx".to_string()], &tree())[0];
        let sibs = siblings(item, &tree(), None);
        let files = vec![(
            "src/pages/all.tsx".to_string(),
            "<Hero /> <Cards /> <Banner />".to_string(),
        )];
        let places = registration_places(item, &sibs, &files);
        assert_eq!(places, vec![("src/pages/all.tsx".to_string(), false)]);
        let placed = vec![(
            "src/pages/all.tsx".to_string(),
            "<Hero /> <Cards /> <Banner />".to_string(),
            false,
        )];
        let m = missing(item, &sibs, &placed, &|_| Some("<Hero />".to_string()));
        assert_eq!(m.len(), 1);
        assert!(blocking(&m).is_empty());
        assert!(m[0].line().contains("check by hand"));
        assert!(!feedback(&m).contains("src/pages/all.tsx"));
    }

    #[test]
    fn an_item_of_another_kind_than_its_siblings_is_skipped() {
        let sib = |n: &str| {
            format!("import {{ defineType }} from 'sanity'\nexport default defineType({{ name: '{n}' }})\n")
        };
        let sibs = vec![sib("author"), sib("blog"), sib("class")];
        let helper = "export function validatePromotion(v: string) { return v.length > 0 }\n";
        assert!(other_kind(helper, &sibs));
        let doc =
            "import { defineType } from 'sanity'\nexport default defineType({ name: 'promo' })\n";
        assert!(!other_kind(doc, &sibs));
        // Too little to judge: never skips.
        assert!(!other_kind(helper, &sibs[..1]));
        assert_eq!(
            imported_packages("import x from '@scope/pkg/sub'\nconst y = require(\"lodash/get\")\nimport z from './local'"),
            ["@scope/pkg", "lodash"].iter().map(|s| s.to_string()).collect()
        );
    }

    // A content studio's schema folder: three registered document schemas,
    // a documents index that imports each one's default export, and a
    // structure file that lists them by name.
    fn studio_tree() -> Vec<String> {
        [
            "studio/schemaTypes/documents/author.ts",
            "studio/schemaTypes/documents/blog.ts",
            "studio/schemaTypes/documents/blog-index.ts",
            "studio/schemaTypes/documents/index.ts",
            "studio/structure.ts",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn studio_doc(name: &str) -> String {
        format!(
            "import {{defineField, defineType}} from 'cms-kit'\n\nexport default defineType({{\n  \
             name: '{name}',\n  type: 'document',\n  fields: [defineField({{name: 'title', type: 'string'}})],\n}})\n"
        )
    }

    fn studio_places() -> Vec<(String, String)> {
        vec![
            (
                "studio/schemaTypes/documents/index.ts".to_string(),
                "import author from './author'\nimport blog from './blog'\nimport blogIndex from './blog-index'\n\nexport const documents = [author, blog, blogIndex]\n".to_string(),
            ),
            (
                "studio/structure.ts".to_string(),
                "export const structure = (S) =>\n  S.list().items([\n    S.documentTypeListItem('author'),\n    S.documentTypeListItem('blog'),\n    S.documentTypeListItem('blog-index'),\n  ])\n".to_string(),
            ),
        ]
    }

    /// The whole check after its reads, as `check` runs it.
    fn run_rule(new_path: &str, new_text: &str) -> Vec<Missing> {
        let tree = studio_tree();
        let item = &new_items(&[new_path.to_string()], &tree)[0];
        let sibs = siblings(item, &tree, None);
        assert_eq!(sibs, vec!["author", "blog", "blog-index"]);
        let sib_files: Vec<(String, String)> = sibs
            .iter()
            .map(|s| (sibling_file(item, s, &tree).unwrap(), studio_doc(s)))
            .collect();
        let files = studio_places();
        let keep = registration_places(item, &sibs, &files);
        let places: Vec<(String, String, bool)> = files
            .into_iter()
            .filter_map(|(p, t)| keep.iter().find(|(k, _)| *k == p).map(|(_, r)| (p, t, *r)))
            .collect();
        assert_eq!(places.len(), 2, "both registries are places");
        let registries: Vec<&str> = places
            .iter()
            .filter(|(_, _, r)| *r)
            .map(|(_, t, _)| t.as_str())
            .collect();
        let kind = kind_of(Some((new_path, new_text)), &sib_files, &registries, &sibs);
        // The branch registers nothing new.
        let unchanged: Vec<(String, String)> = studio_places();
        let lookup = |p: &str| {
            unchanged
                .iter()
                .find(|(q, _)| q == p)
                .map(|(_, t)| t.clone())
        };
        judge(item, &sibs, &kind, &places, &lookup)
    }

    #[test]
    fn a_validation_helper_beside_schema_documents_is_not_a_new_item() {
        // A standalone module of validation predicates next to the schema
        // documents: it imports the same package (types only), but exports
        // no schema. It was blocked as an unregistered document.
        let helper = "import type {ValidationContext} from 'cms-kit'\n\n\
                      export function endsAfterStart(value: unknown, context: ValidationContext) {\n  \
                      return true\n}\n\n\
                      export const requireCode = (rule: any) => rule.custom((v: string) => !!v)\n";
        let m = run_rule(
            "studio/schemaTypes/documents/promotion-validation.ts",
            helper,
        );
        assert!(m.is_empty(), "{m:?}");
        assert!(blocking(&m).is_empty());
    }

    #[test]
    fn a_real_new_schema_document_not_registered_is_still_flagged() {
        let m = run_rule(
            "studio/schemaTypes/documents/promotion.ts",
            &studio_doc("promotion"),
        );
        let b = blocking(&m);
        assert_eq!(b.len(), 2, "{m:?}");
        assert!(b
            .iter()
            .any(|x| x.path == "studio/schemaTypes/documents/index.ts"));
        assert!(b.iter().any(|x| x.path == "studio/structure.ts"));
        // Declared, then exported by name: the same kind.
        let later = "import {defineType} from 'cms-kit'\n\nconst promotion = defineType({name: 'promotion'})\n\nexport default promotion\n";
        assert_eq!(
            blocking(&run_rule(
                "studio/schemaTypes/documents/promotion.ts",
                later
            ))
            .len(),
            2
        );
    }

    #[test]
    fn when_the_structure_cant_tell_the_places_are_only_noted() {
        let tree = studio_tree();
        let item = &new_items(
            &["studio/schemaTypes/documents/promotion.ts".to_string()],
            &tree,
        )[0];
        let sibs = siblings(item, &tree, None);
        // The new file couldn't be read.
        let sib_files: Vec<(String, String)> = sibs
            .iter()
            .map(|s| (format!("{s}.ts"), studio_doc(s)))
            .collect();
        let kind = kind_of(None, &sib_files, &[], &sibs);
        assert!(matches!(kind, Kind::Unknown(_)));
        // Siblings with nothing in common to compare.
        let loose = vec![
            ("a.ts".to_string(), "export const a = 1\n".to_string()),
            ("b.ts".to_string(), "export function b() {}\n".to_string()),
        ];
        assert!(matches!(
            kind_of(Some(("n.ts", "export const n = 2\n")), &loose, &[], &sibs),
            Kind::Unknown(_)
        ));
        let places: Vec<(String, String, bool)> = studio_places()
            .into_iter()
            .map(|(p, t)| (p, t, true))
            .collect();
        let m = judge(item, &sibs, &kind, &places, &|p: &str| {
            studio_places()
                .into_iter()
                .find(|(q, _)| q == p)
                .map(|(_, t)| t)
        });
        assert_eq!(m.len(), 2);
        assert!(blocking(&m).is_empty());
        assert!(m[0].line().contains("check by hand"));
        assert!(m[0].line().contains("couldn't tell"));
    }

    #[test]
    fn shapes_are_read_by_structure_in_any_language() {
        let s = shape("x.ts", "export default defineType({name: 'a'})\n");
        assert!(s.default_export);
        assert!(s.definers.contains("defineType"));
        let s = shape("x.tsx", "export default class Hero extends Block {}\n");
        assert!(s.definers.contains("class:Block"));
        let s = shape("x.js", "module.exports = createRoute('/a')\n");
        assert!(s.default_export && s.definers.contains("createRoute"));
        let s = shape(
            "x.ts",
            "// export default defineType()\nexport function helper() {}\n",
        );
        assert_eq!(s, Shape::default());
        let s = shape("m.py", "@register\nclass Article(Model):\n    pass\n");
        assert!(s.definers.contains("class:Model"));
        assert!(s.definers.contains("@register"));
        let s = shape("q.rs", "pub struct Quote;\nimpl Block for Quote {}\n");
        assert!(s.definers.contains("impl:Block"));

        // Python models beside a helper module: the helper is another kind.
        let models = vec![
            (
                "app/models/user.py".to_string(),
                "from django.db import models\n\nclass User(models.Model):\n    pass\n".to_string(),
            ),
            (
                "app/models/order.py".to_string(),
                "from django.db import models\n\nclass Order(models.Model):\n    pass\n"
                    .to_string(),
            ),
        ];
        let helper = "from django.db import models\n\ndef money(v):\n    return round(v, 2)\n";
        assert!(matches!(
            kind_of(Some(("app/models/money.py", helper)), &models, &[], &[]),
            Kind::Other(_)
        ));
        let model = "from django.db import models\n\nclass Coupon(models.Model):\n    pass\n";
        assert_eq!(
            kind_of(Some(("app/models/coupon.py", model)), &models, &[], &[]),
            Kind::Same
        );
    }

    #[test]
    fn a_registry_that_imports_defaults_needs_a_default_export() {
        assert!(imports_default(
            "import blogIndex from './blog-index'",
            "blog-index"
        ));
        assert!(imports_default(
            "import Hero, { heroQuery } from \"@/blocks/Hero\"",
            "Hero"
        ));
        assert!(!imports_default("import { Hero } from './Hero'", "Hero"));
        assert!(!imports_default("import * as Hero from './Hero'", "Hero"));
        assert!(!imports_default("import x from './HeroBanner'", "Hero"));
        // Siblings share nothing but the registry imports their defaults.
        let sibs = vec!["a".to_string(), "b".to_string()];
        let files = vec![
            (
                "a.ts".to_string(),
                "const a = {}\nexport default a\n".to_string(),
            ),
            (
                "b.ts".to_string(),
                "export default function b() {}\n".to_string(),
            ),
        ];
        let registry = "import a from './a'\nimport b from './b'\n";
        assert!(matches!(
            kind_of(
                Some(("n.ts", "export const n = 1\n")),
                &files,
                &[registry],
                &sibs
            ),
            Kind::Other(_)
        ));
        assert_eq!(
            kind_of(
                Some(("n.ts", "export default {}\n")),
                &files,
                &[registry],
                &sibs
            ),
            Kind::Same
        );
    }

    #[test]
    fn types_and_generated_files_are_never_items() {
        let added = vec![
            "src/blocks/types.d.ts".to_string(),
            "src/blocks/schema.generated.ts".to_string(),
            "src/__generated__/blocks.ts".to_string(),
        ];
        assert!(new_items(&added, &tree()).is_empty());
    }

    #[test]
    fn path_names_match_whole_segments_only() {
        assert!(names_as_path(
            "import A from '@/components/ui/accordion'",
            "accordion"
        ));
        assert!(names_as_path("\"./Hero.tsx\"", "Hero"));
        assert!(names_as_path(
            "S.documentTypeListItem('blog-index')",
            "blog-index"
        ));
        assert!(!names_as_path("\"@radix-ui/react-accordion\"", "accordion"));
        assert!(!names_as_path("accordion without quotes", "accordion"));
    }
}
