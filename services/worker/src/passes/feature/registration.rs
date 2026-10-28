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
//!    is not a registration), found by code search on the default branch;
//! 3. every such place must name the new item on the run's branch.
//!
//! What is missing goes back to the coder once with the list; what is still
//! missing blocks the pull request with the list.

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

/// The items the added files make. `added` are the paths the change adds;
/// `tree` is the default branch's file list.
pub fn new_items(added: &[String], tree: &[String]) -> Vec<NewItem> {
    let mut out: Vec<NewItem> = Vec::new();
    for p in added {
        if skipped(p) || is_test(p) {
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
        if skipped(t) || is_test(t) {
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
/// three and 60% of them). `files` are (path, default-branch text).
pub fn registration_places(
    item: &NewItem,
    sibs: &[String],
    files: &[(String, String)],
) -> Vec<String> {
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
        .filter(|(p, _)| !skipped(p) && !sibling_own(p, item, sibs) && *p != item.path)
        .filter(|(_, text)| sibs.iter().filter(|s| names_item(text, s)).count() >= need)
        .map(|(p, _)| p.clone())
        .collect()
}

/// A place a new item is missing from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Missing {
    pub item: String,
    pub path: String,
    /// The siblings that place names, for the message.
    pub siblings: Vec<String>,
}

impl Missing {
    pub fn line(&self) -> String {
        format!(
            "`{}` isn't registered in `{}` (it names {})",
            self.item,
            self.path,
            self.siblings
                .iter()
                .take(3)
                .map(|s| format!("`{s}`"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// The places that don't name the item on the branch. `on_branch`: path to
/// the place's text on the run's branch (`None`: removed on the branch).
pub fn missing(
    item: &NewItem,
    sibs: &[String],
    places: &[(String, String)],
    on_branch: &dyn Fn(&str) -> Option<String>,
) -> Vec<Missing> {
    let mut out = Vec::new();
    for (path, base_text) in places {
        let Some(text) = on_branch(path) else {
            continue;
        };
        if !names_item(&text, &item.id) {
            out.push(Missing {
                item: item.id.clone(),
                path: path.clone(),
                siblings: sibs
                    .iter()
                    .filter(|s| names_item(base_text, s))
                    .cloned()
                    .collect(),
            });
        }
    }
    out
}

/// What the coder is told.
pub fn feedback(missing: &[Missing]) -> String {
    let mut s = String::from(
        "\nThe change adds items of a kind that already exists, but doesn't register them \
         everywhere their siblings are registered. Add each one to these places, following how \
         the siblings appear there, then regenerate any generated files that list them:\n",
    );
    for m in missing {
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
        let places: Vec<(String, String)> = {
            let keep = registration_places(&item, &sibs, &files);
            files
                .into_iter()
                .filter(|(p, _)| keep.contains(p))
                .collect()
        };
        checked += places.len();
        let mut on_branch: Vec<(String, Option<String>)> = Vec::new();
        for (p, _) in &places {
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
        for m in missing(&item, &sibs, &places, &lookup) {
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
        assert_eq!(places, vec!["src/blocks/index.ts", "e2e/test-map.json"]);

        let placed: Vec<(String, String)> = files
            .into_iter()
            .filter(|(p, _)| places.contains(p))
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
}
