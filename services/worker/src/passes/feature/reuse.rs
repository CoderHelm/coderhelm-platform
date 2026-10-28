//! Rules 3 and 4: reuse before writing, and mirror the closest sibling.
//!
//! A feature plan lists, in design.md:
//!
//! - under [`REUSE_HEADING`], the existing utilities, components, constants
//!   and data-fetch functions it will reuse, found by searching the code for
//!   similar names and behaviour (or says what it searched and found none);
//! - under [`SIBLING_HEADING`], when it adds a component, block, route,
//!   handler or any file of a kind that already exists, the closest existing
//!   one, by path, and what it copies from it: props, layout wrappers,
//!   overflow and scroll handling, accessibility labels.
//!
//! [`plan_gaps`] holds the plan to both in code. Before the pull request,
//! the self-review flags new code that duplicates an existing exported
//! helper: the new exports are read from the diff ([`new_exports`]),
//! existing exports with similar names are found by code search
//! ([`similar_existing`]), and a model judges each pair ([`judge`]).

use std::collections::BTreeSet;

use serde::Deserialize;
use tracing::warn;

use crate::agent::provider::{self, ModelProvider};
use crate::models::TokenUsage;
use crate::WorkerState;

pub const REUSE_HEADING: &str = "## Reuse";
pub const SIBLING_HEADING: &str = "## Closest sibling";

/// The text under a `## Heading` (to the next `## ` heading), if present.
pub fn section<'a>(text: &'a str, heading: &str) -> Option<&'a str> {
    let start = text
        .lines()
        .scan(0usize, |off, l| {
            let at = *off;
            *off += l.len() + 1;
            Some((at, l))
        })
        .find(|(_, l)| l.trim().eq_ignore_ascii_case(heading))
        .map(|(at, l)| at + l.len() + 1)?;
    let rest = text.get(start.min(text.len())..).unwrap_or("");
    let end = rest
        .lines()
        .scan(0usize, |off, l| {
            let at = *off;
            *off += l.len() + 1;
            Some((at, l))
        })
        .find(|(_, l)| l.starts_with("## "))
        .map(|(at, _)| at)
        .unwrap_or(rest.len());
    Some(&rest[..end])
}

/// Whether a section names a repository path (`a/b.ext` or `a/b/`).
fn names_path(s: &str) -> bool {
    s.split(|c: char| c.is_whitespace() || matches!(c, '`' | '(' | ')' | ',' | ';'))
        .any(|t| t.contains('/') && !t.contains("://") && t.len() > 2)
}

/// The sibling path the plan recorded, the first path in its section.
pub fn recorded_sibling(design: &str) -> Option<String> {
    let s = section(design, SIBLING_HEADING)?;
    s.split(|c: char| c.is_whitespace() || matches!(c, '`' | '(' | ')' | ',' | ';'))
        .find(|t| t.contains('/') && !t.contains("://") && t.len() > 2)
        .map(|t| t.trim_end_matches([':', '.']).to_string())
}

/// What a feature plan leaves out: the reuse list, and the sibling when it
/// creates files.
pub fn plan_gaps(design: &str, creates_files: bool) -> Vec<String> {
    let mut out = Vec::new();
    match section(design, REUSE_HEADING) {
        Some(s) if names_path(s) || s.to_lowercase().contains("searched") => {}
        _ => out.push(format!(
            "design.md has no `{REUSE_HEADING}` section listing the existing utilities, \
             components, constants and data-fetch functions this change reuses, each with its \
             path (search the code for similar names and behaviour first; if none fits, say what \
             you searched)."
        )),
    }
    if creates_files {
        match section(design, SIBLING_HEADING) {
            Some(s) if names_path(s) => {}
            _ => out.push(format!(
                "design.md has no `{SIBLING_HEADING}` section: this plan adds files, so name the \
                 closest existing file of the same kind by path, and follow its structure and \
                 usage exactly (props, layout wrappers, overflow and scroll handling, \
                 accessibility labels)."
            )),
        }
    }
    out
}

/// Names a line exports, by language.
fn exported_name(line: &str, path: &str) -> Option<String> {
    let l = line.trim_start();
    let ident = |s: &str| -> Option<String> {
        let n: String = s
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
            .collect();
        (n.len() >= 3).then_some(n)
    };
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        "ts" | "tsx" | "js" | "jsx" | "mjs" | "cjs" | "vue" | "svelte" => {
            let rest = l.strip_prefix("export ")?;
            let rest = rest.strip_prefix("default ").unwrap_or(rest);
            let rest = rest.strip_prefix("async ").unwrap_or(rest);
            for kw in ["function* ", "function ", "const ", "let ", "class "] {
                if let Some(r) = rest.strip_prefix(kw) {
                    return ident(r.trim_start());
                }
            }
            None
        }
        "rs" => {
            let rest = l.strip_prefix("pub ")?;
            let rest = rest.strip_prefix("async ").unwrap_or(rest);
            for kw in ["fn ", "const ", "struct ", "enum "] {
                if let Some(r) = rest.strip_prefix(kw) {
                    return ident(r.trim_start());
                }
            }
            None
        }
        "py" => {
            // Top level only, and not private.
            let r = line.strip_prefix("def ")?;
            ident(r).filter(|n| !n.starts_with('_'))
        }
        "go" => {
            let r = l.strip_prefix("func ")?;
            let n = ident(r)?;
            n.chars()
                .next()
                .is_some_and(|c| c.is_ascii_uppercase())
                .then_some(n)
        }
        _ => None,
    }
}

fn is_test_path(p: &str) -> bool {
    let l = p.to_lowercase();
    l.contains(".test.")
        || l.contains(".spec.")
        || l.contains("/__tests__/")
        || l.contains("/tests/")
        || l.contains("/e2e/")
        || l.starts_with("e2e/")
        || l.starts_with("tests/")
        || l.ends_with("_test.go")
        || l.contains("/test_")
}

/// The names the diff's added lines export, as (path, name), outside tests.
pub fn new_exports(files: &[(String, String, String)]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (path, status, patch) in files {
        if status == "removed" || is_test_path(path) {
            continue;
        }
        let removed: BTreeSet<String> = patch
            .lines()
            .filter(|l| l.starts_with('-') && !l.starts_with("---"))
            .filter_map(|l| exported_name(&l[1..], path))
            .collect();
        for l in patch.lines() {
            if !l.starts_with('+') || l.starts_with("+++") {
                continue;
            }
            if let Some(n) = exported_name(&l[1..], path) {
                // A moved or re-signed export isn't new.
                if !removed.contains(&n) && !out.iter().any(|(_, m)| m == &n) {
                    out.push((path.clone(), n));
                }
            }
        }
    }
    out
}

/// The words of an identifier: `formatEventDate` and `format_event_date`
/// are both `format event date`.
pub fn name_words(name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        let sep = !c.is_ascii_alphanumeric();
        let upper_start = c.is_ascii_uppercase()
            && i > 0
            && (chars[i - 1].is_ascii_lowercase()
                || chars[i - 1].is_ascii_digit()
                || (chars[i - 1].is_ascii_uppercase()
                    && chars.get(i + 1).is_some_and(|n| n.is_ascii_lowercase())));
        if (sep || upper_start) && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
        if !sep {
            cur.push(c.to_ascii_lowercase());
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out.retain(|w| w.len() >= 2);
    out
}

/// How alike two identifiers' words are, 0 to 1.
pub fn name_similarity(a: &str, b: &str) -> f64 {
    let a: BTreeSet<String> = name_words(a).into_iter().collect();
    let b: BTreeSet<String> = name_words(b).into_iter().collect();
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    a.intersection(&b).count() as f64 / a.union(&b).count() as f64
}

/// The search term for a new export: its longest word.
pub fn search_term(name: &str) -> Option<String> {
    name_words(name)
        .into_iter()
        .filter(|w| w.len() >= 4)
        .max_by_key(|w| w.len())
}

/// An existing exported helper close to a new one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub new_path: String,
    pub new_name: String,
    pub path: String,
    pub name: String,
    pub line: usize,
}

/// Existing exports close to `new_name` among search hits (path, fragments
/// as `line: text`), outside the files the change touches.
pub fn similar_existing(
    new_path: &str,
    new_name: &str,
    hits: &[(String, Vec<String>)],
    changed: &[String],
) -> Vec<Candidate> {
    let mut out = Vec::new();
    for (path, frags) in hits {
        if changed.contains(path) || is_test_path(path) {
            continue;
        }
        for f in frags {
            let Some((n, text)) = f.split_once(": ") else {
                continue;
            };
            let Some(name) = exported_name(text, path) else {
                continue;
            };
            if name_similarity(new_name, &name) >= 0.5 {
                out.push(Candidate {
                    new_path: new_path.to_string(),
                    new_name: new_name.to_string(),
                    path: path.clone(),
                    name,
                    line: n.trim().parse().unwrap_or(1),
                });
            }
        }
    }
    out.truncate(4);
    out
}

/// One new helper the judge found duplicating an existing one.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Duplicate {
    pub new: String,
    pub existing: String,
    pub path: String,
    #[serde(default)]
    pub why: String,
}

impl Duplicate {
    pub fn line(&self) -> String {
        format!(
            "`{}` duplicates the existing `{}` in `{}`{}",
            self.new,
            self.existing,
            self.path,
            if self.why.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", self.why.trim())
            }
        )
    }
}

#[derive(Deserialize)]
struct Judged {
    #[serde(default)]
    duplicates: Vec<Duplicate>,
}

const SYSTEM: &str = "You review a code change for new helpers that duplicate existing ones. \
For each pair you get the new code and an existing exported helper. A pair is a duplicate only \
when the existing helper already does what the new code does (same inputs, same result) and \
the new code could call it instead; similar names alone are not a duplicate. Answer with JSON \
only: {\"duplicates\":[{\"new\":\"<new name>\",\"existing\":\"<existing name>\",\"path\":\"<existing path>\",\"why\":\"<one sentence>\"}]}; \
an empty list when none is a duplicate.";

/// The judge's prompt: each pair with both bodies.
pub fn prompt(pairs: &[(Candidate, String, String)]) -> String {
    let mut out = String::from("Pairs to judge:\n");
    for (c, new_body, old_body) in pairs {
        out.push_str(&format!(
            "\n### New `{}` in `{}`\n```\n{}\n```\n### Existing `{}` in `{}`\n```\n{}\n```\n",
            c.new_name,
            c.new_path,
            common::truncate_str(new_body, 2_500),
            c.name,
            c.path,
            common::truncate_str(old_body, 2_500)
        ));
    }
    out
}

/// Reads the judge's reply; only pairs that were asked about count.
pub fn parse(reply: &str, asked: &[Candidate]) -> Vec<Duplicate> {
    let block = match reply.rfind("```json") {
        Some(s) => {
            let after = &reply[s + 7..];
            after.find("```").map(|e| &after[..e]).unwrap_or(after)
        }
        None => reply,
    };
    let Ok(j) = serde_json::from_str::<Judged>(block.trim()) else {
        return Vec::new();
    };
    j.duplicates
        .into_iter()
        .filter(|d| {
            asked
                .iter()
                .any(|c| c.new_name == d.new && c.name == d.existing && c.path == d.path)
        })
        .collect()
}

/// The model's verdict. Empty when it can't answer: a judge that can't
/// answer never flags anything.
pub async fn judge(
    state: &WorkerState,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    pairs: &[(Candidate, String, String)],
) -> Vec<Duplicate> {
    if pairs.is_empty() {
        return Vec::new();
    }
    let asked: Vec<Candidate> = pairs.iter().map(|(c, _, _)| c.clone()).collect();
    match provider::converse_simple(
        state,
        provider,
        provider.primary_model_id(),
        SYSTEM,
        &prompt(pairs),
        usage,
    )
    .await
    {
        Ok(r) => parse(&r, &asked),
        Err(e) => {
            warn!(error = %e, "Duplicate-helper check errored");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_feature_plan_lists_what_it_reuses_and_its_sibling() {
        let bare = "## Approach\nBuild it.";
        assert_eq!(plan_gaps(bare, true).len(), 2);
        assert_eq!(plan_gaps(bare, false).len(), 1);
        let full = "## Reuse\n- `src/lib/date.ts` formatDate\n\n## Closest sibling\n\
                    `src/blocks/Cards.tsx`: same props and the Section wrapper\n";
        assert!(plan_gaps(full, true).is_empty());
        assert_eq!(
            recorded_sibling(full).as_deref(),
            Some("src/blocks/Cards.tsx")
        );
        // Searched and found nothing is an answer too.
        let none = "## Reuse\nSearched for date and format helpers; none fits.\n";
        assert!(plan_gaps(none, false).is_empty());
    }

    #[test]
    fn reads_new_exports_from_added_lines_only() {
        let files = vec![
            (
                "src/lib/date.ts".to_string(),
                "modified".to_string(),
                "@@\n+export function formatEventDate(d: Date) {\n-export const oldThing = 1\n+export const oldThing = 2\n+const local = 1\n".to_string(),
            ),
            (
                "src/lib/date.test.ts".to_string(),
                "added".to_string(),
                "+export function helperForTests() {}\n".to_string(),
            ),
            (
                "src/api/mod.rs".to_string(),
                "modified".to_string(),
                "+pub async fn fetch_events() {}\n+fn private() {}\n".to_string(),
            ),
        ];
        assert_eq!(
            new_exports(&files),
            vec![
                ("src/lib/date.ts".to_string(), "formatEventDate".to_string()),
                ("src/api/mod.rs".to_string(), "fetch_events".to_string()),
            ]
        );
    }

    #[test]
    fn similar_names_across_styles_are_candidates() {
        assert_eq!(
            name_words("formatEventDate"),
            vec!["format", "event", "date"]
        );
        assert_eq!(
            name_words("format_event_date"),
            vec!["format", "event", "date"]
        );
        assert_eq!(name_words("parseHTMLDate"), vec!["parse", "html", "date"]);
        assert!(name_similarity("formatEventDate", "formatDate") >= 0.5);
        assert_eq!(search_term("formatEventDate").as_deref(), Some("format"));
        let hits = vec![
            (
                "src/utils/dates.ts".to_string(),
                vec!["12: export function formatDate(d: Date): string {".to_string()],
            ),
            (
                "src/lib/date.ts".to_string(),
                vec!["3: export function formatEventDate(d: Date) {".to_string()],
            ),
            (
                "src/utils/money.ts".to_string(),
                vec!["4: export function formatMoney(n: number) {".to_string()],
            ),
        ];
        let c = similar_existing(
            "src/lib/date.ts",
            "formatEventDate",
            &hits,
            &["src/lib/date.ts".to_string()],
        );
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].name, "formatDate");
        assert_eq!(c[0].line, 12);
    }

    #[test]
    fn the_judge_only_flags_pairs_it_was_asked_about() {
        let asked = vec![Candidate {
            new_path: "src/lib/date.ts".into(),
            new_name: "formatEventDate".into(),
            path: "src/utils/dates.ts".into(),
            name: "formatDate".into(),
            line: 12,
        }];
        let reply = "```json\n{\"duplicates\":[{\"new\":\"formatEventDate\",\"existing\":\"formatDate\",\"path\":\"src/utils/dates.ts\",\"why\":\"same output\"},{\"new\":\"x\",\"existing\":\"y\",\"path\":\"z\"}]}\n```";
        let d = parse(reply, &asked);
        assert_eq!(d.len(), 1);
        assert!(d[0].line().contains("duplicates the existing `formatDate`"));
        assert!(parse("not json", &asked).is_empty());
    }
}
