//! The Risk a pull request description states follows from its diff.
//!
//! A description called a change "Low" risk while it edited a query fragment
//! every page imports and added a network call to each page render. The
//! writer judged it from the plan, and nothing checked the level against
//! the change.
//!
//! The rules here:
//!
//! - [`floor`] reads the diff: a file every page or route goes through (a
//!   shared query or fragment, a layout, middleware, global config, or a
//!   file the code graph says many files import) and a network call added
//!   where pages render each raise the lowest level the description may
//!   state. Both together, or a change the reviewer's rubric forces HIGH
//!   (a migration, auth, payments), make it High. Each reason names the
//!   file.
//! - [`enforce`] reads the level the description states and, when it is
//!   lower than the floor (or no Risk section says one), rewrites the Risk
//!   section's level and puts the reasons first. It never lowers a level
//!   the writer chose.

use std::collections::HashMap;

/// A risk level, lowest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Low,
    Medium,
    High,
}

impl Level {
    pub fn word(self) -> &'static str {
        match self {
            Level::Low => "Low",
            Level::Medium => "Medium",
            Level::High => "High",
        }
    }

    fn parse(word: &str) -> Option<Level> {
        match word.to_ascii_lowercase().as_str() {
            "low" => Some(Level::Low),
            "medium" | "moderate" => Some(Level::Medium),
            "high" | "critical" => Some(Level::High),
            _ => None,
        }
    }
}

/// The lowest level the diff allows, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Floor {
    pub level: Level,
    pub reasons: Vec<String>,
}

/// Importers from the code graph at which a file counts as shared.
pub const SHARED_IMPORTERS: usize = 8;

/// File names (without extension) every page or request goes through.
const SHARED_STEMS: &[&str] = &[
    "layout",
    "_app",
    "_document",
    "root",
    "middleware",
    "global",
    "globals",
    "app.config",
    "next.config",
    "nuxt.config",
    "vite.config",
    "astro.config",
    "svelte.config",
    "remix.config",
];

/// Folder names whose files many pages import.
const SHARED_DIRS: &[&str] = &["shared", "common", "fragments", "fragment"];

fn stem(path: &str) -> String {
    let base = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    match base.rsplit_once('.') {
        Some((s, ext)) if !s.is_empty() && ext.len() <= 5 => s.to_string(),
        _ => base,
    }
}

fn is_test(path: &str) -> bool {
    let l = path.to_ascii_lowercase();
    l.contains(".test.")
        || l.contains(".spec.")
        || l.contains("__tests__")
        || l.contains("/test/")
        || l.contains("/tests/")
        || l.contains("/e2e/")
        || l.starts_with("e2e/")
        || l.starts_with("test/")
        || l.starts_with("tests/")
}

fn is_code(path: &str) -> bool {
    let l = path.to_ascii_lowercase();
    [
        ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".vue", ".svelte", ".astro", ".py", ".rb",
        ".go", ".rs", ".php", ".groq", ".graphql", ".gql",
    ]
    .iter()
    .any(|e| l.ends_with(e))
}

/// Why a file is one every page or request goes through, if it is.
pub fn shared_reason(path: &str, importers: usize) -> Option<String> {
    if is_test(path) {
        return None;
    }
    if importers >= SHARED_IMPORTERS {
        return Some(format!("`{path}` is imported by {importers} files"));
    }
    let s = stem(path);
    if SHARED_STEMS.contains(&s.as_str()) {
        return Some(format!("`{path}` wraps every page or request"));
    }
    if !is_code(path) {
        return None;
    }
    let lower = path.to_ascii_lowercase();
    let dirs: Vec<&str> = lower.split('/').collect();
    let in_shared_dir = dirs[..dirs.len().saturating_sub(1)]
        .iter()
        .any(|d| SHARED_DIRS.contains(d));
    let shared_query = (s.contains("fragment") || s.contains("query") || s.contains("queries"))
        && (s.contains("shared")
            || s.contains("common")
            || s.contains("base")
            || s.contains("global")
            || s.contains("page")
            || s.contains("fragment"));
    if in_shared_dir || shared_query {
        return Some(format!("`{path}` is a shared module pages import"));
    }
    None
}

/// Whether a file renders pages or handles requests (where a call added runs
/// once per page view or request).
fn renders(path: &str) -> bool {
    let l = path.to_ascii_lowercase();
    let s = stem(path);
    l.ends_with(".tsx")
        || l.ends_with(".jsx")
        || l.ends_with(".vue")
        || l.ends_with(".svelte")
        || l.ends_with(".astro")
        || l.contains("/pages/")
        || l.starts_with("pages/")
        || l.contains("/app/")
        || l.starts_with("app/")
        || l.contains("/routes/")
        || l.starts_with("routes/")
        || matches!(
            s.as_str(),
            "page" | "layout" | "route" | "loader" | "server"
        )
}

/// Calls that go over the network.
const NETWORK_CALLS: &[&str] = &[
    "fetch(",
    "axios(",
    "axios.",
    "got(",
    "ky(",
    "ky.",
    "superagent",
    "http.get(",
    "https.get(",
    "http.request(",
    "requests.get(",
    "requests.post(",
    "httpx.",
    "urlopen(",
    "graphqlrequest(",
    "apollo",
    "usequery(",
];

/// The network call a patch's added lines make, if any.
pub fn added_network_call(patch: &str) -> Option<String> {
    patch
        .lines()
        .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
        .map(|l| l[1..].trim())
        .filter(|l| !l.starts_with("//") && !l.starts_with('*') && !l.starts_with('#'))
        .find(|l| {
            let low = l.to_ascii_lowercase();
            NETWORK_CALLS.iter().any(|c| low.contains(c))
        })
        .map(|l| common::truncate_str(l, 80).to_string())
}

/// The lowest level the diff (path, status, patch) allows. `importers`: how
/// many files import each changed file, from the code graph (missing when
/// the graph can't say).
pub fn floor(files: &[(String, String, String)], importers: &HashMap<String, usize>) -> Floor {
    let mut shared: Vec<String> = Vec::new();
    let mut calls: Vec<String> = Vec::new();
    for (path, status, patch) in files {
        if status == "removed" || is_test(path) {
            continue;
        }
        let shared_why = shared_reason(path, importers.get(path).copied().unwrap_or(0));
        if let Some(why) = &shared_why {
            shared.push(why.clone());
        }
        if shared_why.is_some() || renders(path) {
            if let Some(call) = added_network_call(patch) {
                calls.push(format!(
                    "`{path}` adds a network call where pages render (`{call}`), once per page \
                     view or request"
                ));
            }
        }
    }
    let (_, _, hard) = super::review_risk::classify_sensitive(files);
    let mut reasons: Vec<String> = Vec::new();
    if let Some(h) = &hard {
        reasons.push(h.clone());
    }
    reasons.extend(shared.iter().take(3).cloned());
    reasons.extend(calls.iter().take(3).cloned());
    let level = if hard.is_some() || (!shared.is_empty() && !calls.is_empty()) {
        Level::High
    } else if !shared.is_empty() || !calls.is_empty() {
        Level::Medium
    } else {
        Level::Low
    };
    Floor { level, reasons }
}

/// A compare response's files as (path, status, patch).
pub fn diff_files(diff: &serde_json::Value) -> Vec<(String, String, String)> {
    diff["files"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|f| {
            (
                f["filename"].as_str().unwrap_or("").to_string(),
                f["status"].as_str().unwrap_or("").to_string(),
                f["patch"].as_str().unwrap_or("").to_string(),
            )
        })
        .filter(|(p, _, _)| !p.is_empty())
        .collect()
}

/// How many files import each changed file, from the repository's code
/// graph when it is built (empty otherwise). At most 20 files are looked up.
pub async fn importers(
    state: &crate::WorkerState,
    team_id: &str,
    installation_id: u64,
    repo: &str,
    files: &[(String, String, String)],
) -> HashMap<String, usize> {
    let mut out = HashMap::new();
    let super::code_graph::Avail::Ready(graph) =
        super::code_graph::availability(state, team_id, repo, installation_id).await
    else {
        return out;
    };
    for (path, status, _) in files.iter().filter(|(_, s, _)| s != "added").take(20) {
        if status == "removed" {
            continue;
        }
        let n = graph.importers(state, path).await.len();
        if n > 0 {
            out.insert(path.clone(), n);
        }
    }
    out
}

/// What the writer is told about the floor. Empty when the diff allows Low.
pub fn writer_note(f: &Floor) -> String {
    if f.level == Level::Low {
        return String::new();
    }
    format!(
        "\n\n## Risk (computed from the diff)\nThe Risk level is at least **{}**, because: {}. \
         State that level (or higher) and these reasons; never call this change Low.",
        f.level.word(),
        f.reasons.join("; ")
    )
}

fn is_risk_heading(line: &str) -> bool {
    let t = line.trim();
    let marked =
        t.starts_with('#') || t.starts_with("**") || t.starts_with(|c: char| c.is_ascii_digit());
    if !marked {
        return false;
    }
    let t = t.trim_start_matches('#').trim_start();
    let t = t.trim_start_matches(|c: char| c.is_ascii_digit() || c == '.' || c == ')');
    let t = t.trim_start().trim_start_matches("**").trim_start();
    let low = t.to_ascii_lowercase();
    low.starts_with("risk") && !low[4..].chars().next().is_some_and(|c| c.is_alphanumeric())
}

fn is_heading(line: &str) -> bool {
    let t = line.trim();
    if t.starts_with('#') || t.starts_with("<!--") {
        return true;
    }
    // `**Verification**` or `**Verification:** …`, but not a bold level
    // (`**Low** — …`), which is the Risk section's own content.
    t.strip_prefix("**")
        .and_then(|r| r.find("**").map(|i| &r[..i]))
        .is_some_and(|bold| {
            let name = bold.trim().trim_end_matches(':');
            !name.is_empty() && name.len() <= 40 && Level::parse(name).is_none()
        })
}

/// The Risk section's lines: (heading index, end index exclusive).
fn risk_span(lines: &[&str]) -> Option<(usize, usize)> {
    let start = lines.iter().position(|l| is_risk_heading(l))?;
    let end = lines[start + 1..]
        .iter()
        .position(|l| is_heading(l) || is_numbered_heading(l))
        .map(|i| start + 1 + i)
        .unwrap_or(lines.len());
    Some((start, end))
}

fn is_numbered_heading(line: &str) -> bool {
    let t = line.trim_start();
    t.split_once(". **")
        .is_some_and(|(n, _)| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
}

/// The first level word in `text` (bold or not).
fn level_in(text: &str) -> Option<Level> {
    text.split(|c: char| !c.is_ascii_alphabetic())
        .filter(|w| !w.is_empty())
        .find_map(Level::parse)
}

/// The level the description's Risk section states.
pub fn stated(body: &str) -> Option<Level> {
    let lines: Vec<&str> = body.lines().collect();
    let (start, end) = risk_span(&lines)?;
    let heading_rest = {
        let h = lines[start].to_ascii_lowercase();
        h.find("risk")
            .map(|i| h[i + 4..].to_string())
            .unwrap_or_default()
    };
    level_in(&heading_rest).or_else(|| level_in(&lines[start + 1..end].join("\n")))
}

/// The description with its Risk at least the floor, the reasons stated.
/// Unchanged when the floor is Low, or the stated level is at least the
/// floor and already names each reason's file.
pub fn enforce(body: &str, f: &Floor) -> String {
    if f.level == Level::Low {
        return body.to_string();
    }
    let reasons_line = format!(
        "**{}**: {}.",
        f.level.word(),
        f.reasons.join("; ").trim_end_matches('.')
    );
    let lines: Vec<&str> = body.lines().collect();
    let Some((start, end)) = risk_span(&lines) else {
        // No Risk section: one goes before Verification, else at the end.
        let section = format!("**Risk**\n{reasons_line}");
        let at = lines.iter().position(|l| {
            let low = l.to_ascii_lowercase();
            (l.trim_start().starts_with('#') || l.trim_start().starts_with("**"))
                && low.contains("verification")
        });
        return match at {
            Some(i) => format!(
                "{}\n\n{section}\n\n{}",
                lines[..i].join("\n").trim_end(),
                lines[i..].join("\n")
            ),
            None => format!("{}\n\n{section}", body.trim_end()),
        };
    };
    let current = stated(body);
    let names_every_file = f.reasons.iter().all(|r| {
        r.split('`')
            .nth(1)
            .is_none_or(|file| lines[start..end].iter().any(|l| l.contains(file)))
    });
    if current.is_some_and(|c| c >= f.level) && names_every_file {
        return body.to_string();
    }
    // The heading stays (without a level on its line); the reasons go first,
    // then what the writer said that isn't a level claim.
    let heading = {
        let h = lines[start];
        match current {
            Some(c) if c < f.level => replace_level_words(h, f.level),
            _ => h.to_string(),
        }
    };
    let rest: Vec<String> = lines[start + 1..end]
        .iter()
        .map(|l| match current {
            Some(c) if c < f.level => replace_level_words(l, f.level),
            _ => l.to_string(),
        })
        .filter(|l| !l.trim().is_empty())
        .collect();
    let heading_has_level = level_in(
        &heading
            .to_ascii_lowercase()
            .split_once("risk")
            .map(|x| x.1.to_string())
            .unwrap_or_default(),
    )
    .is_some();
    let mut out: Vec<String> = lines[..start].iter().map(|l| l.to_string()).collect();
    if heading_has_level {
        out.push(heading);
        out.push(format!(
            "Why {}: {}.",
            f.level.word(),
            f.reasons.join("; ").trim_end_matches('.')
        ));
    } else {
        out.push(heading);
        out.push(reasons_line);
    }
    out.extend(rest);
    out.push(String::new());
    out.extend(lines[end..].iter().map(|l| l.to_string()));
    let mut s = out.join("\n");
    while s.contains("\n\n\n") {
        s = s.replace("\n\n\n", "\n\n");
    }
    let s = s.trim_end().to_string();
    if body.ends_with('\n') {
        s + "\n"
    } else {
        s
    }
}

/// `line` with each level word lower than `to` replaced by `to`.
fn replace_level_words(line: &str, to: Level) -> String {
    let mut out = String::new();
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String| {
        match Level::parse(word) {
            Some(l) if l < to => {
                let w = to.word();
                if word.chars().all(|c| c.is_ascii_uppercase()) {
                    out.push_str(&w.to_ascii_uppercase());
                } else {
                    out.push_str(w);
                }
            }
            _ => out.push_str(word),
        }
        word.clear();
    };
    for c in line.chars() {
        if c.is_ascii_alphabetic() {
            word.push(c);
        } else {
            flush(&mut word, &mut out);
            out.push(c);
        }
    }
    flush(&mut word, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(path: &str, patch: &str) -> (String, String, String) {
        (path.into(), "modified".into(), patch.into())
    }

    #[test]
    fn a_shared_query_with_a_new_call_per_render_is_high() {
        let files = vec![
            f(
                "src/lib/queries/pageFragments.ts",
                "@@\n+  gallery { title, images[] }\n",
            ),
            f(
                "src/app/[...slug]/page.tsx",
                "@@\n+  const extra = await fetch(`${API}/gallery/${id}`)\n",
            ),
            f(
                "src/components/Gallery.tsx",
                "@@\n+export function Gallery() {}\n",
            ),
        ];
        let fl = floor(&files, &HashMap::new());
        assert_eq!(fl.level, Level::High, "{fl:?}");
        assert!(fl.reasons.iter().any(|r| r.contains("pageFragments.ts")));
        assert!(fl.reasons.iter().any(|r| r.contains("network call")));
    }

    #[test]
    fn a_file_many_import_is_shared_and_a_leaf_change_is_low() {
        let files = vec![f("src/lib/format.ts", "@@\n+export const x = 1\n")];
        let mut imp = HashMap::new();
        imp.insert("src/lib/format.ts".to_string(), 14);
        let fl = floor(&files, &imp);
        assert_eq!(fl.level, Level::Medium);
        assert!(fl.reasons[0].contains("imported by 14 files"));
        assert_eq!(floor(&files, &HashMap::new()).level, Level::Low);
        // Layout and middleware wrap every page.
        assert_eq!(
            floor(&[f("app/layout.tsx", "@@\n+<Banner/>\n")], &HashMap::new()).level,
            Level::Medium
        );
        // Tests never raise it.
        assert_eq!(
            floor(
                &[f("e2e/shared/gallery.spec.ts", "@@\n+await fetch(x)\n")],
                &HashMap::new()
            )
            .level,
            Level::Low
        );
    }

    #[test]
    fn a_stated_low_is_raised_with_the_reasons() {
        let body = "**Summary**\nAdds a gallery block.\n\n**Risk**\n**Low** — schema only, not registered or rendered.\n\n**Verification**\n1. Run `npm test`\n";
        let fl = Floor {
            level: Level::High,
            reasons: vec![
                "`src/lib/queries/pageFragments.ts` is a shared module pages import".into(),
            ],
        };
        assert_eq!(stated(body), Some(Level::Low));
        let out = enforce(body, &fl);
        assert_eq!(stated(&out), Some(Level::High), "{out}");
        assert!(out.contains("pageFragments.ts"));
        assert!(!out.contains("**Low**"), "{out}");
        assert!(out.contains("**Verification**\n1. Run `npm test`"));
        // Already right: left alone. Never lowered.
        assert_eq!(enforce(&out, &fl), out);
        let medium = Floor {
            level: Level::Medium,
            ..fl.clone()
        };
        assert_eq!(stated(&enforce(&out, &medium)), Some(Level::High));
    }

    #[test]
    fn a_numbered_heading_with_the_level_on_its_line_is_read() {
        let body = "1. **Summary** — x\n2. **Changes** — y\n3. **Risk** — **Low**, small.\n4. **Verification** — z\n";
        assert_eq!(stated(body), Some(Level::Low));
        let fl = Floor {
            level: Level::Medium,
            reasons: vec!["`app/layout.tsx` wraps every page or request".into()],
        };
        let out = enforce(body, &fl);
        assert_eq!(stated(&out), Some(Level::Medium), "{out}");
        assert!(out.contains("Why Medium: `app/layout.tsx`"));
        assert!(out.contains("4. **Verification** — z"));
    }

    #[test]
    fn inline_bold_headings_end_the_risk_section() {
        let body = "**Summary:** x\n**Risk:** Low, a small change.\n**Verification:** run the low-traffic check\n";
        let fl = Floor {
            level: Level::Medium,
            reasons: vec!["`middleware.ts` wraps every page or request".into()],
        };
        let out = enforce(body, &fl);
        assert_eq!(stated(&out), Some(Level::Medium), "{out}");
        assert!(
            out.contains("**Verification:** run the low-traffic check"),
            "{out}"
        );
    }

    #[test]
    fn a_description_without_risk_gets_one_before_verification() {
        let body = "## Summary\nx\n\n## Verification\n- y\n";
        let fl = Floor {
            level: Level::Medium,
            reasons: vec!["`middleware.ts` wraps every page or request".into()],
        };
        let out = enforce(body, &fl);
        assert!(out.find("**Risk**").unwrap() < out.find("## Verification").unwrap());
        assert_eq!(stated(&out), Some(Level::Medium));
        assert_eq!(
            enforce(
                body,
                &Floor {
                    level: Level::Low,
                    reasons: vec![]
                }
            ),
            body
        );
        assert!(writer_note(&fl).contains("at least **Medium**"));
    }
}
