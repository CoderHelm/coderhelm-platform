//! Rule 7: the verification checklist a feature meets before it is ready.
//!
//! Derived from the repository and the ticket, never from the coder's own
//! account:
//!
//! - type-check, lint and unit tests, with the repository's own scripts;
//! - the unit tests the acceptance criteria imply: one per listed option or
//!   case ([`acceptance_cases`]), counted in the diff ([`unit_cases_added`]);
//! - every value of each new configurable thing the change adds (a schema
//!   field's option list, a string-literal union prop, an on/off field's
//!   shown and hidden cases) named in the tests ([`new_options`],
//!   [`untested_options`]);
//! - the end-to-end spec, and its entry in any test map the repository keeps
//!   ([`missing_from_test_maps`]);
//! - the checks of each test package outside the main workspace;
//! - the new spec run repeatedly (`--repeat-each=5`) against the preview;
//! - for UI work, a no-horizontal-scroll check at a narrow and a wide
//!   viewport in the spec ([`responsive_checked`]);
//! - a new or changed end-to-end test never skips itself when the content
//!   or data it needs is missing: it fails, naming what is missing
//!   ([`data_skips`]). Only a skip for a stated browser or platform
//!   condition is allowed; a test that skips on missing content can never
//!   fail.
//!
//! The counts and results go in the pull request; an item that fails keeps it
//! a draft.

use common::e2e::test_package::Step;
use serde::{Deserialize, Serialize};

/// Repeats of the new spec.
pub const REPEATS: u32 = 5;
/// The narrowest and widest viewports the responsive check needs.
pub const NARROW_MAX: u32 = 480;
pub const WIDE_MIN: u32 = 1280;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    Pass,
    Fail,
    /// Not run here; the detail says where it runs.
    NotRun,
}

impl Status {
    fn word(self) -> &'static str {
        match self {
            Status::Pass => "PASS",
            Status::Fail => "FAIL",
            Status::NotRun => "not run here",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    pub name: String,
    pub status: Status,
    pub detail: String,
}

impl Item {
    pub fn new(name: &str, status: Status, detail: impl Into<String>) -> Self {
        Item {
            name: name.to_string(),
            status,
            detail: detail.into(),
        }
    }

    pub fn line(&self) -> String {
        format!("- [{}] {}: {}", self.status.word(), self.name, self.detail)
    }
}

/// The checklist as the description shows it.
pub fn markdown(items: &[Item]) -> String {
    if items.is_empty() {
        return String::new();
    }
    let mut s = String::from("## Verification checklist\n");
    for i in items {
        s.push_str(&i.line());
        s.push('\n');
    }
    s
}

/// The failing items, one line each.
pub fn failing(items: &[Item]) -> Vec<String> {
    items
        .iter()
        .filter(|i| i.status == Status::Fail)
        .map(|i| format!("{}: {}", i.name, i.detail))
        .collect()
}

fn list_item(l: &str) -> bool {
    let t = l.trim_start();
    t.starts_with("- ")
        || t.starts_with("* ")
        || t.split_once(". ")
            .is_some_and(|(n, _)| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
}

/// How many cases the acceptance criteria list: the list items under an
/// "acceptance criteria" heading in the ticket, else the scenarios in the
/// plan's spec.md.
pub fn acceptance_cases(ticket_body: &str, spec_md: &str) -> usize {
    let mut in_ac = false;
    let mut n = 0;
    for l in ticket_body.lines() {
        let low = l.trim().to_lowercase();
        if low.contains("acceptance criteria") {
            in_ac = true;
            continue;
        }
        if in_ac {
            if l.trim_start().starts_with('#') || (low.ends_with(':') && !list_item(l)) {
                break;
            }
            if list_item(l) {
                n += 1;
            }
        }
    }
    if n > 0 {
        return n;
    }
    spec_md
        .lines()
        .filter(|l| {
            let t = l
                .trim_start()
                .trim_start_matches(['#', '*', '-', ' '])
                .to_lowercase();
            t.starts_with("scenario")
        })
        .count()
}

fn is_e2e(path: &str) -> bool {
    super::content::is_e2e_spec(path)
}

/// Test cases the diff adds outside end-to-end specs.
pub fn unit_cases_added(files: &[(String, String, String)]) -> usize {
    files
        .iter()
        .filter(|(p, s, _)| s != "removed" && !is_e2e(p))
        .map(|(_, _, patch)| {
            patch
                .lines()
                .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
                .map(|l| l[1..].trim_start())
                .filter(|l| {
                    l.starts_with("it(")
                        || l.starts_with("test(")
                        || l.starts_with("it.each")
                        || l.starts_with("test.each")
                        || l.starts_with("#[test]")
                        || l.starts_with("#[tokio::test]")
                        || l.starts_with("def test_")
                        || l.starts_with("func Test")
                })
                .count()
        })
        .sum()
}

/// A new configurable thing the change adds and the values tests must set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOption {
    pub name: String,
    pub values: Vec<String>,
    /// An on/off field: its values are `true` and `false`.
    pub toggle: bool,
}

/// Whether a path is a test file.
pub fn is_test_path(path: &str) -> bool {
    let l = path.to_lowercase();
    l.contains(".test.")
        || l.contains(".spec.")
        || l.contains("/test/")
        || l.contains("/tests/")
        || l.contains("__tests__")
        || l.starts_with("test/")
        || l.starts_with("tests/")
        || l.rsplit('/').next().is_some_and(|n| n.starts_with("test_"))
        || is_e2e(path)
}

/// Source files whose new options count: code a person writes, never
/// generated output.
fn option_source(path: &str) -> bool {
    let l = path.to_lowercase();
    let code = [
        ".ts", ".tsx", ".js", ".jsx", ".mjs", ".vue", ".svelte", ".py", ".rb", ".go", ".rs", ".kt",
        ".java", ".cs",
    ]
    .iter()
    .any(|e| l.ends_with(e));
    code && !is_test_path(path)
        && !l.ends_with(".d.ts")
        && ![
            "generated",
            ".gen.",
            "sanity.types",
            "graphql.ts",
            "schema.ts.snap",
        ]
        .iter()
        .any(|m| l.contains(m))
}

/// The string literals in `text`, in order.
fn literals(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == '\'' || c == '"' || c == '`' {
            let rest = &text[i + 1..];
            if let Some(end) = rest.find(c) {
                let lit = &rest[..end];
                if !lit.is_empty() && !lit.contains('\n') {
                    out.push(lit.to_string());
                }
                // Skip past the closing quote.
                for _ in 0..lit.chars().count() + 1 {
                    chars.next();
                }
            }
        }
    }
    out
}

/// The quoted value after `key:` anywhere on a line (`name: 'tabs'` and
/// `defineField({ name: 'tabs', ...` both give `tabs`).
fn keyed(line: &str, key: &str) -> Option<String> {
    let mut from = 0;
    while let Some(i) = line[from..].find(key) {
        let at = from + i;
        from = at + key.len();
        let before_ok = line[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        let Some(rest) = line[from..].trim_start().strip_prefix(':') else {
            continue;
        };
        let rest = rest.trim_start();
        if before_ok && rest.starts_with(['\'', '"', '`']) {
            return literals(rest).into_iter().next();
        }
    }
    None
}

fn option_list_start(line: &str) -> bool {
    let t = line.trim_start();
    ["list", "options", "enum", "choices", "values"]
        .iter()
        .any(|k| {
            t.strip_prefix(k)
                .and_then(|r| r.trim_start().strip_prefix(':'))
                .is_some_and(|r| r.trim_start().starts_with('['))
        })
}

/// A string-literal union on one line (`variant?: 'a' | 'b'`,
/// `type Size = "sm" | "lg"`): its name and values.
fn union(line: &str) -> Option<(String, Vec<String>)> {
    let t = line.trim().trim_end_matches([';', ',']);
    let (head, body) = if let Some((h, b)) = t.split_once('=') {
        (h, b)
    } else {
        t.split_once(':')?
    };
    let body = body.trim();
    let parts: Vec<&str> = body
        .split('|')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();
    if parts.len() < 2 {
        return None;
    }
    let mut values = Vec::new();
    for p in &parts {
        let q = p.chars().next()?;
        if !(q == '\'' || q == '"') || !p.ends_with(q) || p.len() < 3 {
            return None;
        }
        values.push(p[1..p.len() - 1].to_string());
    }
    let name = head
        .trim()
        .trim_start_matches("export ")
        .trim_start_matches("type ")
        .trim_end_matches('?')
        .trim()
        .to_string();
    (!name.is_empty() && !name.contains(' ')).then_some((name, values))
}

/// The new configurable things the diff's added lines declare in source
/// files: option lists (`list: [...]`, `options: [...]`, with `value:`
/// keys or plain strings), string-literal unions, and on/off fields
/// (`type: 'boolean'`), each named by the nearest `name:` before it.
pub fn new_options(files: &[(String, String, String)]) -> Vec<NewOption> {
    let mut out: Vec<NewOption> = Vec::new();
    for (path, status, patch) in files {
        if status == "removed" || !option_source(path) {
            continue;
        }
        let added: Vec<&str> = patch
            .lines()
            .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
            .map(|l| &l[1..])
            .collect();
        let mut last_name: Option<String> = None;
        let mut i = 0;
        while i < added.len() {
            let line = added[i];
            if let Some(n) = keyed(line, "name") {
                last_name = Some(n);
            }
            if option_list_start(line) {
                // Collect the list to its closing bracket.
                let mut depth = 0i32;
                let mut text = String::new();
                let mut j = i;
                while j < added.len() {
                    let l = added[j];
                    let from = if j == i { l.find('[').unwrap_or(0) } else { 0 };
                    for c in l[from..].chars() {
                        match c {
                            '[' => depth += 1,
                            ']' => depth -= 1,
                            _ => {}
                        }
                    }
                    text.push_str(l);
                    text.push('\n');
                    if depth <= 0 {
                        break;
                    }
                    j += 1;
                }
                let keyed_values: Vec<String> =
                    text.lines().filter_map(|l| keyed(l, "value")).collect();
                let values = if keyed_values.is_empty() {
                    literals(&text[text.find('[').map_or(0, |k| k + 1)..])
                } else {
                    keyed_values
                };
                if values.len() >= 2 {
                    out.push(NewOption {
                        name: last_name
                            .clone()
                            .unwrap_or_else(|| "an option list".to_string()),
                        values,
                        toggle: false,
                    });
                }
                i = j + 1;
                continue;
            }
            if let Some(ty) = keyed(line, "type") {
                if ty == "boolean" {
                    // The field's name is before it or just after.
                    let name = last_name.clone().or_else(|| {
                        added[i + 1..added.len().min(i + 6)]
                            .iter()
                            .find_map(|l| keyed(l, "name"))
                    });
                    if let Some(name) = name {
                        out.push(NewOption {
                            name,
                            values: vec!["true".into(), "false".into()],
                            toggle: true,
                        });
                    }
                }
            }
            if let Some((name, values)) = union(line) {
                out.push(NewOption {
                    name,
                    values,
                    toggle: false,
                });
            }
            i += 1;
        }
    }
    out.dedup();
    out
}

/// The values of `options` no test names, as "name: value". A value is
/// named when the tests quote it; an on/off field's case when a test line
/// names the field with `true` (shown) or with `false` (hidden).
pub fn untested_options(options: &[NewOption], tests: &str) -> Vec<String> {
    let mut out = Vec::new();
    for o in options {
        for v in &o.values {
            let named = if o.toggle {
                tests
                    .lines()
                    .any(|l| l.contains(o.name.as_str()) && l.contains(v.as_str()))
            } else {
                ['\'', '"', '`']
                    .iter()
                    .any(|q| tests.contains(&format!("{q}{v}{q}")))
            };
            if !named {
                let case = match (o.toggle, v.as_str()) {
                    (true, "true") => "true (shown)".to_string(),
                    (true, _) => "false (hidden)".to_string(),
                    _ => format!("`{v}`"),
                };
                out.push(format!("`{}` to {case}", o.name));
            }
        }
    }
    out
}

/// Widths a spec sets its viewport to.
fn widths(text: &str) -> Vec<u32> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(i) = text[from..].find("width") {
        let at = from + i + 5;
        from = at;
        let rest = text[at..].trim_start_matches([' ', ':', '=', '\'', '"']);
        let n: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(w) = n.parse::<u32>() {
            if (200..=4000).contains(&w) {
                out.push(w);
            }
        }
    }
    out
}

/// Whether a spec checks there is no horizontal scroll at a narrow and a
/// wide viewport.
pub fn responsive_checked(text: &str) -> bool {
    let w = widths(text);
    let measures = text.contains("scrollWidth") || text.contains("overflow");
    measures && w.iter().any(|&x| x <= NARROW_MAX) && w.iter().any(|&x| x >= WIDE_MIN)
}

/// Whether the change is UI work.
pub fn is_ui(paths: &[String]) -> bool {
    paths.iter().any(|p| {
        let l = p.to_lowercase();
        [".tsx", ".jsx", ".vue", ".svelte", ".css", ".scss", ".astro"]
            .iter()
            .any(|e| l.ends_with(e))
            && !l.contains(".test.")
            && !l.contains(".spec.")
    })
}

/// Whether a path is a test map: a file that lists specs by name.
pub fn is_test_map(path: &str) -> bool {
    let l = path.to_lowercase();
    let name = l.rsplit('/').next().unwrap_or(&l);
    [
        "test-map",
        "testmap",
        "test_map",
        "spec-map",
        "specmap",
        "test-registry",
    ]
    .iter()
    .any(|m| name.contains(m))
}

/// The new specs a test map that lists specs doesn't name, as (map, spec).
pub fn missing_from_test_maps(
    maps: &[(String, String)],
    specs: &[String],
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (map, text) in maps {
        for s in specs {
            let base = s.rsplit('/').next().unwrap_or(s);
            let stem = base.split('.').next().unwrap_or(base);
            if !text.contains(base) && !text.contains(stem) {
                out.push((map.clone(), s.clone()));
            }
        }
    }
    out
}

/// Which package manager the root uses, by its lockfile.
fn runner(tree: &[String]) -> &'static str {
    if tree.iter().any(|p| p == "pnpm-lock.yaml") {
        "pnpm"
    } else if tree.iter().any(|p| p == "yarn.lock") {
        "yarn"
    } else if tree.iter().any(|p| p == "bun.lockb" || p == "bun.lock") {
        "bun"
    } else {
        "npm"
    }
}

fn install(tree: &[String]) -> String {
    match runner(tree) {
        "pnpm" => "pnpm install --frozen-lockfile".into(),
        "yarn" => "yarn install --frozen-lockfile".into(),
        "bun" => "bun install --frozen-lockfile".into(),
        _ if tree.iter().any(|p| p == "package-lock.json") => "npm ci".into(),
        _ => "npm install".into(),
    }
}

fn safe_script(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || ":-_".contains(c))
}

/// The root's type-check, lint and unit-test steps, from its package.json
/// scripts. Empty when the root has none of them.
pub fn root_steps(package_json: &str, tree: &[String]) -> Vec<Step> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(package_json) else {
        return Vec::new();
    };
    let Some(scripts) = v["scripts"].as_object() else {
        return Vec::new();
    };
    let r = runner(tree);
    let find = |names: &[&str], skip: &[&str]| {
        names.iter().find_map(|n| {
            let cmd = scripts.get(*n)?.as_str()?;
            (safe_script(n) && !skip.iter().any(|s| cmd.contains(s))).then(|| n.to_string())
        })
    };
    let mut steps = Vec::new();
    let mut add = |kind: &'static str, script: Option<String>| {
        if let Some(s) = script {
            steps.push(Step {
                kind,
                cmd: format!("{r} run {s}"),
            });
        }
    };
    add(
        "typecheck",
        find(&["typecheck", "type-check", "check-types", "tsc"], &[]),
    );
    add("lint", find(&["lint"], &[]));
    add(
        "unit",
        find(
            &["test:unit", "unit", "test"],
            &["playwright", "cypress", "e2e"],
        ),
    );
    if steps.is_empty() {
        return steps;
    }
    steps.insert(
        0,
        Step {
            kind: "install",
            cmd: install(tree),
        },
    );
    steps
}

/// Words that make a skip a browser or platform condition (allowed).
const PLATFORM_WORDS: &[&str] = &[
    "browsername",
    "isMobile",
    "ismobile",
    "process.platform",
    "platform",
    "webkit",
    "firefox",
    "chromium",
    "safari",
    "devicename",
    "project.name",
    "testinfo.project",
    "viewport",
];

/// Skip calls in end-to-end test code.
const SKIP_CALLS: &[&str] = &[
    "test.skip(",
    "it.skip(",
    "describe.skip(",
    "test.describe.skip(",
    "test.fixme(",
    "testinfo.skip(",
    "this.skip(",
    "cy.skip(",
    "pending(",
];

/// The skips a spec's diff adds that aren't for a browser or platform
/// condition, as "spec: line". Read from the added lines of the patch with
/// the lines just before each (an `if (!block) test.skip()` reads its
/// condition there).
pub fn data_skips(path: &str, patch: &str) -> Vec<String> {
    let lines: Vec<&str> = patch.lines().filter(|l| !l.starts_with("@@")).collect();
    let mut out = Vec::new();
    for (i, l) in lines.iter().enumerate() {
        if !(l.starts_with('+') && !l.starts_with("+++")) {
            continue;
        }
        let code = l[1..].trim();
        if code.starts_with("//") || code.starts_with('*') {
            continue;
        }
        let low = code.to_ascii_lowercase().replace(' ', "");
        if !SKIP_CALLS.iter().any(|c| low.contains(c)) {
            continue;
        }
        let from = i.saturating_sub(3);
        let context = lines[from..=i]
            .iter()
            .map(|x| x.get(1..).unwrap_or("").to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(" ");
        if PLATFORM_WORDS
            .iter()
            .any(|w| context.contains(&w.to_ascii_lowercase()))
        {
            continue;
        }
        out.push(format!("{path}: `{}`", common::truncate_str(code, 120)));
    }
    out
}

/// What the static part of the checklist reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Facts {
    pub cases_required: usize,
    pub cases_added: usize,
    pub ui: bool,
    /// The end-to-end specs the change adds or changes.
    pub specs: Vec<String>,
    /// Specs with the responsive check.
    pub responsive_specs: Vec<String>,
    /// Test maps that don't name a new spec, as "map: spec".
    pub map_missing: Vec<String>,
    /// The repository has end-to-end tests.
    pub has_e2e: bool,
    /// How many new configurable things the change adds.
    #[serde(default)]
    pub options_found: usize,
    /// Their values no test names, as "name: value".
    #[serde(default)]
    pub options_untested: Vec<String>,
    /// Skips the new or changed specs add for missing content or data, as
    /// "spec: line" (see [`data_skips`]).
    #[serde(default)]
    pub data_skips: Vec<String>,
}

/// The items that read the diff alone.
pub fn static_items(f: &Facts) -> Vec<Item> {
    let mut out = Vec::new();
    if f.cases_required > 0 {
        let status = if f.cases_added >= f.cases_required {
            Status::Pass
        } else {
            Status::Fail
        };
        out.push(Item::new(
            "unit tests for the acceptance criteria",
            status,
            format!(
                "{} test case(s) added for {} listed case(s)",
                f.cases_added, f.cases_required
            ),
        ));
    }
    if f.options_found > 0 {
        if f.options_untested.is_empty() {
            out.push(Item::new(
                "tests for each option",
                Status::Pass,
                format!(
                    "every value of {} new option(s) is named in a test",
                    f.options_found
                ),
            ));
        } else {
            let mut shown: Vec<String> = f.options_untested.iter().take(8).cloned().collect();
            if f.options_untested.len() > 8 {
                shown.push(format!("{} more", f.options_untested.len() - 8));
            }
            out.push(Item::new(
                "tests for each option",
                Status::Fail,
                format!("no test sets {}", shown.join("; ")),
            ));
        }
    }
    if f.has_e2e && f.ui {
        if f.specs.is_empty() {
            out.push(Item::new(
                "end-to-end spec",
                Status::Fail,
                "the change adds UI but no end-to-end spec",
            ));
        } else {
            out.push(Item::new(
                "end-to-end spec",
                Status::Pass,
                f.specs.join(", "),
            ));
        }
    }
    if !f.specs.is_empty() || !f.map_missing.is_empty() {
        if f.map_missing.is_empty() {
            out.push(Item::new(
                "test map entry",
                Status::Pass,
                "every new spec is listed in the repository's test maps (or it keeps none)",
            ));
        } else {
            out.push(Item::new(
                "test map entry",
                Status::Fail,
                format!("not listed: {}", f.map_missing.join("; ")),
            ));
        }
    }
    if !f.specs.is_empty() {
        if f.data_skips.is_empty() {
            out.push(Item::new(
                "end-to-end tests fail when their content is missing",
                Status::Pass,
                "no new or changed spec skips itself on missing content",
            ));
        } else {
            out.push(Item::new(
                "end-to-end tests fail when their content is missing",
                Status::Fail,
                format!(
                    "skips on missing content, so the test can never fail: {}",
                    f.data_skips.join("; ")
                ),
            ));
        }
    }
    if f.ui && !f.specs.is_empty() {
        if f.responsive_specs.is_empty() {
            out.push(Item::new(
                "no horizontal scroll at narrow and wide viewports",
                Status::Fail,
                format!(
                    "no spec checks scrollWidth at a width of {NARROW_MAX}px or less and {WIDE_MIN}px or more"
                ),
            ));
        } else {
            out.push(Item::new(
                "no horizontal scroll at narrow and wide viewports",
                Status::Pass,
                format!("checked in {}", f.responsive_specs.join(", ")),
            ));
        }
    }
    out
}

/// What the coder is told about failing static items.
pub fn feedback(items: &[Item]) -> String {
    let fails = failing(items);
    if fails.is_empty() {
        return String::new();
    }
    let mut s = String::from(
        "\nThe verification checklist for this feature isn't met yet. Fix each item:\n",
    );
    for f in fails {
        s.push_str(&format!("- {f}\n"));
    }
    s.push_str(&format!(
        "Unit tests: one test per option or case the acceptance criteria list, and one per \
         value of each new option the change adds (each value of an option list or union, the \
         shown and the hidden case of an on/off field). A new \
         end-to-end spec goes in every test map the repository keeps. For UI, the spec checks \
         `document.documentElement.scrollWidth <= window.innerWidth` at a viewport {NARROW_MAX}px \
         wide or less and one {WIDE_MIN}px wide or more. An end-to-end test never calls \
         `test.skip` (or `fixme`, `it.skip`) when the content or data it needs is missing: rewrite \
         it to fail with a message naming the missing content (for example \
         `expect(block, 'staging page /x has no gallery block').toBeVisible()`); a skip is only \
         for a stated browser or platform condition.\n"
    ));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_spec_that_skips_on_missing_content_fails_the_checklist() {
        let patch = "@@ -0,0 +1,12 @@\n+test('gallery renders', async ({ page }) => {\n+  await page.goto('/events')\n+  const block = page.locator('[data-block=gallery]')\n+  if ((await block.count()) === 0) {\n+    test.skip(true, 'no gallery block on staging')\n+  }\n+  // test.skip() in a comment is fine\n+  test.skip(browserName === 'webkit', 'webkit has no clipboard')\n+})\n";
        let skips = data_skips("e2e/gallery.spec.ts", patch);
        assert_eq!(skips.len(), 1, "{skips:?}");
        assert!(skips[0].contains("no gallery block on staging"));
        let f = Facts {
            specs: vec!["e2e/gallery.spec.ts".into()],
            data_skips: skips,
            ..Default::default()
        };
        let items = static_items(&f);
        let item = items
            .iter()
            .find(|i| i.name.contains("content is missing"))
            .unwrap();
        assert_eq!(item.status, Status::Fail);
        assert!(feedback(&items).contains("never calls `test.skip`"));
        // A platform skip alone passes.
        let ok = data_skips(
            "e2e/a.spec.ts",
            "@@\n+test.skip(process.platform === 'win32', 'paths')\n",
        );
        assert!(ok.is_empty());
    }

    #[test]
    fn counts_the_cases_the_acceptance_criteria_list() {
        let body = "Build the filter.\n\nAcceptance criteria:\n- Shows all events by default\n- Filters by venue\n1. Filters by date\n\n## Notes\n- not a case\n";
        assert_eq!(acceptance_cases(body, ""), 3);
        let spec = "### Scenario: default\nGiven...\n### Scenario: by venue\n";
        assert_eq!(acceptance_cases("No criteria here", spec), 2);
    }

    #[test]
    fn counts_unit_cases_outside_e2e_specs() {
        let files = vec![
            (
                "src/filter.test.ts".to_string(),
                "added".to_string(),
                "+it('shows all', () => {})\n+  test('by venue', () => {})\n+const x = 1\n"
                    .to_string(),
            ),
            (
                "e2e/filter.spec.ts".to_string(),
                "added".to_string(),
                "+test('e2e', async () => {})\n".to_string(),
            ),
        ];
        assert_eq!(unit_cases_added(&files), 2);
    }

    #[test]
    fn a_responsive_check_needs_both_viewports_and_a_measure() {
        let ok = "await page.setViewportSize({ width: 375, height: 800 });\nexpect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(375);\nawait page.setViewportSize({ width: 1440, height: 900 });";
        assert!(responsive_checked(ok));
        assert!(!responsive_checked(
            "setViewportSize({ width: 375 }) scrollWidth"
        ));
        assert!(!responsive_checked("width: 375 width: 1440"));
    }

    #[test]
    fn finds_test_maps_missing_a_new_spec() {
        assert!(is_test_map("e2e/test-map.json"));
        assert!(!is_test_map("e2e/quote.spec.ts"));
        let maps = vec![(
            "e2e/test-map.json".to_string(),
            "{\"hero\": \"hero.spec.ts\"}".to_string(),
        )];
        let m = missing_from_test_maps(&maps, &["e2e/quote.spec.ts".to_string()]);
        assert_eq!(
            m,
            vec![(
                "e2e/test-map.json".to_string(),
                "e2e/quote.spec.ts".to_string()
            )]
        );
        assert!(missing_from_test_maps(&maps, &["e2e/hero.spec.ts".to_string()]).is_empty());
    }

    #[test]
    fn root_steps_come_from_the_repositorys_scripts() {
        let pj = r#"{"scripts":{"typecheck":"tsc --noEmit","lint":"eslint .","test":"playwright test","test:unit":"vitest run"}}"#;
        let tree = vec!["pnpm-lock.yaml".to_string()];
        let steps = root_steps(pj, &tree);
        let cmds: Vec<&str> = steps.iter().map(|s| s.cmd.as_str()).collect();
        assert_eq!(
            cmds,
            vec![
                "pnpm install --frozen-lockfile",
                "pnpm run typecheck",
                "pnpm run lint",
                "pnpm run test:unit"
            ]
        );
        // `test` that runs the browser suite isn't the unit tests.
        let pj = r#"{"scripts":{"test":"playwright test"}}"#;
        assert!(root_steps(pj, &tree).is_empty());
    }

    #[test]
    fn the_static_items_hold_the_feature_to_its_tests() {
        let f = Facts {
            cases_required: 3,
            cases_added: 2,
            ui: true,
            specs: vec!["e2e/quote.spec.ts".into()],
            responsive_specs: vec![],
            map_missing: vec!["e2e/test-map.json: e2e/quote.spec.ts".into()],
            has_e2e: true,
            options_found: 0,
            options_untested: vec![],
            data_skips: vec![],
        };
        let items = static_items(&f);
        assert_eq!(failing(&items).len(), 3);
        assert!(markdown(&items)
            .contains("[FAIL] unit tests for the acceptance criteria: 2 test case(s) added for 3"));
        assert!(feedback(&items).contains("scrollWidth"));
        let ok = Facts {
            cases_added: 3,
            responsive_specs: vec!["e2e/quote.spec.ts".into()],
            map_missing: vec![],
            ..f
        };
        assert!(failing(&static_items(&ok)).is_empty());
    }

    /// The observed kind of feature: a CMS block with an option list per
    /// tab, an on/off field and a prop union. Every value needs a test.
    #[test]
    fn every_value_of_a_new_option_is_named_in_a_test() {
        let schema = "+export const heroBlock = defineType({\n\
            +  name: 'heroBlock',\n\
            +  fields: [\n\
            +    defineField({\n\
            +      name: 'sortBy',\n\
            +      type: 'string',\n\
            +      options: {\n\
            +        list: [\n\
            +          { title: 'Start date', value: 'startDate' },\n\
            +          { title: 'Spots left', value: 'spotsLeft' },\n\
            +        ],\n\
            +      },\n\
            +    }),\n\
            +    defineField({ name: 'showBlurb', type: 'boolean' }),\n\
            +  ],\n\
            +});\n";
        let component = "+type HeroProps = {\n+  size?: 'small' | 'large';\n+};\n";
        let files = vec![
            (
                "studio/blocks/hero.ts".to_string(),
                "added".to_string(),
                schema.to_string(),
            ),
            (
                "web/src/hero.tsx".to_string(),
                "added".to_string(),
                component.to_string(),
            ),
            (
                "web/src/sanity.types.ts".to_string(),
                "modified".to_string(),
                "+  kind?: 'a' | 'b';\n".to_string(),
            ),
        ];
        let opts = new_options(&files);
        let names: Vec<&str> = opts.iter().map(|o| o.name.as_str()).collect();
        assert_eq!(names, vec!["sortBy", "showBlurb", "size"], "{opts:?}");
        assert_eq!(opts[0].values, vec!["startDate", "spotsLeft"]);
        assert!(opts[1].toggle);

        let tests = "it('sorts by start date', () => render({ sortBy: 'startDate' }))\n\
            it('shows the blurb', () => render({ showBlurb: true }))\n\
            it('is small', () => render({ size: \"small\" }))\n";
        let missing = untested_options(&opts, tests);
        assert_eq!(
            missing,
            vec![
                "`sortBy` to `spotsLeft`".to_string(),
                "`showBlurb` to false (hidden)".to_string(),
                "`size` to `large`".to_string(),
            ]
        );
        let all = format!(
            "{tests}it('spots', () => render({{ sortBy: 'spotsLeft' }}))\n\
             it('hides', () => render({{ showBlurb: false }}))\n\
             it('large', () => render({{ size: 'large' }}))\n"
        );
        assert!(untested_options(&opts, &all).is_empty());

        let f = Facts {
            options_found: opts.len(),
            options_untested: missing,
            ..Default::default()
        };
        let items = static_items(&f);
        assert!(markdown(&items)
            .contains("[FAIL] tests for each option: no test sets `sortBy` to `spotsLeft`"));
        assert!(feedback(&items).contains("the shown and the hidden case"));
    }

    #[test]
    fn test_files_and_unrelated_brackets_are_not_options() {
        let files = vec![
            (
                "src/a.test.ts".to_string(),
                "added".to_string(),
                "+const x: 'a' | 'b' = 'a';\n".to_string(),
            ),
            (
                "src/b.ts".to_string(),
                "added".to_string(),
                "+const opts = { options: [...defaults, extra] };\n+const n = a | b;\n".to_string(),
            ),
        ];
        assert!(new_options(&files).is_empty());
        assert!(is_test_path("e2e/tests/hero.spec.ts"));
        assert!(is_test_path("tests/test_hero.py"));
        assert!(!is_test_path("src/hero.tsx"));
    }
}
