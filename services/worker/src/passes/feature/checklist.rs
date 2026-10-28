//! Rule 7: the verification checklist a feature meets before it is ready.
//!
//! Derived from the repository and the ticket, never from the coder's own
//! account:
//!
//! - type-check, lint and unit tests, with the repository's own scripts;
//! - the unit tests the acceptance criteria imply: one per listed option or
//!   case ([`acceptance_cases`]), counted in the diff ([`unit_cases_added`]);
//! - the end-to-end spec, and its entry in any test map the repository keeps
//!   ([`missing_from_test_maps`]);
//! - the checks of each test package outside the main workspace;
//! - the new spec run repeatedly (`--repeat-each=5`) against the preview;
//! - for UI work, a no-horizontal-scroll check at a narrow and a wide
//!   viewport in the spec ([`responsive_checked`]).
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
        "Unit tests: one test per option or case the acceptance criteria list. A new \
         end-to-end spec goes in every test map the repository keeps. For UI, the spec checks \
         `document.documentElement.scrollWidth <= window.innerWidth` at a viewport {NARROW_MAX}px \
         wide or less and one {WIDE_MIN}px wide or more.\n"
    ));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
