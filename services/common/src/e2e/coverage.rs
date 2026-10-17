//! Which parts of an app the E2E tests cover.
//!
//! A repo can keep a test map: areas of the app, the code paths each covers,
//! and the tag its tests carry. The format, in `e2e/test-map.json` (or
//! `test/test-map.json`, or `.coderhelm/test-map.json`):
//!
//! ```json
//! { "areas": { "checkout": { "tag": "@checkout", "description": "…",
//!                            "paths": ["apps/web/src/features/checkout/**"] } },
//!   "runAll": ["package.json"], "ignore": ["**/*.md"] }
//! ```
//!
//! Paths are repo-relative globs: `*` is one path segment, `**` any depth.

use std::sync::LazyLock;

use regex::Regex;
use serde::Serialize;
use serde_json::Value;

pub const TEST_MAP_PATHS: &[&str] = &[
    "e2e/test-map.json",
    "test/test-map.json",
    ".coderhelm/test-map.json",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Area {
    pub name: String,
    pub tag: String,
    pub description: String,
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TestMap {
    pub areas: Vec<Area>,
    pub run_all: Vec<String>,
    pub ignore: Vec<String>,
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s.as_str().map(str::to_string))
        .collect()
}

pub fn parse_test_map(text: &str) -> Option<TestMap> {
    let v: Value = serde_json::from_str(text).ok()?;
    let areas = v["areas"].as_object()?;
    let mut out: Vec<Area> = areas
        .iter()
        .map(|(name, a)| Area {
            name: name.clone(),
            tag: a["tag"]
                .as_str()
                .unwrap_or("")
                .trim_start_matches('@')
                .to_string(),
            description: a["description"].as_str().unwrap_or("").to_string(),
            paths: strings(&a["paths"]),
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Some(TestMap {
        areas: out,
        run_all: strings(&v["runAll"]),
        ignore: strings(&v["ignore"]),
    })
}

/// Turns a repo glob into a regex: `**` any depth, `*` one segment.
pub fn glob_regex(glob: &str) -> Option<Regex> {
    let mut re = String::from("^");
    let mut chars = glob.trim().trim_start_matches("./").chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' if chars.peek() == Some(&'*') => {
                chars.next();
                if chars.peek() == Some(&'/') {
                    chars.next();
                    re.push_str("(?:.*/)?");
                } else {
                    re.push_str(".*");
                }
            }
            '*' => re.push_str("[^/]*"),
            '?' => re.push_str("[^/]"),
            c => re.push_str(&regex::escape(&c.to_string())),
        }
    }
    re.push('$');
    Regex::new(&re).ok()
}

pub fn matches(glob: &str, path: &str) -> bool {
    glob_regex(glob).is_some_and(|r| r.is_match(path))
}

impl TestMap {
    /// The areas whose paths cover a file.
    pub fn areas_for(&self, path: &str) -> Vec<&Area> {
        self.areas
            .iter()
            .filter(|a| a.paths.iter().any(|g| matches(g, path)))
            .collect()
    }

    pub fn ignored(&self, path: &str) -> bool {
        self.ignore.iter().any(|g| matches(g, path))
    }

    /// Changed files that no area covers (and that aren't ignored or run-all).
    pub fn uncovered<'a>(&self, paths: &'a [String]) -> Vec<&'a str> {
        paths
            .iter()
            .map(String::as_str)
            .filter(|p| {
                !self.ignored(p)
                    && !self.run_all.iter().any(|g| matches(g, p))
                    && self.areas_for(p).is_empty()
            })
            .collect()
    }
}

static APP_PAGE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:^|/)app/(?P<route>.*?)/?page\.(?:tsx|jsx|ts|js|mdx)$").expect("valid")
});
static PAGES_PAGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^|/)pages/(?P<route>.+)\.(?:tsx|jsx|ts|js)$").expect("valid"));
static GROUP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\([^)/]*\)/?").expect("valid"));

/// The route a page file serves, for Next.js-style app and pages folders.
pub fn route_of(path: &str) -> Option<String> {
    let test_dir =
        |seg: &str| matches!(seg, "node_modules" | "test" | "tests" | "__tests__" | "e2e");
    if path.split('/').any(test_dir) {
        return None;
    }
    if let Some(c) = APP_PAGE.captures(path) {
        let r = GROUP.replace_all(c.name("route").map(|m| m.as_str()).unwrap_or(""), "");
        let r = r.trim_end_matches('/');
        return Some(format!("/{r}"));
    }
    if let Some(c) = PAGES_PAGE.captures(path) {
        let r = c.name("route")?.as_str();
        if r.starts_with('_') || r.starts_with("api/") {
            return None;
        }
        let r = r.trim_end_matches("index").trim_end_matches('/');
        return Some(format!("/{r}"));
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Route {
    pub route: String,
    pub file: String,
}

/// Page files in a repo tree that no area of the test map covers.
pub fn uncovered_routes(map: &TestMap, tree: &[String]) -> Vec<Route> {
    let mut out: Vec<Route> = tree
        .iter()
        .filter_map(|f| route_of(f).map(|r| (r, f)))
        .filter(|(_, f)| !map.ignored(f) && map.areas_for(f).is_empty())
        .map(|(route, f)| Route {
            route,
            file: f.clone(),
        })
        .collect();
    out.sort_by(|a, b| a.route.cmp(&b.route));
    out.dedup_by(|a, b| a.route == b.route);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAP: &str = r#"{
      "areas": {
        "checkout": {"tag": "@checkout", "description": "Pay for an order", "paths": ["apps/web/src/features/checkout/**", "apps/web/src/app/checkout/page.tsx"]},
        "home": {"tag": "@home", "paths": ["apps/web/src/app/page.tsx", "apps/web/src/components/*.tsx"]}
      },
      "runAll": ["pnpm-lock.yaml"],
      "ignore": ["**/*.md"]
    }"#;

    #[test]
    fn globs_follow_segment_rules() {
        assert!(matches(
            "apps/web/src/features/checkout/**",
            "apps/web/src/features/checkout/a/b.tsx"
        ));
        assert!(matches(
            "apps/web/src/components/*.tsx",
            "apps/web/src/components/Hero.tsx"
        ));
        assert!(!matches(
            "apps/web/src/components/*.tsx",
            "apps/web/src/components/blocks/Hero.tsx"
        ));
        assert!(matches("**/*.md", "README.md"));
        assert!(matches("**/*.md", "docs/a/b.md"));
        assert!(!matches("apps/*.ts", "apps/a/b.ts"));
    }

    #[test]
    fn the_map_finds_areas_and_gaps() {
        let m = parse_test_map(MAP).unwrap();
        assert_eq!(m.areas.len(), 2);
        assert_eq!(m.areas[0].name, "checkout");
        assert_eq!(m.areas[0].tag, "checkout");
        assert_eq!(
            m.areas_for("apps/web/src/features/checkout/Pay.tsx")[0].name,
            "checkout"
        );
        let changed = vec![
            "apps/web/src/features/checkout/Pay.tsx".to_string(),
            "apps/web/src/features/account/Profile.tsx".to_string(),
            "README.md".to_string(),
            "pnpm-lock.yaml".to_string(),
        ];
        assert_eq!(
            m.uncovered(&changed),
            vec!["apps/web/src/features/account/Profile.tsx"]
        );
        assert!(parse_test_map("{}").is_none());
        assert!(parse_test_map("not json").is_none());
    }

    #[test]
    fn routes_come_from_page_files() {
        assert_eq!(route_of("apps/web/src/app/page.tsx").as_deref(), Some("/"));
        assert_eq!(
            route_of("apps/web/src/app/(marketing)/about/page.tsx").as_deref(),
            Some("/about")
        );
        assert_eq!(
            route_of("app/account/[id]/page.tsx").as_deref(),
            Some("/account/[id]")
        );
        assert_eq!(
            route_of("src/pages/settings/index.tsx").as_deref(),
            Some("/settings")
        );
        assert_eq!(route_of("src/pages/_app.tsx"), None);
        assert_eq!(route_of("src/pages/api/hook.ts"), None);
        assert_eq!(route_of("src/lib/page.ts"), None);
        assert_eq!(
            route_of("app/testimonials/page.tsx").as_deref(),
            Some("/testimonials")
        );
        assert_eq!(route_of("src/test/app/page.tsx"), None);
        let m = parse_test_map(MAP).unwrap();
        let tree = vec![
            "apps/web/src/app/page.tsx".to_string(),
            "apps/web/src/app/checkout/page.tsx".to_string(),
            "apps/web/src/app/account/page.tsx".to_string(),
        ];
        let gaps = uncovered_routes(&m, &tree);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].route, "/account");
    }
}
