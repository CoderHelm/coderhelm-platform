//! The app map for runs: the cross-repository map the App map page draws
//! (`common::app_map`), read by the run's own agents.
//!
//! A run plans, codes and reviews one repository at a time. The map knows
//! what that repository's code is to the rest of the app: which other
//! repositories call its endpoints, which screens and journeys stand on a
//! file, which outside services it reaches and which cloud resources deploy
//! or start it. So:
//!
//! - **Every pass that reads code** (the planner, the coder and every round
//!   that runs through it: CI fixes, test fixes, incident and alarm fix runs;
//!   the reviewer) gets the map lookups (`map_overview`, `map_find`,
//!   `map_part`, `map_screen`, `map_file`, `map_flow`, `map_impact`) next to
//!   the code graph's, through [`super::code_graph::RepoGraphs`]: the one
//!   registry every agent's executor already asks. The map is read from the
//!   bucket once per step ([`RunMap::load`]) and kept for the step.
//! - **The planner** reads a short overview of the map in its first message
//!   ([`RunMap::planner_section`], at most [`OVERVIEW_CHARS`]): the parts,
//!   the repositories, and the parts the ticket's likely files sit in. Its
//!   design carries an `## Affected elsewhere` section from `map_impact` on
//!   the files it plans to change; a plan that leaves out what the map shows
//!   goes back once ([`RunMap::plan_gaps`]), and every citation in the
//!   section is checked against the map before the plan is kept
//!   ([`RunMap::checked_design`]).
//! - **The reviewer** is shown the cross-repository impact of the change
//!   ([`RunMap::review_section`]): each endpoint the diff changes that other
//!   repositories call, with the callers' `file:line`, to be raised as a
//!   breaking change when the contract changes.
//!
//! No map, a map that doesn't hold the work's repository, or one older than
//! [`STALE_SECS`]: nothing is offered and nothing is said. A run never waits
//! on the map and never fails for want of one.

use std::collections::BTreeSet;
use std::sync::Mutex;

use common::app_map::{self as am, Lookups};
use serde_json::Value;
use tracing::info;

use crate::agent::llm::ToolDefinition;
use crate::WorkerState;

/// A map older than this is not read by runs: what it says about callers
/// may no longer hold.
pub const STALE_SECS: i64 = 30 * 24 * 3600;
/// The longest overview the planner reads (about 1,500 tokens).
pub const OVERVIEW_CHARS: usize = 6_000;
/// The longest answer one lookup returns to a run's agent (about 2,000 tokens).
pub const RESULT_CHARS: usize = 8_000;
/// The longest impact section the reviewer reads (about 1,000 tokens).
pub const REVIEW_CHARS: usize = 4_000;
/// The heading the planner's design carries the cross-repository effects under.
pub const AFFECTED_HEADING: &str = "## Affected elsewhere";

/// Tokens a text costs, roughly (four characters a token).
pub fn tokens(text: &str) -> usize {
    text.len().div_ceil(4)
}

/// The team's app map for one step of a run on one repository.
pub struct RunMap {
    repo: String,
    folder: String,
    lookups: Mutex<Lookups>,
}

impl RunMap {
    /// Reads the team's map once for this step. `None` (and nothing said)
    /// when there is no map, it doesn't hold `repo`, or it is stale.
    pub async fn load(state: &WorkerState, team_id: &str, repo: &str) -> Option<RunMap> {
        let (graph, at) =
            am::load_graph_dated(&state.s3, &state.config.bucket_name, team_id).await?;
        let now = chrono::Utc::now().timestamp();
        if at > 0 && now - at > STALE_SECS {
            info!(
                team_id,
                repo, "App map: older than a month, not offered to the run"
            );
            return None;
        }
        let map = Self::of(graph, repo)?;
        info!(team_id, repo, "App map: offered to the run");
        Some(map)
    }

    /// The map over a graph already read (tests; and callers that hold it).
    /// `None` when the map doesn't hold `repo`.
    pub fn of(graph: Value, repo: &str) -> Option<RunMap> {
        let folder = graph["coderhelm"]["repos"]
            .as_object()?
            .iter()
            .find(|(_, r)| r.as_str().is_some_and(|r| r.eq_ignore_ascii_case(repo)))
            .map(|(f, _)| f.clone())?;
        Some(RunMap {
            repo: repo.to_string(),
            folder,
            lookups: Mutex::new(Lookups::new(graph)),
        })
    }

    fn with<T>(&self, f: impl FnOnce(&mut Lookups) -> T) -> T {
        let mut l = self.lookups.lock().unwrap_or_else(|p| p.into_inner());
        f(&mut l)
    }

    /// The map lookups, as the run's agents read them.
    pub fn tools(&self) -> Vec<ToolDefinition> {
        am::tool_defs()
            .into_iter()
            .map(|d| ToolDefinition {
                name: d["name"].as_str().unwrap_or("").to_string(),
                description: d["description"].as_str().unwrap_or("").to_string(),
                input_schema: d["input_schema"].clone(),
            })
            .collect()
    }

    /// What an agent is told about the map, by role.
    pub fn prompt_line(&self, role: super::code_graph::Role) -> String {
        use super::code_graph::Role;
        let folder = &self.folder;
        let intro = format!(
            "\n\nThe team has an APP MAP across all its repositories (map_overview, map_find, \
             map_part, map_screen, map_file, map_flow, map_impact). On it, this repository's \
             files start with `{folder}/`. map_impact on a file, an endpoint (METHOD /path) or a \
             part gives the callers in other repositories, the screens and journeys that use \
             it, the outside services and cloud resources behind it, each with a file:line."
        );
        let what = match role {
            Role::Planner => format!(
                " Before you plan a change to shared code or an endpoint, call map_impact on \
                 each file you plan to change. In design.md, add a `{AFFECTED_HEADING}` section: \
                 every other repository, screen (with its app and whether it needs a signed-in \
                 user), journey, outside service and cloud resource the change reaches, one \
                 bullet each with the file:line map_impact gave. Cite only places the map gave. \
                 When map_impact finds nothing elsewhere, write `None found on the app map.`"
            ),
            Role::Coder => " Before you change an endpoint or a file other repositories call, \
                 call map_impact on it and keep every caller working (the plan's \
                 `## Affected elsewhere` lists them)."
                .to_string(),
            Role::Reviewer => " For a changed endpoint or a shared file, call map_impact to find \
                 its callers in other repositories and its screens; a change to an endpoint's \
                 method, path, parameters, body or response that other repositories call is a \
                 breaking change: raise it, naming each caller's file:line. For each user journey \
                 map_impact lists on a changed file (map_flow gives its steps), check the journey \
                 still works end to end with the change."
                .to_string(),
        };
        format!("{intro}{what}")
    }

    /// Runs one map lookup for an agent: `None` when `name` isn't one.
    pub fn handle(&self, name: &str, input: &Value) -> Option<String> {
        if !am::is_tool(name) {
            return None;
        }
        let (v, label) = self.with(|l| l.lookup(name, input));
        let text = common::head_tail_str(&v.to_string(), RESULT_CHARS);
        info!(
            repo = %self.repo,
            lookup = name,
            found = v.get("error").is_none(),
            tokens = tokens(&text),
            "App map: {label}"
        );
        Some(text)
    }

    /// A repository path as the map writes it (`folder/path`), when the map
    /// has that file.
    pub fn map_path(&self, repo: &str, path: &str) -> Option<String> {
        self.with(|l| am::map_path(&l.graph, repo, path))
    }

    /// The planner's short overview of the map: the repositories, the
    /// parts, and the parts the ticket's likely files sit in. At most
    /// [`OVERVIEW_CHARS`].
    pub fn planner_section(&self, likely: &[String]) -> String {
        let (repos, parts, touched) = self.with(|l| {
            let g = &l.graph;
            let repos: Vec<String> = g["coderhelm"]["repos"]
                .as_object()
                .map(|o| {
                    o.iter()
                        .map(|(f, r)| format!("{} (`{f}/`)", r.as_str().unwrap_or(f)))
                        .collect()
                })
                .unwrap_or_default();
            let parts: Vec<String> = g["nodes"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|n| n["parent"].is_null())
                .map(|n| {
                    let kind = match n["role"].as_str() {
                        Some(role) => format!("{}, {role}", n["kind"].as_str().unwrap_or("")),
                        None => n["kind"].as_str().unwrap_or("").to_string(),
                    };
                    format!(
                        "- {} `{}` ({kind}): {}",
                        n["label"].as_str().unwrap_or(""),
                        n["id"].as_str().unwrap_or(""),
                        common::truncate_str(n["summary"].as_str().unwrap_or(""), 140)
                    )
                })
                .collect();
            let ids: Vec<String> = likely
                .iter()
                .filter_map(|p| am::map_path(g, &self.repo, p))
                .map(|p| format!("f:{p}"))
                .collect();
            let touched: Vec<String> = am::parts_of(g, &ids).into_iter().collect();
            (repos, parts, touched)
        });
        let mut out = format!(
            "\n\n## The app map (every repository of the team)\nRepositories: {}.\n\nParts:\n",
            repos.join(", ")
        );
        let tail =
            format!(
            "\nThe ticket's likely files sit in: {}.\nCall map_impact on each file you plan to \
             change and list what it reaches under `{AFFECTED_HEADING}` in design.md.\n",
            if touched.is_empty() {
                "no part found yet (use map_find)".to_string()
            } else {
                touched.iter().map(|t| format!("`{t}`")).collect::<Vec<_>>().join(", ")
            }
        );
        let room = OVERVIEW_CHARS.saturating_sub(out.len() + tail.len() + 40);
        let mut left = parts.len();
        for p in &parts {
            if out.len() + p.len() + 1 > room {
                break;
            }
            out.push_str(p);
            out.push('\n');
            left -= 1;
        }
        if left > 0 {
            out.push_str(&format!("- and {left} more (map_overview lists them)\n"));
        }
        out.push_str(&tail);
        info!(
            repo = %self.repo,
            tokens = tokens(&out),
            cap = tokens(&"x".repeat(OVERVIEW_CHARS)),
            "App map: the planner's overview"
        );
        out
    }

    /// What the map shows for one file of the work's repository: callers in
    /// other repositories, screens, journeys, outside services and cloud
    /// resources, each with its file:line. Empty when nothing elsewhere.
    pub fn reaches(&self, repo: &str, path: &str) -> Vec<String> {
        let Some(p) = self.map_path(repo, path) else {
            return Vec::new();
        };
        let Some(v) = self.with(|l| am::impact(&l.graph, &p)) else {
            return Vec::new();
        };
        let s = |x: &Value| x.as_str().unwrap_or("").to_string();
        let mut out = Vec::new();
        for c in v["callers_elsewhere"].as_array().into_iter().flatten() {
            out.push(format!(
                "{} ({}) calls it at {}",
                s(&c["from"]),
                s(&c["repo"]),
                s(&c["at"])
            ));
        }
        for sc in v["screens"].as_array().into_iter().flatten() {
            out.push(format!(
                "screen {} in {} ({}) at {}",
                s(&sc["name"]),
                s(&sc["app"]),
                sc["gate"].as_str().unwrap_or("gate not known"),
                s(&sc["at"])
            ));
        }
        for j in v["journeys"].as_array().into_iter().flatten() {
            // Every step of the journey that stands on it, each at its line.
            let at: Vec<String> = j["at"]
                .as_array()
                .into_iter()
                .flatten()
                .map(s)
                .take(6)
                .collect();
            out.push(format!("journey {} at {}", s(&j["title"]), at.join(", ")));
        }
        for o in v["outside_services"].as_array().into_iter().flatten() {
            out.push(format!(
                "{} {} at {}",
                s(&o["direction"]),
                s(&o["label"]),
                s(&o["at"])
            ));
        }
        for c in v["cloud"].as_array().into_iter().flatten() {
            out.push(format!(
                "{} {} ({}) at {}",
                s(&c["label"]),
                s(&c["how"]),
                s(&c["resource"]),
                s(&c["at"])
            ));
        }
        out
    }

    /// Plan gaps: the planned files the map shows reaching other
    /// repositories, screens or services, when design.md has no
    /// `## Affected elsewhere` section. Sent back once with what the map
    /// found, so the plan names them.
    pub fn plan_gaps(&self, files: &[(String, String)], design: &str) -> Vec<String> {
        if has_section(design) {
            return Vec::new();
        }
        let mut found: Vec<String> = Vec::new();
        for (repo, path) in files.iter().take(30) {
            // What the change itself reaches: services a few imports away
            // are not the plan's to name.
            let r: Vec<String> = self
                .reaches(repo, path)
                .into_iter()
                .filter(|x| !x.starts_with("reaches "))
                .collect();
            if !r.is_empty() {
                found.push(format!(
                    "{path}: {}",
                    r.into_iter().take(6).collect::<Vec<_>>().join("; ")
                ));
            }
        }
        if found.is_empty() {
            return Vec::new();
        }
        let text = format!(
            "design.md has no `{AFFECTED_HEADING}` section, but the app map shows the planned \
             files reach beyond this repository. Call map_impact on them and add the section, one \
             bullet each with its file:line: {}",
            found.join(" | ")
        );
        vec![common::truncate_str(&text, 3_000).to_string()]
    }

    /// design.md with its `## Affected elsewhere` section checked against
    /// the map: a bullet whose every citation is not a real place on the map
    /// is dropped. Returns the design and how many bullets were dropped.
    pub fn checked_design(&self, design: &str) -> (String, usize) {
        self.with(|l| check_section(&l.graph, &self.repo, design))
    }

    /// The reviewer's cross-repository impact section for a pull request's
    /// changed files (`(path, patch)`), at most [`REVIEW_CHARS`]; empty when
    /// the map shows nothing beyond this repository.
    pub fn review_section(&self, changed: &[(String, String)]) -> String {
        let flagged = self.endpoint_changes(changed);
        let mut shared: Vec<String> = Vec::new();
        let mut journeys: Vec<String> = Vec::new();
        for (path, _) in changed.iter().take(40) {
            let Some(p) = self.map_path(&self.repo, path) else {
                continue;
            };
            if flagged.iter().any(|f| f.file == p) {
                continue;
            }
            let r = self.reaches(&self.repo, path);
            for j in r.iter().filter(|x| x.starts_with("journey ")) {
                let line = format!("- {path} is on {j}");
                if !journeys.contains(&line) {
                    journeys.push(line);
                }
            }
            let elsewhere: Vec<&String> =
                r.iter().filter(|x| x.contains(" calls it at ")).collect();
            if !elsewhere.is_empty() {
                shared.push(format!(
                    "- {path} is called from other repositories: {}",
                    elsewhere
                        .iter()
                        .take(6)
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join("; ")
                ));
            }
        }
        if flagged.is_empty() && shared.is_empty() && journeys.is_empty() {
            return String::new();
        }
        let mut out = String::from("\n## Cross-repository impact (from the app map)\n");
        if !flagged.is_empty() {
            out.push_str(
                "Endpoints this pull request changes that OTHER repositories call. If the change \
                 alters the method, path, parameters, request body or response shape, raise a \
                 finding (a breaking change) naming each caller's file:line below; if the change \
                 keeps the contract, say nothing about it:\n",
            );
            for f in &flagged {
                out.push_str(&format!("- {} {} ({}):\n", f.method, f.path, f.at));
                for c in &f.callers {
                    out.push_str(&format!("  - called from {c}\n"));
                }
                for s in &f.screens {
                    out.push_str(&format!("  - screen {s}\n"));
                }
            }
        }
        if !shared.is_empty() {
            out.push_str("Shared files this pull request changes:\n");
            out.push_str(&shared.join("\n"));
            out.push('\n');
        }
        if !journeys.is_empty() {
            out.push_str(
                "User journeys this pull request changes a step of. Check each still works end to \
                 end with the change (the step it changes, and the steps after it); raise a finding \
                 naming the journey and the step's file:line when it breaks:\n",
            );
            out.push_str(
                &journeys
                    .iter()
                    .take(12)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
            out.push('\n');
        }
        let out = common::truncate_str(&out, REVIEW_CHARS).to_string();
        info!(repo = %self.repo, endpoints = flagged.len(), tokens = tokens(&out), "App map: the reviewer's impact check");
        out
    }

    /// The endpoints a diff changes (a removed or rewritten line at the
    /// endpoint's definition, or one naming its path) that other
    /// repositories call.
    pub fn endpoint_changes(&self, changed: &[(String, String)]) -> Vec<EndpointChange> {
        let mut out = Vec::new();
        for (path, patch) in changed {
            let Some(p) = self.map_path(&self.repo, path) else {
                continue;
            };
            let routes: Vec<(usize, String, String, u64)> = self.with(|l| {
                l.graph["routes"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .enumerate()
                    .filter(|(_, r)| r["f"] == p.as_str())
                    .map(|(i, r)| {
                        (
                            i,
                            r["m"].as_str().unwrap_or("").to_string(),
                            r["p"].as_str().unwrap_or("").to_string(),
                            r["l"].as_u64().unwrap_or(0),
                        )
                    })
                    .collect()
            });
            let removed = removed_lines(patch);
            for (i, method, route, line) in routes {
                let touched = removed.iter().any(|(n, text)| {
                    *n == line || (!route.is_empty() && text.contains(route.as_str()))
                });
                if !touched {
                    continue;
                }
                let Some(v) = self.with(|l| am::impact(&l.graph, &format!("r:{i}"))) else {
                    continue;
                };
                let callers: Vec<String> = v["callers_elsewhere"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|c| {
                        format!(
                            "{} ({})",
                            c["at"].as_str().unwrap_or(""),
                            c["repo"].as_str().unwrap_or("")
                        )
                    })
                    .collect();
                if callers.is_empty() {
                    continue;
                }
                let screens = v["screens"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|s| {
                        s["file"]
                            .as_str()
                            .is_some_and(|f| !f.starts_with(&format!("{}/", self.folder)))
                    })
                    .map(|s| {
                        format!(
                            "{} in {} ({}) at {}",
                            s["name"].as_str().unwrap_or(""),
                            s["app"].as_str().unwrap_or(""),
                            s["gate"].as_str().unwrap_or("gate not known"),
                            s["at"].as_str().unwrap_or("")
                        )
                    })
                    .collect();
                out.push(EndpointChange {
                    method,
                    path: route,
                    file: p.clone(),
                    at: format!("{p}:{line}"),
                    callers,
                    screens,
                });
            }
        }
        out
    }
}

/// An endpoint a pull request changes that other repositories call.
#[derive(Debug, Clone, PartialEq)]
pub struct EndpointChange {
    pub method: String,
    pub path: String,
    /// The file on the map.
    pub file: String,
    /// Where it is defined.
    pub at: String,
    /// `file:line (repo)` of each caller in another repository.
    pub callers: Vec<String>,
    /// Screens in other repositories that stand on it.
    pub screens: Vec<String>,
}

/// The base-side line numbers and text of the lines a patch removes (a
/// rewritten line is removed and added).
pub fn removed_lines(patch: &str) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    let mut old: u64 = 0;
    for l in patch.lines() {
        if let Some(h) = l.strip_prefix("@@ -") {
            old = h
                .split([',', ' '])
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            continue;
        }
        if let Some(t) = l.strip_prefix('-') {
            out.push((old, t.to_string()));
            old += 1;
        } else if l.starts_with('+') {
            // added lines don't move the base side
        } else {
            old += 1;
        }
    }
    out
}

fn has_section(design: &str) -> bool {
    design
        .lines()
        .any(|l| l.trim().eq_ignore_ascii_case(AFFECTED_HEADING))
}

/// The `file:line` citations in a line of text.
pub fn cites_in(line: &str) -> Vec<String> {
    let re = regex::Regex::new(
        r"[A-Za-z0-9_@.\[\]()+-]+(?:/[A-Za-z0-9_@.\[\]()+-]+)*\.[A-Za-z0-9]+:\d+",
    )
    .expect("citation pattern");
    re.find_iter(line).map(|m| m.as_str().to_string()).collect()
}

/// [`RunMap::checked_design`] over a graph.
pub fn check_section(graph: &Value, repo: &str, design: &str) -> (String, usize) {
    if !has_section(design) {
        return (design.to_string(), 0);
    }
    let places = am::known_places(graph);
    let mut out = Vec::new();
    let mut inside = false;
    let mut dropped = 0;
    let mut skipping = false;
    for l in design.lines() {
        let t = l.trim();
        if t.starts_with("## ") {
            inside = t.eq_ignore_ascii_case(AFFECTED_HEADING);
            skipping = false;
        }
        if inside && (t.starts_with("- ") || t.starts_with("* ")) {
            let cites = cites_in(t);
            skipping = !cites.is_empty()
                && !cites
                    .iter()
                    .any(|c| am::real_cite_in(&places, graph, repo, c));
            if skipping {
                dropped += 1;
                continue;
            }
        } else if inside && skipping && l.starts_with(' ') {
            // the dropped bullet's own lines
            continue;
        } else {
            skipping = false;
        }
        out.push(l);
    }
    let mut s = out.join("\n");
    if design.ends_with('\n') {
        s.push('\n');
    }
    (s, dropped)
}

/// The paths of files a plan changes, with their repository: the
/// multi-repository groups when there are any, else the work's repository.
pub fn planned(
    default_repo: &str,
    tasks: &str,
    groups: &[(String, String)],
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut seen = BTreeSet::new();
    let mut add = |repo: &str, t: &str| {
        for f in super::feature::plan_files::planned_files(t) {
            if seen.insert((repo.to_string(), f.path.clone())) {
                out.push((repo.to_string(), f.path));
            }
        }
    };
    if groups.is_empty() {
        add(default_repo, tasks);
    } else {
        for (repo, t) in groups {
            add(repo, t);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn harbor() -> Value {
        let mut g: Value =
            serde_json::from_str(include_str!("../../floorplan/fixtures/harbor-graph.json"))
                .unwrap();
        am::stamp_repos(
            &mut g,
            &am::folders(&[
                "harbor/harbor-web".into(),
                "harbor/harbor-api".into(),
                "harbor/harbor-mobile".into(),
                "harbor/harbor-infra".into(),
            ]),
        );
        g
    }

    fn api() -> RunMap {
        RunMap::of(harbor(), "harbor/harbor-api").unwrap()
    }

    #[test]
    fn a_map_without_the_repository_offers_nothing() {
        assert!(RunMap::of(harbor(), "harbor/elsewhere").is_none());
        assert!(RunMap::of(json!({"schema": am::GRAPH_SCHEMA}), "harbor/harbor-api").is_none());
    }

    /// A journey traced through the code (a step the map has no node for,
    /// standing on its cited line) reaches the run's agents: the planner's
    /// map_impact lists it with its step's file:line, and the reviewer is
    /// asked to check it still works.
    #[test]
    fn the_planner_and_the_reviewer_get_the_traced_journeys_a_file_is_on() {
        let mut g = harbor();
        let store = json!({"flows": [{
            "id": "book-a-slip", "title": "Book a slip", "entry": "screen:web:/slips", "origin": "trace",
            "steps": [
                {"key": "screen:web:/slips", "kind": "screen", "role": "screen", "view": "f:harbor-web/app/slips/page.tsx", "note": "Pick a slip.",
                 "evidence": [{"file": "harbor-web/app/slips/page.tsx", "line": 1, "token": "Slips"}]},
                {"key": "code:harbor-api/src/bookings.js#book", "kind": "request", "role": "handler", "view": "f:harbor-api/src/bookings.js", "note": "The API books it.",
                 "evidence": [{"file": "harbor-api/src/bookings.js", "line": 3, "token": "book"}]},
                {"key": "n:stripe", "kind": "outside", "role": "outside", "view": "n:stripe", "note": "Stripe charges.",
                 "evidence": [{"file": "harbor-api/src/bookings.js", "line": 5, "token": "stripe"}]}
            ]
        }]});
        super::super::app_map_flows::put_in_graph(&mut g, &store);
        let m = RunMap::of(g, "harbor/harbor-api").unwrap();
        let hits = m.reaches("harbor/harbor-api", "src/bookings.js");
        let journey = hits
            .iter()
            .find(|h| h.starts_with("journey Book a slip"))
            .unwrap_or_else(|| panic!("{hits:?}"));
        assert!(
            journey.contains("harbor-api/src/bookings.js:3"),
            "{journey}"
        );
        let review = m.review_section(&[(
            "src/bookings.js".to_string(),
            "@@ -3 +3 @@\n-old\n+new".to_string(),
        )]);
        assert!(
            review.contains("User journeys this pull request changes a step of"),
            "{review}"
        );
        assert!(
            review.contains("journey Book a slip at harbor-api/src/bookings.js:3"),
            "{review}"
        );
    }

    /// The planner's overview: the repositories, the parts, where the
    /// ticket's files sit; capped.
    #[test]
    fn the_planner_overview_is_small_and_names_the_likely_parts() {
        let m = api();
        let s = m.planner_section(&["src/bookings.js".into(), "src/nowhere.js".into()]);
        assert!(s.contains("harbor/harbor-web (`harbor-web/`)"), "{s}");
        assert!(
            s.contains("- harbor-api `api` (service, Booking service)"),
            "{s}"
        );
        assert!(
            s.contains("The ticket's likely files sit in: `api`."),
            "{s}"
        );
        assert!(s.contains(AFFECTED_HEADING));
        assert!(s.len() <= OVERVIEW_CHARS);
        assert!(tokens(&s) <= 1_500);
        // A map with many parts stays within the cap and says how many more.
        let mut g = harbor();
        let many: Vec<Value> = (0..400)
            .map(|i| json!({"id": format!("p{i}"), "parent": null, "label": format!("Part {i}"), "kind": "module", "summary": "x".repeat(120)}))
            .collect();
        g["nodes"] = json!(many);
        let big = RunMap::of(g, "harbor/harbor-api")
            .unwrap()
            .planner_section(&[]);
        assert!(big.len() <= OVERVIEW_CHARS, "{}", big.len());
        assert!(big.contains("more (map_overview lists them)"));
    }

    /// The tools ride with the code graph's in every pass that reads code.
    #[test]
    fn the_lookups_answer_for_the_run() {
        let m = api();
        let names: Vec<String> = m.tools().into_iter().map(|t| t.name).collect();
        assert_eq!(names, am::TOOLS.to_vec());
        let out = m
            .handle("map_impact", &json!({"target": "POST /slips/:id/bookings"}))
            .unwrap();
        assert!(out.contains("harbor-web/lib/slips.ts:2"), "{out}");
        assert!(m.handle("read_file", &json!({})).is_none());
        assert!(m
            .prompt_line(super::super::code_graph::Role::Planner)
            .contains("`harbor-api/`"));
    }

    /// A plan that changes the booking endpoint's file without saying what
    /// it reaches goes back with what the map found; one that says so stays.
    #[test]
    fn a_plan_names_what_its_files_reach_elsewhere() {
        let m = api();
        let files = planned(
            "harbor/harbor-api",
            "- [ ] src/bookings.js: [modify] Take a promo code\n",
            &[],
        );
        assert_eq!(
            files,
            [(
                "harbor/harbor-api".to_string(),
                "src/bookings.js".to_string()
            )]
        );
        let gaps = m.plan_gaps(&files, "## Files\n- src/bookings.js\n");
        assert_eq!(gaps.len(), 1);
        assert!(gaps[0].contains("harbor-web/lib/slips.ts:2"), "{}", gaps[0]);
        assert!(gaps[0].contains("harbor-mobile/app/screens/SlipDetail.tsx:6"));
        assert!(m
            .plan_gaps(
                &files,
                &format!("{AFFECTED_HEADING}\n- harbor-web/lib/slips.ts:2\n")
            )
            .is_empty());
        // A file nothing else uses needs no section.
        let quiet = planned(
            "harbor/harbor-api",
            "- [ ] src/server.js: [modify] Log\n",
            &[],
        );
        assert!(m.plan_gaps(&quiet, "").is_empty());
    }

    /// Every citation in the section is a real place on the map: a bullet
    /// citing only places the map doesn't have is dropped.
    #[test]
    fn citations_in_the_section_must_be_real() {
        let m = api();
        let design = format!(
            "## Approach\n- src/x.ts:99 is fine here\n\n{AFFECTED_HEADING}\n\
             - harbor-web (Slip page) calls POST /slips/:id/bookings at harbor-web/lib/slips.ts:2\n\
             - harbor-mobile calls it at harbor-mobile/app/screens/SlipDetail.tsx:600\n  continued\n\
             - Stripe, reached through src/billing.js:1\n\
             - None found for the receipts\n\n## Risks\n- harbor-web/lib/slips.ts:999\n"
        );
        let (out, dropped) = m.checked_design(&design);
        assert_eq!(dropped, 1);
        assert!(out.contains("harbor-web/lib/slips.ts:2"));
        assert!(!out.contains("SlipDetail.tsx:600"));
        assert!(!out.contains("  continued"));
        assert!(out.contains("src/billing.js:1"));
        assert!(out.contains("None found for the receipts"));
        // Outside the section nothing is touched.
        assert!(out.contains("src/x.ts:99") && out.contains("slips.ts:999"));
    }

    /// Review: a diff that rewrites the booking endpoint, which the web app
    /// and the mobile app call, is flagged with both callers.
    #[test]
    fn review_flags_an_endpoint_change_with_external_callers() {
        let m = api();
        let patch = "@@ -4,3 +4,3 @@\n const router = express.Router();\n-router.post('/:id/bookings', async (req, res) => {\n+router.post('/:id/reservations', async (req, res) => {\n   await charge(req.params.id);\n";
        let changed = vec![("src/bookings.js".to_string(), patch.to_string())];
        let flagged = m.endpoint_changes(&changed);
        assert_eq!(flagged.len(), 1);
        assert_eq!(flagged[0].method, "POST");
        assert_eq!(flagged[0].at, "harbor-api/src/bookings.js:5");
        assert_eq!(
            flagged[0].callers,
            [
                "harbor-mobile/app/screens/SlipDetail.tsx:6 (harbor-mobile)",
                "harbor-web/lib/slips.ts:2 (harbor-web)"
            ]
        );
        assert!(flagged[0]
            .screens
            .iter()
            .any(|s| s.contains("SlipDetail in harbor-mobile (signed_in)")));
        let section = m.review_section(&changed);
        assert!(section.contains("## Cross-repository impact (from the app map)"));
        assert!(section.contains("raise a finding (a breaking change)"));
        assert!(section.contains("called from harbor-web/lib/slips.ts:2 (harbor-web)"));
        assert!(section.len() <= REVIEW_CHARS);
        // A change inside the handler that keeps the endpoint's line is not
        // flagged as an endpoint change; the file is still named as shared.
        let body = "@@ -7 +7 @@\n-  res.json({ ok: true });\n+  res.json({ ok: true, id });\n";
        let inside = vec![("src/bookings.js".to_string(), body.to_string())];
        assert!(m.endpoint_changes(&inside).is_empty());
        assert!(m
            .review_section(&inside)
            .contains("src/bookings.js is called from other repositories"));
        // A file nobody else calls: nothing to say.
        let quiet = vec![("src/server.js".to_string(), body.to_string())];
        assert_eq!(m.review_section(&quiet), "");
    }

    /// The registry every pass's executor asks: the map's lookups ride with
    /// the graph's for the planner, the coder (CI fixes, test fixes,
    /// incident and alarm fix runs go through it) and the reviewer, and the
    /// map's lookups are offered even where the code graph is off.
    #[test]
    fn the_registry_offers_the_map_in_every_pass_that_reads_code() {
        use super::super::code_graph::{offer, Avail, RepoGraphs, Role, TOOLS as GRAPH};
        for role in [Role::Planner, Role::Coder, Role::Reviewer] {
            let graphs = RepoGraphs::with(
                "t",
                1,
                "harbor/harbor-api",
                Avail::Building(common::code_graph::Shown::Queued),
                Vec::new(),
                "run",
                0,
            )
            .with_map(Some(api()));
            let mut tools = Vec::new();
            let mut system = String::new();
            assert!(offer(&graphs, role, &mut tools, &mut system));
            for name in GRAPH.iter().chain(am::TOOLS.iter()) {
                assert!(tools.iter().any(|t| t.name == *name), "{role:?} {name}");
            }
            assert!(
                system.contains("APP MAP") && system.contains("map_impact"),
                "{role:?}"
            );
        }
        // The graph off for the repository: only the map's lookups, and no
        // word about a code graph.
        let off = RepoGraphs::with(
            "t",
            1,
            "harbor/harbor-api",
            Avail::Off,
            Vec::new(),
            "coder",
            0,
        )
        .with_map(Some(api()));
        let mut tools = Vec::new();
        let mut system = String::new();
        assert!(offer(&off, Role::Coder, &mut tools, &mut system));
        assert_eq!(
            tools.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            am::TOOLS.to_vec()
        );
        assert!(!system.contains("CODE GRAPH"));
        // No map: nothing about one.
        let none = RepoGraphs::with(
            "t",
            1,
            "harbor/harbor-api",
            Avail::Off,
            Vec::new(),
            "coder",
            0,
        );
        assert!(none.tools().is_empty() && none.map().is_none());
        // Each pass reads the map for its step.
        for (file, src) in [
            ("plan.rs", include_str!("plan.rs")),
            ("implement.rs", include_str!("implement.rs")),
            ("review_pr.rs", include_str!("review_pr.rs")),
        ] {
            assert!(src.contains(".load_map(state)"), "{file}");
        }
        assert_eq!(
            include_str!("review_pr.rs")
                .matches(".load_map(state)")
                .count(),
            2
        );
    }

    /// The planner's first message carries the overview when there is a
    /// map, and its design is checked before it is kept.
    #[test]
    fn the_planner_reads_the_overview_and_its_citations_are_checked() {
        let src = include_str!("plan.rs");
        let run = &src[src.find("pub async fn run(").unwrap()..];
        let overview = run.find("m.planner_section(&likely)").unwrap();
        let first_message = run.find("let mut content_blocks = vec![").unwrap();
        assert!(overview < first_message);
        assert!(run.contains("m.plan_gaps("));
        let checked = run.find("m.checked_design(&files.design)").unwrap();
        let written = run.find("(\"design.md\", &files.design)").unwrap();
        assert!(checked < written);
        // The review's impact check is in front of the reviewer.
        assert!(include_str!("review_pr.rs").contains("m.review_section(&changed)"));
    }

    #[test]
    fn removed_lines_carry_the_base_side_numbers() {
        let p = "@@ -5,3 +5,3 @@\n a\n-b\n+c\n d\n@@ -20 +20 @@\n-x\n";
        assert_eq!(
            removed_lines(p),
            [(6, "b".to_string()), (20, "x".to_string())]
        );
    }
}
