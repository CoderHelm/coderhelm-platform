//! The one AI read per repo: the facts, the last merged pull requests and
//! the current settings in; one JSON object of recommended values out, each
//! with the evidence it is based on.
//!
//! The answer is never trusted as it comes: [`parse`] checks every field's
//! type and range, keeps only evidence that points at files the repo has
//! and at its own GitHub pages, and drops any field left without evidence.
//! Garbage in gives an empty read; every setting then falls back to what
//! the detectors found or to CoderHelm's default.

use std::collections::HashMap;

use serde_json::{json, Value};

use super::facts::RepoFacts;
use super::registry::Current;
use super::Evidence;

/// The prompt is cut to this many characters.
pub const MAX_PROMPT_CHARS: usize = 48_000;
/// The answer's token cap.
pub const MAX_OUTPUT_TOKENS: i32 = 4_000;
const MAX_EVIDENCE: usize = 6;

/// [`SYSTEM`] with the shared rule for the README, AGENTS.md and CLAUDE.md
/// setup notes the coder's instructions are drafted from: the build and test
/// commands are kept, a person's laptop setup is not.
pub fn system(caps: &[crate::host_steps::Capability]) -> String {
    format!(
        "{SYSTEM}{}",
        crate::host_steps::section(
            &[
                crate::host_steps::Reader::SetupDocs,
                crate::host_steps::Reader::RepoGuidance,
            ],
            caps,
        )
    )
}

pub const SYSTEM: &str = "You recommend settings for CoderHelm, an AI teammate that reviews pull requests and writes code, for one GitHub repository. \
You get facts read from the repository, its last merged pull requests, and the current settings. \
For each field, give a value and the evidence it is based on: short sentences that point at a file in the repository (path, and line when you know it) or a pull request (its URL). \
Only use evidence from what you were given. When the facts don't support a value, return an empty evidence list for that field: that field is then left to CoderHelm's default. \
Fields: \
review_focus: what reviews of this repo should look at first, as short instructions (plain text, at most 2000 characters), from the repo's review guidelines and the review comments people repeat. \
require_tests: whether a pull request that changes code must change tests, from how often merged pull requests changed tests. \
coder_instructions: a draft of the coder's instructions for this repo (commands to build and test, conventions, what to avoid), at most 4000 characters, from the README, AGENTS.md or CLAUDE.md, the manifests and the workflows. \
ci_labels_allow: the repository labels the reviewer may add to start CI jobs, only labels that exist and that workflows run on, never deploy, staging or production labels. \
stale_days: days without activity before a pull request counts as stale, from how long pull requests here usually take to merge (between 3 and 90). \
auto_merge_fits: whether merging a pull request once the reviewer approves it and every required check passes fits this repo, from its branch protection and how its pull requests merge.";

fn ev_schema() -> Value {
    json!({
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "text": { "type": "string" },
                "file": { "type": "string" },
                "line": { "type": "integer" },
                "url": { "type": "string" }
            },
            "required": ["text", "file", "line", "url"],
            "additionalProperties": false
        }
    })
}

fn field(value: Value) -> Value {
    json!({
        "type": "object",
        "properties": { "value": value, "evidence": ev_schema() },
        "required": ["value", "evidence"],
        "additionalProperties": false
    })
}

/// The answer's shape. `file`, `line` and `url` are empty or 0 when unknown.
pub fn output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "review_focus": field(json!({"type": "string"})),
            "require_tests": field(json!({"type": "boolean"})),
            "coder_instructions": field(json!({"type": "string"})),
            "ci_labels_allow": field(json!({"type": "array", "items": {"type": "string"}})),
            "stale_days": field(json!({"type": "integer"})),
            "auto_merge_fits": field(json!({"type": "boolean"}))
        },
        "required": ["review_focus", "require_tests", "coder_instructions", "ci_labels_allow", "stale_days", "auto_merge_fits"],
        "additionalProperties": false
    })
}

/// The answer's fields and the setting each one fills.
pub const FIELDS: &[(&str, &str)] = &[
    ("review_focus", "reviewer.instructions"),
    ("require_tests", "tests.require_tests"),
    ("coder_instructions", "instructions.repo"),
    ("ci_labels_allow", "ci_labels.allow"),
    ("stale_days", "stale_prs.stale_days"),
    ("auto_merge_fits", "auto_merge.enabled"),
];

/// Whether a run over these settings needs the AI read at all.
pub fn needed(setting_ids: &[&str]) -> bool {
    FIELDS.iter().any(|(_, id)| setting_ids.contains(id))
}

fn cut(s: &str, n: usize) -> String {
    crate::truncate_str(s, n).to_string()
}

/// The user message: facts first, then the pull requests, then the current
/// settings, cut to [`MAX_PROMPT_CHARS`].
pub fn prompt(f: &RepoFacts, current: &HashMap<&'static str, Current>) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Repository: {} (default branch {}, {})\n",
        f.repo, f.default_branch, f.html_url
    ));
    out.push_str(&format!(
        "Required checks on {}: {}\n",
        f.default_branch,
        if f.required_checks.is_empty() {
            if f.protection_read {
                "none".to_string()
            } else {
                "unknown".to_string()
            }
        } else {
            f.required_checks.join(", ")
        }
    ));
    out.push_str(&format!("Labels: {}\n", f.labels.join(", ")));
    if let Some(h) = f.median_hours_to_merge() {
        out.push_str(&format!("Median time to merge: {:.0} hours\n", h));
    }
    let with_tests = f
        .merged
        .iter()
        .filter(|p| p.files_read && p.tests_changed())
        .count();
    let read = f.merged.iter().filter(|p| p.files_read).count();
    out.push_str(&format!(
        "Merged pull requests that changed tests: {with_tests} of {read}\n"
    ));
    out.push_str("\nFiles:\n");
    for p in f.paths.iter().take(400) {
        out.push_str(p);
        out.push('\n');
    }
    for (path, text) in &f.files {
        out.push_str(&format!("\n--- {path} ---\n{}\n", cut(text, 4_000)));
    }
    for (path, text) in f.workflows.iter().take(8) {
        out.push_str(&format!("\n--- {path} ---\n{}\n", cut(text, 2_000)));
    }
    out.push_str("\nLast merged pull requests:\n");
    for p in &f.merged {
        out.push_str(&format!(
            "- #{} {} ({}) labels [{}] {}{}\n",
            p.number,
            cut(&p.title, 120),
            p.url,
            p.labels.join(", "),
            p.hours_to_merge
                .map(|h| format!("merged after {h:.0}h"))
                .unwrap_or_default(),
            if p.files_read {
                format!(
                    "; {} files{}",
                    p.files.len(),
                    if p.tests_changed() {
                        ", tests changed"
                    } else {
                        ""
                    }
                )
            } else {
                String::new()
            }
        ));
        for c in p.review_comments.iter().take(5) {
            out.push_str(&format!("    review: {}\n", cut(c, 200)));
        }
    }
    out.push_str("\nCurrent settings:\n");
    let mut ids: Vec<&&str> = current.keys().collect();
    ids.sort();
    for id in ids {
        let c = &current[*id];
        out.push_str(&format!(
            "{id} = {}{}\n",
            cut(&c.value.to_string(), 300),
            if c.stored { "" } else { " (default)" }
        ));
    }
    cut(&out, MAX_PROMPT_CHARS)
}

/// One checked recommendation from the read.
#[derive(Debug, Clone, PartialEq)]
pub struct Inferred {
    pub value: Value,
    pub evidence: Vec<Evidence>,
}

/// Evidence that points only at what the repo has: a listed file (else the
/// file is dropped, and the line with it), a URL under the repo's own
/// GitHub page (else dropped). A line with no text is dropped.
fn evidence(raw: &Value, f: &RepoFacts) -> Vec<Evidence> {
    let mut out = Vec::new();
    for e in raw.as_array().into_iter().flatten().take(MAX_EVIDENCE * 2) {
        let text = e["text"].as_str().unwrap_or("").trim();
        if text.is_empty() {
            continue;
        }
        let file = e["file"]
            .as_str()
            .map(str::trim)
            .filter(|p| !p.is_empty() && f.has_path(p))
            .map(str::to_string);
        let line = file
            .as_ref()
            .and(e["line"].as_u64())
            .filter(|n| (1..=1_000_000).contains(n));
        let url = e["url"]
            .as_str()
            .map(str::trim)
            .filter(|u| {
                !f.html_url.is_empty()
                    && (u.starts_with(&format!("{}/", f.html_url)) || *u == f.html_url)
            })
            .map(str::to_string);
        out.push(Evidence {
            text: cut(text, 300),
            file,
            line,
            url,
        });
        if out.len() == MAX_EVIDENCE {
            break;
        }
    }
    out
}

/// The read, checked. Keyed by setting id.
pub fn parse(answer: &Value, f: &RepoFacts) -> HashMap<&'static str, Inferred> {
    let mut out = HashMap::new();
    if !answer.is_object() {
        return out;
    }
    for (key, id) in FIELDS {
        let raw = &answer[*key];
        let ev = evidence(&raw["evidence"], f);
        if ev.is_empty() {
            continue;
        }
        let v = &raw["value"];
        let value = match *key {
            "review_focus" | "coder_instructions" => {
                let max = if *key == "review_focus" { 2_000 } else { 4_000 };
                match v.as_str().map(str::trim) {
                    Some(t) if !t.is_empty() => {
                        let t: String = t
                            .chars()
                            .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
                            .collect();
                        json!(cut(&t, max))
                    }
                    _ => continue,
                }
            }
            "require_tests" => match v.as_bool() {
                Some(b) => json!(b),
                None => continue,
            },
            "auto_merge_fits" => match v.as_bool() {
                // Only where branch protection requires checks.
                Some(true) if f.required_checks.is_empty() => continue,
                Some(b) => json!(b),
                None => continue,
            },
            "stale_days" => match v.as_i64() {
                Some(n) if (3..=90).contains(&n) => json!(n),
                _ => continue,
            },
            "ci_labels_allow" => {
                let Some(list) = v.as_array() else { continue };
                let mut keep: Vec<String> = Vec::new();
                for l in list.iter().filter_map(|x| x.as_str()) {
                    // Only labels the repo has, never a deploy label.
                    if let Some(real) = f.labels.iter().find(|x| x.eq_ignore_ascii_case(l.trim())) {
                        if !crate::e2e::workflow_gate::is_deploy_label(real)
                            && !keep.iter().any(|k| k == real)
                        {
                            keep.push(real.clone());
                        }
                    }
                }
                if keep.is_empty() {
                    continue;
                }
                json!(keep.join(", "))
            }
            _ => continue,
        };
        out.insert(
            *id,
            Inferred {
                value,
                evidence: ev,
            },
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::facts::tests::web_app;

    fn good() -> Value {
        json!({
            "review_focus": {"value": "Check SQL migrations.", "evidence": [
                {"text": "Reviewers ask for migration notes", "file": "", "line": 0,
                 "url": "https://github.com/acme/web/pull/3"}]},
            "require_tests": {"value": true, "evidence": [
                {"text": "6 of 6 merged pull requests changed tests", "file": "src/__tests__/app.test.ts", "line": 4, "url": ""}]},
            "coder_instructions": {"value": "Run npm test.", "evidence": [
                {"text": "package.json test script", "file": "package.json", "line": 1, "url": ""}]},
            "ci_labels_allow": {"value": ["E2E:IOS", "deploy-staging", "made-up"], "evidence": [
                {"text": "e2e.yml runs on E2E:IOS", "file": ".github/workflows/e2e.yml", "line": 0, "url": ""}]},
            "stale_days": {"value": 14, "evidence": [{"text": "Median merge time is 1 day", "file": "", "line": 0, "url": ""}]},
            "auto_merge_fits": {"value": true, "evidence": [{"text": "build and e2e are required", "file": "", "line": 0, "url": ""}]}
        })
    }

    #[test]
    fn a_good_answer_is_kept_and_cleaned() {
        let r = parse(&good(), &web_app());
        assert_eq!(r.len(), 6);
        assert_eq!(r["tests.require_tests"].value, true);
        assert_eq!(r["tests.require_tests"].evidence[0].line, Some(4));
        // Deploy labels and labels the repo doesn't have are dropped.
        assert_eq!(r["ci_labels.allow"].value, "E2E:IOS");
        assert_eq!(r["stale_prs.stale_days"].value, 14);
        assert_eq!(
            r["reviewer.instructions"].evidence[0].url.as_deref(),
            Some("https://github.com/acme/web/pull/3")
        );
    }

    #[test]
    fn garbage_gives_an_empty_read() {
        let f = web_app();
        for g in [
            json!(null),
            json!("text"),
            json!([1, 2]),
            json!({"review_focus": 7}),
            json!({"require_tests": {"value": "yes", "evidence": [{"text": "x"}]}}),
            json!({"stale_days": {"value": 900, "evidence": [{"text": "x"}]}}),
            json!({"coder_instructions": {"value": "   ", "evidence": [{"text": "x"}]}}),
        ] {
            assert!(parse(&g, &f).is_empty(), "{g}");
        }
    }

    #[test]
    fn a_value_without_evidence_is_dropped_and_invented_sources_too() {
        let f = web_app();
        let mut a = good();
        a["require_tests"]["evidence"] = json!([]);
        // Evidence naming a file the repo doesn't have keeps its text but
        // loses the file; a URL outside the repo is dropped.
        a["review_focus"]["evidence"] = json!([{"text": "From the guide", "file": "nope.md",
            "line": 9, "url": "https://evil.example/x"}]);
        // Blank evidence text is no evidence.
        a["coder_instructions"]["evidence"] =
            json!([{"text": "  ", "file": "package.json", "line": 1, "url": ""}]);
        let r = parse(&a, &f);
        assert!(!r.contains_key("tests.require_tests"));
        assert!(!r.contains_key("instructions.repo"));
        let ev = &r["reviewer.instructions"].evidence[0];
        assert_eq!(
            (ev.file.as_deref(), ev.line, ev.url.as_deref()),
            (None, None, None)
        );
    }

    #[test]
    fn auto_merge_is_never_inferred_without_required_checks() {
        let mut f = web_app();
        f.required_checks.clear();
        assert!(!parse(&good(), &f).contains_key("auto_merge.enabled"));
    }

    #[test]
    fn the_prompt_is_bounded() {
        let mut f = web_app();
        f.paths = (0..10_000).map(|i| format!("src/file_{i}.ts")).collect();
        f.files.insert("README.md".into(), "x".repeat(100_000));
        let p = prompt(&f, &HashMap::new());
        assert!(p.len() <= MAX_PROMPT_CHARS);
        assert!(p.contains("Required checks on main: build, e2e"));
    }
}
