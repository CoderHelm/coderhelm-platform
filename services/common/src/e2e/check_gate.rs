//! Whether a test fix's pull request may leave draft: the repository's
//! required checks and its test-related checks (end-to-end, deploy preview,
//! integration) on the head commit, read from GitHub.
//!
//! A deploy-preview or end-to-end check that was skipped or neutral on the
//! head commit tested nothing, so it is started again; until every one of
//! those checks is green on the head (and the repeat run passed), the pull
//! request stays a draft.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One check on the head commit (the newest of each name).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Check {
    pub name: String,
    /// `queued` | `in_progress` | `completed`
    pub status: String,
    /// `success` | `failure` | `skipped` | `neutral` | …; empty until done.
    pub conclusion: String,
    /// The GitHub Actions workflow run it belongs to, if any.
    pub run_id: Option<u64>,
}

fn run_id_of(c: &Value) -> Option<u64> {
    ["details_url", "html_url"].iter().find_map(|k| {
        let url = c[*k].as_str()?;
        let rest = url.split("/actions/runs/").nth(1)?;
        rest.split(['/', '?', '#']).next()?.parse().ok()
    })
}

/// The newest check of each name in a `check-runs` listing, plus legacy
/// commit statuses (`statuses` of the combined status).
pub fn checks(check_runs: &[Value], statuses: &[Value]) -> Vec<Check> {
    let mut out: Vec<(String, Check)> = Vec::new();
    for c in check_runs {
        let name = c["name"].as_str().unwrap_or("").to_string();
        if name.is_empty() {
            continue;
        }
        let started = c["started_at"].as_str().unwrap_or("").to_string();
        let check = Check {
            name: name.clone(),
            status: c["status"].as_str().unwrap_or("").to_string(),
            conclusion: c["conclusion"].as_str().unwrap_or("").to_string(),
            run_id: run_id_of(c),
        };
        match out.iter_mut().find(|(_, x)| x.name == name) {
            Some((at, x)) if started >= *at => {
                *at = started;
                *x = check;
            }
            Some(_) => {}
            None => out.push((started, check)),
        }
    }
    for s in statuses {
        let name = s["context"].as_str().unwrap_or("").to_string();
        if name.is_empty() || out.iter().any(|(_, x)| x.name == name) {
            continue;
        }
        let state = s["state"].as_str().unwrap_or("");
        let (status, conclusion) = match state {
            "pending" => ("in_progress", ""),
            "success" => ("completed", "success"),
            _ => ("completed", "failure"),
        };
        out.push((
            String::new(),
            Check {
                name,
                status: status.into(),
                conclusion: conclusion.into(),
                run_id: None,
            },
        ));
    }
    out.into_iter().map(|(_, c)| c).collect()
}

/// Required checks from GitHub's rules for a branch
/// (`GET /repos/{o}/{r}/rules/branches/{b}`) and from classic branch
/// protection (`…/branches/{b}/protection/required_status_checks`).
pub fn required(rules: Option<&Value>, protection: Option<&Value>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for r in rules.and_then(Value::as_array).into_iter().flatten() {
        if r["type"].as_str() != Some("required_status_checks") {
            continue;
        }
        for c in r["parameters"]["required_status_checks"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if let Some(n) = c["context"].as_str() {
                out.push(n.to_string());
            }
        }
    }
    if let Some(p) = protection {
        for n in p["contexts"].as_array().into_iter().flatten() {
            if let Some(n) = n.as_str() {
                out.push(n.to_string());
            }
        }
        for c in p["checks"].as_array().into_iter().flatten() {
            if let Some(n) = c["context"].as_str() {
                out.push(n.to_string());
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Whether a check's name says it tests the running app or deploys it for
/// testing: end-to-end, browser, preview, integration and smoke checks.
pub fn is_test_related(name: &str) -> bool {
    let n = name.to_lowercase().replace(['_', '-'], " ");
    let words: Vec<&str> = n.split(|c: char| !c.is_ascii_alphanumeric()).collect();
    let has = |w: &str| words.contains(&w);
    has("e2e")
        || n.contains("end to end")
        || has("playwright")
        || has("cypress")
        || has("selenium")
        || has("webdriver")
        || has("wdio")
        || has("preview")
        || has("integration")
        || has("acceptance")
        || has("smoke")
        || (has("ui") && (has("test") || has("tests")))
        || (n.contains("deploy") && (has("preview") || has("staging") || has("pr") || has("test")))
}

/// Where the gated checks stand on the head commit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Standing {
    pub green: Vec<String>,
    pub waiting: Vec<String>,
    pub failed: Vec<String>,
    /// Skipped or neutral: they ran nothing.
    pub skipped: Vec<String>,
    /// Workflow runs of skipped checks, to start again.
    pub rerun: Vec<u64>,
    /// Required checks that haven't reported on the head at all.
    pub missing: Vec<String>,
}

impl Standing {
    pub fn ready(&self) -> bool {
        self.waiting.is_empty()
            && self.failed.is_empty()
            && self.skipped.is_empty()
            && self.missing.is_empty()
    }

    /// Why the pull request stays a draft, in one sentence; empty when ready.
    pub fn why_not(&self) -> String {
        let mut parts = Vec::new();
        let list = |v: &[String]| {
            v.iter()
                .map(|n| format!("`{n}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        if !self.failed.is_empty() {
            parts.push(format!("failing: {}", list(&self.failed)));
        }
        if !self.skipped.is_empty() {
            parts.push(format!("skipped on this commit: {}", list(&self.skipped)));
        }
        if !self.waiting.is_empty() {
            parts.push(format!("still running: {}", list(&self.waiting)));
        }
        if !self.missing.is_empty() {
            parts.push(format!(
                "required but not reported: {}",
                list(&self.missing)
            ));
        }
        parts.join("; ")
    }
}

/// The standing of the required and test-related checks.
pub fn standing(checks: &[Check], required: &[String]) -> Standing {
    let mut s = Standing::default();
    for c in checks {
        let gated = required.iter().any(|r| r == &c.name) || is_test_related(&c.name);
        if !gated {
            continue;
        }
        if c.status != "completed" {
            s.waiting.push(c.name.clone());
            continue;
        }
        match c.conclusion.as_str() {
            "success" => s.green.push(c.name.clone()),
            "skipped" | "neutral" | "stale" => {
                s.skipped.push(c.name.clone());
                if let Some(id) = c.run_id {
                    if !s.rerun.contains(&id) {
                        s.rerun.push(id);
                    }
                }
            }
            _ => s.failed.push(c.name.clone()),
        }
    }
    for r in required {
        if !checks.iter().any(|c| &c.name == r) {
            s.missing.push(r.clone());
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cr(name: &str, status: &str, conclusion: Option<&str>, run: u64, at: &str) -> Value {
        json!({
            "name": name, "status": status, "conclusion": conclusion,
            "started_at": at,
            "details_url": format!("https://github.com/acme/web/actions/runs/{run}/job/1"),
        })
    }

    #[test]
    fn the_newest_check_of_each_name_counts() {
        let c = checks(
            &[
                cr(
                    "e2e",
                    "completed",
                    Some("failure"),
                    1,
                    "2026-10-01T00:00:00Z",
                ),
                cr(
                    "e2e",
                    "completed",
                    Some("success"),
                    2,
                    "2026-10-02T00:00:00Z",
                ),
            ],
            &[json!({"context": "ci/preview", "state": "pending"})],
        );
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].conclusion, "success");
        assert_eq!(c[0].run_id, Some(2));
        assert_eq!(c[1].status, "in_progress");
    }

    #[test]
    fn test_related_checks_are_named_generically() {
        for yes in [
            "e2e",
            "E2E tests (chromium)",
            "Playwright",
            "deploy-preview",
            "Deploy Preview",
            "integration-tests",
            "smoke",
            "UI tests",
            "end-to-end",
        ] {
            assert!(is_test_related(yes), "{yes}");
        }
        for no in [
            "lint",
            "build",
            "typecheck",
            "unit",
            "CodeQL",
            "deploy production",
        ] {
            assert!(!is_test_related(no), "{no}");
        }
    }

    #[test]
    fn a_skipped_preview_holds_the_pr_and_is_started_again() {
        let c = checks(
            &[
                cr("build", "completed", Some("success"), 5, "t"),
                cr("deploy-preview", "completed", Some("skipped"), 6, "t"),
                cr("e2e", "completed", Some("neutral"), 6, "t"),
            ],
            &[],
        );
        let s = standing(&c, &["build".into()]);
        assert!(!s.ready());
        assert_eq!(s.skipped, vec!["deploy-preview", "e2e"]);
        assert_eq!(s.rerun, vec![6]);
        assert!(s.why_not().contains("skipped on this commit"));
    }

    #[test]
    fn required_checks_must_report_and_pass() {
        let rules = json!([{ "type": "required_status_checks", "parameters": { "required_status_checks": [{ "context": "ci" }] } }, { "type": "pull_request" }]);
        let prot = json!({ "contexts": ["ci", "e2e"], "checks": [{ "context": "e2e" }] });
        let req = required(Some(&rules), Some(&prot));
        assert_eq!(req, vec!["ci", "e2e"]);
        let c = checks(&[cr("ci", "completed", Some("success"), 1, "t")], &[]);
        let s = standing(&c, &req);
        assert_eq!(s.missing, vec!["e2e"]);
        assert!(!s.ready());
        let all = checks(
            &[
                cr("ci", "completed", Some("success"), 1, "t"),
                cr("e2e", "completed", Some("success"), 2, "t"),
                cr("lint", "completed", Some("failure"), 3, "t"),
            ],
            &[],
        );
        // A failing check that is neither required nor test-related is the
        // CI-fix loop's, not this gate's.
        assert!(standing(&all, &req).ready());
        let red = checks(&[cr("e2e", "completed", Some("failure"), 2, "t")], &[]);
        assert_eq!(standing(&red, &[]).failed, vec!["e2e"]);
        let running = checks(&[cr("e2e", "in_progress", None, 2, "t")], &[]);
        assert_eq!(standing(&running, &[]).waiting, vec!["e2e"]);
    }
}
