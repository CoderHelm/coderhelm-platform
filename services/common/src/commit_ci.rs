//! A commit's CI as one listing: its check runs and its commit statuses.
//!
//! Many repositories report CI through commit statuses rather than check
//! runs (external CI servers, preview deploy bots, some deploy tools). A
//! decision that read only check runs called such a commit green with a red
//! status on it, or "no CI" with a status still pending. [`merge`] folds the
//! combined status's `statuses` into a `check-runs` listing, each one shaped
//! as a check run, so every reader of a listing sees both.

use serde_json::{json, Value};

/// Marks a check run in a merged listing that is a commit status.
pub const STATUS_SOURCE: &str = "commit_status";

/// A commit status as a check run: its context is the name, `pending` is
/// in progress, `success` succeeded, `failure` and `error` failed. It has no
/// id (it can't be re-run or annotated); its link is the status's target.
pub fn status_as_check_run(s: &Value) -> Option<Value> {
    let name = s["context"].as_str().filter(|n| !n.is_empty())?;
    let (status, conclusion) = match s["state"].as_str().unwrap_or("") {
        "pending" => ("in_progress", Value::Null),
        "success" => ("completed", json!("success")),
        _ => ("completed", json!("failure")),
    };
    let url = s["target_url"].clone();
    let creator = s["creator"]["login"].as_str().unwrap_or("");
    let completed = if status == "completed" {
        s["updated_at"].clone()
    } else {
        Value::Null
    };
    Some(json!({
        "name": name,
        "status": status,
        "conclusion": conclusion,
        "details_url": url,
        "html_url": url,
        "started_at": s["created_at"],
        "completed_at": completed,
        "app": {"slug": creator.trim_end_matches("[bot]")},
        "output": {"title": s["description"], "summary": s["description"]},
        "source": STATUS_SOURCE,
    }))
}

/// `check_runs` (a `check-runs` listing) with the statuses of `combined`
/// (`GET /commits/{ref}/status`) added. A status named like a check run is
/// the same check reported twice and is left out. `total_count` counts both.
pub fn merge(check_runs: Value, combined: Option<&Value>) -> Value {
    let mut out = if check_runs.is_object() {
        check_runs
    } else {
        json!({"total_count": 0, "check_runs": []})
    };
    let mut runs = out["check_runs"].as_array().cloned().unwrap_or_default();
    let before = runs.len();
    for s in combined
        .and_then(|c| c["statuses"].as_array())
        .into_iter()
        .flatten()
    {
        let Some(run) = status_as_check_run(s) else {
            continue;
        };
        if runs.iter().any(|r| r["name"] == run["name"]) {
            continue;
        }
        runs.push(run);
    }
    let added = (runs.len() - before) as u64;
    let total = out["total_count"].as_u64().unwrap_or(before as u64) + added;
    out["check_runs"] = Value::Array(runs);
    out["total_count"] = json!(total);
    out
}

/// The GitHub Actions workflow run a check run belongs to, from its link.
/// None for a commit status and for any check outside Actions, whose links
/// may look alike (`…/runs/123` on another CI) but name nothing GitHub can
/// give logs for.
pub fn actions_run_id(run: &Value) -> Option<u64> {
    if is_status(run) {
        return None;
    }
    ["details_url", "html_url"].iter().find_map(|k| {
        let url = run[*k].as_str()?;
        let (_, rest) = url.split_once("/actions/runs/")?;
        rest.split(['/', '?', '#']).next()?.parse().ok()
    })
}

/// Whether a merged listing's check run is a commit status.
pub fn is_status(run: &Value) -> bool {
    run["source"].as_str() == Some(STATUS_SOURCE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(context: &str, state: &str) -> Value {
        json!({
            "context": context, "state": state,
            "target_url": format!("https://ci.example.com/{context}"),
            "description": "build", "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:05:00Z",
            "creator": {"login": "ci-bot[bot]"}
        })
    }

    #[test]
    fn statuses_read_as_check_runs() {
        let v = merge(
            json!({"total_count": 1, "check_runs": [
                {"name": "lint", "status": "completed", "conclusion": "success"}
            ]}),
            Some(&json!({"state": "failure", "statuses": [
                st("ci/build", "failure"), st("preview", "pending"),
                st("ci/test", "success"), st("ci/other", "error")
            ]})),
        );
        assert_eq!(v["total_count"], 5);
        let runs = v["check_runs"].as_array().unwrap();
        let by = |n: &str| runs.iter().find(|r| r["name"] == n).unwrap().clone();
        assert_eq!(by("ci/build")["conclusion"], "failure");
        assert_eq!(by("ci/other")["conclusion"], "failure");
        assert_eq!(by("preview")["status"], "in_progress");
        assert!(by("preview")["conclusion"].is_null());
        assert_eq!(by("ci/test")["conclusion"], "success");
        assert_eq!(
            by("ci/build")["details_url"],
            "https://ci.example.com/ci/build"
        );
        assert_eq!(by("ci/build")["app"]["slug"], "ci-bot");
        assert!(is_status(&by("ci/build")));
        assert!(!is_status(&by("lint")));
        assert!(by("ci/build")["id"].is_null());
    }

    #[test]
    fn a_status_named_like_a_check_run_is_one_check() {
        let v = merge(
            json!({"total_count": 1, "check_runs": [
                {"name": "build", "status": "completed", "conclusion": "success"}
            ]}),
            Some(&json!({"statuses": [st("build", "failure")]})),
        );
        assert_eq!(v["total_count"], 1);
        assert_eq!(v["check_runs"][0]["conclusion"], "success");
    }

    #[test]
    fn no_statuses_leaves_the_listing_as_it_was() {
        let runs = json!({"total_count": 2, "check_runs": [{"name": "a"}, {"name": "b"}]});
        assert_eq!(merge(runs.clone(), None), runs);
        assert_eq!(merge(runs.clone(), Some(&json!({"statuses": []}))), runs);
    }

    #[test]
    fn statuses_alone_are_ci() {
        let v = merge(
            Value::Null,
            Some(&json!({"statuses": [st("jenkins", "success")]})),
        );
        assert_eq!(v["total_count"], 1);
        assert_eq!(v["check_runs"][0]["status"], "completed");
    }

    #[test]
    fn only_an_actions_link_names_a_workflow_run() {
        let run = |url: &str| json!({"details_url": url});
        assert_eq!(
            actions_run_id(&run("https://github.com/o/r/actions/runs/42/job/7")),
            Some(42)
        );
        assert_eq!(
            actions_run_id(&run("https://ci.example.com/o/r/runs/42")),
            None
        );
        let mut s = status_as_check_run(&st("ci", "failure")).unwrap();
        s["details_url"] = json!("https://github.com/o/r/actions/runs/42");
        assert_eq!(actions_run_id(&s), None);
    }

    #[test]
    fn a_status_without_a_context_is_skipped() {
        assert!(status_as_check_run(&json!({"state": "success"})).is_none());
    }
}
