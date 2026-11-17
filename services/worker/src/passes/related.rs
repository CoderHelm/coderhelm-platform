//! The worker's side of related tickets (`common::related_tickets`): the
//! run keeps the criteria it took on from linked tickets, and before its
//! pull request is called ready each one, and each address change of a
//! pull request closed in favor of it, is checked where it can be, or
//! listed as not covered with why.

use crate::clients::github::GitHubClient;
use crate::passes::attr_s;
use crate::WorkerState;
use common::related_tickets::{self as rt, Criterion, Verdict};
use tracing::warn;

/// The run record's field holding the criteria taken on from related
/// tickets (JSON).
pub const FIELD: &str = "related_criteria";

/// Keeps the criteria of `related` on the run (nothing when none).
pub async fn keep(state: &WorkerState, team_id: &str, run_id: &str, related: &[rt::Related]) {
    let criteria = rt::fold(related);
    if criteria.is_empty() {
        return;
    }
    let Ok(json) = serde_json::to_string(&criteria) else {
        return;
    };
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #f = :v")
        .expression_attribute_names("#f", FIELD)
        .expression_attribute_values(":v", attr_s(&json))
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not keep the related tickets' criteria");
    }
}

/// The criteria the run took on from related tickets.
pub async fn kept(state: &WorkerState, team_id: &str, run_id: &str) -> Vec<Criterion> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("#f")
        .expression_attribute_names("#f", FIELD)
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| i.get(FIELD).and_then(|v| v.as_s().ok()).cloned())
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

/// The pull requests in a timeline that were closed in favor of this one:
/// in this repository, they link this pull request and were closed without
/// merging. (number, title).
pub fn closed_in_favor(timeline: &serde_json::Value, repo_full: &str) -> Vec<(u64, String)> {
    let mut out: Vec<(u64, String)> = Vec::new();
    for e in timeline.as_array().into_iter().flatten() {
        if e["event"].as_str() != Some("cross-referenced") {
            continue;
        }
        let i = &e["source"]["issue"];
        let same_repo = i["repository"]["full_name"]
            .as_str()
            .is_none_or(|r| r.eq_ignore_ascii_case(repo_full));
        let closed_unmerged = i["state"].as_str() == Some("closed")
            && i["pull_request"].is_object()
            && i["pull_request"]["merged_at"].is_null();
        if let (true, true, Some(n)) = (same_repo, closed_unmerged, i["number"].as_u64()) {
            if !out.iter().any(|(m, _)| *m == n) {
                out.push((n, i["title"].as_str().unwrap_or("").to_string()));
            }
        }
    }
    out
}

/// The most closed pull requests and address moves read for one check.
const MAX_CLOSED: usize = 3;
const MAX_MOVES: usize = 8;

/// Every criterion with what checking it found: the address moves of the
/// pull requests closed in favor of this one, on the head's files; the
/// related tickets' link criteria, on the preview page; anything else
/// listed as not covered with why.
#[allow(clippy::too_many_arguments)]
pub async fn check(
    state: &WorkerState,
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    pr: u64,
    head: &str,
    preview_url: &str,
    criteria: &[Criterion],
) -> Vec<(Criterion, Verdict)> {
    let mut out = Vec::new();
    let timeline = gh
        .list_issue_timeline(owner, repo, pr)
        .await
        .unwrap_or_default();
    for (n, title) in closed_in_favor(&timeline, &format!("{owner}/{repo}"))
        .into_iter()
        .take(MAX_CLOSED)
    {
        let files = gh.list_pull_files(owner, repo, n).await.unwrap_or_default();
        let moves: Vec<rt::MovedLink> = files
            .iter()
            .flat_map(|(path, patch)| rt::moved_links(path, patch))
            .take(MAX_MOVES)
            .collect();
        if moves.is_empty() {
            out.push((
                Criterion {
                    from: format!("#{n}"),
                    text: title.clone(),
                },
                Verdict::NotCovered(
                    "closed in favor of this pull request; not checked: its change moves no \
                     address this check can compare"
                        .into(),
                ),
            ));
            continue;
        }
        for m in moves {
            let content = gh
                .read_file_if_exists(owner, repo, &m.path, head)
                .await
                .ok()
                .flatten();
            let v = rt::check_source(&m, content.as_deref());
            out.push((
                Criterion {
                    from: format!("#{n}"),
                    text: format!("`{}` goes to `{}` ({})", m.path, m.to, title.trim()),
                },
                v,
            ));
        }
    }
    let wants_page = criteria
        .iter()
        .any(|c| rt::link_expectation(&c.text).is_some());
    let page = if wants_page && preview_url.starts_with("https://") {
        fetch(state, preview_url).await
    } else {
        None
    };
    for c in criteria {
        let v = match (rt::link_expectation(&c.text), &page) {
            (Some(e), Some(html)) => rt::check_page("the preview", html, &e).unwrap_or_else(|| {
                Verdict::NotCovered(format!(
                    "not checked: the preview page shows no \"{}\" link (it may open in a drawer or \
                     load later)",
                    e.label
                ))
            }),
            (Some(_), None) => {
                Verdict::NotCovered("not checked: no preview of this pull request to load".into())
            }
            (None, _) => Verdict::NotCovered(
                "not checked: it names no link or address a check can load".into(),
            ),
        };
        out.push((c.clone(), v));
    }
    out
}

/// The preview page's HTML (a page of at most 2 MB), `None` when it can't
/// be loaded.
async fn fetch(state: &WorkerState, url: &str) -> Option<String> {
    let resp = state
        .http
        .get(url)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let text = resp.text().await.ok()?;
    Some(text.chars().take(2_000_000).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_pull_request_closed_unmerged_that_links_this_one_is_in_favor_of_it() {
        let t = json!([
            {"event": "cross-referenced", "source": {"issue": {"number": 12, "title": "Point the drawer button to the new page", "state": "closed", "pull_request": {"merged_at": null}, "repository": {"full_name": "acme/site"}}}},
            {"event": "cross-referenced", "source": {"issue": {"number": 13, "state": "closed", "pull_request": {"merged_at": "2026-01-01"}, "repository": {"full_name": "acme/site"}}}},
            {"event": "cross-referenced", "source": {"issue": {"number": 14, "state": "open", "pull_request": {"merged_at": null}, "repository": {"full_name": "acme/site"}}}},
            {"event": "cross-referenced", "source": {"issue": {"number": 15, "state": "closed", "repository": {"full_name": "acme/site"}}}},
            {"event": "labeled"}
        ]);
        assert_eq!(
            closed_in_favor(&t, "acme/site"),
            vec![(12, "Point the drawer button to the new page".to_string())]
        );
    }
}
