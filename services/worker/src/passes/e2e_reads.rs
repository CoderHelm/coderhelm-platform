//! An end-to-end result is read, never handed to a person.
//!
//! A run once listed two items for a repository admin "before merge":
//! reading the end-to-end comment and its workflow run, and reading one
//! spec's case from the end-to-end run, each "a permission only a person
//! grants". Nothing had been refused: the installation's token lacked
//! `Actions: write`, so the step that wanted to start the end-to-end
//! workflow by `workflow_dispatch` gave up before trying, and the step's
//! words ("not allowed to", "permission") were read as a person's grant.
//! Reading a result needs no write at all.
//!
//! The rules:
//!
//! - [`read`] reads the head's end-to-end result from every route the
//!   installation can read, first answer wins: the check runs on the head
//!   (with the failing tests from their annotations), the workflow runs on
//!   the head, the end-to-end comment on the pull request (issues API) and
//!   the published report it links to (on a host the page check may reach).
//! - A route the installation can't read is skipped, never a person's
//!   item. A permission the whole install lacks is one install-level item
//!   (`github_grants::install_item`, on the install's setup checks).

use serde_json::Value;

use super::outside_exec::Ctx;

/// What a route said about the head's end-to-end tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Passed(String),
    Failed(String),
    Running,
}

/// Whether a check or workflow name is an end-to-end run.
pub fn is_e2e_name(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    ["e2e", "playwright", "cypress", "end-to-end", "end to end"]
        .iter()
        .any(|w| n.contains(w))
}

/// The head's end-to-end check runs: passed, failed (with names) or
/// running. None when there is no completed or running one.
pub fn checks_verdict(checks: &[Value]) -> Option<Verdict> {
    let e2e: Vec<&Value> = checks
        .iter()
        .filter(|c| is_e2e_name(c["name"].as_str().unwrap_or("")))
        .filter(|c| c["conclusion"].as_str() != Some("skipped"))
        .collect();
    if e2e.is_empty() {
        return None;
    }
    if e2e
        .iter()
        .any(|c| c["status"].as_str() != Some("completed"))
    {
        return Some(Verdict::Running);
    }
    let failed: Vec<String> = e2e
        .iter()
        .filter(|c| {
            !matches!(
                c["conclusion"].as_str().unwrap_or(""),
                "success" | "neutral"
            )
        })
        .map(|c| format!("`{}`", c["name"].as_str().unwrap_or("a check")))
        .collect();
    let names: Vec<String> = e2e
        .iter()
        .map(|c| format!("`{}`", c["name"].as_str().unwrap_or("a check")))
        .collect();
    Some(if failed.is_empty() {
        Verdict::Passed(format!("{} passed (check runs)", names.join(", ")))
    } else {
        Verdict::Failed(format!("{} failed (check runs)", failed.join(", ")))
    })
}

/// What an end-to-end comment on the pull request says.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommentVerdict {
    pub passed: Option<u32>,
    pub failed: Option<u32>,
    /// The report it links to.
    pub report: Option<String>,
    /// The commit it reports on, when it names one.
    pub sha: Option<String>,
}

fn count_before(t: &str, word: &str) -> Option<u32> {
    let i = t.find(word)?;
    let before = t[..i]
        .trim_end()
        .trim_end_matches([':', '*', '|'])
        .trim_end();
    let digits: String = before
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    digits.parse().ok()
}

fn count_after(t: &str, word: &str) -> Option<u32> {
    let i = t.find(word)?;
    let after = t[i + word.len()..].trim_start_matches([':', '*', '|', ' ']);
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Reads an end-to-end comment (a report bot's summary): its passed and
/// failed counts, the report link and the commit. None for a comment that
/// isn't one.
pub fn comment_verdict(body: &str) -> Option<CommentVerdict> {
    let t = body.to_ascii_lowercase();
    if !is_e2e_name(&t) {
        return None;
    }
    let mut v = CommentVerdict {
        passed: count_before(&t, "passed").or_else(|| count_after(&t, "passed")),
        failed: count_before(&t, "failed").or_else(|| count_after(&t, "failed")),
        ..Default::default()
    };
    for (i, _) in body.match_indices("https://") {
        let rest = &body[i..];
        let end = rest
            .find(|c: char| c.is_whitespace() || matches!(c, ')' | '"' | '\'' | '>' | ']'))
            .unwrap_or(rest.len());
        let url = &rest[..end];
        let low = url.to_ascii_lowercase();
        if (low.contains("report") || low.ends_with(".html") || low.ends_with('/'))
            && (!low.contains("github.com/") || low.contains("/artifacts/"))
        {
            v.report = Some(url.to_string());
            break;
        }
    }
    v.sha = t
        .split(|c: char| !c.is_ascii_hexdigit())
        .find(|w| (7..=40).contains(&w.len()) && w.chars().any(|c| c.is_ascii_digit()))
        .map(str::to_string);
    (v.passed.is_some() || v.failed.is_some() || v.report.is_some()).then_some(v)
}

impl CommentVerdict {
    fn verdict(&self, at: &str) -> Option<Verdict> {
        match (self.passed, self.failed) {
            (_, Some(f)) if f > 0 => Some(Verdict::Failed(format!(
                "{f} end-to-end test(s) failed ({at})"
            ))),
            (Some(p), _) if p > 0 => Some(Verdict::Passed(format!(
                "{p} end-to-end test(s) passed ({at})"
            ))),
            _ => None,
        }
    }
}

/// Whether a comment's commit is the head (a short sha prefix of it, or
/// none named, which is read as the latest).
fn on_head(c: &CommentVerdict, head: &str) -> bool {
    c.sha
        .as_deref()
        .is_none_or(|s| head.to_ascii_lowercase().starts_with(s))
}

/// The head's end-to-end result from every route the installation can
/// read, in order; None when no route had one.
pub async fn read(cx: &Ctx<'_>, head: &str) -> Option<Verdict> {
    // 1. Check runs on the head (Checks: read), with the failing tests from
    //    their annotations.
    if let Ok(v) = cx
        .github
        .list_check_runs_for_ref(cx.owner, cx.repo, head)
        .await
    {
        let checks = v["check_runs"].as_array().cloned().unwrap_or_default();
        match checks_verdict(&checks) {
            Some(Verdict::Failed(why)) => {
                let notes = cx
                    .github
                    .get_check_run_annotations(cx.owner, cx.repo, head)
                    .await
                    .unwrap_or_default();
                let failing = super::outside_exec::failing_tests(&notes);
                return Some(Verdict::Failed(if failing.is_empty() {
                    why
                } else {
                    format!("{why}: {}", failing.join(", "))
                }));
            }
            Some(v) => return Some(v),
            None => {}
        }
    }
    // 2. Workflow runs on the head (Actions: read).
    if let Ok(runs) = cx
        .github
        .list_workflow_runs_for_sha(cx.owner, cx.repo, head)
        .await
    {
        let names: Vec<String> = runs
            .iter()
            .filter_map(|r| r["name"].as_str())
            .filter(|n| is_e2e_name(n))
            .map(str::to_string)
            .collect();
        if !names.is_empty() {
            let v = super::outside_exec::runs_verdict(&runs, &names);
            if !v.running.is_empty() {
                return Some(Verdict::Running);
            }
            if let Some((n, why, _)) = v.failed.first() {
                return Some(Verdict::Failed(format!("`{n}` {why} (workflow run)")));
            }
            if !v.passed.is_empty() {
                return Some(Verdict::Passed(format!(
                    "{} passed (workflow run)",
                    v.passed.join(", ")
                )));
            }
        }
    }
    // 3. The end-to-end comment on the pull request (Issues: read), and the
    //    report it links to.
    if let Ok(comments) = cx
        .github
        .list_issue_comments(cx.owner, cx.repo, cx.pr_number)
        .await
    {
        let latest = comments
            .iter()
            .rev()
            .filter_map(|c| comment_verdict(c["body"].as_str().unwrap_or("")))
            .find(|c| on_head(c, head));
        if let Some(c) = latest {
            if let Some(v) = c.verdict("the end-to-end comment on the pull request") {
                return Some(v);
            }
            if let Some(url) = &c.report {
                if let Some(v) = report(cx, url).await {
                    return Some(v);
                }
            }
        }
    }
    None
}

/// The published report's verdict, read with the page check (only a host
/// the team's sites allow).
async fn report(cx: &Ctx<'_>, url: &str) -> Option<Verdict> {
    let web = super::web_check::WebCheck::load(
        cx.state,
        cx.team_id,
        &format!("{}/{}", cx.owner, cx.repo),
        "CoderHelm (outside steps)",
        cx.pr_number,
        url,
    )
    .await
    .with_run(cx.run_id, "outside steps".to_string());
    let find = vec!["failed".to_string(), "passed".to_string()];
    let ck = web
        .verify_find(cx.state, cx.github, url, &find)
        .await
        .ok()?;
    if ck.status() != Some(200) {
        return None;
    }
    let text = ck
        .found
        .iter()
        .flat_map(|(_, hits)| hits.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    let c = comment_verdict(&format!("e2e {text}"))?;
    c.verdict("the published end-to-end report")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn check_runs_give_the_end_to_end_result() {
        let checks = vec![
            json!({"name": "unit tests", "status": "completed", "conclusion": "success"}),
            json!({"name": "E2E Playwright", "status": "completed", "conclusion": "failure"}),
            json!({"name": "E2E on preview", "status": "completed", "conclusion": "skipped"}),
        ];
        assert_eq!(
            checks_verdict(&checks),
            Some(Verdict::Failed(
                "`E2E Playwright` failed (check runs)".into()
            ))
        );
        let running = vec![json!({"name": "playwright", "status": "in_progress"})];
        assert_eq!(checks_verdict(&running), Some(Verdict::Running));
        let none = vec![json!({"name": "lint", "status": "completed", "conclusion": "success"})];
        assert_eq!(checks_verdict(&none), None);
    }

    #[test]
    fn the_end_to_end_comment_is_read_with_its_report() {
        let body = "## Playwright E2E results for abc1234\n\n41 passed, 2 failed\n\n[Full report](https://reports.example.com/pr-12/index.html)";
        let c = comment_verdict(body).unwrap();
        assert_eq!(c.failed, Some(2));
        assert_eq!(
            c.report.as_deref(),
            Some("https://reports.example.com/pr-12/index.html")
        );
        assert_eq!(c.sha.as_deref(), Some("abc1234"));
        assert!(on_head(&c, "abc1234ffff"));
        assert!(!on_head(&c, "def5678"));
        let ok = comment_verdict("E2E: 12 passed, 0 failed").unwrap();
        assert_eq!(
            ok.verdict("x"),
            Some(Verdict::Passed("12 end-to-end test(s) passed (x)".into()))
        );
        assert_eq!(comment_verdict("Thanks for the review!"), None);
    }
}
