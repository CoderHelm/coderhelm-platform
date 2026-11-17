//! A red check, reproduced before anything is decided about it: the
//! failure read from its own report, the failing test run against the base
//! branch's deployment and against the pull request's, and the result kept
//! on the run (`common::ci_repro`, whose module page has the rules).
//!
//! - [`read_failure`] reads what the failure comment, the check's
//!   annotations and the job's log say: the failing test, its error, the
//!   screenshot and the report link.
//! - [`compare`] runs that test with the head's test code against staging
//!   and against the preview in the sandbox, once per head and check, and
//!   records both sides on the run (`ci_repro`).
//! - [`from_ci`] records, for each failing check, its result on the base
//!   branch's head next to this head's.
//! - [`screenshot`] fetches the failure's screenshot so the fixer sees it.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::ci_repro::{self, Evidence, Leg, Record};
use common::e2e::test_package;
use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::TicketMessage;
use crate::WorkerState;

/// The comparisons kept on the run.
pub(crate) async fn load(state: &WorkerState, team_id: &str, run_id: &str) -> Vec<Record> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .projection_expression("#f")
        .expression_attribute_names("#f", ci_repro::FIELD)
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| i.get(ci_repro::FIELD).and_then(|v| v.as_s().ok()).cloned())
        .map(|s| ci_repro::from_json(&s))
        .unwrap_or_default()
}

/// Keeps `r` on the run (in place of an older one for its head and check),
/// and says it on the run page when `say` (a test run again; a check's own
/// results are kept quietly).
pub(crate) async fn record(state: &WorkerState, team_id: &str, run_id: &str, r: Record, say: bool) {
    let line = r.line();
    let all = ci_repro::upsert(&load(state, team_id, run_id).await, r);
    let saved = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression("SET #f = :v")
        .expression_attribute_names("#f", ci_repro::FIELD)
        .expression_attribute_values(":v", AttributeValue::S(ci_repro::to_json(&all)))
        .send()
        .await;
    if let Err(e) = saved {
        warn!(run_id, error = %e, "Could not keep the failing test's comparison on the run");
    }
    if say {
        super::add_progress_note(
            state,
            team_id,
            run_id,
            &format!("Failing check compared with the base branch: {line}"),
        )
        .await;
    }
}

fn is_reporter(c: &serde_json::Value) -> bool {
    let login = c["user"]["login"].as_str().unwrap_or("");
    let bot = c["user"]["type"].as_str() == Some("Bot") || login.ends_with("[bot]");
    bot && !super::is_own_login(login)
}

/// What the failure's own report says: the newest end-to-end comment on the
/// pull request (by a CI reporter), else the job's log (`logs`, which holds
/// the annotations too). `None` when nothing names a failure.
pub(crate) async fn read_failure(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr: u64,
    check: &str,
    logs: &str,
) -> Option<Evidence> {
    let comment = github
        .list_issue_comments(owner, repo, pr)
        .await
        .unwrap_or_default()
        .iter()
        .rev()
        .filter(|c| is_reporter(c))
        .map(|c| c["body"].as_str().unwrap_or("").to_string())
        .find(|b| super::e2e_reads::is_e2e_name(b))
        .unwrap_or_default();
    evidence_of(check, &comment, logs)
}

/// [`read_failure`] from what was read.
pub(crate) fn evidence_of(check: &str, comment: &str, logs: &str) -> Option<Evidence> {
    let read = common::e2e::failure_read::read(
        check,
        &common::e2e::failure_read::Sources {
            comment: comment.to_string(),
            annotations: Vec::new(),
            log: logs.to_string(),
        },
    )
    .ok()?;
    if read.tests.is_empty() && read.error.trim().is_empty() {
        return None;
    }
    Some(Evidence {
        check: check.to_string(),
        tests: read.tests,
        error: read.error,
        source: read.source,
        shots: ci_repro::shots_in(comment),
        report: super::e2e_reads::comment_verdict(comment).and_then(|v| v.report),
    })
}

/// For each failing check, its result on the base branch's head next to
/// this head's (`ci` records), kept on the run.
pub(crate) async fn from_ci(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    run_id: &str,
    head: &str,
    failing: &[String],
) -> Vec<Record> {
    let (owner, repo) = (&msg.repo_owner, &msg.repo_name);
    let base = &msg.base_branch;
    let base_sha = Some(super::round::branch_head(github, owner, repo, base).await)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| base.clone());
    let Ok(checks) = github.list_ci_for_ref(owner, repo, &base_sha).await else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for check in failing {
        let base_failed = base_result(&checks, check);
        let r = ci_repro::from_checks(head, &base_sha, check, base_failed);
        record(state, &msg.team_id, run_id, r.clone(), false).await;
        out.push(r);
    }
    out
}

/// A check's result on a ref (`Some(true)`: failed), by its name or the
/// workflow it belongs to; `None` when it didn't run there.
fn base_result(checks: &serde_json::Value, name: &str) -> Option<bool> {
    let runs = checks["check_runs"].as_array()?;
    let mut seen = None;
    for r in runs {
        let n = r["name"].as_str().unwrap_or("");
        if n != name && !n.starts_with(&format!("{name} /")) && !name.starts_with(&format!("{n} /"))
        {
            continue;
        }
        match r["conclusion"].as_str().unwrap_or("") {
            "failure" | "timed_out" => return Some(true),
            "success" => seen = Some(false),
            _ => {}
        }
    }
    seen
}

/// The failing test run with the head's test code against the base
/// branch's deployment and against this pull request's, once per head and
/// check, kept on the run. `None` when the evidence names no test.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn compare(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    provider: &ModelProvider,
    run_id: &str,
    (pr, branch, head): (u64, &str, &str),
    ev: &Evidence,
) -> Option<Record> {
    let test = ev.tests.first()?.clone();
    if let Some(r) = load(state, &msg.team_id, run_id)
        .await
        .into_iter()
        .rev()
        .find(|r| r.source == "sandbox" && r.head == head && r.tests.first() == Some(&test))
    {
        return Some(r);
    }
    let r = two_way(
        state,
        github,
        provider.scope.ctx(common::agent_policy::Agent::Coder),
        Target {
            team_id: &msg.team_id,
            run_id,
            owner: &msg.repo_owner,
            repo: &msg.repo_name,
            pr,
            branch,
            head,
        },
        &ev.check,
        &test,
    )
    .await;
    info!(run_id, verdict = ?r.verdict(), "Failing test compared with the base branch");
    record(state, &msg.team_id, run_id, r.clone(), true).await;
    Some(r)
}

/// The pull request a test is run for.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Target<'a> {
    pub team_id: &'a str,
    pub run_id: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    pub pr: u64,
    pub branch: &'a str,
    pub head: &'a str,
}

/// One Playwright test (`file › title`) with the head's test code, run in
/// the sandbox against the base branch's deployment (`base`, staging) and
/// against the pull request's preview (`change`). A side that couldn't run
/// says why in its note. Nothing is recorded here: the red-check
/// comparison ([`compare`]) and the two-way proof (`e2e_proof`) keep it.
pub(crate) async fn two_way(
    state: &WorkerState,
    github: &GitHubClient,
    ctx: common::agent_policy::GateCtx,
    t: Target<'_>,
    check: &str,
    test: &str,
) -> Record {
    let (owner, repo, head) = (t.owner, t.repo, t.head);
    let mut r = Record {
        head: head.to_string(),
        check: check.to_string(),
        tests: vec![test.to_string()],
        source: "sandbox".into(),
        at: chrono::Utc::now().to_rfc3339(),
        ..Default::default()
    };
    let not_run = |r: &mut Record, why: &str| {
        r.base.note = why.to_string();
        r.change.note = why.to_string();
    };
    let (spec, title) = ci_repro::spec_and_title(test);
    let tree: Vec<String> = match github.get_tree(owner, repo, head).await {
        Ok(t) => t.into_iter().map(|e| e.path).collect(),
        Err(e) => {
            not_run(&mut r, &format!("the head's files couldn't be read ({e})"));
            return r;
        }
    };
    let Some(path) = tree
        .iter()
        .find(|p| p.ends_with(&spec) || p.ends_with(&format!("/{spec}")))
        .cloned()
    else {
        not_run(&mut r, &format!("`{spec}` isn't a file on the head"));
        return r;
    };
    // The package the spec lives in, and its runner.
    let mut texts: HashMap<String, String> = HashMap::new();
    let dir = test_package::package_dir(&tree, &path).unwrap_or_default();
    for p in [format!("{dir}/package.json"), "package.json".to_string()] {
        let p = p.trim_start_matches('/').to_string();
        if let Ok(t) = github.read_file(owner, repo, &p, head).await {
            texts.insert(p, t);
        }
    }
    let pkg = test_package::package_of(&tree, &path, &|p| texts.get(p).cloned());
    let Some(pkg) = pkg.filter(|p| p.runner == Some(test_package::Runner::Playwright)) else {
        not_run(
            &mut r,
            "the spec's runner isn't Playwright, the one CoderHelm runs against an address",
        );
        return r;
    };
    let rel = path
        .strip_prefix(&format!("{}/", pkg.dir))
        .unwrap_or(&path)
        .to_string();
    // Where each side runs: the configured test address and the preview,
    // else the staging address the repository's end-to-end setup names.
    let configured = test_address(state, t.team_id, &format!("{owner}/{repo}")).await;
    let mut setup: Vec<(String, String)> = Vec::new();
    for p in tree
        .iter()
        .filter(|p| {
            let low = p.to_ascii_lowercase();
            low.contains("playwright.config")
                || (low.starts_with(".github/workflows/")
                    && (low.contains("e2e") || low.contains("playwright")))
        })
        .take(6)
    {
        if let Ok(text) = github.read_file(owner, repo, p, head).await {
            setup.push((p.clone(), text));
        }
    }
    let base_url = ci_repro::base_target(configured.as_deref(), &setup);
    let preview = super::preview::load(state, t.team_id, t.run_id)
        .await
        .filter(|p| p.head == head && !p.url.trim().is_empty())
        .map(|p| p.url);
    let change_url = preview.or_else(|| {
        configured
            .as_deref()
            .filter(|c| c.contains("{pr}"))
            .map(|c| c.replace("{pr}", &t.pr.to_string()))
    });
    r.base.target = base_url.clone().unwrap_or_default();
    r.change.target = change_url.clone().unwrap_or_default();
    let Some(base_url) = base_url else {
        not_run(
            &mut r,
            "no address for the base branch's deployment: the Tests settings have none without `{pr}`, and the end-to-end setup names no staging address",
        );
        return r;
    };
    let mut legs: Vec<(&str, String)> = Vec::new();
    match ci_repro::leg_cmd(&pkg.dir, &rel, &title, &base_url) {
        Some(c) => legs.push(("base", c)),
        None => r.base.note = format!("`{base_url}` can't be run against"),
    }
    match change_url
        .as_deref()
        .and_then(|u| ci_repro::leg_cmd(&pkg.dir, &rel, &title, u))
    {
        Some(c) => legs.push(("change", c)),
        None => r.change.note = "no preview address for this pull request's head".into(),
    }
    if legs.is_empty() {
        return r;
    }
    if state.config.sandbox_bucket_name.is_empty() || state.config.sandbox_project_name.is_empty() {
        not_run(&mut r, "no sandbox here to run the test");
        return r;
    }
    // The team's agent permissions decide whether the commands may run.
    let gate = crate::agent::policy::gate(state, ctx, None);
    let install = test_package::install_cmd(&pkg);
    let mut steps: Vec<(&str, String)> = vec![("install", install)];
    steps.extend(legs);
    for (side, cmd) in &steps {
        if !matches!(
            gate.check_command(
                cmd,
                "Run the test against the base branch's deployment and the preview"
            )
            .await,
            common::agent_policy::Outcome::Run { .. }
        ) {
            not_run(
                &mut r,
                &format!("the team's agent permissions don't allow the {side} command"),
            );
            return r;
        }
    }
    let sandbox = crate::clients::sandbox::SandboxClient::new(
        &state.codebuild,
        &state.s3,
        &state.logs,
        &state.config.sandbox_bucket_name,
        &state.config.sandbox_project_name,
    )
    .with_egress(crate::agent::policy::sandbox_egress(state, t.team_id).await);
    let source = match sandbox
        .ensure_source(t.run_id, head, || {
            github.download_tarball(owner, repo, head)
        })
        .await
    {
        Ok(k) => k,
        Err(e) => {
            not_run(
                &mut r,
                &format!("the branch couldn't be prepared in the sandbox ({e})"),
            );
            return r;
        }
    };
    let script = ci_repro::script(&steps);
    let node = super::detect_node_version(github, owner, repo, t.branch).await;
    match sandbox
        .run_checks(&source, &script, node.as_deref(), None, None)
        .await
    {
        Ok(o) if o.ran => {
            for (side, leg) in [("base", &mut r.base), ("change", &mut r.change)] {
                let target = leg.target.clone();
                if let Some(out) = ci_repro::split(&o.output, side) {
                    *leg = ci_repro::leg_of(&target, &out);
                } else if leg.note.is_empty() {
                    *leg = Leg {
                        target,
                        note: "no output".into(),
                        ..Default::default()
                    };
                }
            }
        }
        Ok(o) => not_run(
            &mut r,
            &format!("the sandbox gave no result ({})", o.reason),
        ),
        Err(e) => not_run(&mut r, &format!("the sandbox couldn't run ({e})")),
    }
    r
}

/// The team's test address for the repository (Tests settings), as set.
async fn test_address(state: &WorkerState, team_id: &str, repo: &str) -> Option<String> {
    let st = common::e2e::store::Store::new(&state.dynamo, &state.config.tests_table_name);
    if !st.enabled() {
        return None;
    }
    let s = st.settings(team_id).await.ok()?;
    let t = s.report_source(repo)?.target_url.trim().to_string();
    (!t.is_empty()).then_some(t)
}

/// Whether CoderHelm fetches an image from `host`: GitHub's attachment and
/// user content hosts, and the team's own domains.
pub(crate) fn image_host_allowed(host: &str, path: &str, team_domains: &[String]) -> bool {
    let h = host.to_ascii_lowercase();
    (h == "github.com" && path.starts_with("/user-attachments/"))
        || h.ends_with(".githubusercontent.com")
        || team_domains
            .iter()
            .any(|d| h == *d || h.ends_with(&format!(".{d}")))
}

/// The failure's screenshot, kept in the run's own storage so the fixer's
/// first message carries it as an image. `None` when it can't be fetched
/// (an unknown host, not an image, too large).
pub(crate) async fn screenshot(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    url: &str,
) -> Option<crate::models::ImageAttachment> {
    let (u, host) = super::web_check::vet_url(url).ok()?;
    let domains = super::web_check::team_domains(state, team_id).await;
    if !image_host_allowed(&host, u.path(), &domains) {
        info!(run_id, %host, "The failure's screenshot is on a host CoderHelm doesn't fetch from; its link goes to the fixer");
        return None;
    }
    let addrs = tokio::net::lookup_host((host.as_str(), 443)).await.ok()?;
    let addrs: Vec<_> = addrs.collect();
    if addrs.is_empty() || !addrs.iter().all(|a| super::web_check::is_public_ip(a.ip())) {
        return None;
    }
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        state.http.get(u.as_str()).send(),
    )
    .await
    .ok()?
    .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let media = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    if !matches!(
        media.as_str(),
        "image/png" | "image/jpeg" | "image/webp" | "image/gif"
    ) {
        return None;
    }
    let bytes = resp.bytes().await.ok()?;
    if bytes.is_empty() || bytes.len() > 5 * 1024 * 1024 {
        return None;
    }
    let ext = media.trim_start_matches("image/");
    let key = format!(
        "ci-evidence/{team_id}/{run_id}/{}.{ext}",
        chrono::Utc::now().timestamp_millis()
    );
    state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(&key)
        .content_type(&media)
        .body(bytes.to_vec().into())
        .send()
        .await
        .ok()?;
    Some(crate::models::ImageAttachment {
        s3_key: key,
        media_type: media,
        filename: "failure-screenshot".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_failure_comment_gives_the_test_the_error_and_the_screenshot() {
        let comment = "## Playwright E2E results for eb32df1\n\n1 failed, 40 passed\n\n  1) [chromium] › tests/browse/camp-registration.spec.ts:88:7 › Camp registration › selecting a camp adds it to the cart while registration is open\n\n    Error: expect(locator).not.toHaveText(expected) failed\n    Received: Subtotal$0\n\n![failure](https://github.com/user-attachments/assets/shot.png)\n\n[Full report](https://reports.example.com/pr-12/index.html)";
        let ev = evidence_of("E2E on preview", comment, "").unwrap();
        assert_eq!(
            ev.tests,
            vec!["tests/browse/camp-registration.spec.ts › selecting a camp adds it to the cart while registration is open".to_string()]
        );
        assert!(ev
            .error
            .starts_with("Error: expect(locator).not.toHaveText"));
        assert_eq!(
            ev.shots,
            vec!["https://github.com/user-attachments/assets/shot.png".to_string()]
        );
        assert_eq!(
            ev.report.as_deref(),
            Some("https://reports.example.com/pr-12/index.html")
        );
        assert!(evidence_of("x", "", "").is_none());
    }

    #[test]
    fn a_check_on_the_base_is_found_by_name_or_workflow() {
        let checks = json!({"check_runs": [
            {"name": "Test Suite", "conclusion": "success"},
            {"name": "E2E after staging deploy / Playwright e2e (all tests)", "conclusion": "failure"}
        ]});
        assert_eq!(base_result(&checks, "Test Suite"), Some(false));
        assert_eq!(base_result(&checks, "E2E after staging deploy"), Some(true));
        assert_eq!(base_result(&checks, "Deploy Preview"), None);
    }

    #[test]
    fn images_come_only_from_known_hosts() {
        let team = vec!["example.com".to_string()];
        assert!(image_host_allowed(
            "github.com",
            "/user-attachments/assets/x",
            &team
        ));
        assert!(image_host_allowed(
            "private-user-images.githubusercontent.com",
            "/x.png",
            &team
        ));
        assert!(image_host_allowed(
            "reports.example.com",
            "/pr-1/a.png",
            &team
        ));
        assert!(!image_host_allowed(
            "github.com",
            "/acme/site/pull/1",
            &team
        ));
        assert!(!image_host_allowed("evil.test", "/a.png", &team));
    }
}
