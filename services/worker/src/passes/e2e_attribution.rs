//! Whether an end-to-end failure on Coderhelm's own pull request comes from
//! the change (see `common::e2e::attribution` for the rules), and what
//! happens when it doesn't.
//!
//! For each failing test the report is read again for what the page showed
//! (the failing attempt's screenshot, read by a model, and its page
//! snapshot), the failing steps are mapped to the app code they run (the
//! locators in the step, the spec and the page objects it imports, and the
//! requests the trace recorded, each searched in the app's code), and the
//! diff is checked against that path. An unrelated failure gets no fix
//! rounds: a separate small pull request fixes the test, the original pull
//! request says so and links it, and its CI counts as blocked on that fix
//! (see `quarantine::Gate::Unrelated`), not failing.

use common::e2e::attribution::{self, Attribution, Hook, Verdict};
use common::e2e::messages::TestIngest;
use common::e2e::model::ParsedTest;
use common::e2e::report_source::Location;
use common::e2e::store::Store;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::agent::llm;
use crate::agent::provider::{self, ModelProvider};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;

use super::test_fix::{read_run, CiReports, Reports};

/// Failing tests attributed per run.
const MAX_TESTS: usize = 3;
/// Hooks searched in the app's code per test.
const MAX_HOOKS: usize = 8;
/// Largest screenshot sent to the model.
const MAX_SCREENSHOT: usize = 4 * 1024 * 1024;

pub const FIX_PREFIX: &str = "E2E-TESTFIX-";

/// What was decided for one commit of a pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub pr: u64,
    /// Workflow runs whose failures were attributed.
    pub runs: Vec<u64>,
    pub tests: Vec<Attribution>,
}

impl Record {
    /// Every failure of every run in `runs` is unrelated to the change.
    pub fn blocks(&self, runs: &[u64]) -> bool {
        !runs.is_empty()
            && runs.iter().all(|r| self.runs.contains(r))
            && !self.tests.is_empty()
            && self.tests.iter().all(|t| t.verdict == Verdict::Unrelated)
    }

    pub fn blocked_lines(&self) -> Vec<String> {
        self.tests
            .iter()
            .filter_map(Attribution::blocked_line)
            .collect()
    }
}

fn record_sk(repo: &str, sha: &str) -> String {
    format!("E2E_ATTR#{repo}#{sha}")
}

pub async fn load(state: &WorkerState, team_id: &str, repo: &str, sha: &str) -> Option<Record> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(&record_sk(repo, sha)))
        .send()
        .await
        .ok()?
        .item
        .clone()?;
    serde_json::from_str(item.get("data")?.as_s().ok()?).ok()
}

async fn save(state: &WorkerState, team_id: &str, repo: &str, sha: &str, r: &Record) {
    let ttl = (chrono::Utc::now() + chrono::Duration::days(30)).timestamp();
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", super::attr_s(team_id))
        .item("sk", super::attr_s(&record_sk(repo, sha)))
        .item(
            "data",
            super::attr_s(&serde_json::to_string(r).unwrap_or_default()),
        )
        .item(
            "ttl",
            aws_sdk_dynamodb::types::AttributeValue::N(ttl.to_string()),
        )
        .send()
        .await
    {
        warn!(error = %e, "Could not save the failure attribution");
    }
}

/// The lines that block CI on `sha` when every failing run's failures are
/// unrelated to the change; `None` otherwise.
pub async fn blocked(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    sha: &str,
    runs: &[u64],
) -> Option<Vec<String>> {
    let r = load(state, team_id, repo, sha).await?;
    r.blocks(runs).then(|| r.blocked_lines())
}

/// Reads the page at the failure: the screenshot (when there is one) and
/// the snapshot, by a model.
#[allow(clippy::too_many_arguments)]
async fn read_page(
    state: &WorkerState,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    test: &str,
    error: &str,
    expected: &str,
    snapshot: &str,
    screenshot: Option<&[u8]>,
) -> Option<attribution::PageRead> {
    use base64::Engine;
    let mut blocks = vec![serde_json::json!({
        "type": "text",
        "text": attribution::page_prompt(test, error, expected, snapshot),
    })];
    if let Some(png) =
        screenshot.filter(|b| b.len() <= MAX_SCREENSHOT && b.starts_with(&[0x89, b'P', b'N', b'G']))
    {
        blocks.push(serde_json::json!({
            "type": "image",
            "source": { "type": "base64", "media_type": "image/png", "data": base64::engine::general_purpose::STANDARD.encode(png) }
        }));
    }
    let mut messages = vec![("user".to_string(), blocks)];
    let reply = provider::converse(
        state,
        provider,
        common::agent_policy::Agent::Reviewer,
        provider.primary_model_id(),
        attribution::PAGE_SYSTEM,
        &mut messages,
        &[],
        &super::triage::NoOpExecutor,
        usage,
        llm::ConverseOptions {
            max_turns: 1,
            max_tokens: 1_000,
            deadline: None,
            effort: Some("low"),
        },
        None,
        None,
    )
    .await;
    match reply {
        Ok(t) => attribution::parse_page(&t),
        Err(e) => {
            warn!(error = %e, "Could not read the page at the failure");
            None
        }
    }
}

/// The request paths a trace recorded, from its zip.
fn trace_paths(zip_bytes: &[u8]) -> Vec<String> {
    use std::io::Read;
    let Ok(mut zip) = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes)) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for i in 0..zip.len() {
        let Ok(f) = zip.by_index(i) else { continue };
        if !f.name().ends_with(".network") {
            continue;
        }
        let mut text = String::new();
        if f.take(20 * 1024 * 1024).read_to_string(&mut text).is_ok() {
            for p in attribution::trace_requests(&text) {
                if !out.contains(&p) {
                    out.push(p);
                }
            }
        }
    }
    out
}

/// Where each hook is found in the repository at `git_ref`.
async fn find_hooks(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
    hooks: &[Hook],
) -> Vec<(Hook, Vec<String>)> {
    let mut out = Vec::new();
    for h in hooks.iter().take(MAX_HOOKS) {
        let term = h.term();
        if term.trim().len() < 3 {
            continue;
        }
        if let Ok(found) = gh.search_code(owner, repo, git_ref, term).await {
            let paths: Vec<String> = found.into_iter().map(|r| r.path).take(10).collect();
            out.push((h.clone(), paths));
        }
    }
    out
}

/// Attributes each failing test of a run on a pull request's head.
pub async fn attribute(
    state: &WorkerState,
    gh: &GitHubClient,
    msg: &TestIngest,
    failing: &[&ParsedTest],
) -> Vec<Attribution> {
    let (owner, repo) = (msg.repo_owner.as_str(), msg.repo_name.as_str());
    let full = format!("{owner}/{repo}");
    let Ok(provider) = ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await
    else {
        return Vec::new();
    };
    let mut usage = TokenUsage::default();
    let diff = gh
        .compare_changed_files(owner, repo, &msg.default_branch, &msg.head_sha)
        .await
        .unwrap_or_default();
    let tree: Vec<String> = gh
        .get_tree(owner, repo, &msg.head_sha)
        .await
        .map(|t| t.into_iter().map(|e| e.path).collect())
        .unwrap_or_default();
    // The run's report, from where the team keeps it or the run's artifacts.
    let st = Store::new(&state.dynamo, &state.config.tests_table_name);
    let loc = if st.enabled() {
        st.settings(&msg.team_id)
            .await
            .ok()
            .and_then(|s| s.report_source(&full).and_then(|r| r.location().ok()))
    } else {
        None
    };
    let mut tried = Vec::new();
    let s3 = match &loc {
        Some(l @ Location::S3 { .. }) => {
            super::test_fix::open_s3(state, &msg.team_id, l, &mut tried).await
        }
        _ => None,
    };
    let ci = CiReports::new(gh, owner, repo, vec![(msg.run_id, msg.run_attempt)]);
    let reports: &dyn Reports = match &s3 {
        Some(r) => r,
        None => &ci,
    };
    let mut out = Vec::new();
    for t in failing.iter().take(MAX_TESTS) {
        let title = t.full_title();
        let names = vec![t.title.clone()];
        let Ok(rr) = read_run(reports, msg.run_id, msg.run_attempt, &names).await else {
            continue;
        };
        let Some(f) = rr
            .failures
            .into_iter()
            .find(|f| f.test == t.title || title.ends_with(&f.test))
        else {
            continue;
        };
        let screenshot = match f.attachments.iter().find(|a| a.ends_with(".png")) {
            Some(p) => reports.file(msg.run_id, msg.run_attempt, p).await,
            None => None,
        };
        let requests = match f.attachments.iter().find(|a| a.ends_with(".zip")) {
            Some(p) => reports
                .file(msg.run_id, msg.run_attempt, p)
                .await
                .map(|z| trace_paths(&z))
                .unwrap_or_default(),
            None => Vec::new(),
        };
        // The spec and the page objects it imports: the locators the
        // failing step uses live there.
        let spec = tree
            .iter()
            .find(|p| {
                p.ends_with(&format!("/{}", f.file.trim_start_matches("./"))) || **p == f.file
            })
            .cloned();
        let mut texts: Vec<String> =
            vec![f.failed_step.clone(), f.snippet.clone(), f.error.clone()];
        if let Some(spec) = &spec {
            if let Ok(src) = gh.read_file(owner, repo, spec, &msg.head_sha).await {
                for import in common::e2e::test_package::relative_imports(spec, &src, &tree)
                    .into_iter()
                    .take(5)
                {
                    if let Ok(po) = gh.read_file(owner, repo, &import, &msg.head_sha).await {
                        texts.push(po);
                    }
                }
            }
        }
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        let mut hooks = attribution::hooks(&refs);
        hooks.extend(requests.into_iter().map(Hook::Request));
        let found = find_hooks(gh, owner, repo, &msg.head_sha, &hooks).await;
        let (path, path_evidence) = attribution::failing_path(&found);
        let touched = attribution::touched(&diff, &path);
        let page = read_page(
            state,
            &provider,
            &mut usage,
            &title,
            &f.error,
            &f.snippet,
            &f.page_state,
            screenshot.as_deref(),
        )
        .await
        .unwrap_or_default();
        let verdict = attribution::decide(&touched, page.valid_for_data);
        out.push(Attribution {
            test: title,
            file: spec.unwrap_or_else(|| f.file.clone()),
            run_id: msg.run_id,
            error: f.error.clone(),
            shown: page.shown,
            state: page.state,
            dialogs: f.dialogs.clone(),
            page_state: f.page_state.clone(),
            path,
            path_evidence,
            touched,
            diff: diff.clone(),
            valid_for_data: page.valid_for_data,
            reason: page.reason,
            verdict,
            fix_pr: None,
            source: f.source.clone(),
        });
    }
    info!(repo = %full, sha = %msg.head_sha, attributed = out.len(), "End-to-end failures attributed");
    out
}

/// What the separate test-fix run carries: where it came from and the
/// evidence the expectation was wrong.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seed {
    pub owner: String,
    pub repo: String,
    pub pr: u64,
    pub sha: String,
    pub run_id: String,
    pub attribution: Attribution,
}

fn seed_sk(ticket_id: &str) -> String {
    format!("TESTFIX_SEED#{ticket_id}")
}

pub async fn load_seed(state: &WorkerState, team_id: &str, ticket_id: &str) -> Option<Seed> {
    if !ticket_id.starts_with(FIX_PREFIX) {
        return None;
    }
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(&seed_sk(ticket_id)))
        .send()
        .await
        .ok()?
        .item
        .clone()?;
    serde_json::from_str(item.get("data")?.as_s().ok()?).ok()
}

/// The fix ticket's id for a test on a pull request: one per test per PR.
pub fn fix_ticket_id(pr: u64, test: &str) -> String {
    format!("{FIX_PREFIX}{pr}-{}", &common::content_hash(test)[..8])
}

/// Starts the separate pull request that fixes an unrelated test, once.
async fn start_fix(
    state: &WorkerState,
    msg: &TestIngest,
    pr: u64,
    run_id: &str,
    a: &Attribution,
) -> bool {
    let ticket_id = fix_ticket_id(pr, &a.test);
    let seed = Seed {
        owner: msg.repo_owner.clone(),
        repo: msg.repo_name.clone(),
        pr,
        sha: msg.head_sha.clone(),
        run_id: run_id.to_string(),
        attribution: a.clone(),
    };
    let ttl = (chrono::Utc::now() + chrono::Duration::days(30)).timestamp();
    // Written only when absent: a second failing run doesn't start a second fix.
    let put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", super::attr_s(&msg.team_id))
        .item("sk", super::attr_s(&seed_sk(&ticket_id)))
        .item(
            "data",
            super::attr_s(&serde_json::to_string(&seed).unwrap_or_default()),
        )
        .item(
            "ttl",
            aws_sdk_dynamodb::types::AttributeValue::N(ttl.to_string()),
        )
        .condition_expression("attribute_not_exists(sk)")
        .send()
        .await;
    if put.is_err() {
        return false;
    }
    let original = format!("{}/{}#{pr}", msg.repo_owner, msg.repo_name);
    let ticket = TicketMessage {
        team_id: msg.team_id.clone(),
        installation_id: msg.installation_id,
        source: TicketSource::Alert,
        ticket_id: ticket_id.clone(),
        title: common::truncate_str(&format!("Fix end-to-end test: {}", a.test), 200).to_string(),
        body: attribution::fix_ticket_body(a, &original),
        repo_owner: msg.repo_owner.clone(),
        repo_name: msg.repo_name.clone(),
        issue_number: 0,
        sender: "e2e".into(),
        requester: Some(common::requester::Requester::coderhelm(
            common::requester::E2E,
        )),
        base_branch: msg.default_branch.clone(),
        image_attachments: vec![],
        continuation: 0,
        continuation_run_id: None,
        first_attempt_ms: 0,
        model_retries: 0,
        ticket_update_note: None,
        ticket_history_note: None,
        plan_cache_key: None,
        team_instructions: String::new(),
        person_direction: None,
        answers: Vec::new(),
        new_run_id: None,
    };
    let Ok(body) = serde_json::to_string(&crate::models::WorkerMessage::Ticket(ticket)) else {
        return false;
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %e, "Could not start the test fix");
            false
        }
    }
}

/// Records what the original run is blocked on.
async fn note_blocked(state: &WorkerState, team_id: &str, run_id: &str, line: &str) {
    if run_id.is_empty() {
        return;
    }
    super::add_progress_note(state, team_id, run_id, line).await;
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression("SET ci_blocked_on = :b")
        .expression_attribute_values(":b", super::attr_s(line))
        .send()
        .await;
}

/// Handles failures on Coderhelm's own pull request. Returns the tests left
/// for the fix loop: the related ones and those nothing settled. Unrelated
/// ones are said once, fixed in a separate pull request and block CI.
pub async fn handle_own_pr(
    state: &WorkerState,
    gh: &GitHubClient,
    msg: &TestIngest,
    pr: u64,
    failing: &[&ParsedTest],
) -> Vec<Attribution> {
    let full = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let found = attribute(state, gh, msg, failing).await;
    if found.is_empty() {
        return found;
    }
    let mut rec = load(state, &msg.team_id, &full, &msg.head_sha)
        .await
        .unwrap_or_default();
    rec.pr = pr;
    if !rec.runs.contains(&msg.run_id) {
        rec.runs.push(msg.run_id);
    }
    // A test attributed on this commit before keeps its fix.
    for a in &found {
        match rec.tests.iter_mut().find(|t| t.test == a.test) {
            Some(t) => {
                let fix = t.fix_pr;
                *t = a.clone();
                t.fix_pr = fix;
            }
            None => rec.tests.push(a.clone()),
        }
    }
    // Tests that failed in this run but couldn't be attributed keep CI
    // failing: the record only blocks when every failure is unrelated.
    if found.len() < failing.len() {
        rec.tests.push(Attribution {
            test: "(not attributed)".into(),
            verdict: Verdict::Unknown,
            ..Default::default()
        });
    }
    save(state, &msg.team_id, &full, &msg.head_sha, &rec).await;
    let run_id = super::e2e::run_for_branch(state, &msg.team_id, &full, &msg.head_branch)
        .await
        .map(|(id, _)| id)
        .unwrap_or_default();
    for a in found.iter().filter(|a| a.verdict == Verdict::Unrelated) {
        if start_fix(state, msg, pr, &run_id, a).await {
            let _ = gh
                .create_issue_comment(&msg.repo_owner, &msg.repo_name, pr, &a.reply())
                .await;
            if let Some(l) = a.blocked_line() {
                note_blocked(state, &msg.team_id, &run_id, &l).await;
            }
        }
    }
    found
}

/// Called when a test-fix run opened its pull request: links it from the
/// original pull request, which is now blocked on it.
pub async fn link_fix(
    state: &WorkerState,
    gh: &GitHubClient,
    team_id: &str,
    ticket_id: &str,
    fix_pr: u64,
) {
    let Some(seed) = load_seed(state, team_id, ticket_id).await else {
        return;
    };
    let full = format!("{}/{}", seed.owner, seed.repo);
    let mut a = seed.attribution.clone();
    a.fix_pr = Some(fix_pr);
    if let Some(mut rec) = load(state, team_id, &full, &seed.sha).await {
        let Some(t) = rec.tests.iter_mut().find(|t| t.test == a.test) else {
            return;
        };
        // Said once: a re-run of the fix updates the same pull request.
        if t.fix_pr == Some(fix_pr) {
            return;
        }
        t.fix_pr = Some(fix_pr);
        save(state, team_id, &full, &seed.sha, &rec).await;
    }
    let _ = gh
        .create_issue_comment(&seed.owner, &seed.repo, seed.pr, &a.reply())
        .await;
    if let Some(l) = a.blocked_line() {
        note_blocked(state, team_id, &seed.run_id, &l).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn att(v: Verdict) -> Attribution {
        Attribution {
            test: "registers".into(),
            verdict: v,
            fix_pr: Some(9),
            ..Default::default()
        }
    }

    #[test]
    fn ci_is_blocked_only_when_every_failing_run_is_unrelated() {
        let rec = Record {
            pr: 7,
            runs: vec![100],
            tests: vec![att(Verdict::Unrelated)],
        };
        assert!(rec.blocks(&[100]));
        assert_eq!(rec.blocked_lines()[0], "Blocked on #9: `registers` fails for a reason outside this change; re-run E2E after #9 merges.");
        // Another failing run nothing attributed keeps CI failing.
        assert!(!rec.blocks(&[100, 200]));
        assert!(!rec.blocks(&[]));
        let mixed = Record {
            tests: vec![att(Verdict::Unrelated), att(Verdict::Unknown)],
            ..rec.clone()
        };
        assert!(!mixed.blocks(&[100]));
        let related = Record {
            tests: vec![att(Verdict::Related)],
            ..rec
        };
        assert!(!related.blocks(&[100]));
    }

    #[test]
    fn one_fix_ticket_per_test_per_pr() {
        let a = fix_ticket_id(7, "Classes › registers");
        assert!(a.starts_with("E2E-TESTFIX-7-"));
        assert_eq!(a, fix_ticket_id(7, "Classes › registers"));
        assert_ne!(a, fix_ticket_id(8, "Classes › registers"));
    }

    #[test]
    fn traces_give_their_requests() {
        use std::io::Write;
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut z = zip::ZipWriter::new(&mut buf);
            let o: zip::write::SimpleFileOptions = Default::default();
            z.start_file("0-trace.network", o).unwrap();
            z.write_all(br#"{"type":"resource-snapshot","snapshot":{"request":{"url":"https://staging.example.com/api/classes?term=fall"}}}"#)
                .unwrap();
            z.start_file("0-trace.trace", o).unwrap();
            z.write_all(b"{}").unwrap();
            z.finish().unwrap();
        }
        assert_eq!(trace_paths(&buf.into_inner()), vec!["/api/classes"]);
    }
}
