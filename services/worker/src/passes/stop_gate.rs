//! The stop rules (`common::stop_rules`) where a run would stop.
//!
//! - [`diagnosis`]: what the ticket itself settles (root cause, fix plan,
//!   acceptance criteria), read once per run and given to every agent.
//! - [`screen_questions`]: the planner's questions held to the rules before
//!   any is asked: no tooling questions, no environment differences, and
//!   with a diagnosed ticket only contradictions backed by evidence that
//!   checks out.
//! - [`repair`]: a tooling failure gets the package's dependencies
//!   installed with its own manager and the command run again, in the
//!   sandbox; [`tooling_fields`] is what a tooling failure records on the run.
//! - [`existing_work`] and [`finish_covered`]: an open pull request that
//!   already does the work ends the run as `covered_by_pr`.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex};

use aws_sdk_dynamodb::types::AttributeValue;
use common::e2e::test_package;
use common::run_questions::Question;
use common::run_status::EndedValues;
use common::stop_rules::existing_work::{self, Covered, OpenPr};
use common::stop_rules::screen::{self, Citation, Screened, TicketDiagnosis};
use common::stop_rules::tooling::{self, Repair, ToolingFailure};
use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;

use super::{attr_n, attr_s, RunUsageValues};

/// Diagnoses read this process, by run.
static DIAGNOSES: LazyLock<Mutex<HashMap<String, TicketDiagnosis>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The heading of the diagnosis block in the run's instructions.
const DIAGNOSIS_HEAD: &str = "## The ticket's own diagnosis (follow it)";

/// The ticket's own diagnosis for the run, read once (a model quotes it;
/// quotes not in the ticket's text or thread are dropped).
pub async fn diagnosis(
    state: &WorkerState,
    provider: &ModelProvider,
    msg: &TicketMessage,
    run_id: &str,
    thread: &str,
    usage: &mut TokenUsage,
) -> TicketDiagnosis {
    if let Some(d) = cached_diagnosis(run_id) {
        return d;
    }
    let text = format!("{}\n{}\n{}", msg.title, msg.body, thread);
    let d = match crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        screen::DIAGNOSIS_SYSTEM,
        &screen::diagnosis_prompt(&msg.title, &msg.body, thread),
        &screen::diagnosis_schema(),
        true,
        "low",
        1024,
        usage,
    )
    .await
    {
        Ok(v) => screen::parse_diagnosis(&v, &text),
        Err(e) => {
            warn!(run_id, error = %e, "Could not read the ticket's own diagnosis");
            TicketDiagnosis::default()
        }
    };
    info!(
        run_id,
        complete = d.complete(),
        separate = d.asks_separate_change(),
        "Ticket diagnosis read"
    );
    if let Ok(mut m) = DIAGNOSES.lock() {
        if m.len() > 500 {
            m.clear();
        }
        m.insert(run_id.to_string(), d.clone());
    }
    d
}

/// The diagnosis read for `run_id` in this process, if any.
pub fn cached_diagnosis(run_id: &str) -> Option<TicketDiagnosis> {
    DIAGNOSES.lock().ok()?.get(run_id).cloned()
}

/// The run's instructions with the diagnosis block once (replacing an
/// earlier one); unchanged when the ticket doesn't state a full diagnosis.
pub fn with_diagnosis(instructions: &str, d: &TicketDiagnosis) -> String {
    let block = d.markdown();
    if block.is_empty() {
        return instructions.to_string();
    }
    let base = match instructions.find(DIAGNOSIS_HEAD) {
        Some(i) => {
            let rest = &instructions[i + DIAGNOSIS_HEAD.len()..];
            let end = rest.find("\n## ").map(|j| i + DIAGNOSIS_HEAD.len() + j);
            let mut s = instructions[..i].trim_end().to_string();
            if let Some(e) = end {
                s.push_str("\n\n");
                s.push_str(instructions[e..].trim_start());
            }
            s
        }
        None => instructions.trim_end().to_string(),
    };
    if base.is_empty() {
        block
    } else {
        format!("{base}\n\n{block}")
    }
}

/// The "Where the tests run" section of the run's instructions, if any.
pub fn environment_section(instructions: &str) -> String {
    let head = "## Where the tests run";
    let Some(i) = instructions.find(head) else {
        return String::new();
    };
    let rest = &instructions[i..];
    let end = rest[head.len()..]
        .find("\n## ")
        .map(|j| j + head.len())
        .unwrap_or(rest.len());
    rest[..end].trim().to_string()
}

/// Whether a lookup citation names a lookup the run made (its id, or words
/// from its entry).
fn lookup_made(cited: &str, lookups: &[String]) -> bool {
    let c = cited.trim();
    if c.is_empty() {
        return false;
    }
    let id = format!("[{}]", c.to_uppercase());
    lookups
        .iter()
        .any(|l| l.contains(&id) || (c.len() >= 8 && l.to_lowercase().contains(&c.to_lowercase())))
}

/// The planner's questions held to the stop rules. A model reads what
/// kind of stop each one is; citations are checked against the repository
/// (the file exists and has that line) and the lookups the planner made.
#[allow(clippy::too_many_arguments)]
pub async fn screen_questions(
    state: &WorkerState,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    msg: &TicketMessage,
    github: &GitHubClient,
    run_id: &str,
    questions: &[Question],
    lookups: &[String],
) -> Screened {
    if questions.is_empty() {
        return Screened::default();
    }
    let d = cached_diagnosis(run_id).unwrap_or_default();
    let env = environment_section(&msg.team_instructions);
    let reads = match crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        screen::QUESTIONS_SYSTEM,
        &screen::questions_prompt(Some(&d), &env, questions),
        &screen::questions_schema(),
        true,
        "low",
        2048,
        usage,
    )
    .await
    {
        Ok(v) => screen::parse_reads(&v, questions),
        Err(e) => {
            warn!(run_id, error = %e, "Could not read the planner's questions");
            screen::parse_reads(&serde_json::Value::Null, questions)
        }
    };
    // Each cited file read once: does it have that line?
    let mut files: HashMap<String, usize> = HashMap::new();
    let cited: Vec<Citation> = questions
        .iter()
        .flat_map(|q| q.evidence.iter())
        .chain(reads.iter().flat_map(|r| r.evidence.iter()))
        .filter_map(|e| screen::citation(e))
        .collect();
    for c in &cited {
        if let Citation::File { path, .. } = c {
            if files.contains_key(path) || files.len() >= 10 {
                continue;
            }
            let lines = github
                .read_file(&msg.repo_owner, &msg.repo_name, path, &msg.base_branch)
                .await
                .map(|t| t.lines().count())
                .unwrap_or(0);
            files.insert(path.clone(), lines);
        }
    }
    let real: HashSet<String> = cited
        .iter()
        .filter(|c| match c {
            Citation::File { path, line } => files.get(path).is_some_and(|n| *line as usize <= *n),
            Citation::Lookup(what) => lookup_made(what, lookups),
        })
        .map(|c| format!("{c:?}"))
        .collect();
    let s = screen::screen(questions, &reads, d.complete(), &|c| {
        real.contains(&format!("{c:?}"))
    });
    if s.changed() {
        info!(
            run_id,
            asked = s.ask.len(),
            dropped = s.dropped.len(),
            tooling = s.tooling.len(),
            "Stop rules held questions back"
        );
        for n in s.notes() {
            super::add_progress_note(state, &msg.team_id, run_id, &n).await;
        }
    }
    s
}

/// Installs the dependencies of the package the failed command runs in,
/// with its own manager, and runs the command again in the sandbox, at
/// `git_ref`. `Fixed` when it passes now; otherwise the failure to end the
/// run with, naming what was tried.
pub async fn repair(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    provider: &ModelProvider,
    run_id: &str,
    git_ref: &str,
    failure: &ToolingFailure,
) -> Repair {
    let give_up = |why: &str| {
        let mut f = failure.clone();
        f.tried.push(why.to_string());
        Repair::Failed(f)
    };
    if state.config.sandbox_bucket_name.is_empty() || state.config.sandbox_project_name.is_empty() {
        return give_up("no sandbox is set up to install and retry in");
    }
    let (owner, repo) = (&msg.repo_owner, &msg.repo_name);
    let Ok(tree) = github.get_tree(owner, repo, git_ref).await else {
        return give_up("the branch's files couldn't be listed");
    };
    let tree: Vec<String> = tree.into_iter().map(|e| e.path).collect();
    let dir = tooling::command_dir(&failure.command).unwrap_or_default();
    let manifest = if dir.is_empty() {
        "package.json".to_string()
    } else {
        format!("{dir}/package.json")
    };
    let mut texts: HashMap<String, String> = HashMap::new();
    for p in [manifest.clone(), "package.json".to_string()] {
        if tree.contains(&p) && !texts.contains_key(&p) {
            if let Ok(t) = github.read_file(owner, repo, &p, git_ref).await {
                texts.insert(p, t);
            }
        }
    }
    let Some(pkg) = test_package::package_of(&tree, &manifest, &|p| texts.get(p).cloned()) else {
        return give_up("no package.json was found for the command's folder");
    };
    let steps = tooling::repair_steps(&pkg, &failure.command);
    let gate = crate::agent::policy::gate(
        state,
        provider.scope.ctx(common::agent_policy::Agent::Coder),
        None,
    );
    for s in &steps {
        match gate.check_command(&s.cmd, "Repair the run's tooling").await {
            common::agent_policy::Outcome::Run { .. } => {}
            common::agent_policy::Outcome::Refused { message, .. }
            | common::agent_policy::Outcome::Pending { message, .. } => {
                return give_up(&format!("`{}` not run ({message})", s.cmd));
            }
            common::agent_policy::Outcome::Stop(_) => {
                return give_up("the team's agent permissions stopped the run");
            }
        }
    }
    let head = github
        .get_ref(owner, repo, git_ref)
        .await
        .unwrap_or_else(|_| git_ref.to_string());
    let sandbox = crate::clients::sandbox::SandboxClient::new(
        &state.codebuild,
        &state.s3,
        &state.logs,
        &state.config.sandbox_bucket_name,
        &state.config.sandbox_project_name,
    )
    .with_egress(crate::agent::policy::sandbox_egress(state, &msg.team_id).await);
    let source = match sandbox
        .ensure_source(run_id, &head, || {
            github.download_tarball(owner, repo, &head)
        })
        .await
    {
        Ok(k) => k,
        Err(e) => {
            return give_up(&format!(
                "the branch couldn't be prepared in the sandbox ({e})"
            ))
        }
    };
    let script = test_package::sandbox_script(&steps, None);
    let node = super::detect_node_version(github, owner, repo, git_ref).await;
    super::add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!(
            "Tooling failure on `{}`: installing the package's dependencies with its own manager and trying again",
            failure.command
        ),
    )
    .await;
    match sandbox
        .run_checks(&source, &script, node.as_deref(), None, None)
        .await
    {
        Ok(o) if o.ran => {
            let r = tooling::repair_result(&steps, &o.output, failure);
            info!(
                run_id,
                fixed = matches!(r, Repair::Fixed { .. }),
                "Tooling repair ran"
            );
            r
        }
        Ok(o) => give_up(&format!("the sandbox gave no result ({})", o.reason)),
        Err(e) => give_up(&format!("the sandbox couldn't run ({e})")),
    }
}

/// What a tooling failure records on the run, beside the failed status and
/// its reason.
pub fn tooling_fields(reason: &str) -> Vec<(&'static str, AttributeValue)> {
    match ToolingFailure::from_reason(reason) {
        Some(f) => vec![
            ("stop_kind", attr_s(tooling::STOP_KIND)),
            (
                "tooling_failure",
                attr_s(&serde_json::to_string(&f).unwrap_or_default()),
            ),
        ],
        None => Vec::new(),
    }
}

/// An open pull request (any author) that already does the run's work.
/// `own_files` are the run's changed files (empty before it has any).
/// PRs on `own_branch`, CoderHelm's own earlier PRs for the ticket
/// (`own_prs`) and PRs a person already decided about (`decided`, as
/// `owner/name#n`) are left out; the ticket asking for a separate change
/// skips the check.
#[allow(clippy::too_many_arguments)]
pub async fn existing_work(
    state: &WorkerState,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    msg: &TicketMessage,
    github: &GitHubClient,
    run_id: &str,
    own_branch: &str,
    own_prs: &[u64],
    decided: &[String],
    tests: &[String],
    spec_files: &[String],
    own_files: &[String],
    failure: &str,
) -> Option<Covered> {
    if msg.person_direction.is_some() {
        return None;
    }
    if cached_diagnosis(run_id).is_some_and(|d| d.asks_separate_change()) {
        info!(
            run_id,
            "The ticket asks for a separate change: open PRs not checked"
        );
        return None;
    }
    let (owner, repo) = (&msg.repo_owner, &msg.repo_name);
    let full = format!("{owner}/{repo}");
    let list = github.list_open_pulls(owner, repo).await.ok()?;
    let mut prs: Vec<OpenPr> = list
        .as_array()?
        .iter()
        .filter_map(|p| OpenPr::from_github(&full, p))
        .take(60)
        .collect();
    let skip = |p: &OpenPr| {
        (!own_branch.is_empty() && p.head == own_branch) || own_prs.contains(&p.number)
    };
    // Changed files are read only when they can matter.
    let need_files = !spec_files.is_empty() || !own_files.is_empty();
    let mut patches: HashMap<u64, String> = HashMap::new();
    if need_files {
        for p in prs.iter_mut().filter(|p| !skip(p)).take(20) {
            if let Ok(files) = github.list_pull_files(owner, repo, p.number).await {
                let patch: String = files
                    .iter()
                    .filter(|(f, _)| {
                        spec_files
                            .iter()
                            .chain(own_files)
                            .any(|s| s.ends_with(f.as_str()) || f.ends_with(s.as_str()))
                    })
                    .map(|(f, patch)| format!("--- {f}\n{patch}\n"))
                    .collect();
                patches.insert(p.number, patch);
                p.files = files.into_iter().map(|(f, _)| f).collect();
            }
        }
    }
    let found =
        existing_work::candidates(&prs, &skip, &msg.ticket_id, tests, spec_files, own_files, 3);
    if found.is_empty() {
        return None;
    }
    let mut read = Vec::new();
    for (pr, signals) in found {
        let v = match crate::agent::anthropic::converse_json(
            &provider.client(),
            &state.config.light_model_id,
            existing_work::SYSTEM,
            &existing_work::prompt(
                &msg.title,
                &msg.body,
                failure,
                &pr,
                &signals,
                patches.get(&pr.number).map(String::as_str).unwrap_or(""),
            ),
            &existing_work::schema(),
            true,
            "low",
            512,
            usage,
        )
        .await
        {
            Ok(v) => existing_work::parse_verdict(&v),
            Err(e) => {
                warn!(run_id, pr = pr.number, error = %e, "Could not compare an open PR with the ticket");
                continue;
            }
        };
        info!(run_id, pr = pr.number, addresses = v.addresses, reason = %v.reason, "Open PR compared with the ticket");
        read.push((pr, signals, v));
    }
    existing_work::decide(&read, false, &|p| decided.contains(&p.subject()))
}

/// Ends the run as covered by an open pull request: the summary on the
/// run, "Already being fixed in #N" on the ticket, status `covered_by_pr`.
#[allow(clippy::too_many_arguments)]
pub async fn finish_covered(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    github: &GitHubClient,
    covered: &Covered,
    usage: &TokenUsage,
    start: &std::time::Instant,
) {
    let summary = covered.summary();
    info!(
        run_id,
        pr = covered.pr.number,
        "Run covered by an open pull request"
    );
    super::add_progress_note(state, &msg.team_id, run_id, &summary).await;
    match msg.source {
        TicketSource::Github if msg.issue_number > 0 => {
            if let Err(e) = github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.issue_number,
                    &covered.ticket_note(true),
                )
                .await
            {
                warn!(run_id, error = %e, "Could not note the covering PR on the issue");
            }
        }
        TicketSource::Jira if !msg.ticket_id.is_empty() => {
            if let Err(e) = super::post_jira_comment(
                state,
                &msg.team_id,
                &msg.ticket_id,
                &covered.ticket_note(false),
                "covered",
                "Already being fixed",
            )
            .await
            {
                warn!(run_id, error = %e, "Could not note the covering PR on the ticket");
            }
        }
        _ => {}
    }
    let record = serde_json::json!({
        "repo": covered.pr.repo,
        "number": covered.pr.number,
        "url": covered.pr.url,
        "title": covered.pr.title,
        "author": covered.pr.author,
        "reason": covered.reason,
        "signals": covered.signals.lines(),
    });
    let status = common::run_status::COVERED_BY_PR;
    let delta = usage.unrecorded();
    let written = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, covered_by = :cb, error_message = :em, \
             tokens_in = if_not_exists(tokens_in, :uzero) + :ti, tokens_out = if_not_exists(tokens_out, :uzero) + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :uzero) + :crt, \
             cache_write_tokens = if_not_exists(cache_write_tokens, :uzero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :uzero) + :c, duration_s = :d, updated_at = :t, current_pass = :cp",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .ended_values()
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", attr_s(status))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
        .expression_attribute_values(":cb", attr_s(&record.to_string()))
        .expression_attribute_values(":em", attr_s(&summary))
        .usage_values(&delta)
        .expression_attribute_values(":d", attr_n(start.elapsed().as_secs()))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":cp", attr_s("done"))
        .send()
        .await;
    if written.is_ok() {
        usage.mark_recorded(&delta);
    }
    super::webhooks::emit_run(state, &msg.team_id, run_id, status).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete() -> TicketDiagnosis {
        TicketDiagnosis {
            root_cause: "test bug, not code bug".into(),
            fix_plan: "select by name".into(),
            acceptance: "passes 20 times".into(),
            separate_change: String::new(),
        }
    }

    #[test]
    fn the_diagnosis_goes_in_the_instructions_once() {
        let once = with_diagnosis("Team rules.", &complete());
        assert!(once.starts_with("Team rules.\n\n## The ticket's own diagnosis"));
        let twice = with_diagnosis(&format!("{once}\n\n## Other\nkept"), &complete());
        assert_eq!(twice.matches(DIAGNOSIS_HEAD).count(), 1);
        assert!(twice.contains("## Other\nkept"));
        assert_eq!(
            with_diagnosis("Team rules.", &TicketDiagnosis::default()),
            "Team rules."
        );
    }

    #[test]
    fn the_environment_section_is_cut_from_the_instructions() {
        let text = "x\n\n## Where the tests run\n- Base address: https://staging.example.test\n\nrule\n## Next\ny";
        assert_eq!(
            environment_section(text),
            "## Where the tests run\n- Base address: https://staging.example.test\n\nrule"
        );
        assert_eq!(environment_section("nothing"), "");
    }

    #[test]
    fn lookups_are_matched_by_id_or_words() {
        let made = vec!["- [L2] cards where name = 'Mine' → 0 rows".to_string()];
        assert!(lookup_made("L2", &made));
        assert!(lookup_made("cards where name = 'Mine'", &made));
        assert!(!lookup_made("L3", &made));
        assert!(!lookup_made("", &made));
    }

    #[test]
    fn a_failing_test_run_gives_the_existing_work_check_its_tests_and_specs() {
        use common::e2e::failure::FailureExtract;
        let ev = super::super::test_fix::Evidence {
            read: common::e2e::fix_ticket::TicketRead {
                about_tests: true,
                flaky: false,
                tests: vec![
                    "shows the card it created".into(),
                    "e2e/tests/cards.spec.ts".into(),
                ],
            },
            failures: vec![FailureExtract {
                test: "cards list › shows the card it created".into(),
                file: "tests/cards.spec.ts".into(),
                error: "Expected: 'Mine'".into(),
                ..Default::default()
            }],
            spec_files: vec!["e2e/tests/cards.spec.ts".into()],
            ..Default::default()
        };
        let (tests, specs, failure) = super::super::existing_work_inputs(Some(&ev));
        assert_eq!(
            tests,
            vec![
                "shows the card it created".to_string(),
                "cards list › shows the card it created".to_string()
            ]
        );
        assert_eq!(
            specs,
            vec![
                "e2e/tests/cards.spec.ts".to_string(),
                "tests/cards.spec.ts".to_string()
            ]
        );
        assert_eq!(failure, "Expected: 'Mine'");
        assert_eq!(
            super::super::existing_work_inputs(None),
            (Vec::new(), Vec::new(), String::new())
        );
    }

    #[test]
    fn the_gate_keeps_a_tooling_failure_in_its_own_words() {
        let f = ToolingFailure::new("cd 'e2e' && pnpm exec tsc", "sh: 1: tsc: not found");
        assert_eq!(
            super::super::test_fix::blocked_reason(f.reason()),
            f.reason()
        );
        assert!(super::super::test_fix::blocked_reason("waits added".into())
            .starts_with("The failing-test rules blocked the pull request: "));
    }

    #[test]
    fn a_tooling_reason_records_its_kind_and_command() {
        let f = ToolingFailure::new("cd 'e2e' && npx --no-install tsc", "sh: 1: tsc: not found");
        let fields = tooling_fields(&f.reason());
        assert_eq!(fields[0].0, "stop_kind");
        assert_eq!(fields[0].1.as_s().unwrap(), "tooling");
        assert!(fields[1].1.as_s().unwrap().contains("tsc: not found"));
        assert!(tooling_fields("The plan failed").is_empty());
    }
}
