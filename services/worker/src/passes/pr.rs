use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::agent::{llm, provider};
use crate::clients::github::{FileOp, GitHubClient};
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::passes::checks::CiCommand;
use crate::passes::plan::PlanResult;
use crate::passes::syntax_check;
use crate::WorkerState;

#[allow(dead_code)]
pub struct PrResult {
    pub pr_number: u64,
    pub pr_url: String,
    pub branch: String,
    pub draft: bool,
    pub node_id: String,
}

/// What the run proved about the change, for its description: what it
/// looked up (`evidence`, the writer's source for an Evidence list), what is
/// still open (`gaps`, the Open items section at the bottom), what only a
/// person can check (`manual`, steps for the Verification section), the
/// check commands the repository's CI runs on the PR (`ci`), which the
/// Verification section never asks a person to run, and the existing blocks
/// the change copied (`mirrors`, from the coder's notes, rendered under
/// Changes).
pub struct Proof<'a> {
    pub evidence: &'a str,
    pub gaps: &'a str,
    pub manual: &'a str,
    pub ci: &'a [CiCommand],
    pub mirrors: &'a [super::completeness::Mirror],
    /// For a run started from a log finding: what its evidence step found.
    /// The description is checked against it (see `log_fix_checks`).
    pub log: Option<&'a super::log_evidence::LogEvidence>,
    /// The live link and redirect checks' section (see `link_checks`),
    /// empty when the change has no redirects or links.
    pub links: &'a str,
    /// For a failing- or flaky-test fix: the failure, the counts and the
    /// checks (see `test_fix`).
    pub test_fix: Option<&'a super::test_fix::PrFacts>,
    /// For a new feature: earlier attempts, reuse and registration, staging
    /// content, the verification checklist and open product questions (see
    /// `feature`).
    pub feature: Option<&'a super::feature::PrFacts>,
}

/// What the writer is told about the checks CI runs on the PR: never a
/// Verification step asking a person to run one; say CI runs it. Empty when
/// CI runs none.
fn ci_instructions(ci: &[CiCommand]) -> String {
    if ci.is_empty() {
        return String::new();
    }
    format!(
        "\n\n## Checks the repository's CI runs on this pull request\nCI runs each of these on \
         this PR and its result shows there. In the Verification section, never tell a person to \
         run any of them (or the same tool another way). Where verifying the change needs one, \
         write that CI runs it, e.g. \"CI runs `<command>` on this PR.\"; for a plan or preview, \
         \"CI runs `<command>` on this PR; read its output in the PR's checks.\"\n{}",
        super::proof::ci_runs(ci).trim_end()
    )
}

/// Whether a description line is a section heading: a `#` heading, or a
/// line that is only bold text (`**Verification**`, `4. **Verification**:`).
fn is_heading(line: &str) -> bool {
    let t = line.trim();
    if t.starts_with('#') {
        return true;
    }
    let t = t.trim_start_matches(|c: char| c.is_ascii_digit());
    let t = t.strip_prefix(". ").unwrap_or(t).trim_start();
    let Some(rest) = t.strip_prefix("**") else {
        return false;
    };
    match rest.find("**") {
        Some(end) => {
            let after = rest[end + 2..].trim();
            after.is_empty() || after == ":" || after.starts_with('—') || after.starts_with("- ")
        }
        None => false,
    }
}

/// Whether a heading names the Verification section.
fn is_verification_heading(line: &str) -> bool {
    let l = line.to_lowercase();
    l.contains("verif") || l.contains("how to test") || l.contains("test plan")
}

/// The list marker a line starts with (`1. `, `* `, `- `), with its indent.
fn list_marker(line: &str) -> &str {
    let indent = line.len() - line.trim_start().len();
    let t = &line[indent..];
    let digits = t.len() - t.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    let marker = if digits > 0 && t[digits..].starts_with(". ") {
        digits + 2
    } else if t.starts_with("* ") || t.starts_with("- ") {
        2
    } else {
        0
    };
    &line[..indent + marker]
}

/// The CI command a line asks a person to run: a command in backticks that
/// runs the same tool (by the tool table) as a command CI runs on the PR.
fn ci_command_in<'a>(line: &str, ci: &'a [CiCommand]) -> Option<&'a CiCommand> {
    line.split('`').skip(1).step_by(2).find_map(|code| {
        let m = super::checks::classify_command(code)?;
        ci.iter().find(|c| c.tool == m.tool)
    })
}

/// What a Verification step says about a check CI runs on the PR.
fn ci_step(c: &CiCommand) -> String {
    if c.kind == super::checks::CheckKind::Plan {
        format!(
            "CI runs {} on this PR; read its output in the PR's checks.",
            c.display()
        )
    } else {
        format!("CI runs {} on this PR.", c.display())
    }
}

/// The description with each Verification step that tells a person to run a
/// check CI already runs on the PR said as what it is: "CI runs `X` on this
/// PR". A second step for the same CI command is dropped. Only commands in
/// backticks are read, through the tool table; the rest of the description
/// is left as it is.
pub fn ci_verification(body: &str, ci: &[CiCommand]) -> String {
    if ci.is_empty() {
        return body.to_string();
    }
    let mut out: Vec<String> = Vec::new();
    let mut in_verification = false;
    let mut said: Vec<&str> = Vec::new();
    for line in body.lines() {
        if is_heading(line) {
            in_verification = is_verification_heading(line);
            out.push(line.to_string());
            continue;
        }
        if in_verification {
            if let Some(c) = ci_command_in(line, ci) {
                if !said.contains(&c.id.as_str()) {
                    said.push(&c.id);
                    out.push(format!("{}{}", list_marker(line), ci_step(c)));
                }
                continue;
            }
        }
        out.push(line.to_string());
    }
    let mut joined = out.join("\n");
    if body.ends_with('\n') {
        joined.push('\n');
    }
    joined
}

/// The shape every description follows, and what the writer must not write
/// because CoderHelm adds it from the plan and the coder's notes.
const SHAPE_RULES: &str = "\
- CoderHelm adds these sections itself, from the plan and the coder's notes: the existing block \
the change copies, the merge order of a multi-repo change, **Not in this PR** and **Open items**. \
Don't write them, and don't add a warning or note banner anywhere.
- Write real values: every path, name and id in full, with its environment filled in \
(`/app/prod/db-password`, never `/app//db-password`), never a template left as `${env}`, `<env>`, \
`{}` or empty backticks.
- When the change touches more than one environment, the Evidence list has one bullet per \
environment: what confirmed it, or exactly which environment couldn't be checked and what access \
is missing.";

/// What the writer is told about the checks only a person can do: each one
/// a numbered Verification step, and nowhere else. Empty when there are none.
fn manual_instructions(manual: &str) -> String {
    if manual.trim().is_empty() {
        return String::new();
    }
    format!(
        "\n\n## Checks only a person can do\nAdd each of these as a numbered step in the \
         Verification section (and nowhere else in the description), saying what to do and what \
         to look for:\n{}",
        manual.trim_end()
    )
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    plan: &PlanResult,
    voice: &str,
    proof: &Proof<'_>,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    run_id: &str,
) -> Result<PrResult, Box<dyn std::error::Error + Send + Sync>> {
    // Get diff for body summary
    let diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await?;
    let diff_summary = format_diff_summary(&diff);

    // Check for PR template in the repo
    let pr_template =
        fetch_pr_template(github, &msg.repo_owner, &msg.repo_name, &msg.base_branch).await;

    // Generate PR body via LLM
    let voice_block = if voice.is_empty() {
        String::new()
    } else {
        format!("\n\nIMPORTANT — Match the team's voice and tone as described below:\n{voice}")
    };
    // The team's standards set to always apply to PR descriptions (a
    // writing guide, say), in full; path-scoped ones match this diff.
    let changed: Vec<String> = diff["files"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| f["filename"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let standards = super::skills::TeamSkills::load(
        state,
        &msg.team_id,
        super::skills::Reader::Writer,
        super::skills::ticket_origin(
            &msg.repo_owner,
            &msg.repo_name,
            &msg.ticket_id,
            msg.issue_number,
        ),
        super::skills::Work::new(
            format!("{}/{}", msg.repo_owner, msg.repo_name),
            &["pr"],
            changed,
        ),
    )
    .await
    .map(|s| s.with_run(Some(run_id)));
    let standards_block = match &standards {
        Some(s) => {
            s.record_applied(state).await;
            s.standards_section()
        }
        None => String::new(),
    };
    let system = writer_system(&voice_block, &standards_block, &msg.team_instructions);

    let ticket_ref = match msg.source {
        TicketSource::Github => format!("#{}", msg.issue_number),
        TicketSource::Jira | TicketSource::Alert => msg.ticket_id.clone(),
    };

    let template_block = if let Some(ref tmpl) = pr_template {
        format!(
            "\n\n## PR Template\nThe repository has a pull request template. Follow its structure and fill in the sections:\n\n```\n{tmpl}\n```\n\nFill in ALL sections from the template. Replace placeholder text with actual content."
        )
    } else {
        String::new()
    };

    // A small failing-test fix is described in four sections (see
    // `test_fix::writer_block`), unless the repository has a template.
    let small_test_fix = pr_template.is_none() && proof.test_fix.is_some_and(|t| t.small);
    let instructions = if small_test_fix {
        "Follow the four sections given under Failing-test evidence above."
    } else if pr_template.is_some() {
        "Follow the PR template above. Fill in every section with relevant content from this change. Remove any placeholder/instruction text."
    } else {
        "Write a PR description following this structure, and nothing else:\n\n\
         1. **Summary** — One sentence: the problem and what this change does about it.\n\
         2. **Changes** — Bolded per-area headers with bullet details. Keep it tight.\n\
         3. **Risk** — State risk level in bold (**Low**, **Medium**), then why it's safe.\n\
         4. **Verification** — Numbered steps to verify the change."
    };
    let evidence_block = format!(
        "{}{}{}{}",
        evidence_instructions(proof.evidence),
        manual_instructions(proof.manual),
        ci_instructions(proof.ci),
        proof
            .test_fix
            .map(super::test_fix::writer_block)
            .unwrap_or_default()
    );

    let diff_patches = format_diff_patches(&diff);
    let prompt = format!(
        r#"Write a concise pull request description for ticket {ticket_ref}: {title}

## Plan (context — what was INTENDED)
{summary}

## Files Changed
{diff_summary}

## The full diff (the source of truth — read all of it, describe THIS)
{diff_patches}{template_block}{evidence_block}

## Instructions
Describe what the DIFF actually does, after reading every file in it. Where the diff differs from the plan, the diff wins. Name every behavior change, including ones the plan didn't mention (a changed default, a renamed id, a new constraint, a migration).
{instructions}

Rules:
- Keep it short. Don't pad short changes with long descriptions.
- No filler phrases, no hedging, no emojis.
- Backticks for file paths, function names, env vars, CLI commands.
- Bold for emphasis on key concepts.
- Use asterisks (*) for bullet lists, never dashes (-).
{shape_rules}

Return ONLY the markdown body text."#,
        ticket_ref = ticket_ref,
        title = msg.title,
        summary = plan.proposal,
        diff_summary = diff_summary,
        template_block = template_block,
        instructions = instructions,
        shape_rules = SHAPE_RULES,
    );

    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

    let model_id = provider.primary_model_id();
    let write_opts = llm::ConverseOptions {
        max_turns: 1,
        max_tokens: 4096,
        deadline: None,
        effort: Some("low"),
    };
    let mut body_text = provider::converse(
        state,
        provider,
        common::agent_policy::Agent::Coder,
        model_id,
        &system,
        &mut messages,
        &[],
        &super::triage::NoOpExecutor,
        usage,
        write_opts,
        None,
        None,
    )
    .await?;

    // The team's prose-review agents read the draft (at most two, each
    // with a time limit); with gaps, the writer gets one revision on the
    // same conversation. Fails open: the draft stands.
    if let Some(s) = standards
        .as_ref()
        .filter(|s| !s.reviewers_for("prose").is_empty())
    {
        let started = std::time::Instant::now();
        let before = usage.clone();
        let host = super::agent_tools::Host {
            reads: Some(super::agent_tools::RepoReads {
                github,
                owner: &msg.repo_owner,
                repo: &msg.repo_name,
                git_ref: branch,
                http: &state.http,
            }),
            parent: None,
            offered: &[],
        };
        let draft = format!(
            "## Pull request description draft\n{body_text}\n\n## What the change does (files)\n{diff_summary}"
        );
        let reviews = s
            .review(state, "prose", "pull request description", &draft, &host)
            .await;
        s.drain_usage_into(usage);
        let advice = super::skills::advice_block(&reviews);
        let mut revised = false;
        if !advice.is_empty() {
            messages.push((
                "user".to_string(),
                vec![serde_json::json!({"type": "text", "text": format!(
                    "{advice}\n\nRevise the description once with these notes, only where a note \
                     points to a real problem. Keep every rule above. Return ONLY the full \
                     markdown body again."
                )})],
            ));
            match provider::converse(
                state,
                provider,
                common::agent_policy::Agent::Coder,
                model_id,
                &system,
                &mut messages,
                &[],
                &super::triage::NoOpExecutor,
                usage,
                write_opts,
                None,
                None,
            )
            .await
            {
                Ok(text) if !text.trim().is_empty() => {
                    body_text = text;
                    revised = true;
                }
                Ok(_) => warn!(
                    run_id,
                    "PR description revision came back empty; the draft stands"
                ),
                Err(e) => {
                    warn!(run_id, error = %e, "PR description revision failed; the draft stands")
                }
            }
        }
        let cost = (usage.cost_usd - before.cost_usd).max(0.0);
        super::write_pass_trace_with(
            state,
            &msg.team_id,
            run_id,
            "pr_review",
            started,
            &before,
            usage,
            None,
            Some(&super::skills::review_detail("prose", &reviews, revised)),
        )
        .await;
        super::add_progress_note(
            state,
            &msg.team_id,
            run_id,
            &super::skills::review_note("pull request description", &reviews, revised, cost),
        )
        .await;
    }

    // A fix from a log finding: the description is checked against the
    // run's evidence, sent back once, and rewritten where it still fails.
    let body_text = match proof.log {
        Some(ev) => {
            super::log_fix_checks::enforce(
                state,
                msg,
                run_id,
                &diff,
                body_text,
                ev,
                provider,
                &system,
                &mut messages,
                write_opts,
                usage,
            )
            .await
        }
        None => body_text,
    };

    // The description names only entries and files the diff contains.
    let body_text = super::link_checks::enforce_claims(
        state,
        provider,
        &system,
        &mut messages,
        write_opts,
        usage,
        &diff,
        body_text,
    )
    .await;

    // A Verification step never asks a person to run what CI runs on the PR.
    let body_text = ci_verification(&body_text, proof.ci);
    // A failing-test fix quotes the real error; a small one stays four
    // sections.
    let body_text = match proof.test_fix {
        Some(t) => super::test_fix::shape_body(body_text, t),
        None => body_text,
    };
    // A feature's checklist, staging content and open questions are
    // written from what the run checked, never by the writer.
    let body_text = match proof.feature {
        Some(f) => super::feature::shape_body(body_text, f),
        None => body_text,
    };

    // Add issue link for GitHub tickets.
    // Strip any "Closes #N" the model may have included to avoid duplication.
    let clean_body: String = body_text
        .lines()
        .filter(|line| {
            let trimmed = line.trim().to_lowercase();
            !(trimmed.starts_with("closes #")
                || trimmed.starts_with("fixes #")
                || trimmed.starts_with("resolves #"))
        })
        .collect::<Vec<_>>()
        .join("\n");
    let clean_body = clean_body.trim();

    // A fix from a log finding says where it came from, first, in its own
    // words (not the writer's): "Started automatically from log finding X".
    let log_ticket = if matches!(msg.source, TicketSource::Alert) {
        common::log_findings::load_ticket(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            &msg.ticket_id,
        )
        .await
    } else {
        None
    };
    let clean_body = match &log_ticket {
        Some(t) => {
            let origin = common::log_findings::origin_line(t.auto, &t.title);
            let rest: String = clean_body
                .lines()
                .filter(|l| l.trim() != origin)
                .collect::<Vec<_>>()
                .join("\n");
            format!("{origin}\n\n{}", rest.trim())
        }
        None => clean_body.to_string(),
    };
    let clean_body = clean_body.as_str();
    let full_body = if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        format!(
            "Closes #{number}\n\n{clean_body}",
            number = msg.issue_number,
        )
    } else {
        // Avoid duplicating "Source ticket:" if it's already in the body
        if clean_body.contains(&format!("Source ticket: {}", msg.ticket_id)) {
            clean_body.to_string()
        } else {
            format!("Source ticket: {}\n\n{clean_body}", msg.ticket_id)
        }
    };
    // CoderHelm's own sections go in their places around the writer's: the
    // blocks the change copied (under Changes), every step outside this PR
    // with its owner (the plan's checklist, and a change outside the code),
    // then Open items at the bottom. Which team agents the planner and coder
    // asked closes it, where reviewers look.
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let full_body = format!(
        "{}{}",
        super::completeness::assemble(&full_body, &owned_sections(plan, proof, &repo)),
        super::skills::run_consulted(state, &msg.team_id, run_id).await
    );

    // The PR title and squash commit follow the team's templates.
    let conventions = super::load_git_conventions(state, &msg.team_id, &msg.ticket_id).await;
    let tokens = super::ticket_tokens(msg);
    let title = pr_title(&conventions, &tokens, msg);

    // Check if a PR already exists for this branch (e.g. from a re-run or retry)
    let existing_pr = github
        .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await?;

    let (pr_number, pr_url, node_id) = if let Some(pr_data) = existing_pr {
        let number = pr_data
            .get("number")
            .and_then(|v| v.as_u64())
            .ok_or("Missing PR number")?;
        let url = pr_data
            .get("html_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let nid = pr_data
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        info!(pr_number = number, pr_url = %url, "Using existing PR");
        // A re-run changed the code: the description is rewritten from this
        // diff, never left describing the earlier version.
        if let Err(e) = github
            .update_pr_body(&msg.repo_owner, &msg.repo_name, number, &full_body)
            .await
        {
            warn!(pr_number = number, error = %e, "Could not update the existing PR's description");
        }
        (number, url, nid)
    } else if let Some(pr_data) = reopen_prior_pr(github, msg, branch).await {
        let number = pr_data
            .get("number")
            .and_then(|v| v.as_u64())
            .ok_or("Missing PR number")?;
        let url = pr_data
            .get("html_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let nid = pr_data
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        info!(pr_number = number, pr_url = %url, "Reopened prior PR for branch");
        (number, url, nid)
    } else {
        // Squash the run's many per-tool-call commits into one clean commit
        // before opening the PR. Safe here: the main flow is sequential and
        // single-writer at this point (webhook-driven CI-fix/feedback writes
        // only start after the PR exists). Best-effort — a failure just leaves
        // the noisier history. Skipped for existing/reopened PRs above to avoid
        // rewriting a PR a human may already be reviewing.
        let squash_msg = squash_message(&conventions, &tokens, msg);
        if let Err(e) = github
            .squash_branch(
                &msg.repo_owner,
                &msg.repo_name,
                branch,
                &msg.base_branch,
                &squash_msg,
            )
            .await
        {
            warn!(branch, error = %e, "Squash before PR failed — proceeding with full history");
        }

        // Create draft PR — CI triggers on PR creation, and we'll mark it ready after tests/review pass
        let pr_data = github
            .create_pull_request(
                &msg.repo_owner,
                &msg.repo_name,
                &title,
                &full_body,
                branch,
                &msg.base_branch,
                true,
            )
            .await?;

        let number = pr_data
            .get("number")
            .and_then(|v| v.as_u64())
            .ok_or("Missing PR number")?;
        let url = pr_data
            .get("html_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let nid = pr_data
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        info!(pr_number = number, pr_url = %url, "PR created");
        if let Some(t) = &log_ticket {
            super::log_autofix::notify_pr(
                state,
                &msg.team_id,
                t,
                &format!("{}/{}", msg.repo_owner, msg.repo_name),
                number,
                &title,
                &url,
            )
            .await;
        } else if matches!(msg.source, TicketSource::Alert) {
            notify_alert_pr(state, msg, number, &title, &url).await;
        }
        (number, url, nid)
    };

    // A failing-test fix leaves draft only when its checks and repeat run
    // pass on its head (see `test_fix::ready_gate`).
    if let Some(t) = proof.test_fix {
        super::test_fix::register_pr(state, msg, run_id, pr_number, t).await;
    }
    // A feature stays a draft until its verification checklist passes (see
    // `feature::ready_hold`).
    if let Some(f) = proof.feature {
        super::feature::register_pr(state, msg, run_id, pr_number, f).await;
    }

    // Label CoderHelm's own PR at creation — this is the PR MAKER, so it owns the
    // PR's labels. Two things get added here, only when the reviewer is enabled for
    // the repo; best-effort (a failure just means no auto-review, never blocks PR
    // creation). Idempotent — re-adding any label is a no-op.
    //   1. The review TRIGGER label (e.g. `ch-review`). Post-#60 every PR is gated
    //      on it, so a bot PR must carry it to be reviewed + run the self-fix loop.
    //   2. The configured DEPLOY label(s) (e.g. `CI:DEPLOY_PREVIEW`), so the repo's
    //      own deploy / preview / E2E CI runs from the START — not gated behind
    //      approval by the merge gate. `deploy_label` is a comma-separated list.
    //      The auto-merge gate later only adds a label that's still MISSING, so for
    //      these bot PRs it's a no-op there and simply waits for that CI to go green.
    if let Some(label) = super::review_pr::enabled_trigger_label(
        state,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
    )
    .await
    {
        let mut to_add: Vec<String> = vec![label];
        let cfg = super::review_actions::OnApproveConfig::load(
            state,
            &msg.team_id,
            &msg.repo_owner,
            &msg.repo_name,
        )
        .await;
        to_add.extend(
            cfg.deploy_label
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
        );
        match github
            .add_labels(&msg.repo_owner, &msg.repo_name, pr_number, &to_add)
            .await
        {
            Ok(_) => {
                info!(pr_number, labels = ?to_add, "Labeled own PR (review + deploy) at creation")
            }
            Err(e) => warn!(pr_number, error = %e, "Could not label own PR (non-fatal)"),
        }
    }

    // What GitHub has is what people read: holes a template or Markdown
    // left in it are filled once.
    fill_placeholders_once(
        state,
        github,
        &msg.repo_owner,
        &msg.repo_name,
        pr_number,
        &diff_patches,
        provider,
        usage,
    )
    .await;

    Ok(PrResult {
        pr_number,
        pr_url,
        branch: branch.to_string(),
        draft: true,
        node_id,
    })
}

/// For a run started by an alert, post a follow-up card with the new PR's link
/// to the alert's channels (Teams, Slack). Best effort: never affects the run.
async fn notify_alert_pr(
    state: &WorkerState,
    msg: &TicketMessage,
    number: u64,
    title: &str,
    url: &str,
) {
    let table = &state.config.settings_table_name;
    let Some(ticket) = state
        .dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(msg.team_id.clone()))
        .key(
            "sk",
            AttributeValue::S(format!("ALERT_TICKET#{}", msg.ticket_id)),
        )
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
    else {
        return;
    };
    let s = |k: &str| {
        ticket
            .get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let cipher = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: "",
    };
    let channels = common::alert_notify::notify_target(
        &state.dynamo,
        &cipher,
        table,
        &msg.team_id,
        &s("topic_arn"),
    )
    .await;
    if channels.is_empty() {
        return;
    }
    let base = super::skills::app_url(&state.config.stage);
    let alert_id = s("alert_id");
    let alert_url = if alert_id.is_empty() {
        String::new()
    } else {
        format!("{base}/alerts/detail?id={alert_id}")
    };
    let alert_title = Some(s("title"))
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| msg.title.clone());
    let card = common::alert_notify::fix_message(
        &alert_title,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        number,
        title,
        url,
        &alert_url,
    );
    common::notify::deliver_recorded(
        &common::notify::Http(&state.http),
        &channels,
        &card,
        "alert_pr",
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await;
}

/// Reopen the branch's prior closed-unmerged PR, if any. A ticket-update
/// re-run force-resets the branch to base, which makes GitHub auto-close the
/// PR ("all commits removed"); once the fresh implementation has pushed new
/// commits, reopening keeps the PR number and its review history instead of
/// creating a new PR every re-run. Failures fall back to PR creation.
async fn reopen_prior_pr(
    github: &GitHubClient,
    msg: &TicketMessage,
    branch: &str,
) -> Option<serde_json::Value> {
    let prior = github
        .find_reopenable_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await
        .ok()
        .flatten()?;
    let number = prior.get("number").and_then(|v| v.as_u64())?;
    match github
        .reopen_pull_request(&msg.repo_owner, &msg.repo_name, number)
        .await
    {
        Ok(()) => Some(prior),
        Err(e) => {
            warn!(pr_number = number, error = %e, "Could not reopen prior PR; creating a new one");
            None
        }
    }
}

/// Attempt to merge main into the feature branch before creating the PR.
/// If there are conflicts, resolve them with the LLM and commit the resolution.
/// Returns Ok(true) if conflicts were found and resolved, Ok(false) if no conflicts.
pub async fn resolve_conflicts(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    // The merge commit follows the team's commit convention like every
    // other commit the run makes, with the run's real ticket key.
    let merge_message = super::load_git_conventions(state, &msg.team_id, &msg.ticket_id)
        .await
        .commit_message(
            &super::ticket_tokens(msg),
            &format!("Merge {} into {}", msg.base_branch, branch),
        );
    // Try to merge main into the feature branch
    let merged = github
        .merge_branch(
            &msg.repo_owner,
            &msg.repo_name,
            branch,
            &msg.base_branch,
            &merge_message,
        )
        .await?;

    if merged {
        info!(branch, "Branch is up-to-date with base (no conflicts)");
        return Ok(false);
    }

    info!(branch, "Merge conflicts detected, resolving with LLM");

    // Get SHAs for the merge commit parents
    let branch_sha = github
        .get_ref(&msg.repo_owner, &msg.repo_name, branch)
        .await?;
    let base_sha = github
        .get_ref(&msg.repo_owner, &msg.repo_name, &msg.base_branch)
        .await?;

    // Use the compare API to find the merge base and files changed on each side
    let compare = github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await?;

    let merge_base_sha = compare
        .pointer("/merge_base_commit/sha")
        .and_then(|v| v.as_str())
        .unwrap_or(&base_sha);

    // Get files changed on the BASE side (merge_base → base_branch)
    let base_diff = github
        .get_diff(
            &msg.repo_owner,
            &msg.repo_name,
            merge_base_sha,
            &msg.base_branch,
        )
        .await?;
    let base_changed: std::collections::HashSet<String> = base_diff
        .get("files")
        .and_then(|v| v.as_array())
        .map(|files| {
            files
                .iter()
                .filter_map(|f| f.get("filename").and_then(|v| v.as_str()).map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // Get files changed on the BRANCH side (merge_base → branch)
    let branch_diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, merge_base_sha, branch)
        .await?;
    let branch_files = branch_diff
        .get("files")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Only files changed on BOTH sides are actual conflicts
    let conflicting: Vec<&serde_json::Value> = branch_files
        .iter()
        .filter(|f| {
            f.get("filename")
                .and_then(|v| v.as_str())
                .map(|name| base_changed.contains(name))
                .unwrap_or(false)
        })
        .collect();

    info!(
        branch,
        total_branch_files = branch_files.len(),
        base_changed = base_changed.len(),
        conflicting = conflicting.len(),
        "Identified conflicting files (changed on both sides)"
    );

    let mut resolved_files: Vec<FileOp> = Vec::new();

    // For files only changed on base (not touched by branch), take the base version
    for base_file in &base_changed {
        let touched_by_branch = branch_files
            .iter()
            .any(|f| f.get("filename").and_then(|v| v.as_str()) == Some(base_file));
        if !touched_by_branch {
            // Only a 404 means base deleted the file. Any other failed read
            // aborts the resolution: reading it as a deletion (or as empty
            // content) would commit that loss into the branch.
            match github
                .read_file_if_exists(&msg.repo_owner, &msg.repo_name, base_file, &msg.base_branch)
                .await?
            {
                Some(content) => {
                    resolved_files.push(FileOp::Write {
                        path: base_file.clone(),
                        content,
                    });
                }
                None => {
                    resolved_files.push(FileOp::Delete {
                        path: base_file.clone(),
                    });
                }
            }
        }
    }

    // For files changed on BOTH sides, do 3-way merge with LLM
    for file in &conflicting {
        let path = match file.get("filename").and_then(|v| v.as_str()) {
            Some(p) => p,
            None => continue,
        };

        // Read all three versions: merge base, base branch (main), feature
        // branch. None means the file is absent on that side; a failed read
        // aborts instead of passing for a deletion.
        let ancestor = github
            .read_file_if_exists(&msg.repo_owner, &msg.repo_name, path, merge_base_sha)
            .await?;
        let main_content = github
            .read_file_if_exists(&msg.repo_owner, &msg.repo_name, path, &msg.base_branch)
            .await?;
        let branch_content = github
            .read_file_if_exists(&msg.repo_owner, &msg.repo_name, path, branch)
            .await?;

        // If either side deleted the file, prefer branch's decision
        let (main_content, branch_content) = match (main_content, branch_content) {
            (Some(m), Some(b)) if m == b => continue, // no actual conflict
            (Some(m), Some(b)) => (m, b),
            (None, Some(b)) => {
                // Main deleted, branch still has it — keep branch version
                resolved_files.push(FileOp::Write {
                    path: path.to_string(),
                    content: b,
                });
                continue;
            }
            (Some(_), None) => {
                // Branch deleted, main still has it — keep branch's deletion
                resolved_files.push(FileOp::Delete {
                    path: path.to_string(),
                });
                continue;
            }
            (None, None) => continue,
        };

        let cap = |s: &str, limit: usize| -> String {
            if s.len() > limit {
                let head = common::truncate_str(s, limit);
                let cut = head.rfind('\n').unwrap_or(head.len());
                format!("{}... (truncated, {} bytes total)", &head[..cut], s.len())
            } else {
                s.to_string()
            }
        };

        // Build a 3-way merge prompt
        let system = "You are a precise merge conflict resolver. You will be given three versions of a file: the common ancestor, the main branch version, and the feature branch version. Produce the correctly merged file that incorporates changes from BOTH sides. When changes conflict directly, prefer the feature branch version. Return ONLY the file content — no explanations, no markdown fences, no commentary.".to_string();

        let mut prompt = format!("Merge these versions of `{path}`.\n\n");

        if let Some(ref anc) = ancestor {
            prompt.push_str(&format!(
                "## Common ancestor\n```\n{}\n```\n\n",
                cap(anc, 12_000)
            ));
        }

        prompt.push_str(&format!(
            "## main branch version (their changes)\n```\n{}\n```\n\n",
            cap(&main_content, 16_000)
        ));
        prompt.push_str(&format!(
            "## feature branch version (our changes — prefer when conflicting)\n```\n{}\n```\n\n",
            cap(&branch_content, 16_000)
        ));

        if ancestor.is_some() {
            prompt.push_str(
                "Use the common ancestor to understand what each side changed. \
                 Include changes from BOTH sides. Only prefer the feature branch \
                 when both sides modified the exact same lines.",
            );
        } else {
            prompt.push_str(
                "Include changes from BOTH sides. Only prefer the feature branch \
                 when both sides modified the exact same lines.",
            );
        }

        let mut messages = vec![(
            "user".to_string(),
            vec![serde_json::json!({"type": "text", "text": prompt})],
        )];

        let model_id = provider.primary_model_id();
        let merged_content = provider::converse(
            state,
            provider,
            common::agent_policy::Agent::Coder,
            model_id,
            &system,
            &mut messages,
            &[],
            &super::triage::NoOpExecutor,
            usage,
            llm::ConverseOptions {
                max_turns: 40,
                max_tokens: 16384,
                deadline: None,
                effort: None,
            },
            None,
            None,
        )
        .await?;

        // Validate the model's merge before it becomes a commit — the output
        // used to be committed verbatim (dropped hunks, stray fences,
        // truncation all landed as file corruption). Any invalid file aborts
        // the whole resolution: an unresolved conflict GitHub can display is
        // strictly safer than a silently corrupted merge.
        let merged_content = strip_code_fences(&merged_content);
        let min_side = branch_content.len().min(main_content.len());
        if merged_content.trim().is_empty() && min_side > 0 {
            warn!(
                path,
                "LLM merge produced empty output — aborting conflict resolution"
            );
            return Ok(false);
        }
        if min_side > 200 && merged_content.len() < min_side * 3 / 10 {
            warn!(
                path,
                merged = merged_content.len(),
                min_side,
                "LLM merge dropped most of the file — aborting conflict resolution"
            );
            return Ok(false);
        }
        if let Err(problem) =
            syntax_check::validate_change(path, Some(&branch_content), &merged_content)
        {
            warn!(
                path,
                problem, "LLM merge is syntactically broken — aborting conflict resolution"
            );
            return Ok(false);
        }

        resolved_files.push(FileOp::Write {
            path: path.to_string(),
            content: merged_content,
        });
    }

    if resolved_files.is_empty() && !conflicting.is_empty() {
        warn!(
            branch,
            "Conflict detected but no files to resolve — all files matched"
        );
        // Try the merge one more time in case it was transient
        let retry = github
            .merge_branch(
                &msg.repo_owner,
                &msg.repo_name,
                branch,
                &msg.base_branch,
                &merge_message,
            )
            .await?;
        return Ok(retry);
    }

    info!(
        branch,
        resolved = resolved_files.len(),
        llm_resolved = conflicting.len(),
        auto_resolved = resolved_files.len().saturating_sub(conflicting.len()),
        "Creating merge commit"
    );

    // Create a proper merge commit with TWO parents — this tells Git the merge is done
    github
        .create_merge_commit(
            &msg.repo_owner,
            &msg.repo_name,
            branch,
            &branch_sha,
            &base_sha,
            &merge_message,
            &resolved_files,
        )
        .await?;

    Ok(true)
}

/// Strip a wrapping markdown code fence the model may have added despite
/// instructions ("```lang\n…\n```"). Inner fences are left untouched.
fn strip_code_fences(s: &str) -> String {
    let trimmed = s.trim();
    // Only strip a WRAPPING fence: starts with ``` AND ends with ```.
    // A file that legitimately begins with a fence (markdown docs) but has
    // content after its last fence must pass through untouched.
    if let Some(rest) = trimmed.strip_prefix("```") {
        if let Some(inner) = rest.strip_suffix("```") {
            let after_lang = inner.find('\n').map(|i| i + 1).unwrap_or(0);
            return inner[after_lang..].trim_end().to_string() + "\n";
        }
    }
    s.to_string()
}

/// The PR description writer's system prompt: the team's voice and
/// standards, then its instructions (the sections a PR must have, say),
/// which win over the default structure.
fn writer_system(voice_block: &str, standards_block: &str, team_instructions: &str) -> String {
    format!(
        "You are writing a pull request description. Be concise and technical. Return only markdown.{voice_block}{standards_block}{team}",
        team = super::team_instructions_block(team_instructions),
    )
}

/// The PR's title: the team's template, or `{ticket}: {title}` (cut to 72
/// characters) as always.
pub(crate) fn pr_title(
    conventions: &common::git_conventions::GitConventions,
    tokens: &common::git_conventions::Tokens,
    msg: &TicketMessage,
) -> String {
    if let Some(t) = &conventions.pr_title {
        return common::git_conventions::pr_title(t, tokens);
    }
    let title = match msg.source {
        TicketSource::Github => format!("#{}: {}", msg.issue_number, msg.title),
        TicketSource::Jira | TicketSource::Alert => format!("{}: {}", msg.ticket_id, msg.title),
    };
    if title.len() > 72 {
        // char-boundary-safe: String::truncate panics mid-codepoint, and
        // ticket titles routinely contain em-dashes/emoji.
        format!("{}...", common::truncate_str(&title, 69))
    } else {
        title
    }
}

/// The message of the one commit a run's work is squashed into: the team's
/// commit template (its `{summary}` from the ticket's title), or
/// `{ticket}: {title}` as always.
fn squash_message(
    conventions: &common::git_conventions::GitConventions,
    tokens: &common::git_conventions::Tokens,
    msg: &TicketMessage,
) -> String {
    if let Some(t) = &conventions.commit {
        return common::git_conventions::commit(t, tokens);
    }
    format!(
        "{}: {}",
        match msg.source {
            TicketSource::Github => format!("#{}", msg.issue_number),
            TicketSource::Jira | TicketSource::Alert => msg.ticket_id.clone(),
        },
        msg.title
    )
}

/// What the writer is told about the run's evidence: report it, never call
/// something unverified that a lookup answered. Empty when the run gathered
/// none.
fn evidence_instructions(evidence: &str) -> String {
    if evidence.trim().is_empty() {
        return String::new();
    }
    format!(
        "\n{evidence}\n\
         Add an **Evidence** list at the end of the Verification section: each fact about another system the change \
         relies on, on its own line that starts `- Confirmed:` (the lookup that confirmed it and \
         what came back: the field and its value, as the lookup returned it) or `- Unconfirmed:` \
         (what nothing confirmed, and why: a missing source, or a lookup marked FAILED or EMPTY, \
         which confirms nothing); how many records the change affects, where they were counted, \
         and the few examples named above. Take these only from the lookups and notes above, \
         never from guesses. Never write that something was not verified, not measured or \
         unconfirmed when the lookups above answer it; when the notes say no connected source \
         could answer something, say which source is missing.\n"
    )
}

fn format_diff_summary(diff: &serde_json::Value) -> String {
    let files = match diff.get("files").and_then(|v| v.as_array()) {
        Some(f) => f,
        None => return "(no changes)".to_string(),
    };

    files
        .iter()
        .map(|f| {
            let path = f.get("filename").and_then(|v| v.as_str()).unwrap_or("");
            let status = f
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("modified");
            let adds = f.get("additions").and_then(|v| v.as_u64()).unwrap_or(0);
            let dels = f.get("deletions").and_then(|v| v.as_u64()).unwrap_or(0);
            format!("- {path} ({status}: +{adds}/-{dels})")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The diff the PR body writer reads. It previously saw a 1.2k excerpt per
/// file, so descriptions were written from part of the change; now it reads
/// every patch whole (lockfiles and generated files are named, not shown),
/// and anything still left out is named so the writer knows it didn't see it.
pub(super) fn format_diff_patches(diff: &serde_json::Value) -> String {
    const PER_FILE: usize = 40_000;
    const TOTAL: usize = 150_000;
    let files = match diff.get("files").and_then(|v| v.as_array()) {
        Some(f) => f,
        None => return String::new(),
    };
    let mut out = String::new();
    let mut named_only: Vec<String> = Vec::new();
    for f in files {
        let path = f.get("filename").and_then(|v| v.as_str()).unwrap_or("");
        let Some(patch) = f.get("patch").and_then(|v| v.as_str()) else {
            // Binary, or too large for the API to return a patch.
            named_only.push(format!("{path} (no patch from GitHub)"));
            continue;
        };
        if is_lockfile(path) {
            named_only.push(format!("{path} (lockfile)"));
            continue;
        }
        if out.len() + patch.len().min(PER_FILE) > TOTAL {
            named_only.push(format!("{path} (over the size budget)"));
            continue;
        }
        let excerpt = common::truncate_str(patch, PER_FILE);
        let marker = if excerpt.len() < patch.len() {
            format!("\n[… cut at {PER_FILE} characters of {} …]", patch.len())
        } else {
            String::new()
        };
        out.push_str(&format!("\n### {path}\n```diff\n{excerpt}{marker}\n```\n"));
    }
    if !named_only.is_empty() {
        out.push_str(
            "\nChanged but not shown above (don't describe what's in them beyond the name):\n",
        );
        for n in named_only {
            out.push_str(&format!("- {n}\n"));
        }
    }
    out
}

fn is_lockfile(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    matches!(
        name,
        "package-lock.json"
            | "yarn.lock"
            | "pnpm-lock.yaml"
            | "Cargo.lock"
            | "poetry.lock"
            | "Pipfile.lock"
            | "uv.lock"
            | "Gemfile.lock"
            | "composer.lock"
            | "go.sum"
            | "bun.lockb"
    )
}

/// CoderHelm's own sections for a new description: the blocks the change
/// copied, every step outside this PR with its owner, and Open items. The
/// merge order is added once every PR of a multi-repo run exists
/// ([`link_siblings`]).
fn owned_sections(plan: &PlanResult, proof: &Proof<'_>, repo: &str) -> super::completeness::Owned {
    let small_test_fix = proof.test_fix.is_some_and(|t| t.small);
    super::completeness::Owned {
        links: proof.links.to_string(),
        mirrors: super::completeness::mirrors_section(proof.mirrors),
        merge_order: String::new(),
        not_in_pr: super::completeness::for_people_section(
            &super::fix_location::not_in_pr_steps(plan.fix.as_ref(), repo),
            if small_test_fix {
                &[][..]
            } else {
                plan.fix.as_ref().map_or(&[][..], |d| &d.questions)
            },
        ),
        // A small failing-test fix carries no open items or questions.
        open_items: if small_test_fix {
            String::new()
        } else {
            proof.gaps.to_string()
        },
    }
}

/// Where a rewrite's account of what it changed starts.
const CHANGES_MARK: &str = "---CHANGES---";

/// Splits a rewrite into the description and its list of what changed (one
/// item per bullet after [`CHANGES_MARK`]).
fn split_changes(text: &str) -> (String, Vec<String>) {
    let Some(at) = text.find(CHANGES_MARK) else {
        return (text.trim().to_string(), Vec::new());
    };
    let changes = text[at + CHANGES_MARK.len()..]
        .lines()
        .map(|l| l.trim().trim_start_matches(['-', '*']).trim())
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect();
    (text[..at].trim().to_string(), changes)
}

/// Whether `new` keeps every CoderHelm marker `old` has: a rewrite that lost
/// one would lose the section it wraps, or CI's mark on an item.
fn keeps_markers(old: &str, new: &str) -> bool {
    old.lines()
        .flat_map(|l| l.match_indices("<!-- ").map(move |(i, _)| &l[i..]))
        .filter_map(|m| m.find("-->").map(|e| &m[..e + 3]))
        .filter(|m| m.contains("coderhelm:"))
        .all(|m| new.matches(m).count() >= old.matches(m).count())
}

/// The instructions for a rewrite a person asked for.
fn request_block(requests: &[String]) -> String {
    if requests.is_empty() {
        return String::new();
    }
    let asks: String = requests
        .iter()
        .map(|r| format!("- {}\n", r.split_whitespace().collect::<Vec<_>>().join(" ")))
        .collect();
    format!(
        "\n## What a person with write access asked to change in the description\n{asks}\
         Apply exactly these changes, whether or not the code changed. Where they ask for a \
         section with an owner per item, every item names its owner.\n\
         - Keep this shape: Summary, Changes (with the block the change copies), Merge order, \
         Not in this PR (one bullet per step, each with its owner), Risk, Verification, and Open \
         items at the bottom (each item, then what settles it). No warning or note banner \
         anywhere.\n\
         - Keep each `<!-- coderhelm:... -->` line with the section it wraps, and the \
         `<!-- coderhelm:ci -->` mark on any item CI settles. Put a new Not in this PR section \
         between `<!-- coderhelm:not-in-pr -->` and `<!-- /coderhelm:not-in-pr -->` lines, and a \
         new open item inside the `<!-- coderhelm:unverified -->` section under `## Open items`.\n"
    )
}

/// Rewrites an open pull request's description from its current full diff
/// after later commits (review feedback, CI fixes), so it never describes an
/// earlier version of the change, and applies what a person with write
/// access asked to change in it (`requests`, empty when nobody asked).
/// Keeps the description's structure and its ticket line, and CoderHelm's
/// own sections in their places (Open items at the bottom); anything it says
/// that the diff no longer bears out goes. Returns what changed when a new
/// description was saved (one entry per change a person asked for), `None`
/// when it was left alone, whether nothing changed or anything failed.
#[allow(clippy::too_many_arguments)]
pub async fn refresh_description(
    state: &WorkerState,
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr_number: u64,
    team_instructions: &str,
    requests: &[String],
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Option<Vec<String>> {
    let pr = github.get_pull_request(owner, repo, pr_number).await.ok()?;
    let base = pr["base"]["ref"].as_str()?.to_string();
    let head = pr["head"]["ref"].as_str()?.to_string();
    let current = pr["body"].as_str().unwrap_or("").trim().to_string();
    // CoderHelm's own sections (Open items above all) are its record, kept
    // word for word (only CI's result changes Open items, see
    // `settle_unverified_by_ci`): the writer rewrites the rest. A person's
    // request may change them too, so then the writer sees the whole body.
    let (owned, rest) = super::completeness::take_owned(&current);
    let shaped = super::completeness::assemble(&rest, &owned);
    let old = if requests.is_empty() {
        rest.trim().to_string()
    } else {
        shaped.clone()
    };
    if old.is_empty() && requests.is_empty() {
        return None;
    }
    let diff = github.get_diff(owner, repo, &base, &head).await.ok()?;
    let patches = format_diff_patches(&diff);
    let lead = if requests.is_empty() {
        "This pull request got more commits after its description was written. Rewrite the description so every statement in it is true of the diff below, which is the whole change as it stands now."
    } else {
        "A person asked for changes to this pull request's description. Make them, and keep every statement true of the diff below, which is the whole change as it stands now."
    };
    let prompt = format!(
        r#"{lead}

## Current description
{old}
{requests}
## Files changed
{files}

## The full diff (the source of truth — read all of it)
{patches}

## Instructions
- Keep the description's structure, headings and tone.
- Keep any line that starts with "Source ticket:", "Closes #", "Fixes #" or "Resolves #" exactly as it is, and keep any HTML comments and `<sub>` lines at the end exactly as they are.
- Remove or correct anything the diff no longer bears out (a field name, a guess, a step, a file), and describe what the later commits changed.
- Keep the Evidence list (what was looked up and what came back), unless the diff no longer relies on what it describes.
{shape_rules}
- Don't add filler. If the description is already true and nothing was asked, return it unchanged.

Return ONLY the markdown body text, then a line `{CHANGES_MARK}` and one bullet per change you made to the description, in a few words each."#,
        requests = request_block(requests),
        files = format_diff_summary(&diff),
        shape_rules = SHAPE_RULES,
    );
    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];
    let system = format!(
        "You keep pull request descriptions accurate to their code.{}",
        super::team_instructions_block(team_instructions)
    );
    let opts = llm::ConverseOptions {
        max_turns: 1,
        max_tokens: 4096,
        deadline: None,
        effort: Some("low"),
    };
    let text = match provider::converse(
        state,
        provider,
        common::agent_policy::Agent::Coder,
        provider.primary_model_id(),
        &system,
        &mut messages,
        &[],
        &super::triage::NoOpExecutor,
        usage,
        opts,
        None,
        None,
    )
    .await
    {
        Ok(b) => b,
        Err(e) => {
            warn!(pr_number, error = %e, "Could not rewrite the PR description");
            return None;
        }
    };
    let (written, changes) = split_changes(&text);
    // Rewritten from the diff on every push: it names only entries and
    // files the diff contains.
    let written = super::link_checks::enforce_claims(
        state,
        provider,
        &system,
        &mut messages,
        opts,
        usage,
        &diff,
        written,
    )
    .await;
    let written = split_changes(&written).0;
    if !keeps_ticket_line(&old, &written) {
        warn!(
            pr_number,
            "Rewritten PR description dropped its ticket line; keeping the old one"
        );
        return None;
    }
    let body = if requests.is_empty() {
        super::completeness::assemble(&written, &owned)
    } else {
        // The sections the person kept go back in their places.
        let (kept, rest) = super::completeness::take_owned(&written);
        super::completeness::assemble(&rest, &kept)
    };
    if body == current {
        return None;
    }
    if let Err(e) = github.update_pr_body(owner, repo, pr_number, &body).await {
        warn!(pr_number, error = %e, "Could not save the rewritten PR description");
        return None;
    }
    info!(
        pr_number,
        asked = requests.len(),
        "PR description rewritten to match the latest diff"
    );
    fill_placeholders_once(
        state, github, owner, repo, pr_number, &patches, provider, usage,
    )
    .await;
    Some(changes)
}

/// Reads the description back from GitHub, which is what people see, and
/// fills the holes a template or Markdown left in it (see
/// `completeness::placeholder_holes`) once: the writer is given each hole
/// and the change, and writes the real value. Nothing is saved when the fix
/// drops the ticket line or a CoderHelm marker; holes still there after the
/// one fix are logged, never retried.
#[allow(clippy::too_many_arguments)]
async fn fill_placeholders_once(
    state: &WorkerState,
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr_number: u64,
    change: &str,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) {
    let Ok(pr) = github.get_pull_request(owner, repo, pr_number).await else {
        return;
    };
    let body = pr["body"].as_str().unwrap_or("").to_string();
    let holes = super::completeness::placeholder_holes(&body);
    if holes.is_empty() {
        return;
    }
    warn!(pr_number, holes = ?holes, "PR description has empty placeholders; filling them once");
    let prompt = format!(
        "The pull request description below, as GitHub has it, has holes where a value should \
         be:\n{}\n\nReplace each with the real value, from the change below and the rest of the \
         description (a path with its environment filled in, a real name). Where the value \
         can't be known, say it in words instead of a placeholder. Change nothing else, and keep \
         every line that starts with `<!--` or `<sub>` exactly as it is. Return ONLY the full \
         description.\n\n## The change\n{change}\n\n## The description\n{body}",
        holes
            .iter()
            .map(|h| format!("- {h}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let fixed = match provider::converse_simple(
        state,
        provider,
        provider.primary_model_id(),
        "You fix pull request descriptions. Return only markdown.",
        &prompt,
        usage,
    )
    .await
    {
        Ok(t) => strip_code_fences(&t).trim().to_string(),
        Err(e) => {
            warn!(pr_number, error = %e, "Could not fill the PR description's placeholders");
            return;
        }
    };
    if !keeps_ticket_line(&body, &fixed) || !keeps_markers(&body, &fixed) {
        warn!(
            pr_number,
            "Placeholder fix dropped the ticket line or a section marker; keeping the description"
        );
        return;
    }
    if fixed == body.trim() {
        return;
    }
    match github.update_pr_body(owner, repo, pr_number, &fixed).await {
        Ok(()) => {
            let left = super::completeness::placeholder_holes(&fixed);
            if left.is_empty() {
                info!(
                    pr_number,
                    filled = holes.len(),
                    "Filled the PR description's placeholders"
                );
            } else {
                warn!(pr_number, holes = ?left, "PR description still has placeholders after one fix");
            }
        }
        Err(e) => warn!(pr_number, error = %e, "Could not save the filled PR description"),
    }
}

/// Every PR of a multi-repo run names its siblings and the order they merge
/// in (the plan's repository order). Best effort per PR.
pub async fn link_siblings(github: &GitHubClient, prs: &[(String, String, u64)]) {
    if prs.len() < 2 {
        return;
    }
    let siblings: Vec<super::completeness::Sibling> = prs
        .iter()
        .map(|(o, n, num)| super::completeness::Sibling {
            repo: format!("{o}/{n}"),
            number: *num,
        })
        .collect();
    for (i, (owner, name, number)) in prs.iter().enumerate() {
        let Ok(pr) = github.get_pull_request(owner, name, *number).await else {
            continue;
        };
        let body = pr["body"].as_str().unwrap_or("");
        let new = super::completeness::with_section(
            body,
            super::completeness::Section::MergeOrder,
            super::completeness::merge_order_section(&siblings, i),
        );
        if new == body {
            continue;
        }
        match github.update_pr_body(owner, name, *number, &new).await {
            Ok(()) => info!(
                pr_number = number,
                "Named the run's sibling PRs and merge order"
            ),
            Err(e) => warn!(pr_number = number, error = %e, "Could not add the merge order"),
        }
    }
}

/// The PR's CI passed on its head: the Open items CI settles
/// (lint, type-check, build or tests that hadn't run before the PR opened)
/// come out of the description, and the whole section when none is left. A
/// warning that stayed after CI passed those checks told reviewers the PR was
/// unverified when it wasn't. Leaves the description alone when there is
/// nothing to settle or anything fails.
pub async fn settle_unverified_by_ci(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr_number: u64,
) {
    if pr_number == 0 {
        return;
    }
    let Ok(pr) = github.get_pull_request(owner, repo, pr_number).await else {
        return;
    };
    let Some(body) = super::proof::settle_by_ci(pr["body"].as_str().unwrap_or("")) else {
        return;
    };
    match github.update_pr_body(owner, repo, pr_number, &body).await {
        Ok(()) => info!(
            pr_number,
            "CI passed: removed the checks it settled from the PR's Open items"
        ),
        Err(e) => {
            warn!(pr_number, error = %e, "Could not update the PR description after CI passed")
        }
    }
}

/// Whether `new` is a usable rewrite of `old`: not empty, and keeping the
/// ticket link line (`Source ticket:` / `Closes #N`) when `old` had one.
fn keeps_ticket_line(old: &str, new: &str) -> bool {
    if new.trim().is_empty() {
        return false;
    }
    let link = |s: &str| {
        s.lines()
            .map(str::trim)
            .find(|l| {
                let lower = l.to_lowercase();
                lower.starts_with("source ticket:")
                    || lower.starts_with("closes #")
                    || lower.starts_with("fixes #")
                    || lower.starts_with("resolves #")
            })
            .map(str::to_string)
    };
    match link(old) {
        Some(l) => link(new).as_deref() == Some(l.as_str()),
        None => true,
    }
}

/// Try to fetch a PR template from common locations in the repo.
async fn fetch_pr_template(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base_branch: &str,
) -> Option<String> {
    let paths = [
        ".github/pull_request_template.md",
        ".github/PULL_REQUEST_TEMPLATE.md",
        "pull_request_template.md",
        "PULL_REQUEST_TEMPLATE.md",
        "docs/pull_request_template.md",
    ];
    for path in &paths {
        if let Ok(content) = github.read_file(owner, repo, path, base_branch).await {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                info!(owner, repo, path, "Found PR template");
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod refresh_tests {
    use super::{
        evidence_instructions, keeps_ticket_line, manual_instructions, pr_title, squash_message,
        writer_system,
    };

    #[test]
    fn checks_only_a_person_can_do_become_verification_steps() {
        assert_eq!(manual_instructions(""), "");
        let block = manual_instructions("- open the drawer in a real browser\n");
        assert!(block.contains("numbered step in the Verification section"));
        assert!(block.contains("and nowhere else"));
        assert!(block.ends_with("- open the drawer in a real browser"));
    }

    #[test]
    fn the_writer_gets_the_teams_instructions_when_set() {
        let team = "PR descriptions have a Rollback section.";
        let with = writer_system("", "", team);
        assert!(with.contains("## Your team's instructions"));
        assert!(with.contains(team));
        assert!(!writer_system("", "", "").contains("team's instructions"));
    }
    use common::git_conventions::{GitConventions, Tokens};

    fn msg(source: &str, id: &str, number: u64, title: &str) -> crate::models::TicketMessage {
        serde_json::from_value(serde_json::json!({
            "team_id": "t", "installation_id": 1, "source": source,
            "ticket_id": id, "title": title, "body": "",
            "repo_owner": "acme", "repo_name": "web", "issue_number": number,
            "sender": "", "base_branch": "main"
        }))
        .unwrap()
    }

    #[test]
    fn titles_and_squash_commits_follow_the_teams_templates() {
        let m = msg("jira", "ABC-12", 0, "Fix the drawer.");
        let t = Tokens::new("ABC-12", "ABC-12", &m.title);
        let none = GitConventions::default();
        assert_eq!(pr_title(&none, &t, &m), "ABC-12: Fix the drawer.");
        assert_eq!(squash_message(&none, &t, &m), "ABC-12: Fix the drawer.");
        let gh = msg("github", "GH-4", 4, "Fix bug");
        let gt = Tokens::new("#4", "GH-4", "Fix bug");
        assert_eq!(pr_title(&none, &gt, &gh), "#4: Fix bug");
        let team = GitConventions {
            branch: None,
            commit: Some("{ticket_lower}: {summary}".into()),
            pr_title: Some("[{ticket}] {title}".into()),
        };
        assert_eq!(pr_title(&team, &t, &m), "[ABC-12] Fix the drawer.");
        assert_eq!(squash_message(&team, &t, &m), "abc-12: fix the drawer");
    }

    #[test]
    fn the_writer_reports_the_runs_evidence() {
        assert_eq!(evidence_instructions(""), "");
        let block = evidence_instructions(
            "- Ran api_get: /v1/products/7\n  Came back: {\"title\": \"Swim\"}",
        );
        assert!(block.contains("Came back: {\"title\": \"Swim\"}"));
        assert!(block.contains("Add an **Evidence** list at the end of the Verification section"));
        assert!(block.contains("Never write that something was not verified, not measured"));
        // The reviewer reads these markers back: an `Unconfirmed:` line holds
        // its approval.
        assert!(block.contains("`- Confirmed:`") && block.contains("`- Unconfirmed:`"));
    }

    #[test]
    fn the_writer_leaves_coderhelms_sections_to_it_and_writes_real_values() {
        use super::SHAPE_RULES;
        assert!(SHAPE_RULES.contains("**Not in this PR** and **Open items**"));
        assert!(SHAPE_RULES.contains("don't add a warning or note banner"));
        assert!(SHAPE_RULES.contains("`/app/prod/db-password`, never `/app//db-password`"));
        assert!(SHAPE_RULES.contains("one bullet per environment"));
    }

    #[test]
    fn a_pr_gets_its_mirrors_steps_outside_it_and_open_items_in_order() {
        use crate::passes::completeness::{assemble, Mirror};
        use crate::passes::fix_location::{EndToEndStep, FixDecision};
        let plan = crate::passes::plan::PlanResult {
            proposal: String::new(),
            design: String::new(),
            tasks: String::new(),
            spec: String::new(),
            repo_tasks: vec![],
            fix: Some(FixDecision {
                end_to_end: vec![
                    EndToEndStep {
                        step: "Create the reporting user".into(),
                        by: "this_pr".into(),
                        ..Default::default()
                    },
                    EndToEndStep {
                        step: "Allow the vendor's IP range".into(),
                        by: "person".into(),
                        owner: "the network team".into(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
        };
        let mirrors = [Mirror {
            path: "envs/prod/users.tf".into(),
            name: "billing_reader".into(),
            differences: vec!["read-only, because it only reads reports".into()],
        }];
        let gaps = crate::passes::proof::gaps_section(&[crate::passes::proof::Finding {
            point: "ci_pending".into(),
            detail: "tests did not run".into(),
            ..Default::default()
        }]);
        let proof = super::Proof {
            evidence: "",
            gaps: &gaps,
            manual: "",
            ci: &[],
            mirrors: &mirrors,
            log: None,
            links: "",
            test_fix: None,
            feature: None,
        };
        let owned = super::owned_sections(&plan, &proof, "acme/infra");
        let body = assemble(
            "Source ticket: OPS-7\n\n**Summary** x\n\n**Changes**\n* y\n\n**Risk** **Low**\n\n**Verification**\n1. z",
            &owned,
        );
        let at = |s: &str| body.find(s).unwrap_or_else(|| panic!("{s}:\n{body}"));
        assert!(at("**Changes**") < at("mirrors `billing_reader` in `envs/prod/users.tf`"));
        assert!(at("mirrors `billing_reader`") < at("## Not in this PR"));
        assert!(at("* Allow the vendor's IP range (owner: the network team)") < at("**Risk**"));
        assert!(!body.contains("Create the reporting user"), "in this PR");
        assert!(at("**Verification**") < at("## Open items"));
        assert!(body.trim_end().ends_with("<!-- /coderhelm:unverified -->"));
        assert!(
            crate::passes::completeness::placeholder_holes(&body).is_empty(),
            "{body}"
        );
    }

    #[test]
    fn a_rewrite_reports_what_it_changed_and_keeps_every_marker() {
        use super::{keeps_markers, request_block, split_changes};
        let (body, changes) = split_changes(
            "**Summary** x\n\n---CHANGES---\n- Removed the warning banner\n* Added Not in this PR\n\n",
        );
        assert_eq!(body, "**Summary** x");
        assert_eq!(
            changes,
            vec!["Removed the warning banner", "Added Not in this PR"]
        );
        assert_eq!(
            split_changes("just a body\n"),
            ("just a body".to_string(), vec![])
        );

        let old = "x\n<!-- coderhelm:unverified -->\n## Open items\n- a <!-- coderhelm:ci -->\n<!-- /coderhelm:unverified -->";
        assert!(keeps_markers(old, old));
        assert!(!keeps_markers(
            old,
            "x\n<!-- coderhelm:unverified -->\n## Open items\n- a\n<!-- /coderhelm:unverified -->"
        ));
        assert!(keeps_markers("no markers <!-- other -->", "anything"));

        assert_eq!(request_block(&[]), "");
        let r = request_block(&["drop the   banner".to_string()]);
        assert!(r.contains("asked to change in the description\n- drop the banner\n"));
        assert!(r.contains("every item names its owner"));
        assert!(r.contains("and Open items at the bottom (each item, then what settles it)"));
        assert!(r.contains("<!-- coderhelm:not-in-pr -->"));
    }

    #[test]
    fn a_rewrite_must_keep_the_ticket_line() {
        let old = "Source ticket: ABC-1\n\n**Problem** x";
        assert!(keeps_ticket_line(
            old,
            "Source ticket: ABC-1\n\n**Problem** y"
        ));
        assert!(!keeps_ticket_line(old, "**Problem** y"));
        assert!(!keeps_ticket_line(old, "   "));
        assert!(keeps_ticket_line("no link here", "rewritten"));
    }
}

#[cfg(test)]
mod ci_verification_tests {
    use super::{ci_instructions, ci_verification};
    use crate::passes::checks::{pr_commands, Workflow};

    fn terraform_ci() -> Vec<crate::passes::checks::CiCommand> {
        pr_commands(
            &[Workflow {
                path: ".github/workflows/ci.yml".into(),
                text: "on: pull_request\njobs:\n  tf:\n    steps:\n      - run: terraform fmt -recursive -check\n      - run: |\n          terraform init\n          terraform plan -input=false\n".into(),
            }],
            "main",
        )
    }

    #[test]
    fn verification_says_ci_runs_what_ci_runs_on_the_pr() {
        let ci = terraform_ci();
        let body = "**Problem**\nTag the buckets.\n\n**Verification**\n1. Run `terraform fmt -check` in the repo root.\n2. Run `terraform plan` for staging and prod and confirm only tags change.\n3. Run `terraform plan -var-file=prod.tfvars` again.\n4. Open the console and check the bucket's tags.\n\n**Risk**\nRun `terraform plan` locally if unsure.\n";
        assert_eq!(
            ci_verification(body, &ci),
            "**Problem**\nTag the buckets.\n\n**Verification**\n1. CI runs `terraform fmt -recursive -check` on this PR.\n2. CI runs `terraform plan -input=false` on this PR; read its output in the PR's checks.\n4. Open the console and check the bucket's tags.\n\n**Risk**\nRun `terraform plan` locally if unsure.\n"
        );
        // A heading in markdown form, and nothing CI runs: left alone.
        let other = "## Verification\n* Run `pnpm test`.\n";
        assert_eq!(ci_verification(other, &ci), other);
        assert_eq!(ci_verification(body, &[]), body);
    }

    #[test]
    fn the_writer_is_told_never_to_ask_a_person_to_run_a_ci_check() {
        let ci = terraform_ci();
        let block = ci_instructions(&ci);
        assert!(block.contains("never tell a person to run any of them"));
        assert!(block.contains("\"CI runs `<command>` on this PR.\""));
        assert!(block.ends_with(
            "- `terraform fmt -recursive -check` (format)\n- `terraform plan -input=false` (plan)"
        ));
        assert_eq!(ci_instructions(&[]), "");
    }
}

#[cfg(test)]
mod diff_tests {
    use super::format_diff_patches;
    use serde_json::json;

    #[test]
    fn the_writer_sees_whole_patches_and_is_told_what_it_didnt_see() {
        let long = "+x\n".repeat(2_000); // 6k: was cut at 1.2k before
        let d = json!({"files": [
            {"filename": "src/a.ts", "patch": long},
            {"filename": "package-lock.json", "patch": "+lots"},
            {"filename": "img.png"},
        ]});
        let out = format_diff_patches(&d);
        assert!(out.contains(&long), "the whole patch is shown");
        assert!(!out.contains("+lots"));
        assert!(out.contains("- package-lock.json (lockfile)"));
        assert!(out.contains("- img.png (no patch from GitHub)"));
    }
}
