use serde_json::json;
use tracing::{info, warn};

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::agent::mcp;
use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

/// The planner's logic version. Part of every cached plan's key: bump it on
/// any change to how plans are made or what a plan must carry, so a plan
/// written by an older planner is never reused.
pub const PLANNER_VERSION: &str = "2026-10-27.intent-history-env-near-match-stop-rules";

/// The openspec file holding the plan's end-to-end checklist, readable.
pub const END_TO_END_FILE: &str = "end_to_end.md";

/// The cached plan's file holding its key.
pub const PLAN_KEY_FILE: &str = "plan_key.txt";

/// The cached plan's file naming the run that made it (or took it up): a
/// continuation of that run reuses the plan whatever its key now says.
pub const PLAN_RUN_FILE: &str = "plan_run.txt";

/// Records `run_id` as the run whose plan the cached files are.
pub async fn mark_plan_run(state: &WorkerState, msg: &TicketMessage, run_id: &str) {
    let prefix = crate::passes::openspec_prefix(
        &msg.team_id,
        &msg.ticket_id,
        &msg.repo_owner,
        &msg.repo_name,
    );
    if let Err(e) = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(format!("{prefix}/{PLAN_RUN_FILE}"))
        .body(run_id.as_bytes().to_vec().into())
        .content_type("text/plain")
        .send()
        .await
    {
        warn!(run_id, error = %e, "Could not record which run the plan belongs to");
    }
}

/// The key a plan is cached under: everything that can change the plan.
/// The ticket's content (title, body, images; a Jira comment that re-runs
/// the ticket rides in the body), what memory recalled for it (team lessons
/// included), the ticket's history (earlier runs, how their PRs ended,
/// lessons, people's notes), the planner's version, and the plan review it
/// goes through (`skills::plan_review_key`: the review's version and each
/// reviewing team agent's text), so a reused plan never skips a review.
pub fn plan_cache_key(
    context_hash: &str,
    memory_context: &str,
    history_digest: &str,
    team_instructions: &str,
    review_key: &str,
) -> String {
    let memory = common::sha256_hex(&[memory_context]);
    let instructions = common::sha256_hex(&[team_instructions]);
    common::sha256_hex(&[
        PLANNER_VERSION,
        context_hash,
        &memory,
        history_digest,
        &instructions,
        review_key,
    ])
}

/// Whether a draft plan is one the team's agents review: a plan of work,
/// not a finding that nothing is needed or a question back, and one that
/// parsed.
pub fn reviewable(files: &PlanResult) -> bool {
    let tasks = files.tasks.trim_start();
    !tasks.is_empty()
        && !files.proposal.trim().is_empty()
        && !tasks.starts_with("NO_CHANGES_NEEDED")
        && !tasks.starts_with("CLARIFICATION_NEEDED")
}

/// The draft as the reviewing agents read it: proposal, spec and tasks.
pub fn review_draft(files: &PlanResult) -> String {
    let mut out = format!("## proposal.md\n{}\n", files.proposal.trim());
    if !files.spec.trim().is_empty() {
        out.push_str(&format!("\n## spec.md\n{}\n", files.spec.trim()));
    }
    out.push_str(&format!("\n## tasks.md\n{}\n", files.tasks.trim()));
    out
}

/// What the planner is told after the review: the notes as advice, and to
/// answer with the whole plan again.
pub fn revision_prompt(advice: &str) -> String {
    format!(
        "{advice}\n\nRevise your plan once with these notes: change it only where a note points \
         to a real gap for this change. Then output the COMPLETE plan again in exactly the same \
         format as before (every file block, the fix decision included), not just the changes."
    )
}

/// The revised plan when it parsed into a plan, else the draft. The
/// decision on where the fix belongs stays the draft's: it was checked
/// against the planner's trace, and a review doesn't move it.
pub fn take_revision(draft: PlanResult, revised: PlanResult) -> (PlanResult, bool) {
    let lost_design = revised.design.trim().is_empty() && !draft.design.trim().is_empty();
    if !reviewable(&revised) || lost_design {
        return (draft, false);
    }
    (
        PlanResult {
            fix: draft.fix,
            ..revised
        },
        true,
    )
}

/// Result of the plan pass: the four openspec files.
pub struct PlanResult {
    pub proposal: String,
    pub design: String,
    pub tasks: String,
    pub spec: String,
    /// Per-repo task breakdown. Empty = single-repo (use msg.repo_owner/repo_name).
    pub repo_tasks: Vec<RepoTasks>,
    /// Where the fix belongs, as the planner decided it. `None` for plans
    /// written before the decision existed: those are code plans.
    pub fix: Option<super::fix_location::FixDecision>,
}

/// Tasks scoped to a specific repo for multi-repo orchestration.
#[derive(Clone, Debug)]
pub struct RepoTasks {
    pub owner: String,
    pub name: String,
    pub tasks: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    triage: &super::triage::TriageResult,
    repo_instructions: &str,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    run_id: &str,
    ticket_history: &super::ticket_history::TicketHistory,
) -> Result<PlanResult, Box<dyn std::error::Error + Send + Sync>> {
    let intent = triage.intent;
    // Fetch all enabled repos for this team so the planner can explore cross-repo
    let team_repos = fetch_team_repos(state, &msg.team_id).await;
    let default_repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let repos_list = if team_repos.is_empty() {
        format!("- {default_repo} (this repo)")
    } else {
        team_repos
            .iter()
            .map(|r| {
                if r == &default_repo {
                    format!("- {r} (this repo — default)")
                } else {
                    format!("- {r}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let team_block = super::team_instructions_block(&msg.team_instructions);
    let instructions_block = if repo_instructions.is_empty() {
        String::new()
    } else {
        format!(
            "\n\n## Repository Context (AGENTS.md)\nUse this to understand the repo structure and conventions. \
             This should reduce or eliminate the need to browse the file tree:\n\n{repo_instructions}"
        )
    };

    // Repos with "Require tests" on: the checklist carries the test tasks, so the
    // implementation pass (which works task by task) writes them.
    let tests_block =
        if super::review_pr::tests_required(state, &msg.team_id, &msg.repo_owner, &msg.repo_name)
            .await
        {
            "\n\n## Tests are required in this repo\n\
         tasks.md MUST include the test work: for every task that adds or changes behavior, add a \
         task that adds or updates the tests covering it, at the exact test file path, following \
         the repo's test conventions from AGENTS.md / CLAUDE.md (and nested ones such as \
         `e2e/CLAUDE.md`) — find the nearest existing test for the same code with your tools. \
         Include any e2e coverage bookkeeping the repo documents (e.g. adding a new spec to its \
         test map). No test tasks for pure refactors, docs, copy, styling, config values, \
         dependency bumps or generated files."
        } else {
            ""
        };
    let system = format!(
        "You are a planning agent for the {owner}/{repo} repository. \
         Research the codebase using the provided tools, then generate an implementation plan.\n\n\
         You have access to these repos:\n{repos_list}\n\n\
         All tools default to {owner}/{repo} when the `repo` parameter is omitted. \
         To explore another repo, pass `repo` as `owner/name` (e.g. `\"{default_repo}\"`).{instructions_block}{team_block}{tests_block}{feature_block}",
        owner = msg.repo_owner,
        repo = msg.repo_name,
        feature_block = super::feature::planner_block(intent),
    );

    let complexity_guidance = match triage.complexity.as_str() {
        "simple" => {
            r#"## Complexity: SIMPLE (1-3 files)

This is a simple change. Be extremely efficient:
- Use 1-2 `search_code` calls to locate the relevant file(s) and line numbers
- Use `read_file_lines` on the specific section (max 100 lines), NOT whole files
- You should need 2-4 tool calls total, then output the plan
- The plan usually has 1-2 tasks; when the change needs more files, list every one"#
        }
        "medium" => {
            r#"## Complexity: MEDIUM (4-10 files)

This is a medium-sized change. Be efficient:
- Start with `search_code` to find relevant files, symbols, and line numbers
- Use `read_file_lines` for targeted reads (max 100 lines each)
- You should need 4-8 tool calls total
- One task per file the change needs"#
        }
        _ => {
            r#"## Complexity: COMPLEX (10+ files)

This is a complex change. Research thoroughly but stay focused:
- Start with `search_code` to find relevant symbols and entry points
- Use `list_directory` only if you need to understand a module's structure
- Use `read_file_lines` for targeted reads (max 100 lines each)
- Map out the affected code paths before planning
- One task per file the change needs, however many that is"#
        }
    };

    let has_images = !msg.image_attachments.is_empty();
    let image_hint = if has_images {
        "\n\n**Note:** This issue includes image attachments (screenshots, mockups, or designs) below the text. \
         Study them carefully — they may show UI designs to replicate, error messages to fix, \
         or visual specifications that define what to build. Use the images as primary context alongside the text."
    } else {
        ""
    };

    let prompt = format!(
        r#"Generate an implementation plan (openspec) for this issue.

## Issue
Title: {title}
Summary: {summary}

## Original Issue Body
{body}{image_hint}

## What the ticket asks for
Triage read this ticket as: **{intent}** (bug: something is wrong; feature: something new to build; chore: upkeep; question: a request for information). Plan accordingly; the guidance below is for this kind of ticket.

{complexity_guidance}

## Research Instructions

**Start with `search_code`** to find relevant files, symbols, and line numbers.
Then use `read_file_lines` for targeted reads (max 100 lines per call).
Do NOT read entire files. Do NOT browse the file tree unless you need module structure.
Be surgical — search, read the exact lines you need, then output the plan.

**Verify before planning:** Check if the requested change is ALREADY in place. If it is,
set tasks.md to EXACTLY `NO_CHANGES_NEEDED: <reason>`.

**Ambiguous values:** If the issue asks to change a value but does NOT include the new value:
1. Check if the issue body contains URLs. If so, call the appropriate MCP tool to fetch them.
2. Do NOT proactively search external tools unless a URL is present.
3. If you cannot determine the value, set tasks.md to `CLARIFICATION_NEEDED:` followed by one
   block per question a person must answer (a short plain question; why it blocks the change;
   candidate answers you found in the code or the data, separated by `|`, when you have any):
   ```
   CLARIFICATION_NEEDED:
   - question: Which environments get the new limit?
     why: config/limits.yml holds one value per environment.
     options: production only | every environment
     evidence: config/limits.yml:12
   ```
   Ask only what blocks the change, at most a few questions, and never a question the ticket or
   the resolved decisions below already settle. Never ask a person to decide what is CoderHelm's
   own call: closing or replacing a pull request someone else opened is never asked; when told to
   open its own, it leaves the other one open.
   Never ask about CoderHelm's own tooling (missing modules or dependencies, a command not found,
   an install, type-check or lint that can't run): the checks install and retry on their own.
   Never ask about a difference between environments (production content differs from staging):
   the tests run in their own environment with their own data, and that is not a contradiction.
   When the ticket states the root cause, a fix plan and acceptance criteria, follow them; ask only
   when the code or the data contradicts the ticket, with an `evidence:` line citing it
   (`path/to/file.ts:42`, or the lookup id such as `L2`). A question without evidence that checks
   out is dropped and the run follows the ticket's plan.

Otherwise, generate four openspec files:

1. **proposal.md** — Problem statement, proposed approach, scope boundaries, risks
2. **design.md** — Technical design: which files to modify/create (use full paths from repo root e.g. `src/services/payments.ts`), patterns to follow, data flow
3. **tasks.md** — Step-by-step implementation checklist. Each task MUST follow this format:
   {task_format}
   Tasks should be atomic and ordered. Include the full file path from the repo root in every task. \
Only include tasks that can be accomplished by writing code (creating files, editing files, updating config). \
Do NOT include manual verification tasks, post-deploy checks, browser testing, or anything requiring human interaction.

   **Scope:** a task changes a file the ticket names or the change needs (including its tests, \
migrations and generated output). Every file the ticket names gets its task, and there is no \
limit on how many files a plan changes: never leave a needed file out to keep the plan small. \
Leave out padding: refactors, renames, cleanups or optional follow-ups the ticket didn't ask for \
and the change doesn't need.

   **Multi-repo:** If the issue requires changes in MORE THAN ONE repo, group tasks under `## owner/repo` headers:
   ```
   ## owner/repo-a
   - [ ] src/file.rs: Change something
   ## owner/repo-b
   - [ ] src/other.ts: Change something else
   ```
   Only use multi-repo headers when changes span 2+ repos. For single-repo issues, omit headers.
   List the repo groups in the order their pull requests must merge: each PR names the others in that order.
4. **spec.md** — Acceptance criteria as Given/When/Then scenarios

{fix_guidance}

After researching, output the four files using this exact format:

```proposal.md
(content)
```

```design.md
(content)
```

```tasks.md
(content)
```

```spec.md
(content)
```

Then the ```fix_location.json block described above."#,
        fix_guidance = super::fix_location::planner_guidance(intent),
        task_format = super::feature::plan_files::PLANNER_FORMAT,
        intent = intent.as_str(),
        title = msg.title,
        summary = triage.summary,
        body = msg.body,
        complexity_guidance = complexity_guidance,
    );
    // The ticket's history comes right after the issue's own text, then
    // the answers people gave to an earlier run's questions, word for word.
    let prompt = match msg.ticket_history_note.as_deref() {
        Some(h) if !h.trim().is_empty() => format!("{prompt}{h}"),
        _ => prompt,
    };
    let prompt = format!(
        "{prompt}{}",
        common::run_questions::answers_note(&msg.answers)
    );

    let mut tools = read_only_tools();

    // Only load MCP plugins if the issue body contains URLs or external references
    let has_external_refs = msg.body.contains("http://")
        || msg.body.contains("https://")
        || msg.body.contains("notion.so")
        || msg.body.contains("jira")
        || msg.body.contains("confluence");

    // Load MCP plugins and their cached tool schemas
    let mut loaded_mcp_plugins = Vec::new();
    if has_external_refs {
        let mcp_table = if state.config.mcp_configs_table_name.is_empty() {
            &state.config.settings_table_name
        } else {
            &state.config.mcp_configs_table_name
        };
        let mcp_plugins = mcp::merge_plugins(
            mcp::load_team_plugins(&state.dynamo, &state.kms, mcp_table, &msg.team_id).await,
            mcp::load_remote_plugins(
                &state.dynamo,
                &state.kms,
                &msg.team_id,
                &mcp::RemoteScope {
                    table: mcp_table,
                    key_id: &state.config.secrets_key_id,
                    user: super::requester_email(msg),
                    actor: format!("planner, run {run_id}"),
                    read_only: true,
                },
            )
            .await,
        );

        for plugin in &mcp_plugins {
            // Try S3 cache first, then invoke proxy to list tools
            let Some(schemas) = mcp::schemas_for(
                &state.s3,
                &state.config.bucket_name,
                &state.lambda,
                &state.config.mcp_proxy_function_name,
                plugin,
            )
            .await
            else {
                continue;
            };

            let mcp_tool_defs = mcp::to_tool_definitions(&plugin.server_id, &schemas);
            tools.extend(mcp_tool_defs);
            loaded_mcp_plugins.push(plugin.clone());
        }

        // Filter MCP tools for plan pass — only keep read-only tools.
        // Remote servers were already filtered by what each tool says it does.
        let hinted = mcp::hint_filtered(&loaded_mcp_plugins);
        tools.retain(|t| {
            // Keep all built-in tools (no server_id__ prefix)
            if !t.name.contains("__") {
                return true;
            }
            if t.name
                .split_once("__")
                .is_some_and(|(sid, _)| hinted.contains(sid))
            {
                return true;
            }
            // Everything else keeps its risk from the one tool registry:
            // only read-only connection tools stay.
            common::tool_registry::is_read_only(&t.name)
        });
    }

    // Add MCP context to system prompt if we have active plugins
    let mut full_system = system.clone();
    if !loaded_mcp_plugins.is_empty() {
        let plugin_lines: Vec<String> = loaded_mcp_plugins
            .iter()
            .map(|p| format!("- {}", p.server_id))
            .collect();
        full_system.push_str(&format!(
            "\n\nYou have tool-call access to these MCP servers. \
             Only use them if the issue description contains URLs or explicit references \
             to external sources:\n{}",
            plugin_lines.join("\n")
        ));
    }
    for plugin in &loaded_mcp_plugins {
        if let Some(ref prompt) = plugin.custom_prompt {
            full_system.push_str(&format!(
                "\n\n## MCP Server: {}\n{}",
                plugin.server_id, prompt
            ));
        }
    }

    // Live systems the team connected for this repo, if any: read-only lookups
    // so the plan and the code rest on the system's real shape.
    let live = super::live_sources::LiveSources::load(
        state,
        super::live_sources::Role::Planner,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        0,
    )
    .await
    .map(|l| {
        l.with_run(
            run_id,
            super::skills::ticket_origin(
                &msg.repo_owner,
                &msg.repo_name,
                &msg.ticket_id,
                msg.issue_number,
            ),
        )
    });
    if let Some(l) = &live {
        tools.extend(l.tools());
        full_system.push_str(&l.prompt_section());
    }
    // Repository history through the GitHub App: no setup.
    let history = super::github_history::GithubHistory::new(
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        team_repos.clone(),
        "planner",
        0,
    )
    .with_run(
        run_id,
        super::skills::ticket_origin(
            &msg.repo_owner,
            &msg.repo_name,
            &msg.ticket_id,
            msg.issue_number,
        ),
    );
    tools.push(super::github_history::tool());
    full_system.push_str(super::github_history::prompt_line());
    // The code graph: what a change reaches, so the plan names every
    // affected file and repository. A repo without a graph yet gets its
    // first build queued here.
    let graphs = super::code_graph::RepoGraphs::load(
        state,
        &msg.team_id,
        msg.installation_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        team_repos.clone(),
        "planner",
        0,
    )
    .await
    .with_run(
        run_id,
        super::skills::ticket_origin(
            &msg.repo_owner,
            &msg.repo_name,
            &msg.ticket_id,
            msg.issue_number,
        ),
    );
    super::code_graph::offer(
        &graphs,
        super::code_graph::Role::Planner,
        &mut tools,
        &mut full_system,
    );
    // What the repository lets the run do itself (its CMS read API, deploy
    // and end-to-end workflows, migrations and fixtures folders): the
    // planner decides each outside step against it, and queries content
    // before writing about it.
    let abilities = {
        let env =
            super::load_codegen_env(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
        let staging_write = super::feature::load_connection(
            state,
            &msg.team_id,
            &format!("{}/{}", msg.repo_owner, msg.repo_name),
        )
        .await
        .is_some();
        super::outside_work::load(
            github,
            &msg.repo_owner,
            &msg.repo_name,
            &msg.base_branch,
            &env,
            staging_write,
        )
        .await
    };
    if let Some(s) = &abilities.source {
        tools.push(super::outside_work::tool(s));
    }
    full_system.push_str(&abilities.prompt_section());
    // `check_url` on the team's own public pages.
    let web = super::web_check::WebCheck::load(
        state,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        "planner",
        0,
        &format!("{}\n{}", msg.title, msg.body),
    )
    .await
    .with_run(
        run_id,
        super::skills::ticket_origin(
            &msg.repo_owner,
            &msg.repo_name,
            &msg.ticket_id,
            msg.issue_number,
        ),
    );
    tools.push(super::web_check::tool());
    full_system.push_str(&web.prompt_section());
    let ops = super::ops_tools::OpsTools::load(
        state,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        "planner",
        0,
    )
    .await
    .map(|o| {
        o.with_run(
            run_id,
            super::skills::ticket_origin(
                &msg.repo_owner,
                &msg.repo_name,
                &msg.ticket_id,
                msg.issue_number,
            ),
        )
    });
    if let Some(o) = &ops {
        tools.extend(o.tools());
        full_system.push_str(&o.prompt_section());
    }
    // The team's own written skills, read-only guidance.
    let skills = super::skills::TeamSkills::load(
        state,
        &msg.team_id,
        super::skills::Reader::Planner,
        super::skills::ticket_origin(
            &msg.repo_owner,
            &msg.repo_name,
            &msg.ticket_id,
            msg.issue_number,
        ),
        // The files aren't known before the plan: an item scoped to paths
        // is named for the planner to read if the plan touches them.
        super::skills::Work::new(
            format!("{}/{}", msg.repo_owner, msg.repo_name),
            &["planner"],
            Vec::new(),
        ),
    )
    .await
    .map(|s| {
        s.with_run(Some(run_id))
            .with_task(&format!("Planning: {}", msg.title))
    });
    if let Some(s) = &skills {
        tools.extend(s.tools());
        s.offer(&tools);
        full_system.push_str(&s.prompt_section());
        s.record_applied(state).await;
    }

    // Knowledge bases the team attached to the planner.
    let knowledge = super::knowledge::KnowledgeTools::load(state, &msg.team_id, "planner").await;
    if let Some(k) = &knowledge {
        tools.extend(k.tools());
        full_system.push_str(&k.prompt_section());
    }

    let executor = CombinedToolExecutor {
        read_only: ReadOnlyToolExecutor {
            github,
            default_owner: &msg.repo_owner,
            default_repo: &msg.repo_name,
            base_branch: &msg.base_branch,
            allowed_repos: &team_repos,
            read_paths: std::sync::Mutex::new(Vec::new()),
        },
        mcp_plugins: &loaded_mcp_plugins,
        lambda: &state.lambda,
        mcp_proxy_function_name: &state.config.mcp_proxy_function_name,
        history: Some(&history),
        graphs: Some(&graphs),
        ops: ops.as_ref(),
        live: live.as_ref(),
        skills: skills.as_ref(),
        knowledge: knowledge.as_ref(),
        web: Some(&web),
        abilities: Some(&abilities),
        state,
    };

    let mut content_blocks = vec![serde_json::json!({"type": "text", "text": prompt})];
    for img in &msg.image_attachments {
        if let Some(b64) =
            super::download_image_as_base64(&state.s3, &state.config.bucket_name, &img.s3_key).await
        {
            content_blocks.push(serde_json::json!({
                "type": "image",
                "source": { "type": "base64", "media_type": img.media_type, "data": b64 }
            }));
        }
    }

    let mut messages = vec![("user".to_string(), content_blocks)];

    let plan_opts = llm::ConverseOptions {
        // A planner reads the code before it plans; 5 and 8 steps ran out in
        // seconds on ordinary tickets (16 failed runs). The last step is
        // always an answer from what it read (see the agent loop).
        max_turns: planner_turns(
            &triage.complexity,
            live.as_ref().is_some_and(|l| !l.tools().is_empty()),
        ),
        max_tokens: 16384,
        deadline: None,
        effort: None,
    };

    let model_id = provider.primary_model_id();
    let can_look_up = live.as_ref().is_some_and(|l| !l.tools().is_empty());
    let lookups = || live.as_ref().map_or(0, |l| l.lookups_made());
    let mut response = provider::converse(
        state,
        provider,
        common::agent_policy::Agent::Planner,
        model_id,
        &full_system,
        &mut messages,
        &tools,
        &executor,
        usage,
        plan_opts,
        None,
        None,
    )
    .await?;
    // A decision about data that skipped part of the trace (no lookups, no
    // producer, a producer it never opened, no scope) goes back once.
    let read = || {
        executor
            .read_only
            .read_paths
            .lock()
            .map(|r| r.clone())
            .unwrap_or_default()
    };
    let draft = parse_openspec_files(&response, intent);
    let gaps = super::fix_location::gaps(draft.fix.as_ref(), can_look_up, lookups(), &read());
    // The plan's own check (see plan_judge): its history, the environments
    // its lookups hit, and anything close to what the ticket names.
    let history_note = msg.ticket_history_note.as_deref().unwrap_or("");
    let judge_prompt = |files: &PlanResult| {
        super::plan_judge::prompt(
            &msg.title,
            &format!(
                "{}{}",
                msg.body,
                common::run_questions::answers_note(&msg.answers)
            ),
            history_note,
            &live.as_ref().map(|l| l.connected()).unwrap_or_default(),
            &live
                .as_ref()
                .map(|l| l.lookups().into_iter().map(|x| x.line).collect::<Vec<_>>())
                .unwrap_or_default(),
            files,
        )
    };
    let judged = if super::plan_judge::worth_judging(ticket_history, lookups()) {
        super::plan_judge::judge(state, provider, usage, &judge_prompt(&draft)).await
    } else {
        Vec::new()
    };
    let judge_found = !judged.is_empty();
    let plan_findings = super::plan_judge::merge(
        super::plan_judge::structural(&draft, ticket_history, can_look_up),
        judged,
    );
    let mut back = gaps.clone();
    back.extend(
        plan_findings
            .iter()
            .map(super::plan_judge::PlanFinding::line),
    );
    // The new-feature rules' plan sections (see feature): earlier attempts,
    // reuse and the closest sibling.
    let feature_gaps = super::feature::plan_gaps(&draft, intent, &ticket_history.prior);
    back.extend(feature_gaps.iter().cloned());
    // The outside steps: decided in order (the change, CoderHelm, a person
    // with a reason), looked up before claimed, ordered, valued, confirmable.
    let note_gaps = super::outside_work::plan_gaps(&draft, &abilities);
    back.extend(note_gaps.iter().cloned());
    if !back.is_empty() {
        info!(
            ticket_id = %msg.ticket_id,
            trace_gaps = gaps.len(),
            plan_gaps = plan_findings.len(),
            "Plan is incomplete — sending it back"
        );
        messages.push((
            "user".to_string(),
            vec![
                serde_json::json!({"type": "text", "text": super::fix_location::send_back(&back)}),
            ],
        ));
        response = provider::converse(
            state,
            provider,
            common::agent_policy::Agent::Planner,
            model_id,
            &full_system,
            &mut messages,
            &tools,
            &executor,
            usage,
            plan_opts,
            None,
            None,
        )
        .await?;
    }
    if let Some(s) = &skills {
        s.drain_usage_into(usage);
    }

    let mut files = parse_openspec_files(&response, intent);
    // A plan that asks what is settled on the ticket (answers given in
    // CoderHelm, choices people made on its notices), or what CoderHelm
    // decides itself, goes back once with the answers (the model judges
    // each question against them, see `resolved`). What it still asks
    // after that is filtered below.
    let asked = clarification_of(&files.tasks);
    let verdicts = super::resolved::judge(state, provider, &asked, &msg.answers, usage).await;
    let repeats: Vec<(common::run_questions::Question, super::resolved::Verdict)> = asked
        .iter()
        .cloned()
        .zip(verdicts)
        .filter(|(_, v)| !v.may_ask())
        .collect();
    if !repeats.is_empty() {
        info!(
            ticket_id = %msg.ticket_id,
            repeated = repeats.len(),
            "Plan asks a settled question again — sending it back"
        );
        messages.push((
            "user".to_string(),
            vec![serde_json::json!({
                "type": "text",
                "text": super::resolved::send_back(&repeats, &msg.answers),
            })],
        ));
        response = provider::converse(
            state,
            provider,
            common::agent_policy::Agent::Planner,
            model_id,
            &full_system,
            &mut messages,
            &tools,
            &executor,
            usage,
            plan_opts,
            None,
            None,
        )
        .await?;
        files = parse_openspec_files(&response, intent);
    }
    // The stop rules (see stop_gate): a question about the run's own
    // tooling is never asked, one resting on a difference between
    // environments is refused, and when the ticket states its own diagnosis
    // only a contradiction backed by evidence that checks out is asked.
    // Held-back questions send the plan back once; what is still not
    // admissible after that never reaches a person.
    let lookup_lines = || {
        live.as_ref()
            .map(|l| l.lookups().into_iter().map(|x| x.line).collect::<Vec<_>>())
            .unwrap_or_default()
    };
    let asked = clarification_of(&files.tasks);
    if !asked.is_empty() {
        let s = super::stop_gate::screen_questions(
            state,
            provider,
            usage,
            msg,
            github,
            run_id,
            &asked,
            &lookup_lines(),
        )
        .await;
        if s.changed() {
            let diagnosis = super::stop_gate::cached_diagnosis(run_id);
            messages.push((
                "user".to_string(),
                vec![serde_json::json!({
                    "type": "text",
                    "text": common::stop_rules::screen::send_back(&s, diagnosis.as_ref()),
                })],
            ));
            response = provider::converse(
                state,
                provider,
                common::agent_policy::Agent::Planner,
                model_id,
                &full_system,
                &mut messages,
                &tools,
                &executor,
                usage,
                plan_opts,
                None,
                None,
            )
            .await?;
            files = parse_openspec_files(&response, intent);
            let again = clarification_of(&files.tasks);
            if !again.is_empty() {
                let s = super::stop_gate::screen_questions(
                    state,
                    provider,
                    usage,
                    msg,
                    github,
                    run_id,
                    &again,
                    &lookup_lines(),
                )
                .await;
                if s.changed() {
                    if s.ask.is_empty() {
                        if let Some(f) = s.tooling_failure() {
                            return Err(f.reason().into());
                        }
                        return Err(format!(
                            "The planner stopped only on questions the stop rules refuse, even \
                             after it was sent back to plan: {}",
                            s.notes().join(" ")
                        )
                        .into());
                    }
                    files.tasks = common::stop_rules::screen::clarification(&s.ask);
                }
            }
        }
    }
    // The decision's own questions (where the fix belongs) are held to the
    // same rules: what may not stop the run is left out.
    if let Some(d) = files.fix.as_mut().filter(|d| !d.questions.is_empty()) {
        let qs = common::run_questions::from_lines(&d.questions);
        let s = super::stop_gate::screen_questions(
            state,
            provider,
            usage,
            msg,
            github,
            run_id,
            &qs,
            &lookup_lines(),
        )
        .await;
        if s.changed() {
            d.questions = s.ask.into_iter().map(|q| q.question).collect();
        }
    }
    let gaps = super::fix_location::gaps(files.fix.as_ref(), can_look_up, lookups(), &read());
    if !gaps.is_empty() && msg.person_direction.is_some() {
        // A person told CoderHelm where the fix is and to go ahead: their
        // call stands; what the trace didn't show is noted, not blocking.
        warn!(ticket_id = %msg.ticket_id, gaps = ?gaps, "Plan's trace incomplete — proceeding as a person directed");
        super::add_progress_note(
            state,
            &msg.team_id,
            run_id,
            &format!(
                "Proceeding as directed ({}). Not shown by the plan's own trace: {}",
                common::truncate_str(msg.person_direction.as_deref().unwrap_or(""), 200),
                gaps.join(" ")
            ),
        )
        .await;
    } else if !gaps.is_empty() {
        warn!(ticket_id = %msg.ticket_id, gaps = ?gaps, "Plan's trace still incomplete — no code on it");
        files.fix = files.fix.take().map(|d| {
            if super::fix_location::needs_lookup(Some(&d), can_look_up, lookups(), &read()) {
                super::fix_location::mark_unchecked(d)
            } else {
                super::fix_location::mark_untraced(d, &gaps)
            }
        });
    }
    if !note_gaps.is_empty() {
        let left = super::outside_work::plan_gaps(&files, &abilities);
        if !left.is_empty() {
            warn!(ticket_id = %msg.ticket_id, gaps = ?left, "Outside steps still incomplete after one send-back");
            super::add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Plan check (outside steps): {}", left.join(" ")),
            )
            .await;
            if let Some(d) = files.fix.as_mut() {
                d.open_gaps.extend(left);
            }
        }
    }
    if !feature_gaps.is_empty() {
        let left = super::feature::plan_gaps(&files, intent, &ticket_history.prior);
        if !left.is_empty() {
            warn!(ticket_id = %msg.ticket_id, gaps = ?left, "Feature plan sections still missing after one send-back");
            super::add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Plan check (new-feature rules): {}", left.join(" ")),
            )
            .await;
        }
    }
    // What the plan's own check still finds after the send-back stays on
    // the decision: the ticket, the PR and the reviewer see it.
    if !plan_findings.is_empty() {
        let mut left = super::plan_judge::structural(&files, ticket_history, can_look_up);
        if judge_found {
            let again =
                super::plan_judge::judge(state, provider, usage, &judge_prompt(&files)).await;
            left = super::plan_judge::merge(left, again);
        }
        if !left.is_empty() {
            warn!(ticket_id = %msg.ticket_id, gaps = ?left, "Plan check still finds gaps after one send-back");
            let lines: Vec<String> = left
                .iter()
                .map(super::plan_judge::PlanFinding::line)
                .collect();
            super::add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &format!("Plan check: {} left open: {}", lines.len(), lines.join(" ")),
            )
            .await;
            if let Some(d) = files.fix.as_mut() {
                d.open_gaps = lines;
            }
        }
    }

    // The team's plan-review agents read the draft before it's kept; with
    // gaps, the planner gets one revision turn on the same conversation.
    if let Some(s) = skills.as_ref().filter(|_| reviewable(&files)) {
        if !s.reviewers_for("plan").is_empty() {
            let started = std::time::Instant::now();
            let before = usage.clone();
            let host = s.host(
                &executor,
                Some(super::agent_tools::RepoReads {
                    github,
                    owner: &msg.repo_owner,
                    repo: &msg.repo_name,
                    git_ref: &msg.base_branch,
                    http: &state.http,
                }),
            );
            let reviews = s
                .review(state, "plan", "draft plan", &review_draft(&files), &host)
                .await;
            s.drain_usage_into(usage);
            let advice = super::skills::advice_block(&reviews);
            let mut revised = false;
            if !advice.is_empty() {
                messages.push((
                    "user".to_string(),
                    vec![serde_json::json!({"type": "text", "text": revision_prompt(&advice)})],
                ));
                let one_turn = llm::ConverseOptions {
                    max_turns: 1,
                    ..plan_opts
                };
                match provider::converse(
                    state,
                    provider,
                    common::agent_policy::Agent::Planner,
                    model_id,
                    &full_system,
                    &mut messages,
                    &tools,
                    &executor,
                    usage,
                    one_turn,
                    None,
                    None,
                )
                .await
                {
                    Ok(text) => {
                        let (kept, took) =
                            take_revision(files, parse_openspec_files(&text, intent));
                        files = kept;
                        revised = took;
                        if !took {
                            warn!(ticket_id = %msg.ticket_id, "Plan revision after review didn't parse; the draft stands");
                        }
                    }
                    Err(e) => {
                        warn!(ticket_id = %msg.ticket_id, error = %e, "Plan revision after review failed; the draft stands")
                    }
                }
            }
            let cost = (usage.cost_usd - before.cost_usd).max(0.0);
            info!(
                ticket_id = %msg.ticket_id,
                reviewers = reviews.len(),
                with_gaps = reviews.iter().filter(|r| r.gaps.is_some()).count(),
                revised,
                cost_usd = cost,
                "Plan reviewed by team agents"
            );
            super::write_pass_trace_with(
                state,
                &msg.team_id,
                run_id,
                "plan_review",
                started,
                &before,
                usage,
                None,
                Some(&super::skills::review_detail("plan", &reviews, revised)),
            )
            .await;
            super::add_progress_note(
                state,
                &msg.team_id,
                run_id,
                &super::skills::review_note("plan", &reviews, revised, cost),
            )
            .await;
        }
    }

    // Never ask a person what is settled, or what CoderHelm decides itself:
    // the decision's questions and a clarification are checked once more.
    if !msg.answers.is_empty() {
        let asked = clarification_of(&files.tasks);
        if !asked.is_empty() {
            let v = super::resolved::judge(state, provider, &asked, &msg.answers, usage).await;
            if v.iter().any(|v| !v.may_ask()) {
                files.tasks = super::resolved::keep_open(&asked, &v);
            }
        }
    }
    // The decision's questions don't stop the run; each is checked against
    // everything that settles it on the ticket: the answers, the ticket's
    // own text (a ticket that says it replaces earlier work answers a
    // question about that work) and what people wrote on it. A settled one
    // is dropped here, so it is neither asked on the ticket nor listed in
    // the pull request again.
    if let Some(d) = files.fix.as_mut().filter(|d| !d.questions.is_empty()) {
        let notes = super::open_pr_choice::load_record(state, &msg.team_id, &msg.ticket_id)
            .await
            .notes;
        let settled = super::resolved::settled_on_ticket(
            &msg.answers,
            &ticket_text(&msg.title, &msg.body),
            &notes,
        );
        let qs = common::run_questions::from_lines(&d.questions);
        let v = super::resolved::judge(state, provider, &qs, &settled, usage).await;
        if v.iter().any(|v| !v.may_ask()) {
            info!(
                ticket_id = %msg.ticket_id,
                dropped = v.iter().filter(|v| !v.may_ask()).count(),
                "The ticket already settles some of the plan's questions — not asking them"
            );
            d.questions = super::resolved::open_only(qs, &v)
                .into_iter()
                .map(|q| q.question)
                .collect();
        }
    }

    // The questions only a person can answer are part of the plan people read.
    if let Some(d) = files.fix.as_ref() {
        files.proposal = with_questions(&files.proposal, d);
    }

    // Write openspec to S3
    let prefix = crate::passes::openspec_prefix(
        &msg.team_id,
        &msg.ticket_id,
        &msg.repo_owner,
        &msg.repo_name,
    );
    for (name, content) in [
        ("proposal.md", &files.proposal),
        ("design.md", &files.design),
        ("tasks.md", &files.tasks),
        ("spec.md", &files.spec),
    ] {
        let key = format!("{prefix}/{name}");
        state
            .s3
            .put_object()
            .bucket(&state.config.bucket_name)
            .key(&key)
            .body(content.as_bytes().to_vec().into())
            .content_type("text/markdown")
            .send()
            .await?;
    }

    // The whole job end to end, as an openspec file people can read; the
    // structured steps ride in the decision's JSON below.
    let end_to_end = files
        .fix
        .as_ref()
        .map(super::fix_location::end_to_end_markdown)
        .unwrap_or_default();
    let _ = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(format!("{prefix}/{END_TO_END_FILE}"))
        .body(end_to_end.trim().as_bytes().to_vec().into())
        .content_type("text/markdown")
        .send()
        .await;

    // The decision on where the fix belongs, so a reused plan keeps it.
    if let Some(fix) = &files.fix {
        if let Ok(json) = serde_json::to_string(fix) {
            let _ = state
                .s3
                .put_object()
                .bucket(&state.config.bucket_name)
                .key(format!("{prefix}/{}", super::fix_location::FILE))
                .body(json.into_bytes().into())
                .content_type("application/json")
                .send()
                .await;
        }
        info!(
            fix_location = fix.effective_location().as_str(),
            "Planner decided where the fix belongs"
        );
    } else {
        // No decision this time: an older one must not ride along with it.
        let _ = state
            .s3
            .delete_object()
            .bucket(&state.config.bucket_name)
            .key(format!("{prefix}/{}", super::fix_location::FILE))
            .send()
            .await;
    }
    // The key this plan is cached under; without it the plan is never reused.
    match msg.plan_cache_key.as_deref() {
        Some(k) => {
            let _ = state
                .s3
                .put_object()
                .bucket(&state.config.bucket_name)
                .key(format!("{prefix}/{PLAN_KEY_FILE}"))
                .body(k.as_bytes().to_vec().into())
                .content_type("text/plain")
                .send()
                .await;
        }
        None => {
            let _ = state
                .s3
                .delete_object()
                .bucket(&state.config.bucket_name)
                .key(format!("{prefix}/{PLAN_KEY_FILE}"))
                .send()
                .await;
        }
    }

    // This run's plan now: its continuation picks it up as is.
    mark_plan_run(state, msg, run_id).await;

    info!("Plan openspec written to S3");

    // Store context hash so we can detect when ticket content changes
    let context_hash = compute_ticket_context_hash(msg);
    let hash_key = format!("{prefix}/context_hash.txt");
    let _ = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(&hash_key)
        .body(context_hash.as_bytes().to_vec().into())
        .content_type("text/plain")
        .send()
        .await;
    // And the ticket text itself, so a later edit can show the agent exactly
    // what changed (old → new) instead of only the new text.
    let _ = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(format!("{prefix}/{TICKET_TEXT_FILE}"))
        .body(ticket_text(&msg.title, &msg.body).into_bytes().into())
        .content_type("text/markdown")
        .send()
        .await;

    Ok(files)
}

/// The questions in a `CLARIFICATION_NEEDED:` tasks file; none otherwise.
fn clarification_of(tasks: &str) -> Vec<common::run_questions::Question> {
    tasks
        .trim_start()
        .strip_prefix("CLARIFICATION_NEEDED:")
        .map(common::run_questions::parse)
        .unwrap_or_default()
}

/// The proposal with the decision's questions for a person after it, once.
fn with_questions(proposal: &str, d: &super::fix_location::FixDecision) -> String {
    let qs = super::fix_location::questions_markdown(d);
    if qs.is_empty() || proposal.contains(QUESTIONS_HEADING) {
        return proposal.to_string();
    }
    let list = qs.split_once('\n').map_or("", |(_, rest)| rest).trim_end();
    format!("{}\n\n{QUESTIONS_HEADING}\n{list}", proposal.trim_end())
}

/// The proposal's heading for what only a person can answer.
const QUESTIONS_HEADING: &str = "## Questions only a person can answer";

fn parse_openspec_files(response: &str, intent: super::triage::TicketIntent) -> PlanResult {
    let filenames = ["proposal.md", "design.md", "tasks.md", "spec.md"];
    let mut contents: Vec<String> = Vec::new();

    for filename in &filenames {
        let marker = format!("```{filename}");
        if let Some(start) = response.find(&marker) {
            let content_start = response[start..]
                .find('\n')
                .map(|i| start + i + 1)
                .unwrap_or(start);
            let end = response[content_start..]
                .find("```")
                .map(|i| content_start + i)
                .unwrap_or(response.len());
            contents.push(response[content_start..end].trim().to_string());
        } else {
            contents.push(String::new());
        }
    }

    let tasks_content = contents[2].clone();
    let repo_tasks = parse_repo_tasks(&tasks_content);

    PlanResult {
        proposal: contents.remove(0),
        design: contents.remove(0),
        tasks: contents.remove(0),
        spec: contents.remove(0),
        repo_tasks,
        fix: super::fix_location::parse_for(response, intent),
    }
}

/// Parse `## owner/repo` headers in tasks.md for multi-repo support.
/// Returns empty vec if no repo headers found (single-repo mode).
fn parse_repo_tasks(tasks: &str) -> Vec<RepoTasks> {
    let mut result = Vec::new();
    let mut current_owner = String::new();
    let mut current_name = String::new();
    let mut current_tasks = String::new();

    for line in tasks.lines() {
        if let Some(repo_str) = line.strip_prefix("## ") {
            // Save previous repo if any
            if !current_owner.is_empty() {
                result.push(RepoTasks {
                    owner: current_owner.clone(),
                    name: current_name.clone(),
                    tasks: current_tasks.trim().to_string(),
                });
            }
            // Parse owner/name
            let repo_str = repo_str.trim();
            if let Some((owner, name)) = repo_str.split_once('/') {
                current_owner = owner.to_string();
                current_name = name.to_string();
                current_tasks = String::new();
            }
        } else if !current_owner.is_empty() {
            current_tasks.push_str(line);
            current_tasks.push('\n');
        }
    }

    // Save last repo
    if !current_owner.is_empty() && !current_tasks.trim().is_empty() {
        result.push(RepoTasks {
            owner: current_owner,
            name: current_name,
            tasks: current_tasks.trim().to_string(),
        });
    }

    // Only return multi-repo if we found 2+ repos
    if result.len() < 2 {
        return vec![];
    }

    result
}

// ─── Read-only tools for plan pass ──────────────────────────

fn read_only_tools() -> Vec<ToolDefinition> {
    let repo_prop = json!({"type": "string", "description": "Repository as owner/name. Omit to use the default (issue) repo."});
    vec![
        ToolDefinition {
            name: "search_code".to_string(),
            description: "Search for code by keyword, function name, type, or symbol. Returns matching file paths with line numbers and code fragments. Always start here.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query (e.g. function name, type, import, error message)"},
                    "repo": repo_prop
                },
                "required": ["query"]
            }),
        },
        ToolDefinition {
            name: "read_file_lines".to_string(),
            description: "Read specific lines from a file (1-indexed, inclusive). Use search_code first to find the right file and line numbers.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "start_line": {"type": "integer", "description": "First line to read (1-indexed)"},
                    "end_line": {"type": "integer", "description": "Last line to read (inclusive, max 100 lines per call)"},
                    "repo": repo_prop
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "list_directory".to_string(),
            description: "List contents of a directory. Use to understand module structure.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Directory path relative to repo root"},
                    "repo": repo_prop
                },
                "required": ["path"]
            }),
        },
    ]
}

struct ReadOnlyToolExecutor<'a> {
    github: &'a GitHubClient,
    default_owner: &'a str,
    default_repo: &'a str,
    base_branch: &'a str,
    allowed_repos: &'a [String],
    /// `owner/repo:path` of every file read, to check a decision cites code
    /// the planner actually opened.
    read_paths: std::sync::Mutex<Vec<String>>,
}

impl<'a> ReadOnlyToolExecutor<'a> {
    /// Resolve owner/repo from the optional `repo` param in tool input.
    /// Falls back to default (issue) repo. Returns an error if the repo isn't in the allowed list.
    fn resolve_repo(
        &self,
        input: &serde_json::Value,
    ) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
        let repo_param = input.get("repo").and_then(|v| v.as_str());
        match repo_param {
            Some(full) => {
                let parts: Vec<&str> = full.splitn(2, '/').collect();
                if parts.len() != 2 {
                    return Err(format!("Invalid repo format: {full}. Use owner/name.").into());
                }
                // If we have an allowed list, check it; otherwise allow any (fallback for empty list)
                if !self.allowed_repos.is_empty() && !self.allowed_repos.iter().any(|r| r == full) {
                    return Err(format!("Repo {full} is not in the connected repos list.").into());
                }
                Ok((parts[0].to_string(), parts[1].to_string()))
            }
            None => Ok((
                self.default_owner.to_string(),
                self.default_repo.to_string(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for ReadOnlyToolExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let (owner, repo) = self.resolve_repo(input)?;
        let branch = &self.base_branch;
        match name {
            "read_file_lines" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let start = input
                    .get("start_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1) as usize;
                let end = input
                    .get("end_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(start as u64 + 100) as usize;
                // Cap at 100 lines per call to limit token usage
                let end = end.min(start + 100);
                let content = self
                    .github
                    .read_file_lines(&owner, &repo, path, branch, start, end)
                    .await?;
                if let Ok(mut r) = self.read_paths.lock() {
                    r.push(format!("{owner}/{repo}:{}", path.trim_start_matches('/')));
                }
                Ok(json!(content))
            }
            "search_code" => {
                let query = input
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing query")?;
                let results = self
                    .github
                    .search_code(&owner, &repo, branch, query)
                    .await?;
                let lines: Vec<String> = results
                    .iter()
                    .map(|r| {
                        if r.matches.is_empty() {
                            r.path.clone()
                        } else {
                            format!("{}\n{}", r.path, r.matches.join("\n"))
                        }
                    })
                    .collect();
                Ok(json!(lines.join("\n---\n")))
            }
            "list_directory" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let entries = self
                    .github
                    .list_directory(&owner, &repo, path, branch)
                    .await?;
                let lines: Vec<String> = entries
                    .iter()
                    .map(|e| format!("{}: {}", e.entry_type, e.name))
                    .collect();
                Ok(json!(lines.join("\n")))
            }
            _ => Err(format!("Unknown tool: {name}").into()),
        }
    }
}

/// Fetch enabled repos for a team from DynamoDB.
pub async fn fetch_team_repos(state: &WorkerState, team_id: &str) -> Vec<String> {
    fetch_team_repo_list(state, team_id)
        .await
        .into_iter()
        .filter(|r| r.enabled)
        .map(|r| r.full_name)
        .collect()
}

/// Every repo connected to the team (a REPO# row), enabled for runs or not.
/// Jira repo selection needs the disabled ones too, so a ticket that names a
/// connected-but-disabled repo stops out loud instead of being re-routed.
pub async fn fetch_team_repo_list(
    state: &WorkerState,
    team_id: &str,
) -> Vec<super::repo_pick::TeamRepo> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .expression_attribute_values(
            ":prefix",
            aws_sdk_dynamodb::types::AttributeValue::S("REPO#".to_string()),
        )
        .send()
        .await;

    match result {
        Ok(output) => output
            .items()
            .iter()
            .filter_map(|item| {
                let full_name = item.get("repo_name").and_then(|v| v.as_s().ok())?.clone();
                let enabled = item
                    .get("enabled")
                    .and_then(|v| v.as_bool().ok())
                    .copied()
                    .unwrap_or(false);
                Some(super::repo_pick::TeamRepo { full_name, enabled })
            })
            .collect(),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch team repos for planner");
            Vec::new()
        }
    }
}

/// Fetch human-assigned repo roles (repo_name → role) for a team. Only repos
/// that have a role set are returned. Used by Jira repo selection to resolve
/// ambiguous tickets to the team's designated repo for that domain.
pub async fn fetch_team_repo_roles(
    state: &WorkerState,
    team_id: &str,
) -> std::collections::HashMap<String, String> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .expression_attribute_values(
            ":prefix",
            aws_sdk_dynamodb::types::AttributeValue::S("REPO#".to_string()),
        )
        .send()
        .await;

    match result {
        Ok(output) => output
            .items()
            .iter()
            .filter_map(|item| {
                let name = item.get("repo_name").and_then(|v| v.as_s().ok())?;
                let role = item
                    .get("role")
                    .and_then(|v| v.as_s().ok())
                    .filter(|s| !s.is_empty())?;
                Some((name.clone(), role.clone()))
            })
            .collect(),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch repo roles");
            std::collections::HashMap::new()
        }
    }
}

// ─── Combined tool executor (read-only + MCP) ──────────────

struct CombinedToolExecutor<'a> {
    read_only: ReadOnlyToolExecutor<'a>,
    mcp_plugins: &'a [mcp::McpPlugin],
    lambda: &'a aws_sdk_lambda::Client,
    mcp_proxy_function_name: &'a str,
    /// Repository history (commits, blame, pull requests, releases, deploys).
    history: Option<&'a super::github_history::GithubHistory>,
    /// The code graph, shared with every agent that reads code.
    graphs: Option<&'a super::code_graph::RepoGraphs>,
    /// Monitoring, docs and pipeline connections (New Relic, Confluence,
    /// CloudWatch metrics, Airflow) and the AWS identity check.
    ops: Option<&'a super::ops_tools::OpsTools>,
    /// The team's connected live systems, read-only.
    live: Option<&'a super::live_sources::LiveSources>,
    /// `check_url` on the team's own public pages.
    web: Option<&'a super::web_check::WebCheck>,
    /// `query_content` on the repository's CMS read API.
    abilities: Option<&'a super::outside_work::Abilities>,
    /// The team's written skills, read-only.
    skills: Option<&'a super::skills::TeamSkills>,
    /// Knowledge bases attached to the planner.
    knowledge: Option<&'a super::knowledge::KnowledgeTools>,
    state: &'a WorkerState,
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for CombinedToolExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(k) = self.knowledge {
            if let Some(out) = k.handle(self.state, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(graphs) = self.graphs {
            if let Some(out) = graphs.handle(self.state, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(history) = self.history {
            if let Some(out) = history
                .handle(self.state, self.read_only.github, name, input)
                .await
            {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(a) = self.abilities {
            if let Some(out) = super::outside_work::handle(&self.state.http, a, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(web) = self.web {
            if let Some(out) = web
                .handle(self.state, self.read_only.github, name, input)
                .await
            {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(ops) = self.ops {
            if let Some(out) = ops.handle(self.state, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(live) = self.live {
            if let Some(out) = live.handle(self.state, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(skills) = self.skills {
            if let Some(out) = skills
                .handle(
                    self.state,
                    name,
                    input,
                    &skills.host(
                        self,
                        Some(super::agent_tools::RepoReads {
                            github: self.read_only.github,
                            owner: self.read_only.default_owner,
                            repo: self.read_only.default_repo,
                            git_ref: self.read_only.base_branch,
                            http: &self.state.http,
                        }),
                    ),
                )
                .await
            {
                return Ok(serde_json::json!(out));
            }
        }
        // Check if this is an MCP tool (prefixed with server_id__)
        if let Some((server_id, tool_name)) = name.split_once("__") {
            let plugin = self
                .mcp_plugins
                .iter()
                .find(|p| p.server_id == server_id)
                .ok_or_else(|| format!("No MCP plugin found for server: {server_id}"))?;

            return mcp::call_tool(
                self.lambda,
                self.mcp_proxy_function_name,
                plugin,
                tool_name,
                input,
            )
            .await;
        }

        // Fall through to read-only tools
        self.read_only.execute(name, input).await
    }
}

/// S3 file (next to the openspec) holding the ticket text a plan was built from.
pub const TICKET_TEXT_FILE: &str = "ticket.md";

/// Steps the planner may take. With live data to look up, a ticket triaged
/// as simple can still be a data question that takes several lookups to
/// trace, so it always gets room to follow the data.
fn planner_turns(complexity: &str, can_look_up_data: bool) -> usize {
    let by_size = match complexity {
        "simple" => 8,
        "medium" => 15,
        _ => 25,
    };
    if can_look_up_data {
        by_size.max(20)
    } else {
        by_size
    }
}

/// The ticket text as stored next to its plan and compared on an update.
pub fn ticket_text(title: &str, body: &str) -> String {
    format!("# {title}\n\n{body}")
}

/// Compute a hash of ticket content (title + body + image attachment keys)
/// for change detection. Delegates to the shared crate so the gateway's
/// webhook gate and the worker's plan cache / run records all agree.
/// (Existing S3 plan caches keyed by the old DefaultHasher format are
/// invalidated once by this change — plans simply regenerate.)
pub fn compute_ticket_context_hash(msg: &TicketMessage) -> String {
    let image_keys: Vec<String> = msg
        .image_attachments
        .iter()
        .map(|img| img.s3_key.clone())
        .collect();
    common::ticket_context_hash(&msg.title, &msg.body, &image_keys)
}

#[cfg(test)]
mod planner_turns_tests {
    use super::planner_turns;

    #[test]
    fn live_data_gives_room_to_trace_it() {
        assert_eq!(planner_turns("simple", false), 8);
        assert_eq!(planner_turns("simple", true), 20);
        assert_eq!(planner_turns("medium", true), 20);
        assert_eq!(planner_turns("complex", true), 25);
    }
}

#[cfg(test)]
mod plan_review_tests {
    use super::*;

    fn plan(tasks: &str) -> PlanResult {
        PlanResult {
            proposal: "Add a rollback path".into(),
            design: "Design".into(),
            tasks: tasks.into(),
            spec: "Spec".into(),
            repo_tasks: vec![],
            fix: None,
        }
    }

    #[test]
    fn only_a_plan_of_work_is_reviewed() {
        assert!(reviewable(&plan("- [ ] 1. Add the path")));
        assert!(!reviewable(&plan("NO_CHANGES_NEEDED: already there")));
        assert!(!reviewable(&plan("  CLARIFICATION_NEEDED: which env?")));
        assert!(!reviewable(&plan("")));
        let d = review_draft(&plan("- [ ] 1. Add the path"));
        assert!(d.contains("## proposal.md\nAdd a rollback path"));
        assert!(d.contains("## spec.md\nSpec"));
        assert!(d.contains("## tasks.md\n- [ ] 1. Add the path"));
        assert!(
            !d.contains("design.md"),
            "the reviewers read proposal, spec and tasks"
        );
    }

    #[test]
    fn a_revision_that_doesnt_parse_keeps_the_draft() {
        let draft = plan("- [ ] 1. Add the path");
        let (kept, took) = take_revision(
            draft,
            parse_openspec_files("Sorry, here are my thoughts.", Default::default()),
        );
        assert!(!took);
        assert_eq!(kept.tasks, "- [ ] 1. Add the path");

        let revised = "```proposal.md\nAdd a rollback path, tested\n```\n```design.md\nD2\n```\n\
                       ```tasks.md\n- [ ] 1. Add the path\n- [ ] 2. Test the rollback\n```\n\
                       ```spec.md\nS2\n```";
        let (kept, took) = take_revision(
            plan("- [ ] 1. Add the path"),
            parse_openspec_files(revised, Default::default()),
        );
        assert!(took);
        assert!(kept.tasks.contains("Test the rollback"));
        assert!(revision_prompt("## notes").contains("COMPLETE plan again"));
    }

    #[test]
    fn a_cached_plan_is_keyed_to_the_review_it_went_through() {
        let k = |review: &str| plan_cache_key("ctx", "mem", "hist", "team", review);
        assert_ne!(k("no-reviewers"), k("one-reviewer"));
        assert_eq!(k("same"), k("same"));
    }
}

#[cfg(test)]
mod questions_tests {
    use super::*;
    use crate::passes::triage::TicketIntent;

    #[test]
    fn the_plans_questions_go_in_the_proposal_once() {
        let reply = "```proposal.md\nCreate the reporting user.\n```\n```design.md\nD\n```\n\
                     ```tasks.md\n- [ ] a.tf: add the secret\n```\n```spec.md\nS\n```\n\
                     ```fix_location.json\n{\"fix_location\":\"code\",\"questions\":[\"Use the existing schema?\"]}\n```";
        let files = parse_openspec_files(reply, TicketIntent::Feature);
        let d = files.fix.as_ref().unwrap();
        assert_eq!(d.intent, TicketIntent::Feature);
        let p = with_questions(&files.proposal, d);
        assert_eq!(
            p,
            "Create the reporting user.\n\n## Questions only a person can answer\n1. Use the existing schema?"
        );
        assert_eq!(with_questions(&p, d), p, "added once");
        let none = crate::passes::fix_location::FixDecision::default();
        assert_eq!(with_questions("P", &none), "P");
    }

    #[test]
    fn the_planner_is_versioned_for_intent_and_history() {
        assert!(PLANNER_VERSION.contains("intent-history"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_planner_gets_the_graph_tools() {
        use super::super::code_graph::{offer, Avail, RepoGraphs, Role, TOOLS};
        let graphs = RepoGraphs::with(
            "t",
            1,
            "acme/app",
            Avail::Building(common::code_graph::Shown::Queued),
            vec!["acme/api".into()],
            "planner",
            0,
        );
        let mut tools = read_only_tools();
        let mut system = String::new();
        assert!(offer(&graphs, Role::Planner, &mut tools, &mut system));
        for name in TOOLS {
            let t = tools.iter().find(|t| t.name == name).expect(name);
            // The planner may look at the team's other repositories.
            assert!(t.input_schema["properties"].get("repo").is_some());
        }
        assert!(system.contains("graph_impact"));
    }
}
