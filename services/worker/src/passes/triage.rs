use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::agent::{llm, provider};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TriageResult {
    pub complexity: String,
    pub summary: String,
    pub branch_slug: String,
    pub clarity: String,
    pub questions: Vec<String>,
}

pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Result<TriageResult, Box<dyn std::error::Error + Send + Sync>> {
    let system = "You are a ticket triage agent for a GitHub issue. \
                  Analyze the issue and classify it. Return only valid JSON.";

    // Give the classifier a glimpse of the actual codebase — complexity is
    // defined in file counts, which is unknowable from ticket text alone.
    // Top-level structure is cheap (one tree call) and materially grounds
    // the simple/medium/complex guess. Best-effort: skipped on any error.
    let tree_hint = match github
        .get_tree(&msg.repo_owner, &msg.repo_name, &msg.base_branch)
        .await
    {
        Ok(tree) => {
            let mut top_dirs: Vec<String> = tree
                .iter()
                .filter_map(|e| e.path.split('/').next().map(|s| s.to_string()))
                .collect();
            top_dirs.sort();
            top_dirs.dedup();
            top_dirs.truncate(40);
            if top_dirs.is_empty() {
                String::new()
            } else {
                format!(
                    "\n\nRepository top-level structure (for sizing the change):\n{}",
                    top_dirs.join(", ")
                )
            }
        }
        Err(_) => String::new(),
    };

    let has_images = !msg.image_attachments.is_empty();
    let image_hint = if has_images {
        "\n\nIMPORTANT: This ticket includes image attachments (screenshots, mockups, or designs) below. \
         Treat them as primary context — they may contain UI designs, error messages, architecture diagrams, \
         or visual specifications that fully describe what needs to be built. \
         A ticket with a clear title and descriptive images IS clear enough to implement, even if the text body is short or empty."
    } else {
        ""
    };

    let prompt = format!(
        r#"Analyze this GitHub issue and return a JSON classification.

## Issue
Repository: {owner}/{repo}
Number: #{number}
Title: {title}

Body:
{body}{image_hint}{tree_hint}

## Instructions
Return a JSON object with these fields:
- "complexity": "simple" (1-3 files), "medium" (4-10 files), "complex" (10+ files)
- "summary": one-line summary of what needs to be implemented
- "branch_slug": short kebab-case slug for the branch suffix (e.g., "add-retry-logic")
- "clarity": "clear" if you have enough info to implement, "needs_clarification" if not
- "questions": list of specific questions if clarity is "needs_clarification" (empty list if clear)

Rules:
- If the issue is vague, ambiguous, or missing acceptance criteria, set clarity to "needs_clarification".
- If attached images clearly show what to build (e.g., a UI mockup, a design, an error to fix), that counts as sufficient context — set clarity to "clear".
- Return ONLY the JSON object, no other text."#,
        owner = msg.repo_owner,
        repo = msg.repo_name,
        number = msg.issue_number,
        title = msg.title,
        body = msg.body,
        image_hint = image_hint,
        tree_hint = tree_hint,
    );

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

    let model_id = provider.primary_model_id();
    let response = provider::converse(
        state,
        provider,
        model_id,
        system,
        &mut messages,
        &[],
        &NoOpExecutor,
        usage,
        llm::ConverseOptions {
            max_turns: 1,
            max_tokens: 16384,
            deadline: None,
        },
        None,
        None,
    )
    .await?;

    // Parse JSON from response. Tolerant extraction: take the outermost
    // {...} span, so a prose preamble or trailing commentary (which used to
    // hard-fail the whole run on serde error) doesn't matter.
    let raw = response.trim();
    let end = raw.rfind('}').map(|e| e + 1).unwrap_or(raw.len());
    // Prose preambles can contain '{' ("Analyzing {ticket}…") — try each
    // candidate start until one parses.
    let mut result: Option<TriageResult> = None;
    let mut search_from = 0usize;
    for _ in 0..5 {
        let Some(start) = raw[search_from..].find('{').map(|p| p + search_from) else {
            break;
        };
        if start >= end {
            break;
        }
        if let Ok(parsed) = serde_json::from_str::<TriageResult>(&raw[start..end]) {
            result = Some(parsed);
            break;
        }
        search_from = start + 1;
    }
    let result = result.ok_or_else(|| {
        format!(
            "Triage returned unparseable JSON: {}",
            common::truncate_str(raw, 200)
        )
    })?;
    info!(complexity = %result.complexity, clarity = %result.clarity, "Triage complete");
    Ok(result)
}

/// No-op tool executor for passes that don't need tools.
pub(crate) struct NoOpExecutor;

#[async_trait::async_trait]
impl llm::ToolExecutor for NoOpExecutor {
    async fn execute(
        &self,
        name: &str,
        _input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!("No tools available in triage pass (called: {name})").into())
    }
}

/// Extract distinctive, code-like identifiers from a ticket that are likely to
/// appear verbatim in the target repo's source: filenames, kebab-case tokens
/// (`tracking-tighter`), camelCase (`letterSpacing`), PascalCase (`ClassRow`),
/// and snake_case. These ground repo selection in WHERE the relevant code
/// actually lives — repo descriptions alone mis-route generic tickets (a
/// "Tailwind tracking-tighter" change has no repo hint in its prose).
///
/// Most-distinctive patterns first; capped so the caller bounds its searches.
pub(crate) fn extract_ticket_search_terms(title: &str, body: &str) -> Vec<String> {
    use regex::Regex;
    let text = format!("{title}\n{body}");
    let patterns = [
        // filenames with a code-ish extension
        r"[A-Za-z0-9_.-]+\.(?:tsx?|jsx?|mjs|cjs|css|scss|less|json|rs|py|go|rb|java|kt|ya?ml|toml|graphql|prisma|sql)\b",
        // kebab-case (tracking-tighter, letter-spacing)
        r"\b[a-z][a-z0-9]*(?:-[a-z0-9]+)+\b",
        // camelCase (letterSpacing, mapClassRow)
        r"\b[a-z][a-z0-9]*(?:[A-Z][a-z0-9]+)+\b",
        // PascalCase (ClassRow, GroupCard)
        r"\b[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]+)+\b",
        // snake_case (school_break_camp)
        r"\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b",
    ];
    // Hyphenated English that is not a code identifier — harmless to search but
    // wastes a slot, so drop the obvious ones.
    const STOP: &[&str] = &[
        "step-by-step",
        "out-of-scope",
        "opt-in",
        "opt-out",
        "day-to-day",
        "up-to-date",
        "e-g",
        "what-why",
    ];
    let mut seen = std::collections::HashSet::new();
    let mut terms = Vec::new();
    for pat in patterns {
        let Ok(re) = Regex::new(pat) else { continue };
        for m in re.find_iter(&text) {
            let t = m
                .as_str()
                .trim_matches(|c: char| c == '.' || c == '-' || c == '_');
            if t.len() < 4 {
                continue;
            }
            let key = t.to_lowercase();
            if STOP.contains(&key.as_str()) {
                continue;
            }
            if seen.insert(key) {
                terms.push(t.to_string());
            }
        }
    }
    terms.truncate(6);
    terms
}

/// Select the best repo for a Jira ticket when no explicit project→repo mapping
/// exists. Grounds the choice in CODE EVIDENCE — searches each candidate repo for
/// the ticket's identifiers (`extract_ticket_search_terms`) and short-circuits to
/// a uniquely-matching repo; otherwise asks the LLM with that evidence in hand,
/// falling back to repo descriptions only when nothing matched.
pub async fn select_repo(
    state: &WorkerState,
    msg: &TicketMessage,
    repos: &[String],
    github: &GitHubClient,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let system = "You are a repo-selection agent. Given a Jira ticket and a list of repositories \
                  with descriptions, pick the single best repository for implementation. \
                  Rules: \
                  1. Read the repository descriptions and languages carefully — match the ticket to the repo that OWNS that feature area. \
                  2. Data/analytics/ETL/pipeline repos (e.g. airflow, dbt) are ONLY for data pipeline work, NOT for application features or API integrations. \
                  3. If the ticket mentions specific services, APIs, SDKs, or integrations (e.g. Adyen, Exerp API, billing), pick the repo that implements or calls those services — typically an API/backend repo, not a data repo. \
                  4. Terraform/infrastructure repos are ONLY for infra changes (IAM, networking, deployment config). \
                  5. When unsure, prefer application/API repos over data or infrastructure repos. \
                  Think step by step: (1) what is this ticket about, (2) which repo owns that area, (3) return ONLY the repo in owner/name format.";

    // Enrich repo list with language, description, and top-level directory from GitHub
    let mut repo_lines = Vec::new();
    for r in repos {
        if let Some((owner, name)) = r.split_once('/') {
            let (lang, desc) = github
                .get_repo_info(owner, name)
                .await
                .unwrap_or((None, None));
            let top_dirs = match github.list_directory(owner, name, "", "HEAD").await {
                Ok(entries) => entries
                    .iter()
                    .take(20)
                    .map(|e| e.name.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
                Err(_) => String::new(),
            };
            let mut parts = Vec::new();
            if let Some(l) = lang {
                parts.push(format!("lang: {l}"));
            }
            if let Some(d) = desc {
                parts.push(d);
            }
            if !top_dirs.is_empty() {
                parts.push(format!("files: [{top_dirs}]"));
            }
            if parts.is_empty() {
                repo_lines.push(format!("- {r}"));
            } else {
                repo_lines.push(format!("- {r} — {}", parts.join(" · ")));
            }
        } else {
            repo_lines.push(format!("- {r}"));
        }
    }
    let repo_list = repo_lines.join("\n");

    // Ground the choice in CODE EVIDENCE: search each candidate repo for the
    // ticket's distinctive identifiers and see where they actually live. This is
    // what makes selection "smart" — descriptions mislead on generic tickets, but
    // the repo that literally contains `tracking-tighter` / `ClassRow` is almost
    // always the right one. `search_code` uses an in-memory snapshot (no rate
    // limit; large repos fall back to the Code Search API), so this is bounded.
    let search_terms = extract_ticket_search_terms(&msg.title, &msg.body);
    let probe_terms: Vec<String> = search_terms.iter().take(4).cloned().collect();
    let mut evidence_lines: Vec<String> = Vec::new();
    // (repo, distinct terms matched, total files matched)
    let mut repo_scores: Vec<(String, usize, usize)> = Vec::new();
    let mut searches_attempted = 0usize;
    let mut searches_failed = 0usize;
    // Bound the fan-out: only probe when we have terms and a sane repo count.
    if !probe_terms.is_empty() && repos.len() <= 10 {
        for r in repos {
            let Some((owner, name)) = r.split_once('/') else {
                continue;
            };
            // Search the repo's DEFAULT branch. search_code snapshots by git ref,
            // and "HEAD" does NOT resolve for the snapshot's ref lookup (it 404s
            // and silently falls back to the rate-limited Code Search API), so
            // resolve the real branch first to stay on the fast, un-rate-limited
            // snapshot path.
            let branch = github
                .get_default_branch(owner, name)
                .await
                .unwrap_or_else(|_| "main".to_string());
            let mut matched: Vec<String> = Vec::new();
            let mut total_files = 0usize;
            for term in &probe_terms {
                searches_attempted += 1;
                match github.search_code(owner, name, &branch, term).await {
                    Ok(hits) => {
                        if !hits.is_empty() {
                            matched.push(format!("{term} ({})", hits.len()));
                            total_files += hits.len();
                        }
                    }
                    Err(e) => {
                        searches_failed += 1;
                        warn!(repo = %r, term = %term, error = %e, "code-evidence search failed");
                    }
                }
            }
            evidence_lines.push(if matched.is_empty() {
                format!("- {r} — no matches")
            } else {
                format!("- {r} — matched: {}", matched.join(", "))
            });
            repo_scores.push((r.clone(), matched.len(), total_files));
        }

        // If searches failed systemically (rate-limit / network), the "no
        // matches" lines are unreliable — drop the evidence rather than mislead
        // the picker or short-circuit on corrupted data.
        if searches_attempted > 0 && searches_failed * 2 >= searches_attempted {
            warn!(
                searches_attempted,
                searches_failed, "code-evidence unreliable — using descriptions only"
            );
            evidence_lines.clear();
            repo_scores.clear();
        }

        // Deterministic short-circuit ONLY on strong evidence: one repo uniquely
        // matched at least TWO distinct identifiers. A single generic token
        // matching one repo is too weak to bypass the LLM (it could be the very
        // mis-route we're preventing) — that case falls through to the LLM with
        // the evidence in hand.
        let max_terms = repo_scores.iter().map(|(_, m, _)| *m).max().unwrap_or(0);
        if max_terms >= 2 {
            let leaders: Vec<&(String, usize, usize)> = repo_scores
                .iter()
                .filter(|(_, m, _)| *m == max_terms)
                .collect();
            if leaders.len() == 1 {
                let repo = leaders[0].0.clone();
                info!(
                    repo = %repo,
                    terms = ?probe_terms,
                    matched = max_terms,
                    files = leaders[0].2,
                    "Repo selected by code evidence (unique strong match)"
                );
                return Ok(repo);
            }
        }
    }
    let evidence_section = if evidence_lines.is_empty() {
        String::new()
    } else {
        format!(
            "\n## Code evidence — where the ticket's identifiers actually appear\n{}\n\
             STRONGLY prefer the repo with concrete code matches above; fall back to \
             descriptions only when no repo matched.\n",
            evidence_lines.join("\n")
        )
    };

    // Human-assigned repo roles are the AUTHORITATIVE tiebreak for ambiguous
    // tickets (e.g. two frontends that both use Tailwind): the team designates
    // which repo owns each domain, so selection follows that instead of guessing
    // from auto-generated descriptions.
    let roles = crate::passes::plan::fetch_team_repo_roles(state, &msg.team_id).await;
    let roles_section = {
        let mut lines: Vec<String> = repos
            .iter()
            .filter_map(|r| roles.get(r).map(|role| format!("- {r} → {role}")))
            .collect();
        lines.sort();
        if lines.is_empty() {
            String::new()
        } else {
            format!(
                "\n## Repo roles (human-assigned — AUTHORITATIVE)\n{}\n\
                 Meanings: web=primary web frontend, backend=primary API/backend, \
                 mobile=mobile app, cms=content/CMS, infra=infrastructure, data=data/ETL.\n\
                 These roles OVERRIDE repo descriptions and incidental code matches — a human set \
                 them to resolve exactly the cases where several repos could match. Classify the \
                 ticket's domain and pick the repo whose role matches it, unless the ticket \
                 explicitly names a different repo.\n",
                lines.join("\n")
            )
        }
    };

    let global_agents = super::load_content(state, &msg.team_id, "AGENTS#GLOBAL").await;

    let context_section = if global_agents.is_empty() {
        String::new()
    } else {
        format!("\n## Repository context\n{global_agents}\n")
    };

    let prompt = format!(
        r#"Pick the best repository for this Jira ticket.

## Ticket
Key: {ticket_id}
Title: {title}

Description:
{body}
{context_section}
## Available repositories
{repo_list}
{evidence_section}{roles_section}
Think step by step:
1. What is this ticket about, and what DOMAIN is it — web/frontend, backend/API, mobile, cms, infra, or data? (one sentence)
2. If a repo has a ROLE matching that domain (see Repo roles), pick it — roles are authoritative and override descriptions. Otherwise weigh the CODE EVIDENCE (which repo actually contains the ticket's identifiers) ABOVE the repo descriptions.
3. Why is it NOT any of the other repos?

Then on the LAST line, return ONLY the repository in `owner/name` format."#,
        ticket_id = msg.ticket_id,
        title = msg.title,
        body = msg.body,
    );

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

    let model_id = provider.primary_model_id();
    let response = provider::converse(
        state,
        provider,
        model_id,
        system,
        &mut messages,
        &[],
        &NoOpExecutor,
        usage,
        llm::ConverseOptions {
            max_turns: 1,
            // The prompt asks for step-by-step reasoning THEN the answer on
            // the last line — 512 tokens routinely cut the completion before
            // the answer line existed, and the mid-sentence fragment fell
            // through to the repos[0] fallback.
            max_tokens: 2048,
            deadline: None,
        },
        None,
        None,
    )
    .await?;

    info!(response = %response.trim(), "Triage repo selection reasoning");

    if let Some(repo) = extract_repo_choice(&response, repos) {
        info!(repo = %repo, "Repo selected for Jira ticket");
        return Ok(repo);
    }

    // Fall back to first repo — should now be rare (truncation fixed and the
    // parser scans every line); the progress notes make it visible upstream.
    info!(fallback = %repos[0], "LLM pick not in list, falling back");
    Ok(repos[0].clone())
}

/// Scan the response from the LAST line upward for a line that names one of
/// the team's repos. Tolerates markdown wrapping, punctuation, and prose
/// around the `owner/name` token.
fn extract_repo_choice(response: &str, repos: &[String]) -> Option<String> {
    for line in response.trim().lines().rev() {
        let cleaned = line.trim().trim_matches(['`', '*', '_', '"', '\'', '.']);
        if cleaned.is_empty() {
            continue;
        }
        // Exact / case-insensitive line match first
        for repo in repos {
            if repo.eq_ignore_ascii_case(cleaned) {
                return Some(repo.clone());
            }
        }
        // Then substring: "SWITCH to owner/name" / "Answer: owner/name".
        // Longest name wins (Org/app must not shadow Org/app-web) and
        // negated mentions ("not Org/mobile") don't count.
        let lower = cleaned.to_ascii_lowercase();
        if lower.contains(" not ") || lower.contains("n't ") {
            continue;
        }
        let mut candidates: Vec<&String> = repos.iter().collect();
        candidates.sort_by_key(|r| std::cmp::Reverse(r.len()));
        for repo in candidates {
            if lower.contains(&repo.to_ascii_lowercase()) {
                return Some(repo.clone());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{extract_repo_choice, extract_ticket_search_terms};

    #[test]
    fn extracts_distinctive_identifiers() {
        // CPM-7881: the Tailwind token ticket that mis-routed. The kebab tokens
        // are the signal that would find the repo containing them.
        let terms = extract_ticket_search_terms(
            "Override Tailwind \"tighter\" letter-spacing token",
            "Override the token so tracking-tighter renders at -.0333em everywhere, \
             not the default -0.05em. Other letter-spacing tokens unchanged.",
        );
        assert!(
            terms.iter().any(|t| t == "tracking-tighter"),
            "got {terms:?}"
        );
        assert!(terms.iter().any(|t| t == "letter-spacing"), "got {terms:?}");

        // CPM-7838: PascalCase component names are the signal.
        let terms = extract_ticket_search_terms(
            "render ClassRow tags as styled badges to match GroupCard",
            "Style the tags in ClassRow to match the badges in GroupCard.",
        );
        assert!(terms.iter().any(|t| t == "ClassRow"), "got {terms:?}");
        assert!(terms.iter().any(|t| t == "GroupCard"), "got {terms:?}");

        // A prose-only ticket yields no code identifiers (falls back to the LLM).
        let terms = extract_ticket_search_terms(
            "Improve the checkout experience",
            "Users are confused by the flow. Make it clearer and faster.",
        );
        assert!(terms.is_empty(), "expected no code terms, got {terms:?}");
    }

    #[test]
    fn repo_choice_parsing() {
        let repos = vec!["Org/speedboat".to_string(), "Org/mobile".to_string()];
        // Plain last line
        assert_eq!(
            extract_repo_choice("reasoning...\nOrg/speedboat", &repos).as_deref(),
            Some("Org/speedboat")
        );
        // Markdown + prose wrapping
        assert_eq!(
            extract_repo_choice("1. thinking\n2. more\nAnswer: **Org/mobile**.", &repos).as_deref(),
            Some("Org/mobile")
        );
        // Truncated mid-reasoning but an earlier line names the repo
        assert_eq!(
            extract_repo_choice(
                "The ticket belongs to Org/speedboat because the pages",
                &repos
            )
            .as_deref(),
            Some("Org/speedboat")
        );
        // Nothing matches
        assert_eq!(extract_repo_choice("no idea", &repos), None);
        // Case-insensitive
        assert_eq!(
            extract_repo_choice("org/SPEEDBOAT", &repos).as_deref(),
            Some("Org/speedboat")
        );
    }
}
