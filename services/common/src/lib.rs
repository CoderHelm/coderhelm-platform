//! Logic shared between the gateway and worker services.

/// A repository's generated agent file: which files shape it, when a push
/// makes it stale, and when the sweep writes it again by itself.
pub mod agent_file;
/// The agent log of a run: its file format, outcomes, order and redaction
/// (worker writes it, gateway serves it).
pub mod agent_log;
/// The app map: CoderHelm's adapter around the vendored Floorplan map
/// (`vendor/floorplan`): storage, CoderHelm's overlays and lookups.
pub mod app_map;
/// What a Chat question asks for (an orientation, a status, a how-to, a
/// correction of Chat itself, ...), read before the model answers.
pub mod chat_intent;
/// A repository's code map: sections stamped with the commit they describe,
/// rebuilt only where a change touched them, given to the planner and coder.
pub mod code_map;
/// Distributed fencing lock for a team+repo memory snapshot. Lives in common
/// because BOTH writers must honor it: the worker's run-scoped memory open and
/// the gateway's dashboard memory-delete (an unlocked delete raced a concurrent
/// run's upload, silently resurrecting deleted memories).
pub mod memlock;
pub mod model_access;
pub mod model_ids;
/// Non-functional checklists a plan answers when its change reaches
/// infrastructure, performance, data, security or observability.
pub mod nfr_check;
/// Each acceptance criterion traced to the code, the tests and the proof.
pub mod trace;

/// Expiring once-only claims (dedup markers, leases) on the settings table.
/// Shared so every claim compares its expiry the same way.
pub mod claim;
/// A re-review started by an edit of a pull request's description: what
/// counts as a change, and the keys that queue one review per edit burst.
pub mod prose;
pub mod review_description;

/// A workflow run's jobs across attempts: what fails now, where, and what
/// passed when re-run.
pub mod actions_jobs;
/// The code graph's per-repo switch and build status (gateway and worker).
pub mod code_graph;
/// GitHub Code Quality findings on a pull request's files, as reviewer
/// context.
pub mod code_quality;
/// A repository's public codegen environment: what may be kept, and what
/// a failed generator is missing.
pub mod codegen_env;
/// A commit's check runs and commit statuses as one CI listing.
pub mod commit_ci;
/// Every delivery in and out (webhooks, relay forwards, notifications,
/// email), with what happened, for the Deliveries page.
pub mod deliveries;
pub mod deliveries_rollup;
/// Dependency fix runs: the risk-aware plan, sandbox verification and
/// bisect, resumable state.
pub mod dep_audit;
pub mod dep_fix;
pub mod dep_head;
pub mod description_handoff;
/// The shared dead-letter queue: whose each failed message is, whether the
/// worker can read it, and when the cleanup archives it.
pub mod dlq;
/// One DNS TXT lookup through the system resolver (the email check's DMARC).
pub mod dns_txt;
pub mod file_claims;
/// Hybrid self-hosting: CoderHelm addresses, the event relay, the token
/// broker and the signatures between an install and the hosted side.
pub mod hybrid;
/// Which emails an install sends, and how: its own SES or CoderHelm's mail
/// relay (templates only, listed recipients, never CoderHelm's addresses).
pub mod mail;
/// Support tickets from self-hosted installs: sent to CoderHelm's support
/// queue, and support's answers fetched back (install and hosted sides).
pub mod support_relay;
/// Outbound webhooks: the event catalog, endpoints, signing and delivery.
pub mod webhooks;

/// Agent permissions: one policy engine (allow, ask, deny) every agent loop
/// asks before every call, approval cards, sign-in prompts and the pause.
pub mod agent_policy;
// What a shell command an agent sent means here, and the tool names
// models are trained on with their CoderHelm equivalents.
/// Alert notifications: which channels an alert posts to, and its messages.
pub mod alarm_evidence;
pub mod alert_episodes;
pub mod alert_notify;
/// A read the team's AWS role lacks: one install-level setup item.
pub mod aws_role_reads;
pub mod coder_shell;
/// Chat notifications (Teams and Slack): one message model, a renderer per
/// channel, sealed webhook URLs.
pub mod notify;
pub mod role_move;
pub mod role_wait;
pub mod tool_aliases;
pub mod waf_logs;

/// The team's daily alert token budget (alerts route and Setup).
pub mod alert_budget;
/// Dockerfile base images: their package lists matched against OSV's
/// distribution data, fixed by pinning the same tag to a newer digest.
pub mod base_images;
pub mod connected_access;
/// The repository's deploy label as a verification gate: its workflow's
/// runs on a head, the wait before ready, and putting the label back once
/// per head.
pub mod deploy_label;
/// Why a finished run's pull request is still a draft, on the run record.
pub mod draft_hold;
/// Read-only lookups against a team's own systems (their API today). Shared so
/// the dashboard's test path and the reviewer's tool path enforce the same rules.
pub mod e2e;
pub mod followup_dedupe;
pub mod followup_tickets;
/// A team's branch, commit and PR title templates, and how they render.
pub mod git_conventions;
/// What an installation's token may read: permissions a feature uses when
/// granted and does without otherwise.
pub mod github_budget;
pub mod github_grants;
pub mod github_wait;
/// Infrastructure vulnerabilities: public AWS feeds, a team's AWS inventory,
/// and matching by version (no model calls).
pub mod infra_vuln;
/// Custom instructions, voice and agent context rows (routes and Setup).
pub mod instructions;
/// The Jira app version each site runs, the latest release, and what an
/// older version lacks.
pub mod jira_app_version;
pub mod jira_connection;
pub mod live;
/// Log findings and the fix runs started from them.
pub mod log_findings;
pub mod log_incident;
/// One ticket, one run: the canonical ticket key and the claim every run
/// start takes on it.
pub mod one_run;
/// Reachability read from the code: imports, calls, lockfile parents.
pub mod reachability;
/// One repo's reviewer settings: the page's view and the saved item.
pub mod review_config;
/// Language runtimes (a JDK, Node.js, Python, Go) against their own advisories.
pub mod runtime_vuln;
/// Security fixes: advisories, triage, fix runs and cards (gateway and worker).
pub mod security_fixes;
pub mod security_release;
pub mod settings_index;
/// Setup: the settings registry, the repo analysis, runs, apply and undo.
pub mod setup;
pub mod upgrade_notes;
pub mod upgrade_quality;
// Which repo a log group's or alarm's code lives in, from evidence.
/// Sealed credentials for a team's MCP servers (KMS, bound to team and server).
pub mod envelope;
pub mod log_reader;
pub mod log_repo;
pub mod log_triage;
/// The MCP servers a team can switch on: one catalog for gateway and worker.
pub mod mcp_catalog;
pub mod mcp_credentials;
/// Remote MCP servers over HTTP: OAuth sign-in, tokens, allowed tools.
pub mod mcp_remote;
pub mod memory_meta;
/// The auto-merge gate's per-PR record: keys and states (the worker runs the
/// gate, the gateway re-arms it on GitHub events).
pub mod merge_gate;
/// Where a finished run's pull request stands on its way to merge, in the
/// one reason that is true.
pub mod merge_standing;
/// No second run on a ticket whose pull request already merged, unless a
/// person asked for one after the merge.
pub mod merged_ticket;
/// Every message on the worker queues: one enum the gateway and worker share.
pub mod messages;
/// Retrying model API calls on transient failures, classified by status.
pub mod model_retry;
/// The index of teams that have per-repository reviewer configs.
pub mod review_config_index;
/// Index partitions in the settings table, so sweeps read records by key.
/// A repository's staging-only CMS write connection (for content the
/// end-to-end tests read).
pub mod staging_cms;
// Anthropic SSE calls, metered per attempt (drops, retries, the deadline)
pub mod model_stream;
// Titan embeddings, metered as CoderHelm's own cost
pub mod embed;
/// Knowledge bases: files a team uploads, read into cited passages that the
/// brain chat and the team's agents search.
pub mod knowledge;
// Reading the team's repositories for Chat (overview, files, search).
pub mod repo_read;
// The test that every model call site is a metered helper
pub mod ci_cover;
/// The lines a pull request carries about its own CI, true for its head.
pub mod ci_lines;
/// A red check reproduced against the base branch before anything is
/// decided about it.
pub mod ci_repro;
/// A red check the CI fix round showed isn't the change's: re-run once,
/// then classified on its own outcome, never the run's failure.
pub mod ci_unrelated;
pub mod ci_watch;
pub mod content_gap;
/// The last check on a pull request description: no section twice, no
/// deploy maps, no empty "who this affects".
pub mod description_lint;
/// A test fixture uses the ids the run looked up, not made-up ones.
pub mod fixture_ids;
// A diff holds the lines the change needs: formatting-only hunks are told
// apart and put back.
pub mod diff_shape;
/// Each ticket criterion mapped to an end-to-end test and proven both
/// ways: failing on staging, passing on the preview.
pub mod e2e_proof;
#[cfg(test)]
mod model_call_sites;
/// Which pull requests CoderHelm may push to, edit or close: its own.
pub mod own_pr;
/// Paginated DynamoDB lookups (follow LastEvaluatedKey to a match).
pub mod paged;
/// Placeholder tokens: never learned, never written into a commit or title.
pub mod placeholders;
/// Which waiting plan tasks a merged run releases.
pub mod plan_dependents;
pub mod plan_state;
pub mod plan_sync;
/// Every body CoderHelm sends GitHub fits GitHub's limit.
pub mod pr_body;
pub mod pr_body_fit;
/// A description checked against the recorded facts, sentence by sentence.
pub mod pr_claims;
/// What is recorded about a pull request at one head, and the lead rendered
/// from it.
pub mod pr_conventions;
/// A dependency pull request's facts, from what the pipeline records.
pub mod pr_dependency;
pub mod pr_facts;
/// Where each part of a description goes, and how a reader would judge it.
pub mod pr_layout;
/// What happened to a PR Coderhelm opened, read from GitHub.
pub mod pr_outcome;
/// The last read of a description before it is saved, verified.
pub mod pr_reader;
/// The final read of a description before it is posted.
pub mod pr_review;
/// The work around a pull request that is not in its diff, each step with
/// one owner and one state.
pub mod pr_steps;
/// A repository's pull request template as the description's structure.
pub mod pr_template;
pub mod preview;
pub mod preview_access;
pub mod preview_find;
/// What a fix must show before it is committed and pushed.
pub mod push_gate;
/// A bug reproduced on the base before code is planned, fixed in the
/// mechanism the reproduction lands in, questions looked up first.
pub mod repro_first;
pub mod run_prs;
/// Scope decided once at plan time, held in every round and in the
/// description.
pub mod scope_ledger;
pub mod waiting_line;
// review_stats: time-ordered review records for analytics
/// How a comment reads: the renderer and lint every comment passes before
/// it is posted (short, plain, no pasted plans, no history).
pub mod comment_style;
/// What people write on a ticket while a run works on it: kept, acknowledged, acted on.
/// One engineer's voice per ticket and per pull request: every comment
/// CoderHelm writes reads the thread first, then stays quiet, edits its own
/// comment, answers people or posts.
pub mod conversation;
/// Where `conversation` reads and writes (the Jira app with its ledger, GitHub).
pub mod conversation_io;
pub mod decision_log;
pub mod deploy_stats;
/// Flaky or broken: whose failure a red check is, and the repository's
/// record of checks that failed and then passed on one commit.
pub mod flaky_checks;
/// Lessons the reviewer learns from people's pushback on its findings.
pub mod guide;
/// Run statuses; which ones a person set; closing and reopening a run.
pub mod handback;
/// Work CoderHelm started on its own pull request and hasn't landed, and
/// what a person's mention there does about it.
pub mod held_work;
/// Instructions written for a person's machine, read on Coderhelm: one rule
/// and one capability catalogue for every reader.
pub mod host_steps;
pub mod jira_text;
/// A lockfile out of sync with its manifest: read from a log, repaired by
/// code with the package manager's own command, never written by a model.
pub mod lockfile_sync;
/// Anthropic API or Amazon Bedrock, and Bedrock's model ids.
pub mod model_provider;
/// Files a run keeps as its outputs (reports, tables, snapshots).
pub mod outputs;
pub mod person_gate;
/// A person's lines on a CoderHelm branch stay theirs: writes that would undo them are
/// kept out.
pub mod person_lines;
/// Plugins: packages of skills, agents, tools, triggers and outputs a team installs.
pub mod plugins;
/// One shape for "something is wrong": set by a check, cleared by the same
/// check passing, with when it recovered (every status producer uses it).
pub mod problems;
/// Where this deployment's dashboard and API are (links, redirects, webhooks).
pub mod public_urls;
pub mod registry_check;
/// Linked tickets and pull requests closed in favor of this one: their criteria are
/// part of the change and checked before it is called ready.
pub mod related_tickets;
/// What CoderHelm says back to a person, written from the thread and the run's records.
pub mod reply_compose;
/// Who asked for a run: one identity per person, for per-person analytics.
pub mod requester;
/// What the reviewer did while it read a pull request, for the session page.
pub mod review_activity;
/// The PR's "CoderHelm review" check run, from scheduled to finished.
pub mod review_check;
pub mod review_lessons;
/// Where the reviewer is with each PR right now (queued, reviewing, retrying,
/// done), and the one status a PR shows on the dashboard.
pub mod review_live;
/// The "PR Overview" a review leads with: what the PR does, file by file.
pub mod review_overview;
pub mod review_session;
pub mod review_stats;
/// CoderHelm's earlier reviews on a pull request, collapsed once it posts a
/// new one: one line each, hidden comments, resolved threads, dismissed.
pub mod review_supersede;
pub mod reviewer_prefs;
/// Learning from outcomes: reward events, scores, calibration, variants,
/// examples and evidence-backed lessons, per team.
pub mod rewards;
/// The one way a run is marked failed: always with a reason and a pass.
pub mod run_events;
pub mod run_fail;
/// The questions a run asks when it needs a person's answer, and the
/// answers people give in CoderHelm.
pub mod run_questions;
/// Fix rounds on an open pull request: each with its own time budget,
/// resumable, and never a failed run when one doesn't finish.
pub mod run_round;
pub mod run_status;
/// What a run waiting on a person is waiting for: never nothing.
pub mod run_wait;
/// Proof that a repository builds and starts after its dependencies change:
/// what to run (found in its CI and manifests), the sandbox script, and what
/// the bumps are blamed for.
pub mod runtime_proof;
pub mod scorecard;
#[cfg(test)]
mod selfhost_links;
pub mod skill_apply;
pub mod skill_compat;
/// Scheduled skill runs: a team's synced skill run on a schedule or on "Run now".
pub mod skill_runs;
pub mod skills;
/// Stale pull request cleanup: settings, activity and the decision.
pub mod stale_prs;
/// Who may ask CoderHelm for work from a GitHub comment or review.
pub mod standing;
pub mod stop_judge;
pub mod stop_rules;
/// A team's run and PR-reviewed counts, each added exactly once.
pub mod team_counts;
pub mod test_env;
pub mod ticket_comments;
/// What a person answered on the ticket is a decision in every run on it; a
/// description never lists an answered item as pending.
pub mod ticket_decisions;
/// A ticket's comments as CoderHelm read them, and whether its work is done.
pub mod ticket_state;
/// A team's monthly token limits: new tokens, cache read, cache write.
pub mod token_limits;
/// Whether a tool works right now: one failure, one diagnosis, one report.
pub mod tool_health;
/// Every tool an agent can be given, with its risk (read, write, consequential).
pub mod tool_registry;
/// Signed links that turn off one email for one person, without signing in.
pub mod unsubscribe;
/// Model usage accounting: every model call is counted here, once.
pub mod usage;
pub mod web_domains;
pub mod work_usage;

/// GitHub's per-request override for the installation-token format, sent on
/// `POST /app/installations/{id}/access_tokens`. GitHub is moving installation
/// tokens from short opaque strings to ~520-char stateless JWT-style `ghs_…`
/// tokens (two dots); this header pins the format instead of following the
/// rollout. CoderHelm treats tokens as opaque strings, so it opts in by default.
/// The header is temporary — GitHub will deprecate it.
pub const STATELESS_S2S_TOKEN_HEADER: &str = "X-GitHub-Stateless-S2S-Token";

/// The value to send for [`STATELESS_S2S_TOKEN_HEADER`], from the
/// `GITHUB_STATELESS_S2S_TOKEN` env var: `enabled` (default) or `disabled` pin
/// the format; `default` sends no header (GitHub's rollout decides).
pub fn stateless_s2s_token_mode() -> Option<&'static str> {
    stateless_s2s_token_mode_from(std::env::var("GITHUB_STATELESS_S2S_TOKEN").ok().as_deref())
}

pub fn stateless_s2s_token_mode_from(v: Option<&str>) -> Option<&'static str> {
    match v.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("enabled") => Some("enabled"),
        Some("disabled") => Some("disabled"),
        // "default" / anything else: no header, GitHub's rollout applies.
        _ => None,
    }
}

/// True for Claude models that think on every request (Opus 5.x, Sonnet 5.x,
/// Fable, Mythos). They reject non-default sampling params, `budget_tokens` and
/// disabled thinking, lead responses with `thinking` blocks that must be echoed
/// back unchanged, and default to effort "medium"/"high" per model — so callers
/// set effort explicitly and never send `temperature`. Judged on the
/// canonical id ([`usage::normalize_model`]), so a Bedrock id
/// (`us.anthropic.claude-opus-5-5`, `global.…`, `anthropic.…-v1:0`) is the
/// same model as its Anthropic API id.
pub fn is_always_thinking_model(model_id: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "claude-opus-5",
        "claude-sonnet-5",
        "claude-fable-",
        "claude-mythos-",
    ];
    let id = usage::normalize_model(model_id);
    PREFIXES.iter().any(|p| id.starts_with(p))
}

use sha2::{Digest, Sha256};

/// Stable SHA-256 hex digest of a string.
pub fn content_hash(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

/// SHA-256 (hex) of the parts, each followed by a zero byte. A stable key
/// for caches built from several inputs.
pub fn sha256_hex(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for p in parts {
        hasher.update(p.as_bytes());
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize())
}

/// Stable content hash for a ticket's context: title + body + image keys.
///
/// The gateway computes this at webhook time to decide whether a ticket
/// update actually changed anything worth re-running; the worker stores it
/// on the run record and uses it for plan-cache invalidation. Both sides
/// MUST agree on the value, which is why this lives in a shared crate and
/// uses SHA-256 rather than std's DefaultHasher (whose output is not
/// guaranteed stable across Rust releases or compilations).
///
/// Title and body are whitespace-normalised first (every run of whitespace
/// collapses to one space, ends trimmed), so an editor re-saving a ticket
/// with different line endings, trailing spaces or re-wrapped lines does not
/// count as a ticket change and re-run the whole pipeline.
pub fn ticket_context_hash(title: &str, body: &str, image_keys: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(collapse_whitespace(title).as_bytes());
    hasher.update([0u8]);
    hasher.update(collapse_whitespace(body).as_bytes());
    for key in image_keys {
        hasher.update([0u8]);
        hasher.update(key.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// Collapse every run of whitespace (spaces, tabs, CR/LF) to a single space
/// and trim both ends. Deterministic and purely lexical.
pub fn collapse_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Truncate to at most `max_bytes`, never splitting a UTF-8 codepoint.
/// Replaces the raw `&s[..n]` slices that panicked whenever a multibyte
/// character (emoji, em-dash, CJK) straddled the cut point.
pub fn truncate_str(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Keep at most `max_bytes` from the END of the string, char-boundary safe.
pub fn tail_str(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut start = s.len() - max_bytes;
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

/// Keep the head and tail of a long text (e.g. CI logs, where the root-cause
/// error is usually at the head and the final status at the tail), joined by
/// an elision marker. Tail-only truncation was discarding compile cascades'
/// first — and only meaningful — error.
pub fn head_tail_str(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let half = max_bytes / 2;
    format!(
        "{}\n\n[… {} bytes elided …]\n\n{}",
        truncate_str(s, half),
        s.len() - 2 * half,
        tail_str(s, half)
    )
}

/// Verdict from an LLM quality gate.
#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
}

/// Parse a pass/fail verdict from an LLM response, failing CLOSED.
///
/// The old gates were `response.starts_with("ISSUES_FOUND:")` and
/// `final_text.contains("SECURITY_PASS")` — a reviewer that wrote any
/// preamble, wrapped the token in markdown, or mentioned the pass token in
/// prose ("this falls short of a SECURITY_PASS") was treated as passing and
/// its findings silently discarded.
///
/// Rules, in priority order:
/// 1. A line whose leading markdown/punctuation-stripped text starts with
///    `fail_token` → Fail (fail beats pass if both appear).
/// 2. Otherwise a line starting with `pass_token` → Pass.
/// 3. Neither token found → Fail (unparseable output cannot vouch for code).
pub fn parse_verdict(response: &str, pass_token: &str, fail_token: &str) -> Verdict {
    let normalize = |line: &str| -> String {
        line.trim()
            .trim_start_matches(['#', '*', '>', '-', '`', '_', ' '])
            .trim_end_matches(['*', '`', '_'])
            .to_ascii_uppercase()
    };
    let fail_upper = fail_token.to_ascii_uppercase();
    let pass_upper = pass_token.to_ascii_uppercase();

    // Token must be word-bounded: "CLEAN up the handler" is prose, not a
    // CLEAN verdict.
    let bounded = |norm: &str, token: &str| {
        norm.starts_with(token)
            && !norm[token.len()..]
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    let mut saw_pass = false;
    for line in response.lines() {
        let norm = normalize(line);
        if bounded(&norm, &fail_upper) {
            return Verdict::Fail;
        }
        if bounded(&norm, &pass_upper) {
            saw_pass = true;
        }
    }
    if saw_pass {
        return Verdict::Pass;
    }
    // Multi-turn passes often end with a prose summary; accept a trailing
    // line that IS the verdict token (with optional punctuation) even when
    // no line starts with it.
    for line in response
        .lines()
        .rev()
        .filter(|l| !l.trim().is_empty())
        .take(3)
    {
        let norm = normalize(line);
        let bare = norm.trim_end_matches(['.', '!', ':', ' ']);
        if bare == pass_upper {
            return Verdict::Pass;
        }
    }
    Verdict::Fail
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_never_splits_codepoints() {
        // "héllo" — é is 2 bytes starting at index 1
        let s = "héllo";
        assert_eq!(truncate_str(s, 2), "h"); // cutting mid-é backs off
        assert_eq!(truncate_str(s, 3), "hé");
        assert_eq!(truncate_str(s, 100), s);
        let emoji = "ab🚀cd";
        for n in 0..=emoji.len() {
            let _ = truncate_str(emoji, n); // must never panic
            let _ = tail_str(emoji, n);
        }
        assert_eq!(tail_str("héllo", 3), "llo"); // byte 3 is a boundary (after é)
        assert_eq!(tail_str("héllo", 100), "héllo");
    }

    #[test]
    fn head_tail_keeps_both_ends() {
        let s = "START-".to_string() + &"x".repeat(1000) + "-END";
        let out = head_tail_str(&s, 100);
        assert!(out.starts_with("START-"));
        assert!(out.ends_with("-END"));
        assert!(out.contains("elided"));
        assert_eq!(head_tail_str("short", 100), "short");
    }

    #[test]
    fn verdict_fail_closed_and_robust() {
        // Plain tokens
        assert_eq!(parse_verdict("LGTM", "LGTM", "ISSUES_FOUND"), Verdict::Pass);
        assert_eq!(
            parse_verdict("ISSUES_FOUND: bug in x", "LGTM", "ISSUES_FOUND"),
            Verdict::Fail
        );
        // Preamble before the fail token — old starts_with() called this a pass
        assert_eq!(
            parse_verdict(
                "I reviewed the diff carefully.\nISSUES_FOUND: off-by-one",
                "LGTM",
                "ISSUES_FOUND"
            ),
            Verdict::Fail
        );
        // Markdown-wrapped token
        assert_eq!(
            parse_verdict("**ISSUES_FOUND:** broken import", "LGTM", "ISSUES_FOUND"),
            Verdict::Fail
        );
        // Pass token mentioned mid-prose does NOT pass (must start a line)
        assert_eq!(
            parse_verdict(
                "These findings fall short of a SECURITY_PASS being granted",
                "SECURITY_PASS",
                "SECURITY_FAIL"
            ),
            Verdict::Fail
        );
        // Line-anchored pass works with preamble lines
        assert_eq!(
            parse_verdict(
                "Audit complete.\nSECURITY_PASS — no exploitable findings",
                "SECURITY_PASS",
                "SECURITY_FAIL"
            ),
            Verdict::Pass
        );
        // Both tokens → fail wins
        assert_eq!(
            parse_verdict("LGTM\nISSUES_FOUND: actually no", "LGTM", "ISSUES_FOUND"),
            Verdict::Fail
        );
        // Unparseable → fail closed
        assert_eq!(
            parse_verdict("The code looks fine to me!", "LGTM", "ISSUES_FOUND"),
            Verdict::Fail
        );
    }

    #[test]
    fn ticket_context_hash_ignores_whitespace_only_edits() {
        let a = ticket_context_hash("Fix  login", "Line one\nline two", &[]);
        assert_eq!(
            a,
            ticket_context_hash(" Fix login ", "Line one \r\n\n  line two\n", &[])
        );
        assert_ne!(
            a,
            ticket_context_hash("Fix login", "Line one line three", &[])
        );
        assert_eq!(collapse_whitespace("  a\t b\n\nc  "), "a b c");
    }

    #[test]
    fn hash_is_stable_and_sensitive() {
        let a = ticket_context_hash("t", "b", &["k1".into()]);
        assert_eq!(a, ticket_context_hash("t", "b", &["k1".into()]));
        assert_ne!(a, ticket_context_hash("t", "b2", &["k1".into()]));
        assert_ne!(a, ticket_context_hash("t", "b", &[]));
        // Field boundaries matter: ("ab","c") != ("a","bc")
        assert_ne!(
            ticket_context_hash("ab", "c", &[]),
            ticket_context_hash("a", "bc", &[])
        );
    }

    #[test]
    fn always_thinking_models() {
        assert!(is_always_thinking_model("claude-opus-5-5"));
        assert!(is_always_thinking_model("claude-sonnet-5-5"));
        assert!(is_always_thinking_model("claude-fable-5-1"));
        assert!(!is_always_thinking_model("claude-opus-4-8"));
        assert!(!is_always_thinking_model("claude-sonnet-4-6"));
        assert!(!is_always_thinking_model("claude-haiku-4-5"));
    }

    #[test]
    fn always_thinking_models_by_their_bedrock_ids() {
        // Bedrock ids carry a region or global prefix, the vendor prefix, and
        // sometimes a version suffix; a miss sent `temperature` to a model
        // that rejects it (400).
        for base in [
            "claude-opus-5-5",
            "claude-opus-5",
            "claude-sonnet-5-5",
            "claude-sonnet-5",
            "claude-fable-5",
            "claude-fable-5-1",
            "claude-mythos-1",
        ] {
            for id in [
                format!("anthropic.{base}"),
                format!("us.anthropic.{base}"),
                format!("eu.anthropic.{base}"),
                format!("apac.anthropic.{base}"),
                format!("global.anthropic.{base}"),
                format!("us.anthropic.{base}-v1:0"),
                format!("anthropic.{base}-v1:0"),
                format!("{base}[1m]"),
                format!("US.Anthropic.{base}"),
            ] {
                assert!(is_always_thinking_model(&id), "{id}");
            }
        }
        for id in [
            "us.anthropic.claude-haiku-4-5-20251001-v1:0",
            "global.anthropic.claude-haiku-4-5-20251001-v1:0",
            "us.anthropic.claude-sonnet-4-6",
            "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
            "us.anthropic.claude-opus-4-8",
            "us.anthropic.claude-opus-4-5-20251101-v1:0",
            "anthropic.claude-3-sonnet-20240229-v1:0",
        ] {
            assert!(!is_always_thinking_model(id), "{id}");
        }
    }

    #[test]
    fn stateless_s2s_mode_defaults_to_enabled() {
        assert_eq!(stateless_s2s_token_mode_from(None), Some("enabled"));
        assert_eq!(stateless_s2s_token_mode_from(Some("")), Some("enabled"));
        assert_eq!(
            stateless_s2s_token_mode_from(Some(" Enabled ")),
            Some("enabled")
        );
        assert_eq!(
            stateless_s2s_token_mode_from(Some("disabled")),
            Some("disabled")
        );
        assert_eq!(stateless_s2s_token_mode_from(Some("default")), None);
        assert_eq!(stateless_s2s_token_mode_from(Some("bogus")), None);
    }
}
