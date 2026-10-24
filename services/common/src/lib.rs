//! Logic shared between the gateway and worker services.

/// Distributed fencing lock for a team+repo memory snapshot. Lives in common
/// because BOTH writers must honor it: the worker's run-scoped memory open and
/// the gateway's dashboard memory-delete (an unlocked delete raced a concurrent
/// run's upload, silently resurrecting deleted memories).
pub mod memlock;
pub mod model_ids;

/// Expiring once-only claims (dedup markers, leases) on the settings table.
/// Shared so every claim compares its expiry the same way.
pub mod claim;

/// The code graph's per-repo switch and build status (gateway and worker).
pub mod code_graph;
/// The shared dead-letter queue: whose each failed message is, whether the
/// worker can read it, and when the cleanup archives it.
pub mod dlq;

/// Alert notifications: which channels an alert posts to, and its messages.
pub mod alert_notify;
/// Chat notifications (Teams and Slack): one message model, a renderer per
/// channel, sealed webhook URLs.
pub mod notify;

/// Read-only lookups against a team's own systems (their API today). Shared so
/// the dashboard's test path and the reviewer's tool path enforce the same rules.
pub mod e2e;
/// A team's branch, commit and PR title templates, and how they render.
pub mod git_conventions;
pub mod live;
/// Log findings and the fix runs started from them.
pub mod log_findings;
/// Sealed credentials for a team's MCP servers (KMS, bound to team and server).
pub mod mcp_credentials;
/// Remote MCP servers over HTTP: OAuth sign-in, tokens, allowed tools.
pub mod mcp_remote;
pub mod memory_meta;
/// The auto-merge gate's per-PR record: keys and states (the worker runs the
/// gate, the gateway re-arms it on GitHub events).
pub mod merge_gate;
/// Retrying model API calls on transient failures, classified by status.
pub mod model_retry;
// Anthropic SSE calls, metered per attempt (drops, retries, the deadline)
pub mod model_stream;
// Titan embeddings, metered as CoderHelm's own cost
pub mod embed;
// The test that every model call site is a metered helper
#[cfg(test)]
mod model_call_sites;
/// Paginated DynamoDB lookups (follow LastEvaluatedKey to a match).
pub mod paged;
/// What happened to a PR Coderhelm opened, read from GitHub.
pub mod pr_outcome;
// review_stats: time-ordered review records for analytics
pub mod jira_text;
/// Where this deployment's dashboard and API are (links, redirects, webhooks).
pub mod public_urls;
/// Who asked for a run: one identity per person, for per-person analytics.
pub mod requester;
/// Lessons the reviewer learns from people's pushback on its findings.
pub mod review_lessons;
/// Where the reviewer is with each PR right now (queued, reviewing, retrying,
/// done), and the one status a PR shows on the dashboard.
pub mod review_live;
pub mod review_stats;
/// Learning from outcomes: reward events, scores, calibration, variants,
/// examples and evidence-backed lessons, per team.
pub mod rewards;
/// The one way a run is marked failed: always with a reason and a pass.
pub mod run_fail;
/// Run statuses; which ones a person set; closing and reopening a run.
pub mod run_status;
pub mod skill_apply;
pub mod skill_compat;
pub mod skills;
/// A ticket's comments as CoderHelm read them, and whether its work is done.
pub mod ticket_state;
/// A team's monthly token limits: new tokens, cache read, cache write.
pub mod token_limits;
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
