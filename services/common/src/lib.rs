//! Logic shared between the gateway and worker services.

use sha2::{Digest, Sha256};

/// Stable content hash for a ticket's context: title + body + image keys.
///
/// The gateway computes this at webhook time to decide whether a ticket
/// update actually changed anything worth re-running; the worker stores it
/// on the run record and uses it for plan-cache invalidation. Both sides
/// MUST agree on the value, which is why this lives in a shared crate and
/// uses SHA-256 rather than std's DefaultHasher (whose output is not
/// guaranteed stable across Rust releases or compilations).
pub fn ticket_context_hash(title: &str, body: &str, image_keys: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(title.as_bytes());
    hasher.update([0u8]);
    hasher.update(body.as_bytes());
    for key in image_keys {
        hasher.update([0u8]);
        hasher.update(key.as_bytes());
    }
    format!("{:x}", hasher.finalize())
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
}
