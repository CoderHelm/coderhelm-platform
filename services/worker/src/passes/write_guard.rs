//! Ticket-scope write guard for the post-PR FIX loops (CI-fix, review-fix,
//! security/test-fix, CI-only feedback).
//!
//! The failure it kills: repo CI logs surface pre-existing lint/warnings in
//! files the PR never touched, and the fix agent "cleans them up" to get CI
//! green — churning unrelated files (unused-import removals, ternary rewrites,
//! useEffect-dep additions) into a feature PR. A fix cycle may only write to
//! files the PR already changed; anything else pre-exists on the base branch
//! and is out of scope.
//!
//! Same bounded-escape shape as the generated-file guard: after
//! [`MAX_SCOPE_REJECTS`] nudges on one path the guard opens permanently for
//! that path, so a legitimate cross-file fix (a type the PR changed breaking a
//! consumer, a genuinely-needed new file) can never deadlock the agent.
//! Initial implementation and human-directed feedback run UNSCOPED — a human
//! asking for a change outside the diff is authorization, not overreach.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// Nudges per path before the guard opens (escape hatch — never deadlocks).
pub const MAX_SCOPE_REJECTS: u32 = 2;

/// Compare-API responses cap at 300 files; near the cap the list may be
/// incomplete, so callers fall back to unscoped rather than false-block.
pub const MAX_TRUSTED_SCOPE_FILES: usize = 290;

pub struct ScopeGuard {
    /// None = unscoped (initial implement, human-directed feedback, or the
    /// changed-file listing was unavailable/untrustworthy — fail open).
    scope: Option<HashSet<String>>,
    rejects: Mutex<HashMap<String, u32>>,
    /// Paths granted into scope mid-pass (e.g. files run_codegen just
    /// regenerated and committed — they're now part of the PR).
    allowed: Mutex<HashSet<String>>,
}

/// Normalize tool-supplied paths to the repo-relative form the compare API
/// uses ("apps/web/src/x.ts" — no leading "./" or "/").
fn normalize(path: &str) -> &str {
    path.trim_start_matches("./").trim_start_matches('/')
}

/// Pure block decision (unit-tested): block only when scoped, out of scope,
/// and still under the per-path reject cap.
pub fn scope_block_decision(scoped: bool, in_scope: bool, prior_rejects: u32) -> bool {
    scoped && !in_scope && prior_rejects < MAX_SCOPE_REJECTS
}

impl ScopeGuard {
    pub fn unscoped() -> Self {
        Self {
            scope: None,
            rejects: Mutex::new(HashMap::new()),
            allowed: Mutex::new(HashSet::new()),
        }
    }

    /// Scoped to the PR's changed files. Falls back to unscoped when the list
    /// is empty (fresh/reset branch — nothing to anchor scope to) or too large
    /// to trust (compare-API cap).
    pub fn scoped(files: Vec<String>) -> Self {
        if files.is_empty() || files.len() >= MAX_TRUSTED_SCOPE_FILES {
            return Self::unscoped();
        }
        Self {
            scope: Some(files.iter().map(|f| normalize(f).to_string()).collect()),
            rejects: Mutex::new(HashMap::new()),
            allowed: Mutex::new(HashSet::new()),
        }
    }

    /// Grant a path into scope mid-pass (files run_codegen just committed are
    /// now legitimately part of the PR).
    pub fn allow(&self, path: &str) {
        self.allowed
            .lock()
            .unwrap()
            .insert(normalize(path).to_string());
    }

    /// Should a write to `path` be rejected as out-of-ticket-scope? Counts the
    /// nudge only when it actually blocks. Callers apply this to EDITS of
    /// existing files only — creating a new file is never scope-blocked (a
    /// needed new module is not "pre-existing repo debt").
    pub fn should_block(&self, path: &str) -> bool {
        let norm = normalize(path);
        let in_scope = match &self.scope {
            None => return false,
            Some(s) => s.contains(norm) || self.allowed.lock().unwrap().contains(norm),
        };
        let mut rejects = self.rejects.lock().unwrap();
        let n = rejects.entry(norm.to_string()).or_insert(0);
        if scope_block_decision(true, in_scope, *n) {
            *n += 1;
            true
        } else {
            false
        }
    }

    /// The reject message steering the agent back to ticket scope.
    pub fn reject_msg(path: &str) -> String {
        format!(
            "REJECTED editing {path}: this existing file is NOT part of this PR's changes, so a \
             lint/type warning in it pre-exists on the base branch and is OUT OF SCOPE for this \
             ticket. Do NOT fix pre-existing repo issues (unused imports, style warnings, \
             hook-deps) in unrelated files just to quiet CI — mention them in your summary \
             instead. ONLY if this PR's own changes genuinely broke this file (e.g. a type you \
             changed is consumed here), state that in the commit message and retry."
        )
    }
}

#[cfg(test)]
mod scope_guard_tests {
    use super::*;

    #[test]
    fn blocks_out_of_scope_then_opens() {
        let g = ScopeGuard::scoped(vec!["apps/web/src/feature.tsx".into()]);
        // In-scope file: never blocked.
        assert!(!g.should_block("apps/web/src/feature.tsx"));
        assert!(!g.should_block("./apps/web/src/feature.tsx")); // normalized
                                                                // Out-of-scope: blocked MAX_SCOPE_REJECTS times, then opens forever.
        for _ in 0..MAX_SCOPE_REJECTS {
            assert!(g.should_block("apps/web/src/services/unrelated.ts"));
        }
        assert!(!g.should_block("apps/web/src/services/unrelated.ts"));
        assert!(!g.should_block("apps/web/src/services/unrelated.ts"));
    }

    #[test]
    fn unscoped_and_untrusted_never_block() {
        assert!(!ScopeGuard::unscoped().should_block("anything.ts"));
        // Empty scope (fresh/reset branch) → unscoped.
        assert!(!ScopeGuard::scoped(vec![]).should_block("anything.ts"));
        // Near the compare cap → unscoped.
        let big: Vec<String> = (0..MAX_TRUSTED_SCOPE_FILES)
            .map(|i| format!("f{i}.ts"))
            .collect();
        assert!(!ScopeGuard::scoped(big).should_block("anything.ts"));
    }

    #[test]
    fn decision_is_pure_and_bounded() {
        assert!(scope_block_decision(true, false, 0));
        assert!(scope_block_decision(true, false, MAX_SCOPE_REJECTS - 1));
        assert!(!scope_block_decision(true, false, MAX_SCOPE_REJECTS)); // escape
        assert!(!scope_block_decision(true, true, 0)); // in scope
        assert!(!scope_block_decision(false, false, 0)); // unscoped
    }
}
