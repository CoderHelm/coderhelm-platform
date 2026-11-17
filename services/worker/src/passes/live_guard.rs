//! What a write may carry: selectors seen on the live page, and only the
//! lines the change needs.
//!
//! A portal spec was written twice from guesses. The first time the coder
//! tried to load the vendor's page with Playwright, its shell refused the
//! command, and it read test-id strings out of the vendor's script bundle
//! instead; the second time a fix round kept that code untouched while it
//! edited the file around it. Neither ever saw what a signed-out visitor
//! gets. The same rounds re-wrapped lines in files CI never formats.
//!
//! In every coder round now, before a write is made:
//!
//! - **An end-to-end file that drives another company's page carries only
//!   test ids a probe saw** ([`LiveGuard::check_e2e`]): the ids the write
//!   adds and the ones it keeps. The probe is the `probe_page` tool, its
//!   record committed on the branch by the round itself
//!   ([`LiveGuard::saw_probe`]). Until there is one, the write is refused
//!   with what to probe. When the probe of that page can't run (a refused
//!   host, a sandbox failure), the write goes ahead and the selectors are
//!   kept as unprobed in the round's evidence, which the pull request
//!   lists: never a dead end, never silent.
//! - **A write that only reformats is not made, and a write's formatting
//!   hunks are put back** ([`LiveGuard::shape`]), unless the repository's
//!   CI checks that file's formatting and the change touches it anyway.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use common::diff_shape;
use common::e2e::probe::{self, ProbeRecord};

/// What a write's text becomes, or why it is not made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shaped {
    /// Write this text (the write's own, or with formatting hunks put back:
    /// how many).
    Write(String, usize),
    /// Not written: why, for the coder.
    Refused(String),
}

/// Selectors written without a probe: path, origin, ids, why none ran.
type Kept = (String, String, Vec<String>, String);

/// The guard of one coder round.
#[derive(Debug, Default)]
pub struct LiveGuard {
    /// CI's formatter checks (`prettier --check`, `cargo fmt --check`, ...).
    ci_formats: Vec<String>,
    /// The team's own hosts: their pages need no probe.
    own: Vec<String>,
    /// Probe records: the branch's, and this round's.
    records: Mutex<Vec<ProbeRecord>>,
    /// Hosts a probe was tried on and couldn't run, with why.
    failed: Mutex<Vec<(String, String)>>,
    /// Selectors written without a probe because none could run: path,
    /// origin, ids, why.
    unprobed: Mutex<Vec<Kept>>,
    /// Formatting hunks put back: path, how many.
    dropped: Mutex<Vec<(String, usize)>>,
}

/// Whether a path is end-to-end test code.
pub fn is_e2e(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    let code = [".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs"]
        .iter()
        .any(|e| p.ends_with(e));
    code && (p.starts_with("e2e/")
        || p.contains("/e2e/")
        || p.contains("playwright/")
        || p.contains("cypress/")
        || p.contains(".e2e.")
        || p.contains(".cy."))
}

impl LiveGuard {
    /// A round's guard: CI's formatter commands, the team's own hosts and
    /// the probe records on the branch.
    pub fn new(ci_formats: Vec<String>, own: Vec<String>, records: Vec<ProbeRecord>) -> Self {
        Self {
            ci_formats,
            own,
            records: Mutex::new(records),
            ..Default::default()
        }
    }

    /// The text a write of `path` makes, from `old` (the branch's) to
    /// `new`. `touched` says whether the change already changes the file
    /// against the base branch.
    pub fn shape(&self, path: &str, old: &str, new: &str, touched: bool) -> Shaped {
        if old == new {
            return Shaped::Write(new.to_string(), 0);
        }
        let ci = diff_shape::ci_formats(&self.ci_formats, path);
        if diff_shape::format_only(path, old, new) {
            if touched && ci {
                // CI checks this file's formatting and the change is in it:
                // the formatter's lines are the change's to carry.
                return Shaped::Write(new.to_string(), 0);
            }
            return Shaped::Refused(format!(
                "Not written: this write only reformats `{path}` (whitespace, wrapping, quotes, trailing commas). {} Keep the file as it is and change only the lines the fix needs.",
                if ci {
                    "The change doesn't otherwise touch it, so CI's format check already passes on it."
                } else {
                    "The repository's CI doesn't check its formatting, so a reformat is churn a reviewer has to read past."
                }
            ));
        }
        if ci {
            return Shaped::Write(new.to_string(), 0);
        }
        let (kept, n) = diff_shape::drop_format_hunks(path, old, new);
        if n > 0 {
            self.dropped.lock().unwrap().push((path.to_string(), n));
        }
        Shaped::Write(kept, n)
    }

    /// Records a probe tool's answer: its record (committed by the caller)
    /// or the host it couldn't probe.
    pub fn saw_probe(&self, input: &serde_json::Value, answer: &str) -> Option<ProbeRecord> {
        if let Some(r) = probe::record_in(answer) {
            let mut records = self.records.lock().unwrap();
            records.retain(|x| !x.origin.eq_ignore_ascii_case(&r.origin));
            records.push(r.clone());
            return Some(r);
        }
        let url = input["url"].as_str().unwrap_or("");
        let host = url
            .split("://")
            .nth(1)
            .unwrap_or(url)
            .split(['/', '?', '#'])
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        if !host.is_empty() {
            self.failed
                .lock()
                .unwrap()
                .push((host, common::truncate_str(answer.trim(), 200).to_string()));
        }
        None
    }

    /// Whether a write of `path` with the full text `new` may go ahead:
    /// every test id it uses on a page another company serves was seen by
    /// a probe of that page, or a probe of it was tried and couldn't run.
    pub fn check_e2e(&self, path: &str, new: &str) -> Result<(), String> {
        if !is_e2e(path) {
            return Ok(());
        }
        let records = self.records.lock().unwrap().clone();
        let gaps = probe::unprobed(
            &[(path.to_string(), new.to_string(), String::new())],
            &records,
            &self.own,
        );
        let mut refuse = Vec::new();
        for g in gaps {
            let failed = self
                .failed
                .lock()
                .unwrap()
                .iter()
                .find(|(h, _)| g.origin == probe::FRAMED || h.eq_ignore_ascii_case(&g.origin))
                .map(|(_, why)| why.clone());
            match failed {
                Some(why) => {
                    let mut u = self.unprobed.lock().unwrap();
                    u.retain(|(p, o, _, _)| !(p == path && *o == g.origin));
                    u.push((path.to_string(), g.origin.clone(), g.missing.clone(), why));
                }
                None => refuse.push(g),
            }
        }
        if refuse.is_empty() {
            return Ok(());
        }
        Err(format!(
            "Not written: {} Then write again: the probe's record is committed for you and this write goes through once every test id it uses (added or kept) is one the probe saw.",
            refuse
                .iter()
                .map(|g| g.body())
                .collect::<Vec<_>>()
                .join(" ")
        ))
    }

    /// The round's evidence lines: selectors kept without a probe (and why
    /// none could run), and formatting put back. The habits check and the
    /// pull request read them.
    pub fn evidence_lines(&self) -> String {
        let mut out = String::new();
        for (path, origin, ids, why) in self.unprobed.lock().unwrap().iter() {
            out.push_str(&format!(
                "Unprobed: {path}: {origin}: {}: the probe couldn't run ({why})\n",
                ids.join(", ")
            ));
        }
        for (path, n) in self.dropped.lock().unwrap().iter() {
            out.push_str(&format!(
                "Formatting put back: {path}: {n} hunk(s) that only reformatted\n"
            ));
        }
        out
    }
}

/// The probe records committed on `branch` (`.coderhelm/probes/`).
pub async fn branch_records(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    branch: &str,
) -> Vec<ProbeRecord> {
    let Ok(tree) = github.get_tree(owner, repo, branch).await else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for e in tree.iter().filter(|e| probe::is_record(&e.path)).take(20) {
        if let Ok(text) = github.read_file(owner, repo, &e.path, branch).await {
            if let Ok(r) = serde_json::from_str::<ProbeRecord>(&text) {
                out.push(r);
            }
        }
    }
    out
}

/// Commits a probe's record at its path on `branch` (`message` is the
/// commit's). The path, or why it couldn't be written.
pub async fn commit_record(
    github: &crate::clients::github::GitHubClient,
    (owner, repo, branch): (&str, &str, &str),
    record: &ProbeRecord,
    message: &str,
) -> Result<String, String> {
    let path = probe::record_path(&record.origin);
    let body = serde_json::to_string_pretty(record).unwrap_or_default() + "\n";
    let sha = github.get_file_sha(owner, repo, &path, branch).await.ok();
    github
        .write_file(owner, repo, &path, &body, branch, message, sha.as_deref())
        .await
        .map(|_| path)
        .map_err(|e| e.to_string())
}

/// The CI formatter commands among a round's CI steps.
pub fn format_cmds(steps: &[super::checks::Step]) -> Vec<String> {
    steps
        .iter()
        .filter(|s| s.kinds.contains(&super::checks::CheckKind::Format))
        .map(|s| s.cmd.clone())
        .collect()
}

// ── After the build: what the pull request says and what goes back ─────────

/// A test branch's arm and what is known of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArmLine {
    pub path: String,
    /// The branch's line.
    pub line: usize,
    /// Whether its condition holds on this arm.
    pub when: bool,
    /// How a person reads it ("when `isEnabled()` on `…` is true").
    pub words: String,
    /// `ran`: a recorded run took it (`why` says which); `now`: the page
    /// shows its state now (`why` says which probe); `unverified`.
    pub state: String,
    #[serde(default)]
    pub why: String,
}

/// Selectors a probe of their page didn't see.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnprobedLine {
    pub path: String,
    pub origin: String,
    pub missing: Vec<String>,
    /// Why no probe could run; empty when none was tried.
    #[serde(default)]
    pub why_none: String,
}

/// What the check reads from the change's end-to-end files and diff.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LiveFacts {
    /// The probes of the pages the change's end-to-end files drive.
    pub probes: Vec<ProbeRecord>,
    pub unprobed: Vec<UnprobedLine>,
    /// Arms of test branches on a page's state.
    pub arms: Vec<ArmLine>,
    /// Changed files whose change is only formatting.
    pub reformatted: Vec<String>,
    /// Formatting the rounds put back: path, hunks.
    pub put_back: Vec<(String, usize)>,
}

/// The evidence line a coder writes once it ran a branch's arm:
/// `Branch ran: <path>:<line> when <true|false>: <the command and result>`.
pub const RAN_LINE: &str = "branch ran:";

fn evidence(notes: &str, mark: &str) -> Vec<String> {
    notes
        .lines()
        .filter_map(|l| {
            let l = l.trim().trim_start_matches(['-', '*']).trim();
            let at = l.to_ascii_lowercase().find(mark)?;
            Some(l[at + mark.len()..].trim().to_string())
        })
        .collect()
}

/// Reads the change: `files` are (path, base text when it existed, head
/// text); `records` the probes on the branch; `fresh` probes taken now;
/// `notes` the rounds' evidence lines.
pub fn assess(
    files: &[(String, Option<String>, String)],
    records: &[ProbeRecord],
    fresh: &[ProbeRecord],
    own: &[String],
    notes: &str,
) -> LiveFacts {
    let mut f = LiveFacts::default();
    let latest = |origin: &str| -> Option<&ProbeRecord> {
        fresh
            .iter()
            .chain(records.iter())
            .find(|r| r.origin.eq_ignore_ascii_case(origin))
    };
    let couldnt: Vec<(String, String)> = evidence(notes, "unprobed:")
        .into_iter()
        .filter_map(|rest| {
            let mut parts = rest.splitn(4, ": ");
            let path = parts.next()?.trim().to_string();
            let origin = parts.next()?.trim().to_string();
            let _ids = parts.next();
            Some((
                format!("{path}\u{1}{origin}"),
                parts.next().unwrap_or("").to_string(),
            ))
        })
        .collect();
    let ran = evidence(notes, RAN_LINE);
    for (path, base, head) in files {
        if let Some(base) = base {
            if diff_shape::format_only(path, base, head) {
                f.reformatted.push(path.clone());
            }
        }
        if !is_e2e(path) {
            continue;
        }
        let origins = probe::outside_origins(head, own);
        for o in &origins {
            let rs: Vec<&ProbeRecord> = if o == probe::FRAMED {
                fresh.iter().chain(records.iter()).collect()
            } else {
                latest(o).into_iter().collect()
            };
            for r in rs {
                if !f.probes.iter().any(|p| p.origin == r.origin) {
                    f.probes.push(r.clone());
                }
            }
        }
        let all: Vec<ProbeRecord> = fresh.iter().chain(records.iter()).cloned().collect();
        for g in probe::unprobed(&[(path.clone(), head.clone(), String::new())], &all, own) {
            let why_none = couldnt
                .iter()
                .find(|(k, _)| *k == format!("{}\u{1}{}", g.path, g.origin))
                .map(|(_, w)| w.clone())
                .unwrap_or_default();
            f.unprobed.push(UnprobedLine {
                path: g.path,
                origin: g.origin,
                missing: g.missing,
                why_none,
            });
        }
        if origins.is_empty() {
            continue;
        }
        for b in common::e2e::branches::state_branches(path, head) {
            let probe_now = origins
                .iter()
                .filter_map(|o| latest(o))
                .find_map(|r| common::e2e::branches::runnable_now(&b, r).map(|w| (w, r)));
            for arm in &b.arms {
                let key = format!("{path}:{} when {}", b.line, arm.when);
                let (state, why) = if let Some(r) = ran.iter().find(|r| r.starts_with(&key)) {
                    ("ran", r.split_once(": ").map_or("", |x| x.1).to_string())
                } else {
                    match probe_now {
                        Some((w, r)) if w == arm.when => (
                            "now",
                            format!("the probe of `{}` at {} shows that state", r.origin, r.at),
                        ),
                        _ => ("unverified", String::new()),
                    }
                };
                f.arms.push(ArmLine {
                    path: path.clone(),
                    line: b.line,
                    when: arm.when,
                    words: b.arm_words(arm),
                    state: state.to_string(),
                    why,
                });
            }
        }
    }
    for rest in evidence(notes, "formatting put back:") {
        if let Some((path, n)) = rest.split_once(": ") {
            let n = n
                .split_whitespace()
                .next()
                .and_then(|x| x.parse().ok())
                .unwrap_or(1);
            f.put_back.push((path.trim().to_string(), n));
        }
    }
    f
}

impl LiveFacts {
    /// What the coder must fix, one line each.
    pub fn blocking(&self) -> Vec<String> {
        let mut out = Vec::new();
        for u in self.unprobed.iter().filter(|u| u.why_none.is_empty()) {
            out.push(
                probe::Unprobed {
                    path: u.path.clone(),
                    origin: u.origin.clone(),
                    missing: u.missing.clone(),
                }
                .body(),
            );
        }
        for p in &self.reformatted {
            out.push(format!(
                "`{p}` is only reformatted: the fix changes nothing in it. Put it back with `restore_file` (omit `from_ref`)."
            ));
        }
        for a in self.arms.iter().filter(|a| a.state == "now") {
            out.push(format!(
                "`{}:{}`: the branch {} has never run in a recorded run, and {} now. Run that spec against the live page now (the repository's Playwright command with its base address set to staging or the vendor), fix the branch from what the page really does, and when it passes write `Branch ran: {}:{} when {}: <the command and its result>` in your Evidence.",
                a.path,
                a.line,
                a.words,
                a.why,
                a.path,
                a.line,
                a.when
            ));
        }
        out
    }

    /// The pull request's lines: the probes quoted, selectors no probe saw
    /// (with why), each test branch's arm and whether a run took it, and
    /// formatting put back. Empty when there is nothing to say.
    pub fn section(&self) -> String {
        let mut out = String::new();
        if !self.probes.is_empty() {
            out.push_str("**Outside pages, as a probe saw them**\n");
            for r in &self.probes {
                out.push_str(&probe::quote(r));
            }
            out.push('\n');
        }
        let kept: Vec<&UnprobedLine> = self.unprobed.iter().collect();
        if !kept.is_empty() {
            out.push_str("**Selectors no probe saw**\n");
            for u in kept {
                let why = if u.why_none.is_empty() {
                    "no probe yet; CoderHelm probes the page in its next round".to_string()
                } else {
                    format!("no probe could run: {}", u.why_none)
                };
                out.push_str(&format!(
                    "- `{}` on `{}`: {} ({why}).\n",
                    u.path,
                    u.origin,
                    u.missing
                        .iter()
                        .map(|m| format!("`{m}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            out.push('\n');
        }
        if !self.arms.is_empty() {
            out.push_str("**Test branches on the page's state**\n");
            for a in &self.arms {
                let state = match a.state.as_str() {
                    "ran" => format!("ran: {}", a.why),
                    "now" => format!(
                        "unverified: no recorded run took it; {}, so CoderHelm runs it in its next round",
                        a.why
                    ),
                    _ => "unverified: no recorded run took it; CoderHelm runs it once the page shows that state".to_string(),
                };
                out.push_str(&format!(
                    "- `{}:{}` {}: {state}.\n",
                    a.path, a.line, a.words
                ));
            }
            out.push('\n');
        }
        if !self.put_back.is_empty() {
            out.push_str("**Formatting put back** (the fix's lines only)\n");
            for (p, n) in &self.put_back {
                out.push_str(&format!("- `{p}`: {n} hunk(s) that only reformatted.\n"));
            }
            out.push('\n');
        }
        out
    }

    /// The Open items point and how it settles, for one of [`blocking`]'s
    /// lines.
    pub fn point_of(&self, line: &str) -> Option<(&'static str, &'static str)> {
        if !self.blocking().iter().any(|b| b == line) {
            return None;
        }
        Some(if line.contains("no probe of that page observed") {
            (
                "Selectors no probe saw",
                "CoderHelm probes the page and rebuilds the selectors from what it saw in its next round on this pull request.",
            )
        } else if line.contains("is only reformatted") {
            (
                "Reformatted a file the fix doesn't change",
                "CoderHelm puts it back in its next round on this pull request.",
            )
        } else {
            (
                "Test branch never run",
                "CoderHelm runs it against the live page and fixes it from what it does in its next round on this pull request.",
            )
        })
    }
}

/// Probes the pages the change's end-to-end branches read again, now (at
/// most two), from their committed records' address and clicks: a branch
/// that couldn't run before is re-checked once the page can take it.
pub async fn fresh_probes(
    state: &crate::WorkerState,
    team_id: &str,
    records: &[ProbeRecord],
    files: &[(String, Option<String>, String)],
    own: &[String],
) -> Vec<ProbeRecord> {
    let mut wanted: Vec<&ProbeRecord> = Vec::new();
    for (path, _, head) in files.iter().filter(|(p, _, _)| is_e2e(p)) {
        if common::e2e::branches::state_branches(path, head)
            .iter()
            .all(|b| b.subject.is_none())
        {
            continue;
        }
        for o in probe::outside_origins(head, own) {
            if let Some(r) = records.iter().find(|r| r.origin.eq_ignore_ascii_case(&o)) {
                if !wanted.iter().any(|w| w.origin == r.origin) {
                    wanted.push(r);
                }
            }
        }
    }
    let mut out = Vec::new();
    for r in wanted.into_iter().take(2) {
        let clicks: Vec<String> = r.steps.iter().map(|s| s.click.clone()).collect();
        match super::web_check::run_probe(state, team_id, &r.url, &r.origin, &clicks).await {
            Ok(n) => out.push(n),
            Err(e) => {
                tracing::warn!(origin = %r.origin, error = %e, "Could not probe the page again")
            }
        }
    }
    out
}

/// The facts for the habits check: the changed files' base and head text,
/// the branch's probes, a fresh probe of the pages whose state the
/// branches read (when there is time), and the rounds' evidence lines.
pub async fn facts_for(
    w: &super::proof::Work<'_>,
    diff: &[(String, String, String)],
    notes: &str,
    probe_again: bool,
) -> LiveFacts {
    let (owner, repo) = (&w.msg.repo_owner, &w.msg.repo_name);
    let mut files = Vec::new();
    for (path, status, _) in diff
        .iter()
        .filter(|(p, s, _)| s != "removed" && !probe::is_record(p))
        .take(40)
    {
        let Ok(head) = w.github.read_file(owner, repo, path, w.branch).await else {
            continue;
        };
        let base = if status == "added" {
            None
        } else {
            w.github
                .read_file(owner, repo, path, &w.msg.base_branch)
                .await
                .ok()
        };
        files.push((path.clone(), base, head));
    }
    let records = branch_records(w.github, owner, repo, w.branch).await;
    let own = super::web_check::team_domains(w.state, &w.msg.team_id).await;
    let fresh = if probe_again {
        fresh_probes(w.state, &w.msg.team_id, &records, &files, &own).await
    } else {
        Vec::new()
    };
    assess(&files, &records, &fresh, &own, notes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const PORTAL: &str = "const URL = 'https://portal.vendor.test/p/1';\nexport class P {\n  cart() { return this.f.getByTestId('cart-subtotal'); }\n}\n";

    #[test]
    fn an_outside_selector_waits_for_a_probe_or_its_failure() {
        let g = LiveGuard::new(vec![], vec!["acme.test".into()], vec![]);
        let e = g.check_e2e("e2e/pages/portal.ts", PORTAL).unwrap_err();
        assert!(e.contains("`cart-subtotal`"));
        assert!(e.contains("probe_page"));
        // Not end-to-end code: no gate.
        assert!(g.check_e2e("apps/web/src/a.tsx", PORTAL).is_ok());
        // A probe that saw it lets the write through.
        let r = ProbeRecord {
            origin: "portal.vendor.test".into(),
            testids: vec!["cart-subtotal".into()],
            ..Default::default()
        };
        let answer = format!(
            "Probe of x.\n{}\n{}",
            probe::RECORD_MARK,
            serde_json::to_string(&r).unwrap()
        );
        assert_eq!(
            g.saw_probe(&json!({"url": "https://portal.vendor.test/p/1"}), &answer),
            Some(r)
        );
        assert!(g.check_e2e("e2e/pages/portal.ts", PORTAL).is_ok());
    }

    #[test]
    fn a_probe_that_cannot_run_is_no_dead_end_and_is_said() {
        let g = LiveGuard::new(vec![], vec![], vec![]);
        g.saw_probe(
            &json!({"url": "https://portal.vendor.test/p/1"}),
            "https://portal.vendor.test/p/1: the sandbox couldn't run the probe (timeout)",
        );
        assert!(g.check_e2e("e2e/pages/portal.ts", PORTAL).is_ok());
        assert!(g.evidence_lines().contains(
            "Unprobed: e2e/pages/portal.ts: portal.vendor.test: cart-subtotal: the probe couldn't run"
        ));
    }

    #[test]
    fn reformatting_is_not_written_and_formatting_hunks_go_back() {
        let g = LiveGuard::new(vec![], vec![], vec![]);
        let old = "expect(\n  a('x')\n).toBe(1)\nconst n = 1\n";
        let reformat = "expect(a(\"x\")).toBe(1);\nconst n = 1;\n";
        assert!(matches!(
            g.shape("e2e/a.spec.ts", old, reformat, false),
            Shaped::Refused(m) if m.contains("only reformats")
        ));
        let mixed = "expect(a(\"x\")).toBe(1);\nconst n = 2;\n";
        assert_eq!(
            g.shape("e2e/a.spec.ts", old, mixed, true),
            Shaped::Write("expect(\n  a('x')\n).toBe(1)\nconst n = 2;\n".into(), 1)
        );
        assert!(g
            .evidence_lines()
            .contains("Formatting put back: e2e/a.spec.ts: 1 hunk(s)"));
        // Where CI checks formatting and the change is in the file, the
        // formatter's lines stay.
        let ci = LiveGuard::new(vec!["npx prettier --check .".into()], vec![], vec![]);
        assert_eq!(
            ci.shape("web/a.ts", old, reformat, true),
            Shaped::Write(reformat.into(), 0)
        );
        assert!(matches!(
            ci.shape("web/a.ts", old, reformat, false),
            Shaped::Refused(_)
        ));
    }
}
