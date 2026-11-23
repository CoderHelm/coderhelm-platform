//! A red check is reproduced before anything is decided about it.
//!
//! A pull request's end-to-end test failed on a vendor's portal that had
//! just opened registration: the test was red on every branch. The fix
//! rounds first wrote that a different test was "broken on main", then the
//! supervisor reverted seven of the pull request's own files to an older
//! head, which failed the same way. The failure comment on the pull request
//! named the test, its error and had a screenshot of what the page showed;
//! the run still wrote that it "couldn't confirm" the cause. Four other
//! runs reverted their own work the same way, one of them for a cloud
//! account's load balancer limit in the preview deploy.
//!
//! The rules, each enforced in code:
//!
//! - **A comparison is on record before a decision.** [`Record`]: the
//!   failing test run with the head's test code against the base branch's
//!   deployment (staging, `base`) and against the pull request's (the
//!   preview, `change`), or the same check's result on the base branch's
//!   head next to its result on this head. [`Record::verdict`] reads it:
//!   red on the base too is not this change's; green on the base and red
//!   here is this change's; a leg that did not run (no address, no browser,
//!   the host unreachable) proves nothing.
//! - **No revert without it.** [`may_revert`]: CoderHelm's own work comes
//!   out for red checks only when a record on this head shows this change
//!   makes a check fail, and never when one shows it fails on the base too.
//! - **No blame without it.** [`hold_blame`]: a sentence that calls a
//!   failure the base branch's, flaky, pre-existing or not this change's is
//!   replaced unless a record on this head shows the test fails on the base
//!   too. A sentence that quotes a measured count (`0/40 passed`) carries
//!   its own evidence and stays.
//! - **Read evidence is never "unconfirmed".** [`hold_unread`]: a sentence
//!   that says a failure couldn't be confirmed or read, while its report was
//!   read, is replaced by what the report says.
//! - **The fixer is told.** [`brief`]: the failing test, its error, the
//!   screenshot and report links, and the comparison with what it means for
//!   the fix: red on the base too means the test (or the code it checks) is
//!   fixed on this branch and said so, never a revert.

use serde::{Deserialize, Serialize};

/// The run record attribute that keeps the comparisons, as JSON.
pub const FIELD: &str = "ci_repro";

/// The marker the comparison script prints around each side's run.
pub const MARK: &str = "CODERHELM_REPRO";

/// One side of a comparison: where the test ran and what it counted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Leg {
    /// The address the test ran against, or `ci@<sha>` for a check's run.
    pub target: String,
    /// A real run happened (the browser started, the host answered).
    pub ran: bool,
    pub passed: u32,
    pub failed: u32,
    /// Why it didn't run, or the line that says what it found.
    #[serde(default)]
    pub note: String,
}

impl Leg {
    pub fn red(&self) -> bool {
        self.ran && self.failed > 0
    }

    pub fn green(&self) -> bool {
        self.ran && self.failed == 0 && self.passed > 0
    }

    fn line(&self, side: &str) -> String {
        let at = if self.target.is_empty() {
            String::new()
        } else {
            format!(" ({})", self.target)
        };
        if !self.ran {
            let why = if self.note.trim().is_empty() {
                "no result".to_string()
            } else {
                crate::truncate_str(self.note.trim(), 200).to_string()
            };
            return format!("{side}{at}: didn't run ({why})");
        }
        let result = if self.failed > 0 {
            format!("failed ({} failed, {} passed)", self.failed, self.passed)
        } else {
            format!("passed ({} passed)", self.passed)
        };
        format!("{side}{at}: {result}")
    }
}

/// A failing check's comparison on one head.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// The pull request's head it was made on.
    pub head: String,
    pub check: String,
    /// The failing tests compared (`file › title`); empty for a check.
    #[serde(default)]
    pub tests: Vec<String>,
    /// The base branch's deployment (staging), or the check on its head.
    pub base: Leg,
    /// The pull request's preview, or the check on this head.
    pub change: Leg,
    /// `sandbox` (the test was run) or `ci` (the check's own results).
    pub source: String,
    pub at: String,
}

/// What a comparison shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The test passes on the base and fails with this change.
    ThisChange(String),
    /// The test fails on the base too: the cause is outside this change.
    NotThisChange(String),
    /// Nothing is shown yet (a side didn't run, or both passed).
    Unproven(String),
}

impl Record {
    fn what(&self) -> String {
        match self.tests.first() {
            Some(t) => format!("`{t}`"),
            None => format!("`{}`", self.check),
        }
    }

    pub fn verdict(&self) -> Verdict {
        let what = self.what();
        if self.base.red() {
            return Verdict::NotThisChange(format!(
                "{what} fails on the base branch too ({})",
                self.base.line("base")
            ));
        }
        if self.base.green() && self.change.red() {
            return Verdict::ThisChange(format!(
                "{what} passes on the base branch and fails with this change"
            ));
        }
        if self.base.green() && self.change.green() {
            return Verdict::Unproven(format!(
                "{what} passed on both sides when run again, so the failure didn't reproduce"
            ));
        }
        let missing: Vec<String> = [("base", &self.base), ("this change", &self.change)]
            .iter()
            .filter(|(_, l)| !l.ran)
            .map(|(s, l)| l.line(s))
            .collect();
        Verdict::Unproven(format!(
            "{what} has no comparison yet: {}",
            if missing.is_empty() {
                "the base side didn't fail or pass".to_string()
            } else {
                missing.join("; ")
            }
        ))
    }

    /// One line for the run page and the pull request.
    pub fn line(&self) -> String {
        let how = match self.source.as_str() {
            "ci" => "the check's own runs",
            "history" => "the test's own runs on the base branch",
            _ => "run again in CoderHelm's sandbox",
        };
        let v = match self.verdict() {
            Verdict::ThisChange(w) => format!("This change's: {w}."),
            Verdict::NotThisChange(w) => format!("Not this change's: {w}."),
            Verdict::Unproven(w) => format!("Not shown either way: {w}."),
        };
        format!(
            "{v} Compared on {} ({how}): {}; {}.",
            short(&self.head),
            self.base.line("base"),
            self.change.line("this change")
        )
    }
}

fn short(sha: &str) -> String {
    sha.chars().take(7).collect()
}

/// The records kept on a run, as JSON.
pub fn to_json(records: &[Record]) -> String {
    serde_json::to_string(records).unwrap_or_default()
}

pub fn from_json(s: &str) -> Vec<Record> {
    serde_json::from_str(s).unwrap_or_default()
}

/// The records for `head`, newest kept when a check was compared twice.
pub fn on_head<'a>(records: &'a [Record], head: &str) -> Vec<&'a Record> {
    let mut out: Vec<&Record> = Vec::new();
    for r in records.iter().rev() {
        if !head.is_empty() && same_sha(&r.head, head) && !out.iter().any(|o| o.check == r.check) {
            out.push(r);
        }
    }
    out
}

/// Whether two commit ids name the same commit (one may be short).
pub fn same_sha(a: &str, b: &str) -> bool {
    let (a, b) = (a.trim(), b.trim());
    !a.is_empty() && !b.is_empty() && (a.starts_with(b) || b.starts_with(a))
}

/// `records` with `r` in place of an older one for the same head and check.
pub fn upsert(records: &[Record], r: Record) -> Vec<Record> {
    let mut out: Vec<Record> = records
        .iter()
        .filter(|o| !(same_sha(&o.head, &r.head) && o.check == r.check))
        .cloned()
        .collect();
    out.push(r);
    // A run keeps a handful: the newest twenty.
    let n = out.len();
    if n > 20 {
        out.drain(..n - 20);
    }
    out
}

/// A check's comparison from its own runs: its result on this head next to
/// its result on the base branch's head (`base_failed`: `None` when it
/// didn't run there).
pub fn from_checks(head: &str, base_sha: &str, check: &str, base_failed: Option<bool>) -> Record {
    let base = match base_failed {
        Some(failed) => Leg {
            target: format!("ci@{}", short(base_sha)),
            ran: true,
            passed: u32::from(!failed),
            failed: u32::from(failed),
            note: String::new(),
        },
        None => Leg {
            target: format!("ci@{}", short(base_sha)),
            ran: false,
            note: format!("`{check}` doesn't run on the base branch's head"),
            ..Default::default()
        },
    };
    Record {
        head: head.to_string(),
        check: check.to_string(),
        tests: Vec::new(),
        base,
        change: Leg {
            target: format!("ci@{}", short(head)),
            ran: true,
            passed: 0,
            failed: 1,
            note: String::new(),
        },
        source: "ci".into(),
        at: String::new(),
    }
}

/// A failing test's comparison from its own runs: its newest result on the
/// base branch (`(sha, failed, at)`, from the repository's test history)
/// next to this head's failure. The base for a check that never runs on the
/// base branch, such as a preview deploy's end-to-end tests.
pub fn from_history(
    head: &str,
    check: &str,
    test: &str,
    base_branch: &str,
    (sha, failed, at): (&str, bool, &str),
) -> Record {
    Record {
        head: head.to_string(),
        check: check.to_string(),
        tests: vec![test.to_string()],
        base: Leg {
            target: format!("tests@{base_branch}@{}", short(sha)),
            ran: true,
            passed: u32::from(!failed),
            failed: u32::from(failed),
            note: format!("its newest run on `{base_branch}` ({at})"),
        },
        change: Leg {
            target: format!("ci@{}", short(head)),
            ran: true,
            passed: 0,
            failed: 1,
            note: String::new(),
        },
        source: "history".into(),
        at: at.to_string(),
    }
}

// ── The revert gate ─────────────────────────────────────────────────────────

/// Whether CoderHelm may take its own work out for red checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevertGate {
    /// A record on this head shows the change makes a check fail.
    Allowed(String),
    /// Refused, with why: no record shows it, or one shows the failure is
    /// the base branch's too.
    Refused(String),
}

/// The revert gate (see the module's rules).
pub fn may_revert(records: &[Record], head: &str) -> RevertGate {
    let here = on_head(records, head);
    let not_ours: Vec<String> = here
        .iter()
        .filter_map(|r| match r.verdict() {
            Verdict::NotThisChange(w) => Some(w),
            _ => None,
        })
        .collect();
    if !not_ours.is_empty() {
        return RevertGate::Refused(format!(
            "{}: taking this pull request's work out would not make it pass",
            not_ours.join("; ")
        ));
    }
    let ours: Vec<String> = here
        .iter()
        .filter_map(|r| match r.verdict() {
            Verdict::ThisChange(w) => Some(w),
            _ => None,
        })
        .collect();
    if !ours.is_empty() {
        return RevertGate::Allowed(ours.join("; "));
    }
    let unproven: Vec<String> = here
        .iter()
        .filter_map(|r| match r.verdict() {
            Verdict::Unproven(w) => Some(w),
            _ => None,
        })
        .collect();
    RevertGate::Refused(if unproven.is_empty() {
        format!(
            "no comparison of the failing checks with the base branch is on record for {}",
            short(head)
        )
    } else {
        unproven.join("; ")
    })
}

/// Whether a record on `head` shows a failure is not this change's.
pub fn shows_not_ours(records: &[Record], head: &str) -> bool {
    on_head(records, head)
        .iter()
        .any(|r| matches!(r.verdict(), Verdict::NotThisChange(_)))
}

// ── What CoderHelm writes about a failure ───────────────────────────────────

const BLAME: &[&str] = &[
    "broken on main",
    "broken on `main`",
    "broken on the base",
    "fails on main",
    "fails on `main`",
    "failing on main",
    "failing on `main`",
    "red on main",
    "red on `main`",
    "also fails on",
    "also failing on",
    "pre-existing",
    "preexisting",
    "not this change",
    "not caused by this",
    "isn't caused by this",
    "is not caused by this",
    "unrelated to this change",
    "unrelated to this pull request",
    "unrelated failure",
    "unrelated test",
    "flaky",
    "flake",
    "outside this change",
    "didn't cause",
    "did not cause",
    "not ours",
];

fn sentences(text: &str) -> Vec<(usize, usize)> {
    // Spans of sentences: split at ". ", "! ", "? " and line ends.
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        let end = if b[i] == b'\n' {
            Some(i)
        } else if matches!(b[i], b'.' | b'!' | b'?') && (i + 1 == b.len() || b[i + 1] == b' ') {
            Some(i + 1)
        } else {
            None
        };
        if let Some(e) = end {
            if e > start {
                out.push((start, e));
            }
            start = e;
        }
        i += 1;
    }
    if start < b.len() {
        out.push((start, b.len()));
    }
    out
}

/// A sentence that quotes a measured count (`0/40 passed`, `3 of 20 runs`)
/// carries its own evidence.
fn measured(s: &str) -> bool {
    let low = s.to_ascii_lowercase();
    let b = low.as_bytes();
    let slash = b
        .windows(3)
        .any(|w| w[0].is_ascii_digit() && w[1] == b'/' && w[2].is_ascii_digit());
    slash || (low.contains(" of ") && low.contains(" runs") && b.iter().any(u8::is_ascii_digit))
}

/// Whether `sentence` says a failure is the base branch's, flaky,
/// pre-existing or not this change's.
pub fn blames(sentence: &str) -> bool {
    let low = sentence.to_ascii_lowercase();
    if low.trim_start().starts_with('#') || measured(&low) {
        return false;
    }
    // "Not flaky" says the opposite.
    let low = low.replace("not flaky", "").replace("isn't flaky", "");
    // A sentence about fixing a flaky test describes the work, not a blame.
    let about_the_work = ["fix", "deflake", "stabil"].iter().any(|w| low.contains(w));
    BLAME
        .iter()
        .filter(|p| !(about_the_work && matches!(**p, "flaky" | "flake")))
        .any(|p| low.contains(p))
}

/// What replaces a blame with no comparison behind it.
pub const BLAME_HELD: &str = "Whether this change caused the failure is not shown yet: no run of \
     the failing test against the base branch is on record.";

/// `text` with every sentence that blames the base branch, flakiness or
/// something outside this change replaced by [`BLAME_HELD`], unless a
/// record on `head` shows the failure on the base too. Returns whether
/// anything was replaced.
pub fn hold_blame(text: &str, records: &[Record], head: &str) -> (String, bool) {
    if shows_not_ours(records, head) {
        return (text.to_string(), false);
    }
    replace_sentences(text, blames, BLAME_HELD)
}

const UNREAD: &[&str] = &[
    "couldn't confirm",
    "could not confirm",
    "can't confirm",
    "cannot confirm",
    "unconfirmed",
    "couldn't read",
    "could not read",
    "can't read",
    "wasn't readable",
    "isn't readable",
    "couldn't see",
    "could not see",
    "haven't seen",
    "have not seen",
];

const ABOUT_A_FAILURE: &[&str] = &[
    "test",
    "e2e",
    "end-to-end",
    "check",
    " ci",
    "ci ",
    "failure",
    "failing",
    "report",
    "screenshot",
    "playwright",
    "log",
];

/// Whether `sentence` says a failure couldn't be confirmed or read.
pub fn says_unread(sentence: &str) -> bool {
    let low = format!(" {} ", sentence.to_ascii_lowercase());
    UNREAD.iter().any(|p| low.contains(p)) && ABOUT_A_FAILURE.iter().any(|w| low.contains(w))
}

/// `text` with every sentence that says a failure couldn't be confirmed or
/// read replaced by what its report says (`read`, one line), when it was
/// read. Returns whether anything was replaced.
pub fn hold_unread(text: &str, read: Option<&str>) -> (String, bool) {
    match read.map(str::trim).filter(|r| !r.is_empty()) {
        Some(r) => replace_sentences(
            text,
            says_unread,
            &format!("The failure's report says: {r}"),
        ),
        None => (text.to_string(), false),
    }
}

fn replace_sentences(text: &str, hit: fn(&str) -> bool, with: &str) -> (String, bool) {
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    let mut changed = false;
    let mut said = false;
    for (a, e) in sentences(text) {
        let s = &text[a..e];
        out.push_str(&text[last..a]);
        if hit(s) {
            changed = true;
            let lead = &s[..s.len() - s.trim_start().len()];
            let bullet = s.trim_start();
            let prefix: String = bullet
                .chars()
                .take_while(|c| matches!(c, '-' | '*' | ' '))
                .collect();
            if !said {
                out.push_str(lead);
                out.push_str(&prefix);
                out.push_str(with);
                said = true;
            }
        } else {
            out.push_str(s);
        }
        last = e;
    }
    out.push_str(&text[last..]);
    (out, changed)
}

// ── The evidence the fixer reads ────────────────────────────────────────────

/// What a failure's own report says, for the fixer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    pub check: String,
    pub tests: Vec<String>,
    pub error: String,
    /// Where it was read (`comment on the pull request`, `job log`, ...).
    pub source: String,
    /// Screenshots linked from the failure comment.
    pub shots: Vec<String>,
    /// The published report it links to.
    pub report: Option<String>,
}

impl Evidence {
    /// One line: the test, the error, the screenshot.
    pub fn line(&self) -> String {
        let mut s = format!("`{}` failed", self.check);
        if let Some(t) = self.tests.first() {
            s.push_str(&format!(" in `{t}`"));
        }
        if !self.error.trim().is_empty() {
            s.push_str(&format!(
                ": {}",
                crate::truncate_str(self.error.trim(), 200).replace('`', "'")
            ));
        }
        if let Some(shot) = self.shots.first() {
            s.push_str(&format!(" (screenshot: {shot})"));
        }
        s
    }
}

/// The image links in a failure comment: Markdown images, `<img src>` and
/// links to an image file.
pub fn shots_in(comment: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |u: &str| {
        let u = u.trim().trim_matches(|c| c == '"' || c == '\'');
        if u.starts_with("https://") && !out.iter().any(|o| o == u) {
            out.push(u.to_string());
        }
    };
    for (i, _) in comment.match_indices("![") {
        let rest = &comment[i..];
        if let Some(open) = rest.find("](") {
            let after = &rest[open + 2..];
            if let Some(close) = after.find(')') {
                push(&after[..close]);
            }
        }
    }
    for (i, _) in comment.match_indices("src=") {
        let rest = &comment[i + 4..];
        let q = rest.chars().next().unwrap_or(' ');
        if q == '"' || q == '\'' {
            if let Some(end) = rest[1..].find(q) {
                push(&rest[1..1 + end]);
            }
        }
    }
    for (i, _) in comment.match_indices("https://") {
        let rest = &comment[i..];
        let end = rest
            .find(|c: char| c.is_whitespace() || matches!(c, ')' | '"' | '\'' | '>' | ']'))
            .unwrap_or(rest.len());
        let url = &rest[..end];
        let low = url.to_ascii_lowercase();
        let path = low.split(['?', '#']).next().unwrap_or("");
        if [".png", ".jpg", ".jpeg", ".webp", ".gif"]
            .iter()
            .any(|e| path.ends_with(e))
        {
            push(url);
        }
    }
    out.truncate(4);
    out
}

/// The fixer's section about the failure: what its report says and what
/// the comparison shows, with what that means for the fix.
pub fn brief(evidence: Option<&Evidence>, records: &[Record], head: &str) -> String {
    let mut s = String::new();
    if let Some(e) = evidence {
        s.push_str("\n\n## The failure, as its own report says\n");
        s.push_str(&format!(
            "- Check: `{}` (read from the {})\n",
            e.check, e.source
        ));
        for t in e.tests.iter().take(5) {
            s.push_str(&format!("- Failing test: `{t}`\n"));
        }
        if !e.error.trim().is_empty() {
            s.push_str(&format!(
                "- Error: `{}`\n",
                crate::truncate_str(e.error.trim(), 400).replace('`', "'")
            ));
        }
        for shot in &e.shots {
            s.push_str(&format!(
                "- Screenshot at the failure: {shot} (attached to this message as an image: look at it before deciding)\n"
            ));
        }
        if let Some(r) = &e.report {
            s.push_str(&format!("- Report: {r}\n"));
        }
        s.push_str(
            "Read these before you write anything about the failure; never write that it \
             couldn't be confirmed or read.\n",
        );
    }
    let here = on_head(records, head);
    s.push_str("\n## The failing test against the base branch and against this pull request\n");
    if here.is_empty() {
        s.push_str(
            "- No comparison is on record for this head.\n\
             Before you decide the failure isn't this pull request's, or take any of its work \
             out, run the failing test in the sandbox against the base branch's deployment \
             (staging) and against this pull request's preview (its base URL set to each), and \
             say what each did.\n",
        );
    }
    for r in &here {
        s.push_str(&format!("- {}\n", r.line()));
        s.push_str(match r.verdict() {
            Verdict::NotThisChange(_) => {
                "  It fails on the base branch too, so the cause is outside this change \
                 (data, a service, the test itself). Fix the test, or the code it checks, on \
                 this branch, and say so in one line starting `Fails on the base branch too:` \
                 that names the test and what is different outside the change. Never revert \
                 this pull request's work for it.\n"
            }
            Verdict::ThisChange(_) => {
                "  This change makes it fail: fix the change, or the test when the change \
                 means to behave differently.\n"
            }
            Verdict::Unproven(_) => {
                "  Nothing is shown either way yet. Reproduce it before deciding: run the \
                 failing test against both addresses. Don't write that it is the base \
                 branch's, flaky or not this change's: that needs the comparison.\n"
            }
        });
    }
    s
}

// ── Running the comparison ──────────────────────────────────────────────────

/// Lines that say a leg never reached the page: a browser that didn't start,
/// a host that didn't answer, a runner that found nothing to run.
const NOT_RUN: &[&str] = &[
    "executable doesn't exist",
    "browsertype.launch",
    "host system is missing dependencies",
    "err_name_not_resolved",
    "enotfound",
    "getaddrinfo",
    "econnrefused",
    "err_connection_refused",
    "no tests found",
    "command not found",
    "cannot find module",
    "err_internet_disconnected",
];

/// One side's result from its output.
pub fn leg_of(target: &str, output: &str) -> Leg {
    let low = output.to_ascii_lowercase();
    if let Some(p) = NOT_RUN.iter().find(|p| low.contains(*p)) {
        let line = output
            .lines()
            .find(|l| l.to_ascii_lowercase().contains(p))
            .unwrap_or(p)
            .trim();
        return Leg {
            target: target.to_string(),
            ran: false,
            note: crate::truncate_str(line, 200).to_string(),
            ..Default::default()
        };
    }
    let mut leg = Leg {
        target: target.to_string(),
        ..Default::default()
    };
    for l in output.lines() {
        let mut w = l.split_whitespace();
        let (Some(n), Some(what)) = (w.next(), w.next()) else {
            continue;
        };
        let Ok(n) = n.parse::<u32>() else { continue };
        match what {
            "passed" | "flaky" => {
                leg.passed = n;
                leg.ran = true;
            }
            "failed" => {
                leg.failed = n;
                leg.ran = true;
            }
            _ => {}
        }
    }
    if !leg.ran {
        leg.note = "the run printed no count".into();
    } else if let Some(e) = output.lines().map(str::trim).find(|l| {
        let low = l.to_ascii_lowercase();
        low.starts_with("error:") || low.starts_with("expect(")
    }) {
        leg.note = crate::truncate_str(e, 200).to_string();
    }
    leg
}

fn quote(s: &str) -> Option<String> {
    (!s.is_empty() && !s.contains('\'') && !s.contains('\n') && !s.starts_with('-'))
        .then(|| format!("'{s}'"))
}

/// A test title as a `--grep` pattern: its regex characters escaped.
fn grep_of(title: &str) -> String {
    let mut out = String::new();
    for c in title.chars() {
        if "\\^$.|?*+()[]{}".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// The command that runs one Playwright test (`spec` relative to the
/// package folder `dir`, `title` its name) once against `target`.
pub fn leg_cmd(dir: &str, spec: &str, title: &str, target: &str) -> Option<String> {
    let host = target
        .strip_prefix("https://")?
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("");
    if host.is_empty()
        || target.contains("{pr}")
        || host
            .chars()
            .all(|c| c.is_ascii_digit() || c == '.' || c == ':')
    {
        return None;
    }
    crate::e2e::report_source::check_target(target).ok()?;
    let dir = if dir.is_empty() { "." } else { dir };
    let dir = quote(dir)?;
    let spec = quote(spec)?;
    let t = quote(target)?;
    let grep = if title.trim().is_empty() {
        String::new()
    } else {
        format!(" --grep {}", quote(&grep_of(title.trim()))?)
    };
    Some(format!(
        "cd {dir} && {install}; \
         BASE_URL={t} PLAYWRIGHT_BASE_URL={t} PLAYWRIGHT_TEST_BASE_URL={t} E2E_BASE_URL={t} \
         npx --no-install playwright test {spec}{grep} --retries=0 --workers=1 --reporter=line",
        install = crate::e2e::test_package::BROWSER_INSTALL
    ))
}

/// One script that runs each side in turn and prints its output between
/// markers.
pub fn script(legs: &[(&str, String)]) -> String {
    let mut s = String::new();
    for (side, cmd) in legs {
        s.push_str(&format!(
            "echo \"{MARK} start {side}\"; ( {cmd} ) > /tmp/ch-repro-{side}.log 2>&1; echo \"{MARK} exit {side} $?\"; tail -c 3000 /tmp/ch-repro-{side}.log; echo; echo \"{MARK} end {side}\"\n"
        ));
    }
    s.push_str("true\n");
    s
}

/// Each side's output from the script's.
pub fn split(output: &str, side: &str) -> Option<String> {
    let start = format!("{MARK} start {side}");
    let end = format!("{MARK} end {side}");
    let a = output.find(&start)? + start.len();
    let b = output[a..]
        .find(&end)
        .map(|i| a + i)
        .unwrap_or(output.len());
    Some(output[a..b].to_string())
}

/// The spec file and the title of a failing test named `file › title`.
pub fn spec_and_title(test: &str) -> (String, String) {
    match test.split_once(" › ") {
        Some((f, t)) => (f.trim().to_string(), t.trim().to_string()),
        None => (test.trim().to_string(), String::new()),
    }
}

/// The base branch's deployment for the comparison: a configured test
/// address with no `{pr}` in it, else the first staging address the
/// repository's end-to-end setup names (`texts`: path and content).
pub fn base_target(configured: Option<&str>, texts: &[(String, String)]) -> Option<String> {
    if let Some(c) = configured
        .map(str::trim)
        .filter(|c| !c.is_empty() && !c.contains("{pr}"))
    {
        return Some(c.to_string());
    }
    for (_, text) in texts {
        for (i, _) in text.match_indices("https://") {
            let rest = &text[i + 8..];
            let host: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '-')
                .collect();
            let h = host.to_ascii_lowercase();
            let staging = h
                .split(['.', '-'])
                .any(|p| matches!(p, "staging" | "stage" | "stg"));
            if staging && h.contains('.') && !h.contains("{pr}") {
                return Some(format!("https://{host}"));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leg(ran: bool, passed: u32, failed: u32) -> Leg {
        Leg {
            target: "https://x.example.com".into(),
            ran,
            passed,
            failed,
            note: String::new(),
        }
    }

    fn rec(base: Leg, change: Leg) -> Record {
        Record {
            head: "abc1234def".into(),
            check: "E2E".into(),
            tests: vec!["tests/a.spec.ts › adds it to the cart".into()],
            base,
            change,
            source: "sandbox".into(),
            at: String::new(),
        }
    }

    #[test]
    fn the_verdict_comes_from_both_sides() {
        assert!(matches!(
            rec(leg(true, 0, 1), leg(true, 0, 1)).verdict(),
            Verdict::NotThisChange(_)
        ));
        assert!(matches!(
            rec(leg(true, 1, 0), leg(true, 0, 1)).verdict(),
            Verdict::ThisChange(_)
        ));
        assert!(matches!(
            rec(leg(false, 0, 0), leg(true, 0, 1)).verdict(),
            Verdict::Unproven(_)
        ));
        assert!(matches!(
            rec(leg(true, 1, 0), leg(true, 1, 0)).verdict(),
            Verdict::Unproven(_)
        ));
    }

    #[test]
    fn a_revert_needs_this_change_on_this_head() {
        let ours = vec![rec(leg(true, 1, 0), leg(true, 0, 1))];
        assert!(matches!(
            may_revert(&ours, "abc1234"),
            RevertGate::Allowed(_)
        ));
        // Another head's record proves nothing about this one.
        assert!(matches!(
            may_revert(&ours, "fff0000"),
            RevertGate::Refused(_)
        ));
        let base_red = vec![rec(leg(true, 0, 1), leg(true, 0, 1))];
        match may_revert(&base_red, "abc1234") {
            RevertGate::Refused(w) => assert!(w.contains("fails on the base branch too"), "{w}"),
            g => panic!("{g:?}"),
        }
        assert!(matches!(may_revert(&[], "abc1234"), RevertGate::Refused(_)));
    }

    #[test]
    fn blame_needs_a_record_and_a_count_is_evidence() {
        let text = "The fix is in.\n- `summer camp pricing` is broken on main since #12. Nothing else changed.";
        let (held, changed) = hold_blame(text, &[], "abc1234");
        assert!(changed);
        assert!(!held.contains("broken on main"), "{held}");
        assert!(held.contains(BLAME_HELD), "{held}");
        assert!(held.contains("The fix is in."));
        let base_red = vec![rec(leg(true, 0, 1), leg(true, 0, 1))];
        assert_eq!(
            hold_blame(text, &base_red, "abc1234"),
            (text.to_string(), false)
        );
        assert!(!blames(
            "Not flaky: the test failed every run (0/40 passed on the base branch)."
        ));
        assert!(!blames("It failed 0/640 runs on main, so it is not flaky."));
        assert!(blames("This looks flaky."));
        assert!(!blames("Fixes the flaky `timeofday` test."));
        assert!(!blames("### Why the flaky test failed"));
    }

    #[test]
    fn read_evidence_is_never_unconfirmed() {
        let text = "I couldn't confirm why the e2e test fails. The link is gone.";
        let (held, changed) = hold_unread(
            text,
            Some("`E2E` failed in `a.spec.ts › adds it`: Subtotal$0"),
        );
        assert!(changed);
        assert!(
            held.starts_with("The failure's report says: `E2E` failed"),
            "{held}"
        );
        assert!(held.contains("The link is gone."));
        assert!(!hold_unread(text, None).1);
        assert!(!says_unread("Unconfirmed: the staging page's colour."));
    }

    #[test]
    fn a_leg_that_never_reached_the_page_did_not_run() {
        let out = "Running 1 test\n  1) [chromium] › a.spec.ts › x\n    Error: browserType.launch: Executable doesn't exist at /root/.cache\n  1 failed\n";
        let l = leg_of("https://staging.example.com", out);
        assert!(!l.ran, "{l:?}");
        let ok = leg_of(
            "https://staging.example.com",
            "Running 1 test\n  1 passed (4.1s)\n",
        );
        assert!(ok.green());
        let red = leg_of(
            "https://staging.example.com",
            "  1) a.spec.ts › x\n    Error: expect(locator).not.toHaveText(expected) failed\n  1 failed\n",
        );
        assert!(red.red());
        assert!(red.note.starts_with("Error: expect"), "{red:?}");
    }

    #[test]
    fn the_comparison_command_and_targets() {
        let c = leg_cmd(
            "e2e",
            "tests/browse/camp.spec.ts",
            "adds it (AM)",
            "https://staging.example.com",
        )
        .unwrap();
        assert!(c.contains("--grep 'adds it \\(AM\\)'"), "{c}");
        assert!(c.contains("E2E_BASE_URL='https://staging.example.com'"));
        assert_eq!(leg_cmd("e2e", "a.spec.ts", "x", "http://10.0.0.1"), None);
        let texts = vec![(
            "e2e/playwright.config.ts".to_string(),
            "baseURL: process.env.E2E_BASE_URL ?? 'https://staging-shop.example.com/'".to_string(),
        )];
        assert_eq!(
            base_target(Some("https://pr-{pr}.preview.example.com"), &texts).as_deref(),
            Some("https://staging-shop.example.com")
        );
        assert_eq!(
            base_target(Some("https://qa.example.com"), &texts).as_deref(),
            Some("https://qa.example.com")
        );
        let s = script(&[("base", "true".into()), ("change", "true".into())]);
        let out = format!("{MARK} start base\n1 failed\n{MARK} end base\n{MARK} start change\n1 passed\n{MARK} end change\n");
        assert!(s.contains("ch-repro-base.log"));
        assert_eq!(split(&out, "base").unwrap().trim(), "1 failed");
        assert_eq!(split(&out, "change").unwrap().trim(), "1 passed");
    }

    #[test]
    fn screenshots_are_found_in_the_failure_comment() {
        let c = "## E2E failed\n![failure](https://github.com/user-attachments/assets/abc.png)\n<img src=\"https://reports.example.com/pr-1/data/shot.jpeg\">\n[report](https://reports.example.com/pr-1/index.html)";
        assert_eq!(
            shots_in(c),
            vec![
                "https://github.com/user-attachments/assets/abc.png".to_string(),
                "https://reports.example.com/pr-1/data/shot.jpeg".to_string()
            ]
        );
    }

    #[test]
    fn a_test_red_on_the_base_branch_history_is_not_this_changes() {
        let r = from_history(
            "abc1234def",
            "Deploy Preview",
            "Drop-ins › filter",
            "main",
            ("fff9999aaa", true, "2026-11-23T17:30:00Z"),
        );
        assert!(matches!(r.verdict(), Verdict::NotThisChange(_)), "{r:?}");
        assert!(r.line().contains("the test's own runs on the base branch"));
        assert!(r.base.target.starts_with("tests@main@fff9999"));
        let green = from_history("abc1234", "E", "t", "main", ("fff", false, ""));
        assert!(matches!(green.verdict(), Verdict::ThisChange(_)));
    }

    #[test]
    fn the_check_record_reads_like_the_sandbox_one() {
        let r = from_checks("abc1234", "fff9999", "Test Suite", Some(false));
        assert!(matches!(r.verdict(), Verdict::ThisChange(_)));
        let r = from_checks("abc1234", "fff9999", "Deploy Preview", None);
        assert!(matches!(r.verdict(), Verdict::Unproven(_)));
        let all = upsert(
            std::slice::from_ref(&r),
            from_checks("abc1234", "fff9999", "Deploy Preview", Some(true)),
        );
        assert_eq!(all.len(), 1);
        assert!(matches!(all[0].verdict(), Verdict::NotThisChange(_)));
        assert_eq!(from_json(&to_json(&all)), all);
    }
}
