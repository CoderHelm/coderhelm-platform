//! Which existing files a round may edit, decided by what the round is for.
//!
//! The failure this guard was written for: a repository's CI log shows lint
//! warnings in files the pull request never touched, and a fix round "cleans
//! them up" to get CI green, so unrelated files churn into a feature pull
//! request.
//!
//! Its first form held every fix round to "the files the pull request
//! already changes", and let an edit through after two refusals of the same
//! path. Both halves were wrong. A round whose whole job is work the pull
//! request has not done yet (a proof gap, a planned task still not done, a
//! dependent the plan updates, a finding that names another file) was
//! refused the very file it was started for; and a refusal that gives way
//! to repetition is no rule at all: a drive-by edit got through on the
//! third try, a needed one cost two wasted calls first.
//!
//! Now:
//!
//! - **The scope comes from the round's purpose** ([`RoundFor`]). A bounded
//!   round may edit the files the pull request changes, the plan's task
//!   files (done or not) and the dependents the plan lists, and the files
//!   named by what the round was started for: the gap, the finding, the
//!   failing check's output ([`named_files`]). The first build and a
//!   person's ask have no file boundary.
//! - **Anything else is refused, every time** ([`ScopeGuard::check`]). The
//!   one message says how to justify the edit: the same call again with
//!   `outside_plan` set to why the change needs the file. The count of
//!   tries changes nothing.
//! - **A justified edit is made and recorded.** The reason is kept as the
//!   habits check's evidence line (`Outside the plan: <path>: <why>`,
//!   [`ScopeGuard::evidence_lines`]) and reaches the pull request's
//!   "Changed outside the plan's files" list.
//! - **What stayed refused is the round's to report**
//!   ([`ScopeGuard::refused`]): a round that ends with a refusal standing
//!   and nothing closed made no progress, whatever else it pushed
//!   (`supervisor::progressed`).
//!
//! Creating a new file is never refused here (a needed new module is not
//! repository debt). Generated files and lockfiles keep their own refusals.

use std::collections::HashSet;
use std::sync::Mutex;

use super::plan::PlanResult;

/// Compare-API responses cap at 300 files; near the cap the list may be
/// incomplete, so callers fall back to unscoped rather than false-block.
pub const MAX_TRUSTED_SCOPE_FILES: usize = 290;

/// The fewest words a reason for an edit outside the round's files has: a
/// sentence, never a bare "needed".
pub const MIN_REASON_WORDS: usize = 3;

/// The tool input that carries the reason for an edit outside the round's
/// files (`write_file`, `edit_file`, `restore_file`, each `batch_write`
/// file).
pub const REASON_INPUT: &str = "outside_plan";

/// What a code-writing round is for. It decides which existing files the
/// round may edit and how the files its instructions name are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundFor {
    /// The build of the plan (the first pass, a continuation, a dependency
    /// or generator run): any file.
    Build,
    /// A person asked (a review, a comment): their ask is the
    /// authorization, any file.
    PersonAsk,
    /// A bug the failing-test rules moved into the app: where it lives is
    /// what the round finds out, so no file list can bound it.
    AppBug,
    /// What the new-feature rules found missing: registrations and test
    /// maps live outside the diff, in files the round reads its way to.
    FeatureRules,
    /// Gaps the proof check found, planned work not done among them.
    ProofGap,
    /// Planned tasks still not done (the plan-coverage round before the
    /// pull request, the planned-work rounds on it).
    PlannedWork,
    /// The tests and scope check: a test that proves nothing, a dependent
    /// the plan updates that the branch doesn't touch, a file outside the
    /// plan with no reason.
    Habits,
    /// CoderHelm's own review findings.
    OwnReview,
    /// Failing checks on the pull request, or in the sandbox before it.
    CiFix,
    /// Problems the test pass found in the change.
    TestFix,
    /// Issues the security check found.
    SecurityFix,
    /// Broken links and redirects.
    LinkFix,
    /// A check an open item waits on that fails in the sandbox.
    Settle,
}

/// How the files a round's instructions name are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reads {
    /// Findings written for the round: every path they name.
    Findings,
    /// A check's raw output: the paths on its failing lines, never the ones
    /// it only warns about.
    CheckOutput,
}

impl RoundFor {
    /// Whether the round has a file boundary at all.
    pub fn bounded(self) -> bool {
        !matches!(
            self,
            RoundFor::Build | RoundFor::PersonAsk | RoundFor::AppBug | RoundFor::FeatureRules
        )
    }

    /// How the round's instructions are read for the files they name.
    pub fn reads(self) -> Reads {
        match self {
            RoundFor::CiFix | RoundFor::Settle => Reads::CheckOutput,
            _ => Reads::Findings,
        }
    }

    /// What the round was started for, in the refusal's words.
    pub fn what(self) -> &'static str {
        match self {
            RoundFor::Build => "the plan",
            RoundFor::PersonAsk => "a person's request",
            RoundFor::AppBug => "the bug the tests showed",
            RoundFor::FeatureRules => "the new-feature rules",
            RoundFor::ProofGap => "the gaps the proof check listed",
            RoundFor::PlannedWork => "the planned tasks still not done",
            RoundFor::Habits => "the items the tests and scope check listed",
            RoundFor::OwnReview => "the review findings",
            RoundFor::CiFix => "the failing check's output",
            RoundFor::TestFix => "the test problems",
            RoundFor::SecurityFix => "the security issues",
            RoundFor::LinkFix => "the link problems",
            RoundFor::Settle => "the failing check's output",
        }
    }

    /// A round that is `bounded` to `self` when a bot started it, and has no
    /// boundary when a person's ask rides along.
    pub fn unless_person(self, person_asked: bool) -> RoundFor {
        if person_asked {
            RoundFor::PersonAsk
        } else {
            self
        }
    }

    /// The purpose of a round on the pull request, from its record: its
    /// kind, and whether a person's ask rode along (`scoped` false).
    pub fn of_round(kind: common::run_round::Kind, scoped: bool) -> RoundFor {
        use common::run_round::Kind;
        match kind {
            // Planned work is bounded by the plan whoever recorded it.
            Kind::PlannedWork => RoundFor::PlannedWork,
            _ if !scoped => RoundFor::PersonAsk,
            Kind::ReviewFix => RoundFor::OwnReview,
            Kind::SecurityFix => RoundFor::SecurityFix,
            Kind::CiFix | Kind::Feedback | Kind::Conflict => RoundFor::CiFix,
        }
    }
}

/// Why a path is inside a round's files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// The pull request already changes it.
    Pr,
    /// A task of the plan names it (or its directory), or the plan says it
    /// is updated with the change.
    Plan,
    /// The plan lists it as a dependent of the change.
    Dependent,
    /// What the round was started for names it.
    Named,
}

/// The files a bounded round may edit, each with why.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scope {
    pr: HashSet<String>,
    plan: HashSet<String>,
    /// Directories a task names (`src/blocks/`): everything under them.
    plan_dirs: Vec<String>,
    dependents: HashSet<String>,
    named: Vec<String>,
}

/// Tool-supplied paths in the repository-relative form the compare API
/// uses (`apps/web/src/x.ts`: no leading `./` or `/`, no backticks).
fn normalize(path: &str) -> &str {
    path.trim()
        .trim_matches('`')
        .trim_start_matches("./")
        .trim_start_matches('/')
}

/// Whether a path a text names is `path`: the same, or one the end of the
/// other at a directory boundary (a log prints absolute or package-relative
/// paths).
fn names(named: &str, path: &str) -> bool {
    named == path || named.ends_with(&format!("/{path}")) || path.ends_with(&format!("/{named}"))
}

impl Scope {
    /// The scope of a round: the pull request's files, the plan's, and the
    /// ones `reason` (the round's instructions) names.
    pub fn of(
        round: RoundFor,
        pr_files: &[String],
        plan: Option<&PlanResult>,
        own_repo: &str,
        reason: &str,
    ) -> Scope {
        let mut s = Scope {
            pr: pr_files.iter().map(|f| normalize(f).to_string()).collect(),
            named: named_files(reason, round.reads()),
            ..Default::default()
        };
        let Some(plan) = plan else {
            return s;
        };
        let own = plan
            .repo_tasks
            .iter()
            .find(|r| format!("{}/{}", r.owner, r.name).eq_ignore_ascii_case(own_repo))
            .map(|r| r.tasks.as_str());
        for tasks in std::iter::once(plan.tasks.as_str()).chain(own) {
            for f in super::feature::plan_files::planned_files(tasks) {
                let p = normalize(&f.path).to_string();
                if p.ends_with('/') {
                    if !s.plan_dirs.contains(&p) {
                        s.plan_dirs.push(p);
                    }
                } else if !p.is_empty() {
                    s.plan.insert(p);
                }
            }
        }
        if let Some(d) = plan.fix.as_ref() {
            for a in &d.habits.affected {
                if a.repo().is_some_and(|r| !r.eq_ignore_ascii_case(own_repo)) {
                    continue;
                }
                let p = normalize(a.path()).to_string();
                if p.is_empty() {
                    continue;
                }
                if a.updates() {
                    s.plan.insert(p);
                } else {
                    s.dependents.insert(p);
                }
            }
        }
        s
    }

    /// Why `path` is one of the round's files; `None` when it is not.
    pub fn source(&self, path: &str) -> Option<Source> {
        let p = normalize(path);
        if self.pr.contains(p) {
            Some(Source::Pr)
        } else if self.plan.contains(p) || self.plan_dirs.iter().any(|d| p.starts_with(d.as_str()))
        {
            Some(Source::Plan)
        } else if self.dependents.contains(p) {
            Some(Source::Dependent)
        } else if self.named.iter().any(|n| names(n, p)) {
            Some(Source::Named)
        } else {
            None
        }
    }
}

/// Whether a token reads as a file path: no spaces, a file name with an
/// extension, not a link or a flag. A bare file name (`package.json`) only
/// counts where the text quoted it (`quoted`).
fn path_like(token: &str, quoted: bool) -> bool {
    if token.is_empty()
        || token.len() > 300
        || token.contains(char::is_whitespace)
        || token.contains("://")
        || token.starts_with('-')
        || token.starts_with('@')
        || token.ends_with('/')
    {
        return false;
    }
    let name = token.rsplit('/').next().unwrap_or(token);
    let Some((stem, ext)) = name.rsplit_once('.') else {
        return false;
    };
    let ext_ok = !ext.is_empty()
        && ext.len() <= 8
        && ext.chars().all(|c| c.is_ascii_alphanumeric())
        && ext.chars().any(|c| c.is_ascii_alphabetic());
    ext_ok && !stem.is_empty() && (quoted || token.contains('/'))
}

/// A token as a log or a sentence prints a path, down to the path: quotes
/// and brackets around it, and the position after it (`:12:5`, `(12,5)`).
fn bare_path(token: &str) -> &str {
    let mut t = token.trim_matches(|c: char| "\"'`,;<>*".contains(c));
    t = t.trim_end_matches(['.', ':', ')']);
    // `src/a.ts(12,5)`: the position, never a part of a route folder name
    // (`app/(shop)/page.tsx` has no digit after its bracket).
    if let Some(i) = t.find('(') {
        if t[i + 1..].starts_with(|c: char| c.is_ascii_digit()) {
            t = &t[..i];
        }
    }
    // `src/a.ts:12:5`
    while let Some((head, tail)) = t.rsplit_once(':') {
        if !tail.is_empty() && tail.chars().all(|c| c.is_ascii_digit()) {
            t = head;
        } else {
            break;
        }
    }
    let t = t.trim_start_matches('(');
    normalize(t)
}

/// The paths one line names: every quoted span that reads as a path, and
/// every other word that does.
fn paths_in(line: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |p: &str| {
        if !out.iter().any(|o| o == p) {
            out.push(p.to_string());
        }
    };
    for (i, span) in line.split('`').enumerate() {
        if i % 2 == 1 {
            let p = bare_path(span.trim());
            if path_like(p, true) {
                push(p);
            }
        } else {
            for word in span.split_whitespace() {
                let p = bare_path(word);
                if path_like(p, false) {
                    push(p);
                }
            }
        }
    }
    out
}

/// Whether a line of a check's output only warns: it says "warn" and
/// neither "error" nor "fail".
fn only_warns(line: &str) -> bool {
    let l = line.to_ascii_lowercase();
    l.contains("warn") && !(l.contains("error") || l.contains("fail"))
}

/// The files a round's instructions name. Findings name the files they are
/// about. A check's output names many more: a linter prints every warning
/// it has, in files the change never touched, and those are exactly the
/// drive-by edits this guard exists to stop, so only the files on lines
/// that fail count (and a file heading counts when an entry under it
/// fails, or when it has none).
pub fn named_files(text: &str, reads: Reads) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let add = |paths: Vec<String>, out: &mut Vec<String>| {
        for p in paths {
            if !out.contains(&p) {
                out.push(p);
            }
        }
    };
    if reads == Reads::Findings {
        for line in text.lines() {
            add(paths_in(line), &mut out);
        }
        return out;
    }
    // A heading: a line that is one path and nothing else, with its
    // entries indented under it (a linter's report per file).
    struct Heading {
        path: String,
        entries: bool,
        failing: bool,
    }
    let mut heading: Option<Heading> = None;
    let close = |h: Option<Heading>, out: &mut Vec<String>| {
        if let Some(h) = h {
            if (!h.entries || h.failing) && !out.contains(&h.path) {
                out.push(h.path);
            }
        }
    };
    for line in text.lines() {
        let t = line.trim();
        let paths = paths_in(line);
        if paths.len() == 1 && !t.contains(char::is_whitespace) {
            close(heading.take(), &mut out);
            heading = Some(Heading {
                path: paths[0].clone(),
                entries: false,
                failing: false,
            });
            continue;
        }
        let indented = line.starts_with([' ', '\t']);
        match heading.as_mut() {
            Some(h) if !t.is_empty() && indented => {
                h.entries = true;
                h.failing |= !only_warns(line);
            }
            Some(_) => close(heading.take(), &mut out),
            None => {}
        }
        if !only_warns(line) {
            add(paths, &mut out);
        }
    }
    close(heading.take(), &mut out);
    out
}

/// The reason given for an edit outside the round's files, when it is one:
/// a sentence of its own, not empty, not the path again.
pub fn stated_reason<'a>(why: Option<&'a str>, path: &str) -> Option<&'a str> {
    let why = why?.trim();
    let words = why.split_whitespace().count();
    (words >= MIN_REASON_WORDS && normalize(why) != normalize(path)).then_some(why)
}

/// The reason a tool call carries for an edit outside the round's files.
pub fn reason_input(input: &serde_json::Value) -> Option<&str> {
    input.get(REASON_INPUT).and_then(|v| v.as_str())
}

/// The `outside_plan` property of a write tool's input schema.
pub fn reason_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "string",
        "description": "Only after an edit of this file was refused as outside this round's files, and only when the work this round was started for needs the file: one sentence saying why the change needs it. It is recorded on the pull request as `Outside the plan: <path>: <why>`."
    })
}

/// Adds the reason input to a write tool's schema (each file's entry for
/// `batch_write`). Only a bounded round's tools carry it.
pub fn offer_reason(tool: &str, schema: &mut serde_json::Value) {
    let props = if tool == "batch_write" {
        &mut schema["properties"]["files"]["items"]["properties"]
    } else {
        &mut schema["properties"]
    };
    if let Some(p) = props.as_object_mut() {
        p.insert(REASON_INPUT.to_string(), reason_schema());
    }
}

/// An edit the guard refused that was never made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub path: String,
    /// Why, in a few words, for the run page and the next round.
    pub why: String,
}

/// Why a protected CI file can't be edited, for [`ScopeGuard::refuse`].
pub const PROTECTED_WHY: &str =
    "CI/CD workflow files are protected: CoderHelm has no permission to change them";

pub struct ScopeGuard {
    /// None = no file boundary (the first build, a person's ask, or the
    /// changed-file listing was unavailable or untrustworthy: fail open).
    scope: Option<(RoundFor, Scope)>,
    /// Paths granted mid-pass: files the round itself created, files
    /// `run_codegen` regenerated and committed, files it gave a reason for.
    allowed: Mutex<HashSet<String>>,
    /// Edits outside the plan's files that were made, with why.
    outside: Mutex<Vec<(String, String)>>,
    /// Edits refused and not made since.
    refused: Mutex<Vec<Refusal>>,
}

impl ScopeGuard {
    pub fn unscoped() -> Self {
        Self {
            scope: None,
            allowed: Mutex::new(HashSet::new()),
            outside: Mutex::new(Vec::new()),
            refused: Mutex::new(Vec::new()),
        }
    }

    /// The guard of a round. Without a boundary when the round has none
    /// ([`RoundFor::bounded`]), when the pull request changes no file yet
    /// (a fresh or reset branch: nothing to anchor a scope to) or when the
    /// list is too large to trust (the compare API's cap).
    pub fn for_round(
        round: RoundFor,
        pr_files: &[String],
        plan: Option<&PlanResult>,
        own_repo: &str,
        reason: &str,
    ) -> Self {
        if !round.bounded() || pr_files.is_empty() || pr_files.len() >= MAX_TRUSTED_SCOPE_FILES {
            return Self::unscoped();
        }
        Self {
            scope: Some((round, Scope::of(round, pr_files, plan, own_repo, reason))),
            ..Self::unscoped()
        }
    }

    /// Whether the round has a file boundary.
    pub fn bounded(&self) -> bool {
        self.scope.is_some()
    }

    /// Grant a path mid-pass: a file the round just created, or one
    /// `run_codegen` just regenerated and committed, is part of the pull
    /// request now.
    pub fn allow(&self, path: &str) {
        let norm = normalize(path);
        self.allowed.lock().unwrap().insert(norm.to_string());
        self.refused.lock().unwrap().retain(|r| r.path != norm);
    }

    fn record_outside(&self, path: &str, why: &str) {
        let mut outside = self.outside.lock().unwrap();
        if !outside.iter().any(|(p, _)| p == path) {
            outside.push((path.to_string(), why.to_string()));
        }
    }

    /// Whether the round may edit the existing file `path`. `why` is the
    /// reason the call carries ([`REASON_INPUT`]). `Err` is the refusal the
    /// model reads. Callers apply this to edits of existing files only:
    /// creating a file is never refused.
    ///
    /// The answer for a path never depends on how often it was asked: a
    /// file outside the round's files is refused until a reason is given,
    /// and edited from then on.
    pub fn check(&self, path: &str, why: Option<&str>) -> Result<(), String> {
        let Some((round, scope)) = &self.scope else {
            return Ok(());
        };
        let norm = normalize(path);
        if self.allowed.lock().unwrap().contains(norm) {
            return Ok(());
        }
        match scope.source(norm) {
            Some(Source::Pr | Source::Plan) => Ok(()),
            // Inside the round's files but no task of the plan: the edit is
            // made and the pull request says why this file changed.
            Some(Source::Dependent) => {
                self.record_outside(
                    norm,
                    &format!(
                        "the plan lists it as a dependent of this change; edited for {}",
                        round.what()
                    ),
                );
                Ok(())
            }
            Some(Source::Named) => {
                self.record_outside(norm, &format!("named by {}", round.what()));
                Ok(())
            }
            None => match stated_reason(why, norm) {
                Some(why) => {
                    self.record_outside(norm, why);
                    self.allowed.lock().unwrap().insert(norm.to_string());
                    self.refused.lock().unwrap().retain(|r| r.path != norm);
                    Ok(())
                }
                None => {
                    self.refuse(norm, &format!("outside the files of {}", round.what()));
                    Err(Self::refusal(path, *round))
                }
            },
        }
    }

    /// Records an edit refused for a reason no retry changes (a protected
    /// CI file), so the round reports it.
    pub fn refuse(&self, path: &str, why: &str) {
        let norm = normalize(path);
        let mut refused = self.refused.lock().unwrap();
        if !refused.iter().any(|r| r.path == norm) {
            refused.push(Refusal {
                path: norm.to_string(),
                why: why.to_string(),
            });
        }
    }

    /// The refusal the model reads: what the round's files are, and the one
    /// way to edit a file outside them.
    pub fn refusal(path: &str, round: RoundFor) -> String {
        format!(
            "NOT EDITED: {path} is outside this round's files. This round may edit the files \
             this pull request changes, the plan's task files and the dependents it lists, and \
             the files named by {what}; creating a new file is always allowed. If {what} cannot \
             be settled without changing {path}, send the same call again with `{REASON_INPUT}` \
             set to one sentence saying why the change needs this file: the edit is then made, \
             and the reason is recorded on the pull request as `Outside the plan: {path}: \
             <why>`. If the file only has an issue that was there before this change (a lint or \
             style warning, an unused import, a cleanup), leave it alone and name it in your \
             summary. The same call without `{REASON_INPUT}` is refused again, however often it \
             is sent.",
            what = round.what()
        )
    }

    /// The edits outside the plan's files that were made, with why.
    pub fn outside(&self) -> Vec<(String, String)> {
        self.outside.lock().unwrap().clone()
    }

    /// The edits refused and never made.
    pub fn refused(&self) -> Vec<Refusal> {
        self.refused.lock().unwrap().clone()
    }

    /// The reasons as the habits check reads them from the coder's notes
    /// (`habits_check::reasons`), one line each; empty when there are none.
    pub fn evidence_lines(outside: &[(String, String)]) -> String {
        outside
            .iter()
            .map(|(p, why)| format!("- Outside the plan: `{p}`: {why}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// What a round is told about the edits its last try was refused, once:
/// which files, why, and the two ways on. Empty when nothing was refused.
pub fn refused_note(refused: &[Refusal]) -> String {
    if refused.is_empty() {
        return String::new();
    }
    let lines: Vec<String> = refused
        .iter()
        .map(|r| format!("- `{}`: {}", r.path, r.why))
        .collect();
    format!(
        "\n\n## Edits the last round was refused\n{}\nA file outside the round's files is edited \
         by sending the call with `{REASON_INPUT}` set to one sentence saying why the work needs \
         it. A protected file is never edited: do the work without it, and say in your summary \
         what that file would need.",
        lines.join("\n")
    )
}

#[cfg(test)]
mod scope_guard_tests {
    use super::*;
    use crate::passes::fix_location::FixDecision;
    use crate::passes::habits::{Affected, Habits};

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn plan(tasks: &str, affected: &[(&str, &str)]) -> PlanResult {
        PlanResult {
            proposal: String::new(),
            tasks: tasks.into(),
            spec: String::new(),
            design: String::new(),
            repo_tasks: vec![],
            fix: Some(FixDecision {
                habits: Habits {
                    affected: affected
                        .iter()
                        .map(|(w, a)| Affected {
                            where_: w.to_string(),
                            action: a.to_string(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                ..Default::default()
            }),
        }
    }

    const TASKS: &str = "- [x] apps/web/src/routes/gift.tsx: [create] The gift route\n\
                         - [ ] apps/web/src/lib/metadata.ts: [modify] Page metadata for the gift route\n";

    const ROUNDS: [RoundFor; 13] = [
        RoundFor::Build,
        RoundFor::PersonAsk,
        RoundFor::AppBug,
        RoundFor::FeatureRules,
        RoundFor::ProofGap,
        RoundFor::PlannedWork,
        RoundFor::Habits,
        RoundFor::OwnReview,
        RoundFor::CiFix,
        RoundFor::TestFix,
        RoundFor::SecurityFix,
        RoundFor::LinkFix,
        RoundFor::Settle,
    ];

    #[test]
    fn each_kind_of_round_gets_the_scope_its_purpose_gives() {
        let pr = s(&["apps/web/src/routes/gift.tsx"]);
        let p = plan(
            TASKS,
            &[
                ("apps/web/src/nav/links.ts", "update"),
                ("apps/web/src/sitemap.ts", "safe"),
                ("acme/other:src/client.ts", "update"),
            ],
        );
        let reason = "- **Unproven assumption:** `apps/web/src/lib/seo.ts` builds the title.";
        for round in ROUNDS {
            let g = ScopeGuard::for_round(round, &pr, Some(&p), "acme/site", reason);
            assert_eq!(g.bounded(), round.bounded(), "{round:?}");
            // A file no source names.
            let stray = g.check("apps/web/src/unrelated/date.ts", None);
            assert_eq!(stray.is_err(), round.bounded(), "{round:?}");
            if !round.bounded() {
                continue;
            }
            // The pull request's files, the plan's task files (done or
            // not), the dependents it lists, and what the round names.
            for inside in [
                "apps/web/src/routes/gift.tsx",
                "./apps/web/src/routes/gift.tsx",
                "apps/web/src/lib/metadata.ts",
                "apps/web/src/nav/links.ts",
                "apps/web/src/sitemap.ts",
                "apps/web/src/lib/seo.ts",
            ] {
                assert!(g.check(inside, None).is_ok(), "{round:?} {inside}");
            }
            // Another repository's dependent is not this round's file.
            assert!(g.check("src/client.ts", None).is_err(), "{round:?}");
            assert!(stray.unwrap_err().contains(round.what()), "{round:?}");
        }
        // The rounds with no boundary are the build, a person's ask, and
        // the two whose files the round itself finds out.
        let open: Vec<RoundFor> = ROUNDS.into_iter().filter(|r| !r.bounded()).collect();
        assert_eq!(
            open,
            [
                RoundFor::Build,
                RoundFor::PersonAsk,
                RoundFor::AppBug,
                RoundFor::FeatureRules
            ]
        );
    }

    #[test]
    fn a_rounds_record_gives_its_purpose() {
        use common::run_round::Kind;
        assert_eq!(RoundFor::of_round(Kind::CiFix, true), RoundFor::CiFix);
        assert_eq!(RoundFor::of_round(Kind::Feedback, true), RoundFor::CiFix);
        assert_eq!(
            RoundFor::of_round(Kind::ReviewFix, true),
            RoundFor::OwnReview
        );
        assert_eq!(
            RoundFor::of_round(Kind::SecurityFix, true),
            RoundFor::SecurityFix
        );
        // A person's ask rode along: no boundary.
        for kind in [Kind::CiFix, Kind::ReviewFix, Kind::Feedback] {
            assert_eq!(RoundFor::of_round(kind, false), RoundFor::PersonAsk);
        }
        // Planned work is recorded "not scoped" and is bounded by the plan.
        assert_eq!(
            RoundFor::of_round(Kind::PlannedWork, false),
            RoundFor::PlannedWork
        );
        assert_eq!(RoundFor::CiFix.unless_person(true), RoundFor::PersonAsk);
        assert_eq!(RoundFor::CiFix.unless_person(false), RoundFor::CiFix);
    }

    #[test]
    fn a_file_the_round_needs_is_editable_at_the_first_try() {
        let pr = s(&["apps/web/src/routes/gift.tsx"]);
        // A planned task not done yet, in a file the branch doesn't touch.
        let g = ScopeGuard::for_round(
            RoundFor::ProofGap,
            &pr,
            Some(&plan(TASKS, &[])),
            "acme/site",
            "",
        );
        assert!(g.check("apps/web/src/lib/metadata.ts", None).is_ok());
        assert!(g.refused().is_empty());
        assert!(g.outside().is_empty(), "a task's file needs no reason");
        // A file under a directory a task names.
        let dir = plan("- [ ] apps/web/src/blocks/: [modify] The blocks\n", &[]);
        let g = ScopeGuard::for_round(RoundFor::PlannedWork, &pr, Some(&dir), "acme/site", "");
        assert!(g.check("apps/web/src/blocks/card.tsx", None).is_ok());
        // A dependent the plan updates: the habits check's own instruction.
        let dep = plan(TASKS, &[("apps/web/src/nav/links.ts", "update")]);
        let g = ScopeGuard::for_round(RoundFor::Habits, &pr, Some(&dep), "acme/site", "");
        assert!(g.check("apps/web/src/nav/links.ts", None).is_ok());
        // A finding that names another file, with no plan at hand.
        let g = ScopeGuard::for_round(
            RoundFor::OwnReview,
            &pr,
            None,
            "acme/site",
            "1. `apps/web/src/lib/price.ts:41` still rounds the old way.",
        );
        assert!(g.check("apps/web/src/lib/price.ts", None).is_ok());
        // Named, and no task of the plan: the pull request says why.
        assert_eq!(
            g.outside(),
            vec![(
                "apps/web/src/lib/price.ts".to_string(),
                "named by the review findings".to_string()
            )]
        );
    }

    #[test]
    fn a_drive_by_edit_is_refused_however_often_it_is_tried() {
        let g = ScopeGuard::for_round(
            RoundFor::CiFix,
            &s(&["apps/web/src/feature.tsx"]),
            None,
            "acme/site",
            "",
        );
        let first = g
            .check("apps/web/src/services/unrelated.ts", None)
            .unwrap_err();
        for _ in 0..25 {
            assert_eq!(
                g.check("apps/web/src/services/unrelated.ts", None)
                    .unwrap_err(),
                first,
                "the same refusal every time"
            );
        }
        // An empty or one-word reason, or the path again, is no reason.
        for why in ["", "   ", "needed", "apps/web/src/services/unrelated.ts"] {
            assert!(g
                .check("apps/web/src/services/unrelated.ts", Some(why))
                .is_err());
        }
        assert!(first.contains(REASON_INPUT));
        assert!(first.contains("Outside the plan: apps/web/src/services/unrelated.ts: <why>"));
        assert!(first.contains("refused again"));
        // One refusal standing, however often it was tried.
        assert_eq!(g.refused().len(), 1);
        assert_eq!(g.refused()[0].path, "apps/web/src/services/unrelated.ts");
        assert!(g.outside().is_empty());
    }

    #[test]
    fn a_justified_edit_is_made_and_recorded() {
        let g = ScopeGuard::for_round(
            RoundFor::ProofGap,
            &s(&["apps/web/src/routes/gift.tsx"]),
            None,
            "acme/site",
            "",
        );
        let path = "apps/web/src/lib/metadata.ts";
        assert!(g.check(path, None).is_err());
        let why = "the gift route's metadata is built by the function in this file";
        assert!(g.check(path, Some(why)).is_ok());
        // From then on the file is the round's: no reason asked again.
        assert!(g.check(path, None).is_ok());
        assert!(g.refused().is_empty(), "the refusal no longer stands");
        assert_eq!(g.outside(), vec![(path.to_string(), why.to_string())]);
        // The record is the line the habits check reads.
        let lines = ScopeGuard::evidence_lines(&g.outside());
        assert_eq!(
            crate::passes::habits_check::reasons(&lines),
            vec![(path.to_string(), why.to_string())]
        );
    }

    #[test]
    fn files_the_round_made_and_codegen_wrote_are_its_own() {
        let g = ScopeGuard::for_round(RoundFor::CiFix, &s(&["src/a.ts"]), None, "acme/site", "");
        g.allow("./src/new.ts");
        assert!(g.check("src/new.ts", None).is_ok());
        assert!(g.outside().is_empty());
    }

    #[test]
    fn no_boundary_without_a_trusted_list() {
        assert!(ScopeGuard::unscoped().check("anything.ts", None).is_ok());
        // A fresh or reset branch: nothing to anchor a scope to.
        let g = ScopeGuard::for_round(RoundFor::CiFix, &[], None, "acme/site", "");
        assert!(g.check("anything.ts", None).is_ok());
        // Near the compare cap.
        let big: Vec<String> = (0..MAX_TRUSTED_SCOPE_FILES)
            .map(|i| format!("f{i}.ts"))
            .collect();
        let g = ScopeGuard::for_round(RoundFor::CiFix, &big, None, "acme/site", "");
        assert!(g.check("anything.ts", None).is_ok());
    }

    #[test]
    fn findings_name_every_path_they_quote_or_print() {
        let named = named_files(
            "- **Unproven assumption:** `apps/web/app/[locale]/(shop)/page.tsx` assumes the \
             title from apps/web/src/lib/metadata.ts:88, see `package.json`. E.g. version 1.2 \
             of https://example.com/a.js and/or client/server.",
            Reads::Findings,
        );
        assert_eq!(
            named,
            s(&[
                "apps/web/app/[locale]/(shop)/page.tsx",
                "apps/web/src/lib/metadata.ts",
                "package.json",
            ])
        );
    }

    #[test]
    fn a_checks_output_names_the_files_that_fail_never_the_ones_it_warns_about() {
        let log = "\
/home/runner/work/site/site/apps/web/src/consumer.ts
  12:5  error  Property 'total' does not exist on type 'Cart'  ts(2339)

/home/runner/work/site/site/apps/web/src/legacy/old.ts
  3:1  warning  'React' is defined but never used  no-unused-vars
  9:7  warning  Unexpected any  no-explicit-any

src/pages/cart.tsx(41,9): error TS2322: Type 'string' is not assignable to type 'number'.
warning: unused variable in src/util/dates.rs:14:9
FAIL src/cart/total.test.ts
";
        let named = named_files(log, Reads::CheckOutput);
        assert_eq!(
            named,
            s(&[
                "home/runner/work/site/site/apps/web/src/consumer.ts",
                "src/pages/cart.tsx",
                "src/cart/total.test.ts",
            ])
        );
        let g = ScopeGuard::for_round(
            RoundFor::CiFix,
            &s(&["apps/web/src/cart.ts"]),
            None,
            "acme/site",
            log,
        );
        // The consumer the change broke is the round's file.
        assert!(g.check("apps/web/src/consumer.ts", None).is_ok());
        // The file with warnings that were always there is not.
        assert!(g.check("apps/web/src/legacy/old.ts", None).is_err());
        assert!(g.check("src/util/dates.rs", None).is_err());
    }

    #[test]
    fn a_bounded_rounds_write_tools_take_the_reason() {
        let mut one =
            serde_json::json!({"type": "object", "properties": {"path": {"type": "string"}}});
        offer_reason("edit_file", &mut one);
        assert!(one["properties"][REASON_INPUT]["description"]
            .as_str()
            .unwrap()
            .contains("Outside the plan"));
        let mut batch = serde_json::json!({"type": "object", "properties": {"files": {
            "type": "array", "items": {"type": "object", "properties": {"path": {"type": "string"}}}}}});
        offer_reason("batch_write", &mut batch);
        assert!(batch["properties"]["files"]["items"]["properties"][REASON_INPUT].is_object());
        assert!(batch["properties"].get(REASON_INPUT).is_none());
    }

    #[test]
    fn a_refused_edit_is_told_to_the_next_round_once() {
        assert_eq!(refused_note(&[]), "");
        let note = refused_note(&[Refusal {
            path: "apps/web/src/lib/metadata.ts".into(),
            why: "outside the files of the planned tasks still not done".into(),
        }]);
        assert!(note.contains("`apps/web/src/lib/metadata.ts`"));
        assert!(note.contains(REASON_INPUT));
    }
}
