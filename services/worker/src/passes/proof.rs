//! Before a pull request opens, the change must rest on what the run proved.
//! A change that relied on a vendor field nobody looked at, said "not
//! measured" while a connected source could count it, or logged every record
//! at INFO on every scheduled sync, shipped as a PR because nothing checked
//! these before it opened. This gate does: a judge reads the change, the plan,
//! the sources the team connected and every lookup the run made, and names
//! each gap. A gap sends the work back for one fix round with the finding;
//! what is still unproven after it is listed under Open items at the bottom
//! of the PR description.

use serde::Deserialize;
use tracing::{info, warn};

use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::checks::{classify_command, CheckKind, CiCommand};
use crate::passes::plan::PlanResult;
use crate::passes::FileCache;
use crate::WorkerState;

/// How many fix rounds a gap gets before the PR opens with it named.
pub const MAX_FIX_ROUNDS: u32 = 1;

/// What every change must do, whatever it touches. Each rule is a way a
/// change shipped resting on a guess.
pub const CODER_RULES: &str = "
## Prove what the change rests on (every change)
- Prove an assumption before you write code on it. When the fix relies on a field, value or behaviour of a system this code doesn't own (a vendor API, another service, a table another system writes), confirm it from the real thing first: one read through a connected data source (a live lookup, `api_get`, a database query, a search) or that system's own data in the warehouse. Read an actual payload or row; a type, a schema or a name in the code is not proof of what the field holds.
- Measure the impact when the data is in a connected source: count the records the change affects, with a query, and note a few concrete examples (ids or names).
- Logging: INFO only for actual changes and decisions; diagnostics go to DEBUG. Never add an INFO log that fires for every record or on every scheduled run.
- Never leave \"unconfirmed\", \"not verified\" or \"not measured\" for something a connected source could answer: look it up. When no connected source can reach it, say exactly which source is missing.
- A lookup that failed or came back empty (an empty list, no rows) confirms nothing about the system: it is not a payload. Find out why (a wrong path, a parent id from another record, a wrong table) and read again until a real record comes back.

## Reading another system's data (every change that does)
- Actual data first, always: before deciding how to read another system's records, read real ones (a payload, a row) for the ticket's records and a broad sample of others, and list every field on them. Decide from what you saw, never from names in the code, a schema or a guess.
- Key logic on the source system's structured fields and flags (booleans, type fields, numeric quantities, ids), never on parsing names, titles or descriptions, which people rename at any time. Pattern matching or a regex on text is the last resort, only when the real records have no structured field that says the same thing. Then say so in the `Evidence:` section with the fields you checked, test the pattern against every live value (counts of matches and non-matches), and log a WARNING for records it doesn't match.
- Survey a field across all live records, not only the ticket's. Group the records by shape, say what the field means in each group, and scope the rule with the source's own flag where meanings differ. Put the count per shape and one example id each in the `Evidence:` section.
- When a record doesn't match the expected shape, don't silently fall back: log a WARNING with the record id and add a count to the run's summary. The INFO/DEBUG rule above is for expected records; an unexpected shape is a WARNING.
- Tests: build fixtures from real payloads (trimmed, with the real field names and values), one per shape the survey found, and include a test that fails on the old code and passes on the new one.
- Existing bad records are corrected by the normal pipeline or job on its next run. No manual backfills or one-off scripts unless unavoidable; then list them in the verification steps.
- Fix the rule, never the one record: no hard-coded ids, names, seasons or prices.

## Copy the existing pattern, and say which (every change that adds a case)
- When the change adds something that already exists for another case (another user, secret, certificate, route, config entry, job, permission), find that block and copy it: same structure, same naming, same order. Change only what must differ for the new case.
- Name the block you copied and every intentional difference, with its reason, in the `mirrors.json` block of your closing notes (see below). A copy nobody can trace to its original is reviewed as new design.

## Every environment the change touches
- When the change edits more than one environment (`envs/*`, `staging` and `prod` directories, per-environment config files), confirm each one with a read-only lookup where a connected source can reach it (the resource, name or path exists as the change expects there): one `- Confirmed:` line per environment, naming it.
- When an environment can't be checked, write one `- Unconfirmed:` line naming that environment and exactly what access is missing. Never let one environment's lookup stand for another.
- Write real values everywhere: a path with its environment filled in (`/app/prod/db-password`), never a template left as `${env}`, `<env>`, `{}` or an empty segment (`/app//db-password`).

## Which environment a lookup hit (every change whose evidence spans environments)
- When the change or its evidence concerns more than one environment and a live data source is connected, prove which environment each lookup hit with data that only exists there (a record, a name, a setting the team's instructions or the source's guide name for that environment), and quote it on the `- Confirmed:` line.
- Never write that you didn't confirm which environment a lookup hit while a lookup could confirm it: make that lookup. Only when no connected source reaches it, write `- Unconfirmed:` naming the missing access.

## Something close to what the ticket names (every change)
- When a lookup or a search finds an existing object close to what the ticket names (a similar name, the same purpose: a schema, user, role, table, bucket, queue, job, secret), follow it to the end before writing around it: who owns it, what grants or permissions it has, what is in it (look them up), and what created it (search the team's repositories for its name).
- Put those findings in the `Evidence:` section, with ONE decisive question for a person (\"use the existing X instead of creating Y?\").

## The ticket's earlier pull requests (every change that has them)
- When the plan lists earlier pull requests for this ticket, read what they changed and what reviewers wrote on them before you write code: reuse what was right, and don't repeat what a reviewer turned down.
";

/// How the coder ends: its evidence becomes the PR description's.
pub const EVIDENCE_OUTPUT: &str = "When you finish, output a one-line summary, then a section that starts with `Evidence:` listing: each fact about a system this code doesn't own that the change relies on, on its own line that starts `- Confirmed:` (the fact, the lookup that confirmed it and what came back: quote the field and its value) or `- Unconfirmed [external:<the system>]:` (the fact, and why nothing confirmed it: the source that is missing, or the lookup that failed or came back empty); a check of this repository's own (lint, type-check, tests, build, format) that didn't run or pass goes on a line of its own, `- Unconfirmed [check:<lint|typecheck|tests|build|format>]:`, never as an outside fact; how many records the change affects, where you counted them, and a few examples (ids or names); and anything no connected source could answer, naming the source that is missing. Write `Evidence: none needed` when the change relies on nothing outside the code and changes no stored records. Do NOT describe each code change in detail. Last, when the change copies an existing block, a fenced ```mirrors.json block: `[{\"path\": \"the file of the block you copied\", \"name\": \"the block (resource, function, key)\", \"differences\": [\"each intentional difference, and why\"]}]`; `[]` when it copies none.";

/// How one lookup ended, as recorded when it ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupOutcome {
    /// Data came back.
    Answered,
    /// The system answered with no records (an empty list, no rows): nothing
    /// about what its records hold was read.
    Empty,
    /// It failed or was refused: nothing came back.
    Failed,
}

/// One lookup a run or a review made, as recorded when it ran.
#[derive(Debug, Clone, PartialEq)]
pub struct Lookup {
    /// Its id in this work (`L3`), as the agent was told; findings cite it.
    pub id: String,
    /// The source it asked, by name.
    pub source: String,
    pub outcome: LookupOutcome,
    /// What was asked and what came back, as one entry.
    pub line: String,
}

impl Lookup {
    /// The entry for a lookup: `- [id] asked` and what came back, by outcome.
    pub fn new(id: String, source: &str, outcome: LookupOutcome, asked: &str, got: &str) -> Self {
        let flat = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
        let got = flat(got);
        let got = common::truncate_str(&got, 500);
        let came_back = match outcome {
            LookupOutcome::Answered => format!("Came back: {got}"),
            LookupOutcome::Empty => {
                format!("Came back EMPTY (no records, so it confirms nothing): {got}")
            }
            LookupOutcome::Failed => format!("FAILED (nothing came back): {got}"),
        };
        let asked = flat(asked);
        Self {
            line: format!(
                "- [{id}] {}\n  {came_back}",
                common::truncate_str(&asked, 300)
            ),
            id,
            source: source.to_string(),
            outcome,
        }
    }

    /// Something about the system came back.
    pub fn gave_data(&self) -> bool {
        self.outcome == LookupOutcome::Answered
    }
}

/// What the run gathered that the change rests on.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunEvidence {
    /// The data sources this work could read, one line each.
    pub sources: Vec<String>,
    /// Each lookup made and how it ended.
    pub lookups: Vec<Lookup>,
    /// The coder's own account: its closing summary and `Evidence:` section.
    pub notes: String,
    /// What the last sandbox check run came to, one line per step (kind,
    /// command, PASS or FAIL), as run_checks recorded it. Empty when none ran.
    pub checks: Vec<String>,
    /// Which kinds of check the last sandbox run reported, which passed, or
    /// why it gave no result.
    pub sandbox: SandboxRecord,
    /// The kinds of check the repository's CI runs on the pull request.
    pub ci_gates: Vec<CheckKind>,
    /// Each check command the repository's CI runs on the pull request, with
    /// its id (`C1`): what the judge maps a finding to, and what the PR's
    /// Verification section says CI runs.
    pub ci_commands: Vec<CiCommand>,
    /// The environments the diff changes, read from its paths when the
    /// change is judged (see `completeness::changed_environments`).
    pub environments: Vec<String>,
}

/// What the sandbox came to on the run's last check: the kinds of check it
/// reported a result for and those that passed, or why it gave none.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SandboxRecord {
    pub ran: Vec<CheckKind>,
    pub passed: Vec<CheckKind>,
    /// Why the last attempt gave no result (the build's status and phases).
    pub failure: Option<String>,
}

impl SandboxRecord {
    /// From one check run's steps and each step's exit code.
    pub fn from_steps(steps: &[crate::passes::checks::Step], codes: &[Option<i32>]) -> Self {
        let mut out = Self::default();
        for (step, code) in steps.iter().zip(codes) {
            let Some(code) = code else { continue };
            for k in &step.kinds {
                if !out.ran.contains(k) {
                    out.ran.push(*k);
                }
                if *code == 0 && !out.passed.contains(k) {
                    out.passed.push(*k);
                }
            }
        }
        // A kind with a failing step didn't pass, whatever another step did.
        for (step, code) in steps.iter().zip(codes) {
            if matches!(code, Some(c) if *c != 0) {
                out.passed.retain(|k| !step.kinds.contains(k));
            }
        }
        out
    }

    /// Why the sandbox didn't run a check of `kind`, in a clause, when the
    /// run's record says: it gave no result, or its checks had no such step.
    fn why_not(&self, kind: CheckKind) -> Option<String> {
        if let Some(f) = &self.failure {
            return Some(f.clone());
        }
        (!self.ran.is_empty() && !self.ran.contains(&kind)).then(|| {
            format!(
                "the sandbox ran {} but has no {} step for this repository",
                self.ran
                    .iter()
                    .map(|k| k.name())
                    .collect::<Vec<_>>()
                    .join(", "),
                kind.name()
            )
        })
    }
}

/// Starts a fix round's notes inside [`RunEvidence::notes`].
const FIX_ROUND_MARK: &str = "After a fix round:";

impl RunEvidence {
    /// The coder's latest account: the last fix round's notes, or the first
    /// pass's when no round wrote any. A fact a fix round confirmed is no
    /// longer held against the change by what the first pass wrote.
    pub fn latest_notes(&self) -> &str {
        match self.notes.rfind(FIX_ROUND_MARK) {
            Some(i) => &self.notes[i + FIX_ROUND_MARK.len()..],
            None => &self.notes,
        }
    }

    /// Adds a later pass's evidence (a fix round) to this run's.
    pub fn merge(&mut self, later: RunEvidence) {
        for s in later.sources {
            if !self.sources.contains(&s) {
                self.sources.push(s);
            }
        }
        self.lookups.extend(later.lookups);
        // The latest check run is what the branch now stands on.
        if !later.checks.is_empty() {
            self.checks = later.checks;
            self.sandbox = later.sandbox;
        }
        for k in later.ci_gates {
            if !self.ci_gates.contains(&k) {
                self.ci_gates.push(k);
            }
        }
        self.ci_gates.sort();
        // CI's commands come from the same workflows each pass: the first
        // list keeps the ids the judge has already seen.
        if self.ci_commands.is_empty() {
            self.ci_commands = later.ci_commands;
        }
        if !later.environments.is_empty() {
            self.environments = later.environments;
        }
        let notes = later.notes.trim();
        if !notes.is_empty() {
            if self.notes.trim().is_empty() {
                self.notes = notes.to_string();
            } else {
                self.notes = format!("{}\n\n{FIX_ROUND_MARK}\n{notes}", self.notes.trim());
            }
        }
    }

    /// The CI command `id` names (`C2`, `[C2]`), when CI runs one by that id.
    pub fn ci_command(&self, id: &str) -> Option<&CiCommand> {
        let id = id.trim().trim_matches(|c| c == '[' || c == ']').trim();
        if id.is_empty() {
            return None;
        }
        self.ci_commands
            .iter()
            .find(|c| c.id.eq_ignore_ascii_case(id))
    }

    /// What the judge reads about the checks CI runs on the PR: each command
    /// with its id, or the kinds when only those are known.
    fn ci_for_judge(&self) -> String {
        if !self.ci_commands.is_empty() {
            return self
                .ci_commands
                .iter()
                .map(CiCommand::judge_line)
                .collect::<Vec<_>>()
                .join("\n");
        }
        match self.ci_gates.as_slice() {
            [] => "(none found: nothing checks this PR after it opens)".to_string(),
            g => g.iter().map(|k| k.name()).collect::<Vec<_>>().join(", "),
        }
    }

    /// What the judge reads about the run's evidence.
    pub fn for_judge(&self) -> String {
        let list = |items: &[&str], none: &str| {
            if items.is_empty() {
                format!("{none}\n")
            } else {
                items
                    .iter()
                    .map(|i| format!("{}\n", i.trim_end()))
                    .collect()
            }
        };
        let sources: Vec<&str> = self.sources.iter().map(String::as_str).collect();
        let lookups: Vec<&str> = self.lookups.iter().map(|l| l.line.as_str()).collect();
        let checks: Vec<&str> = self.checks.iter().map(String::as_str).collect();
        format!(
            "## Data sources this team connected for this repository\n{}\n\
             ## Lookups the run made, and what came back\n{}\n\
             ## Checks run in the sandbox before the PR opened (recorded by CoderHelm, not the coder)\n{}\n\
             ## Checks the repository's CI runs on this pull request\n{}\n\n\
             ## Environments the diff changes (read from its file paths)\n{}\n\n\
             ## The coder's evidence notes (they become the PR description's evidence)\n{}\n",
            list(&sources, "(none connected)"),
            list(&lookups, "(no lookups were made)"),
            list(&checks, "(no sandbox checks ran)"),
            self.ci_for_judge(),
            match self.environments.as_slice() {
                [] => "(none)".to_string(),
                e => e.join(", "),
            },
            match self.notes.trim() {
                "" => "(the coder wrote none)",
                n => n,
            },
        )
    }

    /// What the PR description writer is given to report.
    pub fn for_pr(&self) -> String {
        if self.lookups.is_empty() && self.notes.trim().is_empty() {
            return String::new();
        }
        let mut out =
            String::from("\n## What the run looked up (the source for the Evidence section)\n");
        for l in &self.lookups {
            out.push_str(&format!("{}\n", l.line.trim_end()));
        }
        if !self.notes.trim().is_empty() {
            out.push_str(&format!("\nThe coder's notes:\n{}\n", self.notes.trim()));
        }
        out
    }
}

/// One of the things the gate judges.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Finding {
    /// One of the judge's points: `external_fact`, `impact`, `logging`,
    /// `assumption`, `structured_field`, `survey`, `silent_fallback`,
    /// `fixtures`, `one_record`, `team_rule`, `ci_pending`,
    /// `manual_check`, `pattern_cited`, `end_to_end`, `environment`,
    /// `env_unproven`, `near_match_unresolved`, `history_used` or
    /// `planned_work`; or `unrun_check`, which only [`classify`] sets, or
    /// `optional_task`, which only the plan check sets.
    pub point: String,
    /// What is unverified, in one sentence.
    pub detail: String,
    /// How to settle it, in one sentence.
    #[serde(default)]
    pub settle: String,
    /// The kind of check (`format`, `lint`, `typecheck`, `build`, `tests`,
    /// `plan`, `security`) when the finding is only that a check didn't run
    /// (or failed) before the PR.
    #[serde(default)]
    pub check: String,
    /// The command the finding asks to run (`terraform fmt -check`), as the
    /// judge wrote it: matched to CI's commands through the tool table.
    #[serde(default)]
    pub command: String,
    /// The id of the CI command (`C1`) the judge says runs this check.
    /// [`classify`] keeps it only when CI runs a command by that id.
    #[serde(default)]
    pub ci: String,
    /// The CI command that settles this finding, as people read it, set by
    /// [`classify`]; never read from the judge.
    #[serde(skip)]
    pub ci_cmd: String,
}

/// What settles a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settler {
    /// A gap in what the change rests on: a fix round, then the warning.
    Proof,
    /// A check the PR's CI runs: a pending item its green result removes.
    Ci,
    /// Only a person can check it (a real browser, a device): a step in the
    /// PR's Verification section, never in Open items.
    Person,
    /// Something outside the code the plan left open (a step of the whole
    /// job nobody owns): an Open item for a person, never a fix round, since
    /// the coder can't change the plan.
    Owner,
}

/// The most a warning bullet says about what's unverified, and about how to
/// settle it, in characters.
const MAX_WHAT: usize = 220;
const MAX_SETTLE: usize = 160;

impl Finding {
    /// What settles this finding, by its point.
    pub fn settler(&self) -> Settler {
        match self.point.trim() {
            "ci_pending" => Settler::Ci,
            "manual_check" => Settler::Person,
            "end_to_end" | "optional_task" => Settler::Owner,
            _ => Settler::Proof,
        }
    }

    fn label(&self) -> &str {
        match self.point.trim() {
            "ci_pending" => "Waiting on CI",
            "manual_check" => "Check by hand",
            "unrun_check" => "Check not run",
            "external_fact" => "Unconfirmed external field or behaviour",
            "impact" => "Impact not measured",
            "logging" => "Noisy logging",
            "assumption" => "Unproven assumption",
            "structured_field" => "Name parsed where a structured field exists",
            "survey" => "Field not surveyed across live records",
            "silent_fallback" => "Unexpected data handled silently",
            "fixtures" => "Tests not built from real payloads",
            "one_record" => "Fixes one record, not the rule",
            "team_rule" => "Team instruction not met",
            "pattern_cited" => "Copied block not named",
            "end_to_end" => "Missing from the end-to-end checklist",
            "environment" => "Environment not confirmed",
            "env_unproven" => "Environment a lookup hit not proven",
            "near_match_unresolved" => "Close match not followed up",
            "history_used" => "Earlier pull requests not built on",
            "planned_work" => "Planned work not done",
            "optional_task" => "Optional task left out",
            other => other,
        }
    }

    /// One bullet: what's unverified, then how to settle it, each cut to one
    /// sentence and a length cap at a word boundary.
    fn line(&self) -> String {
        let what = sentence(&self.detail, MAX_WHAT);
        let settle = match (self.settler(), sentence(&self.settle, MAX_SETTLE)) {
            (Settler::Ci, _) if !self.ci_cmd.is_empty() => {
                format!(
                    " CI runs {} on this PR; this settles when it passes.",
                    self.ci_cmd
                )
            }
            (_, s) if !s.is_empty() => format!(" Settle: {s}"),
            (Settler::Ci, _) => " Settles when this PR's CI passes.".to_string(),
            _ => String::new(),
        };
        format!("- **{}:** {what}{settle}", self.label())
    }
}

/// The first sentence of `text`, whitespace collapsed, cut to `max`
/// characters at a word boundary (with an ellipsis) when longer.
fn sentence(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    // A sentence ends at `.`, `!` or `?` followed by a space and a capital
    // letter, so `e.g. this` and `v1.2` stay whole.
    let chars: Vec<char> = flat.chars().collect();
    let mut end = chars.len();
    for i in 0..chars.len() {
        if matches!(chars[i], '.' | '!' | '?')
            && chars.get(i + 1) == Some(&' ')
            && chars.get(i + 2).is_some_and(|c| c.is_uppercase())
        {
            end = i + 1;
            break;
        }
    }
    let first: String = chars[..end].iter().collect();
    if first.chars().count() <= max {
        return first;
    }
    let cut: String = first.chars().take(max).collect();
    let at_word = match cut.rfind(' ') {
        Some(i) if i > 0 => &cut[..i],
        _ => cut.as_str(),
    };
    format!("{}…", at_word.trim_end_matches([',', ';', ':']))
}

/// How a finding was tied to the CI command that settles it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tie {
    /// The command the finding asks for runs the same tool as CI's command
    /// (through the tool table).
    Tool,
    /// The judge named the CI command's id, and CI runs a command by it.
    Id,
    /// Only the kind of check matches.
    Kind,
}

/// The CI command that settles a finding, and how it was tied: the command
/// the finding asks for matched through the tool table to a CI command of
/// the same tool; else the CI command the judge named, when CI runs one by
/// that id and it is of the kind the finding names; else a CI command of the
/// kind the finding (or its command) names.
fn covering<'a>(f: &Finding, ev: &'a RunEvidence) -> Option<(&'a CiCommand, Tie)> {
    let asked = classify_command(&f.command);
    if let Some(m) = &asked {
        if let Some(c) = ev.ci_commands.iter().find(|c| c.tool == m.tool) {
            return Some((c, Tie::Tool));
        }
    }
    let kind = CheckKind::parse(&f.check).or(asked.map(|m| m.kind));
    if let Some(c) = ev.ci_command(&f.ci) {
        if kind.is_none_or(|k| k == c.kind) {
            return Some((c, Tie::Id));
        }
    }
    let kind = kind?;
    ev.ci_commands
        .iter()
        .find(|c| c.kind == kind)
        .map(|c| (c, Tie::Kind))
}

/// Holds the judge's findings to the run's facts. A finding that is only
/// "this check didn't run before the PR" is not a proof gap when the
/// repository's CI runs that check on the PR: it becomes a `ci_pending` item
/// CI's result settles, whatever point the judge gave it, naming the CI
/// command that runs it. The tie is structured: the finding's `command`
/// matched to CI's through the tool table, else the CI command id the judge
/// gave (kept only when CI runs a command by it), else the kind. When CI
/// doesn't run that check, nothing will settle it after the PR opens, so it
/// is a gap (`unrun_check`); a plan or preview, which needs credentials the
/// sandbox never has, is a step for a person instead. A `ci_pending` that
/// names no check waits on CI only when CI runs any check at all. A
/// `manual_check` becomes `ci_pending` only when tied to a CI command by its
/// command or id, or by a kind other than tests (a person's click-through is
/// not CI's test suite).
pub fn classify(findings: Vec<Finding>, evidence: &RunEvidence) -> Vec<Finding> {
    findings
        .into_iter()
        .map(|mut f| {
            let manual = f.point.trim() == "manual_check";
            let kind =
                CheckKind::parse(&f.check).or_else(|| classify_command(&f.command).map(|m| m.kind));
            f.ci_cmd.clear();
            // A finding about something else (an unconfirmed field) never
            // waits on CI because the judge put an id on it.
            let about_a_check = kind.is_some() || manual || f.point.trim() == "ci_pending";
            if let Some((c, tie)) = covering(&f, evidence).filter(|_| about_a_check) {
                if !manual || tie != Tie::Kind || c.kind != CheckKind::Tests {
                    f.point = "ci_pending".into();
                    f.check = c.kind.name().into();
                    f.ci = c.id.clone();
                    f.ci_cmd = c.display();
                    return f;
                }
            }
            f.ci.clear();
            if manual {
                return f;
            }
            match kind {
                Some(k) if evidence.ci_gates.contains(&k) => f.point = "ci_pending".into(),
                Some(CheckKind::Plan) => f.point = "manual_check".into(),
                Some(k) => {
                    f.point = "unrun_check".into();
                    f.check = k.name().into();
                    // Named from the run's record, not the judge's guess at
                    // why: what the sandbox came to, and that CI won't run it.
                    if let Some(why) = evidence.sandbox.why_not(k) {
                        f.detail = format!(
                            "{} never ran before the PR: {why}, and the repository's CI does not \
                             run {} on this pull request.",
                            capitalised(k.name()),
                            k.name()
                        );
                    }
                }
                None if f.point.trim() == "ci_pending" && evidence.ci_gates.is_empty() => {
                    f.point = "unrun_check".into();
                }
                None => {}
            }
            f
        })
        .collect()
}

/// `text` with its first letter in capitals.
fn capitalised(text: &str) -> String {
    let mut c = text.chars();
    match c.next() {
        Some(f) => f.to_uppercase().chain(c).collect(),
        None => String::new(),
    }
}

/// What the repository's CI runs on the pull request, one line per
/// command, for the PR writer. Empty when CI runs no check.
pub fn ci_runs(ci: &[CiCommand]) -> String {
    ci.iter()
        .map(|c| format!("- {} ({})\n", c.display(), c.kind.name()))
        .collect()
}

#[derive(Deserialize)]
struct Judged {
    #[serde(default)]
    findings: Vec<Finding>,
}

/// The judge's system prompt.
pub const JUDGE_SYSTEM: &str = "You check a code change before its pull request opens. You judge \
     the points below from the evidence you are given: the diff, the plan, the data sources the team \
     connected, every lookup the run made with what came back, and the coder's evidence notes \
     (they become the PR description's evidence).\n\
     1. external_fact: the change relies on a field, value or behaviour of a system it does not \
     own (a vendor API, another service, a table another system writes). A miss when no lookup \
     listed confirms it from the real thing (a payload, a row) and the notes don't quote what \
     was seen. Not a miss when no connected source can reach that system and the notes name \
     exactly which source is missing.\n\
     2. impact: the change alters behaviour for stored records whose data sits in a connected \
     source. A miss when the notes give no count of affected records from a lookup with a few \
     concrete examples (ids or names), or when they say impact was not measured, not verified or \
     unconfirmed while a connected source could have answered.\n\
     3. logging: a miss when the diff adds an INFO (or louder) log that fires for every record or \
     on every scheduled run without reporting an actual change or decision. Diagnostics belong \
     at DEBUG.\n\
     4. assumption: a miss when the change rests on an assumption (what another system sends, \
     what values a field takes, uniqueness, ordering) that nothing in the lookups or the code \
     proves.\n\
     5. structured_field: a miss when the change reads another system's data by parsing a name, \
     title or description (a regex, a pattern, a substring) while the payload has a structured \
     field or flag (a boolean, a type, a quantity, an id) that says the same thing, or when it \
     parses text with no lookup of real records showing no such field exists. Parsing is \
     acceptable only as a last resort: the lookups show the records have no structured field, \
     the notes say so, and the pattern was checked against every live value.\n\
     6. survey: a miss when a rule about another system's field rests on the ticket's records \
     only, with no count of the live records per shape and one example id each, or applies one \
     meaning to records whose shapes differ without scoping it by the source's own flag.\n\
     7. silent_fallback: a miss when the change quietly falls back (a default, a skip) on data \
     that doesn't match the expected shape, instead of a WARNING log with the record id and a \
     count in the run's summary. A WARNING for an unexpected shape is not noisy logging.\n\
     8. fixtures: a miss when the change alters how another system's data is handled and its \
     tests use invented payloads instead of trimmed real ones (one per shape found), or no test \
     fails on the old code.\n\
     9. one_record: a miss when the change hard-codes ids, names, seasons or prices to fix the \
     ticket's records, or corrects existing records with a manual backfill or one-off script \
     the normal pipeline could do on its next run (an unavoidable one must be in the \
     verification steps).\n\
     10. team_rule: when the team's instructions are given, a miss when the change breaks one \
     of them (name the rule). Where they conflict with the points above, the team's win. A rule \
     to run lint, formatting, a type-check, a build, tests or a plan before pushing is not a \
     team_rule miss: report a check that didn't run or failed as ci_pending (point 11).\n\
     11. ci_pending: a check (format, lint, typecheck or validate, build, tests, plan or preview, \
     security scan) that the sandbox checks listed did not run, or ran and failed, before the PR \
     opened. When the repository's CI runs that check on the pull request (the CI list gives each \
     command it runs, with an id like C1), its result settles it, so it is not a proof gap. Set \
     `check` to the kind, `command` to the command the check needs (e.g. `terraform fmt -check`), \
     and `ci` to the id of the CI command that runs it, or empty when CI runs none. A check the \
     sandbox list shows as PASS is not a finding.\n\
     12. manual_check: something only a person can verify (in a real browser, on a device, a \
     manual click-through). Never a gap; it becomes a numbered step in the PR's Verification \
     section. Never a command the CI list shows: running that is CI's job, so it is ci_pending \
     with its `ci` id.\n\
     13. pattern_cited: the diff adds a block (a user, secret, certificate, route, config entry, \
     job, permission) that closely resembles one the repository already has for another case. A \
     miss when the coder's notes have no `mirrors.json` entry naming that existing block (its \
     path and name), or when the new block differs from it in ways the entry doesn't list with a \
     reason. Name the existing block.\n\
     14. end_to_end: the plan's end-to-end checklist (every step the ticket needs to work in \
     production, each with who does it). A miss when it leaves out an obvious step for this kind \
     of change: credentials and who reads them, the resource itself, network access and \
     allowlists, permissions and read grants, the handoff to whoever uses it, the deploy and \
     merge order. Name the missing step and who would own it. It is listed in the PR's Open \
     items for a person, not sent back to the coder.\n\
     15. environment: when the diff changes more than one environment (listed below), a miss for \
     each one that no evidence line names: a `- Confirmed:` line from a read-only lookup, or one \
     `- Unconfirmed:` line saying exactly what access is missing. One environment's lookup never \
     stands for another's.\n\
     16. env_unproven: a miss when the evidence hedges about which environment a lookup hit (it \
     didn't confirm it was production, isn't sure which cluster or account) while a connected \
     source could have shown it with data only that environment has. Not a miss when no \
     connected source reaches it and the notes name the missing access.\n\
     17. near_match_unresolved: a miss when the evidence mentions an existing object close to \
     what the ticket names (a similar name, the same purpose) without saying who owns it, what \
     is in it and what created it, or without one decisive question for a person about using it \
     instead.\n\
     18. history_used: when the plan names earlier pull requests for this ticket, a miss when \
     the diff repeats a change a reviewer turned down on one of them, or ignores what the plan \
     says it reuses from them.\n\
     19. planned_work: a task in the plan's task checklist (below the plan) that the diff doesn't \
     do, or does only in part (a file it creates as an empty shell, a step it skips). Name the \
     task and its file. Not a miss for a task the checklist marks optional. Planned work is \
     never a note for later: it is sent back to the coder, and the pull request doesn't open \
     without it.\n\
     Many changes touch none of these: a change that relies on nothing outside its own code, \
     changes no stored records and adds no logs passes all of them. Judge only what the evidence \
     shows and do not invent concerns. A fact the lookups confirm is proven even when the notes \
     are short. A lookup marked FAILED or EMPTY confirms nothing, and a fact the notes mark \
     `Unconfirmed:` is not proven. Each finding is short: `detail` is ONE sentence naming what is unverified (the \
     exact field, records, log line, assumption or check); `settle` is ONE sentence on how to \
     settle it (which connected source to read, what to count, which log to lower, which check \
     result). No lists, no preamble, no repeating the rule's text.\n\
     Output ONLY a fenced ```json block: {\"findings\":[{\"point\":\"external_fact|impact|logging|assumption|structured_field|survey|silent_fallback|fixtures|one_record|team_rule|ci_pending|manual_check|pattern_cited|end_to_end|environment|env_unproven|near_match_unresolved|history_used|planned_work\",\"detail\":\"one sentence\",\"settle\":\"one sentence\",\"check\":\"format|lint|typecheck|build|tests|plan|security, or empty\",\"command\":\"the command the check needs, or empty\",\"ci\":\"the CI command id (C1), or empty\"}]}. \
     An empty list when all pass.";

/// The judge's prompt: the change and everything the run gathered.
pub fn judge_prompt(
    title: &str,
    plan_context: &str,
    diff: &str,
    evidence: &RunEvidence,
    team_instructions: &str,
) -> String {
    format!(
        "# Change for: {title}\n\n## Plan\n{plan}{team}\n\n{evidence}\n## The diff\n{diff}\n",
        plan = match plan_context.trim() {
            "" => "(no plan carried into this check)",
            p => p,
        },
        team = super::team_instructions_block(team_instructions),
        evidence = evidence.for_judge(),
    )
}

/// The judge's findings, or `None` when its reply can't be read.
pub fn parse_findings(reply: &str) -> Option<Vec<Finding>> {
    let block = match reply.rfind("```json") {
        Some(s) => {
            let after = &reply[s + 7..];
            after.find("```").map(|e| &after[..e])?
        }
        None => reply,
    };
    let judged: Judged = serde_json::from_str(block.trim()).ok()?;
    Some(
        judged
            .findings
            .into_iter()
            .filter(|f| !f.detail.trim().is_empty())
            .collect(),
    )
}

/// What happens after a judgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    /// Nothing to prove (or the judge couldn't answer): open the PR.
    Open,
    /// Send the findings back to the coder for a fix round.
    FixRound,
    /// Gaps remain and no round is left: open the PR with them listed under Open items.
    OpenWithGaps,
    /// Planned work is still not done and no round is left: the PR doesn't
    /// open. Planned work is never downgraded to an Open item.
    Unfinished,
}

/// Decides what follows a judgment. `None` is a judge that couldn't answer:
/// it never holds a PR. Only proof gaps earn a fix round: what CI or a person
/// settles opens the PR as it is.
pub fn next(findings: Option<&[Finding]>, rounds_done: u32, has_time: bool) -> Next {
    match findings.map_or(0, proof_gaps) {
        0 => Next::Open,
        _ if rounds_done < MAX_FIX_ROUNDS && has_time => Next::FixRound,
        _ if findings.is_some_and(|f| !planned_work(f).is_empty()) => Next::Unfinished,
        _ => Next::OpenWithGaps,
    }
}

/// The findings that are planned work not done: never Open items.
pub fn planned_work(findings: &[Finding]) -> Vec<&Finding> {
    findings
        .iter()
        .filter(|f| f.point.trim() == "planned_work")
        .collect()
}

/// Why the run stops when planned work is still not done after its rounds:
/// one plain sentence naming the tasks.
pub fn unfinished_reason(findings: &[Finding]) -> String {
    let items: Vec<String> = planned_work(findings)
        .iter()
        .map(|f| sentence(&f.detail, 120))
        .collect();
    format!(
        "Stopped before opening a pull request: the check before it found planned work not done: {}",
        items.join("; ")
    )
}

/// How many of `findings` are proof gaps.
fn proof_gaps(findings: &[Finding]) -> usize {
    findings
        .iter()
        .filter(|f| f.settler() == Settler::Proof)
        .count()
}

/// How an evidence line that names a fact nothing confirmed starts (after
/// any `>` quote and `-` bullet), and what such a line is about when its
/// marker alone says. `- Unconfirmed:` is what the coder is told to write
/// ([`EVIDENCE_OUTPUT`]), with a tag; the last is how [`gaps_section`] names
/// an open `external_fact` gap in a PR's Open items.
const UNCONFIRMED_MARKERS: &[(&str, bool)] = &[
    ("unconfirmed external field or behaviour", true),
    ("unconfirmed", false),
    ("unverified", false),
    ("not confirmed", false),
    ("not verified", false),
];

/// What an unconfirmed evidence line is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum About {
    /// The repository's own checks (lint, type-check, tests, build, format):
    /// the sandbox or CI settles them, never an outside system.
    Checks(Vec<CheckKind>),
    /// A system the code doesn't own, by name when the line's tag gives one.
    External(Option<String>),
    /// Untagged and not about the repository's checks: held as an outside
    /// fact, as before tags.
    Unknown,
}

/// One fact an evidence text marks as unconfirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unconfirmed {
    pub fact: String,
    pub about: About,
}

/// What a line's tag (`check:lint`, `checks:typecheck,tests`,
/// `external:vendor API`) says it is about.
fn about_tag(tag: &str) -> Option<About> {
    let (kind, value) = match tag.split_once(':') {
        Some((k, v)) => (k.trim().to_ascii_lowercase(), v.trim()),
        None => (tag.trim().to_ascii_lowercase(), ""),
    };
    match kind.as_str() {
        "check" | "checks" => {
            let kinds: Vec<CheckKind> = value
                .split([',', '+', '/', ' '])
                .filter_map(CheckKind::parse)
                .fold(Vec::new(), |mut v, k| {
                    if !v.contains(&k) {
                        v.push(k);
                    }
                    v
                });
            (!kinds.is_empty()).then_some(About::Checks(kinds))
        }
        "external" | "outside" | "system" => Some(About::External(
            (!value.is_empty()).then(|| value.to_string()),
        )),
        _ => None,
    }
}

/// Words that may sit around check names in a line about checks only
/// ("lint, type-check and tests passing").
const CHECK_LINE_WORDS: &[&str] = &[
    "and",
    "or",
    "the",
    "all",
    "a",
    "its",
    "this",
    "of",
    "repo",
    "repository's",
    "repository",
    "own",
    "check",
    "checks",
    "suite",
    "step",
    "steps",
    "passing",
    "pass",
    "passes",
    "passed",
    "green",
    "clean",
    "run",
    "runs",
    "ran",
    "running",
    "not",
    "never",
    "no",
    "did",
    "didn't",
    "locally",
    "in",
    "on",
    "the",
    "sandbox",
    "result",
    "results",
    "were",
    "was",
    "is",
    "are",
    "be",
    "could",
    "couldn't",
    "verified",
    "confirmed",
    "unit",
    "type",
    "ci",
    "before",
    "pr",
];

/// The checks an untagged line is about, read through the tool table: a
/// backticked command it names (`npm run lint`), or a line made only of
/// check names and the words around them. `None` when it names anything
/// else, which keeps it an outside fact.
fn about_checks(fact: &str) -> Option<Vec<CheckKind>> {
    let mut kinds: Vec<CheckKind> = Vec::new();
    let mut add = |k: CheckKind| {
        if !kinds.contains(&k) {
            kinds.push(k);
        }
    };
    // Backticked commands, matched against the CI tool table.
    let spans: Vec<&str> = fact.split('`').skip(1).step_by(2).collect();
    let commands: Vec<CheckKind> = spans
        .iter()
        .filter_map(|c| crate::passes::checks::classify_command(c).map(|m| m.kind))
        .collect();
    let rest: String = fact.split('`').step_by(2).collect::<Vec<_>>().join(" ");
    for k in commands {
        add(k);
    }
    // Every other word is a check name or a word around one.
    let words: Vec<String> = rest
        .split(|c: char| {
            c.is_whitespace() || matches!(c, ',' | ';' | '.' | '(' | ')' | '/' | '&' | ':')
        })
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect();
    for (i, w) in words.iter().enumerate() {
        if let Some(k) = CheckKind::parse(w) {
            add(k);
        } else if w == "type" && words.get(i + 1).is_some_and(|n| n.starts_with("check")) {
            add(CheckKind::Typecheck);
        } else if !CHECK_LINE_WORDS.contains(&w.as_str()) {
            return None;
        }
    }
    (!kinds.is_empty()).then_some(kinds)
}

/// The facts an evidence text marks as unconfirmed, each with what it is
/// about: its tag when it has one (`- Unconfirmed [check:lint]:`), else the
/// repository's checks when the line names only those, else unknown. Reads
/// the coder's notes before a PR opens and a PR description in review.
pub fn unconfirmed_items(text: &str) -> Vec<Unconfirmed> {
    text.lines()
        .filter_map(|line| {
            let mut l = line.trim();
            loop {
                let before = l;
                l = l.trim_start_matches('>').trim_start();
                if let Some(rest) = l.strip_prefix("- ").or_else(|| l.strip_prefix("* ")) {
                    l = rest.trim_start();
                }
                if l == before {
                    break;
                }
            }
            let l = l.trim_start_matches("**");
            let lower = l.to_ascii_lowercase();
            let (marker, external) = UNCONFIRMED_MARKERS
                .iter()
                .find(|(m, _)| lower.starts_with(m))?;
            let mut rest = l[marker.len()..].trim_start();
            let mut tag = None;
            if let Some(open) = rest.strip_prefix('[').or_else(|| rest.strip_prefix('(')) {
                let close = open.find([']', ')'])?;
                tag = Some(open[..close].to_string());
                rest = open[close + 1..].trim_start();
            }
            let rest = rest.trim_start_matches("**").trim_start();
            let fact = rest.strip_prefix(':')?.trim_start_matches("**").trim();
            if fact.is_empty() {
                return None;
            }
            let about = match tag.as_deref().and_then(about_tag) {
                Some(a) => a,
                None if *external => About::External(None),
                None => match about_checks(fact) {
                    Some(kinds) => About::Checks(kinds),
                    None => About::Unknown,
                },
            };
            Some(Unconfirmed {
                fact: fact.to_string(),
                about,
            })
        })
        .collect()
}

/// The outside facts an evidence text marks as unconfirmed: each
/// [`unconfirmed_items`] line that isn't about the repository's own checks.
pub fn unconfirmed_facts(text: &str) -> Vec<String> {
    unconfirmed_items(text)
        .into_iter()
        .filter(|u| !matches!(u.about, About::Checks(_)))
        .map(|u| u.fact)
        .collect()
}

/// A fact's text reduced to compare it with another: lowercase letters and
/// digits only.
fn fact_key(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// Gaps the run's own record shows, whatever the judge says: a source the
/// run looked up where no lookup brought a record back (each failed or came
/// back empty), and each fact the coder's notes mark unconfirmed. Either way
/// nothing the change relies on from that system was read from a real
/// payload.
pub fn recorded_gaps(evidence: &RunEvidence) -> Vec<Finding> {
    let mut sources: Vec<&str> = Vec::new();
    for l in &evidence.lookups {
        if !sources.contains(&l.source.as_str()) {
            sources.push(&l.source);
        }
    }
    let mut out = Vec::new();
    for source in sources {
        let of: Vec<&Lookup> = evidence
            .lookups
            .iter()
            .filter(|l| l.source == source)
            .collect();
        if of.iter().any(|l| l.gave_data()) {
            continue;
        }
        let empty = of
            .iter()
            .filter(|l| l.outcome == LookupOutcome::Empty)
            .count();
        let failed = of.len() - empty;
        let ids: Vec<&str> = of.iter().map(|l| l.id.as_str()).collect();
        out.push(Finding {
            point: "external_fact".into(),
            detail: format!(
                "No lookup against {source} brought a record back ({failed} failed, {empty} came \
                 back empty: {}), so nothing the change relies on from it was read from a real \
                 payload.",
                ids.join(", ")
            ),
            settle: format!(
                "Read one real record from {source}, checking the path, table and every id in it \
                 against where the code builds them, and quote what came back."
            ),
            ..Default::default()
        });
    }
    // Each unconfirmed line once: an outside fact is a proof gap; one about
    // the repository's own checks is settled by CI or named as not run, the
    // way a judge's finding about a check is.
    let mut checks: Vec<Finding> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for u in unconfirmed_items(evidence.latest_notes()) {
        let key = fact_key(&u.fact);
        if key.is_empty() || seen.contains(&key) {
            continue;
        }
        seen.push(key);
        match u.about {
            About::Checks(kinds) => {
                for k in kinds {
                    if evidence.sandbox.passed.contains(&k)
                        || checks.iter().any(|c| c.check == k.name())
                    {
                        continue;
                    }
                    checks.push(Finding {
                        point: "ci_pending".into(),
                        detail: format!(
                            "{} is not confirmed to pass: the evidence notes say so ({}).",
                            capitalised(k.name()),
                            u.fact
                        ),
                        check: k.name().into(),
                        ..Default::default()
                    });
                }
            }
            About::External(_) | About::Unknown => out.push(Finding {
                point: "external_fact".into(),
                detail: format!("The evidence notes mark this unconfirmed: {}", u.fact),
                settle: "Confirm it through a connected source and quote what came back, or \
                         change the code so it no longer depends on it."
                    .into(),
                ..Default::default()
            }),
        }
    }
    out.extend(classify(checks, evidence));
    // Each environment the diff changes needs its own evidence line.
    out.extend(super::completeness::env_gaps(
        &evidence.environments,
        evidence.latest_notes(),
    ));
    out
}

/// The judge's findings with the [`recorded_gaps`] added. A recorded gap
/// stands even when the judge passed the change or couldn't answer. An
/// outside fact is a proof gap; a check the notes leave unconfirmed went
/// through [`classify`] like the judge's. Nothing is listed twice: a check
/// the judge already named, or an outside fact it already holds, is kept once.
pub fn with_recorded(judged: Option<Vec<Finding>>, evidence: &RunEvidence) -> Option<Vec<Finding>> {
    let recorded = recorded_gaps(evidence);
    if recorded.is_empty() {
        return judged;
    }
    let mut all = judged.unwrap_or_default();
    // Which environments lack evidence is read from the paths and notes in
    // code; the judge's own account of it would only repeat that.
    if recorded.iter().any(|g| g.point == "environment") {
        all.retain(|f| f.point.trim() != "environment");
    }
    for g in recorded {
        if !all.iter().any(|f| same_item(f, &g)) {
            all.push(g);
        }
    }
    Some(all)
}

/// Whether two findings name the same open item: the same finding, a check
/// of the same kind, or the same outside fact (one's text holds the other's).
fn same_item(a: &Finding, b: &Finding) -> bool {
    if a == b {
        return true;
    }
    let kind = |f: &Finding| CheckKind::parse(&f.check);
    if let (Some(x), Some(y)) = (kind(a), kind(b)) {
        return x == y;
    }
    if a.point.trim() == "external_fact" && b.point.trim() == "external_fact" {
        let (x, y) = (fact_key(&a.detail), fact_key(&b.detail));
        let marked = fact_key("The evidence notes mark this unconfirmed:");
        let (x, y) = (
            x.strip_prefix(&marked).unwrap_or(&x).to_string(),
            y.strip_prefix(&marked).unwrap_or(&y).to_string(),
        );
        return !x.is_empty() && !y.is_empty() && (x.contains(&y) || y.contains(&x));
    }
    false
}

/// The findings with what the plan's end-to-end checklist leaves open added
/// (see `completeness::checklist_gaps`): Open items for a person, never a
/// fix round. A plan with no decision carries no checklist to hold.
pub fn with_checklist(
    findings: Option<Vec<Finding>>,
    decision: Option<&super::fix_location::FixDecision>,
    repo: &str,
) -> Option<Vec<Finding>> {
    let open = super::completeness::checklist_gaps(decision, repo);
    if open.is_empty() {
        return findings;
    }
    let mut all = findings.unwrap_or_default();
    // The judge may name the same missing checklist; the code's account stands.
    if open
        .iter()
        .any(|g| g.detail.contains("no end-to-end checklist"))
    {
        all.retain(|f| f.point.trim() != "end_to_end");
    }
    for g in open {
        if !all.contains(&g) {
            all.push(g);
        }
    }
    Some(all)
}

/// What the coder is told in a fix round: the proof gaps only.
pub fn fix_feedback(findings: &[Finding]) -> String {
    let lines: Vec<String> = findings
        .iter()
        .filter(|f| f.settler() == Settler::Proof)
        .map(Finding::line)
        .collect();
    format!(
        "Before the pull request opens, the check of what this change rests on found these gaps. \
         Settle each one: look the fact up through a connected data source and quote what came \
         back, count the affected records and name a few, lower per-record or per-run INFO logs \
         to DEBUG, read the structured field instead of a name, survey the live records by \
         shape, warn on unexpected shapes, build fixtures from real payloads, fix the rule instead \
         of the record, follow the team's instructions, do the planned task a `Planned work not \
         done` item names (in full: every file the task needs), or change the code so it no longer \
         depends on what you can't prove. When no \
         connected source can answer, say which source is missing. Then write your `Evidence:` \
         section again in full.\n\n{}",
        lines.join("\n")
    )
}

/// Where the Open items section (once "Not yet verified", at the top) starts
/// and ends in a PR description, and the mark on each item the PR's CI
/// settles. They let the section be found and rewritten when CI finishes,
/// kept whole when the description is rewritten, and read by the reviewer.
pub const UNVERIFIED_START: &str = "<!-- coderhelm:unverified -->";
pub const UNVERIFIED_END: &str = "<!-- /coderhelm:unverified -->";
const CI_MARK: &str = "<!-- coderhelm:ci -->";

/// The Open items section, put at the bottom of the PR description: one
/// bullet per proof gap, per step the plan left without an owner, and per
/// check the PR's CI still has to settle, each saying what settles it. What
/// only a person can check never goes here (see [`manual_steps`]). No
/// callout banner. Empty when there is nothing open.
pub fn gaps_section(findings: &[Finding]) -> String {
    let of = |s: Settler| findings.iter().filter(move |f| f.settler() == s);
    let items: Vec<String> = of(Settler::Proof)
        .chain(of(Settler::Owner))
        .map(Finding::line)
        .chain(of(Settler::Ci).map(|f| format!("{} {CI_MARK}", f.line())))
        .collect();
    if items.is_empty() {
        return String::new();
    }
    format!(
        "{UNVERIFIED_START}\n## Open items\n{}\n{UNVERIFIED_END}",
        items.join("\n")
    )
}

/// What only a person can check, for the PR writer to put as numbered steps
/// in the Verification section. Empty when there is none.
pub fn manual_steps(findings: &[Finding]) -> String {
    findings
        .iter()
        .filter(|f| f.settler() == Settler::Person)
        .map(|f| {
            let what = sentence(&f.detail, MAX_WHAT);
            match sentence(&f.settle, MAX_SETTLE) {
                s if s.is_empty() => format!("- {what}\n"),
                s => format!("- {what} {s}\n"),
            }
        })
        .collect()
}

/// The description once the PR's CI passed: the items CI settled are gone,
/// and so is the whole section when nothing is left in it. What stays is
/// Open items at the bottom, whether the section was written there or at
/// the top as before. `None` when there is nothing to change.
pub fn settle_by_ci(body: &str) -> Option<String> {
    let (mut owned, rest) = super::completeness::take_owned(body);
    if !owned.open_items.contains(CI_MARK) {
        return None;
    }
    let kept: Vec<&str> = owned
        .open_items
        .lines()
        .filter(|l| !l.contains(CI_MARK))
        .collect();
    let any_item = kept.iter().any(|l| {
        let l = l.trim_start_matches('>').trim_start();
        l.starts_with("- ") || l.starts_with("* ")
    });
    let section = if any_item {
        kept.join("\n")
    } else {
        String::new()
    };
    owned.set(super::completeness::Section::OpenItems, section);
    Some(super::completeness::assemble(&rest, &owned))
}

/// What the plan says the change is for, for the judge.
fn plan_context(plan: &PlanResult) -> String {
    let mut out = plan.proposal.trim().to_string();
    if let Some(d) = &plan.fix {
        out.push_str(&super::fix_location::reviewer_context(d));
    }
    // The checklist the change is held to (point 19).
    if !plan.tasks.trim().is_empty() {
        out.push_str("\n\n### The plan's task checklist\n");
        out.push_str(common::truncate_str(plan.tasks.trim(), 8_000));
    }
    out
}

/// The judge's reading of the change on `branch`. `None` when it couldn't
/// judge (an API or model failure, an unreadable reply). `evidence` gains
/// the environments the diff changes, read from its paths.
#[allow(clippy::too_many_arguments)]
pub async fn judge(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    plan: &PlanResult,
    evidence: &mut RunEvidence,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Option<Vec<Finding>> {
    let diff = match github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await
    {
        Ok(d) => {
            let paths: Vec<String> = d["files"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|f| f["filename"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            evidence.environments = super::completeness::changed_environments(&paths);
            super::pr::format_diff_patches(&d)
        }
        Err(e) => {
            warn!(error = %e, "Proof check could not read the diff");
            return None;
        }
    };
    let evidence = &*evidence;
    let prompt = judge_prompt(
        &msg.title,
        &plan_context(plan),
        &diff,
        evidence,
        &msg.team_instructions,
    );
    match provider::converse_simple(
        state,
        provider,
        provider.heavy_model_id(),
        JUDGE_SYSTEM,
        &prompt,
        usage,
    )
    .await
    {
        Ok(reply) => {
            let parsed = parse_findings(&reply).map(|f| classify(f, evidence));
            if parsed.is_none() {
                warn!(
                    reply = common::truncate_str(&reply, 300),
                    "Proof check reply was unreadable"
                );
            }
            parsed
        }
        Err(e) => {
            warn!(error = %e, "Proof check errored");
            None
        }
    }
}

/// Everything a fix round needs, as the pipeline has it.
pub struct Work<'a> {
    pub state: &'a WorkerState,
    pub msg: &'a TicketMessage,
    pub github: &'a GitHubClient,
    pub plan: &'a PlanResult,
    pub branch: &'a str,
    pub rules: &'a [String],
    pub repo_instructions: &'a str,
    pub provider: &'a ModelProvider,
    pub file_cache: &'a FileCache,
    pub run_id: &'a str,
    /// When the fix round must wrap up (the invocation's deadline).
    pub deadline: Option<std::time::Instant>,
}

/// Judges the change, runs the fix round a gap earns, and judges again.
/// Returns what is still open when the PR is about to open: proof gaps,
/// checks the PR's CI will settle, and checks only a person can do (empty
/// when everything is proven or the judge couldn't answer). `evidence` gains
/// what the fix round looked up.
pub async fn prove(
    w: &Work<'_>,
    evidence: &mut RunEvidence,
    usage: &mut TokenUsage,
    has_time: impl Fn() -> bool,
) -> Vec<Finding> {
    let mut rounds = 0;
    loop {
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let judged = judge(
            w.state, w.msg, w.github, w.branch, w.plan, evidence, w.provider, usage,
        )
        .await;
        let recorded = recorded_gaps(evidence).len();
        let findings = with_checklist(
            with_recorded(judged, evidence),
            w.plan.fix.as_ref(),
            &format!("{}/{}", w.msg.repo_owner, w.msg.repo_name),
        );
        super::write_pass_trace(
            w.state,
            &w.msg.team_id,
            w.run_id,
            "proof",
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;
        info!(
            run_id = w.run_id,
            judged = findings.is_some(),
            gaps = findings.as_ref().map_or(0, Vec::len),
            recorded,
            rounds,
            "Proof check complete"
        );
        match next(findings.as_deref(), rounds, has_time()) {
            Next::Open => {
                if findings.is_some() {
                    super::add_progress_note(
                        w.state,
                        &w.msg.team_id,
                        w.run_id,
                        "Proof check passed: what the change rests on is confirmed",
                    )
                    .await;
                }
                // What CI or a person settles still reaches the PR.
                return findings.unwrap_or_default();
            }
            Next::Unfinished => {
                let gaps = findings.unwrap_or_default();
                super::add_progress_note(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    &format!(
                        "Proof check: {} planned task(s) still not done; the pull request won't open without them",
                        planned_work(&gaps).len()
                    ),
                )
                .await;
                return gaps;
            }
            Next::OpenWithGaps => {
                let gaps = findings.unwrap_or_default();
                super::add_progress_note(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    &format!(
                        "Proof check: {} gap(s) remain, listed under Open items in the PR",
                        proof_gaps(&gaps)
                    ),
                )
                .await;
                return gaps;
            }
            Next::FixRound => {
                let gaps = findings.unwrap_or_default();
                rounds += 1;
                super::add_progress_note(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    &format!("Proof check found {} gap(s), fixing", proof_gaps(&gaps)),
                )
                .await;
                if super::check_cancelled(w.state, &w.msg.team_id, w.run_id)
                    .await
                    .is_err()
                {
                    return gaps;
                }
                let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "implement").await;
                let pass_start = std::time::Instant::now();
                let usage_before = usage.clone();
                // Planned work may need files the branch doesn't change yet.
                let scoped = planned_work(&gaps).is_empty();
                let fixed = super::implement::run(
                    w.state,
                    w.msg,
                    w.github,
                    w.plan,
                    w.branch,
                    w.rules,
                    w.repo_instructions,
                    Some(&fix_feedback(&gaps)),
                    "medium",
                    w.provider,
                    usage,
                    w.file_cache,
                    Some(w.run_id),
                    w.deadline,
                    scoped, // bot-driven proof fix — scoped to the PR
                )
                .await;
                super::write_pass_trace(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    "implement:proof_fix",
                    pass_start,
                    &usage_before,
                    usage,
                    None,
                )
                .await;
                match fixed {
                    Ok(r) => evidence.merge(r.evidence),
                    Err(e) => {
                        warn!(run_id = w.run_id, error = %e, "Proof fix round failed");
                        return gaps;
                    }
                }
                let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "review").await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(point: &str, detail: &str) -> Finding {
        Finding {
            point: point.into(),
            detail: detail.into(),
            ..Default::default()
        }
    }

    fn check_finding(point: &str, check: &str) -> Finding {
        Finding {
            point: point.into(),
            detail: format!("{check} did not run before the PR opened"),
            check: check.into(),
            ..Default::default()
        }
    }

    fn answered(id: &str, source: &str, asked: &str, got: &str) -> Lookup {
        Lookup::new(id.into(), source, LookupOutcome::Answered, asked, got)
    }

    fn ended(id: &str, source: &str, outcome: LookupOutcome) -> Lookup {
        Lookup::new(
            id.into(),
            source,
            outcome,
            "GET /v1/orders/7/lines",
            "0 items",
        )
    }

    #[test]
    fn a_source_no_lookup_read_is_a_gap_whatever_the_judge_says() {
        // The run asked the vendor API twice: one empty list, one failure.
        // The judge passed the change; the record still holds it.
        let ev = RunEvidence {
            sources: vec!["vendor (api)".into(), "warehouse (database)".into()],
            lookups: vec![
                ended("T1", "vendor", LookupOutcome::Empty),
                ended("T2", "vendor", LookupOutcome::Failed),
                answered("L1", "warehouse", "select count(*)", "42"),
            ],
            notes: "Evidence: relies on `line_type` from the vendor's order lines.".into(),
            // CI runs every check: still nothing CI settles.
            ci_gates: ["lint", "typecheck", "build", "tests"]
                .iter()
                .filter_map(|k| CheckKind::parse(k))
                .collect(),
            ..Default::default()
        };
        let gaps = recorded_gaps(&ev);
        assert_eq!(gaps.len(), 1, "{gaps:?}");
        assert_eq!(gaps[0].point, "external_fact");
        assert!(gaps[0].detail.contains("No lookup against vendor"));
        assert!(gaps[0]
            .detail
            .contains("1 failed, 1 came back empty: T1, T2"));
        let judged = with_recorded(Some(vec![]), &ev).unwrap();
        assert_eq!(judged, gaps);
        assert_eq!(next(Some(&judged), 0, true), Next::FixRound);
        // A judge that couldn't answer doesn't wave it through either.
        assert_eq!(with_recorded(None, &ev), Some(gaps.clone()));
        // A real proof gap: never waiting on CI, and it is an Open item in
        // the PR, with how to settle it.
        assert!(judged.iter().all(|f| f.settler() == Settler::Proof));
        let with_ci = with_recorded(Some(vec![check_finding("ci_pending", "tests")]), &ev).unwrap();
        assert_eq!(proof_gaps(&with_ci), 1);
        let top = gaps_section(&judged);
        assert!(top.starts_with(UNVERIFIED_START), "{top}");
        assert!(top.contains("## Open items"));
        assert!(!top.contains("[!WARNING]"));
        assert!(top
            .contains("\n- **Unconfirmed external field or behaviour:** No lookup against vendor"));
        assert!(top.contains("Settle: Read one real record from vendor"));
        assert!(!top.contains(CI_MARK));
        assert_eq!(settle_by_ci(&format!("{top}Body")), None);
        assert_eq!(unconfirmed_facts(&top).len(), 1);
        // The entries say so to the judge and the PR writer.
        assert!(ev
            .for_judge()
            .contains("[T1] GET /v1/orders/7/lines\n  Came back EMPTY"));
        assert!(ev
            .for_judge()
            .contains("[T2] GET /v1/orders/7/lines\n  FAILED"));

        // One real record from the vendor settles it.
        let mut fixed = ev.clone();
        fixed.merge(RunEvidence {
            lookups: vec![answered(
                "T1",
                "vendor",
                "GET /v1/orders/8/lines",
                "[{\"line_type\": \"fee\"}]",
            )],
            ..Default::default()
        });
        assert!(recorded_gaps(&fixed).is_empty());
        assert_eq!(with_recorded(None, &fixed), None);
        assert_eq!(with_recorded(Some(vec![]), &fixed), Some(vec![]));
    }

    #[test]
    fn a_fact_the_notes_mark_unconfirmed_is_a_gap_until_a_round_confirms_it() {
        let mut ev = RunEvidence {
            notes: "Fixed the sync.\n\nEvidence:\n- Confirmed: `id` is numeric (L1 came back 7)\n- Unconfirmed: `line_type` values on refunds; the payload was never read".into(),
            ..Default::default()
        };
        let gaps = recorded_gaps(&ev);
        assert_eq!(gaps.len(), 1);
        assert!(gaps[0]
            .detail
            .contains("`line_type` values on refunds; the payload was never read"));
        ev.merge(RunEvidence {
            notes: "Evidence:\n- Confirmed: `line_type` is `refund` on refunds (T3)".into(),
            ..Default::default()
        });
        assert!(recorded_gaps(&ev).is_empty(), "{:?}", recorded_gaps(&ev));
    }

    #[test]
    fn unconfirmed_lines_are_read_in_every_form_they_take() {
        let text = "Evidence:\n- Confirmed: a\n- Unconfirmed: the vendor's `status` field\n* **Not verified:** refunds carry `amount`\n> - **Unconfirmed external field or behaviour:** confirm `name` from the vendor API\nunverified:   \nThe payload is unconfirmed: prose isn't a marker";
        assert_eq!(
            unconfirmed_facts(text),
            vec![
                "the vendor's `status` field".to_string(),
                "refunds carry `amount`".to_string(),
                "confirm `name` from the vendor API".to_string(),
            ]
        );
        // What the PR's top note says about an open gap reads back the same.
        let top = gaps_section(&[finding(
            "external_fact",
            "confirm `name` from the vendor API",
        )]);
        assert_eq!(
            unconfirmed_facts(&top),
            vec!["confirm `name` from the vendor API".to_string()]
        );
        assert!(unconfirmed_facts(&gaps_section(&[finding("logging", "x")])).is_empty());
        assert!(EVIDENCE_OUTPUT.contains("`- Unconfirmed [external:<the system>]:`"));
        assert!(EVIDENCE_OUTPUT
            .contains("`- Unconfirmed [check:<lint|typecheck|tests|build|format>]:`"));
        assert!(JUDGE_SYSTEM.contains("FAILED or EMPTY confirms nothing"));
    }

    #[test]
    fn reads_the_judges_findings() {
        let reply = "Looked at it.\n```json\n{\"findings\":[{\"point\":\"external_fact\",\"detail\":\"`name` on products is never read from a real payload\"},{\"point\":\"logging\",\"detail\":\"\"}]}\n```";
        let f = parse_findings(reply).unwrap();
        assert_eq!(
            f,
            vec![finding(
                "external_fact",
                "`name` on products is never read from a real payload"
            )]
        );
        assert_eq!(parse_findings("{\"findings\":[]}"), Some(vec![]));
        assert_eq!(parse_findings("LGTM"), None);
        assert_eq!(parse_findings("```json\n{\"findings\": [\n"), None);
    }

    #[test]
    fn a_gap_earns_one_fix_round_then_opens_with_it_named() {
        let gaps = [finding("impact", "affected records not counted")];
        assert_eq!(next(Some(&gaps), 0, true), Next::FixRound);
        assert_eq!(next(Some(&gaps), 1, true), Next::OpenWithGaps);
        assert_eq!(next(Some(&gaps), 0, false), Next::OpenWithGaps);
        assert_eq!(next(Some(&[]), 0, true), Next::Open);
        // A judge that couldn't answer never holds the PR.
        assert_eq!(next(None, 0, true), Next::Open);
    }

    #[test]
    fn gaps_are_open_items_and_the_fix_round_gets_them() {
        let gaps = [
            finding(
                "external_fact",
                "confirm `name` on products from the vendor API",
            ),
            finding("logging", "INFO log per record in the sync loop"),
        ];
        let top = gaps_section(&gaps);
        assert!(top.starts_with("<!-- coderhelm:unverified -->\n## Open items\n- "));
        assert!(top.ends_with("<!-- /coderhelm:unverified -->"));
        assert!(!top.contains("[!"), "no banner: {top}");
        assert!(top.contains("\n- **Unconfirmed external field or behaviour:** confirm `name`"));
        assert!(top.contains("\n- **Noisy logging:** INFO log per record"));
        assert_eq!(gaps_section(&[]), "");
        let fb = fix_feedback(&gaps);
        assert!(fb.contains("- **Noisy logging:** INFO log per record in the sync loop"));
        assert!(fb.contains("write your `Evidence:` section again"));
    }

    #[test]
    fn the_judge_sees_sources_lookups_and_notes() {
        let ev = RunEvidence {
            sources: vec!["warehouse (database)".into()],
            lookups: vec![answered("L1", "warehouse", "select count(*) ...", "42")],
            notes: "Evidence: 42 records affected".into(),
            ..Default::default()
        };
        let p = judge_prompt("Fix sync", "Use the vendor field", "+ x", &ev, "");
        assert!(p.contains("warehouse (database)"));
        assert!(p.contains("Came back: 42"));
        assert!(p.contains("Evidence: 42 records affected"));
        assert!(p.contains("## The diff\n+ x"));
        let empty = judge_prompt("t", "", "", &RunEvidence::default(), "");
        assert!(!empty.contains("team's instructions"));
        assert!(empty.contains("(none connected)"));
        assert!(empty.contains("(no lookups were made)"));
        assert!(empty.contains("(the coder wrote none)"));
        for point in [
            "external_fact",
            "impact",
            "logging",
            "assumption",
            "structured_field",
            "survey",
            "silent_fallback",
            "fixtures",
            "one_record",
            "team_rule",
            "ci_pending",
            "manual_check",
            "pattern_cited",
            "end_to_end",
            "environment",
        ] {
            assert!(JUDGE_SYSTEM.contains(point), "{point}");
            assert_ne!(finding(point, "x").label(), point, "{point} has a label");
        }
        assert!(JUDGE_SYSTEM.contains("A WARNING for an unexpected shape is not noisy logging"));
    }

    #[test]
    fn the_judge_holds_the_change_to_the_teams_instructions_when_set() {
        let team = "Every PR description has a Rollback section.";
        let p = judge_prompt("t", "plan", "+ x", &RunEvidence::default(), team);
        assert!(p.contains("## Your team's instructions"));
        assert!(p.contains(team));
    }

    #[test]
    fn a_fix_round_adds_to_the_runs_evidence() {
        let one = answered("L1", "vendor", "GET /v1/one", "{}");
        let two = answered("L1", "vendor", "GET /v1/two", "{}");
        let mut ev = RunEvidence {
            sources: vec!["vendor (api)".into()],
            lookups: vec![one.clone()],
            notes: "Evidence: none needed".into(),
            ..Default::default()
        };
        ev.merge(RunEvidence {
            sources: vec!["vendor (api)".into()],
            lookups: vec![two.clone()],
            notes: "Evidence: field confirmed".into(),
            ..Default::default()
        });
        assert_eq!(ev.sources, vec!["vendor (api)".to_string()]);
        assert_eq!(ev.lookups, vec![one, two]);
        assert!(ev
            .notes
            .ends_with("After a fix round:\nEvidence: field confirmed"));
        assert!(ev.for_pr().contains("GET /v1/two"));
        assert_eq!(RunEvidence::default().for_pr(), "");
    }

    #[test]
    fn the_rules_cover_all_four_points() {
        assert!(CODER_RULES.contains("Prove an assumption before you write code on it"));
        assert!(CODER_RULES.contains("count the records the change affects"));
        assert!(CODER_RULES.contains("diagnostics go to DEBUG"));
        assert!(CODER_RULES.contains("say exactly which source is missing"));
        assert!(CODER_RULES.contains("structured fields and flags"));
        // Real records come before any rule, and text matching is the last resort.
        assert!(CODER_RULES.contains("Actual data first"));
        assert!(CODER_RULES.contains("last resort"));
        assert!(JUDGE_SYSTEM.contains("last resort"));
        assert!(CODER_RULES.contains("Survey a field across all live records"));
        assert!(CODER_RULES.contains("log a WARNING with the record id"));
        assert!(CODER_RULES.contains("fixtures from real payloads"));
        assert!(CODER_RULES.contains("No manual backfills or one-off scripts"));
        assert!(CODER_RULES.contains("no hard-coded ids, names, seasons or prices"));
        assert!(EVIDENCE_OUTPUT.contains("`Evidence:`"));
    }

    #[test]
    fn the_coder_copies_the_existing_block_names_it_and_confirms_each_environment() {
        assert!(CODER_RULES.contains("find that block and copy it"));
        assert!(CODER_RULES.contains("another user, secret, certificate, route, config entry"));
        assert!(CODER_RULES.contains("one `- Confirmed:` line per environment"));
        assert!(CODER_RULES.contains("exactly what access is missing"));
        assert!(CODER_RULES.contains("never a template left as `${env}`"));
        assert!(EVIDENCE_OUTPUT.contains("```mirrors.json"));
        assert!(
            EVIDENCE_OUTPUT.contains("\"differences\": [\"each intentional difference, and why\"]")
        );
        // The judge holds each one, and names the missing pieces.
        assert!(JUDGE_SYSTEM.contains("13. pattern_cited"));
        assert!(JUDGE_SYSTEM.contains("no `mirrors.json` entry naming that existing block"));
        assert!(JUDGE_SYSTEM.contains("14. end_to_end"));
        assert!(JUDGE_SYSTEM.contains("network access and allowlists"));
        assert!(JUDGE_SYSTEM.contains("15. environment"));
        assert_eq!(finding("pattern_cited", "x").settler(), Settler::Proof);
        assert_eq!(finding("environment", "x").settler(), Settler::Proof);
        assert_eq!(finding("end_to_end", "x").settler(), Settler::Owner);
    }

    #[test]
    fn an_unconfirmed_environment_is_a_gap_whatever_the_judge_says() {
        let mut ev = RunEvidence {
            notes: "Evidence:\n- Confirmed: the prod secret exists (L1)".into(),
            environments: vec!["prod".into(), "staging".into()],
            ..Default::default()
        };
        assert!(ev
            .for_judge()
            .contains("Environments the diff changes (read from its file paths)\nprod, staging\n"));
        // The judge named it too: one account, the code's.
        let judged = Some(vec![finding("environment", "staging unchecked")]);
        let all = with_recorded(judged, &ev).unwrap();
        assert_eq!(all.len(), 1, "{all:?}");
        assert!(all[0].detail.contains("changes the staging environment"));
        assert_eq!(next(Some(&all), 0, true), Next::FixRound);
        // Saying exactly what access is missing is an Open item, not silence.
        ev.merge(RunEvidence {
            notes: "Evidence:\n- Confirmed: the prod secret exists (L1)\n- Unconfirmed: staging, no read role in the staging account".into(),
            ..Default::default()
        });
        let left = recorded_gaps(&ev);
        assert_eq!(left.len(), 1, "{left:?}");
        assert!(left[0]
            .detail
            .contains("no read role in the staging account"));
        assert!(gaps_section(&left).contains("staging, no read role"));
    }

    #[test]
    fn an_unowned_step_is_an_open_item_never_a_fix_round() {
        let d = crate::passes::fix_location::FixDecision {
            end_to_end: vec![crate::passes::fix_location::EndToEndStep {
                step: "Allow the vendor's IP range".into(),
                by: "person".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let all = with_checklist(Some(vec![]), Some(&d), "acme/infra").unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(next(Some(&all), 0, true), Next::Open);
        let open = gaps_section(&all);
        assert!(open.contains("- **Missing from the end-to-end checklist:** No owner is named for: Allow the vendor's IP range"));
        assert!(!fix_feedback(&all).contains("No owner"));
        assert_eq!(with_checklist(None, None, "acme/infra"), None);
    }

    fn gated(kinds: &[CheckKind]) -> RunEvidence {
        RunEvidence {
            ci_gates: kinds.to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn a_check_ci_runs_on_the_pr_is_pending_not_a_gap_whatever_the_judge_called_it() {
        let ev = gated(&[CheckKind::Lint, CheckKind::Tests]);
        // The incident: the team rule "run lint and tests before pushing"
        // came back as team_rule; CI runs both on the PR, so CI settles them.
        let f = classify(
            vec![
                check_finding("team_rule", "lint"),
                check_finding("ci_pending", "tests"),
                check_finding("ci_pending", "build"),
                finding("external_fact", "`name` never read from a payload"),
                finding("manual_check", "open the drawer in a real browser"),
            ],
            &ev,
        );
        let points: Vec<&str> = f.iter().map(|f| f.point.as_str()).collect();
        assert_eq!(
            points,
            [
                "ci_pending",
                "ci_pending",
                // CI doesn't build: nothing settles it after the PR opens.
                "unrun_check",
                "external_fact",
                "manual_check"
            ]
        );
        assert_eq!(f[0].settler(), Settler::Ci);
        assert_eq!(f[2].settler(), Settler::Proof);
        assert_eq!(f[4].settler(), Settler::Person);
        // A pending item naming no kind waits on CI only when CI runs anything.
        let unnamed = || vec![finding("ci_pending", "checks did not run")];
        assert_eq!(classify(unnamed(), &ev)[0].point, "ci_pending");
        assert_eq!(classify(unnamed(), &gated(&[]))[0].point, "unrun_check");
    }

    #[test]
    fn only_proof_gaps_earn_a_fix_round() {
        let ev = gated(&[CheckKind::Lint]);
        let open = classify(
            vec![
                check_finding("team_rule", "lint"),
                finding("manual_check", "click through checkout on a phone"),
            ],
            &ev,
        );
        assert_eq!(next(Some(&open), 0, true), Next::Open);
        let fb = fix_feedback(&[
            finding("impact", "affected records not counted"),
            open[0].clone(),
        ]);
        assert!(fb.contains("Impact not measured"));
        assert!(!fb.contains("Waiting on CI"));
    }

    #[test]
    fn a_warning_bullet_is_one_short_sentence_on_what_and_one_on_how() {
        let f = Finding {
            point: "impact".into(),
            detail: "Affected bookings were not counted. The notes say \"not measured\" and \
                     list nothing else at all."
                .into(),
            settle: format!(
                "Count them in the warehouse, e.g. by season. Then {}",
                "x ".repeat(200)
            ),
            ..Default::default()
        };
        let line = f.line();
        assert_eq!(
            line,
            "- **Impact not measured:** Affected bookings were not counted. Settle: Count them \
             in the warehouse, e.g. by season."
        );
        // A run-on sentence is cut at a word boundary.
        let long = sentence(&"word ".repeat(100), 30);
        assert_eq!(long, "word word word word word word…");
        assert!(long.chars().count() <= 31);
        // A pending item says what settles it even when the judge didn't.
        let p = check_finding("ci_pending", "lint");
        assert!(p.line().ends_with("Settles when this PR's CI passes."));
        assert!(!JUDGE_SYSTEM.contains("Breaks the team's instructions"));
    }

    #[test]
    fn only_ci_pending_items_make_a_note_and_hand_checks_never_reach_it() {
        let ev = gated(&[CheckKind::Lint, CheckKind::Tests]);
        let f = classify(
            vec![
                check_finding("team_rule", "lint"),
                finding("manual_check", "open the page in a real browser"),
            ],
            &ev,
        );
        let top = gaps_section(&f);
        assert!(top.contains("## Open items"));
        assert!(!top.contains("[!NOTE]"));
        assert!(top.contains("\n- **Waiting on CI:** lint did not run"));
        assert!(top.contains("<!-- coderhelm:ci -->"));
        assert!(!top.contains("browser"));
        assert_eq!(
            manual_steps(&f),
            "- open the page in a real browser\n",
            "a hand check is a Verification step"
        );
        assert_eq!(gaps_section(&[finding("manual_check", "on a device")]), "");
        assert_eq!(manual_steps(&[]), "");
    }

    #[test]
    fn ci_passing_removes_what_it_settled_and_the_section_when_nothing_is_left() {
        let ev = gated(&[CheckKind::Lint]);
        let pending = classify(vec![check_finding("team_rule", "lint")], &ev);
        let rest = "Closes #4\n\n**Problem** x\n\n**Verification**\n1. y";
        let footer = "\n\n<sub>Consulted team agents: a</sub>";
        let body = format!("{rest}\n\n{}{footer}", gaps_section(&pending));
        assert_eq!(
            settle_by_ci(&body).as_deref(),
            Some(format!("{rest}{footer}").as_str())
        );

        // A proof gap stays, under its heading at the bottom; only the CI
        // item goes.
        let mut mixed = pending.clone();
        mixed.insert(0, finding("impact", "affected records not counted"));
        let body = format!("{rest}\n\n{}{footer}", gaps_section(&mixed));
        let settled = settle_by_ci(&body).unwrap();
        assert!(settled.starts_with(rest));
        assert!(settled.contains(
            "\n\n<!-- coderhelm:unverified -->\n## Open items\n- **Impact not measured:**"
        ));
        assert!(!settled.contains("Waiting on CI"));
        assert!(settled.ends_with(&format!("<!-- /coderhelm:unverified -->{footer}")));
        // Nothing left for CI to settle: the description is left alone.
        assert_eq!(settle_by_ci(&settled), None);
        assert_eq!(settle_by_ci(rest), None);

        // A PR from before, with the banner at the top: the CI item goes and
        // what's left moves to the bottom as Open items.
        let old = format!(
            "<!-- coderhelm:unverified -->\n> [!WARNING]\n> **Not yet verified**\n> - **Impact not measured:** x\n> - **Waiting on CI:** lint <!-- coderhelm:ci -->\n<!-- /coderhelm:unverified -->\n\n{rest}"
        );
        let moved = settle_by_ci(&old).unwrap();
        assert!(moved.starts_with("Closes #4"), "{moved}");
        assert!(moved.ends_with(
            "## Open items\n- **Impact not measured:** x\n<!-- /coderhelm:unverified -->"
        ));
        assert!(!moved.contains("[!WARNING]"));
    }

    #[test]
    fn the_section_splits_off_whole_for_a_description_rewrite() {
        let gaps = gaps_section(&[finding("impact", "affected records not counted")]);
        let body = format!("Closes #4\n\nx\n\n{gaps}\n");
        let split = |b: &str| {
            super::super::completeness::split_section(b, UNVERIFIED_START, UNVERIFIED_END)
        };
        let (section, rest) = split(&body);
        assert_eq!(section, format!("{gaps}\n"));
        assert_eq!(rest, "Closes #4\n\nx\n\n");
        assert_eq!(split("Closes #4"), (String::new(), "Closes #4".to_string()));
    }

    #[test]
    fn the_judge_reads_which_checks_ran_and_which_ci_runs() {
        let ev = RunEvidence {
            checks: vec!["- typecheck: PASS, `pnpm check-types`".into()],
            ci_gates: vec![CheckKind::Lint, CheckKind::Tests],
            ..Default::default()
        };
        let p = judge_prompt("t", "", "", &ev, "");
        assert!(p.contains("recorded by CoderHelm, not the coder)\n- typecheck: PASS"));
        assert!(p.contains("runs on this pull request\nlint, tests\n"));
        let none = judge_prompt("t", "", "", &RunEvidence::default(), "");
        assert!(none.contains("(no sandbox checks ran)"));
        assert!(none.contains("(none found: nothing checks this PR after it opens)"));

        // A later run's checks replace the earlier ones; CI's kinds add up.
        let mut ev = ev;
        ev.merge(RunEvidence {
            checks: vec!["- lint: FAIL (exit 1), `pnpm lint`".into()],
            ci_gates: vec![CheckKind::Typecheck],
            ..Default::default()
        });
        assert_eq!(
            ev.checks,
            vec!["- lint: FAIL (exit 1), `pnpm lint`".to_string()]
        );
        assert_eq!(
            ev.ci_gates,
            vec![CheckKind::Typecheck, CheckKind::Lint, CheckKind::Tests]
        );
    }

    /// An invented Terraform repository whose PR workflows format-check
    /// every PR and plan each environment, read the way `implement` reads
    /// them.
    fn terraform_ci() -> RunEvidence {
        use crate::passes::checks::{kinds_from, pr_commands, Workflow};
        let lint = Workflow {
            path: ".github/workflows/lint.yml".into(),
            text: "on:\n  pull_request:\n    types: [opened, synchronize, reopened, labeled]\njobs:\n  fmt:\n    steps:\n      - uses: hashicorp/setup-terraform@v3\n      - run: terraform fmt -recursive -check\n".into(),
        };
        let plan = Workflow {
            path: ".github/workflows/plan.yml".into(),
            text: "on: pull_request\njobs:\n  plan:\n    strategy:\n      matrix:\n        env: [staging, prod]\n    defaults:\n      run:\n        working-directory: envs/${{ matrix.env }}\n    steps:\n      - run: |\n          terraform init -input=false\n          terraform plan -input=false -no-color\n".into(),
        };
        let ci = pr_commands(&[lint, plan], "main");
        RunEvidence {
            ci_gates: kinds_from(&ci),
            ci_commands: ci,
            ..Default::default()
        }
    }

    #[test]
    fn terraform_checks_ci_runs_wait_on_ci_and_clear_when_it_passes() {
        // The incident: the judge told a person to run `terraform fmt -check`
        // and `terraform plan`, which the repo's PR workflows already run.
        let ev = terraform_ci();
        let judged = vec![
            Finding {
                point: "manual_check".into(),
                detail: "Formatting of the changed .tf files was not checked.".into(),
                settle: "Run terraform fmt -check.".into(),
                command: "terraform fmt -check".into(),
                ..Default::default()
            },
            Finding {
                point: "assumption".into(),
                detail: "The plan for staging and prod was not reviewed.".into(),
                settle: "Run terraform plan for staging and prod.".into(),
                check: "plan".into(),
                command: "terraform plan".into(),
                ..Default::default()
            },
            // Tied only by the CI id the judge gave: code checks it exists.
            Finding {
                point: "ci_pending".into(),
                detail: "Formatting did not run in the sandbox.".into(),
                ci: "[C1]".into(),
                ..Default::default()
            },
            // An id CI doesn't have ties nothing.
            Finding {
                point: "ci_pending".into(),
                detail: "tflint did not run.".into(),
                command: "tflint".into(),
                ci: "C9".into(),
                ..Default::default()
            },
            Finding {
                point: "impact".into(),
                detail: "Affected buckets were not counted.".into(),
                ci: "C1".into(),
                ..Default::default()
            },
        ];
        let f = classify(judged, &ev);
        let got: Vec<(&str, &str, &str)> = f
            .iter()
            .map(|f| (f.point.as_str(), f.check.as_str(), f.ci.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("ci_pending", "format", "C1"),
                ("ci_pending", "plan", "C2"),
                ("ci_pending", "format", "C1"),
                // An unrun check names its kind.
                ("unrun_check", "lint", ""),
                ("impact", "", ""),
            ]
        );
        // Neither CI-run check is a gap or a step for a person.
        assert_eq!(manual_steps(&f[..3]), "");
        assert_eq!(next(Some(&f[..3]), 0, true), Next::Open);
        let top = gaps_section(&f[..3]);
        assert!(top.contains("## Open items"));
        assert!(!top.contains("[!NOTE]"));
        assert!(top.contains(
            "CI runs `terraform fmt -recursive -check` on this PR; this settles when it passes."
        ));
        assert!(top.contains("CI runs `terraform plan -input=false -no-color` (in `envs/${{ matrix.env }}`) on this PR"));
        assert!(!top.contains("Run terraform"));
        // A green CI clears them, and the section with them, from the
        // bottom of the description.
        let rest = "Source ticket: OPS-1\n\n**Problem** x";
        let body = crate::passes::completeness::assemble(
            rest,
            &crate::passes::completeness::Owned {
                open_items: top.clone(),
                ..Default::default()
            },
        );
        assert!(body.ends_with("<!-- /coderhelm:unverified -->"), "{body}");
        assert_eq!(settle_by_ci(&body).as_deref(), Some(rest));
        // A workflow expression in a CI command is not an unfilled template.
        assert!(
            crate::passes::completeness::placeholder_holes(&body).is_empty(),
            "{:?}",
            crate::passes::completeness::placeholder_holes(&body)
        );

        // Without CI running them: formatting is a gap, a plan is for a
        // person (the sandbox never has credentials).
        let none = classify(
            vec![
                check_finding("ci_pending", "format"),
                check_finding("ci_pending", "plan"),
            ],
            &RunEvidence::default(),
        );
        assert_eq!(none[0].point, "unrun_check");
        assert_eq!(none[1].point, "manual_check");
    }

    #[test]
    fn the_judge_reads_each_command_ci_runs_with_its_id() {
        let ev = terraform_ci();
        let p = judge_prompt("t", "", "", &ev, "");
        assert!(p.contains(
            "runs on this pull request\n- [C1] format (terraform fmt): `terraform fmt -recursive -check` (workflow `lint.yml`, job `fmt`)\n- [C2] plan (terraform plan):"
        ));
        assert!(JUDGE_SYSTEM.contains("\"ci\":\"the CI command id (C1), or empty\""));
        assert!(JUDGE_SYSTEM.contains("Never a command the CI list shows"));
        let reply = "```json\n{\"findings\":[{\"point\":\"ci_pending\",\"detail\":\"fmt did not run\",\"check\":\"format\",\"command\":\"terraform fmt -check\",\"ci\":\"C1\"}]}\n```";
        let f = parse_findings(reply).unwrap();
        assert_eq!(
            (f[0].command.as_str(), f[0].ci.as_str()),
            ("terraform fmt -check", "C1")
        );
        // The ids stay those the judge saw when a fix round adds evidence.
        let mut merged = ev.clone();
        merged.merge(terraform_ci());
        assert_eq!(merged.ci_commands, ev.ci_commands);
    }

    #[test]
    fn the_judge_writes_short_findings_with_how_to_settle_and_the_check_kind() {
        let reply = "```json\n{\"findings\":[{\"point\":\"ci_pending\",\"detail\":\"lint did not run\",\"settle\":\"The PR's lint check.\",\"check\":\"lint\"}]}\n```";
        let f = parse_findings(reply).unwrap();
        assert_eq!(f[0].settle, "The PR's lint check.");
        assert_eq!(f[0].check, "lint");
        assert!(JUDGE_SYSTEM.contains("`detail` is ONE sentence"));
        assert!(JUDGE_SYSTEM.contains("\"settle\":\"one sentence\""));
        assert!(JUDGE_SYSTEM.contains("is not a team_rule miss"));
    }

    fn app_ci() -> Vec<CiCommand> {
        // An app whose PR workflow runs the type-check and the tests, not lint.
        crate::passes::checks::pr_commands(
            &[crate::passes::checks::Workflow {
                path: ".github/workflows/pr.yml".into(),
                text: "on: pull_request\njobs:\n  ci:\n    steps:\n      - run: npm ci\n      - run: npm run typecheck\n      - run: npm test -- --ci\n".into(),
            }],
            "develop",
        )
    }

    #[test]
    fn unconfirmed_lines_say_what_they_are_about() {
        let text = "Evidence:\n\
            - Unconfirmed [check:lint]: lint did not run in the sandbox\n\
            - Unconfirmed [checks:typecheck, tests]: never confirmed to pass\n\
            - Unconfirmed [external:billing API]: refunds carry `amount`\n\
            - Unconfirmed: lint, type-check and tests passing\n\
            - Unconfirmed: `npm run lint` was not run\n\
            - Unconfirmed: the vendor's `status` field passes validation\n\
            - **Unconfirmed external field or behaviour:** confirm `name`";
        let items = unconfirmed_items(text);
        let about: Vec<&About> = items.iter().map(|u| &u.about).collect();
        assert_eq!(
            about,
            vec![
                &About::Checks(vec![CheckKind::Lint]),
                &About::Checks(vec![CheckKind::Typecheck, CheckKind::Tests]),
                &About::External(Some("billing API".into())),
                &About::Checks(vec![
                    CheckKind::Lint,
                    CheckKind::Typecheck,
                    CheckKind::Tests
                ]),
                &About::Checks(vec![CheckKind::Lint]),
                &About::Unknown,
                &About::External(None),
            ]
        );
        assert_eq!(items[2].fact, "refunds carry `amount`");
        // Only what isn't about the repository's own checks is an outside fact,
        // in a review as before a PR opens.
        assert_eq!(
            unconfirmed_facts(text),
            vec![
                "refunds carry `amount`".to_string(),
                "the vendor's `status` field passes validation".to_string(),
                "confirm `name`".to_string(),
            ]
        );
    }

    #[test]
    fn a_sandbox_record_reads_each_steps_result() {
        use crate::passes::checks::Step;
        let steps = [
            Step {
                kinds: vec![CheckKind::Typecheck],
                cmd: "npm ci && npm run typecheck".into(),
                scope: None,
            },
            Step {
                kinds: vec![CheckKind::Tests],
                cmd: "npm test -- a.test.tsx".into(),
                scope: None,
            },
            Step {
                kinds: vec![CheckKind::Lint],
                cmd: "npm run lint".into(),
                scope: None,
            },
        ];
        let r = SandboxRecord::from_steps(&steps, &[Some(0), Some(1), None]);
        assert_eq!(r.ran, vec![CheckKind::Typecheck, CheckKind::Tests]);
        assert_eq!(r.passed, vec![CheckKind::Typecheck]);
        assert_eq!(r.failure, None);
        assert!(r
            .why_not(CheckKind::Lint)
            .unwrap()
            .contains("has no lint step"));
        assert_eq!(r.why_not(CheckKind::Typecheck), None);
    }

    /// The incident, with invented names: an app whose CI runs
    /// the type-check and the tests but not lint; the coder's notes marked
    /// "lint, type-check and tests passing" unconfirmed; the sandbox gave no
    /// result four times; the judge said lint never ran.
    #[test]
    fn the_repos_own_checks_are_never_an_outside_fact() {
        let ci = app_ci();
        let ev = RunEvidence {
            notes: "Moved the card to the shared button.\n\nEvidence:\n\
                    - Confirmed: the button takes `overrideOnPress` (read src/Button.tsx)\n\
                    - Unconfirmed: lint, type-check and tests passing\n\
                    - Unconfirmed: lint, type-check and tests passing"
                .into(),
            checks: vec![
                "- typecheck: no result (the run stopped before this step reported), `npm ci && npm run typecheck`".into(),
                "- sandbox: no result, the sandbox build ended SUCCEEDED and printed no result, 4 builds in a row".into(),
            ],
            sandbox: SandboxRecord {
                failure: Some(
                    "the sandbox build ended SUCCEEDED and printed no result, 4 builds in a row"
                        .into(),
                ),
                ..Default::default()
            },
            ci_gates: crate::passes::checks::kinds_from(&ci),
            ci_commands: ci,
            ..Default::default()
        };
        assert_eq!(ev.ci_gates, vec![CheckKind::Typecheck, CheckKind::Tests]);
        let judged = classify(
            vec![Finding {
                point: "ci_pending".into(),
                detail: "Lint never ran: the sandbox stopped with no result.".into(),
                check: "lint".into(),
                command: "npm run lint".into(),
                ..Default::default()
            }],
            &ev,
        );
        let all = with_recorded(Some(judged), &ev).unwrap();
        let got: Vec<(&str, &str)> = all
            .iter()
            .map(|f| (f.point.as_str(), f.check.as_str()))
            .collect();
        // Lint once (the judge's and the notes' are the same item), the
        // type-check and tests wait on CI, and no outside fact at all.
        assert_eq!(
            got,
            vec![
                ("unrun_check", "lint"),
                ("ci_pending", "typecheck"),
                ("ci_pending", "tests"),
            ]
        );
        assert!(all.iter().all(|f| f.point != "external_fact"));
        // Lint names the sandbox's real failure and that CI doesn't run it.
        assert_eq!(
            all[0].detail,
            "Lint never ran before the PR: the sandbox build ended SUCCEEDED and printed no \
             result, 4 builds in a row, and the repository's CI does not run lint on this pull \
             request."
        );
        // CI settles the other two: they leave Open items when it passes.
        assert!(all[1].ci_cmd.contains("npm run typecheck"), "{:?}", all[1]);
        let section = gaps_section(&all);
        assert!(section.contains("- **Check not run:** Lint never ran before the PR"));
        assert!(!section.contains("Unconfirmed external field"));
        let after_ci = settle_by_ci(&format!("Body\n\n{section}")).unwrap();
        assert!(after_ci.contains("Check not run"));
        assert!(!after_ci.contains("Waiting on CI") && !after_ci.contains("CI runs `"));
        // Only lint is a proof gap: one fix round, then the PR opens with it.
        assert_eq!(proof_gaps(&all), 1);

        // Checks the sandbox passed are not held open by the notes.
        let mut passed = ev.clone();
        passed.sandbox = SandboxRecord {
            ran: vec![CheckKind::Typecheck, CheckKind::Tests],
            passed: vec![CheckKind::Typecheck, CheckKind::Tests],
            failure: None,
        };
        let kinds: Vec<String> = recorded_gaps(&passed)
            .into_iter()
            .map(|f| f.check)
            .collect();
        assert_eq!(kinds, vec!["lint".to_string()]);
        // An outside fact is still a proof gap, listed once.
        let mut vendor = ev.clone();
        vendor.notes = "Evidence:\n- Unconfirmed [external:billing API]: refunds carry `amount`\n\
                        - Unconfirmed: refunds carry `amount`"
            .into();
        let gaps = with_recorded(
            Some(vec![finding(
                "external_fact",
                "The evidence notes mark this unconfirmed: refunds carry `amount`",
            )]),
            &vendor,
        )
        .unwrap();
        assert_eq!(gaps.len(), 1, "{gaps:?}");
    }
}

#[cfg(test)]
mod planned_work_tests {
    use super::*;

    fn f(point: &str, detail: &str) -> Finding {
        Finding {
            point: point.into(),
            detail: detail.into(),
            ..Default::default()
        }
    }

    #[test]
    fn planned_work_gets_a_round_then_holds_the_pr() {
        let gaps = vec![
            f(
                "planned_work",
                "The tabs in src/blocks/gallery/tabs.tsx are not built.",
            ),
            f("impact", "Records affected were not counted."),
        ];
        assert_eq!(next(Some(&gaps), 0, true), Next::FixRound);
        // No round left: never "open with the gaps listed".
        assert_eq!(next(Some(&gaps), 1, true), Next::Unfinished);
        assert_eq!(next(Some(&gaps), 0, false), Next::Unfinished);
        // Other gaps alone still open with Open items.
        assert_eq!(next(Some(&gaps[1..]), 1, true), Next::OpenWithGaps);
        let why = unfinished_reason(&gaps);
        assert!(why.starts_with("Stopped before opening a pull request"));
        assert!(why.contains("tabs.tsx"));
        assert!(!why.contains("Records affected"));
        assert_eq!(gaps[0].settler(), Settler::Proof);
        assert!(fix_feedback(&gaps).contains("Planned work not done"));
    }

    #[test]
    fn optional_tasks_are_open_items_for_a_person() {
        let opt = f("optional_task", "`docs/x.md`: mention it");
        assert_eq!(opt.settler(), Settler::Owner);
        let section = gaps_section(std::slice::from_ref(&opt));
        assert!(section.contains("Optional task left out"));
        assert_eq!(next(Some(&[opt]), 1, true), Next::Open);
    }

    #[test]
    fn the_judge_sees_the_task_checklist() {
        let plan = PlanResult {
            proposal: "Add a gallery block.".into(),
            tasks: "- [ ] `src/a.ts`: Create it".into(),
            spec: String::new(),
            design: String::new(),
            repo_tasks: vec![],
            fix: None,
        };
        let ctx = plan_context(&plan);
        assert!(ctx.contains("The plan's task checklist"));
        assert!(ctx.contains("`src/a.ts`"));
    }
}

#[cfg(test)]
mod env_near_history_tests {
    use super::*;

    #[test]
    fn the_coder_and_the_judge_hold_environments_near_matches_and_history() {
        assert!(CODER_RULES.contains("## Which environment a lookup hit"));
        assert!(CODER_RULES.contains("data that only exists there"));
        assert!(CODER_RULES.contains("## Something close to what the ticket names"));
        assert!(CODER_RULES.contains("search the team's repositories for its name"));
        assert!(CODER_RULES.contains("## The ticket's earlier pull requests"));
        assert!(JUDGE_SYSTEM.contains("16. env_unproven"));
        assert!(JUDGE_SYSTEM.contains("17. near_match_unresolved"));
        assert!(JUDGE_SYSTEM.contains("18. history_used"));
        assert!(JUDGE_SYSTEM.contains("19. planned_work"));
        assert!(
            JUDGE_SYSTEM.contains("environment|env_unproven|near_match_unresolved|history_used")
        );
        let f = |p: &str| Finding {
            point: p.into(),
            detail: "x".into(),
            ..Default::default()
        };
        for p in ["env_unproven", "near_match_unresolved", "history_used"] {
            assert_eq!(f(p).settler(), Settler::Proof, "{p}");
            assert_ne!(f(p).label(), p, "{p} has a label");
        }
    }
}
