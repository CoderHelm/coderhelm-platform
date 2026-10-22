//! Before a pull request opens, the change must rest on what the run proved.
//! A change that relied on a vendor field nobody looked at, said "not
//! measured" while a connected source could count it, or logged every record
//! at INFO on every scheduled sync, shipped as a PR because nothing checked
//! these before it opened. This gate does: a judge reads the change, the plan,
//! the sources the team connected and every lookup the run made, and names
//! each gap. A gap sends the work back for one fix round with the finding;
//! what is still unproven after it is put at the top of the PR description.

use serde::Deserialize;
use tracing::{info, warn};

use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::checks::CheckKind;
use crate::passes::plan::PlanResult;
use crate::passes::FileCache;
use crate::WorkerState;

/// How many fix rounds a gap gets before the PR opens with it named.
pub const MAX_FIX_ROUNDS: u32 = 1;

/// What every change must do, whatever it touches. Each rule is a way a
/// change shipped resting on a guess.
pub const CODER_RULES: &str = "
## Prove what the change rests on (every change)
- Prove an assumption before you write code on it. When the fix relies on a field, value or behaviour of a system this code doesn't own (a vendor API, another service, a table another system writes), confirm it from the real thing first: one read through a connected data source (a live lookup, `rest_get`, a database query, a search) or that system's own data in the warehouse. Read an actual payload or row; a type, a schema or a name in the code is not proof of what the field holds.
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
";

/// How the coder ends: its evidence becomes the PR description's.
pub const EVIDENCE_OUTPUT: &str = "When you finish, output a one-line summary, then a section that starts with `Evidence:` listing: each fact about a system this code doesn't own that the change relies on, on its own line that starts `- Confirmed:` (the fact, the lookup that confirmed it and what came back: quote the field and its value) or `- Unconfirmed:` (the fact, and why nothing confirmed it: the source that is missing, or the lookup that failed or came back empty); how many records the change affects, where you counted them, and a few examples (ids or names); and anything no connected source could answer, naming the source that is missing. Write `Evidence: none needed` when the change relies on nothing outside the code and changes no stored records. Do NOT describe each code change in detail.";

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
    /// The kinds of check the repository's CI runs on the pull request.
    pub ci_gates: Vec<CheckKind>,
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
        }
        for k in later.ci_gates {
            if !self.ci_gates.contains(&k) {
                self.ci_gates.push(k);
            }
        }
        self.ci_gates.sort();
        let notes = later.notes.trim();
        if !notes.is_empty() {
            if self.notes.trim().is_empty() {
                self.notes = notes.to_string();
            } else {
                self.notes = format!("{}\n\n{FIX_ROUND_MARK}\n{notes}", self.notes.trim());
            }
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
             ## The coder's evidence notes (they become the PR description's evidence)\n{}\n",
            list(&sources, "(none connected)"),
            list(&lookups, "(no lookups were made)"),
            list(&checks, "(no sandbox checks ran)"),
            match self.ci_gates.as_slice() {
                [] => "(none found: nothing checks this PR after it opens)".to_string(),
                g => g.iter().map(|k| k.name()).collect::<Vec<_>>().join(", "),
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
    /// `fixtures`, `one_record`, `team_rule`, `ci_pending` or
    /// `manual_check`; or `unrun_check`, which only [`classify`] sets.
    pub point: String,
    /// What is unverified, in one sentence.
    pub detail: String,
    /// How to settle it, in one sentence.
    #[serde(default)]
    pub settle: String,
    /// The kind of check (`lint`, `typecheck`, `build`, `tests`) when the
    /// finding is only that a check didn't run (or failed) before the PR.
    #[serde(default)]
    pub check: String,
}

/// What settles a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settler {
    /// A gap in what the change rests on: a fix round, then the warning.
    Proof,
    /// A check the PR's CI runs: a pending item its green result removes.
    Ci,
    /// Only a person can check it (a real browser, a device): a step in the
    /// PR's Verification section, never in the warning.
    Person,
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
            other => other,
        }
    }

    /// One bullet: what's unverified, then how to settle it, each cut to one
    /// sentence and a length cap at a word boundary.
    fn line(&self) -> String {
        let what = sentence(&self.detail, MAX_WHAT);
        let settle = match (self.settler(), sentence(&self.settle, MAX_SETTLE)) {
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

/// Holds the judge's findings to the run's facts. A finding that is only
/// "this check didn't run before the PR" (its `check` names the kind) is not a
/// proof gap when the repository's CI runs that check on the PR: it becomes a
/// `ci_pending` item CI's result settles, whatever point the judge gave it.
/// When CI doesn't run that check, nothing will settle it after the PR opens,
/// so it is a gap (`unrun_check`). A `ci_pending` that names no kind waits on
/// CI only when CI runs any check at all.
pub fn classify(findings: Vec<Finding>, evidence: &RunEvidence) -> Vec<Finding> {
    findings
        .into_iter()
        .map(|mut f| {
            if f.point.trim() == "manual_check" {
                return f;
            }
            let ci_settles = match CheckKind::parse(&f.check) {
                Some(k) => Some(evidence.ci_gates.contains(&k)),
                None if f.point.trim() == "ci_pending" => Some(!evidence.ci_gates.is_empty()),
                None => None,
            };
            match ci_settles {
                Some(true) => f.point = "ci_pending".into(),
                Some(false) => f.point = "unrun_check".into(),
                None => {}
            }
            f
        })
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
     to run lint, a type-check, a build or tests before pushing is not a team_rule miss: report \
     a check that didn't run or failed as ci_pending (point 11).\n\
     11. ci_pending: a check (lint, typecheck, build or tests) that the sandbox checks listed did \
     not run, or ran and failed, before the PR opened. When the repository's CI runs that check \
     on the pull request, its result settles it, so it is not a proof gap. Set `check` to the \
     kind. A check the sandbox list shows as PASS is not a finding.\n\
     12. manual_check: something only a person can verify (in a real browser, on a device, a \
     manual click-through). Never a gap; it becomes a numbered step in the PR's Verification \
     section.\n\
     Many changes touch none of these: a change that relies on nothing outside its own code, \
     changes no stored records and adds no logs passes all of them. Judge only what the evidence \
     shows and do not invent concerns. A fact the lookups confirm is proven even when the notes \
     are short. A lookup marked FAILED or EMPTY confirms nothing, and a fact the notes mark \
     `Unconfirmed:` is not proven. Each finding is short: `detail` is ONE sentence naming what is unverified (the \
     exact field, records, log line, assumption or check); `settle` is ONE sentence on how to \
     settle it (which connected source to read, what to count, which log to lower, which check \
     result). No lists, no preamble, no repeating the rule's text.\n\
     Output ONLY a fenced ```json block: {\"findings\":[{\"point\":\"external_fact|impact|logging|assumption|structured_field|survey|silent_fallback|fixtures|one_record|team_rule|ci_pending|manual_check\",\"detail\":\"one sentence\",\"settle\":\"one sentence\",\"check\":\"lint|typecheck|build|tests, or empty\"}]}. \
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
    /// Gaps remain and no round is left: open the PR with them at the top.
    OpenWithGaps,
}

/// Decides what follows a judgment. `None` is a judge that couldn't answer:
/// it never holds a PR. Only proof gaps earn a fix round: what CI or a person
/// settles opens the PR as it is.
pub fn next(findings: Option<&[Finding]>, rounds_done: u32, has_time: bool) -> Next {
    match findings.map_or(0, proof_gaps) {
        0 => Next::Open,
        _ if rounds_done < MAX_FIX_ROUNDS && has_time => Next::FixRound,
        _ => Next::OpenWithGaps,
    }
}

/// How many of `findings` are proof gaps.
fn proof_gaps(findings: &[Finding]) -> usize {
    findings
        .iter()
        .filter(|f| f.settler() == Settler::Proof)
        .count()
}

/// How an evidence line that names a fact nothing confirmed starts (after
/// any `>` quote and `-` bullet). `- Unconfirmed:` is what the coder is told
/// to write ([`EVIDENCE_OUTPUT`]); the last is how [`gaps_section`] names an
/// open `external_fact` gap at the top of a PR.
const UNCONFIRMED_MARKERS: &[&str] = &[
    "unconfirmed:",
    "unverified:",
    "not confirmed:",
    "not verified:",
    "unconfirmed external field or behaviour:",
];

/// The facts an evidence text itself marks as unconfirmed: each line that
/// starts with one of [`UNCONFIRMED_MARKERS`], with the marker taken off.
/// Reads the coder's notes before a PR opens and a PR description in review.
pub fn unconfirmed_facts(text: &str) -> Vec<String> {
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
            let marker = UNCONFIRMED_MARKERS.iter().find(|m| lower.starts_with(*m))?;
            let fact = l[marker.len()..].trim_start_matches("**").trim();
            (!fact.is_empty()).then(|| fact.to_string())
        })
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
    for fact in unconfirmed_facts(evidence.latest_notes()) {
        out.push(Finding {
            point: "external_fact".into(),
            detail: format!("The evidence notes mark this unconfirmed: {fact}"),
            settle: "Confirm it through a connected source and quote what came back, or change \
                     the code so it no longer depends on it."
                .into(),
            ..Default::default()
        });
    }
    out
}

/// The judge's findings with the [`recorded_gaps`] added. A recorded gap
/// stands even when the judge passed the change or couldn't answer, and it is
/// added after [`classify`], so it is always a proof gap (never `ci_pending`).
pub fn with_recorded(judged: Option<Vec<Finding>>, evidence: &RunEvidence) -> Option<Vec<Finding>> {
    let recorded = recorded_gaps(evidence);
    if recorded.is_empty() {
        return judged;
    }
    let mut all = judged.unwrap_or_default();
    for g in recorded {
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
         of the record, follow the team's instructions, or change the code so it no longer \
         depends on what you can't prove. When no \
         connected source can answer, say which source is missing. Then write your `Evidence:` \
         section again in full.\n\n{}",
        lines.join("\n")
    )
}

/// Where the "Not yet verified" section starts and ends in a PR description,
/// and the mark on each item the PR's CI settles. They let the section be
/// found and rewritten when CI finishes, and kept whole when the description
/// is rewritten.
const UNVERIFIED_START: &str = "<!-- coderhelm:unverified -->";
const UNVERIFIED_END: &str = "<!-- /coderhelm:unverified -->";
const CI_MARK: &str = "<!-- coderhelm:ci -->";

/// The section put at the top of the PR description: one bullet per proof
/// gap and per check the PR's CI still has to settle. What only a person can
/// check never goes here (see [`manual_steps`]). A WARNING when a proof gap
/// remains, a NOTE when only CI's result is awaited; empty when neither.
pub fn gaps_section(findings: &[Finding]) -> String {
    let gaps: Vec<String> = findings
        .iter()
        .filter(|f| f.settler() == Settler::Proof)
        .map(|f| format!("> {}", f.line()))
        .collect();
    let pending: Vec<String> = findings
        .iter()
        .filter(|f| f.settler() == Settler::Ci)
        .map(|f| format!("> {} {CI_MARK}", f.line()))
        .collect();
    if gaps.is_empty() && pending.is_empty() {
        return String::new();
    }
    let callout = if gaps.is_empty() { "NOTE" } else { "WARNING" };
    format!(
        "{UNVERIFIED_START}\n> [!{callout}]\n> **Not yet verified**\n{}\n{UNVERIFIED_END}\n\n",
        gaps.into_iter()
            .chain(pending)
            .collect::<Vec<_>>()
            .join("\n")
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

/// Splits a PR description into its "Not yet verified" section (with the
/// blank line after it) and the rest. The section is empty when there is
/// none.
pub fn split_unverified(body: &str) -> (String, String) {
    let Some(start) = body.find(UNVERIFIED_START) else {
        return (String::new(), body.to_string());
    };
    let Some(end) = body[start..].find(UNVERIFIED_END).map(|e| start + e) else {
        return (String::new(), body.to_string());
    };
    let mut close = end + UNVERIFIED_END.len();
    while body[close..].starts_with('\n') {
        close += 1;
    }
    (
        body[start..close].to_string(),
        format!("{}{}", &body[..start], &body[close..]),
    )
}

/// The description once the PR's CI passed: the items CI settled are gone,
/// and so is the whole section when nothing is left in it. `None` when there
/// is nothing to change.
pub fn settle_by_ci(body: &str) -> Option<String> {
    let (section, rest) = split_unverified(body);
    if !section.contains(CI_MARK) {
        return None;
    }
    let start = body.find(UNVERIFIED_START)?;
    let kept: Vec<&str> = section
        .trim_end_matches('\n')
        .lines()
        .filter(|l| !l.contains(CI_MARK))
        .collect();
    if !kept.iter().any(|l| l.starts_with("> - ")) {
        return Some(rest);
    }
    let section = format!("{}\n\n", kept.join("\n"));
    Some(format!("{}{section}{}", &rest[..start], &rest[start..]))
}

/// What the plan says the change is for, for the judge.
fn plan_context(plan: &PlanResult) -> String {
    let mut out = plan.proposal.trim().to_string();
    if let Some(d) = &plan.fix {
        out.push_str(&super::fix_location::reviewer_context(d));
    }
    out
}

/// The judge's reading of the change on `branch`. `None` when it couldn't
/// judge (an API or model failure, an unreadable reply).
#[allow(clippy::too_many_arguments)]
pub async fn judge(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    plan: &PlanResult,
    evidence: &RunEvidence,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Option<Vec<Finding>> {
    let diff = match github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await
    {
        Ok(d) => super::pr::format_diff_patches(&d),
        Err(e) => {
            warn!(error = %e, "Proof check could not read the diff");
            return None;
        }
    };
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
        let findings = with_recorded(judged, evidence);
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
            Next::OpenWithGaps => {
                let gaps = findings.unwrap_or_default();
                super::add_progress_note(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    &format!(
                        "Proof check: {} gap(s) remain, named at the top of the PR",
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
                    None,
                    true, // bot-driven proof fix — scoped to the PR
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
        // A real proof gap: never waiting on CI, and it leads the PR as a
        // WARNING in the "Not yet verified" section, with how to settle it.
        assert!(judged.iter().all(|f| f.settler() == Settler::Proof));
        let with_ci = with_recorded(Some(vec![check_finding("ci_pending", "tests")]), &ev).unwrap();
        assert_eq!(proof_gaps(&with_ci), 1);
        let top = gaps_section(&judged);
        assert!(top.starts_with(UNVERIFIED_START), "{top}");
        assert!(top.contains("> [!WARNING]"));
        assert!(top
            .contains("> - **Unconfirmed external field or behaviour:** No lookup against vendor"));
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
        assert!(EVIDENCE_OUTPUT.contains("`- Unconfirmed:`"));
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
    fn gaps_lead_the_pr_description_and_the_fix_round_gets_them() {
        let gaps = [
            finding(
                "external_fact",
                "confirm `name` on products from the vendor API",
            ),
            finding("logging", "INFO log per record in the sync loop"),
        ];
        let top = gaps_section(&gaps);
        assert!(top
            .starts_with("<!-- coderhelm:unverified -->\n> [!WARNING]\n> **Not yet verified**\n"));
        assert!(top.ends_with("<!-- /coderhelm:unverified -->\n\n"));
        assert!(top.contains("> - **Unconfirmed external field or behaviour:** confirm `name`"));
        assert!(top.contains("> - **Noisy logging:** INFO log per record"));
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
        assert!(top.contains("> [!NOTE]"));
        assert!(top.contains("> - **Waiting on CI:** lint did not run"));
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
    fn ci_passing_removes_what_it_settled_and_the_warning_when_nothing_is_left() {
        let ev = gated(&[CheckKind::Lint]);
        let pending = classify(vec![check_finding("team_rule", "lint")], &ev);
        let rest = "Closes #4\n\n**Problem** x";
        let body = format!("{}{rest}", gaps_section(&pending));
        assert_eq!(settle_by_ci(&body).as_deref(), Some(rest));

        // A proof gap stays, under its heading; only the CI item goes.
        let mut mixed = pending.clone();
        mixed.insert(0, finding("impact", "affected records not counted"));
        let body = format!("{}{rest}", gaps_section(&mixed));
        let settled = settle_by_ci(&body).unwrap();
        assert!(settled.starts_with("<!-- coderhelm:unverified -->\n> [!WARNING]\n> **Not yet verified**\n> - **Impact not measured:**"));
        assert!(!settled.contains("Waiting on CI"));
        assert!(settled.ends_with(&format!("<!-- /coderhelm:unverified -->\n\n{rest}")));
        // Nothing left for CI to settle: the description is left alone.
        assert_eq!(settle_by_ci(&settled), None);
        assert_eq!(settle_by_ci(rest), None);
    }

    #[test]
    fn the_section_splits_off_whole_for_a_description_rewrite() {
        let gaps = gaps_section(&[finding("impact", "affected records not counted")]);
        let body = format!("{gaps}Closes #4\n\nx");
        let (section, rest) = split_unverified(&body);
        assert_eq!(section, gaps);
        assert_eq!(rest, "Closes #4\n\nx");
        assert_eq!(
            split_unverified("Closes #4"),
            (String::new(), "Closes #4".to_string())
        );
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
}
