//! Where a ticket's fix belongs: in the code, outside it (data or settings in
//! another system), both, or not yet known.
//!
//! The planner decides this as structured output (a ```fix_location.json
//! block next to the openspec files) from what it looked up: the rule the code
//! is meant to follow, and what the data or settings show. When the code works
//! as designed and the outcome comes from a source system (a booking system's
//! setting, a CMS entry, a feature flag in another tool), the fix is a change
//! there. Overriding that system's data in code (a hardcoded list of record
//! ids) hides the real cause and goes stale, so the run reports the change to
//! make instead of opening a pull request.

use serde::{Deserialize, Deserializer, Serialize};

/// The fence the planner writes its decision in.
pub const FENCE: &str = "fix_location.json";

/// Stored next to the openspec files, so a reused plan keeps its decision.
pub const FILE: &str = "fix_location.json";

/// The run status when the whole fix is a change outside the code.
pub const STATUS_OUTSIDE: &str = "needs_outside_change";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixLocation {
    /// A change in this code (what every run did before this field existed).
    #[default]
    Code,
    /// The code behaves as designed; data or settings in another system
    /// produce the outcome, and changing them there is the fix.
    OutsideCode,
    /// A code change and a change in another system are both needed.
    Both,
    /// The planner looked and could not tell which.
    Unknown,
}

impl FixLocation {
    /// Reads the planner's value; anything it can't name is `Unknown`.
    pub fn parse(s: &str) -> FixLocation {
        match s
            .trim()
            .to_ascii_lowercase()
            .replace(['-', ' '], "_")
            .as_str()
        {
            "code" => FixLocation::Code,
            "outside_code" | "outside_the_code" | "outside" => FixLocation::OutsideCode,
            "both" => FixLocation::Both,
            _ => FixLocation::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            FixLocation::Code => "code",
            FixLocation::OutsideCode => "outside_code",
            FixLocation::Both => "both",
            FixLocation::Unknown => "unknown",
        }
    }
}

impl<'de> Deserialize<'de> for FixLocation {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(FixLocation::parse(&s))
    }
}

/// What the decision rests on.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixEvidence {
    /// What was looked up (the query or tool, and a summary of what came
    /// back), one item per lookup.
    #[serde(default, deserialize_with = "string_or_list")]
    pub looked_up: Vec<String>,
    /// The rule the code is meant to follow, in plain words.
    #[serde(default)]
    pub code_rule: String,
    /// What the data or settings show.
    #[serde(default)]
    pub data_shows: String,
    /// What the decision rests on: `code` (a bug in code the planner read),
    /// `data` (stored records or a source system) or `feature` (new work,
    /// nothing wrong to trace). Decides what the trace must show.
    #[serde(default)]
    pub basis: String,
    /// What writes the wrong value, when the ticket is about one.
    #[serde(default)]
    pub producer: Producer,
    /// How many records have the same defect, from a query.
    #[serde(default)]
    pub scope: String,
}

/// What writes a wrong value: code (a repository file and function the
/// planner read) or a record in the source system.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Producer {
    /// `code` or `source`.
    #[serde(default)]
    pub kind: String,
    /// For code: `owner/repo:path` and the function. For a source record:
    /// the system and the record.
    #[serde(default, rename = "where")]
    pub where_: String,
}

/// The change a person makes outside the code.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutsideChange {
    /// The system the change is made in ("the booking system").
    #[serde(default)]
    pub system: String,
    /// Exactly what to change.
    #[serde(default)]
    pub change: String,
    /// Who makes it, or where in that system.
    #[serde(default, rename = "where")]
    pub where_: String,
    /// How to confirm it worked afterwards.
    #[serde(default)]
    pub verify: String,
}

impl OutsideChange {
    fn is_usable(&self) -> bool {
        !self.change.trim().is_empty() && !self.system.trim().is_empty()
    }
}

/// The planner's structured decision.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixDecision {
    #[serde(default)]
    pub fix_location: FixLocation,
    /// The cause, in one or two plain sentences.
    #[serde(default)]
    pub cause: String,
    #[serde(default)]
    pub evidence: FixEvidence,
    #[serde(default)]
    pub outside_change: Option<OutsideChange>,
    /// When the ticket has history (an earlier CoderHelm pull request closed
    /// without merging, a lesson or a person saying the fix is elsewhere):
    /// why a code change is still right, answering that history. Empty when
    /// the ticket has none.
    #[serde(default)]
    pub history_check: String,
}

fn string_or_list<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    Ok(match Option::<OneOrMany>::deserialize(d)? {
        None => Vec::new(),
        Some(OneOrMany::One(s)) if s.trim().is_empty() => Vec::new(),
        Some(OneOrMany::One(s)) => vec![s],
        Some(OneOrMany::Many(v)) => v.into_iter().filter(|s| !s.trim().is_empty()).collect(),
    })
}

/// What the run does with the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// Implement and open a pull request, as before.
    Code,
    /// No branch, no pull request: report the change to make outside the code.
    OutsideOnly,
    /// Implement the code part; the PR and the ticket also carry the outside change.
    CodeAndOutside,
    /// Not sure where the fix belongs: report what was found and ask.
    Ask,
}

impl FixDecision {
    /// What the decision actually supports. An outside change with nothing to
    /// change is not a decision: it is reported as unknown, never coded around.
    pub fn effective_location(&self) -> FixLocation {
        let usable = self
            .outside_change
            .as_ref()
            .is_some_and(OutsideChange::is_usable);
        match self.fix_location {
            FixLocation::OutsideCode | FixLocation::Both if !usable => FixLocation::Unknown,
            l => l,
        }
    }
}

/// What a settled decision about data still lacks, in plain instructions
/// (empty when nothing is missing): a lookup at all, what writes the wrong
/// value, that code named as the writer was actually opened in this run
/// (`read_paths`: `owner/repo:path` of every file the planner read), and how
/// many records share the defect.
/// Whether the decision traces the problem to code the planner opened in
/// this run: a bug in the code itself (a style, a branch, a mapping) rests on
/// that code, and no data lookup could add to it.
fn rests_on_opened_code(d: &FixDecision, read_paths: &[String]) -> bool {
    let p = &d.evidence.producer;
    let cited = p.where_.trim();
    p.kind.trim() == "code"
        && !cited.is_empty()
        && read_paths.iter().any(|r| {
            let file = r.split_once(':').map_or(r.as_str(), |(_, f)| f);
            !file.is_empty() && cited.contains(file)
        })
}

/// Whether the decision rests on data it never looked up: its basis is
/// data, it could look things up and it made no lookup.
pub fn needs_lookup(
    d: Option<&FixDecision>,
    can_look_up: bool,
    lookups: usize,
    _read_paths: &[String],
) -> bool {
    can_look_up && lookups == 0 && d.is_some_and(|d| basis_of(d) == "data")
}

pub fn gaps(
    d: Option<&FixDecision>,
    can_look_up: bool,
    lookups: usize,
    read_paths: &[String],
) -> Vec<String> {
    let Some(d) = d else { return Vec::new() };
    let mut out = Vec::new();
    let basis = basis_of(d);
    let settled = matches!(
        d.fix_location,
        FixLocation::Code | FixLocation::OutsideCode | FixLocation::Both
    );
    if basis == "data" && can_look_up && lookups == 0 {
        out.push("Your decision rests on data, but you made no lookups. Check the records the ticket and the comments name against the connected data (stored values by id, and the sync or pipeline that writes them).".to_string());
    }
    if !settled || basis == "feature" {
        return out;
    }
    if basis.is_empty() {
        out.push("Say what your decision rests on (`basis`): `code` when the bug is in code you read, `data` when it depends on stored records or a source system, `feature` for new work.".to_string());
        return out;
    }
    let p = &d.evidence.producer;
    if p.kind.trim() == "code" && d.fix_location == FixLocation::OutsideCode {
        out.push("You found that code writes the wrong value, so it is our bug: the fix is in that code (`code`, or `both` when a source record must change too), not outside it. A display fallback alone isn't the fix.".to_string());
    }
    match p.kind.trim() {
        "code" => {
            let cited = p.where_.trim();
            if cited.is_empty() {
                out.push("Name the file and function that write the wrong value (`producer.where`: owner/repo:path:line function).".to_string());
            } else if !rests_on_opened_code(d, read_paths) {
                out.push(format!(
                    "You named `{cited}` as what writes the wrong value but didn't open it in this run. Read the code that writes it (the write statement itself) before deciding."
                ));
            }
        }
        "source" if !p.where_.trim().is_empty() => {}
        _ => out.push("Say what writes the wrong value (`producer`): the code (repository file and function) or the source record. Read it before deciding.".to_string()),
    }
    if d.evidence.scope.trim().is_empty() {
        out.push("Say how widespread it is (`scope`): how many records have the same defect (from a query), or how many places in the code.".to_string());
    }
    out
}

/// The decision's basis as declared; for a decision without one, what its
/// producer says (`code` or `data`), else `feature` when it claims nothing
/// about data. Empty when it can't be told.
fn basis_of(d: &FixDecision) -> &'static str {
    match d.evidence.basis.trim() {
        "code" => return "code",
        "data" => return "data",
        "feature" => return "feature",
        _ => {}
    }
    match d.evidence.producer.kind.trim() {
        "code" => "code",
        "source" => "data",
        _ if d.evidence.data_shows.trim().is_empty() => "feature",
        _ => "",
    }
}

/// The message that sends a planner back with what its decision lacks.
pub fn send_back(gaps: &[String]) -> String {
    format!(
        "Before this decision stands:\n{}\nThen answer again in full: the four openspec files and the fix_location.json block, with `data_shows` saying only what your lookups returned.",
        gaps.iter().map(|g| format!("- {g}")).collect::<Vec<_>>().join("\n")
    )
}

/// A decision still missing part of its trace after being sent back: no
/// code is written on it. It becomes `unknown`, saying what is missing.
pub fn mark_untraced(mut d: FixDecision, gaps: &[String]) -> FixDecision {
    d.fix_location = FixLocation::Unknown;
    d.cause = format!(
        "Not traced far enough to decide ({}): {}",
        gaps.first()
            .map(String::as_str)
            .unwrap_or("the trace is incomplete"),
        d.cause.trim()
    );
    d
}

/// A decision still unchecked after being sent back: no code is written on
/// it. It becomes `unknown`, saying the data wasn't checked.
pub fn mark_unchecked(mut d: FixDecision) -> FixDecision {
    d.fix_location = FixLocation::Unknown;
    d.cause = format!(
        "Not checked against the data (no lookups were made): {}",
        d.cause.trim()
    );
    d
}

/// How the run proceeds. A plan without a decision (a model that left the
/// block out, or one that didn't read) is not a code plan: where the fix
/// belongs was never decided, so the run asks instead of coding.
pub fn route(decision: Option<&FixDecision>) -> Route {
    match decision.map(FixDecision::effective_location) {
        None => Route::Ask,
        Some(FixLocation::Code) => Route::Code,
        Some(FixLocation::OutsideCode) => Route::OutsideOnly,
        Some(FixLocation::Both) => Route::CodeAndOutside,
        Some(FixLocation::Unknown) => Route::Ask,
    }
}

/// The sentinel tasks.md carries when the planner plans no code.
pub const NO_CODE_CHANGE: &str = "NO_CODE_CHANGE:";

/// How the run proceeds for this plan. A plan that says it changes no code
/// but decided `code` contradicts itself: nothing would be implemented, so it
/// is reported and asked about instead.
pub fn route_for_plan(decision: Option<&FixDecision>, tasks: &str) -> Route {
    match route(decision) {
        Route::Code | Route::CodeAndOutside if tasks.trim_start().starts_with(NO_CODE_CHANGE) => {
            Route::Ask
        }
        r => r,
    }
}

/// The report as Jira shows it: the Jira app turns each paragraph into one
/// plain-text paragraph (no markdown, single newlines become spaces), so
/// every line becomes its own paragraph and the emphasis marks go.
pub fn jira_text(markdown: &str) -> String {
    markdown
        .lines()
        .map(|l| l.replace("**", "").trim().to_string())
        .filter(|l| !l.is_empty())
        .map(|l| match l.strip_prefix("- ") {
            Some(item) => format!("• {item}"),
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// The decision in the planner's reply, if it wrote one that reads.
pub fn parse(response: &str) -> Option<FixDecision> {
    let marker = format!("```{FENCE}");
    let start = response.find(&marker)?;
    let body_start = start + response[start..].find('\n')? + 1;
    let end = response[body_start..]
        .find("```")
        .map(|i| body_start + i)
        .unwrap_or(response.len());
    let raw = response[body_start..end].trim();
    match serde_json::from_str::<FixDecision>(raw) {
        Ok(d) => Some(d),
        Err(e) => {
            tracing::warn!(error = %e, "Planner's fix_location block didn't read");
            None
        }
    }
}

/// The planner's instructions for the decision, appended to its prompt.
pub const PLANNER_GUIDANCE: &str = r#"## Where the fix belongs (decide before you plan code)

Before writing tasks, decide whether the fix belongs in this code at all.

- Find the rule the code is meant to follow for the case in the ticket (read the code path; read its comments, names and tests for intent).
- When a live source, data lookup or check can show the actual record or setting, look it up and compare it with that rule.
- Debug it; don't guess. For a wrong value on a page or in a result:
  - Find the exact field the code reads to produce it (for example the id a displayed name or address is looked up by), then look up that stored value for the ticket's own records by id. A display name, label or short code is not the value: several records can share one.
  - Follow it hop by hop (the record, the row it points to, the setting it is matched against) until you reach the value that is shown.
  - When a saved lookup doesn't return the field you need, write your own read-only query against the connected database.
  - When the ticket names a page you can check with `check_url`, look at what it shows.
  - Your cause must explain the exact wrong value in the ticket (why that one and not another). If it doesn't, keep looking.
  - Production first: every claim about data or behaviour comes from production; use a test environment only to try a change. Label each check with where it ran (for example `[prod database]`, `[staging API]`).
  - Values, not schema: confirm a field by querying it on the same path the failing screen uses and reading the value it returns. A field existing in a schema or type says nothing about what it holds.
  - Record the value at every step it passes through (source record, pipeline output, database, search index, API response, the code that maps it, the page). The first step where it is wrong is where the bug is.
  - When the data comes from another system and a copy of that system's own records is connected (a raw or staging copy in a warehouse), compare the source record with what the pipeline wrote, so you know whether the source, the pipeline or the code is wrong.
  - Don't stop at "not sure where the fix belongs" while you can still read the code that serves the request: read the handler or resolver that actually answers it.
  - Start from what was actually shown: check the page with `check_url` and `find` (the record id, the field, the wrong text), or query the live API, before reading the code you expect ran.
  - A field's name can carry another record's value. Read what the code that fills it assigns (the resolver, serializer or mapper), then confirm it with a live query.
  - A value on a parent record (a group, a category, a summary) is not the value of each item under it. Anything shown for one item must come from that item; check whether a parent spans items with different values.
  - Data often lives in several copies (the database, search indexes, caches). Compare them before calling something a sync or cache problem.
  - Timestamps copied from a source system are the source's, not when the row was written.
  - Before showing a per-item value instead of the parent's, look for items whose value has nothing to show (no mapping, an internal record) and keep the parent's as the fallback.
  - When the trace crosses repositories, read the files in those repositories; a path belongs to the repository that contains it. Plan one change per repository, in the order they must ship.
- What people wrote on the ticket are leads, not facts. Check a person's finding against the data before you build on it, and say which lookup confirmed it.
- When the wrong value is written by a sync or pipeline, the producer is where the cause usually is: read that pipeline's code (in its repository) and the rows it writes. A change in the code that reads the data instead is a workaround; plan one only when you can say why the producer can't or shouldn't be fixed.
- If a person says the fix is tracked in another ticket, check that ticket's fix against your own trace. If it covers the cause you found, use `outside_code` naming that ticket. If your trace finds a deeper cause it doesn't cover, say so and plan the fix for that cause; the other ticket can stay as a safeguard.
- Never call a cause settled while you haven't read the code that writes the wrong value. If you write "I did not read" about the step that produces it, read it before deciding.
- When records that should be separate are merged, or a value flips between runs, read the table's unique constraints and indexes (query them from the database), the upsert's conflict target (`ON CONFLICT`) and the code that matches existing rows: a key that leaves out a field forces records that differ in that field into one, and the last write wins.
- Only call it a data problem when the source system's own record holds the wrong value: look the record up in the source (or its raw copy) and quote the value. If the source is right and ours is wrong, it is our bug, in the code that copies or transforms it.
- "Can't reproduce" is a finding, not noise: when a value differs between checks, look for what rewrites it (sync order, last write wins, a cache or index rebuilt at another moment) before setting it aside.
- When the cause needs changes in several repositories (a schema migration, then the code that relies on it, then a one-time data fix), plan all of them for this ticket, one change per repository in merge order. Needing more than one repository is not a reason to defer the cause to a follow-up. Write a one-time data fix as a migration or script that can run twice without changing anything the second time, in the repository that owns data migrations, and say which one you picked.
- Fix the cause, not the display. A fallback in the screen that shows the value is an extra safeguard, never the whole fix, while the stored value stays wrong.
- Measure how widespread the problem is with a query (how many records share the defect) before choosing a fix, and say the number.
- A fix that changes the shape of data (splitting or merging records, a new unique key) needs the readers of that data: search the team's other repositories for code that depends on the old shape, and say which links or ids would change.
- A plan that changes a unique key or constraint lists, as tasks, every writer of that table you found across the repositories (each `ON CONFLICT`, insert or upsert that names the old key), the migration's registration, and a read-only count of the rows the data fix will change, taken on production before it runs. Search open pull requests for the ticket key first, and build on one that exists instead of starting over.
- Before keying logic on a field's value (a vendor's `quantity`, `type`, a status code), query that field across the whole dataset, not only the ticket's records: list its distinct values and what they mean on other kinds of records, and confirm it means the same thing everywhere your change reads it. Say what else it is used for.
- Count the records whose result your change alters beyond the ticket's (with a query against the real data), and name a few of them. A fix that is right for the ticket and wrong for others is not a fix; narrow it until those others are unchanged or are meant to change.
- Before your first query on a table, read its columns (describe it); don't guess column names from other tables' conventions.
- Never name a cause in a system you couldn't read. If the trace through what you can read is complete and leads into another system, say so with that trace. If you couldn't finish the trace, use `unknown`, say where it stopped, and list the next lookups a person should make, without a likely culprit.
- If the code behaves as designed and the outcome comes from data or configuration in a source system (a booking or scheduling system, a CMS, a ticket tool's configuration, a feature flag held in another system, a cloud console setting), the fix is a change in that system, not in this code. Set `fix_location` to `outside_code`, describe the exact change, and set tasks.md to `NO_CODE_CHANGE: <one line>`.
- Never override source data in code: no hardcoded record ids, allowlists, denylists or "force" lists that change behavior for specific records the source system already decides. That hides the cause and goes stale. If that is the only way you see to make the ticket's symptom go away, the fix is outside the code.
- If a code change is needed AND a change in another system, use `both`: plan the code part normally and describe the outside change.
- If you looked and still cannot tell whether the code or the data is at fault, use `unknown`, say what you checked and what a person needs to confirm, and set tasks.md to `NO_CODE_CHANGE: <what is unclear>`. Prefer asking over coding.
- Ordinary feature work and bugs in the code's own logic are `code`.
- Quote values only as the tools returned them; a value shown as `[hidden]` stays hidden.
- If the prompt has a "This ticket's history" section (an earlier pull request for this ticket closed without merging, a lesson, or a person saying the fix is elsewhere), read it first. The default then is NOT to plan code again: use `outside_code` or `unknown`. Plan code only when you can say why a code change is right despite that history, in `history_check`; a change like the one already closed is never right.

After the four openspec files, output this block (JSON, all fields required; `outside_change` is null for `code`):

```fix_location.json
{
  "fix_location": "code" | "outside_code" | "both" | "unknown",
  "cause": "one or two plain sentences: why the ticket's symptom happens, only as far as your lookups show it",
  "evidence": {
    "looked_up": ["each lookup you made and a short summary of what came back"],
    "code_rule": "the rule the code is meant to follow for this case",
    "data_shows": "what the data or settings show for the records in the ticket; empty when no stored data is involved (a bug in the code itself)",
    "basis": "code | data | feature: code when the bug is in code you read (no data lookup needed), data when the decision depends on stored records or a source system (lookups needed), feature for new work",
    "producer": {"kind": "code | source", "where": "for code: owner/repo:path:line and the function that writes the wrong value (a file you opened); for source: the system, the record and the wrong value it holds"},
    "scope": "how many records (from a query) or places in the code have the same defect"
  },
  "outside_change": {
    "system": "the system the change is made in",
    "change": "exactly what to change, naming the record(s)",
    "where": "who can make it, or where in that system",
    "verify": "how to confirm afterwards (for example: after the next sync, the item shows on the site)"
  },
  "history_check": "only when the prompt has a ticket history: why a code change is right despite it; otherwise empty"
}
```"#;

fn push_evidence(out: &mut String, d: &FixDecision) {
    let ev = &d.evidence;
    if !ev.looked_up.is_empty() {
        out.push_str("**What I checked**\n");
        for l in &ev.looked_up {
            out.push_str(&format!("- {}\n", one_line(l)));
        }
        out.push('\n');
    }
    if !ev.code_rule.trim().is_empty() {
        out.push_str(&format!(
            "**What the code is meant to do:** {}\n\n",
            ev.code_rule.trim()
        ));
    }
    if !ev.data_shows.trim().is_empty() {
        out.push_str(&format!(
            "**What the data shows:** {}\n\n",
            ev.data_shows.trim()
        ));
    }
    if !ev.producer.where_.trim().is_empty() {
        out.push_str(&format!(
            "**What writes it:** {}\n\n",
            ev.producer.where_.trim()
        ));
    }
    if !ev.scope.trim().is_empty() {
        out.push_str(&format!("**How widespread:** {}\n\n", ev.scope.trim()));
    }
}

fn push_change(out: &mut String, c: &OutsideChange) {
    out.push_str(&format!("- **Where:** {}", c.system.trim()));
    if !c.where_.trim().is_empty() {
        out.push_str(&format!(" ({})", c.where_.trim()));
    }
    out.push('\n');
    out.push_str(&format!("- **What to change:** {}\n", c.change.trim()));
    if !c.verify.trim().is_empty() {
        out.push_str(&format!("- **How to confirm:** {}\n", c.verify.trim()));
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The report for a run whose whole fix is outside the code: the ticket
/// comment, and what the run page shows. `heading` adds the title line (the
/// Jira comment carries its own title).
pub fn outside_report(d: &FixDecision, heading: bool) -> String {
    let mut out = String::new();
    if heading {
        out.push_str("**Needs a change outside the code**\n\n");
    }
    if !d.cause.trim().is_empty() {
        out.push_str(&format!("**Cause:** {}\n\n", d.cause.trim()));
    }
    push_evidence(&mut out, d);
    if let Some(c) = d.outside_change.as_ref().filter(|c| c.is_usable()) {
        out.push_str("**The change to make**\n");
        push_change(&mut out, c);
        out.push('\n');
    }
    out.push_str(
        "No code was changed and no pull request was opened here: the fix is the change above. \
         Once it is made, nothing else is needed here.",
    );
    out
}

/// The decision a run asks with when the plan carried none.
pub fn undecided() -> FixDecision {
    FixDecision {
        fix_location: FixLocation::Unknown,
        cause: "The plan didn't say where the fix belongs (in this code or in data or settings \
                in another system), so I stopped before changing any code."
            .to_string(),
        ..Default::default()
    }
}

/// The report when the planner could not tell where the fix belongs.
pub fn unknown_report(d: &FixDecision, heading: bool) -> String {
    let mut out = String::new();
    if heading {
        out.push_str("**Not sure yet where the fix belongs**\n\n");
    }
    if !d.cause.trim().is_empty() {
        out.push_str(&format!("**What I found:** {}\n\n", d.cause.trim()));
    }
    push_evidence(&mut out, d);
    if let Some(c) = d.outside_change.as_ref() {
        if !c.change.trim().is_empty() {
            out.push_str(&format!(
                "**Possibly a change outside the code:** {}\n\n",
                c.change.trim()
            ));
        }
    }
    out.push_str(
        "I didn't change any code. Please confirm whether the code or the data is at fault \
         (reply here with what you find), and I'll pick it up from there.",
    );
    out
}

/// The section a pull request (and its ticket comment) carries when a change
/// outside the code is needed too. Empty when it isn't.
pub fn also_outside_section(d: Option<&FixDecision>) -> String {
    let Some(d) = d else {
        return String::new();
    };
    if d.effective_location() != FixLocation::Both {
        return String::new();
    }
    let Some(c) = d.outside_change.as_ref() else {
        return String::new();
    };
    let mut out = String::from("\n\n## Also needed outside the code\n\n");
    if !d.cause.trim().is_empty() {
        out.push_str(&format!("{}\n\n", d.cause.trim()));
    }
    push_change(&mut out, c);
    out.push_str("\nThis pull request does not make that change; it has to be made there.\n");
    out
}

/// What the reviewer is told about the plan behind a CoderHelm pull request,
/// so it can judge a diff against what the planner found in the data.
pub fn reviewer_context(d: &FixDecision) -> String {
    let mut out = format!(
        "\n## What the planner found (where the fix belongs: {})\n",
        d.effective_location().as_str()
    );
    if !d.cause.trim().is_empty() {
        out.push_str(&format!("Cause: {}\n", d.cause.trim()));
    }
    for l in &d.evidence.looked_up {
        out.push_str(&format!("- Looked up: {}\n", one_line(l)));
    }
    if !d.evidence.code_rule.trim().is_empty() {
        out.push_str(&format!("Code's rule: {}\n", d.evidence.code_rule.trim()));
    }
    if !d.evidence.data_shows.trim().is_empty() {
        out.push_str(&format!("Data shows: {}\n", d.evidence.data_shows.trim()));
    }
    if let Some(c) = &d.outside_change {
        if !c.change.trim().is_empty() {
            out.push_str(&format!(
                "Change outside the code: {} — {}\n",
                c.system.trim(),
                c.change.trim()
            ));
        }
    }
    out
}

#[cfg(test)]
pub mod fixture {
    //! A ticket where the code is right and the data decides: an item is
    //! missing from the site because its term is not published on the web
    //! in the source booking system, and the sync job correctly marks it
    //! inactive.

    /// The planner's reply on that ticket, as the model writes it.
    pub const PLANNER_REPLY: &str = r#"I looked up the program and its term.

```proposal.md
The Saturday class (booking system program 12345) is missing from the site.
```

```design.md
No code change: the sync job marks programs inactive when their term is not available on the web.
```

```tasks.md
NO_CODE_CHANGE: the term's "available on web" setting is off in the booking system.
```

```spec.md
Given the term is available on web, when the sync runs, then the class shows on the site.
```

```fix_location.json
{
  "fix_location": "outside_code",
  "cause": "The class's term is set to not be available on the web in the booking system, so the sync job marks the class inactive, as it is designed to.",
  "evidence": {
    "looked_up": [
      "programs where external id = 12345: one row, term id 678, active = false",
      "terms where id = 678: available_on_web = false"
    ],
    "code_rule": "The sync job marks a program inactive when its term is not available on the web (dags/sync_programs.py, is_program_active).",
    "data_shows": "Term 678 has available_on_web = false in the booking system, so program 12345 is correctly inactive."
  },
  "outside_change": {
    "system": "the booking system",
    "change": "Set term 678 (the term of program 12345) to available on web.",
    "where": "the term's settings page in the booking system's admin, by someone who manages the schedule",
    "verify": "After the next sync runs, program 12345 is active and the Saturday class shows on the site."
  }
}
```"#;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_data_case_is_outside_the_code_and_opens_no_pr() {
        let d = parse(fixture::PLANNER_REPLY).expect("the block reads");
        assert_eq!(d.fix_location, FixLocation::OutsideCode);
        assert_eq!(route(Some(&d)), Route::OutsideOnly);
        assert_eq!(d.evidence.looked_up.len(), 2);
        assert!(d.evidence.code_rule.contains("inactive"));
        let c = d.outside_change.as_ref().unwrap();
        assert_eq!(c.system, "the booking system");
        assert!(c.verify.contains("next sync"));
    }

    #[test]
    fn the_ticket_comment_says_cause_evidence_change_and_how_to_confirm() {
        let d = parse(fixture::PLANNER_REPLY).unwrap();
        let report = outside_report(&d, true);
        assert!(report.starts_with("**Needs a change outside the code**"));
        assert!(report.contains("**Cause:** The class's term is set to not be available"));
        assert!(report.contains("- terms where id = 678: available_on_web = false"));
        assert!(report.contains("**What the code is meant to do:** The sync job marks"));
        assert!(report.contains("**What the data shows:** Term 678"));
        assert!(report.contains("- **Where:** the booking system (the term's settings page"));
        assert!(report.contains("- **What to change:** Set term 678"));
        assert!(report.contains("- **How to confirm:** After the next sync runs"));
        assert!(report.contains("no pull request was opened"));
        // The Jira comment carries its own title.
        assert!(!outside_report(&d, false).contains("**Needs a change outside the code**"));
    }

    #[test]
    fn a_plan_without_a_decision_asks_and_never_codes() {
        assert_eq!(route(None), Route::Ask);
        assert!(unknown_report(&undecided(), false).contains("stopped before changing any code"));
        assert_eq!(parse("```tasks.md\n- [ ] a.rs: x\n```"), None);
        let d = FixDecision::default();
        assert_eq!(route(Some(&d)), Route::Code);
    }

    #[test]
    fn outside_without_a_change_to_make_is_unknown_and_asks() {
        let d = FixDecision {
            fix_location: FixLocation::OutsideCode,
            cause: "Something in the data.".into(),
            ..Default::default()
        };
        assert_eq!(route(Some(&d)), Route::Ask);
        let both = FixDecision {
            fix_location: FixLocation::Both,
            outside_change: Some(OutsideChange {
                system: "the CMS".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(route(Some(&both)), Route::Ask);
        assert!(unknown_report(&d, true).contains("I didn't change any code"));
    }

    #[test]
    fn values_are_read_loosely_but_never_guessed() {
        assert_eq!(FixLocation::parse("Outside-Code"), FixLocation::OutsideCode);
        assert_eq!(FixLocation::parse("BOTH"), FixLocation::Both);
        assert_eq!(FixLocation::parse("code"), FixLocation::Code);
        assert_eq!(FixLocation::parse("maybe"), FixLocation::Unknown);
        let d: FixDecision = serde_json::from_str(
            r#"{"fix_location":"code","evidence":{"looked_up":"one lookup"},"outside_change":null}"#,
        )
        .unwrap();
        assert_eq!(d.evidence.looked_up, vec!["one lookup".to_string()]);
        assert!(d.outside_change.is_none());
    }

    #[test]
    fn both_puts_the_outside_change_in_the_pr_body() {
        let mut d = parse(fixture::PLANNER_REPLY).unwrap();
        assert_eq!(also_outside_section(Some(&d)), "");
        d.fix_location = FixLocation::Both;
        assert_eq!(route(Some(&d)), Route::CodeAndOutside);
        let s = also_outside_section(Some(&d));
        assert!(s.contains("## Also needed outside the code"));
        assert!(s.contains("Set term 678"));
        assert!(s.contains("After the next sync runs"));
        assert_eq!(also_outside_section(None), "");
    }

    #[test]
    fn a_plan_that_codes_nothing_is_never_sent_to_implement() {
        let code = FixDecision::default();
        assert_eq!(
            route_for_plan(Some(&code), "NO_CODE_CHANGE: data is off"),
            Route::Ask
        );
        assert_eq!(route_for_plan(None, "NO_CODE_CHANGE: x"), Route::Ask);
        // No decision is never a code plan: the run asks (fail closed).
        assert_eq!(route_for_plan(None, "- [ ] a.rs: x"), Route::Ask);
        assert_eq!(route(None), Route::Ask);
        assert_eq!(route(Some(&code)), Route::Code);
        let d = parse(fixture::PLANNER_REPLY).unwrap();
        let tasks = "NO_CODE_CHANGE: the term's setting is off";
        assert_eq!(route_for_plan(Some(&d), tasks), Route::OutsideOnly);
    }

    #[test]
    fn the_jira_comment_is_one_plain_paragraph_per_line() {
        let d = parse(fixture::PLANNER_REPLY).unwrap();
        let jira = jira_text(&outside_report(&d, false));
        assert!(!jira.contains("**"));
        assert!(jira.contains("\n\n• terms where id = 678: available_on_web = false\n\n"));
        assert!(jira.starts_with("Cause: The class's term"));
        assert!(jira.contains("• How to confirm: After the next sync runs"));
    }

    #[test]
    fn the_decision_round_trips_through_storage() {
        let d = parse(fixture::PLANNER_REPLY).unwrap();
        let stored = serde_json::to_string(&d).unwrap();
        assert!(stored.contains("\"fix_location\":\"outside_code\""));
        assert!(stored.contains("\"where\":"));
        let back: FixDecision = serde_json::from_str(&stored).unwrap();
        assert_eq!(back, d);
    }
}

#[cfg(test)]
mod unchecked_data_tests {
    use super::*;

    fn decision(data_shows: &str) -> FixDecision {
        let block = format!(
            "```fix_location.json\n{}\n```",
            serde_json::json!({
                "fix_location": "code",
                "cause": "The parent record's value is overwritten on each sync.",
                "evidence": {"looked_up": [], "code_rule": "", "data_shows": data_shows},
                "outside_change": null,
                "history_check": ""
            })
        );
        parse(&block).expect("parses")
    }

    #[test]
    fn a_decision_resting_on_data_needs_a_lookup() {
        let mut d = decision("Per the ticket: the parent record is overwritten on each sync.");
        d.evidence.basis = "data".into();
        assert!(needs_lookup(Some(&d), true, 0, &[]));
        // It looked: fine. It couldn't look: nothing to send it back for.
        assert!(!needs_lookup(Some(&d), true, 3, &[]));
        assert!(!needs_lookup(Some(&d), false, 0, &[]));
        // A bug in code, or new work: no lookup needed.
        d.evidence.basis = "code".into();
        assert!(!needs_lookup(Some(&d), true, 0, &[]));
        d.evidence.basis = "feature".into();
        assert!(!needs_lookup(Some(&d), true, 0, &[]));
        // No basis and no producer: it has to say which (a gap), not a lookup.
        d.evidence.basis.clear();
        assert!(!needs_lookup(Some(&d), true, 0, &[]));
        assert!(gaps(Some(&d), true, 0, &[])
            .iter()
            .any(|g| g.contains("`basis`")));
        assert!(!needs_lookup(None, true, 0, &[]));
    }

    #[test]
    fn an_unchecked_decision_writes_no_code() {
        let d = mark_unchecked(decision("Per the ticket: it is overwritten."));
        assert_eq!(d.fix_location, FixLocation::Unknown);
        assert!(d.cause.starts_with("Not checked against the data"));
        assert_ne!(route(Some(&d)), route(Some(&decision(""))));
    }
}

#[cfg(test)]
mod gaps_tests {
    use super::*;

    fn decision(location: &str, producer: serde_json::Value, scope: &str) -> FixDecision {
        let block = format!(
            "```fix_location.json\n{}\n```",
            serde_json::json!({
                "fix_location": location,
                "cause": "The parent record's value is overwritten on each sync.",
                "evidence": {
                    "looked_up": ["[prod database] the item and its parent"],
                    "code_rule": "",
                    "data_shows": "The item is at one place, its parent at another.",
                    "producer": producer,
                    "scope": scope
                },
                "outside_change": null,
                "history_check": ""
            })
        );
        parse(&block).expect("parses")
    }

    const SYNC: &str = "acme/pipelines:dags/sync.py";

    #[test]
    fn a_settled_decision_needs_a_producer_it_opened_and_a_scope() {
        let code = serde_json::json!({"kind": "code", "where": "acme/pipelines:dags/sync.py sync_groups()"});
        let full = decision("code", code.clone(), "50 parents, 530 items");
        assert!(gaps(Some(&full), true, 4, &[SYNC.into()]).is_empty());

        // Named but never opened in this run.
        let g = gaps(
            Some(&full),
            true,
            4,
            &["acme/pipelines:dags/other.py".into()],
        );
        assert_eq!(g.len(), 1, "{g:?}");
        assert!(g[0].contains("didn't open it"), "{g:?}");

        // Resting on data with no producer and no scope: both are asked for.
        let mut bare = decision("outside_code", serde_json::json!({}), "");
        bare.evidence.basis = "data".into();
        assert_eq!(gaps(Some(&bare), true, 4, &[]).len(), 2);
        // No basis at all: it has to say what the decision rests on.
        bare.evidence.basis.clear();
        assert_eq!(gaps(Some(&bare), true, 4, &[]).len(), 1);

        // A source record needs no file.
        let src = decision(
            "outside_code",
            serde_json::json!({"kind": "source", "where": "booking system, record 42"}),
            "3 records",
        );
        assert!(gaps(Some(&src), true, 4, &[]).is_empty());
    }

    #[test]
    fn only_settled_data_decisions_are_held_to_it() {
        let mut open = decision("unknown", serde_json::json!({}), "");
        open.evidence.basis = "data".into();
        assert!(
            gaps(Some(&open), true, 4, &[]).is_empty(),
            "unknown says where it stopped"
        );
        assert_eq!(
            gaps(Some(&open), true, 0, &[]).len(),
            1,
            "still needs a lookup"
        );
        let mut feature = decision("code", serde_json::json!({}), "");
        feature.evidence.data_shows.clear();
        assert!(
            gaps(Some(&feature), true, 0, &[]).is_empty(),
            "not a data question"
        );
        assert!(gaps(None, true, 0, &[]).is_empty());
    }

    #[test]
    fn an_untraced_decision_writes_no_code_and_says_why() {
        let d = decision("code", serde_json::json!({}), "");
        let g = gaps(Some(&d), true, 4, &[]);
        let m = mark_untraced(d, &g);
        assert_eq!(m.fix_location, FixLocation::Unknown);
        assert!(m.cause.starts_with("Not traced far enough to decide"));
        assert!(send_back(&g).starts_with("Before this decision stands:\n- "));
    }
}

#[cfg(test)]
mod our_bug_tests {
    use super::*;

    fn decision(location: &str, kind: &str, where_: &str) -> FixDecision {
        let block = format!(
            "```fix_location.json\n{}\n```",
            serde_json::json!({
                "fix_location": location,
                "cause": "A copy step overwrites the value.",
                "evidence": {
                    "looked_up": ["[prod database] the record"],
                    "data_shows": "Ours differs from the source.",
                    "producer": {"kind": kind, "where": where_},
                    "scope": "12 records"
                },
                "outside_change": {"system": "the booking system", "change": "edit the record", "where": "", "verify": ""},
                "history_check": ""
            })
        );
        parse(&block).expect("parses")
    }

    #[test]
    fn code_that_writes_the_wrong_value_is_our_bug() {
        let read = vec!["acme/pipelines:dags/sync.py".to_string()];
        let outside = decision(
            "outside_code",
            "code",
            "acme/pipelines:dags/sync.py:120 sync_groups()",
        );
        let g = gaps(Some(&outside), true, 3, &read);
        assert_eq!(g.len(), 1, "{g:?}");
        assert!(g[0].contains("our bug"), "{g:?}");
        assert!(gaps(
            Some(&decision(
                "code",
                "code",
                "acme/pipelines:dags/sync.py:120 sync_groups()"
            )),
            true,
            3,
            &read
        )
        .is_empty());
        assert!(gaps(
            Some(&decision(
                "both",
                "code",
                "acme/pipelines:dags/sync.py:120 sync_groups()"
            )),
            true,
            3,
            &read
        )
        .is_empty());
        // The source holding the wrong value is a change outside the code.
        assert!(gaps(
            Some(&decision(
                "outside_code",
                "source",
                "booking system, record 42 has the old address"
            )),
            true,
            3,
            &read
        )
        .is_empty());
    }
}

#[cfg(test)]
mod code_only_tests {
    use super::*;

    /// A style bug in a component: the planner opened the file, made no data
    /// lookups (there is no stored record), and said so in `data_shows`.
    fn style_bug() -> FixDecision {
        let block = format!(
            "```fix_location.json\n{}\n```",
            serde_json::json!({
                "fix_location": "code",
                "cause": "The question is a plain Text with no font family.",
                "evidence": {
                    "looked_up": ["[code] src/components/faq/index.tsx line 27"],
                    "data_shows": "No stored record is involved; the text is a constant in the app.",
                    "producer": {"kind": "code", "where": "acme/app:src/components/faq/index.tsx:27 FAQs render"},
                    "scope": "1 component"
                },
                "outside_change": null,
                "history_check": ""
            })
        );
        parse(&block).expect("parses")
    }

    #[test]
    fn a_bug_in_code_it_opened_needs_no_data_lookup() {
        let read = vec!["acme/app:src/components/faq/index.tsx".to_string()];
        let d = style_bug();
        assert!(!needs_lookup(Some(&d), true, 0, &read));
        assert!(
            gaps(Some(&d), true, 0, &read).is_empty(),
            "{:?}",
            gaps(Some(&d), true, 0, &read)
        );
        // Not opened in this run: it still has to read it. A bug in code never
        // needs a data lookup.
        let g = gaps(Some(&d), true, 0, &[]);
        assert!(g.iter().any(|x| x.contains("didn't open it")), "{g:?}");
        assert!(!g.iter().any(|x| x.contains("made no lookups")), "{g:?}");
        assert!(!needs_lookup(Some(&d), true, 0, &[]));
    }
}
