//! The plan is the smallest correct change, and says so in five sections.
//!
//! Two plans went wrong before a line was written. One, for linking four
//! camps to a second sport, planned a new map and helpers beside the
//! existing overrides map and a sync that "replaces" links, so the build
//! deleted links nobody asked about. The other, for embedding a vendor's
//! portal, planned end-to-end assertions about the vendor's page from
//! guesses, and when the vendor's page broke a criterion ("no double
//! scrollbars") it planned to change the test instead of the page.
//!
//! Each plan now writes, in design.md:
//!
//! - [`REUSE`]: the existing maps, helpers, config and tests it extends,
//!   by path (found by search; the worker also hands the planner what the
//!   ticket's own words find, [`existing_block`]);
//! - [`MUST_NOT_CHANGE`]: behaviour that stays as it is (no deletes where
//!   the code only adds, shared logging, other pipelines);
//! - [`MEASURE_FIRST`]: what outside systems it measures before writing
//!   against them (a `probe_page` record, a lookup), or that there are none;
//! - [`SMALLEST_CHANGE`]: the change in a few lines, and why nothing
//!   smaller does it;
//! - [`PROOF`]: the one test (or the check) that shows it, in proportion;
//! - [`OUT_OF_SCOPE`]: what the change leaves out, each with why: the one
//!   scope decision every later round and the description are held to
//!   (`common::scope_ledger`). An open product question is decided here
//!   with its default, never left for the pull request to ask.
//!
//! - `## Criteria map` ([`criteria_gaps`], `common::e2e_proof`): each
//!   acceptance criterion (or Expected line) that names a page or what a
//!   person sees, with the page, an end-to-end test in the suite that loads
//!   it, and what it asserts. Unit tests go with it, never in its place.
//!
//! [`plan_gaps`] holds the draft to them and rejects a plan that proposes a
//! structure parallel to one it names under Reuse, replacing or deleting
//! semantics or log changes the ticket doesn't ask for, assertions about an
//! outside page with nothing measured, or a test change where a criterion
//! says the product must change. What is missing goes back to the planner
//! in the plan's one send-back.

use super::cross_repo::RepoSearch;
use super::feature::reuse::section;
use super::minimal_change::{self, Def, DefKind};
use super::plan::PlanResult;

pub const REUSE: &str = super::feature::reuse::REUSE_HEADING;
pub const MUST_NOT_CHANGE: &str = "## Must not change";
pub const MEASURE_FIRST: &str = "## Measure first";
pub const SMALLEST_CHANGE: &str = "## Smallest change";
pub const PROOF: &str = "## Proof";
/// Scope decided once (`common::scope_ledger`).
pub const OUT_OF_SCOPE: &str = common::scope_ledger::OUT_HEADING;

/// What the planner is told, for every ticket kind.
pub const PLANNER: &str = "\n\n## The smallest correct change (CoderHelm checks these)\n\
Before you plan, read the code the change extends: the whole map, function or query, and its \
writers and readers. Then write these sections in design.md:\n\
- `## Reuse`: the existing maps, helpers, constants, config and tests this change extends, each \
with its path (search the ticket's names and the structure you would add). For each helper that \
writes data, read its body and say what it does to rows already stored: `inserts only` or \
`replaces/deletes` (a `DELETE`, an `ON CONFLICT DO UPDATE`, a delete-then-insert). A helper that \
replaces is never reused on a path that only inserts unless the ticket asks to replace. Add entries to an \
existing map or a parameter to an existing helper; never a second map or helper beside it.\n\
- `## Must not change`: what stays as it is. A path that only inserts keeps only inserting (no \
`DELETE`, no `ON CONFLICT DO UPDATE`, no replacing sync) unless the ticket asks for it; log \
levels, shared logging and other pipelines stay untouched; no new counters or reports the \
ticket doesn't ask for.\n\
- `## Measure first`: every outside system the change or its tests rest on (a vendor page or \
embed, an API, a webhook payload) and how you measure it before writing against it: the \
`probe_page` tool for a page (its test ids, sizes, what is enabled), a read-only lookup for \
data. Write `None: the change stays inside this repository` when there are none. Never plan \
selectors, fields or behaviour of another company's system from a guess.\n\
- `## Smallest change`: the change in a few lines (for example \"4 entries in \
`CAMP_OVERRIDES`; an insert with `ON CONFLICT DO NOTHING` in the 3 write branches\"), and why \
nothing smaller does it.\n\
- `## Proof`: the one test or check that shows it, sized to the change: the new behaviour and \
that nothing else changed, through the real entry point, with real or probed data. When an \
acceptance criterion and an outside system disagree, plan the product change that meets the \
criterion first; a test is changed to match the outside system only with the measured reason.\n\
- `## Out of scope`: scope is decided once, here. One `- <item>: <why>` line for each thing the \
change leaves out, naming the code (backticked names, paths, routes) that would show it, or \
`None`. A product decision the ticket doesn't settle is decided now with its default (today's \
behaviour, or the smallest choice the ticket allows) and listed here as `- <item>: decided with \
the default: <default>`; never write open questions for the team. No task may name an item \
listed here, and no task is open-ended (\"switch any other caller\"): search now and name each \
file. Every later round and the pull request description are held to this list.\n";

fn names_something(s: &str) -> bool {
    s.contains('`')
        || s.split_whitespace()
            .any(|t| t.contains('/') && !t.contains("://"))
        || s.to_lowercase().contains("searched")
        || s.to_lowercase().contains("none")
}

fn stops(files: &PlanResult) -> bool {
    let t = files.tasks.trim_start();
    t.is_empty() || t.starts_with("NO_CHANGES_NEEDED") || t.starts_with("CLARIFICATION_NEEDED")
}

/// Backticked identifiers in `text` (names of code, not paths or prose).
fn backticked(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (i, part) in text.split('`').enumerate() {
        if i % 2 == 0 {
            continue;
        }
        let p = part.trim().trim_end_matches("()");
        let ok = p.len() >= 4
            && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && (p.contains('_') || p.chars().skip(1).any(|c| c.is_ascii_uppercase()));
        if ok && !out.iter().any(|o| o == p) {
            out.push(p.to_string());
        }
    }
    out
}

fn as_def(name: &str) -> Def {
    let table = name
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    Def {
        name: name.to_string(),
        kind: if table {
            DefKind::Table
        } else {
            DefKind::Function
        },
    }
}

const NEW_STRUCTURE: &[&str] = &[
    "new map",
    "new dict",
    "new mapping",
    "separate map",
    "separate mapping",
    "separate dict",
    "parallel map",
    "second map",
    "new lookup",
    "new override",
    "new constant",
    "new helper",
    "new config",
];

const REPLACING_WORDS: &[&str] = &[
    "upsert",
    "do update",
    "delete ",
    "deletes ",
    "deleting",
    "replace the",
    "replaces the",
    "replacing",
    "overwrite",
    "truncate",
    "prune",
    "purge",
    "remove stale",
    "removes stale",
    "sync removals",
];

const LOG_WORDS: &[&str] = &[
    "log level",
    "to debug",
    "to info",
    "setlevel",
    "info to debug",
    "debug to info",
];

const REPORT_WORDS: &[&str] = &[
    "summary counter",
    "counters",
    "count of skipped",
    "warning per run",
    "report the skipped",
    "report skipped",
];

const OUTSIDE_WORDS: &[&str] = &[
    "iframe",
    "embed",
    "vendor",
    "third-party",
    "third party",
    "portal",
    "widget",
    "webhook",
    "external api",
    "partner",
];

const MEASURED_WORDS: &[&str] = &[
    "probe",
    "measured",
    "observed",
    "lookup",
    "looked up",
    "fetched",
    "curl",
    "none:",
    "none.",
];

const TEST_TO_VENDOR: &[&str] = &[
    "update the test to match",
    "update the tests to match",
    "change the test to",
    "change the assertion",
    "adjust the assertion",
    "relax the assertion",
    "expect the vendor",
    "accept the vendor",
    "match the vendor",
    "match the portal",
];

/// The plan's words that propose things, without its Must not change
/// section (where "no deletes" is said).
fn proposing_text(files: &PlanResult) -> String {
    let design = match section(&files.design, MUST_NOT_CHANGE) {
        Some(s) => files.design.replace(s, ""),
        None => files.design.clone(),
    };
    format!("{}\n{}\n{}", files.proposal, design, files.tasks).to_lowercase()
}

fn found<'a>(text: &str, words: &'a [&str]) -> Option<&'a str> {
    words.iter().find(|w| text.contains(*w)).copied()
}

/// What a draft misses or proposes past its ticket, one line each for the
/// send-back. `ticket` is the ticket's own words (title, body, answers);
/// `is_feature` skips the Reuse section, which the new-feature rules
/// already ask for; `existing` holds names the worker found for the
/// ticket's words.
pub fn plan_gaps(
    files: &PlanResult,
    ticket: &str,
    is_feature: bool,
    existing: &[String],
) -> Vec<String> {
    if stops(files) {
        return Vec::new();
    }
    let mut out = Vec::new();
    let design = &files.design;
    let mut need = vec![MUST_NOT_CHANGE, MEASURE_FIRST, SMALLEST_CHANGE, PROOF];
    if !common::scope_ledger::has_out_section(design) {
        need.push(OUT_OF_SCOPE);
    }
    if !is_feature {
        need.insert(0, REUSE);
    }
    let missing: Vec<&str> = need
        .into_iter()
        .filter(|h| *h == OUT_OF_SCOPE || !section(design, h).is_some_and(|s| !s.trim().is_empty()))
        .collect();
    if !missing.is_empty() {
        out.push(format!(
            "design.md lacks {}. Write each: Reuse (existing structures by path), Must not change, Measure first (outside systems and how they are measured, or `None`), Smallest change (the change in a few lines), Proof (the one test or check), Out of scope (`- <item>: <why>` for each thing left out, open product questions decided with their default, or `None`).",
            missing
                .iter()
                .map(|h| format!("`{h}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let reuse = section(design, REUSE).unwrap_or("");
    if !reuse.trim().is_empty() && !names_something(reuse) {
        out.push(format!(
            "`{REUSE}` names no existing structure. Search the ticket's names and the structure you would add, and list what you extend by path, or say what you searched."
        ));
    }
    let text = proposing_text(files);
    // A new structure beside an existing one.
    let mut known: Vec<String> = backticked(reuse);
    known.extend(existing.iter().cloned());
    let proposed: Vec<String> = backticked(&format!("{}\n{}", files.tasks, files.design))
        .into_iter()
        .filter(|n| !known.contains(n))
        .collect();
    let mut parallel: Vec<(String, String)> = Vec::new();
    for p in &proposed {
        if let Some(k) = known
            .iter()
            .find(|k| minimal_change::parallels(&as_def(p), &as_def(k)))
        {
            parallel.push((p.clone(), k.clone()));
        }
    }
    if let Some((p, k)) = parallel.first() {
        out.push(format!(
            "The plan adds `{p}` next to the existing `{k}`. Extend `{k}` (add the entries or a parameter there) instead of a parallel structure."
        ));
    } else if let Some(w) = found(&text, NEW_STRUCTURE) {
        if !known.is_empty() {
            out.push(format!(
                "The plan proposes a {w} while `{REUSE}` names `{}`. Extend the existing one, or say under `{SMALLEST_CHANGE}` why it can't hold this.",
                known[0]
            ));
        }
    }
    if !minimal_change::ticket_asks(ticket, minimal_change::REPLACE_ASKS) {
        if let Some(w) = found(&text, REPLACING_WORDS) {
            out.push(format!(
                "The plan would delete or overwrite (\"{}\") but the ticket asks for no deletes or replacements. Keep the write additive (insert, `ON CONFLICT DO NOTHING`) and list the existing rows under `{MUST_NOT_CHANGE}`.",
                w.trim()
            ));
        }
    }
    if !minimal_change::ticket_asks(ticket, minimal_change::LOG_ASKS) {
        if let Some(w) = found(&text, LOG_WORDS) {
            out.push(format!(
                "The plan changes logging (\"{}\") the ticket doesn't ask about. Leave log levels and shared logging as they are.",
                w.trim()
            ));
        }
    }
    if !minimal_change::ticket_asks(ticket, minimal_change::REPORT_ASKS) {
        if let Some(w) = found(&text, REPORT_WORDS) {
            out.push(format!(
                "The plan adds reporting (\"{}\") the ticket doesn't ask for. Leave it out.",
                w.trim()
            ));
        }
    }
    let ticket_l = ticket.to_lowercase();
    let outside = found(&ticket_l, OUTSIDE_WORDS).or_else(|| found(&text, OUTSIDE_WORDS));
    if let Some(w) = outside {
        let measured = section(design, MEASURE_FIRST)
            .map(|s| found(&s.to_lowercase(), MEASURED_WORDS).is_some())
            .unwrap_or(false);
        if !measured {
            out.push(format!(
                "The change rests on an outside system (\"{w}\") and `{MEASURE_FIRST}` plans no measurement. Probe it first (`probe_page` for a page: its test ids, frame size and scrolling, what is enabled; a read-only lookup for data) and plan the code and tests from what it shows."
            ));
        }
        let criteria = super::stop_judge::acceptance(ticket);
        if !criteria.is_empty() {
            if let Some(w) = found(&text, TEST_TO_VENDOR) {
                out.push(format!(
                    "The plan would change a test to match the outside system (\"{w}\") while the ticket's criteria say what the product must do. Plan the product change that meets the criterion first; change the test only with the measured reason it can't."
                ));
            }
        }
    }
    // Scope decided once: no question left open, a reason for each item, no
    // task naming an item left out, no open-ended task.
    out.extend(common::scope_ledger::decision_gaps(
        &files.proposal,
        &files.design,
        &files.tasks,
        &super::stop_judge::acceptance(ticket),
    ));
    out
}

/// What the plan's criteria map misses (`common::e2e_proof::map_gaps`):
/// the ticket's criteria read from its acceptance list or Expected lines,
/// each that names a page or what a person sees mapped to an end-to-end
/// test of that page. `suite`: the repository has an end-to-end suite.
pub fn criteria_gaps(files: &PlanResult, title: &str, body: &str, suite: bool) -> Vec<String> {
    if stops(files) {
        return Vec::new();
    }
    let criteria = common::e2e_proof::criteria(title, body, &super::stop_judge::acceptance(body));
    common::e2e_proof::map_gaps(
        &criteria,
        &common::e2e_proof::parse_map(&files.design),
        suite,
    )
}

// ── What reused helpers do to stored rows ──────────────────────────────────

/// A helper the plan reuses, and what it does to stored rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Helper {
    pub name: String,
    pub path: String,
    pub writes: minimal_change::Writes,
}

/// What the plan's reused helpers and the planned files do to stored rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteFacts {
    pub helpers: Vec<Helper>,
    /// The planned code files, without the reused helpers' own bodies, only
    /// insert today.
    pub path_inserts_only: bool,
}

/// The function-like names under the plan's Reuse section.
pub fn reused_functions(files: &PlanResult) -> Vec<String> {
    let reuse = section(&files.design, REUSE).unwrap_or("");
    backticked(reuse)
        .into_iter()
        .filter(|n| n.chars().any(|c| c.is_ascii_lowercase()))
        .collect()
}

/// The facts from the helpers' sources (`(name, path, file text)`) and the
/// planned code files' base texts (`(path, text)`).
pub fn write_facts(
    sources: &[(String, String, String)],
    planned: &[(String, String)],
) -> WriteFacts {
    let mut helpers = Vec::new();
    let mut bodies: Vec<String> = Vec::new();
    for (name, path, text) in sources {
        if let Some(body) = minimal_change::body_of(text, path, name) {
            helpers.push(Helper {
                name: name.clone(),
                path: path.clone(),
                writes: minimal_change::write_semantics(&body),
            });
            bodies.push(body);
        }
    }
    let rest: Vec<String> = planned
        .iter()
        .map(|(_, t)| {
            let mut t = t.clone();
            for b in &bodies {
                t = t.replace(b.as_str(), "");
            }
            t
        })
        .collect();
    let refs: Vec<&str> = rest.iter().map(String::as_str).collect();
    WriteFacts {
        helpers,
        path_inserts_only: !refs.is_empty() && minimal_change::inserts_only(&refs),
    }
}

const SAYS_REPLACES: &[&str] = &["replace", "delete", "overwrite", "do update", "prune"];

/// What the plan misses about the rows its reused helpers touch: a
/// replacing helper the Reuse section doesn't say replaces, and a replacing
/// helper reused where the code only inserts, when the ticket asks for no
/// replacement.
pub fn write_gaps(files: &PlanResult, ticket: &str, facts: &WriteFacts) -> Vec<String> {
    if stops(files) {
        return Vec::new();
    }
    let reuse = section(&files.design, REUSE).unwrap_or("").to_lowercase();
    let mut out = Vec::new();
    for h in &facts.helpers {
        let minimal_change::Writes::Replacing(how) = &h.writes else {
            continue;
        };
        let line = reuse
            .lines()
            .find(|l| l.contains(&h.name.to_lowercase()))
            .unwrap_or("");
        if !SAYS_REPLACES.iter().any(|w| line.contains(w)) {
            out.push(format!(
                "`{REUSE}` names `{}` without what it does to stored rows: it replaces them ({}, in `{}`). Say so next to it.",
                h.name,
                how.join(", "),
                h.path
            ));
        }
        if facts.path_inserts_only
            && !minimal_change::ticket_asks(ticket, minimal_change::REPLACE_ASKS)
        {
            out.push(format!(
                "The plan reuses `{}`, which replaces stored rows ({}), on a path that only inserts today, and the ticket asks for no replacement. Keep the path additive: insert the new rows with `ON CONFLICT DO NOTHING` (or the insert-only helper the path already uses), and list the existing rows under `{MUST_NOT_CHANGE}`.",
                h.name,
                how.join(", ")
            ));
        }
    }
    out
}

/// Reads the facts: each reused function's definition (found by search,
/// read at `base`) and the planned code files at `base`.
pub async fn read_write_facts(
    search: &dyn RepoSearch,
    github: &crate::clients::github::GitHubClient,
    (owner, repo, base): (&str, &str, &str),
    files: &PlanResult,
) -> WriteFacts {
    let own = format!("{owner}/{repo}");
    let mut sources = Vec::new();
    for name in reused_functions(files).into_iter().take(6) {
        let Ok(hits) = search.search(&own, &name).await else {
            continue;
        };
        let defining = hits.iter().find_map(|h| {
            let path = h.lines().next()?.to_string();
            let defines = [
                format!("def {name}("),
                format!("function {name}"),
                format!("fn {name}"),
                format!("const {name} ="),
            ];
            h.lines()
                .skip(1)
                .any(|l| defines.iter().any(|d| l.contains(d.as_str())))
                .then_some(path)
        });
        let Some(path) = defining else {
            continue;
        };
        if let Ok(text) = github.read_file(owner, repo, &path, base).await {
            sources.push((name, path, text));
        }
    }
    let mut planned = Vec::new();
    for f in super::feature::plan_files::planned_files(&files.tasks)
        .into_iter()
        .filter(|f| !super::habits::is_test_path(&f.path))
        .take(8)
    {
        if let Ok(text) = github.read_file(owner, repo, &f.path, base).await {
            planned.push((f.path.clone(), text));
        }
    }
    write_facts(&sources, &planned)
}

// ── What the ticket's words find, before the plan ──────────────────────────

const STOP: &[&str] = &[
    "The",
    "This",
    "That",
    "These",
    "Those",
    "When",
    "Then",
    "Given",
    "And",
    "But",
    "For",
    "With",
    "From",
    "Add",
    "Make",
    "Update",
    "Change",
    "Fix",
    "Remove",
    "Link",
    "Please",
    "Should",
    "Acceptance",
    "Criteria",
    "Ticket",
    "Summary",
    "Description",
    "Steps",
    "Expected",
    "Actual",
];

/// The search terms a ticket's own words give: code names in backticks,
/// identifiers, quoted names and capitalised names mid-sentence, at most
/// six, the most specific first.
pub fn ticket_terms(title: &str, body: &str) -> Vec<String> {
    let text = format!("{title}\n{body}");
    let mut ids: Vec<String> = backticked(&text);
    for w in text.split(|c: char| c.is_whitespace() || matches!(c, ',' | ';' | '(' | ')' | ':')) {
        let w = w.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_');
        let id = w.len() >= 5
            && w.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && (w.contains('_') || w.chars().skip(1).any(|c| c.is_ascii_uppercase()))
            && w.chars().any(|c| c.is_ascii_lowercase() || c == '_');
        if id && !ids.iter().any(|i| i == w) {
            ids.push(w.to_string());
        }
    }
    let mut quoted = Vec::new();
    for q in ['"', '\u{201c}'] {
        let close = if q == '"' { '"' } else { '\u{201d}' };
        let mut rest = text.as_str();
        while let Some(i) = rest.find(q) {
            let after = &rest[i + q.len_utf8()..];
            let Some(j) = after.find(close) else { break };
            let s = after[..j].trim();
            if (3..=40).contains(&s.len()) && !s.contains('\n') && !quoted.iter().any(|x| x == s) {
                quoted.push(s.to_string());
            }
            rest = &after[j + close.len_utf8()..];
        }
    }
    let mut proper = Vec::new();
    for sentence in text.split(['.', '\n', '!', '?']) {
        for (i, w) in sentence.split_whitespace().enumerate() {
            let w = w.trim_matches(|c: char| !c.is_ascii_alphanumeric());
            if i > 0
                && w.len() >= 4
                && w.chars().next().is_some_and(|c| c.is_ascii_uppercase())
                && w.chars().skip(1).all(|c| c.is_ascii_lowercase())
                && !STOP.contains(&w)
                && !proper.iter().any(|p| p == w)
            {
                proper.push(w.to_string());
            }
        }
    }
    ids.into_iter()
        .chain(quoted)
        .chain(proper)
        .take(6)
        .collect()
}

/// What the ticket's words find in the repository, as (term, hits), at
/// most eight hits a term.
pub async fn existing_for_ticket(
    search: &dyn RepoSearch,
    repo: &str,
    title: &str,
    body: &str,
) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    for t in ticket_terms(title, body) {
        if let Ok(hits) = search.search(repo, &t).await {
            if !hits.is_empty() {
                out.push((t, hits.into_iter().take(8).collect()));
            }
        }
    }
    out
}

/// The top-level names the hits show (maps, helpers), for the plan gate.
pub fn existing_names(found: &[(String, Vec<String>)]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (_, hits) in found {
        for h in hits {
            let path = h.lines().next().unwrap_or("");
            for line in h.lines().skip(1) {
                // A match may carry its line number: `12: CODE`.
                let line = match line.split_once(": ") {
                    Some((n, l)) if n.trim().chars().all(|c| c.is_ascii_digit()) => l,
                    _ => line,
                };
                if let Some(d) = minimal_change::top_level_def(line, path) {
                    if !out.contains(&d.name) {
                        out.push(d.name);
                    }
                }
            }
        }
    }
    out
}

/// The planner's block for what the ticket's words found: read these
/// first, and extend them.
pub fn existing_block(found: &[(String, Vec<String>)]) -> String {
    if found.is_empty() {
        return String::new();
    }
    let mut s = String::from(
        "\n\n## Existing code the ticket's words find (read it before planning)\nCoderHelm searched the repository for the ticket's own names. Read each hit that the change touches in full (the whole map, function or query) and extend it; list it under `## Reuse`.\n",
    );
    for (term, hits) in found {
        s.push_str(&format!("\n### `{term}`\n"));
        for h in hits {
            s.push_str(&format!("- {}\n", common::truncate_str(h.trim(), 300)));
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(design: &str, tasks: &str) -> PlanResult {
        PlanResult {
            proposal: String::new(),
            design: design.into(),
            tasks: tasks.into(),
            spec: String::new(),
            ..Default::default()
        }
    }

    const GOOD: &str = "## Reuse\n- `CAMP_MULTI_SPORT_OVERRIDES` and `get_camp_multi_sport_names` in dags/camps/sports.py\n\n## Must not change\nNo deletes: existing links stay. Logging stays as it is.\n\n## Measure first\nNone: the change stays inside this repository.\n\n## Smallest change\n4 entries in `CAMP_MULTI_SPORT_OVERRIDES`; in the 3 write branches insert links for `get_camp_multi_sport_names` with ON CONFLICT DO NOTHING.\n\n## Proof\nOne test: the 4 names get Swimming and their own sport, others only their own, nothing deleted.\n\n## Out of scope\nNone: the ticket names the four camps and nothing else.\n";

    #[test]
    fn the_small_plan_passes() {
        let p = plan(
            GOOD,
            "- [ ] dags/camps/sports.py: add 4 entries\n- [ ] tests/test_sports.py: one test",
        );
        assert!(plan_gaps(
            &p,
            "Link four camps to Swimming as well as their sport",
            false,
            &[]
        )
        .is_empty());
    }

    #[test]
    fn missing_sections_go_back() {
        let p = plan("## Overview\nstuff", "- [ ] a.py: x");
        let g = plan_gaps(&p, "x", false, &[]);
        assert_eq!(g.len(), 1);
        for h in [
            REUSE,
            MUST_NOT_CHANGE,
            MEASURE_FIRST,
            SMALLEST_CHANGE,
            PROOF,
            OUT_OF_SCOPE,
        ] {
            assert!(g[0].contains(h), "{h}");
        }
        // A plan that stops plans nothing.
        assert!(plan_gaps(&plan("", "NO_CHANGES_NEEDED: done"), "x", false, &[]).is_empty());
    }

    #[test]
    fn a_parallel_map_and_a_replacing_sync_are_rejected() {
        let design = GOOD.replace(
            "## Smallest change\n",
            "## Smallest change\nAdd `CAMP_SWIMMING_OVERRIDES` and `get_camp_swimming_names`; the sync upserts links and deletes stale ones. Change the INFO log to debug.\n",
        );
        let g = plan_gaps(
            &plan(&design, "- [ ] dags/camps/sports.py: x"),
            "Link four camps to Swimming",
            false,
            &[],
        );
        assert!(
            g.iter().any(|l| l.contains(
                "`CAMP_SWIMMING_OVERRIDES` next to the existing `CAMP_MULTI_SPORT_OVERRIDES`"
            )),
            "{g:?}"
        );
        assert!(g.iter().any(|l| l.contains("delete or overwrite")), "{g:?}");
        assert!(g.iter().any(|l| l.contains("changes logging")), "{g:?}");
    }

    #[test]
    fn an_outside_page_is_measured_and_the_product_changes_first() {
        let design = GOOD.replace(
            "None: the change stays inside this repository.",
            "The vendor portal is loaded in an iframe; we assume it has no inner scroll.",
        ) + "\nIf the vendor scrolls, update the test to match the vendor.\n";
        let ticket = "Embed the booking portal.\n\nAcceptance criteria:\n- No double scrollbars on desktop\n";
        let g = plan_gaps(
            &plan(&design, "- [ ] e2e/portal.spec.ts: x"),
            ticket,
            false,
            &[],
        );
        assert!(
            g.iter().any(|l| l.contains("plans no measurement")),
            "{g:?}"
        );
        assert!(
            g.iter()
                .any(|l| l.contains("product change that meets the criterion first")),
            "{g:?}"
        );
        let measured = design
            .replace(
                "we assume it has no inner scroll",
                "probe_page measured the frame and its test ids",
            )
            .replace(
                "If the vendor scrolls, update the test to match the vendor.",
                "",
            );
        assert!(plan_gaps(
            &plan(&measured, "- [ ] e2e/portal.spec.ts: x"),
            ticket,
            false,
            &[]
        )
        .is_empty());
    }

    #[test]
    fn the_tickets_words_become_searches() {
        let t = ticket_terms(
            "Link full-day camps to Swimming",
            "These camps should also show under Swimming: \"Aqua Day Camp\", \"Swim Week\". See `CAMP_MULTI_SPORT_OVERRIDES`.",
        );
        assert_eq!(t[0], "CAMP_MULTI_SPORT_OVERRIDES");
        assert!(t.contains(&"Aqua Day Camp".to_string()));
        assert!(t.contains(&"Swimming".to_string()));
        let names = existing_names(&[(
            "Swimming".into(),
            vec!["dags/camps/sports.py\nCAMP_MULTI_SPORT_OVERRIDES = {".into()],
        )]);
        assert_eq!(names, vec!["CAMP_MULTI_SPORT_OVERRIDES"]);
        assert!(
            existing_block(&[("Swimming".into(), vec!["dags/a.py".into()])]).contains("## Reuse")
        );
    }
}
