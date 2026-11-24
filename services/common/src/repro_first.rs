//! A bug is reproduced on the base before any code is planned, and the fix
//! goes in the mechanism the reproduction lands in.
//!
//! A ticket named four charters filed under the wrong activity. The
//! charters' names carried a suffix ("Kayak Charter-Half Day"), and the
//! existing overrides table matched names exactly, so its entries for those
//! charters never matched. The run's own notes said as much, then planned a
//! keyword matcher beside the table, rewrote how every charter of that source
//! got its activity, moved other records to a different lookup and asked
//! the team a question one page load answered. The right change stripped the suffix in
//! the lookup and added two entries, with a test built from the exact names
//! stored on staging that failed on the base.
//!
//! The rules here, pure text, read by the plan check:
//!
//! - **`## Reproduce`** ([`reproduce_gaps`]): a bug plan writes the exact
//!   inputs (the ticket's ids and names, with every variant a read-only
//!   lookup found), the call or command it ran on the base with its actual
//!   output, and the one line where the existing code goes wrong. The
//!   command must be one this run ran on the base (`reproduce_on_base`,
//!   [`Attempt`]), and the output is quoted from what came back. A run that
//!   truly can't reproduce writes `Unverified:` with why; it is kept as
//!   unverified, never skipped silently.
//! - **Fix the mechanism found** ([`reuse_gaps`]): when the wrong line is
//!   in an existing lookup, table or mapping, `## Smallest change` names it
//!   and changes it, or says why it can't; a plan that adds a new
//!   resolution path beside it goes back.
//! - **Questions are looked up first** ([`question_gaps`]): a question the
//!   plan decides with a default, about what a page shows or what the data
//!   holds, carries what the lookup showed (`Looked up:`) or why no lookup
//!   settles it (`No lookup settles it:`).
//! - **The ticket's records** ([`ticket_ids`]): the ids and quoted names a
//!   ticket names, for the reproduction's inputs and for sizing the diff
//!   against the ticket (`minimal_change`).

use serde::{Deserialize, Serialize};

/// The plan's section.
pub const HEADING: &str = "## Reproduce";
/// The planner's tool that runs a call or command on the base.
pub const TOOL: &str = "reproduce_on_base";

const INPUTS: &str = "inputs:";
const RAN: &str = "ran on base:";
const OUTPUT: &str = "output:";
const WRONG: &str = "wrong line:";
const UNVERIFIED: &str = "unverified:";
const FIELDS: &[&str] = &[INPUTS, RAN, OUTPUT, WRONG, UNVERIFIED];

/// One run of the reproduction tool on the base tree.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub command: String,
    /// The command's exit code; `None` when the sandbox gave none.
    #[serde(default)]
    pub exit: Option<i32>,
    /// What it printed (the end of it, when long).
    #[serde(default)]
    pub output: String,
    /// Why it couldn't run, when it didn't.
    #[serde(default)]
    pub error: String,
}

impl Attempt {
    /// Whether the command ran and gave an answer.
    pub fn ran(&self) -> bool {
        self.error.is_empty() && self.exit.is_some()
    }
}

/// The plan's `## Reproduce` section, field by field.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reproduce {
    pub inputs: String,
    pub ran: String,
    pub output: String,
    pub wrong_line: String,
    pub unverified: String,
}

fn lower(s: &str) -> String {
    s.to_lowercase()
}

fn flat(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// A markdown section's body: from its heading to the next `## ` heading.
pub fn section<'a>(text: &'a str, heading: &str) -> Option<&'a str> {
    let h = lower(heading);
    let mut start = None;
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        let t = line.trim();
        if let Some(s) = start {
            if t.starts_with("## ") {
                return Some(&text[s..offset]);
            }
        } else if lower(t) == h {
            start = Some(offset + line.len());
        }
        offset += line.len();
    }
    start.map(|s| &text[s..])
}

/// Reads the section's fields. A field runs from its label to the next
/// label; fences and bullets are kept as written.
pub fn read(design: &str) -> Option<Reproduce> {
    let body = section(design, HEADING)?;
    let mut out = Reproduce::default();
    let mut current: Option<&str> = None;
    for line in body.lines() {
        let t = line.trim().trim_start_matches(['-', '*']).trim();
        let t = t.trim_start_matches("**");
        let l = lower(t);
        let label = FIELDS
            .iter()
            .find(|f| l.starts_with(**f) || l.replacen("**", "", 2).starts_with(**f));
        let rest = if let Some(f) = label {
            current = Some(f);
            let cut = t.to_lowercase().find(':').map_or(0, |i| i + 1);
            t[cut..].trim().trim_start_matches("**").trim().to_string()
        } else {
            line.to_string()
        };
        let slot = match current {
            Some(INPUTS) => &mut out.inputs,
            Some(RAN) => &mut out.ran,
            Some(OUTPUT) => &mut out.output,
            Some(WRONG) => &mut out.wrong_line,
            Some(UNVERIFIED) => &mut out.unverified,
            _ => continue,
        };
        if !slot.is_empty() {
            slot.push('\n');
        }
        slot.push_str(&rest);
    }
    for s in [
        &mut out.inputs,
        &mut out.ran,
        &mut out.output,
        &mut out.wrong_line,
        &mut out.unverified,
    ] {
        *s = s.trim().to_string();
    }
    Some(out)
}

fn is_uuid(w: &str) -> bool {
    let parts: Vec<&str> = w.split('-').collect();
    parts.len() == 5
        && [8, 4, 4, 4, 12]
            .iter()
            .zip(&parts)
            .all(|(n, p)| p.len() == *n && p.chars().all(|c| c.is_ascii_hexdigit()))
}

/// The records a ticket names: its uuids, numbered ids (`id 8204`, `#8204`,
/// `(8204)`), and backticked or quoted names, each once.
pub fn ticket_ids(ticket: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |s: &str| {
        let s = s.trim();
        if !s.is_empty() && !out.iter().any(|o| o.eq_ignore_ascii_case(s)) {
            out.push(s.to_string());
        }
    };
    let words: Vec<&str> = ticket
        .split(|c: char| c.is_whitespace() || matches!(c, '(' | ')' | ',' | ';' | '[' | ']'))
        .collect();
    for (i, w) in words.iter().enumerate() {
        let w = w.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-');
        if is_uuid(w) {
            push(w);
            continue;
        }
        let digits = w.trim_start_matches('#');
        let numbered = digits.len() >= 3 && digits.chars().all(|c| c.is_ascii_digit());
        let after_id = i > 0 && {
            let p = words[i - 1].to_lowercase();
            p == "id" || p == "id:" || p.ends_with("_id") || p.ends_with("id:")
        };
        if numbered && (after_id || words[i].starts_with('#')) {
            push(digits);
        }
    }
    for (open, close) in [('`', '`'), ('"', '"'), ('\u{201c}', '\u{201d}')] {
        let mut rest = ticket;
        while let Some(i) = rest.find(open) {
            let after = &rest[i + open.len_utf8()..];
            let Some(j) = after.find(close) else { break };
            let s = &after[..j];
            if (3..=80).contains(&s.trim().len()) && !s.contains('\n') && !s.contains("://") {
                push(s);
            }
            rest = &after[j + close.len_utf8()..];
        }
    }
    out
}

/// What a bug plan's `## Reproduce` misses, one line each for the
/// send-back. `ticket` is the ticket's words; `attempts` the runs of
/// [`TOOL`] this run made; `can_run` says the sandbox can run on the base;
/// `can_look_up` says the team has a data source connected.
pub fn reproduce_gaps(
    design: &str,
    ticket: &str,
    attempts: &[Attempt],
    can_run: bool,
    can_look_up: bool,
) -> Vec<String> {
    let Some(r) = read(design) else {
        return vec![format!(
            "design.md lacks `{HEADING}`. This is a bug: before planning code, take the exact names or ids from the ticket (and every variant a read-only lookup finds in the real data), run them through the current code on the base with `{TOOL}`, and write `Inputs:`, `Ran on base:` (the command), `Output:` (what it printed) and `Wrong line:` (`path:line` and the code there that goes wrong)."
        )];
    };
    let mut out = Vec::new();
    let ids = ticket_ids(ticket);
    let inputs = lower(&r.inputs);
    let missing: Vec<&String> = ids.iter().filter(|i| !inputs.contains(&lower(i))).collect();
    if r.inputs.is_empty() {
        out.push(format!(
            "`{HEADING}` has no `Inputs:`. List the exact names or ids the ticket gives, as they are stored, with every variant a read-only lookup finds."
        ));
    } else if !missing.is_empty() {
        out.push(format!(
            "`{HEADING}` `Inputs:` leaves out what the ticket names: {}. Reproduce with each of them.",
            missing
                .iter()
                .take(6)
                .map(|m| format!("`{m}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if can_look_up && !r.inputs.is_empty() && !cites_lookup(&r.inputs) {
        out.push(format!(
            "`{HEADING}` `Inputs:` doesn't say where the names came from. Query the connected data read-only for the ticket's names (a `LIKE` on the name, not an exact match) and list every variant as stored, citing the lookup."
        ));
    }
    if !r.unverified.is_empty() {
        if can_run && attempts.is_empty() {
            out.push(format!(
                "`{HEADING}` says `Unverified:` but this run never tried `{TOOL}`. Run the call on the base first; only a run that can't reproduce is unverified."
            ));
        }
        if !has_location(&r.wrong_line) {
            out.push(wrong_line_gap());
        }
        return out;
    }
    if r.ran.is_empty() {
        out.push(format!(
            "`{HEADING}` has no `Ran on base:`. Run the ticket's inputs through the current code with `{TOOL}` (for example `python -c \"from module import fn; print(fn('<name>'))\"`) and write the command and its output. When it truly can't run, write `Unverified: <why>`."
        ));
    } else {
        match matching_attempt(&r.ran, attempts) {
            None => out.push(format!(
                "`{HEADING}` `Ran on base:` is not a command this run ran. Run it with `{TOOL}` and quote that command."
            )),
            Some(a) if !a.ran() => out.push(format!(
                "`{HEADING}`: `{}` didn't run on the base ({}). Fix the command and run it again, or write `Unverified: <why>`.",
                common_trunc(&a.command, 120),
                if a.error.is_empty() { "no result" } else { a.error.as_str() }
            )),
            Some(a) => {
                if r.output.is_empty() || !quoted_from(&r.output, &a.output) {
                    out.push(format!(
                        "`{HEADING}` `Output:` is not what `{}` printed. Quote its output as it came back.",
                        common_trunc(&a.command, 120)
                    ));
                }
            }
        }
    }
    if !has_location(&r.wrong_line) {
        out.push(wrong_line_gap());
    }
    out
}

fn wrong_line_gap() -> String {
    format!(
        "`{HEADING}` `Wrong line:` doesn't name where the existing code goes wrong. Write `path:line` and the code on that line (backticked), the one line the reproduction shows is wrong."
    )
}

fn common_trunc(s: &str, n: usize) -> String {
    let s = flat(s);
    if s.chars().count() <= n {
        s
    } else {
        format!("{}...", s.chars().take(n).collect::<String>())
    }
}

fn cites_lookup(inputs: &str) -> bool {
    let l = lower(inputs);
    [
        "lookup",
        "looked up",
        "query",
        "queried",
        "select ",
        "database",
        "staging",
        "prod",
        "page",
        "check_url",
        "probe",
        "rows",
        "stored",
    ]
    .iter()
    .any(|w| l.contains(w))
        || l.split(|c: char| !c.is_ascii_alphanumeric()).any(|w| {
            w.len() >= 2 && w.starts_with('l') && w[1..].chars().all(|c| c.is_ascii_digit())
        })
}

/// `path:line`, a file path with a line number.
fn has_location(s: &str) -> bool {
    s.split(|c: char| c.is_whitespace() || c == '`' || c == '(' || c == ')')
        .any(|w| {
            let Some((path, line)) = w.rsplit_once(':') else {
                return false;
            };
            let line = line.split(['-', ',']).next().unwrap_or("");
            path.contains('.')
                && !path.contains("://")
                && !line.is_empty()
                && line.chars().all(|c| c.is_ascii_digit())
        })
}

fn strip_fence(s: &str) -> String {
    s.lines()
        .filter(|l| !l.trim_start().starts_with("```"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The attempt a written command is: the same command once whitespace and
/// backticks are set aside.
pub fn matching_attempt<'a>(written: &str, attempts: &'a [Attempt]) -> Option<&'a Attempt> {
    let w = flat(&strip_fence(written).replace('`', ""));
    if w.is_empty() {
        return None;
    }
    attempts.iter().rev().find(|a| {
        let c = flat(&a.command);
        !c.is_empty() && (c == w || w.contains(&c) || (c.len() >= 20 && c.contains(&w)))
    })
}

/// Whether the written output is quoted from what the command printed:
/// each of its non-empty lines appears in the real output.
pub fn quoted_from(written: &str, real: &str) -> bool {
    let real = flat(real);
    let lines: Vec<String> = strip_fence(written)
        .lines()
        .map(|l| flat(l.trim().trim_matches('`')))
        .filter(|l| !l.is_empty())
        .collect();
    !lines.is_empty() && lines.iter().all(|l| real.contains(l.as_str()))
}

/// Backticked identifiers on the wrong line: the function, table or map
/// the reproduction lands in.
pub fn wrong_names(wrong_line: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (i, part) in wrong_line.split('`').enumerate() {
        if i % 2 == 0 {
            continue;
        }
        for tok in part.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
            let ident = tok.len() >= 4
                && tok
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && (tok.contains('_') || tok.chars().skip(1).any(|c| c.is_ascii_uppercase()));
            if ident && !out.iter().any(|o| o == tok) {
                out.push(tok.to_string());
            }
        }
    }
    out
}

fn wrong_path(wrong_line: &str) -> Option<String> {
    wrong_line
        .split(|c: char| c.is_whitespace() || c == '`' || c == '(' || c == ')')
        .find_map(|w| {
            let (path, line) = w.rsplit_once(':')?;
            (path.contains('.') && line.chars().next().is_some_and(|c| c.is_ascii_digit()))
                .then(|| path.to_string())
        })
}

/// Words that put a new resolution path beside an existing lookup.
const NEW_PATH: &[&str] = &[
    "keyword match",
    "keyword step",
    "keyword fallback",
    "substring match",
    "fuzzy match",
    "new resolver",
    "new resolution",
    "resolution path",
    "new lookup",
    "second lookup",
    "new matching",
    "rewrite the resolution",
    "rewrites the resolution",
    "replace the lookup",
    "replaces the lookup",
    "instead of the override",
    "bypass the override",
    "fallback chain",
];

const CANT: &[&str] = &["can't", "cannot", "can not", "doesn't hold", "won't hold"];

/// What a bug plan misses about the mechanism its reproduction landed in:
/// `## Smallest change` must name it and change it, or say why it can't,
/// and no new resolution path goes beside it. `smallest` is the plan's
/// Smallest change section; `proposing` the words that propose (proposal,
/// design without Must not change, tasks).
pub fn reuse_gaps(design: &str, smallest: &str, proposing: &str) -> Vec<String> {
    let Some(r) = read(design) else {
        return Vec::new();
    };
    let names = wrong_names(&r.wrong_line);
    let path = wrong_path(&r.wrong_line);
    if names.is_empty() && path.is_none() {
        return Vec::new();
    }
    let s = lower(smallest);
    let says_why = CANT.iter().any(|w| s.contains(w));
    let mut out = Vec::new();
    let named = names.iter().any(|n| s.contains(&lower(n)));
    let first = names.first().cloned().or(path.clone()).unwrap_or_default();
    if !named && !says_why {
        out.push(format!(
            "The reproduction lands in `{first}`, and `## Smallest change` doesn't change it. Fix the lookup that already exists (its matching, or its entries) and name it there, or say why it can't hold the fix."
        ));
    }
    let p = lower(proposing);
    if let Some(w) = NEW_PATH.iter().find(|w| p.contains(*w)) {
        if !says_why {
            out.push(format!(
                "The plan adds a new resolution path (\"{w}\") beside `{first}`, where the reproduction says the existing one goes wrong. Change `{first}` instead; a second path beside it is sent back unless `## Smallest change` says why the existing one can't hold the fix."
            ));
        }
    }
    out
}

/// Words of a question a page load answers.
const PAGE_WORDS: &[&str] = &[
    "show", "appear", "visible", "display", "listed", "list ", "filter", "chip", "render", "page",
    "see ",
];
/// Words of a question a read-only query answers.
const DATA_WORDS: &[&str] = &[
    "active",
    "inactive",
    "exist",
    "how many",
    "count",
    "stored",
    "rows",
    "linked",
    "mapped",
    "which records",
    "duplicate",
    "value of",
    "null",
];

/// How a question can be settled by looking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup {
    Page,
    Data,
}

/// The lookup that would answer a question, if one would.
pub fn answered_by(question: &str) -> Option<Lookup> {
    let q = lower(question);
    if PAGE_WORDS.iter().any(|w| q.contains(w)) {
        Some(Lookup::Page)
    } else if DATA_WORDS.iter().any(|w| q.contains(w)) {
        Some(Lookup::Data)
    } else {
        None
    }
}

const LOOKED: &[&str] = &[
    "looked up:",
    "looked up ",
    "no lookup settles it",
    "checked:",
    "loaded ",
    "queried ",
    "check_url",
    "probe_page",
];

/// Each question the plan decides with a default (or still asks) that a
/// page load or a query would answer, and that says nothing of the lookup.
/// `questions` are the question lines and the entries decided with a
/// default, as written.
pub fn question_gaps(questions: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for q in questions {
        let Some(kind) = answered_by(q) else {
            continue;
        };
        let l = lower(q);
        if LOOKED.iter().any(|w| l.contains(w)) {
            continue;
        }
        let how = match kind {
            Lookup::Page => "load the page that shows it (`check_url`, with a cache-busting parameter) and read whether it is there",
            Lookup::Data => "query the connected data read-only for it",
        };
        out.push(format!(
            "Before deciding \"{}\", {how}. Write what it showed after it (`Looked up: <what it showed>`), or `No lookup settles it: <why>`; a question a lookup answers is never asked.",
            common_trunc(q.trim().trim_start_matches(['-', '*']).trim(), 140)
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const TICKET: &str = "Observed\nWhen filtering for Fishing, Kayak Charter-Half Day (a0000002-0000-4000-8000-000000000002) shows.\nExpected\nKayak Charter- Full Day (a0000003-0000-4000-8000-000000000003)";

    fn design(reproduce: &str) -> String {
        format!(
            "## Reuse\n- `CHARTER_OVERRIDES`\n\n## Reproduce\n{reproduce}\n\n## Smallest change\nx\n"
        )
    }

    fn attempt(cmd: &str, out: &str) -> Attempt {
        Attempt {
            command: cmd.into(),
            exit: Some(0),
            output: out.into(),
            error: String::new(),
        }
    }

    const CMD: &str =
        "python -c \"from dags.common import get_override as g; print(g('Kayak Charter-Half Day'))\"";

    fn good() -> String {
        design(&format!(
            "Inputs: a0000002-0000-4000-8000-000000000002, a0000003-0000-4000-8000-000000000003; staging lookup L2: `Kayak Charter-Half Day`, `Kayak Charter-Full Day`, `Kayak Charter- Full Day`\nRan on base: `{CMD}`\nOutput:\n```\nNone\n```\nWrong line: dags/common.py:99 `return CHARTER_OVERRIDES.get(normalized)` matches the whole name, suffix included."
        ))
    }

    #[test]
    fn the_ticket_names_its_records() {
        let ids = ticket_ids(TICKET);
        assert_eq!(
            ids,
            vec![
                "a0000002-0000-4000-8000-000000000002",
                "a0000003-0000-4000-8000-000000000003"
            ]
        );
        let ids = ticket_ids(
            "Group id 8204 and #8214, the `Sailing Charter` and \"Kayak Charter\", not 2026.",
        );
        assert_eq!(
            ids,
            vec!["8204", "8214", "Sailing Charter", "Kayak Charter"]
        );
    }

    #[test]
    fn a_reproduction_quotes_a_run_on_the_base() {
        let a = vec![attempt(CMD, "===\nNone\n")];
        assert!(
            reproduce_gaps(&good(), TICKET, &a, true, true).is_empty(),
            "{:?}",
            reproduce_gaps(&good(), TICKET, &a, true, true)
        );
        // No section at all.
        let g = reproduce_gaps("## Reuse\nx\n", TICKET, &a, true, true);
        assert!(g[0].contains("lacks `## Reproduce`"));
        // A command this run never ran.
        let g = reproduce_gaps(&good(), TICKET, &[], true, true);
        assert!(
            g.iter().any(|l| l.contains("not a command this run ran")),
            "{g:?}"
        );
        // Output that isn't what came back.
        let a2 = vec![attempt(CMD, "b0000001\n")];
        let g = reproduce_gaps(&good(), TICKET, &a2, true, true);
        assert!(g.iter().any(|l| l.contains("is not what")), "{g:?}");
        // A ticket id left out of the inputs.
        let d = good().replace("a0000003-0000-4000-8000-000000000003; ", "");
        let g = reproduce_gaps(&d, TICKET, &a, true, true);
        assert!(g.iter().any(|l| l.contains("a0000003")), "{g:?}");
        // Inputs from nowhere when data could be looked up.
        let d = good().replace("staging lookup L2: ", "");
        let g = reproduce_gaps(&d, TICKET, &a, true, true);
        assert!(
            g.iter().any(|l| l.contains("where the names came from")),
            "{g:?}"
        );
        assert!(reproduce_gaps(&d, TICKET, &a, true, false).is_empty());
        // No wrong line.
        let d = good().replace("dags/common.py:99", "the lookup");
        let g = reproduce_gaps(&d, TICKET, &a, true, true);
        assert!(g.iter().any(|l| l.contains("Wrong line:")), "{g:?}");
    }

    #[test]
    fn unverified_is_kept_only_after_trying() {
        let d = design("Inputs: a0000002-0000-4000-8000-000000000002 a0000003-0000-4000-8000-000000000003 from staging rows\nUnverified: the module imports Airflow, which the sandbox can't install\nWrong line: dags/common.py:99 `CHARTER_OVERRIDES.get`");
        let g = reproduce_gaps(&d, TICKET, &[], true, true);
        assert!(g.iter().any(|l| l.contains("never tried")), "{g:?}");
        let failed = Attempt {
            command: CMD.into(),
            exit: Some(1),
            output: "ModuleNotFoundError".into(),
            error: String::new(),
        };
        assert!(reproduce_gaps(&d, TICKET, &[failed], true, true).is_empty());
        // With no sandbox it can't be tried.
        assert!(reproduce_gaps(&d, TICKET, &[], false, true).is_empty());
    }

    #[test]
    fn the_fix_goes_in_the_lookup_the_reproduction_lands_in() {
        let d = good();
        // A new keyword step beside the override table.
        let g = reuse_gaps(
            &d,
            "Add `resolve_charter_activity_id`: override first, then a keyword match on the name.",
            "a keyword step is added after the override",
        );
        assert_eq!(g.len(), 2, "{g:?}");
        assert!(g[0].contains("`CHARTER_OVERRIDES`"));
        assert!(g[1].contains("keyword"));
        // The lookup itself, fixed.
        let g = reuse_gaps(
            &d,
            "Strip the trailing `-Half Day`/`-Full Day` before `CHARTER_OVERRIDES.get`; add 2 entries.",
            "strip the suffix in the lookup",
        );
        assert!(g.is_empty(), "{g:?}");
        // A stated reason keeps a new path.
        let g = reuse_gaps(
            &d,
            "`CHARTER_OVERRIDES` can't hold this: the source gives no name.",
            "a keyword match",
        );
        assert!(g.is_empty(), "{g:?}");
        assert_eq!(
            wrong_names("dags/a.py:9 `return MAP.get(k)` in `get_activity`"),
            vec!["get_activity"]
        );
    }

    #[test]
    fn a_question_a_lookup_answers_is_looked_up_first() {
        let q = vec![
            "- Inactive Kayak charters: decided with the default: they show under the filter as today".to_string(),
            "- Brand colours: decided with the default: unchanged".to_string(),
            "- Is the activity active in staging? Looked up: L7 shows active=false".to_string(),
        ];
        let g = question_gaps(&q);
        assert_eq!(g.len(), 1, "{g:?}");
        assert!(g[0].contains("check_url"));
        assert_eq!(answered_by("How many rows are linked?"), Some(Lookup::Data));
        assert_eq!(answered_by("Which copy do we use?"), None);
    }
}
