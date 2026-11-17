//! The change is the smallest one that does what the ticket asks.
//!
//! A ticket asked to link four camps to a second sport. The pull request
//! added a second map and helpers beside the existing overrides map, turned
//! an insert-only link sync into a replacing upsert that deleted links
//! nobody asked to delete, added warnings, three summary counters and a log
//! level change in code other pipelines share, and a 432-line test with
//! fake hooks and a payload nobody had seen. The right change was four map
//! entries, an insert that does nothing on conflict in the three write
//! branches, and one small test.
//!
//! Each way the change grew is read here from the diff, with no model:
//!
//! - **A parallel structure** ([`Kind::Parallel`]): a new top-level map,
//!   list, constant table or helper whose name shares its words with an
//!   existing one in the same file (or another changed file). Extend the
//!   existing one.
//! - **Replacing where the base was additive** ([`Kind::Replacing`]): the
//!   diff adds deleting or overwriting semantics (`DELETE`, `ON CONFLICT DO
//!   UPDATE`, `TRUNCATE TABLE`, `REPLACE INTO`, an ORM delete) the base
//!   didn't have there. It blocks unless the ticket asks to delete, replace
//!   or overwrite; no reason in the coder's notes excuses it.
//! - **A log level changed** ([`Kind::LogLevel`]): the same log line or
//!   logger setting at another level. Other code reads those logs.
//! - **Reporting nobody asked for** ([`Kind::Reporting`]): new counters and
//!   log calls in a change whose ticket asks for no report, count or log.
//! - **A test out of proportion** ([`Kind::TestSize`]): many more test
//!   lines than changed code lines.
//! - **An invented outside payload** ([`Kind::InventedPayload`]): a test
//!   that fakes an outside call with a large payload literal while no probe
//!   record (`common::e2e::probe`) shows what the outside system returns.
//!
//! The coder may keep a [`Kind::Parallel`], [`Kind::LogLevel`],
//! [`Kind::Reporting`] or [`Kind::TestSize`] item by saying why on a line
//! `Wider than the ticket: <path>: <why>` in its notes ([`reasons`]).
//!
//! [`vendor_flips`] is the reviewer's check for a test assertion changed to
//! match another company's page while an acceptance criterion of the ticket
//! says otherwise: the product side changes first.

use serde::{Deserialize, Serialize};

use super::feature::reuse::{name_similarity, name_words};

/// Changed files: (path, status, patch), as the compare API gives them.
pub type Diff = [(String, String, String)];

/// How a change grew past its ticket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Parallel,
    Replacing,
    LogLevel,
    Reporting,
    TestSize,
    InventedPayload,
}

/// One way the change is wider than its ticket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Widening {
    pub kind: Kind,
    pub path: String,
    /// What the diff shows, in a few words (names, the line).
    pub what: String,
}

impl Widening {
    /// The heading a report shows it under.
    pub fn point(&self) -> &'static str {
        match self.kind {
            Kind::Parallel => "A parallel structure next to an existing one",
            Kind::Replacing => "Deletes or overwrites where the code only added",
            Kind::LogLevel => "A log level changed",
            Kind::Reporting => "Reporting the ticket doesn't ask for",
            Kind::TestSize => "A test out of proportion to the change",
            Kind::InventedPayload => "A test payload nobody observed",
        }
    }

    /// Whether a reason in the coder's notes may keep it.
    pub fn excusable(&self) -> bool {
        !matches!(self.kind, Kind::Replacing | Kind::InventedPayload)
    }

    /// What the coder is told to do, one line.
    pub fn instruction(&self) -> String {
        let why = if self.excusable() {
            format!(
                " If the ticket truly needs it, say why on a line `Wider than the ticket: {}: <why>`.",
                self.path
            )
        } else {
            String::new()
        };
        let what = match self.kind {
            Kind::Parallel => format!(
                "`{}` adds {}. Extend the existing one (add the entries or the parameter there) and remove the new one.",
                self.path, self.what
            ),
            Kind::Replacing => format!(
                "`{}` now deletes or overwrites stored data ({}) where the base only added, and the ticket doesn't ask for that. Keep the write additive (insert, `ON CONFLICT DO NOTHING`) and remove the delete or overwrite.",
                self.path, self.what
            ),
            Kind::LogLevel => format!(
                "`{}` changes a log level ({}). Other code and people read those logs; put it back.",
                self.path, self.what
            ),
            Kind::Reporting => format!(
                "`{}` adds reporting the ticket doesn't ask for ({}). Remove the new counters and log lines; follow how the surrounding code already handles the case.",
                self.path, self.what
            ),
            Kind::TestSize => format!(
                "The tests are {} for this change. Cut them to the cases the ticket names: the new behaviour and that nothing else changed, through the code's real entry point.",
                self.what
            ),
            Kind::InventedPayload => format!(
                "`{}` fakes an outside call with a payload nobody observed ({}). Build the fixture from a real response (probe it, or copy an existing fixture in the repository), or test the change below the outside call.",
                self.path, self.what
            ),
        };
        format!("{what}{why}")
    }
}

// ── Words ──────────────────────────────────────────────────────────────────

/// Whether the ticket's words ask for any of `words`.
pub fn ticket_asks(ticket: &str, words: &[&str]) -> bool {
    let t = ticket.to_lowercase();
    words.iter().any(|w| t.contains(w))
}

/// Ticket words that ask to delete, replace or overwrite.
pub const REPLACE_ASKS: &[&str] = &[
    "delete",
    "remove",
    "replace",
    "overwrite",
    "prune",
    "purge",
    "clean up",
    "cleanup",
    "dedup",
    "upsert",
    "update existing",
    "resync",
    "re-sync",
    "truncate",
    "unlink",
    "no longer",
    "drop ",
];

/// Ticket words that ask about logging.
pub const LOG_ASKS: &[&str] = &["log", "verbose", "noisy", "debug"];

/// Ticket words that ask for reporting.
pub const REPORT_ASKS: &[&str] = &[
    "report", "count", "metric", "summary", "warn", "alert", "log", "monitor", "observab",
];

const OUTSIDE_LINE: &str = "wider than the ticket:";

/// The coder's reasons for keeping a wider change, from its notes
/// (`Wider than the ticket: <path>: <why>`).
pub fn reasons(notes: &str) -> Vec<(String, String)> {
    notes
        .lines()
        .filter_map(|l| {
            let l = l.trim().trim_start_matches(['-', '*']).trim();
            let at = l.to_ascii_lowercase().find(OUTSIDE_LINE)?;
            let rest = l[at + OUTSIDE_LINE.len()..].trim();
            let (path, why) = rest.split_once(": ")?;
            let path = path.trim().trim_matches('`').trim();
            let why = why.trim();
            (!path.is_empty() && !why.is_empty()).then(|| (path.to_string(), why.to_string()))
        })
        .collect()
}

// ── Files and lines ────────────────────────────────────────────────────────

fn is_test(path: &str) -> bool {
    super::habits::is_test_path(path)
        || path.to_ascii_lowercase().contains("/e2e/")
        || path.to_ascii_lowercase().starts_with("e2e/")
        || path.contains("/fixtures/")
        || path.contains("/__fixtures__/")
        || path.contains("/testdata/")
}

fn code_file(path: &str) -> bool {
    let l = path.to_lowercase();
    [
        ".sql", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".py", ".rb", ".go", ".rs", ".kt", ".java",
        ".cs", ".php", ".scala",
    ]
    .iter()
    .any(|e| l.ends_with(e))
        && !l.contains(".d.ts")
        && !l.contains("/generated/")
}

fn ext(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or("")
}

/// A patch's added (`+`) or removed (`-`) lines, without headers.
fn lines_of(patch: &str, sign: char) -> Vec<&str> {
    let header = if sign == '+' { "+++" } else { "---" };
    patch
        .lines()
        .filter(|l| l.starts_with(sign) && !l.starts_with(header))
        .map(|l| &l[1..])
        .collect()
}

/// A patch's unchanged context lines.
fn context_of(patch: &str) -> Vec<&str> {
    patch
        .lines()
        .filter(|l| l.starts_with(' '))
        .map(|l| &l[1..])
        .collect()
}

fn non_blank(lines: &[&str]) -> usize {
    lines.iter().filter(|l| !l.trim().is_empty()).count()
}

// ── Definitions ────────────────────────────────────────────────────────────

/// What a top-level definition is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefKind {
    /// A map, list, set or object literal: a lookup table or config.
    Table,
    /// A function or method at the top level.
    Function,
}

/// A top-level definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Def {
    pub name: String,
    pub kind: DefKind,
}

fn ident(s: &str) -> Option<String> {
    let n: String = s
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
        .collect();
    (n.len() >= 3 && !n.chars().next().is_some_and(|c| c.is_ascii_digit())).then_some(n)
}

fn table_value(v: &str) -> bool {
    let v = v.trim_start();
    [
        "{",
        "[",
        "(",
        "dict(",
        "frozenset(",
        "set(",
        "mappingproxytype(",
        "new map",
        "new set",
        "object.freeze(",
        "map[",
        "phf_map!",
        "hashmap",
        "btreemap",
        "&[",
    ]
    .iter()
    .any(|p| v.to_lowercase().starts_with(p))
}

/// The definition a line makes at the top level of a file, by language.
/// Indented lines (inside a class or function) define nothing here.
pub fn top_level_def(line: &str, path: &str) -> Option<Def> {
    if line.starts_with([' ', '\t']) || line.trim().is_empty() {
        return None;
    }
    let l = line.trim_end();
    match ext(path) {
        "py" => {
            if let Some(r) = l
                .strip_prefix("def ")
                .or_else(|| l.strip_prefix("async def "))
            {
                return ident(r).map(|name| Def {
                    name,
                    kind: DefKind::Function,
                });
            }
            let (lhs, rhs) = l.split_once('=')?;
            if rhs.starts_with('=') || lhs.contains('(') {
                return None;
            }
            let name = ident(lhs.split(':').next()?.trim())?;
            table_value(rhs).then_some(Def {
                name,
                kind: DefKind::Table,
            })
        }
        "ts" | "tsx" | "js" | "jsx" | "mjs" | "cjs" => {
            let r = l.strip_prefix("export ").unwrap_or(l);
            let r = r.strip_prefix("default ").unwrap_or(r);
            let r = r.strip_prefix("async ").unwrap_or(r);
            if let Some(f) = r.strip_prefix("function ") {
                return ident(f.trim_start_matches('*').trim_start()).map(|name| Def {
                    name,
                    kind: DefKind::Function,
                });
            }
            let decl = ["const ", "let ", "var "]
                .iter()
                .find_map(|k| r.strip_prefix(k))?;
            let name = ident(decl)?;
            let rhs = decl.split_once('=')?.1.trim_start();
            let rhs_l = rhs.to_lowercase();
            if rhs_l.starts_with("(") && (rhs_l.contains("=>") || rhs_l.starts_with("(async"))
                || rhs_l.starts_with("async")
                || rhs_l.starts_with("function")
            {
                return Some(Def {
                    name,
                    kind: DefKind::Function,
                });
            }
            table_value(rhs).then_some(Def {
                name,
                kind: DefKind::Table,
            })
        }
        "rs" => {
            let r = l
                .strip_prefix("pub(crate) ")
                .or_else(|| l.strip_prefix("pub(super) "))
                .or_else(|| l.strip_prefix("pub "))
                .unwrap_or(l);
            let r = r.strip_prefix("async ").unwrap_or(r);
            if let Some(f) = r.strip_prefix("fn ") {
                return ident(f).map(|name| Def {
                    name,
                    kind: DefKind::Function,
                });
            }
            let decl = ["static ", "const "]
                .iter()
                .find_map(|k| r.strip_prefix(k))?;
            let name = ident(decl)?;
            let (ty, rhs) = decl.split_once('=').unwrap_or((decl, ""));
            let ty = ty.to_lowercase();
            (ty.contains("map") || ty.contains('[') || ty.contains("set") || table_value(rhs))
                .then_some(Def {
                    name,
                    kind: DefKind::Table,
                })
        }
        "go" => {
            if let Some(f) = l.strip_prefix("func ") {
                return ident(f).map(|name| Def {
                    name,
                    kind: DefKind::Function,
                });
            }
            let decl = l.strip_prefix("var ")?;
            let name = ident(decl)?;
            let rhs = decl.split_once('=')?.1;
            table_value(rhs).then_some(Def {
                name,
                kind: DefKind::Table,
            })
        }
        "rb" => {
            let (lhs, rhs) = l.split_once('=')?;
            let name = ident(lhs.trim())?;
            (name
                .chars()
                .all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
                && table_value(rhs))
            .then_some(Def {
                name,
                kind: DefKind::Table,
            })
        }
        _ => None,
    }
}

/// Every top-level definition in `text`.
pub fn defs(text: &str, path: &str) -> Vec<Def> {
    text.lines()
        .filter_map(|l| top_level_def(l, path))
        .collect()
}

/// Whether a new definition parallels an existing one: the same kind, and
/// names that share at least two words and half of the smaller name's.
pub fn parallels(new: &Def, old: &Def) -> bool {
    if new.kind != old.kind || new.name == old.name {
        return false;
    }
    let a = name_words(&new.name);
    let b = name_words(&old.name);
    let shared = a.iter().filter(|w| b.contains(w)).count();
    let smaller = a.len().min(b.len()).max(1);
    shared >= 2 && shared * 2 >= smaller && name_similarity(&new.name, &old.name) >= 0.34
}

/// The new definitions of an edit that parallel one already in the file,
/// as (new, existing). An existing one the edit removes was renamed, not
/// paralleled.
pub fn parallel_in_edit(path: &str, old: &str, new: &str) -> Vec<(Def, Def)> {
    let before = defs(old, path);
    let after = defs(new, path);
    let mut out = Vec::new();
    for d in after
        .iter()
        .filter(|d| !before.iter().any(|b| b.name == d.name))
    {
        if let Some(e) = before
            .iter()
            .filter(|b| after.iter().any(|a| a.name == b.name))
            .find(|b| parallels(d, b))
        {
            out.push((d.clone(), e.clone()));
        }
    }
    out
}

/// The parallel structures a diff adds: new top-level definitions next to
/// a similar existing one in the same file's base (`base`, by path) or its
/// patch's context, or in another changed file's base.
///
/// Both sides are checked against the base: a definition the base already
/// has is never new (a changed signature shows as removed and added), and
/// when the file's base was read, "existing" means on the base. A review
/// once called a function that had been on the main branch for months the
/// new one, beside the builder the branch had really added.
pub fn parallel_structures(diff: &Diff, base: &[(String, String)]) -> Vec<Widening> {
    let mut out = Vec::new();
    for (path, status, patch) in diff {
        if status == "removed" || !code_file(path) || is_test(path) {
            continue;
        }
        let removed: Vec<Def> = lines_of(patch, '-')
            .into_iter()
            .filter_map(|l| top_level_def(l, path))
            .collect();
        let added: Vec<Def> = lines_of(patch, '+')
            .into_iter()
            .filter_map(|l| top_level_def(l, path))
            .collect();
        let base_read = base.iter().any(|(p, _)| p == path);
        let on_base: Vec<Def> = base
            .iter()
            .filter(|(p, _)| p == path)
            .flat_map(|(_, t)| defs(t, path))
            .collect();
        let mut existing: Vec<Def> = on_base.clone();
        existing.extend(
            context_of(patch)
                .into_iter()
                .filter_map(|l| top_level_def(l, path))
                .filter(|d| !base_read || on_base.iter().any(|b| b.name == d.name)),
        );
        // Removed and not added back: gone. Removed and added back: its
        // signature changed, and it is still the existing one.
        existing.retain(|e| {
            !removed.iter().any(|r| r.name == e.name) || added.iter().any(|a| a.name == e.name)
        });
        for d in &added {
            let was_there = on_base.iter().any(|b| b.name == d.name)
                || removed.iter().any(|r| r.name == d.name);
            if was_there || existing.iter().any(|e| e.name == d.name) {
                continue;
            }
            if let Some(e) = existing.iter().find(|e| parallels(d, e)) {
                let what = format!("`{}` next to the existing `{}`", d.name, e.name);
                if !out.iter().any(|w: &Widening| w.what == what) {
                    out.push(Widening {
                        kind: Kind::Parallel,
                        path: path.clone(),
                        what,
                    });
                }
            }
        }
    }
    out
}

// ── Replacing semantics ────────────────────────────────────────────────────

/// Statements that delete or overwrite stored rows, with how a report
/// names each.
const REPLACING: &[(&str, &str)] = &[
    ("do update", "`ON CONFLICT DO UPDATE`"),
    ("on_conflict_do_update", "`on_conflict_do_update`"),
    ("delete from", "`DELETE FROM`"),
    ("truncate table", "`TRUNCATE TABLE`"),
    ("insert or replace", "`INSERT OR REPLACE`"),
    ("replace into", "`REPLACE INTO`"),
    ("when matched then update", "`MERGE ... UPDATE`"),
    ("when matched then delete", "`MERGE ... DELETE`"),
    (
        "when not matched by source",
        "`MERGE ... NOT MATCHED BY SOURCE`",
    ),
    (".delete()", "an ORM delete"),
    ("delete_many(", "`delete_many`"),
    ("destroy_all", "`destroy_all`"),
    ("bulk_delete", "a bulk delete"),
    ("drop table", "`DROP TABLE`"),
];

fn count(lines: &[&str], token: &str) -> usize {
    lines
        .iter()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("--") || t.starts_with('#') || t.starts_with("//"))
        })
        .map(|l| l.to_lowercase().matches(token).count())
        .sum()
}

/// Deleting or overwriting statements the diff adds where its base had
/// none of that kind: an insert-only path made replacing.
pub fn replacing(diff: &Diff, base: &[(String, String)]) -> Vec<Widening> {
    let mut out = Vec::new();
    for (path, status, patch) in diff {
        if status == "removed" || !code_file(path) || is_test(path) {
            continue;
        }
        let added = lines_of(patch, '+');
        let removed = lines_of(patch, '-');
        let base_text = base
            .iter()
            .find(|(p, _)| p == path)
            .map(|(_, t)| t.as_str());
        let mut found = Vec::new();
        for (token, name) in REPLACING {
            let a = count(&added, token);
            if a == 0 || a <= count(&removed, token) {
                continue;
            }
            // The base file had none of it: the path was additive. In a new
            // file every one is new.
            let had = base_text
                .map(|t| t.to_lowercase().matches(token).count())
                .unwrap_or_else(|| count(&context_of(patch), token));
            if status == "added" || had == 0 {
                found.push(*name);
            }
        }
        if !found.is_empty() {
            out.push(Widening {
                kind: Kind::Replacing,
                path: path.clone(),
                what: found.join(", "),
            });
        }
    }
    out
}

// ── What a helper does to stored rows ──────────────────────────────────────

/// What a function does to rows already stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Writes {
    /// It adds rows and leaves existing ones as they are.
    InsertOnly,
    /// It deletes, overwrites or replaces existing rows; the statements
    /// that show it.
    Replacing(Vec<String>),
    /// It writes nothing the check can see.
    None,
}

const INSERTS: &[&str] = &[
    "insert into",
    "do nothing",
    "on_conflict_do_nothing",
    "bulk_create(",
    "insert_many(",
    "insert_one(",
    ".add(",
    ".create(",
    "get_or_create(",
];

/// The text of the top-level definition `name` in `text`: from its line to
/// the next top-level definition (or a line back at the left margin that
/// isn't a closing bracket or a decorator).
pub fn body_of(text: &str, path: &str, name: &str) -> Option<String> {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines
        .iter()
        .position(|l| top_level_def(l, path).is_some_and(|d| d.name == name))?;
    let mut out = vec![lines[start]];
    for l in &lines[start + 1..] {
        let t = l.trim_start();
        let at_margin = !l.is_empty() && !l.starts_with([' ', '\t']);
        if at_margin && !(t.starts_with('}') || t.starts_with(')') || t.starts_with(']')) {
            break;
        }
        out.push(l);
    }
    Some(out.join("\n"))
}

/// What a function body does to stored rows: deleting or overwriting
/// statements (or a delete followed by an insert, a `.set(` that replaces a
/// relation) make it replacing; an insert with none of them, insert only.
pub fn write_semantics(body: &str) -> Writes {
    let lines: Vec<&str> = body
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with('#') || t.starts_with("//") || t.starts_with("--"))
        })
        .collect();
    let low = lines.join("\n").to_lowercase();
    let mut how: Vec<String> = REPLACING
        .iter()
        .filter(|(t, _)| low.contains(t))
        .map(|(_, n)| n.to_string())
        .collect();
    let writes_db = [
        "execute(", "query(", ".save(", "insert", "session.", "cursor",
    ]
    .iter()
    .any(|w| low.contains(w));
    if low.contains(".set(") && writes_db {
        how.push("a `.set(` that replaces a relation".to_string());
    }
    let delete = low.find("delete ").or_else(|| low.find("delete("));
    let insert = low.rfind("insert");
    if let (Some(d), Some(i)) = (delete, insert) {
        if d < i && !how.iter().any(|h| h.contains("DELETE")) {
            how.push("a delete followed by an insert".to_string());
        }
    }
    if !how.is_empty() {
        return Writes::Replacing(how);
    }
    if INSERTS.iter().any(|t| low.contains(t)) {
        return Writes::InsertOnly;
    }
    Writes::None
}

/// Whether the code paths in `texts` only insert: at least one insert and
/// no deleting or overwriting statement.
pub fn inserts_only(texts: &[&str]) -> bool {
    let all = texts.join("\n");
    matches!(write_semantics(&all), Writes::InsertOnly)
}

// ── Logging and reporting ──────────────────────────────────────────────────

const LEVELS: &[&str] = &[
    "warning", "critical", "debug", "info", "warn", "error", "fatal", "trace",
];

fn level_free(line: &str) -> String {
    let mut s = line.trim().to_lowercase();
    for l in LEVELS {
        s = s.replace(l, "<lvl>");
    }
    s
}

fn has_level(line: &str) -> bool {
    let l = line.to_lowercase();
    LEVELS.iter().any(|w| l.contains(w))
        && (l.contains("log") || l.contains("!(") || l.contains("setlevel") || l.contains("level"))
}

/// Log lines and logger settings the diff keeps but moves to another level.
pub fn log_levels(diff: &Diff) -> Vec<Widening> {
    let mut out = Vec::new();
    for (path, status, patch) in diff {
        if status == "removed" || is_test(path) {
            continue;
        }
        let removed: Vec<&str> = lines_of(patch, '-')
            .into_iter()
            .filter(|l| has_level(l))
            .collect();
        for a in lines_of(patch, '+').into_iter().filter(|l| has_level(l)) {
            if let Some(r) = removed.iter().find(|r| {
                level_free(r) == level_free(a) && r.trim().to_lowercase() != a.trim().to_lowercase()
            }) {
                out.push(Widening {
                    kind: Kind::LogLevel,
                    path: path.clone(),
                    what: format!(
                        "`{}` became `{}`",
                        common::truncate_str(r.trim(), 80),
                        common::truncate_str(a.trim(), 80)
                    ),
                });
                break;
            }
        }
    }
    out
}

fn is_counter(line: &str) -> bool {
    let l = line.to_lowercase().replace(' ', "");
    l.contains("+=1")
        || l.contains("++;")
        || l.contains(".inc(")
        || l.contains("counter(")
        || l.contains("fetch_add(")
}

fn is_log_call(line: &str) -> bool {
    let l = line.trim_start().to_lowercase();
    [
        "logger.",
        "logging.",
        "log.",
        "self.log.",
        "warn!(",
        "info!(",
        "debug!(",
        "console.warn(",
        "console.info(",
        "console.debug(",
        "warnings.warn(",
    ]
    .iter()
    .any(|p| l.starts_with(p) || l.contains(&format!(" {p}")) || l.contains(&format!("({p}")))
}

/// New counters and log calls in non-test code: (counters, log calls).
pub fn reporting_added(diff: &Diff) -> Vec<(String, usize, usize)> {
    diff.iter()
        .filter(|(p, s, _)| s != "removed" && code_file(p) && !is_test(p))
        .map(|(p, _, patch)| {
            let added = lines_of(patch, '+');
            let removed = lines_of(patch, '-');
            let counters = added.iter().filter(|l| is_counter(l)).count();
            let logs = added.iter().filter(|l| is_log_call(l)).count();
            let was_counters = removed.iter().filter(|l| is_counter(l)).count();
            let was_logs = removed.iter().filter(|l| is_log_call(l)).count();
            (
                p.clone(),
                counters.saturating_sub(was_counters),
                logs.saturating_sub(was_logs),
            )
        })
        .filter(|(_, c, l)| *c > 0 || *l > 0)
        .collect()
}

/// Reporting the diff adds that the ticket doesn't ask for: two or more new
/// counters, a counter and a log call, or three or more log calls in a file.
pub fn reporting(diff: &Diff) -> Vec<Widening> {
    reporting_added(diff)
        .into_iter()
        .filter(|(_, c, l)| *c >= 2 || (*c >= 1 && *l >= 1) || *l >= 3)
        .map(|(path, c, l)| Widening {
            kind: Kind::Reporting,
            path,
            what: format!("{c} new counter(s), {l} new log call(s)"),
        })
        .collect()
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Added test lines against added code lines: a test of more than 200
/// lines and twice the code, or more than 120 lines and four times it.
pub fn test_size(diff: &Diff) -> Vec<Widening> {
    let mut tests = 0;
    let mut code = 0;
    let mut biggest = (String::new(), 0usize);
    for (path, status, patch) in diff {
        if status == "removed" {
            continue;
        }
        let n = non_blank(&lines_of(patch, '+'));
        if is_test(path) {
            tests += n;
            if n > biggest.1 {
                biggest = (path.clone(), n);
            }
        } else if code_file(path) {
            code += n;
        }
    }
    let out_of_proportion = (tests > 200 && tests > 2 * code) || (tests > 120 && tests > 4 * code);
    if !out_of_proportion {
        return Vec::new();
    }
    vec![Widening {
        kind: Kind::TestSize,
        path: biggest.0,
        what: format!("{tests} added lines against {code} lines of changed code"),
    }]
}

const FAKES: &[&str] = &[
    "class fake",
    "class mock",
    "class stub",
    "magicmock(",
    "mock.patch(",
    "@patch(",
    "monkeypatch.setattr(",
    "responses.add(",
    "nock(",
    "route.fulfill(",
    "jest.mock(",
    "vi.mock(",
    "mockresolvedvalue(",
    "mockreturnvalue(",
    "httpretty.register_uri(",
    "requests_mock",
];

fn payload_key(line: &str) -> bool {
    let t = line.trim_start();
    let Some(q) = t.chars().next().filter(|c| matches!(c, '"' | '\'')) else {
        return false;
    };
    let rest = &t[1..];
    let Some(end) = rest.find(q) else {
        return false;
    };
    let key = &rest[..end];
    !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        && rest[end + 1..].trim_start().starts_with(':')
}

/// Tests that fake an outside call with a large payload literal (twelve or
/// more keys), or add a JSON fixture of that size, while no probe record
/// shows what the outside system returns.
pub fn invented_payloads(diff: &Diff, has_probe: bool) -> Vec<Widening> {
    if has_probe {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (path, status, patch) in diff {
        if status == "removed" || !is_test(path) {
            continue;
        }
        let added = lines_of(patch, '+');
        let keys = added.iter().filter(|l| payload_key(l)).count();
        let low = added.join("\n").to_lowercase();
        let fakes = FAKES.iter().filter(|f| low.contains(*f)).count();
        let fixture = path.ends_with(".json") && status == "added";
        if keys >= 12 && (fakes > 0 || fixture) {
            out.push(Widening {
                kind: Kind::InventedPayload,
                path: path.clone(),
                what: format!(
                    "{keys} payload keys{}",
                    if fakes > 0 {
                        format!(" behind {fakes} kind(s) of fake")
                    } else {
                        " in a new fixture".to_string()
                    }
                ),
            });
        }
    }
    out
}

// ── The check ──────────────────────────────────────────────────────────────

/// Everything a diff adds past its ticket. `base` holds the base text of
/// changed files where read; `ticket` is the ticket's own words (title,
/// body, people's answers); `has_probe` says a probe record is in the tree.
pub fn check(
    diff: &Diff,
    base: &[(String, String)],
    ticket: &str,
    has_probe: bool,
) -> Vec<Widening> {
    let mut out = parallel_structures(diff, base);
    if !ticket_asks(ticket, REPLACE_ASKS) {
        out.extend(replacing(diff, base));
    }
    if !ticket_asks(ticket, LOG_ASKS) {
        out.extend(log_levels(diff));
    }
    if !ticket_asks(ticket, REPORT_ASKS) {
        out.extend(reporting(diff));
    }
    out.extend(test_size(diff));
    out.extend(invented_payloads(diff, has_probe));
    out
}

/// What still stands after the coder's reasons: an excusable item with a
/// reason for its path is kept as said, the rest go back.
pub fn standing<'a>(found: &'a [Widening], notes: &str) -> Vec<&'a Widening> {
    let why = reasons(notes);
    found
        .iter()
        .filter(|w| !(w.excusable() && why.iter().any(|(p, _)| p.trim() == w.path.trim())))
        .collect()
}

// ── Test assertions changed to match another company's page ────────────────

/// An assertion a test change flipped to what an outside page does, while
/// an acceptance criterion of the ticket says otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VendorFlip {
    pub path: String,
    pub removed: String,
    pub added: String,
    pub criterion: String,
}

impl VendorFlip {
    /// The review finding's text.
    pub fn body(&self) -> String {
        format!(
            "`{}` changes `{}` to `{}`, matching what the outside page does, while the ticket's acceptance criterion says \"{}\". Change the product side first (the layout around the frame, the page's own code, the vendor's documented embed options) so the criterion holds, and keep the assertion. Change the test only when the product can't meet the criterion, with the measured reason in the plan and the pull request.",
            self.path,
            common::truncate_str(self.removed.trim(), 120),
            common::truncate_str(self.added.trim(), 120),
            self.criterion
        )
    }
}

/// The subject of an `expect(...)` and the rest of the assertion.
fn assertion(line: &str) -> Option<(String, String)> {
    let at = line.find("expect(")?;
    let rest = &line[at + 7..];
    let mut depth = 1;
    for (i, c) in rest.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((
                        rest[..i].trim().to_string(),
                        rest[i + 1..].trim().to_string(),
                    ));
                }
            }
            _ => {}
        }
    }
    None
}

/// Assertions a patch changed: a removed and an added `expect` on the same
/// subject with another matcher or value.
pub fn assertion_flips(patch: &str) -> Vec<(String, String)> {
    let removed: Vec<&str> = lines_of(patch, '-');
    let mut out = Vec::new();
    for a in lines_of(patch, '+') {
        let Some((sa, ra)) = assertion(a) else {
            continue;
        };
        if let Some(r) = removed
            .iter()
            .find(|r| assertion(r).is_some_and(|(sr, rr)| sr == sa && rr != ra))
        {
            out.push((r.to_string(), a.to_string()));
        }
    }
    out
}

const CODE_WORDS: &[&str] = &[
    "expect",
    "await",
    "const",
    "locator",
    "frame",
    "frames",
    "tobe",
    "true",
    "false",
    "toequal",
    "page",
    "getbytestid",
    "evaluate",
    "document",
    "window",
    "toBeVisible",
    "tohave",
];

/// The words of `text` worth matching between a change and a criterion:
/// five-letter stems, code words left out.
pub(crate) fn stems(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .flat_map(name_words)
        .filter(|w| w.len() >= 5 && !CODE_WORDS.iter().any(|c| c.eq_ignore_ascii_case(w)))
        .map(|w| w.chars().take(5).collect())
        .collect()
}

/// The vendor flips in a changed test file: `outside` says the file drives
/// another company's page; each flipped assertion is matched to the
/// acceptance criterion it shares words with.
pub fn vendor_flips(
    path: &str,
    patch: &str,
    outside: bool,
    criteria: &[String],
) -> Vec<VendorFlip> {
    if !outside || criteria.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (removed, added) in assertion_flips(patch) {
        let words = stems(&format!("{removed} {added}"));
        let best = criteria
            .iter()
            .map(|c| {
                let cw = stems(c);
                (words.iter().filter(|w| cw.contains(w)).count(), c)
            })
            .filter(|(n, _)| *n > 0)
            .max_by_key(|(n, _)| *n);
        if let Some((_, c)) = best {
            out.push(VendorFlip {
                path: path.to_string(),
                removed,
                added,
                criterion: c.clone(),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(path: &str, status: &str, patch: &str) -> (String, String, String) {
        (path.into(), status.into(), patch.into())
    }

    #[test]
    fn definitions_by_language() {
        let py =
            "CAMP_MULTI_SPORT_OVERRIDES = {\ndef get_camp_multi_sport_names(name):\n    X = {}";
        let got = defs(py, "dags/camps.py");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].kind, DefKind::Table);
        assert_eq!(got[1].kind, DefKind::Function);
        let ts = "export const SPORT_MAP: Record<string, string> = {\nexport function sportFor(x) {\nconst handler = async (e) => {";
        let got = defs(ts, "src/a.ts");
        assert_eq!(
            got.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(),
            vec!["SPORT_MAP", "sportFor", "handler"]
        );
        let rs = "pub const RULES: &[&str] = &[\npub(crate) fn rules_for() {}";
        assert_eq!(defs(rs, "a.rs").len(), 2);
    }

    #[test]
    fn a_second_map_next_to_the_first_is_a_parallel_structure() {
        let base = vec![(
            "dags/camps.py".to_string(),
            "CAMP_MULTI_SPORT_OVERRIDES = {\n    \"a\": [\"Soccer\"],\n}\n\ndef get_camp_multi_sport_names(name):\n    return CAMP_MULTI_SPORT_OVERRIDES.get(name, [])\n".to_string(),
        )];
        let patch = "@@ -1,3 +1,9 @@\n CAMP_MULTI_SPORT_OVERRIDES = {\n+CAMP_SWIMMING_OVERRIDES = {\n+    \"b\": [\"Swimming\"],\n+}\n+def get_camp_swimming_names(name):\n+    return CAMP_SWIMMING_OVERRIDES.get(name, [])\n";
        let w = parallel_structures(&[d("dags/camps.py", "modified", patch)], &base);
        assert_eq!(w.len(), 2, "{w:?}");
        assert!(w[0].what.contains("CAMP_SWIMMING_OVERRIDES"));
        assert!(w[0].what.contains("CAMP_MULTI_SPORT_OVERRIDES"));
        assert!(w[1].what.contains("get_camp_multi_sport_names"));
        // Four new entries in the existing map are no new structure.
        let small = "@@ -1,3 +1,7 @@\n CAMP_MULTI_SPORT_OVERRIDES = {\n+    \"c\": [\"Swimming\", \"Tennis\"],\n";
        assert!(parallel_structures(&[d("dags/camps.py", "modified", small)], &base).is_empty());
        // A rename is not a parallel.
        let rename = "-CAMP_MULTI_SPORT_OVERRIDES = {\n+CAMP_SPORT_OVERRIDES = {\n";
        assert!(parallel_structures(&[d("dags/camps.py", "modified", rename)], &base).is_empty());
    }

    #[test]
    fn insert_only_made_replacing_blocks_unless_asked() {
        let patch = "-    ON CONFLICT (camp_id, sport_id) DO NOTHING\n+    ON CONFLICT (camp_id, sport_id) DO UPDATE SET sport_id = EXCLUDED.sport_id\n+DELETE FROM camp_sports WHERE camp_id = %s AND sport_id <> ALL(%s)\n";
        let base = vec![(
            "dags/sync.py".to_string(),
            "INSERT INTO camp_sports VALUES (%s, %s) ON CONFLICT (camp_id, sport_id) DO NOTHING"
                .to_string(),
        )];
        let diff = [d("dags/sync.py", "modified", patch)];
        let w = check(
            &diff,
            &base,
            "Link these four camps to Swimming as well",
            false,
        );
        let r: Vec<&Widening> = w.iter().filter(|w| w.kind == Kind::Replacing).collect();
        assert_eq!(r.len(), 1);
        assert!(r[0].what.contains("DO UPDATE") && r[0].what.contains("DELETE FROM"));
        assert!(!r[0].excusable());
        assert!(standing(&w, "Wider than the ticket: dags/sync.py: cleaner").len() == w.len());
        // A ticket that asks to remove stale links may.
        assert!(
            check(&diff, &base, "Remove links that no longer apply", false)
                .iter()
                .all(|w| w.kind != Kind::Replacing)
        );
        // An additive insert is fine.
        let add = "+    INSERT INTO camp_sports VALUES (%s, %s) ON CONFLICT DO NOTHING\n";
        assert!(replacing(&[d("dags/sync.py", "modified", add)], &base).is_empty());
    }

    #[test]
    fn log_levels_and_new_reporting() {
        let patch = "-logger.setLevel(logging.INFO)\n+logger.setLevel(logging.DEBUG)\n+    skipped += 1\n+    linked += 1\n+    log.warning(\"skipped %s\", ids)\n";
        let diff = [d("dags/common/log.py", "modified", patch)];
        let w = check(&diff, &[], "Link four camps to Swimming", false);
        assert!(w.iter().any(|w| w.kind == Kind::LogLevel), "{w:?}");
        assert!(w.iter().any(|w| w.kind == Kind::Reporting), "{w:?}");
        // A reason keeps an excusable item.
        let kept = standing(
            &w,
            "Wider than the ticket: dags/common/log.py: the ticket's comment asks for the count",
        );
        assert!(kept.is_empty());
        // A ticket that asks for logging gets it.
        assert!(check(&diff, &[], "Log how many camps were skipped", false).is_empty());
        // One new log line in a change is ordinary.
        let one = [d("a.py", "modified", "+    log.info(\"done\")\n")];
        assert!(reporting(&one).is_empty());
    }

    #[test]
    fn tests_in_proportion_and_payloads_observed() {
        let code: String = (0..20).map(|i| format!("+    x{i} = 1\n")).collect();
        let test: String = (0..430).map(|i| format!("+    assert y{i}\n")).collect();
        let diff = [
            d("dags/sync.py", "modified", &code),
            d("tests/test_sync.py", "added", &test),
        ];
        let w = test_size(&diff);
        assert_eq!(w.len(), 1);
        assert!(w[0].what.contains("430 added lines against 20"));
        let small: String = (0..30).map(|i| format!("+    assert y{i}\n")).collect();
        assert!(test_size(&[
            d("dags/sync.py", "modified", &code),
            d("tests/test_sync.py", "added", &small)
        ])
        .is_empty());

        let payload: String = std::iter::once("+class FakeVendorHook:\n".to_string())
            .chain((0..14).map(|i| format!("+    \"field_{i}\": \"v\",\n")))
            .collect();
        let diff = [d("tests/test_sync.py", "added", &payload)];
        assert_eq!(invented_payloads(&diff, false).len(), 1);
        assert!(invented_payloads(&diff, true).is_empty());
    }

    #[test]
    fn a_test_flipped_to_the_vendor_while_a_criterion_says_otherwise() {
        let patch = "-  expect(await frameScrolls(page)).toBe(false);\n+  expect(await frameScrolls(page)).toBe(true);\n";
        let criteria = vec![
            "The portal loads inside the page".to_string(),
            "No double scrollbars on desktop".to_string(),
        ];
        let f = vendor_flips("e2e/portal.spec.ts", patch, true, &criteria);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].criterion, "No double scrollbars on desktop");
        assert!(f[0].body().contains("Change the product side first"));
        // Our own page, or no criterion, is a test's own business.
        assert!(vendor_flips("e2e/portal.spec.ts", patch, false, &criteria).is_empty());
        assert!(vendor_flips("e2e/portal.spec.ts", patch, true, &[]).is_empty());
    }

    #[test]
    fn what_a_helper_does_to_existing_rows() {
        let text = "def upsert_multi_sport_links(cur, group_id, sport_ids):\n    cur.execute(\"DELETE FROM group_sport WHERE group_id = %s AND NOT (sport_id = ANY(%s))\", (group_id, sport_ids))\n    for s in sport_ids:\n        cur.execute(\"INSERT INTO group_sport VALUES (%s, %s) ON CONFLICT DO NOTHING\", (group_id, s))\n\n\ndef upsert_group_sport_link(cur, group_id, sport_id):\n    cur.execute(\"INSERT INTO group_sport VALUES (%s, %s) ON CONFLICT DO NOTHING\", (group_id, sport_id))\n\n\ndef sport_names(name):\n    return OVERRIDES.get(name, [])\n";
        let replacing = body_of(text, "a.py", "upsert_multi_sport_links").unwrap();
        assert!(!replacing.contains("def upsert_group_sport_link"));
        assert_eq!(
            write_semantics(&replacing),
            Writes::Replacing(vec!["`DELETE FROM`".to_string()])
        );
        let insert = body_of(text, "a.py", "upsert_group_sport_link").unwrap();
        assert_eq!(write_semantics(&insert), Writes::InsertOnly);
        let read = body_of(text, "a.py", "sport_names").unwrap();
        assert_eq!(write_semantics(&read), Writes::None);
        assert!(inserts_only(&[&insert]));
        assert!(!inserts_only(&[&insert, &replacing]));
        // An upsert that overwrites, and a relation replaced with `.set(`.
        assert!(matches!(
            write_semantics("INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET v = 1"),
            Writes::Replacing(_)
        ));
        assert!(matches!(
            write_semantics("group.sports.set(ids)\ngroup.save()"),
            Writes::Replacing(_)
        ));
        assert_eq!(write_semantics("cache.set(k, v)"), Writes::None);
        // A TypeScript helper body ends at its closing brace.
        let ts = "export function linkAll(db, ids) {\n  return db.query('DELETE FROM l WHERE id = $1', ids);\n}\nexport const other = 1;\n";
        assert!(matches!(
            write_semantics(&body_of(ts, "a.ts", "linkAll").unwrap()),
            Writes::Replacing(_)
        ));
    }

    #[test]
    fn an_edit_that_adds_a_parallel_helper() {
        let old = "SPORT_OVERRIDES = {\n}\ndef sport_overrides_for(n):\n    pass\n";
        let new = format!("{old}SWIM_SPORT_OVERRIDES = {{\n}}\n");
        let p = parallel_in_edit("a.py", old, &new);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].1.name, "SPORT_OVERRIDES");
        assert!(parallel_in_edit("a.py", old, old).is_empty());
    }
}
