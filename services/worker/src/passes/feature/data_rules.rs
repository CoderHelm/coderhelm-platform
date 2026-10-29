//! Four rules for a change that reads or writes stored data, for every
//! ticket kind. Each is a way a data-pipeline change shipped broken:
//!
//! 1. **Cursor and watermark writes** ([`cursor_writes`], [`readers`]). A
//!    change stopped advancing `updated_at` on some update and upsert paths
//!    to keep another column stable. Two incremental readers depended on it
//!    (one took `MAX(updated_at)` as its sync watermark, one used
//!    `GREATEST(parent.updated_at, MAX(child.updated_at))` as a change key),
//!    so edited rows refetched every run and other changes stopped reaching
//!    the search index. A change to how a timestamp, sequence or version
//!    column is written names every reader of it in the plan, keeps the
//!    column advancing (`GREATEST(existing, new)`) and adds a test that it
//!    still advances.
//! 2. **Incremental and full paths agree** ([`selections`],
//!    [`other_selections`]). A change picked a winner among sibling rows by
//!    start date and broke ties differently from the full sync, which ordered
//!    by start, then `updated_at`, then id. Code that picks a winner, latest
//!    or first among rows uses the very ordering key (tie-breaks and nulls
//!    included) of every other path that makes the same choice; the plan
//!    names that path and a test covers ties and nulls in both.
//! 3. **Unexpected data is reported once per run** ([`skips`]). Missing or
//!    invalid records were skipped silently. Records that don't have the
//!    expected shape are collected and reported in ONE warning per run that
//!    names their ids (deduplicated, capped), with counts in the run's
//!    summary; never a log line per row, never silent. Expected exclusions
//!    are counted, not warned.
//! 4. **Verified on real data** ([`before_after`]). With a read-only data
//!    source connected, the evidence and the description's Verification
//!    section show the ticket's records and the edge cases the change touches
//!    before and after, with the result each ends with (the proof check's
//!    `real_data` point and the description writer).
//!
//! The planner reads [`PLANNER`] and the coder [`CODER_RULES`]; the plan's
//! own check adds [`plan_gaps`]; the gate before the pull request adds
//! [`items`] to the verification checklist.

use super::checklist::{Item, Status};

/// The plan section that lists the readers of a cursor column.
pub const READERS_HEADING: &str = "Readers of the cursor column";
/// The plan section that names the other path making the same selection.
pub const PARITY_HEADING: &str = "Same selection elsewhere";

/// Item names, as the description's checklist shows them.
pub const CURSOR_ITEM: &str = "cursor column keeps advancing";
pub const PARITY_ITEM: &str = "same ordering as the other path";
pub const SKIP_ITEM: &str = "unexpected data reported once per run";

/// What the planner is told, for every ticket kind.
pub const PLANNER: &str = "## Data rules (CoderHelm checks these)\n\
- Cursor columns: when the change alters how a timestamp, sequence or version column is written \
(`updated_at`, `modified_at`, `version`, a sequence: sets it conditionally, stops advancing it, \
or moves it back), search the repository for every reader of that column (`MAX(col)`, \
`GREATEST(... col ...)`, `WHERE col >`, `ORDER BY col`, and code that compares it) and list each \
one by path in design.md under `### Readers of the cursor column`. Keep the column advancing \
(`GREATEST(existing, new)`) and plan a test that proves it still advances.\n\
- Same selection everywhere: when the change picks a winner, the latest or the first among rows, \
find any other path that makes the same choice (full and incremental, batch and stream, backfill \
and live) and name it with its ordering key under `### Same selection elsewhere` (or what you \
searched when none exists). Both paths use the identical ordering key, tie-breaks and null \
handling included; plan a test with ties and nulls in both modes.\n\
- Unexpected data: records without the expected shape are collected and reported in ONE warning \
per run naming their ids (deduplicated, capped), with counts in the run's summary; never a log \
line per row and never a silent skip. Expected exclusions are counted, not warned. Where the \
repository or the team's instructions already set a format, follow it.\n\
- Real data: when the change affects stored data and a read-only data source is connected, plan \
to read the records the ticket names and the edge cases the change touches (ties, nulls, missing \
records) before and after, for the pull request's Verification section.\n";

/// What the coder is told, for every change.
pub const CODER_RULES: &str = "
## Changes to stored data (every change that reads or writes it)
- Cursor columns: when you change how a timestamp, sequence or version column is written (set it conditionally, stop advancing it, move it back), first search for every reader of that column: SQL `MAX(col)`, `GREATEST(... col ...)`, `WHERE col >`, `ORDER BY col`, and code that compares it. List them in your notes. Keep the column advancing (`GREATEST(existing, new)`), even when another column must stay stable, and add a test proving it still advances.
- Same selection everywhere: when code picks a winner, latest or first among rows and another path makes the same choice (full and incremental sync, batch and stream, backfill and live), use the identical ordering key in both, tie-breaks and null handling included (a shared function where you can). Add a test with ties and nulls that both modes pass.
- Unexpected data: collect records that don't have the expected shape and report them in ONE warning per run naming their ids (deduplicated, capped), with counts in the run's summary. Never a log line per row, never a silent `continue`. Expected exclusions (filtered-out categories) are counted, not warned. Follow the repository's or team's format when one is set.
";

/// A code or SQL file whose lines count.
fn code_file(path: &str) -> bool {
    let l = path.to_lowercase();
    [
        ".sql", ".ts", ".tsx", ".js", ".mjs", ".py", ".rb", ".go", ".rs", ".kt", ".java", ".cs",
        ".php", ".scala",
    ]
    .iter()
    .any(|e| l.ends_with(e))
        && !l.contains(".d.ts")
        && !l.contains("/generated/")
}

/// The added and removed lines of a patch, without headers.
fn changed_lines(patch: &str, sign: char) -> Vec<String> {
    let header = if sign == '+' { "+++" } else { "---" };
    patch
        .lines()
        .filter(|l| l.starts_with(sign) && !l.starts_with(header))
        .map(|l| l[1..].trim().to_string())
        .filter(|l| !is_comment(l) && !l.is_empty())
        .collect()
}

fn is_comment(l: &str) -> bool {
    let t = l.trim_start();
    t.starts_with("//") || t.starts_with("--") || t.starts_with('#') || t.starts_with('*')
}

/// `updatedAt` as `updated_at`.
fn snake(word: &str) -> String {
    let mut out = String::new();
    let mut prev_lower = false;
    for c in word.chars() {
        if c.is_ascii_uppercase() {
            if prev_lower {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
        prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
    }
    out
}

/// A column that readers use as a cursor, watermark or change key.
pub fn is_cursor_name(word: &str) -> bool {
    let w = snake(word);
    const EXACT: &[&str] = &[
        "updated_at",
        "modified_at",
        "changed_at",
        "last_modified",
        "last_modified_at",
        "last_updated",
        "last_updated_at",
        "synced_at",
        "last_synced_at",
        "version",
        "row_version",
        "lock_version",
        "seq",
        "sequence",
        "sequence_number",
        "revision",
        "watermark",
    ];
    EXACT.contains(&w.as_str())
        || [
            "_updated_at",
            "_modified_at",
            "_version",
            "_seq",
            "_watermark",
        ]
        .iter()
        .any(|s| w.ends_with(s))
}

fn ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// The identifiers on a line with where each ends.
fn tokens(line: &str) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in line.char_indices() {
        match (ident_char(c), start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                out.push((line[s..i].to_string(), i));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s) = start {
        out.push((line[s..].to_string(), line.len()));
    }
    out
}

/// Whether `word` appears on `line` as a whole identifier (any case).
fn has_token(line: &str, word: &str) -> bool {
    let w = snake(word);
    tokens(line).iter().any(|(t, _)| snake(t) == w)
}

/// The cursor columns a line assigns: `col = ...`, `col: ...`,
/// `"col" => ...`, never a comparison in a `WHERE`.
fn assigned_cursors(line: &str) -> Vec<String> {
    let low = line.to_ascii_lowercase();
    let lead = low.trim_start();
    if lead.starts_with("where ")
        || lead.starts_with("and ")
        || lead.starts_with("or ")
        || lead.starts_with("assert")
    {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (t, end) in tokens(line) {
        if !is_cursor_name(&t) {
            continue;
        }
        if low[..end.saturating_sub(t.len())].contains(" where ") {
            continue;
        }
        let rest: String = line[end..]
            .chars()
            .skip_while(|c| matches!(c, ' ' | '"' | '\'' | '`' | ']'))
            .collect();
        let mut r = rest.chars();
        let (a, b) = (r.next(), r.next());
        // `col: value` is a write only when the value is an expression
        // (a call, a field, an index, a literal), never a type annotation.
        let value = rest.get(1..).unwrap_or("");
        let expression = !value.contains('<')
            && (value.contains('(')
                || value.contains('.')
                || value.contains('[')
                || value.contains('"')
                || value.contains('\''));
        let assigns = match (a, b) {
            (Some('='), Some('=' | '>')) => false,
            (Some('='), _) => true,
            (Some(':'), Some(':')) => false,
            (Some(':'), _) => expression,
            _ => false,
        };
        let name = snake(&t);
        if assigns && !out.contains(&name) {
            out.push(name);
        }
    }
    out
}

/// A write that chooses its value: it may keep the old one or go back.
fn conditional(line: &str) -> bool {
    let l = line.to_ascii_lowercase();
    [
        "case when",
        "coalesce(",
        "nullif(",
        "least(",
        " if ",
        " ? ",
        "iif(",
    ]
    .iter()
    .any(|w| l.contains(w))
        || l.trim_start().starts_with("if ")
        || l.trim_end().ends_with(" case")
}

/// A write that can only move the column forward.
fn advancing(line: &str) -> bool {
    let l = line.to_lowercase().replace(' ', "");
    if l.contains("greatest(") || l.contains("max(") {
        return true;
    }
    let fresh = [
        "now()",
        "current_timestamp",
        "clock_timestamp",
        "utcnow",
        "utc::now",
        "systemtime::now",
        "date.now",
        "newdate(",
        "timezone.now",
        "datetime.now",
        "time.now",
        "sysdate",
        "+1",
        "nextval(",
    ]
    .iter()
    .any(|w| l.contains(w));
    fresh && !conditional(line)
}

/// One change to how a cursor column is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorWrite {
    pub path: String,
    pub column: String,
    /// The write lines the change removes and adds.
    pub removed: Vec<String>,
    pub added: Vec<String>,
}

impl CursorWrite {
    /// Every new write can only move the column forward, and the change
    /// doesn't drop a write without replacing it.
    pub fn monotonic(&self) -> bool {
        !self.added.is_empty() && self.added.iter().all(|l| advancing(l))
    }
}

fn norm(l: &str) -> String {
    l.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches([',', ';'])
        .to_lowercase()
}

/// The diff's changes to how a cursor column is written: a write it
/// removes or changes, or a new one that chooses its value. A write moved
/// unchanged, or a new plain `now()` write, is not one.
pub fn cursor_writes(files: &[(String, String, String)]) -> Vec<CursorWrite> {
    let mut out: Vec<CursorWrite> = Vec::new();
    for (path, status, patch) in files {
        if status == "removed" || !code_file(path) || super::checklist::is_test_path(path) {
            continue;
        }
        let removed = changed_lines(patch, '-');
        let added = changed_lines(patch, '+');
        let mut cols: Vec<String> = Vec::new();
        for l in removed.iter().chain(&added) {
            for c in assigned_cursors(l) {
                if !cols.contains(&c) {
                    cols.push(c);
                }
            }
        }
        for col in cols {
            let of = |lines: &[String]| -> Vec<String> {
                lines
                    .iter()
                    .filter(|l| assigned_cursors(l).contains(&col))
                    .cloned()
                    .collect()
            };
            let (r, a) = (of(&removed), of(&added));
            let (mut rn, mut an): (Vec<String>, Vec<String>) = (
                r.iter().map(|l| norm(l)).collect(),
                a.iter().map(|l| norm(l)).collect(),
            );
            rn.sort();
            an.sort();
            let changed = rn != an && (!r.is_empty() || a.iter().any(|l| conditional(l)));
            if changed {
                out.push(CursorWrite {
                    path: path.clone(),
                    column: col,
                    removed: r,
                    added: a,
                });
            }
        }
    }
    out
}

/// One place that reads a cursor column as a cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reader {
    pub path: String,
    pub line: String,
}

/// Whether a line reads `col` as a cursor: `MAX(col)`, `GREATEST(..col..)`,
/// `ORDER BY col`, or a `>`/`<` comparison with it.
pub fn reads_as_cursor(line: &str, col: &str) -> bool {
    if !has_token(line, col) {
        return false;
    }
    let low = line.to_ascii_lowercase();
    let after = |w: &str| low.find(w).is_some_and(|i| has_token(&line[i..], col));
    if after("max(") || after("greatest(") || after("order by") || after("max_by") {
        return true;
    }
    // A comparison next to the column: `col >`, `> t.col`, `col.gt(`.
    let c = snake(col);
    tokens(line).into_iter().any(|(t, end)| {
        if snake(&t) != c {
            return false;
        }
        let before = compact_before(&low[..end - t.len()]);
        let next: String = low[end..].chars().filter(|ch| *ch != ' ').take(7).collect();
        let cmp_after = (next.starts_with('>') || next.starts_with('<'))
            && !next.starts_with("=>")
            && !next.starts_with("->");
        let cmp_before =
            (before.ends_with('>') && !before.ends_with("=>") && !before.ends_with("->"))
                || (before.ends_with('<') && !before.ends_with("<-"));
        let method = [".gt(", ".gte(", ".lt(", ".after(", ".before("]
            .iter()
            .any(|m| next.starts_with(m));
        cmp_after || cmp_before || method
    })
}

/// The text before an identifier without spaces or its table prefix
/// (`> t.` reads as `>`).
fn compact_before(s: &str) -> String {
    let mut t: String = s.chars().filter(|c| *c != ' ').collect();
    if t.ends_with('.') {
        t.pop();
        while t.ends_with(ident_char) {
            t.pop();
        }
    }
    t
}

/// The readers of `col` among code search hits (`(path, ["N: line"])`),
/// leaving out tests and docs. At most 12.
pub fn readers(col: &str, hits: &[(String, Vec<String>)]) -> Vec<Reader> {
    let mut out: Vec<Reader> = Vec::new();
    for (path, lines) in hits {
        if !code_file(path) || super::checklist::is_test_path(path) {
            continue;
        }
        for l in lines {
            let text = l.split_once(": ").map_or(l.as_str(), |(_, t)| t).trim();
            if is_comment(text) || !reads_as_cursor(text, col) {
                continue;
            }
            let r = Reader {
                path: path.clone(),
                line: common::truncate_str(text, 140).to_string(),
            };
            if !out.contains(&r) {
                out.push(r);
            }
        }
    }
    out.truncate(12);
    out
}

/// One place the code picks a winner, the latest or the first among rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    pub path: String,
    /// The lines that make the choice, joined.
    pub text: String,
    /// The ordering key, normalized (see [`order_key`]), when it reads.
    pub key: Option<String>,
}

const STRONG: &[&str] = &[
    "distinct on",
    "row_number()",
    "rank()",
    "dense_rank()",
    "first_value(",
    "last_value(",
    "max_by",
    "min_by",
    "maxby(",
    "minby(",
];
const PICKS: &[&str] = &[
    "limit 1",
    "[0]",
    ".first()",
    ".last()",
    "[-1]",
    "fetch first",
    "head(1)",
    "take(1)",
    ".next()",
];

fn selection_at(lines: &[String], i: usize) -> Option<String> {
    let l = lines[i].to_lowercase();
    let from = i.saturating_sub(3);
    let to = (i + 4).min(lines.len());
    let window = lines[from..to].join(" ").to_lowercase();
    let strong = STRONG.iter().any(|s| l.contains(s))
        || ((l.contains("max(") || l.contains("min(")) && l.contains("key="));
    let sorts = l.contains("order by")
        || l.contains("sort_by")
        || l.contains(".sort(")
        || l.contains("sorted(")
        || l.contains("orderby(");
    (strong || (sorts && PICKS.iter().any(|p| window.contains(p))))
        .then(|| lines[from..to].join(" "))
}

/// The ordering a selection reads: what follows `ORDER BY` (to `LIMIT`, a
/// closing parenthesis or the end), or a sort or `max_by` call's argument.
/// Lowercase, spaces collapsed, table prefixes dropped (`t.start` is
/// `start`).
pub fn order_key(text: &str) -> Option<String> {
    let low = text.to_lowercase();
    let raw = if let Some(i) = low.find("order by") {
        let rest = &low[i + 8..];
        let mut depth = 0i32;
        let mut end = rest.len();
        for (j, c) in rest.char_indices() {
            match c {
                '(' => depth += 1,
                ')' if depth == 0 => {
                    end = j;
                    break;
                }
                ')' => depth -= 1,
                ';' => {
                    end = j;
                    break;
                }
                _ => {}
            }
            if rest[j..].starts_with(" limit") || rest[j..].starts_with(" fetch") {
                end = j;
                break;
            }
        }
        rest[..end].to_string()
    } else {
        let at = ["key=", "sort_by", "max_by", "min_by", "sorted(", ".sort("]
            .iter()
            .find_map(|m| low.find(m).map(|i| i + m.len()))?;
        let rest = &low[at..];
        let open = usize::from(rest.starts_with('('));
        let mut depth = 0i32;
        let mut end = rest.len();
        for (j, c) in rest[open..].char_indices() {
            match c {
                '(' => depth += 1,
                ')' if depth == 0 => {
                    end = open + j;
                    break;
                }
                ')' => depth -= 1,
                _ => {}
            }
        }
        rest[open.min(end)..end].to_string()
    };
    let parts: Vec<String> = raw
        .split(',')
        .map(|p| {
            p.split_whitespace()
                .map(|w| w.rsplit_once('.').map_or(w, |(_, c)| c).to_string())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|p| !p.is_empty())
        .collect();
    (!parts.is_empty()).then(|| parts.join(", "))
}

/// The first column of an ordering key.
fn lead(key: &str) -> &str {
    key.split([',', ' ']).next().unwrap_or("").trim()
}

/// The selections the diff adds or changes.
pub fn selections(files: &[(String, String, String)]) -> Vec<Selection> {
    let mut out: Vec<Selection> = Vec::new();
    for (path, status, patch) in files {
        if status == "removed" || !code_file(path) || super::checklist::is_test_path(path) {
            continue;
        }
        let lines: Vec<String> = patch
            .lines()
            .filter(|l| !l.starts_with("@@") && !l.starts_with("+++") && !l.starts_with("---"))
            .filter(|l| !l.starts_with('-'))
            .map(|l| l.get(1..).unwrap_or("").to_string())
            .collect();
        let added: Vec<bool> = patch
            .lines()
            .filter(|l| !l.starts_with("@@") && !l.starts_with("+++") && !l.starts_with("---"))
            .filter(|l| !l.starts_with('-'))
            .map(|l| l.starts_with('+'))
            .collect();
        for (i, line) in lines.iter().enumerate() {
            if !added[i] || is_comment(line) {
                continue;
            }
            if let Some(text) = selection_at(&lines, i) {
                let s = Selection {
                    path: path.clone(),
                    key: order_key(&text),
                    text: common::truncate_str(&norm(&text), 200).to_string(),
                };
                if !out.iter().any(|o| o.path == s.path && o.key == s.key) {
                    out.push(s);
                }
            }
        }
    }
    out
}

/// Selections in nearby files (`(path, whole text)`: the changed file and
/// its siblings) other than the ones the diff adds: the other paths that may
/// make the same choice. At most 8.
pub fn other_selections(new: &[Selection], texts: &[(String, String)]) -> Vec<Selection> {
    let mut out: Vec<Selection> = Vec::new();
    for (path, text) in texts {
        if !code_file(path) || super::checklist::is_test_path(path) {
            continue;
        }
        let lines: Vec<String> = text.lines().map(str::to_string).collect();
        for (i, line) in lines.iter().enumerate() {
            if is_comment(line) {
                continue;
            }
            let Some(t) = selection_at(&lines, i) else {
                continue;
            };
            let s = Selection {
                path: path.clone(),
                key: order_key(&t),
                text: common::truncate_str(&norm(&t), 200).to_string(),
            };
            let same_as_new = new.iter().any(|n| n.path == s.path && n.key == s.key);
            if !same_as_new && !out.iter().any(|o| o.path == s.path && o.key == s.key) {
                out.push(s);
            }
        }
    }
    out.truncate(8);
    out
}

/// The other selections a new one must agree with, but doesn't: the same
/// leading column with a different full key (`(new, other)`).
pub fn key_mismatches<'a>(
    new: &'a [Selection],
    others: &'a [Selection],
) -> Vec<(&'a Selection, &'a Selection)> {
    let mut out = Vec::new();
    for n in new {
        let Some(nk) = &n.key else { continue };
        for o in others {
            let Some(ok) = &o.key else { continue };
            if lead(nk) == lead(ok) && nk != ok {
                out.push((n, o));
            }
        }
    }
    out
}

/// One skip over records the diff adds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skip {
    pub path: String,
    pub line: String,
    /// The skip adds to a count or a list near it.
    pub counted: bool,
    /// The record (an id) goes into a list for the run's report.
    pub collected: bool,
    /// A log line at INFO or louder sits next to it: one per row.
    pub per_row_log: bool,
}

fn is_skip(code: &str) -> bool {
    let l = code.trim().to_lowercase();
    l == "continue"
        || l.starts_with("continue;")
        || l.contains("{ continue")
        || l.contains("=> continue")
        || l.contains("else continue")
        || (l.starts_with("if ") && l.ends_with(": continue"))
        || l.starts_with("next if ")
        || l.starts_with("next unless ")
        || l.ends_with(" continue;")
}

const LOUD_LOGS: &[&str] = &[
    "warn!(",
    "info!(",
    "error!(",
    "log.warn",
    "log.info",
    "log.error",
    "logger.warn",
    "logger.info",
    "logger.error",
    "logging.warn",
    "logging.info",
    "logging.error",
    "console.warn",
    "console.log",
    "console.error",
    "println!(",
    "eprintln!(",
    "print(",
    "slog.warn",
    "zap.l().warn",
];

const WARNS: &[&str] = &[
    "warn!(",
    "log.warn",
    "logger.warn",
    "logging.warn",
    "console.warn",
    "warning(",
    ".warn(",
];

fn loud(l: &str) -> bool {
    let l = l.to_lowercase();
    LOUD_LOGS.iter().any(|w| l.contains(w))
}

fn warns(l: &str) -> bool {
    let l = l.to_lowercase();
    WARNS.iter().any(|w| l.contains(w))
}

/// Data code: a path that names a pipeline or a patch with SQL in it.
fn data_code(path: &str, patch: &str) -> bool {
    let p = path.to_lowercase();
    let pl = patch.to_lowercase();
    [
        "sync",
        "etl",
        "pipeline",
        "ingest",
        "import",
        "export",
        "loader",
        "extract",
        "transform",
        "backfill",
        "dags/",
        "jobs/",
        "warehouse",
        "feed",
        "consumer",
        "webhook",
        "records",
    ]
    .iter()
    .any(|w| p.contains(w))
        || [
            "select ",
            "insert into",
            "on conflict",
            "upsert",
            "update ",
            "merge into",
        ]
        .iter()
        .any(|w| pl.contains(w))
}

/// The skips over records the diff adds in data code: a `continue` (or
/// `next`) inside a loop, each with whether it is counted, collected, or
/// logged on every row.
pub fn skips(files: &[(String, String, String)]) -> Vec<Skip> {
    let mut out = Vec::new();
    for (path, status, patch) in files {
        if status == "removed"
            || !code_file(path)
            || super::checklist::is_test_path(path)
            || !data_code(path, patch)
        {
            continue;
        }
        let lines: Vec<(bool, String)> = patch
            .lines()
            .filter(|l| !l.starts_with("@@") && !l.starts_with("+++") && !l.starts_with("---"))
            .filter(|l| !l.starts_with('-'))
            .map(|l| (l.starts_with('+'), l.get(1..).unwrap_or("").to_string()))
            .collect();
        for (i, (added, code)) in lines.iter().enumerate() {
            if !*added || is_comment(code) || !is_skip(code) {
                continue;
            }
            let before = lines[i.saturating_sub(12)..i]
                .iter()
                .map(|(_, l)| l.to_lowercase())
                .collect::<Vec<_>>()
                .join("\n");
            let in_loop = ["for ", "for_each", "foreach", "while ", "loop {", ".each "]
                .iter()
                .any(|w| before.contains(w));
            if !in_loop {
                continue;
            }
            let window: Vec<String> = lines[i.saturating_sub(4)..(i + 2).min(lines.len())]
                .iter()
                .map(|(_, l)| l.to_lowercase())
                .collect();
            let collected = window.iter().any(|l| {
                [
                    ".push(",
                    ".insert(",
                    ".append(",
                    ".add(",
                    ".extend(",
                    ".push_back(",
                ]
                .iter()
                .any(|w| l.contains(w))
            });
            let counted = collected
                || window.iter().any(|l| {
                    l.replace(' ', "").contains("+=1")
                        || l.contains("+= 1")
                        || l.contains("++")
                        || l.contains(".inc(")
                        || l.contains("increment")
                        || l.contains("fetch_add(")
                });
            let per_row_log = window.iter().any(|l| loud(l));
            out.push(Skip {
                path: path.clone(),
                line: common::truncate_str(code.trim(), 120).to_string(),
                counted,
                collected,
                per_row_log,
            });
        }
    }
    out
}

/// Whether the diff reports collected records once: a warning outside every
/// skip's neighbourhood, in a non-test file's added lines.
pub fn reports_once(files: &[(String, String, String)]) -> bool {
    files.iter().any(|(path, status, patch)| {
        if status == "removed" || super::checklist::is_test_path(path) {
            return false;
        }
        let lines: Vec<(bool, String)> = patch
            .lines()
            .filter(|l| !l.starts_with("@@") && !l.starts_with("+++") && !l.starts_with("---"))
            .filter(|l| !l.starts_with('-'))
            .map(|l| (l.starts_with('+'), l.get(1..).unwrap_or("").to_string()))
            .collect();
        let near_skip = |i: usize| {
            lines[i.saturating_sub(2)..(i + 5).min(lines.len())]
                .iter()
                .any(|(_, l)| is_skip(l))
        };
        lines
            .iter()
            .enumerate()
            .any(|(i, (added, l))| *added && warns(l) && !near_skip(i))
    })
}

/// The test text the diff adds: added lines of test files and of any file
/// that adds a test case (a Rust module's own tests).
pub fn tests_added(files: &[(String, String, String)]) -> String {
    let mut out = String::new();
    for (path, status, patch) in files {
        if status == "removed" {
            continue;
        }
        let added: Vec<&str> = patch
            .lines()
            .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
            .map(|l| l[1..].trim())
            .collect();
        let has_case = super::checklist::is_test_path(path)
            || added.iter().any(|l| {
                l.starts_with("#[test]")
                    || l.starts_with("#[tokio::test]")
                    || l.starts_with("def test_")
                    || l.starts_with("func Test")
                    || l.starts_with("it(")
                    || l.starts_with("test(")
            });
        if has_case {
            for l in added {
                out.push_str(l);
                out.push('\n');
            }
        }
    }
    out
}

/// A test proves `col` still advances: it names the column and compares it.
pub fn advance_tested(tests: &str, col: &str) -> bool {
    let low = tests.to_lowercase();
    tests.lines().any(|l| has_token(l, col))
        && [
            "advanc", "monoton", "greater", "increas", "newer", "later", " > ", ">=", ".gt(",
            "is_after", "after(",
        ]
        .iter()
        .any(|w| low.contains(w))
}

/// A test covers ties and nulls.
pub fn ties_and_nulls_tested(tests: &str) -> bool {
    let low = tests.to_lowercase();
    low.contains("tie")
        && ["null", "none", "nil", "missing"]
            .iter()
            .any(|w| low.contains(w))
}

/// Whether `text` (the plan and the coder's notes) names `path`, by its
/// path or its file name.
fn names_path(text: &str, path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    text.contains(path) || (!name.is_empty() && text.contains(name))
}

/// The section of `text` under a heading containing `heading`, to the next
/// heading. `None` when there is no such heading.
pub fn section<'a>(text: &'a str, heading: &str) -> Option<&'a str> {
    let low = text.to_ascii_lowercase();
    let h = heading.to_ascii_lowercase();
    let at = low.match_indices(&h).map(|(i, _)| i).find(|i| {
        low[..*i]
            .rsplit('\n')
            .next()
            .is_some_and(|p| p.trim_start().starts_with('#'))
    })?;
    let body_start = text[at..].find('\n').map_or(text.len(), |n| at + n + 1);
    let rest = &text[body_start..];
    let end = rest
        .lines()
        .scan(0usize, |pos, l| {
            let here = *pos;
            *pos += l.len() + 1;
            Some((here, l))
        })
        .find(|(_, l)| l.trim_start().starts_with('#'))
        .map_or(rest.len(), |(p, _)| p);
    Some(&rest[..end])
}

/// What the data part of the checklist reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Facts {
    pub writes: Vec<CursorWrite>,
    /// Readers of each written column, by column.
    pub readers: Vec<(String, Vec<Reader>)>,
    pub selections: Vec<Selection>,
    pub others: Vec<Selection>,
    pub skips: Vec<Skip>,
    pub reports_once: bool,
    /// The test text the diff adds ([`tests_added`]).
    pub tests: String,
}

impl Facts {
    /// From the diff alone; readers and other selections are looked up by
    /// the caller.
    pub fn from_diff(files: &[(String, String, String)]) -> Self {
        Facts {
            writes: cursor_writes(files),
            selections: selections(files),
            skips: skips(files),
            reports_once: reports_once(files),
            tests: tests_added(files),
            ..Default::default()
        }
    }

    /// Nothing here applies to the change.
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty() && self.selections.is_empty() && self.skips.is_empty()
    }
}

fn shown(items: &[String]) -> String {
    let mut s: Vec<String> = items.iter().take(6).cloned().collect();
    if items.len() > 6 {
        s.push(format!("{} more", items.len() - 6));
    }
    s.join("; ")
}

/// The checklist items, read against `text` (the plan and the coder's
/// notes, where readers and the other path are named).
pub fn items(f: &Facts, text: &str) -> Vec<Item> {
    let mut out = Vec::new();
    let mut cols: Vec<&str> = Vec::new();
    for w in &f.writes {
        if !cols.contains(&w.column.as_str()) {
            cols.push(&w.column);
        }
    }
    for col in cols {
        let writes: Vec<&CursorWrite> = f.writes.iter().filter(|w| w.column == col).collect();
        let readers: Vec<&Reader> = f
            .readers
            .iter()
            .filter(|(c, _)| c == col)
            .flat_map(|(_, r)| r)
            .collect();
        let mut problems = Vec::new();
        let back: Vec<String> = writes
            .iter()
            .filter(|w| !w.monotonic())
            .map(|w| {
                if w.added.is_empty() {
                    format!("`{}` no longer writes `{col}`", w.path)
                } else {
                    format!(
                        "`{}` writes `{}`, which can keep or move it back",
                        w.path,
                        common::truncate_str(&w.added[0], 100)
                    )
                }
            })
            .collect();
        if !back.is_empty() {
            problems.push(format!(
                "not kept advancing ({}); write `GREATEST(existing, new)`",
                shown(&back)
            ));
        }
        let unnamed: Vec<String> = readers
            .iter()
            .filter(|r| !names_path(text, &r.path))
            .map(|r| format!("`{}`: `{}`", r.path, r.line))
            .collect();
        if !unnamed.is_empty() {
            problems.push(format!(
                "readers not listed under `{READERS_HEADING}`: {}",
                shown(&unnamed)
            ));
        } else if readers.is_empty() && section(text, READERS_HEADING).is_none() {
            problems.push(format!(
                "no `{READERS_HEADING}` section saying where readers were searched"
            ));
        }
        if !advance_tested(&f.tests, col) {
            problems.push(format!("no test proves `{col}` still advances"));
        }
        out.push(if problems.is_empty() {
            Item::new(
                CURSOR_ITEM,
                Status::Pass,
                format!(
                    "`{col}` keeps advancing; {} reader(s) listed; a test proves it",
                    readers.len()
                ),
            )
        } else {
            Item::new(
                CURSOR_ITEM,
                Status::Fail,
                format!("`{col}`: {}", problems.join("; ")),
            )
        });
    }
    if !f.selections.is_empty() {
        let mut problems = Vec::new();
        let mismatched: Vec<String> = key_mismatches(&f.selections, &f.others)
            .into_iter()
            .map(|(n, o)| {
                format!(
                    "`{}` orders by `{}` but `{}` orders by `{}`",
                    n.path,
                    n.key.as_deref().unwrap_or(""),
                    o.path,
                    o.key.as_deref().unwrap_or("")
                )
            })
            .collect();
        if !mismatched.is_empty() {
            problems.push(format!("different ordering keys: {}", shown(&mismatched)));
        }
        let unnamed: Vec<String> = f
            .others
            .iter()
            .filter(|o| !names_path(text, &o.path))
            .map(|o| format!("`{}`", o.path))
            .collect();
        if section(text, PARITY_HEADING).is_none() {
            problems.push(format!(
                "no `{PARITY_HEADING}` section naming the other path and its ordering"
            ));
        } else if !unnamed.is_empty() {
            problems.push(format!(
                "other selections nearby not named: {}",
                shown(&unnamed)
            ));
        }
        if !ties_and_nulls_tested(&f.tests) {
            problems.push("no test with ties and nulls".to_string());
        }
        out.push(if problems.is_empty() {
            Item::new(
                PARITY_ITEM,
                Status::Pass,
                format!(
                    "{} selection(s) use the same ordering as {} other path(s); ties and nulls tested",
                    f.selections.len(),
                    f.others.len()
                ),
            )
        } else {
            Item::new(PARITY_ITEM, Status::Fail, problems.join("; "))
        });
    }
    if !f.skips.is_empty() {
        let mut problems = Vec::new();
        let silent: Vec<String> = f
            .skips
            .iter()
            .filter(|s| !s.counted)
            .map(|s| format!("`{}`: `{}`", s.path, s.line))
            .collect();
        if !silent.is_empty() {
            problems.push(format!("skipped silently: {}", shown(&silent)));
        }
        let per_row: Vec<String> = f
            .skips
            .iter()
            .filter(|s| s.per_row_log)
            .map(|s| format!("`{}`: `{}`", s.path, s.line))
            .collect();
        if !per_row.is_empty() {
            problems.push(format!("a log line per row: {}", shown(&per_row)));
        }
        if f.skips.iter().any(|s| s.collected) && !f.reports_once {
            problems
                .push("records are collected but no single WARNING per run names them".to_string());
        }
        out.push(if problems.is_empty() {
            Item::new(
                SKIP_ITEM,
                Status::Pass,
                format!(
                    "{} skip(s) counted, unexpected records reported once",
                    f.skips.len()
                ),
            )
        } else {
            Item::new(SKIP_ITEM, Status::Fail, problems.join("; "))
        });
    }
    out
}

/// Whether a checklist item is one of the data rules'.
pub fn is_data_item(i: &Item) -> bool {
    [CURSOR_ITEM, PARITY_ITEM, SKIP_ITEM].contains(&i.name.as_str())
}

/// What the coder is told about failing data items.
pub fn feedback(items: &[Item]) -> String {
    let fails: Vec<&Item> = items
        .iter()
        .filter(|i| i.status == Status::Fail && is_data_item(i))
        .collect();
    if fails.is_empty() {
        return String::new();
    }
    let mut s = String::from("\nThe data rules aren't met yet. Fix each item:\n");
    for f in fails {
        s.push_str(&format!("- {}: {}\n", f.name, f.detail));
    }
    s.push_str(&format!(
        "A cursor column (a timestamp, sequence or version readers use as a watermark or change \
         key) keeps advancing: write `GREATEST(existing, new)`, list every reader by path in \
         design.md or your notes under `### {READERS_HEADING}`, and add a test that proves it \
         still advances. A selection of a winner, latest or first row uses the same ordering key \
         as every other path making that choice, tie-breaks and nulls included; name that path \
         under `### {PARITY_HEADING}` and add a test with ties and nulls. A record without the \
         expected shape is collected (its id) and counted; the run logs ONE warning naming the \
         ids (deduplicated, capped) and puts the counts in its summary; no log line per row. An \
         expected exclusion is counted, not warned.\n"
    ));
    s
}

/// The plan's own check: a plan that changes how a cursor column is written
/// without its readers section, or picks a winner among rows without naming
/// the other path. `(point, detail, settle)`.
pub fn plan_gaps(text: &str) -> Vec<(&'static str, String, String)> {
    let mut out = Vec::new();
    const ALTERS: &[&str] = &[
        "freeze",
        "frozen",
        "stop advancing",
        "stops advancing",
        "stop updating",
        "stop bumping",
        "no longer",
        "only when",
        "only if",
        "conditional",
        "unchanged",
        "preserv",
        "stable",
        "not update",
        "n't update",
        "not advance",
        "n't advance",
        "not bump",
        "n't bump",
        "case when",
        "coalesce",
        "backwards",
        "rewind",
        "instead of",
    ];
    let cursor_lines: Vec<String> = text
        .lines()
        .filter(|l| {
            let low = l.to_lowercase();
            // A column, not a library's version: a timestamp name, or a line
            // that is about writing a column.
            let column = |t: &str| {
                is_cursor_name(t)
                    && (snake(t).ends_with("_at")
                        || ["column", "on conflict", "upsert", " set ", "update "]
                            .iter()
                            .any(|w| low.contains(w)))
            };
            tokens(l).iter().any(|(t, _)| column(t)) && ALTERS.iter().any(|w| low.contains(w))
        })
        .map(|l| l.trim().to_string())
        .collect();
    if !cursor_lines.is_empty() && section(text, READERS_HEADING).is_none() {
        out.push((
            "cursor_readers",
            format!(
                "The plan changes how a cursor column is written (\"{}\") without listing its readers.",
                common::truncate_str(&cursor_lines[0], 140)
            ),
            format!(
                "Search for every reader of the column (MAX, GREATEST, WHERE col >, ORDER BY, code \
                 comparing it), list each by path under `### {READERS_HEADING}`, keep it advancing \
                 with GREATEST(existing, new), and plan a test that it still advances."
            ),
        ));
    }
    const PICK: &[&str] = &[
        "winner",
        "latest",
        "most recent",
        "newest",
        "earliest",
        "pick the",
        "picks the",
        "choose the",
        "chooses the",
        "first row",
        "dedup",
        "distinct on",
        "row_number",
        "tie-break",
        "tiebreak",
    ];
    const ROWS: &[&str] = &[
        "row",
        "record",
        "sibling",
        "among",
        "per ",
        "group",
        "duplicate",
        "child",
        "item",
    ];
    let pick_line = text.lines().find(|l| {
        let low = l.to_lowercase();
        PICK.iter().any(|w| low.contains(w)) && ROWS.iter().any(|w| low.contains(w))
    });
    if let Some(l) = pick_line {
        if section(text, PARITY_HEADING).is_none() {
            out.push((
                "selection_parity",
                format!(
                    "The plan picks one row among several (\"{}\") without naming the other path that makes the same choice.",
                    common::truncate_str(l.trim(), 140)
                ),
                format!(
                    "Find the other path (full and incremental, batch and stream, backfill and \
                     live), name it with its ordering key under `### {PARITY_HEADING}`, use the \
                     identical key with tie-breaks and nulls, and plan a test with ties and nulls \
                     in both."
                ),
            ));
        }
    }
    out
}

/// What the coder's notes say about real data before and after the change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeforeAfter {
    /// A `Before and after` list with this many records.
    Examples(usize),
    /// The notes say no data source could reach the data.
    Unreachable,
    /// The notes have no such list.
    Missing,
}

/// Reads the `Before and after` list in the coder's notes.
pub fn before_after(notes: &str) -> BeforeAfter {
    let lines: Vec<&str> = notes.lines().collect();
    let Some(at) = lines.iter().position(|l| {
        let low = l.to_lowercase();
        low.contains("before and after") || low.contains("before/after")
    }) else {
        return BeforeAfter::Missing;
    };
    let head = lines[at].to_lowercase();
    if [
        "no data source",
        "not reachable",
        "unreachable",
        "no source",
        "no connected",
    ]
    .iter()
    .any(|w| head.contains(w))
    {
        return BeforeAfter::Unreachable;
    }
    let n = lines[at + 1..]
        .iter()
        .take_while(|l| {
            let t = l.trim_start();
            !t.starts_with('#') && !t.to_lowercase().starts_with("evidence")
        })
        .filter(|l| {
            let t = l.trim_start();
            t.starts_with("- ") || t.starts_with("* ") || t.starts_with('|')
        })
        .filter(|l| !l.contains("---"))
        .count();
    if n == 0 {
        BeforeAfter::Missing
    } else {
        BeforeAfter::Examples(n)
    }
}

/// Whether a diff changes stored data: SQL or a migration, a write or
/// selection the rules above read, or a skip over records.
pub fn touches_data(files: &[(String, String, String)]) -> bool {
    let f = Facts::from_diff(files);
    !f.is_empty()
        || files.iter().any(|(p, s, patch)| {
            let p = p.to_lowercase();
            if s == "removed" || super::checklist::is_test_path(&p) {
                return false;
            }
            p.ends_with(".sql")
                || p.contains("/migrations/")
                || patch.lines().any(|l| {
                    (l.starts_with('+') || l.starts_with('-'))
                        && !l.starts_with("+++")
                        && !l.starts_with("---")
                        && {
                            let low = l.to_lowercase();
                            [
                                "insert into",
                                "on conflict",
                                "delete from",
                                "merge into",
                                "upsert(",
                                "bulk_create",
                                "bulk_write",
                            ]
                            .iter()
                            .any(|w| low.contains(w))
                        }
                })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, patch: &str) -> (String, String, String) {
        (path.into(), "modified".into(), patch.into())
    }

    /// The upsert that froze `updated_at` to keep `first_seen_at` stable.
    const FROZEN: &str = "@@ -10,6 +10,6 @@\n INSERT INTO item (id, group_id, first_seen_at, updated_at)\n VALUES ($1, $2, $3, $4)\n ON CONFLICT (id) DO UPDATE SET\n   group_id = EXCLUDED.group_id,\n-  updated_at = EXCLUDED.updated_at\n+  updated_at = CASE WHEN item.group_id = EXCLUDED.group_id THEN item.updated_at ELSE EXCLUDED.updated_at END\n";
    const ADVANCING: &str = "@@ -10,6 +10,6 @@\n ON CONFLICT (id) DO UPDATE SET\n   group_id = EXCLUDED.group_id,\n-  updated_at = EXCLUDED.updated_at\n+  updated_at = GREATEST(item.updated_at, EXCLUDED.updated_at)\n";

    #[test]
    fn a_frozen_cursor_write_is_found_and_isnt_monotonic() {
        let w = cursor_writes(&[file("pipeline/upsert_items.sql", FROZEN)]);
        assert_eq!(w.len(), 1, "{w:?}");
        assert_eq!(w[0].column, "updated_at");
        assert!(!w[0].monotonic());
        // Removing the write altogether freezes it too.
        let gone = "@@ -1,3 +1,2 @@\n   group_id = EXCLUDED.group_id,\n-  updated_at = EXCLUDED.updated_at\n";
        let w = cursor_writes(&[file("pipeline/upsert_items.sql", gone)]);
        assert_eq!(w.len(), 1);
        assert!(!w[0].monotonic());
        // GREATEST(existing, new) keeps it advancing.
        let w = cursor_writes(&[file("pipeline/upsert_items.sql", ADVANCING)]);
        assert!(w[0].monotonic(), "{w:?}");
    }

    #[test]
    fn a_plain_new_write_or_a_moved_one_is_not_a_cursor_change() {
        let fresh = "@@ -1,2 +1,3 @@\n UPDATE item SET\n+  updated_at = now(),\n   name = $1\n";
        assert!(cursor_writes(&[file("jobs/rename.sql", fresh)]).is_empty());
        let moved =
            "@@ -1,4 +1,4 @@\n-  updated_at = now(),\n   name = $1,\n+  updated_at = now(),\n";
        assert!(cursor_writes(&[file("jobs/rename.sql", moved)]).is_empty());
        // A comparison in a WHERE is a read, not a write.
        let read = "@@ -1,2 +1,2 @@\n-WHERE updated_at > $1\n+WHERE updated_at >= $1\n";
        assert!(cursor_writes(&[file("jobs/read.sql", read)]).is_empty());
        // Code assignments count, in camelCase too.
        let code =
            "@@ -1,2 +1,2 @@\n-    row.updatedAt = now;\n+    if (changed) row.updatedAt = now;\n";
        let w = cursor_writes(&[file("src/sync/items.ts", code)]);
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].column, "updated_at");
    }

    #[test]
    fn readers_are_the_watermark_and_change_key_queries() {
        let hits = vec![
            (
                "pipeline/incremental.sql".to_string(),
                vec![
                    "12: SELECT MAX(updated_at) FROM item".to_string(),
                    "30: SELECT name, updated_at FROM item".to_string(),
                ],
            ),
            (
                "search/index_changes.py".to_string(),
                vec![
                    "8: key = GREATEST(g.updated_at, MAX(i.updated_at))".to_string(),
                    "20: if row.updated_at > since:".to_string(),
                ],
            ),
            (
                "tests/test_items.py".to_string(),
                vec!["3: assert MAX(updated_at)".to_string()],
            ),
            (
                "docs/sync.md".to_string(),
                vec!["1: ORDER BY updated_at".to_string()],
            ),
        ];
        let r = readers("updated_at", &hits);
        let lines: Vec<&str> = r.iter().map(|x| x.line.as_str()).collect();
        assert_eq!(
            lines,
            vec![
                "SELECT MAX(updated_at) FROM item",
                "key = GREATEST(g.updated_at, MAX(i.updated_at))",
                "if row.updated_at > since:"
            ]
        );
        assert!(reads_as_cursor("WHERE t.updated_at > :since", "updated_at"));
        assert!(reads_as_cursor("ORDER BY updated_at DESC", "updated_at"));
        assert!(!reads_as_cursor(
            "SELECT updated_at FROM item",
            "updated_at"
        ));
        assert!(!reads_as_cursor("const updated_at => x", "updated_at"));
    }

    #[test]
    fn the_cursor_item_needs_readers_named_monotonic_writes_and_a_test() {
        let files = vec![file("pipeline/upsert_items.sql", FROZEN)];
        let mut f = Facts::from_diff(&files);
        f.readers = vec![(
            "updated_at".into(),
            vec![Reader {
                path: "pipeline/incremental.sql".into(),
                line: "SELECT MAX(updated_at) FROM item".into(),
            }],
        )];
        let it = items(&f, "## Design\nKeep first_seen_at stable.");
        assert_eq!(it[0].status, Status::Fail);
        assert!(it[0].detail.contains("GREATEST(existing, new)"));
        assert!(it[0].detail.contains("pipeline/incremental.sql"));
        assert!(it[0].detail.contains("still advances"));
        // The fix: GREATEST, the readers listed, a test that it advances.
        let fixed = vec![
            file("pipeline/upsert_items.sql", ADVANCING),
            (
                "tests/test_upsert.py".into(),
                "added".into(),
                "@@ -0,0 +1,3 @@\n+def test_updated_at_still_advances():\n+    upsert(row)\n+    assert after.updated_at > before.updated_at\n".into(),
            ),
        ];
        let mut f = Facts::from_diff(&fixed);
        f.readers = vec![(
            "updated_at".into(),
            vec![Reader {
                path: "pipeline/incremental.sql".into(),
                line: "SELECT MAX(updated_at) FROM item".into(),
            }],
        )];
        let text = format!(
            "### {READERS_HEADING}\n- pipeline/incremental.sql: MAX(updated_at) watermark\n"
        );
        let it = items(&f, &text);
        assert_eq!(it[0].status, Status::Pass, "{it:?}");
    }

    const INCREMENTAL: &str = "@@ -1,3 +1,6 @@\n def pick_current(children):\n+    for child in children:\n+        pass\n+    return sorted(children, key=lambda c: c.start_date)[0]\n";

    #[test]
    fn a_selection_is_compared_with_the_other_path_in_the_same_place() {
        let s = selections(&[file("sync/incremental.py", INCREMENTAL)]);
        assert_eq!(s.len(), 1, "{s:?}");
        assert_eq!(s[0].key.as_deref(), Some("lambda c: start_date"));
        let full = "SELECT DISTINCT ON (c.group_id) c.*\nFROM child c\nORDER BY c.group_id, c.start_date, c.updated_at, c.id\n";
        let others = other_selections(&s, &[("sync/full.sql".into(), full.into())]);
        assert_eq!(others.len(), 1);
        assert_eq!(
            others[0].key.as_deref(),
            Some("group_id, start_date, updated_at, id")
        );
        // Same lead in SQL on both sides, different tie-breaks: a mismatch.
        let new = vec![Selection {
            path: "sync/incremental.sql".into(),
            text: String::new(),
            key: order_key("ORDER BY c.group_id, c.start_date LIMIT 1"),
        }];
        let m = key_mismatches(&new, &others);
        assert_eq!(m.len(), 1);
        let f = Facts {
            selections: new.clone(),
            others: others.clone(),
            ..Default::default()
        };
        let it = items(&f, "");
        assert_eq!(it[0].name, PARITY_ITEM);
        assert_eq!(it[0].status, Status::Fail);
        assert!(it[0].detail.contains("different ordering keys"));
        assert!(it[0].detail.contains(PARITY_HEADING));
        assert!(it[0].detail.contains("ties and nulls"));
        // The same key, the other path named, ties and nulls tested.
        let same = vec![Selection {
            path: "sync/incremental.sql".into(),
            text: String::new(),
            key: order_key("ORDER BY c.group_id, c.start_date, c.updated_at, c.id LIMIT 1"),
        }];
        let f = Facts {
            selections: same,
            others,
            tests: "def test_tie_on_start_date_and_null_end():\n".into(),
            ..Default::default()
        };
        let text = format!(
            "### {PARITY_HEADING}\n`sync/full.sql` orders by group, start, updated_at, id.\n"
        );
        let it = items(&f, &text);
        assert_eq!(it[0].status, Status::Pass, "{it:?}");
    }

    const SILENT: &str = "@@ -1,4 +1,8 @@\n def sync_items(rows):\n     for row in rows:\n+        if row.get(\"parent_id\") is None:\n+            continue\n         upsert(row)\n";
    const PER_ROW: &str = "@@ -1,4 +1,9 @@\n def sync_items(rows):\n     for row in rows:\n+        if row.get(\"parent_id\") is None:\n+            logger.warning(\"missing parent %s\", row[\"id\"])\n+            continue\n         upsert(row)\n";
    const ONCE: &str = "@@ -1,4 +1,12 @@\n def sync_items(rows):\n+    missing = set()\n     for row in rows:\n+        if row.get(\"parent_id\") is None:\n+            missing.add(row[\"id\"])\n+            continue\n         upsert(row)\n+\n+\n+\n+    if missing:\n+        logger.warning(\"%d items without a parent: %s\", len(missing), sorted(missing)[:20])\n+    summary[\"missing_parent\"] = len(missing)\n";

    #[test]
    fn a_skip_is_counted_and_reported_once_never_silent_or_per_row() {
        let silent = vec![file("pipeline/sync_items.py", SILENT)];
        let f = Facts::from_diff(&silent);
        assert_eq!(f.skips.len(), 1);
        let it = items(&f, "");
        assert_eq!(it[0].status, Status::Fail);
        assert!(it[0].detail.contains("skipped silently"));
        let per_row = vec![file("pipeline/sync_items.py", PER_ROW)];
        let it = items(&Facts::from_diff(&per_row), "");
        assert!(it[0].detail.contains("a log line per row"), "{it:?}");
        let once = vec![file("pipeline/sync_items.py", ONCE)];
        let f = Facts::from_diff(&once);
        assert!(f.reports_once);
        let it = items(&f, "");
        assert_eq!(it[0].status, Status::Pass, "{it:?}");
        // An expected exclusion is counted, not warned.
        let excluded = "@@ -1,4 +1,8 @@\n def sync_items(rows):\n     for row in rows:\n+        if row[\"category\"] in EXCLUDED:\n+            excluded += 1\n+            continue\n         upsert(row)\n";
        let it = items(
            &Facts::from_diff(&[file("pipeline/sync_items.py", excluded)]),
            "",
        );
        assert_eq!(it[0].status, Status::Pass, "{it:?}");
        // A continue outside data code is not this rule's business.
        let args = "@@ -1,3 +1,5 @@\n for arg in argv:\n+    if arg.startswith(\"-\"):\n+        continue\n";
        assert!(skips(&[file("cli/args.py", args)]).is_empty());
    }

    #[test]
    fn plan_gaps_ask_for_readers_and_the_other_path() {
        let plan = "## Design\nKeep first_seen_at stable: stop advancing updated_at when only the group changes.\nPick the latest child row per group by start_date.\n";
        let g = plan_gaps(plan);
        let points: Vec<&str> = g.iter().map(|(p, _, _)| *p).collect();
        assert_eq!(points, vec!["cursor_readers", "selection_parity"]);
        let done = format!(
            "{plan}\n### {READERS_HEADING}\n- pipeline/incremental.sql\n### {PARITY_HEADING}\n- sync/full.sql orders by start_date, updated_at, id\n"
        );
        assert!(plan_gaps(&done).is_empty());
        // A data fix that bumps updated_at = now() alters nothing.
        assert!(plan_gaps("UPDATE class SET start_time = '08:00', updated_at = now() WHERE id = 42; bump updated_at").is_empty());
    }

    #[test]
    fn before_and_after_is_read_from_the_notes() {
        let notes = "Evidence:\n- Confirmed: x\n\nBefore and after (read with query_content):\n- item 41 (tie on start_date): before child 7, after child 7 (same as the full sync)\n- item 42 (null end): before missing, after child 9\n";
        assert_eq!(before_after(notes), BeforeAfter::Examples(2));
        assert_eq!(
            before_after("Before and after: no data source reachable (no warehouse connection)"),
            BeforeAfter::Unreachable
        );
        assert_eq!(before_after("Evidence: none needed"), BeforeAfter::Missing);
        assert!(touches_data(&[file("pipeline/upsert_items.sql", FROZEN)]));
        assert!(!touches_data(&[file(
            "web/src/Button.tsx",
            "@@ -1 +1 @@\n-<b>x</b>\n+<i>x</i>\n"
        )]));
    }
}
