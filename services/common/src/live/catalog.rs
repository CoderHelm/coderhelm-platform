//! What a connected source really has, read from the description saved when
//! it was connected (or refreshed): GraphQL types, unions, interfaces, enums
//! and inputs, or database tables and their columns.
//!
//! Agents use it two ways: to look a name up before they write a query
//! ([`Catalog::describe`], [`Catalog::list`], [`Catalog::search`]), and to
//! turn a failed lookup into the real names they should have used
//! ([`graphql_hints`], [`sql_hints`]). Close names are ranked by edit distance
//! on the identifiers themselves.
//!
//! Only names are read from here, never data. Hidden fields and columns are
//! left out of suggestions (a lookup naming one is refused anyway) and marked
//! as hidden where a whole type or table is shown.

use std::sync::LazyLock;

use regex::Regex;

use super::graphql::IMPLEMENTED_BY;
use super::redact::{hidden_terms, hidden_terms_for, is_hidden, TableRef};
use super::sql::table_aliases;

/// Written under a description that was cut short (more columns than one
/// read returns), so a name missing from it is not taken as proof that the
/// source lacks it.
pub const INCOMPLETE_MARK: &str = "# (more columns than were read; this list is incomplete)";
/// A GraphQL description stops growing at this size (see
/// `graphql::schema_from_introspection`); one near it may be missing types.
const API_DESCRIPTION_FULL_BYTES: usize = 290_000;
/// Columns read per database before a description is cut short: Redshift
/// reads 3,000 per database, PostgreSQL 10,000 in all.
const WAREHOUSE_COLUMNS_READ: usize = 3_000;
const DATABASE_COLUMNS_READ: usize = 10_000;

/// Most close names offered for one wrong name.
const MAX_SUGGESTIONS: usize = 5;
/// Most names listed when a whole type or table is shown in a hint.
const MAX_LISTED: usize = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Type,
    Interface,
    Union,
    Enum,
    Input,
    Table,
}

impl EntryKind {
    fn noun(self) -> &'static str {
        match self {
            EntryKind::Type => "type",
            EntryKind::Interface => "interface",
            EntryKind::Union => "union",
            EntryKind::Enum => "enum",
            EntryKind::Input => "input",
            EntryKind::Table => "table",
        }
    }
}

/// One field or column: its name and the rest of its line (arguments and
/// type), as described.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub rest: String,
}

/// One type or table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub kind: EntryKind,
    /// As described: `Order`, `orders`, `billing.invoices`, `public.orders`.
    pub name: String,
    pub fields: Vec<Field>,
    /// A union's members, or the types implementing an interface.
    pub members: Vec<String>,
    /// The block exactly as saved.
    pub block: String,
    /// For a warehouse table, the database it is in (`table public.orders
    /// (database dev)`).
    pub database: Option<String>,
}

impl Entry {
    /// The table or type this entry is, for matching table-scoped hidden
    /// entries: a database table without a schema is in `public`.
    pub fn table_ref(&self) -> TableRef {
        let mut r = TableRef::parse(&self.name);
        if self.kind == EntryKind::Table && r.schema.is_none() {
            r.schema = Some("public".into());
        }
        r
    }

    /// The hidden terms for this entry's own fields. A table gets the
    /// entries scoped to it; an API type gets every entry by field name
    /// (an API's answers don't say which type a field came from).
    pub fn hidden_terms(&self, hidden: &[String]) -> Vec<String> {
        if self.kind == EntryKind::Table {
            hidden_terms_for(hidden, &[self.table_ref()])
        } else {
            hidden_terms(hidden)
        }
    }

    /// The last part of a dotted name: `invoices` for `billing.invoices`.
    fn short(&self) -> &str {
        self.name.rsplit('.').next().unwrap_or(&self.name)
    }

    fn field(&self, name: &str) -> Option<&Field> {
        self.fields
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(name))
    }
}

/// A source's description, parsed.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub entries: Vec<Entry>,
    /// The description may be missing tables or types (it was cut short), so
    /// a name missing from it doesn't prove anything.
    pub incomplete: bool,
    /// Which databases' lists were cut short ("" for a database described
    /// without one).
    pub cut: std::collections::HashSet<String>,
}

fn unquote(s: &str) -> String {
    s.trim().trim_matches('"').to_string()
}

impl Catalog {
    /// Parses the text built by `graphql::schema_from_introspection`,
    /// `db::schema_text` or `redshift::schema_text`. Blocks it doesn't
    /// recognise are skipped.
    pub fn parse(text: &str) -> Catalog {
        let mut entries: Vec<Entry> = Vec::new();
        let mut cut: std::collections::HashSet<String> = Default::default();
        for block in text.split("\n\n") {
            let block = block.trim_matches('\n');
            if block.trim() == INCOMPLETE_MARK {
                // Written after the list it belongs to.
                let db = entries
                    .last()
                    .and_then(|e| e.database.clone())
                    .unwrap_or_default();
                cut.insert(db);
                continue;
            }
            let mut lines = block.lines();
            let Some(head) = lines.next() else { continue };
            let head = head.trim_end();
            let (kind, rest) = if let Some(r) = head.strip_prefix("type ") {
                (EntryKind::Type, r)
            } else if let Some(r) = head.strip_prefix("interface ") {
                (EntryKind::Interface, r)
            } else if let Some(r) = head.strip_prefix("union ") {
                (EntryKind::Union, r)
            } else if let Some(r) = head.strip_prefix("enum ") {
                (EntryKind::Enum, r)
            } else if let Some(r) = head.strip_prefix("input ") {
                (EntryKind::Input, r)
            } else if let Some(r) = head.strip_prefix("table ") {
                (EntryKind::Table, r)
            } else {
                continue;
            };
            let mut members = Vec::new();
            let mut fields = Vec::new();
            let name = match kind {
                EntryKind::Union | EntryKind::Enum => {
                    let (n, list) = rest.split_once('=').unwrap_or((rest, ""));
                    let values: Vec<String> = list
                        .split('|')
                        .map(str::trim)
                        .filter(|v| !v.is_empty() && *v != "...")
                        .map(str::to_string)
                        .collect();
                    if kind == EntryKind::Union {
                        members = values;
                    } else {
                        fields = values
                            .into_iter()
                            .map(|name| Field {
                                name,
                                rest: String::new(),
                            })
                            .collect();
                    }
                    n.trim().to_string()
                }
                // `table orders`, `table public.orders (database dev)`
                EntryKind::Table => rest.split(" (").next().unwrap_or(rest).trim().to_string(),
                _ => rest.trim_end_matches('{').trim().to_string(),
            };
            for line in lines {
                if let Some(list) = line.strip_prefix(IMPLEMENTED_BY) {
                    members = list
                        .split('|')
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .map(str::to_string)
                        .collect();
                    continue;
                }
                let Some(body) = line.strip_prefix("  ") else {
                    continue;
                };
                let cut = body.find(['(', ':']).unwrap_or(body.len());
                let fname = body[..cut].trim();
                if !fname.is_empty() {
                    fields.push(Field {
                        name: fname.to_string(),
                        rest: body[cut..].to_string(),
                    });
                }
            }
            let database = (kind == EntryKind::Table)
                .then(|| {
                    head.split_once(" (database ")
                        .map(|(_, d)| d.trim_end_matches(')').trim().to_string())
                })
                .flatten();
            if !name.is_empty() {
                entries.push(Entry {
                    kind,
                    name,
                    fields,
                    members,
                    block: block.to_string(),
                    database,
                });
            }
        }
        cut.extend(cut_short(text, &entries));
        Catalog {
            incomplete: !cut.is_empty(),
            entries,
            cut,
        }
    }

    /// Only the tables of one warehouse database, when the description says
    /// which database each table is in; otherwise the whole description.
    /// Cut short when that database's list was, or when it lists nothing.
    pub fn in_database(&self, database: Option<&str>) -> Catalog {
        let Some(db) = database.filter(|_| self.entries.iter().any(|e| e.database.is_some()))
        else {
            return self.clone();
        };
        let entries: Vec<Entry> = self
            .entries
            .iter()
            .filter(|e| e.database.as_deref() == Some(db))
            .cloned()
            .collect();
        let cut: std::collections::HashSet<String> = self
            .cut
            .iter()
            .filter(|c| c.as_str() == db)
            .cloned()
            .collect();
        Catalog {
            incomplete: !cut.is_empty() || entries.is_empty(),
            entries,
            cut,
        }
    }

    /// Whether this describes database tables (rather than an API's types).
    pub fn has_tables(&self) -> bool {
        self.is_tables()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn is_tables(&self) -> bool {
        self.entries.iter().any(|e| e.kind == EntryKind::Table)
    }

    /// The entry called `name`, ignoring case and quotes. A table also
    /// answers to its name without a schema or database (`orders` finds
    /// `public.orders`) when only one table has that name.
    pub fn find(&self, name: &str) -> Option<&Entry> {
        let name = unquote(name);
        let name = name.trim_end_matches(['!', ']']).trim_start_matches('[');
        if let Some(e) = self
            .entries
            .iter()
            .find(|e| e.name.eq_ignore_ascii_case(name))
        {
            return Some(e);
        }
        let short = name.rsplit('.').next().unwrap_or(name);
        let mut same: Vec<&Entry> = self
            .entries
            .iter()
            .filter(|e| e.kind == EntryKind::Table && e.short().eq_ignore_ascii_case(short))
            .collect();
        // `public` is where an unqualified name resolves first.
        same.sort_by_key(|e| !e.name.starts_with("public."));
        match same.as_slice() {
            [one] => Some(one),
            [first, ..] if first.name.starts_with("public.") => Some(first),
            _ => None,
        }
    }

    /// The names of the entries closest to `name` (input types left out: no
    /// query selects from one).
    pub fn closest(&self, name: &str) -> Vec<&str> {
        closest(
            name,
            self.entries
                .iter()
                .filter(|e| e.kind != EntryKind::Input)
                .map(|e| e.name.as_str()),
            MAX_SUGGESTIONS,
        )
    }

    /// One type or table in full, with hidden fields marked. A union or
    /// interface is followed by its members' blocks, while they fit.
    pub fn describe(&self, name: &str, hidden: &[String], max_bytes: usize) -> Option<String> {
        let e = self.find(name)?;
        let mut out = mark_hidden(&e.block, &e.hidden_terms(hidden));
        match e.kind {
            EntryKind::Union => out.push_str(&format!(
                "\n({} has no fields of its own. Select from it with inline fragments: \
                 `{{ ... on {} {{ id }} }}`.)\n",
                e.name,
                e.members.first().map(String::as_str).unwrap_or("Member")
            )),
            EntryKind::Interface if !e.members.is_empty() => out.push_str(&format!(
                "\n(Fields above work on every type implementing {}. For a field only one \
                 of them has, use `... on {} {{ field }}`.)\n",
                e.name, e.members[0]
            )),
            _ => {}
        }
        for m in &e.members {
            let Some(member) = self.find(m) else { continue };
            let b = mark_hidden(&member.block, &member.hidden_terms(hidden));
            if out.len() + b.len() + 2 > max_bytes {
                out.push_str("\n(more members left out; look each one up by name)\n");
                break;
            }
            out.push('\n');
            out.push_str(&b);
            out.push('\n');
        }
        Some(crate::head_tail_str(&out, max_bytes))
    }

    /// Every table, or every type and its kind, by name only.
    pub fn list(&self, max_bytes: usize) -> String {
        let mut out = if self.is_tables() {
            format!("{} tables:\n", self.entries.len())
        } else {
            format!("{} types:\n", self.entries.len())
        };
        for (i, e) in self.entries.iter().enumerate() {
            let item = if e.kind == EntryKind::Table {
                e.name.clone()
            } else {
                format!("{} ({})", e.name, e.kind.noun())
            };
            if out.len() + item.len() + 2 > max_bytes {
                out.push_str(&format!(
                    "\n({} more left out; search for a word to narrow it)",
                    self.entries.len() - i
                ));
                break;
            }
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&item);
        }
        out
    }

    /// The blocks whose name or fields contain `term`; when none do, the
    /// closest names instead of nothing.
    pub fn search(&self, term: &str, hidden: &[String], max_bytes: usize) -> String {
        let term = term.trim().to_lowercase();
        let mut out = String::new();
        for e in &self.entries {
            if !e.block.to_lowercase().contains(&term) {
                continue;
            }
            let b = mark_hidden(&e.block, &e.hidden_terms(hidden));
            if out.len() + b.len() + 2 > max_bytes {
                out.push_str("\n(more matches left out; search for something narrower)\n");
                break;
            }
            out.push_str(b.trim_end());
            out.push_str("\n\n");
        }
        if !out.is_empty() {
            return out;
        }
        let mut near = self.closest(&term);
        if near.is_empty() {
            // Field and column names too: a word may only be a column's name.
            let mut by_field: Vec<&str> = Vec::new();
            for e in &self.entries {
                let names: Vec<&str> = e.fields.iter().map(|f| f.name.as_str()).collect();
                if !closest(&term, names, 1).is_empty() {
                    by_field.push(e.name.as_str());
                }
            }
            by_field.truncate(MAX_SUGGESTIONS);
            near = by_field;
        }
        if near.is_empty() {
            format!(
                "Nothing here matches \"{term}\". Leave the search empty to list every {}.",
                if self.is_tables() { "table" } else { "type" }
            )
        } else {
            format!(
                "Nothing here matches \"{term}\". Closest: {}. Look one up by name.",
                near.join(", ")
            )
        }
    }
}

/// Appends ` (hidden: refused)` to each field or column line whose name is
/// hidden, so an agent doesn't spend a lookup on it.
fn mark_hidden(block: &str, terms: &[String]) -> String {
    if terms.is_empty() {
        return block.to_string();
    }
    let mut out = String::with_capacity(block.len() + 32);
    for line in block.lines() {
        out.push_str(line);
        if let Some(body) = line.strip_prefix("  ") {
            let cut = body.find(['(', ':']).unwrap_or(body.len());
            if is_hidden(body[..cut].trim(), terms) {
                out.push_str("  (hidden: refused)");
            }
        }
        out.push('\n');
    }
    out
}

/// Lowercase without `_`, `-`, quotes or spaces, so `order_items`,
/// `OrderItems` and `"orderItems"` compare equal.
fn norm(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// Levenshtein distance between two strings, by character.
pub fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0; b.len() + 1];
    for i in 1..=a.len() {
        cur[0] = i;
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// How far `candidate` is from `wanted`, 0 (same) to 1 (nothing alike).
/// Compares the normalised names, and for a dotted name its last part too.
fn distance(wanted: &str, candidate: &str) -> f64 {
    let w = norm(wanted.rsplit('.').next().unwrap_or(wanted));
    let mut best = f64::MAX;
    for c in [candidate, candidate.rsplit('.').next().unwrap_or(candidate)] {
        let c = norm(c);
        if w.is_empty() || c.is_empty() {
            continue;
        }
        let longest = w.chars().count().max(c.chars().count()) as f64;
        let mut d = edit_distance(&w, &c) as f64 / longest;
        // One name inside the other (`order` in `orders`, `ordertype` in
        // `type`) is close whatever the length difference.
        let shortest = w.chars().count().min(c.chars().count());
        if shortest >= 3 && (c.contains(&w) || w.contains(&c)) {
            d = d.min(0.3);
        }
        // The same with plurals taken off: `shipments` is close to
        // `order_shipment`, `people` isn't helped but `orders` is.
        let (ws, cs) = (singular(&w), singular(&c));
        if ws.chars().count().min(cs.chars().count()) >= 4 && (c.contains(&ws) || w.contains(&cs)) {
            d = d.min(0.35);
        }
        best = best.min(d);
    }
    best
}

/// A word without a plural ending: `boxes` → `box`, `entries` →
/// `entry`, `orders` → `order`.
fn singular(w: &str) -> String {
    if let Some(s) = w.strip_suffix("ies") {
        return format!("{s}y");
    }
    for end in ["sses", "shes", "ches", "xes"] {
        if w.ends_with(end) {
            return w[..w.len() - 2].to_string();
        }
    }
    match w.strip_suffix('s') {
        Some(s) if !s.ends_with('s') => s.to_string(),
        _ => w.to_string(),
    }
}

/// Up to `n` of `candidates` closest to `wanted`, closest first. Names too
/// far away to be a plausible fix are left out, so the list can be empty.
pub fn closest<'a>(
    wanted: &str,
    candidates: impl IntoIterator<Item = &'a str>,
    n: usize,
) -> Vec<&'a str> {
    let mut scored: Vec<(f64, &str)> = candidates
        .into_iter()
        .map(|c| (distance(wanted, c), c))
        .filter(|(d, c)| *d <= 0.5 && !c.eq_ignore_ascii_case(wanted))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(b.1)));
    scored.dedup_by(|a, b| a.1 == b.1);
    scored.into_iter().take(n).map(|(_, c)| c).collect()
}

/// Up to [`MAX_LISTED`] names, comma separated, with how many were left out.
fn listing<'a>(names: impl IntoIterator<Item = &'a str>) -> String {
    let all: Vec<&str> = names.into_iter().collect();
    let mut s = all
        .iter()
        .take(MAX_LISTED)
        .copied()
        .collect::<Vec<_>>()
        .join(", ");
    if all.len() > MAX_LISTED {
        s.push_str(&format!(", and {} more", all.len() - MAX_LISTED));
    }
    s
}

fn visible<'a>(e: &'a Entry, terms: &'a [String]) -> impl Iterator<Item = &'a str> + 'a {
    e.fields
        .iter()
        .map(|f| f.name.as_str())
        .filter(move |n| !is_hidden(n, terms))
}

static GQL_FIELD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"Cannot query field "([^"]+)" on type "([^"]+)""#).expect("valid")
});
static GQL_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"Unknown type "([^"]+)""#).expect("valid"));
static GQL_ARG: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"Unknown argument "([^"]+)" on field "([^"]+)"(?: of type "([^"]+)")?"#)
        .expect("valid")
});
static GQL_SUBFIELDS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"Field "([^"]+)" of type "([^"]+)" must have a selection of subfields"#)
        .expect("valid")
});

/// Real names to use instead of the ones a GraphQL API rejected, one line
/// each. `error` is the API's own message (or several, in any order).
pub fn graphql_hints(catalog: &Catalog, error: &str, hidden: &[String]) -> Vec<String> {
    let terms = hidden_terms(hidden);
    let mut out: Vec<String> = Vec::new();
    for c in GQL_FIELD.captures_iter(error) {
        let (field, on) = (&c[1], &c[2]);
        let Some(e) = catalog.find(on) else {
            out.push(not_described(catalog, on));
            continue;
        };
        out.push(field_hint(catalog, e, field, &terms));
    }
    for c in GQL_TYPE.captures_iter(error) {
        let near = catalog.closest(&c[1]);
        out.push(if near.is_empty() {
            format!(
                "No type is called {}. Look up the field you select from to see the type it returns.",
                &c[1]
            )
        } else {
            format!("No type is called {}. Closest real types: {}.", &c[1], near.join(", "))
        });
    }
    for c in GQL_ARG.captures_iter(error) {
        let arg = &c[1];
        // `Type.field` in newer servers; `field` plus `of type "Type"` in older.
        let (on, field) = match (c.get(3), c[2].split_once('.')) {
            (Some(t), _) => (t.as_str().to_string(), c[2].to_string()),
            (None, Some((t, f))) => (t.to_string(), f.to_string()),
            (None, None) => (String::new(), c[2].to_string()),
        };
        let f = catalog
            .find(&on)
            .and_then(|e| e.field(&field))
            .or_else(|| catalog.entries.iter().find_map(|e| e.field(&field)));
        match f {
            Some(f) if f.rest.starts_with('(') => out.push(format!(
                "{field} takes no argument {arg}. Its arguments: {}",
                f.rest
                    .split("):")
                    .next()
                    .unwrap_or(&f.rest)
                    .trim_start_matches('(')
            )),
            Some(_) => out.push(format!("{field} takes no arguments.")),
            None => {}
        }
    }
    for c in GQL_SUBFIELDS.captures_iter(error) {
        let ty = c[2].trim_matches(|ch| ch == '[' || ch == ']' || ch == '!');
        if let Some(e) = catalog.find(ty) {
            out.push(match e.kind {
                EntryKind::Union | EntryKind::Interface if !e.members.is_empty() => format!(
                    "{} returns {ty}, a {} of {}: select fields inside `... on {} {{ }}`.",
                    &c[1],
                    e.kind.noun(),
                    e.members.join(", "),
                    e.members[0]
                ),
                _ => format!(
                    "{} returns {ty}; pick fields from it: {}.",
                    &c[1],
                    listing(visible(e, &terms))
                ),
            });
        }
    }
    out.dedup();
    out
}

fn not_described(catalog: &Catalog, name: &str) -> String {
    let near = catalog.closest(name);
    if near.is_empty() {
        format!("{name} isn't in this source's saved description.")
    } else {
        format!(
            "{name} isn't in this source's saved description. Closest: {}.",
            near.join(", ")
        )
    }
}

fn field_hint(catalog: &Catalog, e: &Entry, field: &str, terms: &[String]) -> String {
    let union_like = matches!(e.kind, EntryKind::Union | EntryKind::Interface);
    if union_like && !e.members.is_empty() {
        let with: Vec<&str> = e
            .members
            .iter()
            .filter(|m| catalog.find(m).is_some_and(|me| me.field(field).is_some()))
            .map(String::as_str)
            .collect();
        let example = with.first().copied().unwrap_or(&e.members[0]);
        let mut s = format!(
            "{} is a {} of {}. Its members' fields are selected inside inline fragments: \
             `... on {example} {{ {field} }}`.",
            e.name,
            e.kind.noun(),
            e.members.join(", "),
        );
        if e.kind == EntryKind::Interface && !e.fields.is_empty() {
            s.push_str(&format!(
                " Fields every member has: {}.",
                listing(visible(e, terms))
            ));
        }
        if with.is_empty() {
            s.push_str(&format!(" No member has a field called {field}."));
        } else if with.len() < e.members.len() {
            s.push_str(&format!(" Members with {field}: {}.", with.join(", ")));
        }
        return s;
    }
    let names: Vec<&str> = visible(e, terms).collect();
    let near = closest(field, names.iter().copied(), MAX_SUGGESTIONS);
    let mut s = format!("{} has no field {field}.", e.name);
    if !near.is_empty() {
        s.push_str(&format!(" Closest: {}.", near.join(", ")));
    }
    s.push_str(&format!(" Its fields: {}.", listing(names)));
    s
}

static SQL_RELATION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"relation "([^"]+)" does not exist"#).expect("valid"));
static SQL_COLUMN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"column ((?:"[^"]+"|[A-Za-z0-9_$]+)(?:\.(?:"[^"]+"|[A-Za-z0-9_$]+))*) does not exist(?: in ([A-Za-z0-9_."]+))?"#)
        .expect("valid")
});

/// True when an error from a source says a type, field, argument, table or
/// column the lookup named doesn't exist.
pub fn is_name_error(error: &str) -> bool {
    [
        &GQL_FIELD,
        &GQL_TYPE,
        &GQL_ARG,
        &GQL_SUBFIELDS,
        &SQL_RELATION,
        &SQL_COLUMN,
    ]
    .iter()
    .any(|re| re.is_match(error))
}

/// Real names to use instead of the ones a database rejected, one line each.
/// `error` is the database's message; `sql` is the lookup that failed, used
/// to tell which table an alias stands for.
pub fn sql_hints(catalog: &Catalog, error: &str, sql: &str, hidden: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for c in SQL_RELATION.captures_iter(error) {
        let wanted = &c[1];
        let near = catalog.closest(wanted);
        out.push(if near.is_empty() {
            format!(
                "No table is called {wanted}. Call live_describe with an empty search to list the tables."
            )
        } else {
            format!("No table is called {wanted}. Closest real tables: {}.", near.join(", "))
        });
    }
    let aliases = table_aliases(sql);
    for c in SQL_COLUMN.captures_iter(error) {
        let parts: Vec<String> = c[1].split('.').map(unquote).collect();
        let Some((column, qualifier)) = parts.split_last() else {
            continue;
        };
        let qualifier = qualifier.join(".").to_lowercase();
        // Which tables the column could belong to: the one the error names,
        // the one its qualifier stands for, or every table in the lookup.
        let named: Vec<String> = if let Some(t) = c.get(2) {
            vec![unquote(t.as_str())]
        } else if !qualifier.is_empty() {
            aliases
                .iter()
                .filter(|(called, full)| *called == qualifier || *full == qualifier)
                .map(|(_, full)| full.clone())
                .collect()
        } else {
            aliases.iter().map(|(_, full)| full.clone()).collect()
        };
        let mut tables: Vec<&Entry> = named.iter().filter_map(|t| catalog.find(t)).collect();
        tables.dedup_by(|a, b| a.name == b.name);
        if tables.is_empty() {
            continue;
        }
        for e in tables {
            let terms = e.hidden_terms(hidden);
            let names: Vec<&str> = visible(e, &terms).collect();
            let near = closest(column, names.iter().copied(), MAX_SUGGESTIONS);
            let mut s = format!("{} has no column {column}.", e.name);
            if !near.is_empty() {
                s.push_str(&format!(" Closest: {}.", near.join(", ")));
            }
            s.push_str(&format!(" Its columns: {}.", listing(names)));
            out.push(s);
        }
    }
    out.dedup();
    out
}

/// Whether a description saved before [`INCOMPLETE_MARK`] existed was cut
/// short: as many columns as one read returns, or an API description at its
/// size limit.
fn cut_short(text: &str, entries: &[Entry]) -> Vec<String> {
    if !entries.iter().any(|e| e.kind == EntryKind::Table) {
        let full = !entries.is_empty() && text.len() >= API_DESCRIPTION_FULL_BYTES;
        return if full {
            vec![String::new()]
        } else {
            Vec::new()
        };
    }
    let mut per_db: std::collections::HashMap<&str, usize> = Default::default();
    for e in entries.iter().filter(|e| e.kind == EntryKind::Table) {
        *per_db
            .entry(e.database.as_deref().unwrap_or(""))
            .or_default() += e.fields.len();
    }
    per_db
        .into_iter()
        .filter(|(db, n)| {
            if db.is_empty() {
                *n >= DATABASE_COLUMNS_READ
            } else {
                // Warehouses read this many per database before 10,000.
                *n == WAREHOUSE_COLUMNS_READ || *n >= DATABASE_COLUMNS_READ
            }
        })
        .map(|(db, _)| db.to_string())
        .collect()
}

/// Schemas and name prefixes of a database's own system tables. They are
/// never in a source's description, so they are never called unknown.
fn is_system_table(r: &TableRef) -> bool {
    const SCHEMAS: &[&str] = &[
        "pg_catalog",
        "information_schema",
        "pg_internal",
        "pg_toast",
    ];
    const PREFIXES: &[&str] = &["pg_", "stl_", "stv_", "svl_", "svv_", "sys_"];
    r.schema.as_deref().is_some_and(|s| SCHEMAS.contains(&s))
        || (r.schema.is_none() && PREFIXES.iter().any(|p| r.table.starts_with(p)))
}

/// Words a database reads as a value where a column could go, so a bare one
/// is never called an unknown column.
const PSEUDO_COLUMNS: &[&str] = &[
    "sysdate",
    "getdate",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "current_role",
    "current_schema",
    "current_catalog",
    "current_database",
    "session_user",
    "user",
    "localtime",
    "localtimestamp",
    "true",
    "false",
    "null",
    "oid",
    "ctid",
    "xmin",
    "xmax",
    "cmin",
    "cmax",
    "tableoid",
];

/// Functions whose first argument is a date part (`dateadd(day, -30, x)`),
/// a keyword the database reads, never a column.
const DATE_PART_FUNCTIONS: &[&str] = &[
    "dateadd",
    "datediff",
    "date_add",
    "date_diff",
    "datepart",
    "date_part",
    "datetrunc",
    "date_trunc",
    "datename",
    "timestampadd",
    "timestampdiff",
    "time_slice",
];

/// What a SQL lookup names, read from its parsed form.
#[derive(Default)]
struct SqlNames {
    /// Names WITH gives a query; reading one is not reading a table.
    ctes: std::collections::HashSet<String>,
    /// Every table read: the reference, and what the lookup calls it.
    tables: Vec<(TableRef, String)>,
    /// Names that stand for something other than a described table: a
    /// subquery's alias, a function's, a system table's.
    opaque: std::collections::HashSet<String>,
    /// Every `x.y` used in an expression.
    qualified: Vec<(String, String)>,
    /// Every bare name used in an expression.
    bare: Vec<String>,
    /// Bare names given as a date part (see [`DATE_PART_FUNCTIONS`]).
    date_parts: std::collections::HashSet<String>,
    /// Names a SELECT gives its own output columns.
    output: std::collections::HashSet<String>,
    /// A factor that isn't a plain table (subquery, function, VALUES).
    has_other_factor: bool,
}

fn ident_name(i: &sqlparser::ast::Ident) -> String {
    i.value.to_lowercase()
}

fn output_names(body: &sqlparser::ast::SetExpr, out: &mut std::collections::HashSet<String>) {
    use sqlparser::ast::{SelectItem, SetExpr};
    match body {
        SetExpr::Select(sel) => {
            for item in &sel.projection {
                if let SelectItem::ExprWithAlias { alias, .. } = item {
                    out.insert(ident_name(alias));
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            output_names(left, out);
            output_names(right, out);
        }
        _ => {}
    }
}

impl sqlparser::ast::Visitor for SqlNames {
    type Break = ();

    fn pre_visit_query(&mut self, q: &sqlparser::ast::Query) -> std::ops::ControlFlow<()> {
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                self.ctes.insert(ident_name(&cte.alias.name));
            }
        }
        output_names(&q.body, &mut self.output);
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(
        &mut self,
        t: &sqlparser::ast::TableFactor,
    ) -> std::ops::ControlFlow<()> {
        use sqlparser::ast::TableFactor;
        match t {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                let full = name
                    .0
                    .iter()
                    .filter_map(|p| p.as_ident())
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                let r = TableRef::parse(&full);
                let called = alias
                    .as_ref()
                    .map(|a| ident_name(&a.name))
                    .unwrap_or_else(|| r.table.clone());
                if args.is_some() {
                    // `FROM generate_series(1, 3) AS g`: a function, not a table.
                    self.has_other_factor = true;
                    self.opaque.insert(called);
                } else {
                    self.tables.push((r, called));
                }
            }
            TableFactor::NestedJoin { alias, .. } => {
                if let Some(a) = alias {
                    self.opaque.insert(ident_name(&a.name));
                }
            }
            other => {
                self.has_other_factor = true;
                let alias = match other {
                    TableFactor::Derived { alias, .. }
                    | TableFactor::TableFunction { alias, .. }
                    | TableFactor::Function { alias, .. }
                    | TableFactor::UNNEST { alias, .. } => alias.as_ref(),
                    _ => None,
                };
                if let Some(a) = alias {
                    self.opaque.insert(ident_name(&a.name));
                }
            }
        }
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &sqlparser::ast::Expr) -> std::ops::ControlFlow<()> {
        use sqlparser::ast::Expr;
        match e {
            Expr::Identifier(i) if i.quote_style.is_none() || i.quote_style == Some('"') => {
                self.bare.push(ident_name(i))
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => self
                .qualified
                .push((ident_name(&parts[0]), ident_name(&parts[1]))),
            Expr::Function(f) => {
                use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
                let name = f
                    .name
                    .0
                    .last()
                    .and_then(|p| p.as_ident())
                    .map(|i| i.value.to_ascii_lowercase())
                    .unwrap_or_default();
                if DATE_PART_FUNCTIONS.contains(&name.as_str()) {
                    if let FunctionArguments::List(list) = &f.args {
                        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            Expr::Identifier(i),
                        ))) = list.args.first()
                        {
                            if i.quote_style.is_none() {
                                self.date_parts.insert(ident_name(i));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        std::ops::ControlFlow::Continue(())
    }
}

impl Catalog {
    /// The described tables a reference could be: by schema and name when
    /// it names a schema, by name in any schema when it doesn't (the
    /// database's search path decides which).
    fn tables_for(&self, r: &TableRef) -> Vec<&Entry> {
        self.entries
            .iter()
            .filter(|e| e.kind == EntryKind::Table)
            .filter(|e| {
                let er = e.table_ref();
                er.table == r.table
                    && match &r.schema {
                        Some(s) => er.schema.as_deref() == Some(s.as_str()),
                        None => true,
                    }
            })
            .collect()
    }

    /// Up to five described table names closest to `wanted`, by name.
    fn closest_tables(&self, wanted: &str) -> Vec<&str> {
        closest(
            wanted,
            self.entries
                .iter()
                .filter(|e| e.kind == EntryKind::Table)
                .map(|e| e.name.as_str()),
            MAX_SUGGESTIONS,
        )
    }
}

/// Checks a SQL lookup's tables and columns against the source's
/// description before anything is sent. Returns one line per name the
/// source doesn't have, with the closest real names; empty when every name
/// is known, or when the description can't settle it (none saved, cut
/// short, or SQL that doesn't parse: the read-only check refuses that).
///
/// Columns are checked when it is certain which table they belong to: a
/// `t.column` whose `t` is a described table or its alias, or a bare column
/// in a lookup that reads exactly one table and nothing else. Names WITH
/// gives a query and subquery aliases are never taken for tables.
/// [`unknown_sql_names`] for a lookup run in one warehouse database: only
/// that database's tables count.
pub fn unknown_sql_names_in(
    catalog: &Catalog,
    database: Option<&str>,
    sql: &str,
    hidden: &[String],
) -> Vec<String> {
    unknown_sql_names(&catalog.in_database(database), sql, hidden)
}

pub fn unknown_sql_names(catalog: &Catalog, sql: &str, hidden: &[String]) -> Vec<String> {
    use sqlparser::ast::Visit;
    if catalog.incomplete || !catalog.is_tables() {
        return Vec::new();
    }
    let Ok(statements) =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, sql)
    else {
        return Vec::new();
    };
    let mut names = SqlNames::default();
    for st in &statements {
        let _ = st.visit(&mut names);
    }
    let mut out: Vec<String> = Vec::new();
    // What each name a lookup calls a table stands for: `Some(entry)` when it
    // is one described table, `None` when it can't be checked.
    let mut called: std::collections::HashMap<String, Option<&Entry>> = Default::default();
    let mut real_tables = 0usize;
    for (r, call) in &names.tables {
        if r.schema.is_none() && names.ctes.contains(&r.table) {
            called.insert(call.clone(), None);
            continue;
        }
        if is_system_table(r) {
            called.insert(call.clone(), None);
            names.has_other_factor = true;
            continue;
        }
        real_tables += 1;
        let found = catalog.tables_for(r);
        match found.as_slice() {
            [] => {
                let wanted = match &r.schema {
                    Some(s) => format!("{s}.{}", r.table),
                    None => r.table.clone(),
                };
                let near = catalog.closest_tables(&wanted);
                out.push(if near.is_empty() {
                    format!(
                        "No table named {wanted}. Call live_describe with an empty search to list the tables."
                    )
                } else {
                    format!("No table named {wanted}. Closest: {}.", near.join(", "))
                });
                called.insert(call.clone(), None);
            }
            [one] => {
                // Two tables under one name make it unclear; never guess.
                let entry = match called.get(call) {
                    Some(Some(prev)) if prev.name != one.name => None,
                    Some(None) => None,
                    _ => Some(*one),
                };
                called.insert(call.clone(), entry);
            }
            _ => {
                called.insert(call.clone(), None);
            }
        }
    }
    for o in &names.opaque {
        called.insert(o.clone(), None);
    }
    let column_line = |e: &Entry, column: &str| -> String {
        let terms = e.hidden_terms(hidden);
        let visible_names: Vec<&str> = visible(e, &terms).collect();
        let near = closest(column, visible_names.iter().copied(), MAX_SUGGESTIONS);
        let mut s = format!("{} has no column {column}.", e.name);
        if !near.is_empty() {
            s.push_str(&format!(" Closest: {}.", near.join(", ")));
        }
        s.push_str(&format!(" Its columns: {}.", listing(visible_names)));
        s
    };
    for (q, column) in &names.qualified {
        if let Some(Some(e)) = called.get(q) {
            if e.field(column).is_none() {
                out.push(column_line(e, column));
            }
        }
    }
    // A bare column is only checked when one described table is all the
    // lookup reads.
    let only: Vec<&Entry> = called.values().filter_map(|e| *e).collect();
    if real_tables == 1
        && names.ctes.is_empty()
        && !names.has_other_factor
        && names.tables.len() == 1
        && only.len() == 1
    {
        let e = only[0];
        for column in &names.bare {
            if names.output.contains(column)
                || names.date_parts.contains(column)
                || PSEUDO_COLUMNS.contains(&column.as_str())
                || called.contains_key(column)
            {
                continue;
            }
            if e.field(column).is_none() {
                out.push(column_line(e, column));
            }
        }
    }
    out.dedup();
    let mut seen = std::collections::HashSet::new();
    out.retain(|l| seen.insert(l.clone()));
    out
}

/// The named type a field returns: `Order` for `(first: Int): [Order!]!`.
fn returned_type(rest: &str) -> String {
    let after = if rest.starts_with('(') {
        rest.rsplit_once("):").map(|(_, t)| t).unwrap_or("")
    } else {
        rest.trim_start_matches(':')
    };
    after
        .trim()
        .trim_matches(|c| c == '[' || c == ']' || c == '!')
        .to_string()
}

/// The argument names a field takes: `["first", "status"]`.
fn argument_names(rest: &str) -> Vec<String> {
    if !rest.starts_with('(') {
        return Vec::new();
    }
    let inside = rest[1..].split("):").next().unwrap_or("");
    inside
        .split(',')
        .filter_map(|a| a.split(':').next())
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty())
        .collect()
}

/// Checks a GraphQL lookup's fields, arguments and fragment types against
/// the API's description before anything is sent: one line per name the API
/// doesn't have, with the real ones. Empty when everything is known or the
/// description can't settle it.
pub fn unknown_graphql_names(catalog: &Catalog, query: &str, hidden: &[String]) -> Vec<String> {
    use graphql_parser::query::{
        parse_query, Definition, FragmentDefinition, OperationDefinition, Selection, SelectionSet,
        TypeCondition,
    };
    let Some(root) = catalog
        .entries
        .first()
        .filter(|e| e.kind == EntryKind::Type)
    else {
        return Vec::new();
    };
    let Ok(doc) = parse_query::<String>(query.trim()) else {
        return Vec::new();
    };
    let terms = hidden_terms(hidden);
    let mut fragments: std::collections::HashMap<&str, &FragmentDefinition<String>> =
        Default::default();
    let mut operation: Option<&SelectionSet<String>> = None;
    for d in &doc.definitions {
        match d {
            Definition::Fragment(f) => {
                fragments.insert(f.name.as_str(), f);
            }
            Definition::Operation(OperationDefinition::SelectionSet(s)) => operation = Some(s),
            Definition::Operation(OperationDefinition::Query(q)) => {
                operation = Some(&q.selection_set)
            }
            _ => return Vec::new(),
        }
    }
    let Some(operation) = operation else {
        return Vec::new();
    };

    struct Walk<'a> {
        catalog: &'a Catalog,
        fragments: &'a std::collections::HashMap<&'a str, &'a FragmentDefinition<'a, String>>,
        terms: &'a [String],
        out: Vec<String>,
    }
    impl<'a> Walk<'a> {
        fn type_named(&mut self, name: &str) -> Option<&'a Entry> {
            let found = self.catalog.find(name);
            if found.is_none() && !self.catalog.incomplete {
                self.out.push(not_described_type(self.catalog, name));
            }
            found
        }

        fn set(&mut self, set: &'a SelectionSet<'a, String>, on: &'a Entry, depth: usize) {
            if depth > 12 {
                return;
            }
            for item in &set.items {
                match item {
                    Selection::Field(f) => {
                        if f.name.starts_with("__") {
                            continue;
                        }
                        let field = on
                            .field(&f.name)
                            .filter(|_| !matches!(on.kind, EntryKind::Union | EntryKind::Enum));
                        let Some(field) = field else {
                            self.out
                                .push(field_hint(self.catalog, on, &f.name, self.terms));
                            continue;
                        };
                        let takes = argument_names(&field.rest);
                        for (arg, _) in &f.arguments {
                            if !takes.iter().any(|t| t == arg) {
                                self.out.push(if takes.is_empty() {
                                    format!("{} takes no arguments.", f.name)
                                } else {
                                    format!(
                                        "{} takes no argument {arg}. Its arguments: {}.",
                                        f.name,
                                        takes.join(", ")
                                    )
                                });
                            }
                        }
                        if !f.selection_set.items.is_empty() {
                            if let Some(next) = self.catalog.find(&returned_type(&field.rest)) {
                                self.set(&f.selection_set, next, depth + 1);
                            }
                        }
                    }
                    Selection::InlineFragment(fr) => {
                        let next = match &fr.type_condition {
                            Some(TypeCondition::On(t)) => self.type_named(t),
                            None => Some(on),
                        };
                        if let Some(next) = next {
                            self.set(&fr.selection_set, next, depth + 1);
                        }
                    }
                    Selection::FragmentSpread(sp) => {
                        let Some(fr) = self.fragments.get(sp.fragment_name.as_str()) else {
                            continue;
                        };
                        let TypeCondition::On(t) = &fr.type_condition;
                        if let Some(next) = self.type_named(t) {
                            self.set(&fr.selection_set, next, depth + 1);
                        }
                    }
                }
            }
        }
    }
    let mut w = Walk {
        catalog,
        fragments: &fragments,
        terms: &terms,
        out: Vec::new(),
    };
    w.set(operation, root, 1);
    let mut seen = std::collections::HashSet::new();
    w.out.retain(|l| seen.insert(l.clone()));
    w.out
}

fn not_described_type(catalog: &Catalog, name: &str) -> String {
    let near = catalog.closest(name);
    if near.is_empty() {
        format!("No type named {name}.")
    } else {
        format!("No type named {name}. Closest: {}.", near.join(", "))
    }
}

/// What an agent is told when a lookup names something the source doesn't
/// have: nothing was sent, the real names, and to look the shape up first.
pub fn unknown_names_message(source: &str, lines: &[String]) -> String {
    let mut s =
        format!("Not sent to {source}: the lookup names something this source doesn't have.");
    for l in lines {
        s.push_str("\n- ");
        s.push_str(l);
    }
    s.push_str(
        "\nDescribe the table or type with live_describe (`name`) before querying, then use only the names it lists.",
    );
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const API: &str = "type Query {\n  orders(first: Int, status: OrderStatus): [Order!]!\n  search(term: String!): [SearchResult]\n}\n\n\
        type Order {\n  id: ID!\n  placedAt: DateTime\n  totalCents: Int\n  customerEmail: String\n}\n\n\
        type Refund {\n  id: ID!\n  amountCents: Int\n  placedAt: DateTime\n}\n\n\
        type Customer {\n  id: ID!\n  name: String\n}\n\n\
        union SearchResult = Order | Refund | Customer\n\n\
        interface Node {\n  id: ID!\n}\n# implemented by: Order | Customer\n\n\
        enum OrderStatus = OPEN | SHIPPED\n\n\
        type OrderLine {\n  sku: String\n}\n";

    const DB: &str = "table orders\n  id: integer\n  status_code: text\n  placed_at: timestamp\n  contact_email: text\n\n\
        table order_items\n  id: integer\n  order_id: integer\n\n\
        table billing.invoices\n  id: uuid\n  ordertype: text\n";

    fn api() -> Catalog {
        Catalog::parse(API)
    }
    fn db() -> Catalog {
        Catalog::parse(DB)
    }

    #[test]
    fn parses_every_kind_of_block() {
        let c = api();
        let kinds: Vec<(EntryKind, &str)> = c
            .entries
            .iter()
            .map(|e| (e.kind, e.name.as_str()))
            .collect();
        assert_eq!(
            kinds,
            vec![
                (EntryKind::Type, "Query"),
                (EntryKind::Type, "Order"),
                (EntryKind::Type, "Refund"),
                (EntryKind::Type, "Customer"),
                (EntryKind::Union, "SearchResult"),
                (EntryKind::Interface, "Node"),
                (EntryKind::Enum, "OrderStatus"),
                (EntryKind::Type, "OrderLine"),
            ]
        );
        assert_eq!(
            c.find("searchresult").unwrap().members,
            ["Order", "Refund", "Customer"]
        );
        assert_eq!(c.find("Node").unwrap().members, ["Order", "Customer"]);
        let q = c.find("Query").unwrap();
        assert_eq!(q.fields[0].name, "orders");
        assert_eq!(
            q.fields[0].rest,
            "(first: Int, status: OrderStatus): [Order!]!"
        );
        let d = db();
        assert_eq!(d.find("invoices").unwrap().name, "billing.invoices");
        assert_eq!(d.find("\"orders\"").unwrap().fields.len(), 4);
        let rs = Catalog::parse("table public.orders (database dev)\n  id: integer\n");
        assert_eq!(rs.find("orders").unwrap().name, "public.orders");
        assert_eq!(rs.find("public.orders").unwrap().fields[0].name, "id");
    }

    #[test]
    fn closest_names_rank_by_edit_distance() {
        assert_eq!(edit_distance("kitten", "sitting"), 3);
        assert_eq!(edit_distance("", "abc"), 3);
        let names = ["OrderLine", "Order", "Refund", "Customer"];
        assert_eq!(closest("Ordr", names, 3), vec!["Order"]);
        assert_eq!(closest("OrderLines", names, 3), vec!["OrderLine", "Order"]);
        // Case, underscores and plurals don't hide a match.
        assert_eq!(closest("ORDER_LINE", names, 1), vec!["OrderLine"]);
        let tables = ["orders", "order_items", "billing.invoices"];
        assert_eq!(closest("order", tables, 5)[0], "orders");
        assert_eq!(closest("invoice", tables, 5), vec!["billing.invoices"]);
        // Nothing plausible means nothing offered.
        assert!(closest("zzzzzz", names, 3).is_empty());
        // The name itself is never offered as its own fix.
        assert!(!closest("Order", names, 5).contains(&"Order"));
    }

    #[test]
    fn a_field_on_a_union_points_at_inline_fragments() {
        let h = graphql_hints(
            &api(),
            r#"Cannot query field "placedAt" on type "SearchResult"."#,
            &[],
        );
        assert_eq!(h.len(), 1);
        assert!(
            h[0].contains("SearchResult is a union of Order, Refund, Customer"),
            "{}",
            h[0]
        );
        assert!(h[0].contains("`... on Order { placedAt }`"), "{}", h[0]);
        assert!(
            h[0].contains("Members with placedAt: Order, Refund."),
            "{}",
            h[0]
        );
        let h = graphql_hints(&api(), r#"Cannot query field "sku" on type "Node"."#, &[]);
        assert!(h[0].contains("interface of Order, Customer"), "{}", h[0]);
        assert!(h[0].contains("Fields every member has: id."), "{}", h[0]);
        assert!(
            h[0].contains("No member has a field called sku."),
            "{}",
            h[0]
        );
    }

    #[test]
    fn an_unknown_field_or_type_gets_the_real_names() {
        let h = graphql_hints(
            &api(),
            r#"Cannot query field "total" on type "Order". Did you mean "totalCents"?"#,
            &["email".to_string()],
        );
        assert!(
            h[0].starts_with("Order has no field total. Closest: totalCents."),
            "{}",
            h[0]
        );
        assert!(
            h[0].contains("Its fields: id, placedAt, totalCents."),
            "{}",
            h[0]
        );
        assert!(
            !h[0].contains("customerEmail"),
            "hidden fields aren't suggested: {}",
            h[0]
        );
        let h = graphql_hints(&api(), r#"Unknown type "OrderLines"."#, &[]);
        assert_eq!(
            h,
            vec![
                "No type is called OrderLines. Closest real types: OrderLine, Order, OrderStatus."
            ]
        );
        let h = graphql_hints(
            &api(),
            r#"Unknown argument "limit" on field "Query.orders"."#,
            &[],
        );
        assert_eq!(
            h,
            vec!["orders takes no argument limit. Its arguments: first: Int, status: OrderStatus"]
        );
        let h = graphql_hints(
            &api(),
            r#"Field "search" of type "[SearchResult]" must have a selection of subfields. Did you mean "search { ... }"?"#,
            &[],
        );
        assert!(
            h[0].contains(
                "a union of Order, Refund, Customer: select fields inside `... on Order { }`"
            ),
            "{}",
            h[0]
        );
        // Several errors at once each get a line.
        let both = graphql_hints(
            &api(),
            r#"Cannot query field "x" on type "Order". | Unknown type "Refnd"."#,
            &[],
        );
        assert_eq!(both.len(), 2);
        // A type that isn't described says so, with what is close.
        let h = graphql_hints(&api(), r#"Cannot query field "a" on type "Ordr"."#, &[]);
        assert!(
            h[0].contains("isn't in this source's saved description. Closest: Order"),
            "{}",
            h[0]
        );
        // Errors that aren't about names give nothing.
        assert!(graphql_hints(&api(), "Not authorised", &[]).is_empty());
    }

    #[test]
    fn an_unknown_table_or_column_gets_the_real_names() {
        let h = sql_hints(
            &db(),
            "ERROR: relation \"order\" does not exist\n  Position: 15",
            "SELECT id FROM \"order\"",
            &[],
        );
        assert_eq!(
            h,
            vec!["No table is called order. Closest real tables: orders, order_items."]
        );
        let h = sql_hints(
            &db(),
            "relation \"invoice\" does not exist",
            "SELECT 1",
            &[],
        );
        assert_eq!(
            h,
            vec!["No table is called invoice. Closest real tables: billing.invoices."]
        );
        // A qualified column: the alias says which table to look in.
        let h = sql_hints(
            &db(),
            "ERROR: column o.status does not exist\n  Hint: Perhaps you meant to reference the column \"o.status_code\".",
            "SELECT o.status FROM orders o JOIN order_items i ON i.order_id = o.id",
            &["email".to_string()],
        );
        assert_eq!(h.len(), 1, "{h:?}");
        assert!(
            h[0].starts_with("orders has no column status. Closest: status_code."),
            "{}",
            h[0]
        );
        assert!(
            h[0].contains("Its columns: id, status_code, placed_at."),
            "{}",
            h[0]
        );
        assert!(
            !h[0].contains("contact_email"),
            "hidden columns aren't suggested"
        );
        // Unqualified: every table in the lookup is checked.
        let h = sql_hints(
            &db(),
            "column \"order_type\" does not exist",
            "SELECT order_type FROM billing.invoices",
            &[],
        );
        assert!(
            h[0].starts_with("billing.invoices has no column order_type. Closest: ordertype."),
            "{}",
            h[0]
        );
        // Redshift names the table in the message.
        let h = sql_hints(
            &db(),
            "column \"placed\" does not exist in orders",
            "SELECT 1",
            &[],
        );
        assert!(h[0].contains("Closest: placed_at."), "{}", h[0]);
        // Tables that aren't described give nothing to say.
        assert!(sql_hints(
            &db(),
            "column x does not exist",
            "SELECT x FROM elsewhere",
            &[]
        )
        .is_empty());
        assert!(sql_hints(&db(), "permission denied", "SELECT 1", &[]).is_empty());
    }

    #[test]
    fn name_errors_are_told_apart_from_other_failures() {
        for e in [
            "The database couldn't run that (BadRequestException: ERROR: relation \"x\" does not exist).",
            "ERROR: column t.x does not exist",
            "The API couldn't answer that lookup: Cannot query field \"a\" on type \"B\".",
            "Unknown type \"X\".",
            "Unknown argument \"a\" on field \"Q.f\".",
        ] {
            assert!(is_name_error(e), "{e}");
        }
        for e in [
            "permission denied for table x",
            "timed out",
            "syntax error at or near \"FROM\"",
        ] {
            assert!(!is_name_error(e), "{e}");
        }
    }

    #[test]
    fn describe_shows_a_union_with_its_members_and_marks_hidden_fields() {
        let c = api();
        let d = c
            .describe("searchresult", &["email".to_string()], 10_000)
            .unwrap();
        assert!(
            d.starts_with("union SearchResult = Order | Refund | Customer\n"),
            "{d}"
        );
        assert!(d.contains("`{ ... on Order { id } }`"), "{d}");
        assert!(
            d.contains("type Refund {\n  id: ID!\n  amountCents: Int"),
            "{d}"
        );
        assert!(
            d.contains("customerEmail: String  (hidden: refused)"),
            "{d}"
        );
        assert!(c.describe("Nope", &[], 1000).is_none());
        let small = c.describe("SearchResult", &[], 160).unwrap();
        assert!(small.len() <= 200, "{small}");
    }

    #[test]
    fn list_and_search() {
        let d = db();
        assert_eq!(
            d.list(1000),
            "3 tables:\norders, order_items, billing.invoices"
        );
        assert!(d.list(30).contains("more left out"));
        assert!(api()
            .list(1000)
            .contains("SearchResult (union), Node (interface)"));
        assert!(d
            .search("order_id", &[], 1000)
            .starts_with("table order_items"));
        assert_eq!(
            d.search("ordr", &[], 1000),
            "Nothing here matches \"ordr\". Closest: orders. Look one up by name."
        );
        // A word that is only close to a column points at that column's table.
        assert!(d.search("placedat", &[], 1000).contains("Closest: orders."));
        assert!(d
            .search("zzzzzz", &[], 1000)
            .contains("Leave the search empty"));
    }
    const SHOP: &str = "table public.orders (database dev)\n  id: integer\n  status: text\n  member_id: integer\n\n\
        table public.order_shipment (database dev)\n  id: integer\n  title: text\n\n\
        table public.members (database dev)\n  id: integer\n  name: text\n  contact_email: text\n\n\
        table billing.invoices (database dev)\n  id: integer\n  order_id: integer\n";

    fn shop() -> Catalog {
        Catalog::parse(SHOP)
    }

    fn unknown(sql: &str) -> Vec<String> {
        unknown_sql_names(&shop(), sql, &["email".to_string()])
    }

    #[test]
    fn an_unknown_table_is_caught_before_it_is_sent() {
        let u = unknown("select c.id from shipments c where c.id = 1");
        assert_eq!(u.len(), 1, "{u:?}");
        assert!(
            u[0].starts_with("No table named shipments. Closest: "),
            "{}",
            u[0]
        );
        assert!(u[0].contains("public.order_shipment"), "{}", u[0]);
        let msg = unknown_names_message("Production", &u);
        assert!(msg.starts_with("Not sent to Production"), "{msg}");
        assert!(msg.contains("live_describe"), "{msg}");
        // Known tables pass, with or without their schema.
        for ok in [
            "select id, status from orders",
            "select o.id from public.orders o",
            "select id from dev.public.orders",
            "select i.id from billing.invoices i join orders o on o.id = i.order_id",
            "select table_name from information_schema.columns",
            "select * from pg_catalog.pg_tables",
            "select count(*) from svv_table_info",
        ] {
            assert!(unknown(ok).is_empty(), "{ok}: {:?}", unknown(ok));
        }
    }

    #[test]
    fn a_schema_must_match_when_one_is_named() {
        let u = unknown("select id from billing.orders");
        assert_eq!(u.len(), 1, "{u:?}");
        assert!(
            u[0].starts_with("No table named billing.orders."),
            "{}",
            u[0]
        );
        // Unqualified, any schema's table of that name will do.
        assert!(unknown("select id from invoices").is_empty());
    }

    #[test]
    fn columns_are_checked_through_aliases() {
        let u = unknown("select o.id, o.state from orders o join members m on m.id = o.member_id");
        assert_eq!(u.len(), 1, "{u:?}");
        assert!(
            u[0].starts_with("public.orders has no column state. Closest: status."),
            "{}",
            u[0]
        );
        // The table's own name works as a qualifier too.
        assert_eq!(unknown("select orders.nope from orders").len(), 1);
        // A bare column in a one-table lookup is checked...
        let u = unknown("select id, titel from order_shipment where id > 3");
        assert_eq!(u.len(), 1, "{u:?}");
        assert!(u[0].contains("Closest: title."), "{}", u[0]);
        // ...but not output names, functions or a database's own words.
        assert!(unknown(
            "select id as n, title from order_shipment where current_date > sysdate order by n"
        )
        .is_empty());
        // A date part is a keyword, not a column; the date's own column still is.
        assert!(unknown(
            "select id, count(*) from order_shipment where id > datediff(day, getdate(), getdate()) group by 1"
        )
        .is_empty());
        let u =
            unknown("select id from order_shipment where dateadd(month, -1, titel) > getdate()");
        assert_eq!(u.len(), 1, "{u:?}");
        assert!(u[0].contains("no column titel"), "{}", u[0]);
        // In a join, a bare column could be either table's: not checked.
        assert!(
            unknown("select title, nope from order_shipment c join orders o on o.id = c.id")
                .is_empty()
        );
        // Hidden columns are never offered as the fix.
        let u = unknown("select m.contact from members m");
        assert!(!u[0].contains("contact_email"), "{}", u[0]);
    }

    #[test]
    fn ctes_and_subqueries_are_not_tables() {
        for ok in [
            "with recent as (select id, status from orders) select r.id, r.whatever from recent r",
            "with recent as (select id from orders) select id from recent",
            "select x.n from (select count(*) as n from orders) x",
            "select s.id from (select id from orders) as s where s.id in (select order_id from billing.invoices)",
            "select g from generate_series(1, 3) as g",
            "select o.id from orders o where exists (select 1 from members m where m.id = o.member_id)",
        ] {
            assert!(unknown(ok).is_empty(), "{ok}: {:?}", unknown(ok));
        }
        // A real unknown table inside a CTE or subquery is still caught.
        assert_eq!(
            unknown("with r as (select id from shipments) select id from r").len(),
            1
        );
        assert_eq!(
            unknown("select id from orders where id in (select id from shipments)").len(),
            1
        );
    }

    #[test]
    fn a_cut_short_description_settles_nothing() {
        let mut text = SHOP.to_string();
        text.push_str(&format!("\n{INCOMPLETE_MARK}\n"));
        let c = Catalog::parse(&text);
        assert!(c.incomplete);
        assert!(unknown_sql_names(&c, "select id from shipments", &[]).is_empty());
        // A description saved before the mark, at the read limit, is cut short too.
        let mut big = String::from("table public.wide (database dev)\n");
        for i in 0..WAREHOUSE_COLUMNS_READ {
            big.push_str(&format!("  c{i}: text\n"));
        }
        assert!(Catalog::parse(&big).incomplete);
        assert!(!shop().incomplete);
        // An API description has no tables to check against.
        assert!(unknown_sql_names(&api(), "select 1 from x", &[]).is_empty());
    }

    #[test]
    fn graphql_names_are_checked_before_sending() {
        let c = api();
        assert!(unknown_graphql_names(&c, "{ orders(first: 2) { id placedAt } }", &[]).is_empty());
        assert!(unknown_graphql_names(
            &c,
            "query { search(term: \"a\") { __typename ... on Order { id } ... on Refund { amountCents } } }",
            &[]
        )
        .is_empty());
        let u = unknown_graphql_names(&c, "{ orders { id total } }", &[]);
        assert_eq!(u.len(), 1, "{u:?}");
        assert!(u[0].starts_with("Order has no field total."), "{}", u[0]);
        let u = unknown_graphql_names(&c, "{ orders(limit: 2) { id } }", &[]);
        assert_eq!(
            u,
            vec!["orders takes no argument limit. Its arguments: first, status."]
        );
        let u = unknown_graphql_names(&c, "{ search(term: \"a\") { placedAt } }", &[]);
        assert!(u[0].contains("SearchResult is a union"), "{}", u[0]);
        let u = unknown_graphql_names(&c, "{ search(term: \"a\") { ... on Refnd { id } } }", &[]);
        assert!(
            u[0].starts_with("No type named Refnd. Closest: Refund"),
            "{}",
            u[0]
        );
        let u = unknown_graphql_names(&c, "{ order { id } }", &[]);
        assert!(
            u[0].starts_with("Query has no field order. Closest: orders."),
            "{}",
            u[0]
        );
        // Fragments are followed.
        let u = unknown_graphql_names(
            &c,
            "query { orders { ...F } } fragment F on Order { id nope }",
            &[],
        );
        assert!(u[0].starts_with("Order has no field nope."), "{}", u[0]);
    }

    #[test]
    fn hidden_columns_are_marked_per_table() {
        let c = shop();
        let hidden = vec!["public.members.name".to_string()];
        let m = c.describe("members", &hidden, 10_000).unwrap();
        assert!(m.contains("name: text  (hidden: refused)"), "{m}");
        let o = c
            .describe("order_shipment", &["order_shipment.title".into()], 10_000)
            .unwrap();
        assert!(o.contains("title: text  (hidden: refused)"), "{o}");
        let o = c.describe("order_shipment", &hidden, 10_000).unwrap();
        assert!(!o.contains("hidden"), "{o}");
    }
    #[test]
    fn a_warehouse_is_checked_one_database_at_a_time() {
        let mut text =
            String::from("table public.course (database sales)\n  id: integer\n  name: text\n\n");
        text.push_str("table public.wide (database big)\n  c: text\n\n");
        text.push_str(INCOMPLETE_MARK);
        text.push('\n');
        let c = Catalog::parse(&text);
        assert!(c.incomplete);
        assert_eq!(c.cut.iter().collect::<Vec<_>>(), vec!["big"]);
        // The whole warehouse can't settle a name; one complete database can.
        assert!(unknown_sql_names(&c, "select id from shipments", &[]).is_empty());
        assert_eq!(
            unknown_sql_names_in(&c, Some("sales"), "select id from shipments", &[]).len(),
            1
        );
        assert!(
            unknown_sql_names_in(&c, Some("sales"), "select id, name from public.course", &[])
                .is_empty()
        );
        assert!(unknown_sql_names_in(&c, Some("big"), "select id from shipments", &[]).is_empty());
        // A database with nothing described settles nothing either.
        assert!(
            unknown_sql_names_in(&c, Some("other"), "select id from shipments", &[]).is_empty()
        );
    }
}
