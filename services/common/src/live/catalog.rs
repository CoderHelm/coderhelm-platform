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
use super::redact::{hidden_terms, is_hidden};
use super::sql::table_aliases;

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
}

impl Entry {
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
}

fn unquote(s: &str) -> String {
    s.trim().trim_matches('"').to_string()
}

impl Catalog {
    /// Parses the text built by `graphql::schema_from_introspection`,
    /// `db::schema_text` or `redshift::schema_text`. Blocks it doesn't
    /// recognise are skipped.
    pub fn parse(text: &str) -> Catalog {
        let mut entries = Vec::new();
        for block in text.split("\n\n") {
            let block = block.trim_matches('\n');
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
            if !name.is_empty() {
                entries.push(Entry {
                    kind,
                    name,
                    fields,
                    members,
                    block: block.to_string(),
                });
            }
        }
        Catalog { entries }
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
        let terms = hidden_terms(hidden);
        let mut out = mark_hidden(&e.block, &terms);
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
            let b = mark_hidden(&member.block, &terms);
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
        let terms = hidden_terms(hidden);
        let mut out = String::new();
        for e in &self.entries {
            if !e.block.to_lowercase().contains(&term) {
                continue;
            }
            let b = mark_hidden(&e.block, &terms);
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
        best = best.min(d);
    }
    best
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
    let terms = hidden_terms(hidden);
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
}
