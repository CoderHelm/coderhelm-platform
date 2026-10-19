//! Suggested hidden fields: columns a source's tables likely use for a
//! person's details (a member's name, a contact's phone), worked out by a
//! model from table and column names only, never from data.
//!
//! A suggestion is only shown to an admin, who accepts or dismisses it.
//! Nothing here hides or un-hides a column by itself.

use std::collections::HashSet;

use serde_json::{json, Value};

use super::catalog::{Catalog, EntryKind};
use super::redact::{is_hidden, scoped};

/// Most characters of table and column names sent in one call.
const MAX_LISTING_CHARS: usize = 60_000;
/// Most suggestions kept from one answer.
const MAX_SUGGESTIONS: usize = 200;

pub const SYSTEM: &str = "You help an admin decide which database columns hold personal \
     details about people, so an AI assistant reading the database never sees them. You get \
     only table and column names, never data. Name the columns that hold a specific person's \
     name, contact details (email, phone, street address), date of birth, government or tax \
     id, bank or card details, or similar details that identify one person. Look at what the \
     TABLE describes: in a table of people (members, customers, contacts, participants, \
     guests, users, employees, staff, instructors, patients, students, leads, accounts that \
     are people), a plain `name`, `full_name`, `first_name`, `last_name` or `display_name` \
     column is a person's name. In a table of things (shipments, courses, products, \
     locations, programs, plans, rooms, companies, teams, events), `name` or `title` is the \
     thing's name and is NOT personal. Columns that only point at a person (person_id, \
     member_id) are not personal. When unsure whether a table describes people, leave it \
     out. The reason is one short sentence an admin reads.";

/// The answer's shape.
pub fn output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "table": { "type": "string" },
                        "column": { "type": "string" },
                        "reason": { "type": "string" }
                    },
                    "required": ["table", "column", "reason"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["columns"],
        "additionalProperties": false
    })
}

/// A table's name with its schema, as entries and suggestions spell it:
/// `public.orders` (a PostgreSQL table described without one is in public).
fn qualified(e: &super::catalog::Entry) -> String {
    let r = e.table_ref();
    match r.schema {
        Some(s) => format!("{s}.{}", r.table),
        None => r.table,
    }
}

/// The tables and their columns, by name only, one line per table. Tables
/// that appear in several databases are listed once.
pub fn user_message(catalog: &Catalog) -> String {
    let mut out = String::from("Tables and their columns:\n");
    let mut seen = HashSet::new();
    for e in catalog
        .entries
        .iter()
        .filter(|e| e.kind == EntryKind::Table)
    {
        let name = qualified(e);
        if !seen.insert(name.clone()) {
            continue;
        }
        let line = format!(
            "{name}: {}\n",
            e.fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        if out.len() + line.len() > MAX_LISTING_CHARS {
            out.push_str("(more tables left out)\n");
            break;
        }
        out.push_str(&line);
    }
    out
}

/// A key for the names a description holds: suggestions are worked out again
/// only when a table or column is added, renamed or removed.
pub fn catalog_key(catalog: &Catalog) -> String {
    use std::hash::{Hash, Hasher};
    let mut names: Vec<String> = catalog
        .entries
        .iter()
        .filter(|e| e.kind == EntryKind::Table)
        .flat_map(|e| {
            let t = qualified(e);
            e.fields.iter().map(move |f| format!("{t}.{}", f.name))
        })
        .collect();
    names.sort();
    names.dedup();
    let mut h = std::collections::hash_map::DefaultHasher::new();
    names.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// One suggested entry for the hidden list.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Suggestion {
    /// `schema.table.column`, the form the hidden list takes.
    pub entry: String,
    pub reason: String,
}

/// The answer's columns that really exist, aren't hidden already and weren't
/// dismissed before, as entries for the hidden list.
pub fn suggestions(
    answer: &Value,
    catalog: &Catalog,
    hidden: &[String],
    dismissed: &[String],
) -> Vec<Suggestion> {
    let dismissed: HashSet<String> = dismissed.iter().map(|d| d.to_lowercase()).collect();
    let mut out: Vec<Suggestion> = Vec::new();
    let mut seen = HashSet::new();
    for c in answer["columns"].as_array().into_iter().flatten() {
        let (Some(table), Some(column)) = (c["table"].as_str(), c["column"].as_str()) else {
            continue;
        };
        let Some(e) = catalog.find(table).filter(|e| e.kind == EntryKind::Table) else {
            continue;
        };
        let Some(f) = e
            .fields
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(column))
        else {
            continue;
        };
        if is_hidden(&f.name, &e.hidden_terms(hidden)) {
            continue;
        }
        let entry = format!("{}.{}", qualified(e), f.name.to_lowercase());
        if scoped(&entry).is_none() || dismissed.contains(&entry) || !seen.insert(entry.clone()) {
            continue;
        }
        out.push(Suggestion {
            entry,
            reason: crate::truncate_str(c["reason"].as_str().unwrap_or("").trim(), 200).to_string(),
        });
        if out.len() >= MAX_SUGGESTIONS {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: &str = "table member\n  id: integer\n  name: text\n  phone_number: text\n\n\
        table course\n  id: integer\n  name: text\n\n\
        table crm.contact\n  id: integer\n  display_name: text\n";

    #[test]
    fn only_names_are_sent() {
        let c = Catalog::parse(DB);
        let m = user_message(&c);
        assert_eq!(
            m,
            "Tables and their columns:\npublic.member: id, name, phone_number\npublic.course: id, name\ncrm.contact: id, display_name\n"
        );
        assert_eq!(catalog_key(&c), catalog_key(&Catalog::parse(DB)));
        assert_ne!(
            catalog_key(&c),
            catalog_key(&Catalog::parse("table member\n  id: integer\n"))
        );
    }

    #[test]
    fn suggestions_are_real_new_and_not_dismissed() {
        let c = Catalog::parse(DB);
        let answer = json!({"columns": [
            {"table": "member", "column": "name", "reason": "A member is a person."},
            {"table": "public.member", "column": "phone_number", "reason": "Phone."},
            {"table": "crm.contact", "column": "display_name", "reason": "A contact's name."},
            {"table": "member", "column": "made_up", "reason": "x"},
            {"table": "nowhere", "column": "name", "reason": "x"},
            {"table": "member", "column": "NAME", "reason": "again"}
        ]});
        let got = suggestions(
            &answer,
            &c,
            &["phone".to_string()],
            &["crm.contact.display_name".to_string()],
        );
        assert_eq!(
            got,
            vec![Suggestion {
                entry: "public.member.name".into(),
                reason: "A member is a person.".into()
            }]
        );
        // Once accepted, it isn't suggested again.
        assert!(suggestions(
            &answer,
            &c,
            &[
                "public.member.name".into(),
                "phone".into(),
                "crm.contact.display_name".into()
            ],
            &[]
        )
        .is_empty());
    }
}
