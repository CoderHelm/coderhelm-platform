//! The read-only rule for GraphQL, and helpers that turn an API's own
//! description of itself into something a person or a model can browse.

use std::collections::HashMap;

use graphql_parser::query::{
    parse_query, Definition, FragmentDefinition, OperationDefinition, Selection, SelectionSet,
};
use serde_json::Value;

use super::StepError;

const MAX_QUERY_BYTES: usize = 16_000;
const MAX_DEPTH: usize = 12;
const MAX_FIELDS: usize = 400;

/// A request that passed the read-only rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadQuery {
    /// The exact text that was checked. Send this and nothing else.
    pub text: String,
    /// The top-level fields it reads, for the activity log.
    pub fields: Vec<String>,
}

fn refuse(message: &str) -> StepError {
    StepError::new(
        "read_only",
        message,
        "Only lookups that read data are allowed. Nothing was sent.",
    )
}

/// Accepts a request only when it parses as exactly one GraphQL **query**.
///
/// Refused: anything that fails to parse, mutations, subscriptions, and
/// documents holding more than one operation (so an operation name can never
/// select a different one than the one that was checked). Size, nesting depth
/// and field count are capped so one lookup cannot ask for a whole dataset.
pub fn check_read_only(text: &str) -> Result<ReadQuery, StepError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(refuse("The lookup is empty."));
    }
    if text.len() > MAX_QUERY_BYTES {
        return Err(refuse("The lookup is too long."));
    }
    let doc = parse_query::<String>(text)
        .map_err(|_| refuse("The lookup isn't a valid GraphQL query."))?;

    let mut fragments: HashMap<&str, &FragmentDefinition<String>> = HashMap::new();
    let mut operations: Vec<&SelectionSet<String>> = Vec::new();
    for def in &doc.definitions {
        match def {
            Definition::Fragment(f) => {
                fragments.insert(f.name.as_str(), f);
            }
            Definition::Operation(OperationDefinition::SelectionSet(s)) => operations.push(s),
            Definition::Operation(OperationDefinition::Query(q)) => {
                operations.push(&q.selection_set)
            }
            Definition::Operation(OperationDefinition::Mutation(_)) => {
                return Err(refuse("This lookup would change data."));
            }
            Definition::Operation(OperationDefinition::Subscription(_)) => {
                return Err(refuse("Subscriptions aren't lookups."));
            }
        }
    }
    let root = match operations.as_slice() {
        [one] => *one,
        [] => return Err(refuse("The lookup doesn't ask for anything.")),
        _ => return Err(refuse("Send one lookup at a time.")),
    };

    let mut count = 0usize;
    walk(root, &fragments, 1, &mut count)?;

    let mut fields: Vec<String> = Vec::new();
    top_fields(root, &fragments, 0, &mut fields);
    fields.dedup();
    Ok(ReadQuery {
        text: text.to_string(),
        fields,
    })
}

fn walk(
    set: &SelectionSet<String>,
    fragments: &HashMap<&str, &FragmentDefinition<String>>,
    depth: usize,
    count: &mut usize,
) -> Result<(), StepError> {
    if set.items.is_empty() {
        return Ok(());
    }
    if depth > MAX_DEPTH {
        return Err(refuse("The lookup is nested too deeply."));
    }
    for item in &set.items {
        *count += 1;
        if *count > MAX_FIELDS {
            return Err(refuse("The lookup asks for too many fields."));
        }
        match item {
            Selection::Field(f) => walk(&f.selection_set, fragments, depth + 1, count)?,
            Selection::InlineFragment(f) => walk(&f.selection_set, fragments, depth + 1, count)?,
            // Following the spread at depth + 1 means a fragment that refers to
            // itself runs into the depth cap instead of looping forever.
            Selection::FragmentSpread(s) => match fragments.get(s.fragment_name.as_str()) {
                Some(f) => walk(&f.selection_set, fragments, depth + 1, count)?,
                None => return Err(refuse("The lookup refers to a fragment it doesn't define.")),
            },
        }
    }
    Ok(())
}

fn top_fields(
    set: &SelectionSet<String>,
    fragments: &HashMap<&str, &FragmentDefinition<String>>,
    depth: usize,
    out: &mut Vec<String>,
) {
    if depth > MAX_DEPTH {
        return;
    }
    for item in &set.items {
        match item {
            Selection::Field(f) => out.push(f.name.clone()),
            Selection::InlineFragment(f) => top_fields(&f.selection_set, fragments, depth + 1, out),
            Selection::FragmentSpread(s) => {
                if let Some(f) = fragments.get(s.fragment_name.as_str()) {
                    top_fields(&f.selection_set, fragments, depth + 1, out);
                }
            }
        }
    }
}

/// The standard question that asks an API to describe itself. It is itself a
/// read and goes through [`check_read_only`] like any other lookup.
///
/// `wrap_levels` is how far to unwrap list and required markers around a type
/// (`[Order!]!` needs 3). APIs that cap query depth refuse the deep form, so
/// callers start at [`INTROSPECTION_LEVELS`] and step down.
pub fn introspection_query(wrap_levels: usize) -> String {
    let mut type_ref = String::from("kind name");
    for _ in 0..wrap_levels {
        type_ref = format!("kind name ofType {{ {type_ref} }}");
    }
    format!(
        "query {{ __schema {{ queryType {{ name }} mutationType {{ name }} types {{ kind name \
         fields {{ name args {{ name type {{ {type_ref} }} }} type {{ {type_ref} }} }} }} }} }}"
    )
}

/// The deepest form tried first.
pub const INTROSPECTION_LEVELS: usize = 3;

/// What an API said about itself, reduced to what the product needs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    /// Number of things that can be looked up (fields on the query root).
    pub kinds: usize,
    /// The API also defines operations that change data. They can never be
    /// sent from here; this is only so the dashboard can say so.
    pub has_mutations: bool,
    /// One `type Name { field(arg: T): T }` block per type, query root first.
    pub text: String,
    /// Ready-made lookups a person can run to see it working.
    pub samples: Vec<(String, String)>,
}

fn type_ref(t: &Value) -> String {
    match t["kind"].as_str() {
        Some("NON_NULL") => format!("{}!", type_ref(&t["ofType"])),
        Some("LIST") => format!("[{}]", type_ref(&t["ofType"])),
        _ => t["name"].as_str().unwrap_or("?").to_string(),
    }
}

fn named(t: &Value) -> (&str, &str) {
    match t["kind"].as_str() {
        Some("NON_NULL") | Some("LIST") => named(&t["ofType"]),
        kind => (kind.unwrap_or(""), t["name"].as_str().unwrap_or("")),
    }
}

fn humanize(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c == '_' {
            out.push(' ');
        } else if c.is_uppercase() && i > 0 {
            out.push(' ');
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out.to_lowercase()
}

const MAX_SCHEMA_BYTES: usize = 300_000;

/// Builds a [`Schema`] from an introspection response, or `None` when the API
/// did not describe itself (many turn this off in production).
pub fn schema_from_introspection(body: &Value) -> Option<Schema> {
    let s = &body["data"]["__schema"];
    let types = s["types"].as_array()?;
    let query_name = s["queryType"]["name"].as_str()?;
    let has_mutations = s["mutationType"]["name"].as_str().is_some();
    let mutation_name = s["mutationType"]["name"].as_str().unwrap_or("");

    let by_name: HashMap<&str, &Value> = types
        .iter()
        .filter_map(|t| t["name"].as_str().map(|n| (n, t)))
        .collect();

    let block = |t: &Value| -> Option<String> {
        let name = t["name"].as_str()?;
        let fields = t["fields"].as_array()?;
        if name.starts_with("__") || fields.is_empty() {
            return None;
        }
        let mut b = format!("type {name} {{\n");
        for f in fields {
            let args: Vec<String> = f["args"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|x| {
                            format!(
                                "{}: {}",
                                x["name"].as_str().unwrap_or("?"),
                                type_ref(&x["type"])
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            let args = if args.is_empty() {
                String::new()
            } else {
                format!("({})", args.join(", "))
            };
            b.push_str(&format!(
                "  {}{}: {}\n",
                f["name"].as_str().unwrap_or("?"),
                args,
                type_ref(&f["type"])
            ));
        }
        b.push_str("}\n");
        Some(b)
    };

    let query_type = by_name.get(query_name)?;
    let mut text = block(query_type).unwrap_or_default();
    for t in types {
        let name = t["name"].as_str().unwrap_or("");
        // The write side of the API is left out entirely: it can't be used.
        if name == query_name || name == mutation_name {
            continue;
        }
        if let Some(b) = block(t) {
            if text.len() + b.len() > MAX_SCHEMA_BYTES {
                break;
            }
            text.push('\n');
            text.push_str(&b);
        }
    }

    let query_fields = query_type["fields"].as_array().cloned().unwrap_or_default();
    let mut samples = Vec::new();
    for f in &query_fields {
        if samples.len() >= 4 {
            break;
        }
        let Some(name) = f["name"].as_str() else {
            continue;
        };
        let args = f["args"].as_array().cloned().unwrap_or_default();
        // Skip lookups that need an input we can't guess.
        if args.iter().any(|a| a["type"]["kind"] == "NON_NULL") {
            continue;
        }
        let limit = args
            .iter()
            .filter_map(|a| a["name"].as_str())
            .find(|n| matches!(*n, "first" | "limit" | "take"))
            .map(|n| format!("({n}: 3)"))
            .unwrap_or_default();
        let (kind, type_name) = named(&f["type"]);
        let query = match kind {
            "SCALAR" | "ENUM" => format!("{{ {name}{limit} }}"),
            "OBJECT" => {
                let leaves: Vec<&str> = by_name
                    .get(type_name)
                    .and_then(|t| t["fields"].as_array())
                    .map(|fs| {
                        fs.iter()
                            .filter(|x| {
                                matches!(named(&x["type"]).0, "SCALAR" | "ENUM")
                                    && x["args"].as_array().map(|a| a.is_empty()).unwrap_or(true)
                            })
                            .filter_map(|x| x["name"].as_str())
                            .take(5)
                            .collect()
                    })
                    .unwrap_or_default();
                if leaves.is_empty() {
                    continue;
                }
                format!("{{ {name}{limit} {{ {} }} }}", leaves.join(" "))
            }
            _ => continue,
        };
        samples.push((format!("Look up {}", humanize(name)), query));
    }

    Some(Schema {
        kinds: query_fields.len(),
        has_mutations,
        text,
        samples,
    })
}

/// The `type` blocks whose name or fields mention `term`, for browsing a large
/// schema a piece at a time. An empty term returns the first block (what can
/// be looked up).
pub fn search_schema(schema_text: &str, term: &str, max_bytes: usize) -> String {
    let term = term.trim().to_lowercase();
    let mut out = String::new();
    for (i, block) in schema_text.split("\n\n").enumerate() {
        let hit = if term.is_empty() {
            i == 0
        } else {
            block.to_lowercase().contains(&term)
        };
        if !hit {
            continue;
        }
        if out.len() + block.len() > max_bytes {
            out.push_str("\n(more matches left out; search for something narrower)\n");
            break;
        }
        out.push_str(block.trim_end());
        out.push_str("\n\n");
    }
    if out.is_empty() {
        "Nothing in this API matches that.".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn refused(q: &str) -> String {
        let e = check_read_only(q).unwrap_err();
        assert!(e.is_refusal());
        e.message
    }

    #[test]
    fn plain_reads_pass() {
        let q = check_read_only("{ orders(first: 3) { id status } version }").unwrap();
        assert_eq!(q.fields, vec!["orders", "version"]);
        assert!(check_read_only("query Q($id: ID!) { order(id: $id) { id } }").is_ok());
        assert!(check_read_only("query { __schema { queryType { name } } }").is_ok());
        for levels in 0..=INTROSPECTION_LEVELS {
            assert!(check_read_only(&introspection_query(levels)).is_ok());
        }
        assert!(!introspection_query(0).contains("ofType"));
        assert_eq!(introspection_query(2).matches("ofType").count(), 4);
    }

    #[test]
    fn fragments_are_followed() {
        let q = check_read_only(
            "query { ...Top } fragment Top on Query { orders { ...O } } fragment O on Order { id }",
        )
        .unwrap();
        assert_eq!(q.fields, vec!["orders"]);
        assert!(refused("{ ...Missing }").contains("fragment"));
    }

    #[test]
    fn writes_are_refused() {
        assert!(refused("mutation { deleteOrder(id: 1) { id } }").contains("change data"));
        assert!(refused("mutation M { __typename }").contains("change data"));
        assert!(refused("  MUTATION { x }").contains("valid"));
        assert!(refused("subscription { orderChanged { id } }").contains("Subscriptions"));
    }

    #[test]
    fn a_write_cannot_hide_behind_a_read() {
        // Two operations: the server would run whichever `operationName` picks.
        assert!(refused("query A { a } mutation B { b }").contains("change data"));
        assert!(refused("mutation B { b } query A { a }").contains("change data"));
        assert!(refused("query A { a } query B { b }").contains("one lookup"));
        assert!(refused("{ a } { b }").contains("one lookup"));
        // Comments and strings that only mention reads don't make a write a read.
        assert!(refused("# query\nmutation { b }").contains("change data"));
        assert!(refused("mutation { note(text: \"query { a }\") }").contains("change data"));
        // A fragment on the mutation type is harmless without a mutation operation,
        // and useless: the operation that spreads it is still a query.
        assert!(check_read_only("query { a } fragment F on Mutation { b }").is_ok());
    }

    #[test]
    fn junk_is_refused() {
        assert!(refused("").contains("empty"));
        assert!(refused("   ").contains("empty"));
        assert!(refused("DROP TABLE orders;").contains("valid"));
        assert!(refused("fragment F on Order { id }").contains("doesn't ask"));
        assert!(refused("{ a } } mutation { b }").contains("valid"));
    }

    #[test]
    fn size_depth_and_field_caps() {
        assert!(refused(&format!("{{ a {} }}", " ".repeat(MAX_QUERY_BYTES))).contains("too long"));
        let deep = format!(
            "{}{}",
            "{ a ".repeat(MAX_DEPTH + 1),
            "}".repeat(MAX_DEPTH + 1)
        );
        assert!(refused(&deep).contains("nested"));
        let ok = format!("{}{}", "{ a ".repeat(MAX_DEPTH), "}".repeat(MAX_DEPTH));
        assert!(check_read_only(&ok).is_ok());
        let wide = format!("{{ {} }}", "f ".repeat(MAX_FIELDS + 1));
        assert!(refused(&wide).contains("too many"));
        // A fragment that spreads itself stops at the depth cap.
        assert!(refused("{ ...F } fragment F on Q { a { ...F } }").contains("nested"));
    }

    fn introspection() -> Value {
        let scalar = |n: &str| json!({ "kind": "SCALAR", "name": n });
        let nn = |t: Value| json!({ "kind": "NON_NULL", "name": null, "ofType": t });
        let list = |t: Value| json!({ "kind": "LIST", "name": null, "ofType": t });
        let obj = |n: &str| json!({ "kind": "OBJECT", "name": n });
        json!({ "data": { "__schema": {
            "queryType": { "name": "Query" },
            "mutationType": { "name": "Mutation" },
            "types": [
                { "kind": "OBJECT", "name": "Query", "fields": [
                    { "name": "orders", "args": [{ "name": "first", "type": scalar("Int") }], "type": nn(list(nn(obj("Order")))) },
                    { "name": "order", "args": [{ "name": "id", "type": nn(scalar("ID")) }], "type": obj("Order") },
                    { "name": "apiVersion", "args": [], "type": nn(scalar("String")) }
                ] },
                { "kind": "OBJECT", "name": "Order", "fields": [
                    { "name": "id", "args": [], "type": nn(scalar("ID")) },
                    { "name": "refundedAt", "args": [], "type": scalar("DateTime") },
                    { "name": "customer", "args": [], "type": obj("Customer") }
                ] },
                { "kind": "OBJECT", "name": "Customer", "fields": [
                    { "name": "id", "args": [], "type": nn(scalar("ID")) }
                ] },
                { "kind": "OBJECT", "name": "Mutation", "fields": [
                    { "name": "deleteOrder", "args": [], "type": scalar("Boolean") }
                ] },
                { "kind": "OBJECT", "name": "__Type", "fields": [{ "name": "name", "args": [], "type": scalar("String") }] },
                { "kind": "SCALAR", "name": "ID", "fields": null }
            ]
        } } })
    }

    #[test]
    fn schema_summary_leaves_out_the_write_side() {
        let s = schema_from_introspection(&introspection()).unwrap();
        assert_eq!(s.kinds, 3);
        assert!(s.has_mutations);
        assert!(s.text.starts_with(
            "type Query {\n  orders(first: Int): [Order!]!\n  order(id: ID!): Order\n"
        ));
        assert!(s
            .text
            .contains("type Order {\n  id: ID!\n  refundedAt: DateTime\n"));
        assert!(!s.text.contains("deleteOrder"));
        assert!(!s.text.contains("__Type"));
    }

    #[test]
    fn samples_are_runnable_reads() {
        let s = schema_from_introspection(&introspection()).unwrap();
        assert_eq!(
            s.samples,
            vec![
                (
                    "Look up orders".to_string(),
                    "{ orders(first: 3) { id refundedAt } }".to_string()
                ),
                (
                    "Look up api version".to_string(),
                    "{ apiVersion }".to_string()
                ),
            ]
        );
        for (_, q) in &s.samples {
            assert!(check_read_only(q).is_ok());
        }
    }

    #[test]
    fn no_schema_when_the_api_keeps_it_private() {
        assert!(
            schema_from_introspection(&json!({ "errors": [{ "message": "disabled" }] })).is_none()
        );
        assert!(schema_from_introspection(&json!({ "data": null })).is_none());
    }

    #[test]
    fn schema_search() {
        let s = schema_from_introspection(&introspection()).unwrap();
        assert!(search_schema(&s.text, "", 10_000).starts_with("type Query"));
        let hit = search_schema(&s.text, "refundedat", 10_000);
        assert!(hit.contains("type Order") && !hit.contains("type Customer {"));
        assert_eq!(
            search_schema(&s.text, "zzz", 10_000),
            "Nothing in this API matches that."
        );
        assert!(search_schema(&s.text, "id", 60).contains("more matches left out"));
    }
}
