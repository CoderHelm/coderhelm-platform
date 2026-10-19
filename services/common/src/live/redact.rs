//! Hides the values of sensitive-looking fields before a lookup result is
//! shown to a person or handed to a model.

use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

/// Field names hidden unless a team removes them. Matching is by substring on
/// the lowercased name with punctuation removed, so `customerEmail` and
/// `billing_address` are both covered.
pub const DEFAULT_HIDDEN: &[&str] = &[
    "password", "secret", "token", "api_key", "ssn", "card", "cvv", "email", "phone", "address",
    "birthday",
];

pub const HIDDEN_MARK: &str = "[hidden]";

fn norm(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// A hidden entry that names one table's column (`table.column` or
/// `schema.table.column`), or for an API one type's field (`Type.field`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scoped {
    /// Lowercase, without quotes; `None` when the entry names no schema.
    pub schema: Option<String>,
    pub table: String,
    /// Normalized like any other term.
    pub column: String,
}

fn ident_part(p: &str) -> Option<String> {
    let p = p.trim().trim_matches('"').trim();
    let ok = !p.is_empty()
        && p.chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '$' | '-' | ' '));
    ok.then(|| p.to_lowercase())
}

/// The table-scoped form of an entry, or `None` for a global term. An entry
/// with a dot in it is scoped: `course.title`, `public.person.name` (a
/// leading `=` is allowed and means the same).
pub fn scoped(entry: &str) -> Option<Scoped> {
    let e = entry.trim();
    let e = e.strip_prefix('=').unwrap_or(e);
    if !e.contains('.') {
        return None;
    }
    let parts: Vec<Option<String>> = e.split('.').map(ident_part).collect();
    let column = |c: &Option<String>| c.as_deref().map(norm).filter(|c| c.len() >= 2);
    match parts.as_slice() {
        [Some(t), c] => Some(Scoped {
            schema: None,
            table: t.clone(),
            column: column(c)?,
        }),
        [Some(s), Some(t), c] => Some(Scoped {
            schema: Some(s.clone()),
            table: t.clone(),
            column: column(c)?,
        }),
        _ => None,
    }
}

/// Why a hidden entry can't be saved, or `None` when it is fine. Global
/// terms need two letters or digits; scoped ones need two or three
/// non-empty dotted parts.
pub fn entry_problem(entry: &str) -> Option<String> {
    let e = entry.trim();
    if e.is_empty() {
        return Some("An entry is empty.".into());
    }
    if e.len() > 200 {
        return Some(format!("\"{}…\" is too long.", crate::truncate_str(e, 40)));
    }
    let bare = e.strip_prefix('=').unwrap_or(e);
    if bare.contains('.') {
        let parts = bare.split('.').count();
        if parts > 3 || scoped(e).is_none() {
            return Some(format!(
                "\"{e}\" isn't a column name. Use table.column or schema.table.column."
            ));
        }
        return None;
    }
    if norm(bare).len() < 2 {
        return Some(format!(
            "\"{e}\" is too short: it needs at least two letters or digits."
        ));
    }
    None
}

/// A table a statement reads, or a table or type in a catalog: lowercase,
/// without quotes. `schema` is `None` when the reference doesn't say (an
/// unqualified name, which the database finds through its search path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub schema: Option<String>,
    pub table: String,
}

impl TableRef {
    /// From a dotted name: `orders`, `public.orders`, `dev.public.orders`
    /// (the database part is ignored).
    pub fn parse(name: &str) -> TableRef {
        let parts: Vec<String> = name
            .split('.')
            .map(|p| p.trim().trim_matches('"').to_lowercase())
            .collect();
        match parts.as_slice() {
            [.., s, t] => TableRef {
                schema: Some(s.clone()),
                table: t.clone(),
            },
            [t] => TableRef {
                schema: None,
                table: t.clone(),
            },
            [] => TableRef {
                schema: None,
                table: String::new(),
            },
        }
    }
}

impl Scoped {
    /// Whether this entry is about `t`. An unqualified reference could be
    /// any schema's table, so it matches whatever schema the entry names.
    pub fn applies_to(&self, t: &TableRef) -> bool {
        if !self.table.eq_ignore_ascii_case(&t.table) {
            return false;
        }
        match (&self.schema, &t.schema) {
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
            _ => true,
        }
    }
}

/// The normalized terms to match field names against, when the table a
/// field comes from isn't known (API responses, logs, free text).
/// An entry starting with `=` matches only a field with exactly that name
/// (`=ip` hides `ip` and `IP`, not `membership`); any other entry matches
/// every field whose name contains it. A scoped entry (`table.column`) hides
/// that exact column name everywhere here, since nothing says which table a
/// field came from: hiding too much is safe, hiding too little is not.
pub fn hidden_terms(hidden: &[String]) -> Vec<String> {
    terms_where(hidden, |_| true)
}

/// The terms for a lookup that reads `tables`: every global term, and each
/// scoped entry whose table is one of them (as an exact column name). Used
/// wherever the tables are known (a checked SQL statement, a described
/// table), so `person.name` hides `name` only in lookups that read `person`.
pub fn hidden_terms_for(hidden: &[String], tables: &[TableRef]) -> Vec<String> {
    terms_where(hidden, |s| tables.iter().any(|t| s.applies_to(t)))
}

fn terms_where(hidden: &[String], keep: impl Fn(&Scoped) -> bool) -> Vec<String> {
    let mut terms: Vec<String> = hidden
        .iter()
        .filter_map(|h| {
            let h = h.trim();
            if let Some(s) = scoped(h) {
                return keep(&s).then(|| format!("={}", s.column));
            }
            if h.strip_prefix('=').unwrap_or(h).contains('.') {
                // A malformed scoped entry hides nothing rather than guessing.
                return None;
            }
            match h.strip_prefix('=') {
                // Exact names may be short; `=` keeps them from matching inside others.
                Some(exact) => Some(format!("={}", norm(exact))).filter(|t| t.len() >= 2),
                None => Some(norm(h)).filter(|t| t.len() >= 2),
            }
        })
        .collect();
    terms.sort();
    terms.dedup();
    terms
}

pub fn is_hidden(field: &str, terms: &[String]) -> bool {
    let f = norm(field);
    terms.iter().any(|t| match t.strip_prefix('=') {
        Some(exact) => f == exact,
        None => f.contains(t.as_str()),
    })
}

/// Replaces the value of every hidden field, at any depth, and returns how
/// many values were replaced. A hidden field's whole subtree is replaced.
pub fn redact(value: &mut Value, terms: &[String]) -> usize {
    match value {
        Value::Object(map) => {
            let mut n = 0;
            for (k, v) in map.iter_mut() {
                if is_hidden(k, terms) {
                    if !v.is_null() {
                        *v = Value::String(HIDDEN_MARK.to_string());
                        n += 1;
                    }
                } else {
                    n += redact(v, terms);
                }
            }
            n
        }
        Value::Array(items) => items.iter_mut().map(|v| redact(v, terms)).sum(),
        _ => 0,
    }
}

/// Values that are sensitive wherever they appear in free text (a log line, a
/// text column), whatever the field is called.
static TEXT_PATTERNS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    [
        // key=value and "key": "value" pairs whose key names a secret
        (r#"(?i)((?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|authorization|cookie|session)["']?\s*[:=]\s*["']?)(?:bearer\s+|basic\s+)?[^\s"',;&}]+"#, "$1[hidden]"),
        (r"(?i)\b(bearer|basic)\s+[a-z0-9._~+/=-]{8,}", "$1 [hidden]"),
        // JSON web tokens
        (r"\beyJ[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}", "[hidden]"),
        // AWS access key ids
        (r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b", "[hidden]"),
        (r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b", "[hidden email]"),
    ]
    .into_iter()
    .map(|(p, r)| (Regex::new(p).expect("valid pattern"), r))
    .collect()
});

static CARD_LIKE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d(?:[ -]?\d){12,18}\b").expect("valid pattern"));

fn luhn(digits: &[u32]) -> bool {
    let sum: u32 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &d)| {
            if i % 2 == 1 {
                let x = d * 2;
                if x > 9 {
                    x - 9
                } else {
                    x
                }
            } else {
                d
            }
        })
        .sum();
    sum.is_multiple_of(10)
}

/// Replaces secrets, email addresses and card numbers inside free text.
/// Returns the cleaned text and how many values were replaced.
pub fn redact_text(text: &str) -> (String, usize) {
    let mut out = text.to_string();
    let mut n = 0;
    for (re, with) in TEXT_PATTERNS.iter() {
        let count = re.find_iter(&out).count();
        if count > 0 {
            n += count;
            out = re.replace_all(&out, *with).into_owned();
        }
    }
    let mut cards = 0;
    out = CARD_LIKE
        .replace_all(&out, |c: &regex::Captures| {
            let digits: Vec<u32> = c[0].chars().filter_map(|ch| ch.to_digit(10)).collect();
            if luhn(&digits) {
                cards += 1;
                "[hidden card]".to_string()
            } else {
                c[0].to_string()
            }
        })
        .into_owned();
    (out, n + cards)
}

/// [`redact`] by field name, then [`redact_text`] on every string left.
pub fn redact_all(value: &mut Value, terms: &[String]) -> usize {
    let mut n = redact(value, terms);
    walk_strings(value, &mut |s| {
        let (clean, k) = redact_text(s);
        if k > 0 {
            *s = clean;
            n += k;
        }
    });
    n
}

fn walk_strings(value: &mut Value, f: &mut impl FnMut(&mut String)) {
    match value {
        Value::String(s) if s != HIDDEN_MARK => f(s),
        Value::Array(items) => items.iter_mut().for_each(|v| walk_strings(v, f)),
        Value::Object(map) => map.values_mut().for_each(|v| walk_strings(v, f)),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn defaults() -> Vec<String> {
        hidden_terms(
            &DEFAULT_HIDDEN
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn hides_sensitive_fields_at_any_depth() {
        let mut v = json!({
            "data": { "people": [
                { "id": 7, "firstName": "A", "Email": "a@example.com", "billing_address": { "city": "X" } },
                { "id": 8, "phoneNumber": null, "apiKey": "k", "cardLast4": "4242" }
            ] }
        });
        let n = redact(&mut v, &defaults());
        assert_eq!(n, 4);
        let p = &v["data"]["people"];
        assert_eq!(p[0]["id"], 7);
        assert_eq!(p[0]["firstName"], "A");
        assert_eq!(p[0]["Email"], HIDDEN_MARK);
        assert_eq!(p[0]["billing_address"], HIDDEN_MARK);
        assert!(p[1]["phoneNumber"].is_null());
        assert_eq!(p[1]["apiKey"], HIDDEN_MARK);
        assert_eq!(p[1]["cardLast4"], HIDDEN_MARK);
    }

    #[test]
    fn exact_names_match_only_themselves() {
        let terms = hidden_terms(&[
            "=ip".to_string(),
            "=age".to_string(),
            "=name".to_string(),
            "salary".to_string(),
        ]);
        for hidden in ["ip", "IP", "age", "Name", "base_salary"] {
            assert!(is_hidden(hidden, &terms), "{hidden}");
        }
        for visible in [
            "membership",
            "participant",
            "page",
            "percentage",
            "stage",
            "class_name",
            "zip_code",
        ] {
            assert!(!is_hidden(visible, &terms), "{visible}");
        }
        let mut v = json!({"name": "A", "class_name": "Yoga", "ip": "1.2.3.4"});
        assert_eq!(redact(&mut v, &terms), 2);
        assert_eq!(v["class_name"], "Yoga");
    }

    #[test]
    fn custom_terms_and_empty_list() {
        let mut v = json!({ "salary": 10, "name": "n" });
        assert_eq!(redact(&mut v, &hidden_terms(&["Salary".to_string()])), 1);
        assert_eq!(v["salary"], HIDDEN_MARK);
        let mut v = json!({ "email": "a@example.com" });
        assert_eq!(redact(&mut v, &[]), 0);
        assert_eq!(v["email"], "a@example.com");
        // one-letter terms would hide almost everything, so they are ignored
        assert!(hidden_terms(&["e".to_string()]).is_empty());
    }

    #[test]
    fn secrets_in_free_text_are_hidden() {
        let line = r#"POST /login user=a.b@example.com password=hunter2 Authorization: Bearer abcdefghijklmnop token":"xyz123" card 4242 4242 4242 4242 order 1234567890123 eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U AKIAABCDEFGHIJKLMNOP"#;
        let (clean, n) = redact_text(line);
        for gone in [
            "a.b@example.com",
            "hunter2",
            "abcdefghijklmnop",
            "xyz123",
            "4242 4242 4242 4242",
            "eyJhbGci",
            "AKIAABCDEFGHIJKLMNOP",
        ] {
            assert!(!clean.contains(gone), "{gone} survived: {clean}");
        }
        // an order number that fails the card check stays readable
        assert!(clean.contains("1234567890123"));
        assert!(clean.contains("POST /login"));
        assert!(n >= 7);
    }

    #[test]
    fn redact_all_cleans_names_and_text() {
        let mut v = json!({"rows": [{"id": 1, "email": "a@example.com", "note": "reach me at b@example.com"}]});
        let n = redact_all(&mut v, &defaults());
        assert_eq!(n, 2);
        assert_eq!(v["rows"][0]["email"], HIDDEN_MARK);
        assert_eq!(v["rows"][0]["note"], "reach me at [hidden email]");
        assert_eq!(v["rows"][0]["id"], 1);
    }
    #[test]
    fn scoped_entries_hide_one_tables_column() {
        let hidden = vec![
            "email".to_string(),
            "person.name".to_string(),
            "crm.contact.display_name".to_string(),
        ];
        let person = TableRef::parse("public.person");
        let course = TableRef::parse("public.course");
        let t = hidden_terms_for(&hidden, std::slice::from_ref(&course));
        assert!(!is_hidden("name", &t));
        assert!(is_hidden("contact_email", &t));
        let t = hidden_terms_for(&hidden, &[course.clone(), person]);
        assert!(is_hidden("name", &t));
        assert!(!is_hidden("first_name", &t), "scoped entries are exact");
        // The schema has to match when both name one; an unqualified
        // reference could be any schema's table.
        assert!(!is_hidden(
            "display_name",
            &hidden_terms_for(&hidden, &[TableRef::parse("sales.contact")])
        ));
        assert!(is_hidden(
            "display_name",
            &hidden_terms_for(&hidden, &[TableRef::parse("contact")])
        ));
        assert!(is_hidden(
            "Display_Name",
            &hidden_terms_for(&hidden, &[TableRef::parse("CRM.\"Contact\"")])
        ));
        // Where the table isn't known, a scoped entry hides the name everywhere.
        let all = hidden_terms(&hidden);
        assert!(is_hidden("name", &all) && is_hidden("display_name", &all));
        let mut v = json!({"id": 1, "name": "x"});
        assert_eq!(redact(&mut v, &all), 1);
    }

    #[test]
    fn entries_are_validated() {
        for ok in [
            "email",
            "=ip",
            "person.name",
            "public.person.name",
            "=crm.contact.display_name",
            "\"Person\".\"Name\"",
        ] {
            assert_eq!(entry_problem(ok), None, "{ok}");
        }
        for bad in [
            "",
            "e",
            "=a",
            "a.b.c.d",
            ".name",
            "person.",
            "per$on..name",
            "a b.c;d",
        ] {
            assert!(entry_problem(bad).is_some(), "{bad}");
        }
        assert_eq!(
            scoped("Public.Person.Name"),
            Some(Scoped {
                schema: Some("public".into()),
                table: "person".into(),
                column: "name".into()
            })
        );
        assert_eq!(scoped("=name"), None);
        // A malformed dotted entry hides nothing rather than something odd.
        assert!(hidden_terms(&["a.b.c.d".to_string()]).is_empty());
    }
}
