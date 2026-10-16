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

/// The normalized terms to match field names against.
pub fn hidden_terms(hidden: &[String]) -> Vec<String> {
    let mut terms: Vec<String> = hidden
        .iter()
        .map(|h| norm(h))
        .filter(|h| h.len() >= 2)
        .collect();
    terms.sort();
    terms.dedup();
    terms
}

pub fn is_hidden(field: &str, terms: &[String]) -> bool {
    let f = norm(field);
    terms.iter().any(|t| f.contains(t.as_str()))
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
}
