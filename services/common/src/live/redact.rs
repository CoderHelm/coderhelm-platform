//! Hides the values of sensitive-looking fields before a lookup result is
//! shown to a person or handed to a model.

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
}
