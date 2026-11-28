//! DynamoDB's reserved words (`dynamo_reserved_words.txt`, the list AWS
//! publishes in the DynamoDB developer guide, "Reserved words in
//! DynamoDB"), and a check that an expression never names an attribute by
//! one of them without a `#name` placeholder. DynamoDB refuses such an
//! expression with a ValidationException on every call, so a write that
//! uses one never lands, whatever the item.
//!
//! The test at the bottom reads every update, condition, projection, filter
//! and key condition expression the services write as a literal and fails
//! on a bare reserved word.

use std::collections::HashSet;
use std::sync::OnceLock;

const WORDS: &str = include_str!("dynamo_reserved_words.txt");

/// The expression grammar's own words where they are syntax (always
/// written in capitals here); as attribute names they are still reserved.
const SYNTAX: &[&str] = &[
    "SET", "ADD", "REMOVE", "DELETE", "AND", "OR", "NOT", "IN", "BETWEEN",
];

fn reserved() -> &'static HashSet<&'static str> {
    static SET: OnceLock<HashSet<&'static str>> = OnceLock::new();
    SET.get_or_init(|| WORDS.split_whitespace().collect())
}

/// True when DynamoDB reserves `name` (any case).
pub fn is_reserved(name: &str) -> bool {
    reserved().contains(name.to_ascii_uppercase().as_str())
}

/// The attribute names `expr` writes bare that DynamoDB reserves. Values
/// (`:v`), placeholders (`#n`), functions (`if_not_exists(`) and the
/// grammar's own capitalised words are skipped; `{...}` (a `format!`
/// argument) is skipped too.
pub fn bare_reserved(expr: &str) -> Vec<String> {
    let mut out = Vec::new();
    let b = expr.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;
    while i < b.len() {
        let c = b[i];
        if c == b'{' {
            depth += 1;
            i += 1;
            continue;
        }
        if c == b'}' {
            depth = depth.saturating_sub(1);
            i += 1;
            continue;
        }
        if depth > 0 {
            i += 1;
            continue;
        }
        if c == b':' || c == b'#' {
            // A value or a placeholder: skip its name.
            i += 1;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            let word = &expr[start..i];
            let mut j = i;
            while j < b.len() && b[j] == b' ' {
                j += 1;
            }
            let call = j < b.len() && b[j] == b'(';
            if !call && !SYNTAX.contains(&word) && is_reserved(word) {
                out.push(word.to_string());
            }
            continue;
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    #[test]
    fn the_list_is_awss_whole_list() {
        // The developer guide's list has 573 words.
        assert_eq!(reserved().len(), 573);
        for w in ["READS", "TTL", "STATUS", "NAME", "AGENT", "USERS"] {
            assert!(is_reserved(w), "{w}");
        }
        for w in ["memories", "uses", "last_at", "pk", "sk"] {
            assert!(!is_reserved(w), "{w}");
        }
    }

    #[test]
    fn a_bare_reserved_name_is_found_and_an_aliased_one_is_not() {
        assert_eq!(
            bare_reserved("ADD reads :one, memories :n SET last_at = :now, #ttl = :ttl"),
            vec!["reads"]
        );
        assert!(
            bare_reserved("ADD #reads :one, memories :n SET last_at = :now, #ttl = :ttl")
                .is_empty()
        );
        assert!(bare_reserved("SET a = if_not_exists(a, :z) REMOVE b").is_empty());
        assert!(bare_reserved("attribute_not_exists(pk) OR size(x) < :n").is_empty());
        assert_eq!(bare_reserved("SET {set}, status = :s"), vec!["status"]);
        assert_eq!(bare_reserved("doc.name = :n"), vec!["name"]);
    }

    fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                rust_files(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }

    /// The string literal right after `call(` (or `call(format!(` /
    /// `call(&format!(`), unescaped well enough to read names from.
    fn literals_after<'a>(src: &'a str, call: &str) -> Vec<(usize, &'a str)> {
        let mut out = Vec::new();
        let mut from = 0;
        while let Some(at) = src[from..].find(call) {
            let start = from + at + call.len();
            from = start;
            let rest = src[start..].trim_start();
            let rest = rest
                .strip_prefix("&format!(")
                .or_else(|| rest.strip_prefix("format!("))
                .unwrap_or(rest)
                .trim_start();
            let Some(body) = rest.strip_prefix('"') else {
                continue;
            };
            let mut end = 0;
            let bytes = body.as_bytes();
            while end < bytes.len() && bytes[end] != b'"' {
                end += if bytes[end] == b'\\' { 2 } else { 1 };
            }
            let line = src[..start].matches('\n').count() + 1;
            out.push((line, &body[..end.min(body.len())]));
        }
        out
    }

    #[test]
    fn no_expression_the_services_send_names_a_reserved_word_bare() {
        let services = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut files = Vec::new();
        for c in ["common/src", "gateway/src", "worker/src"] {
            rust_files(&services.join(c), &mut files);
        }
        assert!(files.len() > 100, "the services' sources were found");
        let mut bad = Vec::new();
        let mut seen = 0;
        for f in &files {
            let src = std::fs::read_to_string(f).unwrap();
            for call in [
                ".update_expression(",
                ".condition_expression(",
                ".projection_expression(",
                ".filter_expression(",
                ".key_condition_expression(",
            ] {
                for (line, expr) in literals_after(&src, call) {
                    seen += 1;
                    let words = bare_reserved(expr);
                    if !words.is_empty() {
                        bad.push(format!("{}:{line}: {words:?} in {expr:?}", f.display()));
                    }
                }
            }
        }
        assert!(seen > 500, "the expressions were read ({seen})");
        assert!(
            bad.is_empty(),
            "DynamoDB refuses these on every call; name the attribute with a #placeholder:\n{}",
            bad.join("\n")
        );
    }
}
