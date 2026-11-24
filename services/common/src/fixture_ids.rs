//! A test fixture uses the ids the run looked up, not made-up ones.
//!
//! A run queried the production database, found the two records behind a
//! duplicated filter and quoted their ids in its evidence, then wrote test
//! fixtures with `berth-a` and `berth-b`. The test passed on data that
//! doesn't exist, and nobody could tell from it which records the bug was
//! about.
//!
//! [`looked_up_ids`] reads the ids out of the run's lookups (UUIDs, their
//! eight-character prefixes, long hex and numeric ids); [`made_up`] finds an
//! id-like value in a test's added lines that looks invented (`berth-a`,
//! `id-1`, `test-id`, `fake_user`) while real ids are on record. The check
//! of the change against the plan (`habits_check`) sends each one back.

/// Ids a run's lookups returned, in order, each once.
pub fn looked_up_ids(lines: &[String]) -> Vec<String> {
    let re = regex::Regex::new(
        r"\b(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{8}(?:…|\.\.\.)|[0-9a-f]{24,40})",
    )
    .expect("valid regex");
    let mut out: Vec<String> = Vec::new();
    for l in lines {
        // Only what came back: the question may hold ids typed by hand.
        let got = l.split("Came back:").nth(1).unwrap_or("");
        for m in re.find_iter(got) {
            let id = m.as_str().trim_end_matches('…').trim_end_matches("...");
            if id.chars().any(|c| c.is_ascii_digit()) && !out.iter().any(|o| o == id) {
                out.push(id.to_string());
            }
        }
    }
    out
}

/// Whether a fixture value looks invented rather than read.
pub fn looks_made_up(value: &str) -> bool {
    let v = value.to_lowercase();
    let letter_or_digit = regex::Regex::new(r"^[a-z]+[-_][a-z0-9]$").expect("valid regex");
    let numbered = regex::Regex::new(r"^(?:id|item|row|rec)[-_]?\d{1,3}$").expect("valid regex");
    let filler = regex::Regex::new(
        r"^(?:test|fake|mock|dummy|placeholder|sample|some|foo|bar)(?:[-_][a-z0-9-_]*)?$",
    )
    .expect("valid regex");
    letter_or_digit.is_match(&v) || numbered.is_match(&v) || filler.is_match(&v)
}

/// One invented fixture id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MadeUp {
    pub path: String,
    pub value: String,
}

/// Invented id values on the added lines of test files (`diff`: path,
/// status, patch), when the run looked up real ids (`looked`). An id-like
/// key: `id`, `…Id`, `…_id`, `ids`.
pub fn made_up(diff: &[(String, String, String)], looked: &[String]) -> Vec<MadeUp> {
    if looked.is_empty() {
        return Vec::new();
    }
    let key =
        regex::Regex::new(r#"(?i)\b[a-z_]*(?:id|ids)\s*[:=]\s*\[?\s*["'`]([^"'`]{1,40})["'`]"#)
            .expect("valid regex");
    let mut out: Vec<MadeUp> = Vec::new();
    for (path, _, patch) in diff
        .iter()
        .filter(|(p, _, _)| crate::push_gate::is_test_file(p))
    {
        for line in patch
            .lines()
            .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
        {
            for c in key.captures_iter(line) {
                let value = c[1].to_string();
                if looks_made_up(&value)
                    && !looked.iter().any(|l| l == &value)
                    && !out.iter().any(|m| m.path == *path && m.value == value)
                {
                    out.push(MadeUp {
                        path: path.clone(),
                        value,
                    });
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn made_up_ids_are_found_only_when_real_ones_were_looked_up() {
        let lines = vec![
            "- [L1] Asked Production database: SELECT id FROM berth WHERE ... Came back: 4c2d9e10-1111-4a2b-8c3d-0a2b3c4d5e6f 'Guest-berth'; b7f3a2c8… 'Guest-Berth'".to_string(),
            "- [L2] Asked it: SELECT 9f8e7d6c-0000-0000-0000-000000000000 Came back EMPTY".to_string(),
        ];
        let ids = looked_up_ids(&lines);
        assert_eq!(
            ids,
            vec!["4c2d9e10-1111-4a2b-8c3d-0a2b3c4d5e6f", "b7f3a2c8"]
        );
        let diff = vec![
            (
                "src/test/filters.test.ts".to_string(),
                "modified".to_string(),
                "+  { id: 'berth-a', name: 'Guest-berth' },\n+  { id: \"berth-b\", name: 'Guest-Berth' },\n+  { berthId: '4c2d9e10-1111-4a2b-8c3d-0a2b3c4d5e6f' },\n-  { id: 'old-1' }".to_string(),
            ),
            (
                "src/utils/filters.ts".to_string(),
                "modified".to_string(),
                "+const id = 'berth-a';".to_string(),
            ),
        ];
        let found = made_up(&diff, &ids);
        assert_eq!(
            found.iter().map(|m| m.value.as_str()).collect::<Vec<_>>(),
            vec!["berth-a", "berth-b"]
        );
        assert!(made_up(&diff, &[]).is_empty());
        assert!(looks_made_up("id-1") && looks_made_up("test-id") && looks_made_up("fake_user"));
        assert!(!looks_made_up("guest-berth") && !looks_made_up("4c2d9e10"));
    }
}
