//! Text settings saved as one `content` attribute: custom instructions,
//! voice and agent context (`INSTRUCTIONS#`, `VOICE#`, `AGENTS#`, each with
//! a `GLOBAL` row and a `REPO#<owner>/<name>` row).
//!
//! The settings routes and Setup both read and write them here.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

pub type Item = HashMap<String, AttributeValue>;

/// Longest text a save accepts. Repo instructions often carry whole
/// conventions, which run past 10 KB; every agent in a run reads them.
pub const MAX_BYTES: usize = 32_768;

/// One repository's custom instructions row.
pub fn repo_sk(repo: &str) -> String {
    format!("INSTRUCTIONS#REPO#{repo}")
}

/// The saved text, empty when there is none.
pub fn content_of(item: Option<&Item>) -> String {
    item.and_then(|i| i.get("content"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// The text is longer than [`MAX_BYTES`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TooLarge;

/// The item a save writes.
pub fn item(team_id: &str, sk: &str, content: &str, now: &str) -> Result<Item, TooLarge> {
    if content.len() > MAX_BYTES {
        return Err(TooLarge);
    }
    let s = |v: &str| AttributeValue::S(v.to_string());
    Ok(HashMap::from([
        ("pk".to_string(), s(team_id)),
        ("sk".to_string(), s(sk)),
        ("content".to_string(), s(content)),
        ("updated_at".to_string(), s(now)),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saves_and_reads_back_and_refuses_too_long() {
        let i = item("T", &repo_sk("o/r"), "Use tabs.", "now").unwrap();
        assert_eq!(content_of(Some(&i)), "Use tabs.");
        assert_eq!(content_of(None), "");
        assert_eq!(
            item("T", "x", &"a".repeat(MAX_BYTES + 1), "now"),
            Err(TooLarge)
        );
    }
}
