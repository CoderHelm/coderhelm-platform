//! The run record's growing lists stay bounded.
//!
//! A run record is one DynamoDB item, at most 400 KB. Large runs already
//! write 28 to 47 KB with up to ~200 progress notes (every tool call adds
//! one), and a run that keeps going for many steps would pass 400 KB, after
//! which every status write to the run fails. So:
//!
//! - progress notes: an append is conditional on the list holding fewer than
//!   [`MAX_NOTES`]; at the limit, the oldest are moved to S3
//!   (`runs/<team>/<run>/notes/<ms>.json`), the item keeps the newest
//!   [`KEEP_NOTES`] and counts what moved (`notes_archived`), and the append
//!   goes in. Each note's text is at most [`MAX_NOTE_CHARS`].
//! - pass history: the same, at [`MAX_PASSES`], keeping [`KEEP_PASSES`];
//!   the oldest are dropped (the passes are in the traces too).
//!
//! The normal append costs nothing extra: the condition is checked by
//! DynamoDB on the write itself.

use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
use tracing::warn;

/// Notes on the item at most; at this many, the oldest move to S3.
pub const MAX_NOTES: usize = 250;
/// Notes the item keeps after a move.
pub const KEEP_NOTES: usize = 200;
/// A note's text at most, in characters.
pub const MAX_NOTE_CHARS: usize = 1000;
/// Pass history entries at most.
pub const MAX_PASSES: usize = 200;
/// Pass history entries kept after a trim.
pub const KEEP_PASSES: usize = 150;

/// How many of a list's oldest entries go before one more is appended, so
/// that it then holds `keep`. Zero below `max`.
pub fn to_trim(len: usize, max: usize, keep: usize) -> usize {
    if len < max {
        0
    } else {
        len + 1 - keep
    }
}

/// A note's text as stored.
pub fn note_text(message: &str) -> String {
    common::truncate_str(message, MAX_NOTE_CHARS).to_string()
}

/// Where a list is bounded and, for notes, archived.
pub struct Target<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
    pub team_id: &'a str,
    pub run_id: &'a str,
    /// S3 for the notes moved off the item (None: they are dropped).
    pub archive: Option<(&'a aws_sdk_s3::Client, &'a str)>,
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn conditional(e: &impl std::fmt::Debug) -> bool {
    format!("{e:?}").contains("ConditionalCheckFailed")
}

/// Append one progress note, keeping the list bounded.
pub async fn append_note(t: &Target<'_>, message: &str) {
    let now = chrono::Utc::now().to_rfc3339();
    let entry = AttributeValue::M(HashMap::from([
        ("message".to_string(), s(&note_text(message))),
        ("timestamp".to_string(), s(&now)),
    ]));
    let _ = append(
        t,
        "progress_notes",
        entry,
        &now,
        MAX_NOTES,
        KEEP_NOTES,
        None,
    )
    .await;
}

/// Append to `attr` (with `updated_at`, and any extra SET clause), keeping
/// it under `max` entries. Errors other than the bound are returned.
pub async fn append(
    t: &Target<'_>,
    attr: &str,
    entry: AttributeValue,
    now: &str,
    max: usize,
    keep: usize,
    extra: Option<(&str, &str, AttributeValue)>,
) -> Result<(), aws_sdk_dynamodb::Error> {
    for attempt in 0..2 {
        let mut set =
            "SET #l = list_append(if_not_exists(#l, :empty), :entry), updated_at = :t".to_string();
        if let Some((clause, _, _)) = &extra {
            set.push_str(", ");
            set.push_str(clause);
        }
        let mut req = t
            .dynamo
            .update_item()
            .table_name(t.table)
            .key("team_id", s(t.team_id))
            .key("run_id", s(t.run_id))
            .update_expression(set)
            .condition_expression("attribute_not_exists(#l) OR size(#l) < :max")
            .expression_attribute_names("#l", attr)
            .expression_attribute_values(":entry", AttributeValue::L(vec![entry.clone()]))
            .expression_attribute_values(":empty", AttributeValue::L(vec![]))
            .expression_attribute_values(":t", s(now))
            .expression_attribute_values(":max", AttributeValue::N(max.to_string()));
        if let Some((_, name, value)) = &extra {
            req = req.expression_attribute_values(*name, value.clone());
        }
        match req.send().await {
            Ok(_) => return Ok(()),
            Err(e) if conditional(&e) && attempt == 0 => trim(t, attr, max, keep).await,
            Err(e) => {
                if !conditional(&e) {
                    return Err(e.into());
                }
                warn!(
                    run_id = t.run_id,
                    attr, "Run list still at its bound after a trim"
                );
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Moves the oldest entries off the item (notes to S3), so one more fits
/// under `max` with `keep` on the item.
async fn trim(t: &Target<'_>, attr: &str, max: usize, keep: usize) {
    let Ok(out) = t
        .dynamo
        .get_item()
        .table_name(t.table)
        .key("team_id", s(t.team_id))
        .key("run_id", s(t.run_id))
        .projection_expression("#l")
        .expression_attribute_names("#l", attr)
        .send()
        .await
    else {
        return;
    };
    let list = out
        .item()
        .and_then(|i| i.get(attr))
        .and_then(|v| v.as_l().ok())
        .cloned()
        .unwrap_or_default();
    let n = to_trim(list.len(), max, keep);
    if n == 0 {
        return;
    }
    if let Some((s3, bucket)) = t.archive {
        let moved: Vec<serde_json::Value> = list[..n]
            .iter()
            .filter_map(|v| v.as_m().ok())
            .map(|m| {
                serde_json::Value::Object(
                    m.iter()
                        .filter_map(|(k, v)| {
                            v.as_s()
                                .ok()
                                .map(|x| (k.clone(), serde_json::Value::String(x.clone())))
                        })
                        .collect(),
                )
            })
            .collect();
        let key = format!(
            "runs/{}/{}/notes/{}.json",
            t.team_id,
            t.run_id,
            chrono::Utc::now().timestamp_millis()
        );
        let body = serde_json::to_vec(&moved).unwrap_or_default();
        if let Err(e) = s3
            .put_object()
            .bucket(bucket)
            .key(&key)
            .content_type("application/json")
            .body(body.into())
            .send()
            .await
        {
            // The item's limit comes first: a note lost beats every
            // status write to the run failing.
            warn!(run_id = t.run_id, error = %e, "Could not archive old progress notes");
        }
    }
    let removes: Vec<String> = (0..n).map(|i| format!("#l[{i}]")).collect();
    let mut req = t
        .dynamo
        .update_item()
        .table_name(t.table)
        .key("team_id", s(t.team_id))
        .key("run_id", s(t.run_id))
        .expression_attribute_names("#l", attr)
        // Only the list this read saw: a concurrent trim already did it.
        .condition_expression("size(#l) = :len")
        .expression_attribute_values(":len", AttributeValue::N(list.len().to_string()));
    let mut expr = format!("REMOVE {}", removes.join(", "));
    if attr == "progress_notes" {
        expr.push_str(" ADD notes_archived :n");
        req = req.expression_attribute_values(":n", AttributeValue::N(n.to_string()));
    }
    if let Err(e) = req.update_expression(expr).send().await {
        if !conditional(&e) {
            warn!(run_id = t.run_id, attr, error = %e, "Could not trim the run list");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// DynamoDB's size of an item: attribute names plus values, with a few
    /// bytes per list and map element.
    fn note_bytes(message: &str) -> usize {
        "message".len() + message.len() + "timestamp".len() + 32 + 3 + 3
    }

    /// 2,000 notes (every one at the longest a note may be, the worst case)
    /// on top of the largest run record seen (47 KB) and a full pass
    /// history: the item never reaches DynamoDB's 400 KB.
    #[test]
    fn two_thousand_notes_keep_the_item_under_the_limit() {
        const ITEM_LIMIT: usize = 400 * 1024;
        let base = 47 * 1024;
        let pass_entry = "pass".len() + 12 + "started_at".len() + 32 + 6;
        let passes = MAX_PASSES * pass_entry;
        let long = "x".repeat(5000);
        let mut notes: Vec<usize> = Vec::new();
        let mut archived = 0;
        for _ in 0..2000 {
            let n = to_trim(notes.len(), MAX_NOTES, KEEP_NOTES);
            notes.drain(..n);
            archived += n;
            notes.push(note_bytes(&note_text(&long)));
            assert!(notes.len() <= MAX_NOTES);
            let size = base + passes + notes.iter().sum::<usize>() + "notes_archived".len() + 8;
            assert!(size < ITEM_LIMIT, "item at {size} bytes");
        }
        assert_eq!(
            archived + notes.len(),
            2000,
            "every note is kept or archived"
        );
    }

    #[test]
    fn trimming_starts_at_the_bound_and_keeps_the_newest() {
        assert_eq!(to_trim(0, MAX_NOTES, KEEP_NOTES), 0);
        assert_eq!(to_trim(MAX_NOTES - 1, MAX_NOTES, KEEP_NOTES), 0);
        assert_eq!(
            to_trim(MAX_NOTES, MAX_NOTES, KEEP_NOTES),
            MAX_NOTES + 1 - KEEP_NOTES
        );
        assert_eq!(
            to_trim(MAX_PASSES, MAX_PASSES, KEEP_PASSES),
            MAX_PASSES + 1 - KEEP_PASSES
        );
        assert_eq!(note_text(&"y".repeat(5000)).chars().count(), MAX_NOTE_CHARS);
    }

    /// Every progress note write goes through the bounded append.
    #[test]
    fn no_unbounded_note_or_pass_append_is_left() {
        for (name, src) in [
            ("mod.rs", include_str!("mod.rs")),
            ("implement.rs", include_str!("implement.rs")),
        ] {
            assert!(
                !src.contains("progress_notes = list_append"),
                "{name} appends progress notes without the bound"
            );
            assert!(
                !src.contains("pass_history = list_append"),
                "{name} appends pass history without the bound"
            );
        }
    }
}
