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
//! - a progress note the run already wrote last, less than
//!   [`NOTE_REPEAT_SECS`] ago, isn't written again: retries and steps that
//!   overlap once wrote "Waiting for GitHub's hourly limit" four times in two
//!   minutes. The repeat is counted on the record (`last_note_repeats`).
//!
//! The normal append costs nothing extra: the conditions are checked by
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
/// How long the note written last stays one note when it is written again.
pub const NOTE_REPEAT_SECS: i64 = 15 * 60;

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

/// The earliest time a note written last still counts as a repeat, for a
/// write at `now`. Timestamps are stored as RFC 3339 in UTC, so they order
/// as strings.
pub fn repeat_cutoff(now: chrono::DateTime<chrono::Utc>) -> String {
    (now - chrono::Duration::seconds(NOTE_REPEAT_SECS)).to_rfc3339()
}

/// Append one progress note, keeping the list bounded. The note written
/// last, written again within [`NOTE_REPEAT_SECS`], stays one note: the
/// repeat is counted instead.
pub async fn append_note(t: &Target<'_>, message: &str) {
    let at = chrono::Utc::now();
    let now = at.to_rfc3339();
    let text = note_text(message);
    let cutoff = repeat_cutoff(at);
    let entry = AttributeValue::M(HashMap::from([
        ("message".to_string(), s(&text)),
        ("timestamp".to_string(), s(&now)),
    ]));
    for attempt in 0..2 {
        let res = t
            .dynamo
            .update_item()
            .table_name(t.table)
            .key("team_id", s(t.team_id))
            .key("run_id", s(t.run_id))
            .update_expression(
                "SET #l = list_append(if_not_exists(#l, :empty), :entry), updated_at = :t, \
                 last_note = :msg, last_note_at = :t, last_note_repeats = :zero",
            )
            .condition_expression(
                "(attribute_not_exists(#l) OR size(#l) < :max) AND \
                 (attribute_not_exists(last_note) OR last_note <> :msg OR last_note_at < :cut)",
            )
            .expression_attribute_names("#l", "progress_notes")
            .expression_attribute_values(":entry", AttributeValue::L(vec![entry.clone()]))
            .expression_attribute_values(":empty", AttributeValue::L(vec![]))
            .expression_attribute_values(":t", s(&now))
            .expression_attribute_values(":msg", s(&text))
            .expression_attribute_values(":cut", s(&cutoff))
            .expression_attribute_values(":zero", AttributeValue::N("0".into()))
            .expression_attribute_values(":max", AttributeValue::N(MAX_NOTES.to_string()))
            .send()
            .await;
        let Err(e) = res else {
            return;
        };
        if !conditional(&e) {
            warn!(run_id = t.run_id, error = %e, "Could not write a progress note");
            return;
        }
        // The same note as the last one, a moment ago: counted, not written.
        if count_repeat(t, &text, &cutoff, &now).await {
            return;
        }
        if attempt == 0 {
            trim(t, "progress_notes", MAX_NOTES, KEEP_NOTES).await;
        } else {
            warn!(
                run_id = t.run_id,
                attr = "progress_notes",
                "Run list still at its bound after a trim"
            );
        }
    }
}

/// Counts one more writing of the note written last, when it is `text` and
/// was written after `cutoff`. False when it isn't (the write was refused
/// for the list's bound instead).
async fn count_repeat(t: &Target<'_>, text: &str, cutoff: &str, now: &str) -> bool {
    t.dynamo
        .update_item()
        .table_name(t.table)
        .key("team_id", s(t.team_id))
        .key("run_id", s(t.run_id))
        .update_expression("ADD last_note_repeats :one SET last_note_repeat_at = :t")
        .condition_expression("last_note = :msg AND last_note_at >= :cut")
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .expression_attribute_values(":t", s(now))
        .expression_attribute_values(":msg", s(text))
        .expression_attribute_values(":cut", s(cutoff))
        .send()
        .await
        .is_ok()
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

    /// The note written last is a repeat for a quarter of an hour, and the
    /// stored timestamps order as their strings do.
    #[test]
    fn a_note_written_again_soon_after_is_a_repeat() {
        use chrono::TimeZone;
        let at = chrono::Utc
            .with_ymd_and_hms(2026, 11, 23, 21, 54, 18)
            .unwrap();
        let cut = repeat_cutoff(at);
        assert_eq!(cut, "2026-11-23T21:39:18+00:00");
        // The four notes of 21:52 to 21:54 all fall after the cutoff of the
        // last; a note from 21:38 doesn't.
        for (earlier, repeat) in [
            ("2026-11-23T21:52:39.512391044+00:00", true),
            ("2026-11-23T21:39:18+00:00", true),
            ("2026-11-23T21:38:59.999+00:00", false),
            ("2026-11-22T23:59:59+00:00", false),
        ] {
            assert_eq!(earlier >= cut.as_str(), repeat, "{earlier}");
        }
        // What chrono writes for a note's time sorts the same way.
        let later = (at + chrono::Duration::milliseconds(1)).to_rfc3339();
        assert!(later.as_str() > at.to_rfc3339().as_str());
    }

    /// The write itself refuses the repeat: one conditional write, no read.
    #[test]
    fn the_repeat_is_refused_by_the_write_itself() {
        let src = include_str!("run_lists.rs");
        let at = src.find("pub async fn append_note").unwrap();
        let body = &src[at..];
        let body = &body[..body.find("\n}\n").unwrap()];
        assert!(body.contains("last_note <> :msg OR last_note_at < :cut"));
        assert!(body.contains("count_repeat"));
        assert!(!body.contains("get_item"));
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
