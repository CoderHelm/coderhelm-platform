//! Removes lessons and reviewer asks that hold a placeholder, once.
//!
//! A commit prefix `KEY-1:` was learned from an example. New lessons can no
//! longer hold a placeholder (`common::placeholders`, checked where each is
//! learned), but an install may already keep one. This pass reads every
//! kept reward lesson and reviewer ask and deletes the ones a placeholder
//! is found in. It runs from the periodic maintenance tick, is claimed once
//! per install (and released when it could not finish, so the next tick
//! tries again), and running it twice removes nothing more.

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

use crate::WorkerState;

/// The claim that marks the cleanup as done for this install.
pub const CLAIM_PK: &str = "SYSTEM";
pub const CLAIM_SK: &str = "LESSONCLEANUP#placeholders-1";
/// Ten years: once.
const CLAIM_TTL_SECS: u64 = 10 * 365 * 24 * 3600;

/// Whether a kept record goes: it is a reward lesson or a reviewer ask, and
/// its rule or its check holds a placeholder.
pub fn purgeable(sk: &str, text: &str, check: &str) -> bool {
    let kept = sk.starts_with(common::rewards::store::LESSON_PREFIX)
        || sk.starts_with(common::reviewer_prefs::PREF_PREFIX);
    kept && (common::placeholders::has(text, &[]) || common::placeholders::has(check, &[]))
}

/// What one run did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Report {
    pub read: usize,
    pub removed: usize,
}

async fn sweep(state: &WorkerState) -> Result<Report, String> {
    let table = &state.config.settings_table_name;
    let mut report = Report::default();
    let mut start = None;
    loop {
        let page = state
            .dynamo
            .scan()
            .table_name(table)
            .filter_expression("begins_with(sk, :a) OR begins_with(sk, :b)")
            .expression_attribute_values(
                ":a",
                AttributeValue::S(common::rewards::store::LESSON_PREFIX.to_string()),
            )
            .expression_attribute_values(
                ":b",
                AttributeValue::S(common::reviewer_prefs::PREF_PREFIX.to_string()),
            )
            .projection_expression("pk, sk, #t, #c")
            .expression_attribute_names("#t", "text")
            .expression_attribute_names("#c", "check")
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())?;
        for item in page.items() {
            let get = |k: &str| {
                item.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            let (pk, sk) = (get("pk"), get("sk"));
            report.read += 1;
            if !purgeable(&sk, &get("text"), &get("check")) {
                continue;
            }
            state
                .dynamo
                .delete_item()
                .table_name(table)
                .key("pk", AttributeValue::S(pk.clone()))
                .key("sk", AttributeValue::S(sk.clone()))
                .send()
                .await
                .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())?;
            report.removed += 1;
            info!(team_id = %pk, record = %sk, "Removed a kept lesson that held a placeholder");
        }
        start = page.last_evaluated_key().cloned();
        if start.is_none() {
            return Ok(report);
        }
    }
}

/// Runs the cleanup when this install has not run it yet.
pub async fn run_once(state: &WorkerState) {
    let table = &state.config.settings_table_name;
    let claim =
        common::claim::claim(&state.dynamo, table, CLAIM_PK, CLAIM_SK, CLAIM_TTL_SECS).await;
    if !matches!(claim, common::claim::Claim::Won) {
        return;
    }
    match sweep(state).await {
        Ok(r) => info!(
            read = r.read,
            removed = r.removed,
            "Placeholder lesson cleanup done"
        ),
        Err(e) => {
            warn!(error = %e, "Placeholder lesson cleanup could not finish; the next tick tries again");
            let _ = state
                .dynamo
                .delete_item()
                .table_name(table)
                .key("pk", AttributeValue::S(CLAIM_PK.to_string()))
                .key("sk", AttributeValue::S(CLAIM_SK.to_string()))
                .send()
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_kept_lesson_with_a_placeholder_goes() {
        let lesson = format!("{}acme/site#l1", common::rewards::store::LESSON_PREFIX);
        let ask = format!("{}dana#a1", common::reviewer_prefs::PREF_PREFIX);
        assert!(purgeable(
            &lesson,
            "Prefix every commit subject with `KEY-1: `.",
            "The subject starts with the key."
        ));
        assert!(purgeable(
            &ask,
            "Title pull requests `ABC-123: <summary>`.",
            ""
        ));
        assert!(!purgeable(
            &lesson,
            "Call `walk()` with a depth limit in `src/scan.rs`.",
            "`walk(` has a second argument."
        ));
        // Another kind of record is never touched, whatever it says.
        assert!(!purgeable(
            "SETTINGS#GIT_CONVENTIONS",
            "{ticket}: {title}",
            ""
        ));
    }

    #[test]
    fn it_runs_from_the_maintenance_tick_once() {
        let main = include_str!("../main.rs");
        let tick = &main[main
            .find("models::WorkerMessage::ReviewReminder =>")
            .unwrap()..];
        assert!(tick.contains("passes::lesson_cleanup::run_once(&state)"));
        let src = include_str!("lesson_cleanup.rs");
        let run = &src[src.find("pub async fn run_once(").unwrap()..];
        assert!(
            run.contains("common::claim::claim("),
            "claimed once per install"
        );
        assert!(
            run.contains(".delete_item()"),
            "released when it could not finish"
        );
    }
}
