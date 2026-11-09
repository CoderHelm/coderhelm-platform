//! The reviewer's activity, live: the review's tool loop reports each call
//! and each note to a [`common::review_activity::Recorder`], and a feed
//! appends what was recorded to the review's session every few seconds, so
//! the session page shows the review as it happens. The writes are
//! best-effort (the session is a view): a failed one is logged and the
//! review goes on.

use std::sync::Arc;
use std::time::Duration;

use common::review_activity::Recorder;
use serde_json::Value;

use crate::agent::llm::LoopObserver;

/// How often the feed appends what was recorded.
pub(crate) const FLUSH_EVERY: Duration = Duration::from_secs(3);

/// The reviewer's tool loop, seen by its recorder.
pub(crate) struct ActivityObserver(pub Arc<Recorder>);

impl LoopObserver for ActivityObserver {
    fn on_text(&self, text: &str) {
        self.0.note(text);
    }
    fn on_tool_call(&self, name: &str, input: &Value, _duration_ms: u64, is_error: bool) {
        self.0.tool(name, input, is_error);
    }
}

/// Where a feed writes: one review job's session.
#[derive(Clone)]
pub(crate) struct SessionTarget {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub table: String,
    pub team_id: String,
    pub job: String,
}

/// Appends a recorder's events to a session while a review runs. Closing it
/// writes the rest; dropping it without closing (an early return) still
/// writes the rest, in the background.
pub(crate) struct ActivityFeed {
    recorder: Arc<Recorder>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ActivityFeed {
    pub(crate) fn start(target: SessionTarget) -> Self {
        let recorder = Arc::new(Recorder::new());
        let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
        let rec = recorder.clone();
        let task = tokio::spawn(async move {
            loop {
                // A sent stop and a dropped sender both end the feed.
                let last = tokio::select! {
                    _ = &mut stopped => true,
                    _ = tokio::time::sleep(FLUSH_EVERY) => false,
                };
                flush(&target, &rec).await;
                if last {
                    break;
                }
            }
        });
        Self {
            recorder,
            stop: Some(stop),
            task: Some(task),
        }
    }

    /// The recorder the review's tool loop reports to.
    pub(crate) fn recorder(&self) -> &Arc<Recorder> {
        &self.recorder
    }

    /// Write what is left and stop.
    pub(crate) async fn close(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ActivityFeed {
    fn drop(&mut self) {
        // The sender drops with the feed; the task sees it, writes what is
        // left and ends on its own.
        self.stop.take();
    }
}

async fn flush(target: &SessionTarget, recorder: &Recorder) {
    let (events, dropped) = recorder.drain();
    common::review_session::append_activity(
        &target.dynamo,
        &target.table,
        &target.team_id,
        &target.job,
        &events,
        dropped,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_observer_records_notes_and_calls_but_never_results() {
        let rec = Arc::new(Recorder::new());
        let o = ActivityObserver(rec.clone());
        o.on_text("**Checking callers**\nTwo places call `settle`.");
        o.on_tool_call("search_code", &json!({"query": "settle("}), 12, false);
        o.on_tool_call(
            "db_query",
            &json!({"sql": "select secret from t"}),
            40,
            true,
        );
        let (events, dropped) = rec.drain();
        assert_eq!(dropped, 0);
        let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
        assert_eq!(kinds, vec!["note", "search", "lookup"]);
        assert_eq!(events[0].heading, "Checking callers");
        assert_eq!(events[1].label, "settle(");
        assert!(events[2].failed);
        assert!(!format!("{events:?}").contains("secret"));
    }
}
