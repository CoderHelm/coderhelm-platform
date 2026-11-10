//! How a tool call ended, said by the tool that ran it.
//!
//! The tool loop runs each call inside [`capture`]; a tool executor that
//! knows more than "it returned" (an edit that matched nothing, checks that
//! failed, how many matches a search found) calls [`note`] with the
//! outcome, from the code that has the real values. A call nobody noted is
//! `ok` or `error` by how it returned ([`ToolOutcome::default_for`]): never
//! guessed from its text.

use std::cell::RefCell;
use std::future::Future;

pub use common::agent_log::ToolOutcome;

tokio::task_local! {
    static SLOT: RefCell<Option<ToolOutcome>>;
}

/// Records the outcome of the tool call running now. Outside a call (a
/// test, a spawned task) it does nothing. The last note of a call wins.
pub fn note(outcome: ToolOutcome) {
    let _ = SLOT.try_with(|s| *s.borrow_mut() = Some(outcome));
}

/// Runs one tool call and returns what it returned with the outcome its
/// tool noted, if it noted one.
pub async fn capture<F: Future>(call: F) -> (F::Output, Option<ToolOutcome>) {
    SLOT.scope(RefCell::new(None), async move {
        let out = call.await;
        let noted = SLOT.with(|s| s.borrow_mut().take());
        (out, noted)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_noted_outcome_is_captured_and_the_last_note_wins() {
        let (v, o) = capture(async {
            note(ToolOutcome::ok("first"));
            tokio::task::yield_now().await;
            note(ToolOutcome::soft_fail("NO edits applied"));
            7
        })
        .await;
        assert_eq!(v, 7);
        assert_eq!(o, Some(ToolOutcome::soft_fail("NO edits applied")));
    }

    #[tokio::test]
    async fn no_note_is_none_and_notes_never_leak_between_calls() {
        let (_, o) = capture(async {}).await;
        assert_eq!(o, None);
        // A nested call (a sub-agent's tool) keeps its own outcome.
        let (_, outer) = capture(async {
            let (_, inner) = capture(async { note(ToolOutcome::error("inner")) }).await;
            assert_eq!(inner, Some(ToolOutcome::error("inner")));
        })
        .await;
        assert_eq!(outer, None);
        // Outside any call a note does nothing (and never panics).
        note(ToolOutcome::ok("nowhere"));
    }
}
