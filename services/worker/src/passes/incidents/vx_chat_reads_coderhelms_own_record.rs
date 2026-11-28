//! vx. A person asked Chat why auto-merge was held on a run. The run page
//! said CoderHelm's own review still asked for changes, with its open
//! finding, but Chat's run record never read it, so Chat blamed the branch
//! rules. "What needs my answer?" listed runs that asked nothing as
//! questions, ranked runs with no requester first, and an answer came from
//! a knowledge source whose sync was failing without saying so. The failed tool calls
//! and the answers stopped on the budget left nothing in the logs.
//!
//! Guarantees: one reader of a run's decisions for the run page and Chat;
//! a need only with its evidence, ranked by who it waits on; a knowledge
//! base's sync health on its passages and in the setup check; failed tool
//! calls and each answer's turns, forced flag, time and intent logged.
//! The gateway's own tests hold the behaviour (`run_facts`,
//! `chat_work_needs`, `brain`, `chat_log`); this case keeps the health
//! reading and checks each piece is still wired where it acts.

use super::fixture::fn_src;
use common::knowledge::health::of_base;
use common::knowledge::sources::{Source, SourceError, Status};

const API: &str = include_str!("../../../../gateway/src/routes/api.rs");
const RUNS: &str = include_str!("../../../../gateway/src/routes/chat_work_runs.rs");
const NEEDS: &str = include_str!("../../../../gateway/src/routes/chat_work_needs.rs");
const BRAIN: &str = include_str!("../../../../gateway/src/routes/brain.rs");

#[test]
fn the_run_page_and_chat_read_one_record_of_coderhelms_decisions() {
    let page = fn_src(API, "pub(crate) fn run_of(");
    assert!(page.contains("super::run_facts::of_item(item)"), "{page}");
    assert!(page.contains("merge_standing,") && page.contains("own_review,"));
    let chat = fn_src(RUNS, "pub fn record_with(");
    assert!(chat.contains("super::run_facts::of_item(item)"), "{chat}");
}

#[test]
fn a_run_that_asked_nothing_is_never_a_question() {
    let runs = fn_src(NEEDS, "pub fn from_runs(");
    assert!(runs.contains("waiting_run_need(i, &updated)"), "{runs}");
    assert!(!runs.contains("Answer what the run asked"));
    assert!(runs.contains("Urgency::Stopped"));
    // No requester recorded ranks last, never "now".
    assert!(
        runs.contains("(_, false, true) => Urgency::Later"),
        "{runs}"
    );
}

#[test]
fn a_failing_wiki_says_so_on_every_passage() {
    let now = chrono::DateTime::parse_from_rfc3339("2030-04-02T12:00:00Z")
        .unwrap()
        .timestamp();
    let wiki = Source {
        id: "s1".into(),
        kb_id: "kb-northwind".into(),
        name: "Northwind wiki".into(),
        status: Status::Error,
        last_success_at: "2030-04-01T22:00:00Z".into(),
        last_sync_at: "2030-04-02T11:50:00Z".into(),
        stopped_since: "2030-04-01T22:30:00Z".into(),
        error: Some(SourceError {
            message: "The sign-in was refused.".into(),
            fix: "Reconnect to sign in again.".into(),
            action: "reconnect".into(),
            at: "2030-04-02T11:50:00Z".into(),
        }),
        ..Default::default()
    };
    let h = of_base("kb-northwind", "Northwind", &[wiki], now);
    let line = h.line();
    assert!(
        line.starts_with("NOT CURRENT: last good sync 2030-04-01T22:00:00Z"),
        "{line}"
    );
    assert!(
        line.contains("failing since 2030-04-01T22:30:00Z"),
        "{line}"
    );
    assert!(h.warning().unwrap().contains("first line"));
    // Wired: passages, the search result, the notes and the setup check.
    assert!(BRAIN.contains("add_hits_with_health(&found.hits, &|kb| run.health_line(kb))"));
    assert!(BRAIN.contains("notes.push_str(&run.health_warnings(&run.kb_ids));"));
    assert!(BRAIN.contains("l => format!(\"{name} (sync: {l})\")"));
}

#[test]
fn failed_calls_and_forced_answers_reach_the_logs() {
    assert!(BRAIN.contains("super::chat_log::tool_failed(&self.run.team_id, name, &kind, &text);"));
    assert!(BRAIN.contains("super::chat_log::Counted::new(&api_model)"));
    assert!(BRAIN.contains(".log(&run.team_id);"));
}
