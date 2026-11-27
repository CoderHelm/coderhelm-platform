//! du. Someone pasted a production alarm into Chat: a target group's p99
//! response time over its threshold. Chat spent its lookups searching and
//! reading the alarm definitions in the infrastructure repository, ran one
//! metric query and one log group listing, and answered with a guess and a
//! list headed "Not checked (I ran out of lookups)": the target group's p99
//! and request count, the service's CPU, its logs for the alarm's window,
//! and the rest of a cut-off result. A fixed count of model turns ended the
//! answer, whatever the question, and the guess compared slow-call counts
//! with a busier hour's instead of breaking latency down by path and lining
//! it up with traffic.
//!
//! Guarantee: an alert, an error or an incident reads as a diagnosis
//! (`common::chat_intent::Intent::Diagnosis`), whose live evidence comes
//! before the code (`common::chat_evidence::EvidenceFirst`). No count ends an
//! answer: one that lists checks it didn't run goes back to run them
//! (`says_unfinished`), a hedged diagnosis goes back to run the lookup that
//! settles it (`says_hedged`, `FIRM_UP`), and a cut-off result says what was
//! cut and how to read the rest (`fit_result`). A senior asker gets the
//! conclusion first at expert depth (`Reading::senior`). The loop and its
//! background continuation are the gateway's (`chat_agent`), replayed there
//! with a made-up service.

use common::chat_evidence::{
    fit_result, says_hedged, says_unfinished, was_cut, EvidenceFirst, FIRM_UP,
};
use common::chat_intent::{read, Intent, Signals};

/// A made-up alarm, pasted as it arrives.
const ALARM: &str = "ALARM: \"prod-quayside-api-p99\" in US East. Threshold Crossed: 3 datapoints were greater than the threshold (2.0). TargetResponseTime p99 for the quayside-api target group.";

/// The answer as it went wrong (made-up names): a guess, and checks left
/// undone for want of lookups.
const STOPPED_SHORT: &str = "The upstream wasn't slow: its slow-call count is below the daytime count. Probably one or two slow requests at night.\n\n**Not checked (I ran out of lookups)**\n- the target group's p99 and request count\n- the service's CPU\n- the service's logs around the alarm";

#[test]
fn a_pasted_alarm_is_a_diagnosis_that_reads_live_evidence_first() {
    let r = read(ALARM, None, Signals::default());
    assert_eq!(r.intent, Intent::Diagnosis);
    assert!(r.intent.evidence_first());
    let note = r.note();
    assert!(note.contains("break the metric down by path"), "{note}");
    assert!(note.contains("minimum request count"), "{note}");
    // Code reads past two wait for the live evidence; live reads never do.
    let e = EvidenceFirst::over(vec!["metrics_query".into(), "logs_search".into()]).unwrap();
    let made: Vec<String> = vec!["github__search_code".into(), "github__read_file".into()];
    assert!(e.holds("github__read_file", &made));
    assert!(!e.holds("metrics_query", &made));
}

#[test]
fn the_answer_that_stopped_short_is_never_kept_as_written() {
    assert!(says_unfinished(STOPPED_SHORT));
    assert!(says_hedged(STOPPED_SHORT));
    assert!(FIRM_UP.contains("line it up with request volume"));
    let firm = "One path is slow all day, and the alarm fires only when traffic is low: GET /v1/tides/forecast averages 3.4 s, and overnight traffic drops to about 110 requests per 5 minutes. Cache that call or speed it up, and add a minimum request count to the alarm.";
    assert!(!says_unfinished(firm) && !says_hedged(firm));
}

#[test]
fn a_cut_off_result_says_what_was_cut_and_how_to_read_the_rest() {
    let long = "02:41 p99 3.9 s on 104 requests; ".repeat(1_000);
    let fit = fit_result(&long);
    assert!(was_cut(&fit));
    assert!(fit.contains("Narrow this lookup"));
}

#[test]
fn a_senior_asker_gets_the_conclusion_first_never_more_hedging() {
    let q = format!("I'm the VP of engineering. {ALARM} Why does this keep firing?");
    let r = read(&q, None, Signals::default());
    assert_eq!(r.intent, Intent::Diagnosis);
    assert!(r.senior);
    let note = r.note();
    assert!(
        note.contains("conclusion first") && note.contains("never more hedging"),
        "{note}"
    );
}
