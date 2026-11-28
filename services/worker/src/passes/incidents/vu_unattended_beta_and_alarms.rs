//! vu. A self-hosted install on the beta channel never took a beta by
//! itself, and a broken install went unnoticed:
//!
//! - Every release was a "permission change". CloudFormation reports a
//!   Modify for each policy that names another part's output (its value is
//!   known only after apply), on every release, with the documents byte for
//!   byte the same. The updater counted any IAM Modify, and a scheduled
//!   update that changes permissions waits for a person, so the hourly
//!   check could never apply one.
//! - The maintenance window was one hour a week (or a day), so at most one
//!   of several daily betas could install.
//! - The watch after an update probed the worker and the gateway only. Chat
//!   runs in its own function with its own role; it lost a grant and refused
//!   every question with both of those healthy.
//! - The alarms counted Lambda Errors, which stay at 0 because the services
//!   catch and log every error: no alarm moved while ERROR lines, GitHub
//!   rate limits and the Chat outage piled up.
//!
//! Guarantees: a Modify counts only when the resource, resolved the way the
//! template resolves it on each side (the running stack's values, the
//! change set's, an import kept when its source is the same), differs; an
//! Add, a real widening, or anything that can't be compared still waits.
//! The beta channel's `day: "any"` installs at any hour, `gap_hours` after
//! the last install. The watch makes Chat's own calls through the streaming
//! function and rolls back on a refusal, as for a failed worker probe. Every
//! install alarms on ERROR lines, rate-limit lines and Chat answers that
//! failed, each as a share of its traffic. The Python, CDK and gateway tests
//! named below hold each one; this case holds the worker's part and the
//! wiring between them.

const UPDATER: &str = include_str!("../../../../../lambda/updater/handler.py");
const MONITORING: &str = include_str!("../../../../../infra/lib/monitoring-stack.ts");
const GITHUB: &str = include_str!("../../clients/github.rs");

/// The Python function `name` (up to the next top-level definition).
fn py_fn<'a>(src: &'a str, name: &str) -> &'a str {
    let head = format!("\ndef {name}(");
    let start = src.find(&head).unwrap_or_else(|| panic!("{name} is gone"));
    let rest = &src[start + 1..];
    let end = rest[1..]
        .find("\ndef ")
        .or_else(|| rest[1..].find("\nclass "))
        .map(|i| i + 1)
        .unwrap_or(rest.len());
    &rest[..end]
}

#[test]
fn a_modify_counts_only_when_the_resource_differs() {
    let f = py_fn(UPDATER, "permission_changes");
    assert!(f.contains("policy_difference("), "{f}");
    assert!(
        f.contains("if not differs:"),
        "an unchanged policy is dropped"
    );
    // What can't be compared still counts.
    assert!(f.contains("couldn't be compared"));
    // The hold reads the list as before.
    assert!(py_fn(UPDATER, "native_hold").contains("entry.get(\"permission_changes\")"));
}

#[test]
fn the_beta_channel_installs_at_any_hour_after_a_gap() {
    let w = py_fn(UPDATER, "in_window");
    assert!(w.contains("ANY_HOUR") && w.contains("takes_prereleases"));
    let d = py_fn(UPDATER, "decide");
    assert!(d.contains("gap_wait("));
    let v = py_fn(UPDATER, "validate_settings");
    assert!(v.contains("is for the beta channel"));
    let system = include_str!("../../../../gateway/src/routes/system.rs");
    assert!(system.contains("\"gap_hours\""));
}

#[test]
fn the_watch_probes_chat_and_rolls_back_on_its_failure() {
    let r = py_fn(UPDATER, "probe_release");
    assert!(r.contains("probe_worker(") && r.contains("probe_chat("));
    assert!(py_fn(UPDATER, "run_probe").contains("probe_release"));
    let chat = py_fn(UPDATER, "probe_chat");
    assert!(chat.contains("invoke_with_response_stream"));
    let probe = include_str!("../../../../gateway/src/routes/stream_probe.rs");
    assert!(probe.contains("check_scoped") && probe.contains("streaming-dynamodb.json"));
    assert!(UPDATER.contains(r#"CHAT_PROBE_HOST = "coderhelm-probe.invalid""#));
    assert!(probe.contains(r#"PROBE_HOST: &str = "coderhelm-probe.invalid""#));
}

#[test]
fn every_rate_limit_is_one_structured_line() {
    // The two places a request is refused for GitHub's limit build the
    // error only through `RateLimited::logged`, which writes the line the
    // install's alarm reads.
    // The client itself, before its tests (which build the error directly).
    let client = GITHUB.split("\n#[cfg(test)]\nmod ").next().unwrap();
    assert!(!client.contains("Box::new(RateLimited {"));
    assert_eq!(client.matches("Box::new(RateLimited::logged(").count(), 2);
    assert!(GITHUB.contains("event = common::github_budget::RATE_LIMITED_EVENT"));
    assert_eq!(
        common::github_budget::RATE_LIMITED_EVENT,
        "github_rate_limited"
    );
    assert!(MONITORING.contains(r#"rateLimited: "github_rate_limited""#));
}

#[test]
fn the_alarms_are_shares_of_traffic() {
    for name in ["error-lines", "github-rate-limited", "chat-answer-failed"] {
        assert!(MONITORING.contains(name), "{name}");
    }
    assert!(MONITORING.contains("100 * errors / calls"));
    assert!(MONITORING.contains("100 * failed / (failed + answered)"));
    assert!(MONITORING.contains("new actions.SnsAction(alertTopic)"));
}
