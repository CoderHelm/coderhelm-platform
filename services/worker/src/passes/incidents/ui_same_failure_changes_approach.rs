//! ui. CI fix rounds met the same failure again and again and repeated
//! themselves. On one run several rounds read the same missing-content
//! failure and each concluded "I made no code change"; on another, rounds
//! read the same cloud-account limit; on a third the error
//! signature hashed only the report-upload tail of the log, so two
//! unrelated runs had the same signature. The stall count rose to one,
//! but nothing told the next round what the last one had concluded or
//! that it had to do something else, and the budget ran out first.
//!
//! Guarantee: the failure signature reads the failing report's own error
//! line with the job logs. Each round's account is kept on the run. A
//! round that meets the same failure as the round before it, with no
//! progress, is told what that round concluded and must take another
//! approach: a different hypothesis it checks with a command first, or
//! the tolerant-code route when the failure names content. The stall halt
//! still bounds the rounds after that.

use common::content_gap;

#[test]
fn a_repeat_is_told_the_last_account_and_to_change_approach() {
    let last = "I made no code change this round. The failing test is right: staging has no library addresses.";
    let gap = content_gap::names_gap(
        "Error: libraryPage /library-a metadata.address is incomplete in this dataset; run apps/studio/migrations/fill-library-addresses",
    );
    let b = content_gap::repeat_brief(1, last, gap.as_ref());
    assert!(b.contains("Do not repeat their approach"));
    assert!(b.contains("staging has no library addresses"));
    assert!(b.contains("tolerant-code route"));
    let b = content_gap::repeat_brief(2, "The account limit was reached.", None);
    assert!(b.contains("different hypothesis"));
    assert!(b.contains("command's output"));
}

#[test]
fn the_round_reads_the_repeat_and_keeps_its_account() {
    let src = include_str!("../resume.rs");
    let fb = src
        .find("let mut feedback = format!(")
        .expect("the round's brief");
    let rest = &src[fb..];
    let repeat = rest
        .find("content_gap::repeat_brief(")
        .expect("the repeat brief");
    let round = rest.find("run_round::Round::start(").expect("the round");
    assert!(repeat < round, "told before the round starts");
    assert!(rest[..round].contains("\"stall_count\""));
    assert!(rest[..round].contains("\"last_fix_account\""));
    // Each round keeps its account for the next.
    let log = src
        .find("&format!(\"ci_fix_{}_{attempt}\", cycle + 1)")
        .expect("the round's log");
    assert!(src[log..log + 600].contains("save_fix_account("));
    // The signature reads the report's error line with the logs.
    assert!(src.contains("check_and_record_stall(state, &msg.team_id, &msg.run_id, &gap_text)"));
    assert!(src.contains("let gap_text = format!(\"{excerpt_line}\\n{logs}\");"));
}
