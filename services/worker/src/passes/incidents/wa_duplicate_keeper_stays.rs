//! wa. Two duplicate sweeps closed every pull request on one topic, and a
//! family of follow-ups filed before the one-per-topic rule kept its
//! overlapping pull requests open with nothing to settle them:
//!
//! - each hourly sweep ranked a group afresh from each ticket's latest run
//!   (finished first, then a pull request, then the earliest run). The
//!   keeper was never recorded, and a ticket the sweep had closed itself
//!   still ranked. One sweep kept a finished ticket and closed the other
//!   two; later its run went back to work on a review, no longer counted
//!   as finished, and the next sweep kept the earliest one, already closed,
//!   and closed the last open pull request in its favour;
//! - a close needed the topic matcher to read two tickets as the same
//!   topic, while filing (since the one-per-topic rule) treats a maybe the
//!   small check confirms as the same topic. Tickets written in other words
//!   were flagged every hour and never closed;
//! - the sweep closed pull requests without the agent-permission gate and
//!   without looking at who had worked on them.
//!
//! Guarantees: a group's keeper is decided once and recorded before
//! anything closes, and later sweeps keep the recorded one; a member that
//! is out (closed, cancelled, covered, its pull request closed) is never
//! kept, and when the recorded keeper went out the keeper is picked again
//! from the open members; a duplicate closes only in favour of a keeper it
//! is paired with itself; a close never takes the topic's or the family's
//! last open pull request; the keeper is read again right before a close;
//! a family is grouped on the topic key filing uses; each pull request is
//! closed through the agent-permission gate with a note naming the
//! keeper's pull request, its branch kept, and never when a person
//! committed to it or asked for changes on it; Jira gets only the close
//! note it always got.

use super::fixture::fn_src;
use common::followup_dedupe::{self as fd, Candidate, Kept, Plan};

fn c(n: u32, title: &str, status: &str) -> Candidate {
    Candidate {
        key: format!("ABC-{n}"),
        title: title.into(),
        body: String::new(),
        run_id: format!("01M8{n:04}"),
        status: status.into(),
        has_pr: true,
        pr_open: true,
        pr_merged: false,
    }
}

/// A sweep's plan applied: its records stamped, its closes done.
fn apply(v: &mut [Candidate], p: &Plan, kept: &mut Vec<Kept>, at: &str) {
    for r in &p.record {
        kept.push(Kept {
            at: at.into(),
            ..r.clone()
        });
    }
    for x in &p.close {
        let d = v.iter_mut().find(|y| y.key == x.dup).unwrap();
        d.status = "closed".into();
        d.pr_open = false;
    }
}

fn closes(p: &Plan) -> Vec<(&str, &str)> {
    p.close
        .iter()
        .map(|c| (c.dup.as_str(), c.keep.as_str()))
        .collect()
}

#[test]
fn the_second_sweep_never_closes_the_keeper_or_keeps_a_closed_ticket() {
    let title = "Retry the export when the upload times out";
    // X finished; Y and Z still working; one topic, every pull request open.
    let mut v = vec![
        c(4, title, "running"),
        c(9, title, "completed"),
        c(13, title, "running"),
    ];
    let (x, y, z) = ("ABC-9", "ABC-4", "ABC-13");
    let same = [(0, 1), (0, 2), (1, 2)];
    let mut kept = Vec::new();
    // Sweep A keeps X and closes Y and Z.
    let a = fd::plan(&v, &same, &[], &kept);
    assert_eq!(closes(&a), vec![(z, x), (y, x)]);
    assert_eq!(
        a.record.iter().map(|k| k.keep.as_str()).collect::<Vec<_>>(),
        vec![x]
    );
    apply(&mut v, &a, &mut kept, "2026-01-01T10:00:00Z");
    // X's run goes back to work on a review: ranked afresh, Y (the earliest
    // run, closed by sweep A) used to come first.
    v[1].status = "running".into();
    let b = fd::plan(&v, &same, &[], &kept);
    assert!(b.close.is_empty(), "X is never closed: {b:?}");
    assert!(b.record.is_empty());
    assert_eq!(b.covering(y).map(|c| c.keep.as_str()), Some(x));
    assert_eq!(b.covering(z).map(|c| c.keep.as_str()), Some(x));
    // A person closes X: the next sweep keeps nobody closed, and closes
    // nothing, since every member is out.
    v[1].status = "closed".into();
    v[1].pr_open = false;
    assert_eq!(fd::plan(&v, &same, &[], &kept), Plan::default());
}

#[test]
fn a_keeper_a_person_closed_is_picked_again_from_the_open_members() {
    let title = "Show the export retry count";
    let mut v = vec![
        c(2, title, "closed"),
        c(5, title, "completed"),
        c(7, title, "running"),
    ];
    v[0].pr_open = false;
    let kept = vec![Kept {
        keep: "ABC-2".into(),
        members: vec!["ABC-2".into(), "ABC-5".into(), "ABC-7".into()],
        topic: title.into(),
        at: "2026-01-01T10:00:00Z".into(),
    }];
    let p = fd::plan(&v, &[(0, 1), (0, 2), (1, 2)], &[], &kept);
    assert_eq!(p.record[0].keep, "ABC-5");
    assert_eq!(closes(&p), vec![("ABC-7", "ABC-5")]);
}

#[tokio::test]
async fn a_family_filed_before_the_topic_rule_is_reconciled_on_it() {
    struct Check;
    #[async_trait::async_trait]
    impl common::log_repo::live::Judge for Check {
        async fn judge(&self, _system: &str, _user: &str) -> Result<String, String> {
            Ok("SAME ABC-30 ABC-31\nSAME ABC-30 ABC-32\nOPPOSED ABC-33 ABC-34".into())
        }
    }
    // One source fanned out: three tickets on one topic in other words,
    // a fix and a delete of the same components, and unrelated work.
    let fam = vec![
        c(
            30,
            "Retry the export when the upload times out",
            "completed",
        ),
        c(31, "Export upload gets one retry", "completed"),
        c(32, "Export upload gets a second try", "completed"),
        c(
            33,
            "Fix the colors of the settings page dark theme",
            "completed",
        ),
        c(34, "Remove the settings page dark theme", "completed"),
        c(35, "Show the export retry count", "completed"),
    ];
    let (same, maybe) = fd::matcher_pairs(&fam);
    for p in [(0, 1), (0, 2)] {
        assert!(
            !same.contains(&p) && maybe.contains(&p),
            "{p:?}: {same:?} {maybe:?}"
        );
    }
    let plan = fd::decide(&fam, Some(&Check), &[]).await;
    // One keeper for the topic, its two duplicates closed in its favour.
    assert_eq!(
        closes(&plan),
        vec![("ABC-31", "ABC-30"), ("ABC-32", "ABC-30")]
    );
    assert_eq!(plan.record.len(), 1);
    assert_eq!(plan.record[0].keep, "ABC-30");
    // The fix against the delete is flagged, never closed; the rest is
    // untouched.
    assert!(plan
        .flag
        .iter()
        .any(|f| f.opposed && f.a == "ABC-33" && f.b == "ABC-34"));
    assert!(plan.covering("ABC-35").is_none());
}

#[test]
fn the_sweep_records_before_it_closes_and_reads_the_keeper_again() {
    let src = include_str!("../followup_dedupe.rs");
    let team = fn_src(src, "\nasync fn sweep_team(");
    let read = team.find("kept_records(").unwrap();
    let decide = team.find("fd::decide(&cands, Some(judge), &kept)").unwrap();
    let record = team.find("record_keeper(").unwrap();
    let again = team.find("keeper_now(").unwrap();
    let close = team.find("close_duplicate(").unwrap();
    assert!(read < decide && decide < record && record < again && again < close);
    assert!(team.contains("unrecorded.contains(&c.keep)"));
    let rec = fn_src(src, "\nasync fn record_keeper(");
    assert!(rec.contains("attribute_not_exists(sk)"));
    assert!(rec.contains("fd::keep_record_sk(root, &k.keep)"));
    let now = fn_src(src, "\nasync fn keeper_now(");
    assert!(now.contains("k.out()"));
    assert!(now.contains("p[\"state\"].as_str() == Some(\"open\")"));
    // The gateway's sweep (a person starting a follow-up) keeps the same
    // records and the same checks.
    let gw = include_str!("../../../../gateway/src/routes/followup_dedupe.rs");
    let sweep = fn_src(gw, "\npub async fn sweep(");
    for step in [
        "kept_records(",
        "fd::decide(&cands, Some(&judge), &kept)",
        "record_keeper(",
        "!k.out() && (!dup_open || k.holds_work())",
        "no_person_holds(",
        "close_as_coderhelm(",
    ] {
        assert!(sweep.contains(step), "the gateway sweep lacks {step}");
    }
}

#[test]
fn each_pull_request_closes_through_the_gate_unless_a_person_holds_it() {
    let src = include_str!("../followup_dedupe.rs");
    let pr = fn_src(src, "\nasync fn closable_pr(");
    assert!(pr.contains("super::is_own_login(author)"));
    assert!(pr.contains("fd::person_holds(&commits, &reviews)"));
    let close = fn_src(src, "\nasync fn close_duplicate(");
    // Every pull request is read and allowed before the run is written.
    let read = close.find("closable_pr(").unwrap();
    let gate = close.find("super::may_close_own_pr(").unwrap();
    let write = close.find("#s = :closed").unwrap();
    let shut = close.find("super::close_own_pr(").unwrap();
    assert!(read < gate && gate < write && write < shut);
    assert!(close.contains("fd::pr_close_note(&c.keep, &c.topic, keeper_pr, &p.branch)"));
    // No pull request closes past the gate, and no branch is deleted.
    assert!(!src.contains(".close_pull_request("));
    assert!(!src.contains("delete_branch") && !src.contains("git/refs/heads"));
    // Jira gets the close note it always got, and nothing more.
    assert_eq!(src.matches("conversation::jira(").count(), 2);
    assert!(close.contains("Say::status(\"run\", said.clone())"));
    let note = fd::pr_close_note(
        "ABC-30",
        "Retry export",
        "harbor/web#41",
        "coderhelm/abc-31",
    );
    assert!(note.contains("harbor/web#41") && note.contains("`coderhelm/abc-31` is kept"));
}
