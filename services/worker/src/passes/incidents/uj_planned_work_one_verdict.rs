//! uj. A run's planned-work round finished and its check found "no planned
//! work left". Later, on the same head, the proof
//! check judged the branch again and found "1 planned task(s) still not
//! done", held the pull request as a draft, and sent the run back to work.
//! The two checks each asked the judge on their own and kept their own
//! result, so they disagreed, and the run went round until its
//! continuation ceiling stopped it and handed it to a person. Other runs
//! did the same.
//!
//! Guarantee: there is one verdict on planned work per head
//! (`proof::PLANNED_FIELD`). Whichever check judges a head first keeps its
//! planned work there; the other reads it and never judges that head again,
//! so on one head they always agree.

use super::fixture::fn_src;
use crate::passes::proof::{self, Finding, PlannedVerdict};

fn f(point: &str, detail: &str) -> Finding {
    Finding {
        point: point.into(),
        detail: detail.into(),
        ..Default::default()
    }
}

#[test]
fn one_head_has_one_planned_verdict() {
    // The planned-work round's check said none left on this head.
    let stored = PlannedVerdict {
        head: "c0ffee1".into(),
        left: vec![],
    };
    // The proof check's own judge says one is left on the same head.
    let judged = vec![
        f(
            "planned_work",
            "The time chip clear step in the spec is not built.",
        ),
        f("impact", "Records affected were not counted."),
    ];
    let left = proof::planned_on(Some(&stored), "c0ffee1").expect("judged on this head");
    let settled = proof::with_planned(judged.clone(), left);
    assert!(proof::planned_work(&settled).is_empty(), "{settled:?}");
    assert_eq!(settled.len(), 1, "other findings stay");
    // A new head is judged anew.
    assert!(proof::planned_on(Some(&stored), "d00d2").is_none());
    assert!(proof::planned_on(Some(&stored), "").is_none());
}

#[test]
fn both_checks_read_and_write_the_one_verdict() {
    let proof_src = include_str!("../proof.rs");
    let prove = fn_src(proof_src, "\npub async fn prove(");
    assert!(
        prove.contains("settle_planned("),
        "the proof check reads it"
    );
    let settle = fn_src(proof_src, "\npub async fn settle_planned(");
    assert!(settle.contains("load_planned(") && settle.contains("save_planned("));
    let resume = include_str!("../resume.rs");
    let recheck = fn_src(resume, "\nasync fn recheck_planned_work(");
    let read = recheck
        .find("planned_on(")
        .expect("the round's check reads it");
    let judge = recheck
        .find("proof::judge(")
        .expect("judges only when it isn't there");
    assert!(read < judge);
    assert!(
        recheck.contains("save_planned("),
        "and keeps what it judged"
    );
}
