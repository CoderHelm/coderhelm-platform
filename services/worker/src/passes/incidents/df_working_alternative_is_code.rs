//! df. A ticket said a charter card showed a broken image: the page used an
//! older image address that returned an error, while the address the
//! reporter expected was stored in our database. The trace found it all:
//! the charter's own record in the source system holds the broken address,
//! a second source record of the same kind (its fleet) holds a working
//! one, and our pipeline gives the charter's own image priority, so the
//! broken one is stored on the charter and rendered. Many other charters
//! carried addresses of the same kind. The run decided the fix is
//! outside the code (edit the one source record), and a later run of the
//! unchanged ticket stopped on that earlier verdict: "No new pull request:
//! the ticket is unchanged since CoderHelm learned the fix is outside the
//! code". A careful engineer makes the pipeline fall back to the working
//! image, for every record.
//!
//! Guarantee: a broken source value that our code chooses over a working
//! one in its reach is a code fix. When a decision says `outside_code` and
//! its own trace shows a layer holding a working value of the same kind as
//! a wrong layer, and a layer that is code in one of our repositories, the
//! decision goes back to the planner to fix that code
//! (`fix_location::working_alternative`, read by `fix_location::gaps`),
//! and the planner's guidance says so. An earlier "outside the code"
//! conclusion is judged again under the same rule before it can stop a
//! plan (`TicketHistory::guard`).

use crate::passes::fix_location::{
    self, FixDecision, FixEvidence, FixLocation, Layer, OutsideChange, Producer, Route,
};
use crate::passes::ticket_history::{Guard, PriorRun, TicketHistory};
use crate::passes::triage::TicketIntent;

fn layer(layer: &str, where_: &str, shows: &str, state: &str) -> Layer {
    Layer {
        layer: layer.into(),
        where_: where_.into(),
        shows: shows.into(),
        state: state.into(),
    }
}

/// The decision the run reached, as its trace recorded it.
fn outside_decision() -> FixDecision {
    FixDecision {
        fix_location: FixLocation::OutsideCode,
        cause: "The charter record's own image address returns an error, and the pipeline gives \
                the charter's image priority over its fleet's."
            .into(),
        intent: TicketIntent::Bug,
        evidence: FixEvidence {
            looked_up: vec!["[prod database] the charter and its fleet".into()],
            data_shows: "The charter holds the broken address; the fleet holds a working one."
                .into(),
            basis: "data".into(),
            producer: Producer {
                kind: "source".into(),
                where_: "booking system, charter record 1001: image address returns 502".into(),
            },
            scope:
                "One charter confirmed; hundreds of active charters carry addresses of that kind."
                    .into(),
            layers: vec![
                layer(
                    "source (charter record)",
                    "booking system, raw copy in the warehouse",
                    "image = old-bucket/a.jpg (the image proxy returns 502)",
                    "wrong",
                ),
                layer(
                    "source (fleet record)",
                    "booking system, raw copy in the warehouse",
                    "image = files-bucket/b.jpg (200)",
                    "ok",
                ),
                layer(
                    "pipeline",
                    "acme/pipelines:dags/charter_pipeline.py:212",
                    "copies the charter's own image when set, per its rule",
                    "ok",
                ),
                layer(
                    "app database",
                    "charter table",
                    "fleet files-bucket/b.jpg; charter old-bucket/a.jpg",
                    "wrong",
                ),
                layer(
                    "page",
                    "acme/web via the schedule API",
                    "renders old-bucket/a.jpg",
                    "wrong",
                ),
            ],
            ..Default::default()
        },
        outside_change: Some(OutsideChange {
            system: "the booking system".into(),
            change: "Replace the charter record's image with a working one".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn a_broken_source_value_with_a_working_one_in_reach_goes_back_as_a_code_fix() {
    let d = outside_decision();
    let gaps = fix_location::gaps(Some(&d), true, 6, &[]);
    let all = gaps.join("\n");
    assert!(all.contains("working value in reach"), "{all}");
    assert!(
        all.contains("acme/pipelines:dags/charter_pipeline.py:212"),
        "{all}"
    );
    assert!(all.contains("fall back to"), "{all}");
    // The same trace decided as a code fix stands.
    let mut code = d.clone();
    code.fix_location = FixLocation::Code;
    assert!(fix_location::working_alternative(&code).is_some());
    assert!(!fix_location::gaps(Some(&code), true, 6, &[])
        .join("\n")
        .contains("working value in reach"));
}

#[test]
fn a_wrong_source_with_no_working_value_in_reach_stays_outside() {
    let mut d = outside_decision();
    // No other record of that kind holds a working value: the source
    // record is the only place to fix.
    d.evidence.layers.remove(1);
    assert!(fix_location::working_alternative(&d).is_none());
    assert!(fix_location::gaps(Some(&d), true, 6, &[]).is_empty());
}

#[test]
fn the_planner_is_told_a_working_alternative_is_a_code_fix() {
    let g = fix_location::planner_guidance(TicketIntent::Bug);
    assert!(
        g.contains("prefer or fall back to the working value"),
        "{g}"
    );
}

/// The later run of the unchanged ticket: the earlier verdict is judged
/// again and no longer stops the plan.
#[test]
fn an_earlier_outside_verdict_is_judged_again_before_it_stops_a_plan() {
    let earlier = PriorRun {
        run_id: "r1".into(),
        status: fix_location::STATUS_OUTSIDE.into(),
        fix_location: "outside_code".into(),
        fix_decision: Some(outside_decision()),
        context_hash: "h1".into(),
        ..Default::default()
    };
    let history = TicketHistory {
        runs: vec![earlier.clone()],
        ..Default::default()
    };
    let mut now = outside_decision();
    now.fix_location = FixLocation::Code;
    now.outside_change = None;
    assert_eq!(fix_location::route(Some(&now)), Route::Code);
    assert!(
        matches!(history.guard(Some(&now), "h1"), Guard::Proceed),
        "an earlier verdict whose trace shows a working value stops nothing"
    );
    // A verdict with no working value in reach still holds an unchanged
    // ticket.
    let mut held = earlier;
    let mut d = outside_decision();
    d.evidence.layers.remove(1);
    held.fix_decision = Some(d);
    let history = TicketHistory {
        runs: vec![held],
        ..Default::default()
    };
    assert!(matches!(
        history.guard(Some(&now), "h1"),
        Guard::Stop { .. }
    ));
}
