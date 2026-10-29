//! b. A person retried a ticket whose earlier run had an open pull request.
//! The retry started over on the same branch while the earlier run was
//! still fixing CI on it: two writers on one branch, and a second PR.
//!
//! Guarantee: a retry continues the earlier run's open pull request as its
//! update, on its branch, and opens no other. While the earlier run still
//! works on it (running, waiting on CI, a writer slot held, a round under
//! way or parked), the retry stops before writing anything and says why.

use super::fixture::*;
use crate::passes::pr_holder::{self, Holder};
use crate::passes::ticket_history::{PriorRun, TicketHistory};

const RUN_1: &str = "01JRUNEARLIER0000000000000";
const RUN_2: &str = "01JRUNRETRY000000000000000";
const PR: u64 = 12;

fn history() -> TicketHistory {
    TicketHistory {
        runs: vec![PriorRun {
            run_id: RUN_1.into(),
            status: "completed".into(),
            repo: format!("{OWNER}/{REPO}"),
            pr_number: PR,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The retry, as `run_passes` runs it on a branch with an open pull
/// request: find the earlier runs that opened it, read each one's record,
/// and continue or stop. `record` is the earlier run's DynamoDB item.
fn retry(
    gh: &mut FakeGitHub,
    record: std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    now: chrono::DateTime<chrono::Utc>,
) -> Ending {
    let mut end = Ending::default();
    let pr = gh.open_pr_for_branch(BRANCH).expect("the earlier run's PR");
    let number = pr["number"].as_u64().unwrap();
    let earlier = pr_holder::holder_runs(&history(), &format!("{OWNER}/{REPO}"), number);
    assert_eq!(earlier, vec![RUN_1.to_string()]);
    let me = pr_holder::holder_name(RUN_2);
    for run in &earlier {
        let h = Holder::from_item(run, &record);
        if let Some(why) = pr_holder::busy(&h, now, &me) {
            end.said(pr_holder::busy_note(number, run, &why));
            end.status = common::run_status::COVERED_BY_PR.into();
            end.pr = Some(number);
            return end;
        }
    }
    end.said(pr_holder::continue_note(number, &earlier));
    gh.commit(&me, BRANCH, &[("apps/web/src/blocks/card.tsx", "v2")]);
    end.status = common::run_status::AWAITING_CI.into();
    end.pr = Some(number);
    end
}

fn with_open_pr() -> FakeGitHub {
    let mut gh = FakeGitHub::new();
    gh.commit(RUN_1, BRANCH, &[("apps/web/src/blocks/card.tsx", "v1")]);
    gh.prs.push(FakePr {
        number: PR,
        branch: BRANCH.into(),
        body: "## Summary\nAdds the card block.".into(),
        draft: false,
    });
    gh
}

fn now() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2026-10-28T15:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
}

#[test]
fn a_retry_continues_the_earlier_runs_pull_request() {
    let mut gh = with_open_pr();
    let end = retry(&mut gh, run_item(&[("status", s("completed"))]), now());
    assert_eq!(end.status, common::run_status::AWAITING_CI);
    assert_eq!(end.pr, Some(PR));
    assert!(end.says("continues it as its update, on its branch, and opens no other"));
    assert_eq!(gh.prs.len(), 1, "no second pull request");
    assert_eq!(gh.writes.last().unwrap().1, pr_holder::holder_name(RUN_2));
}

#[test]
fn a_retry_never_writes_while_the_earlier_run_works_on_the_branch() {
    let later = now().timestamp() + 600;
    let busy_records = [
        run_item(&[("status", s("awaiting_ci"))]),
        run_item(&[("status", s("running"))]),
        run_item(&[
            ("status", s("completed")),
            ("writer_until", n(later)),
            ("writer_holder", s("feedback#01JFEEDBACK")),
        ]),
        // A planned-work round parked on the earlier run's PR.
        run_item(&[
            ("status", s("awaiting_ci")),
            (
                common::run_round::FIELD,
                crate::passes::finish_planned::first_round(
                    &crate::passes::proof::unfinished_section(&[crate::passes::proof::Finding {
                        point: "planned_work".into(),
                        detail: "The card component is not written.".into(),
                        ..Default::default()
                    }]),
                    now(),
                )
                .unwrap()
                .to_attr(),
            ),
        ]),
    ];
    for record in busy_records {
        let mut gh = with_open_pr();
        let writes_before = gh.writes.len();
        let end = retry(&mut gh, record.clone(), now());
        assert_eq!(end.status, common::run_status::COVERED_BY_PR, "{record:?}");
        assert!(
            end.says("Stopped before writing anything"),
            "{:?}",
            end.words
        );
        assert!(end.says("Two runs never write the same branch"));
        assert_eq!(
            gh.writes.len(),
            writes_before,
            "nothing written: {record:?}"
        );
        assert_eq!(gh.prs.len(), 1);
    }
}

#[test]
fn the_pipeline_claims_the_earlier_runs_before_it_writes() {
    let rp = run_passes_src();
    let claim = rp.find("pr_holder::claim(").unwrap();
    let implement = rp.find("implement::run(").unwrap();
    assert!(claim < implement, "the claim comes before any write");
    assert!(rp.contains("pr_holder::stop_busy("));
    assert!(rp.contains("pr_holder::continue_note("));
}
