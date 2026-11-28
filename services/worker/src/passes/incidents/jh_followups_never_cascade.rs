//! jh. Follow-up tickets CoderHelm filed from a Jira ticket carried the
//! team's trigger label (and, on the default trigger, labels that count as
//! the trigger too), so each follow-up started its own run, and each of
//! those runs filed follow-ups of its own. Nothing told a follow-up from any
//! other ticket at the trigger, and nothing limited how many one source
//! could produce, so the follow-ups multiplied and several runs did the same
//! work twice.
//!
//! Guarantee: a follow-up is filed with `coderhelm-followup` and
//! `coderhelm:followup:<source>` only, never a label the team's trigger
//! matches, and never assigned (`followup_tickets::labels`; the Jira app
//! drops any other label, `followups.followupLabels`); a follow-up label is
//! never a trigger (`followup_tickets::is_trigger_label`); the Jira webhook
//! starts no run on a follow-up (its label, CoderHelm's `FOLLOWUP_OF#`
//! record, or its text), even when a person adds the trigger label, and
//! comments once that a person starts it from CoderHelm (`start_run` reads
//! the ticket from the record); a run on a follow-up files none of its own
//! (`followups::followup_source`, depth one); and one source files at most
//! `DAILY_CAP_PER_SOURCE` a day (`followups::take_daily_slot`).

use common::followup_tickets::{self as ft, Draft};

/// The default trigger label.
const TRIGGER: &str = "coderhelm";

#[test]
fn a_filed_followup_never_carries_the_trigger() {
    let labels = ft::labels("ABC-10", TRIGGER);
    assert_eq!(
        labels,
        vec!["coderhelm-followup", "coderhelm:followup:ABC-10"]
    );
    for l in &labels {
        assert!(!ft::is_trigger_label(l, TRIGGER), "{l} is a trigger");
    }
    // What follow-ups used to carry: the first two are triggers, and the
    // follow-up label no longer is.
    assert!(ft::is_trigger_label("coderhelm", TRIGGER));
    assert!(ft::is_trigger_label("coderhelm:acme/web", TRIGGER));
    assert!(!ft::is_trigger_label("coderhelm:followup:ABC-10", TRIGGER));
    // Another trigger the team set: still never carried.
    for trigger in ["ai-fix", "coderhelm-followup", "CODERHELM"] {
        assert!(ft::labels("ABC-1", trigger)
            .iter()
            .all(|l| !ft::is_trigger_label(l, trigger)));
    }
}

#[test]
fn a_followup_is_known_however_it_arrives() {
    // By its labels, even after a person adds the trigger label by hand.
    assert_eq!(
        ft::source_of_labels(&["coderhelm", "coderhelm:followup:ABC-10"]),
        Some("ABC-10".into())
    );
    // By its text, for the follow-ups filed before the marker existed and
    // for a run started from its run record.
    let body = ft::description(
        &Draft {
            summary: "Validate alt on banner images".into(),
            description: "Add the check.".into(),
        },
        "ABC-10",
        "Audit",
        "https://app.example/runs/detail?id=r1",
    );
    assert_eq!(ft::source_of_body(&body), Some("ABC-10".into()));
    assert_eq!(ft::source_of_labels(&["coderhelm"]), None);
    assert_eq!(ft::source_of_body("Add alt text to the banner."), None);
}

#[test]
fn a_run_on_a_followup_files_none_and_a_source_files_a_few_a_day() {
    let src = include_str!("../followups.rs");
    let file = src.find("pub async fn file(").expect("file");
    let depth = src[file..]
        .find("followup_source(state, msg).await")
        .map(|i| i + file)
        .expect("the depth check in file");
    let first_filing = src[file..].find("file_one(").map(|i| i + file).unwrap();
    assert!(
        depth < first_filing,
        "the depth check runs before any filing"
    );
    assert!(src[depth..first_filing].contains("ft::depth_refusal"));
    // Every new ticket takes one of the source's daily slots first.
    let one = src.find("async fn file_one(").unwrap();
    let slot = src[one..].find("take_daily_slot(").unwrap() + one;
    let create = src[one..].find(".post(&j.create_url)").unwrap() + one;
    assert!(
        slot < create,
        "the daily slot is taken before the ticket is created"
    );
    assert!(src.contains("ft::labels(source, trigger)"));
    assert!(ft::DAILY_CAP_PER_SOURCE > 0 && ft::DAILY_CAP_PER_SOURCE as usize <= ft::MAX_DRAFTS);
    assert!(ft::depth_refusal("ABC-10").contains("ABC-10"));
}

#[test]
fn the_jira_webhook_holds_a_followup_before_any_run_starts() {
    let gw = include_str!("../../../../gateway/src/routes/jira_webhook.rs");
    let process = gw.find("async fn process_jira_payload(").unwrap();
    let hold = gw[process..].find("\"followup_held\"").unwrap() + process;
    let gate = gw[process..].find("gate_ticket_trigger(").unwrap() + process;
    let queue = gw[process..]
        .find("send_to_queue(state, &state.config.ticket_queue_url, &message)")
        .unwrap()
        + process;
    assert!(
        hold < gate && hold < queue,
        "a follow-up returns before the gate and the queue"
    );
    assert!(gw[process..hold].contains("source_of_labels(&issue_labels)"));
    assert!(gw[process..hold].contains("followup_record_source("));
    assert!(gw[process..hold].contains("source_of_body(&body_text)"));
    // A person starts it explicitly, from CoderHelm's record.
    let start = include_str!("../../../../gateway/src/routes/run_start.rs");
    assert!(start.contains("followup_record(state, &claims.team_id, key)"));
    assert!(start.contains("followup_of: Some(source)"));
    let said = ft::held_comment("ABC-10");
    assert!(
        said.contains("follow-up") && said.contains("Start a run"),
        "{said}"
    );
}

/// The small check, faked: its answer for the family below.
struct FakeCheck(&'static str);

#[async_trait::async_trait]
impl common::log_repo::live::Judge for FakeCheck {
    async fn judge(&self, _system: &str, user: &str) -> Result<String, String> {
        assert!(user.contains("ABC-12") && user.contains("ABC-22"));
        Ok(self.0.to_string())
    }
}

/// A family of follow-ups of ABC-10, with each latest run.
fn family() -> Vec<common::followup_dedupe::Candidate> {
    let c = |n: u32, title: &str, status: &str| common::followup_dedupe::Candidate {
        key: format!("ABC-{n}"),
        title: title.into(),
        body: String::new(),
        run_id: format!("01M87{n:04}"),
        status: status.into(),
        has_pr: true,
        pr_open: true,
        pr_merged: false,
    };
    vec![
        c(
            12,
            "Stop using file names as alt text in the image fragment",
            "completed",
        ),
        c(
            22,
            "Stop using file names as alt text in the image fragment query",
            "completed",
        ),
        c(
            14,
            "Replace generic placeholder alt text with real or empty alt",
            "running",
        ),
        c(
            19,
            "Replace generic placeholder alt text with real or empty alt text",
            "running",
        ),
        c(
            16,
            "Fix alt on the unused banner and card components",
            "completed",
        ),
        c(
            18,
            "Remove the unused banner and card components",
            "awaiting_ci",
        ),
        c(
            21,
            "Apply the missing-alt warning to every image field",
            "completed",
        ),
        c(
            20,
            "Editor: warn when an image's alt is missing",
            "awaiting_ci",
        ),
    ]
}

#[tokio::test]
async fn a_duplicate_closes_only_when_the_check_and_the_matcher_lean_the_same_way() {
    use common::followup_dedupe as fd;
    let fam = family();
    // What the matcher reads on its own: 12/22 and 14/19 the same, 21/20 not.
    let (near_same, _) = fd::matcher_pairs(&fam);
    assert!(near_same.contains(&(0, 1)), "{near_same:?}");
    assert!(near_same.contains(&(2, 3)), "{near_same:?}");
    assert!(!near_same.contains(&(6, 7)), "{near_same:?}");
    // The check agrees on 12/22, says nothing of 14/19, reads 21/20 as the
    // same, and 16/18 as opposed.
    let check = FakeCheck("SAME ABC-12 ABC-22\nSAME ABC-21 ABC-20\nOPPOSED ABC-16 ABC-18");
    let plan = fd::decide(&fam, Some(&check), &[]).await;
    let closes: Vec<(&str, &str)> = plan
        .close
        .iter()
        .map(|c| (c.dup.as_str(), c.keep.as_str()))
        .collect();
    // 21/20 the matcher finds near (maybe) and the check reads as the same
    // work: the topic key filing uses, so it closes too
    // (`wa_duplicate_keeper_stays`).
    assert_eq!(closes, vec![("ABC-20", "ABC-21"), ("ABC-22", "ABC-12")]);
    // Disagreements and the fix against the delete are flagged, never closed.
    let flags: Vec<(&str, &str, bool)> = plan
        .flag
        .iter()
        .map(|f| (f.a.as_str(), f.b.as_str(), f.opposed))
        .collect();
    assert!(flags.contains(&("ABC-14", "ABC-19", false)), "{flags:?}");
    assert!(flags.contains(&("ABC-16", "ABC-18", true)), "{flags:?}");
    let c = &plan.close[1];
    assert_eq!(
        fd::close_comment(&c.keep, &c.topic),
        "Duplicate of ABC-12 (Stop using file names as alt text in the image fragment). \
         Closing; the work continues there. Closed by CoderHelm."
    );
    // Without the check's answer nothing closes.
    assert_eq!(fd::decide(&fam, None, &[]).await, fd::Plan::default());
}

#[test]
fn duplicates_close_on_the_hourly_sweep_with_a_persons_close_steps() {
    let close = include_str!("../../../../gateway/src/routes/run_close.rs");
    let at = close
        .find("pub(crate) async fn close_as_coderhelm(")
        .unwrap();
    let body = &close[at..];
    assert!(body.contains("close_pr: true"));
    assert!(body.contains("ctx.close_one(run_id)"));
    let sweep = include_str!("../followup_dedupe.rs");
    for step in [
        "#s = :closed",
        "release_ticket_lock(",
        "merge_gate::stop(",
        "may_close_own_pr(",
        "close_own_pr(",
        "conversation::jira(",
        "log_close(",
        "take_close_slot(",
        "claim_once_ttl(",
    ] {
        assert!(sweep.contains(step), "the sweep lacks {step}");
    }
    let main = include_str!("../../main.rs");
    assert!(main.contains("passes::followup_dedupe::sweep(&state)"));
}
