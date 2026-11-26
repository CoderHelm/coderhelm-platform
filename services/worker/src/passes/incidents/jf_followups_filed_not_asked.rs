//! jf. A run on a Jira audit ticket finished the audit, wrote several
//! follow-up ticket drafts into proposal.md and stopped with a question
//! saying the run had no Jira access to file them, asking a person to
//! connect Jira or file them by hand, marked as needing a
//! person with no default. The team's Jira was connected: the run had been
//! started from that Jira ticket and had commented on it. The planner's
//! capability list never named the ticket tracker (only "Connected apps:
//! not set up"), it had no tool to file tickets, and nothing stopped a
//! question asking a person for access CoderHelm already had.
//!
//! Guarantee: the planner reads the team's tracker as a capability with its
//! tool (`host_steps::tracker`, `followups::planner_caps`); the plan's
//! follow-ups are filed by CoderHelm in the source ticket's project, once
//! per draft across runs (`followup_tickets::draft_id`, the app's label
//! search), linked and labelled, with one comment listing the keys
//! (`followups::prepare`, `followups::file`); a plan written before that field files the list
//! under proposal.md's follow-up heading, so Retry on the stuck run files
//! its drafts; a run that codes nothing and leaves nothing for a person ends
//! delivered (`followups::nothing_left`, `finish_delivered`); and no ask,
//! hold or plan step may ask a person to connect a system the run reaches
//! (`connected_access`, in the stop judge's rules and its last pass).

use common::connected_access;
use common::followup_tickets::{self as ft, Filed, Linked, Tracker};
use common::stop_judge::{Kind, Picture};
use serde_json::json;

use crate::passes::fix_location::{
    EndToEndStep, FixDecision, FixLocation, NextStep, OutsideChange, TicketIntent,
};
use crate::passes::followups;
use crate::passes::outside_work::NoteStep;

/// proposal.md as the run wrote it, with the names changed.
const PROPOSAL: &str = "# Audit: alt-text fallbacks on shared image components

## Problem
The ticket asks for an audit that sorts each component into (a), (b) or (c). Follow-up tickets then carry the fixes.

## Answer (the audit)
| Class | Component (line) | Today | Decision |
|---|---|---|---|
| (a) schema | `image-with-alt.ts` | alt optional | Add validation |

## Follow-up tickets to file (additive only: no deletes, no replacements)
1. Record in the ticket that `Hero.tsx`, `FeatureSection.tsx` and `ContactCard.tsx` are not rendered. There is no code change; any future wiring takes a required alt.
2. Add alt-required validation to `imageWithAlt` (when an asset is set). Then run typegen and take a before-count per dataset.
3. Add alt-required validation to writer images. Fill in the missing alt on the production writers with `setIfMissing`.
4. Replace the placeholder fallbacks (`richtext`, `add-on-block`, `slot-promo`) with schema-required alt.
5. Replace the whitespace alts (`WritersCarousel.tsx:40` with the writer name, `SubNavigationMenu.tsx:79` with `\"\"`).
6. `wing-navigation-client.tsx:41`: drop the generic \"Wing icon\".
7. Promotion alt quality: replace \"bookshelf\" on `a1b2c3d4` with a real description.

## Scope
An audit only.
";

const TASKS: &str =
    "NO_CODE_CHANGE: the full classification and 7 additive follow-up tickets are in proposal.md.";

/// The plan's decision as the run wrote it.
fn plan() -> FixDecision {
    FixDecision {
        fix_location: FixLocation::Unknown,
        intent: TicketIntent::Question,
        cause: "The three named components are unused; the real gaps are the optional alt in the shared schema.".into(),
        outside_change: Some(OutsideChange {
            system: "Jira".into(),
            change: "File the 7 additive follow-up tickets listed in proposal.md".into(),
            where_: "Jira ABC project".into(),
            steps: vec![
                NoteStep {
                    step: "Post the audit table, the production records and the follow-up list on the ticket".into(),
                    by: "coderhelm".into(),
                    tool: "ticket comment".into(),
                    ..Default::default()
                },
                NoteStep {
                    step: "File the 7 follow-up tickets".into(),
                    by: "person".into(),
                    owner: "the ticket's reporter".into(),
                    how: "Jira, from proposal.md 'Follow-up tickets to file'".into(),
                    why_person: "CoderHelm has no Jira connection (Connected apps: not set up), so it cannot create tickets".into(),
                    grant: "Connect Jira in Settings, Integrations".into(),
                    reason: "outside_reach".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }),
        end_to_end: vec![
            EndToEndStep {
                step: "Audit posted on the ticket".into(),
                by: "this_pr".into(),
                ..Default::default()
            },
            EndToEndStep {
                step: "Follow-up tickets filed".into(),
                by: "person".into(),
                owner: "ticket reporter".into(),
                ..Default::default()
            },
        ],
        next_step: Some(NextStep {
            action: "File the 7 follow-up tickets from proposal.md in the Jira ABC project, or connect Jira so CoderHelm can file them".into(),
            why_person: "this run has no Jira access".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

const SITE: &str = "https://example.atlassian.net";

#[test]
fn the_planner_reads_the_connected_tracker_and_its_tool() {
    let caps = common::host_steps::tracker(Some((SITE, true)), false);
    assert!(caps.available);
    assert_eq!(caps.tools, vec![ft::TOOL.to_string()]);
    assert!(caps
        .does
        .contains("Jira is connected through the Coderhelm app on https://example.atlassian.net"));
    assert!(caps.does.contains("followup_tickets"));
    let section = common::host_steps::section(&[common::host_steps::Reader::Plan], &[caps]);
    assert!(
        section.contains("- Ticket tracker (file_followup_tickets):"),
        "{section}"
    );
    assert!(!section.contains("Ticket tracker: not set up"), "{section}");
    // The planner is told to write the drafts, never a person step.
    let src = include_str!("../fix_location.rs");
    assert!(src.contains("\"followup_tickets\": [{\"summary\""));
    assert!(src.contains("Filing them is never a `person` step"));
    // And plan.rs gives the planner the team's caps, not the bare catalogue.
    let plan_src = include_str!("../plan.rs");
    assert!(plan_src.contains("super::followups::planner_caps(state, msg, github)"));
}

#[test]
fn the_stuck_run_s_seven_drafts_come_from_its_proposal() {
    let d = plan();
    assert!(d.followup_tickets.is_empty(), "written before the field");
    assert!(followups::asks_to_file(&d));
    let drafts = ft::drafts(&d.followup_tickets, PROPOSAL);
    assert_eq!(drafts.len(), 7, "{drafts:?}");
    assert_eq!(
        drafts[1].summary,
        "Add alt-required validation to imageWithAlt (when an asset is set)"
    );
    assert!(drafts[1].description.contains("before-count per dataset"));
    assert_eq!(
        drafts[5].summary,
        "wing-navigation-client.tsx:41: drop the generic \"Wing icon\""
    );
    // Filed once per draft for this source, whichever run files it.
    let ids: Vec<String> = drafts.iter().map(|x| ft::draft_id("ABC-123", x)).collect();
    let again: Vec<String> = ft::drafts(&[], PROPOSAL)
        .iter()
        .map(|x| ft::draft_id("ABC-123", x))
        .collect();
    assert_eq!(ids, again);
    assert_eq!(
        followups::record_sk("abc-123", &ids[0]),
        format!("FOLLOWUP#ABC-123#{}", ids[0])
    );
    // In the source ticket's project, labelled like every CoderHelm ticket.
    assert_eq!(
        ft::tracker("ABC-123", true, SITE, "acme/web", Some(true)),
        Tracker::Jira {
            project: "ABC".into(),
            site: SITE.into()
        }
    );
    assert_eq!(
        // Never the trigger label (see `jh_followups_never_cascade`).
        ft::labels("ABC-123", "coderhelm"),
        vec!["coderhelm-followup", "coderhelm:followup:ABC-123"]
    );
}

#[test]
fn once_filed_the_run_ends_delivered_and_asks_no_one() {
    let mut d = plan();
    assert!(
        !followups::nothing_left(&d, TASKS, &[]),
        "the old run waited on a person"
    );
    let reach = [connected_access::jira(SITE, true)];
    followups::drop_connect_asks(&mut d, &reach);
    let keys: Vec<String> = (1..=7).map(|n| format!("ABC-9{n:02}")).collect();
    followups::mark_filed(
        &mut d,
        &format!("Filed 7 of 7 follow-up tickets: {}", keys.join(", ")),
        true,
    );
    assert!(followups::nothing_left(&d, TASKS, &[]));
    assert!(d.next_step.is_none());
    let report = crate::passes::fix_location::unknown_report(&d, false);
    let low = report.to_lowercase();
    for never in [
        "connect jira",
        "no jira access",
        "needs a person",
        "i need your call",
    ] {
        assert!(!low.contains(never), "{never}: {report}");
    }
    // The one comment lists every new key.
    let drafts = ft::drafts(&[], PROPOSAL);
    let filed: Vec<Filed> = keys
        .iter()
        .map(|k| Filed::Ticket {
            key: k.clone(),
            url: format!("{SITE}/browse/{k}"),
            existing: false,
            linked: Linked::Yes,
        })
        .collect();
    let comment = ft::comment(
        &drafts,
        &filed,
        &Tracker::Jira {
            project: "ABC".into(),
            site: SITE.into(),
        },
    );
    assert!(comment.starts_with("Filed 7 follow-up tickets from this ticket's findings:"));
    for k in &keys {
        assert!(comment.contains(&format!("{k}: ")), "{k}: {comment}");
    }
    // The pipeline settles the plan's filing before the stop judge looks at
    // the question, and a findings plan ends delivered there (filed only
    // then: see `jg_followups_once_per_topic`).
    let src = include_str!("../mod.rs");
    let prepare = src
        .find("followups::prepare(state, msg, run_id, decision, &proposal)")
        .expect("prepare is called");
    let judge = src
        .find("\"the plan doesn't settle where the fix belongs: {}\"")
        .expect("the fix-location judge");
    assert!(
        prepare < judge,
        "follow-ups are settled before the question is judged"
    );
    assert!(src[prepare..judge].contains("followups::finish_delivered("));
}

#[test]
fn asking_to_connect_a_connected_jira_never_reaches_a_person() {
    let mut p = Picture::new(
        Kind::Ask,
        "fix_location",
        "the plan doesn't settle where the fix belongs",
    );
    p.connected = vec![connected_access::jira(SITE, true)];
    let ask = json!({
        "verdict": "ask",
        "question": "The audit is done and proposal.md holds 7 follow-up ticket drafts, but this run has no Jira access to file them in ABC. Can you connect Jira so CoderHelm files them (recommended), or would you rather file the 7 from proposal.md yourself?",
        "person_only": "access",
        "tried": ["Found no Jira connection in this run to file the tickets in ABC"],
        "options": ["Connect Jira to CoderHelm so it files the 7 tickets", "You file the 7 tickets yourself"],
        "default": "Connect Jira to CoderHelm so it files the 7 tickets",
        "deadline_hours": 0
    });
    let err = common::stop_judge::validate(&p, &ask).expect_err("refused");
    assert!(err.starts_with("Jira is already connected"), "{err}");
    let j = crate::passes::stop_judge::judged_from(p.clone(), vec![ask.clone(), ask]);
    assert!(j.proceeds(), "{:?}", j.verdict());
    // The judge reads what the run reaches.
    assert!(common::stop_judge::prompt(&p).contains("## Systems CoderHelm already reaches"));
    // A permission Jira named is still a person's, by its name.
    let perm = json!({
        "verdict": "ask",
        "question": "The Coderhelm app on the Jira site lacks the Create Issues permission in ABC. Can a Jira admin grant it to the atlassian-addons-project-access role?",
        "person_only": "access",
        "tried": ["Filed through the Jira app: it answered missing_permission CREATE_ISSUES"],
        "options": ["Grant Create Issues", "Leave the drafts on the ticket"],
        "default": "Leave the drafts on the ticket",
        "deadline_hours": 24
    });
    assert!(common::stop_judge::validate(&p, &perm).is_ok());
    // The worker's judge fills the picture from the team's connection.
    let judge_src = include_str!("../stop_judge.rs");
    assert!(
        judge_src.contains("picture.connected = super::followups::reaches(state, team_id).await;")
    );
}

#[test]
fn the_app_s_refusal_is_named_exactly() {
    let refused = json!({"error": "missing_permission", "missing": ["CREATE_ISSUES"], "words": "Create Issues", "project": "ABC"});
    let Filed::NotFiled(why) = followups::jira_filed(403, &refused, SITE, "ABC") else {
        panic!("refused");
    };
    assert!(
        why.contains("lacks the Jira project permission Create Issues in ABC"),
        "{why}"
    );
    assert!(why.contains("atlassian-addons-project-access"), "{why}");
    assert!(
        connected_access::asks_to_connect(&why, &[connected_access::jira(SITE, true)]).is_none()
    );
    // An app found filed by an earlier run is the same ticket, not a new one.
    let found = json!({"key": "ABC-901", "existing": true, "linked": {"ok": true, "existing": true}, "followup": 1});
    assert_eq!(
        followups::jira_filed(200, &found, SITE, "ABC"),
        Filed::Ticket {
            key: "ABC-901".into(),
            url: format!("{SITE}/browse/ABC-901"),
            existing: true,
            linked: Linked::Yes
        }
    );
}

#[test]
fn without_jira_the_drafts_go_to_issues_or_the_comment_and_the_run_completes() {
    assert_eq!(
        ft::tracker("#41", false, "", "acme/web", None),
        Tracker::Github {
            repo: "acme/web".into()
        }
    );
    let t = ft::tracker("#41", false, "", "acme/web", Some(false));
    let drafts = ft::drafts(&[], PROPOSAL);
    let c = ft::comment(&drafts, &[], &t);
    assert!(c.contains("They weren't filed because"), "{c}");
    assert!(c.contains("7. Promotion alt quality"), "{c}");
    assert!(!c.to_lowercase().contains("connect"), "{c}");
}
