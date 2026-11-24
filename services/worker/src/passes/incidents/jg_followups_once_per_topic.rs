//! jg. A Retry of a Jira audit ticket (whose earlier run had written
//! follow-up drafts) planned again instead of taking up that plan, filed
//! follow-up tickets from its first plan, then had that plan sent back by a
//! later check ("the plan stopped on a change outside the code that leaves
//! nothing for a person and waits for nothing"), planned again, filed more
//! tickets in other words (several of them the same work as tickets it had
//! just filed), and then failed with the same message. Duplicate tickets,
//! and a failed run whose deliverable (the audit and its follow-ups) was
//! done.
//!
//! The causes: the follow-ups were filed right after each plan was written,
//! before the plan's last check; a draft counted as filed only under the
//! exact same summary; the check read "a person has nothing to do" as "the
//! plan stopped short" even when the plan was the findings; and the earlier
//! run's plan was never read back, because its empty spec.md made every
//! cached-plan read come back "no plan".
//!
//! Guarantee: follow-ups are read when the plan is written
//! (`followups::prepare`) and filed only on the plan the run ends with or
//! implements (`followups::file`); before filing, the follow-ups already
//! filed for the source are read (records, CoderHelm's comment, the
//! tracker's list) and a draft whose topic one covers is not filed again
//! (`followup_tickets::near`, the small check for near ones); a plan whose
//! deliverable is its findings ends delivered and never plans again
//! (`followups::delivers_findings`); and a new run of the unchanged ticket
//! takes up such a plan (`followups::reusable_findings`, a cached plan with
//! an empty spec.md is whole).

use common::followup_tickets::{self as ft, Draft, Existing, Near};

use crate::passes::fix_location::{
    EndToEndStep, FixDecision, FixLocation, NextStep, Outcome, TicketIntent,
};
use crate::passes::followups;
use crate::passes::plan::PlanResult;

/// The six tickets the first plan filed (keys renumbered).
const FIRST: [(&str, &str); 6] = [
    (
        "ABC-92",
        "Stop using image filenames as alt text in imageFragmentFields",
    ),
    (
        "ABC-93",
        "Require alt text on imageWithAlt and add alt to mediaAsset images",
    ),
    (
        "ABC-94",
        "Replace generic placeholder alt text with data-derived or empty alt",
    ),
    (
        "ABC-95",
        "Skipper images: fall back to the skipper name for alt; empty when the name is adjacent",
    ),
    (
        "ABC-96",
        "Fix alt on the unused Hero, ContactCard, FeatureSection and SkippersCarousel components",
    ),
    ("ABC-97", "Confirm hardcoded alt=\"\" images are decorative"),
];

/// The second plan's four drafts, as it wrote them.
const SECOND: [&str; 4] = [
    "Remove unused Hero, FeatureSection and ContactCard prototype components",
    "Replace placeholder and whitespace alt text with data-derived or decorative alt",
    "Studio: warn when image alt is missing on imageWithAlt and skipper image",
    "Content-image alt: apply the same missing-alt warning to block alt fields and title fallbacks on the web",
];

fn filed() -> Vec<Existing> {
    FIRST
        .iter()
        .map(|(k, s)| Existing {
            key: k.to_string(),
            url: String::new(),
            summary: s.to_string(),
            marks: Vec::new(),
        })
        .collect()
}

/// The second plan's decision: the question answered, no outside change,
/// a content editor's step that is only a recommendation.
fn second_plan() -> FixDecision {
    FixDecision {
        fix_location: FixLocation::Unknown,
        intent: TicketIntent::Question,
        cause: "The three named components are unused prototypes that take plain URL strings; the real gaps are the optional alt on the shared schema and placeholder alts.".into(),
        end_to_end: vec![
            EndToEndStep {
                step: "File the 4 follow-up tickets linked to this one".into(),
                by: "this_pr".into(),
                ..Default::default()
            },
            EndToEndStep {
                step: "Fill alt text on the production records that have none".into(),
                by: "person".into(),
                owner: "Content editor".into(),
                ..Default::default()
            },
        ],
        next_step: Some(NextStep {
            action: "A content editor sets the alt text on the records that have none".into(),
            why_person: "the run has no write connection to the content store".into(),
            outcomes: vec![
                Outcome {
                    if_: "all are set".into(),
                    means: "the schema warning flags nothing".into(),
                },
                Outcome {
                    if_: "they stay empty".into(),
                    means: "the warning shows on those records".into(),
                },
            ],
        }),
        followup_tickets: SECOND
            .iter()
            .map(|s| Draft {
                summary: s.to_string(),
                description: String::new(),
            })
            .collect(),
        ..Default::default()
    }
}

const TASKS: &str = "NO_CODE_CHANGE: the decision for each component is in design.md, and the fixes are filed as 4 follow-up tickets.";

#[test]
fn a_reworded_plan_files_only_the_topic_not_yet_filed() {
    let earlier = filed();
    let drafts = ft::drafts(&second_plan().followup_tickets, "");
    let placed: Vec<Option<String>> = drafts
        .iter()
        .map(|d| match ft::near(d, &earlier) {
            Near::Same(e) => Some(e.key.clone()),
            Near::Maybe(c) => c.first().map(|e| e.key.clone()),
            Near::New => None,
        })
        .collect();
    assert_eq!(
        placed,
        vec![
            Some("ABC-96".to_string()),
            Some("ABC-94".to_string()),
            Some("ABC-93".to_string()),
            None,
        ],
        "three of the four are topics filed already; one is new"
    );
    // A retry of the first plan's own drafts finds them by draft id, and
    // by topic when worded again the same way.
    for (_, s) in FIRST {
        let d = Draft {
            summary: s.to_lowercase(),
            description: String::new(),
        };
        assert!(matches!(ft::near(&d, &earlier), Near::Same(_)), "{s}");
    }
    // A covered draft is listed with the ticket that covers it, not filed.
    let f = vec![
        ft::Filed::Covered {
            key: "ABC-96".into(),
            url: String::new(),
            title: FIRST[4].1.into(),
        },
        ft::Filed::Ticket {
            key: "ABC-98".into(),
            url: String::new(),
            existing: false,
            linked: ft::Linked::Yes,
        },
    ];
    let line = followups::settled_line(
        &drafts[2..4],
        &f,
        &ft::Tracker::Jira {
            project: "ABC".into(),
            site: String::new(),
        },
    );
    assert_eq!(
        line,
        "Filed 1 of 2 follow-up tickets: ABC-98; already filed from this ticket: ABC-96"
    );
}

#[test]
fn nothing_is_filed_until_the_plan_is_final() {
    let src = include_str!("../mod.rs");
    assert!(
        !src.contains("followups::settle("),
        "no filing right after a plan"
    );
    let prepare = src
        .find("followups::prepare(state, msg, run_id, decision, &proposal)")
        .expect("the plan's follow-ups are read");
    let files: Vec<usize> = src
        .match_indices("followups::file(")
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        files.len(),
        4,
        "the delivered end, the no-code ends, the implement pass"
    );
    assert!(files.iter().all(|&i| i > prepare));
    // The no-code ending files only once it knows the run ends with this
    // plan: never on the arm that plans again.
    let arm = src
        .find("NoCodeEnd::PlanAgain(why) => {")
        .expect("the plan-again arm");
    let next_arm = src[arm..].find("NoCodeEnd::Ended =>").unwrap() + arm;
    assert!(!src[arm..next_arm].contains("followups::file("));
    // The implement pass files right before it starts.
    let implement = src.find("// --- Pass 3: Implement ---").unwrap();
    let last = *files.last().unwrap();
    assert!(last < implement && implement - last < 600);
    // Reading a plan's follow-ups writes nothing: the pure part is all of it.
    let mut d = second_plan();
    let (_, due) = followups::prepare_plan(&mut d, &[], "");
    assert_eq!(due.map(|x| x.drafts.len()), Some(4));
}

#[test]
fn findings_are_the_deliverable_never_a_plan_that_stopped_short() {
    let d = second_plan();
    assert!(
        !followups::nothing_left(&d, TASKS, &[]),
        "the old reading: a person's step, so not done"
    );
    assert!(followups::delivers_findings(&d, TASKS, &[], true));
    // The check that failed the run steps aside for a findings plan.
    let src = include_str!("../mod.rs");
    let body = &src[src.find("async fn finish_without_code(").unwrap()..];
    let delivered = body
        .find("return NoCodeEnd::Delivered;")
        .expect("delivered");
    let plan_again = body
        .find("leaves nothing for a")
        .expect("the plan-again message");
    assert!(delivered < plan_again, "findings are checked first");
    // The delivered report asks nothing and says the person's step is a
    // recommendation.
    let report = followups::delivered_report(&d, "# Audit\n\n| table |", None);
    for never in [
        "leaves nothing for a person",
        "couldn't write a usable plan",
        "The one step I need from you",
    ] {
        assert!(!report.contains(never), "{never}: {report}");
    }
    assert!(
        report.contains("this ticket is done without it"),
        "{report}"
    );
}

#[test]
fn a_new_run_takes_up_the_findings_plan_instead_of_planning_again() {
    // The earlier run's plan: findings, drafts in proposal.md, no spec.
    let plan = PlanResult {
        proposal: "# Audit\n\n## Follow-up tickets to file\n1. Add alt validation to `imageWithAlt`.\n2. Fall back to the skipper name.\n".into(),
        design: String::new(),
        tasks: "NO_CODE_CHANGE: the audit and 2 follow-up tickets are in proposal.md.".into(),
        spec: String::new(),
        fix: Some(FixDecision {
            followup_tickets: Vec::new(),
            ..second_plan()
        }),
        ..Default::default()
    };
    assert!(followups::reusable_findings(&plan));
    let src = include_str!("../mod.rs");
    // An empty spec.md (or design.md) no longer makes the plan "missing".
    assert!(src.contains("if text.trim().is_empty() && !may_be_empty(name) {"));
    assert!(src.contains("matches!(name, \"spec.md\" | \"design.md\")"));
    // A new run of the unchanged ticket takes it up before planning.
    let taken = src
        .find("} else if let Some(plan) = findings_plan {")
        .expect("the findings plan is taken up");
    let chain = src.find("} else if let Some(plan) = own_plan {").unwrap();
    let fresh = src[taken..].find("plan::run(").unwrap() + taken;
    assert!(chain < taken && taken < fresh);
    assert!(src.contains(".filter(followups::reusable_findings)"));
}
