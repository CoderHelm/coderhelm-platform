//! bf. A security update's run page and pull request handed its reader a
//! pile of CoderHelm's own work. Under Open items: "Run `npm view
//! <package>@<version> maintainers dist.integrity` and compare the result to
//! the lockfile hash"; then nine near-identical lines, "Assumed instead of
//! asking: fix location: The ticket is an explicit, self-contained
//! security-update request, so CoderHelm opens a new pull request ...
//! Settle: Correct it in a comment on this pull request if it's wrong;
//! otherwise delete this line. Only a person can settle this: a product
//! decision", each cut off with an ellipsis. "Needed from you" repeated
//! six of them, "and 3 more", then "Review: decide whether to merge 48
//! dependency updates. Feedback wanted: does the evidence beside each
//! major version jump cover how this code uses those packages". The owner:
//! "it is a senior developer ... it has more access than I do; it should be
//! able to do it itself".
//!
//! The causes: the settle stage classed every judge assumption as a
//! product decision for a person (`settle::classify`); each proceed the
//! judge recorded on an attempt became its own item, kept apart unless one
//! contained the other word for word (`stop_judge::assumption_items`); the
//! item told the reader to delete CoderHelm's own line; the description
//! read "decide" and "a person" in an item as "needs a person"
//! (`description_facts::listed_open`); lines were cut at a length with an
//! ellipsis (`proof::sentence`); nothing ran a registry lookup; the review
//! line asked for CoderHelm's analysis (`pr_facts::review_line`).
//!
//! A second pull request, a small content validation, asked for a staging
//! deploy (the command three times), a look at a field "only a person can
//! judge", and a checklist step "owned by CoderHelm" as "a product
//! decision". A third run asked the same page-copy question twice while
//! already building its default.
//!
//! Guarantee: every item headed to a person goes through one gate
//! (`common::person_gate`). What CoderHelm can do or verify (a registry
//! lookup, a step it owns) it does and records; what the ticket, the
//! repository or the team settles is a note with its source; a choice
//! nothing settles is decided and noted; a sign-off belongs to the review
//! line; only a listed reason reaches "Needed from you", with one action,
//! and a hard-to-reverse choice there is a yes/no on CoderHelm's
//! recommendation. One entry per decision, never "delete this line", never
//! an ellipsis, and the review line states CoderHelm's conclusion.

use common::decision_log::Record as Decision;
use common::person_gate::{self, Class, Outcome};
use common::pr_facts::{self, DepLine, DependencyFacts, Facts, MajorEvidence};
use common::run_questions::Question;
use common::stop_rules::screen::{self, Form, QuestionRead, StopKind};

use crate::passes::proof::{self, Finding};
use crate::passes::settle::{self, Action, Class as SettleClass, Facts as SettleFacts, Record};
use crate::passes::stop_judge;
use crate::passes::triage::TicketIntent;

/// The nine wordings, as the judge recorded them on nine attempts.
const NINE: [&str; 9] = [
    "The ticket is an explicit, self-contained security-update request, so CoderHelm opens a new pull request applying the planned lockfile bumps.",
    "The ticket asks for every bump in one pull request; the earlier pull request was closed unmerged, so a new one is opened.",
    "A new pull request is opened because the ticket is an explicit request and the earlier one was closed unmerged.",
    "The ticket is an explicit request for the security updates; CoderHelm opens a new pull request for them.",
    "The earlier pull request was closed unmerged, so a fresh one carries the bumps the ticket asks for.",
    "Opening a new pull request: the ticket is an explicit, self-contained request.",
    "The ticket asks for all bumps together; a new pull request is opened since the earlier one closed unmerged.",
    "The ticket is explicit; CoderHelm applies the planned lockfile bumps on a new pull request.",
    "The ticket asks for every bump in one pull request, and the earlier one was closed unmerged, so CoderHelm opens a new one.",
];

const REGISTRY_ITEM: &str =
    "The new version of left-pad was published recently and its maintainers are not confirmed.";
const REGISTRY_SETTLE: &str = "Run npm view left-pad@4.18.0 maintainers dist.integrity and compare the result to the lockfile hash.";

fn proceeds() -> Vec<Decision> {
    NINE.iter()
        .map(|d| Decision {
            kind: "stop".into(),
            step: "fix_location".into(),
            verdict: "proceed".into(),
            detail: d.to_string(),
            ..Default::default()
        })
        .collect()
}

fn dep_run() -> SettleFacts {
    SettleFacts {
        intent: TicketIntent::Chore,
        planned_by_rules: true,
        dep_change: true,
        checks_owed: false,
    }
}

fn registry_item() -> Finding {
    Finding {
        point: "external_fact".into(),
        detail: REGISTRY_ITEM.into(),
        settle: REGISTRY_SETTLE.into(),
        ..Default::default()
    }
}

#[test]
fn nine_wordings_of_a_decision_the_ticket_settled_are_one_note_and_no_item() {
    let items = stop_judge::assumption_items(&proceeds());
    assert_eq!(items.len(), 1, "{items:?}");
    assert_eq!(items[0].detail, NINE[8], "the latest wording is kept");
    let settled = settle::apply(items, &dep_run(), &Record::default());
    assert!(settled.open.is_empty(), "{:?}", settled.open);
    assert_eq!(settled.noted.len(), 1);
    assert!(
        settled.noted[0].contains("settled by the ticket"),
        "{:?}",
        settled.noted
    );
    assert_eq!(settled.counts.human, 0);
    // Even fed nine times to the stage without the judge's merge, it is one.
    let raw: Vec<Finding> = NINE
        .iter()
        .map(|d| Finding {
            point: proof::JUDGE_ASSUMPTION.into(),
            topic: "fix location".into(),
            detail: d.to_string(),
            ..Default::default()
        })
        .collect();
    let again = settle::apply(raw, &dep_run(), &Record::default());
    assert!(again.open.is_empty());
    assert_eq!(again.noted.len(), 1, "{:?}", again.noted);
}

#[test]
fn a_registry_lookup_is_run_by_coderhelm_and_recorded() {
    let f = registry_item();
    assert_eq!(
        settle::classify(&f, &dep_run()),
        SettleClass::Run(vec![Action::Registry])
    );
    assert!(settle::plan(std::slice::from_ref(&f), &dep_run()).registry);
    // The registry agreed: the item is closed with the result.
    let rec = Record {
        registry_checked: true,
        registry: vec![
            "`left-pad@4.18.0`: lockfile integrity matches the registry; maintainers alice".into(),
        ],
        ..Default::default()
    };
    let settled = settle::apply(vec![f.clone()], &dep_run(), &rec);
    assert!(settled.open.is_empty(), "{:?}", settled.open);
    assert_eq!(settled.closed.len(), 1);
    let section = settle::settled_section(&settled, &rec);
    assert!(
        section.contains("lockfile integrity matches the registry"),
        "{section}"
    );
    // It disagreed: a finding CoderHelm holds the pull request on, never a
    // person's chore.
    let bad = Record {
        registry_checked: true,
        registry_findings: vec!["`left-pad@4.18.0`: lockfile integrity `sha512-AAA` differs from the registry's `sha512-BBB`".into()],
        ..Default::default()
    };
    let settled = settle::apply(vec![f], &dep_run(), &bad);
    assert!(settled.open.iter().any(|o| o.point == "security"));
    let text = proof::gaps_section(&settled.open);
    assert!(!text.contains("npm view"), "{text}");
    assert!(!text.contains("Needed from you"), "{text}");
}

#[test]
fn nothing_written_asks_to_delete_a_line_or_is_cut_off() {
    let mut items = stop_judge::assumption_items(&proceeds());
    items.push(registry_item());
    items.push(Finding {
        point: "assumption".into(),
        detail: "The export code is assumed to tolerate the new parser's stricter dates, which matters for every scheduled export the team runs across the nightly and weekly jobs, and for the partner feeds that read the same files after they land in the bucket each morning.".into(),
        ..Default::default()
    });
    let settled = settle::apply(items, &dep_run(), &Record::default());
    let text = proof::gaps_section(&settled.open);
    assert!(person_gate::keeps_the_rules(&text).is_ok(), "{text}");
    for gone in [
        "Assumed instead of asking",
        "Only a person can settle",
        "Settle: Run npm view",
    ] {
        assert!(!text.contains(gone), "{gone}: {text}");
    }
}

#[test]
fn the_review_line_states_coderhelms_conclusion_and_asks_only_to_merge() {
    let f = Facts {
        head: "a1b2c3d4e5f6".into(),
        dependency: Some(DependencyFacts {
            lines: vec![DepLine {
                package: "tiny-yaml".into(),
                from: "1.2.0".into(),
                to: "2.0.0".into(),
                moved: true,
                major: true,
                remaining: vec![],
            }],
            majors: vec![MajorEvidence {
                package: "tiny-yaml".into(),
                from: "1.2.0".into(),
                to: "2.0.0".into(),
                evidence: vec!["2 call sites use `parse`, unchanged in 2.0; the type check and 41 tests passed on the new version".into()],
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    let lead = pr_facts::lead(&f);
    assert!(lead.contains("**Review:** decide whether to merge 1 dependency update. CoderHelm's conclusion: the major version jump is safe for this code"), "{lead}");
    assert!(!lead.contains("Feedback wanted"), "{lead}");
    assert!(lead.contains("**Needed from you:** Nothing."), "{lead}");
}

#[test]
fn a_permission_only_an_owner_grants_still_reaches_a_person_with_one_action() {
    let f = Finding {
        point: "external_fact".into(),
        detail: "The release workflow can't be started: CoderHelm's GitHub App is not allowed to start workflows in this repository; an organization owner grants \"Actions: write\".".into(),
        ..Default::default()
    };
    let settled = settle::apply(vec![f], &dep_run(), &Record::default());
    assert_eq!(settled.open.len(), 1);
    assert_eq!(settled.counts.human, 1);
    assert!(
        settled.open[0].needs.contains("CoderHelm can't:"),
        "{:?}",
        settled.open
    );
    let line = proof::gaps_section(&settled.open);
    assert!(person_gate::keeps_the_rules(&line).is_ok(), "{line}");
}

#[test]
fn the_second_pull_requests_three_items() {
    use common::pr_steps::{State, Step, When};
    // A deploy CoderHelm has no credentials for: one missing thing, the
    // command once.
    let deploy = Step {
        what: "Deploy the staging Studio".into(),
        how: "cd apps/studio && npm run deploy:staging".into(),
        when: When::AfterMerge,
        owner: "a person on the team".into(),
        state: State::Person {
            why: "the repository has no deploy workflow CoderHelm can start".into(),
            unblock: String::new(),
        },
    };
    assert!(deploy.needs_person());
    let ctx = person_gate::Context {
        can_deploy: true,
        ..Default::default()
    };
    assert_eq!(person_gate::decide(&deploy.ask(), &ctx).class(), Class::Do);
    // A look at a field: the reviewer's sign-off, folded into the review.
    let look = Step {
        what: "Look at Studio: the landing document, hero Sub Title field".into(),
        how: String::new(),
        when: When::Any,
        owner: "the reviewer".into(),
        state: State::Person {
            why: "how a page looks is a judgement only a person can make".into(),
            unblock: String::new(),
        },
    };
    assert!(!look.needs_person());
    assert!(look.for_review().is_some());
    let mut f = Facts {
        head: "9f8e7d6c".into(),
        steps: vec![deploy, look.clone()],
        review_notes: vec![look.for_review().unwrap()],
        ..Default::default()
    };
    let lead = pr_facts::lead(&f);
    assert!(
        lead.contains("Your merge decision also signs off: Look at Studio"),
        "{lead}"
    );
    assert_eq!(pr_facts::needed_lines(&f).len(), 1, "{lead}");
    assert_eq!(lead.matches("npm run deploy:staging").count(), 1, "{lead}");
    // A checklist step owned by CoderHelm is CoderHelm's work.
    let e2e = Finding {
        point: "end_to_end".into(),
        detail: "No step lists the 23 documents over the limit.".into(),
        settle:
            "Add a checklist step owned by CoderHelm to query and post all 23 over-limit documents."
                .into(),
        ..Default::default()
    };
    let feature = SettleFacts {
        intent: TicketIntent::Feature,
        ..Default::default()
    };
    assert_eq!(settle::classify(&e2e, &feature), SettleClass::Own);
    f.steps.clear();
    assert!(pr_facts::needed_lines(&f).is_empty());
}

fn question(text: &str, suggestions: &[&str]) -> Question {
    Question {
        id: "q1".into(),
        question: text.into(),
        why: "the four pages need a heading".into(),
        suggestions: suggestions.iter().map(|s| s.to_string()).collect(),
        checked: vec!["the venue pages' current headings".into()],
        changes: "the heading text on four pages".into(),
        ..Default::default()
    }
}

#[test]
fn page_copy_with_a_default_is_decided_never_asked_and_never_twice() {
    let read = QuestionRead {
        id: "q1".into(),
        kind: StopKind::NeedsPerson,
        ..Default::default()
    };
    let qs = vec![
        question(
            "What H1 and intro copy should each of the four venue pages use? (Default: '{Venue} Summer Camp Registration' plus one sentence.)",
            &[],
        ),
        question(
            "Which heading and intro sentence should the four venue pages show? (Default: '{Venue} Summer Camp Registration' plus one sentence.)",
            &[],
        ),
    ];
    let s = screen::screen(&qs, &[read.clone(), read], false, Form::Stop, &|_| true);
    assert!(s.ask.is_empty(), "{:?}", s.ask);
    assert!(s.dropped.iter().all(|(_, why)| why == screen::WHY_DECIDE));
}

#[test]
fn an_irreversible_customer_facing_choice_is_one_yes_no_with_the_work_done() {
    let read = QuestionRead {
        id: "q1".into(),
        kind: StopKind::NeedsPerson,
        ..Default::default()
    };
    let q = question(
        "Should the migration email customers whose bookings move to the new venue?",
        &["email them once", "send nothing"],
    );
    let s = screen::screen(
        std::slice::from_ref(&q),
        &[read],
        false,
        Form::Stop,
        &|_| true,
    );
    assert_eq!(s.ask.len(), 1);
    match person_gate::decide(&screen::question_ask(&q), &Default::default()) {
        Outcome::Human(h) => {
            assert!(h.yes_no());
            assert!(h
                .line()
                .contains("CoderHelm recommends email them once and has done the work that way"));
        }
        o => panic!("{o:?}"),
    }
}

#[test]
fn a_reversible_choice_with_no_guidance_is_decided_and_noted_once() {
    let f = Finding {
        point: proof::JUDGE_ASSUMPTION.into(),
        topic: "test approach".into(),
        detail: "The new check is tested with a unit test beside the validator rather than an end-to-end test.".into(),
        ..Default::default()
    };
    let settled = settle::apply(vec![f.clone(), f], &dep_run(), &Record::default());
    assert!(settled.open.is_empty());
    assert_eq!(settled.noted.len(), 1);
    assert!(
        settled.noted[0].starts_with("Chose "),
        "{:?}",
        settled.noted
    );
    let section = settle::settled_section(&settled, &Record::default());
    assert_eq!(section.matches("Chose ").count(), 1, "{section}");
    // Not in the review line: it doesn't change what merges.
    let lead = pr_facts::lead(&Facts::default());
    assert!(!lead.contains("Chose "), "{lead}");
}
