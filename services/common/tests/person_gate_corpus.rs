//! The regression corpus for the person gate: every person-facing item
//! CoderHelm has produced that the repository keeps (open items, outside
//! steps, "Needed from you" lines, assumptions, questions), from the
//! incident cases, the description eval and the outside-steps fixtures,
//! with every name made up. Each is run through `person_gate::decide` and
//! must land in its class, so a change that lets "do it yourself" items
//! leak back to people fails CI.
//!
//! And the replay rule, as for stuck runs: an item that reaches a person
//! carries one of the listed reasons, one action, who can do it and why
//! CoderHelm can't, and keeps the writing rules.

use common::person_gate::{self, Ask, Class, Context, Kind, Outcome, REASONS};

struct Case {
    kind: Kind,
    topic: &'static str,
    text: &'static str,
    how: &'static str,
    owner: &'static str,
    why_not: &'static str,
    options: &'static [&'static str],
    class: Class,
}

const fn case(kind: Kind, text: &'static str, class: Class) -> Case {
    Case {
        kind,
        topic: "",
        text,
        how: "",
        owner: "",
        why_not: "",
        options: &[],
        class,
    }
}

fn corpus() -> Vec<Case> {
    use Class::*;
    use Kind::*;
    vec![
        // The security update (incident bf).
        Case {
            how: "Run npm view left-pad@4.18.0 maintainers dist.integrity and compare the result to the lockfile hash.",
            ..case(OpenItem, "The new version's maintainers are not confirmed.", Do)
        },
        Case {
            topic: "fix location",
            ..case(Assumption, "The ticket is an explicit, self-contained security-update request, so CoderHelm opens a new pull request applying the planned lockfile bumps.", Settled)
        },
        // The small content validation (description eval, case 2).
        Case {
            how: "cd apps/studio && npm run deploy:staging",
            why_not: "CoderHelm's GitHub App is not allowed to start workflows in this repository. grant the app \"Actions: write\"",
            owner: "a repository admin",
            ..case(Step, "Deploy the staging Studio", Human)
        },
        Case {
            owner: "the reviewer",
            why_not: "how a page looks is a judgement only a person can make",
            ..case(Step, "Look at Studio: the page document, the title field", Review)
        },
        case(OpenItem, "Missing from the end-to-end checklist: no step lists the 23 documents over the limit. Settle: Add a checklist step owned by CoderHelm to query and post all 23 over-limit documents. Only a person can settle this: a product decision.", Do),
        // Outside-steps fixtures.
        Case {
            owner: "a content editor",
            why_not: "CoderHelm never writes production content",
            ..case(Step, "Replace the hero block's content on production (values: title = Classes)", Human)
        },
        Case {
            owner: "a content editor",
            why_not: "only an editor picks the classes shown",
            ..case(Step, "Replace the carousel block's content on production (values: title = Classes)", Human)
        },
        Case {
            why_not: "no connection can start the rebuild job",
            ..case(Step, "Run the full rebuild of the search index", Human)
        },
        Case {
            why_not: "no access to the indexer",
            ..case(Step, "Reindex the catalogue", Human)
        },
        Case {
            why_not: "no production CMS write connection exists. add a production write token",
            ..case(Step, "Replace the hero on production", Human)
        },
        // The settle stage's incident (ai).
        case(OpenItem, "The partner's webhook secret format is unconfirmed: no connection reaches their sandbox.", Human),
        case(OpenItem, "No owner is named for: rotate the signing key", Human),
        case(OpenItem, "The root overrides force parents onto a new major they never declared, and nothing shows those parents work. Confirm with the build results and the tests.", Do),
        // Questions the planner wanted to stop on (the question screen).
        Case {
            options: &["next to the component", "utils/"],
            ..case(Question, "Where should the new helper live?", Decided)
        },
        case(Question, "Use the existing `title_size` field instead of creating a new one?", Decided),
        case(Question, "What H1 and intro copy should each of the four venue pages use? (Default: '{Venue} Summer Camp Registration' plus one sentence.)", Decided),
        case(Question, "Should the card link open in a new tab?", Decided),
        Case {
            options: &["refund them in full", "offer a credit"],
            ..case(Question, "Should cancelled bookings be refunded, or given a credit on the account?", Human)
        },
        case(Question, "Should the export drop the legacy column, which deletes data older customers still read?", Human),
        // A credential.
        Case {
            why_not: "It needs the API key for the payments sandbox",
            ..case(Step, "Run the payments contract tests", Human)
        },
        // An approval the team requires.
        Case {
            why_not: "a team admin approves it on the Agent permissions page: the team's agent permission policy needs approval",
            ..case(Step, "Deploy the production Studio", Human)
        },
    ]
}

fn ask(c: &Case) -> Ask {
    Ask {
        kind: c.kind,
        topic: c.topic.into(),
        text: c.text.into(),
        how: c.how.into(),
        owner: c.owner.into(),
        why_not: c.why_not.into(),
        options: c.options.iter().map(|o| o.to_string()).collect(),
    }
}

#[test]
fn every_item_in_the_corpus_lands_in_its_class() {
    let mut wrong = Vec::new();
    for c in corpus() {
        let got = person_gate::decide(&ask(&c), &Context::default());
        if got.class() != c.class {
            wrong.push(format!(
                "{:?} (want {:?}): {} -> {got:?}",
                got.class(),
                c.class,
                c.text
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn an_item_reaches_a_person_only_with_a_listed_reason_and_one_action() {
    for c in corpus() {
        if let Outcome::Human(h) = person_gate::decide(&ask(&c), &Context::default()) {
            assert!(REASONS.contains(&h.reason()), "{}", c.text);
            assert!(!h.action().trim().is_empty(), "{}", c.text);
            assert!(!h.who().trim().is_empty(), "{}", c.text);
            assert!(!h.why_not().trim().is_empty(), "{}", c.text);
            if h.yes_no() {
                assert!(!h.recommendation().is_empty(), "{}", c.text);
                assert!(h.options().len() >= 2, "{}", c.text);
            }
            let line = h.line();
            assert!(person_gate::keeps_the_rules(&line).is_ok(), "{line}");
        }
    }
}

#[test]
fn the_corpus_keeps_people_items_rare() {
    let all = corpus();
    let human = all
        .iter()
        .filter(|c| person_gate::decide(&ask(c), &Context::default()).class() == Class::Human)
        .count();
    // Every human item in the corpus is one of the listed reasons; a rise
    // here means the gate let CoderHelm's own work through.
    assert!(human <= 12, "{human} of {} reach a person", all.len());
}
