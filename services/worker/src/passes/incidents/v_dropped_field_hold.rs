//! v. A run replaced two old checkboxes on its blocks with one shared
//! setting, and shipped an expand migration and a separate contract
//! migration that removes the old fields. The new-feature rules saw the
//! schema drop both fields, found no staging CMS connection, and listed
//! "remove the field from every document that still has it" for both as a
//! check the change didn't pass. The pull request was held as a draft on
//! work its own contract migration does, for fields that are inert anyway:
//! the CMS ignores a field its schema no longer has, and the code stopped
//! reading it.
//!
//! The same run also spent three fresh steps on one planned file the coder
//! showed needs no change (every query already fetched the new field), and
//! rebuilt its stacked branch at the start of each step though the branch
//! it was stacked on never moved.
//!
//! Guarantees:
//! - A field a migration in the change removes or sets is never staging
//!   work for a person; a dropped field never holds the pull request.
//! - A line written the old way, listing only dropped fields, is settled:
//!   the pull request is not held on it and the line goes when it is marked
//!   ready.
//! - A planned file the coder, told to change it, says needs no change is
//!   listed for a person and ends the rounds.
//! - A branch already holding its stacked branch's head is not rebuilt.

use std::collections::BTreeSet;

use crate::passes::feature::content::{self, Blocked, Need};
use crate::passes::proof;
use crate::passes::ready::{self, Ci, Decision, Facts};

fn dropped(doc_type: &str, field: &str) -> Need {
    Need::Unset {
        doc_type: doc_type.into(),
        field: field.into(),
    }
}

/// The Open items line as the run wrote it.
fn the_line_as_written() -> String {
    let (why, grant) = (
        "there is no staging CMS write connection for o/web".to_string(),
        "add one in Settings, Repositories, o/web, Staging CMS".to_string(),
    );
    let lines: Vec<String> = [
        dropped("heroCenteredText", "isHeadingOne"),
        dropped("activitiesCarousel", "isHeadingTwo"),
    ]
    .iter()
    .map(|n| {
        Blocked {
            what: n.line(),
            why_person: why.clone(),
            grant: grant.clone(),
        }
        .line()
    })
    .collect();
    let why = format!(
        "the change makes the staging content its end-to-end tests read out of date. Change \
         this content on staging: {}",
        lines.join("; ")
    );
    proof::gaps_section(&[proof::check_item("The new-feature rules", &why)])
}

fn facts(body: &str) -> Facts {
    Facts {
        draft: true,
        title: "Add a Title Size setting to the venue page blocks".into(),
        body: format!("## Summary\nOne Title Size setting for every venue block.\n\n{body}"),
        ci: Ci {
            green: true,
            why: String::new(),
        },
        approved: true,
        ..Default::default()
    }
}

#[test]
fn the_changes_own_migration_and_dropped_fields_are_never_staging_work() {
    let needs = vec![
        dropped("heroCenteredText", "isHeadingOne"),
        dropped("activitiesCarousel", "isHeadingTwo"),
        Need::Require {
            doc_type: "heroCenteredText".into(),
            field: "headingLevel".into(),
            default: None,
        },
    ];
    let removes: BTreeSet<String> = ["isHeadingOne".to_string()].into();
    let sets: BTreeSet<String> = ["headingLevel".to_string()].into();
    // No connection: nothing is left for a person.
    assert!(content::still_needed(needs.clone(), &removes, &sets, false).is_empty());
    // With one, CoderHelm still removes a dropped field the migration doesn't.
    assert_eq!(
        content::still_needed(needs.clone(), &removes, &sets, true),
        vec![dropped("activitiesCarousel", "isHeadingTwo")]
    );
    // A required field nothing sets is still real work.
    assert_eq!(
        content::still_needed(needs, &BTreeSet::new(), &BTreeSet::new(), false).len(),
        1
    );
}

#[test]
fn a_line_listing_only_dropped_fields_never_holds_the_pull_request() {
    let body = the_line_as_written();
    assert!(body.contains(proof::BLOCKED_MARK), "{body}");
    let d = ready::decide(&facts(&body));
    assert_eq!(d, Decision::Ready { open: vec![] }, "held on: {body}");
    let after = ready::body_for(&facts(&body).body, &d).expect("the line goes");
    assert!(!after.contains("isHeadingOne"), "{after}");
    assert!(after.contains("One Title Size setting"), "{after}");

    // A line that also names content a person must make still holds.
    let real = body.replace(
        "remove `isHeadingTwo` from every `activitiesCarousel` document that still has it",
        "give every `heroCenteredText` document a value for the now required `headingLevel`",
    );
    assert!(matches!(
        ready::decide(&facts(&real)),
        Decision::Held { .. }
    ));
}

#[test]
fn a_file_the_coder_shows_needs_no_change_ends_the_rounds() {
    // The coder's own words in the run.
    let notes = "What I checked on the branch:\n\
        - **`query.ts` needs no change:** every projection for the 10 blocks starts with `...`, \
        so `headingLevel` is already fetched.";
    let why =
        crate::passes::plan_coverage::declined_by_coder(notes, "apps/web/src/lib/sanity/query.ts")
            .expect("the coder said so");
    assert!(why.contains("already fetched"), "{why}");
    assert!(crate::passes::plan_coverage::declined_by_coder(
        "Updated `query.ts` to fetch headingLevel.",
        "apps/web/src/lib/sanity/query.ts"
    )
    .is_none());
}

#[test]
fn a_branch_holding_its_stacked_branchs_head_is_not_rebuilt() {
    use crate::passes::stack::holds_head;
    assert!(holds_head(
        &serde_json::json!({"ahead_by": 2, "behind_by": 0})
    ));
    assert!(!holds_head(
        &serde_json::json!({"ahead_by": 2, "behind_by": 3})
    ));
    assert!(!holds_head(&serde_json::json!({})));
}
