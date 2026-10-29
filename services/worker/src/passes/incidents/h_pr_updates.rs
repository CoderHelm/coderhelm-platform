//! h. A new-feature PR that CoderHelm updated over several runs: its
//! description kept describing an earlier state (a failed suite from an
//! older commit), the same ticket comment was posted twice, one question was
//! asked on every update, an e2e spec skipped itself when the CMS content
//! was missing, and a content note said it "could not look up" the content
//! while the CMS read API was public.
//!
//! Guarantee: each is caught by its own check. The description is rewritten
//! when a round lands (older rounds keep their headline only); a notice is
//! posted once per ticket; a question is asked once per ticket; a spec that
//! skips on missing content fails the feature checklist; a note that didn't
//! look while it could goes back to the planner.

use super::fixture::*;
use crate::passes::feature::checklist;
use crate::passes::outside_work::{self, PlanFacts};
use crate::passes::review_fix::evidence;
use common::ticket_state::{self, Notice, NoticeAction};

#[test]
fn the_description_drops_an_older_rounds_failure_when_a_newer_round_lands() {
    let failed = evidence::Round {
        date: "2026-10-26".into(),
        commit: "aaa1111".into(),
        headline: "Register the card block".into(),
        lines: vec!["Full test suite FAILED (exit 1): `pnpm test` (12 passed, 1 failed)".into()],
    };
    let passed = evidence::Round {
        date: "2026-10-27".into(),
        commit: "bbb2222".into(),
        headline: "Map the card block to its component".into(),
        lines: vec!["Full test suite passed: `pnpm test` (13 passed)".into()],
    };
    let section = evidence::section(&evidence::section("", &failed), &passed);
    let body = format!("## Summary\nAdds the card block.\n\n{section}\n\n## Risk\nLow");
    let mut end = Ending::default();
    end.said(evidence::current(&body));
    assert!(end.says("Register the card block"));
    assert!(end.says("Full test suite passed"));
    end.never_says("FAILED (exit 1)");
    // A round that lands rewrites the description from the whole diff.
    let resume = include_str!("../resume.rs");
    let round = fn_src(resume, "\nasync fn run_fix_round(");
    let pushed = round.find("Landed::Pushed { files }").unwrap();
    assert!(round[pushed..].contains("pr::refresh_description("));
}

#[test]
fn the_same_ticket_comment_from_two_runs_is_posted_once() {
    let key = "outside_note";
    let first = "**Content:** add the card block on `/events` (run https://app.example.com/runs/01JA, 2026-10-27T10:00:00Z)";
    let second = "**Content:** add the card block on `/events` (run https://app.example.com/runs/01JB, 2026-10-27T10:04:00Z)";
    let posted = vec![Notice {
        key: key.into(),
        text_hash: ticket_state::notice_hash(key, first),
        ..Default::default()
    }];
    assert!(matches!(
        ticket_state::notice_action(&posted, key, &ticket_state::notice_hash(key, second)),
        NoticeAction::Skip(_)
    ));
}

#[test]
fn a_question_is_asked_on_the_ticket_once() {
    let asked = vec![Notice {
        key: super::super::question_key("Should the card link open in a new tab?"),
        ..Default::default()
    }];
    let next_update = vec![
        "Should the card link open in a **new tab**?".to_string(),
        "Which image ratio should the card use on mobile?".to_string(),
    ];
    let fresh = super::super::new_questions(&next_update, &asked);
    assert_eq!(fresh, vec![&next_update[1]]);
}

#[test]
fn an_e2e_spec_that_skips_on_missing_content_is_caught() {
    let patch = "@@ -1,3 +1,9 @@\n test('card on events', async ({ page }) => {\n+  const card = await page.locator('[data-block=card]').count();\n+  if (!card) test.skip(true, 'no card content on staging');\n+  await expect(page.locator('[data-block=card]')).toBeVisible();\n });\n";
    let skips = checklist::data_skips("e2e/events.spec.ts", patch);
    assert_eq!(skips.len(), 1, "{skips:?}");
    assert!(skips[0].starts_with("e2e/events.spec.ts"));
    // A skip for a browser is not about content.
    let platform = "@@ -1 +1,2 @@\n+  test.skip(browserName === 'webkit', 'no hover');\n";
    assert!(checklist::data_skips("e2e/events.spec.ts", platform).is_empty());
}

#[test]
fn a_note_that_could_not_look_up_readable_content_goes_back() {
    let tree: Vec<String> = monorepo().tree;
    let files = vec![(
        ".env.staging".to_string(),
        "NEXT_PUBLIC_SANITY_PROJECT_ID=proj9\nNEXT_PUBLIC_SANITY_DATASET=staging\n".to_string(),
    )];
    let a = outside_work::abilities(&tree, &files, &[], false);
    assert!(a.source.is_some(), "the CMS read API is known");
    let note = crate::passes::fix_location::OutsideChange {
        system: "the CMS".into(),
        change: "Add a card block to the events page".into(),
        existing: "I could not look up existing content; this assumes none exists.".into(),
        ..Default::default()
    };
    let gaps = outside_work::gaps(&note, &a, PlanFacts::default()).join("\n");
    assert!(gaps.contains("the CMS read API is public"), "{gaps}");
    assert!(gaps.contains("query_content"));
}
