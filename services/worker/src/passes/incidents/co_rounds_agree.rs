//! co. A ticket asked two things of a booking vendor's portal page: remove
//! the link that opens booking in a new tab, and let the user scroll past
//! the header instead of freezing it. The build removed the link and sized the
//! frame one window tall (`className="h-svh!"`) so the header scrolls
//! away. CoderHelm's own review asked to take the sizing out; the review
//! round did. The check before the pull request then listed the sizing as
//! planned work not done, and a planned-work round put it back. The review
//! asked again, and the next review round took it out again: build, take
//! out, build, take out, in four commits, and the pull request ended up
//! saying it only removed the link while the ticket's second criterion was
//! gone. Several other branches had a review round and a planned-work
//! round undoing each other (diagnostics added, removed, added and removed
//! again on flaky-test fixes; a listener moved and moved back).
//!
//! Guarantee: planned work a review commit on the pull request took out is
//! not built again (`round_agree::taken_out`, `round_agree::sort`); the run
//! says which commit took it out. When an acceptance criterion of the
//! ticket asks for it, the ticket wins: it is built, and the item carries
//! the criterion so the reply to the review says why it stays. Plain Jira
//! criteria (a bare "Acceptance Criteria" line, one per line) are read
//! (`stop_judge::acceptance`).

use crate::passes::proof::Finding;
use crate::passes::round_agree::{self, answers_review};
use crate::passes::stop_judge::acceptance;

const EMBED: &str = "apps/web/src/components/blocks/portal-embed.tsx";

/// The build's change to the embed, up to the review commit's parent.
const BUILD: &str = "@@ -5,32 +5,24 @@\n-  linkLabel: string;\n }\n /**\n- * Vendor portal embed. The vendor's script sets the iframe's height.\n+ * Vendor portal embed. The iframe is exactly one small viewport\n+ * tall: the `h-svh!` stylesheet rule is `!important`, so it beats the inline\n+ * height the vendor's script writes. The page above the frame scrolls away.\n */\n-export function PortalEmbed({ src, title, linkLabel }: Props) {\n+export function PortalEmbed({ src, title }: Props) {\n   return (\n-      <a href={src} target=\"_blank\">{linkLabel}</a>\n       <iframe\n         src={src}\n+        className=\"h-svh!\"\n         style={{ width: \"100%\", border: 0 }}\n";

/// The review round: the sizing and its comment go, the link stays gone.
const REVIEW: &str = "@@ -8,9 +8,8 @@\n- * Vendor portal embed. The iframe is exactly one small viewport\n- * tall: the `h-svh!` stylesheet rule is `!important`, so it beats the inline\n- * height the vendor's script writes. The page above the frame scrolls away.\n+ * Vendor portal embed. The vendor's script sets the iframe's height.\n         src={src}\n-        className=\"h-svh!\"\n         style={{ width: \"100%\", border: 0 }}\n";

const TICKET: &str = "Acceptance Criteria\nRemove \u{201c}Open booking in a new tab\u{201d} text and link\nDon\u{2019}t freeze the \u{201c}Book a Slot\u{201d} header, allow the user to scroll down the iframe.\nexample page: https://www.example.com/library/slots/book";

const ITEM: &str = "`apps/web/src/components/blocks/portal-embed.tsx` drops the link but never adds `className=\"h-svh!\"` to the iframe and never rewrites the doc comment, so the frame is still sized by the vendor's script and the page never scrolls.";

fn taken() -> Vec<round_agree::TakenOut> {
    round_agree::taken_out(
        &[(EMBED.into(), BUILD.into())],
        &[(EMBED.into(), REVIEW.into())],
        "a1b2c3d4e5f",
    )
}

#[test]
fn the_review_commit_is_known_and_what_it_took_out_is_read() {
    assert!(answers_review("ABC-1: Address the review"));
    assert!(answers_review("Address the review comments\n\nbody"));
    assert!(!answers_review(
        "ABC-1: Finish the planned work still not done"
    ));
    let t = taken();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].path, EMBED);
    assert!(t[0].lines.iter().any(|l| l.contains("h-svh!")));
    // A trim of a line or two is no take-out.
    let trim = "@@ -1,2 +1,1 @@\n-        className=\"h-svh!\"\n";
    assert!(round_agree::taken_out(
        &[(EMBED.into(), BUILD.into())],
        &[(EMBED.into(), trim.into())],
        "x"
    )
    .is_empty());
}

#[test]
fn the_plain_jira_criteria_are_read() {
    let c = acceptance(TICKET);
    assert_eq!(c.len(), 3, "{c:?}");
    assert!(c[1].contains("allow the user to scroll"));
}

/// The ticket asks for the scrolling page: the item is built, with the
/// criterion, never dropped.
#[test]
fn a_criterion_keeps_what_the_review_took_out() {
    let sorted = round_agree::sort(&[ITEM.to_string()], &taken(), &acceptance(TICKET));
    assert!(sorted.skipped.is_empty(), "{sorted:?}");
    assert_eq!(sorted.build.len(), 1);
    assert!(sorted.build[0].contains("a1b2c3d"), "{}", sorted.build[0]);
    assert!(
        sorted.build[0].contains("allow the user to scroll"),
        "{}",
        sorted.build[0]
    );
}

/// No criterion asks for it: the planned-work round doesn't build it again,
/// and the check's findings drop it with the commit that took it out.
#[test]
fn without_a_criterion_the_review_take_out_stands() {
    let ticket = "Remove the new-tab link from the portal page.";
    let sorted = round_agree::sort(&[ITEM.to_string()], &taken(), &acceptance(ticket));
    assert!(sorted.build.is_empty(), "{sorted:?}");
    assert_eq!(sorted.skipped.len(), 1);
    assert!(round_agree::skipped_note(&sorted.skipped).contains("a1b2c3d"));
    let other = Finding {
        point: "external_fact".into(),
        detail: "The live page wasn't read.".into(),
        ..Default::default()
    };
    let planned = Finding {
        point: "planned_work".into(),
        detail: ITEM.into(),
        ..Default::default()
    };
    let (kept, skipped) =
        round_agree::sort_findings(vec![other.clone(), planned], &taken(), &acceptance(ticket));
    assert_eq!(kept, vec![other]);
    assert_eq!(skipped.len(), 1);
}

/// Planned work the review never touched is built as before, and with no
/// review commit nothing changes.
#[test]
fn work_no_review_took_out_is_built() {
    let other = "`apps/web/src/test/components/portal-embed.test.tsx` does not assert the iframe has no tabindex.";
    let sorted = round_agree::sort(&[other.to_string()], &taken(), &[]);
    assert_eq!(sorted.build, vec![other.to_string()]);
    let sorted = round_agree::sort(&[ITEM.to_string()], &[], &[]);
    assert_eq!(sorted.build, vec![ITEM.to_string()]);
}
