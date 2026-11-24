//! sm. A ticket asked a booking vendor's portal page to drop its "open in a
//! new tab" link and to let the header scroll. The pull request changed a
//! handful of files, most of the lines tests. Over many commits its
//! description piled up: an "Earlier attempts" section, "Deferred: reverted
//! N file(s)" and "Waiting on CI" after the work had come back, a note that
//! the page scroll assertions were "dropped from this pull request" while
//! the diff still changed that page object, and implementation notes longer
//! than the change they described. A description is written about the
//! head, not about the run's history; another pull request's grew to
//! several times its diff and contradicted the diff in places.
//!
//! Guarantee: every build of a description, on every push, holds the
//! written text to the head (`description_facts::hold_scope`,
//! `common::scope_ledger`): sections about earlier attempts, pushes and
//! waits go (the revert and "CI is failing" lines stay for `ci_lines`,
//! which drops them once untrue); a sentence that says a file was reverted,
//! put back or dropped while the diff still changes it goes; open product
//! questions become out-of-scope defaults; and the written notes fit a
//! length scaled to the changed lines of code (tests and generated files
//! left out), with the run page for the rest. The visible part stays
//! Problem, Changes (with one Out of scope line), Risk and Verification.

use crate::passes::description_facts;
use common::pr_facts::{Facts, FileChange};
use common::scope_ledger;

fn facts() -> Facts {
    let files = [
        ("apps/web/src/components/blocks/portal-embed.tsx", 14),
        ("apps/web/src/lib/routing/content-renderers.tsx", 4),
        ("apps/web/src/utils/globalConstants.ts", 2),
        ("apps/web/src/test/components/portal-embed.test.tsx", 60),
        ("e2e/pages/slip-booking.ts", 40),
        ("e2e/tests/browse/slip-booking.spec.ts", 72),
    ];
    Facts {
        head: "fedcba9876543210fedcba9876543210fedcba98".into(),
        files: files
            .iter()
            .map(|(p, n)| FileChange {
                path: p.to_string(),
                status: "modified".into(),
                changed: *n,
                ..Default::default()
            })
            .collect(),
        files_complete: true,
        ..Default::default()
    }
}

fn written() -> String {
    let notes = "The embed keeps the vendor's own height logic and the frame fills the window, so the page header scrolls away with the content and nothing is frozen above it. ".repeat(12);
    format!(
        "## Changes\nRemoves the new-tab link from the portal page and lets the header scroll.\n\n\
         ## Earlier attempts\n- The first push froze the header; the review took the sizing out; a planned-work round put it back.\n\n\
         ## Deferred\n- Waiting on CI for the reverted files.\n- Reverted 5 file(s) to the last green head <!-- coderhelm:revert to=a1b2c3d4e paths=e2e/pages/slip-booking.ts -->\n\n\
         ## Notes\nThe page scroll assertions were dropped from this pull request (`slip-booking.ts`).\n\n{notes}\n\n{notes}\n\n{notes}"
    )
}

#[test]
fn the_description_describes_the_head_only() {
    let mut f = facts();
    let (text, corrections) = description_facts::hold_scope(&written(), &mut f);
    // History goes; the revert line stays for `ci_lines` to judge.
    assert!(!text.contains("Earlier attempts"), "{text}");
    assert!(!text.contains("froze the header"));
    assert!(!text.contains("Waiting on CI"));
    assert!(text.contains("coderhelm:revert"));
    // A claim the diff refutes goes.
    assert!(!text.contains("were dropped from this pull request"));
    assert_eq!(corrections.len(), 1, "{corrections:?}");
    assert_eq!(
        corrections[0].kind,
        common::pr_claims::Kind::ScopeContradiction
    );
    // The notes fit the change: 60 changed lines of code, not 1,100 words.
    let code = description_facts::code_lines(&f);
    assert_eq!(code, 60);
    let budget = scope_ledger::budget_words(code);
    assert!(
        scope_ledger::words(&text) <= budget + 20,
        "{} words",
        scope_ledger::words(&text)
    );
    assert!(text.ends_with(scope_ledger::REST));
    // Holding twice changes nothing more.
    let (again, _) = description_facts::hold_scope(&text, &mut f);
    assert_eq!(again, text);
}

#[test]
fn the_shaped_body_stays_four_short_sections() {
    let f = facts();
    let shaped = description_facts::shape(&written(), "ABC-2: Drop the new-tab link", f, None);
    assert!(!shaped.body.contains("Earlier attempts"));
    assert!(!shaped.body.contains("Waiting on CI"));
    let defects = common::pr_body::defects(&shaped.body, "fedcba9");
    assert!(
        !defects
            .iter()
            .any(|d| d.contains("Changes has") || d.contains("question")),
        "{defects:?}"
    );
}
