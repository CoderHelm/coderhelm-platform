//! bx. A person pushed a fixed end-to-end spec to CoderHelm's pull request:
//! they took out assertions that failed and fixed a selector, and the suite
//! passed in full on staging. CoderHelm's next round looked at an older
//! version of the spec and wrote the removed assertions back in; a second
//! commit did it again from another old version. Most of the tests broke.
//! The branch guards stopped only resets and force pushes, and the
//! snapshot check only writes over commits the pass had never read. This
//! pass had read the person's commit and wrote over it anyway.
//!
//! Guarantee: every write CoderHelm commits (`write_file`, `batch_write`,
//! so every edit, restore, revert and fix round) is held against every
//! commit a person made on the branch since its base, not only the ones
//! since CoderHelm's last commit. A part that puts back a line a person
//! removed, or takes out one they added, is not committed; the rest is.
//! The pull request gets one note, edited in place, that names what was
//! kept and asks. A round a person asked for is their answer. Red checks
//! never roll the branch back to an older head, so no revert writes over
//! a person's lines either.

use crate::clients::github::{person_commits, FileOp};
use common::person_lines::{guard, note, refusal, PersonEdit, Undo, NOTE_MARKER};
use serde_json::json;

const SPEC: &str = "e2e/tests/browse/slip-booking.spec.ts";

/// The spec as the person left it: two failing assertions out, the
/// book button found by its new name.
const PERSONS_SPEC: &str = "\
import { test, expect } from '@playwright/test';

test('booking page embeds the portal', async ({ page }) => {
  await page.goto('/marina-a/slips/book');
  await expect(page.frameLocator('iframe').first()).toBeTruthy();
  await page.getByRole('button', { name: 'Book now' }).click();
});
";

/// The person's commit, as GitHub's patch shows it.
const PERSONS_PATCH: &str = "\
@@ -3,8 +3,6 @@ import { test, expect } from '@playwright/test';
 test('booking page embeds the portal', async ({ page }) => {
   await page.goto('/marina-a/slips/book');
-  await expect(page.getByRole('heading', { name: 'Slip Booking' })).toBeVisible();
-  await expect(page.getByRole('dialog', { name: 'Book' }).getByRole('heading', { name: 'Price Per Night' })).toBeVisible();
   await expect(page.frameLocator('iframe').first()).toBeTruthy();
-  await page.getByRole('button', { name: 'Book' }).click();
+  await page.getByRole('button', { name: 'Book now' }).click();
 });
";

/// What the round wrote: an older version of the spec, which puts the
/// assertions and the old selector back, plus one change of its own.
const ROUNDS_SPEC: &str = "\
import { test, expect } from '@playwright/test';

test('booking page embeds the portal', async ({ page }) => {
  await page.goto('/marina-a/slips/book');
  await expect(page.getByRole('heading', { name: 'Slip Booking' })).toBeVisible();
  await expect(page.getByRole('dialog', { name: 'Book' }).getByRole('heading', { name: 'Price Per Night' })).toBeVisible();
  await expect(page.frameLocator('iframe').first()).toBeTruthy();
  await page.getByRole('button', { name: 'Book' }).click();
});

test('booking page has a title', async ({ page }) => {
  await page.goto('/marina-a/slips/book');
  await expect(page).toHaveTitle(/Book/);
});
";

fn persons_edit() -> PersonEdit {
    PersonEdit::from_patch("abc4567b7e", "a-person", SPEC, PERSONS_PATCH)
}

#[test]
fn a_round_that_writes_back_what_a_person_removed_commits_only_its_own_change() {
    let g = guard(SPEC, Some(PERSONS_SPEC), ROUNDS_SPEC, &[persons_edit()]);
    // The person's fix stands.
    assert!(!g.content.contains("Price Per Night"), "{}", g.content);
    assert!(!g.content.contains("'Slip Booking'"));
    assert!(g.content.contains("name: 'Book now'"));
    assert!(!g.content.contains("{ name: 'Book' }).click()"));
    // The round's own new test goes through.
    assert!(g.content.contains("booking page has a title"));
    assert!(g
        .kept
        .iter()
        .all(|k| k.undo == Undo::RestoresWhatTheyRemoved || k.undo == Undo::RemovesTheirLine));
    assert!(g.kept.iter().any(|k| k.line.contains("Price Per Night")));
}

#[test]
fn a_write_that_only_reverts_the_person_is_refused_with_why() {
    // The second commit: the spec restored from an older commit, nothing
    // else.
    let older = ROUNDS_SPEC
        .split("\ntest('booking page has a title'")
        .next()
        .unwrap();
    let g = guard(SPEC, Some(PERSONS_SPEC), older, &[persons_edit()]);
    assert_eq!(g.content, PERSONS_SPEC, "nothing of it is left to commit");
    let why = refusal(&g.kept);
    assert!(why.starts_with("Not committed"), "{why}");
    assert!(why.contains("a-person") && why.contains("abc4567"), "{why}");
    let n = note(&g.kept);
    assert!(n.starts_with(NOTE_MARKER));
    assert!(n.lines().count() <= 6, "{n}");
    assert!(n.contains("Reply here"), "{n}");
}

#[test]
fn the_persons_commit_counts_after_coderhelm_commits_on_top_of_it() {
    // Base, the person's fix, then CoderHelm's first revert: the second
    // revert must still see the person's commit.
    let commits = vec![
        json!({"sha": "1111111", "author": {"login": "coderhelm[bot]", "type": "Bot"}, "parents": [{}]}),
        json!({"sha": "abc4567b7e", "author": {"login": "a-person", "type": "User"}, "parents": [{}]}),
        json!({"sha": "2222222", "author": {"login": "coderhelm[bot]", "type": "Bot"}, "parents": [{}]}),
        // A merge of the base is not the person's work.
        json!({"sha": "3333333", "author": {"login": "a-person", "type": "User"}, "parents": [{}, {}]}),
    ];
    assert_eq!(
        person_commits(&commits),
        vec![("abc4567b7e".to_string(), "a-person".to_string())]
    );
}

#[test]
fn red_checks_never_roll_the_branch_back_over_a_persons_files() {
    let resume = include_str!("../resume.rs");
    let out = super::fixture::fn_src(resume, "async fn take_out_failing_bumps(");
    assert!(!out.contains("list_pr_commits(") && !out.contains("own_revert_paths("));
    assert!(!include_str!("../supervisor.rs").contains("pub fn own_revert_paths("));
}

/// Every commit path goes through the hold, and a person's round lifts it.
#[test]
fn every_write_is_held_and_a_persons_round_is_their_answer() {
    let gh = include_str!("../../clients/github.rs");
    for f in ["pub async fn write_file(", "pub async fn batch_write("] {
        let at = gh.find(f).unwrap();
        let body = &gh[at..at + 1200];
        assert!(body.contains(".hold_for_people("), "{f}");
    }
    // A round's WIP branch is held against its pull request's people too.
    assert!(gh.contains("let wip = common::run_round::wip_branch(\"\");"));
    let implement = include_str!("../implement.rs");
    assert!(implement
        .contains("person_asked_round(round_for == super::write_guard::RoundFor::PersonAsk)"));
    let feedback = include_str!("../feedback.rs");
    assert!(feedback.contains("github.person_asked_round(!ci_only)"));
    // FileOp is cloned into the hold.
    let op = FileOp::Delete { path: SPEC.into() };
    assert_eq!(op.clone(), op);
}
