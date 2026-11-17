//! bx. A person pushed a fixed end-to-end spec to CoderHelm's pull request:
//! they took out assertions that failed and fixed a selector, and the suite
//! passed 30 of 30 on staging. CoderHelm's next round looked at an older
//! version of the spec and wrote the removed assertions back in; a second
//! commit did it again from another old version. Nine of ten tests broke.
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
//! kept and asks. A round a person asked for is their answer. The revert
//! after red checks puts back only CoderHelm's own changes.

use crate::clients::github::{person_commits, FileOp};
use crate::passes::supervisor::{own_revert_paths, AfterGreen};
use common::person_lines::{guard, note, refusal, PersonEdit, Undo, NOTE_MARKER};
use serde_json::json;

const SPEC: &str = "e2e/tests/browse/camp-registration.spec.ts";

/// The spec as the person left it: two failing assertions out, the
/// register button found by its new name.
const PERSONS_SPEC: &str = "\
import { test, expect } from '@playwright/test';

test('register page embeds the portal', async ({ page }) => {
  await page.goto('/venue-a/summer-camps/register');
  await expect(page.frameLocator('iframe').first()).toBeTruthy();
  await page.getByRole('button', { name: 'Register now' }).click();
});
";

/// The person's commit, as GitHub's patch shows it.
const PERSONS_PATCH: &str = "\
@@ -3,8 +3,6 @@ import { test, expect } from '@playwright/test';
 test('register page embeds the portal', async ({ page }) => {
   await page.goto('/venue-a/summer-camps/register');
-  await expect(page.getByRole('heading', { name: 'Summer Camp Registration' })).toBeVisible();
-  await expect(page.getByRole('dialog', { name: 'Register' }).getByRole('heading', { name: 'Price Per Week' })).toBeVisible();
   await expect(page.frameLocator('iframe').first()).toBeTruthy();
-  await page.getByRole('button', { name: 'Register' }).click();
+  await page.getByRole('button', { name: 'Register now' }).click();
 });
";

/// What the round wrote: an older version of the spec, which puts the
/// assertions and the old selector back, plus one change of its own.
const ROUNDS_SPEC: &str = "\
import { test, expect } from '@playwright/test';

test('register page embeds the portal', async ({ page }) => {
  await page.goto('/venue-a/summer-camps/register');
  await expect(page.getByRole('heading', { name: 'Summer Camp Registration' })).toBeVisible();
  await expect(page.getByRole('dialog', { name: 'Register' }).getByRole('heading', { name: 'Price Per Week' })).toBeVisible();
  await expect(page.frameLocator('iframe').first()).toBeTruthy();
  await page.getByRole('button', { name: 'Register' }).click();
});

test('register page has a title', async ({ page }) => {
  await page.goto('/venue-a/summer-camps/register');
  await expect(page).toHaveTitle(/Register/);
});
";

fn persons_edit() -> PersonEdit {
    PersonEdit::from_patch("9f3c2a1b7e", "a-person", SPEC, PERSONS_PATCH)
}

#[test]
fn a_round_that_writes_back_what_a_person_removed_commits_only_its_own_change() {
    let g = guard(SPEC, Some(PERSONS_SPEC), ROUNDS_SPEC, &[persons_edit()]);
    // The person's fix stands.
    assert!(!g.content.contains("Price Per Week"), "{}", g.content);
    assert!(!g.content.contains("'Summer Camp Registration'"));
    assert!(g.content.contains("name: 'Register now'"));
    assert!(!g.content.contains("{ name: 'Register' }).click()"));
    // The round's own new test goes through.
    assert!(g.content.contains("register page has a title"));
    assert!(g
        .kept
        .iter()
        .all(|k| k.undo == Undo::RestoresWhatTheyRemoved || k.undo == Undo::RemovesTheirLine));
    assert!(g.kept.iter().any(|k| k.line.contains("Price Per Week")));
}

#[test]
fn a_write_that_only_reverts_the_person_is_refused_with_why() {
    // The second commit: the spec restored from an older commit, nothing
    // else.
    let older = ROUNDS_SPEC
        .split("\ntest('register page has a title'")
        .next()
        .unwrap();
    let g = guard(SPEC, Some(PERSONS_SPEC), older, &[persons_edit()]);
    assert_eq!(g.content, PERSONS_SPEC, "nothing of it is left to commit");
    let why = refusal(&g.kept);
    assert!(why.starts_with("Not committed"), "{why}");
    assert!(why.contains("a-person") && why.contains("9f3c2a1"), "{why}");
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
        json!({"sha": "9f3c2a1b7e", "author": {"login": "a-person", "type": "User"}, "parents": [{}]}),
        json!({"sha": "0f5d6c6", "author": {"login": "coderhelm[bot]", "type": "Bot"}, "parents": [{}]}),
        // A merge of the base is not the person's work.
        json!({"sha": "3333333", "author": {"login": "a-person", "type": "User"}, "parents": [{}, {}]}),
    ];
    assert_eq!(
        person_commits(&commits),
        vec![("9f3c2a1b7e".to_string(), "a-person".to_string())]
    );
}

#[test]
fn the_revert_after_red_checks_puts_back_only_coderhelms_own_files() {
    let changed: Vec<String> = [SPEC, "src/blocks/portal.tsx", "src/lib/base-only.ts"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let after = vec![
        AfterGreen {
            own: false,
            merge: false,
            files: vec![SPEC.into()],
        },
        AfterGreen {
            own: true,
            merge: false,
            files: vec!["src/blocks/portal.tsx".into()],
        },
        AfterGreen {
            own: true,
            merge: true,
            files: vec!["src/lib/base-only.ts".into()],
        },
    ];
    assert_eq!(
        own_revert_paths(&changed, &after),
        vec!["src/blocks/portal.tsx".to_string()]
    );
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
