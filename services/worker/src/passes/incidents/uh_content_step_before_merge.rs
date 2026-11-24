//! uh. A feature put structured data on marina pages from addresses in the
//! CMS. Staging had none, so the change carried a migration to fill them,
//! and its own end-to-end test failed by name until the migration ran
//! ("... is incomplete in this dataset; run apps/studio/migrations/...").
//! The plan made running that migration CoderHelm's step and told the
//! person "Needed from you: nothing", but the repository had no staging
//! CMS write connection, which CoderHelm only found after the pull request
//! opened. The code was right (it left the block out when the address was
//! missing), so each CI fix round changed nothing; every round met the
//! same failure, spent the run's fix budget and ended the run "failed".
//! A second feature met the same shape with test fixtures a migration
//! adds.
//!
//! Guarantee: a failing test that waits only on the change's own content
//! step CoderHelm can't run ends the CI loop before any fix round
//! (`common::content_gap`): the pull request says what to run before
//! merge, the run is completed with `ci_end` = `awaits_content`, and the
//! pull request goes ready (auto-merge still won't merge a red check). A
//! build or page that fails on missing content is the code's to fix: the
//! round is told to make it tolerant, never to retry. The plan check makes
//! a staging content step with no write connection a person's step before
//! merge, and the new-feature checklist and the review flag app code that
//! throws on missing content.

use super::fixture::fn_src;
use crate::passes::{feature::checklist, outside_work, resume, review_rules};
use common::content_gap::{self, Verdict};

/// The failing check's error, as the report gave it (names made generic).
const CAUSE: &str = "Error: marinaPage /marina-a metadata.address is incomplete in this dataset; run apps/studio/migrations/fill-marina-addresses";
const FIXTURES: &str = "Error: Slip date \"e2e-callout-expired\" is missing from the Slip Indexes: run the add-callout-e2e-fixtures migration on the staging dataset.";

fn decision(migration: &str, how: &str) -> crate::passes::fix_location::FixDecision {
    serde_json::from_value(serde_json::json!({
        "fix_location": "both",
        "cause": "New work",
        "outside_change": {
            "system": "CMS staging dataset",
            "steps": [
                {"step": "Write the content migration", "by": "this_pr", "kind": "migration",
                 "target": "staging", "migration": migration},
                {"step": "Dry run, then run the migration on staging", "by": "coderhelm",
                 "owner": "CoderHelm", "kind": "migration", "target": "staging", "how": how,
                 "migration": migration,
                 "result": "not done: o/web has no staging CMS write connection; grant: add one in Settings, repository, Staging CMS (a staging-only write token)"},
                {"step": "Tag the production release after approval", "by": "coderhelm",
                 "owner": "CoderHelm", "kind": "deploy", "target": "production"}
            ]
        }
    }))
    .expect("a decision")
}

#[test]
fn a_test_waiting_on_the_changes_own_blocked_migration_ends_the_loop() {
    let d = decision(
        "apps/studio/migrations/fill-marina-addresses/index.ts",
        "cd apps/studio && npx sanity migration run fill-marina-addresses --dataset staging",
    );
    let steps = resume::content_steps(&d);
    // The change's own `this_pr` task is not an outside step.
    assert_eq!(steps.len(), 2);
    assert!(steps[0].blocked && steps[0].writes_content && !steps[0].done);
    match content_gap::classify(CAUSE, true, &steps) {
        Verdict::AwaitsStep { gap, step } => {
            assert!(step.contains("fill-marina-addresses"), "{step}");
            let sec = content_gap::section(&gap, &step, "Deploy Staging");
            assert!(sec.contains("Before merge"));
            assert!(sec.contains("spent no CI fix round"));
            assert!(sec.contains("ready for review"));
            assert!(!sec.contains("without getting it green") && !sec.contains("stays a draft"));
        }
        v => panic!("{v:?}"),
    }
    // The second run's shape: fixtures a migration adds.
    let d = decision(
        "apps/studio/migrations/add-callout-e2e-fixtures/index.ts",
        "cd apps/studio && npx sanity migration run add-callout-e2e-fixtures --dataset staging",
    );
    assert!(matches!(
        content_gap::classify(FIXTURES, true, &resume::content_steps(&d)),
        Verdict::AwaitsStep { .. }
    ));
    // The ending lets the run be done, and the ready decision reads it.
    assert!(content_gap::allows_done(content_gap::CI_END));
    assert!(resume::ci_state_of_record("failed", content_gap::CI_END).allows_done());
    assert_eq!(
        crate::passes::GaveUp::AwaitsContent.as_str(),
        content_gap::CI_END
    );
}

#[test]
fn a_build_that_fails_on_missing_content_is_made_tolerant_never_retried() {
    let d = decision("apps/studio/migrations/fill-marina-addresses/index.ts", "");
    // No failing test: the build or deploy failed on the content.
    match content_gap::classify(CAUSE, false, &resume::content_steps(&d)) {
        Verdict::TolerateFirst(g) => {
            let b = content_gap::tolerance_brief(&g);
            assert!(b.contains("Never re-run"));
            assert!(b.contains("must not fail a build or deploy"));
        }
        v => panic!("{v:?}"),
    }
}

#[test]
fn the_ci_loop_reads_the_gap_before_it_claims_a_fix_round() {
    let src = include_str!("../resume.rs");
    let read = src
        .find("content_verdict(state, &msg.team_id")
        .expect("read");
    let claim = src[read..]
        .find("claim_auto_fix_slot(")
        .expect("the claim after it");
    assert!(claim < 4000, "read right before the fix budget is claimed");
    let finish = fn_src(src, "\nasync fn finish_awaits_content(");
    assert!(finish.contains("GaveUp::AwaitsContent"));
    assert!(finish.contains("mark_pr_ready("));
    assert!(finish.contains("common::run_status::COMPLETED"));
    assert!(!finish.contains("fail_run("));
    let ready = include_str!("../ready.rs");
    let reader = fn_src(ready, "\nasync fn red_on_base(");
    assert!(reader.contains("content_gap::allows_done"));
}

#[test]
fn the_plan_makes_a_staging_content_step_with_no_connection_a_persons() {
    let d = decision("apps/studio/migrations/fill-marina-addresses/index.ts", "");
    let c = d.outside_change.unwrap();
    let a = outside_work::Abilities::default();
    assert!(!a.staging_write);
    let gaps = outside_work::gaps(
        &c,
        &a,
        outside_work::PlanFacts {
            schema_changed: false,
            e2e: true,
        },
    );
    assert!(
        gaps.iter()
            .any(|g| g.contains("no staging CMS write connection")
                && g.contains("person's step before merge")
                && g.contains("never throw in a build or deploy")),
        "{gaps:?}"
    );
}

#[test]
fn code_that_throws_on_missing_content_is_flagged_by_the_checklist_and_the_review() {
    let patch = "@@ -0,0 +1,6 @@\n+export async function marinaJsonLd(slug: string) {\n+  const marina = await client.fetch(queryMarina, { slug });\n+  if (!marina?.metadata?.address) {\n+    throw new Error(`marina ${slug} address is missing; run the migration`);\n+  }\n+}\n";
    let files = vec![(
        "apps/web/src/lib/marina-jsonld.ts".to_string(),
        "added".to_string(),
        patch.to_string(),
    )];
    let found = review_rules::content_throw_findings(&files);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].title, content_gap::THROW_TITLE);
    assert_eq!(found[0].severity, "blocking");
    assert!(review_rules::check_started(&found[0]).is_some());
    let f = checklist::Facts {
        content_throws: content_gap::content_throws(&files[0].0, patch),
        ..Default::default()
    };
    assert_eq!(checklist::failing(&checklist::static_items(&f)).len(), 1);
    // The same check in an end-to-end spec is its own rule (fail by name).
    assert!(content_gap::content_throws("e2e/tests/site/seo.spec.ts", patch).is_empty());
    let review = include_str!("../review_pr.rs");
    assert!(review.contains("content_throw_findings(&files)"));
}
