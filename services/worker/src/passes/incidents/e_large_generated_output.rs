//! e. The studio's typegen changed two files: `apps/studio/schema.json` and
//! the site's types, written through `../web/...` and over 5 MB. The sandbox
//! dropped the large one without a word; the run said "regenerated 1
//! file(s)", the plan check found the types task not done, and the run
//! ended with no pull request.
//!
//! Guarantee: every file a generator changes is committed (or named as a
//! tooling failure when it can't be); paths are spelled one way; and a
//! generated task its generator settled is done in every check.

use super::fixture::*;
use crate::clients::sandbox;
use crate::passes::generators::{self, Read};
use crate::passes::plan_coverage;
use crate::passes::proof;

const OLD: &str = "regenerated 1 file(s)";

fn large_types() -> Vec<u8> {
    let line = "export type CardBlock = { _type: 'cardBlock'; title?: string };\n";
    line.repeat(7 * 1024 * 1024 / line.len() + 1).into_bytes()
}

#[tokio::test]
async fn a_large_generated_file_written_through_a_parent_path_is_committed_and_counted_done() {
    // The generator, as detection reads it from the studio's config.
    let repo = monorepo();
    let gens = generators::detect(&repo);
    let typegen = gens
        .iter()
        .find(|g| g.outputs.iter().any(|o| o == TYPES))
        .unwrap_or_else(|| panic!("the studio typegen writes {TYPES}: {gens:?}"));
    assert!(typegen.outputs.iter().all(|o| !o.contains("..")));

    // The build packs both changed files from the repository root.
    let types = large_types();
    assert!(types.len() > 5 * 1024 * 1024);
    let archive = tarball(&[
        (SCHEMA_JSON, b"{\"types\":[\"cardBlock\"]}".to_vec()),
        (TYPES, types),
    ]);
    let log = codebuild_log(&[
        "CODERHELM_CODEGEN_END exit=0",
        "===CODERHELM_CODEGEN_UPLOADED===",
    ]);
    let o = sandbox::codegen_outcome(
        &FakeSandbox::new(vec![archive], log),
        "/aws/codebuild/sandbox",
        "s1",
        "out.tgz",
        true,
    )
    .await;
    let mut end = Ending::default();
    let committed = match generators::read_outcome(o, "pnpm run typegen", || true) {
        Read::Commit {
            changed,
            dropped: None,
        } => changed.into_iter().map(|(p, _)| p).collect::<Vec<_>>(),
        other => panic!("both files come back: {other:?}"),
    };
    assert_eq!(committed, vec![SCHEMA_JSON.to_string(), TYPES.to_string()]);
    let mut gh = FakeGitHub::new();
    gh.commit("generator", BRANCH, &[(SCHEMA_JSON, "new"), (TYPES, "new")]);
    gh.commit(
        "coder",
        BRANCH,
        &[
            ("apps/studio/schemaTypes/blocks/card.ts", "x"),
            ("apps/studio/schemaTypes/index.ts", "x"),
            ("apps/web/src/blocks/card.tsx", "x"),
            ("apps/web/src/blocks/index.ts", "x"),
            ("apps/web/src/lib/sanity/queries.ts", "x"),
            ("e2e/events.spec.ts", "x"),
        ],
    );
    end.said(format!("Regenerated {} file(s)", committed.len()));

    // The plan spells the task `apps/studio/../web/...`; it is done.
    let tasks = plan_coverage::plan_tasks(&plan_tasks_md());
    assert!(tasks.iter().any(|t| t.path == TYPES), "{tasks:?}");
    let settled = typegen.outputs.clone();
    let cov = plan_coverage::settle_generated(
        plan_coverage::coverage(&tasks, &gh.changed(BRANCH)),
        &settled,
    );
    end.said(plan_coverage::progress_note(&cov));
    assert!(
        end.says("Plan check: 7 of 7 planned tasks done"),
        "{:?}",
        end.words
    );

    // The proof judge flags the types file anyway: dropped, in either
    // spelling, and nothing holds the pull request.
    for spelled in [TYPES, &format!("apps/studio/{TYPES_FROM_STUDIO}")] {
        let reply = judge_reply(&[(
            "planned_work",
            &format!("`{spelled}` is not regenerated with the card block."),
        )]);
        let findings = proof::parse_findings(&reply).unwrap();
        let left = proof::drop_settled_generated(findings, &settled);
        assert!(proof::planned_work(&left).is_empty(), "{spelled}: {left:?}");
        assert!(proof::unfinished_section(&left).is_empty());
    }
    end.status = common::run_status::AWAITING_CI.into();
    end.never_says(OLD);
}

#[test]
fn a_file_past_the_cap_is_named_never_dropped_silently() {
    let big = vec![b'a'; 2048];
    let captured = sandbox::read_changed_tar(&tarball(&[(TYPES, big)]), 200, 1024).unwrap();
    assert!(captured.files.is_empty());
    assert_eq!(captured.dropped.len(), 1);
    let why = generators::dropped_reason(&captured.dropped);
    assert!(why.contains(TYPES), "{why}");
}
