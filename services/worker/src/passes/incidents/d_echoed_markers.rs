//! d. CodeBuild echoes every buildspec command into the log before running
//! it, and the codegen command holds the upload markers inside `echo "..."`.
//! The worker read the echoed text as the build's result: every build
//! without an archive "could not be uploaded", and the generator's own
//! failure (or its upload) was never seen.
//!
//! Guarantee: a marker counts only on a line of its own, printed by the
//! build; the exit code is the last one the build printed.

use super::fixture::*;
use crate::clients::sandbox;
use crate::passes::generators::{self, Read};

const OLD: &str = "could not be uploaded";

async fn outcome(sb: FakeSandbox) -> sandbox::CodegenOutcome {
    sandbox::codegen_outcome(&sb, "/aws/codebuild/sandbox", "s1", "out.tgz", true).await
}

#[tokio::test]
async fn a_failed_generator_is_read_as_its_failure_not_an_upload_problem() {
    let log = codebuild_log(&[
        "Error: Unknown type \"cardBlock\" in schemaTypes/index.ts",
        "CODERHELM_CODEGEN_END exit=1",
    ]);
    assert!(
        log.contains("===CODERHELM_CODEGEN_UPLOAD_FAILED==="),
        "the echo holds it"
    );
    let o = outcome(FakeSandbox::new(vec![], log)).await;
    assert!(o.ran && !o.passed, "{}", o.output);
    let mut end = Ending::default();
    match generators::read_outcome(o, "pnpm run typegen", || true) {
        Read::Failed(why, _) => end.said(why),
        other => panic!("{other:?}"),
    }
    assert!(end.says("Unknown type \"cardBlock\""), "{:?}", end.words);
    end.never_says(OLD);
}

#[tokio::test]
async fn an_upload_the_build_printed_is_read_again_when_the_first_read_was_empty() {
    let archive = tarball(&[(SCHEMA_JSON, b"{\"types\":[]}".to_vec())]);
    let log = codebuild_log(&[
        "CODERHELM_CODEGEN_END exit=0",
        "===CODERHELM_CODEGEN_UPLOADED===",
    ]);
    // The first archive read comes back empty (a transient S3 read).
    let o = outcome(FakeSandbox::new(vec![Vec::new(), archive], log)).await;
    assert!(o.ran && o.passed);
    assert_eq!(o.changed.len(), 1, "{}", o.output);
    assert_eq!(o.changed[0].0, SCHEMA_JSON);
}

#[tokio::test]
async fn a_printed_upload_failure_is_one() {
    let log = codebuild_log(&[
        "CODERHELM_CODEGEN_END exit=0",
        "upload failed: AccessDenied",
        "===CODERHELM_CODEGEN_UPLOAD_FAILED===",
    ]);
    let o = outcome(FakeSandbox::new(vec![], log)).await;
    assert!(!o.ran);
    assert!(o.output.contains(OLD), "{}", o.output);
}

#[tokio::test]
async fn a_clean_build_with_nothing_to_regenerate_is_up_to_date() {
    let log = codebuild_log(&["CODERHELM_CODEGEN_END exit=0"]);
    let o = outcome(FakeSandbox::new(vec![], log)).await;
    assert!(o.ran && o.passed && o.changed.is_empty());
    assert!(matches!(
        generators::read_outcome(o, "pnpm run typegen", || true),
        Read::Commit { ref changed, dropped: None } if changed.is_empty()
    ));
}
