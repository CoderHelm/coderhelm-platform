//! The incident regression suite: every run failure that reached a person
//! is a case here, so it can never come back silently.
//!
//! Each case replays the failure as a scenario on one invented monorepo
//! (`fixture`), through the pipeline's own decision code, with fakes only at
//! the edges: GitHub, the model's replies, the sandbox's archive and log,
//! and DynamoDB records. Each asserts how the run ends (its status), what it
//! leaves (the pull request, the notes), and that the old failure message
//! never appears. Where a decision lives inside the pipeline itself, the
//! case also checks that the pipeline still calls it at that place.
//!
//! CI runs the suite as its own step (`docs/incidents.md` has the command).
//! Each case is one file here and one page in `docs/incidents/` (what
//! happened, and the guarantee it protects), named alike. A fix for a new
//! failure adds those two files and edits nothing shared: `worker/build.rs`
//! finds every case file and writes its `mod` line, so two pull requests
//! that each add a case never collide.

mod fixture;

// One `mod` line per case file (`worker/build.rs`), and `CASES`.
include!(concat!(env!("OUT_DIR"), "/incidents.rs"));

#[test]
fn every_case_has_its_page_and_every_page_its_case() {
    let docs = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/incidents");
    let mut pages: Vec<String> = std::fs::read_dir(&docs)
        .expect("docs/incidents")
        .flatten()
        .filter_map(|e| {
            e.file_name()
                .to_string_lossy()
                .strip_suffix(".md")
                .map(str::to_string)
        })
        .collect();
    pages.sort();
    let cases: Vec<String> = CASES.iter().map(|c| c.to_string()).collect();
    let no_page: Vec<&String> = cases.iter().filter(|c| !pages.contains(c)).collect();
    let no_case: Vec<&String> = pages.iter().filter(|p| !cases.contains(p)).collect();
    assert!(
        no_page.is_empty() && no_case.is_empty(),
        "each incident case needs docs/incidents/<case>.md, and each page its case: no page for {no_page:?}; no case for {no_case:?}"
    );
    assert!(cases.len() >= 70, "only {} cases found", cases.len());
}
