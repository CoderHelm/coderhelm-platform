//! vt. Stable promotion was a clock: a beta became stable after a day out
//! with no open `release-blocker` issue naming it, and nothing ever filed
//! one. One beta went to stable without being installed anywhere. Another
//! went to stable while Chat was broken on it, hours after the fix was out
//! in a newer beta that was simply younger. A later promotion then failed
//! with HTTP 403 creating the stable tag, and the schedule that drives
//! promotion started hours late every day.
//!
//! Guarantees: a beta is promoted only on evidence from a live install (a
//! healthy post-update watch, a finished run, a Chat answer where Chat is
//! used) and never when a regression was found in it, the newest such beta
//! wins, and the notes say what the evidence was; the evidence is counts
//! only, sent signed by installs that turned the report on, and counted
//! where the work finishes; the stable tag's ref is created with a token
//! that may create it, and a refusal prints why; every published beta
//! starts the promotion, with the schedule only a backstop.

use super::fixture::fn_src;

const PLANNER: &str = include_str!("../../../../../.github/scripts/auto_release.py");
const WORKFLOW: &str = include_str!("../../../../../.github/workflows/auto-release.yml");
const UPDATER: &str = include_str!("../../../../../lambda/updater/handler.py");
const GATEWAY: &str = include_str!("../../../../gateway/src/main.rs");
const CHAT: &str = include_str!("../../../../gateway/src/routes/brain.rs");
const EVIDENCE: &str = include_str!("../../../../gateway/src/routes/hybrid_release.rs");

fn resume_src() -> &'static str {
    include_str!("../resume.rs")
}

#[test]
fn a_beta_is_promoted_on_evidence_never_on_age() {
    assert!(
        !PLANNER.contains("PROMOTE_AFTER_HOURS"),
        "no age gate: a day out proves nothing"
    );
    let plan = &PLANNER[PLANNER.find("def plan_promote(").unwrap()..];
    let plan = &plan[..plan.find("\ndef ").unwrap()];
    assert!(
        plan.contains("evidence"),
        "the plan reads the installs' reports"
    );
    assert!(plan.contains("unproven(proof(evidence.get("));
    assert!(
        plan.contains("\"the installs' release reports couldn't be read\""),
        "no reports, no promotion"
    );
    let ready = &PLANNER[PLANNER.find("def proof(").unwrap()..];
    let ready = &ready[..ready.find("\ndef ").unwrap()];
    // One install shows it all: a healthy watch, a run, and an answer where Chat is used.
    assert!(
        ready.contains("n(r, \"runs\") >= 1 and (n(r, \"answers\") >= 1 or not r.get(\"chat\"))")
    );
    let why = &PLANNER[PLANNER.find("def unproven(").unwrap()..];
    assert!(
        why.find("rolled_back").unwrap() < why.find("if p[\"ready\"]").unwrap(),
        "a rollback holds a beta even with proof from another install"
    );
    assert!(PLANNER.contains("f.write(evidence_notes(r[\"beta\"], r[\"proof\"]))"));
}

#[test]
fn the_evidence_is_counts_counted_where_the_work_finishes() {
    let end = fn_src(resume_src(), "\nasync fn end_run(");
    let done = end
        .find("info!(run_id, status, \"Run completed\")")
        .unwrap();
    assert!(end[done..].contains("common::release_use::Done::Run"));
    let kept = CHAT.find("common::release_use::Done::Answer").unwrap();
    assert!(
        CHAT[..kept].rfind("Ok(_) =>").unwrap() > CHAT[..kept].rfind("let saved").unwrap(),
        "an answer counts once it is kept"
    );
    assert_eq!(
        common::release_use::version_of("v0.9.1-beta.20300101").as_deref(),
        Some("v0.9.1-beta.20300101")
    );
    assert_eq!(common::release_use::version_of("dev"), None);
    // Sent signed, opt-in, fields fixed.
    assert!(GATEWAY.contains("post(routes::hybrid_release::evidence)"));
    assert!(EVIDENCE.contains("#[serde(deny_unknown_fields)]"));
    assert!(EVIDENCE.contains("hybrid::signed("));
    assert!(UPDATER.contains("\"report\": False,"));
    assert!(UPDATER.contains("release_report(store, env, now, settings)"));
}

#[test]
fn the_tag_is_created_by_a_token_that_may_and_promotion_never_waits_on_the_schedule() {
    assert!(WORKFLOW.contains("permission-workflows: write"));
    assert!(WORKFLOW.contains("REF_TOKEN: ${{ steps.app.outputs.token || github.token }}"));
    assert!(WORKFLOW.contains("GH_TOKEN=\"$REF_TOKEN\" call \"Creating refs/tags/$TAG\""));
    assert!(
        WORKFLOW.contains("gh api -i"),
        "a refusal shows GitHub's answer"
    );
    let start = WORKFLOW.find("- name: Start the promotion").unwrap();
    let step = &WORKFLOW[start..];
    assert!(step.contains("if: matrix.kind == 'beta'"));
    assert!(step.contains("gh workflow run auto-release.yml"));
    assert!(step.contains("-f mode=promote"));
    assert!(WORKFLOW.contains("role/coderhelm-prod-release-evidence"));
}
