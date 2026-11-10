//! ab. A failing-test fix's draft had its staging deploy and staging
//! end-to-end checks skipped. The reading of why (`y_skipped_checks_wait`)
//! went through the staging workflow line by line and kept every quoted
//! string next to a label test as a label name. The job's condition, a
//! `>-` block over several lines, was:
//!
//! ```text
//! (labeled && contains(github.event.label.name, 'CI:DEPLOY_STAGING')) ||
//! (labeled && (github.event.label.name == 'CI:E2E' ||
//!              startsWith(github.event.label.name, 'E2E:')) &&
//!   contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING')) || …
//! ```
//!
//! so the end-to-end check read as "any of `CI:DEPLOY_STAGING`, `CI:E2E`,
//! `E2E:`". The staging label was refused (deploy labels never are added),
//! and the team's settings allowed `CI:E2E` and the family `E2E:*`, whose
//! match let the bare `E2E:` through. CoderHelm added `CI:E2E` and `E2E:`.
//! Adding a label that doesn't exist creates it, so the repository got a
//! label `E2E:` that matches no real area, and `CI:E2E` alone could never
//! run the job: every way needs the staging label too. The next reading
//! saw the labels on the pull request and called the check unknown.
//!
//! Guarantees:
//! - A job's `if:` is read as an expression, multi-line blocks included,
//!   into the label combinations that run it; a job that needs another asks
//!   for that one's labels too.
//! - A prefix or substring test is a pattern, never a label: CoderHelm adds
//!   only an existing repository label that satisfies it, the one the
//!   team's settings name when several do, and nothing when that isn't one
//!   certain label. A family allows `E2E:nav`, never `E2E:`.
//! - A label is added only when it alone runs the check, the team's
//!   settings allow it, it is not a deploy, staging or production label,
//!   and no such label is on the pull request already. Otherwise the hold
//!   names the exact real label for a person to add.
//! - A label CoderHelm added by the old reading that was a bare prefix, or
//!   couldn't run its check alone, comes off the pull request when CoderHelm
//!   itself put it there; a bare-prefix repository label GitHub made for
//!   that add is deleted when nothing else carries it.

use common::e2e::workflow_gate::{self, LabelWay};
use serde_json::json;

use super::fixture::fn_src;
use crate::passes::review_labels::LabelRules;
use crate::passes::skipped_checks;

/// The staging workflow, as its conditions read.
const STAGING: &str = r#"
name: Deploy Staging
on:
  push:
    branches: [main]
  pull_request:
    types: [labeled, synchronize]
jobs:
  deploy_staging_web:
    name: Deploy Web App
    if: >-
      github.event_name == 'push' ||
      (github.event.action == 'labeled' && contains(github.event.label.name, 'CI:DEPLOY_STAGING')) ||
      (github.event.action == 'labeled' &&
       (github.event.label.name == 'CI:E2E' || startsWith(github.event.label.name, 'E2E:')) &&
       contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING')) ||
      (github.event.action == 'synchronize' && contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING'))
    runs-on: ubuntu-latest
  e2e_playwright:
    name: E2E after staging deploy
    needs: deploy_staging_web
    uses: ./.github/workflows/e2e-playwright.yml
"#;

const CHECK: &str = "E2E after staging deploy";

fn repo_labels() -> Vec<String> {
    [
        "ch-review",
        "CI:E2E",
        "CI:DEPLOY_STAGING",
        "CI:DEPLOY_PREVIEW",
        "E2E:join",
        "E2E:nav",
        "E2E:camps",
        // Made by the add.
        "E2E:",
    ]
    .map(String::from)
    .to_vec()
}

fn rows() -> Vec<(String, Option<workflow_gate::WorkflowGate>)> {
    vec![(CHECK.to_string(), Some(workflow_gate::read(STAGING)))]
}

#[test]
fn the_condition_is_read_as_combinations_and_the_prefix_as_a_prefix() {
    let g = workflow_gate::read(STAGING);
    let job = g.job_for(CHECK).unwrap();
    let way = |need: &[&str]| LabelWay {
        need: need.iter().map(|s| s.to_string()).collect(),
        forbid: vec![],
    };
    assert_eq!(
        job.ways,
        vec![
            way(&["*CI:DEPLOY_STAGING*"]),
            way(&["CI:E2E", "CI:DEPLOY_STAGING"]),
            way(&["E2E:*", "CI:DEPLOY_STAGING"]),
            way(&["CI:DEPLOY_STAGING"]),
        ]
    );
    // The old reading's label list had `E2E:` in it; no pattern means it.
    assert!(!g.labels().iter().any(|l| l == "E2E:"));
    assert!(g
        .labels()
        .iter()
        .all(|p| !workflow_gate::matches(p, "E2E:")));
    // Only the staging label runs it alone; `CI:E2E` alone never does.
    let s = workflow_gate::sort(&rows(), &["ch-review".into()]);
    assert_eq!(s.label_gated.len(), 1);
    assert!(!s.label_gated[0].any.iter().any(|p| p == "CI:E2E"));
}

#[test]
fn nothing_is_added_and_the_hold_names_the_real_label() {
    let on_pr = vec!["ch-review".to_string()];
    let mut s = workflow_gate::sort(&rows(), &on_pr);
    // The team names `CI:E2E` and `CI:DEPLOY_STAGING`, and allows `E2E:*`.
    let named: Vec<String> = ["CI:E2E", "CI:DEPLOY_STAGING"].map(String::from).to_vec();
    assert!(workflow_gate::choose(&s, &repo_labels(), &named, &on_pr).is_empty());
    s.resolve_labels(&repo_labels(), &named);
    assert_eq!(
        s.action().unwrap(),
        "Add the `CI:DEPLOY_STAGING` label to run `E2E after staging deploy`, or mark it \
         ready yourself if they aren't needed."
    );
    // A family allows its labels, never the bare prefix.
    let rules = LabelRules::parse("E2E:*, CI:E2E, CI:DEPLOY_STAGING", "", "");
    assert!(rules.allowed("E2E:nav"));
    assert!(!rules.allowed("E2E:"));
    // The settings gate refuses a pattern, a bare prefix and deploy labels
    // before it reads the settings.
    let src = include_str!("../review_pr.rs");
    let may = fn_src(src, "pub(crate) async fn may_add_label(");
    assert!(may.contains("is_bare_prefix(label)"));
    assert!(may.contains("is_deploy_label(label)"));
    assert!(may.contains("label.contains('*')"));
    assert!(may.contains("cfg.auto_labels && cfg.label_rules.allowed(label)"));
}

#[test]
fn a_label_is_added_only_from_the_repo_labels_and_only_when_certain() {
    // A prefix-only job: several `E2E:` labels, none named by the team.
    let prefix = workflow_gate::read(
        "jobs:\n  e2e:\n    name: e2e\n    if: startsWith(github.event.label.name, 'E2E:')\n",
    );
    let rows = vec![("e2e".to_string(), Some(prefix))];
    let s = workflow_gate::sort(&rows, &[]);
    assert!(workflow_gate::choose(&s, &repo_labels(), &[], &[]).is_empty());
    // The team's settings name one: that one, never `E2E:`.
    assert_eq!(
        workflow_gate::choose(&s, &repo_labels(), &["E2E:nav".into()], &[]),
        vec!["E2E:nav"]
    );
    // The repo has only the bare prefix: nothing.
    assert!(workflow_gate::choose(&s, &["E2E:".into()], &[], &[]).is_empty());
    // An exact label the repo lacks is never added (adding would make it).
    let exact = workflow_gate::read(
        "jobs:\n  e2e:\n    name: e2e\n    if: github.event.label.name == 'run-e2e'\n",
    );
    let s = workflow_gate::sort(&[("e2e".to_string(), Some(exact))], &[]);
    assert!(workflow_gate::choose(&s, &repo_labels(), &[], &[]).is_empty());
    assert_eq!(
        workflow_gate::choose(&s, &["run-e2e".into()], &[], &[]),
        vec!["run-e2e"]
    );
    // Nothing next to a deploy label already on the pull request.
    assert!(
        workflow_gate::choose(&s, &["run-e2e".into()], &[], &["CI:DEPLOY_STAGING".into()])
            .is_empty()
    );
    // The ready check adds only what `choose` picks from the repo labels.
    let src = include_str!("../test_fix.rs");
    let gate = fn_src(src, "pub async fn ready_gate(");
    assert!(gate.contains("list_repo_labels("));
    assert!(gate.contains("workflow_gate::choose(&skipped, &repo_labels"));
    assert!(!gate.contains("skipped.labels()"));
    // Gates read the old way are never used again.
    let cache = include_str!("../skipped_checks.rs");
    assert!(cache.contains("WORKFLOW_GATES2#"));
}

#[test]
fn the_labels_added_by_mistake_come_off_and_the_made_label_goes() {
    // The gate record as the old reading wrote it.
    let sha = "4f1c0de2b7a9e3c5d8f60172a4b9c3e1d2f50a6b";
    let records = vec![format!("{sha}:CI:E2E"), format!("{sha}:E2E:")];
    let ours = skipped_checks::added_before(&records);
    assert_eq!(ours, vec!["CI:E2E", "E2E:"]);
    // New records are never taken for the old reading's.
    assert!(skipped_checks::added_before(&[skipped_checks::added_key(sha, "E2E:nav")]).is_empty());
    let on_pr: Vec<String> = ["ch-review", "CI:E2E", "E2E:"].map(String::from).to_vec();
    assert_eq!(
        workflow_gate::to_take_off(&rows(), &on_pr, &ours, &repo_labels(), &[]),
        vec!["CI:E2E", "E2E:"]
    );
    // Only when CoderHelm itself put it on last.
    let events = json!([
        {"event": "labeled", "label": {"name": "CI:E2E"}, "actor": {"login": "coderhelm[bot]"}},
        {"event": "labeled", "label": {"name": "E2E:"}, "actor": {"login": "coderhelm[bot]"}},
        {"event": "unlabeled", "label": {"name": "CI:E2E"}, "actor": {"login": "a-person"}},
        {"event": "labeled", "label": {"name": "CI:E2E"}, "actor": {"login": "a-person"}},
    ]);
    assert!(skipped_checks::labeled_last_by(
        &events,
        "E2E:",
        "coderhelm[bot]"
    ));
    assert!(!skipped_checks::labeled_last_by(
        &events,
        "CI:E2E",
        "coderhelm[bot]"
    ));
    // The repo label GitHub made for the add (grey, no description) may be
    // deleted; one a person made is left.
    assert!(skipped_checks::made_by_an_add(
        &json!({"name": "E2E:", "color": "ededed", "description": null})
    ));
    assert!(!skipped_checks::made_by_an_add(
        &json!({"name": "E2E:", "color": "1d76db", "description": "Area tests"})
    ));
    let take = fn_src(
        include_str!("../skipped_checks.rs"),
        "pub async fn take_off_mistakes(",
    );
    assert!(take.contains("labeled_last_by(&events, &l, &bot)"));
    assert!(take.contains("made && !used"));
}
