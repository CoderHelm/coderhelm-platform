//! The gates a fix passes before it is committed and pushed
//! (`common::push_gate`), read from the repository and the round's own
//! commit.
//!
//! - [`package_steps`]: the sandbox's checks come from the package that
//!   owns each touched file (its test, type-check and lint scripts), in
//!   place of a test command picked from the first workflow that has one.
//! - [`before_push`]: a CI fix round's commit is pushed only when its checks
//!   ran on its tree and passed, or the sandbox couldn't run them (the
//!   commit then says `Unverified:`), and when no test still names what it
//!   took out.
//! - [`FixLedger`] on the run record (`ci_test_fixes`): two pushed fixes
//!   per failing test, then the fix rounds stop with the test named.

use aws_sdk_dynamodb::types::AttributeValue;
use common::push_gate::{self, FileChange, FixLedger, Verdict};

use super::checks::{CheckKind, Step};
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// The run record's field for [`FixLedger`].
pub(crate) const LEDGER_FIELD: &str = "ci_test_fixes";

/// The sandbox steps for `changed`, from each owning package's own scripts
/// (`common::push_gate::package_steps`), read on `git_ref`.
pub(crate) async fn package_steps(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
    changed: &[String],
) -> Vec<Step> {
    if changed.is_empty() {
        return Vec::new();
    }
    let Ok(tree) = github.get_tree(owner, repo, git_ref).await else {
        return Vec::new();
    };
    let root: Vec<String> = tree
        .iter()
        .filter(|e| e.entry_type == "blob" && !e.path.contains('/'))
        .map(|e| e.path.clone())
        .collect();
    let mut manifests = Vec::new();
    for dir in push_gate::manifest_dirs(changed) {
        let path = if dir.is_empty() {
            "package.json".to_string()
        } else {
            format!("{dir}/package.json")
        };
        if !tree.iter().any(|e| e.path == path) {
            continue;
        }
        if let Ok(text) = github.read_file(owner, repo, &path, git_ref).await {
            manifests.push((dir, text));
        }
    }
    to_steps(&push_gate::package_steps(changed, &manifests, &root))
}

/// [`push_gate::PkgStep`]s as sandbox steps.
pub(crate) fn to_steps(pkg: &[push_gate::PkgStep]) -> Vec<Step> {
    pkg.iter()
        .map(|p| {
            let (kind, scope) = match p.kind {
                "tests" => (
                    CheckKind::Tests,
                    "only the test files this change touched (it changed nothing else), with their package's own test script",
                ),
                "suite" => (
                    CheckKind::Tests,
                    "the package's whole unit suite, as CI runs it: a non-test file changed, and its other callers are outside the diff",
                ),
                "types" => (CheckKind::Typecheck, "the touched package's type check"),
                _ => (CheckKind::Lint, "the touched package's lint"),
            };
            Step {
                kinds: vec![kind],
                cmd: p.cmd.clone(),
                scope: Some(format!("{scope} (`{}`)", if p.dir.is_empty() { "." } else { &p.dir })),
            }
        })
        .collect()
}

/// The steps run_checks runs: the derived and CI steps, with the owning
/// packages' steps (`pkg`). A package test step replaces the test step
/// picked from the first workflow with a test line: that one ran another
/// package's suite with `--passWithNoTests`, so it passed without running
/// a touched test. A package's type-check or lint step is added only when
/// no step already checks that.
pub(crate) fn merge_steps(steps: Vec<Step>, pkg: Vec<Step>) -> Vec<Step> {
    let has = |steps: &[Step], k: CheckKind| steps.iter().any(|s| s.kinds.contains(&k));
    let pkg_tests = has(&pkg, CheckKind::Tests);
    let mut out: Vec<Step> = steps
        .into_iter()
        .filter(|s| !(pkg_tests && s.kinds == vec![CheckKind::Tests]))
        .collect();
    for p in pkg {
        let k = p.kinds[0];
        if k == CheckKind::Tests || !has(&out, k) {
            out.push(p);
        }
    }
    out
}

/// The push gate's reading of a round's last check run.
pub(crate) fn checked_of(
    sandbox: &super::proof::SandboxRecord,
    files: &[String],
) -> push_gate::Checked {
    let ran = |k| sandbox.ran.contains(&k);
    let passed = |k| sandbox.passed.contains(&k);
    push_gate::Checked {
        // A pass with a sandbox either ran checks, gave no result, or owes
        // them (`checks_owed`); none of the three is a pass with none.
        sandbox: !sandbox.ran.is_empty() || sandbox.failure.is_some() || sandbox.refused_for_time,
        // Tests must run when a test or any code changed: a changed helper
        // breaks callers outside the diff.
        touched_tests: files
            .iter()
            .any(|f| push_gate::is_test_file(f) || push_gate::is_code_file(f)),
        tests_ran: ran(CheckKind::Tests),
        tests_passed: passed(CheckKind::Tests),
        types_ran: ran(CheckKind::Typecheck) || ran(CheckKind::Build),
        types_passed: (!ran(CheckKind::Typecheck) || passed(CheckKind::Typecheck))
            && (!ran(CheckKind::Build) || passed(CheckKind::Build)),
        failure: sandbox.failure.clone(),
        owed: sandbox.refused_for_time,
    }
}

/// What the gate decides for a round's commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Gate {
    /// Pushed, with these lines added to the commit: which suite ran and
    /// how it ended, or `Unverified:` and why.
    Push(Option<String>),
    /// Not pushed: why, one line each, for the next attempt.
    Hold(Vec<String>),
}

/// The gate for a CI fix round's commit (its WIP branch `wip` since
/// `base_sha`): its checks ([`checked_of`]) and its tests against what it
/// takes out (`push_gate::still_expected`).
pub(crate) async fn before_push(
    pr: &super::round::Pr<'_>,
    wip: &str,
    base_sha: &str,
    sandbox: &super::proof::SandboxRecord,
    checks: &[String],
    files: &[String],
) -> Gate {
    let mut hold = Vec::new();
    let mut unverified = None;
    let ran = suite_lines(checks);
    match push_gate::verdict(&checked_of(sandbox, files)) {
        Verdict::Verified => {
            unverified = (!ran.is_empty()).then(|| format!("Checked before this push:\n{ran}"))
        }
        Verdict::Red(why) => hold.push(format!(
            "Not pushed: {why}. Read the failing output of run_checks, fix it, and run the \
             checks again; a fix is pushed only when they pass on its last edit."
        )),
        Verdict::Owed => hold.push(
            "Not pushed: the checks didn't run on the fix's last edit. Call run_checks on it \
             before you finish; a fix nothing ran on is never pushed."
                .to_string(),
        ),
        Verdict::Unverified(why) => {
            let line = push_gate::unverified_line(&why);
            unverified = Some(if ran.is_empty() {
                format!("{line}\nNo test suite ran.")
            } else {
                format!("{line}\nWhat ran:\n{ran}")
            })
        }
    }
    for stale in stale_tests(pr, wip, base_sha).await {
        hold.push(format!("Not pushed: {}.", stale.line()));
    }
    if hold.is_empty() {
        Gate::Push(unverified)
    } else {
        Gate::Hold(hold)
    }
}

/// The check lines of the last sandbox run (`- Tests: PASS, \`cmd\` (scope)`),
/// as the commit records which suite ran.
pub(crate) fn suite_lines(checks: &[String]) -> String {
    checks
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.trim().to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

/// The tests on the WIP branch that still name a definition the round
/// removed. Empty when nothing was removed or the compare can't be read.
async fn stale_tests(
    pr: &super::round::Pr<'_>,
    wip: &str,
    base_sha: &str,
) -> Vec<push_gate::Stale> {
    let (github, owner, repo) = (pr.github, pr.owner, pr.repo);
    let Ok(compare) = github.get_diff(owner, repo, base_sha, wip).await else {
        return Vec::new();
    };
    let files: Vec<FileChange> = push_gate::changes_of(&compare);
    let removed = push_gate::removed_symbols(&files);
    if removed.is_empty() {
        return Vec::new();
    }
    let tree: Vec<String> = github
        .get_tree(owner, repo, wip)
        .await
        .map(|t| {
            t.into_iter()
                .filter(|e| e.entry_type == "blob")
                .map(|e| e.path)
                .collect()
        })
        .unwrap_or_default();
    let mut tests = Vec::new();
    for path in push_gate::tests_to_read(&files, &tree) {
        if let Ok(text) = github.read_file_live(owner, repo, &path, wip).await {
            tests.push((path, text));
        }
    }
    push_gate::still_expected(&removed, &tests)
}

/// The run's [`FixLedger`].
pub(crate) async fn load_ledger(state: &WorkerState, team_id: &str, run_id: &str) -> FixLedger {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .projection_expression("#f")
        .expression_attribute_names("#f", LEDGER_FIELD)
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| i.get(LEDGER_FIELD).and_then(|v| v.as_s().ok()).cloned())
        .map(|s| FixLedger::from_json(&s))
        .unwrap_or_default()
}

/// One pushed fix for each of `failing`, on the run record.
pub(crate) async fn record_push(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    failing: &[String],
) {
    if failing.is_empty() {
        return;
    }
    let mut ledger = load_ledger(state, team_id, run_id).await;
    ledger.record_push(failing);
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("run_id", AttributeValue::S(run_id.to_string()))
        .update_expression("SET #f = :v")
        .expression_attribute_names("#f", LEDGER_FIELD)
        .expression_attribute_values(":v", AttributeValue::S(ledger.to_json()))
        .send()
        .await;
}

/// What a failure is counted under: the tests its report names
/// (`reported`), else the tests its logs name, else the failing checks.
pub(crate) fn failing_keys(reported: &[String], logs: &str, checks: &[String]) -> Vec<String> {
    if !reported.is_empty() {
        return reported.to_vec();
    }
    let tests = push_gate::failing_tests(logs);
    if tests.is_empty() {
        checks.to_vec()
    } else {
        tests
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(k: CheckKind, cmd: &str) -> Step {
        Step {
            kinds: vec![k],
            cmd: cmd.into(),
            scope: None,
        }
    }

    #[test]
    fn the_owning_packages_test_step_replaces_the_first_workflows() {
        let steps = vec![
            step(CheckKind::Typecheck, "pnpm check-types"),
            step(CheckKind::Lint, "pnpm lint"),
            step(
                CheckKind::Tests,
                "(cd lambdas/resizer && npm test) -- --passWithNoTests a.test.tsx",
            ),
        ];
        let pkg = to_steps(&[
            push_gate::PkgStep {
                kind: "tests",
                dir: "apps/web".into(),
                cmd: "cd apps/web && CI=true pnpm exec vitest run src/a.test.tsx".into(),
            },
            push_gate::PkgStep {
                kind: "types",
                dir: "apps/web".into(),
                cmd: "cd apps/web && pnpm run check-types".into(),
            },
        ]);
        let out = merge_steps(steps, pkg);
        let cmds: Vec<&str> = out.iter().map(|s| s.cmd.as_str()).collect();
        assert_eq!(
            cmds,
            vec![
                "pnpm check-types",
                "pnpm lint",
                "cd apps/web && CI=true pnpm exec vitest run src/a.test.tsx"
            ]
        );
        // Without a package test step, the existing one stays.
        let only = merge_steps(vec![step(CheckKind::Tests, "npm test")], Vec::new());
        assert_eq!(only.len(), 1);
    }

    #[test]
    fn the_push_gate_reads_the_rounds_last_check_run() {
        use super::super::proof::SandboxRecord;
        let files = vec!["src/a.ts".to_string(), "src/a.test.ts".to_string()];
        let green = SandboxRecord {
            ran: vec![CheckKind::Typecheck, CheckKind::Tests],
            passed: vec![CheckKind::Typecheck, CheckKind::Tests],
            ..Default::default()
        };
        assert_eq!(
            push_gate::verdict(&checked_of(&green, &files)),
            Verdict::Verified
        );
        let red = SandboxRecord {
            passed: vec![CheckKind::Typecheck],
            ..green.clone()
        };
        assert!(matches!(
            push_gate::verdict(&checked_of(&red, &files)),
            Verdict::Red(_)
        ));
        // The types alone passed: the touched tests never ran.
        let no_tests = SandboxRecord {
            ran: vec![CheckKind::Typecheck],
            passed: vec![CheckKind::Typecheck],
            ..Default::default()
        };
        assert!(matches!(
            push_gate::verdict(&checked_of(&no_tests, &files)),
            Verdict::Unverified(_)
        ));
        let owed = SandboxRecord {
            refused_for_time: true,
            ..green
        };
        assert_eq!(
            push_gate::verdict(&checked_of(&owed, &files)),
            Verdict::Owed
        );
        let install = SandboxRecord {
            failure: Some("the install exited 1".into()),
            ..Default::default()
        };
        assert!(matches!(
            push_gate::verdict(&checked_of(&install, &files)),
            Verdict::Unverified(w) if w.contains("install")
        ));
        assert!(matches!(
            push_gate::verdict(&checked_of(&SandboxRecord::default(), &files)),
            Verdict::Unverified(w) if w.contains("no sandbox")
        ));
        assert_eq!(
            failing_keys(&[], "nothing here", &["Test Suite".into()]),
            vec!["Test Suite"]
        );
    }
}
