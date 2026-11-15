//! The repository's deploy label as a verification gate, not only a wait
//! before the merge.
//!
//! A repository can name labels its own CI reacts to (`deploy_label` in the
//! reviewer settings): a deploy to a preview or staging environment, often
//! followed by an end-to-end suite on real devices or browsers. CoderHelm put
//! the label on its pull request when it opened it, and the auto-merge gate
//! waited for that CI before merging. Nothing else looked at it: a pull
//! request was marked ready and put in front of a person while the labelled
//! workflow was still running, its description never said whether that
//! workflow passed, and a workflow that takes its own label off when its run
//! ends ("a label as a button") never ran again after a fix round pushed a
//! new commit.
//!
//! The rules:
//!
//! - A deploy-label workflow is one whose jobs ask for one of the configured
//!   labels ([`starts`], read with `e2e::workflow_gate`). Its latest run on
//!   the head ([`latest_per_workflow`]) is the evidence.
//! - The description quotes each such run: its conclusion and a link to the
//!   run (`pr_facts::rows` and the full test evidence).
//! - The pull request leaves draft only once those runs finished
//!   ([`standing`]). A label added for this head whose workflow hasn't
//!   started yet is waited for a short while ([`GRACE_SECS`]), the same
//!   grace the auto-merge gate gives it. A failed run is the repository's CI
//!   failing: it goes to the run's CI fix loop like any other failing check.
//! - A label that came off after its run is put back once on a new head
//!   ([`to_add`]): only when CoderHelm put it on the pull request before,
//!   never twice on the same head (the merge gate's record keeps the head it
//!   was added for, `merge_gate::record_labels_added`), and never when a run
//!   of the workflow already ran on the head. So it can't loop. Only a label
//!   a workflow or CoderHelm took off ([`removed_by`]): one a person took
//!   off, or that CoderHelm can't tell who took off, stays off, and the
//!   description says so ([`KeptOff`]).
//! - A run held for a protected environment's approval waits for a person,
//!   not for time ([`Standing::Approval`]): the ready decision asks for that
//!   approval.

use serde::{Deserialize, Serialize};

use crate::e2e::workflow_gate::{self, WorkflowGate};

/// After a deploy label is added, how long a head without a run of its
/// workflow is still waited for (label-triggered workflows take a moment
/// to register their run). Shared with the auto-merge gate.
pub const GRACE_SECS: i64 = 180;

/// The labels a repository's `deploy_label` setting names (comma separated).
pub fn configured(setting: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for l in setting.split(',').map(str::trim).filter(|l| !l.is_empty()) {
        if !out.iter().any(|o| o.eq_ignore_ascii_case(l)) {
            out.push(l.to_string());
        }
    }
    out
}

/// Whether a workflow's jobs ask for one of `labels`: the workflow the
/// deploy label starts.
pub fn starts(gate: &WorkflowGate, labels: &[String]) -> bool {
    gate.labels()
        .iter()
        .any(|p| labels.iter().any(|l| workflow_gate::matches(p, l)))
}

/// How a run ended, as the gate reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Running,
    /// Held for a protected environment's required approval (GitHub's
    /// `waiting`): it goes on only once a person approves the deployment.
    Approval,
    Passed,
    Failed,
    /// Skipped, cancelled, neutral or waiting for an approval to run at
    /// all: it proved nothing on this head.
    DidNotRun,
}

/// One run of a deploy-label workflow on a head.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeployRun {
    /// The workflow's name, as GitHub shows it.
    pub workflow: String,
    /// Its file (`.github/workflows/deploy.yml`).
    #[serde(default)]
    pub path: String,
    /// GitHub's `status` (`queued`, `in_progress`, `completed`, …).
    pub status: String,
    /// GitHub's `conclusion` once completed.
    #[serde(default)]
    pub conclusion: String,
    /// The run's page.
    #[serde(default)]
    pub url: String,
    /// When it was created (RFC 3339).
    #[serde(default)]
    pub created_at: String,
    /// The run's id.
    #[serde(default)]
    pub id: u64,
    /// The environment a run held for an approval deploys to, when it could
    /// be read.
    #[serde(default)]
    pub environment: String,
}

impl DeployRun {
    /// A run from GitHub's workflow run listing.
    pub fn of(run: &serde_json::Value) -> DeployRun {
        let s = |k: &str| run[k].as_str().unwrap_or("").to_string();
        DeployRun {
            workflow: s("name"),
            path: s("path"),
            status: s("status"),
            conclusion: s("conclusion"),
            url: s("html_url"),
            created_at: s("created_at"),
            id: run["id"].as_u64().unwrap_or(0),
            environment: String::new(),
        }
    }

    pub fn state(&self) -> RunState {
        if self.status == "waiting" {
            return RunState::Approval;
        }
        if self.status != "completed" {
            return RunState::Running;
        }
        match self.conclusion.as_str() {
            "success" => RunState::Passed,
            "failure" | "timed_out" | "startup_failure" => RunState::Failed,
            _ => RunState::DidNotRun,
        }
    }

    /// `created_at` as epoch seconds.
    pub fn created_secs(&self) -> Option<i64> {
        chrono::DateTime::parse_from_rfc3339(&self.created_at)
            .ok()
            .map(|d| d.timestamp())
    }

    /// The run's link, when it has a page.
    pub fn link(&self) -> String {
        if self.url.starts_with("https://") {
            format!("[run]({})", self.url)
        } else {
            String::new()
        }
    }

    /// Its conclusion in words.
    pub fn result(&self) -> String {
        match self.state() {
            RunState::Running => "still running".to_string(),
            RunState::Approval => match self.environment.as_str() {
                "" => "waiting for an approval to deploy".to_string(),
                env => format!(
                    "waiting for an approval to deploy to `{}`",
                    env.replace('`', "'")
                ),
            },
            RunState::Passed => "passed".to_string(),
            RunState::Failed => match self.conclusion.as_str() {
                "timed_out" => "timed out".to_string(),
                "startup_failure" => "failed to start".to_string(),
                _ => "failed".to_string(),
            },
            RunState::DidNotRun => match self.conclusion.as_str() {
                "" => "did not run".to_string(),
                "action_required" => "waiting for an approval to run".to_string(),
                c => c.replace('_', " "),
            },
        }
    }

    /// One line for the full test evidence.
    pub fn evidence_line(&self) -> String {
        let name = self.workflow.replace('`', "'");
        let link = self.link();
        if link.is_empty() {
            format!("- `{name}`: {}", self.result())
        } else {
            format!("- `{name}`: {} ({link})", self.result())
        }
    }
}

/// The latest run of each deploy-label workflow among a head's workflow
/// runs (GitHub's listing, any order), in the order the workflows were
/// first seen. Only pull request runs count: a push run of the same file
/// was not started by the label. A run that ran wins over a newer one that
/// didn't: another label on the pull request starts a run of a
/// `labeled` workflow too, which its jobs skip.
pub fn latest_per_workflow(runs: &[serde_json::Value], deploy_paths: &[String]) -> Vec<DeployRun> {
    let mut out: Vec<DeployRun> = Vec::new();
    for r in runs {
        if !matches!(
            r["event"].as_str(),
            Some("pull_request" | "pull_request_target")
        ) {
            continue;
        }
        let run = DeployRun::of(r);
        if !deploy_paths.iter().any(|p| p == &run.path) {
            continue;
        }
        let ran = |r: &DeployRun| r.state() != RunState::DidNotRun;
        match out.iter_mut().find(|o| o.path == run.path) {
            Some(o) => {
                let newer = o.created_at <= run.created_at;
                if (ran(&run) && !ran(o)) || (ran(&run) == ran(o) && newer) {
                    *o = run;
                }
            }
            None => out.push(run),
        }
    }
    out
}

/// Where the deploy label's workflows stand on a head.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Standing {
    /// No deploy label, or nothing it started on this head to wait for.
    #[default]
    None,
    /// A run is going, or the label was just added and its run hasn't
    /// started: the pull request waits (the why, in words).
    Waiting(String),
    /// The runs held for a protected environment's approval: only a person
    /// can let them go on.
    Approval(Vec<DeployRun>),
    Passed,
    /// The runs that failed. They are failing CI on the head, fixed by the
    /// run's CI fix loop.
    Failed(Vec<DeployRun>),
}

fn names(runs: &[&DeployRun]) -> String {
    runs.iter()
        .map(|r| format!("`{}`", r.workflow.replace('`', "'")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Where the deploy label's workflows stand on the head: `runs` their latest
/// runs on it, `added_at` when CoderHelm added the labels for this head (none
/// when it didn't), at `now`.
pub fn standing(
    labels: &[String],
    runs: &[DeployRun],
    added_at: Option<i64>,
    now: i64,
) -> Standing {
    if labels.is_empty() {
        return Standing::None;
    }
    let held: Vec<DeployRun> = runs
        .iter()
        .filter(|r| r.state() == RunState::Approval)
        .cloned()
        .collect();
    if !held.is_empty() {
        return Standing::Approval(held);
    }
    let running: Vec<&DeployRun> = runs
        .iter()
        .filter(|r| r.state() == RunState::Running)
        .collect();
    if !running.is_empty() {
        return Standing::Waiting(format!(
            "the deploy label's workflow {} is still running on this head",
            names(&running)
        ));
    }
    if let Some(at) = added_at {
        let started = runs
            .iter()
            .filter(|r| r.state() != RunState::DidNotRun)
            .any(|r| r.created_secs().is_some_and(|c| c >= at));
        if !started && now - at < GRACE_SECS {
            let named: Vec<String> = labels.iter().map(|l| format!("`{l}`")).collect();
            return Standing::Waiting(match named.as_slice() {
                [one] => format!("{one} was added to this head; its workflow hasn't started yet"),
                [rest @ .., last] => format!(
                    "{} and {last} were added to this head; their workflows haven't started yet",
                    rest.join(", ")
                ),
                [] => "the deploy label was added to this head; its workflow hasn't started yet"
                    .to_string(),
            });
        }
    }
    let failed: Vec<DeployRun> = runs
        .iter()
        .filter(|r| r.state() == RunState::Failed)
        .cloned()
        .collect();
    if !failed.is_empty() {
        return Standing::Failed(failed);
    }
    if runs.iter().any(|r| r.state() == RunState::Passed) {
        return Standing::Passed;
    }
    Standing::None
}

/// The deploy labels to put back on the pull request for this head: the
/// configured ones missing from it, when CoderHelm added them on an earlier
/// head (`added_head`, from the merge gate's record) and no run of their
/// workflow ran on this one. Empty otherwise: never on a pull request
/// CoderHelm never labelled, never twice on one head.
pub fn to_add(
    want: &[String],
    on_pr: &[String],
    added_head: &str,
    head: &str,
    runs: &[DeployRun],
) -> Vec<String> {
    if want.is_empty() || head.is_empty() || added_head.is_empty() || added_head == head {
        return Vec::new();
    }
    if runs.iter().any(|r| r.state() != RunState::DidNotRun) {
        return Vec::new();
    }
    want.iter()
        .filter(|l| !on_pr.iter().any(|p| p.eq_ignore_ascii_case(l)))
        .cloned()
        .collect()
}

/// Who last took a deploy label off the pull request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemovedBy {
    /// A workflow (`github-actions[bot]`) or CoderHelm's own app.
    Workflow,
    Person,
    /// The pull request's events couldn't be read, or show no removal.
    Unknown,
}

/// Who took `label` off last, from the pull request's events (oldest
/// first, `None` when they couldn't be read). `bots` are the logins that
/// count as a workflow.
pub fn removed_by(events: Option<&serde_json::Value>, label: &str, bots: &[&str]) -> RemovedBy {
    let last = events
        .and_then(|e| e.as_array())
        .into_iter()
        .flatten()
        .rfind(|e| {
            e["event"].as_str() == Some("unlabeled")
                && e["label"]["name"]
                    .as_str()
                    .is_some_and(|n| n.eq_ignore_ascii_case(label))
        });
    match last.and_then(|e| e["actor"]["login"].as_str()) {
        None => RemovedBy::Unknown,
        Some(a) if bots.iter().any(|b| b.eq_ignore_ascii_case(a)) => RemovedBy::Workflow,
        Some(_) => RemovedBy::Person,
    }
}

/// A deploy label CoderHelm left off this head because a workflow didn't
/// take it off.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeptOff {
    pub label: String,
    pub by: RemovedBy,
}

impl KeptOff {
    /// One sentence for the full test evidence.
    pub fn evidence_line(&self) -> String {
        let label = self.label.replace('`', "'");
        let by = match self.by {
            RemovedBy::Person => "a person",
            _ => "someone CoderHelm couldn't identify",
        };
        format!(
            "`{label}` was taken off by {by}, so CoderHelm did not put it back on this head and \
             its workflow did not run here."
        )
    }
}

/// Pure: may a green result be trusted yet, given when deploy labels were
/// added? Label-triggered workflows take a moment to register their checks;
/// until one has started (or [`GRACE_SECS`] passed) green means "not yet".
pub fn settled(labels_added_at: Option<i64>, latest_start: Option<i64>, now: i64) -> bool {
    match labels_added_at {
        None => true,
        Some(added) => now - added >= GRACE_SECS || latest_start.is_some_and(|s| s >= added),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const DEPLOY: &str = ".github/workflows/deploy-preview.yml";

    fn run(status: &str, conclusion: &str, created: &str) -> DeployRun {
        DeployRun {
            workflow: "Deploy preview".into(),
            path: DEPLOY.into(),
            status: status.into(),
            conclusion: conclusion.into(),
            url: "https://github.com/o/r/actions/runs/1".into(),
            created_at: created.into(),
            ..Default::default()
        }
    }

    fn labels() -> Vec<String> {
        vec!["CI:DEPLOY_PREVIEW".into()]
    }

    fn at(t: &str) -> i64 {
        chrono::DateTime::parse_from_rfc3339(t).unwrap().timestamp()
    }

    #[test]
    fn the_setting_is_a_list_of_labels() {
        assert_eq!(
            configured(" CI:DEPLOY_PREVIEW , e2e:devices,,ci:deploy_preview"),
            vec!["CI:DEPLOY_PREVIEW", "e2e:devices"]
        );
        assert!(configured("  ").is_empty());
    }

    #[test]
    fn a_workflow_whose_jobs_ask_for_the_label_is_the_deploy_workflow() {
        let gate = workflow_gate::read(
            "on:\n  pull_request:\n    types: [labeled]\njobs:\n  deploy:\n    if: github.event.label.name == 'CI:DEPLOY_PREVIEW'\n    runs-on: x\n",
        );
        assert!(starts(&gate, &labels()));
        let prefix = workflow_gate::read(
            "jobs:\n  e2e:\n    if: startsWith(github.event.label.name, 'E2E:')\n    runs-on: x\n",
        );
        assert!(starts(&prefix, &["E2E:devices".into()]));
        assert!(!starts(&prefix, &labels()));
        let plain = workflow_gate::read("jobs:\n  test:\n    runs-on: x\n");
        assert!(!starts(&plain, &labels()));
    }

    #[test]
    fn the_latest_pull_request_run_of_each_deploy_workflow_is_kept() {
        let r = |path: &str, event: &str, status: &str, conclusion: &str, created: &str| {
            json!({"name": "Deploy preview", "path": path, "event": event, "status": status,
                   "conclusion": conclusion, "html_url": "https://github.com/o/r/actions/runs/9",
                   "created_at": created})
        };
        let runs = vec![
            r(
                DEPLOY,
                "pull_request",
                "completed",
                "success",
                "2026-11-14T10:05:00Z",
            ),
            r(
                DEPLOY,
                "pull_request",
                "completed",
                "skipped",
                "2026-11-14T10:00:00Z",
            ),
            r(DEPLOY, "push", "in_progress", "", "2026-11-14T10:09:00Z"),
            r(
                ".github/workflows/test.yml",
                "pull_request",
                "completed",
                "failure",
                "2026-11-14T10:01:00Z",
            ),
        ];
        let got = latest_per_workflow(&runs, &[DEPLOY.to_string()]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].state(), RunState::Passed);
        assert_eq!(got[0].created_at, "2026-11-14T10:05:00Z");
    }

    #[test]
    fn conclusions_read_as_the_gate_needs_them() {
        assert_eq!(run("queued", "", "").state(), RunState::Running);
        assert_eq!(run("waiting", "", "").state(), RunState::Approval);
        assert_eq!(run("completed", "success", "").state(), RunState::Passed);
        for c in ["failure", "timed_out", "startup_failure"] {
            assert_eq!(run("completed", c, "").state(), RunState::Failed, "{c}");
        }
        for c in [
            "skipped",
            "cancelled",
            "neutral",
            "action_required",
            "stale",
        ] {
            assert_eq!(run("completed", c, "").state(), RunState::DidNotRun, "{c}");
        }
        assert_eq!(
            run("completed", "failure", "").evidence_line(),
            "- `Deploy preview`: failed ([run](https://github.com/o/r/actions/runs/1))"
        );
        let mut no_page = run("completed", "skipped", "");
        no_page.url.clear();
        assert_eq!(no_page.evidence_line(), "- `Deploy preview`: skipped");
    }

    #[test]
    fn a_running_or_just_labelled_head_waits_and_a_finished_one_says_how_it_ended() {
        let now = at("2026-11-14T10:10:00Z");
        // No deploy label configured: nothing to wait for.
        assert_eq!(
            standing(&[], &[run("in_progress", "", "")], None, now),
            Standing::None
        );
        // A run going.
        match standing(&labels(), &[run("in_progress", "", "")], None, now) {
            Standing::Waiting(why) => assert!(why.contains("`Deploy preview` is still running")),
            s => panic!("{s:?}"),
        }
        // Just added; only the push's skipped run is on the head.
        let skipped = run("completed", "skipped", "2026-11-14T10:00:00Z");
        let added = now - 30;
        match standing(&labels(), std::slice::from_ref(&skipped), Some(added), now) {
            Standing::Waiting(why) => assert_eq!(
                why,
                "`CI:DEPLOY_PREVIEW` was added to this head; its workflow hasn't started yet"
            ),
            s => panic!("{s:?}"),
        }
        let two: Vec<String> = vec!["CI:DEPLOY_PREVIEW".into(), "e2e:devices".into()];
        assert_eq!(
            standing(&two, &[], Some(added), now),
            Standing::Waiting(
                "`CI:DEPLOY_PREVIEW` and `e2e:devices` were added to this head; their workflows \
                 haven't started yet"
                    .into()
            )
        );
        // Past the grace with no run: nothing to wait for, never forever.
        assert_eq!(
            standing(
                &labels(),
                std::slice::from_ref(&skipped),
                Some(now - GRACE_SECS),
                now
            ),
            Standing::None
        );
        // Finished.
        let passed = run("completed", "success", "2026-11-14T10:09:50Z");
        assert_eq!(
            standing(&labels(), &[passed], Some(added), now),
            Standing::Passed
        );
        let failed = run("completed", "failure", "2026-11-14T10:09:50Z");
        assert_eq!(
            standing(&labels(), std::slice::from_ref(&failed), Some(added), now),
            Standing::Failed(vec![failed])
        );
    }

    #[test]
    fn a_skipped_run_from_another_label_never_hides_the_deploy_run() {
        // A `labeled` workflow gets a run per label: the deploy label's runs,
        // the review label's (added in the same call) is skipped a second
        // later.
        let r = |status: &str, conclusion: &str, created: &str| {
            json!({"name": "Deploy preview", "path": DEPLOY, "event": "pull_request",
                   "status": status, "conclusion": conclusion,
                   "html_url": "https://github.com/o/r/actions/runs/9", "created_at": created})
        };
        let paths = [DEPLOY.to_string()];
        let added = at("2026-11-14T10:04:59Z");
        let now = at("2026-11-14T10:20:00Z");
        for (real, state) in [
            ("success", Standing::Passed),
            ("failure", Standing::Failed(vec![])),
        ] {
            let runs = vec![
                r("completed", real, "2026-11-14T10:05:00Z"),
                r("completed", "skipped", "2026-11-14T10:05:01Z"),
            ];
            let got = latest_per_workflow(&runs, &paths);
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].conclusion, real);
            match (standing(&labels(), &got, Some(added), now), state) {
                (Standing::Failed(f), Standing::Failed(_)) => assert_eq!(f, got),
                (s, want) => assert_eq!(s, want),
            }
            // A run ran on this head: the label is not put back over it.
            assert!(to_add(&labels(), &[], "old", "new", &got).is_empty());
        }
        // Only the skipped run registered so far: the label's run is still
        // waited for.
        let only_skipped =
            latest_per_workflow(&[r("completed", "skipped", "2026-11-14T10:05:01Z")], &paths);
        assert!(matches!(
            standing(&labels(), &only_skipped, Some(added), added + 30),
            Standing::Waiting(_)
        ));
    }

    #[test]
    fn a_label_that_came_off_is_put_back_once_per_head_and_never_loops() {
        let want = labels();
        let skipped = run("completed", "skipped", "2026-11-14T10:00:00Z");
        // Added on the head before; the workflow took it off after its run.
        assert_eq!(
            to_add(&want, &[], "old", "new", std::slice::from_ref(&skipped)),
            want
        );
        // Already put back on this head: never again, whatever happened.
        assert!(to_add(&want, &[], "new", "new", &[]).is_empty());
        // Still on the pull request: nothing to add.
        assert!(to_add(&want, &["ci:deploy_preview".into()], "old", "new", &[]).is_empty());
        // A run already ran on this head (its push started it).
        let ran = run("completed", "success", "2026-11-14T10:00:00Z");
        assert!(to_add(&want, &[], "old", "new", &[ran]).is_empty());
        // CoderHelm never labelled this pull request: never added here.
        assert!(to_add(&want, &[], "", "new", &[]).is_empty());
        assert!(to_add(&[], &[], "old", "new", &[]).is_empty());
    }

    #[test]
    fn a_run_held_for_an_environment_approval_is_asked_for_not_waited_on() {
        let now = at("2026-11-14T10:10:00Z");
        let mut held = run("waiting", "", "2026-11-14T10:01:00Z");
        assert_eq!(held.result(), "waiting for an approval to deploy");
        held.environment = "staging".into();
        assert_eq!(
            held.evidence_line(),
            "- `Deploy preview`: waiting for an approval to deploy to `staging` \
             ([run](https://github.com/o/r/actions/runs/1))"
        );
        // Held, even with another run still going: only a person moves it on.
        let going = run("in_progress", "", "2026-11-14T10:02:00Z");
        assert_eq!(
            standing(&labels(), &[going, held.clone()], None, now),
            Standing::Approval(vec![held.clone()])
        );
        // A held run ran on this head: the label is never put back over it.
        assert!(to_add(&labels(), &[], "old", "new", &[held]).is_empty());
    }

    #[test]
    fn only_a_label_a_workflow_took_off_counts_as_taken_off_by_a_workflow() {
        let ev = |event: &str, label: &str, login: &str| {
            json!({"event": event, "label": {"name": label}, "actor": {"login": login}})
        };
        let bots = ["github-actions[bot]", "coderhelm[bot]"];
        let events = json!([
            ev("labeled", "CI:DEPLOY_PREVIEW", "coderhelm[bot]"),
            ev("unlabeled", "CI:DEPLOY_PREVIEW", "github-actions[bot]"),
            ev("labeled", "e2e:devices", "coderhelm[bot]"),
            ev("unlabeled", "e2e:devices", "github-actions[bot]"),
            ev("labeled", "e2e:devices", "coderhelm[bot]"),
            ev("unlabeled", "e2e:devices", "octocat"),
        ]);
        assert_eq!(removed_by(Some(&events), "ci:deploy_preview", &bots), RemovedBy::Workflow);
        // The latest removal decides: a person's, after a workflow's.
        assert_eq!(removed_by(Some(&events), "e2e:devices", &bots), RemovedBy::Person);
        // No removal in the events, or the events unread: can't tell.
        assert_eq!(removed_by(Some(&events), "other", &bots), RemovedBy::Unknown);
        assert_eq!(removed_by(None, "CI:DEPLOY_PREVIEW", &bots), RemovedBy::Unknown);
        let kept = KeptOff {
            label: "CI:DEPLOY_PREVIEW".into(),
            by: RemovedBy::Person,
        };
        assert_eq!(
            kept.evidence_line(),
            "`CI:DEPLOY_PREVIEW` was taken off by a person, so CoderHelm did not put it back on \
             this head and its workflow did not run here."
        );
        let unknown = KeptOff {
            by: RemovedBy::Unknown,
            ..kept
        };
        assert!(unknown
            .evidence_line()
            .contains("taken off by someone CoderHelm couldn't identify"));
    }

    #[test]
    fn green_is_not_trusted_until_label_ci_starts() {
        let added = 1_000;
        assert!(!settled(Some(added), Some(added - 50), added + 30));
        assert!(settled(Some(added), Some(added + 10), added + 30));
        assert!(settled(Some(added), None, added + GRACE_SECS));
        assert!(settled(None, None, added));
    }
}
