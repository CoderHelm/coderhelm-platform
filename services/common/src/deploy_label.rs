//! The repository's deploy label as part of the preview check, not only a
//! wait before the merge.
//!
//! A repository can name labels its own CI reacts to (`deploy_label` in the
//! reviewer settings): a deploy to a preview environment, often followed by
//! end-to-end runs on real devices or browsers. CoderHelm put the label on
//! its pull request when it opened it, and the auto-merge gate waited for
//! that CI before merging. Nothing else looked at it: a pull request was
//! marked ready while the labelled workflow was still running, its
//! description never said whether that workflow passed, and a workflow that
//! takes its own label off when its run ends ("a label as a button") never
//! ran again after a fix round pushed a new commit.
//!
//! The deploy label's workflows are read the way the preview's are, into
//! the one record of the head's preview (`crate::preview::State`,
//! `preview_state` on the run):
//!
//! - [`covers`]: the jobs that ask for a configured deploy label (and the
//!   jobs that need them) cover the change like the preview plan's, each
//!   with the label that starts it. Their runs on the head are read, waited
//!   for and quoted like any covering check.
//! - [`removed_by`] / [`put_back`]: a label is added on a head only when it
//!   was never on the pull request, or a workflow or CoderHelm took it off
//!   (the button case). One a person or another app took off, or that
//!   CoderHelm can't tell who took off, stays off ([`KeptOff`]), and the
//!   description says so. This holds for every label CoderHelm adds, the
//!   preview plan's too, and in the auto-merge gate.

use serde::{Deserialize, Serialize};

use crate::e2e::workflow_gate::{self, WorkflowGate};
use crate::preview::{self as pv, Covering};

/// After a deploy label is added, how long a "green" result is not trusted
/// unless a check has started since (label-triggered workflows take a
/// moment to register their checks). Used by the auto-merge gate.
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

/// The name a job's check is shown under: its `name` (up to an expression,
/// for a matrix), else its key.
fn check_name(id: &str, name: &str) -> String {
    let n = match name.find("${{") {
        Some(i) => name[..i].trim_end().trim_end_matches('(').trim_end(),
        None => name.trim(),
    };
    if n.is_empty() {
        id.to_string()
    } else {
        n.to_string()
    }
}

/// The checks the deploy label's workflows run on a pull request: every job
/// that asks for one of `labels` (with the jobs that need such a job), from
/// each workflow file (`(path, text)`), with the label that starts it.
pub fn covers(workflows: &[(String, String)], labels: &[String]) -> Vec<Covering> {
    let mut out: Vec<Covering> = Vec::new();
    if labels.is_empty() {
        return out;
    }
    for (path, text) in workflows {
        let gate = workflow_gate::read(text);
        for job in &gate.jobs {
            let label = job.ways.iter().find_map(|w| {
                w.need.iter().find_map(|p| {
                    labels
                        .iter()
                        .find(|l| workflow_gate::matches(p, l))
                        .cloned()
                })
            });
            let Some(label) = label else { continue };
            let check = check_name(&job.id, &job.name);
            if out.iter().any(|c| c.check.eq_ignore_ascii_case(&check)) {
                continue;
            }
            out.push(Covering {
                role: Some(pv::role_of(&check)),
                check,
                workflow: path.clone(),
                label,
            });
        }
    }
    out
}

/// The deploy label's covering checks whose workflow skips drafts: they
/// run once the pull request is marked ready (`preview::Plan::draft_gated`).
pub fn draft_gated(workflows: &[(String, String)], covers: &[Covering]) -> Vec<String> {
    covers
        .iter()
        .filter(|c| {
            workflows
                .iter()
                .any(|(path, text)| path == &c.workflow && workflow_gate::read(text).draft)
        })
        .map(|c| c.check.clone())
        .collect()
}

/// The preview plan's covering checks with the deploy label's: one entry
/// per check. A check both name keeps the plan's role and gets the label.
pub fn with_covers(plan: &[Covering], deploy: &[Covering]) -> Vec<Covering> {
    let mut out: Vec<Covering> = plan.to_vec();
    for d in deploy {
        match out
            .iter_mut()
            .find(|c| c.check.eq_ignore_ascii_case(&d.check))
        {
            Some(c) => c.label = d.label.clone(),
            None => out.push(d.clone()),
        }
    }
    out
}

/// Who last took a label off the pull request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemovedBy {
    /// A workflow (`github-actions[bot]`) or CoderHelm's own app.
    Workflow,
    Person,
    /// Another app or bot account (GitHub's `Bot` actors, `…[bot]` logins).
    OtherApp,
    /// The pull request's events couldn't be read, or the removal names no
    /// one.
    Unknown,
    /// The events show no removal: the label was never taken off.
    Never,
}

impl RemovedBy {
    /// Who took the label off, in words.
    pub fn who(self) -> &'static str {
        match self {
            RemovedBy::Workflow => "its workflow",
            RemovedBy::Person => "a person",
            RemovedBy::OtherApp => "another app",
            RemovedBy::Unknown | RemovedBy::Never => "someone CoderHelm couldn't identify",
        }
    }

    /// CoderHelm may put the label on: it was never taken off, or its
    /// workflow (or CoderHelm) took it off.
    pub fn may_put_back(self) -> bool {
        matches!(self, RemovedBy::Workflow | RemovedBy::Never)
    }
}

/// Who took `label` off last, from the pull request's events (oldest
/// first, `None` when they couldn't be read). `bots` are the logins that
/// count as a workflow.
pub fn removed_by(events: Option<&serde_json::Value>, label: &str, bots: &[&str]) -> RemovedBy {
    let Some(events) = events else {
        return RemovedBy::Unknown;
    };
    let last = events.as_array().into_iter().flatten().rfind(|e| {
        e["event"].as_str() == Some("unlabeled")
            && e["label"]["name"]
                .as_str()
                .is_some_and(|n| n.eq_ignore_ascii_case(label))
    });
    let Some(actor) = last.map(|e| &e["actor"]) else {
        return RemovedBy::Never;
    };
    match actor["login"].as_str() {
        None => RemovedBy::Unknown,
        Some(a) if bots.iter().any(|b| b.eq_ignore_ascii_case(a)) => RemovedBy::Workflow,
        Some(a) if actor["type"].as_str() == Some("Bot") || a.ends_with("[bot]") => {
            RemovedBy::OtherApp
        }
        Some(_) => RemovedBy::Person,
    }
}

/// A label CoderHelm left off this head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeptOff {
    pub label: String,
    pub by: RemovedBy,
}

impl KeptOff {
    /// One sentence for the full test evidence and the preview's why.
    pub fn line(&self) -> String {
        format!(
            "`{}` was taken off by {}, so CoderHelm left it off this head",
            self.label.replace('`', "'"),
            self.by.who()
        )
    }
}

/// The labels among `want` CoderHelm may add, and the ones it leaves off,
/// from the pull request's events (`None` when they couldn't be read).
pub fn put_back(
    want: &[String],
    events: Option<&serde_json::Value>,
    bots: &[&str],
) -> (Vec<String>, Vec<KeptOff>) {
    let mut add = Vec::new();
    let mut off = Vec::new();
    for l in want {
        let by = removed_by(events, l, bots);
        if by.may_put_back() {
            add.push(l.clone());
        } else {
            off.push(KeptOff {
                label: l.clone(),
                by,
            });
        }
    }
    (add, off)
}

/// Pure: may a green result be trusted yet, given when deploy labels were
/// added? Until a check has started since (or [`GRACE_SECS`] passed) green
/// means "not yet".
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

    fn labels() -> Vec<String> {
        vec!["CI:DEPLOY_PREVIEW".into()]
    }

    const DEPLOY: &str = r#"
on:
  pull_request:
    types: [labeled, synchronize]
jobs:
  deploy:
    name: Deploy app
    if: contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_PREVIEW')
    runs-on: x
  devices:
    name: Device tests (${{ matrix.platform }})
    needs: deploy
    runs-on: x
  unit:
    name: unit tests
    runs-on: x
"#;

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
        assert!(starts(&workflow_gate::read(DEPLOY), &labels()));
        let prefix = workflow_gate::read(
            "jobs:\n  e2e:\n    if: startsWith(github.event.label.name, 'E2E:')\n    runs-on: x\n",
        );
        assert!(starts(&prefix, &["E2E:devices".into()]));
        assert!(!starts(&prefix, &labels()));
    }

    #[test]
    fn the_deploy_labels_jobs_cover_the_change_with_the_label_that_starts_them() {
        let wfs = vec![(
            ".github/workflows/deploy.yml".to_string(),
            DEPLOY.to_string(),
        )];
        let c = covers(&wfs, &labels());
        let names: Vec<&str> = c.iter().map(|x| x.check.as_str()).collect();
        // The deploy job, and the device job that needs it; never the unit
        // tests, which ask for no label.
        assert_eq!(names, vec!["Deploy app", "Device tests"]);
        assert!(c.iter().all(|x| x.label == "CI:DEPLOY_PREVIEW"));
        assert!(c
            .iter()
            .all(|x| x.workflow == ".github/workflows/deploy.yml"));
        // A matrix variant's check is the covering job's.
        assert!(pv::is_covering(&c, "Device tests (ios)"));
        assert!(covers(&wfs, &[]).is_empty());
        assert!(covers(&wfs, &["other".into()]).is_empty()); // Its workflow runs on drafts: nothing waits for it to be ready.
        assert!(draft_gated(&wfs, &c).is_empty());
        let drafty = vec![(
            ".github/workflows/deploy.yml".to_string(),
            DEPLOY.replace(
                "if: contains(",
                "if: github.event.pull_request.draft == false && contains(",
            ),
        )];
        assert_eq!(
            draft_gated(&drafty, &covers(&drafty, &labels())),
            vec!["Deploy app".to_string(), "Device tests".to_string()]
        );
    }

    #[test]
    fn a_check_the_plan_and_the_label_both_name_is_one_covering_check() {
        let plan = vec![Covering {
            check: "Deploy app".into(),
            role: Some(pv::Role::Preview),
            workflow: "w.yml".into(),
            label: String::new(),
        }];
        let deploy = vec![
            Covering {
                check: "deploy app".into(),
                role: Some(pv::Role::Other),
                workflow: "w.yml".into(),
                label: "CI:DEPLOY_PREVIEW".into(),
            },
            Covering {
                check: "Device tests".into(),
                role: Some(pv::Role::Other),
                workflow: "w.yml".into(),
                label: "CI:DEPLOY_PREVIEW".into(),
            },
        ];
        let all = with_covers(&plan, &deploy);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].role, Some(pv::Role::Preview));
        assert_eq!(all[0].label, "CI:DEPLOY_PREVIEW");
        assert_eq!(all[1].check, "Device tests");
    }

    #[test]
    fn a_label_is_put_back_only_after_its_workflow_took_it_off_or_when_never_removed() {
        let bots = ["github-actions[bot]", "coderhelm[bot]"];
        let ev = |login: &str, kind: Option<&str>| {
            let mut actor = json!({ "login": login });
            if let Some(k) = kind {
                actor["type"] = json!(k);
            }
            json!([
                {"event": "labeled", "label": {"name": "CI:DEPLOY_PREVIEW"}, "actor": {"login": "coderhelm[bot]"}},
                {"event": "unlabeled", "label": {"name": "ci:deploy_preview"}, "actor": actor}
            ])
        };
        let by = |e: Option<&serde_json::Value>| removed_by(e, "CI:DEPLOY_PREVIEW", &bots);
        assert_eq!(
            by(Some(&ev("github-actions[bot]", None))),
            RemovedBy::Workflow
        );
        assert_eq!(by(Some(&ev("coderhelm[bot]", None))), RemovedBy::Workflow);
        assert_eq!(by(Some(&ev("dana", Some("User")))), RemovedBy::Person);
        assert_eq!(
            by(Some(&ev("deploy-bot[bot]", Some("Bot")))),
            RemovedBy::OtherApp
        );
        // Never taken off, or the events unreadable: different answers.
        assert_eq!(by(Some(&json!([]))), RemovedBy::Never);
        assert_eq!(by(None), RemovedBy::Unknown);

        let want = labels();
        let (add, off) = put_back(&want, Some(&ev("github-actions[bot]", None)), &bots);
        assert_eq!((add, off.len()), (want.clone(), 0));
        let (add, off) = put_back(&want, Some(&json!([])), &bots);
        assert_eq!((add, off.len()), (want.clone(), 0));
        for events in [Some(ev("dana", Some("User"))), None] {
            let (add, off) = put_back(&want, events.as_ref(), &bots);
            assert!(add.is_empty());
            assert_eq!(off[0].label, "CI:DEPLOY_PREVIEW");
        }
        let (_, off) = put_back(&want, Some(&ev("dana", Some("User"))), &bots);
        assert_eq!(
            off[0].line(),
            "`CI:DEPLOY_PREVIEW` was taken off by a person, so CoderHelm left it off this head"
        );
        let (_, off) = put_back(&want, None, &bots);
        assert_eq!(
            off[0].line(),
            "`CI:DEPLOY_PREVIEW` was taken off by someone CoderHelm couldn't identify, so \
             CoderHelm left it off this head"
        );
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
