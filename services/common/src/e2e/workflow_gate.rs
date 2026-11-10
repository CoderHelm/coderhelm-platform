//! Why a check was skipped on a pull request's head, read from its
//! workflow file: the job runs only on a pull request that isn't a draft,
//! only with a label, or for a reason CoderHelm can't read.
//!
//! A failing-test fix once stayed a draft waiting for its staging deploy
//! and staging end-to-end checks to go green. Those jobs run only when the
//! pull request is not a draft (`github.event.pull_request.draft == false`),
//! so on the draft they were skipped, and the draft waited on them every ten
//! minutes for twelve hours: draft, so skipped, so still a draft.
//!
//! The rules:
//!
//! - [`read`] reads a workflow's gates from its text: the draft conditions
//!   (`github.event.pull_request.draft == false`,
//!   `!github.event.pull_request.draft`, a `pull_request` trigger whose
//!   `types` start it only on `ready_for_review`) and the labels its `if:`
//!   conditions ask for (`contains(github.event.pull_request.labels.*.name,
//!   'X')`, `github.event.label.name == 'X'`).
//! - [`sort`] tells, for each check skipped on the head, why: a skipped
//!   check on a draft whose workflow is draft gated runs once the pull
//!   request is marked ready ([`Skipped::draft_gated`]); one whose workflow
//!   asks for a label runs once the label is added; any other is unknown.
//! - [`Skipped::action`] is the one plain thing a person can do when
//!   CoderHelm can't start them itself.

use serde::{Deserialize, Serialize};

/// What a workflow file needs before its jobs run on a pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowGate {
    /// Its jobs (or some of them) run only when the pull request is not a
    /// draft.
    #[serde(default)]
    pub draft: bool,
    /// Labels its conditions ask for, as written (`CI:DEPLOY_STAGING`); a
    /// family read from a prefix check ends in `*` (`E2E:*`).
    #[serde(default)]
    pub labels: Vec<String>,
    /// It starts when a pull request is marked ready (`ready_for_review` in
    /// its `pull_request` types). Without it, a job that skips drafts stays
    /// skipped after the pull request is marked ready, until the next push.
    #[serde(default)]
    pub on_ready: bool,
}

const DRAFT_CONDITIONS: &[&str] = &[
    "pull_request.draft==false",
    "pull_request.draft=='false'",
    "pull_request.draft!=true",
    "false==github.event.pull_request.draft",
    "!github.event.pull_request.draft",
    "!(github.event.pull_request.draft)",
];

/// The gates of one workflow file, from its text. Never fails: an
/// unreadable file has no gates.
pub fn read(yaml: &str) -> WorkflowGate {
    let mut g = WorkflowGate::default();
    for line in yaml.lines() {
        let line = line.split(" #").next().unwrap_or(line);
        let compact: String = line.chars().filter(|c| !c.is_whitespace()).collect();
        let lower = compact.to_ascii_lowercase();
        if DRAFT_CONDITIONS.iter().any(|c| lower.contains(c)) {
            g.draft = true;
        }
        for label in labels_in(&compact) {
            if !g.labels.iter().any(|l| l.eq_ignore_ascii_case(&label)) {
                g.labels.push(label);
            }
        }
    }
    let types = pull_request_types(yaml);
    g.on_ready = types.iter().any(|t| t == "ready_for_review");
    // Started only when marked ready: it never starts on a draft's push.
    if g.on_ready
        && !types
            .iter()
            .any(|t| matches!(t.as_str(), "opened" | "reopened" | "synchronize"))
    {
        g.draft = true;
    }
    g
}

/// The `types` of the workflow's `pull_request` (and `pull_request_target`)
/// triggers; empty when it lists none (GitHub's default then leaves out
/// `ready_for_review`).
fn pull_request_types(yaml: &str) -> Vec<String> {
    let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
        return Vec::new();
    };
    let Some(on) = v.get("on").or_else(|| v.get(serde_yaml::Value::Bool(true))) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for ev in ["pull_request", "pull_request_target"] {
        for t in on
            .get(ev)
            .and_then(|e| e.get("types"))
            .and_then(|t| t.as_sequence())
            .into_iter()
            .flatten()
        {
            if let Some(t) = t.as_str() {
                out.push(t.to_string());
            }
        }
    }
    out
}

/// The labels one condition line asks for (whitespace already removed).
fn labels_in(compact: &str) -> Vec<String> {
    let lower = compact.to_ascii_lowercase();
    let mut out = Vec::new();
    for key in ["labels.*.name", "label.name"] {
        for (i, _) in lower.match_indices(key) {
            // `labels.*.name` also contains `label`… only as `labels`, so a
            // `label.name` match inside `labels.*.name` never happens.
            let tail = &compact[i + key.len()..];
            let end = ["&&", "||", "}}"]
                .iter()
                .filter_map(|s| tail.find(s))
                .min()
                .unwrap_or(tail.len());
            let before = &lower[..i];
            // A label that stops the job (`!contains(…, 'skip-e2e')`,
            // `label.name != 'x'`) is not one that runs it.
            if before.ends_with("!contains(github.event.pull_request.")
                || before.ends_with("!contains(join(github.event.pull_request.")
                || tail.starts_with("!=")
            {
                continue;
            }
            let joined = before.ends_with("join(github.event.pull_request.");
            for q in quoted(&tail[..end]) {
                if q.is_empty() || q == "," || q == ", " {
                    continue;
                }
                let name = if joined && !q.ends_with('*') {
                    format!("{q}*")
                } else {
                    q
                };
                if !out.iter().any(|l: &String| l.eq_ignore_ascii_case(&name)) {
                    out.push(name);
                }
            }
        }
    }
    out
}

/// The quoted strings in a condition fragment, in order.
fn quoted(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        if c != '\'' && c != '"' {
            continue;
        }
        let rest = &s[i + 1..];
        match rest.find(c) {
            Some(j) => {
                out.push(rest[..j].to_string());
                for _ in 0..=j {
                    chars.next();
                }
            }
            None => break,
        }
    }
    out
}

/// The checks skipped on a head, sorted by why.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Skipped {
    /// Skipped because the pull request is a draft: they run once it is
    /// marked ready.
    pub draft_gated: Vec<String>,
    /// Skipped because the pull request is a draft, in a workflow that
    /// doesn't start when it is marked ready: they run on the next push
    /// after it is ready.
    pub draft_no_start: Vec<String>,
    /// Skipped because a label is missing: (check, labels that run it).
    pub label_gated: Vec<(String, Vec<String>)>,
    /// Skipped for a reason CoderHelm can't read.
    pub unknown: Vec<String>,
}

/// Whether a label on the pull request is one of `wanted` (a family `E2E:*`
/// matches any label starting with `E2E:`). Case-insensitive.
fn has_label(on_pr: &[String], wanted: &[String]) -> bool {
    wanted.iter().any(|w| {
        let w = w.to_ascii_lowercase();
        on_pr.iter().any(|l| {
            let l = l.to_ascii_lowercase();
            match w.strip_suffix('*') {
                Some(prefix) => l.starts_with(prefix),
                None => l == w,
            }
        })
    })
}

/// Sorts skipped checks by why they were skipped. `skipped` holds each
/// check with its workflow's gate, `None` when the workflow couldn't be
/// read; `on_pr` is the pull request's labels. A workflow that needs a label
/// the pull request lacks is label-gated even on a draft: marking it ready
/// alone wouldn't run it. A draft-gated check skipped on a pull request
/// that is ready now was skipped while it was a draft.
pub fn sort(skipped: &[(String, Option<WorkflowGate>)], on_pr: &[String]) -> Skipped {
    let mut s = Skipped::default();
    for (name, gate) in skipped {
        match gate {
            Some(g) if !g.labels.is_empty() && !has_label(on_pr, &g.labels) => {
                s.label_gated.push((name.clone(), g.labels.clone()))
            }
            Some(g) if g.draft && g.on_ready => s.draft_gated.push(name.clone()),
            Some(g) if g.draft => s.draft_no_start.push(name.clone()),
            _ => s.unknown.push(name.clone()),
        }
    }
    s
}

fn list(v: &[String]) -> String {
    v.iter()
        .map(|n| format!("`{n}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

impl Skipped {
    /// Every skipped check runs once the pull request is marked ready.
    pub fn all_draft_gated(&self) -> bool {
        !self.draft_gated.is_empty()
            && self.draft_no_start.is_empty()
            && self.label_gated.is_empty()
            && self.unknown.is_empty()
    }

    /// The labels that would run the label-gated checks.
    pub fn labels(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for (_, ls) in &self.label_gated {
            for l in ls {
                if !out.iter().any(|o| o.eq_ignore_ascii_case(l)) {
                    out.push(l.clone());
                }
            }
        }
        out
    }

    /// What a person can do, in one sentence, when CoderHelm can't start
    /// the skipped checks (label-gated or unknown) itself.
    pub fn action(&self) -> Option<String> {
        let mut parts = Vec::new();
        if !self.draft_no_start.is_empty() {
            parts.push(format!(
                "{} skip drafts and their workflow doesn't start when a pull request is marked \
                 ready: mark it ready and push to its branch to run them",
                list(&self.draft_no_start)
            ));
        }
        if !self.label_gated.is_empty() {
            let checks: Vec<String> = self.label_gated.iter().map(|(c, _)| c.clone()).collect();
            let labels = self.labels();
            let word = if labels.len() == 1 { "label" } else { "labels" };
            let names: Vec<String> = labels
                .iter()
                .map(|l| match l.strip_suffix('*') {
                    Some(prefix) => format!("`{prefix}…`"),
                    None => format!("`{l}`"),
                })
                .collect();
            parts.push(format!(
                "Add the {} {word} to run {}",
                names.join(" or "),
                list(&checks)
            ));
        }
        if !self.unknown.is_empty() {
            parts.push(format!(
                "Start {} on this head (CoderHelm can't tell from the workflow what runs it)",
                list(&self.unknown)
            ));
        }
        if parts.is_empty() {
            return None;
        }
        Some(format!(
            "{}, or mark it ready yourself if they aren't needed.",
            parts.join("; ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAGING: &str = r#"
name: Deploy to Staging
on:
  pull_request:
    types: [opened, synchronize, reopened, ready_for_review, labeled]
jobs:
  deploy:
    name: Deploy to Staging
    if: github.event.pull_request.draft == false && contains(github.event.pull_request.labels.*.name, 'CI:DEPLOY_STAGING')
    runs-on: ubuntu-latest
    steps: []
  e2e:
    name: E2E after staging deploy
    needs: deploy
    runs-on: ubuntu-latest
    steps: []
"#;

    #[test]
    fn draft_and_label_conditions_are_read() {
        let g = read(STAGING);
        assert!(g.draft);
        assert!(g.on_ready);
        assert!(!read("on: [pull_request]\njobs:\n  a:\n    if: github.event.pull_request.draft == false\n").on_ready);
        assert_eq!(g.labels, vec!["CI:DEPLOY_STAGING"]);
    }

    #[test]
    fn every_way_of_writing_not_a_draft_is_read() {
        for cond in [
            "if: ${{ !github.event.pull_request.draft }}",
            "if: ! github.event.pull_request.draft",
            "if: github.event.pull_request.draft != true",
            "if: ${{ github.event.pull_request.draft == false }}",
            "if: github.event_name != 'pull_request' || github.event.pull_request.draft == false",
        ] {
            assert!(read(&format!("jobs:\n  a:\n    {cond}\n")).draft, "{cond}");
        }
        // Runs only on drafts, or doesn't look at draft at all.
        for cond in [
            "if: github.event.pull_request.draft == true",
            "if: github.ref == 'refs/heads/main'",
        ] {
            assert!(!read(&format!("jobs:\n  a:\n    {cond}\n")).draft, "{cond}");
        }
    }

    #[test]
    fn a_trigger_only_on_ready_for_review_is_draft_gated() {
        let only = "on:\n  pull_request:\n    types: [ready_for_review]\njobs: {}\n";
        assert!(read(only).draft);
        let also = "on:\n  pull_request:\n    types: [opened, synchronize, ready_for_review]\n";
        assert!(!read(also).draft);
    }

    #[test]
    fn label_families_and_label_events_are_read() {
        let fam = "if: contains(join(github.event.pull_request.labels.*.name, ','), 'E2E:')";
        assert_eq!(read(fam).labels, vec!["E2E:*"]);
        let ev = "if: github.event.label.name == 'CI:E2E' || github.ref == 'x'";
        assert_eq!(read(ev).labels, vec!["CI:E2E"]);
        // Labels that stop a job are not labels that run it.
        let skip = "if: ${{ !contains(github.event.pull_request.labels.*.name, 'skip-e2e') }}";
        assert!(read(skip).labels.is_empty());
        assert!(read("if: github.event.label.name != 'wip'")
            .labels
            .is_empty());
        assert!(has_label(&["E2E:nav".into()], &["E2E:*".into()]));
    }

    #[test]
    fn skipped_checks_are_sorted_by_why() {
        let staging = read(STAGING);
        let label_only = WorkflowGate {
            labels: vec!["CI:DEPLOY_STAGING".into()],
            ..Default::default()
        };
        let rows = vec![
            ("Deploy to Staging".to_string(), Some(staging.clone())),
            (
                "E2E after staging deploy".to_string(),
                Some(staging.clone()),
            ),
        ];
        // The label is on the pull request: only the draft keeps them off.
        let s = sort(&rows, &["CI:DEPLOY_STAGING".into()]);
        assert!(s.all_draft_gated());
        assert_eq!(s.action(), None);
        // A workflow that needs only "not a draft" runs once it is ready.
        let draft_only = WorkflowGate {
            draft: true,
            on_ready: true,
            ..Default::default()
        };
        let s = sort(&[("Deploy Web App".into(), Some(draft_only))], &[]);
        assert_eq!(s.draft_gated, vec!["Deploy Web App"]);
        // Skips drafts but never starts on "ready for review": marking it
        // ready alone runs nothing, so a person is asked.
        let no_start = WorkflowGate {
            draft: true,
            ..Default::default()
        };
        let s = sort(&[("Deploy Web App".into(), Some(no_start))], &[]);
        assert!(s.draft_gated.is_empty());
        assert!(s.action().unwrap().contains("push to its branch"));
        // The label is missing: marking it ready alone wouldn't run them.
        let s = sort(&rows, &["E2E:nav".into()]);
        assert!(!s.all_draft_gated());
        let a = s.action().unwrap();
        assert!(
            a.starts_with("Add the `CI:DEPLOY_STAGING` label to run `Deploy to Staging`"),
            "{a}"
        );
        assert!(a.ends_with("or mark it ready yourself if they aren't needed."));
        let s = sort(
            &[
                ("smoke".to_string(), None),
                ("preview".to_string(), Some(label_only)),
            ],
            &[],
        );
        assert_eq!(s.unknown, vec!["smoke"]);
        assert_eq!(s.labels(), vec!["CI:DEPLOY_STAGING"]);
        assert!(s.action().unwrap().contains("Start `smoke` on this head"));
    }
}
