//! CoderHelm fixes what its own reviewer finds on its own pull requests.
//!
//! A ticket run's PR once got CoderHelm's own review with three findings,
//! all posted as notes, and was marked ready for review three seconds later
//! with the merge armed: a person was handed a PR with three known open
//! issues.
//!
//! On CoderHelm's own PRs every finding that isn't strictly optional (a
//! preference the reviewer inferred, or an optional improvement where the
//! code is correct, see [`super::review_severity::optional`]) is work the
//! run does itself:
//!
//! - The review lists them at the top of the description under "Open review
//!   findings" ([`section`]). While the section is there the PR stays a
//!   draft and the merge isn't armed ([`hold`]).
//! - The run takes them in a review-fix round (`run_fix_round`), pushes,
//!   and the next review of the new head resolves the threads the commit
//!   settled and rewrites the section, or removes it when nothing is left.
//! - It goes on while the rounds make progress (fewer open findings, or a
//!   round that pushed changes). Two rounds in a row without progress stop
//!   it ([`next`]), with the progress rule ticket runs use between steps:
//!   the findings stay listed, the PR stays a draft, and the run waits on a
//!   person (`needs_input`).
//!
//! People's PRs are unchanged: the findings are posted for the author.

use super::review_agent::{Evidence, Finding};
use super::steps;

pub const START: &str = "<!-- coderhelm:open-findings -->";
pub const END: &str = "<!-- /coderhelm:open-findings -->";

/// The findings CoderHelm must fix on its own PR, one line each: every
/// finding but the strictly optional ones, questions the change doesn't
/// rest on, and notes a policy made (a follow-up the PR tracks, a problem
/// it didn't cause). `held_late` are blockers held as notes because they
/// came late: they are still defects to fix.
pub fn required(findings: &[Finding], held_late: &[Finding]) -> Vec<String> {
    let line = |f: &Finding| {
        let at = if f.line > 0 {
            format!("`{}:{}`", f.file, f.line)
        } else {
            format!("`{}`", f.file)
        };
        let title = f.title.split_whitespace().collect::<Vec<_>>().join(" ");
        format!("{title} ({at})")
    };
    findings
        .iter()
        .filter(|f| {
            f.holds_merge()
                || (f.note_only.is_none()
                    && f.evidence() != Evidence::Question
                    && !super::review_severity::optional(f))
        })
        .chain(held_late.iter())
        .map(line)
        .collect()
}

/// The description section listing them. Empty when nothing is open (the
/// section is then removed).
pub fn section(items: &[String]) -> String {
    if items.is_empty() {
        return String::new();
    }
    let list: Vec<String> = items.iter().map(|i| format!("- {i}")).collect();
    format!(
        "{START}\n## Open review findings\nCoderHelm's own review of this pull request found \
         these. It fixes them before the pull request is marked ready:\n{}\n{END}",
        list.join("\n")
    )
}

/// The findings a description still lists as open.
pub fn listed(body: &str) -> Vec<String> {
    let Some(s) = body.find(START) else {
        return Vec::new();
    };
    let rest = &body[s + START.len()..];
    let inner = rest.find(END).map_or(rest, |e| &rest[..e]);
    inner
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- "))
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// Why the PR may not be marked ready or merged yet, if it may not: its own
/// review still lists open findings.
pub fn hold(body: &str) -> Option<String> {
    let open = listed(body);
    (!open.is_empty()).then(|| {
        format!(
            "CoderHelm's own review has {} open finding(s) not fixed yet",
            open.len()
        )
    })
}

/// What follows a review of CoderHelm's own PR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Nothing is open: the PR may be marked ready and the merge armed.
    Ready,
    /// Another fix round, with this many rounds without progress before it.
    Fix { stalled: u32 },
    /// Two rounds in a row made no progress: the run stops, the findings
    /// stay listed and the PR stays a draft.
    Stop(String),
}

/// What follows. `before`: the findings open at the previous review of
/// this run (`None` at the first); `after`: those open now;
/// `pushed_files`: what the last fix round changed; `stalled`: rounds
/// without progress so far.
pub fn next(before: Option<usize>, after: &[String], pushed_files: usize, stalled: u32) -> Next {
    if after.is_empty() {
        return Next::Ready;
    }
    let progress = before.is_none_or(|b| after.len() < b) || pushed_files > 0;
    if progress {
        return Next::Fix { stalled: 0 };
    }
    let stalled = stalled.saturating_add(1);
    if steps::stops_for_stall(stalled) {
        return Next::Stop(stop_reason(after));
    }
    Next::Fix { stalled }
}

/// Why the fix rounds stopped, with the findings still open.
pub fn stop_reason(items: &[String]) -> String {
    let list: Vec<String> = items.iter().map(|i| format!("- {i}")).collect();
    format!(
        "Stopped: {} fix rounds in a row didn't fix any of CoderHelm's own review findings. \
         Still open:\n{}\nThe pull request stays a draft with these listed under \"Open review \
         findings\".",
        steps::MAX_STALLED_STEPS,
        list.join("\n")
    )
}

/// What a fix round is told: every open finding, fixed now.
pub fn fix_feedback(items: &[String], review_body: &str) -> String {
    let todo: Vec<String> = items.iter().map(|i| format!("- [ ] {i}")).collect();
    format!(
        "CoderHelm's own review of this pull request found the issues below. Fix every one \
         now on this branch; none of them is optional. Where a finding can't be fixed in the \
         code, say why in the pull request description.\n\n{}\n\nThe review:\n\n{review_body}",
        todo.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn finding(v: serde_json::Value) -> Finding {
        let mut base = json!({"file": "src/a.rs", "line": 3, "title": "t", "body": "b"});
        for (k, val) in v.as_object().unwrap() {
            base[k] = val.clone();
        }
        serde_json::from_value(base).unwrap()
    }

    #[test]
    fn everything_but_the_strictly_optional_is_fixed() {
        let mut tracked = finding(json!({"title": "Tracked", "category": "bug"}));
        super::super::review_policy::demote(&mut tracked, "tracked");
        let fs = vec![
            finding(
                json!({"title": "Watermark skips rows", "category": "correctness", "severity": "medium"}),
            ),
            finding(json!({"title": "Rename x", "category": "naming"})),
            finding(json!({"title": "Could cache this", "category": "perf", "optional": true})),
            finding(
                json!({"title": "Network might be slow", "category": "correctness", "basis": "external", "line": 9}),
            ),
            finding(json!({"title": "Which tab?", "category": "question"})),
            tracked,
        ];
        let late = vec![finding(
            json!({"title": "Late bug", "file": "b.rs", "line": 0}),
        )];
        assert_eq!(
            required(&fs, &late),
            vec![
                "Watermark skips rows (`src/a.rs:3`)".to_string(),
                "Network might be slow (`src/a.rs:9`)".to_string(),
                "Late bug (`b.rs`)".to_string(),
            ]
        );
    }

    #[test]
    fn the_section_holds_the_pr_until_nothing_is_open() {
        let items = vec!["A (`a.rs:1`)".to_string(), "B (`b.rs:2`)".to_string()];
        let s = section(&items);
        let body = format!("{s}\n\n## Summary\nx");
        assert_eq!(listed(&body), items);
        assert!(hold(&body).unwrap().contains("2 open finding(s)"));
        assert!(section(&[]).is_empty());
        assert!(hold("## Summary\nx").is_none());
    }

    #[test]
    fn rounds_go_on_while_they_make_progress_and_stop_after_two_without() {
        let two = vec!["a".to_string(), "b".to_string()];
        assert_eq!(next(Some(2), &[], 0, 1), Next::Ready);
        assert_eq!(next(None, &two, 0, 0), Next::Fix { stalled: 0 });
        assert_eq!(next(Some(3), &two, 0, 1), Next::Fix { stalled: 0 });
        assert_eq!(next(Some(2), &two, 4, 1), Next::Fix { stalled: 0 });
        assert_eq!(next(Some(2), &two, 0, 0), Next::Fix { stalled: 1 });
        match next(Some(2), &two, 0, 1) {
            Next::Stop(why) => {
                assert!(why.contains("- a\n- b"));
                assert!(why.contains("stays a draft"));
            }
            other => panic!("{other:?}"),
        }
    }
}
