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
//!   round that pushed changes), for at most [`MAX_FIX_ROUNDS`] rounds. Two
//!   rounds in a row without progress, or the cap, stop it ([`next`]): the
//!   findings stay listed, the PR stays a draft, and the run waits on a
//!   person (`needs_input`). Uncapped, two runs spent $29 in 17 rounds
//!   where the count of open findings went down by one and back up.
//! - Each round is told what the rounds before it found and looked up
//!   ([`round_note`], [`fix_feedback`]), so it doesn't repeat the same
//!   database and live-data lookups from scratch.
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

/// One line for an earlier review thread still open: its title (the first
/// line of the comment, without markdown emphasis) and where it is, as
/// [`required`] writes a finding.
pub fn thread_line(path: &str, line: u64, body: &str) -> String {
    let title = body
        .lines()
        .map(|l| {
            l.trim()
                .trim_matches(|c| c == '*' || c == '#' || c == '_')
                .trim()
        })
        .find(|l| !l.is_empty())
        .unwrap_or("Earlier finding");
    // Whole: a title is one line, and posted text is never cut mid-sentence.
    let title = title.split_whitespace().collect::<Vec<_>>().join(" ");
    if line > 0 {
        format!("{title} (`{path}:{line}`)")
    } else {
        format!("{title} (`{path}`)")
    }
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

/// Fix rounds a run takes on its own review's findings before it hands the
/// PR to a person with the findings still listed.
pub const MAX_FIX_ROUNDS: u32 = 2;

/// What follows a review of CoderHelm's own PR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Nothing is open: the PR may be marked ready and the merge armed.
    Ready,
    /// Another fix round, with this many rounds without progress before it.
    Fix { stalled: u32 },
    /// Two rounds in a row made no progress, or [`MAX_FIX_ROUNDS`] rounds
    /// ran: the run stops, the findings stay listed and the PR stays a
    /// draft.
    Stop(String),
}

/// What follows. `before`: the findings open at the previous review of
/// this run (`None` at the first); `after`: those open now;
/// `pushed_files`: what the last fix round changed; `stalled`: rounds
/// without progress so far; `rounds`: fix rounds this run already took.
pub fn next(
    before: Option<usize>,
    after: &[String],
    pushed_files: usize,
    stalled: u32,
    rounds: u32,
) -> Next {
    if after.is_empty() {
        return Next::Ready;
    }
    if rounds >= MAX_FIX_ROUNDS {
        return Next::Stop(cap_reason(after));
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

/// Why the fix rounds stopped at the cap, with the findings still open.
pub fn cap_reason(items: &[String]) -> String {
    let list: Vec<String> = items.iter().map(|i| format!("- [ ] {i}")).collect();
    format!(
        "Handed to a person: CoderHelm took {MAX_FIX_ROUNDS} fix rounds on its own review's \
         findings and these are still open:\n{}\nThe pull request stays a draft with them listed \
         under \"Open review findings\". Fix them, or say on the pull request why one doesn't \
         apply, then mark it ready.",
        list.join("\n")
    )
}

/// What a fix round is told: every open finding, fixed now, and what the
/// earlier rounds of this run found and looked up (`earlier`, from
/// [`round_note`]; empty on the first round).
pub fn fix_feedback(items: &[String], review_body: &str, earlier: &str) -> String {
    let todo: Vec<String> = items.iter().map(|i| format!("- [ ] {i}")).collect();
    let earlier = earlier.trim();
    let carried = if earlier.is_empty() {
        String::new()
    } else {
        format!(
            "\n\n## Earlier fix rounds on this pull request\nWhat the rounds before this one \
             were asked to fix and what their lookups already returned. Build on it: don't run \
             a lookup again unless its answer may have changed.\n\n{earlier}"
        )
    };
    format!(
        "CoderHelm's own review of this pull request found the issues below. Fix every one \
         now on this branch; none of them is optional. Where a finding can't be fixed in the \
         code, say why in the pull request description.\n\n{}\n\nThe review:\n\n{review_body}{carried}",
        todo.join("\n")
    )
}

/// The most a run carries from its earlier rounds, in bytes.
pub const MAX_CARRIED_BYTES: usize = 12_000;

/// One fix round's record for the rounds after it: the findings it was
/// given, each lookup it made with what came back, and the coder's own
/// closing account. Appended to `earlier` and kept to the newest
/// [`MAX_CARRIED_BYTES`].
pub fn round_note(
    earlier: &str,
    round: u32,
    items: &[String],
    lookups: &[super::proof::Lookup],
    notes: &str,
) -> String {
    let mut out = format!("### Round {round}\nAsked to fix:\n");
    for i in items {
        out.push_str(&format!("- {i}\n"));
    }
    if !lookups.is_empty() {
        out.push_str("Lookups and what came back:\n");
        for l in lookups {
            out.push_str(&format!("{}\n", l.line.trim()));
        }
    }
    let notes = notes.trim();
    if !notes.is_empty() {
        out.push_str(&format!(
            "The round's own account:\n{}\n",
            common::truncate_str(notes, 2_000)
        ));
    }
    let all = if earlier.trim().is_empty() {
        out
    } else {
        format!("{}\n\n{out}", earlier.trim())
    };
    if all.len() <= MAX_CARRIED_BYTES {
        return all;
    }
    // Keep the newest: drop from the front at a line boundary.
    let mut cut = all.len() - MAX_CARRIED_BYTES;
    while !all.is_char_boundary(cut) {
        cut += 1;
    }
    let tail = &all[cut..];
    match tail.find('\n') {
        Some(i) => tail[i + 1..].to_string(),
        None => tail.to_string(),
    }
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
        assert_eq!(next(Some(2), &[], 0, 1, 1), Next::Ready);
        assert_eq!(next(None, &two, 0, 0, 0), Next::Fix { stalled: 0 });
        assert_eq!(next(Some(3), &two, 0, 1, 1), Next::Fix { stalled: 0 });
        assert_eq!(next(Some(2), &two, 4, 1, 1), Next::Fix { stalled: 0 });
        assert_eq!(next(Some(2), &two, 0, 0, 1), Next::Fix { stalled: 1 });
        match next(Some(2), &two, 0, 1, 1) {
            Next::Stop(why) => {
                assert!(why.contains("- a\n- b"));
                assert!(why.contains("stays a draft"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn two_fix_rounds_at_most_then_a_person() {
        let open = vec!["a (`x.rs:1`)".to_string(), "b (`y.rs:2`)".to_string()];
        // Progress every round still stops at the cap: one fewer open each
        // time, or a round that pushed changes.
        assert_eq!(next(Some(3), &open, 2, 0, 1), Next::Fix { stalled: 0 });
        match next(Some(3), &open, 2, 0, MAX_FIX_ROUNDS) {
            Next::Stop(why) => {
                assert!(why.contains("2 fix rounds"));
                assert!(why.contains("- [ ] a (`x.rs:1`)\n- [ ] b (`y.rs:2`)"));
                assert!(why.contains("stays a draft"));
            }
            other => panic!("{other:?}"),
        }
        // Nothing open is ready whatever the count.
        assert_eq!(next(Some(1), &[], 0, 0, 9), Next::Ready);
    }

    #[test]
    fn each_round_carries_what_the_earlier_rounds_found_and_looked_up() {
        use crate::passes::proof::{Lookup, LookupOutcome};
        let lookups = vec![Lookup::new(
            "L1".into(),
            "warehouse",
            LookupOutcome::Answered,
            "select count(*) from orders where status = 'open'",
            "412",
        )];
        let first = round_note(
            "",
            1,
            &["Watermark skips rows (`a.rs:3`)".into()],
            &lookups,
            "Fixed the cursor.",
        );
        assert!(first.starts_with("### Round 1\nAsked to fix:\n- Watermark skips rows"));
        assert!(first.contains("[L1]") && first.contains("412"));
        assert!(first.contains("Fixed the cursor."));
        let second = round_note(&first, 2, &["Late bug (`b.rs`)".into()], &[], "");
        assert!(second.starts_with("### Round 1"));
        assert!(second.contains("### Round 2\nAsked to fix:\n- Late bug"));
        // The fix round's prompt carries them, after the review.
        let fb = fix_feedback(&["Late bug (`b.rs`)".into()], "review body", &second);
        let review_at = fb.find("review body").unwrap();
        let carried_at = fb.find("## Earlier fix rounds").unwrap();
        assert!(review_at < carried_at);
        assert!(fb.contains("don't run a lookup again"));
        // Nothing carried on the first round.
        assert!(!fix_feedback(&["x".into()], "r", "").contains("Earlier fix rounds"));
        // Bounded: the newest rounds are kept.
        let mut big = String::new();
        for r in 1..200 {
            big = round_note(
                &big,
                r,
                &["same finding (`a.rs:1`)".into()],
                &lookups,
                "notes",
            );
        }
        assert!(big.len() <= MAX_CARRIED_BYTES);
        assert!(big.contains("### Round 199"));
        assert!(!big.contains("### Round 1\n"));
    }

    #[test]
    fn an_open_earlier_thread_is_listed_like_a_finding() {
        assert_eq!(
            thread_line("src/a.rs", 3, "**Watermark skips rows**\n\nThe cursor..."),
            "Watermark skips rows (`src/a.rs:3`)"
        );
        assert_eq!(thread_line("b.rs", 0, ""), "Earlier finding (`b.rs`)");
    }
}
