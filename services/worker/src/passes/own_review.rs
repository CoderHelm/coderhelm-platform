//! CoderHelm fixes what its own reviewer finds on its own pull requests.
//!
//! A ticket run's PR once got CoderHelm's own review with three findings,
//! all posted as notes, and was marked ready for review three seconds later
//! with the merge armed: a person was handed a PR with three known open
//! issues.
//!
//! On CoderHelm's own PRs every finding that isn't pure style (a
//! preference the reviewer inferred: wording, naming, layout, see
//! [`super::review_severity::pure_style`]) is work the
//! run does itself. A finding that asks for a test is work whatever made it
//! a note (tracked, a question, the stop judge), and one about a script
//! carries how to test it ([`test_how`]):
//!
//! - The review lists them at the top of the description under "Open review
//!   findings" ([`section`]). While the section is there the PR stays a
//!   draft and the merge isn't armed ([`hold`]).
//! - The run takes them in a review-fix round (`run_fix_round`), pushes,
//!   and the next review of the new head resolves the threads the commit
//!   settled and rewrites the section, or removes it when nothing is left.
//! - It goes on finding by finding (`own_review_ledger`): a round a tool
//!   or a guard blocked is not a stall and the next one removes the block;
//!   a stalled finding climbs its own ladder (one finding only, a fresh
//!   context with small edits, a re-split, the strongest model). A finding
//!   the ticket, its comments or the plan rule out is settled by their
//!   words; only a person-gate reason defers one, listed under "Deferred"
//!   with that reason ([`defer_findings`]). Never a count of rounds: two
//!   rounds at a cap once deferred a one-line config omission the tools had
//!   blocked. The run's spend ceiling still bounds every loop.
//! - A finished run's PR held on its findings never waits on a round
//!   nothing queued. A round that pushed nothing is read by a review of the
//!   same head first; otherwise the ready decision queues a review-fix round
//!   while the ledger has an approach left, and waits for it. What is left
//!   is listed as CoderHelm's note and the PR goes ready, never held for a
//!   person.
//! - Each round is told what the rounds before it found and looked up
//!   ([`round_note`], [`fix_feedback`]), so it doesn't repeat the same
//!   database and live-data lookups from scratch.
//!
//! People's PRs are unchanged: the findings are posted for the author.

use super::review_agent::{Evidence, Finding};

pub const START: &str = "<!-- coderhelm:open-findings -->";
pub const END: &str = "<!-- /coderhelm:open-findings -->";

/// The findings CoderHelm must fix on its own PR, one line each: every
/// finding but pure style, questions the change doesn't
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
                || about_description(f)
                || super::review_severity::asks_for_a_test(f)
                || (f.note_only.is_none()
                    && f.evidence() != Evidence::Question
                    && !super::review_severity::pure_style(f))
        })
        .chain(held_late.iter())
        .map(|f| match test_how(f) {
            Some(how) if super::review_severity::asks_for_a_test(f) => {
                format!("{} ({how})", line(f))
            }
            _ => line(f),
        })
        .collect()
}

/// How the coder tests a script CI or a person runs (a shell script, a
/// workflow, a CI helper), for a finding that asks for a test of one. A
/// review once asked for a test of a CI script that reads cloud rules, and
/// the run handed it back: the test needed only a stub of the cloud
/// command on PATH and a fixture of its answer.
pub fn test_how(f: &Finding) -> Option<&'static str> {
    let path = f.file.trim().to_ascii_lowercase();
    let script = path.ends_with(".sh")
        || path.ends_with(".bash")
        || path.starts_with(".github/")
        || path.starts_with("scripts/")
        || path.contains("/scripts/")
        || path.contains("/bin/")
        || path.ends_with("makefile")
        || path.ends_with("justfile");
    script.then_some(SCRIPT_TEST)
}

/// [`test_how`]'s instruction.
pub const SCRIPT_TEST: &str = "test the script itself: put stub commands first on PATH (a \
     fake `aws`, `gh` or `curl` that prints a fixture and records its arguments), keep the \
     fixtures beside the test, run the script against them in the repository's test runner, \
     and assert on its output, exit code and the stub's recorded calls";

/// A finding that says the pull request's description does not match the
/// code: it names the description and says it is behind. Never optional
/// and never a person's to fix, whatever its severity: the description is
/// CoderHelm's to rebuild from the head (`description::rebuild`), which is
/// what settles it.
pub fn about_description(f: &Finding) -> bool {
    says_description_is_behind(&format!("{} {}", f.title, f.body))
}

/// [`about_description`] for a finding's words alone (a review thread's
/// first comment).
pub fn says_description_is_behind(text: &str) -> bool {
    let text = text.to_lowercase();
    let names = [
        "pr description",
        "pull request description",
        "pull request's description",
        "pr body",
        "the description",
    ]
    .iter()
    .any(|n| text.contains(n));
    let behind = [
        "still says",
        "still lists",
        "still shows",
        "still counts",
        "still claims",
        "still describes",
        "stale",
        "out of date",
        "outdated",
        "does not match",
        "doesn't match",
        "no longer",
        "mismatch",
        "inaccurate",
        "contradicts",
        "not updated",
        "wasn't updated",
        "was not updated",
        "disagrees with",
    ]
    .iter()
    .any(|b| text.contains(b));
    names && behind
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
         these. The pull request stays a draft while any of them is listed here:\n{}\n{END}",
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
/// review still lists open findings. Says only what is open, never who
/// fixes it: that depends on whether a round is queued ([`fixing_why`],
/// [`person_todo`]).
pub fn hold(body: &str) -> Option<String> {
    let open = listed(body);
    (!open.is_empty()).then(|| format!("{} still open", count(open.len())))
}

/// "1 finding from CoderHelm's review is" / "N findings ... are".
fn count(n: usize) -> String {
    if n == 1 {
        "1 finding from CoderHelm's review is".to_string()
    } else {
        format!("{n} findings from CoderHelm's review are")
    }
}

/// What a person does about findings no queued round will fix.
pub fn person_todo() -> &'static str {
    "Fix them on the branch, or remove a finding from the \"Open review findings\" list once \
     you've judged it."
}

/// The wait while a queued round fixes the `n` findings listed.
pub fn fixing_why(n: usize) -> String {
    if n == 1 {
        "CoderHelm is fixing 1 finding from its review".to_string()
    } else {
        format!("CoderHelm is fixing {n} findings from its review")
    }
}

/// What a finished run has left for a fix round on its own review's
/// findings, read from its record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoundBudget {
    pub status: String,
    /// The run guard stopped the run.
    pub guard_open: bool,
    /// The run's own rounds on its findings ended and deferred what was
    /// left (`own_review_handed`).
    pub handed: bool,
    /// Fix rounds taken on the own review's findings.
    pub rounds: u32,
    /// Automatic fix pushes taken (`MAX_AUTO_FIX_PUSHES`).
    pub pushes: u32,
    /// Review and CI fix cycles taken (`MAX_CI_FIX_CYCLES`).
    pub cycles: usize,
    /// The head a round was last queued for (`own_fix_head`).
    pub queued_head: String,
    /// Findings open at the run's last review (`own_review_open`).
    pub open_before: Option<usize>,
    /// Findings open now, when the caller knows them: fewer than
    /// `open_before` is progress, which earns rounds past the caps.
    pub open_now: Option<usize>,
}

/// Whether the run's rounds on its findings are over, by the supervisor's
/// rule: `Some(why)` when they are (the run ended them, or the caps are
/// reached without progress, or the ceilings are), and what is open is then
/// deferred with that reason.
pub fn rounds_over(b: &RoundBudget, max_pushes: u32, max_cycles: usize) -> Option<String> {
    use super::supervisor::{ceiling, may_extend};
    if b.handed {
        return Some("the run's fix rounds on its own review's findings ended".into());
    }
    let progressed = matches!((b.open_before, b.open_now), (Some(was), Some(now)) if now < was);
    if !may_extend(b.rounds, MAX_FIX_ROUNDS, progressed) {
        return Some(format!(
            "the run took its fix rounds on its own review's findings ({} while they made \
             progress, up to {})",
            b.rounds,
            ceiling(MAX_FIX_ROUNDS)
        ));
    }
    let cycles = u32::try_from(b.cycles).unwrap_or(u32::MAX);
    let max_cycles = u32::try_from(max_cycles).unwrap_or(u32::MAX);
    if !may_extend(b.pushes, max_pushes, progressed) || !may_extend(cycles, max_cycles, progressed)
    {
        return Some("the run's automatic fix budget is spent".into());
    }
    None
}

/// Whether a fix round may be queued now for the findings on `head`:
/// `Err` names why not, for the log. The round counts against the same
/// budgets the run's own rounds draw on ([`rounds_over`]).
pub fn round_left(
    b: &RoundBudget,
    head: &str,
    max_pushes: u32,
    max_cycles: usize,
) -> Result<(), String> {
    use common::run_status::{COMPLETED, NEEDS_INPUT, SUCCESS};
    if head.is_empty() {
        return Err("the pull request's head wasn't read".into());
    }
    if ![COMPLETED, SUCCESS, NEEDS_INPUT].contains(&b.status.as_str()) {
        return Err(format!("the run is {}, not finished", b.status));
    }
    if b.guard_open {
        return Err("the run guard stopped the run".into());
    }
    if let Some(why) = rounds_over(b, max_pushes, max_cycles) {
        return Err(why);
    }
    if b.queued_head == head {
        return Err("a fix round already ran on this head".into());
    }
    Ok(())
}

/// The cap on fix rounds a run takes on its own review's findings. Past it,
/// rounds go on only while they make progress, up to twice it
/// (`supervisor::next_round`); then what is open is deferred.
pub const MAX_FIX_ROUNDS: u32 = 2;

/// What the run says when its rounds on its own findings end: what is
/// deferred and why, and that the run finishes.
pub fn finish_reason(items: &[String], why: &str) -> String {
    let list: Vec<String> = items.iter().map(|i| format!("- [ ] {i}")).collect();
    format!(
        "CoderHelm's fix rounds on its own review's findings ended ({why}). These are deferred, \
         listed under \"Deferred\" on the pull request with that reason, and the run finishes:\n{}",
        list.join("\n")
    )
}

/// The description with the open findings deferred: the "Open review
/// findings" section (which holds the draft) removed, and each finding
/// listed under "Deferred" with `why`. Unchanged when nothing is listed.
pub fn defer_findings(body: &str, items: &[String], why: &str) -> String {
    if items.is_empty() {
        return body.to_string();
    }
    let mut rest = body.to_string();
    if let (Some(a), Some(b)) = (rest.find(START), rest.find(END)) {
        if b > a {
            rest.replace_range(a..b + END.len(), "");
        }
    }
    let rest = rest.trim().replace("\n\n\n", "\n\n");
    let lines: Vec<String> = items
        .iter()
        .map(|i| format!("- {i}: deferred from CoderHelm's own review ({why})"))
        .collect();
    if rest.contains("## Deferred") {
        format!("{}\n{}", rest.trim_end(), lines.join("\n"))
    } else {
        format!("{}\n\n## Deferred\n{}", rest.trim_end(), lines.join("\n"))
    }
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
    fn everything_but_pure_style_is_fixed() {
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
                // An optional improvement to working code is not style: it
                // is acted on before the run finishes.
                "Could cache this (`src/a.rs:3`)".to_string(),
                "Network might be slow (`src/a.rs:9`)".to_string(),
                "Late bug (`b.rs`)".to_string(),
            ],
            "a rename is pure style and stays a note"
        );
    }

    /// The description being behind the code is never a note left for a
    /// person, whatever severity or category the reviewer gave it.
    #[test]
    fn a_finding_that_the_description_is_behind_is_never_optional() {
        let stale = finding(json!({
            "title": "PR description still says the package was not moved",
            "body": "The lockfile resolves path-match to 2.0.3, but the description's table still lists it as not moved on the head.",
            "category": "wording", "severity": "low", "optional": true
        }));
        assert!(about_description(&stale));
        assert_eq!(
            required(std::slice::from_ref(&stale), &[]),
            vec!["PR description still says the package was not moved (`src/a.rs:3`)".to_string()]
        );
        for (title, body) in [
            ("Rename x", "The name does not match the convention."),
            (
                "Typo in the description",
                "The description has a typo in the second paragraph.",
            ),
            (
                "Stale comment",
                "The comment above the function is out of date.",
            ),
            (
                "Meta description missing",
                "The page's meta description tag is no longer set.",
            ),
        ] {
            let f = finding(json!({"title": title, "body": body, "category": "wording"}));
            assert!(!about_description(&f), "{title}");
        }
        let typo = finding(
            json!({"title": "Typo in the description", "body": "The description has a typo.", "category": "naming"}),
        );
        assert!(!about_description(&typo));
        assert!(required(&[typo], &[]).is_empty());
    }

    #[test]
    fn the_section_holds_the_pr_until_nothing_is_open() {
        let items = vec!["A (`a.rs:1`)".to_string(), "B (`b.rs:2`)".to_string()];
        let s = section(&items);
        let body = format!("{s}\n\n## Summary\nx");
        assert_eq!(listed(&body), items);
        assert_eq!(
            hold(&body).unwrap(),
            "2 findings from CoderHelm's review are still open"
        );
        assert!(!s.contains("It fixes them"));
        assert!(section(&[]).is_empty());
        assert!(hold("## Summary\nx").is_none());
    }

    #[test]
    fn deferred_findings_leave_the_hold_and_are_listed_with_why() {
        let items = vec!["A (`a.rs:1`)".to_string(), "B (`README.md`)".to_string()];
        let body = format!("Closes #4\n\n{}\n\n## Summary\nx", section(&items));
        assert!(hold(&body).is_some());
        let now = defer_findings(&body, &items, "the rounds stopped closing findings");
        assert!(hold(&now).is_none(), "{now}");
        assert!(listed(&now).is_empty());
        assert!(now.contains("## Summary\nx"));
        assert!(now.contains(
            "## Deferred\n- A (`a.rs:1`): deferred from CoderHelm's own review (the rounds \
             stopped closing findings)"
        ));
        // An existing "Deferred" section gets the lines added.
        let again = defer_findings("x\n\n## Deferred\n- t: why", &items[..1], "w");
        assert_eq!(again.matches("## Deferred").count(), 1);
        assert!(again.ends_with("- A (`a.rs:1`): deferred from CoderHelm's own review (w)"));
        assert_eq!(defer_findings("x", &[], "w"), "x");
    }

    #[test]
    fn a_finished_runs_rounds_are_over_by_the_same_rule() {
        let base = RoundBudget {
            status: "completed".into(),
            ..Default::default()
        };
        assert_eq!(rounds_over(&base, 8, 10), None);
        let at_cap = RoundBudget {
            rounds: MAX_FIX_ROUNDS,
            open_before: Some(3),
            ..base.clone()
        };
        // At the cap without progress known: over.
        assert!(rounds_over(&at_cap, 8, 10).is_some());
        // At the cap with fewer open than at the last review: one more.
        let closing = RoundBudget {
            open_now: Some(2),
            ..at_cap.clone()
        };
        assert_eq!(rounds_over(&closing, 8, 10), None);
        assert_eq!(round_left(&closing, "h", 8, 10), Ok(()));
        // The ceiling holds whatever the progress.
        let ceiling = RoundBudget {
            rounds: MAX_FIX_ROUNDS * 2,
            ..closing.clone()
        };
        assert!(rounds_over(&ceiling, 8, 10).is_some());
        // Pushes past their cap only while progressing, up to twice it.
        let pushes = RoundBudget {
            pushes: 9,
            ..closing.clone()
        };
        assert_eq!(rounds_over(&pushes, 8, 10), None);
        assert!(rounds_over(
            &RoundBudget {
                pushes: 16,
                ..closing
            },
            8,
            10
        )
        .is_some());
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
