//! A red check CoderHelm's own CI fix round showed isn't the change's is
//! classified, never the run's failure.
//!
//! A small web fix's preview check failed on an end-to-end test of another
//! page. The CI fix round read it and showed, with evidence, that the change
//! didn't cause it: the staging data the test reads changed three minutes
//! before the test ran, staging (the base branch's build) showed the same
//! mismatch, and the change touched nothing on that page. It changed no
//! code, which was right. The run still ended "failed", the pull request
//! stayed a draft, and the run said CoderHelm "made 1 fix attempt(s)
//! without getting it green" (the record said two). The check doesn't run
//! on the base branch's head, so the comparison with the base never ran and
//! the round's proof counted for nothing.
//!
//! The rules, each enforced in code:
//!
//! - **The round's proof is read.** [`classify`]: a round that changed no
//!   code and says the failure isn't the change's is believed when a
//!   comparison on this head shows it fails on the base too, or when the
//!   round confirmed it with evidence lines (`Confirmed: ...`). A failing
//!   test this change edits is always the change's.
//! - **Re-run once, then classify.** The failed jobs run again once
//!   ([`Rerun`]): green, and the run goes on as any green run; red again,
//!   and the failure is the run's own outcome ([`CI_END`]), named with its
//!   proof on the pull request and the run ([`section`], [`run_note`]).
//!   The pull request goes ready; the run is completed, never failed.
//! - **One standing for "not this change's".** [`not_ours`] is the one
//!   reading of `ci_end` the run's ending, the ready decision and analytics
//!   share.
//! - **The test gets its own fix.** The failing test is handed to the
//!   flaky-test fix (one per test, deduplicated by its record), and the
//!   pull request links it.

use serde::{Deserialize, Serialize};

use crate::ci_repro::{self, Record, Verdict};

/// The run record attribute that keeps the standing, as JSON.
pub const FIELD: &str = "ci_unrelated";

/// The run's `ci_end` once a failure is classified as outside the change.
pub const CI_END: &str = "unrelated_test";

/// Whether a run's `ci_end` says its red CI is not its change's: every
/// failing check also fails on the base branch, or the failing test was
/// shown to be outside the change.
pub fn not_ours(ci_end: &str) -> bool {
    matches!(ci_end, "red_on_base" | CI_END)
}

/// Where the one re-run stands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Rerun {
    /// Not asked yet.
    #[default]
    NotYet,
    /// The failed jobs are running again on the head.
    Started,
    /// GitHub wouldn't run them again (no Actions write, or not an Actions
    /// check): the classification goes on without it.
    CouldNot,
}

/// A failure shown to be outside the change, on one head.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Standing {
    pub head: String,
    /// The failing checks.
    pub checks: Vec<String>,
    /// The failing tests, as the report or the round named them.
    #[serde(default)]
    pub tests: Vec<String>,
    /// What shows it isn't the change's, one line each.
    pub proof: Vec<String>,
    #[serde(default)]
    pub rerun: Rerun,
    /// The separate fix for the test, once asked (a line for people).
    #[serde(default)]
    pub fix: String,
    #[serde(default)]
    pub at: String,
}

impl Standing {
    /// Whether this standing is for `head`.
    pub fn on(&self, head: &str) -> bool {
        ci_repro::same_sha(&self.head, head)
    }

    fn what(&self) -> String {
        let checks = if self.checks.is_empty() {
            "the repository's CI".to_string()
        } else {
            self.checks
                .iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self.tests.first() {
            Some(t) => format!("{checks} fails on `{}`", t.replace('`', "'")),
            None => format!("{checks} fails"),
        }
    }
}

pub fn to_json(s: &Standing) -> String {
    serde_json::to_string(s).unwrap_or_default()
}

pub fn from_json(s: &str) -> Option<Standing> {
    serde_json::from_str(s).ok()
}

/// Lines and sentences of a round's account.
fn sentences(text: &str) -> impl Iterator<Item = &str> {
    text.lines()
        .flat_map(|l| l.split(". "))
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// How rounds say a failure isn't the change's, beyond the blame words
/// `ci_repro::blames` holds to a comparison.
const NOT_OURS: &[&str] = &[
    "not because of this",
    "not because of the change",
    "isn't this pr",
    "isn't this pull request",
    "isn't this change",
    "not this pr",
    "not this pull request",
    "not this change",
    "fails the same way",
    "same mismatch",
    "without this pr",
];

/// Whether a round's account says the failure isn't the change's.
pub fn claims_not_ours(account: &str) -> bool {
    sentences(account).any(|s| {
        let low = s.to_ascii_lowercase().replace('\u{2019}', "'");
        ci_repro::blames(s) || NOT_OURS.iter().any(|p| low.contains(p))
    })
}

/// The tests an account names: backticked `Suite › title` spans.
pub fn tests_named(account: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (i, part) in account.split('`').enumerate() {
        let t = part.trim();
        if i % 2 == 1 && t.contains(" › ") && t.len() <= 300 && !out.iter().any(|o| o == t) {
            out.push(t.to_string());
        }
    }
    out.truncate(5);
    out
}

/// The account's evidence lines that say they were confirmed.
pub fn confirmed_lines(account: &str) -> Vec<String> {
    account
        .lines()
        .map(|l| l.trim().trim_start_matches(['-', '*']).trim())
        .filter_map(|l| l.strip_prefix("Confirmed:"))
        .map(|l| crate::truncate_str(l.trim(), 300).to_string())
        .filter(|l| !l.is_empty())
        .take(4)
        .collect()
}

/// The file part of a `file › title` test name, when it names a file.
fn test_file(test: &str) -> Option<&str> {
    let (f, _) = test.split_once(" › ")?;
    let f = f.trim();
    let code = [".ts", ".tsx", ".js", ".jsx", ".mjs", ".py", ".rb", ".go"];
    (f.contains('/') || code.iter().any(|e| f.ends_with(e))).then_some(f)
}

/// Whether the change edits one of the failing tests' files.
fn edits_a_test(tests: &[String], changed: &[String]) -> bool {
    tests.iter().filter_map(|t| test_file(t)).any(|f| {
        let f = f.trim_start_matches("./");
        changed
            .iter()
            .any(|c| c == f || c.ends_with(&format!("/{f}")))
    })
}

/// The standing for a round that changed no code (`account`, its own
/// words) on `head`, or `None` when nothing shows the failure is outside
/// the change. `records`: the run's comparisons; `tests`: the failing
/// tests the report named; `changed`: the files the pull request changes.
pub fn classify(
    account: &str,
    records: &[Record],
    head: &str,
    checks: &[String],
    tests: &[String],
    changed: &[String],
) -> Option<Standing> {
    if head.trim().is_empty() || !claims_not_ours(account) {
        return None;
    }
    let mut all_tests: Vec<String> = tests.to_vec();
    for t in tests_named(account) {
        if !all_tests.contains(&t) {
            all_tests.push(t);
        }
    }
    let here = ci_repro::on_head(records, head);
    // A comparison run on this head (in the sandbox, or the check's own
    // runs) that shows the change makes it fail outweighs any account. A
    // base result from the test's history is older than this head and
    // doesn't.
    if here
        .iter()
        .any(|r| r.source != "history" && matches!(r.verdict(), Verdict::ThisChange(_)))
    {
        return None;
    }
    let compared: Vec<String> = here
        .into_iter()
        .filter(|r| matches!(r.verdict(), Verdict::NotThisChange(_)))
        .map(Record::line)
        .collect();
    // A test this change writes or edits is the change's to fix, whatever
    // the round says, unless a comparison shows it fails on the base too.
    if compared.is_empty() && edits_a_test(&all_tests, changed) {
        return None;
    }
    let mut proof = compared;
    if proof.is_empty() {
        proof = confirmed_lines(account);
    }
    if proof.is_empty() {
        return None;
    }
    all_tests.truncate(3);
    Some(Standing {
        head: head.to_string(),
        checks: checks.to_vec(),
        tests: all_tests,
        proof,
        rerun: Rerun::NotYet,
        fix: String::new(),
        at: String::new(),
    })
}

/// Whether a test named by a report or a round (`named`, `Suite › title`
/// or `file › Suite › title`) is the test a Tests record keeps (`title`,
/// its suite and title, in `file`).
pub fn same_test(named: &str, title: &str, file: &str) -> bool {
    let (named, title) = (named.trim(), title.trim());
    if named.is_empty() || title.is_empty() {
        return false;
    }
    if named == title {
        return true;
    }
    let Some(rest) = named.strip_suffix(title) else {
        return false;
    };
    let Some(prefix) = rest.strip_suffix(" › ") else {
        return false;
    };
    // What comes before the title is its file (or a project).
    let p = prefix.trim().trim_start_matches("./");
    let f = file.trim().trim_start_matches("./");
    f.is_empty() || p.is_empty() || f.ends_with(p) || p.ends_with(f) || !p.contains('.')
}

/// The run page's line while the one re-run runs.
pub fn rerun_note(s: &Standing) -> String {
    format!(
        "{}, and CoderHelm's CI fix round showed it isn't this change's ({}). It changed no \
         code; the failed jobs run again once, since the failure may be a flake or a data race.",
        s.what(),
        crate::truncate_str(&s.proof.join("; "), 400)
    )
}

/// The pull request's section once the failure is classified (its inner
/// text; `common::ci_lines::failing_section` names the head around it).
pub fn section(s: &Standing) -> String {
    let mut lines = vec![
        "## CI failure outside this change".to_string(),
        format!(
            "{}. CoderHelm's CI fix round showed the failure isn't this change's, and made no \
             change for it here:",
            s.what()
        ),
    ];
    for p in &s.proof {
        lines.push(format!("- {}", p.replace('\n', " ")));
    }
    lines.push(format!("- {}", rerun_line(s)));
    if !s.fix.trim().is_empty() {
        lines.push(format!("- Fix for the test: {}", s.fix.trim()));
    }
    lines.push(
        "The pull request is ready for review. It isn't merged while this check fails: \
         merge it when you're satisfied the failure is the test's, or once its fix lands."
            .to_string(),
    );
    lines.join("\n")
}

fn rerun_line(s: &Standing) -> String {
    let at: String = s.head.chars().take(7).collect();
    match s.rerun {
        Rerun::Started => {
            format!("Re-run: the failed jobs ran again once on {at} and failed the same way.")
        }
        Rerun::CouldNot => {
            "Re-run: GitHub didn't let CoderHelm run the failed jobs again.".to_string()
        }
        Rerun::NotYet => "Re-run: not run.".to_string(),
    }
}

/// The run's outcome line once the failure is classified.
pub fn run_note(s: &Standing) -> String {
    let mut out = format!(
        "{}, outside this change: {}. {} The pull request is ready for review; CoderHelm made no \
         change for it.",
        s.what(),
        crate::truncate_str(&s.proof.join("; "), 500),
        rerun_line(s)
    );
    if !s.fix.trim().is_empty() {
        out.push_str(&format!(" Fix for the test: {}", s.fix.trim()));
    }
    out
}

/// What a run says about its CI fix rounds when it stops with checks red:
/// rounds run (`ci_fix_attempts`) and fixes pushed (`ci_fix_commits`), as
/// the record has them. A round that changed no code is never called an
/// attempt that didn't get it green.
pub fn rounds_said(rounds: u32, pushed: u32) -> String {
    let r = |n: u32| if n == 1 { "round" } else { "rounds" };
    match (rounds, pushed) {
        (0, 0) => String::new(),
        (n, 0) => format!(
            "CoderHelm's {n} CI fix {} changed no code: each read the failure and found no change \
             to make on this branch",
            r(n)
        ),
        (n, p) => format!(
            "CoderHelm ran {} CI fix {} and pushed {p} fix{}; the check still fails",
            n.max(1),
            r(n.max(1)),
            if p == 1 { "" } else { "es" }
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci_repro::Leg;

    const ROUND: &str = "I made no code change. The failing e2e test (`Day passes › dock filter shows only that dock's slips and clears`) failed because staging data changed while it ran, not because of this PR.\n\n**What I found:**\n- Base branch fails the same way.\n\nEvidence:\n- Confirmed: on the base branch's build the full-week page still shows slip 900154.\n- Confirmed: the slip went inactive just before the run.\n- Unconfirmed [check:tests]: I couldn't re-run the failing Playwright test.";

    #[test]
    fn a_round_with_confirmed_evidence_is_classified() {
        let s = classify(
            ROUND,
            &[],
            "c6da43eae6ee",
            &["Deploy Preview".into()],
            &[],
            &["apps/web/src/components/block.tsx".into()],
        )
        .expect("classified");
        assert_eq!(
            s.tests,
            vec!["Day passes › dock filter shows only that dock's slips and clears".to_string()]
        );
        assert_eq!(s.proof.len(), 2, "{:?}", s.proof);
        assert!(s.proof[0].starts_with("on the base branch's build"));
        assert!(s.on("c6da43e"));
    }

    #[test]
    fn no_claim_or_no_evidence_is_not_classified() {
        let fix = "I fixed the selector. Evidence:\n- Confirmed: the test passes locally.";
        assert!(classify(fix, &[], "abc1234", &[], &[], &[]).is_none());
        let bare = "This failure isn't caused by this change.";
        assert!(classify(bare, &[], "abc1234", &[], &[], &[]).is_none());
        assert!(classify(ROUND, &[], "", &[], &[], &[]).is_none());
    }

    #[test]
    fn a_test_the_change_edits_is_the_changes() {
        let tests = vec!["e2e/tests/browse/docks.spec.ts › Day passes › filter".to_string()];
        let changed = vec!["e2e/tests/browse/docks.spec.ts".to_string()];
        assert!(classify(ROUND, &[], "abc1234", &[], &tests, &changed).is_none());
        // Unless a comparison shows it fails on the base too.
        let red = |ran| Leg {
            target: "x".into(),
            ran,
            passed: 0,
            failed: 1,
            note: String::new(),
        };
        let rec = Record {
            head: "abc1234".into(),
            check: "E2E".into(),
            tests: tests.clone(),
            base: red(true),
            change: red(true),
            source: "sandbox".into(),
            at: String::new(),
        };
        let s = classify(
            ROUND,
            std::slice::from_ref(&rec),
            "abc1234",
            &[],
            &tests,
            &changed,
        )
        .unwrap();
        assert!(s.proof[0].starts_with("Not this change's"), "{:?}", s.proof);
        // A comparison on this head that shows the change makes it fail
        // outweighs the round's account.
        let ours = Record {
            base: Leg {
                passed: 1,
                failed: 0,
                ..red(true)
            },
            ..rec
        };
        assert!(classify(ROUND, &[ours], "abc1234", &[], &[], &[]).is_none());
    }

    #[test]
    fn the_standing_reads_the_same_everywhere() {
        assert!(not_ours("red_on_base") && not_ours(CI_END));
        assert!(!not_ours("no_change") && !not_ours(""));
        let mut s = classify(
            ROUND,
            &[],
            "c6da43eae6ee",
            &["Deploy Preview".into()],
            &[],
            &[],
        )
        .unwrap();
        s.rerun = Rerun::Started;
        s.fix = "flaky-test fix started for it".into();
        let sec = section(&s);
        assert!(
            sec.starts_with("## CI failure outside this change"),
            "{sec}"
        );
        assert!(sec.contains("`Deploy Preview` fails on `Day passes › dock filter"));
        assert!(sec.contains("ran again once on c6da43e and failed the same way"));
        assert!(sec.contains("Fix for the test: flaky-test fix started"));
        assert!(!sec.contains("fix attempt"));
        assert_eq!(from_json(&to_json(&s)), Some(s.clone()));
        let note = run_note(&s);
        assert!(note.contains("ready for review") && !note.contains("draft"));
    }

    #[test]
    fn a_named_test_finds_its_record() {
        let t = "Day passes › dock filter shows only that dock's slips and clears";
        assert!(same_test(t, t, "browse/docks.spec.ts"));
        assert!(same_test(
            &format!("browse/docks.spec.ts › {t}"),
            t,
            "browse/docks.spec.ts"
        ));
        assert!(!same_test(
            &format!("browse/slips.spec.ts › {t}"),
            t,
            "browse/docks.spec.ts"
        ));
        assert!(!same_test("Day passes › other", t, ""));
        assert!(!same_test("", t, ""));
    }

    #[test]
    fn rounds_are_counted_as_the_record_has_them() {
        assert_eq!(rounds_said(0, 0), "");
        let none = rounds_said(2, 0);
        assert!(
            none.starts_with("CoderHelm's 2 CI fix rounds changed no code"),
            "{none}"
        );
        assert!(!none.contains("without getting it green"));
        assert_eq!(
            rounds_said(2, 1),
            "CoderHelm ran 2 CI fix rounds and pushed 1 fix; the check still fails"
        );
        assert!(rounds_said(1, 0).contains("1 CI fix round changed"));
    }
}
