//! What a repository requires of a change, and the findings that block
//! because of it whatever the model called them.
//!
//! The verdict is a function of the findings and the team's policy, never the
//! model's free choice. Four kinds of finding block by rule ([`mandatory`]),
//! and no later step can make them a note (a critic, the team's calibration,
//! the late-blocker rule, a second look at the same commit, the stop judge):
//!
//! - **A rule the repository requires, broken.** "Require tests" makes a
//!   missing test for a behavior change blocking; a line of the team's or the
//!   repository's instructions written with MUST, MUST NOT, NEVER, REQUIRED or
//!   ALWAYS in capitals is a required rule. Only a stated exemption that the
//!   changed files bear out lifts it ([`check_exemption`]): docs only, config
//!   only, generated files only, tests only, or a pure refactor with the
//!   existing test that proves it. "No test runner in this package" is never
//!   an exemption: the finding says to add the runner the rest of the
//!   repository uses, and the test.
//! - **A failing check on this head** that the PR caused.
//! - **A critical path with no proof**: the change is on something another
//!   system validates or depends on (a signed header, auth, payments, data
//!   writes, a public contract, a deploy step) and no test, check or lookup
//!   shows it holds.
//! - **A possible break of the change's main path, unchecked**: the finding
//!   is about code every request or run of the change goes through
//!   (`main_path`), and neither the reviewer nor the PR's own verification
//!   settled it (no `proof`). It blocks as "needs a check" naming the check
//!   that settles it ([`main_path_unchecked`]); a check that shows it
//!   harmless makes it a note, one that shows it wrong makes it a defect.
//!   "Not verified" is never the last word on a main-path risk.
//!
//! The misses behind it: a new hash for the header a CDN signs uploads with,
//! in a repository that requires tests, was approved with the missing test
//! posted as "a suggestion only" because the package had no test runner
//! (2026-11-10); and an audit of 267 reviews with structured findings found
//! 31 approvals whose own notes named a failing check, a shown defect, a
//! missed goal, a security problem or a critical path with no proof.

use super::review_agent::Finding;

/// The required rule "Require tests" sets.
pub const TESTS_RULE: &str =
    "tests with behavior changes (this repository's \"Require tests\" setting)";

/// Why a required rule broken blocks.
pub const REQUIRED_WHY: &str = "a rule this repository requires is broken";
/// Why a failing check blocks.
pub const FAILING_CHECK_WHY: &str = "a check fails on this commit";
/// Why a critical path without proof blocks.
pub const CRITICAL_WHY: &str = "a critical-path change with no test or proof";
/// Why a possible break of the change's main path, unchecked, blocks.
pub const MAIN_PATH_WHY: &str =
    "a possible break of the change's main path that nothing has checked";

/// Hidden in a posted comment for a required rule: only a fix (or a person
/// resolving the thread) settles it, never an answer.
pub const REQUIRED_MARK: &str = "<!-- coderhelm:required -->";

/// What the repository requires of every change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Policy {
    /// "Require tests" is on.
    pub require_tests: bool,
    /// Lines of the instructions the team marked required.
    pub required_rules: Vec<String>,
}

/// Words that mark a written rule required, in capitals.
const REQUIRED_MARKERS: &[&str] = &["MUST", "NEVER", "REQUIRED", "ALWAYS"];

impl Policy {
    pub fn new(require_tests: bool, instructions: &str) -> Self {
        Self {
            require_tests,
            required_rules: required_rules(instructions),
        }
    }
}

/// The lines of `instructions` written as required: a marker word in
/// capitals (`MUST`, `MUST NOT`, `NEVER`, `REQUIRED`, `ALWAYS`). Lower-case
/// "must" is advice, so a team decides what blocks by how it writes it.
pub fn required_rules(instructions: &str) -> Vec<String> {
    instructions
        .lines()
        .map(|l| l.trim().trim_start_matches(['-', '*', '#', ' ']).trim())
        .filter(|l| {
            l.split(|c: char| !c.is_ascii_alphabetic())
                .any(|w| REQUIRED_MARKERS.contains(&w))
        })
        .map(str::to_string)
        .collect()
}

/// An exemption from the tests rule that the changed files can bear out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exemption {
    DocsOnly,
    ConfigOnly,
    GeneratedOnly,
    TestsOnly,
    /// No behavior change, with the existing test that covers the code.
    Refactor,
}

impl Exemption {
    pub fn as_str(self) -> &'static str {
        match self {
            Exemption::DocsOnly => "only documentation changed",
            Exemption::ConfigOnly => "only configuration or dependency versions changed",
            Exemption::GeneratedOnly => "only generated files changed",
            Exemption::TestsOnly => "only tests changed",
            Exemption::Refactor => {
                "a refactor with no behavior change, covered by an existing test"
            }
        }
    }
}

/// Reasons people give for a missing test that are not exemptions.
const NO_RUNNER_PHRASES: &[&str] = &[
    "no test runner",
    "no runner",
    "no test infra",
    "no testing infra",
    "no test setup",
    "no test framework",
    "no tests in this package",
    "has no tests",
    "no vitest",
    "no jest",
    "no test script",
    "isn't set up for tests",
    "not set up for tests",
    "when the studio gets tests",
    "when it gets tests",
    "once it has tests",
];

/// The text says the package has no way to run tests.
pub fn says_no_runner(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    NO_RUNNER_PHRASES.iter().any(|p| t.contains(p))
}

fn ext(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default()
}

fn is_docs(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    matches!(ext(&p).as_str(), "md" | "mdx" | "rst" | "txt" | "adoc")
        || p.starts_with("docs/")
        || p.contains("/docs/")
        || p.rsplit('/')
            .next()
            .is_some_and(|n| n.starts_with("license") || n.starts_with("changelog"))
}

fn is_generated(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    [
        "generated",
        ".gen.",
        ".pb.go",
        "_pb2.py",
        ".snap",
        "sanity.types.ts",
        "schema.json",
        "pnpm-lock.yaml",
        "package-lock.json",
        "yarn.lock",
        "cargo.lock",
        "go.sum",
    ]
    .iter()
    .any(|m| p.contains(m))
}

fn is_config(path: &str) -> bool {
    matches!(
        ext(path).as_str(),
        "json" | "yaml" | "yml" | "toml" | "ini" | "env" | "properties" | "tfvars" | "cfg" | "conf"
    ) || is_generated(path)
}

/// A test file: in a test directory, or named as a test or spec.
pub fn is_test_file(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    let name = p.rsplit('/').next().unwrap_or(&p);
    p.contains("__tests__/")
        || p.starts_with("test/")
        || p.starts_with("tests/")
        || p.contains("/test/")
        || p.contains("/tests/")
        || p.contains("/e2e/")
        || name.contains(".test.")
        || name.contains(".spec.")
        || name.starts_with("test_")
        || name.ends_with("_test.go")
        || name.ends_with("_test.py")
}

/// Whether `text` names a test file (`path/to/x.test.ts`).
fn cites_test_file(text: &str) -> bool {
    text.split(|c: char| c.is_whitespace() || "`'\"(),;".contains(c))
        .map(|w| w.split(':').next().unwrap_or(w))
        .any(|w| !ext(w).is_empty() && is_test_file(w))
}

/// Checks a claimed exemption against the PR's changed files. Err says why
/// it doesn't apply.
pub fn check_exemption(
    claim: &str,
    proof: Option<&str>,
    changed_files: &[String],
) -> Result<Exemption, String> {
    let c = claim.trim().to_ascii_lowercase();
    if says_no_runner(&c) {
        return Err(
            "a package without a test runner still needs the test: add the runner and the test"
                .into(),
        );
    }
    let all =
        |pred: fn(&str) -> bool| !changed_files.is_empty() && changed_files.iter().all(|f| pred(f));
    let (kind, holds): (Exemption, bool) = if c.contains("refactor") {
        let cited = proof.is_some_and(cites_test_file);
        if !cited {
            return Err(
                "a refactor is exempt only with the existing test that covers the code, named in `proof`"
                    .into(),
            );
        }
        (Exemption::Refactor, true)
    } else if c.contains("generat") {
        (Exemption::GeneratedOnly, all(is_generated))
    } else if c.contains("doc") || c.contains("comment") {
        (Exemption::DocsOnly, all(is_docs))
    } else if c.contains("config") || c.contains("dependenc") || c.contains("bump") {
        (Exemption::ConfigOnly, all(is_config))
    } else if c.contains("test") {
        (Exemption::TestsOnly, all(is_test_file))
    } else {
        return Err(format!(
            "\"{}\" is not an exemption (docs, config, generated, tests only, or a covered refactor)",
            claim.trim()
        ));
    };
    if holds {
        Ok(kind)
    } else {
        Err(format!(
            "the PR changes more than that ({} files, not all of that kind)",
            changed_files.len()
        ))
    }
}

/// The exemption the changed files alone bear out, with no claim needed: a
/// PR that only touches docs, config, generated files or tests.
pub fn exempt_by_files(changed_files: &[String]) -> Option<Exemption> {
    let all =
        |pred: fn(&str) -> bool| !changed_files.is_empty() && changed_files.iter().all(|f| pred(f));
    if all(is_docs) {
        Some(Exemption::DocsOnly)
    } else if all(is_test_file) {
        Some(Exemption::TestsOnly)
    } else if all(is_generated) {
        Some(Exemption::GeneratedOnly)
    } else if all(is_config) {
        Some(Exemption::ConfigOnly)
    } else {
        None
    }
}

/// What a missing runner means for the finding.
pub const NO_RUNNER_ADVICE: &str =
    "No test runner in this package is not a reason to skip the test: \
     add the runner the rest of the repository uses (its dev dependency, a `test` script and the \
     smallest config, as another package here has them), then this test. If CoderHelm wrote this \
     PR, its fix round adds both.";

/// The required-tests line on a finding that blocks.
pub const REQUIRED_TESTS_LINE: &str =
    "This repository requires tests with behavior changes, so this blocks the merge until the test is in the PR.";

/// Phrases that talk a finding down. A finding that blocks never carries
/// them: its words match the verdict.
const HEDGES: &[&str] = &[
    "suggestion only",
    "only a suggestion",
    "just a suggestion",
    "this is optional",
    "optional only",
    "minor cleanup",
    "if you like",
    "if you'd like",
    "not a hold",
    "not a block",
    "only a heads-up",
    "just a heads-up",
    "this is a heads-up",
    "this repository asks for tests with behavior changes",
];

/// `body` without its sentences that call it optional.
pub fn strip_hedges(body: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    for para in body.split("\n\n") {
        let mut kept = String::new();
        let mut rest = para;
        while !rest.is_empty() {
            // A sentence ends at ". ", "! " or "? " (code spans rarely hold one).
            let end = ['.', '!', '?']
                .iter()
                .filter_map(|p| rest.find(&format!("{p} ")).map(|i| i + 2))
                .min()
                .unwrap_or(rest.len());
            let sentence = &rest[..end];
            let lower = sentence.to_ascii_lowercase();
            if !HEDGES.iter().any(|h| lower.contains(h)) {
                kept.push_str(sentence);
            }
            rest = &rest[end..];
        }
        let kept = kept.trim().trim_matches('_').trim();
        if !kept.is_empty() {
            out.push(kept.to_string());
        }
    }
    out.join("\n\n")
}

/// What [`apply`] did, for the logs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// Findings that block because a required rule is broken.
    pub required: usize,
    /// Tests findings a checked exemption lifted.
    pub exempt: usize,
    /// Exemptions claimed that the changed files don't bear out.
    pub refused: usize,
}

fn is_tests(f: &Finding) -> bool {
    ["tests", "test", "test-coverage", "coverage"]
        .iter()
        .any(|c| f.category.trim().eq_ignore_ascii_case(c))
}

/// Marks each finding that breaks a rule the repository requires. A tests
/// finding in a repository with "Require tests" on blocks unless its
/// exemption holds against `changed_files`; a finding whose `team_rule`
/// quotes a required line of the instructions blocks. Runs after the critic
/// and before every step that can make a finding a note.
pub fn apply(findings: &mut [Finding], policy: &Policy, changed_files: &[String]) -> Applied {
    let mut a = Applied::default();
    for f in findings.iter_mut() {
        if f.caused_by_diff == Some(false) {
            continue;
        }
        if policy.require_tests && is_tests(f) {
            let claimed = f.exemption.clone().filter(|e| !e.trim().is_empty());
            let checked = match claimed.as_deref() {
                Some(c) => check_exemption(c, f.proof.as_deref(), changed_files),
                None => exempt_by_files(changed_files).ok_or_else(String::new),
            };
            match checked {
                Ok(e) => {
                    f.exempt = Some(e.as_str().to_string());
                    f.optional = Some(true);
                    f.body = format!(
                        "{}\n\n_Not required here: {}._",
                        strip_hedges(&f.body),
                        e.as_str()
                    );
                    a.exempt += 1;
                    continue;
                }
                Err(why) if !why.is_empty() => {
                    f.body = format!(
                        "{}\n\n_The exemption given ({}) doesn't apply: {why}._",
                        f.body.trim_end(),
                        claimed.as_deref().unwrap_or("").trim()
                    );
                    a.refused += 1;
                }
                Err(_) => {}
            }
            f.required = Some(TESTS_RULE.to_string());
            let mut body = strip_hedges(&f.body);
            let no_runner = says_no_runner(&format!(
                "{} {} {}",
                f.title,
                f.body,
                claimed.as_deref().unwrap_or("")
            ));
            if no_runner && !body.contains(NO_RUNNER_ADVICE) {
                body.push_str(&format!("\n\n{NO_RUNNER_ADVICE}"));
            }
            if !body.contains(REQUIRED_TESTS_LINE) {
                body.push_str(&format!("\n\n_{REQUIRED_TESTS_LINE}_"));
            }
            f.body = body;
            a.required += 1;
            continue;
        }
        let quoted = f.team_rule.as_deref().and_then(|r| {
            policy.required_rules.iter().find(|req| {
                super::review_severity::quotes_team_rule(r, req)
                    || super::review_severity::quotes_team_rule(req, r)
            })
        });
        if let Some(rule) = quoted {
            f.required = Some(rule.clone());
            f.body = strip_hedges(&f.body);
            a.required += 1;
        }
    }
    a
}

/// Phrases that cite a check failing on the PR's own commit.
const FAILING_CHECK_PHRASES: &[&str] = &[
    "fails on this head",
    "failed on this head",
    "failing on this head",
    "red on this head",
    "check is red",
    "checks are red",
    "ci is red",
    "job failed on this commit",
    "failed on this commit",
    "fails on this commit",
];

/// The finding cites a check that fails on this commit.
pub fn cites_failing_check(f: &Finding) -> bool {
    let t = format!("{} {}", f.title, f.body).to_ascii_lowercase();
    FAILING_CHECK_PHRASES.iter().any(|p| t.contains(p))
}

/// Proof that says there is none.
fn no_proof(proof: Option<&str>) -> bool {
    let Some(p) = proof.map(|p| p.trim().to_ascii_lowercase()) else {
        return true;
    };
    p.is_empty()
        || ["none", "no", "n/a", "na", "nothing", "-"].contains(&p.as_str())
        || p.starts_with("no ")
        || p.starts_with("none ")
        || p.starts_with("nothing ")
        || p.contains("checked by hand")
        || p.contains("manual")
}

/// Categories a critical-path finding can't be: a preference or a question
/// about wording is never a critical path.
const NOT_CRITICAL: &[&str] = &[
    "style",
    "nit",
    "naming",
    "formatting",
    "docs",
    "documentation",
    "logging",
    "log-level",
];

/// The finding is on a critical path and nothing proves the change holds.
pub fn critical_unproven(f: &Finding) -> bool {
    f.critical_path
        .as_deref()
        .is_some_and(|c| !c.trim().is_empty())
        && no_proof(f.proof.as_deref())
        && !f.file.trim().is_empty()
        && !NOT_CRITICAL
            .iter()
            .any(|c| f.category.trim().eq_ignore_ascii_case(c))
}

/// Categories a main-path risk can't be: a preference, a request for a test
/// (judged by the tests rule), or a by-hand test suggestion.
const NOT_MAIN_PATH: &[&str] = &[
    "style",
    "nit",
    "naming",
    "formatting",
    "docs",
    "documentation",
    "logging",
    "log-level",
    "convention",
    "manual-check",
    "tests",
    "test",
    "test-coverage",
    "coverage",
    "question",
];

/// Proof that says nothing settled a main-path risk. Unlike a critical path,
/// a run by hand that the PR reports (the exact path, once, against the real
/// system) settles it: that is the check such a finding asks for.
fn nothing_settled(proof: Option<&str>) -> bool {
    let Some(p) = proof.map(|p| p.trim().to_ascii_lowercase()) else {
        return true;
    };
    p.is_empty()
        || ["none", "no", "n/a", "na", "nothing", "-"].contains(&p.as_str())
        || p.starts_with("no ")
        || p.starts_with("none ")
        || p.starts_with("nothing ")
}

/// The finding is a possible break of the change's main path and nothing
/// settled it: no proof from the reviewer's checks or the PR's own.
pub fn main_path_unchecked(f: &Finding) -> bool {
    f.main_path.as_deref().is_some_and(|m| !m.trim().is_empty())
        && nothing_settled(f.proof.as_deref())
        && !f.file.trim().is_empty()
        && f.evidence() != super::review_agent::Evidence::Question
        && !NOT_MAIN_PATH
            .iter()
            .any(|c| f.category.trim().eq_ignore_ascii_case(c))
}

/// The check that settles an unchecked main-path risk: the one the
/// finding names, or a run of the path it hits.
pub fn settling_check(f: &Finding) -> String {
    match f.check.as_deref().map(str::trim).filter(|c| !c.is_empty()) {
        Some(c) => c.trim_end_matches('.').to_string(),
        None => format!(
            "make one real call on that path ({}) and paste the result in the PR",
            f.main_path.as_deref().unwrap_or("the changed path").trim()
        ),
    }
}

/// Why the finding blocks by rule, if it does. Nothing downstream can make
/// such a finding a note; only the PR's change can (a fix, or the thread a
/// person resolves).
pub fn mandatory(f: &Finding) -> Option<&'static str> {
    if f.caused_by_diff == Some(false) || f.exempt.is_some() {
        return None;
    }
    if f.required.is_some() {
        return Some(REQUIRED_WHY);
    }
    if f.tracked == Some(true) {
        return None;
    }
    if cites_failing_check(f) {
        return Some(FAILING_CHECK_WHY);
    }
    if critical_unproven(f) {
        return Some(CRITICAL_WHY);
    }
    if main_path_unchecked(f) {
        return Some(MAIN_PATH_WHY);
    }
    None
}

/// The label a blocking-by-rule finding carries on its comment.
pub fn mandatory_label(f: &Finding) -> Option<String> {
    Some(match mandatory(f)? {
        REQUIRED_WHY if f.required.as_deref() == Some(TESTS_RULE) => {
            "Blocking (required: tests)".to_string()
        }
        REQUIRED_WHY => "Blocking (required rule)".to_string(),
        FAILING_CHECK_WHY => "Blocking (failing check)".to_string(),
        MAIN_PATH_WHY if f.evidence() == super::review_agent::Evidence::VerifiedDefect => {
            "Blocking (main path)".to_string()
        }
        MAIN_PATH_WHY => "Blocking (needs a check)".to_string(),
        _ => "Blocking (critical path, no proof)".to_string(),
    })
}

/// Whether a review's reading settles an earlier thread of CoderHelm's. A
/// thread for a required rule is settled only by a fix: an answer that
/// explains why the test is missing leaves the rule broken.
pub fn settles(thread_body: &str, status: &str) -> bool {
    if thread_body.contains(REQUIRED_MARK) {
        status.eq_ignore_ascii_case("fixed")
    } else {
        ["fixed", "answered", "accepted"]
            .iter()
            .any(|s| status.eq_ignore_ascii_case(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn finding(v: serde_json::Value) -> Finding {
        let mut base = json!({"file": "src/a.ts", "line": 3, "title": "t", "body": "b"});
        for (k, val) in v.as_object().unwrap() {
            base[k] = val.clone();
        }
        serde_json::from_value(base).unwrap()
    }

    fn files(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn required_rules_are_the_lines_written_in_capitals() {
        let rules = required_rules(
            "Focus on:\n- Lockfile: any dependency change MUST include a pnpm-lock.yaml update.\n\
             - Tests: changed components should keep coverage.\n- Secrets NEVER go to the client.\n\
             - you must be kind",
        );
        assert_eq!(
            rules,
            vec![
                "Lockfile: any dependency change MUST include a pnpm-lock.yaml update.",
                "Secrets NEVER go to the client."
            ]
        );
    }

    #[test]
    fn exemptions_are_checked_against_the_changed_files() {
        let docs = files(&["README.md", "docs/setup.md"]);
        assert_eq!(
            check_exemption("docs only", None, &docs),
            Ok(Exemption::DocsOnly)
        );
        let mixed = files(&["README.md", "src/a.ts"]);
        assert!(check_exemption("docs only", None, &mixed).is_err());
        assert_eq!(
            check_exemption(
                "generated",
                None,
                &files(&["apps/web/sanity.types.ts", "apps/studio/schema.json"])
            ),
            Ok(Exemption::GeneratedOnly)
        );
        assert!(check_exemption("refactor", Some("none"), &mixed).is_err());
        assert_eq!(
            check_exemption("refactor", Some("covered by `src/a.test.ts`"), &mixed),
            Ok(Exemption::Refactor)
        );
        // No runner is never an exemption.
        let err = check_exemption("apps/studio has no test runner", None, &mixed).unwrap_err();
        assert!(err.contains("add the runner"), "{err}");
        assert!(check_exemption("will add tests later", None, &mixed).is_err());
        assert_eq!(exempt_by_files(&docs), Some(Exemption::DocsOnly));
        assert_eq!(exempt_by_files(&mixed), None);
    }

    #[test]
    fn hedges_are_taken_out_of_a_blocking_finding() {
        let body = "Nothing keeps that check. When it gets tests, a small case would lock it in. \
                    This is a suggestion only.\n\n_This repository asks for tests with behavior changes._";
        let s = strip_hedges(body);
        assert_eq!(
            s,
            "Nothing keeps that check. When it gets tests, a small case would lock it in."
        );
    }

    #[test]
    fn a_required_test_blocks_and_carries_the_runner_advice() {
        let policy = Policy::new(true, "");
        let mut fs = vec![finding(json!({
            "category": "tests", "severity": "medium", "optional": true,
            "title": "sha256Hex check isn't kept as a test",
            "body": "apps/studio has no test runner (no vitest in its package.json), so nothing keeps that check. This is a suggestion only."
        }))];
        let a = apply(
            &mut fs,
            &policy,
            &files(&["apps/studio/plugins/aws-s3/post-to-upload-handler.ts"]),
        );
        assert_eq!(a.required, 1);
        assert_eq!(mandatory(&fs[0]), Some(REQUIRED_WHY));
        assert!(fs[0].holds_merge());
        assert!(fs[0].body.contains(NO_RUNNER_ADVICE));
        assert!(fs[0].body.contains(REQUIRED_TESTS_LINE));
        assert!(!fs[0].body.to_ascii_lowercase().contains("suggestion only"));
        assert_eq!(
            mandatory_label(&fs[0]).as_deref(),
            Some("Blocking (required: tests)")
        );
        // Off: the same finding is a note.
        let mut off = vec![finding(
            json!({"category": "tests", "optional": true, "body": "add a test"}),
        )];
        apply(&mut off, &Policy::new(false, ""), &[]);
        assert!(!off[0].holds_merge());
    }

    #[test]
    fn a_checked_exemption_lifts_the_rule_and_a_refused_one_says_why() {
        let policy = Policy::new(true, "");
        let mut ok = vec![finding(
            json!({"category": "tests", "exemption": "docs only", "file": "README.md"}),
        )];
        assert_eq!(apply(&mut ok, &policy, &files(&["README.md"])).exempt, 1);
        assert!(!ok[0].holds_merge());
        let mut bad = vec![finding(
            json!({"category": "tests", "exemption": "docs only"}),
        )];
        let a = apply(&mut bad, &policy, &files(&["README.md", "src/a.ts"]));
        assert_eq!((a.refused, a.required), (1, 1));
        assert!(bad[0].holds_merge());
        assert!(bad[0].body.contains("doesn't apply"));
    }

    #[test]
    fn a_required_team_rule_blocks_and_an_unmarked_one_does_not() {
        let policy = Policy::new(false, "- Any dependency change MUST include a matching pnpm-lock.yaml update.\n- Prefer small PRs.");
        let mut fs = vec![
            finding(
                json!({"category": "convention", "team_rule": "Any dependency change MUST include a matching pnpm-lock.yaml update."}),
            ),
            finding(json!({"category": "convention", "team_rule": "Prefer small PRs."})),
        ];
        assert_eq!(apply(&mut fs, &policy, &[]).required, 1);
        assert!(fs[0].holds_merge());
        assert!(!fs[1].holds_merge());
    }

    #[test]
    fn a_critical_path_blocks_without_proof_and_not_with_it() {
        let unproven = finding(
            json!({"category": "correctness", "evidence": "unverified-concern",
            "optional": true, "critical_path": "the x-amz-content-sha256 header CloudFront validates"}),
        );
        assert_eq!(mandatory(&unproven), Some(CRITICAL_WHY));
        assert!(unproven.holds_merge());
        let proven = finding(
            json!({"category": "correctness", "critical_path": "the signed header",
            "proof": "apps/studio/test/hash.test.ts compares it with createHash"}),
        );
        assert_eq!(mandatory(&proven), None);
        let style = finding(json!({"category": "style", "critical_path": "x"}));
        assert_eq!(mandatory(&style), None);
        let not_ours =
            finding(json!({"category": "bug", "critical_path": "x", "caused_by_diff": false}));
        assert_eq!(mandatory(&not_ours), None);
    }

    #[test]
    fn a_failing_check_on_this_head_blocks_whatever_its_category() {
        let lint = finding(json!({"category": "style", "severity": "medium",
            "body": "Prettier rewrites it, and the 'Code linting' job failed on this head."}));
        assert!(lint.holds_merge());
        let base = finding(json!({"category": "style", "caused_by_diff": false,
            "body": "The check is red on main too."}));
        assert!(!base.holds_merge());
    }

    #[test]
    fn a_required_thread_is_settled_only_by_a_fix() {
        let body = format!("🔴 **Blocking (required: tests): x**\n\n{REQUIRED_MARK}");
        assert!(!settles(&body, "answered"));
        assert!(!settles(&body, "accepted"));
        assert!(settles(&body, "fixed"));
        assert!(settles("💬 **Nit: x**", "answered"));
    }
}
