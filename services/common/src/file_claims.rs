//! A sentence a run writes into the repository is held to the same proof as
//! one it writes on the pull request.
//!
//! A run that edits a README, a changelog or a docs page can write "the
//! build passes with these versions" there, and the file is then merged with
//! a claim nothing checked. The pull request's own sections say only what a
//! check recorded; a file the run writes must too.
//!
//! - [`claims_in_patch`] reads the lines a change adds to a prose file and
//!   returns each sentence that states, as a fact, that the build, the
//!   tests, the linter or the type check passes.
//! - [`unproven`] keeps the ones no recorded passing result covers on this
//!   head. The run then runs the check, or takes the sentence out
//!   ([`without`]) before it finishes.
//!
//! The reading is narrow on purpose, since most prose that names a build or
//! a test is not a claim: an instruction ("make sure the tests pass"), a
//! condition ("once CI is green"), a plan ("the build should pass"), a
//! question, a heading, a command, a negative ("the tests fail on Node 18")
//! and a sentence where the word is something else ("the build passes
//! options to the bundler") are all left alone. Only a plain statement with
//! the check as its subject and a passing result as what is said of it
//! counts.

use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

/// The kinds of check a sentence can claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimKind {
    Build,
    Tests,
    Lint,
    Typecheck,
}

impl ClaimKind {
    /// The name the run's check records use (`checks::CheckKind::name`).
    pub fn name(self) -> &'static str {
        match self {
            ClaimKind::Build => "build",
            ClaimKind::Tests => "tests",
            ClaimKind::Lint => "lint",
            ClaimKind::Typecheck => "typecheck",
        }
    }

    fn words(self) -> &'static str {
        match self {
            ClaimKind::Build => "the build",
            ClaimKind::Tests => "the tests",
            ClaimKind::Lint => "the linter",
            ClaimKind::Typecheck => "the type check",
        }
    }
}

/// One sentence a change adds to a file that claims a check passes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Claim {
    pub path: String,
    /// The line in the new file.
    pub line: u64,
    pub sentence: String,
    pub kinds: Vec<ClaimKind>,
}

impl Claim {
    /// What the claim says passes, in words (`the build and the tests`).
    pub fn what(&self) -> String {
        let w: Vec<&str> = self.kinds.iter().map(|k| k.words()).collect();
        match w.as_slice() {
            [] => String::new(),
            [one] => one.to_string(),
            [init @ .., last] => format!("{} and {last}", init.join(", ")),
        }
    }
}

/// A file whose added sentences are read: prose a person reads as fact.
pub fn is_prose_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    [".md", ".mdx", ".rst", ".adoc", ".txt"]
        .iter()
        .any(|e| name.ends_with(e))
        || matches!(
            name,
            "readme" | "changelog" | "changes" | "history" | "news"
        )
}

struct Patterns {
    subjects: Vec<(ClaimKind, Regex)>,
    /// A word that makes the sentence something other than a statement of
    /// fact about now: an instruction, a plan, a requirement.
    not_a_fact: Regex,
    /// A word that, before the check's name, makes what follows a
    /// condition ("once the build is green") and not a statement.
    condition: Regex,
    negative: Regex,
    /// "0 failed", "no failures": a pass said through its failures.
    zero_failures: Regex,
}

/// What may follow the check's name before the passing word: a few plain
/// words ("now", "and lint", "of the web app"), never a clause.
const GAP: &str = r"(?:,?[ \t]+(?:and|&|or|plus)?[ \t]*[\w`'./@:-]+){0,4}?,?[ \t]+";
/// The passing word, and what may follow `pass`: a sentence end or a word
/// that continues the result ("on Node 22", "with these versions"). `pass`
/// with an object ("passes options to") is another verb.
const PASSES: &str = r"(?:(?:(?:all|both|now|still|again|also)[ \t]+)*(?:pass(?:es|ed)?|succeeds?|succeeded)(?:[ \t]+(?:on|with|in|for|again|now|locally|cleanly|successfully|after|against|at|under|and|too|here|there|everywhere)\b|[ \t]*(?:$|[.,;:!)]))|(?:is|are|was|were|stays?|remains?|came back|comes back|runs?|ran)[ \t]+(?:(?:all|now|still|again)[ \t]+)*(?:green|clean|passing|successful)\b|(?:builds?|compiles?|built|compiled)[ \t]+(?:successfully|cleanly|without (?:errors?|warnings?)|with no errors?)\b)";

fn patterns() -> &'static Patterns {
    static P: OnceLock<Patterns> = OnceLock::new();
    P.get_or_init(|| {
        let re = |p: &str| Regex::new(p).expect("a fixed pattern");
        let subject = |names: &str| re(&format!(r"(?i)\b(?:{names})\b{GAP}{PASSES}"));
        Patterns {
            subjects: vec![
                (
                    ClaimKind::Build,
                    subject(r"build|production build|app build|compilation|compile"),
                ),
                (
                    ClaimKind::Tests,
                    subject(r"tests|test suite|unit tests|integration tests|end-to-end tests|e2e tests|specs|suite"),
                ),
                (
                    ClaimKind::Lint,
                    subject(r"lint|linter|linting|eslint|clippy|lint check"),
                ),
                (
                    ClaimKind::Typecheck,
                    subject(r"type[- ]?check|type[- ]?checks|type[- ]?checking|typecheck|tsc"),
                ),
            ],
            not_a_fact: re(
                r"(?i)\b(?:should|must|shall|will|would|could|might|may|can|need|needs|needed|want|wants|expect|expects|expected|require|requires|required|ensure|verify|check that|confirm|make sure|whether|so that|in order|to make|to get|to keep|todo|tbd|try|please|let's|how to|example|e\.g)\b",
            ),
            condition: re(r"(?i)\b(?:if|when|whenever|once|until|unless|before|after|while|as soon as)\b"),
            negative: re(r"(?i)(?:\b(?:not|never|no longer|fail|fails|failed|failing|broken|breaks|flaky|red)\b|n't\b)"),
            zero_failures: re(r"(?i)\b(?:0|zero|no)[ \t]+(?:failed|failures?|errors?)\b|\bwithout (?:failures?|errors?|warnings?)\b|\bwith no errors?\b"),
        }
    })
}

/// The builds a sentence names with the `builds successfully` form, where
/// the check's name and the passing word are one word.
fn build_verb() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        Regex::new(
            r"(?i)\b(?:builds?|compiles?|built|compiled)[ \t]+(?:successfully|cleanly|without (?:errors?|warnings?)|with no errors?)\b",
        )
        .expect("a fixed pattern")
    })
}

/// The kinds of check `sentence` states as passing; empty when it is not
/// such a statement.
pub fn claimed(sentence: &str) -> Vec<ClaimKind> {
    let p = patterns();
    let s = sentence.trim();
    if s.is_empty() || s.ends_with('?') || p.not_a_fact.is_match(s) {
        return vec![];
    }
    // A pass said through its failures ("0 failed") is not a negative.
    let plain = p.zero_failures.replace_all(s, " ");
    if p.negative.is_match(&plain) {
        return vec![];
    }
    let stated = |re: &Regex| {
        re.find(s)
            .is_some_and(|m| !p.condition.is_match(&s[..m.start()]))
    };
    let mut kinds: Vec<ClaimKind> = p
        .subjects
        .iter()
        .filter(|(_, re)| stated(re))
        .map(|(k, _)| *k)
        .collect();
    if !kinds.contains(&ClaimKind::Build) && stated(build_verb()) {
        kinds.insert(0, ClaimKind::Build);
    }
    kinds
}

/// A line that is not prose: a heading, a command, a table rule, an image
/// or badge, a quoted line of someone else's text.
fn skipped_line(line: &str) -> bool {
    let t = line.trim_start();
    t.starts_with('#')
        || t.starts_with('$')
        || t.starts_with("![")
        || t.starts_with("[![")
        || t.starts_with('>')
        || t.starts_with("|-")
        || t.starts_with("- [ ]")
        || t.starts_with("* [ ]")
        || line.starts_with("    ")
        || line.starts_with('\t')
}

/// The text of a line with its inline code taken out: a command in
/// backticks is not a sentence's words.
fn without_inline_code(line: &str) -> String {
    let mut out = String::new();
    let mut code = false;
    for c in line.chars() {
        if c == '`' {
            code = !code;
            out.push('`');
        } else if !code {
            out.push(c);
        }
    }
    out
}

/// The claims a change adds to `path`, read from its patch (a unified diff
/// as GitHub's compare gives it). Nothing for a file that is not prose.
pub fn claims_in_patch(path: &str, patch: &str) -> Vec<Claim> {
    if !is_prose_path(path) {
        return vec![];
    }
    let mut out = vec![];
    let mut line_no: u64 = 0;
    let mut fenced = false;
    for raw in patch.lines() {
        if let Some(rest) = raw.strip_prefix("@@") {
            // `@@ -a,b +c,d @@`: the new file's line the hunk starts at. A
            // hunk never starts inside a fence this reader has seen open.
            line_no = rest
                .split('+')
                .nth(1)
                .and_then(|s| s.split([',', ' ']).next())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(1);
            fenced = false;
            continue;
        }
        let (added, text) = match raw.chars().next() {
            Some('+') => (true, &raw[1..]),
            Some('-') | Some('\\') => continue,
            Some(' ') => (false, &raw[1..]),
            _ => (false, raw),
        };
        let here = line_no;
        line_no += 1;
        let t = text.trim_start();
        if t.starts_with("```") || t.starts_with("~~~") {
            fenced = !fenced;
            continue;
        }
        if !added || fenced || skipped_line(text) {
            continue;
        }
        for (a, b) in crate::prose::sentence_spans(text) {
            // The sentence as the file has it, without its bullet.
            let sentence = text[a..b]
                .trim()
                .trim_start_matches(['-', '*', '+'])
                .trim_start();
            let kinds = claimed(&without_inline_code(sentence));
            if kinds.is_empty() {
                continue;
            }
            out.push(Claim {
                path: path.to_string(),
                line: here,
                sentence: sentence.to_string(),
                kinds,
            });
        }
    }
    out
}

/// The claims no recorded passing result covers: `proven` holds the kinds
/// of check with a passing result recorded on this head.
pub fn unproven(claims: &[Claim], proven: &[ClaimKind]) -> Vec<Claim> {
    claims
        .iter()
        .filter(|c| c.kinds.iter().any(|k| !proven.contains(k)))
        .cloned()
        .collect()
}

/// `text` (the file at `path`) without the sentences of `claims` that are
/// in it. A line left with nothing but its bullet goes too. Returns `None`
/// when no sentence was found, so nothing is written for nothing.
pub fn without(path: &str, text: &str, claims: &[Claim]) -> Option<String> {
    let mine: Vec<&Claim> = claims.iter().filter(|c| c.path == path).collect();
    let mut changed = false;
    let mut out: Vec<String> = vec![];
    for line in text.lines() {
        if !mine.iter().any(|c| line.contains(c.sentence.as_str())) {
            out.push(line.to_string());
            continue;
        }
        let mut kept = line.to_string();
        for c in &mine {
            if let Some(at) = kept.find(c.sentence.as_str()) {
                let end = at + c.sentence.len();
                // The space that joined it to the next sentence goes with it.
                let end = if kept[end..].starts_with(' ') {
                    end + 1
                } else {
                    end
                };
                kept.replace_range(at..end, "");
                changed = true;
            }
        }
        let bare = kept
            .trim()
            .trim_start_matches(['-', '*', '+'])
            .trim_start_matches(|c: char| c.is_ascii_digit() || c == '.')
            .trim();
        if !bare.is_empty() {
            out.push(kept.trim_end().to_string());
        }
    }
    if !changed {
        return None;
    }
    let mut joined = out.join("\n");
    if text.ends_with('\n') {
        joined.push('\n');
    }
    Some(joined)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ClaimKind::*;

    #[test]
    fn a_plain_statement_that_a_check_passes_is_a_claim() {
        for (s, want) in [
            ("All tests pass.", vec![Tests]),
            ("The build passes with these versions.", vec![Build]),
            ("Lint and type check are clean.", vec![Lint, Typecheck]),
            ("The test suite is green on Node 22.", vec![Tests]),
            ("Verified: the production build succeeds.", vec![Build]),
            (
                "The app builds successfully after the upgrade.",
                vec![Build],
            ),
            ("Build, lint and tests all pass.", vec![Build, Tests, Lint]),
            (
                "The unit tests ran clean (212 passed, 0 failed).",
                vec![Tests],
            ),
            ("tsc passes.", vec![Typecheck]),
            ("The e2e tests passed against staging.", vec![Tests]),
            ("The build passes after the upgrade.", vec![Build]),
        ] {
            assert_eq!(claimed(s), want, "{s}");
        }
    }

    #[test]
    fn ordinary_prose_about_builds_and_tests_is_not_a_claim() {
        for s in [
            // Instructions, conditions, plans, requirements.
            "Make sure the tests pass before you open a pull request.",
            "Run `pnpm build` and check that the build passes.",
            "Once the build is green, deploy from the release branch.",
            "After the tests pass, the release job tags the commit.",
            "The build should pass on Node 20 and later.",
            "A pull request needs the tests passing to merge.",
            "If the linter is clean, the hook lets the commit through.",
            "Tests must pass on every supported version.",
            "Does the build pass on Windows?",
            // Negatives.
            "The tests fail on Node 18.",
            "The build does not pass without the env file.",
            "Type checking isn't clean yet for the legacy folder.",
            // The word means something else.
            "The build passes options to the bundler through `--config`.",
            "Each test passes a fixture to the handler.",
            "The suite passes the locale to every spec.",
            "The linter is configured in `eslint.config.js`.",
            "Tests live in `packages/*/test`.",
            "Bumped the build tooling to version 5.",
            "Fixed the failing tests for the date parser.",
            "The build step runs after lint in CI.",
            "Added a test for the retry option.",
            "The types are generated from the schema.",
            "This release changes how the build output is cached.",
            "",
        ] {
            assert!(claimed(s).is_empty(), "{s}: {:?}", claimed(s));
        }
    }

    const PATCH: &str = "@@ -10,6 +10,12 @@ ## Upgrading\n context line\n+## Verified: the build passes\n+The build passes with these versions. Path matching is unchanged.\n+- All tests pass.\n+\n+```sh\n+pnpm build # the build passes\n+```\n+Make sure the tests pass before releasing.\n-An old line where the tests pass.\n unchanged: the tests pass.\n";

    #[test]
    fn only_added_prose_lines_of_prose_files_are_read() {
        let found = claims_in_patch("docs/upgrading.md", PATCH);
        let got: Vec<(u64, &str, &[ClaimKind])> = found
            .iter()
            .map(|c| (c.line, c.sentence.as_str(), c.kinds.as_slice()))
            .collect();
        assert_eq!(
            got,
            vec![
                (12, "The build passes with these versions.", &[Build][..]),
                (13, "All tests pass.", &[Tests][..]),
            ],
            "the heading, the fenced command, the instruction, the removed line and the \
             unchanged line are not claims this change adds"
        );
        assert!(claims_in_patch("src/build.ts", PATCH).is_empty());
        assert!(is_prose_path("CHANGELOG") && is_prose_path("apps/web/README.md"));
        assert!(!is_prose_path("package.json"));
    }

    #[test]
    fn a_claim_stands_only_with_a_recorded_passing_result() {
        let found = claims_in_patch("README.md", PATCH);
        assert_eq!(unproven(&found, &[Build, Tests]), vec![]);
        let open = unproven(&found, &[Tests]);
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].what(), "the build");
        assert_eq!(unproven(&found, &[]).len(), 2);
    }

    #[test]
    fn an_unproven_claim_is_taken_out_and_the_rest_of_the_file_stays() {
        let file = "## Upgrading\n\nThe build passes with these versions. Path matching is unchanged.\n- All tests pass.\n- Node 20 or later.\n";
        let found = vec![
            Claim {
                path: "README.md".into(),
                line: 3,
                sentence: "The build passes with these versions.".into(),
                kinds: vec![Build],
            },
            Claim {
                path: "README.md".into(),
                line: 4,
                sentence: "All tests pass.".into(),
                kinds: vec![Tests],
            },
        ];
        assert_eq!(
            without("README.md", file, &found).unwrap(),
            "## Upgrading\n\nPath matching is unchanged.\n- Node 20 or later.\n"
        );
        assert_eq!(without("docs/other.md", file, &found), None);
        assert_eq!(without("README.md", "Nothing here.\n", &found), None);
    }
}
