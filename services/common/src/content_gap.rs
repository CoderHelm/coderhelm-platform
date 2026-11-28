//! A red check that names missing content, data or a migration is a content
//! gap, never a code failure to retry.
//!
//! A feature read club addresses from the CMS. Staging had none, so the
//! change carried a migration to fill them, and its own end-to-end test
//! failed by name until the migration ran ("... is incomplete in this
//! dataset; run apps/studio/migrations/..."). The repository had no staging
//! CMS write connection, so CoderHelm could never run it. Its CI fix rounds
//! met the same failure four times, changed no code (the code was right:
//! it already left the block out when the address was missing), spent the
//! run's fix budget and ended the run failed. A second feature met the
//! same shape with test fixtures a migration adds.
//!
//! The rules, each enforced in code:
//!
//! - **The failure is read for a content gap first.** [`names_gap`]: an
//!   error that names missing or incomplete content, a dataset, fixtures, a
//!   stale snapshot, or a migration to run.
//! - **A gap a pending step fills ends the CI loop, not the run.**
//!   [`classify`]: when the failing check is a test, and the change's own
//!   outside step that supplies the content is not done and CoderHelm can't
//!   do it (no write connection, or a person's step), the run stops fixing
//!   CI without spending a round: `ci_end` is [`CI_END`], the pull request
//!   says what to run before merge ([`section`]), and the run is completed
//!   with its pull request ready. Auto-merge still won't merge over the red
//!   check.
//! - **Otherwise the round makes the code tolerate the gap first.**
//!   [`tolerance_brief`]: never a re-run, never a content edit; code that
//!   fails a build or deploy on missing content degrades instead (the field
//!   left out, a warning logged), and the content step is listed for before
//!   merge.
//! - **After a round that changed no code, its verdict decides.**
//!   [`after_round`]: every CI fix round ends its account with what the
//!   failure needs ([`CAUSE_ASK`], read by [`cause_of`]). Content the test
//!   reads that isn't there ends the loop with that content named before
//!   merge, whether or not the change carries a step for it: a run never
//!   fails on a content gap it can't fill. The failure's own words
//!   ([`names_gap`]) are only the fallback when a round wrote no verdict.
//! - **Rounds that meet the same failure change approach.**
//!   [`repeat_brief`]: the next round is told what the last one concluded
//!   and must take a different hypothesis, or the tolerant-code route when
//!   the failure names content.

/// The run's `ci_end` when its checks wait on a content step only a person
/// (or a connection CoderHelm lacks) can do before merge.
pub const CI_END: &str = "awaits_content";

/// Whether a run's `ci_end` lets the run end completed with its checks red:
/// the failure isn't the change's (`ci_unrelated::not_ours`), or it waits
/// on a named content step before merge.
pub fn allows_done(ci_end: &str) -> bool {
    crate::ci_unrelated::not_ours(ci_end) || ci_end == CI_END
}

/// What a failure says is missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gap {
    /// The line that names it.
    pub line: String,
    /// The migration, script or command it says to run, when it names one.
    pub run: Option<String>,
}

const GAP_WORDS: &[&str] = &[
    "is missing from",
    "missing from the",
    "is incomplete in",
    "incomplete in this dataset",
    "in this dataset",
    "in the staging dataset",
    "on the staging dataset",
    "not in the dataset",
    "no documents",
    "content is missing",
    "missing content",
    "fixtures are missing",
    "fixture is missing",
    "snapshot is stale",
    "stale snapshot",
];

/// The first line of `text` that names a content gap: missing or
/// incomplete content, a dataset, fixtures, a stale snapshot, or a
/// migration it says to run.
pub fn names_gap(text: &str) -> Option<Gap> {
    for raw in text.lines() {
        let line = raw.trim();
        let low = line.to_lowercase();
        let says_migration = low.contains("migration")
            && (low.contains("run ") || low.contains("migrations/"))
            && !low.contains("migration failed");
        if says_migration || GAP_WORDS.iter().any(|w| low.contains(w)) {
            return Some(Gap {
                line: crate::truncate_str(line, 300).to_string(),
                run: run_named(line),
            });
        }
    }
    None
}

/// What a line says to run: a `migrations/...` path, the word before
/// "migration" after "run the", or the command after "run".
fn run_named(line: &str) -> Option<String> {
    let clean = |s: &str| {
        s.trim_matches(|c: char| {
            c == '`' || c == '"' || c == '\'' || c == '.' || c == ',' || c == ';' || c == ')'
        })
        .to_string()
    };
    if let Some(i) = line.find("migrations/") {
        let start = line[..i]
            .rfind(|c: char| c.is_whitespace() || c == '`' || c == '"')
            .map_or(0, |j| j + 1);
        let rest = &line[start..];
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '`' || c == '"' || c == ';')
            .unwrap_or(rest.len());
        let p = clean(&rest[..end]);
        if !p.is_empty() {
            return Some(p.trim_end_matches("/index.ts").to_string());
        }
    }
    let low = line.to_lowercase();
    if let Some(i) = low.find("run the ") {
        let after = &line[i + "run the ".len()..];
        let words: Vec<&str> = after.split_whitespace().collect();
        if words.len() >= 2 && words[1].to_lowercase().starts_with("migration") {
            return Some(clean(words[0]));
        }
    }
    if let Some(i) = low.find("run ") {
        let after = &line[i + 4..];
        let cmd = after.split(['(', ';']).next().unwrap_or("").trim();
        let cmd = clean(cmd);
        if !cmd.is_empty() && cmd.len() <= 120 {
            return Some(cmd);
        }
    }
    None
}

/// The short name of what a gap says to run, for matching a step: the last
/// path segment, else the first word that isn't a package manager's.
fn key(run: &str) -> String {
    let last = run.trim_end_matches('/').rsplit('/').next().unwrap_or(run);
    last.split_whitespace()
        .find(|w| {
            !matches!(
                *w,
                "pnpm" | "npm" | "npx" | "yarn" | "--filter" | "run" | "sanity" | "migration"
            )
        })
        .unwrap_or(last)
        .to_lowercase()
}

/// One of the change's outside steps, as the classifier needs it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Step {
    /// What the step does, its how and its migration file, as written.
    pub text: String,
    /// It writes content (a migration or a content change).
    pub writes_content: bool,
    pub done: bool,
    /// CoderHelm can't do it: a person's step, or its last try said it
    /// lacks the connection or permission.
    pub blocked: bool,
}

/// What the CI loop does with a failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Not a content gap: the round goes as it would.
    NotContent,
    /// A failing test waits on this not-done step CoderHelm can't do: no
    /// fix round, the run ends with the step before merge.
    AwaitsStep { gap: Gap, step: String },
    /// A content gap with no step CoderHelm is waiting on: the round makes
    /// the code tolerate it first ([`tolerance_brief`]).
    TolerateFirst(Gap),
}

/// Reads a failure (`failure`: the report's error and the job logs) against
/// the change's outside steps. `test_failed`: the check failed in a test
/// (its report named failing tests), not in a build or deploy step.
pub fn classify(failure: &str, test_failed: bool, steps: &[Step]) -> Verdict {
    let Some(gap) = names_gap(failure) else {
        return Verdict::NotContent;
    };
    if !test_failed {
        // A build or deploy that fails on missing content is the code's to
        // fix: it degrades instead.
        return Verdict::TolerateFirst(gap);
    }
    let want = gap.run.as_deref().map(key).filter(|k| k.len() >= 4);
    let waiting = steps
        .iter()
        .filter(|s| s.writes_content && !s.done && s.blocked);
    let found = match &want {
        Some(k) => waiting
            .clone()
            .find(|s| s.text.to_lowercase().contains(k.as_str())),
        None => None,
    }
    .or_else(|| {
        // No name to match: one waiting content step is the one.
        let all: Vec<&Step> = waiting.collect();
        (all.len() == 1 && want.is_none()).then(|| all[0])
    });
    match found {
        Some(s) => Verdict::AwaitsStep {
            gap,
            step: crate::truncate_str(s.text.trim(), 300).to_string(),
        },
        None => Verdict::TolerateFirst(gap),
    }
}

/// What a CI fix round found the failure needs, from the one line its
/// account ends with ([`CAUSE_ASK`]). The round reproduced the failure and
/// read the code; this is its verdict, a value, not words searched for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cause {
    /// The code is wrong: a code change fixes it.
    Code,
    /// The code is right and data it reads isn't there: what is missing,
    /// and where.
    Content(String),
    /// A change outside the repository (infrastructure, a setting).
    OutsideChange(String),
}

/// The line a CI fix round's account starts its verdict with.
pub const CAUSE_LINE: &str = "Cause:";

/// What every CI fix round is asked to end its account with.
pub const CAUSE_ASK: &str = "\n\nEnd your account with one line that says what the failure needs, \
     exactly one of:\n\
     `Cause: code` (you changed the code, or a code change fixes it),\n\
     `Cause: content: <the content that is missing and where, as the failing test reads it>` \
     (the code is right and the test reads CMS, dataset or fixture data that isn't there),\n\
     `Cause: outside_change: <what>` (infrastructure or a setting outside this repository).";

/// The round's verdict, from the last [`CAUSE_LINE`] its account wrote.
/// `None` when it wrote none.
pub fn cause_of(account: &str) -> Option<Cause> {
    let line = account
        .lines()
        .rev()
        .map(|l| l.trim().trim_matches('`').trim())
        .find(|l| l.starts_with(CAUSE_LINE))?;
    let rest = line[CAUSE_LINE.len()..].trim();
    let (kind, what) = rest.split_once(':').unwrap_or((rest, ""));
    let what = what.trim().trim_matches('`').trim().to_string();
    match kind.trim().to_ascii_lowercase().as_str() {
        "code" => Some(Cause::Code),
        "content" if !what.is_empty() => Some(Cause::Content(what)),
        "outside_change" if !what.is_empty() => Some(Cause::OutsideChange(what)),
        _ => None,
    }
}

/// What the CI loop does after a fix round that changed no code
/// (`changed_code` false), judged from what the round found missing
/// ([`cause_of`]), never from a list of words: a failing test that reads
/// content which isn't there ends the loop with that content named before
/// merge, whether or not the change carries a step that supplies it. A
/// waiting content step of the change's own is named when it is the one
/// (`steps`); otherwise the content itself is the step. A round that wrote
/// no verdict falls back to the failure's own words ([`names_gap`]).
pub fn after_round(
    account: &str,
    failure: &str,
    changed_code: bool,
    test_failed: bool,
    steps: &[Step],
) -> Verdict {
    if changed_code || !test_failed {
        return Verdict::NotContent;
    }
    let gap = match cause_of(account) {
        Some(Cause::Content(missing)) => Gap {
            run: run_named(&missing),
            line: crate::truncate_str(&missing, 300).to_string(),
        },
        Some(_) => return Verdict::NotContent,
        None => match names_gap(failure) {
            Some(g) => g,
            None => return Verdict::NotContent,
        },
    };
    let want = gap.run.as_deref().map(key).filter(|k| k.len() >= 4);
    let own = steps
        .iter()
        .filter(|s| s.writes_content && !s.done)
        .find(|s| {
            want.as_deref()
                .is_some_and(|k| s.text.to_lowercase().contains(k))
        })
        .or_else(|| {
            let open: Vec<&Step> = steps
                .iter()
                .filter(|s| s.writes_content && !s.done)
                .collect();
            (open.len() == 1).then(|| open[0])
        });
    let step = match own {
        Some(s) => crate::truncate_str(s.text.trim(), 300).to_string(),
        None => format!(
            "Add the content the test reads, in the environment it runs against: {}",
            gap.line.trim_end_matches('.')
        ),
    };
    Verdict::AwaitsStep { gap, step }
}

/// What a CI fix round is told when the failure names a content gap.
pub fn tolerance_brief(gap: &Gap) -> String {
    format!(
        "\n\nThis failure names missing content or data, not a code error: `{}`.\n\
         - Never re-run the check or retry the same code hoping the content appears, and never \
           edit CMS content or a dataset from here.\n\
         - First make the code tolerate it: code must not fail a build or deploy because content \
           is missing or incomplete. Leave the field or block out, log a warning naming what is \
           missing, and keep the page working. A throw, an assertion or a non-zero exit in build, \
           deploy or page code on CMS data is the bug to fix.\n\
         - A test the change adds that needs that content gets it from its own setup or \
           fixtures when the repository has them; otherwise it fails naming what is missing \
           (never a skip), and the content step is listed on the pull request for before merge.\n\
         - Say in your account which of these you did, and name the content step{} for before \
           merge.",
        gap.line,
        gap.run
            .as_deref()
            .map(|r| format!(" (`{r}`)"))
            .unwrap_or_default()
    )
}

/// What a CI fix round is told when the rounds before it met this same
/// failure (`repeats` times) without progress: what the last one
/// concluded, and that this one takes another approach.
pub fn repeat_brief(repeats: u32, last_account: &str, gap: Option<&Gap>) -> String {
    let last = last_account.trim();
    let mut out = format!(
        "\n\nThe same failure came back after {repeats} earlier CI fix round(s) with no \
         progress. Do not repeat their approach."
    );
    if !last.is_empty() {
        out.push_str(&format!(
            "\nThe last round concluded: {}",
            crate::truncate_str(&last.replace('\n', " "), 600)
        ));
    }
    match gap {
        Some(g) => out.push_str(&format!(
            "\nThe failure names content (`{}`): take the tolerant-code route. Make the code \
             degrade without that content, keep the test failing by name only if the content \
             step is listed for before merge, and change no content yourself.",
            g.line
        )),
        None => out.push_str(
            "\nTake a different hypothesis: re-read the failing test's own report and the \
             code it runs, name what the last round assumed, and show with a command's output \
             whether that assumption holds before you change anything.",
        ),
    }
    out
}

/// The pull request's section when the checks wait on a content step
/// before merge (its inner text; `ci_lines::failing_section` names the
/// head around it).
pub fn section(gap: &Gap, step: &str, check: &str) -> String {
    [
        "## Before merge: a content step".to_string(),
        format!(
            "`{check}` fails until this change's content step runs: `{}`.",
            gap.line
        ),
        format!("- The step: {step}"),
        "- CoderHelm can't run it here (no write connection to that dataset, or it is a \
         person's step), so it spent no CI fix round on it and changed no content."
            .to_string(),
        "- Run it, then re-run the check: it goes green with no code change. The code itself \
         leaves the content out while it is missing."
            .to_string(),
        "The pull request is ready for review. It isn't merged while this check fails.".to_string(),
    ]
    .join("\n")
}

/// The run's outcome line when its checks wait on a content step.
pub fn run_note(gap: &Gap, step: &str, check: &str) -> String {
    format!(
        "`{check}` fails only on content this change's own step supplies (`{}`). Before merge, \
         run: {step}. CoderHelm can't run it here, spent no CI fix round on it, and finished \
         the run with the pull request ready; re-run the check once the step is done.",
        gap.line
    )
}

/// The title of the review finding and checklist item for a throw on
/// content data.
pub const THROW_TITLE: &str = "A build or page that fails when CMS content is missing";

const CONTENT_WORDS: &[&str] = &[
    "sanity",
    "groq",
    "cms",
    "dataset",
    "contentful",
    "storyblok",
    "prismic",
    "strapi",
    "defineQuery",
    "client.fetch",
    "metadata",
    "slug",
    "_type",
    "document",
    "content",
];

const MISSING_WORDS: &[&str] = &[
    "missing",
    "incomplete",
    "not found",
    "is required",
    "required",
    "undefined",
    "empty",
    "no ",
    "not set",
    "migration",
];

fn is_test_path(path: &str) -> bool {
    let p = path.to_lowercase();
    p.contains("/test/")
        || p.contains("/tests/")
        || p.contains("__tests__")
        || p.starts_with("e2e/")
        || p.contains("/e2e/")
        || p.contains(".test.")
        || p.contains(".spec.")
        || p.contains("/migrations/")
        || p.contains("/scripts/")
}

fn is_code_path(path: &str) -> bool {
    [
        ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".py", ".rb", ".go",
    ]
    .iter()
    .any(|e| path.ends_with(e))
}

/// The lines a diff adds to app code (not tests, migrations or scripts)
/// that throw, assert or exit when content read from the CMS is missing or
/// incomplete, as "path: line". Such code fails a build, a deploy or a page
/// on content an editor or a migration hasn't supplied yet; it degrades
/// instead (the field left out, a warning logged).
pub fn content_throws(path: &str, patch: &str) -> Vec<String> {
    if is_test_path(path) || !is_code_path(path) {
        return Vec::new();
    }
    let lines: Vec<&str> = patch.lines().filter(|l| !l.starts_with("@@")).collect();
    let body = |l: &str| l.get(1..).unwrap_or("").to_string();
    let mut out = Vec::new();
    for (i, l) in lines.iter().enumerate() {
        if !(l.starts_with('+') && !l.starts_with("+++")) {
            continue;
        }
        let code = l[1..].trim();
        if code.starts_with("//") || code.starts_with('*') || code.starts_with('#') {
            continue;
        }
        let low = code.to_lowercase();
        let fails = low.starts_with("throw ")
            || low.contains(" throw new ")
            || low.contains("{ throw ")
            || low.starts_with("raise ")
            || low.contains("process.exit(1")
            || low.starts_with("assert(")
            || low.starts_with("invariant(");
        if !fails {
            continue;
        }
        let from = i.saturating_sub(5);
        let to = (i + 2).min(lines.len());
        let context: String = lines[from..to]
            .iter()
            .map(|x| body(x).to_lowercase())
            .collect::<Vec<_>>()
            .join(" ");
        let says_missing = MISSING_WORDS.iter().any(|w| context.contains(w));
        let on_content = CONTENT_WORDS
            .iter()
            .any(|w| context.contains(&w.to_lowercase()) || path.to_lowercase().contains("sanity"));
        if says_missing && on_content {
            out.push(format!("{path}: `{}`", crate::truncate_str(code, 140)));
        }
    }
    out
}

/// What the review finding and the checklist say about such lines.
pub fn throw_instruction(found: &[String]) -> String {
    format!(
        "New code fails when CMS content is missing or incomplete: {}. A build, deploy or page \
         must not fail because content isn't there yet. Leave the field or block out and log a \
         warning naming what is missing, and list the content migration as a step before merge \
         (or a follow-up).",
        found.join("; ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADDR: &str = "Error: libraryClub /library-a metadata.address is incomplete in this dataset; run apps/studio/migrations/fill-library-addresses";
    const FIX: &str = "Error: Tour date \"e2e-callout-expired\" is missing from the Tour Indexes: run the add-callout-e2e-fixtures migration on the staging dataset.";

    fn step(text: &str, blocked: bool) -> Step {
        Step {
            text: text.into(),
            writes_content: true,
            done: false,
            blocked,
        }
    }

    #[test]
    fn names_the_gap_and_what_to_run() {
        let g = names_gap(ADDR).unwrap();
        assert_eq!(
            g.run.as_deref(),
            Some("apps/studio/migrations/fill-library-addresses")
        );
        let g = names_gap(FIX).unwrap();
        assert_eq!(g.run.as_deref(), Some("add-callout-e2e-fixtures"));
        let g = names_gap("sitemap paths that answer 301: /x. The redirect-source snapshot is stale: run pnpm --filter web redirects:sync (docs)").unwrap();
        assert_eq!(g.run.as_deref(), Some("pnpm --filter web redirects:sync"));
        assert!(names_gap("TypeError: x is not a function").is_none());
        assert!(names_gap("Error: expect(received).toEqual(expected)").is_none());
    }

    #[test]
    fn a_test_waiting_on_a_blocked_step_awaits_it() {
        let steps = [
            step("Dry run, then run the migration on staging apps/studio/migrations/fill-library-addresses/index.ts", true),
            step("Tag the release", false),
        ];
        match classify(ADDR, true, &steps) {
            Verdict::AwaitsStep { step, .. } => assert!(step.contains("fill-library-addresses")),
            v => panic!("{v:?}"),
        }
        let steps = [step(
            "Run the fixture migration cd apps/studio && npx sanity migration run add-callout-e2e-fixtures --dataset staging",
            true,
        )];
        assert!(matches!(
            classify(FIX, true, &steps),
            Verdict::AwaitsStep { .. }
        ));
    }

    #[test]
    fn a_build_on_missing_content_is_the_codes_to_tolerate() {
        let steps = [step(
            "run apps/studio/migrations/fill-library-addresses",
            true,
        )];
        assert!(matches!(
            classify(ADDR, false, &steps),
            Verdict::TolerateFirst(_)
        ));
        // A step CoderHelm can still do, or one already done, is not waited on.
        let open = [step(
            "run apps/studio/migrations/fill-library-addresses",
            false,
        )];
        assert!(matches!(
            classify(ADDR, true, &open),
            Verdict::TolerateFirst(_)
        ));
        assert_eq!(classify("Error: boom", true, &steps), Verdict::NotContent);
        let b = tolerance_brief(&names_gap(ADDR).unwrap());
        assert!(b.contains("must not fail a build or deploy"));
        assert!(b.contains("Never re-run"));
    }

    #[test]
    fn repeats_change_approach() {
        let g = names_gap(ADDR);
        let b = repeat_brief(
            1,
            "I made no code change: staging lacks the content.",
            g.as_ref(),
        );
        assert!(b.contains("Do not repeat"));
        assert!(b.contains("tolerant-code route"));
        let b = repeat_brief(1, "", None);
        assert!(b.contains("different hypothesis"));
    }

    #[test]
    fn the_ending_allows_done_and_says_what_to_run() {
        assert!(allows_done(CI_END));
        assert!(allows_done("unrelated_test"));
        assert!(!allows_done("budget"));
        let g = names_gap(ADDR).unwrap();
        let s = section(&g, "run the migration on staging", "Deploy Staging");
        assert!(s.contains("Before merge"));
        assert!(s.contains("ready for review"));
        assert!(!s.contains("stays a draft"));
    }

    /// The round's verdict decides, whatever words the failure used, and a
    /// gap the change has no step for still ends the loop with the content
    /// named before merge.
    #[test]
    fn a_round_that_finds_content_missing_ends_the_loop_with_no_step_of_its_own() {
        let failure =
            "Error: Absent staging content: /blocks needs a Card whose image has alt text";
        // No word of the old list in it.
        assert!(names_gap(failure).is_none());
        let account = "I reproduced it. The code renders alt text when the CMS has it.\n\
                       Cause: content: /blocks needs a Card block whose image has alt text in staging";
        match after_round(account, failure, false, true, &[]) {
            Verdict::AwaitsStep { gap, step } => {
                assert!(gap.line.contains("Card block whose image has alt text"));
                assert!(step.starts_with("Add the content the test reads"), "{step}");
            }
            v => panic!("{v:?}"),
        }
        // A change's own waiting step is the one named.
        let steps = [step("Run the alt-text-cards migration on staging", false)];
        match after_round(
            "Cause: content: run the alt-text-cards migration",
            failure,
            false,
            true,
            &steps,
        ) {
            Verdict::AwaitsStep { step, .. } => assert!(step.contains("alt-text-cards")),
            v => panic!("{v:?}"),
        }
        // Code changed, a code verdict, or a build failure: not a content end.
        assert_eq!(
            after_round(account, failure, true, true, &[]),
            Verdict::NotContent
        );
        assert_eq!(
            after_round("Cause: code", failure, false, true, &[]),
            Verdict::NotContent
        );
        assert_eq!(
            after_round(account, failure, false, false, &[]),
            Verdict::NotContent
        );
        // No verdict written: the failure's own words are the fallback.
        assert!(matches!(
            after_round("no idea", ADDR, false, true, &[]),
            Verdict::AwaitsStep { .. }
        ));
        assert_eq!(cause_of("Cause: content:"), None);
        assert_eq!(cause_of("`Cause: code`"), Some(Cause::Code));
    }

    #[test]
    fn a_throw_on_missing_content_is_found_in_app_code_only() {
        let patch = "@@ -1,3 +1,8 @@\n+export async function clubJsonLd(slug: string) {\n+  const club = await client.fetch(queryClub, { slug });\n+  if (!club?.metadata?.address) {\n+    throw new Error(`club ${slug} address is missing; run the migration`);\n+  }\n";
        let found = content_throws("apps/web/src/lib/club-jsonld.ts", patch);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(throw_instruction(&found).contains("must not fail"));
        assert!(content_throws("e2e/tests/site/seo.spec.ts", patch).is_empty());
        assert!(content_throws("apps/studio/migrations/fill/index.ts", patch).is_empty());
        let plain = "@@\n+function parse(x: string) {\n+  if (x.length > 10) throw new Error('too long');\n+}\n";
        assert!(content_throws("apps/web/src/lib/parse.ts", plain).is_empty());
    }
}
