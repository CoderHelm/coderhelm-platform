//! The quality eval for everything CoderHelm writes into a pull request's
//! description or a ticket comment.
//!
//! Each case is a real output a senior engineer called bad, kept as a
//! fixture with every identifier made up: the text as CoderHelm wrote it,
//! the facts the pipeline had recorded, and the model reply the reader
//! pass got (recorded, so the eval is deterministic and makes no call). A
//! case passes only when, after the same function production runs
//! (`description_facts::shape`):
//!
//! - every failure the reviewer flagged is gone ([`Case::flagged`]) and
//!   what they needed is in sight ([`Case::in_sight`]);
//! - the rubric finds nothing (`pr_layout::rubric`): the layout's checklist
//!   (`pr_body::defects`: the fixed order, the word budget, at most three
//!   folds, the sha once, no "did not run" rows, no arrows or ellipses), no
//!   raw error, query, id or internal tool name in the visible text, and
//!   nothing that contradicts a recorded fact;
//! - the same checklist holds on the HTML a CommonMark renderer makes of it
//!   ([`rendered_defects`]): nothing swallowed into a list item, every
//!   collapsed block parsed, no heading in sight but the three;
//! - shaping the result again changes nothing.
//!
//! It runs with `cargo test --workspace`, which CI runs on every change, so
//! a change to description, comment or review-text code that brings a bad
//! output back fails here.
//!
//! # Adding a case
//!
//! When a new bad output is found:
//!
//! 1. Copy the text into a `const` here and replace every real name, id,
//!    key and URL with a made-up one.
//! 2. Build the facts the pipeline had for it (a `Facts`, or the `Inputs`
//!    `description_facts::facts` reads; an `OutsideChange` for steps
//!    outside the code).
//! 3. Add a function returning a [`Case`]: one entry in `flagged` for each
//!    thing the reviewer called wrong (a phrase that must be gone), one in
//!    `in_sight` for each thing they needed to see.
//! 4. Add it to [`cases`]. If it needs the reader pass, record the reply
//!    the model gave and put it in `reply`.
//! 5. Run `cargo test -p worker description_eval -- --nocapture` to read
//!    the rendered result.

use std::collections::HashMap;

use common::pr_body::{HumanItem, ItemKind, Merge, Screens};
use common::pr_facts::{
    Advisories, CheckFact, DepLine, DependencyFacts, Facts, FileChange, MajorEvidence, Outcome,
    Probe, Risk, Why,
};
use common::pr_layout::{self, Flaw};
use common::pr_steps::{Checked, State as StepState, Step, When};

use super::description_facts::{self, Inputs, Shaped};
use super::fix_location::{FixDecision, FixLocation, OutsideChange};
use super::outside_work::{Confirm, ContentLookup, ExistingItem, Labelled, NoteStep};
use super::reviewer_guide::{Check, State};

/// One recorded bad output and what a good one must be.
pub struct Case {
    pub name: &'static str,
    /// The description as CoderHelm wrote it.
    pub before: String,
    pub title: &'static str,
    pub facts: Facts,
    /// The reader pass's recorded reply, when the case has one.
    pub reply: Option<&'static str>,
    /// What the reviewer flagged: gone from the whole description.
    pub flagged: Vec<&'static str>,
    /// What the reader needed: in the part they see without opening
    /// anything.
    pub in_sight: Vec<&'static str>,
    /// What belongs below: present, and collapsed.
    pub collapsed: Vec<&'static str>,
}

impl Case {
    pub fn shaped(&self) -> Shaped {
        description_facts::shape(&self.before, self.title, self.facts.clone(), self.reply)
    }
}

fn passed(name: &str) -> Check {
    Check {
        name: name.into(),
        url: format!("https://ci.example.com/{}", name.replace(' ', "-")),
        state: State::Passed,
    }
}

// ── Case 1: a dependency pull request with a stale description ──────────

/// The description of a dependency pull request whose code was sound: five
/// statements wrong on the head, a blanket claim, and a major version jump
/// with nothing said about why it is safe.
pub const DEPENDENCY_BEFORE: &str = "## Summary\n\nFixes 31 advisories in 3 package version lines. 1 high advisory remains. The API contract is unchanged.\n\n| Package | Version | Advisories | Reachability | Due |\n| --- | --- | --- | --- | --- |\n| `glob-walk` | 2.1.0, not moved on the head | GHSA-1111-2222-3333 | Reachable | P1 Nov 20 |\n| `tiny-yaml` | 1.2.0 to 2.0.0 (major) | GHSA-4444-5555-6666 | Unknown | P2 Dec 1 |\n| `left-trim` | 0.9.1, not moved on the head | GHSA-aaaa-bbbb-cccc | Unknown | P2 Dec 1 |\n\n## Advisory\n\n- GHSA-1111-2222-3333 · CVSS 7.5\n- GHSA-4444-5555-6666 · CVSS 5.3\n- GHSA-aaaa-bbbb-cccc · CVSS 8.1\n\n## Change\n\nBumps the packages in the table above. GHSA-1111-2222-3333 is not fixed yet.\n\n## Proof\n\nCI passes on this pull request.\n\n<!-- coderhelm:unverified -->\n## Open items\n- The lockfile is out of sync with `package.json`; regenerate it.\n- `glob-walk` is still open.\n<!-- /coderhelm:unverified -->\n\n<!-- coderhelm:security -->";

/// What the lockfile on the head resolved for that pull request.
pub fn dependency_facts() -> Facts {
    let files = vec![
        (
            "package.json".to_string(),
            "modified".to_string(),
            String::new(),
        ),
        (
            "package-lock.json".to_string(),
            "modified".to_string(),
            String::new(),
        ),
    ];
    let floor = super::pr_risk::floor(&files, &HashMap::new());
    let checks = vec![
        passed("build"),
        passed("unit tests"),
        passed("frozen install"),
    ];
    description_facts::facts(&Inputs {
        head: "5e4d3c2b1a0f9e8d",
        body: DEPENDENCY_BEFORE,
        files: &files,
        floor: &floor,
        checks: Some(&checks),
        why: None,
        dependency: Some(DependencyFacts {
            lines: vec![
                DepLine {
                    package: "glob-walk".into(),
                    from: "2.1.0".into(),
                    to: "2.1.4".into(),
                    moved: true,
                    major: false,
                    remaining: vec![],
                    fixed: vec![],
                },
                DepLine {
                    package: "tiny-yaml".into(),
                    from: "1.2.0".into(),
                    to: "2.0.0".into(),
                    moved: true,
                    major: true,
                    remaining: vec![],
                    fixed: vec![],
                },
                DepLine {
                    package: "left-trim".into(),
                    from: "0.9.1".into(),
                    to: "0.9.1".into(),
                    moved: false,
                    major: false,
                    remaining: vec!["GHSA-aaaa-bbbb-cccc".into()],
                    fixed: vec![],
                },
            ],
            advisories: Advisories {
                fixed: vec!["GHSA-1111-2222-3333".into(), "GHSA-4444-5555-6666".into()],
                remaining: vec!["GHSA-aaaa-bbbb-cccc".into(), "GHSA-dddd-eeee-ffff".into()],
                introduced: vec![],
                remaining_by_severity: [("high".to_string(), 2usize)].into_iter().collect(),
            },
            ..Default::default()
        }),
        outside: None,
        run_url: "https://app.example.com/runs/detail?id=run-dep",
        notes: vec![],
        evidence: vec![],
        repo_url: "https://github.com/acme/site",
    })
}

pub fn dependency_case() -> Case {
    Case {
        name: "a dependency pull request with a stale description",
        before: DEPENDENCY_BEFORE.to_string(),
        title: "fix(deps): security updates for web (3 packages, 4 advisories)",
        facts: dependency_facts(),
        reply: None,
        flagged: vec![
            // The five statements wrong on the head.
            "| `glob-walk` | 2.1.0, not moved on the head",
            "The lockfile is out of sync",
            "GHSA-1111-2222-3333 is not fixed yet",
            "1 high advisory remains",
            "`glob-walk` is still open",
            // A blanket claim with nothing behind it, and a wrong total.
            "contract is unchanged",
            "Fixes 31 advisories",
            // Nothing is needed: the section is not there at all.
            "### Needed from you",
        ],
        in_sight: vec![
            // Why each major jump is safe, or that nothing says so.
            "| `tiny-yaml` | 1.2.0 to 2.0.0 | Major; no safety evidence is recorded: read its upgrade notes |",
            "| `left-trim` | 0.9.1 | Security: GHSA-aaaa-bbbb-cccc still open. Not moved on the head |",
            "Updates 2 packages: 1 major, 1 patch. Fixes 2 security advisories; 2 stay open (2 high). 1 line the plan named did not move on the head.",
            "On 5e4d3c2 (head of this PR):",
            "| build (CI) | Passed |",
        ],
        collapsed: vec![
            "| `glob-walk` | 2.1.0 to 2.1.4 |",
            // A line that really did not move keeps saying so.
            "| `left-trim` | 0.9.1, not moved on the head |",
            "- GHSA-aaaa-bbbb-cccc · CVSS 8.1",
        ],
    }
}

// ── Case 2: a small feature with an unreadable outside-steps section ────

/// The body of a three-file feature's pull request as it was written: the
/// outside steps under two headings, a raw error twice, found facts and a
/// query already run listed as items with owners.
pub const FEATURE_BEFORE: &str = "<!-- coderhelm:not-done:start -->\n**Not done: this ticket still needs access CoderHelm doesn't have**\n\n- Deploy the staging Studio after merge How: cd apps/studio && npm run deploy:staging. CoderHelm could not do it: not done: starting `Test Suite` (.github/workflows/test.yml, started with workflow_dispatch) failed: HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/acme/site/actions/workflows/test.yml/dispatches): {\"message\":\"Resource not accessible by integration\",\"documentation_url\":\"https://docs.github.com/rest\",\"status\":\"403\"}.\n<!-- coderhelm:not-done:end -->\n\nSource ticket: WEB-88\n\n## Summary\n\nAdds a non-blocking length warning to the page title field in the Studio.\n\n## Background\n\nNew work: the ticket asks for a warning when a page title is longer than 60 characters. The PR holds the code; the outside part is the Studio deploys, which CoderHelm runs so editors see the warning.\n\n## Changes\n\n* `apps/studio/schemas/page.ts`: the warning rule.\n* `apps/studio/schemas/page.test.ts`: its unit test.\n\n<!-- coderhelm:not-in-pr -->\n## Not in this PR\n* Names: CMS project id `p1x2y3z4`, dataset `production`, dataset `staging` (owner: found by CoderHelm)\n* Looked up `count(*[_type == \"page\" && length(title) > 60])` at https://p1x2y3z4.api.example-cms.io/v2021-10-21/data/query/production: [{\"n\":392}] (owner: found by CoderHelm)\n* Already there: `page-home` (key `k9`), \"Home\"; left by Editor content, not left by any PR. Its content is replaced, not added to. (owner: found by CoderHelm)\n* 1. Deploy the staging Studio after merge How: cd apps/studio && npm run deploy:staging. CoderHelm could not do it: not done: starting `Test Suite` (.github/workflows/test.yml, started with workflow_dispatch) failed: HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/acme/site/actions/workflows/test.yml/dispatches): {\"message\":\"Resource not accessible by integration\",\"documentation_url\":\"https://docs.github.com/rest\",\"status\":\"403\"}. (owner: CoderHelm)\n* 2. Deploy the production Studio (with approval) How: cd apps/studio && npm run deploy:production. CoderHelm does this after the pull request opens. (owner: CoderHelm)\n* 3. Confirm the count of long titles How: query_content on production with the confirm query. CoderHelm does this after the pull request opens. (owner: CoderHelm)\n* How to confirm: query `count(*[_type == \"page\" && length(title) > 60])`, expect exactly one item, n = 392; look at Studio: the page document, the title field; `none, because the website doesn't change. The unit test page.test.ts must pass` must run and pass, not skip. CoderHelm ran the query: [{\"n\":392}] (owner: the owner of the last step)\n<!-- /coderhelm:not-in-pr -->\n\n## Risk\n\n**Low**, the rule only warns.\n\n## Verification\n\n1. Run `npm test --workspace apps/studio`.\n";

const RAW_403: &str = "not done: starting `Test Suite` (.github/workflows/test.yml, started with workflow_dispatch) failed: HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/acme/site/actions/workflows/test.yml/dispatches): {\"message\":\"Resource not accessible by integration\",\"documentation_url\":\"https://docs.github.com/rest\",\"status\":\"403\"}";

/// The outside change the run had recorded for that pull request.
pub fn feature_outside() -> OutsideChange {
    OutsideChange {
        system: "the CMS Studio".into(),
        change: "Deploy the Studio so editors see the warning".into(),
        names: vec![
            Labelled {
                label: "CMS project id".into(),
                value: "p1x2y3z4".into(),
            },
            Labelled {
                label: "dataset".into(),
                value: "production".into(),
            },
        ],
        lookup: Some(ContentLookup {
            endpoint: "https://p1x2y3z4.api.example-cms.io/v2021-10-21/data/query/production"
                .into(),
            query: "count(*[_type == \"page\" && length(title) > 60])".into(),
            result: "[{\"n\":392}]".into(),
            error: String::new(),
        }),
        existing_items: vec![ExistingItem {
            id: "page-home".into(),
            key: "k9".into(),
            title: "Home".into(),
            left_by: "Editor content, not left by any PR".into(),
            ..Default::default()
        }],
        steps: vec![
            NoteStep {
                step: "Deploy the staging Studio after merge".into(),
                by: "coderhelm".into(),
                how: "cd apps/studio && npm run deploy:staging".into(),
                kind: "deploy".into(),
                target: "staging".into(),
                result: RAW_403.into(),
                ..Default::default()
            },
            NoteStep {
                step: "Deploy the production Studio after merge".into(),
                by: "coderhelm".into(),
                how: "cd apps/studio && npm run deploy:production".into(),
                kind: "deploy".into(),
                target: "production".into(),
                ..Default::default()
            },
            NoteStep {
                step: "Confirm the count of long titles".into(),
                by: "coderhelm".into(),
                how: "query_content on production with the confirm query".into(),
                kind: "verify".into(),
                target: "production".into(),
                ..Default::default()
            },
        ],
        confirm: Some(Confirm {
            query: "count(*[_type == \"page\" && length(title) > 60])".into(),
            expect: "exactly one item, n = 392".into(),
            page: "Studio: the page document, the title field".into(),
            e2e: "none, because the website doesn't change. The unit test page.test.ts must pass"
                .into(),
            result: "[{\"n\":392}] (on the `production` dataset)".into(),
        }),
        ..Default::default()
    }
}

/// The body as the pipeline has it once the steps' section is rendered
/// from their states (what `outside_exec::refresh_pr` does), before it is
/// shaped.
pub fn feature_body(c: &OutsideChange) -> String {
    let d = FixDecision {
        fix_location: FixLocation::Both,
        outside_change: Some(c.clone()),
        ..Default::default()
    };
    let (mut owned, rest) = super::completeness::take_owned(FEATURE_BEFORE);
    owned.not_in_pr = super::completeness::for_people_section(
        &super::fix_location::not_in_pr_steps(Some(&d), "acme/site"),
        &d.questions,
    );
    super::outside_work::with_not_done_top(&super::completeness::assemble(&rest, &owned), c)
}

pub fn feature_facts(c: &OutsideChange, body: &str) -> Facts {
    let files = vec![
        (
            "apps/studio/schemas/page.ts".to_string(),
            "modified".to_string(),
            "@@ -1 +1,4 @@\n+warn\n".to_string(),
        ),
        (
            "apps/studio/schemas/page.test.ts".to_string(),
            "added".to_string(),
            "@@ -0,0 +1,9 @@\n+test\n".to_string(),
        ),
        (
            "apps/studio/package.json".to_string(),
            "modified".to_string(),
            "@@ -1 +1 @@\n+x\n".to_string(),
        ),
    ];
    let floor = super::pr_risk::floor(&files, &HashMap::new());
    let checks = vec![passed("Test Suite"), passed("lint")];
    description_facts::facts(&Inputs {
        head: "9f8e7d6c5b4a3f2e",
        body,
        files: &files,
        floor: &floor,
        checks: Some(&checks),
        why: Some(Why {
            reference: "WEB-88".into(),
            title: "Warn when a page title is too long".into(),
            url: String::new(),
        }),
        dependency: None,
        outside: Some(c),
        run_url: "https://app.example.com/runs/detail?id=run-web",
        notes: vec![],
        evidence: vec![],
        repo_url: "https://github.com/acme/site",
    })
}

/// The reply the reader pass got for the feature's description.
pub const FEATURE_REPLY: &str = r#"{"summary": "Adds a non-blocking length warning to the page title field in the Studio.", "check": "npm test --workspace apps/studio", "drop": ["Background"]}"#;

pub fn feature_case() -> Case {
    let c = feature_outside();
    let body = feature_body(&c);
    Case {
        name: "a small feature with an unreadable outside-steps section",
        facts: feature_facts(&c, &body),
        before: body,
        title: "WEB-88: Warn when a page title is too long",
        reply: Some(FEATURE_REPLY),
        flagged: vec![
            // One state per step: never "needs access" beside "owner: CoderHelm".
            "needs access CoderHelm doesn't have",
            "CoderHelm could not do it",
            // Re-evaluated on every build.
            "after the pull request opens",
            // Found facts and a check already made are not items.
            "(owner: found by CoderHelm)",
            "the owner of the last step",
            "How to confirm",
            // Fragments and a stray sentence rendered as a command.
            "not done: not done",
            "`none, because",
            // The ticket restated.
            "New work: the ticket asks",
            // How to let CoderHelm is for its settings, not the team's PR.
            "To let it:",
            "(the reviewer)",
            "(a repository admin)",
        ],
        in_sight: vec![
            "### Needed from you\n- [ ] <!-- ch:deploy-the-staging-studio --> **Repository admin**, after merge: Deploy the staging Studio. Why a person: CoderHelm's GitHub App is not allowed to start workflows in this repository.\n  ```sh\n  cd apps/studio && npm run deploy:staging\n  ```",
            // Looking at the change is review work.
            "- See it: Studio: the page document, the title field.",
            "| Test Suite (CI) | Passed |",
        ],
        collapsed: vec![
            // Waiting on a named event, with who approves and where.
            "Deploy the production Studio. CoderHelm does this once step 1 is done and a team admin has approved it on the Agent permissions page.",
            // What CoderHelm could check, it checked and reports.
            "- Checked: the content query returns exactly one item, n = 392, it does.",
            "- Checked: the tests the change names",
            // Raw evidence, below.
            "Confirm query `count(*[_type == \"page\" && length(title) > 60])` returned [{\"n\":392}]",
            "Names: CMS project id `p1x2y3z4`",
        ],
    }
}

// ── Case 3: an ordinary ticket's pull request ───────────────────────────

pub const TICKET_BEFORE: &str = "Source ticket: OPS-41\n\n1. **Summary** — The export poller retries a timed-out poll three times before it gives up.\n\n2. **Changes**\n\n* `src/export/poller.rs`: the retry loop and its limit.\n* `src/export/writer.rs`: flushes after each batch.\n\n3. **Risk** — **Low**, contract unchanged, one module.\n\n4. **Verification**\n\n1. Run `cargo test -p export`.\n\nAll tests pass and the build is green.\n\n<!-- coderhelm:reviewer-guide -->\n**For the reviewer**\n* **Roll back:** revert this pull request.\n<!-- /coderhelm:reviewer-guide -->\n\n<sub>Consulted: the platform agent</sub>";

pub fn ticket_case() -> Case {
    let files = vec![(
        "src/export/poller.rs".to_string(),
        "modified".to_string(),
        "@@ -1 +1,3 @@\n+retry\n".to_string(),
    )];
    let floor = super::pr_risk::floor(&files, &HashMap::new());
    let checks = vec![
        passed("lint"),
        Check {
            name: "unit tests".into(),
            url: "https://ci.example.com/unit-tests".into(),
            state: State::Failed,
        },
    ];
    Case {
        name: "an ordinary ticket's pull request",
        before: TICKET_BEFORE.to_string(),
        title: "OPS-41: Export poller gives up on the first timeout",
        facts: description_facts::facts(&Inputs {
            head: "abc1234def567890",
            body: TICKET_BEFORE,
            files: &files,
            floor: &floor,
            checks: Some(&checks),
            why: Some(Why {
                reference: "OPS-41".into(),
                title: "Export poller gives up on the first timeout".into(),
                url: String::new(),
            }),
            dependency: None,
            outside: None,
            run_url: "https://app.example.com/runs/detail?id=run-ops",
            notes: vec![],
            evidence: vec![],
            repo_url: "https://github.com/acme/site",
        }),
        reply: Some(
            r#"{"summary": "Retries a timed-out export poll three times before giving up. All tests pass.", "check": "make ship", "drop": []}"#,
        ),
        flagged: vec![
            // A change the diff does not contain.
            "src/export/writer.rs",
            // A check no record shows passing (the suite failed).
            "All tests pass",
            // A blanket word with nothing behind it.
            "contract unchanged",
            // A command the description never gave.
            "make ship",
            "### Needed from you",
            "**Check:**",
        ],
        in_sight: vec![
            "Retries a timed-out export poll three times before giving up (OPS-41).",
            "| unit tests (CI) | Failed: [log](https://ci.example.com/unit-tests) |",
            "On abc1234 (head of this PR):",
        ],
        collapsed: vec![
            "* `src/export/poller.rs`: the retry loop and its limit.",
            "* **Roll back:** revert this pull request.",
            "How to check: `cargo test -p export`.",
            "Source ticket: OPS-41",
        ],
    }
}

// ── The layout's filled examples, as golden files ────────────────────────

fn file(path: &str, status: &str, changed: usize) -> FileChange {
    FileChange {
        path: path.into(),
        status: status.into(),
        changed,
        ..Default::default()
    }
}

fn ci(name: &str, outcome: Outcome) -> CheckFact {
    CheckFact {
        name: name.into(),
        outcome,
        url: format!("https://ci.example.com/{}", name.replace(' ', "-")),
    }
}

/// The written walkthrough of the UI change: what the model wrote.
pub const UI_WRITTEN: &str = "## Implementation notes\n\n- `hero-block-validation.ts`: a `Rule.custom` that returns a warning (not an error) above 160 characters, so publishing is never blocked.\n- `hero-block.ts` and `schemaTypes/index.ts`: register the rule on the Default variant only; other Hero variants have their own layouts.\n- Rejected: `Rule.max(160)`, which is an error and would block 23 published documents from being re-saved.\n";

/// Example A of the layout: a small UI validation change with one thing a
/// person must do after merge.
pub fn ui_facts() -> Facts {
    Facts {
        head: "d78bb24e1f0a9c3b5d7e2f4a6c8b0d1e3f5a7c9b".into(),
        summary: "Adds a Studio warning when a Hero (Default) subtitle is longer than 160 characters, so editors fix long subtitles before they break the hero layout. Editors can still publish.".into(),
        why: Some(Why {
            reference: "KEY-123".into(),
            title: "Hero subtitle character limit".into(),
            url: "https://tracker.example.com/browse/KEY-123".into(),
        }),
        files: vec![
            file("apps/studio/schemaTypes/blocks/hero-block-validation.ts", "added", 21),
            file("apps/studio/schemaTypes/blocks/hero-block.ts", "modified", 4),
            file("apps/studio/schemaTypes/index.ts", "modified", 2),
        ],
        files_complete: true,
        checks: vec![
            ci("Lint and typecheck", Outcome::Passed),
            ci("Unit tests", Outcome::Passed),
            ci("Studio build", Outcome::Passed),
            // A deploy workflow's job, skipped on a pull request: never listed.
            ci("Deploy to Production", Outcome::DidNotRun),
        ],
        checks_read: true,
        risk: Some(Risk {
            level: "Low".into(),
            why: "warning-level validation only; no schema, data, or publish-flow change".into(),
            look_first: Some((
                "apps/studio/schemaTypes/blocks/hero-block-validation.ts".into(),
                "the rule, +21 lines".into(),
            )),
            rollback: "revert this PR and redeploy Studio; nothing to undo in data".into(),
        }),
        needed: vec![HumanItem {
            id: "deploy-staging".into(),
            kind: ItemKind::Run,
            owner: "@studio-owner".into(),
            merge: Merge::After,
            action: "Deploy Studio to staging so editors see the warning".into(),
            why: "the repo has no deploy workflow CoderHelm can start".into(),
            command: "cd apps/studio && npm run deploy:staging".into(),
            ..Default::default()
        }],
        steps: vec![
            Step {
                what: "Post the 23 documents already over the limit on KEY-123".into(),
                how: String::new(),
                when: When::Any,
                owner: "CoderHelm".into(),
                state: StepState::Done {
                    evidence: "posted on the ticket for the content team".into(),
                },
            },
            Step {
                what: "Look at document `/private-events/weddings`, Hero, Sub Title field".into(),
                how: String::new(),
                when: When::Any,
                owner: "the reviewer".into(),
                state: StepState::Person {
                    why: "how a page looks is a judgement only a person can make".into(),
                    unblock: String::new(),
                },
            },
        ],
        checked: vec![Checked {
            what: "Studio in local dev, manual".into(),
            result: "warning shows at 161 characters; Publish stays enabled".into(),
            when: String::new(),
        }],
        how_to_check: "`npm test -- hero-block-validation`".into(),
        review_question: "Approach: is a non-blocking warning what KEY-123 asks for, or should over-limit subtitles block publishing?".into(),
        preview_url: "https://preview.example.com/studio".into(),
        screens: Some(Screens::Images {
            before: "https://github.com/user-attachments/assets/before-example".into(),
            before_alt: "Sub Title field with a 190-character subtitle and no warning".into(),
            after: "https://github.com/user-attachments/assets/after-example".into(),
            after_alt: "Same field showing the warning 'Over 160 characters (190)'".into(),
        }),
        run_url: "https://app.example.com/runs/detail?id=run-4812".into(),
        repo_url: "https://github.com/acme/site".into(),
        ..Default::default()
    }
}

/// Example B of the layout: 48 npm packages, three of them major.
pub fn dependency_48_facts() -> Facts {
    let mut lines = vec![
        DepLine {
            package: "next".into(),
            from: "14.2.15".into(),
            to: "15.1.0".into(),
            moved: true,
            major: true,
            ..Default::default()
        },
        DepLine {
            package: "eslint".into(),
            from: "8.57.0".into(),
            to: "9.17.0".into(),
            moved: true,
            major: true,
            ..Default::default()
        },
        DepLine {
            package: "@types/node".into(),
            from: "20.17.6".into(),
            to: "22.10.2".into(),
            moved: true,
            major: true,
            ..Default::default()
        },
        DepLine {
            package: "cookie-jar".into(),
            from: "0.6.0".into(),
            to: "0.7.2".into(),
            moved: true,
            fixed: vec!["GHSA-aaaa-bbbb-0001".into()],
            ..Default::default()
        },
        DepLine {
            package: "route-match".into(),
            from: "6.2.2".into(),
            to: "6.3.0".into(),
            moved: true,
            fixed: vec!["GHSA-aaaa-bbbb-0002".into()],
            ..Default::default()
        },
    ];
    // Seven more minor updates and thirty-six patches.
    for i in 0..43 {
        let minor = i < 7;
        lines.push(DepLine {
            package: format!("util-{i:02}"),
            from: "1.4.2".into(),
            to: if minor {
                "1.5.0".into()
            } else {
                "1.4.3".into()
            },
            moved: true,
            ..Default::default()
        });
    }
    let majors = vec![
        MajorEvidence {
            package: "next".into(),
            from: "14.2.15".into(),
            to: "15.1.0".into(),
            evidence: vec!["`fetch` is no longer cached by default; restored with explicit `cache: \"force-cache\"` in 2 routes".into()],
        },
        MajorEvidence {
            package: "eslint".into(),
            from: "8.57.0".into(),
            to: "9.17.0".into(),
            evidence: vec!["`.eslintrc` replaced with `eslint.config.mjs`; same rules".into()],
        },
        MajorEvidence {
            package: "@types/node".into(),
            from: "20.17.6".into(),
            to: "22.10.2".into(),
            evidence: vec!["types only; matches the Node 22 runtime already in use".into()],
        },
    ];
    Facts {
        head: "4be19a07c1d2e3f405162738495a6b7c8d9e0f1a".into(),
        why: Some(Why {
            reference: "KEY-456".into(),
            title: "Monthly dependency maintenance".into(),
            url: "https://tracker.example.com/browse/KEY-456".into(),
        }),
        files: vec![
            file("package.json", "modified", 96),
            file("package-lock.json", "modified", 2400),
            file("next.config.ts", "modified", 12),
            file("eslint.config.mjs", "added", 40),
            file(".eslintrc.json", "removed", 30),
            file("app/products/page.ts", "modified", 2),
        ],
        files_complete: true,
        checks: vec![
            ci("Install from lockfile, npm ci", Outcome::Passed),
            ci("Lint", Outcome::Passed),
            ci("Typecheck", Outcome::Passed),
            ci("Unit tests (1/3)", Outcome::Passed),
            ci("Unit tests (2/3)", Outcome::Passed),
            ci("Unit tests (3/3)", Outcome::Passed),
            ci("Build", Outcome::Passed),
            ci("E2E on preview", Outcome::Passed),
            ci("Deploy preview to production", Outcome::DidNotRun),
        ],
        checks_read: true,
        risk: Some(Risk {
            level: "Medium".into(),
            why: "Next 15 changes request caching across all pages; a missed caching default would surface as stale or uncached data, not a crash".into(),
            look_first: Some((
                "next.config.ts".into(),
                "caching defaults changed in Next 15".into(),
            )),
            rollback: "revert this PR; the lockfile restores every version. No data or config outside the repo changes".into(),
        }),
        dependency: Some(DependencyFacts {
            lines,
            advisories: Advisories {
                fixed: vec!["GHSA-aaaa-bbbb-0001".into(), "GHSA-aaaa-bbbb-0002".into()],
                ..Default::default()
            },
            majors,
            probes: vec![Probe {
                what: "Cached routes `/products` and `/blog`".into(),
                before: "cache HIT on second request".into(),
                after: "cache HIT on second request".into(),
            }],
            ecosystem: "npm".into(),
            ..Default::default()
        }),
        run_url: "https://app.example.com/runs/detail?id=run-4907".into(),
        repo_url: "https://github.com/acme/site".into(),
        ..Default::default()
    }
}

/// What the run wrote for the dependency update.
pub const DEPENDENCY_48_WRITTEN: &str = "## Changes\n\n### next 14.2.15 to 15.1.0\n\n- `next.config.ts`: explicit `cache: \"force-cache\"` on the 2 routes that relied on the old default.\n\n### eslint 8.57.0 to 9.17.0\n\n- `eslint.config.mjs` replaces `.eslintrc.json` with the same rules.\n";

/// The description as the layout before this one wrote it for example A:
/// a lead of bold labels, a numbered list that swallowed the lines after
/// it, workflows that never run on a pull request listed as not run, and
/// seven folds.
pub const OLD_LAYOUT_BEFORE: &str = "<!-- coderhelm:lead -->\nAdds a Studio warning when a Hero (Default) subtitle is longer than 160 characters.\n\n**Check:** `cd apps/studio && npm run deploy:staging`; 6 recorded checks passed on `d78bb24`.\n**Risk:** **Low**.\n**Needed from you:**\n1. After merging: Deploy the staging Studio: run `cd apps/studio && npm run deploy:staging`. CoderHelm could not: CoderHelm's GitHub App is not allowed to start workflows in this repository. To let it: add `workflow_dispatch` to the deploy workflow. (a person on the team)\n2. Look at Studio: /private-events/weddings, hero Sub Title field. (the reviewer)\n3. Missing from the end-to-end checklist: the documents already over the limit. Add a checklist step owned by CoderHelm to query and post them. (a person)\n   **Review:** decide whether this does what KEY-123 asks. Feedback wanted: is a warning enough.\n   **Look at first:**\n- `Deploy to Production` did not run on `d78bb24`. What it checks is not verified.\n- `Deploy Preview` did not run on `d78bb24`. What it checks is not verified.\n- and 6 more: [the full list is on the run page](https://app.example.com/runs/detail?id=run-4812).\n<!-- /coderhelm:lead -->\n\nSource ticket: KEY-123\n\n<details><summary><a name=\"coderhelm-description\"></a>Description</summary><!-- fold -->\n\nAdds the warning rule.\n\n</details><!-- fold -->\n\n<details><summary><a name=\"coderhelm-more\"></a>More</summary><!-- fold -->\n\n## Implementation notes\n\n- `hero-block-validation.ts`: a `Rule.custom` that returns a warning (not an error) above 160 characters, so publishing is never blocked.\n\n</details><!-- fold -->";

/// Where the golden files are.
fn golden_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/passes/description_goldens")
        .join(format!("{name}.md"))
}

/// The examples, shaped by the same function production runs: `(name,
/// title, body, facts)`.
pub fn golden_cases() -> Vec<(&'static str, &'static str, String, Facts)> {
    vec![
        (
            "ui_change",
            "feat(studio): warn when Hero subtitle exceeds 160 characters",
            UI_WRITTEN.to_string(),
            ui_facts(),
        ),
        (
            "dependency_48",
            "chore(deps): update 48 npm packages (3 major)",
            DEPENDENCY_48_WRITTEN.to_string(),
            dependency_48_facts(),
        ),
        (
            "before_rewritten",
            "KEY-123: Warn when the Hero subtitle exceeds 160 characters",
            OLD_LAYOUT_BEFORE.to_string(),
            Facts {
                needed: vec![],
                steps: vec![
                    Step {
                        what: "Deploy the staging Studio".into(),
                        how: "cd apps/studio && npm run deploy:staging".into(),
                        when: When::AfterMerge,
                        owner: "Studio owner".into(),
                        state: StepState::Failed {
                            cause: "the repo has no deploy workflow CoderHelm can start".into(),
                            unblock: "add `workflow_dispatch` to the deploy workflow".into(),
                        },
                    },
                    ui_facts().steps[1].clone(),
                    ui_facts().steps[0].clone(),
                ],
                review_question: String::new(),
                screens: None,
                preview_url: String::new(),
                ..ui_facts()
            },
        ),
    ]
}

/// Every case of the eval.
pub fn cases() -> Vec<Case> {
    vec![dependency_case(), feature_case(), ticket_case()]
}

/// What one case's result fails on, in words; empty when it passes.
pub fn failures(case: &Case, out: &Shaped) -> Vec<String> {
    let mut why = Vec::new();
    let seen = pr_layout::visible(&out.body);
    for f in &case.flagged {
        if out.body.contains(f) {
            why.push(format!("still says: {f}"));
        }
    }
    for s in &case.in_sight {
        if !seen.contains(s) {
            why.push(format!("not in sight: {s}"));
        }
    }
    for c in &case.collapsed {
        if !out.body.contains(c) {
            why.push(format!("missing: {c}"));
        } else if seen.contains(c) {
            why.push(format!("in sight, belongs below: {c}"));
        }
    }
    for flaw in pr_layout::rubric(&out.body, &out.facts) {
        why.push(match flaw {
            Flaw::Layout(d) => format!("layout: {d}"),
            Flaw::Machinery(m) => format!("raw machinery in sight: {m}"),
            Flaw::Contradiction(s) => format!("contradicts a recorded fact: {s}"),
        });
    }
    for d in rendered_defects(&out.body) {
        why.push(format!("as GitHub renders it: {d}"));
    }
    let again = description_facts::shape(&out.body, case.title, case.facts.clone(), None);
    if again.body != out.body {
        why.push("shaping it again changes it".to_string());
    }
    why
}

/// The HTML a CommonMark (GFM) renderer makes of `body`.
pub fn rendered(body: &str) -> String {
    use pulldown_cmark::{html, Options, Parser};
    let opts = Options::ENABLE_TABLES | Options::ENABLE_TASKLISTS | Options::ENABLE_STRIKETHROUGH;
    let mut out = String::new();
    html::push_html(&mut out, Parser::new_ext(body, opts));
    out
}

/// `html` without its comments.
fn without_comments(html: &str) -> String {
    let mut t = html.to_string();
    while let (Some(a), Some(b)) = (t.find("<!--"), t.find("-->")) {
        if b < a {
            break;
        }
        t.replace_range(a..b + 3, "");
    }
    t
}

/// Each `<tag ...>...</tag>` of `html`, outermost first (no nesting of the
/// same tag is expected in a description).
fn elements<'a>(html: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(a) = html[at..].find(&open) {
        let a = at + a;
        let Some(b) = html[a..].find(&close) else {
            break;
        };
        out.push(&html[a..a + b + close.len()]);
        at = a + b + close.len();
    }
    out
}

/// The layout's checklist on the rendered structure: what a reader of the
/// pull request page gets, not what the source says.
pub fn rendered_defects(body: &str) -> Vec<String> {
    let html = without_comments(&rendered(body));
    let mut d = Vec::new();
    let folds = elements(&html, "details");
    if folds.len() > common::pr_body::MAX_FOLDS {
        d.push(format!("{} <details>", folds.len()));
    }
    for f in &folds {
        let inner = f.split("</summary>").nth(1).unwrap_or("");
        if !["<p>", "<ul>", "<ol>", "<table>", "<h", "<pre>"]
            .iter()
            .any(|t| inner.contains(t))
        {
            d.push(format!(
                "a <details> whose content was not parsed: {}",
                common::truncate_str(f, 120)
            ));
        }
        let label = f
            .split("<summary>")
            .nth(1)
            .and_then(|x| x.split("</summary>").next())
            .unwrap_or("");
        if label.contains("**") || label.contains('`') {
            d.push(format!("markup shown raw in a summary: {label}"));
        }
        for raw in ["**", "|---", "| ---"] {
            if inner
                .split('<')
                .filter(|seg| !seg.starts_with("code") && !seg.starts_with("pre"))
                .any(|seg| seg.contains(raw))
            {
                d.push(format!("raw `{raw}` inside a <details>"));
            }
        }
    }
    let mut visible = html.clone();
    for f in &folds {
        visible = visible.replacen(f, "", 1);
    }
    let start = visible.trim_start();
    let opens_with_summary = start.starts_with("<p>")
        || (start.starts_with("<blockquote>")
            && start
                .split_once("</blockquote>")
                .is_some_and(|(_, rest)| rest.trim_start().starts_with("<p>")));
    if !opens_with_summary {
        d.push(format!(
            "the first block is not the Summary: {}",
            common::truncate_str(start, 80)
        ));
    }
    let allowed = ["Needed from you", "Review focus", "Verification"];
    for h in ["h1", "h2", "h3"] {
        for e in elements(&visible, h) {
            let text = e
                .split_once('>')
                .map(|(_, r)| r.trim_end_matches(&format!("</{h}>")))
                .unwrap_or("");
            if h != "h3" || !allowed.contains(&text) {
                d.push(format!("a heading in sight: {e}"));
            }
        }
    }
    for li in elements(&visible, "li") {
        for label in [
            "Review",
            "Look at",
            "Risk",
            "Rollback",
            "Verification",
            "Feedback",
        ] {
            if li.contains(&format!("<strong>{label}")) {
                d.push(format!("a section label inside a list item: {li}"));
            }
        }
        if li.matches("<strong>").count() > 1 {
            d.push(format!("a list item holds more than one bold label: {li}"));
        }
    }
    let boxes = visible.matches("type=\"checkbox\"").count();
    if boxes > common::pr_body::MAX_ITEMS {
        d.push(format!("{boxes} task items in sight"));
    }
    for t in elements(&visible, "table") {
        let head = t.split("</thead>").next().unwrap_or("");
        let cols = head.matches("<th>").count() + head.matches("<th ").count();
        if cols > 4 {
            d.push(format!("a table of {cols} columns"));
        }
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_recorded_case_passes() {
        for case in cases() {
            let out = case.shaped();
            println!("──── {} ────\n{}\n", case.name, out.body);
            // One state per step: nothing CoderHelm could not do is also
            // said to be CoderHelm's.
            for l in out.body.lines() {
                assert!(
                    !(l.contains("could not") && l.contains("(owner: CoderHelm)")),
                    "{}: {l}",
                    case.name
                );
            }
            let why = failures(&case, &out);
            assert!(
                why.is_empty(),
                "{}:\n- {}\n\n{}",
                case.name,
                why.join("\n- "),
                out.body
            );
        }
    }

    /// The eval can fail: the text as it was written fails on what the
    /// reviewer flagged, so a pass above is not a check that passes
    /// anything.
    #[test]
    fn the_text_as_it_was_written_fails_the_eval() {
        for (case, before) in [
            (dependency_case(), DEPENDENCY_BEFORE),
            (feature_case(), FEATURE_BEFORE),
            (ticket_case(), TICKET_BEFORE),
        ] {
            let as_written = Shaped {
                body: before.to_string(),
                facts: case.facts.clone(),
                corrections: vec![],
            };
            let why = failures(&case, &as_written);
            assert!(
                why.len() >= case.flagged.len(),
                "{}: only {} failures: {why:?}",
                case.name,
                why.len()
            );
        }
        // The rubric alone names what a reader cannot bear in the feature's
        // section: the buried action and each piece of raw machinery.
        let c = feature_case();
        let flaws = pr_layout::rubric(FEATURE_BEFORE, &c.facts);
        assert!(flaws.iter().any(|f| matches!(f, Flaw::Layout(_))));
        for m in [
            "HTTP status",
            "403 Forbidden",
            "https://api.",
            "query_content",
        ] {
            assert!(
                flaws.contains(&Flaw::Machinery(m.to_string())),
                "{m}: {flaws:?}"
            );
        }
    }

    /// The ticket comment about the outside steps is held to the same
    /// reading: what is needed first, in plain words, each thing once.
    #[test]
    fn the_ticket_note_says_what_is_needed_first_in_plain_words() {
        let c = feature_outside();
        let note = super::super::outside_work::note_markdown(&c);
        assert!(
            note.starts_with(
                "**Needed from you:**\n1. After merging: Deploy the staging Studio: run"
            ),
            "{note}"
        );
        assert_eq!(pr_layout::machinery(&note), Vec::<String>::new(), "{note}");
        assert_eq!(
            note.matches("not allowed to start workflows").count(),
            1,
            "{note}"
        );
        for gone in [
            "How to confirm",
            "(owner:",
            "after the pull request opens",
            "Looked up",
        ] {
            assert!(!note.contains(gone), "{gone}: {note}");
        }
        assert!(
            note.contains(
                "- Checked: the content query returns exactly one item, n = 392, it does."
            ),
            "{note}"
        );
    }

    /// The layout's filled examples and the old layout's output, rewritten:
    /// each matches its golden file (`UPDATE_GOLDENS=1` writes them), passes
    /// the checklist on its source and as GitHub renders it, and shaping it
    /// again changes nothing.
    #[test]
    fn the_examples_match_their_golden_files() {
        for (name, title, body, facts) in golden_cases() {
            let out = description_facts::shape(&body, title, facts.clone(), None);
            println!("──── {name} ────\n{}\n", out.body);
            let path = golden_path(name);
            if std::env::var("UPDATE_GOLDENS").is_ok() {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, &out.body).unwrap();
            }
            let want = std::fs::read_to_string(&path)
                .unwrap_or_else(|_| panic!("{name}: no golden file; run with UPDATE_GOLDENS=1"));
            assert_eq!(
                out.body, want,
                "{name}: the output moved from its golden file"
            );
            let flaws = pr_layout::rubric(&out.body, &out.facts);
            assert_eq!(flaws, vec![], "{name}");
            assert_eq!(rendered_defects(&out.body), Vec::<String>::new(), "{name}");
            let again = description_facts::shape(&out.body, title, facts, None);
            assert_eq!(again.body, out.body, "{name}: shaping twice");
            assert!(
                common::pr_body::visible_words(&out.body) <= common::pr_body::WORD_BUDGET,
                "{name}: {} words in sight",
                common::pr_body::visible_words(&out.body)
            );
        }
    }

    /// A tick a person made survives the rewrite, by its hidden id.
    #[test]
    fn a_tick_survives_the_rewrite() {
        let (_, title, body, facts) = golden_cases().remove(0);
        let out = description_facts::shape(&body, title, facts.clone(), None);
        assert!(out.body.contains("- [ ] <!-- ch:deploy-staging -->"));
        let ticked = out.body.replace(
            "- [ ] <!-- ch:deploy-staging -->",
            "- [x] <!-- ch:deploy-staging -->",
        );
        let again = description_facts::shape(&ticked, title, facts, None);
        assert!(
            again.body.contains("- [x] <!-- ch:deploy-staging -->"),
            "{}",
            again.body
        );
    }

    /// The checklist catches the old layout on its source and as rendered:
    /// the numbered list swallowed the review line, the deploy workflows
    /// that never run on a pull request were listed, the sha was in prose.
    #[test]
    fn the_checklist_catches_the_old_layout() {
        let found = common::pr_body::defects(OLD_LAYOUT_BEFORE, "d78bb24");
        for want in [
            "in sight: did not run",
            "in sight: (a person",
            "in sight: **Check:**",
            "no Review focus section",
            "the risk has no level and reason",
            "short sha is in sight",
        ] {
            assert!(found.iter().any(|f| f.contains(want)), "{want}: {found:#?}");
        }
        let html = rendered_defects(OLD_LAYOUT_BEFORE);
        assert!(
            html.iter()
                .any(|d| d.contains("a section label inside a list item")),
            "{html:#?}"
        );
    }

    /// Nothing to do is said in so many words, and no block of steps
    /// appears.
    #[test]
    fn with_nothing_to_do_the_description_says_so() {
        let out = ticket_case().shaped();
        let seen = pr_layout::visible(&out.body);
        assert!(!seen.contains("Needed from you"), "{seen}");
        assert!(!out.body.contains("Steps and evidence"), "{}", out.body);
    }
}
