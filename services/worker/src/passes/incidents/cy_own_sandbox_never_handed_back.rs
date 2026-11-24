//! cy. A run to fix a flaky end-to-end test could never run the test. While
//! planning, its two repeat runs on the base branch failed before any test
//! started: the sandbox couldn't download Chromium ("Failed to download
//! Chrome for Testing ... Download failure"; "browserType.launch:
//! Executable doesn't exist"). Playwright's CDN redirects the browser build
//! to a shared storage host the network allowlist didn't list, and the
//! `--with-deps` install failed first on the OS package mirrors with its
//! error hidden. Those runs were still counted and none passed, so the
//! test was called "not flaky, failing every run".
//!
//! CoderHelm's own review then asked for more repeat runs. The fix round ran
//! them against the team's staging site and against the pull request's
//! preview: every one failed with `net::ERR_TUNNEL_CONNECTION_FAILED`, the
//! proxy printing `CODERHELM_EGRESS_BLOCKED` for both hosts, while
//! `check_url` read both pages (they are under the domain the team lists as
//! its own). The round ended "NEEDS A PERSON ... outside_reach: add the
//! hosts to CoderHelm's sandbox network allowlist", the review kept asking
//! for changes and the pull request was held for hours. Other runs met
//! the same environment.
//!
//! Guarantee: the sandbox reaches the team's own hosts from its settings
//! and gets Playwright's browsers (through the proxy's mirror, never by
//! listing the shared host, and kept on the build host between builds).
//! Tests that never started are never counted as failures. A round that
//! met CoderHelm's own environment is a block the next round removes, never
//! a person's step: the gate reads "add hosts to CoderHelm's allowlist" as
//! CoderHelm's own work, and once the environment has blocked the proof
//! twice the review judges the evidence the rounds could get.

use super::fixture::*;
use crate::passes::own_review_ledger::{self as ledger, Approach, Blocker, Ledger, Plan};
use common::agent_policy::sandbox::{domain_allowed, own_hosts, run_allowlist};

const STAGING: &str = "staging-web.example.com";
const PREVIEW: &str = "pr-12.preview-web.example.com";
const FINDING: &str =
    "Fix is unproven: no repeat run shows the test passes (`e2e/pages/browse-pages.ts:352`)";

/// The round's account, as the run wrote it.
fn blocked_account() -> String {
    format!(
        "I couldn't do the 20 repeat runs the review asks for. The sandbox's network allowlist \
         blocks both sites the test can run against.\n\
         - Against staging: 20 of 20 failed with `net::ERR_TUNNEL_CONNECTION_FAILED`. The \
         sandbox log says `CODERHELM_EGRESS_BLOCKED {STAGING}`.\n\n\
         NEEDS A PERSON: {FINDING} :: outside_reach :: Add `{STAGING}` and `*.preview-web.example.com` \
         to CoderHelm's sandbox network allowlist, or run the spec with --repeat-each=20 from a \
         machine that can reach the preview."
    )
}

/// The fix round's conversation log: the repeat run's tool result.
fn blocked_log() -> Vec<serde_json::Value> {
    vec![serde_json::json!({
        "role": "user",
        "content": [{
            "type": "tool_result",
            "tool_use_id": "t1",
            "content": format!(
                "exit 1\n  1) [chromium] > tests/browse/marinas.spec.ts:111:3\n    Error: page.goto: net::ERR_TUNNEL_CONNECTION_FAILED at https://{STAGING}/marinas/guest-slips\n  20 failed\nCODERHELM_EGRESS_BLOCKED {STAGING}\n"
            )
        }]
    })]
}

fn round(l: &mut Ledger) {
    l.start_round(&[FINDING.to_string()]);
    let blocked = ledger::blockers_of(&[], &blocked_log());
    assert_eq!(blocked, vec![Blocker::Tooling]);
    l.end_round(blocked, &blocked_account());
    l.observe(&[FINDING.to_string()], "### Ticket: fix the flaky test");
}

#[test]
fn the_sandbox_reaches_the_teams_own_hosts_and_playwrights_downloads() {
    // The team saved the default list before its tooling hosts changed.
    let team: Vec<String> = [
        "registry.npmjs.org",
        "github.com",
        "playwright.azureedge.net",
        "cdn.playwright.dev",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    // Its settings: the domain `check_url` reads, and the preview template.
    let own = own_hosts(
        &["example.com".to_string()],
        &["https://pr-{pr}.preview-web.example.com".to_string()],
    );
    let list = run_allowlist(&team, &own);
    for h in [
        STAGING,
        PREVIEW,
        "cdn.playwright.dev",
        "playwright.download.prss.microsoft.com",
    ] {
        assert!(domain_allowed(&list, h), "{h} not in {list:?}");
    }
    // Tight: the shared storage host and other sites stay refused.
    assert!(!domain_allowed(&list, "storage.googleapis.com"));
    assert!(!domain_allowed(&list, "attacker.example.net"));
    // Every build of the run gets it.
    let build =
        crate::clients::sandbox::build_allowlist(Some(&list.join(",")), &[], false).unwrap();
    assert!(build.contains(STAGING) || build.contains("example.com"));
    // The worker composes it for every sandbox build.
    let policy = include_str!("../../agent/policy.rs");
    let f = fn_src(policy, "pub async fn sandbox_egress(");
    assert!(f.contains("run_allowlist(") && f.contains("own_hosts("));
}

#[test]
fn playwrights_browsers_come_through_the_proxys_mirror_and_are_kept() {
    let proxy = include_str!("../../../../common/src/agent_policy/egress_proxy.py");
    let stack = include_str!("../../../../../infra/lib/worker-stack.ts");
    assert!(proxy.contains("MIRROR_PREFIX = \"/__coderhelm/playwright/\""));
    assert!(proxy.contains("(\"storage.googleapis.com\", \"/chrome-for-testing-public/\")"));
    assert!(stack.contains("export PLAYWRIGHT_DOWNLOAD_HOST=$P/__coderhelm/playwright"));
    assert!(stack.contains("LocalCacheMode.CUSTOM"));
    assert!(stack.contains("/root/.cache/ms-playwright/**/*"));
    // Each browser install falls back to the browser alone when the OS
    // package step can't run.
    let install = common::e2e::test_package::BROWSER_INSTALL;
    assert!(install.contains("|| npx --no-install playwright install chromium"));
    let leg =
        common::ci_repro::leg_cmd("e2e", "tests/a.spec.ts", "t", &format!("https://{STAGING}"))
            .unwrap();
    assert!(leg.contains(install), "{leg}");
    let pkg = common::e2e::test_package::TestPackage {
        dir: "e2e".into(),
        manager: common::e2e::test_package::Manager::Pnpm,
        lockfile: None,
        has_tsconfig: true,
        lint: false,
        runner: Some(common::e2e::test_package::Runner::Playwright),
        files: Vec::new(),
    };
    let repeat =
        common::e2e::test_package::repeat_cmd(&pkg, &["tests/a.spec.ts".into()], 20, None).unwrap();
    assert!(repeat.contains(install), "{repeat}");
    assert!(common::preview::capture_command().contains(install));
}

#[test]
fn tests_that_never_started_are_never_counted_as_failures() {
    let out = "Error: Failed to download Chrome for Testing 153.0.8010.12 (playwright chromium v1243), caused by\nError: Download failure, code=1\n  Error: browserType.launch: Executable doesn't exist at /root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome\n  120 failed\n";
    let r = crate::passes::test_bug::counts_of(out);
    assert!(r.is_err(), "{r:?}");
    assert!(r.unwrap_err().contains("never started"));
    // So the test isn't called deterministic on that count.
    use common::e2e::bug_rules::{nature, Nature};
    assert_ne!(nature(None, None, &[]), Nature::Deterministic);
    // A run that did start is counted as before.
    let ran = crate::passes::test_bug::counts_of("  19 passed\n  1 failed\n").unwrap();
    assert_eq!((ran.passed, ran.failed), (19, 1));
}

#[test]
fn a_round_blocked_by_coderhelms_own_sandbox_is_never_a_persons_step() {
    let mut l = Ledger::default();
    l.observe(&[FINDING.to_string()], "");
    round(&mut l);
    let t = &l.findings[0];
    assert_eq!(t.person_claims, 0, "{t:?}");
    assert!(t.person_reason.is_empty());
    assert_eq!(t.tooling, 1);
    assert_eq!(t.last_blocked, vec![Blocker::Tooling]);
    // The next round removes the block: run again on the widened sandbox,
    // else the repository's CI or the evidence that can be had.
    let plan = l.decide(&[FINDING.to_string()], 1.0, 50.0);
    let Plan::Round { approach, .. } = plan else {
        panic!("not another round: {plan:?}");
    };
    assert_eq!(approach, Approach::Unblock(vec![Blocker::Tooling]));
    let told = ledger::instruction(&approach, &[FINDING.to_string()]);
    assert!(told.contains("github_history") && told.contains("check_url"));
    assert!(told.contains("never a person's step"));
    let rules = ledger::round_rules("");
    assert!(rules.contains("CoderHelm's own sandbox is never a person's"));
    // Blocked the same way again: CoderHelm's note, never deferred to a
    // person.
    round(&mut l);
    match l.decide(&[FINDING.to_string()], 2.0, 50.0) {
        Plan::Unfixed { why } => assert!(!why.contains("needs a person"), "{why}"),
        other => panic!("{other:?}"),
    }
    assert!(l.findings[0].person_reason.is_empty());
}

#[test]
fn the_person_gate_keeps_coderhelms_own_allowlist_as_its_own_work() {
    use common::person_gate::{decide, Ask, Class, Context, Kind};
    for text in [
        "Add `staging-web.example.com` to CoderHelm's sandbox network allowlist (outside every system CoderHelm reaches)",
        "Run the spec 20 times from a machine that can reach the preview",
    ] {
        let o = decide(&Ask::new(Kind::OpenItem, text), &Context::default());
        assert_eq!(o.class(), Class::Do, "{text}: {o:?}");
    }
}

#[test]
fn the_review_judges_the_evidence_its_rounds_could_get() {
    let mut l = Ledger::default();
    l.observe(&[FINDING.to_string()], "");
    round(&mut l);
    let one = crate::passes::review_discipline::unobtainable_items(&l);
    assert!(one.is_empty(), "one blocked round is retried first");
    round(&mut l);
    let items = crate::passes::review_discipline::unobtainable_items(&l);
    assert_eq!(items.len(), 1);
    let mut findings: Vec<crate::passes::review_agent::Finding> =
        serde_json::from_value(serde_json::json!([{
            "file": "e2e/pages/browse-pages.ts", "line": 352, "severity": "high",
            "category": "correctness", "evidence": "verified-defect",
            "title": "Fix is unproven: no repeat run shows the test passes",
            "body": "Run the spec with --repeat-each=20 and show 20 of 20 passing."
        }]))
        .unwrap();
    // Changed code: the deferred-item rule doesn't apply, this one does.
    let changed = std::collections::HashMap::from([(
        "e2e/pages/browse-pages.ts".to_string(),
        std::collections::HashSet::from([352u64]),
    )]);
    let made = crate::passes::review_discipline::judge(
        &mut findings,
        &crate::passes::review_discipline::Facts {
            changed_since_previous: Some(&changed),
            unobtainable: &items,
            ..Default::default()
        },
    );
    assert!(!findings[0].holds_merge());
    assert!(made[0].why.contains("couldn't run"), "{made:?}");
    // A finding that names a defect in the code still holds.
    let mut defect: Vec<crate::passes::review_agent::Finding> =
        serde_json::from_value(serde_json::json!([{
            "file": "e2e/pages/browse-pages.ts", "line": 352, "severity": "high",
            "category": "correctness", "evidence": "verified-defect",
            "title": "The count is read before the list renders",
            "body": "`sessions().count()` runs once, right after open."
        }]))
        .unwrap();
    crate::passes::review_discipline::judge(
        &mut defect,
        &crate::passes::review_discipline::Facts {
            changed_since_previous: Some(&changed),
            unobtainable: &items,
            ..Default::default()
        },
    );
    assert!(defect[0].holds_merge());
    // The review reads the ledger's unobtainable items.
    let review = include_str!("../review_pr.rs");
    assert!(review.contains("review_discipline::unobtainable_items"));
}
