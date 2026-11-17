//! cv. A camp registration page embeds a vendor's portal in a frame. The
//! run that added it wrote an end-to-end page object with two branches on
//! the portal's state: registration closed (the camp's button disabled, it
//! must say so) and registration open (choosing the camp must fill the
//! cart). Its coder tried to load the portal with Playwright; the shell
//! refused the command, and it wrote the open branch from test-id strings
//! it found in the vendor's script bundle. Only the closed branch ever ran.
//! The next ticket's run edited the same file, kept the branch untouched,
//! and re-wrapped a test's lines nobody's formatter checks. The day the
//! vendor opened registration the open branch failed on every branch: a
//! signed-out visitor who picks a camp gets the vendor's sign-in drawer,
//! and the cart stays at $0. The review's probe gate saw none of it: it
//! read only added lines, and only `getByTestId('…')` written on one line,
//! so the page object's wrapped and pattern ids were invisible to it.
//!
//! Guarantee: an end-to-end file that drives another company's page (named
//! by its host, or in a frame) carries only test ids a probe of that page
//! saw, the ones a write adds and the ones it keeps, wrapped or written as
//! patterns; the write is refused until the `probe_page` tool has run, and
//! the round commits its record. The probe clicks through the states a
//! branch reads and lists what each click brings up. Each arm of a test
//! branch on the page's state is labelled unverified in the pull request
//! until a run takes it; when a fresh probe shows the page in the state an
//! arm needs, the coder is sent to run it against the live page. A write
//! that only reformats a file is not made, and a write's formatting hunks
//! are put back where CI doesn't check formatting.

use common::e2e::branches::state_branches;
use common::e2e::probe::{self, ProbeRecord, ProbeState, ProbeStep};

use crate::passes::live_guard::{assess, LiveGuard, Shaped, RAN_LINE};

const PORTAL: &str = "portal.vendor.test";

/// The page object as the first run left it (names changed).
const PAGE_OBJECT: &str = r#"import { expect, type FrameLocator, type Locator, type Page } from "@playwright/test";

export const PORTAL_URL =
  "https://portal.vendor.test/program-portals/organization/1/portal/2";

export class CampRegistration {
  readonly portalFrame: FrameLocator;

  constructor(readonly page: Page) {
    this.portalFrame = page.frameLocator(`iframe[title="Camp registration"]`);
  }

  registrationSection(): Locator {
    return this.portalFrame.getByTestId("portal-registration");
  }

  weeks(): Locator {
    return this.portalFrame.getByTestId(
      /^portal-segment-tag-guest-\d+$/,
    );
  }

  sessions(): Locator {
    return this.portalFrame.getByTestId(
      /^portal-segment-option-guest-\d+$/,
    );
  }

  cartSubtotal(): Locator {
    return this.portalFrame.getByTestId(
      "portal-cart-preview-subtotal",
    );
  }

  async selectFirstCamp(): Promise<{ camp: string; registrationOpen: boolean }> {
    const session = this.sessions().first();
    const camp = (await session.innerText()).split("\n")[0]?.trim() ?? "";
    const registrationOpen = await session.isEnabled();

    if (registrationOpen) {
      await session.click();
      await expect(
        this.cartSubtotal(),
        `Choosing "${camp}" did not add it to the portal's Order Summary`,
      ).not.toHaveText(/\$0(\.00)?\s*$/, { timeout: 60_000 });
    } else {
      await expect(
        session,
        `"${camp}" is disabled but doesn't say registration is closed`,
      ).toContainText(/registration is not open/i);
    }
    return { camp, registrationOpen };
  }
}
"#;

fn state(id: &str, enabled: bool) -> ProbeState {
    ProbeState {
        testid: id.into(),
        visible: true,
        enabled,
        ..Default::default()
    }
}

/// A probe of the portal clicking the first week, then the first camp.
fn probe_of(open: bool) -> ProbeRecord {
    let mut after_camp = vec![state("portal-cart-preview-subtotal", true)];
    if open {
        after_camp.push(state("portal-authentication-drawer", true));
    }
    ProbeRecord {
        origin: PORTAL.into(),
        url: "https://portal.vendor.test/program-portals/organization/1/portal/2".into(),
        at: "2026-11-17T15:40:00Z".into(),
        testids: vec![
            "portal-registration".into(),
            "portal-segment-tag-guest-0".into(),
            "portal-segment-option-guest-0".into(),
            "portal-cart-preview-subtotal".into(),
        ],
        states: vec![
            state("portal-registration", true),
            state("portal-segment-tag-guest-0", true),
            state("portal-cart-preview-subtotal", true),
        ],
        steps: vec![
            ProbeStep {
                click: "portal-segment-tag-guest-0".into(),
                clicked: true,
                states: vec![state("portal-segment-option-guest-0", open)],
            },
            ProbeStep {
                click: "/^portal-segment-option-guest-\\d+$/".into(),
                clicked: open,
                states: after_camp,
            },
        ],
        ..Default::default()
    }
}

fn own() -> Vec<String> {
    vec!["acme-camps.test".into()]
}

#[test]
fn every_portal_id_is_held_to_a_probe_even_wrapped_or_kept() {
    // The ids the gate reads: all four, wrapped and pattern ones too.
    assert_eq!(
        probe::selectors_in(PAGE_OBJECT),
        vec![
            "/^portal-segment-option-guest-\\d+$/",
            "/^portal-segment-tag-guest-\\d+$/",
            "portal-cart-preview-subtotal",
            "portal-registration",
        ]
    );
    // The second run's change added no selector line; the kept ones are
    // still the gate's.
    let files = vec![(
        "e2e/pages/camp-registration.ts".to_string(),
        PAGE_OBJECT.to_string(),
        "    // the page header now scrolls away".to_string(),
    )];
    let gaps = probe::unprobed(&files, &[], &own());
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0].origin, PORTAL);
    assert_eq!(gaps[0].missing.len(), 4);
    assert!(probe::unprobed(&files, &[probe_of(false)], &own()).is_empty());
}

#[test]
fn the_write_waits_for_a_probe_and_the_round_records_it() {
    let g = LiveGuard::new(vec![], own(), vec![]);
    let refusal = g
        .check_e2e("e2e/pages/camp-registration.ts", PAGE_OBJECT)
        .unwrap_err();
    assert!(refusal.starts_with("Not written:"));
    assert!(refusal.contains("`portal-cart-preview-subtotal`"));
    assert!(refusal.contains("`clicks`"));
    // The probe tool's answer carries its record; the round keeps it (and
    // commits it at its path) and the write goes through.
    let answer = format!(
        "Probe of the portal.\n{}\n{}",
        probe::RECORD_MARK,
        serde_json::to_string(&probe_of(false)).unwrap()
    );
    let r = g
        .saw_probe(&serde_json::json!({"url": probe_of(false).url}), &answer)
        .expect("a record");
    assert_eq!(
        probe::record_path(&r.origin),
        ".coderhelm/probes/portal.vendor.test.json"
    );
    assert!(g
        .check_e2e("e2e/pages/camp-registration.ts", PAGE_OBJECT)
        .is_ok());
}

#[test]
fn the_open_branch_is_unverified_until_a_run_takes_it() {
    let b = state_branches("e2e/pages/camp-registration.ts", PAGE_OBJECT);
    assert_eq!(b.len(), 1);
    assert_eq!(
        b[0].subject.as_deref(),
        Some("/^portal-segment-option-guest-\\d+$/")
    );
    let files = vec![(
        "e2e/pages/camp-registration.ts".to_string(),
        Some(PAGE_OBJECT.to_string()),
        PAGE_OBJECT.to_string(),
    )];
    // Registration closed, as on the day the branch was written: the open
    // arm is unverified in the pull request and nothing asks the coder to
    // run what can't run; the closed arm, which the page shows, is the one
    // it runs and records.
    let closed = assess(&files, &[probe_of(false)], &[], &own(), "");
    let open_arm = closed.arms.iter().find(|a| a.when).unwrap();
    assert_eq!(open_arm.state, "unverified");
    let section = closed.section();
    assert!(section.contains("**Outside pages, as a probe saw them**"));
    assert!(section.contains(
        "unverified: no recorded run took it; CoderHelm runs it once the page shows that state"
    ));
    let lines = closed.blocking();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains(" when false: <the command"));
    // The vendor opened registration: the fresh probe shows the camp
    // enabled and, after choosing it, the sign-in drawer. The coder is
    // sent to run the open arm against the live page.
    let opened = assess(&files, &[probe_of(false)], &[probe_of(true)], &own(), "");
    assert_eq!(opened.arms.iter().find(|a| a.when).unwrap().state, "now");
    let lines = opened.blocking();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("has never run in a recorded run"));
    assert!(lines[0].contains("Branch ran: e2e/pages/camp-registration.ts:"));
    assert!(probe::quote(&probe_of(true))
        .contains("newly visible: `portal-authentication-drawer` (visible)"));
    // Once the coder ran it and said so, the arm reads as run.
    let line = b[0].line;
    let notes = format!(
        "{RAN_LINE} e2e/pages/camp-registration.ts:{line} when true: playwright test against staging, 1 passed"
    );
    let ran = assess(&files, &[probe_of(true)], &[], &own(), &notes);
    assert_eq!(ran.arms.iter().find(|a| a.when).unwrap().state, "ran");
    assert!(ran.blocking().is_empty());
}

#[test]
fn a_reformat_is_never_written_and_formatting_goes_back() {
    let g = LiveGuard::new(vec![], own(), vec![]);
    // The unit test the second run touched: the fix dropped two
    // assertions, and a call the fix didn't need was re-wrapped.
    let old = "    expect((iframe as HTMLIFrameElement).style.width).toBe(\"100%\");\n    expect(iframe).toHaveClass(\"h-svh!\");\n  });\n  it(\"has no link\", () => {\n    expect(\n      screen.queryByText(\"Open registration in a new tab\")\n    ).toBeNull();\n  });\n";
    let new = "    expect((iframe as HTMLIFrameElement).style.width).toBe(\"100%\");\n  });\n  it(\"has no link\", () => {\n    expect(screen.queryByText(\"Open registration in a new tab\")).toBeNull();\n  });\n";
    let Shaped::Write(kept, n) = g.shape("web/src/test/portal-embed.test.tsx", old, new, true)
    else {
        panic!("the fix's lines are written");
    };
    assert_eq!(n, 1);
    assert!(!kept.contains("h-svh!"));
    assert!(kept.contains("    expect(\n      screen.queryByText("));
    // An end-to-end file the fix doesn't change is never re-wrapped.
    let e2e_old = "await expect(\n  page.getByRole(\"heading\")\n).toBeVisible();\n";
    let e2e_new = "await expect(page.getByRole('heading')).toBeVisible();\n";
    assert!(matches!(
        g.shape("e2e/tests/browse/camps.spec.ts", e2e_old, e2e_new, false),
        Shaped::Refused(m) if m.contains("only reformats")
    ));
    // A file only reformatted on the branch goes back after the build.
    let facts = assess(
        &[(
            "e2e/tests/browse/camps.spec.ts".to_string(),
            Some(e2e_old.to_string()),
            e2e_new.to_string(),
        )],
        &[],
        &[],
        &own(),
        "",
    );
    assert_eq!(facts.reformatted, vec!["e2e/tests/browse/camps.spec.ts"]);
    assert!(facts.blocking()[0].contains("restore_file"));
}

#[test]
fn every_write_path_and_the_check_after_the_build_use_the_guard() {
    let implement = include_str!("../implement.rs");
    assert_eq!(implement.matches(".live_write(").count(), 3);
    assert!(implement.contains("live.saw_probe(input, &out)"));
    assert!(implement.contains("commit_probe(&record)"));
    let habits = include_str!("../habits_check.rs");
    assert!(habits.contains("live_guard::facts_for("));
    assert!(habits.contains("f.live.blocking()"));
    assert!(habits.contains("f.live.section()"));
    let review = include_str!("../review_pr.rs");
    assert!(review.contains("probe_gaps("));
}
