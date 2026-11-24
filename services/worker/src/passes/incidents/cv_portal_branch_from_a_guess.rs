//! cv. A slip booking page embeds a booking vendor's portal in a frame. The
//! run that added it wrote an end-to-end page object with two branches on
//! the portal's state: booking closed (the slip's button disabled, it
//! must say so) and booking open (choosing the slip must fill the
//! cart). Its coder tried to load the portal with Playwright; the shell
//! refused the command, and it wrote the open branch from test-id strings
//! it found in the vendor's script bundle. Only the closed branch ever ran.
//! The next ticket's run edited the same file, kept the branch untouched,
//! and re-wrapped a test's lines nobody's formatter checks. The day the
//! vendor opened booking the open branch failed on every branch: a
//! signed-out visitor who picks a slip gets the vendor's sign-in drawer,
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
  "https://portal.vendor.test/booking/marina/1/portal/2";

export class SlipBooking {
  readonly portalFrame: FrameLocator;

  constructor(readonly page: Page) {
    this.portalFrame = page.frameLocator(`iframe[title="Slip booking"]`);
  }

  bookingSection(): Locator {
    return this.portalFrame.getByTestId("portal-booking");
  }

  weeks(): Locator {
    return this.portalFrame.getByTestId(
      /^portal-week-tag-guest-\d+$/,
    );
  }

  sessions(): Locator {
    return this.portalFrame.getByTestId(
      /^portal-slot-option-guest-\d+$/,
    );
  }

  cartTotal(): Locator {
    return this.portalFrame.getByTestId(
      "portal-cart-preview-total",
    );
  }

  async selectFirstSlip(): Promise<{ slip: string; bookingOpen: boolean }> {
    const session = this.sessions().first();
    const slip = (await session.innerText()).split("\n")[0]?.trim() ?? "";
    const bookingOpen = await session.isEnabled();

    if (bookingOpen) {
      await session.click();
      await expect(
        this.cartTotal(),
        `Choosing "${slip}" did not add it to the portal's booking total`,
      ).not.toHaveText(/\$0(\.00)?\s*$/, { timeout: 60_000 });
    } else {
      await expect(
        session,
        `"${slip}" is disabled but doesn't say booking is closed`,
      ).toContainText(/booking is not open/i);
    }
    return { slip, bookingOpen };
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

/// A probe of the portal clicking the first week, then the first slip.
fn probe_of(open: bool) -> ProbeRecord {
    let mut after_slip = vec![state("portal-cart-preview-total", true)];
    if open {
        after_slip.push(state("portal-sign-in-drawer", true));
    }
    ProbeRecord {
        origin: PORTAL.into(),
        url: "https://portal.vendor.test/booking/marina/1/portal/2".into(),
        at: "2030-03-17T15:40:00Z".into(),
        testids: vec![
            "portal-booking".into(),
            "portal-week-tag-guest-0".into(),
            "portal-slot-option-guest-0".into(),
            "portal-cart-preview-total".into(),
        ],
        states: vec![
            state("portal-booking", true),
            state("portal-week-tag-guest-0", true),
            state("portal-cart-preview-total", true),
        ],
        steps: vec![
            ProbeStep {
                click: "portal-week-tag-guest-0".into(),
                clicked: true,
                states: vec![state("portal-slot-option-guest-0", open)],
            },
            ProbeStep {
                click: "/^portal-slot-option-guest-\\d+$/".into(),
                clicked: open,
                states: after_slip,
            },
        ],
        ..Default::default()
    }
}

fn own() -> Vec<String> {
    vec!["acme-harbor.test".into()]
}

#[test]
fn every_portal_id_is_held_to_a_probe_even_wrapped_or_kept() {
    // The ids the gate reads: all four, wrapped and pattern ones too.
    assert_eq!(
        probe::selectors_in(PAGE_OBJECT),
        vec![
            "/^portal-slot-option-guest-\\d+$/",
            "/^portal-week-tag-guest-\\d+$/",
            "portal-booking",
            "portal-cart-preview-total",
        ]
    );
    // The second run's change added no selector line; the kept ones are
    // still the gate's.
    let files = vec![(
        "e2e/pages/slip-booking.ts".to_string(),
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
        .check_e2e("e2e/pages/slip-booking.ts", PAGE_OBJECT)
        .unwrap_err();
    assert!(refusal.starts_with("Not written:"));
    assert!(refusal.contains("`portal-cart-preview-total`"));
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
        .check_e2e("e2e/pages/slip-booking.ts", PAGE_OBJECT)
        .is_ok());
}

#[test]
fn the_open_branch_is_unverified_until_a_run_takes_it() {
    let b = state_branches("e2e/pages/slip-booking.ts", PAGE_OBJECT);
    assert_eq!(b.len(), 1);
    assert_eq!(
        b[0].subject.as_deref(),
        Some("/^portal-slot-option-guest-\\d+$/")
    );
    let files = vec![(
        "e2e/pages/slip-booking.ts".to_string(),
        Some(PAGE_OBJECT.to_string()),
        PAGE_OBJECT.to_string(),
    )];
    // Booking closed, as when the branch was written: the open
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
    // The vendor opened booking: the fresh probe shows the slip
    // enabled and, after choosing it, the sign-in drawer. The coder is
    // sent to run the open arm against the live page.
    let opened = assess(&files, &[probe_of(false)], &[probe_of(true)], &own(), "");
    assert_eq!(opened.arms.iter().find(|a| a.when).unwrap().state, "now");
    let lines = opened.blocking();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("has never run in a recorded run"));
    assert!(lines[0].contains("Branch ran: e2e/pages/slip-booking.ts:"));
    assert!(
        probe::quote(&probe_of(true)).contains("newly visible: `portal-sign-in-drawer` (visible)")
    );
    // Once the coder ran it and said so, the arm reads as run.
    let line = b[0].line;
    let notes = format!(
        "{RAN_LINE} e2e/pages/slip-booking.ts:{line} when true: playwright test against staging, 1 passed"
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
    let old = "    expect((iframe as HTMLIFrameElement).style.width).toBe(\"100%\");\n    expect(iframe).toHaveClass(\"h-svh!\");\n  });\n  it(\"has no link\", () => {\n    expect(\n      screen.queryByText(\"Open booking in a new tab\")\n    ).toBeNull();\n  });\n";
    let new = "    expect((iframe as HTMLIFrameElement).style.width).toBe(\"100%\");\n  });\n  it(\"has no link\", () => {\n    expect(screen.queryByText(\"Open booking in a new tab\")).toBeNull();\n  });\n";
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
        g.shape("e2e/tests/browse/slips.spec.ts", e2e_old, e2e_new, false),
        Shaped::Refused(m) if m.contains("only reformats")
    ));
    // A file only reformatted on the branch goes back after the build.
    let facts = assess(
        &[(
            "e2e/tests/browse/slips.spec.ts".to_string(),
            Some(e2e_old.to_string()),
            e2e_new.to_string(),
        )],
        &[],
        &[],
        &own(),
        "",
    );
    assert_eq!(facts.reformatted, vec!["e2e/tests/browse/slips.spec.ts"]);
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
