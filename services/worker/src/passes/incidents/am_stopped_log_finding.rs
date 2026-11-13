//! am. Two log findings of one production service stayed on the Logs page
//! as Critical and New, each with "Create fix PR" and "Not started
//! automatically: not sure the repo is ...; check it and start the fix",
//! a day and a half after their error had stopped: 735 matching lines in
//! one six-hour window, then none while the service wrote its usual volume.
//! The team had fixed the cause the day before. Nothing but time closed a
//! finding (three days without a line), so for those three days a problem
//! that was over read as a live emergency that needed a person.
//!
//! Guarantee: every analysis counts each open finding's own lines on its
//! own log group and keeps the count with the lines the group wrote. Two
//! analyses in a row with no matching line at the service's usual volume
//! make the finding stopped: shown and counted as stopped, never New, never
//! a live critical; no automatic fix starts and the stale "choose a repo"
//! reason is gone. When the service itself went quiet nobody can tell, and
//! it stays as it was. A burst that stopped after a change in its repo
//! resolves after one day instead of three, with that evidence in the
//! reason; an error that keeps coming back keeps its longer period. Seen
//! again, it is live again and its fix may start.

use chrono::{DateTime, Utc};
use common::log_findings::{
    self as lf, ActivityState, AutoFix, Check, Counted, Finding, FixState, GroupOverride,
    QuietBasis, Severity, Skip,
};

const GROUP: &str = "/ecs/prod-ledger-api";
const REPO: &str = "acme/ledger-api";
/// What the row said for a day and a half.
const OLD_WORDS: [&str; 2] = ["check it and start the fix", "not sure the repo is"];

fn at(t: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(t).unwrap().with_timezone(&Utc)
}

fn check(at: &str, lines: u64, scanned: u64, minutes: u64) -> Check {
    Check {
        at: at.into(),
        lines,
        scanned: Some(scanned),
        minutes,
    }
}

/// The finding as the analyzer stored it during the burst, after the
/// automatic sweep could not settle its repo.
fn during_the_burst() -> Finding {
    Finding {
        rec_id: "01dns".into(),
        status: "pending".into(),
        severity: Severity::Critical,
        title: "Search endpoint not found (DNS)".into(),
        source_log_group: GROUP.into(),
        source_account_id: "111122223333".into(),
        error_pattern: "getaddrinfo ENOTFOUND search.ledger.internal".into(),
        created_at: "2026-11-02T12:00:00Z".into(),
        first_seen_at: "2026-11-02T00:05:00Z".into(),
        last_seen_at: "2026-11-02T05:10:00Z".into(),
        daily_lines: vec![Counted {
            name: "2026-11-02".into(),
            lines: 735,
        }],
        activity: vec![check("2026-11-02T06:00:00Z", 735, 100_800, 1440)],
        auto_status: "skipped".into(),
        auto_reason: Skip::RepoUnsure(REPO.into()).reason(),
        ..Default::default()
    }
}

/// Every six-hourly analysis since: no matching line, the usual volume.
fn a_day_and_a_half_later() -> Finding {
    let mut f = during_the_burst();
    for (t, scanned, minutes) in [
        ("2026-11-02T12:00:00Z", 28_700, 410),
        ("2026-11-02T18:00:00Z", 53_900, 770),
        ("2026-11-03T00:00:00Z", 79_100, 1130),
        ("2026-11-03T06:00:00Z", 100_800, 1440),
        ("2026-11-03T12:00:00Z", 100_800, 1440),
    ] {
        f.activity.push(check(t, 0, scanned, minutes));
    }
    f
}

fn settings() -> AutoFix {
    AutoFix {
        enabled: true,
        enabled_at: "2026-11-01T00:00:00Z".into(),
        overrides: vec![GroupOverride {
            log_group: GROUP.into(),
            repo: REPO.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn while_the_burst_runs_it_is_a_live_critical() {
    let f = during_the_burst();
    assert_eq!(f.activity().state, ActivityState::Happening);
    assert_eq!(lf::display_status(&f, FixState::None), "new");
    assert!(lf::is_open_problem(&f, FixState::None));
    // One analysis without a line is never evidence that it stopped.
    let mut one = f.clone();
    one.activity
        .push(check("2026-11-02T12:00:00Z", 0, 28_700, 410));
    assert_eq!(lf::display_status(&one, FixState::None), "new");
}

#[test]
fn a_day_and_a_half_after_it_stopped_it_is_not_a_live_emergency() {
    let f = a_day_and_a_half_later();
    let a = f.activity();
    assert_eq!(
        (a.state, a.quiet_checks, a.normal_checks),
        (ActivityState::Stopped, 5, 5)
    );
    // Shown and counted as stopped: never New, never an open critical.
    assert_eq!(lf::display_status(&f, FixState::None), "stopped");
    assert!(!lf::is_open_problem(&f, FixState::None));
    // No automatic fix starts for it, and what it says is what was checked,
    // not a request to check something.
    let now = at("2026-11-03T16:30:00Z");
    let skip = lf::screen(&settings(), &f_undecided(&f), now).unwrap_err();
    assert!(matches!(skip, Skip::Stopped(_)), "{skip:?}");
    let said = skip.reason();
    assert!(
        said.contains("5 analyses in a row found none while the service kept logging"),
        "{said}"
    );
    for old in OLD_WORDS {
        assert!(!said.contains(old), "{said}");
        assert!(!Skip::RepoUnsure(REPO.into()).reason().contains(old));
        assert!(!Skip::RepoPending.reason().contains(old));
    }
    // It is looked at again every sweep, so a fix starts if it comes back.
    assert_eq!(skip.auto_status(), "waiting");
}

/// The same finding before the sweep decided anything.
fn f_undecided(f: &Finding) -> Finding {
    let mut f = f.clone();
    f.auto_status.clear();
    f.auto_reason.clear();
    f
}

#[test]
fn the_change_that_fixed_it_closes_it_after_a_day_not_three() {
    let mut f = a_day_and_a_half_later();
    // Nothing on record: three days after the last line, as before.
    assert_eq!(lf::quiet_period(&f), (3, QuietBasis::Default));
    assert_eq!(f.resolves_at().as_deref(), Some("2026-11-05T05:10:00Z"));
    // The sweep looks in the repo saved for the log group and finds the
    // commit that landed as the errors stopped.
    assert_eq!(lf::change_wanted(&f, &settings()).as_deref(), Some(REPO));
    let commits = vec![
        (
            "0a1b2c3".to_string(),
            "Point search at the new endpoint".to_string(),
            "2026-11-02T04:50:00Z".to_string(),
        ),
        (
            "9f8e7d6".to_string(),
            "Last week's work".to_string(),
            "2026-10-27T10:00:00Z".to_string(),
        ),
    ];
    let merges = lf::merge_changes(REPO, &commits);
    let change = lf::pick_change(&merges, &f.last_seen_at, &f.activity()).unwrap();
    assert_eq!(change.title, "Point search at the new endpoint");
    f.change = Some(change);
    assert_eq!(lf::quiet_period(&f), (1, QuietBasis::Change));
    // Thirty-five hours after the last line it is past its day: the next
    // analysis resolves it.
    assert_eq!(f.resolves_at().as_deref(), Some("2026-11-03T05:10:00Z"));
    assert!(matches!(
        lf::resolve_state(&f, None, at("2026-11-03T16:30:00Z")),
        Some(lf::ResolveState::Due { .. })
    ));
    assert_eq!(lf::change_wanted(&f, &settings()), None);
    // The same change on an error that keeps coming back shortens nothing.
    let mut back = f.clone();
    back.last_resolved_at = "2026-10-28T00:00:00Z".into();
    back.reopened_at = "2026-11-02T00:05:00Z".into();
    assert_eq!(lf::quiet_period(&back), (10, QuietBasis::Returns));
}

#[test]
fn when_the_service_itself_went_quiet_nobody_claims_it_stopped() {
    let mut f = a_day_and_a_half_later();
    for c in f.activity.iter_mut().skip(1) {
        c.scanned = Some(3);
    }
    assert_eq!(f.activity().state, ActivityState::Unknown);
    assert_eq!(lf::display_status(&f, FixState::None), "new");
    assert!(lf::is_open_problem(&f, FixState::None));
    f.change = Some(lf::Change {
        kind: "merge".into(),
        repo: REPO.into(),
        at: "2026-11-02T04:50:00Z".into(),
        ..Default::default()
    });
    assert_eq!(lf::quiet_period(&f), (3, QuietBasis::Default));
}

#[test]
fn seen_again_it_is_live_again_and_its_fix_may_start() {
    let mut f = f_undecided(&a_day_and_a_half_later());
    f.auto_status = "waiting".into();
    f.activity
        .push(check("2026-11-03T18:00:00Z", 12, 25_200, 360));
    f.last_seen_at = "2026-11-03T17:40:00Z".into();
    assert_eq!(lf::display_status(&f, FixState::None), "new");
    assert_eq!(
        lf::screen(&settings(), &f, at("2026-11-03T18:10:00Z")),
        Ok(Some(REPO.into()))
    );
}

#[test]
fn the_analyzer_and_the_sweep_still_make_these_decisions_where_they_run() {
    // The analyzer counts every unsighted open finding before it resolves
    // anything, and resolves by the evidence-aware period.
    let analyzer = include_str!("../../../../../lambda/log-analyzer/handler.py");
    let counted = analyzer
        .find("check_quiet(team_id, existing, seen_ids, cw_logs, (start, end), failed_groups)")
        .expect("the analysis counts its quiet findings");
    let resolved = analyzer
        .find(
            "resolve_stale_recommendations(team_id, existing, seen_ids, skip_groups=failed_groups)",
        )
        .expect("the analysis resolves quiet findings");
    assert!(counted < resolved);
    assert!(analyzer.contains("days, basis = lifecycle.quiet_period(item)"));
    // The sweep looks for the change before it screens, records a stopped
    // finding once, and never asks a person while the evidence is unweighed.
    let sweep = include_str!("../log_autofix.rs");
    for call in [
        "note_changes(state, team_id, cfg, &findings).await;",
        "Err(Skip::QuietHours | Skip::Stopped(_)) if f.auto_status == \"waiting\" => None",
        "Err((r, _)) if r.unweighed => starter_skip = Some(Skip::RepoPending)",
        "lf::record_repo_ask(",
    ] {
        assert!(sweep.contains(call), "the sweep no longer has: {call}");
    }
}
