//! Learning health: is every learning loop still getting its signal?
//!
//! Each loop (outcomes of CoderHelm's PRs, outcomes of review comments,
//! calibration, the prompt variants, examples, coder lessons, pushback
//! judgments, the stop judge's outcome log) is shown with the time of its
//! last signal, its count over the window, and what should have fed it.
//! A loop that should have fired and stayed quiet (PRs merged but none
//! scored, reviews posted but no comment outcome) is `quiet`: the page turns
//! red, so a broken loop is seen in days, not found in an audit.
//!
//! Pure: the gateway reads the records (`routes::learning::get_health`) and
//! this decides.

use super::event::{RewardEvent, RewardKind, AGENT_CODER, AGENT_REVIEWER};
use super::lessons::{backed, is_coder_setback};
use crate::decision_log::Record;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// The window the page looks at.
pub const WINDOW_DAYS: i64 = 7;
/// Time a closed PR gets to be scored before its absence counts.
pub const SCORE_GRACE_MINUTES: i64 = 60;
/// Reviews in the window before "no comment outcome at all" is a problem.
pub const MIN_REVIEWS_FOR_OUTCOMES: usize = 5;
/// A pushback judgment still pending or judging after this long is stuck.
pub const STUCK_PENDING_HOURS: i64 = 2;
/// Stops and blocks in the window before "no outcome on any" is a problem.
pub const MIN_STOPS_FOR_OUTCOMES: usize = 5;
/// Age of the oldest stop before "no outcome yet" is a problem.
pub const STOP_OUTCOME_GRACE_DAYS: i64 = 3;

/// A loop's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// It got what it should have.
    Ok,
    /// It should have fired and didn't: something is broken.
    Quiet,
    /// Nothing happened that should feed it.
    Idle,
}

/// One loop as the page shows it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Loop {
    pub key: &'static str,
    pub label: &'static str,
    pub state: State,
    /// The newest signal (RFC 3339), empty when none is known.
    pub last_signal_at: String,
    /// Signals in the window.
    pub count: u64,
    /// What should have fed it in the window, when that is known.
    pub expected: Option<u64>,
    /// What the numbers mean, in words.
    pub detail: String,
}

/// A CoderHelm pull request that closed (merged or not) in the window.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClosedPr {
    pub repo: String,
    pub pr: u64,
    pub closed_at: String,
}

/// One prompt variant's pull counter.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArmPull {
    pub agent: String,
    pub pulls: u64,
    pub last_pull_at: String,
}

/// One pushback judgment's pending record.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Pending {
    pub status: String,
    /// Milliseconds since the epoch, as the record keeps it.
    pub last_event_at_ms: i64,
    pub judged_at: String,
    pub learned: u64,
}

/// What the page is built from (already limited to one team).
#[derive(Debug, Clone, Default)]
pub struct Inputs<'a> {
    pub events: &'a [RewardEvent],
    pub closed_prs: &'a [ClosedPr],
    /// When CoderHelm posted each review in the window.
    pub reviews: &'a [String],
    /// Each example's last use.
    pub exemplar_last_used: &'a [String],
    pub arms: &'a [ArmPull],
    /// Each calibration counter's last update.
    pub categories_updated: &'a [String],
    /// Calibration changes: (at, to).
    pub calibration_changes: &'a [(String, String)],
    /// Each coder lesson's last time written or reinforced.
    pub coder_lessons_seen: &'a [String],
    pub pending: &'a [Pending],
    /// Each review lesson's last time written or seen again.
    pub review_lessons_seen: &'a [String],
    pub decisions: &'a [Record],
}

fn parse(t: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(t)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

fn newest<'a>(it: impl Iterator<Item = &'a str>) -> String {
    it.filter(|t| parse(t).is_some())
        .max_by_key(|t| parse(t))
        .unwrap_or("")
        .to_string()
}

fn within(t: &str, since: DateTime<Utc>) -> bool {
    parse(t).is_some_and(|d| d >= since)
}

/// Every loop's health at `now`.
pub fn loops(i: &Inputs<'_>, now: DateTime<Utc>) -> Vec<Loop> {
    let since = now - chrono::Duration::days(WINDOW_DAYS);
    let events: Vec<&RewardEvent> = i.events.iter().filter(|e| within(&e.at, since)).collect();
    let reviews: Vec<&String> = i.reviews.iter().filter(|t| within(t, since)).collect();
    let mut out = Vec::new();

    // 1. Outcomes of CoderHelm's own PRs.
    let scored: BTreeSet<(String, u64)> = events
        .iter()
        .filter(|e| e.agent == AGENT_CODER)
        .filter(|e| {
            matches!(
                e.kind,
                Some(RewardKind::PrMergedUnchanged)
                    | Some(RewardKind::PrMergedEdited)
                    | Some(RewardKind::PrClosedUnmerged)
                    | Some(RewardKind::TimeToMerge)
            )
        })
        .map(|e| (e.repo.clone(), e.pr))
        .collect();
    let grace = now - chrono::Duration::minutes(SCORE_GRACE_MINUTES);
    let due: BTreeSet<(String, u64)> = i
        .closed_prs
        .iter()
        .filter(|p| within(&p.closed_at, since) && parse(&p.closed_at).is_some_and(|d| d <= grace))
        .map(|p| (p.repo.clone(), p.pr))
        .collect();
    let missing: Vec<String> = due
        .difference(&scored)
        .map(|(r, n)| format!("{r}#{n}"))
        .collect();
    out.push(Loop {
        key: "pr_outcomes",
        label: "Outcomes of CoderHelm's pull requests",
        state: if due.is_empty() {
            State::Idle
        } else if missing.is_empty() {
            State::Ok
        } else {
            State::Quiet
        },
        last_signal_at: newest(
            events
                .iter()
                .filter(|e| e.agent == AGENT_CODER)
                .map(|e| e.at.as_str()),
        ),
        count: scored.len() as u64,
        expected: Some(due.len() as u64),
        detail: if missing.is_empty() {
            format!(
                "{} of {} pull requests that closed were scored",
                due.len().min(scored.len()),
                due.len()
            )
        } else {
            format!(
                "{} closed pull request(s) were never scored: {}",
                missing.len(),
                missing
                    .iter()
                    .take(5)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        },
    });

    // 2. Outcomes of review comments (acted on, dismissed, left open).
    let findings: Vec<&&RewardEvent> = events
        .iter()
        .filter(|e| e.kind.is_some_and(RewardKind::is_finding))
        .collect();
    let by_kind = |k: RewardKind| findings.iter().filter(|e| e.kind == Some(k)).count();
    out.push(Loop {
        key: "review_outcomes",
        label: "Outcomes of review comments",
        state: if reviews.is_empty() {
            State::Idle
        } else if findings.is_empty() && reviews.len() >= MIN_REVIEWS_FOR_OUTCOMES {
            State::Quiet
        } else {
            State::Ok
        },
        last_signal_at: newest(findings.iter().map(|e| e.at.as_str())),
        count: findings.len() as u64,
        expected: Some(reviews.len() as u64),
        detail: format!(
            "{} acted on, {} dismissed, {} left open, from {} review(s)",
            by_kind(RewardKind::FindingAccepted),
            by_kind(RewardKind::FindingDismissed),
            by_kind(RewardKind::FindingIgnored),
            reviews.len()
        ),
    });

    // 3. Calibration (counters, and the changes they make).
    let cats_7d = i
        .categories_updated
        .iter()
        .filter(|t| within(t, since))
        .count();
    let changes_7d: Vec<&(String, String)> = i
        .calibration_changes
        .iter()
        .filter(|(at, _)| within(at, since))
        .collect();
    let to = |k: &str| changes_7d.iter().filter(|(_, t)| t == k).count();
    out.push(Loop {
        key: "calibration",
        label: "Reviewer calibration",
        state: if findings.is_empty() {
            State::Idle
        } else if cats_7d == 0 {
            State::Quiet
        } else {
            State::Ok
        },
        last_signal_at: newest(i.categories_updated.iter().map(String::as_str)),
        count: cats_7d as u64,
        expected: None,
        detail: format!(
            "{cats_7d} category counter(s) updated; changes: {} raised, {} downgraded, {} suppressed, {} back to none",
            to("raise"),
            to("downgrade"),
            to("suppress"),
            to("none")
        ),
    });

    // 4 and 5. Prompt variants, per agent.
    for (agent, key, label, expected) in [
        (
            AGENT_REVIEWER,
            "reviewer_variants",
            "Reviewer prompt variants",
            reviews.len(),
        ),
        (
            AGENT_CODER,
            "coder_variants",
            "Coder prompt variants",
            i.closed_prs
                .iter()
                .filter(|p| within(&p.closed_at, since))
                .count(),
        ),
    ] {
        let arms: Vec<&ArmPull> = i.arms.iter().filter(|a| a.agent == agent).collect();
        let last = newest(arms.iter().map(|a| a.last_pull_at.as_str()));
        let credited = events
            .iter()
            .filter(|e| e.agent == agent && !e.variant.is_empty())
            .count();
        let pulls: u64 = arms.iter().map(|a| a.pulls).sum();
        let recent = within(&last, since);
        // The coder's variants are new: PRs of runs started before them
        // close with no pull, which is not a broken loop.
        let not_started = agent == AGENT_CODER && arms.is_empty();
        out.push(Loop {
            key,
            label,
            state: if expected == 0 || not_started {
                State::Idle
            } else if recent || (last.is_empty() && pulls > 0 && credited > 0) {
                State::Ok
            } else {
                State::Quiet
            },
            last_signal_at: last,
            count: credited as u64,
            expected: Some(expected as u64),
            detail: format!(
                "{} arm(s), {pulls} pull(s) in all; {credited} outcome(s) credited to a variant this window",
                arms.len()
            ),
        });
    }

    // 6. Examples put into prompts.
    let used_7d = i
        .exemplar_last_used
        .iter()
        .filter(|t| within(t, since))
        .count();
    out.push(Loop {
        key: "examples",
        label: "Good and avoid examples in prompts",
        state: if i.exemplar_last_used.is_empty() || reviews.is_empty() {
            State::Idle
        } else if used_7d == 0 {
            State::Quiet
        } else {
            State::Ok
        },
        last_signal_at: newest(i.exemplar_last_used.iter().map(String::as_str)),
        count: used_7d as u64,
        expected: None,
        detail: format!(
            "{used_7d} of {} example(s) used this window",
            i.exemplar_last_used.len()
        ),
    });

    // 7. Coder lessons: an area with backed setbacks should have one.
    let mut by_area: BTreeMap<(String, String), Vec<RewardEvent>> = BTreeMap::new();
    for e in events.iter().filter(|e| is_coder_setback(e)) {
        by_area
            .entry((e.repo.clone(), e.area.clone()))
            .or_default()
            .push((*e).clone());
    }
    let backed_areas = by_area.values().filter(|v| backed(v)).count();
    let lessons_7d = i
        .coder_lessons_seen
        .iter()
        .filter(|t| within(t, since))
        .count();
    out.push(Loop {
        key: "coder_lessons",
        label: "Coder lessons from outcomes",
        state: if backed_areas == 0 {
            State::Idle
        } else if lessons_7d == 0 {
            State::Quiet
        } else {
            State::Ok
        },
        last_signal_at: newest(i.coder_lessons_seen.iter().map(String::as_str)),
        count: lessons_7d as u64,
        expected: Some(backed_areas as u64),
        detail: format!(
            "{} setback(s) in {} area(s); {backed_areas} area(s) with enough evidence for a lesson; {lessons_7d} lesson(s) written or reinforced",
            by_area.values().map(Vec::len).sum::<usize>(),
            by_area.len()
        ),
    });

    // 8. Pushback judgments.
    let stuck_before = (now - chrono::Duration::hours(STUCK_PENDING_HOURS)).timestamp_millis();
    let recent_pending: Vec<&Pending> = i
        .pending
        .iter()
        .filter(|p| p.last_event_at_ms >= since.timestamp_millis())
        .collect();
    let stuck = recent_pending
        .iter()
        .filter(|p| p.status != crate::review_lessons::STATUS_JUDGED)
        .filter(|p| p.last_event_at_ms < stuck_before)
        .count();
    let judged: Vec<&&Pending> = recent_pending
        .iter()
        .filter(|p| p.status == crate::review_lessons::STATUS_JUDGED)
        .collect();
    let learned: u64 = judged.iter().map(|p| p.learned).sum();
    out.push(Loop {
        key: "review_lessons",
        label: "Reviewer lessons from people's pushback",
        state: if recent_pending.is_empty() {
            State::Idle
        } else if stuck > 0 {
            State::Quiet
        } else {
            State::Ok
        },
        last_signal_at: newest(
            i.review_lessons_seen
                .iter()
                .map(String::as_str)
                .chain(judged.iter().map(|p| p.judged_at.as_str())),
        ),
        count: judged.len() as u64,
        expected: Some(recent_pending.len() as u64),
        detail: format!(
            "{} pull request(s) with pushback, {} judged, {learned} lesson(s) learned, {stuck} stuck",
            recent_pending.len(),
            judged.len()
        ),
    });

    // 9. The stop judge's outcome log.
    let stops: Vec<&Record> = i
        .decisions
        .iter()
        .filter(|r| r.stopped() && within(&r.created_at, since))
        .collect();
    let with_outcome = stops.iter().filter(|r| r.outcome.is_some()).count();
    let wrong = stops.iter().filter(|r| r.confirmed_wrong()).count();
    let oldest_old = stops.iter().any(|r| {
        parse(&r.created_at)
            .is_some_and(|d| d <= now - chrono::Duration::days(STOP_OUTCOME_GRACE_DAYS))
    });
    out.push(Loop {
        key: "stop_outcomes",
        label: "What people did after asks, stops and blocks",
        state: if stops.is_empty() {
            State::Idle
        } else if with_outcome == 0 && stops.len() >= MIN_STOPS_FOR_OUTCOMES && oldest_old {
            State::Quiet
        } else {
            State::Ok
        },
        last_signal_at: newest(stops.iter().map(|r| r.outcome_at.as_str())),
        count: with_outcome as u64,
        expected: Some(stops.len() as u64),
        detail: format!(
            "{} ask(s), stop(s) and block(s); {with_outcome} with a person's act recorded, {wrong} shown wrong (the judge is given those)",
            stops.len()
        ),
    });
    out
}

/// The page: every loop, and red when any is quiet.
pub fn build(i: &Inputs<'_>, now: DateTime<Utc>) -> serde_json::Value {
    let loops = loops(i, now);
    let quiet: Vec<&str> = loops
        .iter()
        .filter(|l| l.state == State::Quiet)
        .map(|l| l.key)
        .collect();
    serde_json::json!({
        "window_days": WINDOW_DAYS,
        "generated_at": now.to_rfc3339(),
        "state": if quiet.is_empty() { "ok" } else { "red" },
        "quiet": quiet,
        "loops": loops,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 6, 12, 0, 0).unwrap()
    }

    fn ev(kind: RewardKind, agent: &str, pr: u64, at: &str) -> RewardEvent {
        RewardEvent::new("T", kind, agent, at, &format!("{kind:?}{pr}")).with_repo("acme/web", pr)
    }

    fn by_key<'a>(l: &'a [Loop], k: &str) -> &'a Loop {
        l.iter().find(|x| x.key == k).unwrap()
    }

    /// Production: PRs merged that week, but outcomes for some never
    /// landed. The page names them and turns red.
    #[test]
    fn merged_prs_with_no_outcome_turn_the_page_red() {
        let closed = vec![
            ClosedPr {
                repo: "acme/web".into(),
                pr: 1,
                closed_at: "2026-11-05T00:00:00Z".into(),
            },
            ClosedPr {
                repo: "acme/web".into(),
                pr: 2,
                closed_at: "2026-11-05T00:00:00Z".into(),
            },
            // Closed minutes ago: still within the grace.
            ClosedPr {
                repo: "acme/web".into(),
                pr: 3,
                closed_at: "2026-11-06T11:50:00Z".into(),
            },
        ];
        let events = vec![ev(
            RewardKind::TimeToMerge,
            AGENT_CODER,
            1,
            "2026-11-05T00:01:00Z",
        )];
        let i = Inputs {
            events: &events,
            closed_prs: &closed,
            ..Default::default()
        };
        let l = loops(&i, now());
        let pr = by_key(&l, "pr_outcomes");
        assert_eq!(pr.state, State::Quiet);
        assert_eq!(pr.expected, Some(2));
        assert!(pr.detail.contains("acme/web#2"), "{}", pr.detail);
        let page = build(&i, now());
        assert_eq!(page["state"], "red");
        assert!(page["quiet"]
            .as_array()
            .unwrap()
            .iter()
            .any(|q| q == "pr_outcomes"));
    }

    #[test]
    fn reviews_with_no_comment_outcome_are_quiet_and_outcomes_count() {
        let reviews: Vec<String> = (0..6).map(|_| "2026-11-05T00:00:00Z".to_string()).collect();
        let i = Inputs {
            reviews: &reviews,
            ..Default::default()
        };
        let l = loops(&i, now());
        assert_eq!(by_key(&l, "review_outcomes").state, State::Quiet);
        assert_eq!(by_key(&l, "reviewer_variants").state, State::Quiet);
        // The coder's variants before their first pull: idle, not red.
        let closed = vec![ClosedPr {
            repo: "acme/web".into(),
            pr: 9,
            closed_at: "2026-11-05T00:00:00Z".into(),
        }];
        let i2 = Inputs {
            closed_prs: &closed,
            ..Default::default()
        };
        assert_eq!(
            by_key(&loops(&i2, now()), "coder_variants").state,
            State::Idle
        );
        let stale = vec![ArmPull {
            agent: AGENT_CODER.into(),
            pulls: 3,
            last_pull_at: "2026-10-01T00:00:00Z".into(),
        }];
        let i2 = Inputs {
            closed_prs: &closed,
            arms: &stale,
            ..Default::default()
        };
        assert_eq!(
            by_key(&loops(&i2, now()), "coder_variants").state,
            State::Quiet
        );
        let mut events = vec![
            ev(
                RewardKind::FindingIgnored,
                AGENT_REVIEWER,
                4,
                "2026-11-05T01:00:00Z",
            ),
            ev(
                RewardKind::FindingAccepted,
                AGENT_REVIEWER,
                4,
                "2026-11-05T02:00:00Z",
            ),
        ];
        events[0].variant = "focused".into();
        let arms = vec![ArmPull {
            agent: AGENT_REVIEWER.into(),
            pulls: 9,
            last_pull_at: "2026-11-05T00:00:00Z".into(),
        }];
        let cats = vec!["2026-11-05T02:00:00Z".to_string()];
        let i = Inputs {
            reviews: &reviews,
            events: &events,
            arms: &arms,
            categories_updated: &cats,
            ..Default::default()
        };
        let l = loops(&i, now());
        let r = by_key(&l, "review_outcomes");
        assert_eq!(r.state, State::Ok);
        assert!(r.detail.starts_with("1 acted on, 0 dismissed, 1 left open"));
        assert_eq!(r.last_signal_at, "2026-11-05T02:00:00Z");
        assert_eq!(by_key(&l, "reviewer_variants").state, State::Ok);
        assert_eq!(by_key(&l, "calibration").state, State::Ok);
        // Nothing reviewed and nothing closed: idle, never red.
        let l = loops(&Inputs::default(), now());
        assert!(l.iter().all(|x| x.state == State::Idle), "{l:?}");
        assert_eq!(build(&Inputs::default(), now())["state"], "ok");
    }

    #[test]
    fn backed_setbacks_without_a_lesson_are_quiet() {
        let mut a = ev(
            RewardKind::PrClosedUnmerged,
            AGENT_CODER,
            7,
            "2026-11-04T00:00:00Z",
        );
        a.area = "apps/web".into();
        let mut b = ev(
            RewardKind::CiFixRounds,
            AGENT_CODER,
            8,
            "2026-11-04T00:00:00Z",
        )
        .with_value(3.0);
        b.area = "apps/web".into();
        let events = vec![a, b];
        let i = Inputs {
            events: &events,
            ..Default::default()
        };
        assert_eq!(
            by_key(&loops(&i, now()), "coder_lessons").state,
            State::Quiet
        );
        let seen = vec!["2026-11-04T00:01:00Z".to_string()];
        let i = Inputs {
            events: &events,
            coder_lessons_seen: &seen,
            ..Default::default()
        };
        assert_eq!(by_key(&loops(&i, now()), "coder_lessons").state, State::Ok);
    }

    #[test]
    fn a_stuck_pushback_judgment_is_quiet() {
        let ms = |t: &str| parse(t).unwrap().timestamp_millis();
        let pending = vec![
            Pending {
                status: "judged".into(),
                last_event_at_ms: ms("2026-11-05T00:00:00Z"),
                judged_at: "2026-11-05T00:05:00Z".into(),
                learned: 1,
            },
            Pending {
                status: "pending".into(),
                last_event_at_ms: ms("2026-11-06T11:00:00Z"),
                ..Default::default()
            },
        ];
        let i = Inputs {
            pending: &pending,
            ..Default::default()
        };
        let l = by_key(&loops(&i, now()), "review_lessons").clone();
        assert_eq!(l.state, State::Ok, "an hour old is still waiting");
        assert!(l.detail.contains("1 lesson(s) learned"));
        let mut pending = pending;
        pending[1].last_event_at_ms = ms("2026-11-06T08:00:00Z");
        let i = Inputs {
            pending: &pending,
            ..Default::default()
        };
        assert_eq!(
            by_key(&loops(&i, now()), "review_lessons").state,
            State::Quiet
        );
    }

    #[test]
    fn stops_with_no_outcome_after_days_are_quiet() {
        let rec = |at: &str, outcome| Record {
            kind: "block".into(),
            verdict: "hold".into(),
            created_at: at.into(),
            outcome,
            ..Default::default()
        };
        let stops: Vec<Record> = (0..5).map(|_| rec("2026-11-01T00:00:00Z", None)).collect();
        let i = Inputs {
            decisions: &stops,
            ..Default::default()
        };
        assert_eq!(
            by_key(&loops(&i, now()), "stop_outcomes").state,
            State::Quiet
        );
        let mut stops = stops;
        stops[0].outcome = Some(crate::decision_log::Outcome::Overrode);
        stops[0].outcome_at = "2026-11-02T00:00:00Z".into();
        let i = Inputs {
            decisions: &stops,
            ..Default::default()
        };
        let l = by_key(&loops(&i, now()), "stop_outcomes").clone();
        assert_eq!(l.state, State::Ok);
        assert!(l.detail.contains("1 shown wrong"));
        assert_eq!(l.last_signal_at, "2026-11-02T00:00:00Z");
    }
}
