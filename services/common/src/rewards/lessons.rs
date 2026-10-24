//! Lessons distilled from outcomes, kept only with evidence.
//!
//! A lesson is written only when the evidence behind it is more than one
//! weak signal ([`backed`]): two independent outcomes (different pull
//! requests or runs), at least one of them strong. A new lesson that says
//! what a kept one already says ([`duplicate_of`]) adds its evidence to the
//! kept one instead. Lessons fade: each counts `seen × ½^(age / half-life)`
//! ([`weight`]), and ones that fall under [`MIN_WEIGHT`] are no longer given
//! to agents.

use super::event::{RewardEvent, RewardKind};
use serde::Serialize;
use std::collections::BTreeSet;

/// Days for a lesson's weight to halve without new evidence.
pub const HALF_LIFE_DAYS: f64 = 45.0;
/// Weight under which a lesson is no longer recalled.
pub const MIN_WEIGHT: f64 = 0.25;
/// Word overlap at or above which two lessons say the same thing.
pub const DUPLICATE_AT: f64 = 0.6;
/// Most lessons recalled into one prompt.
pub const MAX_RECALLED: usize = 6;
/// Most lessons kept per repository.
pub const MAX_KEPT: usize = 40;

/// Is this evidence enough for a lesson? Never one weak signal: at least two
/// independent outcomes, one of them strong.
pub fn backed(evidence: &[RewardEvent]) -> bool {
    let independent: BTreeSet<String> = evidence
        .iter()
        .map(|e| {
            if e.pr > 0 {
                format!("{}#{}", e.repo, e.pr)
            } else if !e.run_id.is_empty() {
                e.run_id.clone()
            } else {
                e.dedup.clone()
            }
        })
        .collect();
    let strong = evidence
        .iter()
        .any(|e| e.kind.is_some_and(RewardKind::is_strong));
    independent.len() >= 2 && strong
}

fn words(s: &str) -> BTreeSet<String> {
    const STOP: &[&str] = &[
        "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "is", "are", "be", "this",
        "that", "with", "it", "as", "at", "by", "here", "when", "only",
    ];
    s.to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| w.len() > 1 && !STOP.contains(w))
        .map(str::to_string)
        .collect()
}

/// Word overlap (Jaccard) between two lessons, 0..1.
pub fn similarity(a: &str, b: &str) -> f64 {
    let (a, b) = (words(a), words(b));
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let inter = a.intersection(&b).count() as f64;
    let union = a.union(&b).count() as f64;
    inter / union
}

/// The kept lesson `text` repeats, if any (the most similar one).
pub fn duplicate_of<'a>(text: &str, kept: &'a [Lesson]) -> Option<&'a Lesson> {
    kept.iter()
        .map(|l| (similarity(text, &l.text), l))
        .filter(|(s, _)| *s >= DUPLICATE_AT)
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(_, l)| l)
}

/// One lesson.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Lesson {
    pub id: String,
    pub repo: String,
    /// `reviewer` or `coder`.
    pub agent: String,
    pub text: String,
    /// Independent outcomes behind it.
    pub seen: u64,
    pub created_at: String,
    pub last_seen_at: String,
    /// Short evidence lines (kind, PR, detail).
    pub evidence: Vec<String>,
    /// An admin turned it off.
    pub reverted: bool,
}

/// The lesson's weight at `now`: `seen` halved every half-life since it was
/// last seen.
pub fn weight(seen: u64, last_seen_at: &str, now: chrono::DateTime<chrono::Utc>) -> f64 {
    let age_days = chrono::DateTime::parse_from_rfc3339(last_seen_at)
        .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds() as f64 / 86_400.0)
        .unwrap_or(f64::INFINITY)
        .max(0.0);
    seen as f64 * 0.5f64.powf(age_days / HALF_LIFE_DAYS)
}

/// The lessons to give an agent: not reverted, still heavy enough, heaviest
/// first, at most [`MAX_RECALLED`].
pub fn recall(kept: &[Lesson], agent: &str, now: chrono::DateTime<chrono::Utc>) -> Vec<Lesson> {
    let mut live: Vec<(f64, Lesson)> = kept
        .iter()
        .filter(|l| !l.reverted && l.agent == agent)
        .map(|l| (weight(l.seen, &l.last_seen_at, now), l.clone()))
        .filter(|(w, _)| *w >= MIN_WEIGHT)
        .collect();
    live.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    live.into_iter()
        .take(MAX_RECALLED)
        .map(|(_, l)| l)
        .collect()
}

/// The prompt block for recalled lessons; empty when there are none.
pub fn prompt_block(lessons: &[Lesson]) -> String {
    if lessons.is_empty() {
        return String::new();
    }
    let lines: Vec<String> = lessons
        .iter()
        .map(|l| format!("- {} (seen {}×)", l.text.trim(), l.seen))
        .collect();
    format!(
        "\n\n## Lessons from what happened to earlier work in this repository\nEach is backed by at least two outcomes.\n{}",
        lines.join("\n")
    )
}

/// What to do with a candidate lesson.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// Not enough evidence: write nothing.
    Skip,
    /// Add evidence to a kept lesson.
    Reinforce { id: String },
    /// Write a new lesson.
    Write,
}

pub fn plan(text: &str, evidence: &[RewardEvent], kept: &[Lesson]) -> Plan {
    if text.trim().is_empty() || !backed(evidence) {
        return Plan::Skip;
    }
    match duplicate_of(text, kept) {
        Some(l) => Plan::Reinforce { id: l.id.clone() },
        None => Plan::Write,
    }
}

/// The lesson an outcome pattern teaches, in plain words, or None.
/// `negatives` are this repo area's recent bad outcomes for the coder.
pub fn coder_lesson_text(area: &str, negatives: &[RewardEvent]) -> Option<String> {
    if negatives.is_empty() {
        return None;
    }
    let mut kinds: Vec<&str> = negatives
        .iter()
        .filter_map(|e| e.kind)
        .map(|k| match k {
            RewardKind::PrReverted => "reverted",
            RewardKind::PrClosedUnmerged => "closed without merging",
            RewardKind::IncidentLinked => "tied to an alert",
            RewardKind::CiFixRounds => "needed CI fix rounds",
            RewardKind::PrMergedEdited => "rewritten by people before merging",
            _ => "rejected",
        })
        .collect();
    kinds.sort();
    kinds.dedup();
    let prs: BTreeSet<String> = negatives
        .iter()
        .filter(|e| e.pr > 0)
        .map(|e| format!("#{}", e.pr))
        .collect();
    let place = if area.is_empty() {
        "this repository".to_string()
    } else {
        format!("`{area}`")
    };
    Some(format!(
        "Changes in {place} were {} ({}). Read the surrounding code and its tests before changing it, and keep the change small.",
        kinds.join(", "),
        prs.into_iter().collect::<Vec<_>>().join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rewards::event::AGENT_CODER;
    use chrono::TimeZone;

    fn ev(kind: RewardKind, pr: u64) -> RewardEvent {
        RewardEvent::new(
            "t",
            kind,
            AGENT_CODER,
            "2026-10-01T00:00:00Z",
            &format!("d{pr}"),
        )
        .with_repo("acme/web", pr)
    }

    fn lesson(id: &str, text: &str, seen: u64, last: &str) -> Lesson {
        Lesson {
            id: id.into(),
            repo: "acme/web".into(),
            agent: "coder".into(),
            text: text.into(),
            seen,
            last_seen_at: last.into(),
            ..Default::default()
        }
    }

    #[test]
    fn one_weak_signal_never_makes_a_lesson() {
        assert!(!backed(&[ev(RewardKind::CiFixRounds, 1)]));
        assert!(!backed(&[ev(RewardKind::PrReverted, 1)]));
        // Two weak signals are still not enough.
        assert!(!backed(&[
            ev(RewardKind::CiFixRounds, 1),
            ev(RewardKind::RatingDown, 2)
        ]));
        // Two signals on the same PR are one outcome.
        assert!(!backed(&[
            ev(RewardKind::PrReverted, 1),
            ev(RewardKind::CiFixRounds, 1)
        ]));
        assert!(backed(&[
            ev(RewardKind::PrReverted, 1),
            ev(RewardKind::CiFixRounds, 2)
        ]));
        assert_eq!(
            plan("x y z", &[ev(RewardKind::PrReverted, 1)], &[]),
            Plan::Skip
        );
    }

    #[test]
    fn repeated_lessons_reinforce_the_kept_one() {
        let kept = vec![lesson(
            "L1",
            "Changes in `src/billing` were reverted (#3, #9). Read the surrounding code and its tests before changing it, and keep the change small.",
            2,
            "2026-10-01T00:00:00Z",
        )];
        let ev2 = [
            ev(RewardKind::PrReverted, 11),
            ev(RewardKind::PrClosedUnmerged, 12),
        ];
        let same = "Changes in `src/billing` were reverted, closed without merging (#11, #12). Read the surrounding code and its tests before changing it, and keep the change small.";
        assert_eq!(plan(same, &ev2, &kept), Plan::Reinforce { id: "L1".into() });
        let other =
            "Plan tasks about the mobile app are rejected when they skip the design review.";
        assert_eq!(plan(other, &ev2, &kept), Plan::Write);
        assert!(similarity("", "x") == 0.0);
    }

    #[test]
    fn lessons_fade_and_fall_out() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap();
        assert_eq!(weight(2, "2026-10-01T00:00:00Z", now), 2.0);
        let w = weight(2, "2026-08-17T00:00:00Z", now); // 45 days
        assert!((w - 1.0).abs() < 1e-9);
        assert_eq!(weight(5, "garbage", now), 0.0);
        let kept = vec![
            lesson("fresh", "a b c", 1, "2026-09-30T00:00:00Z"),
            lesson("old", "d e f", 1, "2026-01-01T00:00:00Z"),
            lesson("heavy", "g h i", 6, "2026-09-01T00:00:00Z"),
            Lesson {
                reverted: true,
                ..lesson("off", "j k l", 9, "2026-09-30T00:00:00Z")
            },
            Lesson {
                agent: "reviewer".into(),
                ..lesson("rev", "m n o", 9, "2026-09-30T00:00:00Z")
            },
        ];
        let got: Vec<String> = recall(&kept, "coder", now)
            .into_iter()
            .map(|l| l.id)
            .collect();
        assert_eq!(got, vec!["heavy", "fresh"]);
        assert!(prompt_block(&[]).is_empty());
        assert!(prompt_block(&recall(&kept, "coder", now)).contains("(seen 6×)"));
    }

    #[test]
    fn coder_lessons_name_the_area_and_the_evidence() {
        let t = coder_lesson_text(
            "src/billing",
            &[
                ev(RewardKind::PrReverted, 3),
                ev(RewardKind::PrClosedUnmerged, 9),
            ],
        )
        .unwrap();
        assert!(t.contains("`src/billing`"));
        assert!(t.contains("#3, #9"));
        assert!(t.contains("closed without merging, reverted"));
        assert!(coder_lesson_text("", &[]).is_none());
    }
}
