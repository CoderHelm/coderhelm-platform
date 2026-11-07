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
    // Numbers (PR numbers, percentages) say which outcome, not what was
    // learned: two lessons differing only in them are the same lesson.
    s.to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| w.len() > 1 && !STOP.contains(w) && !w.chars().all(|c| c.is_ascii_digit()))
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
        .map(|l| {
            format!(
                "- {} (seen {}×)",
                crate::truncate_str(l.text.trim(), MAX_PROMPT_LESSON_CHARS),
                l.seen
            )
        })
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

/// A coder outcome bad enough to look for a pattern behind it.
pub fn is_coder_setback(e: &RewardEvent) -> bool {
    match e.kind {
        Some(RewardKind::PrClosedUnmerged)
        | Some(RewardKind::PrReverted)
        | Some(RewardKind::IncidentLinked) => true,
        Some(RewardKind::CiFixRounds) => e.value >= 2.0,
        // People changed a real part of it (measured from their own
        // commits, so a merge of the base branch no longer counts).
        Some(RewardKind::PrMergedEdited) => e.value >= SETBACK_EDIT_SHARE,
        _ => false,
    }
}

/// The share of a PR's diff people changed that makes it a setback.
pub const SETBACK_EDIT_SHARE: f64 = 0.3;

/// Most pull requests one coder lesson names, newest first.
pub const MAX_LESSON_PRS: usize = 4;
/// Longest coder lesson.
pub const MAX_LESSON_CHARS: usize = 900;
/// Longest lesson text put into a prompt.
pub const MAX_PROMPT_LESSON_CHARS: usize = 700;

/// The id of the one coder lesson kept per (repository, area): later
/// outcomes in the area rewrite it with what people did most recently,
/// instead of piling up near-copies that differ only in PR numbers.
pub fn area_lesson_id(area: &str) -> String {
    use sha2::{Digest, Sha256};
    let d = Sha256::digest(area.as_bytes());
    let h: String = d.iter().take(8).map(|b| format!("{b:02x}")).collect();
    format!("AREA-{h}")
}

/// A coder lesson written before lessons named what people did (`Changes in
/// `area` were ...`): replaced by the area's lesson when it is written.
pub fn is_generic_area_lesson(text: &str, area: &str) -> bool {
    let place = if area.is_empty() {
        "this repository".to_string()
    } else {
        format!("`{area}`")
    };
    text.starts_with(&format!("Changes in {place} were "))
}

fn quote(s: &str) -> String {
    format!(
        "\"{}\"",
        crate::truncate_str(s.trim().trim_matches('"'), 140)
    )
}

/// What one bad outcome says, specifically: the files people changed and
/// their own words, why a PR was closed, a revert, or CI rounds.
fn outcome_line(e: &RewardEvent) -> Option<String> {
    let pr = if e.pr > 0 {
        format!("PR #{}", e.pr)
    } else {
        "a change".to_string()
    };
    let notes: Vec<String> = e
        .notes
        .iter()
        .filter(|n| !n.trim().is_empty())
        .take(2)
        .map(|n| quote(n))
        .collect();
    let said = if notes.is_empty() {
        String::new()
    } else {
        format!(" ({})", notes.join("; "))
    };
    match e.kind? {
        RewardKind::PrMergedEdited => {
            let files: Vec<String> = e.files.iter().take(3).map(|f| format!("`{f}`")).collect();
            if files.is_empty() && notes.is_empty() {
                return Some(format!(
                    "{pr} was rewritten by people before merging ({}% of the diff)",
                    (e.value * 100.0).round() as u64
                ));
            }
            let what = if files.is_empty() {
                "changed it".to_string()
            } else {
                format!("changed {}", files.join(", "))
            };
            Some(format!("on {pr} people {what} before merging{said}"))
        }
        RewardKind::PrClosedUnmerged => Some(format!("{pr} was closed without merging{said}")),
        RewardKind::PrReverted => Some(format!("{pr} was reverted after merging{said}")),
        RewardKind::IncidentLinked => Some(format!("{pr} was tied to an alert{said}")),
        RewardKind::CiFixRounds => Some(format!(
            "{pr} needed {} CI fix rounds",
            e.value.round() as u64
        )),
        _ => None,
    }
}

/// The lesson an outcome pattern teaches, in plain words, or None.
/// `negatives` are this repo area's recent bad outcomes for the coder. The
/// lesson names what people did (the files they changed and their commit
/// subjects, why they closed a PR), newest first, and what to do about it,
/// so it changes what the coder does rather than restating "be careful".
pub fn coder_lesson_text(area: &str, negatives: &[RewardEvent]) -> Option<String> {
    let mut newest: Vec<&RewardEvent> = negatives.iter().filter(|e| e.kind.is_some()).collect();
    if newest.is_empty() {
        return None;
    }
    newest.sort_by(|a, b| b.at.cmp(&a.at));
    // One line per pull request: the most telling outcome of it.
    let rank = |e: &RewardEvent| match e.kind {
        Some(RewardKind::PrReverted) | Some(RewardKind::IncidentLinked) => 0,
        Some(RewardKind::PrMergedEdited) if !e.files.is_empty() || !e.notes.is_empty() => 1,
        Some(RewardKind::PrClosedUnmerged) if !e.notes.is_empty() => 1,
        Some(RewardKind::PrClosedUnmerged) | Some(RewardKind::PrMergedEdited) => 2,
        _ => 3,
    };
    let mut lines: Vec<String> = Vec::new();
    let mut by_pr: Vec<(&RewardEvent, i32)> = Vec::new();
    for e in &newest {
        match by_pr
            .iter_mut()
            .find(|(x, _)| x.repo == e.repo && x.pr == e.pr)
        {
            Some(slot) if rank(e) < slot.1 => *slot = (e, rank(e)),
            Some(_) => {}
            None => by_pr.push((e, rank(e))),
        }
    }
    for (e, _) in &by_pr {
        if let Some(l) = outcome_line(e) {
            lines.push(l);
        }
        if lines.len() >= MAX_LESSON_PRS {
            break;
        }
    }
    if lines.is_empty() {
        return None;
    }
    let place = if area.is_empty() {
        "this repository".to_string()
    } else {
        format!("`{area}`")
    };
    let edited_files: Vec<String> = {
        let mut f: Vec<String> = Vec::new();
        for e in &newest {
            for p in &e.files {
                if !f.contains(p) {
                    f.push(p.clone());
                }
            }
        }
        f.into_iter().take(4).collect()
    };
    let has = |k: RewardKind| newest.iter().any(|e| e.kind == Some(k));
    let mut todo: Vec<String> = Vec::new();
    if !edited_files.is_empty() {
        todo.push(format!(
            "read {} first and follow what people changed there",
            edited_files
                .iter()
                .map(|f| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if has(RewardKind::PrClosedUnmerged) || has(RewardKind::PrReverted) {
        todo.push("check the reasons above against the plan before writing code".into());
    }
    if has(RewardKind::CiFixRounds) {
        todo.push(format!(
            "run the repository's own checks for {place} before finishing"
        ));
    }
    if todo.is_empty() {
        todo.push("keep the change to what the ticket asks".into());
    }
    let text = format!(
        "In {place}: {}. Next time, {}.",
        lines.join("; "),
        todo.join(", and ")
    );
    Some(crate::truncate_str(&text, MAX_LESSON_CHARS).to_string())
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
        assert!(t.contains("PR #3 was reverted"), "{t}");
        assert!(t.contains("PR #9 was closed without merging"), "{t}");
        assert!(coder_lesson_text("", &[]).is_none());
    }

    /// Production had two lessons for one area that said only "were closed
    /// without merging, needed CI fix rounds, rewritten by people... Read
    /// the surrounding code": nothing a coder could act on. A lesson names
    /// what people changed and said, newest first, and what to do.
    #[test]
    fn coder_lessons_say_what_people_changed() {
        let mut edited = ev(RewardKind::PrMergedEdited, 12).with_value(0.4);
        edited.at = "2026-10-03T00:00:00Z".into();
        edited.files = vec!["apps/web/src/api.ts".into(), "apps/web/src/hooks.ts".into()];
        edited.notes = vec!["Use the shared fetch hook".into()];
        let mut closed = ev(RewardKind::PrClosedUnmerged, 15);
        closed.at = "2026-10-04T00:00:00Z".into();
        closed.notes = vec!["The flag lives in the CMS, not in code".into()];
        let mut ci = ev(RewardKind::CiFixRounds, 15).with_value(3.0);
        ci.at = "2026-10-04T00:00:00Z".into();
        let t = coder_lesson_text("apps/web", &[edited, closed, ci]).unwrap();
        assert!(t.starts_with("In `apps/web`: PR #15 was closed without merging (\"The flag lives in the CMS, not in code\")"), "{t}");
        assert!(t.contains("on PR #12 people changed `apps/web/src/api.ts`, `apps/web/src/hooks.ts` before merging (\"Use the shared fetch hook\")"), "{t}");
        assert!(
            t.contains("read `apps/web/src/api.ts`, `apps/web/src/hooks.ts` first"),
            "{t}"
        );
        assert!(!t.contains("Read the surrounding code"));
        assert!(t.len() <= MAX_LESSON_CHARS);
        // One lesson per area, and the old generic ones are recognised.
        assert_eq!(area_lesson_id("apps/web"), area_lesson_id("apps/web"));
        assert_ne!(area_lesson_id("apps/web"), area_lesson_id("apps/api"));
        assert!(is_generic_area_lesson(
            "Changes in `apps/web` were closed without merging (#1). Read the surrounding code.",
            "apps/web"
        ));
        assert!(!is_generic_area_lesson(&t, "apps/web"));
        // PR numbers alone don't make a different lesson.
        assert!(
            similarity(
                "closed without merging #12 #13",
                "closed without merging #99"
            ) > 0.99
        );
    }
}
