//! Lessons distilled from outcomes, kept only with evidence.
//!
//! A lesson is written only when the evidence behind it is more than one
//! weak signal ([`backed`]): two independent outcomes (different pull
//! requests or runs), at least one of them strong. A new lesson that says
//! what a kept one already says ([`duplicate_of`]) adds its evidence to the
//! kept one instead. Lessons fade: each counts `seen × ½^(age / half-life)`
//! ([`weight`]), and ones that fall under [`MIN_WEIGHT`] are no longer given
//! to agents.
//!
//! A lesson can also come from one outcome people explained themselves:
//! their own commits on top of CoderHelm's pull request, the comment they
//! closed it with, the reason they reverted it ([`Distilled`]). Such a
//! lesson must be specific and checkable ([`checkable`]): a rule that names
//! code, and a way to check the work against it. It is deduplicated against
//! the kept ones, counts each pull request once, and loses weight when a
//! later outcome contradicts it ([`plan_outcome`], [`CONTRADICTION_COST`]):
//! a lesson people contradicted as often as they confirmed it is no longer
//! recalled.

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
    /// How to check the work against it. Only lessons distilled from an
    /// outcome people explained have one.
    pub check: String,
    /// Where it holds: a path or a path prefix, empty for the repository.
    pub applies_to: String,
    /// What it was learned from first: `edited`, `closed` or `reverted`;
    /// empty for a pattern lesson.
    pub source: String,
    /// The pull requests behind it (`owner/name#12`), each once.
    pub prs: Vec<String>,
    /// Later outcomes that went against it.
    pub contradicted: u64,
    /// People merged it into the repository's guide file: the guide carries
    /// it from then on.
    pub in_guide: bool,
    /// People declined it for the guide: never proposed again.
    pub guide_declined: bool,
}

/// What one contradicting outcome takes off a lesson's `seen`.
pub const CONTRADICTION_COST: f64 = 1.5;
/// Independent pull requests a lesson needs before it is proposed for the
/// repository's guide file.
pub const GUIDE_MIN_SEEN: u64 = 2;
/// The weight it needs for that.
pub const GUIDE_MIN_WEIGHT: f64 = 1.5;
/// Most evidence lines kept on one lesson.
pub const MAX_EVIDENCE: usize = 12;
/// Most pull requests named on one lesson.
pub const MAX_PRS: usize = 12;

/// The lesson's weight at `now`: what it was seen, less what contradicted
/// it, halved every half-life since it was last seen.
pub fn weight_of(l: &Lesson, now: chrono::DateTime<chrono::Utc>) -> f64 {
    let net = l.seen as f64 - CONTRADICTION_COST * l.contradicted as f64;
    if net <= 0.0 {
        return 0.0;
    }
    weight(1, &l.last_seen_at, now) * net
}

/// Whether a lesson has reached the confidence to be proposed for the
/// repository's guide file: a checkable coder lesson seen on enough pull
/// requests, never contradicted, not turned off, and not already proposed.
pub fn guide_ready(l: &Lesson, now: chrono::DateTime<chrono::Utc>) -> bool {
    l.agent == super::event::AGENT_CODER
        && !l.check.trim().is_empty()
        && !l.reverted
        && !l.in_guide
        && !l.guide_declined
        && l.contradicted == 0
        && l.seen >= GUIDE_MIN_SEEN
        && weight_of(l, now) >= GUIDE_MIN_WEIGHT
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

/// The lessons to give an agent: not reverted, not already in the guide
/// file (the run reads that), still heavy enough after what contradicted
/// them, heaviest first, at most [`MAX_RECALLED`].
pub fn recall(kept: &[Lesson], agent: &str, now: chrono::DateTime<chrono::Utc>) -> Vec<Lesson> {
    let mut live: Vec<(f64, Lesson)> = kept
        .iter()
        .filter(|l| !l.reverted && !l.in_guide && l.agent == agent)
        .map(|l| (weight_of(l, now), l.clone()))
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
            let check = if l.check.trim().is_empty() {
                String::new()
            } else {
                format!(
                    " Check: {}",
                    crate::truncate_str(l.check.trim(), MAX_CHECK_CHARS)
                )
            };
            format!(
                "- {}{check} (seen {}×)",
                crate::truncate_str(l.text.trim(), MAX_PROMPT_LESSON_CHARS),
                l.seen
            )
        })
        .collect();
    format!(
        "\n\n## Lessons from what happened to earlier work in this repository\nEach comes from what people did with earlier pull requests here (their own edits, why they closed or reverted one). Where a lesson gives a check, run it on your change before you finish.\n{}",
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

// ── Lessons from one outcome people explained ─────────────────────────

/// Longest rule and check of a distilled lesson.
pub const MAX_RULE_CHARS: usize = 400;
pub const MAX_CHECK_CHARS: usize = 300;
const MIN_RULE_CHARS: usize = 20;
const MIN_CHECK_CHARS: usize = 10;

/// What an outcome was: people edited the pull request before merging,
/// closed it without merging, or reverted it after.
pub const SOURCE_EDITED: &str = "edited";
pub const SOURCE_CLOSED: &str = "closed";
pub const SOURCE_REVERTED: &str = "reverted";

/// A lesson distilled from one outcome, as the model gave it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
pub struct Distilled {
    /// What to do, or not to do, naming the code it is about.
    #[serde(default)]
    pub rule: String,
    /// How to check a change against the rule.
    #[serde(default)]
    pub check: String,
    /// A path or path prefix it holds for; empty for the repository.
    #[serde(default)]
    pub applies_to: String,
    /// The id of a kept lesson that already says this.
    #[serde(default)]
    pub duplicate_of: Option<String>,
    /// Ids of kept lessons this outcome goes against.
    #[serde(default)]
    pub contradicts: Vec<String>,
}

/// Why a distilled lesson is not kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotCheckable {
    /// No rule, or one too short or too long to be a rule.
    NoRule,
    /// Nothing says how to check a change against it.
    NoCheck,
    /// It names no code: no path, no identifier, no command.
    NamesNothing,
    /// Advice that fits any change ("be careful", "test thoroughly").
    Vague,
    /// It is about a person, not about the code.
    AboutAPerson,
}

const VAGUE: &[&str] = &[
    "be careful",
    "be more careful",
    "pay attention",
    "pay more attention",
    "double check",
    "double-check",
    "best practice",
    "test thoroughly",
    "test more",
    "write better",
    "higher quality",
    "read the surrounding code",
    "understand the code",
    "think carefully",
    "avoid mistakes",
    "avoid bugs",
];

fn names_code(text: &str) -> bool {
    let mut ticks = text.match_indices('`');
    while let (Some((a, _)), Some((b, _))) = (ticks.next(), ticks.next()) {
        if b > a + 1 {
            return true;
        }
    }
    false
}

/// Is the lesson specific and checkable? A rule of a usable length, a way
/// to check it, at least one named piece of code (in backticks, or the path
/// it applies to), no advice that fits any change, nothing about a person.
pub fn checkable(d: &Distilled) -> Result<(), NotCheckable> {
    let (rule, check) = (d.rule.trim(), d.check.trim());
    if rule.len() < MIN_RULE_CHARS || rule.len() > MAX_RULE_CHARS {
        return Err(NotCheckable::NoRule);
    }
    if check.len() < MIN_CHECK_CHARS || check.len() > MAX_CHECK_CHARS {
        return Err(NotCheckable::NoCheck);
    }
    if rule.contains('@') || check.contains('@') {
        return Err(NotCheckable::AboutAPerson);
    }
    if !names_code(rule) && !names_code(check) && d.applies_to.trim().is_empty() {
        return Err(NotCheckable::NamesNothing);
    }
    let low = rule.to_ascii_lowercase();
    if VAGUE.iter().any(|v| low.contains(v)) && !names_code(rule) {
        return Err(NotCheckable::Vague);
    }
    Ok(())
}

/// What to do with a distilled lesson.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeAction {
    /// Not kept, and why.
    Skip(NotCheckable),
    /// A kept lesson says it already. `counts`: this pull request is new
    /// evidence for it (false when it was already counted).
    Reinforce { id: String, counts: bool },
    /// A new lesson.
    Write,
}

/// How one outcome's lesson lands among the kept ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomePlan {
    pub action: OutcomeAction,
    /// Kept lessons this outcome contradicts (each counted against once per
    /// pull request).
    pub contradict: Vec<String>,
    /// The lightest kept lesson, dropped to make room for a new one.
    pub drop: Option<String>,
}

/// Pure: how the lesson distilled from the outcome of `pr_key`
/// (`owner/name#12`) lands among the repository's kept lessons. `d` is None
/// when the outcome taught no rule; it may still contradict kept ones.
pub fn plan_outcome(
    kept: &[Lesson],
    d: Option<&Distilled>,
    contradicts: &[String],
    pr_key: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> OutcomePlan {
    let coder: Vec<&Lesson> = kept
        .iter()
        .filter(|l| l.agent == super::event::AGENT_CODER)
        .collect();
    let known = |id: &str| coder.iter().find(|l| l.id == id).copied();
    let action = match d {
        None => OutcomeAction::Skip(NotCheckable::NoRule),
        Some(d) => match checkable(d) {
            Err(why) => OutcomeAction::Skip(why),
            Ok(()) => {
                let named = d.duplicate_of.as_deref().and_then(known);
                let similar = || {
                    coder
                        .iter()
                        .filter(|l| !l.check.is_empty())
                        .map(|l| (similarity(&d.rule, &l.text), *l))
                        .filter(|(s, _)| *s >= DUPLICATE_AT)
                        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
                        .map(|(_, l)| l)
                };
                match named.or_else(similar) {
                    Some(l) => OutcomeAction::Reinforce {
                        id: l.id.clone(),
                        counts: !l.prs.iter().any(|p| p == pr_key),
                    },
                    None => OutcomeAction::Write,
                }
            }
        },
    };
    let same = match &action {
        OutcomeAction::Reinforce { id, .. } => Some(id.as_str()),
        _ => None,
    };
    let mut contradict: Vec<String> = Vec::new();
    for id in contradicts {
        // A lesson is never both confirmed and contradicted by one outcome,
        // and an outcome counts against a lesson once.
        if Some(id.as_str()) == same || contradict.contains(id) {
            continue;
        }
        if let Some(l) = known(id) {
            let mark = contradiction_mark(pr_key);
            if !l.evidence.iter().any(|e| e.starts_with(&mark)) {
                contradict.push(id.clone());
            }
        }
    }
    let drop = if action == OutcomeAction::Write && kept.len() >= MAX_KEPT {
        kept.iter()
            .filter(|l| !l.in_guide)
            .min_by(|a, b| {
                weight_of(a, now)
                    .partial_cmp(&weight_of(b, now))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|l| l.id.clone())
    } else {
        None
    };
    OutcomePlan {
        action,
        contradict,
        drop,
    }
}

/// The last `max` of `list` with `more` added: lists on a lesson stay
/// bounded however many pull requests confirm it.
pub fn bounded(list: &[String], more: &[String], max: usize) -> Vec<String> {
    let all: Vec<String> = list.iter().chain(more.iter()).cloned().collect();
    all[all.len().saturating_sub(max)..].to_vec()
}

/// The evidence line that says a pull request went against a lesson.
pub fn contradiction_line(pr_key: &str, evidence: &[String]) -> String {
    format!(
        "{}: {}",
        contradiction_mark(pr_key),
        crate::truncate_str(evidence.first().map(String::as_str).unwrap_or(""), 300)
    )
}

/// The evidence lines one more confirming pull request adds.
pub fn reinforcing_lines(evidence: &[String]) -> Vec<String> {
    evidence
        .iter()
        .take(3)
        .map(|l| crate::truncate_str(l, 500).to_string())
        .collect()
}

/// Pure: the new lesson an outcome writes.
pub fn new_outcome_lesson(
    repo: &str,
    id: &str,
    d: &Distilled,
    source: &str,
    pr: u64,
    evidence: &[String],
    now: &str,
) -> Lesson {
    Lesson {
        id: id.to_string(),
        repo: repo.to_string(),
        agent: super::event::AGENT_CODER.to_string(),
        text: d.rule.trim().to_string(),
        check: d.check.trim().to_string(),
        applies_to: d.applies_to.trim().to_string(),
        source: source.to_string(),
        seen: 1,
        prs: vec![pr_key(repo, pr)],
        created_at: now.to_string(),
        last_seen_at: now.to_string(),
        evidence: evidence
            .iter()
            .take(MAX_EVIDENCE)
            .map(|l| crate::truncate_str(l, 500).to_string())
            .collect(),
        ..Default::default()
    }
}

/// Pure: the kept lessons after an outcome's plan, as the store writes it
/// (`Store::learn_outcome` makes the same changes with atomic updates).
/// Returns the id of the lesson written or reinforced.
#[allow(clippy::too_many_arguments)]
pub fn apply_outcome(
    kept: &mut Vec<Lesson>,
    plan: &OutcomePlan,
    d: Option<&Distilled>,
    repo: &str,
    source: &str,
    pr: u64,
    evidence: &[String],
    new_id: &str,
    now: &str,
) -> Option<String> {
    let key = pr_key(repo, pr);
    for id in &plan.contradict {
        if let Some(l) = kept.iter_mut().find(|l| &l.id == id) {
            l.contradicted += 1;
            l.evidence = bounded(
                &l.evidence,
                &[contradiction_line(&key, evidence)],
                MAX_EVIDENCE,
            );
        }
    }
    match (&plan.action, d) {
        (OutcomeAction::Reinforce { id, counts }, _) => {
            if let Some(l) = kept.iter_mut().find(|l| &l.id == id).filter(|_| *counts) {
                l.seen += 1;
                l.last_seen_at = now.to_string();
                l.prs = bounded(&l.prs, &[key], MAX_PRS);
                l.evidence = bounded(&l.evidence, &reinforcing_lines(evidence), MAX_EVIDENCE);
            }
            Some(id.clone())
        }
        (OutcomeAction::Write, Some(d)) => {
            if let Some(drop) = &plan.drop {
                kept.retain(|l| &l.id != drop);
            }
            kept.push(new_outcome_lesson(
                repo, new_id, d, source, pr, evidence, now,
            ));
            Some(new_id.to_string())
        }
        _ => None,
    }
}

/// How an evidence line that contradicts a lesson starts, so one pull
/// request counts against it once.
pub fn contradiction_mark(pr_key: &str) -> String {
    format!("Contradicted by {pr_key}")
}

/// The pull request key lessons name their evidence by.
pub fn pr_key(repo: &str, pr: u64) -> String {
    format!("{repo}#{pr}")
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

    fn distilled(rule: &str, check: &str) -> Distilled {
        Distilled {
            rule: rule.into(),
            check: check.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_lesson_must_be_specific_and_checkable() {
        let good = distilled(
            "Fetch through the shared `useApi` hook in `apps/web`, never with a bare `fetch` call.",
            "Search the diff for `fetch(` under `apps/web/src`: there should be none.",
        );
        assert_eq!(checkable(&good), Ok(()));
        assert_eq!(
            checkable(&distilled(
                "Be more careful with changes to the API layer.",
                &good.check
            )),
            Err(NotCheckable::Vague)
        );
        assert_eq!(
            checkable(&distilled(
                "Changes to the billing code need a migration file.",
                "Look at the migrations folder."
            )),
            Err(NotCheckable::NamesNothing)
        );
        // The path it applies to is enough of a name.
        let mut scoped = distilled(
            "Changes to the billing code need a migration file.",
            "A new file exists under migrations for the change.",
        );
        scoped.applies_to = "services/billing".into();
        assert_eq!(checkable(&scoped), Ok(()));
        assert_eq!(
            checkable(&distilled(&good.rule, "")),
            Err(NotCheckable::NoCheck)
        );
        assert_eq!(
            checkable(&distilled("Use `x`.", &good.check)),
            Err(NotCheckable::NoRule)
        );
        assert_eq!(
            checkable(&distilled(&"a".repeat(MAX_RULE_CHARS + 1), &good.check)),
            Err(NotCheckable::NoRule)
        );
        assert_eq!(
            checkable(&distilled(
                "Ask @pat before touching `apps/web/src/api.ts` again.",
                &good.check
            )),
            Err(NotCheckable::AboutAPerson)
        );
    }

    fn kept_rule(id: &str, text: &str, prs: &[&str]) -> Lesson {
        Lesson {
            check: "Search the diff for `fetch(`.".into(),
            prs: prs.iter().map(|p| p.to_string()).collect(),
            seen: prs.len().max(1) as u64,
            ..lesson(id, text, 1, "2026-10-01T00:00:00Z")
        }
    }

    #[test]
    fn an_outcome_lesson_is_written_once_and_counts_each_pull_request_once() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap();
        let d = distilled(
            "Fetch through the shared `useApi` hook in `apps/web`, never with a bare `fetch` call.",
            "Search the diff for `fetch(` under `apps/web/src`: there should be none.",
        );
        let p = plan_outcome(&[], Some(&d), &[], "acme/web#12", now);
        assert_eq!(p.action, OutcomeAction::Write);
        assert!(p.contradict.is_empty() && p.drop.is_none());

        // The same rule in other words, from another pull request.
        let kept = vec![kept_rule(
            "L1",
            "In `apps/web` fetch through the shared `useApi` hook, never a bare `fetch` call.",
            &["acme/web#12"],
        )];
        let p = plan_outcome(&kept, Some(&d), &[], "acme/web#15", now);
        assert_eq!(
            p.action,
            OutcomeAction::Reinforce {
                id: "L1".into(),
                counts: true
            }
        );
        // The same pull request again (a redelivery) adds nothing.
        let p = plan_outcome(&kept, Some(&d), &[], "acme/web#12", now);
        assert_eq!(
            p.action,
            OutcomeAction::Reinforce {
                id: "L1".into(),
                counts: false
            }
        );
        // The model named the kept lesson it repeats: that wins over wording.
        let mut named = distilled(
            "Requests from the site go through `useApi`, which adds the session header.",
            "No `fetch(` call in the diff under `apps/web/src`.",
        );
        named.duplicate_of = Some("L1".into());
        let p = plan_outcome(&kept, Some(&named), &[], "acme/web#16", now);
        assert!(
            matches!(p.action, OutcomeAction::Reinforce { ref id, counts: true } if id == "L1")
        );
        // An id the model made up is not a duplicate.
        named.duplicate_of = Some("nope".into());
        let p = plan_outcome(&kept, Some(&named), &[], "acme/web#16", now);
        assert_eq!(p.action, OutcomeAction::Write);
        // A vague one is never kept.
        let p = plan_outcome(
            &kept,
            Some(&distilled("Be careful.", "x")),
            &[],
            "acme/web#17",
            now,
        );
        assert!(matches!(p.action, OutcomeAction::Skip(_)));
    }

    #[test]
    fn a_contradicted_lesson_is_demoted_then_no_longer_recalled() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap();
        let mut once = kept_rule("L1", "Use `useApi` in `apps/web`.", &["acme/web#12"]);
        let mut thrice = kept_rule(
            "L2",
            "Name migrations `NNNN_what.sql` in `db/migrations`.",
            &["acme/web#3", "acme/web#5", "acme/web#8"],
        );
        let kept = vec![once.clone(), thrice.clone()];
        // The outcome contradicts both, names one twice and one that is not
        // kept; it teaches no rule of its own.
        let ids = vec!["L1".to_string(), "L2".into(), "L1".into(), "ghost".into()];
        let p = plan_outcome(&kept, None, &ids, "acme/web#20", now);
        assert_eq!(p.contradict, vec!["L1".to_string(), "L2".to_string()]);
        assert!(matches!(p.action, OutcomeAction::Skip(_)));

        // Seen once, contradicted once: out. Seen three times: demoted.
        once.contradicted = 1;
        thrice.contradicted = 1;
        assert_eq!(weight_of(&once, now), 0.0);
        assert!(weight_of(&thrice, now) > 1.0 && weight_of(&thrice, now) < 2.0);
        let recalled: Vec<String> = recall(&[once.clone(), thrice.clone()], "coder", now)
            .into_iter()
            .map(|l| l.id)
            .collect();
        assert_eq!(recalled, vec!["L2"]);
        // A contradicted lesson is not proposed for the guide either.
        assert!(!guide_ready(&thrice, now));

        // The same pull request never counts against a lesson twice.
        once.evidence = vec![format!(
            "{}: people removed the hook",
            contradiction_mark("acme/web#20")
        )];
        let p = plan_outcome(&[once], None, &["L1".to_string()], "acme/web#20", now);
        assert!(p.contradict.is_empty());

        // An outcome never both confirms and contradicts the same lesson.
        let d = Distilled {
            duplicate_of: Some("L2".into()),
            ..distilled(
                "Name migrations `NNNN_what.sql` in `db/migrations`.",
                "The new file under `db/migrations` starts with four digits.",
            )
        };
        let p = plan_outcome(&[thrice], Some(&d), &["L2".to_string()], "acme/web#21", now);
        assert!(p.contradict.is_empty());
    }

    #[test]
    fn applying_the_plans_takes_a_lesson_from_one_outcome_to_the_guide() {
        let now_t = chrono::Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap();
        let now = "2026-10-02T00:00:00Z";
        let d = distilled(
            "Fetch through the shared `useApi` hook in `apps/web`, never with a bare `fetch` call.",
            "Search the diff for `fetch(` under `apps/web/src`: there should be none.",
        );
        let mut kept: Vec<Lesson> = Vec::new();
        let ev = vec!["PR #12: people edited it before merging".to_string()];
        let p = plan_outcome(&kept, Some(&d), &[], "acme/web#12", now_t);
        let id = apply_outcome(
            &mut kept,
            &p,
            Some(&d),
            "acme/web",
            SOURCE_EDITED,
            12,
            &ev,
            "L1",
            now,
        );
        assert_eq!(id.as_deref(), Some("L1"));
        assert_eq!((kept[0].seen, kept[0].source.as_str()), (1, "edited"));
        assert_eq!(kept[0].evidence, ev);
        assert!(
            !guide_ready(&kept[0], now_t),
            "one pull request is not enough"
        );
        // Delivered again: nothing changes.
        let p = plan_outcome(&kept, Some(&d), &[], "acme/web#12", now_t);
        apply_outcome(
            &mut kept,
            &p,
            Some(&d),
            "acme/web",
            SOURCE_EDITED,
            12,
            &ev,
            "L2",
            now,
        );
        assert_eq!((kept.len(), kept[0].seen), (1, 1));
        // The same thing on another pull request.
        let p = plan_outcome(&kept, Some(&d), &[], "acme/web#15", now_t);
        apply_outcome(
            &mut kept,
            &p,
            Some(&d),
            "acme/web",
            SOURCE_CLOSED,
            15,
            &ev,
            "L2",
            now,
        );
        assert_eq!((kept.len(), kept[0].seen), (1, 2));
        assert_eq!(kept[0].prs, vec!["acme/web#12", "acme/web#15"]);
        assert_eq!(kept[0].source, "edited", "where it was first learned stays");
        assert!(guide_ready(&kept[0], now_t));
        // People then undo exactly that: it is demoted, and out of the guide's reach.
        let p = plan_outcome(&kept, None, &["L1".to_string()], "acme/web#20", now_t);
        let ev20 =
            vec!["PR #20 in people's words: \"the hook is gone, call the client\"".to_string()];
        assert_eq!(
            apply_outcome(
                &mut kept,
                &p,
                None,
                "acme/web",
                SOURCE_EDITED,
                20,
                &ev20,
                "L3",
                now
            ),
            None
        );
        assert_eq!(kept[0].contradicted, 1);
        assert!(kept[0]
            .evidence
            .last()
            .unwrap()
            .starts_with("Contradicted by acme/web#20: PR #20"));
        assert!(!guide_ready(&kept[0], now_t));
        assert_eq!(
            recall(&kept, "coder", now_t).len(),
            1,
            "still recalled, lighter"
        );
        let p = plan_outcome(&kept, None, &["L1".to_string()], "acme/web#21", now_t);
        apply_outcome(
            &mut kept,
            &p,
            None,
            "acme/web",
            SOURCE_REVERTED,
            21,
            &ev20,
            "L3",
            now,
        );
        assert!(
            recall(&kept, "coder", now_t).is_empty(),
            "contradicted as often as confirmed: gone"
        );
    }

    #[test]
    fn a_lessons_lists_stay_bounded() {
        let many: Vec<String> = (0..30).map(|i| format!("line {i}")).collect();
        let kept = bounded(&many, &["newest".to_string()], MAX_EVIDENCE);
        assert_eq!(kept.len(), MAX_EVIDENCE);
        assert_eq!(kept.last().map(String::as_str), Some("newest"));
        assert_eq!(kept[0], "line 19");
        assert_eq!(bounded(&[], &["a".to_string()], 3), vec!["a"]);
        assert_eq!(reinforcing_lines(&many).len(), 3);
        assert!(contradiction_line("acme/web#20", &many)
            .starts_with("Contradicted by acme/web#20: line 0"));
    }

    #[test]
    fn a_lesson_reaches_the_guide_only_with_confidence() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap();
        let ready = kept_rule("L1", "Use `useApi`.", &["acme/web#1", "acme/web#2"]);
        assert!(guide_ready(&ready, now));
        // One pull request is not enough.
        assert!(!guide_ready(
            &kept_rule("L", "Use `useApi`.", &["acme/web#1"]),
            now
        ));
        // A pattern lesson (no check) is a record of what happened, not a rule.
        assert!(!guide_ready(
            &lesson(
                "A",
                "In `x`: PR #3 was reverted.",
                4,
                "2026-10-01T00:00:00Z"
            ),
            now
        ));
        let changes: [fn(&mut Lesson); 6] = [
            |l: &mut Lesson| l.reverted = true,
            |l: &mut Lesson| l.in_guide = true,
            |l: &mut Lesson| l.guide_declined = true,
            |l: &mut Lesson| l.contradicted = 1,
            |l: &mut Lesson| l.agent = "reviewer".into(),
            // Faded: last seen long ago.
            |l: &mut Lesson| l.last_seen_at = "2026-01-01T00:00:00Z".into(),
        ];
        for change in changes {
            let mut l = ready.clone();
            change(&mut l);
            assert!(!guide_ready(&l, now), "{l:?}");
        }
        // A lesson the guide carries is not repeated in the prompt.
        let mut in_guide = ready.clone();
        in_guide.in_guide = true;
        assert!(recall(&[in_guide], "coder", now).is_empty());
        // The prompt gives the check with the rule.
        let block = prompt_block(&[ready]);
        assert!(
            block.contains("- Use `useApi`. Check: Search the diff for `fetch(`. (seen 2×)"),
            "{block}"
        );
    }

    #[test]
    fn the_lightest_lesson_makes_room_for_a_new_one() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap();
        let mut kept: Vec<Lesson> = (0..MAX_KEPT)
            .map(|i| {
                kept_rule(
                    &format!("L{i}"),
                    &format!("Rule number{i} about `thing{i}` here."),
                    &["a/b#1", "a/b#2"],
                )
            })
            .collect();
        kept[7].last_seen_at = "2026-03-01T00:00:00Z".into();
        let d = distilled(
            "Fetch through the shared `useApi` hook in `apps/web`, never with a bare `fetch` call.",
            "Search the diff for `fetch(` under `apps/web/src`: there should be none.",
        );
        let p = plan_outcome(&kept, Some(&d), &[], "acme/web#12", now);
        assert_eq!(p.action, OutcomeAction::Write);
        assert_eq!(p.drop.as_deref(), Some("L7"));
    }
}
