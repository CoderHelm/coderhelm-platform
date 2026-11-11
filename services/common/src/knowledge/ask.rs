//! "Ask this knowledge base" on its page: the hourly limit on asks, the
//! passages a search found as the page shows them, and suggested questions.
//!
//! Suggested questions are built once per base from at most
//! [`MAX_TITLES`] item titles (about 3,000 tokens in, 150 out, on the
//! install's lighter model) and kept on their own record
//! (`KBSUGGEST#<kb>`). They are built again only when the base's searchable
//! items changed by more than [`REBUILD_CHANGE`] or a week went by. Without
//! a model, or when the call fails, they are fixed sentences over the
//! newest titles, at no cost.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::search::Hit;
use super::FileMeta;

/// Asks one person makes from a knowledge base page in an hour (searches
/// and answers together).
pub const MAX_ASKS_PER_HOUR: i64 = 20;
/// Titles a suggestion build reads at most.
pub const MAX_TITLES: usize = 150;
/// Questions suggested.
pub const SUGGESTED: usize = 6;
/// Passages one search on the page returns at most.
pub const MAX_PASSAGES: usize = 10;
/// Characters of a passage the page shows.
pub const PASSAGE_CHARS: usize = 1_600;
/// Share of searchable items that must change before questions are built
/// again.
pub const REBUILD_CHANGE: f32 = 0.10;
/// Questions are built again after this long anyway.
pub const REBUILD_SECS: i64 = 7 * 24 * 3600;
/// Hashes kept to tell how much a base's items changed.
const SIG_LEN: usize = 64;

/// The record that counts one person's asks in one hour.
pub fn ask_sk(email: &str, hour: i64) -> String {
    format!("KBASK#{}#{hour}", email.trim().to_lowercase())
}

/// The hour `now` (unix seconds) is in.
pub fn hour_of(now: i64) -> i64 {
    now.div_euclid(3600)
}

pub fn suggest_sk(kb_id: &str) -> String {
    format!("KBSUGGEST#{kb_id}")
}

/// Suggested questions for one base, cached.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suggested {
    pub questions: Vec<String>,
    /// Unix seconds.
    pub built_at: i64,
    /// `model` or `titles` (the fixed sentences).
    pub from: String,
    /// The smallest hashes of the searchable item ids it was built from.
    #[serde(default)]
    pub sig: Vec<u32>,
}

fn h32(s: &str) -> u32 {
    let d = Sha256::digest(s.as_bytes());
    u32::from_be_bytes([d[0], d[1], d[2], d[3]])
}

/// A small fingerprint of a set of ids: the [`SIG_LEN`] smallest hashes. Two
/// fingerprints estimate how much the sets overlap.
pub fn signature<'a>(ids: impl IntoIterator<Item = &'a str>) -> Vec<u32> {
    let mut v: Vec<u32> = ids.into_iter().map(h32).collect();
    v.sort_unstable();
    v.dedup();
    v.truncate(SIG_LEN);
    v
}

/// The estimated share of two sets in common (0 to 1), from their
/// fingerprints.
pub fn similarity(a: &[u32], b: &[u32]) -> f32 {
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    let mut union: Vec<u32> = a.iter().chain(b).copied().collect();
    union.sort_unstable();
    union.dedup();
    union.truncate(SIG_LEN);
    let both = union
        .iter()
        .filter(|h| a.binary_search(h).is_ok() && b.binary_search(h).is_ok())
        .count();
    both as f32 / union.len() as f32
}

/// Must the questions be built again?
pub fn needs_rebuild(cached: Option<&Suggested>, sig: &[u32], now: i64) -> bool {
    match cached {
        None => true,
        Some(c) => {
            c.questions.is_empty()
                || now - c.built_at >= REBUILD_SECS
                || similarity(&c.sig, sig) < 1.0 - REBUILD_CHANGE
        }
    }
}

/// The titles a build reads: newest first, taking from each space or folder
/// in turn so one big space doesn't fill the list. Each title carries its
/// breadcrumb.
pub fn pick_titles(files: &[FileMeta], max: usize) -> Vec<String> {
    let mut groups: Vec<(String, Vec<&FileMeta>)> = Vec::new();
    let mut ready: Vec<&FileMeta> = files
        .iter()
        .filter(|f| f.ready_version > 0 && !f.name.trim().is_empty())
        .collect();
    ready.sort_by(|a, b| b.as_of.cmp(&a.as_of).then(a.name.cmp(&b.name)));
    for f in ready {
        let top = f.path.split(" / ").next().unwrap_or("").to_string();
        match groups.iter_mut().find(|(t, _)| *t == top) {
            Some((_, v)) => v.push(f),
            None => groups.push((top, vec![f])),
        }
    }
    let mut out = Vec::new();
    let mut i = 0;
    while out.len() < max {
        let mut any = false;
        for (_, v) in &groups {
            if let Some(f) = v.get(i) {
                any = true;
                let title = if f.path.is_empty() {
                    f.name.clone()
                } else {
                    format!("{} › {}", f.path.replace(" / ", " › "), f.name)
                };
                out.push(crate::truncate_str(&title, 160).to_string());
                if out.len() == max {
                    break;
                }
            }
        }
        if !any {
            break;
        }
        i += 1;
    }
    out
}

/// Fixed questions over the newest titles: no model, no cost.
pub fn template_questions(files: &[FileMeta]) -> Vec<String> {
    let mut ready: Vec<&FileMeta> = files
        .iter()
        .filter(|f| f.ready_version > 0 && !f.name.trim().is_empty())
        .collect();
    ready.sort_by(|a, b| b.as_of.cmp(&a.as_of).then(a.name.cmp(&b.name)));
    let forms = [
        "What does {} say?",
        "Summarize {}.",
        "What are the key points of {}?",
    ];
    ready
        .iter()
        .take(SUGGESTED)
        .enumerate()
        .map(|(i, f)| {
            let name = crate::truncate_str(f.name.trim(), 80);
            forms[i % forms.len()].replace("{}", name)
        })
        .collect()
}

/// The instruction a build sends with the titles.
pub fn prompt(base_name: &str, titles: &[String]) -> String {
    format!(
        "These are page titles from a team knowledge base named \"{}\".\n\n{}\n\nWrite {} short questions a teammate would really ask that these pages answer. Use the team's own words from the titles. One question per line, no numbering, each under 90 characters, each ending with a question mark. Nothing else.",
        crate::truncate_str(base_name, 80),
        titles.iter().map(|t| format!("- {t}")).collect::<Vec<_>>().join("\n"),
        SUGGESTED
    )
}

/// The questions in a model's reply: one per line, numbering and bullets
/// dropped, at most [`SUGGESTED`], each a real question of a sane length.
pub fn parse_questions(reply: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in reply.lines() {
        let q = line
            .trim()
            .trim_start_matches(|c: char| {
                c.is_ascii_digit() || matches!(c, '.' | ')' | '-' | '*' | '•' | ' ')
            })
            .trim()
            .trim_matches('"')
            .to_string();
        if q.len() < 8 || q.chars().count() > 140 || !q.ends_with('?') {
            continue;
        }
        if !out.iter().any(|o| o.eq_ignore_ascii_case(&q)) {
            out.push(q);
        }
        if out.len() == SUGGESTED {
            break;
        }
    }
    out
}

pub fn suggest_record(team: &str, kb_id: &str, s: &Suggested) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".into(), AttributeValue::S(team.to_string())),
        ("sk".into(), AttributeValue::S(suggest_sk(kb_id))),
        (
            "data".into(),
            AttributeValue::S(serde_json::to_string(s).unwrap_or_default()),
        ),
    ])
}

/// One passage as the page shows it: its rank, its score against the best
/// (0 to 100), where it is and a link to the original.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Passage {
    pub rank: usize,
    /// The fused score against the best one found, 0 to 100.
    pub score_pct: u32,
    /// The fused score itself, for the curious.
    pub score: f32,
    pub file_id: String,
    pub title: String,
    pub loc: String,
    pub as_of: String,
    pub text: String,
    pub url: String,
    pub source_kind: String,
    pub authoritative: bool,
}

pub fn passages(hits: &[Hit], max: usize) -> Vec<Passage> {
    let best = hits.iter().map(|h| h.score).fold(0.0_f32, f32::max);
    hits.iter()
        .take(max)
        .enumerate()
        .map(|(i, h)| Passage {
            rank: i + 1,
            score_pct: if best > 0.0 {
                ((h.score / best) * 100.0).round().clamp(0.0, 100.0) as u32
            } else {
                0
            },
            score: h.score,
            file_id: h.file_id.clone(),
            title: h.file_name.clone(),
            loc: h.loc.clone(),
            as_of: h.as_of.clone(),
            text: crate::truncate_str(&h.text, PASSAGE_CHARS).to_string(),
            url: super::answer::original_url(&h.url).unwrap_or_default(),
            source_kind: h.source_kind.clone(),
            authoritative: h.authoritative,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(id: &str, name: &str, path: &str, as_of: &str) -> FileMeta {
        FileMeta {
            id: id.into(),
            name: name.into(),
            path: path.into(),
            as_of: as_of.into(),
            ready_version: 1,
            ..Default::default()
        }
    }

    #[test]
    fn questions_are_built_again_only_when_items_changed_enough_or_a_week_went_by() {
        let ids: Vec<String> = (0..500).map(|i| format!("i{i}")).collect();
        let sig = signature(ids.iter().map(String::as_str));
        let cached = Suggested {
            questions: vec!["Q?".into()],
            built_at: 1_000,
            from: "model".into(),
            sig: sig.clone(),
        };
        assert!(needs_rebuild(None, &sig, 1_000));
        assert!(!needs_rebuild(Some(&cached), &sig, 1_000 + 3600));
        // A few changed: the same questions.
        let few: Vec<String> = (10..510).map(|i| format!("i{i}")).collect();
        assert!(!needs_rebuild(
            Some(&cached),
            &signature(few.iter().map(String::as_str)),
            2_000
        ));
        // Most changed: built again.
        let most: Vec<String> = (300..800).map(|i| format!("i{i}")).collect();
        assert!(needs_rebuild(
            Some(&cached),
            &signature(most.iter().map(String::as_str)),
            2_000
        ));
        assert!(needs_rebuild(Some(&cached), &sig, 1_000 + REBUILD_SECS));
        let empty = Suggested {
            questions: vec![],
            ..cached
        };
        assert!(needs_rebuild(Some(&empty), &sig, 1_000));
        assert!((similarity(&sig, &sig) - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn titles_are_newest_first_and_spread_across_spaces_within_the_cap() {
        let mut files: Vec<FileMeta> = (0..300)
            .map(|i| {
                file(
                    &format!("a{i}"),
                    &format!("Big {i}"),
                    "Big space",
                    &format!("2026-01-{:02}", i % 28 + 1),
                )
            })
            .collect();
        files.push(file("s1", "Small one", "Small space / Team", "2025-01-01"));
        files.push(FileMeta {
            ready_version: 0,
            ..file("x", "Never read", "", "2026-12-01")
        });
        let t = pick_titles(&files, MAX_TITLES);
        assert_eq!(t.len(), MAX_TITLES);
        assert!(
            t.iter().any(|x| x == "Small space › Team › Small one"),
            "a small space still shows"
        );
        assert!(!t.iter().any(|x| x.contains("Never read")));
        assert!(t[0].contains("2") || t[0].starts_with("Big space"));
        // About 3,000 tokens in at most: 150 titles of 160 characters is the ceiling.
        assert!(t.iter().map(|x| x.len()).sum::<usize>() <= MAX_TITLES * 160);
    }

    #[test]
    fn a_reply_becomes_questions_and_templates_need_no_model() {
        let reply = "1. How do we backfill the orders table?\n- Who owns the dbt jobs?\nnot a question\n2) How do we backfill the orders table?\n\"Where is PII masked?\"";
        assert_eq!(
            parse_questions(reply),
            [
                "How do we backfill the orders table?",
                "Who owns the dbt jobs?",
                "Where is PII masked?"
            ]
        );
        let files = vec![
            file("1", "Runbook: failover", "", "2026-11-01"),
            file("2", "Old notes", "", "2020-01-01"),
        ];
        let q = template_questions(&files);
        assert_eq!(q[0], "What does Runbook: failover say?");
        assert_eq!(q.len(), 2);
        assert!(prompt("Warehouse", &["A".into()]).contains("- A"));
    }

    #[test]
    fn passages_are_scored_against_the_best_and_cut_to_size() {
        let hit = |s: f32| Hit {
            kb_id: "kb".into(),
            kb_name: "KB".into(),
            file_id: "f".into(),
            file_name: "Runbook".into(),
            loc: "Steps".into(),
            as_of: "2026-10-02".into(),
            text: "x".repeat(5_000),
            score: s,
            authoritative: false,
            ord: 0,
            url: "https://example.atlassian.net/wiki/x".into(),
            source_kind: "confluence".into(),
        };
        let p = passages(&[hit(0.03), hit(0.015)], MAX_PASSAGES);
        assert_eq!((p[0].rank, p[0].score_pct), (1, 100));
        assert_eq!((p[1].rank, p[1].score_pct), (2, 50));
        assert!(p[0].text.len() <= PASSAGE_CHARS);
        assert_eq!(passages(&[hit(0.03)], 0).len(), 0);
        assert_eq!(hour_of(7_199), 1);
        assert_eq!(ask_sk(" Ada@Example.com ", 5), "KBASK#ada@example.com#5");
    }
}
