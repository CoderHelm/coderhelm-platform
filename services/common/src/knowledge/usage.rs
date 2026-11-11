//! What each knowledge base is used for: one event per search of it, from
//! Chat, a knowledge base page, an agent run or an app through MCP, with
//! the question, what it found, what an answer cited and how it ended.
//! Kept 90 days. The page's usage section is worked out from these when
//! it is read ([`summarize`]).
//!
//! Record: `pk` = team, `sk` = `KBUSE#<kb>#<at>#<id>` (one per base the
//! search covered), `ttl`.

use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// How long a search is kept.
pub const USE_TTL_SECS: i64 = 90 * 24 * 3600;
/// Events one summary reads at most (the newest).
pub const MAX_READ: usize = 5_000;
/// Characters of a question kept.
pub const QUESTION_CHARS: usize = 300;
/// Pages one event keeps at most.
const MAX_PAGES: usize = 10;

/// Where a search came from.
pub mod surface {
    pub const CHAT: &str = "chat";
    /// "Ask this knowledge base" on its page.
    pub const PAGE: &str = "page";
    /// An app through the knowledge MCP server or an API key.
    pub const MCP: &str = "mcp";
    /// An agent run: `run:<agent>`.
    pub fn run(agent: &str) -> String {
        format!("run:{agent}")
    }
}

/// How a search ended.
pub mod outcome {
    pub const ANSWERED: &str = "answered";
    /// Passages came back (no answer is written on this surface).
    pub const FOUND: &str = "found";
    /// Nothing matched.
    pub const NO_HITS: &str = "no_hits";
    /// Passages came back, but nothing supported an answer.
    pub const NO_SOURCE: &str = "no_source";
    /// The asker rated the answer down.
    pub const RATED_DOWN: &str = "rated_down";
    pub const RATED_UP: &str = "rated_up";
}

/// One page a search found.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Found {
    pub file_id: String,
    pub title: String,
    #[serde(default)]
    pub cited: bool,
}

/// One search of one base.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UseEvent {
    pub kb_id: String,
    /// [`surface`].
    pub surface: String,
    /// Who asked (an email), or the app's name.
    pub actor: String,
    /// The Chat thread or the run, to link to.
    #[serde(default)]
    pub ref_id: String,
    pub question: String,
    #[serde(default)]
    pub pages: Vec<Found>,
    /// [`outcome`].
    pub outcome: String,
    /// RFC 3339.
    pub at: String,
}

pub fn use_prefix(kb_id: &str) -> String {
    format!("KBUSE#{kb_id}#")
}

/// The records of one search: one per base it covered.
pub fn records(team: &str, ev: &UseEvent, id: &str) -> HashMap<String, AttributeValue> {
    let at = chrono::DateTime::parse_from_rfc3339(&ev.at)
        .map(|t| t.timestamp())
        .unwrap_or(0);
    HashMap::from([
        ("pk".into(), AttributeValue::S(team.to_string())),
        (
            "sk".into(),
            AttributeValue::S(format!("{}{}#{id}", use_prefix(&ev.kb_id), ev.at)),
        ),
        (
            "data".into(),
            AttributeValue::S(serde_json::to_string(ev).unwrap_or_default()),
        ),
        (
            "ttl".into(),
            AttributeValue::N((at + USE_TTL_SECS).to_string()),
        ),
    ])
}

/// The events of one search, one per base, from its hits (`(kb, file id,
/// title)`) and the titles an answer cited.
#[allow(clippy::too_many_arguments)]
pub fn events(
    kb_ids: &[String],
    surface: &str,
    actor: &str,
    ref_id: &str,
    question: &str,
    hits: &[(String, String, String)],
    cited_titles: &[String],
    outcome: &str,
    at: &str,
) -> Vec<UseEvent> {
    kb_ids
        .iter()
        .map(|kb| {
            let mut pages: Vec<Found> = Vec::new();
            for (k, f, t) in hits.iter().filter(|(k, _, _)| k == kb) {
                let _ = k;
                if pages.iter().any(|p| p.file_id == *f) || pages.len() >= MAX_PAGES {
                    continue;
                }
                pages.push(Found {
                    file_id: f.clone(),
                    title: t.clone(),
                    cited: cited_titles.iter().any(|c| c == t),
                });
            }
            UseEvent {
                kb_id: kb.clone(),
                surface: surface.to_string(),
                actor: actor.to_string(),
                ref_id: ref_id.to_string(),
                question: crate::truncate_str(question.trim(), QUESTION_CHARS).to_string(),
                outcome: if outcome == outcome::ANSWERED && pages.is_empty() {
                    outcome::NO_HITS.to_string()
                } else {
                    outcome.to_string()
                },
                pages,
                at: at.to_string(),
            }
        })
        .collect()
}

/// Keeps a search's events. Never fails the search: a lost event is logged.
pub async fn log(dynamo: &aws_sdk_dynamodb::Client, table: &str, team: &str, evs: &[UseEvent]) {
    for ev in evs {
        let id = format!("{:08x}", rand_id());
        if let Err(e) = dynamo
            .put_item()
            .table_name(table)
            .set_item(Some(records(team, ev, &id)))
            .send()
            .await
        {
            tracing::warn!(error = %e, "Knowledge use not recorded");
        }
    }
}

fn rand_id() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    n ^ std::process::id().rotate_left(16)
}

/// A base's newest events, newest first, at most [`MAX_READ`].
pub async fn read(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team: &str,
    kb_id: &str,
) -> Result<Vec<UseEvent>, String> {
    let mut out = Vec::new();
    let mut start = None;
    loop {
        let r = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team.to_string()))
            .expression_attribute_values(":p", AttributeValue::S(use_prefix(kb_id)))
            .scan_index_forward(false)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("knowledge use: {e}"))?;
        out.extend(r.items().iter().filter_map(super::read_data::<UseEvent>));
        match r.last_evaluated_key() {
            Some(k) if out.len() < MAX_READ => start = Some(k.clone()),
            _ => break,
        }
    }
    out.truncate(MAX_READ);
    Ok(out)
}

fn secs(at: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(at)
        .map(|t| t.timestamp())
        .unwrap_or(0)
}

/// The page's usage section from a base's events: searches this week and
/// last, per day and per surface; the most cited pages (30 days); the
/// searchable pages nothing found in 90 days; the runs and chats that used
/// it; and the questions without a good answer. Question text and who
/// asked are left out unless `manager`; a chat links only for its asker.
pub fn summarize(
    evs: &[UseEvent],
    searchable: &[(String, String)],
    now: i64,
    me: &str,
    manager: bool,
) -> Value {
    let day = 86_400;
    let week: Vec<&UseEvent> = evs
        .iter()
        .filter(|e| e.outcome != outcome::RATED_UP && e.outcome != outcome::RATED_DOWN)
        .filter(|e| now - secs(&e.at) < 7 * day)
        .collect();
    let last_week = evs
        .iter()
        .filter(|e| e.outcome != outcome::RATED_UP && e.outcome != outcome::RATED_DOWN)
        .filter(|e| (7 * day..14 * day).contains(&(now - secs(&e.at))))
        .count();
    let mut by_day = [0u32; 7];
    for e in &week {
        let ago = ((now - secs(&e.at)) / day).clamp(0, 6) as usize;
        by_day[6 - ago] += 1;
    }
    let mut by_surface: BTreeMap<String, u32> = BTreeMap::new();
    for e in &week {
        *by_surface.entry(e.surface.clone()).or_default() += 1;
    }
    // Pages, 30 days.
    let mut pages: HashMap<&str, (String, u32, u32)> = HashMap::new();
    for e in evs.iter().filter(|e| now - secs(&e.at) < 30 * day) {
        for p in &e.pages {
            let ent = pages
                .entry(p.file_id.as_str())
                .or_insert_with(|| (p.title.clone(), 0, 0));
            ent.1 += 1;
            if p.cited {
                ent.2 += 1;
            }
        }
    }
    let mut top: Vec<(&str, (String, u32, u32))> = pages.into_iter().collect();
    top.sort_by(|a, b| {
        (b.1 .2, b.1 .1)
            .cmp(&(a.1 .2, a.1 .1))
            .then(a.1 .0.cmp(&b.1 .0))
    });
    let top: Vec<Value> = top
        .iter()
        .take(10)
        .map(|(id, (t, found, cited))| json!({ "file_id": id, "title": t, "found": found, "cited": cited }))
        .collect();
    // Never found, 90 days (events only go back that far).
    let seen: std::collections::HashSet<&str> = evs
        .iter()
        .flat_map(|e| e.pages.iter().map(|p| p.file_id.as_str()))
        .collect();
    let never: Vec<&(String, String)> = searchable
        .iter()
        .filter(|(id, _)| !seen.contains(id.as_str()))
        .collect();
    // Runs and chats that used it.
    let used_in: Vec<Value> = evs
        .iter()
        .filter(|e| {
            e.surface != surface::PAGE
                && e.outcome != outcome::RATED_DOWN
                && e.outcome != outcome::RATED_UP
                && !e.ref_id.is_empty()
        })
        .take(10)
        .map(|e| {
            let mine = e.actor.eq_ignore_ascii_case(me);
            let href = if e.surface.starts_with("run:") {
                Some(format!("/runs/detail?id={}", e.ref_id))
            } else if e.surface == surface::CHAT && mine {
                Some(format!("/chat?thread={}", e.ref_id))
            } else {
                None
            };
            json!({
                "surface": e.surface,
                "actor": if manager || mine { e.actor.clone() } else { String::new() },
                "href": href,
                "at": e.at,
            })
        })
        .collect();
    // No good answer: grouped by the question's words.
    let mut gaps: BTreeMap<String, (String, u32, String, String)> = BTreeMap::new();
    for e in evs.iter().filter(|e| {
        matches!(
            e.outcome.as_str(),
            outcome::NO_HITS | outcome::NO_SOURCE | outcome::RATED_DOWN
        ) && now - secs(&e.at) < 30 * day
    }) {
        let key = e
            .question
            .to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| w.len() > 2)
            .collect::<Vec<_>>()
            .join(" ");
        if key.is_empty() {
            continue;
        }
        let g = gaps
            .entry(key)
            .or_insert_with(|| (e.question.clone(), 0, e.at.clone(), e.outcome.clone()));
        g.1 += 1;
        if e.at > g.2 {
            g.2 = e.at.clone();
        }
    }
    let mut gaps: Vec<(String, u32, String, String)> = gaps.into_values().collect();
    gaps.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));
    let gap_count = gaps.len();
    json!({
        "week": week.len(),
        "last_week": last_week,
        "by_day": by_day,
        "by_surface": by_surface,
        "top_pages": top,
        "never_found": never.len(),
        "never_found_sample": never.iter().take(20).map(|(id, t)| json!({ "file_id": id, "title": t })).collect::<Vec<_>>(),
        "used_in": used_in,
        "no_answer": if manager {
            gaps.iter().take(20).map(|(q, n, at, why)| json!({ "question": q, "times": n, "last_at": at, "why": why })).collect::<Vec<_>>()
        } else {
            Vec::new()
        },
        "no_answer_count": gap_count,
        "kept_days": USE_TTL_SECS / day,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> String {
        chrono::DateTime::from_timestamp(secs, 0)
            .unwrap()
            .to_rfc3339()
    }

    const NOW: i64 = 1_800_000_000;
    const DAY: i64 = 86_400;

    fn hit(kb: &str, f: &str, t: &str) -> (String, String, String) {
        (kb.into(), f.into(), t.into())
    }

    #[test]
    fn a_search_becomes_one_event_per_base_with_what_it_found_and_cited() {
        let evs = events(
            &["kb1".into(), "kb2".into()],
            surface::CHAT,
            "ada@example.com",
            "th1",
            "  How do we backfill?  ",
            &[
                hit("kb1", "f1", "Backfills"),
                hit("kb1", "f1", "Backfills"),
                hit("kb1", "f2", "Orders"),
                hit("kb2", "g1", "Other"),
            ],
            &["Backfills".into()],
            outcome::ANSWERED,
            &at(NOW),
        );
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].question, "How do we backfill?");
        assert_eq!(evs[0].pages.len(), 2, "one entry per page");
        assert!(evs[0].pages[0].cited && !evs[0].pages[1].cited);
        let lonely = events(
            &["kb3".into()],
            surface::CHAT,
            "a",
            "t",
            "q",
            &[],
            &[],
            outcome::ANSWERED,
            &at(NOW),
        );
        assert_eq!(
            lonely[0].outcome,
            outcome::NO_HITS,
            "an answer from another base found nothing here"
        );
        assert_eq!(surface::run("coder"), "run:coder");
        let rec = records("t", &evs[0], "abcd");
        assert!(rec["sk"].as_s().unwrap().starts_with(&use_prefix("kb1")));
        assert_eq!(
            rec["ttl"].as_n().unwrap(),
            &(NOW + USE_TTL_SECS).to_string()
        );
    }

    fn ev(
        surface: &str,
        actor: &str,
        q: &str,
        pages: &[(&str, bool)],
        outcome: &str,
        ago: i64,
    ) -> UseEvent {
        UseEvent {
            kb_id: "kb".into(),
            surface: surface.into(),
            actor: actor.into(),
            ref_id: "r1".into(),
            question: q.into(),
            pages: pages
                .iter()
                .map(|(f, c)| Found {
                    file_id: (*f).into(),
                    title: format!("Page {f}"),
                    cited: *c,
                })
                .collect(),
            outcome: outcome.into(),
            at: at(NOW - ago),
        }
    }

    #[test]
    fn the_summary_counts_ranks_and_keeps_question_words_for_managers() {
        let evs = vec![
            ev(
                surface::CHAT,
                "ada@x.com",
                "backfill orders",
                &[("a", true), ("b", false)],
                outcome::ANSWERED,
                3600,
            ),
            ev(
                &surface::run("coder"),
                "",
                "deploy steps",
                &[("a", true)],
                outcome::FOUND,
                DAY + 10,
            ),
            ev(
                surface::MCP,
                "app@x.com",
                "retention for raw events?",
                &[],
                outcome::NO_HITS,
                2 * DAY,
            ),
            ev(
                surface::PAGE,
                "ben@x.com",
                "Retention for raw events",
                &[],
                outcome::NO_HITS,
                3 * DAY,
            ),
            ev(
                surface::CHAT,
                "ben@x.com",
                "who owns dbt",
                &[("b", false)],
                outcome::RATED_DOWN,
                3 * DAY,
            ),
            ev(
                surface::CHAT,
                "ada@x.com",
                "old",
                &[("c", true)],
                outcome::ANSWERED,
                9 * DAY,
            ),
        ];
        let searchable = vec![
            ("a".to_string(), "Page a".to_string()),
            ("z".to_string(), "Never found".to_string()),
        ];
        let s = summarize(&evs, &searchable, NOW, "ada@x.com", true);
        assert_eq!(s["week"], 4, "ratings aren't searches");
        assert_eq!(s["last_week"], 1);
        assert_eq!(
            s["by_day"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .sum::<u64>(),
            4
        );
        assert_eq!(s["by_day"][6], 1, "today is last");
        assert_eq!(s["by_surface"]["chat"], 1);
        assert_eq!(s["by_surface"]["run:coder"], 1);
        assert_eq!(s["top_pages"][0]["file_id"], "a");
        assert_eq!(s["top_pages"][0]["cited"], 2);
        assert_eq!(s["never_found"], 1);
        assert_eq!(s["never_found_sample"][0]["title"], "Never found");
        let gaps = s["no_answer"].as_array().unwrap();
        assert_eq!(
            gaps[0]["times"], 2,
            "the same question in other words groups"
        );
        assert_eq!(s["no_answer_count"], 2);
        // Chats link only for their asker; runs link for everyone.
        let used = s["used_in"].as_array().unwrap();
        assert_eq!(used[0]["href"], "/chat?thread=r1");
        assert!(used.iter().any(|u| u["href"] == "/runs/detail?id=r1"));
        assert!(!used.iter().any(|u| u["surface"] == surface::PAGE));

        let reader = summarize(&evs, &searchable, NOW, "cy@x.com", false);
        assert!(
            reader["no_answer"].as_array().unwrap().is_empty(),
            "question words are for managers"
        );
        assert_eq!(reader["no_answer_count"], 2);
        assert!(reader["used_in"][0]["href"].is_null());
        assert_eq!(reader["used_in"][0]["actor"], "");
    }
}
