//! What each of a team's reviewers repeatedly asks for on CoderHelm's pull
//! requests, so the next change arrives the way its likely reviewer wants
//! it.
//!
//! When one of CoderHelm's pull requests closes, the worker reads what
//! people wrote in their reviews of it and distils each reviewer's asks
//! into general ones ("add a test for the error path of a new function").
//! An ask is kept per reviewer login within the team ([`Pref`],
//! `REVIEWERPREF#<login>#<id>`), counted once per pull request, and given
//! to the coder and to CoderHelm's review of its own pull request only when
//! it was seen on at least [`MIN_SEEN`] pull requests and that reviewer is
//! a likely reviewer of the change ([`likely_reviewers`]): a code owner of
//! the touched files, or someone who recently reviewed that part of the
//! repository (`REVIEWERSEEN#<repo>#<login>`).
//!
//! Only review-relevant preferences are stored, nothing personal: an ask
//! must be about the code in one of a fixed list of categories
//! ([`CATEGORIES`]), carries no mention, address or link, and is dropped
//! when it is about a person ([`clean`]). What is kept is the ask, its
//! category, and the pull requests it was seen on; never the reviewer's
//! comments themselves. A team can list what is kept and forget a reviewer.
//! Asks fade like lessons do ([`HALF_LIFE_DAYS`]).

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub const PREF_PREFIX: &str = "REVIEWERPREF#";
pub const SEEN_PREFIX: &str = "REVIEWERSEEN#";

/// What an ask may be about.
pub const CATEGORIES: &[&str] = &[
    "tests",
    "error_handling",
    "naming",
    "structure",
    "style",
    "types",
    "docs",
    "performance",
    "security",
    "scope",
    "api",
    "logging",
    "accessibility",
    "description",
];

/// Pull requests an ask must be seen on before it is given to an agent.
pub const MIN_SEEN: u64 = 2;
/// Days for an ask's weight to halve without being seen again.
pub const HALF_LIFE_DAYS: f64 = 90.0;
/// Weight under which an ask is no longer given.
pub const MIN_WEIGHT: f64 = 0.5;
/// Most asks kept per reviewer.
pub const MAX_PER_REVIEWER: usize = 20;
/// Most asks of one reviewer put in a prompt, and most reviewers.
pub const MAX_PROMPT_ASKS: usize = 5;
pub const MAX_PROMPT_REVIEWERS: usize = 3;
/// Longest ask.
pub const MAX_ASK_CHARS: usize = 240;
const MIN_ASK_CHARS: usize = 12;
/// Word overlap at which two asks of one reviewer are the same ask.
pub const SAME_AT: f64 = 0.6;
/// A reviewer who has not reviewed here for this long is not a likely one.
pub const RECENT_DAYS: i64 = 120;
/// Most pull requests named on one ask.
pub const MAX_PRS: usize = 10;

/// A GitHub login as stored: lower case. None when it is not a plain
/// person's login (a bot, a team, anything with other characters).
pub fn login_key(login: &str) -> Option<String> {
    let l = login.trim().trim_start_matches('@').to_ascii_lowercase();
    let plain = !l.is_empty()
        && l.len() <= 39
        && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        && !l.starts_with('-');
    (plain && !l.contains("coderhelm")).then_some(l)
}

pub fn pref_sk(login: &str, id: &str) -> String {
    format!("{PREF_PREFIX}{login}#{id}")
}

pub fn seen_sk(repo: &str, login: &str) -> String {
    format!("{SEEN_PREFIX}{repo}#{login}")
}

/// One thing a reviewer repeatedly asks for.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Pref {
    pub id: String,
    pub login: String,
    pub text: String,
    pub category: String,
    /// Pull requests it was seen on.
    pub seen: u64,
    /// Those pull requests (`owner/name#12`), newest last.
    pub prs: Vec<String>,
    pub created_at: String,
    pub last_seen_at: String,
}

/// An ask as the model distilled it from one pull request's reviews.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Ask {
    #[serde(default)]
    pub reviewer: String,
    #[serde(default)]
    pub ask: String,
    #[serde(default)]
    pub category: String,
    /// The id of a kept ask of this reviewer that says the same.
    #[serde(default)]
    pub same_as: Option<String>,
}

/// Words that make an ask about a person or their circumstances, not about
/// the code. An ask with one (as a whole word or phrase) is dropped whole.
const PERSONAL: &[&str] = &[
    "vacation",
    "holiday",
    "holidays",
    "sick",
    "illness",
    "health",
    "family",
    "salary",
    "mood",
    "angry",
    "annoyed",
    "rude",
    "tired",
    "lazy",
    "personality",
    "attitude",
    "timezone",
    "time zone",
    "working hours",
    "weekend",
    "weekends",
    "lives in",
    "he",
    "she",
    "him",
    "his",
    "her",
];

/// Lower case, every run of other characters one space, padded: a word or
/// phrase is in it when ` word ` is.
fn spaced(text: &str) -> String {
    let words: Vec<String> = text
        .to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_string)
        .collect();
    format!(" {} ", words.join(" "))
}

fn strip_refs(text: &str) -> String {
    text.split_whitespace()
        .filter(|w| {
            let w = w.trim_matches(|c: char| ",.;:()".contains(c));
            !(w.starts_with('@')
                || w.contains("://")
                || (w.contains('@') && w.contains('.'))
                || w.starts_with("www."))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The ask as it may be stored, or None: a plain person's login, a known
/// category, a text about the code with mentions, addresses and links
/// taken out, and nothing personal.
pub fn clean(a: &Ask) -> Option<Ask> {
    let reviewer = login_key(&a.reviewer)?;
    let category = a
        .category
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_");
    if !CATEGORIES.contains(&category.as_str()) {
        return None;
    }
    let text = strip_refs(a.ask.trim());
    let low = spaced(&text);
    if text.len() < MIN_ASK_CHARS || PERSONAL.iter().any(|p| low.contains(&format!(" {p} "))) {
        return None;
    }
    // The reviewer's own name is not part of what they ask for.
    if low.contains(&spaced(&reviewer)) {
        return None;
    }
    Some(Ask {
        reviewer,
        ask: crate::truncate_str(&text, MAX_ASK_CHARS).to_string(),
        category,
        same_as: a.same_as.clone().filter(|s| !s.trim().is_empty()),
    })
}

/// The ask's weight at `now`: pull requests seen, halved every half-life
/// since the last one.
pub fn weight(p: &Pref, now: chrono::DateTime<chrono::Utc>) -> f64 {
    let age_days = chrono::DateTime::parse_from_rfc3339(&p.last_seen_at)
        .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds() as f64 / 86_400.0)
        .unwrap_or(f64::INFINITY)
        .max(0.0);
    p.seen as f64 * 0.5f64.powf(age_days / HALF_LIFE_DAYS)
}

/// What to do with one cleaned ask.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// The reviewer already has it. `counts`: this pull request is new for
    /// it.
    Reinforce { id: String, counts: bool },
    /// A new ask; `drop` is the reviewer's lightest one when they are at
    /// the cap.
    Write { pref: Pref, drop: Option<String> },
}

/// Pure: how a cleaned ask lands among the reviewer's kept ones (`kept`:
/// that reviewer's only).
pub fn plan(
    kept: &[Pref],
    a: &Ask,
    pr_key: &str,
    id: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Plan {
    let mine: Vec<&Pref> = kept.iter().filter(|p| p.login == a.reviewer).collect();
    let named = a
        .same_as
        .as_deref()
        .and_then(|id| mine.iter().find(|p| p.id == id).copied());
    let similar = || {
        mine.iter()
            .map(|p| (crate::rewards::lessons::similarity(&a.ask, &p.text), *p))
            .filter(|(s, _)| *s >= SAME_AT)
            .max_by(|x, y| x.0.partial_cmp(&y.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, p)| p)
    };
    if let Some(p) = named.or_else(similar) {
        return Plan::Reinforce {
            id: p.id.clone(),
            counts: !p.prs.iter().any(|x| x == pr_key),
        };
    }
    let drop = if mine.len() >= MAX_PER_REVIEWER {
        mine.iter()
            .min_by(|x, y| {
                weight(x, now)
                    .partial_cmp(&weight(y, now))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|p| p.id.clone())
    } else {
        None
    };
    let at = now.to_rfc3339();
    Plan::Write {
        pref: Pref {
            id: id.to_string(),
            login: a.reviewer.clone(),
            text: a.ask.clone(),
            category: a.category.clone(),
            seen: 1,
            prs: vec![pr_key.to_string()],
            created_at: at.clone(),
            last_seen_at: at,
        },
        drop,
    }
}

/// Pure: the kept asks after one plan, as the store writes it.
pub fn apply_plan(kept: &mut Vec<Pref>, plan: &Plan, pr_key: &str, now: &str) {
    match plan {
        Plan::Reinforce { counts: false, .. } => {}
        Plan::Reinforce { id, counts: true } => {
            if let Some(p) = kept.iter_mut().find(|p| &p.id == id) {
                p.seen += 1;
                p.last_seen_at = now.to_string();
                p.prs.push(pr_key.to_string());
            }
        }
        Plan::Write { pref, drop } => {
            if let Some(id) = drop {
                kept.retain(|p| &p.id != id);
            }
            kept.push(pref.clone());
        }
    }
}

/// The asks given to an agent for `login`: seen on enough pull requests and
/// still heavy enough, heaviest first.
pub fn active(prefs: &[Pref], login: &str, now: chrono::DateTime<chrono::Utc>) -> Vec<Pref> {
    let mut live: Vec<(f64, &Pref)> = prefs
        .iter()
        .filter(|p| p.login == login && p.seen >= MIN_SEEN)
        .map(|p| (weight(p, now), p))
        .filter(|(w, _)| *w >= MIN_WEIGHT)
        .collect();
    live.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.id.cmp(&b.1.id))
    });
    live.into_iter()
        .take(MAX_PROMPT_ASKS)
        .map(|(_, p)| p.clone())
        .collect()
}

/// A reviewer's record on one repository: the parts of it they reviewed
/// CoderHelm's pull requests in, and when last.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SeenReviewer {
    pub repo: String,
    pub login: String,
    pub areas: BTreeSet<String>,
    pub reviews: u64,
    pub last_at: String,
}

/// The people (not teams) a CODEOWNERS file names for `paths`; with no
/// paths, the owners of the catch-all line.
pub fn code_owners(codeowners: &str, paths: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut add = |owners: Option<String>| {
        for o in owners.unwrap_or_default().split_whitespace() {
            // `@org/team` is a team: no single person's asks apply.
            if o.contains('/') {
                continue;
            }
            if let Some(l) = login_key(o) {
                if !out.contains(&l) {
                    out.push(l);
                }
            }
        }
    };
    if paths.is_empty() {
        add(crate::e2e::owners::owners_of(codeowners, "."));
    }
    for p in paths {
        add(crate::e2e::owners::owners_of(codeowners, p));
    }
    out
}

/// Pure: who is likely to review a change to `paths` in a repository, most
/// likely first: the code owners of those files, then the people who
/// recently reviewed that part of it (with no paths yet: anyone who
/// recently reviewed the repository, most recent first).
pub fn likely_reviewers(
    codeowners: Option<&str>,
    paths: &[String],
    seen: &[SeenReviewer],
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<String> {
    let mut out = codeowners
        .map(|c| code_owners(c, paths))
        .unwrap_or_default();
    let areas: BTreeSet<String> = paths
        .iter()
        .map(|p| crate::rewards::event::file_area(p))
        .collect();
    let since = (now - chrono::Duration::days(RECENT_DAYS)).to_rfc3339();
    let mut recent: Vec<&SeenReviewer> = seen
        .iter()
        .filter(|s| s.last_at.as_str() >= since.as_str())
        .filter(|s| paths.is_empty() || s.areas.iter().any(|a| areas.contains(a)))
        .collect();
    recent.sort_by(|a, b| {
        b.last_at
            .cmp(&a.last_at)
            .then(b.reviews.cmp(&a.reviews))
            .then(a.login.cmp(&b.login))
    });
    for s in recent {
        if !out.contains(&s.login) {
            out.push(s.login.clone());
        }
    }
    out.truncate(MAX_PROMPT_REVIEWERS);
    out
}

/// The prompt block for the likely reviewers' asks; empty when none of
/// them has an ask that is given.
pub fn prompt_block(
    reviewers: &[String],
    prefs: &[Pref],
    now: chrono::DateTime<chrono::Utc>,
) -> String {
    let mut lines = Vec::new();
    for r in reviewers.iter().take(MAX_PROMPT_REVIEWERS) {
        for p in active(prefs, r, now) {
            lines.push(format!(
                "- {r}: {} ({}, on {} pull requests)",
                p.text.trim(),
                p.category,
                p.seen
            ));
        }
    }
    if lines.is_empty() {
        return String::new();
    }
    format!(
        "\n\n## What the likely reviewers of this change ask for\nEach line is something that reviewer asked for on several earlier pull requests here. Where it applies to this change, do it before they have to ask again; where it does not apply, leave it.\n{}",
        lines.join("\n")
    )
}

/// Pure: what the dashboard lists, grouped by reviewer, heaviest first.
pub fn view(prefs: &[Pref], now: chrono::DateTime<chrono::Utc>) -> serde_json::Value {
    let mut by: BTreeMap<String, Vec<&Pref>> = BTreeMap::new();
    for p in prefs {
        by.entry(p.login.clone()).or_default().push(p);
    }
    let reviewers: Vec<serde_json::Value> = by
        .into_iter()
        .map(|(login, mut list)| {
            list.sort_by(|a, b| {
                weight(b, now)
                    .partial_cmp(&weight(a, now))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let asks: Vec<serde_json::Value> = list
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "id": p.id,
                        "text": p.text,
                        "category": p.category,
                        "seen": p.seen,
                        "prs": p.prs,
                        "last_seen_at": p.last_seen_at,
                        "given": p.seen >= MIN_SEEN && weight(p, now) >= MIN_WEIGHT,
                    })
                })
                .collect();
            serde_json::json!({"login": login, "asks": asks})
        })
        .collect();
    serde_json::json!({"min_seen": MIN_SEEN, "reviewers": reviewers})
}

// ── DynamoDB ─────────────────────────────────────────────────────────

type Item = HashMap<String, AttributeValue>;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn gs(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn gu(i: &Item, k: &str) -> u64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

impl Pref {
    pub fn to_item(&self, team: &str) -> Item {
        Item::from([
            ("pk".to_string(), s(team)),
            ("sk".to_string(), s(&pref_sk(&self.login, &self.id))),
            ("record_type".to_string(), s("reviewer_pref")),
            ("id".to_string(), s(&self.id)),
            ("login".to_string(), s(&self.login)),
            ("text".to_string(), s(&self.text)),
            ("category".to_string(), s(&self.category)),
            ("seen".to_string(), AttributeValue::N(self.seen.to_string())),
            (
                "prs".to_string(),
                AttributeValue::L(self.prs.iter().map(|p| s(p)).collect()),
            ),
            ("created_at".to_string(), s(&self.created_at)),
            ("last_seen_at".to_string(), s(&self.last_seen_at)),
        ])
    }

    pub fn from_item(i: &Item) -> Option<Pref> {
        if !gs(i, "sk").starts_with(PREF_PREFIX) || gs(i, "text").is_empty() {
            return None;
        }
        Some(Pref {
            id: gs(i, "id"),
            login: gs(i, "login"),
            text: gs(i, "text"),
            category: gs(i, "category"),
            seen: gu(i, "seen").max(1),
            prs: i
                .get("prs")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
                .unwrap_or_default(),
            created_at: gs(i, "created_at"),
            last_seen_at: gs(i, "last_seen_at"),
        })
    }
}

fn seen_of(i: &Item) -> Option<SeenReviewer> {
    if !gs(i, "sk").starts_with(SEEN_PREFIX) {
        return None;
    }
    Some(SeenReviewer {
        repo: gs(i, "repo"),
        login: gs(i, "login"),
        areas: i
            .get("areas")
            .and_then(|v| v.as_ss().ok())
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default(),
        reviews: gu(i, "reviews"),
        last_at: gs(i, "last_at"),
    })
}

/// The settings table, one team at a time.
pub struct Store {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub table: String,
}

impl Store {
    pub fn new(dynamo: &aws_sdk_dynamodb::Client, table: &str) -> Self {
        Self {
            dynamo: dynamo.clone(),
            table: table.to_string(),
        }
    }

    async fn query(&self, team: &str, prefix: &str) -> Result<Vec<Item>, String> {
        let mut out = vec![];
        let mut start: Option<Item> = None;
        for _ in 0..10 {
            let page = self
                .dynamo
                .query()
                .table_name(&self.table)
                .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                .expression_attribute_values(":pk", s(team))
                .expression_attribute_values(":p", s(prefix))
                .set_exclusive_start_key(start.take())
                .send()
                .await
                .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())?;
            out.extend(page.items.unwrap_or_default());
            match page.last_evaluated_key {
                Some(k) if !k.is_empty() => start = Some(k),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Every kept ask of the team, or one reviewer's.
    pub async fn prefs(&self, team: &str, login: Option<&str>) -> Result<Vec<Pref>, String> {
        let prefix = match login {
            Some(l) => format!("{PREF_PREFIX}{l}#"),
            None => PREF_PREFIX.to_string(),
        };
        Ok(self
            .query(team, &prefix)
            .await?
            .iter()
            .filter_map(Pref::from_item)
            .collect())
    }

    /// The reviewers seen on one repository.
    pub async fn seen(&self, team: &str, repo: &str) -> Result<Vec<SeenReviewer>, String> {
        Ok(self
            .query(team, &format!("{SEEN_PREFIX}{repo}#"))
            .await?
            .iter()
            .filter_map(seen_of)
            .collect())
    }

    /// Notes that `login` reviewed a pull request touching `areas` of
    /// `repo` (counted by the caller once per pull request).
    pub async fn note_seen(
        &self,
        team: &str,
        repo: &str,
        login: &str,
        areas: &BTreeSet<String>,
        at: &str,
    ) -> Result<(), String> {
        let mut u = self
            .dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(&seen_sk(repo, login)))
            .expression_attribute_names("#rt", "record_type")
            .expression_attribute_names("#repo", "repo")
            .expression_attribute_names("#login", "login")
            .expression_attribute_names("#last", "last_at")
            .expression_attribute_names("#reviews", "reviews")
            .expression_attribute_values(":rt", s("reviewer_seen"))
            .expression_attribute_values(":r", s(repo))
            .expression_attribute_values(":l", s(login))
            .expression_attribute_values(":t", s(at))
            .expression_attribute_values(":one", AttributeValue::N("1".into()));
        let areas: Vec<String> = areas.iter().filter(|a| !a.is_empty()).cloned().collect();
        let add_areas = if areas.is_empty() {
            ""
        } else {
            u = u
                .expression_attribute_names("#areas", "areas")
                .expression_attribute_values(":a", AttributeValue::Ss(areas));
            ", #areas :a"
        };
        u.update_expression(format!(
            "SET #rt = :rt, #repo = :r, #login = :l, #last = :t ADD #reviews :one{add_areas}"
        ))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())
    }

    /// Applies one plan.
    pub async fn apply(
        &self,
        team: &str,
        login: &str,
        plan: &Plan,
        pr_key: &str,
        now: &str,
    ) -> Result<(), String> {
        match plan {
            Plan::Reinforce { counts: false, .. } => Ok(()),
            Plan::Reinforce { id, counts: true } => self
                .dynamo
                .update_item()
                .table_name(&self.table)
                .key("pk", s(team))
                .key("sk", s(&pref_sk(login, id)))
                .condition_expression("attribute_exists(sk)")
                .update_expression(
                    "ADD #seen :one SET #last = :t, \
                     #prs = list_append(if_not_exists(#prs, :empty), :pr)",
                )
                .expression_attribute_names("#seen", "seen")
                .expression_attribute_names("#last", "last_seen_at")
                .expression_attribute_names("#prs", "prs")
                .expression_attribute_values(":one", AttributeValue::N("1".into()))
                .expression_attribute_values(":t", s(now))
                .expression_attribute_values(":empty", AttributeValue::L(vec![]))
                .expression_attribute_values(":pr", AttributeValue::L(vec![s(pr_key)]))
                .send()
                .await
                .map(|_| ())
                .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string()),
            Plan::Write { pref, drop } => {
                if let Some(id) = drop {
                    let _ = self
                        .dynamo
                        .delete_item()
                        .table_name(&self.table)
                        .key("pk", s(team))
                        .key("sk", s(&pref_sk(login, id)))
                        .send()
                        .await;
                }
                self.dynamo
                    .put_item()
                    .table_name(&self.table)
                    .set_item(Some(pref.to_item(team)))
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())
            }
        }
    }

    /// Forgets everything kept about one reviewer: their asks, and where
    /// they were seen. Returns how many records went.
    pub async fn forget(&self, team: &str, login: &str) -> Result<usize, String> {
        let mut keys: Vec<String> = self
            .query(team, &format!("{PREF_PREFIX}{login}#"))
            .await?
            .iter()
            .map(|i| gs(i, "sk"))
            .collect();
        keys.extend(
            self.query(team, SEEN_PREFIX)
                .await?
                .iter()
                .filter(|i| gs(i, "login") == login)
                .map(|i| gs(i, "sk")),
        );
        for sk in &keys {
            self.dynamo
                .delete_item()
                .table_name(&self.table)
                .key("pk", s(team))
                .key("sk", s(sk))
                .send()
                .await
                .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())?;
        }
        Ok(keys.len())
    }

    /// Forgets one ask.
    pub async fn forget_ask(&self, team: &str, login: &str, id: &str) -> Result<(), String> {
        self.dynamo
            .delete_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(&pref_sk(login, id)))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 10, 20, 0, 0, 0).unwrap()
    }

    fn ask(reviewer: &str, text: &str, category: &str) -> Ask {
        Ask {
            reviewer: reviewer.into(),
            ask: text.into(),
            category: category.into(),
            same_as: None,
        }
    }

    fn pref(id: &str, login: &str, text: &str, prs: &[&str], last: &str) -> Pref {
        Pref {
            id: id.into(),
            login: login.into(),
            text: text.into(),
            category: "tests".into(),
            seen: prs.len() as u64,
            prs: prs.iter().map(|p| p.to_string()).collect(),
            created_at: last.into(),
            last_seen_at: last.into(),
        }
    }

    #[test]
    fn only_review_relevant_asks_are_kept_and_nothing_personal() {
        let ok = clean(&ask(
            "@Pat-Lee",
            "Add a test for the error path of every new function, see https://wiki.example/x and ask @sam",
            "Tests",
        ))
        .unwrap();
        assert_eq!(ok.reviewer, "pat-lee");
        assert_eq!(ok.category, "tests");
        assert_eq!(
            ok.ask,
            "Add a test for the error path of every new function, see and ask"
        );
        // Not about the code.
        assert_eq!(
            clean(&ask(
                "pat",
                "Is usually on vacation in August, wait for her",
                "scope"
            )),
            None
        );
        assert_eq!(
            clean(&ask("pat", "Gets annoyed by long pull requests", "scope")),
            None
        );
        assert_eq!(
            clean(&ask(
                "pat",
                "Works late; reviews arrive on the weekend",
                "scope"
            )),
            None
        );
        // Not a known category, not a person's login, too short to be an ask.
        assert_eq!(
            clean(&ask(
                "pat",
                "Prefers small functions over long ones",
                "preferences"
            )),
            None
        );
        assert_eq!(
            clean(&ask("acme/backend", "Add tests for new functions", "tests")),
            None
        );
        assert_eq!(
            clean(&ask(
                "coderhelm[bot]",
                "Add tests for new functions",
                "tests"
            )),
            None
        );
        assert_eq!(
            clean(&ask(
                "dependabot[bot]",
                "Add tests for new functions",
                "tests"
            )),
            None
        );
        assert_eq!(clean(&ask("pat", "More tests", "tests")), None);
        // "another" and "other" are not "her".
        assert!(clean(&ask(
            "pat",
            "Move shared helpers to another module, not the handler",
            "structure"
        ))
        .is_some());
        // An address is taken out, never stored.
        let c = clean(&ask(
            "pat",
            "Send schema changes to pat@example.com before merging them",
            "api",
        ))
        .unwrap();
        assert!(!c.ask.contains('@'), "{}", c.ask);
        // The reviewer's name is not part of the ask.
        assert_eq!(
            clean(&ask(
                "pat",
                "Whatever pat says about naming goes here",
                "naming"
            )),
            None
        );
    }

    #[test]
    fn an_ask_counts_each_pull_request_once_and_is_never_stored_twice() {
        let a = clean(&ask(
            "pat",
            "Add a test for the error path of every new function",
            "tests",
        ))
        .unwrap();
        let Plan::Write { pref: first, drop } = plan(&[], &a, "acme/web#12", "P1", now()) else {
            panic!("a first ask is written");
        };
        assert_eq!((first.seen, drop), (1, None));
        assert_eq!(first.prs, vec!["acme/web#12"]);
        // The same ask in other words on another pull request.
        let again = clean(&ask(
            "pat",
            "Every new function needs a test for its error path",
            "tests",
        ))
        .unwrap();
        assert_eq!(
            plan(
                std::slice::from_ref(&first),
                &again,
                "acme/web#15",
                "P2",
                now()
            ),
            Plan::Reinforce {
                id: "P1".into(),
                counts: true
            }
        );
        // The same pull request again adds nothing.
        assert_eq!(
            plan(
                std::slice::from_ref(&first),
                &again,
                "acme/web#12",
                "P2",
                now()
            ),
            Plan::Reinforce {
                id: "P1".into(),
                counts: false
            }
        );
        // Applied: one ask, seen on two pull requests, then given.
        let mut kept = vec![first.clone()];
        assert!(
            active(&kept, "pat", now()).is_empty(),
            "once is not repeatedly"
        );
        let p = plan(&kept, &again, "acme/web#15", "P2", now());
        apply_plan(&mut kept, &p, "acme/web#15", &now().to_rfc3339());
        let p = plan(&kept, &again, "acme/web#15", "P3", now());
        apply_plan(&mut kept, &p, "acme/web#15", &now().to_rfc3339());
        assert_eq!((kept.len(), kept[0].seen), (1, 2));
        assert_eq!(kept[0].prs, vec!["acme/web#12", "acme/web#15"]);
        assert_eq!(active(&kept, "pat", now()).len(), 1);
        // Another reviewer's same words are their own ask.
        let sams = clean(&ask(
            "sam",
            "Every new function needs a test for its error path",
            "tests",
        ))
        .unwrap();
        assert!(matches!(
            plan(
                std::slice::from_ref(&first),
                &sams,
                "acme/web#15",
                "P2",
                now()
            ),
            Plan::Write { .. }
        ));
        // The model named the kept ask: that wins over wording.
        let mut named = clean(&ask(
            "pat",
            "Cover failures of the payment client in unit tests",
            "tests",
        ))
        .unwrap();
        named.same_as = Some("P1".into());
        assert_eq!(
            plan(
                std::slice::from_ref(&first),
                &named,
                "acme/web#16",
                "P2",
                now()
            ),
            Plan::Reinforce {
                id: "P1".into(),
                counts: true
            }
        );
        // An id of another reviewer's ask is not this reviewer's.
        let mut wrong = sams.clone();
        wrong.same_as = Some("P1".into());
        wrong.ask = "Keep handlers under fifty lines each please".into();
        assert!(matches!(
            plan(&[first], &wrong, "acme/web#16", "P2", now()),
            Plan::Write { .. }
        ));
    }

    #[test]
    fn an_ask_is_given_only_when_repeated_and_recent() {
        let prefs = vec![
            pref(
                "once",
                "pat",
                "Seen on one pull request only",
                &["a/b#1"],
                "2026-10-19T00:00:00Z",
            ),
            pref(
                "twice",
                "pat",
                "Add a test for the error path",
                &["a/b#1", "a/b#2"],
                "2026-10-19T00:00:00Z",
            ),
            pref(
                "old",
                "pat",
                "Seen twice long ago",
                &["a/b#1", "a/b#2"],
                "2025-10-01T00:00:00Z",
            ),
            pref(
                "sams",
                "sam",
                "Name handlers after the route",
                &["a/b#1", "a/b#2", "a/b#3"],
                "2026-10-19T00:00:00Z",
            ),
        ];
        let ids: Vec<String> = active(&prefs, "pat", now())
            .into_iter()
            .map(|p| p.id)
            .collect();
        assert_eq!(ids, vec!["twice"]);
        let block = prompt_block(&["pat".into(), "nobody".into()], &prefs, now());
        assert!(
            block.contains("- pat: Add a test for the error path (tests, on 2 pull requests)"),
            "{block}"
        );
        assert!(!block.contains("sam") && !block.contains("one pull request only"));
        assert_eq!(prompt_block(&["nobody".into()], &prefs, now()), "");
        assert_eq!(prompt_block(&[], &prefs, now()), "");
    }

    #[test]
    fn the_cap_drops_the_reviewers_lightest_ask() {
        let mut kept: Vec<Pref> = (0..MAX_PER_REVIEWER)
            .map(|i| {
                pref(
                    &format!("P{i}"),
                    "pat",
                    &format!("ask number{i} about thing{i}"),
                    &["a/b#1", "a/b#2"],
                    "2026-10-19T00:00:00Z",
                )
            })
            .collect();
        kept[4].last_seen_at = "2026-01-01T00:00:00Z".into();
        let a = clean(&ask(
            "pat",
            "Document every exported function with a comment",
            "docs",
        ))
        .unwrap();
        let Plan::Write { drop, .. } = plan(&kept, &a, "a/b#9", "NEW", now()) else {
            panic!("written");
        };
        assert_eq!(drop.as_deref(), Some("P4"));
    }

    #[test]
    fn likely_reviewers_are_code_owners_then_recent_reviewers_of_those_files() {
        let codeowners = "* @acme/everyone @lee\n/apps/web/ @pat @acme/web\n/apps/api/ @sam\n";
        let seen = vec![
            SeenReviewer {
                repo: "acme/shop".into(),
                login: "kim".into(),
                areas: ["apps/web".to_string()].into(),
                reviews: 4,
                last_at: "2026-10-10T00:00:00Z".into(),
            },
            SeenReviewer {
                repo: "acme/shop".into(),
                login: "ravi".into(),
                areas: ["apps/api".to_string()].into(),
                reviews: 9,
                last_at: "2026-10-18T00:00:00Z".into(),
            },
            SeenReviewer {
                repo: "acme/shop".into(),
                login: "gone".into(),
                areas: ["apps/web".to_string()].into(),
                reviews: 30,
                last_at: "2026-01-01T00:00:00Z".into(),
            },
        ];
        let web = vec!["apps/web/src/cart.ts".to_string()];
        assert_eq!(
            likely_reviewers(Some(codeowners), &web, &seen, now()),
            vec!["pat", "kim"]
        );
        // No paths yet (the build has not chosen files): the catch-all's
        // people, then whoever reviewed here recently.
        assert_eq!(
            likely_reviewers(Some(codeowners), &[], &seen, now()),
            vec!["lee", "ravi", "kim"]
        );
        // No CODEOWNERS file.
        assert_eq!(likely_reviewers(None, &web, &seen, now()), vec!["kim"]);
        assert!(likely_reviewers(None, &["docs/a.md".to_string()], &seen, now()).is_empty());
        assert_eq!(
            code_owners(codeowners, &["apps/api/x.rs".to_string()]),
            vec!["sam"]
        );
    }

    #[test]
    fn records_round_trip_and_the_view_says_what_is_given() {
        let p = pref(
            "P1",
            "pat",
            "Add a test for the error path",
            &["a/b#1", "a/b#2"],
            "2026-10-19T00:00:00Z",
        );
        let item = p.to_item("TEAM#a");
        assert_eq!(item["sk"], s("REVIEWERPREF#pat#P1"));
        assert_eq!(Pref::from_item(&item), Some(p.clone()));
        assert_eq!(Pref::from_item(&Item::new()), None);
        let once = pref(
            "P2",
            "pat",
            "Seen on one pull request only",
            &["a/b#1"],
            "2026-10-19T00:00:00Z",
        );
        let v = view(&[once, p], now());
        assert_eq!(v["reviewers"][0]["login"], "pat");
        assert_eq!(v["reviewers"][0]["asks"][0]["id"], "P1");
        assert_eq!(v["reviewers"][0]["asks"][0]["given"], true);
        assert_eq!(v["reviewers"][0]["asks"][1]["given"], false);
        assert_eq!(login_key("Pat-Lee"), Some("pat-lee".into()));
        assert_eq!(login_key("acme/web"), None);
        assert_eq!(login_key(""), None);
    }
}
