//! `GET /api/search?q=&limit=&types=` — the dashboard's command palette
//! (⌘K) searching runs, reviewed pull requests, releases and plans at once.
//!
//! These are the team's open-ended histories: none has a text index, and the
//! reviews and releases lists cannot be searched at all, so the palette could
//! not answer from the list endpoints without paging through everything. One
//! request here reads a bounded slice of each family (newest runs; small
//! projections of the other families' rows), matches the words here, and
//! returns at most `limit` hits per family.
//!
//! Every read is keyed on the caller's team (`claims.team_id`), and every row
//! read is checked again against that team before it can be returned, so a
//! row from another team never reaches the answer even if a read were
//! mis-keyed. The tests prove both with a store that hands back rows of two
//! teams.
//!
//! Smaller catalogs (data sources, skills, members, settings pages) are not
//! here: the palette loads those whole, once, through their own endpoints and
//! their own role checks, and matches them in the browser.

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tracing::error;

use super::run_search;
use crate::models::Claims;
use crate::AppState;

pub type Item = HashMap<String, AttributeValue>;

/// Hits per family when the caller does not say.
const DEFAULT_LIMIT: usize = 5;
/// Most hits per family one request returns.
const MAX_LIMIT: usize = 10;
/// Longest search text read, in characters.
const MAX_QUERY_CHARS: usize = 200;
/// Most words of the search that count.
const MAX_TERMS: usize = 8;

/// Newest runs one search reads, matched or not.
const RUN_BUDGET: usize = 600;
/// Rows of each other family one search reads (small projections, so this
/// stays near one 1 MB DynamoDB page).
const ROW_BUDGET: usize = 3000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    Runs,
    Reviews,
    ReviewTitles,
    Releases,
    Plans,
}

impl Family {
    /// The attribute holding the team on rows of this family.
    fn team_attr(self) -> &'static str {
        match self {
            Family::Runs => "team_id",
            _ => "pk",
        }
    }
}

/// One bounded read of a family's rows for one team.
#[derive(Debug, Clone)]
pub struct Read {
    pub family: Family,
    /// The team whose partition is read. Always the caller's team.
    pub team: String,
    /// Sort-key prefix (empty for runs, which are keyed by run id).
    pub prefix: &'static str,
    pub attrs: &'static [&'static str],
    pub max_items: usize,
}

/// Where the rows come from: DynamoDB in the gateway, a fake in tests.
pub trait Store: Sync {
    fn read(&self, read: Read) -> impl Future<Output = Result<Vec<Item>, StatusCode>> + Send;
}

#[derive(Deserialize)]
pub struct SearchQuery {
    q: Option<String>,
    limit: Option<usize>,
    /// Comma-separated families to search (`runs,reviews,releases,plans`);
    /// omitted searches all four.
    types: Option<String>,
}

/// GET /api/search?q=&limit=&types= — viewer and up, like the lists it
/// searches (runs, plans, reviews and releases are all viewer-readable).
pub async fn search(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<Value>, StatusCode> {
    let store = DynamoStore { state: &state };
    let out = run(
        &store,
        &claims.team_id,
        params.q.as_deref().unwrap_or(""),
        params.limit,
        params.types.as_deref(),
    )
    .await?;
    Ok(Json(out))
}

/// The search itself, over any store. Public to the tests.
pub async fn run<S: Store>(
    store: &S,
    team: &str,
    q: &str,
    limit: Option<usize>,
    types: Option<&str>,
) -> Result<Value, StatusCode> {
    let terms = Terms::new(q);
    let limit = limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let wanted = |name: &str| {
        types
            .map(|t| t.split(',').any(|x| x.trim() == name))
            .unwrap_or(true)
    };
    let mut out = json!({
        "q": terms.text,
        "runs": [],
        "reviews": [],
        "releases": [],
        "plans": [],
    });
    if terms.is_empty() {
        return Ok(out);
    }
    let read = |family, prefix, attrs, max_items| Read {
        family,
        team: team.to_string(),
        prefix,
        attrs,
        max_items,
    };

    let runs = async {
        if !wanted("runs") {
            return Ok(Vec::new());
        }
        let rows = store
            .read(read(Family::Runs, "", RUN_ATTRS, RUN_BUDGET))
            .await?;
        Ok::<_, StatusCode>(match_runs(&own(rows, Family::Runs, team), q, limit))
    };
    let reviews = async {
        if !wanted("reviews") {
            return Ok(Vec::new());
        }
        let (rows, titles) = futures::try_join!(
            store.read(read(Family::Reviews, "REVIEW#", REVIEW_ATTRS, ROW_BUDGET)),
            store.read(read(
                Family::ReviewTitles,
                common::review_live::SK_PREFIX,
                LIVE_ATTRS,
                ROW_BUDGET,
            )),
        )?;
        Ok(match_reviews(
            &own(rows, Family::Reviews, team),
            &own(titles, Family::ReviewTitles, team),
            &terms,
            limit,
        ))
    };
    let releases = async {
        if !wanted("releases") {
            return Ok(Vec::new());
        }
        let rows = store
            .read(read(
                Family::Releases,
                "RELEASE#",
                RELEASE_ATTRS,
                ROW_BUDGET,
            ))
            .await?;
        Ok(match_releases(
            &own(rows, Family::Releases, team),
            &terms,
            limit,
        ))
    };
    let plans = async {
        if !wanted("plans") {
            return Ok(Vec::new());
        }
        let rows = store
            .read(read(Family::Plans, "PLAN#", PLAN_ATTRS, ROW_BUDGET))
            .await?;
        Ok(match_plans(&own(rows, Family::Plans, team), &terms, limit))
    };
    let (runs, reviews, releases, plans) = futures::try_join!(runs, reviews, releases, plans)?;
    out["runs"] = Value::Array(runs);
    out["reviews"] = Value::Array(reviews);
    out["releases"] = Value::Array(releases);
    out["plans"] = Value::Array(plans);
    Ok(out)
}

/// Keep only the caller's team's rows. Reads are keyed on that team already;
/// this is the second lock, so a mis-keyed read still cannot leak.
fn own(rows: Vec<Item>, family: Family, team: &str) -> Vec<Item> {
    let attr = family.team_attr();
    rows.into_iter()
        .filter(|it| s(it, attr) == team && !team.is_empty())
        .collect()
}

// ── Matching ────────────────────────────────────────────────────────────

/// The words of a search, lowercased; a row must match every one.
#[derive(Debug)]
pub struct Terms {
    text: String,
    words: Vec<String>,
}

impl Terms {
    pub fn new(q: &str) -> Self {
        let text: String = q.trim().chars().take(MAX_QUERY_CHARS).collect();
        let words = text
            .to_lowercase()
            .split_whitespace()
            .take(MAX_TERMS)
            .map(str::to_string)
            .collect();
        Self { text, words }
    }

    fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    /// Every word appears in one of `fields`, or (for "42" / "#42") equals
    /// the row's number.
    fn matches(&self, fields: &[&str], number: Option<u64>) -> bool {
        let fields: Vec<String> = fields.iter().map(|f| f.to_lowercase()).collect();
        self.words.iter().all(|w| {
            let digits = w.strip_prefix('#').unwrap_or(w);
            if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
                if number.is_some_and(|n| digits.parse::<u64>().ok() == Some(n)) {
                    return true;
                }
                // A number also matches inside a title or tag ("Node 22", "v2.3").
                return fields.iter().any(|f| f.contains(digits));
            }
            fields.iter().any(|f| f.contains(w.as_str()))
        })
    }
}

const RUN_ATTRS: &[&str] = &[
    "team_id",
    "run_id",
    "status",
    "ticket_source",
    "ticket_id",
    "title",
    "repo",
    "pr_number",
    "pr_numbers",
    "created_at",
];

fn match_runs(rows: &[Item], q: &str, limit: usize) -> Vec<Value> {
    // The runs list's own matching, so ⌘K and the Runs search agree.
    let filter = run_search::RunFilter::new(Some(q), None, None);
    let (found, _) = run_search::take_matches(rows, &filter, limit, rows.len());
    found
        .into_iter()
        .map(|it| {
            json!({
                "run_id": s(it, "run_id"),
                "title": s(it, "title"),
                "ticket_id": s(it, "ticket_id"),
                "repo": s(it, "repo"),
                "status": s(it, "status"),
                "created_at": s(it, "created_at"),
            })
        })
        .collect()
}

const REVIEW_ATTRS: &[&str] = &["pk", "sk", "repo", "pr_number", "verdict", "created_at"];
const LIVE_ATTRS: &[&str] = &["pk", "sk", "pr_title"];

/// `owner/name` and the PR number from `PREFIX{owner}/{name}#{pr:06}[#…]`.
fn repo_pr(sk: &str, prefix: &str) -> Option<(String, u64)> {
    let rest = sk.strip_prefix(prefix)?;
    let mut parts = rest.splitn(3, '#');
    let repo = parts.next()?;
    let pr = parts.next()?.parse().ok()?;
    (!repo.is_empty()).then(|| (repo.to_string(), pr))
}

/// One hit per pull request: its newest review, titled from the live record
/// when the reviewer saw the title.
fn match_reviews(rows: &[Item], live: &[Item], terms: &Terms, limit: usize) -> Vec<Value> {
    let titles: HashMap<(String, u64), String> = live
        .iter()
        .filter_map(|it| {
            let key = repo_pr(s(it, "sk"), common::review_live::SK_PREFIX)?;
            let title = s(it, "pr_title");
            (!title.is_empty()).then(|| (key, title.to_string()))
        })
        .collect();
    let mut newest: HashMap<(String, u64), &Item> = HashMap::new();
    for it in rows {
        let Some(key) = repo_pr(s(it, "sk"), "REVIEW#") else {
            continue;
        };
        let keep = newest
            .get(&key)
            .is_none_or(|cur| s(it, "created_at") > s(cur, "created_at"));
        if keep {
            newest.insert(key, it);
        }
    }
    let mut hits: Vec<(&(String, u64), &Item)> = newest
        .iter()
        .map(|(k, v)| (k, *v))
        .filter(|((repo, pr), _)| {
            let title = titles.get(&(repo.clone(), *pr)).map(String::as_str);
            // The PR number matches whole ("42", "#42"), never inside the repo.
            terms.matches(&[repo, title.unwrap_or("")], Some(*pr))
        })
        .collect();
    hits.sort_by(|a, b| s(b.1, "created_at").cmp(s(a.1, "created_at")));
    hits.into_iter()
        .take(limit)
        .map(|((repo, pr), it)| {
            json!({
                "sk": s(it, "sk"),
                "repo": repo,
                "pr_number": pr,
                "title": titles.get(&(repo.clone(), *pr)).cloned().unwrap_or_default(),
                "verdict": s(it, "verdict"),
                "created_at": s(it, "created_at"),
            })
        })
        .collect()
}

const RELEASE_ATTRS: &[&str] = &["pk", "sk", "headline", "updated_at"];

fn match_releases(rows: &[Item], terms: &Terms, limit: usize) -> Vec<Value> {
    let mut hits: Vec<(&str, &str, &Item)> = rows
        .iter()
        .filter_map(|it| {
            // RELEASE#{owner}/{name}#{tag}
            let (repo, tag) = s(it, "sk").strip_prefix("RELEASE#")?.rsplit_once('#')?;
            terms
                .matches(&[repo, tag, s(it, "headline")], None)
                .then_some((repo, tag, it))
        })
        .collect();
    hits.sort_by(|a, b| s(b.2, "updated_at").cmp(s(a.2, "updated_at")));
    hits.into_iter()
        .take(limit)
        .map(|(repo, tag, it)| {
            json!({
                "repo": repo,
                "tag": tag,
                "headline": s(it, "headline"),
                "updated_at": s(it, "updated_at"),
            })
        })
        .collect()
}

const PLAN_ATTRS: &[&str] = &[
    "pk",
    "sk",
    "plan_id",
    "title",
    "repo",
    "status",
    "created_at",
];

fn match_plans(rows: &[Item], terms: &Terms, limit: usize) -> Vec<Value> {
    let mut hits: Vec<&Item> = rows
        .iter()
        // Task rows (`PLAN#<id>#TASK#…`) share the partition.
        .filter(|it| {
            let sk = s(it, "sk");
            sk.starts_with("PLAN#") && !sk.contains("#TASK#")
        })
        .filter(|it| terms.matches(&[s(it, "title"), s(it, "repo")], None))
        .collect();
    hits.sort_by(|a, b| s(b, "created_at").cmp(s(a, "created_at")));
    hits.into_iter()
        .take(limit)
        .map(|it| {
            json!({
                "plan_id": s(it, "plan_id"),
                "title": s(it, "title"),
                "repo": s(it, "repo"),
                "status": s(it, "status"),
                "created_at": s(it, "created_at"),
            })
        })
        .collect()
}

fn s<'a>(item: &'a Item, key: &str) -> &'a str {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .unwrap_or("")
}

// ── DynamoDB ────────────────────────────────────────────────────────────

struct DynamoStore<'a> {
    state: &'a AppState,
}

impl Store for DynamoStore<'_> {
    async fn read(&self, read: Read) -> Result<Vec<Item>, StatusCode> {
        let cfg = &self.state.config;
        let names: HashMap<String, String> = read
            .attrs
            .iter()
            .enumerate()
            .map(|(i, a)| (format!("#a{i}"), a.to_string()))
            .collect();
        let projection = (0..read.attrs.len())
            .map(|i| format!("#a{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut out: Vec<Item> = Vec::new();
        let mut start: Option<Item> = None;
        loop {
            let room = read.max_items - out.len();
            let mut query = self
                .state
                .dynamo
                .query()
                .projection_expression(&projection)
                .set_expression_attribute_names(Some(names.clone()))
                .set_exclusive_start_key(start.take());
            query = match read.family {
                Family::Runs => query
                    .table_name(&cfg.runs_table_name)
                    .key_condition_expression("team_id = :t")
                    .expression_attribute_values(":t", AttributeValue::S(read.team.clone()))
                    .scan_index_forward(false) // newest first: run ids are ULIDs
                    .limit(room.min(200) as i32),
                Family::Plans => query
                    .table_name(&cfg.plans_table_name)
                    .key_condition_expression("pk = :t AND begins_with(sk, :p)")
                    .expression_attribute_values(":t", AttributeValue::S(read.team.clone()))
                    .expression_attribute_values(":p", AttributeValue::S(read.prefix.into()))
                    .scan_index_forward(false)
                    .limit(room.min(1000) as i32),
                Family::Reviews | Family::ReviewTitles | Family::Releases => query
                    .table_name(&cfg.settings_table_name)
                    .key_condition_expression("pk = :t AND begins_with(sk, :p)")
                    .expression_attribute_values(":t", AttributeValue::S(read.team.clone()))
                    .expression_attribute_values(":p", AttributeValue::S(read.prefix.into()))
                    .limit(room.min(1000) as i32),
            };
            let page = query.send().await.map_err(|e| {
                error!(family = ?read.family, error = %e, "Search read failed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
            out.extend(page.items.unwrap_or_default());
            start = page.last_evaluated_key.filter(|k| !k.is_empty());
            if start.is_none() || out.len() >= read.max_items {
                return Ok(out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const MINE: &str = "TEAM#mine";
    const OTHER: &str = "TEAM#other";

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
            .collect()
    }

    /// Rows of two teams. `leaky` hands back every team's rows whatever
    /// partition is asked for, like a mis-keyed read would.
    struct FakeStore {
        leaky: bool,
        asked: Mutex<Vec<Read>>,
    }

    impl FakeStore {
        fn new(leaky: bool) -> Self {
            Self {
                leaky,
                asked: Mutex::new(Vec::new()),
            }
        }

        fn rows(family: Family, team: &str) -> Vec<Item> {
            let t = if team == MINE { "mine" } else { "other" };
            match family {
                Family::Runs => vec![item(&[
                    ("team_id", team),
                    ("run_id", &format!("01RUN{t}")),
                    ("status", "completed"),
                    (
                        "ticket_id",
                        &format!("ABC-{}", if team == MINE { 1 } else { 2 }),
                    ),
                    ("title", &format!("Fix the login page for {t}")),
                    ("repo", &format!("{t}-org/web")),
                    ("created_at", "2026-01-02T00:00:00Z"),
                ])],
                Family::Reviews => vec![
                    item(&[
                        ("pk", team),
                        (
                            "sk",
                            &format!("REVIEW#{t}-org/web#000042#2026-01-01T00:00:00Z"),
                        ),
                        ("verdict", "COMMENT"),
                        ("created_at", "2026-01-01T00:00:00Z"),
                    ]),
                    item(&[
                        ("pk", team),
                        (
                            "sk",
                            &format!("REVIEW#{t}-org/web#000042#2026-01-03T00:00:00Z"),
                        ),
                        ("verdict", "APPROVE"),
                        ("created_at", "2026-01-03T00:00:00Z"),
                    ]),
                ],
                Family::ReviewTitles => vec![item(&[
                    ("pk", team),
                    ("sk", &format!("REVIEWLIVE#{t}-org/web#000042")),
                    ("pr_title", &format!("Login retries for {t}")),
                ])],
                Family::Releases => vec![item(&[
                    ("pk", team),
                    ("sk", &format!("RELEASE#{t}-org/web#v1.2.0")),
                    ("headline", &format!("Faster login for {t}")),
                    ("updated_at", "2026-01-04T00:00:00Z"),
                ])],
                Family::Plans => vec![
                    item(&[
                        ("pk", team),
                        ("sk", &format!("PLAN#{t}1")),
                        ("plan_id", &format!("{t}1")),
                        ("title", &format!("Login overhaul for {t}")),
                        ("repo", &format!("{t}-org/web")),
                        ("status", "draft"),
                        ("created_at", "2026-01-05T00:00:00Z"),
                    ]),
                    item(&[
                        ("pk", team),
                        ("sk", &format!("PLAN#{t}1#TASK#01")),
                        ("title", "login task row"),
                    ]),
                ],
            }
        }
    }

    impl Store for FakeStore {
        async fn read(&self, read: Read) -> Result<Vec<Item>, StatusCode> {
            self.asked.lock().unwrap().push(read.clone());
            let mut rows = Self::rows(read.family, &read.team);
            if self.leaky {
                let other = if read.team == MINE { OTHER } else { MINE };
                rows.extend(Self::rows(read.family, other));
            }
            Ok(rows)
        }
    }

    fn all_text(v: &Value) -> String {
        v.to_string()
    }

    #[tokio::test]
    async fn every_read_is_keyed_on_the_callers_team() {
        let store = FakeStore::new(false);
        run(&store, MINE, "login", None, None).await.unwrap();
        let asked = store.asked.lock().unwrap();
        assert_eq!(
            asked.len(),
            5,
            "runs, reviews, review titles, releases, plans"
        );
        assert!(asked.iter().all(|r| r.team == MINE));
        // Bounded: no read is open-ended.
        assert!(asked.iter().all(|r| r.max_items <= ROW_BUDGET));
    }

    #[tokio::test]
    async fn another_teams_rows_never_reach_the_answer_even_from_a_leaky_read() {
        let store = FakeStore::new(true);
        let out = run(&store, MINE, "login", Some(10), None).await.unwrap();
        let text = all_text(&out);
        assert!(!text.contains("other"), "leaked: {text}");
        assert_eq!(out["runs"].as_array().unwrap().len(), 1);
        assert_eq!(out["reviews"].as_array().unwrap().len(), 1);
        assert_eq!(out["releases"].as_array().unwrap().len(), 1);
        assert_eq!(out["plans"].as_array().unwrap().len(), 1);
        // Searching for the other team's words finds nothing.
        let none = run(&store, MINE, "other-org", Some(10), None)
            .await
            .unwrap();
        for family in ["runs", "reviews", "releases", "plans"] {
            assert!(none[family].as_array().unwrap().is_empty(), "{family}");
        }
    }

    #[tokio::test]
    async fn the_query_cannot_choose_the_team() {
        // Words that look like keys are only words.
        let store = FakeStore::new(false);
        run(&store, MINE, "TEAM#other pk sk", None, None)
            .await
            .unwrap();
        assert!(store.asked.lock().unwrap().iter().all(|r| r.team == MINE));
    }

    #[tokio::test]
    async fn a_row_without_a_team_is_dropped() {
        let rows = vec![item(&[("sk", "RELEASE#x/y#v1"), ("headline", "login")])];
        assert!(own(rows, Family::Releases, MINE).is_empty());
        let rows = vec![item(&[("pk", MINE), ("sk", "RELEASE#x/y#v1")])];
        assert!(own(rows, Family::Releases, "").is_empty());
    }

    #[tokio::test]
    async fn an_empty_search_reads_nothing() {
        let store = FakeStore::new(false);
        let out = run(&store, MINE, "   ", None, None).await.unwrap();
        assert!(store.asked.lock().unwrap().is_empty());
        assert!(out["runs"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn types_limits_the_families_read() {
        let store = FakeStore::new(false);
        let out = run(&store, MINE, "login", None, Some("releases"))
            .await
            .unwrap();
        let asked = store.asked.lock().unwrap();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].family, Family::Releases);
        assert_eq!(out["releases"].as_array().unwrap().len(), 1);
        assert!(out["runs"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reviews_are_one_hit_per_pr_with_the_newest_review_and_its_title() {
        let store = FakeStore::new(false);
        let out = run(&store, MINE, "#42", None, Some("reviews"))
            .await
            .unwrap();
        let hits = out["reviews"].as_array().unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["pr_number"], 42);
        assert_eq!(hits[0]["verdict"], "APPROVE");
        assert_eq!(hits[0]["title"], "Login retries for mine");
        assert_eq!(hits[0]["repo"], "mine-org/web");
        // "4" is not PR 42.
        let out = run(&store, MINE, "4", None, Some("reviews")).await.unwrap();
        assert!(out["reviews"].as_array().unwrap().is_empty());
    }

    #[test]
    fn limit_is_clamped_per_family() {
        let rows: Vec<Item> = (0..30)
            .map(|i| {
                item(&[
                    ("pk", MINE),
                    ("sk", &format!("RELEASE#a/b#v{i}")),
                    ("headline", "login"),
                    ("updated_at", &format!("2026-01-{:02}", i % 28 + 1)),
                ])
            })
            .collect();
        let t = Terms::new("login");
        assert_eq!(match_releases(&rows, &t, MAX_LIMIT).len(), MAX_LIMIT);
    }

    #[test]
    fn plan_task_rows_are_not_plans() {
        let rows = FakeStore::rows(Family::Plans, MINE);
        let hits = match_plans(&rows, &Terms::new("login"), 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["plan_id"], "mine1");
    }

    #[test]
    fn terms_match_every_word_case_insensitively() {
        let t = Terms::new("LOGIN web");
        assert!(t.matches(&["Fix login", "acme/web"], None));
        assert!(!t.matches(&["Fix login", "acme/api"], None));
        let long = "x ".repeat(500);
        assert!(Terms::new(&long).words.len() <= MAX_TERMS);
    }
}
