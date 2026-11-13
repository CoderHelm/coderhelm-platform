//! The scorecard: what became of the pull requests CoderHelm opened, per
//! repository and per team, kept as counters that webhook events update.
//!
//! Of the pull requests CoderHelm opened: how many merged, closed without
//! merging or were reverted; how many review rounds they took before
//! approval; how long from opening to merging; how many needed a person's
//! commit on top before merging; how many finished with open items.
//!
//! Nothing here is counted by reading runs or pull requests back. Each pull
//! request has one small record ([`PrRecord`], `SCOREPR#<repo>#<number>`)
//! that the events change ([`Event`]): opened, a person's review, a person's
//! push to its branch, closed or merged, reopened, reverted. Every change of
//! the record adds the difference between what the record counted for before
//! and what it counts for now ([`contribution`]) to the counters, in the
//! same transaction that writes the record and only when the record is still
//! the one that was read ([`Store::commit`]). The differences telescope: a
//! redelivered webhook changes nothing, events may arrive in any order, and
//! two writers racing end with each pull request counted once in its last
//! state.
//!
//! Counters live on the settings table under the team's own key:
//! `SCORE#<period>#T` for the team and `SCORE#<period>#R#<owner/name>` for a
//! repository, where the period is the month the pull request was opened in
//! (`2026-11`) or [`ALL`]. A pull request always counts in the month it was
//! opened, so a month's numbers are about one fixed set of pull requests.
//!
//! Two pointers let events that carry no pull request number find it:
//! `SCOREBR#<repo>#<branch>` (a push to the branch) and
//! `SCORESHA#<repo>#<sha>` (a commit that reverts the merge commit).

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub const PR_PREFIX: &str = "SCOREPR#";
pub const AGG_PREFIX: &str = "SCORE#";
pub const BRANCH_PREFIX: &str = "SCOREBR#";
pub const SHA_PREFIX: &str = "SCORESHA#";
/// The period of the all-time counters.
pub const ALL: &str = "ALL";
/// How long a pull request's record and pointers are kept after their last
/// change (a revert a year later still finds them). The counters stay.
pub const KEEP_DAYS: i64 = 400;
/// Most review ids and commit ids kept on one record.
pub const MAX_IDS: usize = 60;
/// Tries at writing a record another writer changed meanwhile.
const MAX_TRIES: usize = 5;

/// Upper bounds, in hours, of the time-to-merge buckets (the last bucket is
/// everything longer).
pub const MERGE_HOURS: &[u64] = &[1, 4, 8, 24, 48, 72, 168, 336];

/// The markers of the sections a pull request's description lists open
/// items under: findings of CoderHelm's own review still open, and what the
/// run deferred. (The worker's tests hold these to the sections it writes.)
pub const OPEN_FINDINGS_START: &str = "<!-- coderhelm:open-findings -->";
pub const OPEN_FINDINGS_END: &str = "<!-- /coderhelm:open-findings -->";
pub const DEFERRED_HEADING: &str = "## Deferred";

pub fn pr_sk(repo: &str, pr: u64) -> String {
    format!("{PR_PREFIX}{repo}#{pr:06}")
}

pub fn branch_sk(repo: &str, branch: &str) -> String {
    format!("{BRANCH_PREFIX}{repo}#{branch}")
}

pub fn sha_sk(repo: &str, sha: &str) -> String {
    format!("{SHA_PREFIX}{repo}#{}", sha.to_ascii_lowercase())
}

pub fn team_sk(period: &str) -> String {
    format!("{AGG_PREFIX}{period}#T")
}

pub fn repo_agg_sk(period: &str, repo: &str) -> String {
    format!("{AGG_PREFIX}{period}#R#{repo}")
}

/// The month (`YYYY-MM`) of an RFC 3339 time; empty when it isn't one.
pub fn month_of(at: &str) -> String {
    match chrono::DateTime::parse_from_rfc3339(at) {
        Ok(t) => t.with_timezone(&chrono::Utc).format("%Y-%m").to_string(),
        Err(_) => String::new(),
    }
}

/// Whether a pull request's description lists open items: an open-findings
/// section with at least one item, or a "Deferred" section with text in it.
pub fn has_open_items(body: &str) -> bool {
    if let Some(start) = body.find(OPEN_FINDINGS_START) {
        let rest = &body[start + OPEN_FINDINGS_START.len()..];
        let section = &rest[..rest.find(OPEN_FINDINGS_END).unwrap_or(rest.len())];
        if section
            .lines()
            .any(|l| l.trim_start().starts_with("- ") || l.trim_start().starts_with("* "))
        {
            return true;
        }
    }
    let mut lines = body.lines();
    while let Some(l) = lines.next() {
        if l.trim() != DEFERRED_HEADING {
            continue;
        }
        for next in lines.by_ref() {
            let t = next.trim();
            if t.starts_with("## ") || t.starts_with("# ") {
                break;
            }
            if !t.is_empty() && !t.starts_with("<!--") {
                return true;
            }
        }
    }
    false
}

/// How a pull request stands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum State {
    #[default]
    Open,
    Merged,
    Closed,
}

impl State {
    pub fn key(self) -> &'static str {
        match self {
            State::Open => "open",
            State::Merged => "merged",
            State::Closed => "closed",
        }
    }

    pub fn from_key(k: &str) -> State {
        match k {
            "merged" => State::Merged,
            "closed" => State::Closed,
            _ => State::Open,
        }
    }
}

/// One pull request CoderHelm opened, as the events left it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrRecord {
    pub repo: String,
    pub pr: u64,
    pub opened_at: String,
    pub state: State,
    /// When it merged or closed.
    pub ended_at: String,
    /// The head branch.
    pub head: String,
    /// People's commits on it, counted on GitHub when it merged (merges of
    /// the base branch left out). None while unknown.
    pub human_commits: Option<u64>,
    /// People's commits seen in pushes to its branch.
    pub pushed: BTreeSet<String>,
    /// People's reviews that asked for changes (review ids).
    pub change_requests: BTreeSet<String>,
    /// The first approval by a person, and how many change requests came
    /// before it.
    pub approved_at: String,
    pub rounds_at_approval: u64,
    /// Its description listed open items when it merged or closed.
    pub open_items: bool,
    pub merge_sha: String,
    pub reverted_at: String,
    /// What reverted it: `#<number>` of the revert pull request, or a sha.
    pub reverted_by: String,
    /// Bumped on every write; a write goes through only on the one read.
    pub rev: u64,
}

/// Something that happened to a pull request CoderHelm opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// Opened (or seen for the first time).
    Opened { at: String, head: String },
    /// Closed without merging and opened again.
    Reopened { opened_at: String, head: String },
    /// A person's review: `state` is GitHub's (`changes_requested`,
    /// `approved`, `commented`).
    Review {
        id: String,
        state: String,
        at: String,
        opened_at: String,
    },
    /// A person pushed these commits to its branch.
    HumanPush { shas: Vec<String> },
    /// It merged or closed.
    Ended {
        merged: bool,
        at: String,
        opened_at: String,
        head: String,
        human_commits: Option<u64>,
        open_items: bool,
        merge_sha: String,
    },
    /// A merged revert pull request or a revert commit named it.
    Reverted { at: String, by: String },
}

impl Event {
    /// Whether the event can start a record. A push or a revert of a pull
    /// request nobody recorded has nothing to count against.
    fn starts_record(&self) -> bool {
        !matches!(self, Event::HumanPush { .. } | Event::Reverted { .. })
    }
}

fn keep_first(slot: &mut String, v: &str) {
    if slot.is_empty() && !v.is_empty() {
        *slot = v.to_string();
    }
}

fn add_capped(set: &mut BTreeSet<String>, id: &str) {
    if !id.is_empty() && (set.len() < MAX_IDS || set.contains(id)) {
        set.insert(id.to_string());
    }
}

/// Pure: the record after `e`. The same event twice leaves it as it was.
pub fn apply(rec: &mut PrRecord, e: &Event) {
    match e {
        Event::Opened { at, head } => {
            keep_first(&mut rec.opened_at, at);
            keep_first(&mut rec.head, head);
        }
        Event::Reopened { opened_at, head } => {
            keep_first(&mut rec.opened_at, opened_at);
            keep_first(&mut rec.head, head);
            // Only a closed pull request reopens; a merged one stays merged.
            if rec.state == State::Closed {
                rec.state = State::Open;
                rec.ended_at.clear();
                rec.open_items = false;
            }
        }
        Event::Review {
            id,
            state,
            at,
            opened_at,
        } => {
            keep_first(&mut rec.opened_at, opened_at);
            match state.as_str() {
                "changes_requested" if rec.approved_at.is_empty() || at < &rec.approved_at => {
                    add_capped(&mut rec.change_requests, id);
                }
                "approved" if rec.approved_at.is_empty() => {
                    rec.approved_at = at.clone();
                    rec.rounds_at_approval = rec.change_requests.len() as u64;
                }
                _ => {}
            }
        }
        Event::HumanPush { shas } => {
            if rec.state == State::Open {
                for s in shas {
                    add_capped(&mut rec.pushed, s);
                }
            }
        }
        Event::Ended {
            merged,
            at,
            opened_at,
            head,
            human_commits,
            open_items,
            merge_sha,
        } => {
            keep_first(&mut rec.opened_at, opened_at);
            keep_first(&mut rec.head, head);
            // A merge is final; a later "closed" delivery never undoes it.
            if rec.state == State::Merged && !*merged {
                return;
            }
            rec.state = if *merged {
                State::Merged
            } else {
                State::Closed
            };
            rec.ended_at = at.clone();
            rec.open_items = *open_items;
            if human_commits.is_some() {
                rec.human_commits = *human_commits;
            }
            if *merged {
                keep_first(&mut rec.merge_sha, merge_sha);
            }
        }
        Event::Reverted { at, by } => {
            if rec.state == State::Merged && rec.reverted_at.is_empty() {
                rec.reverted_at = at.clone();
                rec.reverted_by = by.clone();
            }
        }
    }
}

/// People's commits on the pull request: GitHub's count at the merge when
/// it is known, else what the pushes showed.
pub fn people_commits(rec: &PrRecord) -> u64 {
    rec.human_commits.unwrap_or(rec.pushed.len() as u64)
}

/// Review rounds before approval: change requests before the first
/// approval, or all of them when it merged with no approval on record.
pub fn review_rounds(rec: &PrRecord) -> Option<u64> {
    if !rec.approved_at.is_empty() {
        Some(rec.rounds_at_approval)
    } else if rec.state == State::Merged {
        Some(rec.change_requests.len() as u64)
    } else {
        None
    }
}

fn seconds_between(from: &str, to: &str) -> Option<i64> {
    let a = chrono::DateTime::parse_from_rfc3339(from).ok()?;
    let b = chrono::DateTime::parse_from_rfc3339(to).ok()?;
    let s = (b - a).num_seconds();
    (s >= 0).then_some(s)
}

/// The counter of the time-to-merge bucket `secs` falls in.
pub fn merge_bucket(secs: i64) -> String {
    let hours = secs as f64 / 3600.0;
    match MERGE_HOURS.iter().find(|b| hours <= **b as f64) {
        Some(b) => format!("mt_{b}"),
        None => "mt_more".to_string(),
    }
}

pub type Counters = BTreeMap<String, i64>;

/// Pure: what one pull request counts for, as it stands.
pub fn contribution(rec: &PrRecord) -> Counters {
    let mut c = Counters::new();
    if rec.opened_at.is_empty() {
        return c;
    }
    let mut one = |k: &str| {
        c.insert(k.to_string(), 1);
    };
    one("opened");
    let merged = rec.state == State::Merged;
    match rec.state {
        State::Merged => one("merged"),
        State::Closed => one("closed"),
        State::Open => {}
    }
    if merged && !rec.reverted_at.is_empty() {
        one("reverted");
    }
    if merged && people_commits(rec) > 0 {
        one("human_edited");
    }
    if merged && rec.human_commits == Some(0) {
        one("as_written");
    }
    if rec.state != State::Open && rec.open_items {
        one("open_items");
    }
    if let Some(rounds) = review_rounds(rec) {
        one("reviewed");
        if rounds == 0 {
            one("first_pass");
        }
        c.insert("rounds".to_string(), rounds as i64);
    }
    if merged {
        if let Some(secs) = seconds_between(&rec.opened_at, &rec.ended_at) {
            c.insert("merge_n".to_string(), 1);
            c.insert("merge_secs".to_string(), secs);
            c.insert(merge_bucket(secs), 1);
        }
    }
    c.retain(|_, v| *v != 0);
    c
}

/// Pure: `new - old`, counter by counter, zeros left out.
pub fn difference(old: &Counters, new: &Counters) -> Counters {
    let mut d = Counters::new();
    for k in old.keys().chain(new.keys()) {
        let v = new.get(k).copied().unwrap_or(0) - old.get(k).copied().unwrap_or(0);
        if v != 0 {
            d.insert(k.clone(), v);
        }
    }
    d
}

/// The counter items a pull request counts on.
pub fn aggregate_keys(rec: &PrRecord) -> Vec<String> {
    let month = month_of(&rec.opened_at);
    let mut periods = vec![ALL.to_string()];
    if !month.is_empty() {
        periods.push(month);
    }
    periods
        .iter()
        .flat_map(|p| [team_sk(p), repo_agg_sk(p, &rec.repo)])
        .collect()
}

/// A pointer to a pull request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pointer {
    pub sk: String,
    pub repo: String,
    pub pr: u64,
}

fn pointers_of(rec: &PrRecord) -> Vec<Pointer> {
    let mut out = vec![];
    let p = |sk: String| Pointer {
        sk,
        repo: rec.repo.clone(),
        pr: rec.pr,
    };
    if !rec.head.is_empty() && rec.state == State::Open {
        out.push(p(branch_sk(&rec.repo, &rec.head)));
    }
    if !rec.merge_sha.is_empty() {
        out.push(p(sha_sk(&rec.repo, &rec.merge_sha)));
    }
    out
}

/// One write: the record, the differences for each counter item and the
/// pointers, all or nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub record: PrRecord,
    /// The `rev` the stored record must still have; None when there must be
    /// no record yet.
    pub expect_rev: Option<u64>,
    pub deltas: Vec<(String, Counters)>,
    pub pointers: Vec<Pointer>,
}

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    async fn get_pr(&self, team: &str, repo: &str, pr: u64) -> Result<Option<PrRecord>, String>;
    /// Writes the commit when the stored record is still the one expected.
    /// False when another writer changed it meanwhile (nothing is written).
    async fn commit(&self, team: &str, c: &Commit) -> Result<bool, String>;
    async fn pointer(&self, team: &str, sk: &str) -> Result<Option<Pointer>, String>;
    /// The counter items whose key starts with `prefix`.
    async fn counters(&self, team: &str, prefix: &str) -> Result<Vec<(String, Counters)>, String>;
}

/// Pure: the write that takes the stored record (`old`) through `e`, or
/// None when the event changes nothing (a redelivery) or has no record to
/// change.
pub fn plan(old: Option<&PrRecord>, repo: &str, pr: u64, e: &Event) -> Option<Commit> {
    if old.is_none() && !e.starts_record() {
        return None;
    }
    let before = old.cloned().unwrap_or_else(|| PrRecord {
        repo: repo.to_string(),
        pr,
        ..Default::default()
    });
    let mut after = before.clone();
    apply(&mut after, e);
    if after == before || after.opened_at.is_empty() {
        return None;
    }
    let diff = difference(
        &old.map(contribution).unwrap_or_default(),
        &contribution(&after),
    );
    let deltas = if diff.is_empty() {
        vec![]
    } else {
        aggregate_keys(&after)
            .into_iter()
            .map(|k| (k, diff.clone()))
            .collect()
    };
    after.rev = before.rev + 1;
    Some(Commit {
        pointers: pointers_of(&after),
        record: after,
        expect_rev: old.map(|r| r.rev),
        deltas,
    })
}

/// Records one event on a pull request CoderHelm opened and moves the
/// counters by what it changed. Returns the record as it now stands, or
/// None when there is none (a push or revert of an unrecorded one).
pub async fn record(
    store: &dyn Store,
    team: &str,
    repo: &str,
    pr: u64,
    e: &Event,
) -> Result<Option<PrRecord>, String> {
    if team.is_empty() || repo.is_empty() || pr == 0 {
        return Ok(None);
    }
    for _ in 0..MAX_TRIES {
        let old = store.get_pr(team, repo, pr).await?;
        let Some(commit) = plan(old.as_ref(), repo, pr, e) else {
            return Ok(old);
        };
        if store.commit(team, &commit).await? {
            return Ok(Some(commit.record));
        }
    }
    Err(format!(
        "the scorecard record of {repo}#{pr} kept changing under the write"
    ))
}

// ── What the dashboard shows ─────────────────────────────────────────

fn ratio(num: i64, den: i64) -> Value {
    if den > 0 {
        json!(((num as f64 / den as f64) * 1000.0).round() / 1000.0)
    } else {
        Value::Null
    }
}

fn one_decimal(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

/// The upper bound, in hours, of the bucket the median merge falls in;
/// None when nothing merged, or the median is past the last bound.
pub fn median_merge_hours(c: &Counters) -> Option<u64> {
    let get = |k: &str| c.get(k).copied().unwrap_or(0).max(0);
    let total: i64 = MERGE_HOURS
        .iter()
        .map(|b| get(&format!("mt_{b}")))
        .sum::<i64>()
        + get("mt_more");
    if total == 0 {
        return None;
    }
    let mut seen = 0;
    for b in MERGE_HOURS {
        seen += get(&format!("mt_{b}"));
        if seen * 2 >= total {
            return Some(*b);
        }
    }
    None
}

/// Pure: one scope's counters as the dashboard reads them. A rate is null
/// when nothing is behind it, never a made-up zero.
pub fn row(c: &Counters) -> Value {
    let get = |k: &str| c.get(k).copied().unwrap_or(0).max(0);
    let (opened, merged, closed) = (get("opened"), get("merged"), get("closed"));
    let finished = merged + closed;
    let median = median_merge_hours(c);
    json!({
        "opened": opened,
        "merged": merged,
        "closed_unmerged": closed,
        "open": (opened - finished).max(0),
        "reverted": get("reverted"),
        "merge_rate": ratio(merged, finished),
        "revert_rate": ratio(get("reverted"), merged),
        "human_edited": get("human_edited"),
        "human_edited_rate": ratio(get("human_edited"), merged),
        "merged_as_written": get("as_written"),
        "open_items": get("open_items"),
        "open_items_rate": ratio(get("open_items"), finished),
        "reviewed": get("reviewed"),
        "first_pass": get("first_pass"),
        "avg_review_rounds": if get("reviewed") > 0 {
            json!(one_decimal(get("rounds") as f64 / get("reviewed") as f64))
        } else {
            Value::Null
        },
        "avg_hours_to_merge": if get("merge_n") > 0 {
            json!(one_decimal(get("merge_secs") as f64 / get("merge_n") as f64 / 3600.0))
        } else {
            Value::Null
        },
        "median_hours_to_merge_at_most": median,
        "median_past_hours": if median.is_none() && get("merge_n") > 0 {
            json!(MERGE_HOURS.last())
        } else {
            Value::Null
        },
    })
}

/// A counter item's key, read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggKey {
    pub period: String,
    /// None for the team's item.
    pub repo: Option<String>,
}

pub fn parse_agg_sk(sk: &str) -> Option<AggKey> {
    let rest = sk.strip_prefix(AGG_PREFIX)?;
    let (period, scope) = rest.split_once('#')?;
    if scope == "T" {
        return Some(AggKey {
            period: period.to_string(),
            repo: None,
        });
    }
    let repo = scope.strip_prefix("R#")?;
    (!repo.is_empty()).then(|| AggKey {
        period: period.to_string(),
        repo: Some(repo.to_string()),
    })
}

/// The months (`YYYY-MM`) of a window of `months` ending in `now`'s month.
pub fn window_months(now: chrono::DateTime<chrono::Utc>, months: u32) -> Vec<String> {
    use chrono::Datelike;
    let (mut y, mut m) = (now.year(), now.month() as i32);
    let mut out = vec![];
    for _ in 0..months {
        out.push(format!("{y:04}-{m:02}"));
        m -= 1;
        if m == 0 {
            m = 12;
            y -= 1;
        }
    }
    out
}

fn add_into(into: &mut Counters, from: &Counters) {
    for (k, v) in from {
        *into.entry(k.clone()).or_default() += v;
    }
}

/// Pure: the scorecard from a team's counter items. `months` is how many
/// months back to count pull requests opened in (0: all time); `repo`
/// narrows it to one repository.
pub fn view(
    items: &[(String, Counters)],
    months: u32,
    repo: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Value {
    let window = window_months(now, months);
    let in_window = |period: &str| {
        if months == 0 {
            period == ALL
        } else {
            window.iter().any(|m| m == period)
        }
    };
    let mut team = Counters::new();
    let mut repos: BTreeMap<String, Counters> = BTreeMap::new();
    let mut by_month: BTreeMap<String, Counters> = BTreeMap::new();
    let mut since: Option<String> = None;
    for (sk, c) in items {
        let Some(key) = parse_agg_sk(sk) else {
            continue;
        };
        let wanted_scope = match (&key.repo, repo.is_empty()) {
            (None, true) => true,
            (Some(r), false) => r == repo,
            _ => false,
        };
        if key.period != ALL && wanted_scope {
            add_into(by_month.entry(key.period.clone()).or_default(), c);
            if since.as_deref().is_none_or(|s| key.period.as_str() < s) {
                since = Some(key.period.clone());
            }
        }
        if !in_window(&key.period) {
            continue;
        }
        match &key.repo {
            None if repo.is_empty() => add_into(&mut team, c),
            Some(r) => {
                if repo.is_empty() || r == repo {
                    add_into(repos.entry(r.clone()).or_default(), c);
                }
                if !repo.is_empty() && r == repo {
                    add_into(&mut team, c);
                }
            }
            None => {}
        }
    }
    let mut repo_rows: Vec<(i64, Value)> = repos
        .iter()
        .map(|(name, c)| {
            let mut v = row(c);
            v["repo"] = json!(name);
            (c.get("opened").copied().unwrap_or(0), v)
        })
        .collect();
    repo_rows.sort_by(|a, b| {
        b.0.cmp(&a.0).then(
            a.1["repo"]
                .as_str()
                .unwrap_or("")
                .cmp(b.1["repo"].as_str().unwrap_or("")),
        )
    });
    let trend: Vec<Value> = by_month
        .iter()
        .rev()
        .take(12)
        .rev()
        .map(|(month, c)| {
            let mut v = row(c);
            v["month"] = json!(month);
            v
        })
        .collect();
    json!({
        "months": months,
        "repo": repo,
        "since": since,
        "team": row(&team),
        "repos": repo_rows.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
        "trend": trend,
    })
}

// ── DynamoDB ─────────────────────────────────────────────────────────

type Item = HashMap<String, AttributeValue>;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: impl ToString) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn gs(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn gn(i: &Item, k: &str) -> Option<i64> {
    i.get(k).and_then(|v| v.as_n().ok())?.parse().ok()
}

fn gset(i: &Item, k: &str) -> BTreeSet<String> {
    i.get(k)
        .and_then(|v| v.as_ss().ok())
        .map(|v| v.iter().cloned().collect())
        .unwrap_or_default()
}

impl PrRecord {
    pub fn to_item(&self, team: &str, now: chrono::DateTime<chrono::Utc>) -> Item {
        let mut i = Item::from([
            ("pk".to_string(), s(team)),
            ("sk".to_string(), s(&pr_sk(&self.repo, self.pr))),
            ("record_type".to_string(), s("scorecard_pr")),
            ("repo".to_string(), s(&self.repo)),
            ("pr".to_string(), n(self.pr)),
            ("state".to_string(), s(self.state.key())),
            ("rev".to_string(), n(self.rev)),
            ("rounds_at_approval".to_string(), n(self.rounds_at_approval)),
            (
                "open_items".to_string(),
                AttributeValue::Bool(self.open_items),
            ),
            ("updated_at".to_string(), s(&now.to_rfc3339())),
            ("ttl".to_string(), n(now.timestamp() + KEEP_DAYS * 86_400)),
        ]);
        for (k, v) in [
            ("opened_at", &self.opened_at),
            ("ended_at", &self.ended_at),
            ("head", &self.head),
            ("approved_at", &self.approved_at),
            ("merge_sha", &self.merge_sha),
            ("reverted_at", &self.reverted_at),
            ("reverted_by", &self.reverted_by),
        ] {
            if !v.is_empty() {
                i.insert(k.to_string(), s(v));
            }
        }
        if let Some(h) = self.human_commits {
            i.insert("human_commits".to_string(), n(h));
        }
        for (k, set) in [
            ("pushed", &self.pushed),
            ("change_requests", &self.change_requests),
        ] {
            if !set.is_empty() {
                i.insert(
                    k.to_string(),
                    AttributeValue::Ss(set.iter().cloned().collect()),
                );
            }
        }
        i
    }

    pub fn from_item(i: &Item) -> Option<PrRecord> {
        if !gs(i, "sk").starts_with(PR_PREFIX) {
            return None;
        }
        Some(PrRecord {
            repo: gs(i, "repo"),
            pr: gn(i, "pr")? as u64,
            opened_at: gs(i, "opened_at"),
            state: State::from_key(&gs(i, "state")),
            ended_at: gs(i, "ended_at"),
            head: gs(i, "head"),
            human_commits: gn(i, "human_commits").map(|v| v.max(0) as u64),
            pushed: gset(i, "pushed"),
            change_requests: gset(i, "change_requests"),
            approved_at: gs(i, "approved_at"),
            rounds_at_approval: gn(i, "rounds_at_approval").unwrap_or(0).max(0) as u64,
            open_items: i
                .get("open_items")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false),
            merge_sha: gs(i, "merge_sha"),
            reverted_at: gs(i, "reverted_at"),
            reverted_by: gs(i, "reverted_by"),
            rev: gn(i, "rev").unwrap_or(0).max(0) as u64,
        })
    }
}

/// Counters on a counter item (every number attribute but the bookkeeping).
pub fn counters_of(i: &Item) -> Counters {
    i.iter()
        .filter(|(k, _)| k.as_str() != "ttl")
        .filter_map(|(k, v)| Some((k.clone(), v.as_n().ok()?.parse::<i64>().ok()?)))
        .collect()
}

/// The settings table.
pub struct Dynamo {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub table: String,
}

impl Dynamo {
    pub fn new(dynamo: &aws_sdk_dynamodb::Client, table: &str) -> Self {
        Self {
            dynamo: dynamo.clone(),
            table: table.to_string(),
        }
    }

    async fn get(&self, team: &str, sk: &str) -> Result<Option<Item>, String> {
        self.dynamo
            .get_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(sk))
            .consistent_read(true)
            .send()
            .await
            .map(|r| r.item)
            .map_err(|e| aws_sdk_dynamodb::error::DisplayErrorContext(e).to_string())
    }
}

#[async_trait::async_trait]
impl Store for Dynamo {
    async fn get_pr(&self, team: &str, repo: &str, pr: u64) -> Result<Option<PrRecord>, String> {
        Ok(self
            .get(team, &pr_sk(repo, pr))
            .await?
            .as_ref()
            .and_then(PrRecord::from_item))
    }

    async fn commit(&self, team: &str, c: &Commit) -> Result<bool, String> {
        use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
        let now = chrono::Utc::now();
        let mut put = Put::builder()
            .table_name(&self.table)
            .set_item(Some(c.record.to_item(team, now)));
        put = match c.expect_rev {
            None => put.condition_expression("attribute_not_exists(sk)"),
            Some(rev) => put
                .condition_expression("#rev = :rev")
                .expression_attribute_names("#rev", "rev")
                .expression_attribute_values(":rev", n(rev)),
        };
        // The record goes first: its cancellation reason is at index 0.
        let mut tx = self.dynamo.transact_write_items().transact_items(
            TransactWriteItem::builder()
                .put(put.build().map_err(|e| e.to_string())?)
                .build(),
        );
        for (sk, counters) in &c.deltas {
            let mut adds = vec![];
            let mut update = Update::builder()
                .table_name(&self.table)
                .key("pk", s(team))
                .key("sk", s(sk))
                .expression_attribute_names("#rt", "record_type")
                .expression_attribute_names("#ua", "updated_at")
                .expression_attribute_values(":rt", s("scorecard"))
                .expression_attribute_values(":ua", s(&now.to_rfc3339()));
            for (i, (field, v)) in counters.iter().enumerate() {
                adds.push(format!("#c{i} :c{i}"));
                update = update
                    .expression_attribute_names(format!("#c{i}"), field)
                    .expression_attribute_values(format!(":c{i}"), n(v));
            }
            let update = update
                .update_expression(format!("ADD {} SET #rt = :rt, #ua = :ua", adds.join(", ")))
                .build()
                .map_err(|e| e.to_string())?;
            tx = tx.transact_items(TransactWriteItem::builder().update(update).build());
        }
        for p in &c.pointers {
            let put = Put::builder()
                .table_name(&self.table)
                .item("pk", s(team))
                .item("sk", s(&p.sk))
                .item("record_type", s("scorecard_pointer"))
                .item("repo", s(&p.repo))
                .item("pr", n(p.pr))
                .item("ttl", n(now.timestamp() + KEEP_DAYS * 86_400))
                .build()
                .map_err(|e| e.to_string())?;
            tx = tx.transact_items(TransactWriteItem::builder().put(put).build());
        }
        match tx.send().await {
            Ok(_) => Ok(true),
            Err(e) => {
                // The record changed under the write, or another transaction
                // holds one of the items: read again and try once more.
                let raced = matches!(
                    e.as_service_error(),
                    Some(TransactWriteItemsError::TransactionCanceledException(c))
                        if c.cancellation_reasons().first().and_then(|r| r.code())
                            == Some("ConditionalCheckFailed")
                            || c.cancellation_reasons()
                                .iter()
                                .any(|r| r.code() == Some("TransactionConflict"))
                );
                if raced {
                    Ok(false)
                } else {
                    Err(aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string())
                }
            }
        }
    }

    async fn pointer(&self, team: &str, sk: &str) -> Result<Option<Pointer>, String> {
        Ok(self.get(team, sk).await?.and_then(|i| {
            Some(Pointer {
                sk: sk.to_string(),
                repo: gs(&i, "repo"),
                pr: gn(&i, "pr")? as u64,
            })
        }))
    }

    async fn counters(&self, team: &str, prefix: &str) -> Result<Vec<(String, Counters)>, String> {
        let mut out = vec![];
        let mut start: Option<Item> = None;
        loop {
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
            for i in page.items() {
                out.push((gs(i, "sk"), counters_of(i)));
            }
            match page.last_evaluated_key {
                Some(k) if !k.is_empty() => start = Some(k),
                _ => break,
            }
        }
        Ok(out)
    }
}

/// The store in memory (tests): the same all-or-nothing write on the same
/// condition.
#[derive(Default)]
pub struct Memory {
    pub prs: std::sync::Mutex<BTreeMap<(String, String), PrRecord>>,
    pub counters: std::sync::Mutex<BTreeMap<(String, String), Counters>>,
    pub pointers: std::sync::Mutex<BTreeMap<(String, String), Pointer>>,
    /// Commits refused as if another writer got there first (tests).
    pub refuse: std::sync::Mutex<usize>,
}

impl Memory {
    pub fn count(&self, team: &str, sk: &str, field: &str) -> i64 {
        self.counters
            .lock()
            .unwrap()
            .get(&(team.to_string(), sk.to_string()))
            .and_then(|c| c.get(field).copied())
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl Store for Memory {
    async fn get_pr(&self, team: &str, repo: &str, pr: u64) -> Result<Option<PrRecord>, String> {
        Ok(self
            .prs
            .lock()
            .unwrap()
            .get(&(team.to_string(), pr_sk(repo, pr)))
            .cloned())
    }

    async fn commit(&self, team: &str, c: &Commit) -> Result<bool, String> {
        {
            let mut refuse = self.refuse.lock().unwrap();
            if *refuse > 0 {
                *refuse -= 1;
                return Ok(false);
            }
        }
        let mut prs = self.prs.lock().unwrap();
        let key = (team.to_string(), pr_sk(&c.record.repo, c.record.pr));
        if prs.get(&key).map(|r| r.rev) != c.expect_rev {
            return Ok(false);
        }
        prs.insert(key, c.record.clone());
        let mut counters = self.counters.lock().unwrap();
        for (sk, d) in &c.deltas {
            add_into(
                counters.entry((team.to_string(), sk.clone())).or_default(),
                d,
            );
        }
        let mut pointers = self.pointers.lock().unwrap();
        for p in &c.pointers {
            pointers.insert((team.to_string(), p.sk.clone()), p.clone());
        }
        Ok(true)
    }

    async fn pointer(&self, team: &str, sk: &str) -> Result<Option<Pointer>, String> {
        Ok(self
            .pointers
            .lock()
            .unwrap()
            .get(&(team.to_string(), sk.to_string()))
            .cloned())
    }

    async fn counters(&self, team: &str, prefix: &str) -> Result<Vec<(String, Counters)>, String> {
        Ok(self
            .counters
            .lock()
            .unwrap()
            .iter()
            .filter(|((t, sk), _)| t == team && sk.starts_with(prefix))
            .map(|((_, sk), c)| (sk.clone(), c.clone()))
            .collect())
    }
}

// ── Reading the webhooks ─────────────────────────────────────────────

/// Whether a pull request (GitHub's object) is one the scorecard counts:
/// opened by CoderHelm's bot (`bot`: the app's own login when known) from a
/// branch in the repository itself, and not a guide pull request (those
/// propose lessons, they are not code work).
pub fn counts(pr: &Value, bot: Option<&str>) -> bool {
    let Some(f) = crate::own_pr::PrFacts::from_github(pr) else {
        return false;
    };
    crate::own_pr::is_coderhelm_bot(&f.author, &f.author_type, bot)
        && !f.head.is_empty()
        && (f.head_repo.is_empty() || f.head_repo.eq_ignore_ascii_case(&f.repo))
        && !crate::guide::is_guide_branch(&f.head)
}

/// The event of a `pull_request` webhook (`opened`, `reopened`, `closed`);
/// None for the other actions. `human_commits`: people's commits counted on
/// GitHub at a merge, when they could be.
pub fn pr_event(action: &str, pr: &Value, human_commits: Option<u64>) -> Option<Event> {
    let text = |k: &str| pr[k].as_str().unwrap_or("").to_string();
    let head = pr["head"]["ref"].as_str().unwrap_or("").to_string();
    match action {
        "opened" => Some(Event::Opened {
            at: text("created_at"),
            head,
        }),
        "reopened" => Some(Event::Reopened {
            opened_at: text("created_at"),
            head,
        }),
        "closed" => {
            let merged = pr["merged"].as_bool().unwrap_or(false) || !text("merged_at").is_empty();
            let at = if merged {
                text("merged_at")
            } else {
                text("closed_at")
            };
            // Not closed after all (a pull request read back while still
            // open): there is nothing to count.
            if at.is_empty() {
                return None;
            }
            Some(Event::Ended {
                merged,
                at,
                opened_at: text("created_at"),
                head,
                human_commits: if merged { human_commits } else { None },
                open_items: has_open_items(pr["body"].as_str().unwrap_or("")),
                merge_sha: if merged {
                    text("merge_commit_sha")
                } else {
                    String::new()
                },
            })
        }
        _ => None,
    }
}

/// The event of a person's submitted review (`pull_request_review`); None
/// for a bot's.
pub fn review_event(payload: &Value) -> Option<Event> {
    let review = &payload["review"];
    let who = &review["user"];
    let login = who["login"].as_str().unwrap_or("");
    if login.is_empty() || who["type"].as_str() == Some("Bot") || login.ends_with("[bot]") {
        return None;
    }
    let id = review["id"].as_u64()?;
    Some(Event::Review {
        id: id.to_string(),
        state: review["state"].as_str().unwrap_or("").to_ascii_lowercase(),
        at: review["submitted_at"].as_str().unwrap_or("").to_string(),
        opened_at: payload["pull_request"]["created_at"]
            .as_str()
            .unwrap_or("")
            .to_string(),
    })
}

/// The branch a push went to; None for a tag.
pub fn pushed_branch(payload: &Value) -> Option<&str> {
    payload["ref"].as_str()?.strip_prefix("refs/heads/")
}

/// The commits of a push that a person wrote on top of the branch: not a
/// bot's, not CoderHelm's, and not a merge of another branch into it (a
/// push payload carries no parents, so a merge is known by its message, as
/// git and GitHub's "Update branch" write it).
pub fn human_push_shas(payload: &Value) -> Vec<String> {
    payload["commits"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| {
            let who = c["author"]["username"]
                .as_str()
                .or(c["committer"]["username"].as_str())
                .unwrap_or("");
            let name = c["author"]["name"].as_str().unwrap_or("");
            let bot =
                |s: &str| s.ends_with("[bot]") || s.to_ascii_lowercase().contains("coderhelm");
            let msg = c["message"].as_str().unwrap_or("");
            let merge = msg.starts_with("Merge branch ")
                || msg.starts_with("Merge remote-tracking branch ")
                || msg.starts_with("Merge pull request ");
            !(who.is_empty() && name.is_empty()) && !bot(who) && !bot(name) && !merge
        })
        .filter_map(|c| c["id"].as_str().map(str::to_string))
        .collect()
}

/// A commit that reverts another, as `git revert` writes it: the sha it
/// names and why (the message without git's own lines).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevertCommit {
    pub sha: String,
    pub reverted_sha: String,
    pub reason: String,
    pub at: String,
}

/// The sha a commit message says it reverts.
pub fn reverted_sha(message: &str) -> Option<String> {
    let at = message.find("This reverts commit ")?;
    let sha: String = message[at + "This reverts commit ".len()..]
        .chars()
        .take_while(|c| c.is_ascii_hexdigit())
        .collect();
    (sha.len() >= 7).then(|| sha.to_ascii_lowercase())
}

/// Why a revert was made, in the person's words: the message of the revert
/// commit or pull request without the lines git and GitHub write
/// themselves. Empty when they added nothing.
pub fn revert_reason(text: &str) -> String {
    let kept: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .filter(|l| {
            let low = l.to_ascii_lowercase();
            !(low.starts_with("revert \"")
                || low.starts_with("this reverts commit")
                || low.starts_with("reverts ")
                || low.starts_with("<!--"))
        })
        .collect();
    crate::truncate_str(&kept.join(" "), 400).to_string()
}

/// The revert commits in a push.
pub fn revert_commits(payload: &Value) -> Vec<RevertCommit> {
    payload["commits"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| {
            let msg = c["message"].as_str()?;
            Some(RevertCommit {
                sha: c["id"].as_str()?.to_string(),
                reverted_sha: reverted_sha(msg)?,
                reason: revert_reason(msg),
                at: c["timestamp"].as_str().unwrap_or("").to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    const T: &str = "TEAM#a";
    const R: &str = "acme/web";

    fn opened(at: &str) -> Event {
        Event::Opened {
            at: at.into(),
            head: "coderhelm/abc-1".into(),
        }
    }

    fn ended(merged: bool, at: &str, human: Option<u64>) -> Event {
        Event::Ended {
            merged,
            at: at.into(),
            opened_at: "2026-10-01T00:00:00Z".into(),
            head: "coderhelm/abc-1".into(),
            human_commits: human,
            open_items: false,
            merge_sha: if merged {
                "ABCDEF1234".into()
            } else {
                String::new()
            },
        }
    }

    fn review(id: &str, state: &str, at: &str) -> Event {
        Event::Review {
            id: id.into(),
            state: state.into(),
            at: at.into(),
            opened_at: "2026-10-01T00:00:00Z".into(),
        }
    }

    async fn all(m: &Memory, field: &str) -> (i64, i64, i64, i64) {
        (
            m.count(T, &team_sk(ALL), field),
            m.count(T, &team_sk("2026-10"), field),
            m.count(T, &repo_agg_sk(ALL, R), field),
            m.count(T, &repo_agg_sk("2026-10", R), field),
        )
    }

    #[tokio::test]
    async fn a_merged_pull_request_counts_once_however_often_it_is_delivered() {
        let m = Memory::default();
        for _ in 0..3 {
            record(&m, T, R, 7, &opened("2026-10-01T00:00:00Z"))
                .await
                .unwrap();
            record(&m, T, R, 7, &ended(true, "2026-10-01T03:00:00Z", Some(0)))
                .await
                .unwrap();
        }
        assert_eq!(all(&m, "opened").await, (1, 1, 1, 1));
        assert_eq!(all(&m, "merged").await, (1, 1, 1, 1));
        assert_eq!(all(&m, "as_written").await, (1, 1, 1, 1));
        assert_eq!(all(&m, "closed").await, (0, 0, 0, 0));
        assert_eq!(all(&m, "merge_secs").await.0, 3 * 3600);
        assert_eq!(all(&m, "mt_4").await.0, 1);
        // The merge commit can be found again, whatever its case.
        let p = m.pointer(T, &sha_sk(R, "abcdef1234")).await.unwrap();
        assert_eq!(p.map(|p| p.pr), Some(7));
    }

    #[tokio::test]
    async fn the_merge_arriving_before_the_open_still_counts_one_opened() {
        let m = Memory::default();
        record(&m, T, R, 7, &ended(true, "2026-10-02T00:00:00Z", Some(2)))
            .await
            .unwrap();
        record(&m, T, R, 7, &opened("2026-10-01T00:00:00Z"))
            .await
            .unwrap();
        assert_eq!(all(&m, "opened").await, (1, 1, 1, 1));
        assert_eq!(all(&m, "human_edited").await, (1, 1, 1, 1));
        assert_eq!(all(&m, "as_written").await, (0, 0, 0, 0));
    }

    #[tokio::test]
    async fn closed_then_reopened_then_merged_moves_between_the_counters() {
        let m = Memory::default();
        record(&m, T, R, 7, &opened("2026-10-01T00:00:00Z"))
            .await
            .unwrap();
        record(&m, T, R, 7, &ended(false, "2026-10-02T00:00:00Z", None))
            .await
            .unwrap();
        assert_eq!(all(&m, "closed").await, (1, 1, 1, 1));
        record(
            &m,
            T,
            R,
            7,
            &Event::Reopened {
                opened_at: "2026-10-01T00:00:00Z".into(),
                head: "coderhelm/abc-1".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(all(&m, "closed").await, (0, 0, 0, 0));
        record(&m, T, R, 7, &ended(true, "2026-10-03T00:00:00Z", Some(0)))
            .await
            .unwrap();
        assert_eq!(all(&m, "merged").await, (1, 1, 1, 1));
        // A late "closed" delivery never undoes the merge.
        record(&m, T, R, 7, &ended(false, "2026-10-02T00:00:00Z", None))
            .await
            .unwrap();
        assert_eq!(all(&m, "merged").await, (1, 1, 1, 1));
        assert_eq!(all(&m, "closed").await, (0, 0, 0, 0));
        assert_eq!(all(&m, "opened").await, (1, 1, 1, 1));
    }

    #[tokio::test]
    async fn review_rounds_are_the_change_requests_before_the_first_approval() {
        let m = Memory::default();
        record(&m, T, R, 7, &opened("2026-10-01T00:00:00Z"))
            .await
            .unwrap();
        for (id, state, at) in [
            ("r1", "changes_requested", "2026-10-01T01:00:00Z"),
            ("r1", "changes_requested", "2026-10-01T01:00:00Z"), // redelivered
            ("r2", "commented", "2026-10-01T02:00:00Z"),
            ("r3", "changes_requested", "2026-10-01T03:00:00Z"),
            ("r4", "approved", "2026-10-01T04:00:00Z"),
            // After the approval: not a round to approval.
            ("r5", "changes_requested", "2026-10-01T05:00:00Z"),
            ("r6", "approved", "2026-10-01T06:00:00Z"),
        ] {
            record(&m, T, R, 7, &review(id, state, at)).await.unwrap();
        }
        assert_eq!(all(&m, "reviewed").await, (1, 1, 1, 1));
        assert_eq!(all(&m, "rounds").await, (2, 2, 2, 2));
        assert_eq!(all(&m, "first_pass").await, (0, 0, 0, 0));

        // Approved with no change request: a first pass.
        record(&m, T, R, 8, &opened("2026-10-05T00:00:00Z"))
            .await
            .unwrap();
        record(
            &m,
            T,
            R,
            8,
            &review("a", "approved", "2026-10-05T01:00:00Z"),
        )
        .await
        .unwrap();
        assert_eq!(all(&m, "reviewed").await.0, 2);
        assert_eq!(all(&m, "first_pass").await.0, 1);
        assert_eq!(all(&m, "rounds").await.0, 2);

        // Merged with change requests and no approval on record: its rounds
        // are counted at the merge.
        record(&m, T, R, 9, &opened("2026-10-06T00:00:00Z"))
            .await
            .unwrap();
        record(
            &m,
            T,
            R,
            9,
            &review("x", "changes_requested", "2026-10-06T01:00:00Z"),
        )
        .await
        .unwrap();
        assert_eq!(all(&m, "reviewed").await.0, 2);
        record(&m, T, R, 9, &ended(true, "2026-10-06T02:00:00Z", Some(0)))
            .await
            .unwrap();
        assert_eq!(all(&m, "reviewed").await.0, 3);
        assert_eq!(all(&m, "rounds").await.0, 3);
    }

    #[tokio::test]
    async fn a_persons_push_counts_at_the_merge_and_githubs_count_wins() {
        let m = Memory::default();
        // A push to a branch nobody recorded changes nothing.
        assert_eq!(
            record(
                &m,
                T,
                R,
                7,
                &Event::HumanPush {
                    shas: vec!["c1".into()]
                }
            )
            .await
            .unwrap(),
            None
        );
        record(&m, T, R, 7, &opened("2026-10-01T00:00:00Z"))
            .await
            .unwrap();
        let p = m
            .pointer(T, &branch_sk(R, "coderhelm/abc-1"))
            .await
            .unwrap();
        assert_eq!(p.map(|p| p.pr), Some(7));
        for _ in 0..2 {
            record(
                &m,
                T,
                R,
                7,
                &Event::HumanPush {
                    shas: vec!["c1".into(), "c2".into()],
                },
            )
            .await
            .unwrap();
        }
        // Not merged yet: nobody "needed a commit before merge" so far.
        assert_eq!(all(&m, "human_edited").await.0, 0);
        // GitHub couldn't be asked at the merge: the pushes are the count.
        record(&m, T, R, 7, &ended(true, "2026-10-02T00:00:00Z", None))
            .await
            .unwrap();
        assert_eq!(all(&m, "human_edited").await, (1, 1, 1, 1));
        // GitHub says both were merges of the base branch: its count wins.
        record(&m, T, R, 7, &ended(true, "2026-10-02T00:00:00Z", Some(0)))
            .await
            .unwrap();
        assert_eq!(all(&m, "human_edited").await, (0, 0, 0, 0));
        assert_eq!(all(&m, "as_written").await, (1, 1, 1, 1));
    }

    #[tokio::test]
    async fn a_revert_counts_once_and_only_against_a_merged_pull_request() {
        let m = Memory::default();
        let rev = Event::Reverted {
            at: "2026-10-09T00:00:00Z".into(),
            by: "#12".into(),
        };
        record(&m, T, R, 7, &opened("2026-10-01T00:00:00Z"))
            .await
            .unwrap();
        record(&m, T, R, 7, &rev).await.unwrap();
        assert_eq!(all(&m, "reverted").await.0, 0, "not merged: no revert");
        record(&m, T, R, 7, &ended(true, "2026-10-02T00:00:00Z", Some(0)))
            .await
            .unwrap();
        for _ in 0..2 {
            record(&m, T, R, 7, &rev).await.unwrap();
        }
        assert_eq!(all(&m, "reverted").await, (1, 1, 1, 1));
        let rec = m.get_pr(T, R, 7).await.unwrap().unwrap();
        assert_eq!(rec.reverted_by, "#12");
    }

    #[tokio::test]
    async fn open_items_count_when_the_pull_request_finishes() {
        let m = Memory::default();
        let mut e = ended(true, "2026-10-02T00:00:00Z", Some(0));
        if let Event::Ended { open_items, .. } = &mut e {
            *open_items = true;
        }
        record(&m, T, R, 7, &e).await.unwrap();
        assert_eq!(all(&m, "open_items").await, (1, 1, 1, 1));
        assert!(has_open_items(
            "Summary\n\n## Deferred\n- The export job: its fixture needs a key\n"
        ));
        assert!(has_open_items(&format!(
            "x\n{OPEN_FINDINGS_START}\n### Open review findings\n- A (`a.rs:1`)\n{OPEN_FINDINGS_END}\n"
        )));
        assert!(!has_open_items("Summary\n\n## Deferred\n\n## Testing\nran"));
        assert!(!has_open_items(&format!(
            "x\n{OPEN_FINDINGS_START}\n{OPEN_FINDINGS_END}\n- unrelated list"
        )));
        assert!(!has_open_items("Nothing deferred here."));
    }

    #[tokio::test]
    async fn a_write_that_lost_a_race_reads_again_and_counts_once() {
        let m = Memory::default();
        *m.refuse.lock().unwrap() = 2;
        record(&m, T, R, 7, &opened("2026-10-01T00:00:00Z"))
            .await
            .unwrap();
        assert_eq!(all(&m, "opened").await, (1, 1, 1, 1));
        *m.refuse.lock().unwrap() = MAX_TRIES;
        assert!(record(&m, T, R, 8, &opened("2026-10-01T00:00:00Z"))
            .await
            .is_err());
        assert_eq!(all(&m, "opened").await, (1, 1, 1, 1));
    }

    #[tokio::test]
    async fn teams_and_repositories_never_share_a_counter() {
        let m = Memory::default();
        record(&m, T, R, 7, &opened("2026-10-01T00:00:00Z"))
            .await
            .unwrap();
        record(&m, T, "acme/api", 7, &opened("2026-11-01T00:00:00Z"))
            .await
            .unwrap();
        record(&m, "TEAM#b", R, 7, &opened("2026-10-01T00:00:00Z"))
            .await
            .unwrap();
        assert_eq!(m.count(T, &team_sk(ALL), "opened"), 2);
        assert_eq!(m.count(T, &repo_agg_sk(ALL, R), "opened"), 1);
        assert_eq!(m.count(T, &repo_agg_sk(ALL, "acme/api"), "opened"), 1);
        assert_eq!(m.count(T, &team_sk("2026-11"), "opened"), 1);
        assert_eq!(m.count("TEAM#b", &team_sk(ALL), "opened"), 1);
    }

    #[test]
    fn the_record_round_trips_through_its_item() {
        let mut rec = PrRecord {
            repo: R.into(),
            pr: 7,
            ..Default::default()
        };
        apply(&mut rec, &opened("2026-10-01T00:00:00Z"));
        apply(
            &mut rec,
            &review("r1", "changes_requested", "2026-10-01T01:00:00Z"),
        );
        apply(&mut rec, &review("r2", "approved", "2026-10-01T02:00:00Z"));
        apply(
            &mut rec,
            &Event::HumanPush {
                shas: vec!["c1".into()],
            },
        );
        apply(&mut rec, &ended(true, "2026-10-01T03:00:00Z", Some(1)));
        rec.rev = 4;
        let now = chrono::Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap();
        let item = rec.to_item(T, now);
        assert_eq!(item["sk"], s("SCOREPR#acme/web#000007"));
        assert_eq!(PrRecord::from_item(&item), Some(rec));
        assert_eq!(PrRecord::from_item(&Item::new()), None);
    }

    #[test]
    fn the_view_gives_rates_only_where_something_is_behind_them() {
        let now = chrono::Utc.with_ymd_and_hms(2026, 11, 13, 0, 0, 0).unwrap();
        let c = |pairs: &[(&str, i64)]| -> Counters {
            pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
        };
        let oct = c(&[
            ("opened", 10),
            ("merged", 6),
            ("closed", 2),
            ("reverted", 1),
            ("human_edited", 3),
            ("as_written", 3),
            ("open_items", 2),
            ("reviewed", 4),
            ("rounds", 6),
            ("first_pass", 1),
            ("merge_n", 6),
            ("merge_secs", 6 * 7200),
            ("mt_1", 1),
            ("mt_4", 4),
            ("mt_more", 1),
        ]);
        let nov = c(&[("opened", 2)]);
        let mut all_time = oct.clone();
        add_into(&mut all_time, &nov);
        let items = vec![
            (team_sk(ALL), all_time.clone()),
            (team_sk("2026-10"), oct.clone()),
            (team_sk("2026-11"), nov.clone()),
            (repo_agg_sk(ALL, R), all_time.clone()),
            (repo_agg_sk("2026-10", R), oct.clone()),
            (repo_agg_sk("2026-11", R), nov.clone()),
            (repo_agg_sk(ALL, "acme/api"), Counters::new()),
        ];
        let v = view(&items, 0, "", now);
        assert_eq!(v["team"]["opened"], 12);
        assert_eq!(v["team"]["open"], 4);
        assert_eq!(v["team"]["merge_rate"], 0.75);
        assert_eq!(v["team"]["human_edited_rate"], 0.5);
        assert_eq!(v["team"]["avg_review_rounds"], 1.5);
        assert_eq!(v["team"]["avg_hours_to_merge"], 2.0);
        assert_eq!(v["team"]["median_hours_to_merge_at_most"], 4);
        assert_eq!(v["since"], "2026-10");
        assert_eq!(v["repos"][0]["repo"], R);
        assert_eq!(v["trend"].as_array().unwrap().len(), 2);
        // This month only: nothing finished, so no rate is made up.
        let v = view(&items, 1, "", now);
        assert_eq!(v["team"]["opened"], 2);
        assert!(v["team"]["merge_rate"].is_null());
        assert!(v["team"]["avg_hours_to_merge"].is_null());
        assert!(v["team"]["median_hours_to_merge_at_most"].is_null());
        // Two months, one repository.
        let v = view(&items, 2, R, now);
        assert_eq!(v["team"]["opened"], 12);
        assert_eq!(v["repos"].as_array().unwrap().len(), 1);
        let v = view(&items, 2, "acme/none", now);
        assert_eq!(v["team"]["opened"], 0);
        assert_eq!(
            window_months(
                chrono::Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
                3
            ),
            vec!["2026-01", "2025-12", "2025-11"]
        );
        // A median past the last bound is said so, not given a number.
        let slow = c(&[("merged", 1), ("merge_n", 1), ("mt_more", 1)]);
        assert!(row(&slow)["median_hours_to_merge_at_most"].is_null());
        assert_eq!(row(&slow)["median_past_hours"], 336);
        assert_eq!(merge_bucket(3600), "mt_1");
        assert_eq!(merge_bucket(3601), "mt_4");
        assert_eq!(merge_bucket(400 * 3600), "mt_more");
    }

    #[test]
    fn pushes_and_revert_commits_are_read_from_the_payload() {
        let payload = json!({"commits": [
            {"id": "c1", "message": "Use the shared client", "author": {"username": "pat", "name": "Pat"}},
            {"id": "c2", "message": "Merge branch 'main' into coderhelm/abc-1", "author": {"username": "pat"}},
            {"id": "c3", "message": "ABC-1: fix", "author": {"username": "coderhelm[bot]"}},
            {"id": "c4", "message": "bump", "author": {"username": "dependabot[bot]"}},
            {"id": "c5", "message": "no author", "author": {}},
            {"id": "c6", "timestamp": "2026-10-09T00:00:00Z",
             "message": "Revert \"ABC-1: fix\"\n\nThis reverts commit ABCDEF1234567890.\n\nIt broke checkout for guests.",
             "author": {"username": "sam"}},
        ]});
        assert_eq!(human_push_shas(&payload), vec!["c1", "c6"]);
        let reverts = revert_commits(&payload);
        assert_eq!(reverts.len(), 1);
        assert_eq!(reverts[0].reverted_sha, "abcdef1234567890");
        assert_eq!(reverts[0].reason, "It broke checkout for guests.");
        assert_eq!(reverted_sha("This reverts commit abc."), None);
        assert_eq!(
            revert_reason("Reverts acme/web#12\n\nGuests could not pay."),
            "Guests could not pay."
        );
        assert_eq!(revert_reason("Revert \"x\"\n\nReverts acme/web#12"), "");
    }

    #[test]
    fn webhooks_become_events_only_for_coderhelms_own_code_pull_requests() {
        let pr = |login: &str, kind: &str, head: &str, head_repo: &str| {
            json!({
                "number": 7, "state": "open",
                "user": {"login": login, "type": kind},
                "head": {"ref": head, "sha": "h", "repo": {"full_name": head_repo}},
                "base": {"ref": "main", "repo": {"full_name": "acme/web"}},
                "created_at": "2026-10-01T00:00:00Z",
            })
        };
        let own = pr("coderhelm[bot]", "Bot", "coderhelm/abc-1", "acme/web");
        assert!(counts(&own, Some("coderhelm[bot]")));
        assert!(counts(&own, None));
        assert!(!counts(&pr("pat", "User", "fix", "acme/web"), None));
        assert!(!counts(
            &pr("coderhelm[bot]", "Bot", "coderhelm/abc-1", "someone/web"),
            None
        ));
        // A guide pull request proposes lessons: not counted as code work.
        assert!(!counts(
            &pr(
                "coderhelm[bot]",
                "Bot",
                "coderhelm/guide-lessons-20261020-0930",
                "acme/web"
            ),
            None
        ));
        assert_eq!(
            pr_event("opened", &own, None),
            Some(Event::Opened {
                at: "2026-10-01T00:00:00Z".into(),
                head: "coderhelm/abc-1".into()
            })
        );
        assert_eq!(pr_event("labeled", &own, None), None);
        // "closed" for a pull request that reads as open counts nothing.
        assert_eq!(pr_event("closed", &own, Some(1)), None);
        let mut merged = own.clone();
        merged["merged"] = json!(true);
        merged["merged_at"] = json!("2026-10-02T00:00:00Z");
        merged["closed_at"] = json!("2026-10-02T00:00:00Z");
        merged["merge_commit_sha"] = json!("abc123def");
        merged["body"] = json!("Done.\n\n## Deferred\n- The export job");
        assert_eq!(
            pr_event("closed", &merged, Some(2)),
            Some(Event::Ended {
                merged: true,
                at: "2026-10-02T00:00:00Z".into(),
                opened_at: "2026-10-01T00:00:00Z".into(),
                head: "coderhelm/abc-1".into(),
                human_commits: Some(2),
                open_items: true,
                merge_sha: "abc123def".into(),
            })
        );
        let mut closed = own.clone();
        closed["closed_at"] = json!("2026-10-03T00:00:00Z");
        let Some(Event::Ended {
            merged: false,
            at,
            human_commits: None,
            ..
        }) = pr_event("closed", &closed, Some(2))
        else {
            panic!("a close without a merge");
        };
        assert_eq!(at, "2026-10-03T00:00:00Z");

        let review = |login: &str, kind: &str, state: &str| {
            json!({
                "review": {"id": 91, "state": state, "submitted_at": "2026-10-01T05:00:00Z",
                           "user": {"login": login, "type": kind}},
                "pull_request": {"created_at": "2026-10-01T00:00:00Z"},
            })
        };
        assert_eq!(
            review_event(&review("pat", "User", "CHANGES_REQUESTED")),
            Some(Event::Review {
                id: "91".into(),
                state: "changes_requested".into(),
                at: "2026-10-01T05:00:00Z".into(),
                opened_at: "2026-10-01T00:00:00Z".into()
            })
        );
        assert_eq!(
            review_event(&review("coderhelm[bot]", "Bot", "commented")),
            None
        );
        assert_eq!(
            pushed_branch(&json!({"ref": "refs/heads/coderhelm/abc-1"})),
            Some("coderhelm/abc-1")
        );
        assert_eq!(pushed_branch(&json!({"ref": "refs/tags/v1"})), None);
    }

    #[test]
    fn keys_read_back() {
        assert_eq!(
            parse_agg_sk("SCORE#2026-10#R#acme/web"),
            Some(AggKey {
                period: "2026-10".into(),
                repo: Some("acme/web".into())
            })
        );
        assert_eq!(
            parse_agg_sk("SCORE#ALL#T"),
            Some(AggKey {
                period: "ALL".into(),
                repo: None
            })
        );
        assert_eq!(parse_agg_sk("SCOREPR#acme/web#000007"), None);
        assert_eq!(month_of("2026-10-31T23:30:00-05:00"), "2026-11");
        assert_eq!(month_of("nope"), "");
    }
}
