//! The worker's side of `common::github_budget`: who is asking (a label on
//! the task), what each caller spent of each installation's hour, and the
//! one decision every request takes before it leaves (`github::send_tracked`).
//!
//! The counts live in this process and on the analytics table. They are
//! written once per handled message ([`flush`]), with one `github_budget`
//! log line per installation, and read back at most once a minute per
//! installation ([`refresh`]), so a worker that just started knows what the
//! others spent this hour, the budget GitHub last reported and a limit
//! another worker met.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{LazyLock, Mutex};

use aws_sdk_dynamodb::types::AttributeValue;
use common::github_budget::{self as rules, Budget, Spend};
use tracing::{info, warn};

use crate::WorkerState;

tokio::task_local! {
    static CALLER: &'static str;
}

/// Runs `f` with its GitHub requests counted under `caller`.
pub async fn scoped<F: Future>(caller: &'static str, f: F) -> F::Output {
    CALLER.scope(caller, f).await
}

/// Who the current task's GitHub requests are for.
pub fn caller() -> &'static str {
    CALLER.try_with(|c| *c).unwrap_or(rules::caller::OTHER)
}

/// What one caller did with one installation's budget.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Requests GitHub counted.
    pub sent: u64,
    /// Conditional requests that came back unchanged (not counted by GitHub).
    pub not_modified: u64,
    /// Requests GitHub refused for its rate limit.
    pub limited: u64,
    /// Requests not sent (the limit was spent, or the budget was the runs').
    pub held: u64,
    /// Reads answered from what this process kept (`github_cache`), with
    /// no request: they can't have changed.
    pub cached: u64,
}

#[derive(Default)]
struct Install {
    /// The hour (`now / 3600`) `spent` is about.
    hour: u64,
    /// Counted requests this hour by caller: what other workers recorded
    /// (as of the last refresh) and what this process sent since.
    spent: HashMap<String, u64>,
    /// Not yet written, by hour and caller.
    pending: HashMap<(u64, &'static str), Counts>,
    /// Until when GitHub said the hourly budget is gone (epoch seconds).
    limited_until: u64,
    /// When the budget this process holds was reported (epoch seconds).
    budget_at: u64,
    /// When the record was last read (epoch seconds).
    refreshed_at: u64,
    /// Counted requests not yet logged, by caller and route.
    routes: HashMap<(&'static str, String), u64>,
}

impl Install {
    fn roll(&mut self, now: u64) {
        let hour = now / 3600;
        if self.hour != hour {
            self.hour = hour;
            self.spent.clear();
        }
    }
}

static LEDGER: LazyLock<Mutex<HashMap<u64, Install>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn with<R>(installation_id: u64, now: u64, f: impl FnOnce(&mut Install) -> R) -> Option<R> {
    let mut all = LEDGER.lock().ok()?;
    let inst = all.entry(installation_id).or_default();
    inst.roll(now);
    Some(f(inst))
}

/// Whether `caller` may send a request for the installation now.
pub fn standing(installation_id: u64, caller: &str, now: u64) -> Spend {
    let budget = super::github::budget_of(installation_id);
    with(installation_id, now, |i| {
        rules::decide(
            caller,
            budget,
            (i.limited_until > 0).then_some(i.limited_until),
            i.spent.get(caller).copied().unwrap_or(0),
            now,
        )
    })
    .unwrap_or(Spend::Go)
}

/// How many more counted requests `caller` may make for the installation
/// now (see `common::github_budget::allowance`).
pub fn allowance(installation_id: u64, caller: &str, now: u64) -> u64 {
    let budget = super::github::budget_of(installation_id);
    with(installation_id, now, |i| {
        rules::allowance(
            caller,
            budget,
            (i.limited_until > 0).then_some(i.limited_until),
            i.spent.get(caller).copied().unwrap_or(0),
            now,
        )
    })
    .unwrap_or(u64::MAX)
}

/// The decision for the current task's next request, counted when held.
pub fn admit(installation_id: u64, now: u64) -> Spend {
    let who = caller();
    let s = standing(installation_id, who, now);
    if matches!(s, Spend::Hold { .. }) {
        with(installation_id, now, |i| {
            i.pending.entry((i.hour, who)).or_default().held += 1;
        });
    }
    s
}

/// Counts a response to the current task's request.
pub fn responded(installation_id: u64, status: u16, limited: bool, now: u64) {
    let who = caller();
    with(installation_id, now, |i| {
        let c = i.pending.entry((i.hour, who)).or_default();
        if status == 304 {
            c.not_modified += 1;
            return;
        }
        c.sent += 1;
        if limited {
            c.limited += 1;
        }
        *i.spent.entry(who.to_string()).or_default() += 1;
    });
}

/// A read the current task made was answered from what this process kept,
/// with no request.
pub fn served_kept(installation_id: u64, now: u64) {
    let who = caller();
    with(installation_id, now, |i| {
        i.pending.entry((i.hour, who)).or_default().cached += 1;
    });
}

/// Counts a counted request of the current task by its route
/// ([`route_of`]), for the `github_budget` log line.
pub fn route_sent(installation_id: u64, route: String, now: u64) {
    let who = caller();
    with(installation_id, now, |i| {
        *i.routes.entry((who, route)).or_default() += 1;
    });
}

/// The routes logged on a `github_budget` line at most.
pub const MAX_ROUTES_LOGGED: usize = 15;

/// A request's route with its names taken out, so requests count together
/// by what they ask: `GET /repos/:o/:r/pulls/:n`.
pub fn route_of(method: &str, url: &str) -> String {
    let path = url
        .split_once("://")
        .map(|(_, rest)| rest.find('/').map_or("/", |i| &rest[i..]))
        .unwrap_or(url);
    let path = path.split(['?', '#']).next().unwrap_or("");
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i < segs.len() {
        let seg = segs[i];
        let prev = out.last().map(String::as_str).unwrap_or("");
        let named = |s: &str| {
            if s.bytes().all(|b| b.is_ascii_digit()) {
                ":n".to_string()
            } else if (7..=40).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_hexdigit()) {
                ":sha".to_string()
            } else {
                s.to_string()
            }
        };
        match prev {
            "repos" if i + 1 < segs.len() => {
                out.push(":o/:r".into());
                i += 2;
                continue;
            }
            // Everything after these names one thing.
            "contents" | "ref" | "refs" | "compare" | "tarball" | "zipball" | "branches" => {
                out.push(":name".into());
                break;
            }
            _ => out.push(named(seg)),
        }
        i += 1;
    }
    format!("{method} /{}", out.join("/"))
}

/// The routes counted for the installation since the last call, most first:
/// `run GET /repos/:o/:r/pulls/:n=41`.
pub fn drain_routes(installation_id: u64) -> Vec<(String, u64)> {
    let Ok(mut all) = LEDGER.lock() else {
        return Vec::new();
    };
    let Some(inst) = all.get_mut(&installation_id) else {
        return Vec::new();
    };
    let mut out: Vec<(String, u64)> = inst
        .routes
        .drain()
        .map(|((who, route), n)| (format!("{who} {route}"), n))
        .collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

/// The `github_budget` log line's routes field: the most counted first.
pub fn routes_field(routes: &[(String, u64)]) -> String {
    routes
        .iter()
        .take(MAX_ROUTES_LOGGED)
        .map(|(r, n)| format!("{r}={n}"))
        .collect::<Vec<_>>()
        .join("; ")
}

/// GitHub said the installation's hourly budget is gone until `until`.
pub fn limit_spent(installation_id: u64, until: u64, now: u64) {
    with(installation_id, now, |i| {
        i.limited_until = i.limited_until.max(until);
    });
}

/// This process read the installation's budget from a response at `now`.
pub fn budget_seen(installation_id: u64, now: u64) {
    with(installation_id, now, |i| i.budget_at = now);
}

/// Until when the installation's hourly budget is known to be gone.
pub fn limited_until(installation_id: u64, now: u64) -> Option<u64> {
    with(installation_id, now, |i| i.limited_until).filter(|u| *u > now)
}

/// What another worker recorded for the installation, taken in: the hour's
/// counts by caller (never fewer than this process already knows), its last
/// reported budget when newer than this process's, and a spent limit.
pub fn adopt(
    installation_id: u64,
    hour: u64,
    counts: &HashMap<String, u64>,
    budget: Option<(Budget, u64)>,
    limited: u64,
    now: u64,
) {
    let newer = with(installation_id, now, |i| {
        if hour == i.hour {
            for (caller, n) in counts {
                let unwritten = i
                    .pending
                    .iter()
                    .filter(|((h, c), _)| *h == hour && *c == caller.as_str())
                    .map(|(_, c)| c.sent)
                    .sum::<u64>();
                let known = i.spent.entry(caller.clone()).or_default();
                *known = (*known).max(n + unwritten);
            }
        }
        i.limited_until = i.limited_until.max(limited);
        i.refreshed_at = now;
        match budget {
            Some((b, at)) if at > i.budget_at => {
                i.budget_at = at;
                Some(b)
            }
            _ => None,
        }
    })
    .flatten();
    if let Some(b) = newer {
        super::github::record_budget(installation_id, b);
    }
    if limited > now {
        super::github::mark_limited(installation_id, limited);
    }
}

/// What one installation's callers did since the last write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unwritten {
    pub installation_id: u64,
    pub hour: u64,
    pub counts: Vec<(&'static str, Counts)>,
    pub budget: Option<Budget>,
    pub budget_at: u64,
    pub limited_until: u64,
}

/// Takes everything not yet written, by installation and hour.
pub fn drain() -> Vec<Unwritten> {
    drain_where(None)
}

/// [`drain`] for one installation, leaving the others' (tests run side by
/// side in one process).
#[cfg(test)]
pub fn drain_of(installation_id: u64) -> Vec<Unwritten> {
    drain_where(Some(installation_id))
}

fn drain_where(only: Option<u64>) -> Vec<Unwritten> {
    let Ok(mut all) = LEDGER.lock() else {
        return Vec::new();
    };
    let mut out: Vec<Unwritten> = Vec::new();
    for (id, inst) in all.iter_mut() {
        if only.is_some_and(|o| o != *id) {
            continue;
        }
        let mut by_hour: HashMap<u64, Vec<(&'static str, Counts)>> = HashMap::new();
        for ((hour, caller), c) in inst.pending.drain() {
            by_hour.entry(hour).or_default().push((caller, c));
        }
        for (hour, mut counts) in by_hour {
            counts.sort_by_key(|(c, _)| *c);
            out.push(Unwritten {
                installation_id: *id,
                hour,
                counts,
                budget: super::github::budget_of(*id),
                budget_at: inst.budget_at,
                limited_until: inst.limited_until,
            });
        }
    }
    out.sort_by_key(|u| (u.installation_id, u.hour));
    out
}

/// The `github_budget` log line's caller field: `run=41 sweep:pr_watch=12`.
pub fn callers_field(counts: &[(&'static str, Counts)]) -> String {
    counts
        .iter()
        .filter(|(_, c)| c.sent > 0)
        .map(|(caller, c)| format!("{caller}={}", c.sent))
        .collect::<Vec<_>>()
        .join(" ")
}

fn n(v: u64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn num(item: &HashMap<String, AttributeValue>, k: &str) -> u64 {
    item.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Writes what the handled message spent: the hour's counts by caller
/// (added to what other workers wrote), the budget GitHub last reported,
/// and one log line per installation. Best effort: a failed write keeps
/// nothing back, the log line still says it.
pub async fn flush(state: &WorkerState) {
    let mut logged: HashSet<u64> = HashSet::new();
    let unwritten = drain();
    learn_step_costs(state, &step_costs(&unwritten)).await;
    for u in unwritten {
        let sum = |f: fn(&Counts) -> u64| u.counts.iter().map(|(_, c)| f(c)).sum::<u64>();
        // Where the requests went, once per installation and message.
        let routes = if logged.insert(u.installation_id) {
            routes_field(&drain_routes(u.installation_id))
        } else {
            String::new()
        };
        info!(
            installation = u.installation_id,
            callers = %callers_field(&u.counts),
            routes = %routes,
            sent = sum(|c| c.sent),
            not_modified = sum(|c| c.not_modified),
            cached = sum(|c| c.cached),
            limited = sum(|c| c.limited),
            held = sum(|c| c.held),
            remaining = u.budget.map(|b| b.remaining),
            limit = u.budget.map(|b| b.limit),
            reset = u.budget.map(|b| b.reset),
            "github_budget"
        );
        let table = &state.config.analytics_table_name;
        if table.is_empty() {
            continue;
        }
        let pk = rules::partition(u.installation_id);
        let mut adds: Vec<(String, u64)> = u
            .counts
            .iter()
            .map(|(caller, c)| (rules::count_attr(caller), c.sent))
            .collect();
        adds.push(("not_modified".into(), sum(|c| c.not_modified)));
        adds.push(("cached".into(), sum(|c| c.cached)));
        adds.push(("limited".into(), sum(|c| c.limited)));
        adds.push(("held".into(), sum(|c| c.held)));
        adds.retain(|(_, v)| *v > 0);
        let mut req = state
            .dynamo
            .update_item()
            .table_name(table)
            .key("team_id", s(&pk))
            .key("period", s(&rules::counts_key(u.hour * 3600)))
            .expression_attribute_values(":exp", n(u.hour * 3600 + 3600 + rules::COUNTS_TTL_SECS));
        for (i, (name, value)) in adds.iter().enumerate() {
            req = req
                .expression_attribute_names(format!("#c{i}"), name)
                .expression_attribute_values(format!(":c{i}"), n(*value));
        }
        let adds: Vec<String> = (0..adds.len()).map(|i| format!("#c{i} :c{i}")).collect();
        let expr = if adds.is_empty() {
            "SET expires_at = :exp".to_string()
        } else {
            format!("SET expires_at = :exp ADD {}", adds.join(", "))
        };
        if let Err(e) = req.update_expression(expr).send().await {
            warn!(installation = u.installation_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not record the GitHub request counts");
        }
        // The budget as this worker last saw it, for the others: only a
        // newer reading replaces the one there.
        let Some(b) = u.budget.filter(|_| u.budget_at > 0) else {
            continue;
        };
        let res = state
            .dynamo
            .update_item()
            .table_name(table)
            .key("team_id", s(&pk))
            .key("period", s(rules::STATE_KEY))
            .update_expression(
                "SET remaining = :r, #l = :l, reset_at = :rs, seen_at = :at, limited_until = :lu",
            )
            .condition_expression("attribute_not_exists(seen_at) OR seen_at <= :at")
            .expression_attribute_names("#l", "limit")
            .expression_attribute_values(":r", n(b.remaining))
            .expression_attribute_values(":l", n(b.limit))
            .expression_attribute_values(":rs", n(b.reset))
            .expression_attribute_values(":at", n(u.budget_at))
            .expression_attribute_values(":lu", n(u.limited_until))
            .send()
            .await;
        if let Err(e) = res {
            if !common::run_fail::is_condition_failed(&e) {
                warn!(installation = u.installation_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not record the GitHub budget");
            }
        }
    }
}

/// The callers whose steps are admitted (`passes::github_admission`), and
/// so whose cost per step is learned.
pub const ADMITTED: &[&str] = &[rules::caller::RUN, rules::caller::CI];

/// What one handled message's admitted steps spent, by installation and
/// caller: counted requests, summed over the hours the message crossed.
pub fn step_costs(unwritten: &[Unwritten]) -> Vec<(u64, &'static str, u64)> {
    let mut by: HashMap<(u64, &'static str), u64> = HashMap::new();
    for u in unwritten {
        for (caller, c) in &u.counts {
            if ADMITTED.contains(caller) && c.sent > 0 {
                *by.entry((u.installation_id, caller)).or_default() += c.sent;
            }
        }
    }
    let mut out: Vec<(u64, &'static str, u64)> =
        by.into_iter().map(|((i, c), n)| (i, c, n)).collect();
    out.sort_unstable();
    out
}

/// Adds each step's spend to what admission learns a step costs: one step
/// and its counted requests, per installation and caller.
async fn learn_step_costs(state: &WorkerState, costs: &[(u64, &'static str, u64)]) {
    let table = &state.config.analytics_table_name;
    if table.is_empty() {
        return;
    }
    for (installation, caller, sent) in costs {
        let (sum, steps) = crate::passes::github_admission::cost_attrs(caller);
        let res = state
            .dynamo
            .update_item()
            .table_name(table)
            .key("team_id", s(&rules::partition(*installation)))
            .key("period", s(crate::passes::github_admission::PERIOD))
            .update_expression("ADD #s :s, #n :one")
            .expression_attribute_names("#s", sum)
            .expression_attribute_names("#n", steps)
            .expression_attribute_values(":s", n(*sent))
            .expression_attribute_values(":one", n(1))
            .send()
            .await;
        if let Err(e) = res {
            warn!(installation, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not record what a step spent");
        }
    }
}

/// Seconds between two reads of an installation's record by one worker.
pub const REFRESH_SECS: u64 = 60;

/// Reads what the other workers recorded for the installation (this hour's
/// counts, the last budget, a spent limit), at most once a minute.
pub async fn refresh(state: &WorkerState, installation_id: u64) {
    let table = &state.config.analytics_table_name;
    if installation_id == 0 || table.is_empty() {
        return;
    }
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let due = with(installation_id, now, |i| {
        now.saturating_sub(i.refreshed_at) >= REFRESH_SECS
    })
    .unwrap_or(false);
    if !due {
        return;
    }
    let pk = rules::partition(installation_id);
    let get = |period: String| {
        state
            .dynamo
            .get_item()
            .table_name(table)
            .key("team_id", s(&pk))
            .key("period", s(&period))
            .send()
    };
    let (hour, standing) = tokio::join!(
        get(rules::counts_key(now)),
        get(rules::STATE_KEY.to_string())
    );
    let hour = hour.ok().and_then(|o| o.item).unwrap_or_default();
    let standing = standing.ok().and_then(|o| o.item).unwrap_or_default();
    let counts = counts_of(&hour);
    let limit = num(&standing, "limit");
    let budget = (limit > 0).then(|| {
        (
            Budget {
                remaining: num(&standing, "remaining"),
                limit,
                reset: num(&standing, "reset_at"),
            },
            num(&standing, "seen_at"),
        )
    });
    adopt(
        installation_id,
        now / 3600,
        &counts,
        budget,
        num(&standing, "limited_until"),
        now,
    );
}

/// The hour's counts by caller, from its record.
pub fn counts_of(item: &HashMap<String, AttributeValue>) -> HashMap<String, u64> {
    rules::capped()
        .map(|(c, _)| c)
        .map(|c| (c.to_string(), num(item, &rules::count_attr(c))))
        .filter(|(_, v)| *v > 0)
        .collect()
}

/// Skips already said, by installation, caller and hour.
type Told = HashSet<(u64, &'static str, u64)>;
static TOLD: LazyLock<Mutex<Told>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Whether the current task may not spend the installation's budget now
/// (always false for work a person waits on, unless the limit is spent).
/// A loop asks before each item and stops; the skip is logged once per
/// installation and hour, not per item.
pub fn held(installation_id: u64) -> bool {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let who = caller();
    let Spend::Hold { why, wait_secs } = standing(installation_id, who, now) else {
        return false;
    };
    let first = TOLD
        .lock()
        .map(|mut t| t.insert((installation_id, who, now / 3600)))
        .unwrap_or(false);
    if first {
        info!(
            installation = installation_id,
            caller = who,
            wait_secs,
            "{}",
            rules::skip_line(who, why, wait_secs)
        );
    }
    true
}

/// One sweep's check of an installation's budget: asked before the sweep
/// starts on an installation and before each batch, it says whether the
/// sweep may go on, and logs a skip once per installation, not per item.
pub struct SweepGate {
    caller: &'static str,
    skipped: HashSet<u64>,
    /// The shortest wait a skip named, in seconds.
    soonest: Option<u64>,
}

impl SweepGate {
    pub fn new(caller: &'static str) -> Self {
        Self {
            caller,
            skipped: HashSet::new(),
            soonest: None,
        }
    }

    /// Seconds until an installation this sweep skipped may spend again
    /// (the shortest wait), `None` when it skipped none. The skip line says
    /// "it looks again in about N min": the sweep's caller makes that true
    /// (`sweep_follow_up`).
    pub fn retry_in(&self) -> Option<u64> {
        self.soonest
    }

    /// Installations this sweep skipped.
    pub fn skipped(&self) -> usize {
        self.skipped.len()
    }

    /// Whether the sweep may make `need` more counted requests for the
    /// installation at `now`. The first refusal is logged; the installation
    /// stays skipped for the rest of this sweep.
    pub fn check(&mut self, installation_id: u64, need: u64, now: u64) -> bool {
        if self.skipped.contains(&installation_id) {
            return false;
        }
        let hold = match standing(installation_id, self.caller, now) {
            Spend::Hold { why, wait_secs } => Some((why, wait_secs)),
            Spend::Go if allowance(installation_id, self.caller, now) < need.max(1) => {
                Some((rules::Hold::ReservedForRuns, 60))
            }
            Spend::Go => None,
        };
        let Some((why, wait_secs)) = hold else {
            return true;
        };
        self.skipped.insert(installation_id);
        self.soonest = Some(self.soonest.map_or(wait_secs, |s| s.min(wait_secs)));
        info!(
            installation = installation_id,
            caller = self.caller,
            wait_secs,
            "{}",
            rules::skip_line(self.caller, why, wait_secs)
        );
        false
    }

    /// [`Self::check`] now, after reading what the other workers recorded.
    pub async fn allows(&mut self, state: &WorkerState, installation_id: u64, need: u64) -> bool {
        if self.skipped.contains(&installation_id) {
            return false;
        }
        refresh(state, installation_id).await;
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        self.check(installation_id, need, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::github_budget::{caller as who, Hold};

    const NOW: u64 = 1_794_000_000;

    fn budget(installation_id: u64, remaining: u64) {
        crate::clients::github::record_budget(
            installation_id,
            Budget {
                remaining,
                limit: 5_000,
                reset: NOW + 1_800,
            },
        );
    }

    #[tokio::test]
    async fn every_request_is_counted_under_who_made_it() {
        let id = 910_001;
        budget(id, 4_000);
        assert_eq!(caller(), who::OTHER, "a task nobody labelled");
        scoped(who::RUN, async {
            assert_eq!(caller(), who::RUN);
            for _ in 0..3 {
                assert_eq!(admit(id, NOW), Spend::Go);
                responded(id, 200, false, NOW);
            }
            // A conditional request that came back unchanged isn't spent.
            responded(id, 304, false, NOW);
        })
        .await;
        scoped(who::SWEEP_PR_WATCH, async {
            // A nested label wins for what runs inside it.
            scoped(who::DESCRIPTION, async { responded(id, 200, false, NOW) }).await;
            responded(id, 200, false, NOW);
            responded(id, 403, true, NOW);
        })
        .await;
        let mine: Vec<Unwritten> = drain_of(id);
        assert_eq!(mine.len(), 1);
        let counts: HashMap<&str, Counts> = mine[0].counts.iter().cloned().collect();
        assert_eq!(
            counts[who::RUN],
            Counts {
                sent: 3,
                not_modified: 1,
                ..Default::default()
            }
        );
        assert_eq!(counts[who::DESCRIPTION].sent, 1);
        assert_eq!(
            counts[who::SWEEP_PR_WATCH],
            Counts {
                sent: 2,
                limited: 1,
                ..Default::default()
            }
        );
        assert_eq!(mine[0].hour, NOW / 3600);
        assert_eq!(mine[0].budget.unwrap().remaining, 4_000);
        assert_eq!(
            callers_field(&mine[0].counts),
            "description=1 run=3 sweep:pr_watch=2"
        );
        // Written once: nothing is left to write.
        assert!(drain_of(id).is_empty());
        drain_routes(id);
        // ...and the hour's spend is still known for the cap.
        assert_eq!(
            allowance(id, who::SWEEP_PR_WATCH, NOW),
            common::github_budget::hourly_cap(who::SWEEP_PR_WATCH).unwrap() - 2
        );
    }

    #[test]
    fn requests_count_together_by_what_they_ask() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        for (method, url, route) in [
            (
                "GET",
                "https://api.github.com/repos/acme/site/pulls/1573",
                "GET /repos/:o/:r/pulls/:n",
            ),
            (
                "GET",
                "https://api.github.com/repos/acme/site/pulls/12/files?per_page=100&page=2",
                "GET /repos/:o/:r/pulls/:n/files",
            ),
            (
                "GET",
                &format!("https://api.github.com/repos/acme/site/commits/{sha}/check-runs"),
                "GET /repos/:o/:r/commits/:sha/check-runs",
            ),
            (
                "GET",
                "https://api.github.com/repos/acme/site/git/ref/heads/coderhelm/abc-1",
                "GET /repos/:o/:r/git/ref/:name",
            ),
            (
                "PUT",
                "https://api.github.com/repos/acme/site/contents/apps/web/a.ts",
                "PUT /repos/:o/:r/contents/:name",
            ),
            (
                "GET",
                "https://api.github.com/repos/acme/site/compare/main...feature",
                "GET /repos/:o/:r/compare/:name",
            ),
            ("POST", "https://api.github.com/graphql", "POST /graphql"),
            (
                "GET",
                "http://127.0.0.1:4100/repos/o/r/issues/7/comments",
                "GET /repos/:o/:r/issues/:n/comments",
            ),
        ] {
            assert_eq!(route_of(method, url), route, "{url}");
        }
    }

    #[tokio::test]
    async fn the_log_line_says_where_the_requests_went() {
        let id = 910_007;
        scoped(who::RUN, async {
            for _ in 0..3 {
                route_sent(id, "GET /repos/:o/:r/pulls/:n".into(), NOW);
            }
            route_sent(id, "GET /repos/:o/:r/git/ref/:name".into(), NOW);
            served_kept(id, NOW);
        })
        .await;
        scoped(who::CI, async {
            route_sent(id, "GET /repos/:o/:r/commits/:sha/check-runs".into(), NOW)
        })
        .await;
        let routes = drain_routes(id);
        assert_eq!(
            routes_field(&routes),
            "run GET /repos/:o/:r/pulls/:n=3; ci GET /repos/:o/:r/commits/:sha/check-runs=1; \
             run GET /repos/:o/:r/git/ref/:name=1"
        );
        assert!(drain_routes(id).is_empty(), "logged once");
        let u = drain_of(id).pop().unwrap();
        assert_eq!(
            u.counts,
            vec![(
                who::RUN,
                Counts {
                    cached: 1,
                    ..Default::default()
                }
            )]
        );
    }

    #[tokio::test]
    async fn a_sweep_is_held_at_the_reserve_and_a_run_is_not() {
        let id = 910_002;
        budget(id, 2_400);
        let held = scoped(who::SWEEP_SECURITY, async { admit(id, NOW) }).await;
        assert_eq!(
            held,
            Spend::Hold {
                why: Hold::ReservedForRuns,
                wait_secs: 1_800
            }
        );
        assert_eq!(
            scoped(who::FIX_ROUND, async { admit(id, NOW) }).await,
            Spend::Go
        );
        assert_eq!(
            scoped(who::REVIEW, async { admit(id, NOW) }).await,
            Spend::Go
        );
        // The held request is counted as held, never as sent.
        let u = drain_of(id).pop().unwrap();
        assert_eq!(
            u.counts,
            vec![(
                who::SWEEP_SECURITY,
                Counts {
                    held: 1,
                    ..Default::default()
                }
            )]
        );
    }

    #[tokio::test]
    async fn a_sweep_stops_at_its_hourly_cap_and_starts_again_next_hour() {
        let id = 910_003;
        budget(id, 4_900);
        let cap = common::github_budget::hourly_cap(who::SWEEP_SKILLS).unwrap();
        scoped(who::SWEEP_SKILLS, async {
            for _ in 0..cap {
                assert_eq!(admit(id, NOW), Spend::Go);
                responded(id, 200, false, NOW);
            }
            assert!(matches!(
                admit(id, NOW),
                Spend::Hold {
                    why: Hold::HourlyCap,
                    ..
                }
            ));
            // Another sweep has its own cap.
            assert_eq!(standing(id, who::SWEEP_PR_WATCH, NOW), Spend::Go);
            // The next hour starts from nothing.
            assert_eq!(admit(id, NOW + 3_600), Spend::Go);
        })
        .await;
        drain_of(id);
    }

    #[tokio::test]
    async fn what_other_workers_spent_and_saw_is_taken_in() {
        let id = 910_004;
        // This worker saw nothing yet.
        assert_eq!(standing(id, who::SWEEP_PR_WATCH, NOW), Spend::Go);
        let cap = common::github_budget::hourly_cap(who::SWEEP_PR_WATCH).unwrap();
        let theirs = HashMap::from([(who::SWEEP_PR_WATCH.to_string(), cap)]);
        let seen = Budget {
            remaining: 3_000,
            limit: 5_000,
            reset: NOW + 900,
        };
        adopt(id, NOW / 3600, &theirs, Some((seen, NOW - 5)), 0, NOW);
        assert!(matches!(
            standing(id, who::SWEEP_PR_WATCH, NOW),
            Spend::Hold {
                why: Hold::HourlyCap,
                ..
            }
        ));
        assert_eq!(crate::clients::github::budget_of(id), Some(seen));
        // An older reading never replaces a newer one.
        budget_seen(id, NOW);
        let stale = Budget {
            remaining: 10,
            ..seen
        };
        adopt(
            id,
            NOW / 3600,
            &HashMap::new(),
            Some((stale, NOW - 60)),
            0,
            NOW,
        );
        assert_eq!(crate::clients::github::budget_of(id), Some(seen));
        // A limit another worker met holds every caller here too.
        adopt(id, NOW / 3600, &HashMap::new(), None, NOW + 731, NOW);
        assert_eq!(
            standing(id, who::FIX_ROUND, NOW),
            Spend::Hold {
                why: Hold::LimitSpent,
                wait_secs: 731
            }
        );
        assert_eq!(limited_until(id, NOW), Some(NOW + 731));
        assert_eq!(limited_until(id, NOW + 800), None);
        // The hour's record reads back by caller.
        let item = HashMap::from([
            ("req_sweep_pr_watch".to_string(), n(12)),
            ("req_run".to_string(), n(40)),
            ("held".to_string(), n(3)),
        ]);
        assert_eq!(
            counts_of(&item),
            HashMap::from([(who::SWEEP_PR_WATCH.to_string(), 12)])
        );
    }

    #[tokio::test]
    async fn a_sweep_that_skips_says_so_once_and_stays_skipped() {
        let (low, fine) = (910_005, 910_006);
        budget(low, 2_000);
        budget(fine, 4_000);
        let mut gate = SweepGate::new(who::SWEEP_REVIEW_REMINDER);
        for _ in 0..5 {
            assert!(!gate.check(low, 1, NOW));
            assert!(gate.check(fine, 1, NOW));
        }
        assert_eq!(gate.skipped(), 1, "one skip, however many items asked");
        // A batch that doesn't fit before the reserve isn't started.
        budget(fine, 2_503);
        assert!(gate.check(fine, 3, NOW));
        assert!(!gate.check(fine, 4, NOW));
        assert_eq!(gate.skipped(), 2);
    }
}
