//! Security fixes: releasing a finding from a pull request or a fix run
//! that is over (`common::security_release` has the rules).
//!
//! `reconcile` reads what is true (GitHub's state of each pull request a
//! finding is in, the run record behind each `fixing` finding) and applies
//! the rules. It is called three ways, all through the same code:
//!
//! - a `pr` event: the gateway saw a security pull request close or reopen;
//! - a `run` event: a person cancelled or closed a fix run;
//! - the scheduled sweep, before anything else it does for the team and on
//!   a clock of its own, so a webhook that never arrived (or a record that
//!   went stale before these paths existed) is put right within one sweep,
//!   whatever the rest of the sweep has to do.
//!
//! Dependency findings and infrastructure findings are both a
//! `common::security_release::Claim`, so there is one `reconcile` for the
//! two stores: the same reads of GitHub and the run records (`Reads`), the
//! same bounds, the same events. Each store brings only its own storage
//! (`ReleaseIo::save`) and what is its own on the way (`ReleaseIo::merged`,
//! `ReleaseIo::opened`).
//!
//! It is bounded: at most `MAX_PRS_PER_SWEEP` pull requests and
//! `MAX_TICKETS_PER_SWEEP` runs per team per sweep, the rest in turn on the
//! following sweeps. It is idempotent: a fact applied twice writes nothing
//! the second time.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use chrono::{DateTime, Utc};
use common::security_fixes::{Finding, FindingState, Stage};
use common::security_release::{self as rel, Claim, Closer, OpenPr, PrNow, RunNow, Standing};
use tracing::{info, warn};

use super::{add_event, attr_s, post_card, s, save_finding, team_channels, ticket_branch, Ctx};
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// Seconds one team's reconciliation may take in a sweep.
pub(crate) const TEAM_BUDGET_SECS: u64 = 45;

/// Which findings a call looks at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Scope {
    /// The sweep: every pull request and run a finding waits on, in turn.
    All,
    /// A pull request closed or reopened.
    Pr { repo: String, number: u64 },
    /// A fix run ended.
    Ticket(String),
    /// These findings (a fix unit's).
    Ids(Vec<String>),
}

impl Scope {
    fn has<C: Claim>(&self, c: &C) -> bool {
        match self {
            Scope::All => true,
            Scope::Pr { repo, number } => {
                c.repo().eq_ignore_ascii_case(repo) && c.pr().is_some_and(|p| p.0 == *number)
            }
            Scope::Ticket(t) => c.ticket() == Some(t.as_str()),
            Scope::Ids(ids) => ids.iter().any(|id| id == c.id()),
        }
    }
}

/// What one reconciliation did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Report {
    /// Pull requests and runs read.
    pub prs_read: usize,
    pub runs_read: usize,
    /// Findings given back to open (their pull request closed unmerged, or
    /// their run ended without one).
    pub released: usize,
    /// Findings whose pull request merged.
    pub fixed: usize,
    /// Findings put back in a pull request (reopened, or opened by a run
    /// that never told them).
    pub attached: usize,
    /// Pull requests and runs left for the next sweep (its turn or its
    /// time ran out).
    pub left: usize,
}

impl Report {
    pub fn changed(&self) -> usize {
        self.released + self.fixed + self.attached
    }
}

/// The edges: GitHub, the run records, a store's findings. A trait so the
/// reconciliation runs in tests on a fake GitHub and fake runs, and so both
/// stores (`C`: a dependency finding, or an infrastructure record) go
/// through the same `reconcile`.
#[async_trait::async_trait]
pub(crate) trait ReleaseIo<C: Claim + Send + Sync>: Send + Sync {
    /// What GitHub says about the pull request. `closer`: whether who
    /// closed it is needed (it is not for one already recorded as closed,
    /// read only to see whether a person reopened it).
    async fn pr_now(&self, repo: &str, number: u64, closer: bool) -> PrNow;
    /// The open pull request on the ticket's branch; `Err` when GitHub
    /// could not be read.
    async fn open_pr(&self, repo: &str, ticket: &str) -> Result<Option<OpenPr>, ()>;
    /// What became of the ticket's run started at or after `since`.
    async fn run_now(&self, ticket: &str, since: Option<DateTime<Utc>>) -> RunNow;
    async fn save(&self, c: &C);
    async fn event(&self, finding_id: &str, text: &str, link: Option<&str>);
    /// A pull request merged and fixed its finding: its card, once.
    async fn merged(&self, _c: &C) {}
    /// A pull request the run opened and the store never recorded, for a
    /// store that records one only through its own opening path
    /// (`Claim::opens_through_store`): take it through that path and leave
    /// `c` as it now stands. True when it was taken.
    async fn opened(&self, _c: &mut C, _pr: &OpenPr) -> bool {
        false
    }
    fn out_of_time(&self) -> bool;
}

/// Brings the findings in `scope` in line with GitHub and the run records.
pub(crate) async fn reconcile<C: Claim + Send + Sync>(
    io: &dyn ReleaseIo<C>,
    findings: &mut [C],
    scope: &Scope,
    now: DateTime<Utc>,
) -> Report {
    let mut report = Report::default();
    let sweep = *scope == Scope::All;
    let count = |report: &mut Report, was: Standing, is: Standing| match (was, is) {
        (w, Standing::Fixed) if w != Standing::Fixed => report.fixed += 1,
        (Standing::InPr | Standing::Fixing, Standing::Open) => report.released += 1,
        (Standing::Open | Standing::Fixing, Standing::InPr) => report.attached += 1,
        _ => {}
    };

    // Pull requests: one read each, whatever the number of findings in it.
    let mut prs: BTreeMap<(String, u64), (String, bool)> = BTreeMap::new();
    for c in findings.iter() {
        let Some((number, state)) = c.pr() else {
            continue;
        };
        let watched = if sweep {
            c.watches_pr(now)
        } else {
            scope.has(c) && state != "merged"
        };
        if watched {
            let e = prs
                .entry((c.repo().to_ascii_lowercase(), number))
                .or_insert_with(|| (c.repo().to_string(), false));
            // Not yet recorded as closed by some finding: a close needs
            // its closer.
            e.1 |= state != "closed";
        }
    }
    let all: Vec<((String, u64), (String, bool))> = prs.into_iter().collect();
    let mine = if sweep {
        rel::turn(&all, rel::MAX_PRS_PER_SWEEP, now)
    } else {
        all.clone()
    };
    report.left += all.len() - mine.len();
    for (i, (key, (repo, open))) in mine.iter().enumerate() {
        if io.out_of_time() {
            report.left += mine.len() - i;
            break;
        }
        let fact = io.pr_now(repo, key.1, *open).await;
        report.prs_read += 1;
        let mut carded = false;
        for c in findings.iter_mut() {
            let same =
                c.repo().to_ascii_lowercase() == key.0 && c.pr().is_some_and(|p| p.0 == key.1);
            if !same || !scope.has(c) {
                continue;
            }
            let was = c.standing();
            let Some(change) = c.settle_pr(&fact, now) else {
                continue;
            };
            io.save(c).await;
            io.event(c.id(), &change.text, change.link.as_deref()).await;
            count(&mut report, was, c.standing());
            if was != Standing::Fixed && c.standing() == Standing::Fixed && !carded {
                io.merged(c).await;
                carded = true;
            }
        }
    }

    // Fix runs: one read per ticket.
    let mut tickets: BTreeSet<(String, String)> = BTreeSet::new();
    for c in findings.iter() {
        if c.standing() == Standing::Fixing && scope.has(c) {
            tickets.insert((
                c.ticket().unwrap_or_default().to_string(),
                c.repo().to_string(),
            ));
        }
    }
    let all: Vec<(String, String)> = tickets.into_iter().collect();
    let mine = if sweep {
        rel::turn(&all, rel::MAX_TICKETS_PER_SWEEP, now)
    } else {
        all.clone()
    };
    report.left += all.len() - mine.len();
    for (i, (ticket, repo)) in mine.iter().enumerate() {
        if io.out_of_time() {
            report.left += mine.len() - i;
            break;
        }
        let of_ticket = |c: &C| {
            c.standing() == Standing::Fixing
                && c.ticket().unwrap_or_default() == ticket.as_str()
                && c.repo() == repo.as_str()
        };
        // A finding claimed with no ticket has no run to wait on.
        let (open, run) = if ticket.is_empty() {
            (None, RunNow::Missing)
        } else {
            let Ok(open) = io.open_pr(repo, ticket).await else {
                report.left += 1;
                continue;
            };
            // An open pull request is the whole answer, unless a round is
            // still working on it and its end decides.
            let decides = findings
                .iter()
                .any(|c| of_ticket(c) && c.run_decides_with_pr());
            let run = if open.is_some() && !decides {
                RunNow::Working
            } else {
                let since = findings
                    .iter()
                    .filter(|c| of_ticket(c))
                    .filter_map(|c| c.claimed_at())
                    .min();
                io.run_now(ticket, since).await
            };
            (open, run)
        };
        report.runs_read += 1;
        for c in findings.iter_mut() {
            if !of_ticket(c) || !scope.has(c) {
                continue;
            }
            let was = c.standing();
            if let Some(pr) = &open {
                let recorded = c.pr().is_some_and(|p| p.0 == pr.number && p.1 == "open");
                if c.opens_through_store() && !recorded {
                    if io.opened(c, pr).await {
                        count(&mut report, was, c.standing());
                    }
                    continue;
                }
            }
            let Some(change) = c.settle_run(&run, open.as_ref(), now) else {
                continue;
            };
            io.save(c).await;
            io.event(c.id(), &change.text, change.link.as_deref()).await;
            count(&mut report, was, c.standing());
        }
    }
    report
}

// ── The real edges ──────────────────────────────────────────────────────

/// What both stores read to know the truth: GitHub's pull requests and
/// the run records, for one team.
pub(crate) struct Reads<'a> {
    pub state: &'a WorkerState,
    pub team_id: &'a str,
    pub gh: &'a GitHubClient,
}

impl Reads<'_> {
    /// The run that opened a pull request, when CoderHelm's own account
    /// closed it: a person who closed the run in CoderHelm is the closer.
    async fn closed_through_run(&self, owner: &str, name: &str, number: u64) -> Option<String> {
        let state = self.state;
        let run_id =
            crate::passes::review_pr::lookup_run_by_pr(state, self.team_id, owner, name, number)
                .await?;
        let item = state
            .dynamo
            .get_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(self.team_id))
            .key("run_id", attr_s(&run_id))
            .projection_expression("#s, closed_by_name, closed_by")
            .expression_attribute_names("#s", "status")
            .send()
            .await
            .ok()?
            .item?;
        if s(&item, "status") != common::run_status::CLOSED {
            return None;
        }
        Some(
            [s(&item, "closed_by_name"), s(&item, "closed_by")]
                .into_iter()
                .find(|v| !v.is_empty())
                .unwrap_or_default(),
        )
    }

    pub async fn pr_now(&self, repo: &str, number: u64, closer: bool) -> PrNow {
        let Some((o, n)) = repo.split_once('/') else {
            return PrNow::Unknown;
        };
        let gh = self.gh;
        let pr = match gh.get_pull_request(o, n, number).await {
            Ok(pr) => pr,
            Err(e) if crate::clients::github::is_not_found(e.as_ref()) => return PrNow::Gone,
            Err(e) => {
                warn!(team_id = self.team_id, repo, number, error = %e, "security: a pull request's state couldn't be read; left as it is");
                return PrNow::Unknown;
            }
        };
        closed_fact(&pr, || async {
            if !closer {
                return (String::new(), true);
            }
            // The pull request doesn't carry who closed it; its issue does.
            let login = gh.get_pr_closed_by(o, n, number).await;
            let through_run = match &login {
                Some(l) if crate::passes::is_own_login(l) => {
                    self.closed_through_run(o, n, number).await
                }
                _ => None,
            };
            closer_of(login, through_run)
        })
        .await
    }

    pub async fn open_pr(&self, repo: &str, ticket: &str) -> Result<Option<OpenPr>, ()> {
        let Some((o, n)) = repo.split_once('/') else {
            return Ok(None);
        };
        let Some(branch) = ticket_branch(self.state, self.team_id, ticket).await else {
            return Ok(None);
        };
        match self.gh.find_open_pr_for_branch(o, n, &branch).await {
            Ok(pr) => Ok(pr.map(|pr| OpenPr {
                number: pr["number"].as_u64().unwrap_or(0),
                url: pr["html_url"].as_str().unwrap_or("").to_string(),
            })),
            Err(e) => {
                warn!(team_id = self.team_id, repo, ticket, error = %e, "security: the run's branch couldn't be read; left as it is");
                Err(())
            }
        }
    }

    pub async fn run_now(&self, ticket: &str, since: Option<DateTime<Utc>>) -> RunNow {
        let state = self.state;
        let mut latest: Option<(String, super::Item)> = None;
        let mut start = None;
        loop {
            let out = state
                .dynamo
                .query()
                .table_name(&state.config.runs_table_name)
                .index_name("ticket-index")
                .key_condition_expression("team_id = :tid AND ticket_id = :ticket")
                .expression_attribute_values(":tid", attr_s(self.team_id))
                .expression_attribute_values(":ticket", attr_s(ticket))
                .projection_expression(
                    "created_at, updated_at, #s, closed_by_name, cancelled_by, closed_by",
                )
                .expression_attribute_names("#s", "status")
                .set_exclusive_start_key(start.take())
                .send()
                .await;
            let Ok(out) = out else {
                warn!(
                    team_id = self.team_id,
                    ticket, "security: the ticket's runs couldn't be read; left as it is"
                );
                return RunNow::Unknown;
            };
            for item in out.items() {
                let created = s(item, "created_at");
                if latest.as_ref().is_none_or(|(c, _)| created > *c) {
                    latest = Some((created, item.clone()));
                }
            }
            match out.last_evaluated_key() {
                Some(k) if !k.is_empty() => start = Some(k.clone()),
                _ => break,
            }
        }
        let Some((created, item)) = latest else {
            return RunNow::Missing;
        };
        run_fact(
            &created,
            &s(&item, "status"),
            &s(&item, "updated_at"),
            [
                s(&item, "closed_by_name"),
                s(&item, "cancelled_by"),
                s(&item, "closed_by"),
            ]
            .into_iter()
            .find(|v| !v.is_empty()),
            since,
            Utc::now(),
        )
    }
}

/// Dependency findings' edges: the shared reads, their storage and the
/// team's channels.
pub(super) struct Live<'a, 'c> {
    pub cx: &'a Ctx<'c>,
    pub started: Instant,
    pub budget_secs: u64,
    channels: tokio::sync::OnceCell<common::notify::Channels>,
}

impl<'a, 'c> Live<'a, 'c> {
    pub fn new(cx: &'a Ctx<'c>, budget_secs: u64) -> Self {
        Self {
            cx,
            started: Instant::now(),
            budget_secs,
            channels: tokio::sync::OnceCell::new(),
        }
    }

    pub fn reads(&self) -> Reads<'_> {
        Reads {
            state: self.cx.state,
            team_id: self.cx.team_id,
            gh: &self.cx.gh,
        }
    }
}

#[async_trait::async_trait]
impl ReleaseIo<(Finding, FindingState)> for Live<'_, '_> {
    async fn pr_now(&self, repo: &str, number: u64, closer: bool) -> PrNow {
        self.reads().pr_now(repo, number, closer).await
    }

    async fn open_pr(&self, repo: &str, ticket: &str) -> Result<Option<OpenPr>, ()> {
        self.reads().open_pr(repo, ticket).await
    }

    async fn run_now(&self, ticket: &str, since: Option<DateTime<Utc>>) -> RunNow {
        self.reads().run_now(ticket, since).await
    }

    async fn save(&self, c: &(Finding, FindingState)) {
        save_finding(self.cx.state, self.cx.team_id, &c.0, &c.1).await;
    }

    async fn event(&self, finding_id: &str, text: &str, link: Option<&str>) {
        add_event(self.cx.state, self.cx.team_id, finding_id, text, link).await;
    }

    async fn merged(&self, c: &(Finding, FindingState)) {
        let (f, st) = c;
        if !self.cx.cfg.acts() || !st.cards.iter().any(|c| c == "new") {
            return;
        }
        let Some(p) = &f.pr else { return };
        let channels = self
            .channels
            .get_or_init(|| team_channels(self.cx.state, self.cx.team_id))
            .await;
        let thing = format!("PR#{}#{}", f.repo.to_ascii_lowercase(), p.number);
        post_card(
            self.cx.state,
            self.cx.team_id,
            channels,
            &thing,
            Stage::Merged,
            f,
        )
        .await;
    }

    fn out_of_time(&self) -> bool {
        // Out of time, or of the sweep's share of GitHub's budget: what is
        // left is read at a later sweep (`Report::left`).
        self.started.elapsed().as_secs() >= self.budget_secs || self.cx.gh.held()
    }
}

/// Who closed a pull request, and whether that was a person: `login` is
/// GitHub's closer; `through_run` the person who closed the pull request's
/// run in CoderHelm, when CoderHelm's own account did the closing for
/// them. A person on GitHub (which is also who deletes a branch and so
/// closes its pull request) and a person through CoderHelm are both a
/// person; CoderHelm's account with no such run closed it for a reason of
/// its own. An unknown closer is taken for a person, so that no automatic
/// fix opens against what may have been a person's decision.
pub(super) fn closer_of(login: Option<String>, through_run: Option<String>) -> (String, bool) {
    match (login, through_run) {
        (Some(l), Some(name)) if crate::passes::is_own_login(&l) => {
            (if name.is_empty() { l } else { name }, true)
        }
        (Some(l), _) if crate::passes::is_own_login(&l) => (l, false),
        (Some(l), _) => (l, true),
        (None, _) => (String::new(), true),
    }
}

/// GitHub's pull request as a fact. `closer` is asked only for one closed
/// unmerged: (who, whether a person).
pub(super) async fn closed_fact<F, Fut>(pr: &serde_json::Value, closer: F) -> PrNow
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = (String, bool)>,
{
    if pr["merged"].as_bool() == Some(true) || pr["merged_at"].is_string() {
        return PrNow::Merged;
    }
    match pr["state"].as_str() {
        Some("closed") => {
            let (by, person) = closer().await;
            PrNow::Closed(Closer {
                by,
                person,
                at: pr["closed_at"].as_str().map(str::to_string),
            })
        }
        Some("open") => PrNow::Open,
        // An answer with no state is not an answer.
        _ => PrNow::Unknown,
    }
}

/// The ticket's latest run as a fact for a finding claimed at `since`: a
/// run that began before the claim is an earlier attempt, not this one.
pub(super) fn run_fact(
    created_at: &str,
    status: &str,
    updated_at: &str,
    by: Option<String>,
    since: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> RunNow {
    let began = common::security_fixes::parse_time(created_at);
    // The claim is written just before the run's message is sent; a clock a
    // few minutes apart must not hide the run.
    let earlier = match (began, since) {
        (Some(b), Some(s)) => b < s - chrono::Duration::minutes(rel::RUN_CLOCK_TOLERANCE_MINS),
        _ => false,
    };
    if earlier {
        return RunNow::Missing;
    }
    rel::run_now_of(status, updated_at, by, now)
}

/// The sweep's and the events' entry: reconcile and say what was done.
pub(super) async fn run(
    cx: &Ctx<'_>,
    findings: &mut [(Finding, FindingState)],
    scope: &Scope,
    budget_secs: u64,
) -> Report {
    let io = Live::new(cx, budget_secs);
    let report = reconcile(&io, findings, scope, Utc::now()).await;
    said(cx.team_id, "dependency", scope, &report);
    report
}

/// The line a reconciliation leaves in the log when it did something or
/// left something for later.
pub(crate) fn said(team_id: &str, store: &str, scope: &Scope, report: &Report) {
    if report.changed() > 0 || report.left > 0 {
        info!(
            team_id,
            store,
            scope = ?scope,
            prs_read = report.prs_read,
            runs_read = report.runs_read,
            released = report.released,
            fixed = report.fixed,
            attached = report.attached,
            left = report.left,
            "security: findings brought in line with their pull requests and runs"
        );
    }
}

#[cfg(test)]
pub(crate) mod fake {
    //! A fake GitHub and fake run records for the reconciliation's tests
    //! (here and in the incident suite).
    use super::*;
    use common::security_fixes::{PrRef, Status};
    use std::sync::Mutex;

    #[derive(Default)]
    pub(crate) struct Fake {
        /// What GitHub says, by (repo lowercase, number).
        pub prs: Mutex<BTreeMap<(String, u64), PrNow>>,
        /// The open pull request on a ticket's branch, by ticket.
        pub branch_prs: Mutex<BTreeMap<String, OpenPr>>,
        /// What became of a ticket's run, by ticket.
        pub runs: Mutex<BTreeMap<String, RunNow>>,
        pub saved: Mutex<Vec<String>>,
        pub events: Mutex<Vec<(String, String)>>,
        pub merged_cards: Mutex<Vec<String>>,
        /// Pull requests handed to a store's own opening path: (id, number).
        pub opened_through_store: Mutex<Vec<(String, u64)>>,
        /// Every pull request read, in order, and those whose closer was
        /// asked for.
        pub pr_reads: Mutex<Vec<(String, u64)>>,
        pub closer_asked: Mutex<Vec<u64>>,
        pub run_reads: Mutex<Vec<String>>,
        /// Reads after which the time is up.
        pub time_for: Option<usize>,
        /// GitHub is down for branch lookups.
        pub branches_down: bool,
    }

    impl Fake {
        pub fn pr(&self, repo: &str, number: u64, now: PrNow) {
            self.prs
                .lock()
                .unwrap()
                .insert((repo.to_ascii_lowercase(), number), now);
        }
        pub fn run(&self, ticket: &str, now: RunNow) {
            self.runs.lock().unwrap().insert(ticket.to_string(), now);
        }
        pub fn saves(&self) -> usize {
            self.saved.lock().unwrap().len()
        }
        pub fn reads(&self) -> usize {
            self.pr_reads.lock().unwrap().len() + self.run_reads.lock().unwrap().len()
        }
        pub fn event_texts(&self) -> Vec<String> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .map(|(_, t)| t.clone())
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl<C: Claim + Send + Sync> ReleaseIo<C> for Fake {
        async fn pr_now(&self, repo: &str, number: u64, closer: bool) -> PrNow {
            let key = (repo.to_ascii_lowercase(), number);
            self.pr_reads.lock().unwrap().push(key.clone());
            if closer {
                self.closer_asked.lock().unwrap().push(number);
            }
            // A pull request the fake was never told about is one GitHub
            // could not be asked about.
            self.prs
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or(PrNow::Unknown)
        }
        async fn open_pr(&self, _repo: &str, ticket: &str) -> Result<Option<OpenPr>, ()> {
            if self.branches_down {
                return Err(());
            }
            Ok(self.branch_prs.lock().unwrap().get(ticket).cloned())
        }
        async fn run_now(&self, ticket: &str, _since: Option<DateTime<Utc>>) -> RunNow {
            self.run_reads.lock().unwrap().push(ticket.to_string());
            self.runs
                .lock()
                .unwrap()
                .get(ticket)
                .cloned()
                .unwrap_or(RunNow::Missing)
        }
        async fn save(&self, c: &C) {
            self.saved.lock().unwrap().push(c.id().to_string());
        }
        async fn event(&self, finding_id: &str, text: &str, _link: Option<&str>) {
            self.events
                .lock()
                .unwrap()
                .push((finding_id.to_string(), text.to_string()));
        }
        async fn merged(&self, c: &C) {
            self.merged_cards.lock().unwrap().push(c.id().to_string());
        }
        async fn opened(&self, c: &mut C, pr: &OpenPr) -> bool {
            self.opened_through_store
                .lock()
                .unwrap()
                .push((c.id().to_string(), pr.number));
            true
        }
        fn out_of_time(&self) -> bool {
            self.time_for.is_some_and(|n| self.reads() >= n)
        }
    }

    pub(crate) fn at(t: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(t).unwrap().with_timezone(&Utc)
    }

    /// A finding in pull request `number` of `repo`, claimed by `ticket`.
    pub(crate) fn in_pr(
        id: &str,
        repo: &str,
        number: u64,
        ticket: &str,
    ) -> (Finding, FindingState) {
        (
            Finding {
                id: id.into(),
                repo: repo.into(),
                ecosystem: "npm".into(),
                package: format!("pkg-{id}"),
                version: "1.0.0".into(),
                fixed_version: Some("1.0.1".into()),
                status: Status::PrOpen,
                pr: Some(PrRef {
                    number,
                    url: format!("https://github.com/{repo}/pull/{number}"),
                    ci: Some("passing".into()),
                    state: "open".into(),
                    ..Default::default()
                }),
                updated_at: "2026-11-12T09:00:00Z".into(),
                ..Default::default()
            },
            FindingState {
                ticket: Some(ticket.into()),
                bundled: true,
                cards: vec!["new".into()],
                ..Default::default()
            },
        )
    }

    /// A finding a run of `ticket` is working on.
    pub(crate) fn fixing(id: &str, repo: &str, ticket: &str) -> (Finding, FindingState) {
        let (mut f, mut st) = in_pr(id, repo, 1, ticket);
        f.status = Status::Fixing;
        f.pr = None;
        st.claimed_at = Some("2026-11-13T09:00:00Z".into());
        (f, st)
    }

    pub(crate) fn closed_by(login: &str) -> PrNow {
        PrNow::Closed(Closer {
            by: login.into(),
            person: true,
            at: Some("2026-11-13T09:30:00Z".into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::fake::*;
    use super::*;
    use common::security_fixes::Status;

    const REPO: &str = "acme/web";
    const TICKET: &str = "SEC-AAAAAAAAAAAA";

    fn now() -> DateTime<Utc> {
        at("2026-11-13T10:00:00Z")
    }

    fn statuses(findings: &[(Finding, FindingState)]) -> Vec<Status> {
        findings.iter().map(|(f, _)| f.status).collect()
    }

    #[tokio::test]
    async fn the_sweep_releases_a_closed_pull_requests_findings_with_one_read() {
        // 274 findings in one bundle pull request a person closed; the
        // webhook never arrived.
        let mut findings: Vec<_> = (0..274)
            .map(|i| in_pr(&format!("f{i}"), REPO, 12, TICKET))
            .collect();
        let io = Fake::default();
        io.pr(REPO, 12, closed_by("octo"));
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!(
            r,
            Report {
                prs_read: 1,
                released: 274,
                ..Default::default()
            }
        );
        assert!(findings.iter().all(|(f, st)| f.status == Status::Open
            && st.ticket.is_none()
            && f.pr.as_ref().is_some_and(|p| p.state == "closed"
                && p.closed_by.as_deref() == Some("octo")
                && p.closed_by_person)));
        assert_eq!(io.saves(), 274);
        assert!(io.event_texts()[0].contains("closed without merging by octo"));

        // The next sweep reads it once more (a person may reopen it while
        // the hold is on) and writes nothing.
        let later = now() + chrono::Duration::minutes(20);
        let r = reconcile(&io, &mut findings, &Scope::All, later).await;
        assert_eq!(r.changed(), 0);
        // ...without asking GitHub again who closed it.
        assert_eq!(io.pr_reads.lock().unwrap().len(), 2);
        assert_eq!(*io.closer_asked.lock().unwrap(), vec![12]);
        assert_eq!(io.saves(), 274);
        assert_eq!(io.events.lock().unwrap().len(), 274);
    }

    #[tokio::test]
    async fn a_pull_request_event_releases_only_that_pull_requests_findings() {
        let mut findings = vec![
            in_pr("a", REPO, 12, TICKET),
            in_pr("b", REPO, 13, "SEC-BBBBBBBBBBBB"),
            in_pr("c", "acme/api", 12, "SEC-CCCCCCCCCCCC"),
        ];
        let io = Fake::default();
        io.pr(REPO, 12, closed_by("octo"));
        io.pr(REPO, 13, closed_by("octo"));
        io.pr("acme/api", 12, closed_by("octo"));
        let scope = Scope::Pr {
            repo: "Acme/Web".into(),
            number: 12,
        };
        let r = reconcile(&io, &mut findings, &scope, now()).await;
        assert_eq!((r.prs_read, r.released), (1, 1));
        assert_eq!(
            statuses(&findings),
            vec![Status::Open, Status::PrOpen, Status::PrOpen]
        );
    }

    #[tokio::test]
    async fn a_merge_fixes_the_findings_and_posts_its_card_once() {
        let mut findings = vec![in_pr("a", REPO, 12, TICKET), in_pr("b", REPO, 12, TICKET)];
        let io = Fake::default();
        io.pr(REPO, 12, PrNow::Merged);
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!((r.fixed, r.released), (2, 0));
        assert_eq!(statuses(&findings), vec![Status::Fixed, Status::Fixed]);
        assert_eq!(io.merged_cards.lock().unwrap().len(), 1);
        // Merged pull requests are not read again.
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!(r, Report::default());
    }

    #[tokio::test]
    async fn github_down_changes_nothing_and_the_next_sweep_heals() {
        let mut findings = vec![in_pr("a", REPO, 12, TICKET)];
        let io = Fake::default();
        // The fake was told nothing: the read fails.
        let before = findings.clone();
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!((r.prs_read, r.changed()), (1, 0));
        assert_eq!(findings, before);
        assert_eq!(io.saves(), 0);
        io.pr(REPO, 12, closed_by("octo"));
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!(r.released, 1);
    }

    #[tokio::test]
    async fn a_reopened_pull_request_takes_its_findings_back() {
        let mut findings = vec![in_pr("a", REPO, 12, TICKET)];
        let io = Fake::default();
        io.pr(REPO, 12, closed_by("octo"));
        reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!(findings[0].0.status, Status::Open);
        io.pr(REPO, 12, PrNow::Open);
        let scope = Scope::Pr {
            repo: REPO.into(),
            number: 12,
        };
        let r = reconcile(&io, &mut findings, &scope, now()).await;
        assert_eq!(r.attached, 1);
        assert_eq!(findings[0].0.status, Status::PrOpen);
        assert!(io.event_texts().last().unwrap().contains("reopened"));
        // And the sweep alone finds a reopen whose webhook was missed.
        io.pr(REPO, 12, closed_by("octo"));
        reconcile(&io, &mut findings, &Scope::All, now()).await;
        io.pr(REPO, 12, PrNow::Open);
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!(r.attached, 1);
    }

    #[tokio::test]
    async fn a_cancelled_run_releases_what_it_claimed() {
        let mut findings = vec![
            fixing("a", REPO, TICKET),
            fixing("b", REPO, TICKET),
            fixing("c", REPO, "SEC-BBBBBBBBBBBB"),
        ];
        let io = Fake::default();
        io.run(
            TICKET,
            RunNow::Ended {
                status: "cancelled".into(),
                by: Some("dev@example.com".into()),
            },
        );
        io.run("SEC-BBBBBBBBBBBB", RunNow::Working);
        let r = reconcile(&io, &mut findings, &Scope::Ticket(TICKET.into()), now()).await;
        assert_eq!((r.runs_read, r.released), (1, 2));
        assert_eq!(
            statuses(&findings),
            vec![Status::Open, Status::Open, Status::Fixing]
        );
        assert!(io.event_texts()[0].contains("cancelled by dev@example.com"));
        // The sweep leaves the working run's finding alone, and a second
        // pass over the released ones writes nothing.
        let saves = io.saves();
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!((r.runs_read, r.changed()), (1, 0));
        assert_eq!(io.saves(), saves);
    }

    #[tokio::test]
    async fn a_run_that_opened_a_pull_request_nobody_recorded_puts_its_findings_in_it() {
        let mut findings = vec![fixing("a", REPO, TICKET)];
        let io = Fake::default();
        io.branch_prs.lock().unwrap().insert(
            TICKET.into(),
            OpenPr {
                number: 31,
                url: "https://github.com/acme/web/pull/31".into(),
            },
        );
        io.run(
            TICKET,
            RunNow::Ended {
                status: "failed".into(),
                by: None,
            },
        );
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!(r.attached, 1);
        assert_eq!(findings[0].0.status, Status::PrOpen);
        // The run was not even asked: the pull request is the fact.
        assert!(io.run_reads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_branch_that_cannot_be_read_never_releases_a_finding() {
        let mut findings = vec![fixing("a", REPO, TICKET)];
        let io = Fake {
            branches_down: true,
            ..Default::default()
        };
        io.run(
            TICKET,
            RunNow::Ended {
                status: "failed".into(),
                by: None,
            },
        );
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!((r.changed(), r.left), (0, 1));
        assert_eq!(findings[0].0.status, Status::Fixing);
    }

    #[tokio::test]
    async fn the_sweep_is_bounded_and_every_pull_request_gets_its_turn() {
        // 250 pull requests, each closed, each with one finding.
        let mut findings: Vec<_> = (0..250u64)
            .map(|i| in_pr(&format!("f{i}"), REPO, 1000 + i, TICKET))
            .collect();
        let io = Fake::default();
        for i in 0..250u64 {
            io.pr(REPO, 1000 + i, closed_by("octo"));
        }
        let mut t = now();
        let r = reconcile(&io, &mut findings, &Scope::All, t).await;
        assert_eq!(
            (r.prs_read, r.released, r.left),
            (rel::MAX_PRS_PER_SWEEP, rel::MAX_PRS_PER_SWEEP, 150)
        );
        // Every pull request is released within a few sweeps, none starved.
        for _ in 0..6 {
            t += chrono::Duration::seconds(rel::SWEEP_SECS);
            reconcile(&io, &mut findings, &Scope::All, t).await;
        }
        assert!(findings.iter().all(|(f, _)| f.status == Status::Open));
    }

    #[tokio::test]
    async fn out_of_time_stops_reading_and_says_what_is_left() {
        let mut findings: Vec<_> = (0..10u64)
            .map(|i| in_pr(&format!("f{i}"), REPO, 100 + i, TICKET))
            .collect();
        let io = Fake {
            time_for: Some(3),
            ..Default::default()
        };
        for i in 0..10u64 {
            io.pr(REPO, 100 + i, closed_by("octo"));
        }
        let r = reconcile(&io, &mut findings, &Scope::All, now()).await;
        assert_eq!((r.prs_read, r.released, r.left), (3, 3, 7));
    }

    #[tokio::test]
    async fn github_facts_are_read_as_what_they_say() {
        let nobody = || async { (String::new(), true) };
        let merged = serde_json::json!({"state": "closed", "merged": true});
        assert_eq!(closed_fact(&merged, nobody).await, PrNow::Merged);
        let open = serde_json::json!({"state": "open", "merged": false});
        assert_eq!(closed_fact(&open, nobody).await, PrNow::Open);
        let closed = serde_json::json!({"state": "closed", "merged": false, "merged_at": null, "closed_at": "2026-11-13T09:30:00Z"});
        assert_eq!(
            closed_fact(&closed, || async { ("octo".to_string(), true) }).await,
            closed_by("octo")
        );
        // An answer with no state (an error body) is never open or closed.
        assert_eq!(
            closed_fact(&serde_json::json!({"message": "Server Error"}), nobody).await,
            PrNow::Unknown
        );
    }

    #[test]
    fn the_closer_is_a_person_unless_coderhelm_closed_it_for_its_own_reason() {
        // A person on GitHub; deleting the branch closes it in their name too.
        assert_eq!(closer_of(Some("octo".into()), None), ("octo".into(), true));
        // CoderHelm's account, because a person closed the run in CoderHelm.
        assert_eq!(
            closer_of(Some("coderhelm[bot]".into()), Some("Dana Admin".into())),
            ("Dana Admin".into(), true)
        );
        // CoderHelm's account for a reason of its own (a withdrawn advisory,
        // a stale pull request).
        assert_eq!(
            closer_of(Some("coderhelm[bot]".into()), None),
            ("coderhelm[bot]".into(), false)
        );
        // GitHub did not say: a person, so nothing reopens it by itself.
        assert_eq!(closer_of(None, None), (String::new(), true));
        // A person's login is never replaced by a run's closer.
        assert_eq!(
            closer_of(Some("octo".into()), Some("Dana Admin".into())),
            ("octo".into(), true)
        );
    }

    #[test]
    fn a_run_that_began_before_the_claim_is_not_the_claims_run() {
        let n = now();
        let since = Some(at("2026-11-13T09:00:00Z"));
        // The retried run of yesterday: this claim's run has not started yet.
        assert_eq!(
            run_fact(
                "2026-11-12T08:00:00Z",
                "retried",
                "2026-11-12T09:00:00Z",
                None,
                since,
                n
            ),
            RunNow::Missing
        );
        assert_eq!(
            run_fact(
                "2026-11-13T09:00:05Z",
                "running",
                "2026-11-13T09:55:00Z",
                None,
                since,
                n
            ),
            RunNow::Working
        );
        assert_eq!(
            run_fact(
                "2026-11-13T09:00:05Z",
                "cancelled",
                "2026-11-13T09:20:00Z",
                Some("dev@example.com".into()),
                since,
                n
            ),
            RunNow::Ended {
                status: "cancelled".into(),
                by: Some("dev@example.com".into())
            }
        );
    }
}
