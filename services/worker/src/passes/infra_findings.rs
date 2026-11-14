//! Infrastructure findings: a team's inventory (`RES#`, from
//! `infra_inventory`) matched against the global feeds (`infra_feeds`),
//! stored as `INFRAFINDING#<id>`, and fixed (`common::infra_vuln::matching`
//! and `records` have the rules).
//!
//! - Once a day per team with security fixes on, and on a person's "check
//!   now": the match runs; a finding the new read no longer produces is
//!   fixed. A merged PR alone never closes one.
//! - PR fixes go through the security fix runs: the same daily run and
//!   token limits (`SECRUNS#<day>`), one run per repository at a time,
//!   the policy's cadence per tier, nothing in a dry run. The run's ticket
//!   names the file and the Terraform address.
//! - A record is `fixing` only while its run works and `pr_open` only
//!   while its pull request is open or merged (`release`): the same
//!   reconciliation, events, bounds and holds as dependency findings
//!   (`security_fixes::release`), on this store's records.
//! - Runbook steps and AWS-managed rows are shown, never run.
//! - New P0 and P1 findings post one card each, inside the day's card
//!   limit shared with dependency findings (`SECCARDS#<day>`).

use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::Utc;
use common::infra_vuln::inventory::{Resource, RESOURCE_PREFIX};
use common::infra_vuln::matching::{self, Feeds, FixKind};
use common::infra_vuln::records::{self, Record, Status};
use common::infra_vuln::store;
use common::security_fixes::{self as sec, Admit, Cadence, FixUnit, SecurityConfig, Tier};
use serde_json::Value;
use tracing::{info, warn};

use crate::models::{SecurityEventMessage, TicketMessage, TicketSource, WorkerMessage};
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;

type Item = HashMap<String, AttributeValue>;

/// `SecurityEventMessage.kind` for a person's "check now".
pub const CHECK_KIND: &str = "infrastructure_check";
/// `SecurityEventMessage.kind` for "open the fix PR now" (`repo` = the id).
pub const FIX_KIND: &str = "infrastructure_fix";

fn s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

async fn query(state: &WorkerState, table: &str, pk: &str, prefix: &str) -> Vec<Item> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..50 {
        let Ok(r) = state
            .dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", attr_s(pk))
            .expression_attribute_values(":p", attr_s(prefix))
            .set_exclusive_start_key(start.take())
            .send()
            .await
        else {
            break;
        };
        out.extend(r.items().iter().cloned());
        match r.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    out
}

pub async fn load_records(state: &WorkerState, team_id: &str) -> Vec<Record> {
    query(
        state,
        &state.config.settings_table_name,
        team_id,
        records::FINDING_PREFIX,
    )
    .await
    .iter()
    .filter_map(|i| serde_json::from_str(&s(i, "json")).ok())
    .collect()
}

async fn save(state: &WorkerState, team_id: &str, r: &Record) {
    let Ok(j) = serde_json::to_string(r) else {
        return;
    };
    let mut req = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&records::finding_sk(&r.finding.id)))
        .item("json", attr_s(&j))
        .item(
            "status",
            attr_s(&format!("{:?}", r.status).to_ascii_lowercase()),
        );
    if r.status == Status::Fixed {
        req = req.item(
            "ttl",
            attr_n(Utc::now().timestamp() + records::FIXED_KEEP_DAYS * 86_400),
        );
    }
    if let Err(e) = req.send().await {
        warn!(team_id, error = %e, "infra findings: a finding could not be saved");
    }
}

/// The team's resources and, per account, AWS's latest AL2023 AMIs there.
async fn inventory(
    state: &WorkerState,
    team_id: &str,
) -> (Vec<Resource>, BTreeMap<String, BTreeMap<String, String>>) {
    let table = &state.config.aws_insights_table_name;
    let res: Vec<Resource> = query(state, table, team_id, RESOURCE_PREFIX)
        .await
        .iter()
        .filter_map(|i| serde_json::from_str(&s(i, "json")).ok())
        .collect();
    let mut latest = BTreeMap::new();
    for i in query(state, table, team_id, super::infra_inventory::READ_PREFIX).await {
        if let Ok(v) = serde_json::from_str::<Value>(&s(&i, "json")) {
            let acct = v["account"].as_str().unwrap_or("").to_string();
            let m: BTreeMap<String, String> =
                serde_json::from_value(v["latest_al2023"].clone()).unwrap_or_default();
            latest.insert(acct, m);
        }
    }
    (res, latest)
}

async fn kev(state: &WorkerState) -> BTreeMap<String, String> {
    let o = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(sec::GLOBAL_PK))
        .key("sk", attr_s(sec::KEV_SK))
        .send()
        .await
        .ok();
    o.and_then(|o| o.item().cloned())
        .and_then(|i| serde_json::from_str(&s(&i, "map")).ok())
        .unwrap_or_default()
}

/// Matches the team's inventory now and saves what changed. Returns the
/// stored records after the match.
pub async fn match_team(state: &WorkerState, team_id: &str) -> Vec<Record> {
    let (resources, latest) = inventory(state, team_id).await;
    let mut stored = load_records(state, team_id).await;
    if resources.is_empty() && stored.is_empty() {
        return stored;
    }
    let table = &state.config.settings_table_name;
    let advisories = store::advisories(&state.dynamo, table).await;
    let bulletins = store::bulletins(&state.dynamo, table).await;
    let runtimes = store::runtimes(&state.dynamo, table).await;
    let images = store::latest_images(&state.dynamo, table).await;
    let kev = kev(state).await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let empty = BTreeMap::new();
    let mut by_account: BTreeMap<String, Vec<Resource>> = BTreeMap::new();
    for r in resources {
        by_account.entry(r.account.clone()).or_default().push(r);
    }
    let mut fresh = Vec::new();
    for (account, res) in &by_account {
        let feeds = Feeds {
            advisories: &advisories,
            bulletins: &bulletins,
            runtimes: &runtimes,
            latest: &images,
            kev: &kev,
            latest_al2023: latest.get(account).unwrap_or(&empty),
            today: &today,
        };
        fresh.extend(matching::findings(res, &feeds));
    }
    let changed = records::reconcile(&stored, fresh, Utc::now());
    for r in &changed {
        save(state, team_id, r).await;
        if let Some(e) = &r.event {
            super::security_fixes::add_event(state, team_id, &r.finding.id, e, None).await;
        }
        match stored.iter_mut().find(|x| x.finding.id == r.finding.id) {
            Some(x) => *x = r.clone(),
            None => stored.push(r.clone()),
        }
    }
    stored
}

fn cadence_due(c: Cadence, daily: bool) -> bool {
    match c {
        Cadence::Off => false,
        Cadence::Now | Cadence::Hour => true,
        Cadence::Daily => daily,
        Cadence::Weekly => daily && Utc::now().format("%u").to_string() == "1",
    }
}

pub(crate) fn unit_for(r: &Record) -> Option<FixUnit> {
    // A pull request only where the fix is one and the code was found for
    // sure: a low-confidence location never becomes a pull request.
    if r.finding.fix.kind != Some(FixKind::Pr) {
        return None;
    }
    let loc = r.finding.fix.location.as_ref().filter(|l| l.sure())?;
    let key = format!("infra:{}", r.finding.id);
    let to = r.finding.fix.to.clone();
    let branch_name = format!(
        "coderhelm/security-infra-{}-{}",
        r.finding
            .name
            .to_ascii_lowercase()
            .replace(|c: char| !c.is_ascii_alphanumeric(), "-"),
        to.to_ascii_lowercase()
            .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
    );
    Some(FixUnit {
        repo: loc.repo.clone(),
        ecosystem: records::UNIT_ECOSYSTEM.into(),
        grouped: false,
        key: key.clone(),
        tier: r.finding.tier,
        finding_ids: vec![r.finding.id.clone()],
        bumps: Vec::new(),
        bundle: false,
        cadence: None,
        ticket: sec::ticket_id(&loc.repo, &key),
        branch: common::git_conventions::branch(
            &branch_name,
            &common::git_conventions::Tokens::default(),
            "",
        ),
        title: format!(
            "fix(infra): {} [SECURITY] ({})",
            r.finding.fix.summary,
            r.finding.cves.first().cloned().unwrap_or_else(|| r
                .finding
                .sources
                .first()
                .map(|s| s.id.clone())
                .unwrap_or_default())
        ),
        labels: vec!["security".into(), "infrastructure".into()],
    })
}

/// How long after a tripped guard the one fix round starts: the run that
/// opened the pull request ends first.
const FIX_ROUND_DELAY_SECS: i32 = 300;

/// Puts one ticket for a fix unit on the ticket queue.
async fn send_ticket(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    unit: &FixUnit,
    body: String,
    base: String,
    delay_secs: i32,
) -> Result<(), String> {
    let (owner, name) = unit.repo.split_once('/').ok_or("bad repo")?;
    let msg = WorkerMessage::Ticket(TicketMessage {
        team_id: team_id.to_string(),
        installation_id,
        source: TicketSource::Alert,
        ticket_id: unit.ticket.clone(),
        title: unit.title.clone(),
        body,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        issue_number: 0,
        sender: "security".into(),
        requester: Some(common::requester::Requester::coderhelm(
            common::requester::SECURITY,
        )),
        base_branch: base,
        ..Default::default()
    });
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(serde_json::to_string(&msg).map_err(|e| e.to_string())?)
        .delay_seconds(delay_secs.clamp(0, 900))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Queues one infrastructure fix run.
async fn enqueue(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    r: &mut Record,
) -> Result<(), String> {
    if state.config.ticket_queue_url.is_empty() {
        return Err("no ticket queue".into());
    }
    let unit = unit_for(r).ok_or("no file to change")?;
    let (owner, name) = unit.repo.split_once('/').ok_or("bad repo")?;
    let gh = crate::clients::github::GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation_id,
        &state.http,
    )
    .map_err(|e| e.to_string())?;
    let base = gh
        .get_default_branch(owner, name)
        .await
        .map_err(|e| e.to_string())?;
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&sec::ticket_sk(&unit.ticket)))
        .item(
            "unit",
            attr_s(&serde_json::to_string(&unit).map_err(|e| e.to_string())?),
        )
        .item("repo", attr_s(&unit.repo))
        .item("branch", attr_s(&unit.branch))
        .item("title", attr_s(&unit.title))
        .item("lead_finding", attr_s(&r.finding.id))
        .item("ttl", attr_n(Utc::now().timestamp() + 120 * 86_400))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    send_ticket(
        state,
        team_id,
        installation_id,
        &unit,
        records::ticket_body(r),
        base,
        0,
    )
    .await?;
    r.claim(&unit.ticket, Utc::now());
    r.updated_at = Utc::now().to_rfc3339();
    save(state, team_id, r).await;
    super::security_fixes::add_event(
        state,
        team_id,
        &r.finding.id,
        &format!("Fix run started: {}", unit.title),
        None,
    )
    .await;
    info!(team_id, ticket = %unit.ticket, repo = %unit.repo, "infra findings: fix run queued");
    Ok(())
}

fn slots<'a>(state: &'a WorkerState, team_id: &'a str) -> sec::DynamoSlots<'a> {
    sec::DynamoSlots {
        dynamo: &state.dynamo,
        settings_table: &state.config.settings_table_name,
        analytics_table: &state.config.analytics_table_name,
        team_id,
    }
}

/// Starts the PR fixes that are due, within the day's limits. `only` is a
/// person's request for one finding (started even in a dry run).
async fn start_fixes(
    state: &WorkerState,
    team_id: &str,
    installation_id: u64,
    cfg: &SecurityConfig,
    records: &mut [Record],
    daily: bool,
    only: Option<&str>,
) -> (usize, Option<String>) {
    let day = sec::limit_day(Utc::now());
    let mut started = 0;
    let mut why: Option<String> = None;
    let mut order: Vec<usize> = (0..records.len()).collect();
    order.sort_by_key(|i| records[*i].tier());
    for i in order {
        let r = &records[i];
        let asked = only.is_some_and(|id| id == r.finding.id);
        if only.is_some() && !asked {
            continue;
        }
        if r.status != Status::Open || unit_for(r).is_none() {
            if asked {
                why = Some(if r.finding.fix.decision.is_empty() {
                    "This finding has no PR to open (a runbook step, AWS-managed, or no file found).".into()
                } else {
                    format!("This finding has no PR to open. {}", r.finding.fix.decision)
                });
            }
            continue;
        }
        // Its pull request closed unmerged, or its run ended without one:
        // no new run by itself while the hold is on. A person's own
        // request goes ahead.
        if r.on_hold(Utc::now()) && !asked {
            continue;
        }
        // The guard refused this finding's change and the finding still
        // asks for the same: no new run by itself, until its code or its
        // target changes. A person's own request goes ahead.
        if r.guard_blocks() && !asked {
            continue;
        }
        // Production waits for its non-production copy. A person's own
        // request for this finding goes ahead.
        if !asked {
            let today = Utc::now().format("%Y-%m-%d").to_string();
            if records::held(r, records, &today).is_some() {
                continue;
            }
        }
        if !asked
            && (!cfg.acts() || !cadence_due(cfg.open_prs.get(r.tier().unwrap_or(Tier::P3)), daily))
        {
            continue;
        }
        let repo = r
            .finding
            .fix
            .location
            .as_ref()
            .map(|l| l.repo.clone())
            .unwrap_or_default();
        let busy = records.iter().any(|x| {
            x.status == Status::Fixing
                && x.finding
                    .fix
                    .location
                    .as_ref()
                    .is_some_and(|l| l.repo == repo)
        });
        match sec::admit_run(&slots(state, team_id), cfg, &day, busy).await {
            Admit::Start => {
                let r = &mut records[i];
                // A refusal about a change it no longer asks for, or one
                // a person overrides, is forgotten: a full cycle again.
                r.start_over();
                match enqueue(state, team_id, installation_id, r).await {
                    Ok(()) => started += 1,
                    Err(e) => {
                        use common::security_fixes::DailySlots;
                        slots(state, team_id).give_back(&day).await;
                        warn!(team_id, error = %e, "infra findings: the fix run could not be queued");
                        why = Some(e);
                    }
                }
            }
            Admit::Wait(w) => {
                why = Some(w);
                if !asked {
                    break;
                }
            }
        }
    }
    (started, why)
}

/// New P0 and P1 findings post one card each, inside the shared daily limit.
async fn cards(
    state: &WorkerState,
    team_id: &str,
    cfg: &SecurityConfig,
    records: &[Record],
) -> usize {
    if !cfg.acts() {
        return 0;
    }
    let urgent: Vec<&Record> = records
        .iter()
        .filter(|r| r.status.is_open() && matches!(r.tier(), Some(Tier::P0) | Some(Tier::P1)))
        .collect();
    if urgent.is_empty() {
        return 0;
    }
    let channels = super::security_fixes::team_channels(state, team_id).await;
    if channels.is_empty() {
        return 0;
    }
    let mut posted = 0;
    for r in urgent {
        let sk = sec::card_claim_sk(&format!("infra:{}", r.finding.id), "new");
        let won = common::claim::claim(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            &sk,
            sec::CARD_CLAIM_SECS,
        )
        .await;
        if !matches!(won, common::claim::Claim::Won) {
            continue;
        }
        if super::security_fixes::cards_today(state, team_id, 1).await > cfg.max_cards_per_day {
            common::claim::release(
                &state.dynamo,
                &state.config.settings_table_name,
                team_id,
                &sk,
            )
            .await;
            break;
        }
        let url = format!(
            "{}/security/infrastructure?finding={}",
            super::skills::app_url(&state.config.stage),
            r.finding.id
        );
        let msg = records::card(r, &url);
        let sent = common::notify::deliver_recorded(
            &common::notify::Http(&state.http),
            &channels,
            &msg,
            "security",
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
        )
        .await;
        if sent.any_ok() {
            posted += 1;
        } else {
            common::claim::release(
                &state.dynamo,
                &state.config.settings_table_name,
                team_id,
                &sk,
            )
            .await;
        }
    }
    posted
}

/// The daily match for every team with security fixes on.
pub async fn sweep(state: &WorkerState) -> usize {
    let teams = common::settings_index::members(
        &state.dynamo,
        &state.config.settings_table_name,
        &sec::index_spec(),
    )
    .await
    .unwrap_or_default();
    let day = Utc::now().format("%Y%m%d").to_string();
    let mut n = 0;
    for team in teams {
        let Some(cfg) = super::security_fixes::load_config(state, &team).await else {
            continue;
        };
        if !cfg.enabled {
            continue;
        }
        let won = common::claim::claim(
            &state.dynamo,
            &state.config.settings_table_name,
            &team,
            &format!("INFRA_MATCH#{day}"),
            2 * 86_400,
        )
        .await;
        if !matches!(won, common::claim::Claim::Won) {
            continue;
        }
        let mut recs = match_team(state, &team).await;
        if let Some(iid) = super::review_reminder::team_installation_id(state, &team).await {
            start_fixes(state, &team, iid, &cfg, &mut recs, true, None).await;
        }
        cards(state, &team, &cfg, &recs).await;
        n += 1;
    }
    if n > 0 {
        info!(teams = n, "Infrastructure findings matched");
    }
    n
}

/// A person's "check now" or "open the fix PR now".
pub async fn on_event(state: &WorkerState, msg: &SecurityEventMessage) {
    let team = &msg.team_id;
    let cfg = super::security_fixes::load_config(state, team)
        .await
        .unwrap_or_default();
    if msg.kind == CHECK_KIND {
        for a in common::live::aws_account::list(
            &state.dynamo,
            &state.config.aws_insights_table_name,
            team,
        )
        .await
        .unwrap_or_default()
        {
            if a.infra_on() {
                if let Err(e) = super::infra_inventory::read_account(state, team, &a).await {
                    warn!(team_id = %team, account = %a.account, error = %e, "infra check: the role couldn't be taken on");
                }
            }
        }
        let recs = match_team(state, team).await;
        cards(state, team, &cfg, &recs).await;
    } else if msg.kind == FIX_KIND {
        let mut recs = load_records(state, team).await;
        let iid = if msg.installation_id > 0 {
            Some(msg.installation_id)
        } else {
            super::review_reminder::team_installation_id(state, team).await
        };
        if let Some(iid) = iid {
            // What the record says is checked against GitHub and its run
            // first: a person asking after closing the pull request (or
            // pressing Retry on its run) is never told "already open".
            if let Ok(gh) = crate::clients::github::GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                iid,
                &state.http,
            ) {
                let scope = Scope::Ids(vec![msg.repo.clone()]);
                recs = release(state, team, &gh, &scope).await.0;
            }
            let (n, why) =
                start_fixes(state, team, iid, &cfg, &mut recs, false, Some(&msg.repo)).await;
            if n == 0 {
                if let Some(w) = why {
                    super::security_fixes::add_event(
                        state,
                        team,
                        &msg.repo,
                        &format!("Fix PR requested; not started: {w}"),
                        None,
                    )
                    .await;
                }
            }
        }
    }
}

// ── Release: fixing and pr_open follow GitHub and the run ───────────────

use super::security_fixes::release::{self as rel, ReleaseIo, Scope};
use common::security_release::{OpenPr, PrNow, RunNow};

/// This store's edges for the shared reconciliation
/// (`security_fixes::release::reconcile`): the same reads of GitHub and the
/// run records as dependency findings, and the records' own storage.
struct Release<'a> {
    reads: rel::Reads<'a>,
    started: std::time::Instant,
}

#[async_trait::async_trait]
impl ReleaseIo<Record> for Release<'_> {
    async fn pr_now(&self, repo: &str, number: u64, closer: bool) -> PrNow {
        self.reads.pr_now(repo, number, closer).await
    }
    async fn open_pr(&self, repo: &str, ticket: &str) -> Result<Option<OpenPr>, ()> {
        self.reads.open_pr(repo, ticket).await
    }
    async fn run_now(&self, ticket: &str, since: Option<chrono::DateTime<Utc>>) -> RunNow {
        self.reads.run_now(ticket, since).await
    }
    async fn save(&self, r: &Record) {
        save(self.reads.state, self.reads.team_id, r).await;
        // The guard's second step, reached because the fix round ended
        // without settling the pull request: it becomes a draft on GitHub
        // too, with the reason at its top. Never closed, branch kept.
        if let Some((repo, number)) = newly_drafted(r) {
            draft_with_reason(self.reads.gh, &repo, number, &r.refused).await;
        }
    }
    async fn event(&self, finding_id: &str, text: &str, link: Option<&str>) {
        super::security_fixes::add_event(
            self.reads.state,
            self.reads.team_id,
            finding_id,
            text,
            link,
        )
        .await;
    }
    /// A pull request the run opened that the record never learned: the
    /// replacement guard reads it before anything is recorded.
    async fn opened(&self, r: &mut Record, pr: &OpenPr) -> bool {
        let Some(unit) = unit_for(r) else {
            return false;
        };
        let (state, team_id) = (self.reads.state, self.reads.team_id);
        on_pr_opened(state, team_id, &unit, pr.number, &pr.url).await;
        match load_records(state, team_id)
            .await
            .into_iter()
            .find(|x| x.finding.id == r.finding.id)
        {
            Some(now) => {
                *r = now;
                true
            }
            None => false,
        }
    }
    fn out_of_time(&self) -> bool {
        self.started.elapsed().as_secs() >= rel::TEAM_BUDGET_SECS
    }
}

/// The pull request to make a draft on GitHub when the record was just
/// settled as the guard's second step (`Claim::settle_run`): a drafted
/// record whose pull request is recorded as a draft. Pure.
pub(crate) fn newly_drafted(r: &Record) -> Option<(String, u64)> {
    let pr = r.pr.as_ref().filter(|p| p.state == "draft")?;
    let repo = r.finding.fix.location.as_ref()?.repo.clone();
    (r.drafted && !r.refused.is_empty()).then_some((repo, pr.number))
}

/// Brings the team's infrastructure records in `scope` in line with GitHub
/// and the run records: the sweep (`Scope::All`), a pull request that
/// closed or reopened, a run a person ended, a person's request. Returns
/// the records as they stand and what was done.
pub(crate) async fn release(
    state: &WorkerState,
    team_id: &str,
    gh: &crate::clients::github::GitHubClient,
    scope: &Scope,
) -> (Vec<Record>, rel::Report) {
    let mut recs = load_records(state, team_id).await;
    // Most teams have no infrastructure finding in a run or a pull
    // request: nothing to read then.
    if !recs
        .iter()
        .any(|r| r.pr.is_some() || r.status == Status::Fixing)
    {
        return (recs, rel::Report::default());
    }
    let io = Release {
        reads: rel::Reads { state, team_id, gh },
        started: std::time::Instant::now(),
    };
    let report = rel::reconcile(&io, &mut recs, scope, Utc::now()).await;
    rel::said(team_id, "infrastructure", scope, &report);
    (recs, report)
}

/// What the replacement guard says of a pull request's files
/// (`(path, before, after)`): the reasons to refuse it, empty when none.
pub fn guard_refusals(files: &[(String, String, String)]) -> Vec<String> {
    let changes: Vec<common::infra_vuln::guard::Change> = files
        .iter()
        .map(|(path, before, after)| common::infra_vuln::guard::Change {
            path,
            before,
            after,
        })
        .collect();
    common::infra_vuln::guard::refusals(&changes)
}

/// What the guard needs to act on a pull request.
struct Guarded {
    gh: crate::clients::github::GitHubClient,
    installation_id: u64,
    base: String,
    reasons: Vec<String>,
}

/// Reads each infrastructure file the pull request changed, before and
/// after, and asks the replacement guard. `None` when the files could not
/// be read (the guard then has no say; the description still carries what
/// the plan must show).
async fn guard_pr(
    state: &WorkerState,
    team_id: &str,
    unit: &FixUnit,
    number: u64,
) -> Option<Guarded> {
    let installation_id = super::review_reminder::team_installation_id(state, team_id).await?;
    let gh = crate::clients::github::GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation_id,
        &state.http,
    )
    .ok()?;
    let (owner, name) = unit.repo.split_once('/')?;
    let base = gh.get_default_branch(owner, name).await.ok()?;
    let changed = gh.list_pull_files(owner, name, number).await.ok()?;
    let mut files = Vec::new();
    for (path, _) in changed {
        if !common::infra_vuln::locate::maybe_iac_path(&path) {
            continue;
        }
        let before = gh
            .read_file_if_exists(owner, name, &path, &base)
            .await
            .ok()?
            .unwrap_or_default();
        let after = gh
            .read_file_if_exists(owner, name, &path, &unit.branch)
            .await
            .ok()?
            .unwrap_or_default();
        files.push((path, before, after));
    }
    let reasons = guard_refusals(&files);
    Some(Guarded {
        gh,
        installation_id,
        base,
        reasons,
    })
}

/// What a record shows after each outcome of the guard, and the line for
/// the finding's timeline. Pure.
pub(crate) fn after_guard(
    r: &mut Record,
    reasons: &[String],
    number: u64,
    url: &str,
    now: &str,
) -> (records::GuardStep, String) {
    let step = r.guard_step(reasons);
    let pr = |state: &str| {
        Some(records::PrRef {
            number,
            url: url.to_string(),
            state: state.into(),
            ..Default::default()
        })
    };
    // A pull request stands (or is held): nothing waits on a hold now.
    r.hold_until = None;
    r.release_note = None;
    r.claimed_at = None;
    let event = match step {
        records::GuardStep::Passed => {
            r.status = Status::PrOpen;
            r.pr = pr("open");
            format!("PR #{number} opened")
        }
        records::GuardStep::FixRound => {
            // Still CoderHelm's to finish: the fix round is on its way. Its
            // run starts after the delay: only a run that began from then
            // on is the round (the one that opened the pull request is not).
            r.status = Status::Fixing;
            r.pr = pr("open");
            r.claimed_at = sec::parse_time(now).map(|t| {
                (t + chrono::Duration::seconds(FIX_ROUND_DELAY_SECS as i64)
                    + chrono::Duration::minutes(common::security_release::RUN_CLOCK_TOLERANCE_MINS))
                .to_rfc3339()
            });
            format!(
                "PR #{number}: the change would replace or destroy a resource that holds data ({}). One fix round corrects it and the change is read again",
                reasons.join("; ")
            )
        }
        records::GuardStep::Draft => {
            r.status = Status::Open;
            r.pr = pr("draft");
            format!(
                "PR #{number} is now a draft: after a fix round the change would still replace or destroy a resource that holds data ({}). The finding shows its runbook. {}",
                reasons.join("; "),
                records::GUARD_STOPPED
            )
        }
    };
    r.updated_at = now.to_string();
    (step, event)
}

/// The pull request becomes a draft with the reason at the top of its
/// description. It is never closed and its branch is never deleted.
async fn draft_with_reason(
    gh: &crate::clients::github::GitHubClient,
    repo: &str,
    number: u64,
    reasons: &[String],
) {
    let Some((owner, name)) = repo.split_once('/') else {
        return;
    };
    let pr = match gh.get_pull_request(owner, name, number).await {
        Ok(pr) => pr,
        Err(e) => {
            warn!(number, error = %e, "infra findings: the pull request could not be read to make it a draft");
            return;
        }
    };
    if !pr["draft"].as_bool().unwrap_or(false) {
        if let Some(node) = pr["node_id"].as_str() {
            if let Err(e) = gh.convert_pr_to_draft(node).await {
                warn!(number, error = %e, "infra findings: the pull request could not be made a draft");
            }
        }
    }
    // The reason is in the description whether or not the conversion
    // worked: it alone keeps the pull request from being marked ready.
    let body = common::infra_vuln::guard::put_hold(pr["body"].as_str().unwrap_or(""), reasons);
    if let Err(e) = crate::passes::description::save(gh, owner, name, number, &body).await {
        warn!(number, error = %e, "infra findings: the reason could not be written on the pull request");
    }
}

/// A fix run for an infrastructure finding opened or updated its PR. The
/// replacement guard reads the change first. A change that would replace
/// or destroy a resource that holds data gets one fix round that is told
/// each offending line; if the change still trips the guard after it, the
/// pull request becomes a draft with the reason at its top, the finding
/// falls back to its runbook, and no further attempt starts by itself
/// until the finding's code or target changes. The pull request is never
/// closed and its branch is never deleted.
pub async fn on_pr_opened(
    state: &WorkerState,
    team_id: &str,
    unit: &FixUnit,
    number: u64,
    url: &str,
) {
    let guarded = guard_pr(state, team_id, unit, number).await;
    let reasons: Vec<String> = guarded
        .as_ref()
        .map(|g| g.reasons.clone())
        .unwrap_or_default();
    let mut drafted = false;
    for id in &unit.finding_ids {
        let mut recs = load_records(state, team_id).await;
        let Some(r) = recs.iter_mut().find(|r| &r.finding.id == id) else {
            continue;
        };
        let (step, event) = after_guard(r, &reasons, number, url, &Utc::now().to_rfc3339());
        save(state, team_id, r).await;
        super::security_fixes::add_event(state, team_id, id, &event, Some(url)).await;
        match (step, &guarded) {
            (records::GuardStep::FixRound, Some(g)) => {
                if let Err(e) = send_ticket(
                    state,
                    team_id,
                    g.installation_id,
                    unit,
                    records::fix_round_body(r),
                    g.base.clone(),
                    FIX_ROUND_DELAY_SECS,
                )
                .await
                {
                    warn!(team_id, number, error = %e, "infra findings: the fix round could not be queued");
                }
            }
            (records::GuardStep::Draft, Some(g)) if !drafted => {
                draft_with_reason(&g.gh, &unit.repo, number, &reasons).await;
                drafted = true;
            }
            _ => {}
        }
    }
}

/// The PR description for an infrastructure fix run.
pub async fn pr_body(
    state: &WorkerState,
    team_id: &str,
    unit: &FixUnit,
    change: &str,
) -> Option<String> {
    let id = unit.finding_ids.first()?;
    let recs = load_records(state, team_id).await;
    recs.iter()
        .find(|r| &r.finding.id == id)
        .map(|r| records::pr_body(r, change))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::infra_vuln::inventory::Location;
    use common::infra_vuln::matching::{Fix, InfraFinding};

    #[test]
    fn a_pr_finding_becomes_one_unit_in_the_repo_that_defines_it() {
        let r = Record::new(
            InfraFinding {
                id: "abc".into(),
                name: "web-asg".into(),
                tier: Some(Tier::P1),
                cves: vec!["CVE-2026-98432".into()],
                fix: Fix {
                    kind: Some(FixKind::Pr),
                    summary:
                        "Point the AMI at AWS's latest AL2023 parameter (now 2023.12.20261102)"
                            .into(),
                    to: "2023.12.20261102".into(),
                    location: Some(Location {
                        repo: "acme/infra".into(),
                        file: "main.tf".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            },
            "t",
        );
        let u = unit_for(&r).unwrap();
        assert_eq!(u.repo, "acme/infra");
        assert_eq!(u.ecosystem, records::UNIT_ECOSYSTEM);
        assert!(u.ticket.starts_with("SEC-"));
        assert!(u.branch.contains("security-infra-web-asg"), "{}", u.branch);
        assert!(u.title.contains("[SECURITY] (CVE-2026-98432)"));
        let mut none = r.clone();
        none.finding.fix.location = None;
        assert!(unit_for(&none).is_none(), "no file, no PR");
        // A location that is a guess is never a pull request.
        let mut unsure = r.clone();
        if let Some(l) = unsure.finding.fix.location.as_mut() {
            l.confidence = common::infra_vuln::inventory::Confidence::Low;
        }
        assert!(
            unit_for(&unsure).is_none(),
            "a low-confidence match opens no PR"
        );
        // Nor is a runbook that happens to know where the code is.
        let mut runbook = r.clone();
        runbook.finding.fix.kind = Some(FixKind::Runbook);
        assert!(unit_for(&runbook).is_none());
    }

    fn pr_record() -> Record {
        Record::new(
            InfraFinding {
                id: "abc".into(),
                name: "orders".into(),
                fix: Fix {
                    kind: Some(FixKind::Pr),
                    to: "16.6".into(),
                    location: Some(Location {
                        repo: "acme/infra".into(),
                        file: "db/main.tf".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            },
            "t",
        )
    }

    const WHY: &str = "db/main.tf, line 2: `aws_db_instance.orders` changes `identifier`, which replaces the resource";

    #[test]
    fn a_tripped_guard_gets_one_fix_round_and_the_pull_request_stays_open() {
        let mut r = pr_record();
        let (step, event) = after_guard(&mut r, &[WHY.to_string()], 7, "u", "now");
        assert_eq!(step, records::GuardStep::FixRound);
        // Still CoderHelm's work, the pull request open, never closed.
        assert_eq!(r.status, Status::Fixing);
        assert_eq!(r.pr.as_ref().unwrap().state, "open");
        assert!(event.contains("One fix round corrects it"), "{event}");
        assert!(!event.contains("closed"));
        // What the round is told.
        let body = records::fix_round_body(&r);
        assert!(
            body.contains(WHY) && body.contains("Undo exactly those lines"),
            "{body}"
        );
        // The round's change passes: a normal open pull request again.
        let (step, event) = after_guard(&mut r, &[], 7, "u", "now");
        assert_eq!(step, records::GuardStep::Passed);
        assert_eq!(r.status, Status::PrOpen);
        assert!(r.refused.is_empty());
        assert_eq!(event, "PR #7 opened");
    }

    #[test]
    fn a_second_trip_makes_the_pull_request_a_draft_and_stops_automatic_attempts() {
        let mut r = pr_record();
        after_guard(&mut r, &[WHY.to_string()], 7, "u", "now");
        let (step, event) = after_guard(&mut r, &[WHY.to_string()], 7, "u", "now");
        assert_eq!(step, records::GuardStep::Draft);
        assert_eq!(r.pr.as_ref().unwrap().state, "draft");
        assert!(
            event.contains("is now a draft") && event.contains("runbook"),
            "{event}"
        );
        assert!(!event.contains("closed"));
        // Open again as a finding, shown as a runbook, and not picked up
        // by itself while it asks for the same change.
        assert_eq!(r.status, Status::Open);
        assert_eq!(r.view()["fix"]["kind"], "runbook");
        assert!(unit_for(&r).is_some(), "still a code fix underneath");
        assert!(r.guard_blocks());
        // Its target changes: an attempt may start again, from the top.
        r.finding.fix.to = "16.8".into();
        assert!(!r.guard_blocks());
        r.start_over();
        let (step, _) = after_guard(&mut r, &[WHY.to_string()], 8, "u", "now");
        assert_eq!(step, records::GuardStep::FixRound);
    }

    /// The description's section is what keeps the draft a draft: the one
    /// decision that marks pull requests ready reads it as a hold, and a
    /// draft is never merged (`await_merge`).
    #[test]
    fn the_draft_reason_holds_the_pull_request_and_nothing_closes_it_or_deletes_its_branch() {
        use common::infra_vuln::guard;
        let body = guard::put_hold("<!-- m -->\n\n## Summary\n", &[WHY.to_string()]);
        let holds = crate::passes::ready::body_holds(&body);
        assert_eq!(holds.len(), 1, "{holds:?}");
        assert!(crate::passes::ready::body_holds("## Summary\n").is_empty());
        let decided = crate::passes::ready::decide(&crate::passes::ready::Facts {
            draft: true,
            body,
            ..Default::default()
        });
        assert!(
            matches!(decided, crate::passes::ready::Decision::Held { .. }),
            "{decided:?}"
        );
        // This file never closes a pull request, deletes a branch or
        // marks one ready.
        let src = include_str!("infra_findings.rs");
        let code = src.split("#[cfg(test)]").next().unwrap();
        for never in [
            "close_pull_request",
            "delete_branch",
            "delete_ref",
            "mark_pr_ready",
            "merge_pull_request",
        ] {
            assert!(!code.contains(never), "infra_findings calls {never}");
        }
        // And it writes a description in exactly one place.
        assert_eq!(code.matches("description::save(").count(), 1);
        assert!(!code.contains(&[".update_pr", "_body("].concat()));
    }

    // ── Release: the shared reconciliation on this store's records ──

    use crate::passes::security_fixes::release::{fake::*, reconcile};
    use common::security_release::{Claim, RunNow};

    const TICKET: &str = "SEC-0A1B2C3D4E5F";

    fn day() -> chrono::DateTime<Utc> {
        at("2026-11-13T10:00:00Z")
    }

    /// A record whose fix run opened pull request 7 and the guard passed it.
    fn in_pr_record(id: &str, number: u64) -> Record {
        let mut r = pr_record();
        r.finding.id = id.into();
        r.ticket = TICKET.into();
        after_guard(
            &mut r,
            &[],
            number,
            "https://github.com/acme/infra/pull/7",
            "2026-11-12T09:00:00Z",
        );
        r.updated_at = "2026-11-12T09:00:00Z".into();
        r
    }

    #[tokio::test]
    async fn the_sweep_releases_a_record_whose_pull_request_a_person_closed() {
        let mut recs = vec![in_pr_record("a", 7), in_pr_record("b", 8)];
        let github = Fake::default();
        github.pr("acme/infra", 7, closed_by("octo"));
        github.pr("acme/infra", 8, common::security_release::PrNow::Open);
        let r = reconcile(&github, &mut recs, &Scope::All, day()).await;
        assert_eq!((r.prs_read, r.released), (2, 1));
        assert_eq!(recs[0].status, Status::Open);
        assert_eq!(
            recs[0].pr.as_ref().unwrap().closed_by.as_deref(),
            Some("octo")
        );
        assert_eq!(recs[1].status, Status::PrOpen);
        assert!(recs[0].on_hold(day()));
        assert!(github.event_texts()[0].contains("closed without merging by octo"));
        // Again: read once more while the hold is on, nothing written.
        let saves = github.saves();
        let again = reconcile(&github, &mut recs, &Scope::All, day()).await;
        assert_eq!((again.changed(), github.saves()), (0, saves));
        // The webhook's event ends in the same place as the sweep.
        let mut by_event = vec![in_pr_record("a", 7), in_pr_record("b", 8)];
        let scope = Scope::Pr {
            repo: "acme/infra".into(),
            number: 7,
        };
        let r = reconcile(&github, &mut by_event, &scope, day()).await;
        assert_eq!((r.prs_read, r.released), (1, 1));
        assert_eq!(by_event, recs);
    }

    #[tokio::test]
    async fn github_down_releases_nothing_and_a_merge_never_closes_the_finding() {
        let mut recs = vec![in_pr_record("a", 7)];
        let before = recs.clone();
        let github = Fake::default();
        let r = reconcile(&github, &mut recs, &Scope::All, day()).await;
        assert_eq!((r.changed(), github.saves()), (0, 0));
        assert_eq!(recs, before);
        github.pr("acme/infra", 7, common::security_release::PrNow::Merged);
        let r = reconcile(&github, &mut recs, &Scope::All, day()).await;
        // Recorded, not "fixed": only a read of the account closes it, and
        // no merged card is this store's to post.
        assert_eq!((r.fixed, r.released, github.saves()), (0, 0, 1));
        assert_eq!(recs[0].status, Status::PrOpen);
        assert_eq!(recs[0].pr.as_ref().unwrap().state, "merged");
        assert!(github.merged_cards.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_cancelled_run_releases_its_record_and_a_working_one_is_left() {
        let mut a = pr_record();
        a.claim(TICKET, at("2026-11-13T09:00:00Z"));
        let mut b = pr_record();
        b.finding.id = "other".into();
        b.claim("SEC-BBBBBBBBBBBB", at("2026-11-13T09:00:00Z"));
        let mut recs = vec![a, b];
        let runs = Fake::default();
        runs.run(
            TICKET,
            RunNow::Ended {
                status: "cancelled".into(),
                by: Some("dev@example.com".into()),
            },
        );
        runs.run("SEC-BBBBBBBBBBBB", RunNow::Working);
        let r = reconcile(&runs, &mut recs, &Scope::Ticket(TICKET.into()), day()).await;
        assert_eq!((r.runs_read, r.released), (1, 1));
        assert_eq!(recs[0].status, Status::Open);
        assert!(recs[0].on_hold(day()) && recs[0].ticket.is_empty());
        let r = reconcile(&runs, &mut recs, &Scope::All, day()).await;
        assert_eq!((r.runs_read, r.changed()), (1, 0));
        assert_eq!(recs[1].status, Status::Fixing);
    }

    #[tokio::test]
    async fn a_pull_request_the_record_never_learned_is_read_by_the_guard_first() {
        let mut a = pr_record();
        a.claim(TICKET, at("2026-11-13T09:00:00Z"));
        let mut recs = vec![a];
        let github = Fake::default();
        github.branch_prs.lock().unwrap().insert(
            TICKET.into(),
            OpenPr {
                number: 9,
                url: "https://github.com/acme/infra/pull/9".into(),
            },
        );
        let before = recs.clone();
        reconcile(&github, &mut recs, &Scope::All, day()).await;
        // Handed to the store's opening path (the guard); nothing recorded
        // by the shared rule, and the run was not even asked.
        assert_eq!(
            *github.opened_through_store.lock().unwrap(),
            vec![("abc".to_string(), 9)]
        );
        assert_eq!(recs, before);
        assert_eq!(github.saves(), 0);
        assert!(github.run_reads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_fix_round_that_ended_makes_the_pull_request_a_draft_and_a_draft_is_never_closed() {
        // The guard tripped once: the round is to start in five minutes,
        // and only a run that begins from then on is the round.
        let mut r = pr_record();
        r.ticket = TICKET.into();
        let (step, _) = after_guard(&mut r, &[WHY.to_string()], 7, "u", "2026-11-13T09:00:00Z");
        assert_eq!(step, records::GuardStep::FixRound);
        assert_eq!(Claim::claimed_at(&r), Some(at("2026-11-13T09:15:00Z")));
        assert_eq!(newly_drafted(&r), None);
        let mut recs = vec![r];
        let github = Fake::default();
        github.branch_prs.lock().unwrap().insert(
            TICKET.into(),
            OpenPr {
                number: 7,
                url: "u".into(),
            },
        );
        github.pr("acme/infra", 7, common::security_release::PrNow::Open);
        // The round works: nothing changes.
        github.run(TICKET, RunNow::Working);
        let rep = reconcile(&github, &mut recs, &Scope::All, day()).await;
        assert_eq!((rep.changed(), github.saves()), (0, 0));
        assert_eq!(github.run_reads.lock().unwrap().len(), 1, "the run decides");
        // The round was cancelled: the refused change still stands, so the
        // pull request is to be made a draft and the finding falls back.
        github.run(
            TICKET,
            RunNow::Ended {
                status: "cancelled".into(),
                by: None,
            },
        );
        reconcile(&github, &mut recs, &Scope::Ticket(TICKET.into()), day()).await;
        let r = &recs[0];
        assert_eq!(
            (r.status, r.pr.as_ref().unwrap().state.as_str()),
            (Status::Open, "draft")
        );
        assert!(r.drafted && r.guard_blocks());
        assert_eq!(newly_drafted(r), Some(("acme/infra".to_string(), 7)));
        assert!(github
            .event_texts()
            .last()
            .unwrap()
            .contains("PR #7 is now a draft"));
        // From then on the draft is read every sweep and left as it is:
        // open on GitHub is not closed, and never "reopened" either.
        let saves = github.saves();
        let rep = reconcile(&github, &mut recs, &Scope::All, day()).await;
        assert_eq!((rep.prs_read, rep.changed(), github.saves()), (1, 0, saves));
        assert_eq!(recs[0].pr.as_ref().unwrap().state, "draft");
    }

    #[test]
    fn the_guard_reads_a_pull_requests_files_before_and_after() {
        let before = "resource \"aws_db_instance\" \"orders\" {\n  identifier     = \"orders-prod\"\n  engine_version = \"16.4\"\n}\n";
        let ok = vec![(
            "db/main.tf".to_string(),
            before.to_string(),
            before.replace("16.4", "16.6"),
        )];
        assert!(guard_refusals(&ok).is_empty());
        let bad = vec![(
            "db/main.tf".to_string(),
            before.to_string(),
            before.replace("orders-prod", "orders"),
        )];
        let why = guard_refusals(&bad);
        assert_eq!(why.len(), 1);
        assert!(why[0].contains("replaces the resource"), "{why:?}");
    }

    #[test]
    fn cadence_follows_the_policy() {
        assert!(cadence_due(Cadence::Now, false));
        assert!(!cadence_due(Cadence::Daily, false));
        assert!(cadence_due(Cadence::Daily, true));
        assert!(!cadence_due(Cadence::Off, true));
    }
}
