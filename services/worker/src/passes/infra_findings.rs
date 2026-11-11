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

fn unit_for(r: &Record) -> Option<FixUnit> {
    let loc = r.finding.fix.location.as_ref()?;
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
    let msg = WorkerMessage::Ticket(TicketMessage {
        team_id: team_id.to_string(),
        installation_id,
        source: TicketSource::Alert,
        ticket_id: unit.ticket.clone(),
        title: unit.title.clone(),
        body: records::ticket_body(r),
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
        .send()
        .await
        .map_err(|e| e.to_string())?;
    r.status = Status::Fixing;
    r.ticket = unit.ticket.clone();
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
        if r.status != Status::Open
            || r.finding.fix.kind != Some(FixKind::Pr)
            || r.finding.fix.location.is_none()
        {
            if asked {
                why = Some("This finding has no PR to open (a runbook step, AWS-managed, or no file found).".into());
            }
            continue;
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

/// A fix run for an infrastructure finding opened its PR.
pub async fn on_pr_opened(
    state: &WorkerState,
    team_id: &str,
    unit: &FixUnit,
    number: u64,
    url: &str,
) {
    for id in &unit.finding_ids {
        let mut recs = load_records(state, team_id).await;
        if let Some(r) = recs.iter_mut().find(|r| &r.finding.id == id) {
            r.status = Status::PrOpen;
            r.pr = Some(records::PrRef {
                number,
                url: url.to_string(),
                state: "open".into(),
            });
            r.updated_at = Utc::now().to_rfc3339();
            save(state, team_id, r).await;
            super::security_fixes::add_event(
                state,
                team_id,
                id,
                &format!("PR #{number} opened"),
                Some(url),
            )
            .await;
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
    }

    #[test]
    fn cadence_follows_the_policy() {
        assert!(cadence_due(Cadence::Now, false));
        assert!(!cadence_due(Cadence::Daily, false));
        assert!(cadence_due(Cadence::Daily, true));
        assert!(!cadence_due(Cadence::Off, true));
    }
}
