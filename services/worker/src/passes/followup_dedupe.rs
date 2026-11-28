//! Duplicate follow-ups close themselves, on a schedule
//! (`common::followup_dedupe` decides).
//!
//! It rides on the review-reminder tick and runs each team at most once an
//! hour (a claim per team and hour), so its first run is the first tick
//! after a deploy. Per team:
//!
//! - CoderHelm's follow-up records (`FOLLOWUP#<source>#...`) give the
//!   families: a source that is no follow-up itself, every follow-up filed
//!   from it, and from those ([`families`]);
//! - only a family with more than one run or pull request still open is
//!   read further ([`worth_reading`]);
//! - the topic matcher and the small model check read it, and a run closes
//!   only where both say the same topic (`followup_dedupe::agreed`). What
//!   they disagree on, and a fix against a delete, is flagged on both
//!   tickets instead;
//! - a close takes the same steps as a person's close with "Also close PR":
//!   the status, the ticket lock, auto-merge stopped, the pull request
//!   closed with the comment, the Jira note. At most
//!   `followup_tickets::DAILY_CAP_PER_SOURCE` closes per source a day;
//! - every close is logged (`FOLLOWUP_CLOSE_LOG#`) and traced.

use common::run_ending::EndWrite;
use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::Utc;
use common::followup_dedupe::{self as fd, Candidate, Close};
use common::followup_tickets as ft;
use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::TokenUsage;
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;

type Item = HashMap<String, AttributeValue>;

fn s(item: &Item, key: &str) -> String {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// The most follow-ups one family reads.
const MAX_FAMILY: usize = 40;

/// What one sweep did.
#[derive(Debug, Default)]
pub struct SweepReport {
    pub teams: usize,
    pub families: usize,
    pub closed: usize,
    pub flagged: usize,
}

/// The families in a team's follow-up records (`(source, filed key)`): each
/// root (a source that is no follow-up itself) with every follow-up under
/// it, sorted.
pub fn families(records: &[(String, String)]) -> BTreeMap<String, Vec<String>> {
    let mut filed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (src, key) in records {
        let (src, key) = (src.to_ascii_uppercase(), key.to_ascii_uppercase());
        if src != key {
            filed.entry(src).or_default().push(key);
        }
    }
    let children: std::collections::BTreeSet<String> = filed.values().flatten().cloned().collect();
    let mut out = BTreeMap::new();
    for root in filed.keys().filter(|k| !children.contains(*k)) {
        let mut fam: Vec<String> = Vec::new();
        let mut queue = vec![root.clone()];
        while let Some(k) = queue.pop() {
            for f in filed.get(&k).into_iter().flatten() {
                if f != root && !fam.contains(f) && fam.len() < MAX_FAMILY {
                    fam.push(f.clone());
                    queue.push(f.clone());
                }
            }
        }
        fam.sort();
        out.insert(root.clone(), fam);
    }
    out
}

/// A family is read only when more than one of its runs or pull requests
/// is still open.
pub fn worth_reading(cands: &[Candidate]) -> bool {
    cands.iter().filter(|c| c.closable()).count() > 1
}

/// The `(source, key)` of every follow-up record of the team.
async fn records(state: &WorkerState, team_id: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let page = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :s)")
            .expression_attribute_values(":p", attr_s(team_id))
            .expression_attribute_values(":s", attr_s("FOLLOWUP#"))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(page) = page else { break };
        for i in page.items() {
            let sk = s(i, "sk");
            let src = sk.split('#').nth(1).unwrap_or("").to_string();
            let key = s(i, "key");
            if !src.is_empty() && !key.is_empty() && s(i, "covers") != "1" {
                out.push((src, key));
            }
        }
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

/// One follow-up and its latest run, with the run's record.
async fn candidate(state: &WorkerState, team_id: &str, key: &str) -> Option<(Candidate, Item)> {
    let runs = common::log_findings::runs_for_ticket(
        &state.dynamo,
        &state.config.runs_table_name,
        team_id,
        key,
    )
    .await
    .ok()?;
    let latest = runs.iter().max_by(|a, b| a.run_id.cmp(&b.run_id))?;
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(&latest.run_id))
        .consistent_read(true)
        .send()
        .await
        .ok()?
        .item?;
    let prs = prs_of(&item);
    Some((
        Candidate {
            key: key.to_string(),
            title: s(&item, "title"),
            body: s(&item, "ticket_body"),
            run_id: latest.run_id.clone(),
            status: s(&item, "status"),
            has_pr: !prs.is_empty(),
        },
        item,
    ))
}

/// The pull requests a run opened (`owner`, `repo`, number).
fn prs_of(item: &Item) -> Vec<(String, String, u64)> {
    let mut urls: Vec<String> = item
        .get("pr_urls")
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        .unwrap_or_default();
    urls.push(s(item, "pr_url"));
    let mut out: Vec<(String, String, u64)> = Vec::new();
    for u in urls {
        if let Some(p) = common::pr_outcome::parse_pr_url(&u) {
            if p.2 > 0 && !out.contains(&p) {
                out.push(p);
            }
        }
    }
    out
}

/// The small check, through the team's model.
struct Check<'a> {
    state: &'a WorkerState,
    provider: ModelProvider,
}

#[async_trait::async_trait]
impl common::log_repo::live::Judge for Check<'_> {
    async fn judge(&self, system: &str, user: &str) -> Result<String, String> {
        let mut usage = TokenUsage::default();
        crate::agent::provider::converse_simple(
            self.state,
            &self.provider,
            self.provider.primary_model_id(),
            system,
            user,
            &mut usage,
        )
        .await
        .map_err(|e| e.to_string())
    }
}

/// Takes one of today's close slots for `source`; false when they are
/// taken or the count can't be read (nothing is closed then).
async fn take_close_slot(state: &WorkerState, team_id: &str, source: &str) -> bool {
    let day = Utc::now().format("%Y-%m-%d").to_string();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&ft::close_day_record_sk(source, &day)))
        .update_expression("ADD closed :one")
        .condition_expression("attribute_not_exists(closed) OR closed < :cap")
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":cap", attr_n(ft::DAILY_CAP_PER_SOURCE as i64))
        .send()
        .await
        .is_ok()
}

/// The scheduled sweep (on the review-reminder tick).
pub async fn sweep(state: &WorkerState) -> SweepReport {
    let mut report = SweepReport::default();
    let hour = Utc::now().format("%Y%m%d%H").to_string();
    for team_id in super::learning_wait::teams(state).await {
        let recs = records(state, &team_id).await;
        if recs.is_empty() {
            continue;
        }
        let claim = format!("FOLLOWUP_DEDUPE_SWEEP#{hour}");
        if !super::review_pr::claim_once_ttl(state, &team_id, &claim, 2 * 3600).await {
            continue;
        }
        report.teams += 1;
        sweep_team(state, &team_id, &recs, &mut report).await;
    }
    report
}

async fn sweep_team(
    state: &WorkerState,
    team_id: &str,
    recs: &[(String, String)],
    report: &mut SweepReport,
) {
    let mut check: Option<Option<Check<'_>>> = None;
    for (root, keys) in families(recs) {
        let mut cands = Vec::new();
        let mut items: HashMap<String, Item> = HashMap::new();
        for k in &keys {
            if let Some((c, item)) = candidate(state, team_id, k).await {
                items.insert(c.key.clone(), item);
                cands.push(c);
            }
        }
        if !worth_reading(&cands) {
            continue;
        }
        if check.is_none() {
            check = Some(
                ModelProvider::load_for_team(
                    &state.dynamo,
                    &state.config.settings_table_name,
                    team_id,
                )
                .await
                .ok()
                .map(|provider| Check { state, provider }),
            );
        }
        let Some(Some(judge)) = check.as_ref() else {
            // No model for the team: nothing is decided on the matcher alone.
            return;
        };
        report.families += 1;
        let plan = fd::decide(&cands, Some(judge)).await;
        for c in &plan.close {
            if !take_close_slot(state, team_id, &root).await {
                warn!(team_id, root = %root, "Follow-up duplicates: today's closes for the source are used up");
                break;
            }
            if let Some(item) = items.get(&c.dup) {
                if close_duplicate(state, team_id, &root, c, item).await {
                    report.closed += 1;
                }
            }
        }
        for f in &plan.flag {
            for (on, other) in [(&f.a, &f.b), (&f.b, &f.a)] {
                let say = common::conversation::Say::note(
                    &format!("followup_flag_{}", other.to_ascii_lowercase()),
                    fd::flagged_comment(f, other),
                );
                match super::conversation::jira(state, team_id, on, say, "note", "Two follow-ups")
                    .await
                {
                    Ok(_) => report.flagged += 1,
                    Err(e) => {
                        warn!(team_id, key = %on, error = %e, "Follow-up flag note not posted")
                    }
                }
            }
        }
    }
}

/// Closes one duplicate's run with a person's close steps. True when the
/// run's status was written.
async fn close_duplicate(
    state: &WorkerState,
    team_id: &str,
    root: &str,
    c: &Close,
    item: &Item,
) -> bool {
    let said = fd::close_comment(&c.keep, &c.topic);
    let from = s(item, "status");
    if !matches!(
        common::run_status::close_check(&from),
        common::run_status::CloseCheck::Close
    ) {
        return false;
    }
    let now = Utc::now().to_rfc3339();
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(&c.run_id))
        .update_expression(
            "SET #s = :closed, status_run_id = :sri, updated_at = :t, closed_at = :t, \
             closed_by = :by, closed_by_name = :name, closed_from = :from, close_reason = :why",
        )
        .condition_expression("attribute_exists(run_id) AND #s = :from")
        .expression_attribute_names("#s", "status")
        .run_status(
            ":closed",
            &common::run_ending::gate(
                &c.run_id,
                &common::run_ending::RunEnd::Stopped(common::run_ending::Stopped {
                    reason: common::run_ending::Stop::ClosedBy {
                        who: "CoderHelm".into(),
                        why: format!("a duplicate of {}", c.keep),
                    },
                    tried: Default::default(),
                }),
            ),
        )
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":by", attr_s("coderhelm"))
        .expression_attribute_values(":name", attr_s("CoderHelm"))
        .expression_attribute_values(":from", attr_s(&from))
        .expression_attribute_values(
            ":why",
            attr_s(&common::run_status::close_reason(&said).unwrap_or_default()),
        )
        .send()
        .await;
    if let Err(e) = res {
        warn!(team_id, run_id = %c.run_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Follow-up duplicate not closed");
        return false;
    }
    let ticket = s(item, "ticket_id");
    if !ticket.is_empty() {
        super::release_ticket_lock(state, team_id, &ticket).await;
    }
    let actor = common::merge_gate::Actor {
        by: "coderhelm".into(),
        is_login: false,
        via: common::merge_gate::VIA_DASHBOARD,
    };
    let prs = prs_of(item);
    let done = matches!(s(item, "pr_outcome").as_str(), "merged" | "closed");
    let installation = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|n| *n > 0);
    let installation = match installation {
        Some(i) => Some(i),
        None => super::review_reminder::team_installation_id(state, team_id).await,
    };
    let github = installation.and_then(|iid| {
        GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            iid,
            &state.http,
        )
        .ok()
    });
    for (owner, repo, number) in &prs {
        if let Err(e) = common::merge_gate::stop(
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
            owner,
            repo,
            *number,
            &actor,
            Utc::now().timestamp(),
        )
        .await
        {
            warn!(team_id, pr = number, error = %e, "Follow-up duplicate: auto-merge not stopped");
        }
        if done {
            continue;
        }
        if let Some(gh) = &github {
            let say = common::conversation::Say::status("closed", said.clone());
            if let Err(e) = super::conversation::github(gh, owner, repo, *number, say).await {
                warn!(team_id, pr = number, error = %e, "Follow-up duplicate: PR comment not posted");
            }
            if let Err(e) = gh.close_pull_request(owner, repo, *number).await {
                warn!(team_id, pr = number, error = %e, "Follow-up duplicate: PR not closed");
            }
        }
    }
    if s(item, "ticket_source") == "jira" && !ticket.is_empty() {
        let say = common::conversation::Say::status("run", said.clone());
        if let Err(e) =
            super::conversation::jira(state, team_id, &ticket, say, "status", "Closed").await
        {
            warn!(team_id, ticket = %ticket, error = %e, "Follow-up duplicate: Jira note not posted");
        }
    }
    log_close(state, team_id, root, c, &said).await;
    info!(team_id, root, duplicate = %c.dup, keep = %c.keep, run_id = %c.run_id, prs = prs.len(), "Follow-up duplicate closed by CoderHelm");
    true
}

async fn log_close(state: &WorkerState, team_id: &str, root: &str, c: &Close, said: &str) {
    let now = Utc::now().to_rfc3339();
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&format!("FOLLOWUP_CLOSE_LOG#{now}#{}", c.dup)))
        .item("source", attr_s(root))
        .item("duplicate", attr_s(&c.dup))
        .item("keep", attr_s(&c.keep))
        .item("topic", attr_s(&c.topic))
        .item("run_id", attr_s(&c.run_id))
        .item("said", attr_s(said))
        .item("closed_at", attr_s(&now))
        .send()
        .await
    {
        warn!(team_id, duplicate = %c.dup, error = %e, "Follow-up close not logged");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(a: &str, b: &str) -> (String, String) {
        (a.to_string(), b.to_string())
    }

    #[test]
    fn families_start_at_a_source_that_is_no_followup() {
        let f = families(&[
            p("ABC-1", "ABC-2"),
            p("ABC-1", "ABC-3"),
            p("ABC-2", "ABC-4"),
            p("ABC-9", "ABC-10"),
        ]);
        assert_eq!(f.len(), 2);
        assert_eq!(f["ABC-1"], vec!["ABC-2", "ABC-3", "ABC-4"]);
        assert_eq!(f["ABC-9"], vec!["ABC-10"]);
    }

    #[test]
    fn only_a_family_with_two_open_runs_is_read() {
        let c = |status: &str| Candidate {
            key: "ABC-1".into(),
            run_id: "01A".into(),
            status: status.into(),
            ..Default::default()
        };
        assert!(!worth_reading(&[c("running"), c("merged")]));
        assert!(!worth_reading(&[c("running")]));
        assert!(worth_reading(&[c("running"), c("awaiting_ci")]));
    }
}
