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
//!   only where the check reads the same work and the matcher finds the two
//!   near, the topic key filing uses (`followup_dedupe::agreed`), so a
//!   family filed before that key is reconciled on it too. What they
//!   disagree on, and a fix against a delete, is flagged on both tickets
//!   instead;
//! - each topic's keeper is decided once and recorded (`FOLLOWUP_KEEP#`,
//!   [`record_keeper`]) before anything closes, and every later sweep keeps
//!   the recorded one ([`kept_records`]); a closed member is never kept;
//! - right before a close the keeper is read again ([`keeper_now`]): one
//!   that went out, or whose pull request is no longer open or merged,
//!   closes nothing in its favour;
//! - a close takes a person's close steps: the status, the ticket lock,
//!   auto-merge stopped, the Jira note. Each open pull request is read
//!   first and closed only when it is CoderHelm's own, no person committed
//!   to it or asked for changes on it ([`fd::person_holds`]), and the
//!   team's agent permissions allow it (`close_own_pr`); it gets a short
//!   note naming the keeper's pull request, and its branch stays. Any of
//!   those failing leaves the run and its pull requests as they are. At
//!   most `followup_tickets::DAILY_CAP_PER_SOURCE` closes per source a day;
//! - every close is logged (`FOLLOWUP_CLOSE_LOG#`) and traced.

use common::run_ending::EndWrite;
use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::Utc;
use common::followup_dedupe::{self as fd, Candidate, Close, Kept};
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
    let outcome = s(&item, "pr_outcome");
    Some((
        Candidate {
            key: key.to_string(),
            title: s(&item, "title"),
            body: s(&item, "ticket_body"),
            run_id: latest.run_id.clone(),
            status: s(&item, "status"),
            has_pr: !prs.is_empty(),
            pr_open: !prs.is_empty() && !matches!(outcome.as_str(), "merged" | "closed"),
            pr_merged: outcome == "merged",
        },
        item,
    ))
}

/// The keeper decisions recorded for `root`'s family.
async fn kept_records(state: &WorkerState, team_id: &str, root: &str) -> Option<Vec<Kept>> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let page = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :s)")
            .expression_attribute_values(":p", attr_s(team_id))
            .expression_attribute_values(":s", attr_s(&fd::keep_record_prefix(root)))
            .consistent_read(true)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .ok()?;
        for i in page.items() {
            let members = i
                .get("members")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
                .unwrap_or_default();
            out.push(Kept {
                keep: s(i, "keep"),
                members,
                topic: s(i, "topic"),
                at: s(i, "decided_at"),
            });
        }
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Some(out)
}

/// Records `k` as decided, once (a record already there stands). False
/// when it could not be written: nothing closes in its favour then.
async fn record_keeper(state: &WorkerState, team_id: &str, root: &str, k: &Kept) -> bool {
    let res = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&fd::keep_record_sk(root, &k.keep)))
        .item("source", attr_s(root))
        .item("keep", attr_s(&k.keep))
        .item(
            "members",
            AttributeValue::L(k.members.iter().map(|m| attr_s(m)).collect()),
        )
        .item("topic", attr_s(&k.topic))
        .item("decided_at", attr_s(&Utc::now().to_rfc3339()))
        .condition_expression("attribute_not_exists(sk)")
        .send()
        .await;
    match res {
        Ok(_) => true,
        Err(e)
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
        {
            true
        }
        Err(e) => {
            warn!(team_id, root, keep = %k.keep, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Follow-up keeper not recorded");
            false
        }
    }
}

/// The GitHub client for a run's installation.
async fn github_for(state: &WorkerState, team_id: &str, item: &Item) -> Option<GitHubClient> {
    let installation = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|n| *n > 0);
    let installation = match installation {
        Some(i) => Some(i),
        None => super::review_reminder::team_installation_id(state, team_id).await,
    };
    installation.and_then(|iid| {
        GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            iid,
            &state.http,
        )
        .ok()
    })
}

/// The keeper `keep`, read again right before a close in its favour: its
/// pull request (`owner/repo#number`, empty when it has none) when it is
/// still in the running and, for a duplicate with an open pull request
/// (`dup_has_open_pr`), its own pull request is open or merged on GitHub
/// now. `None` closes nothing.
async fn keeper_now(
    state: &WorkerState,
    team_id: &str,
    keep: &str,
    dup_has_open_pr: bool,
) -> Option<String> {
    let (k, item) = candidate(state, team_id, keep).await?;
    if k.out() {
        return None;
    }
    let pr = prs_of(&item).into_iter().next();
    let label = pr
        .as_ref()
        .map(|(o, r, n)| format!("{o}/{r}#{n}"))
        .unwrap_or_default();
    if !dup_has_open_pr {
        return Some(label);
    }
    let (owner, repo, number) = pr?;
    let gh = github_for(state, team_id, &item).await?;
    let p = gh.get_pull_request(&owner, &repo, number).await.ok()?;
    let live = p["state"].as_str() == Some("open") || p["merged"].as_bool() == Some(true);
    live.then_some(label)
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
        let Some(kept) = kept_records(state, team_id, &root).await else {
            // The recorded keepers can't be read: nothing is decided.
            warn!(team_id, root = %root, "Follow-up duplicates: keeper records not read");
            continue;
        };
        report.families += 1;
        let plan = fd::decide(&cands, Some(judge), &kept).await;
        let mut unrecorded: Vec<String> = Vec::new();
        for k in &plan.record {
            if !record_keeper(state, team_id, &root, k).await {
                unrecorded.push(k.keep.clone());
            }
        }
        for c in &plan.close {
            if unrecorded.contains(&c.keep) {
                continue;
            }
            let Some(item) = items.get(&c.dup) else {
                continue;
            };
            let dup_open = cands.iter().any(|x| x.key == c.dup && x.pr_open);
            let Some(keeper_pr) = keeper_now(state, team_id, &c.keep, dup_open).await else {
                info!(team_id, root = %root, duplicate = %c.dup, keep = %c.keep, "Follow-up duplicate left open: the keeper no longer carries the work");
                continue;
            };
            if !take_close_slot(state, team_id, &root).await {
                warn!(team_id, root = %root, "Follow-up duplicates: today's closes for the source are used up");
                break;
            }
            if close_duplicate(state, &judge.provider, team_id, &root, c, item, &keeper_pr).await {
                report.closed += 1;
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

/// One open pull request of a duplicate, read before it is closed: `Ok`
/// with it when it is CoderHelm's own, still open, and no person holds it;
/// `Ok(None)` when it is no longer open; `Err` (why) when it must stay.
async fn closable_pr(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
) -> Result<Option<super::ticket_history::EarlierPr>, String> {
    let p = gh
        .get_pull_request(owner, repo, number)
        .await
        .map_err(|e| format!("pull request #{number} not read: {e}"))?;
    if p["state"].as_str() != Some("open") {
        return Ok(None);
    }
    let author = p["user"]["login"].as_str().unwrap_or("");
    if !super::is_own_login(author) {
        return Err(format!("pull request #{number} is not CoderHelm's"));
    }
    let commits = gh
        .list_pr_commits_all(owner, repo, number)
        .await
        .map_err(|e| format!("commits of #{number} not read: {e}"))?;
    let reviews = gh
        .list_pr_reviews(owner, repo, number)
        .await
        .map_err(|e| format!("reviews of #{number} not read: {e}"))?;
    let reviews = reviews.as_array().cloned().unwrap_or_default();
    if fd::person_holds(&commits, &reviews) {
        return Err(format!(
            "a person committed to #{number} or asked for changes on it"
        ));
    }
    Ok(Some(super::ticket_history::EarlierPr {
        repo: format!("{owner}/{repo}"),
        number,
        title: p["title"].as_str().unwrap_or("").to_string(),
        author: author.to_string(),
        branch: p["head"]["ref"].as_str().unwrap_or("").to_string(),
        base: p["base"]["ref"].as_str().unwrap_or("").to_string(),
        state: "open".into(),
        for_ticket: true,
        ..Default::default()
    }))
}

/// Closes one duplicate's run with a person's close steps, its open pull
/// requests through the agent-permission gate with a note naming
/// `keeper_pr`. Every pull request is read and allowed before anything is
/// written, so a person's commit or change request, a refusal, or a read
/// that fails leaves the run and its pull requests as they are. True when
/// the run's status was written.
async fn close_duplicate(
    state: &WorkerState,
    provider: &ModelProvider,
    team_id: &str,
    root: &str,
    c: &Close,
    item: &Item,
    keeper_pr: &str,
) -> bool {
    let said = fd::close_comment(&c.keep, &c.topic);
    let from = s(item, "status");
    if !matches!(
        common::run_status::close_check(&from),
        common::run_status::CloseCheck::Close
    ) {
        return false;
    }
    let prs = prs_of(item);
    let done = matches!(s(item, "pr_outcome").as_str(), "merged" | "closed");
    let github = github_for(state, team_id, item).await;
    let mut to_close = Vec::new();
    if !done && !prs.is_empty() {
        let Some(gh) = &github else {
            warn!(team_id, duplicate = %c.dup, "Follow-up duplicate left open: no GitHub access to read its pull request");
            return false;
        };
        for (owner, repo, number) in &prs {
            match closable_pr(gh, owner, repo, *number).await {
                Ok(Some(p)) => to_close.push(p),
                Ok(None) => {}
                Err(why) => {
                    info!(team_id, duplicate = %c.dup, why = %why, "Follow-up duplicate left open");
                    return false;
                }
            }
        }
        for p in &to_close {
            if let Err(why) = super::may_close_own_pr(state, provider, &p.repo, p.number).await {
                info!(team_id, duplicate = %c.dup, pr = p.number, why = %why, "Follow-up duplicate left open: the agent permissions didn't allow the close");
                return false;
            }
        }
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
    }
    if let Some(gh) = &github {
        for p in &to_close {
            let note = fd::pr_close_note(&c.keep, &c.topic, keeper_pr, &p.branch);
            if let Err(why) = super::close_own_pr(state, provider, gh, p, &note).await {
                warn!(team_id, pr = p.number, why = %why, "Follow-up duplicate: PR left open");
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
    info!(team_id, root, duplicate = %c.dup, keep = %c.keep, run_id = %c.run_id, prs = to_close.len(), "Follow-up duplicate closed by CoderHelm");
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
