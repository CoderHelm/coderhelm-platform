//! Reading and writing reward records on the settings table.
//!
//! Every record is under the team's own partition key; no function here takes
//! more than one team, so one team's outcomes cannot shape another's agents.
//!
//! Records:
//! * `REWARD#{at}#{hash}`: one event ([`super::event`]).
//! * `REWARDCAT#{repo}#{category}`: finding outcome counters for calibration.
//! * `REWARDARM#{agent}#{arm}`: runs per prompt variant, and per-signal
//!   counts and shaped sums of their outcomes (so the reward follows the
//!   team's current weights).
//! * `REWARDEX#{repo}#{id}`: examples ([`super::exemplars`]).
//! * `REWARDLESSON#{repo}#{id}`: lessons ([`super::lessons`]).
//! * `LEARNCHANGE#{id}` / `LEARNSTATE#...`: the change log ([`super::changes`]).
//! * `LEARNPIN#{agent}`: an admin's pinned variant.
//! * `SETTINGS#REWARD_WEIGHTS`: the team's weight overrides.

use super::bandit::ArmStats;
use super::calibration::{self, CategoryState, Counts};
use super::changes::{self, Change, State};
use super::event::{RewardEvent, RewardKind};
use super::exemplars::{self, Exemplar};
use super::lessons::Lesson;
use super::score::{self, Weights};
use aws_sdk_dynamodb::error::DisplayErrorContext;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use aws_sdk_dynamodb::Client;
use std::collections::HashMap;
use tracing::{info, warn};

pub const CAT_PREFIX: &str = "REWARDCAT#";
pub const ARM_PREFIX: &str = "REWARDARM#";
pub const LESSON_PREFIX: &str = "REWARDLESSON#";
pub const PIN_PREFIX: &str = "LEARNPIN#";
pub const WEIGHTS_SK: &str = "SETTINGS#REWARD_WEIGHTS";
/// Evidence lines kept on a category counter.
const RECENT_KEPT: usize = 6;
/// Most items one listing reads.
const MAX_ITEMS: usize = 20_000;

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
fn gf(i: &Item, k: &str) -> f64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}
fn gu(i: &Item, k: &str) -> u64 {
    gf(i, k).max(0.0) as u64
}

pub fn cat_sk(repo: &str, category: &str) -> String {
    format!("{CAT_PREFIX}{repo}#{category}")
}
pub fn arm_sk(agent: &str, arm: &str) -> String {
    format!("{ARM_PREFIX}{agent}#{arm}")
}
pub fn lesson_sk(repo: &str, id: &str) -> String {
    format!("{LESSON_PREFIX}{repo}#{id}")
}
pub fn pin_sk(agent: &str) -> String {
    format!("{PIN_PREFIX}{agent}")
}

/// The reward store of one table.
#[derive(Clone)]
pub struct Store {
    pub dynamo: Client,
    pub table: String,
}

impl Store {
    pub fn new(dynamo: &Client, table: &str) -> Self {
        Self {
            dynamo: dynamo.clone(),
            table: table.to_string(),
        }
    }

    /// Every item of `team_id` whose key starts with `prefix` (up to a cap),
    /// optionally from `from_sk` on.
    pub async fn query_prefix(
        &self,
        team_id: &str,
        prefix: &str,
        from_sk: Option<&str>,
    ) -> Result<Vec<Item>, String> {
        let mut out = vec![];
        let mut start: Option<Item> = None;
        loop {
            let mut q = self.dynamo.query().table_name(&self.table);
            q = match from_sk {
                Some(from) => q
                    .key_condition_expression("pk = :pk AND sk BETWEEN :from AND :to")
                    .expression_attribute_values(":from", s(from))
                    .expression_attribute_values(":to", s(&format!("{prefix}\u{10FFFF}"))),
                None => q
                    .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                    .expression_attribute_values(":p", s(prefix)),
            };
            let page = q
                .expression_attribute_values(":pk", s(team_id))
                .set_exclusive_start_key(start.take())
                .send()
                .await
                .map_err(|e| DisplayErrorContext(e).to_string())?;
            out.extend(page.items.unwrap_or_default());
            match page.last_evaluated_key {
                Some(k) if !k.is_empty() && out.len() < MAX_ITEMS => start = Some(k),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn get(&self, team_id: &str, sk: &str) -> Result<Option<Item>, String> {
        self.dynamo
            .get_item()
            .table_name(&self.table)
            .key("pk", s(team_id))
            .key("sk", s(sk))
            .send()
            .await
            .map(|r| r.item)
            .map_err(|e| DisplayErrorContext(e).to_string())
    }

    // ── Events ─────────────────────────────────────────────────────────

    /// Records one event and updates the counters it feeds. Returns false
    /// when the same signal was already recorded (a redelivered webhook).
    /// Best-effort callers log the error and carry on.
    pub async fn record(&self, e: &RewardEvent) -> Result<bool, String> {
        if e.team_id.is_empty() || e.kind.is_none() || e.at.is_empty() {
            return Err("an event needs a team, a kind and a time".into());
        }
        // One marker per signal, whatever time the source gives it: a
        // redelivered webhook or a second click is refused here.
        let ttl = chrono::Utc::now().timestamp() + super::event::KEEP_DAYS * 86_400;
        let claim = self
            .dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(&e.team_id))
            .item("sk", s(&super::event::seen_sk(&e.dedup)))
            .item("record_type", s("reward_seen"))
            .item("ttl", n(ttl))
            .condition_expression("attribute_not_exists(sk)")
            .send()
            .await;
        if let Err(err) = claim {
            let msg = DisplayErrorContext(&err).to_string();
            if msg.contains("ConditionalCheckFailed") {
                return Ok(false);
            }
            return Err(msg);
        }
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(e.to_item()))
            .send()
            .await
            .map_err(|err| DisplayErrorContext(err).to_string())?;
        let kind = e.kind.unwrap_or(RewardKind::RereviewRequested);
        if kind.is_finding() && !e.repo.is_empty() && !e.category.is_empty() {
            if let Err(err) = self.bump_category(e).await {
                warn!(error = %err, "Could not count the finding outcome");
            }
        }
        if !e.variant.is_empty() {
            if let Err(err) = self.bump_arm_outcome(e).await {
                warn!(error = %err, "Could not count the variant's outcome");
            }
        }
        Ok(true)
    }

    /// Records each event; returns how many were new.
    pub async fn record_all(&self, events: &[RewardEvent]) -> usize {
        let mut fresh = 0;
        for e in events {
            match self.record(e).await {
                Ok(true) => fresh += 1,
                Ok(false) => {}
                Err(err) => warn!(kind = ?e.kind, error = %err, "Could not record a reward event"),
            }
        }
        fresh
    }

    /// One team's events from `since` (RFC 3339) on.
    pub async fn events_since(
        &self,
        team_id: &str,
        since: &str,
    ) -> Result<Vec<RewardEvent>, String> {
        let from = format!("{}{since}", super::event::EVENT_PREFIX);
        let items = self
            .query_prefix(team_id, super::event::EVENT_PREFIX, Some(&from))
            .await?;
        Ok(items.iter().filter_map(RewardEvent::from_item).collect())
    }

    // ── Calibration ────────────────────────────────────────────────────

    async fn bump_category(&self, e: &RewardEvent) -> Result<(), String> {
        let field = match e.kind {
            Some(RewardKind::FindingAccepted) => "accepted",
            Some(RewardKind::FindingDismissed) => "dismissed",
            _ => "ignored",
        };
        let line = serde_json::json!({
            "kind": e.kind.map(RewardKind::key).unwrap_or(""),
            "at": e.at,
            "pr": e.pr,
            "run_id": e.run_id,
            "detail": e.detail,
        })
        .to_string();
        let out = self
            .dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", s(&e.team_id))
            .key("sk", s(&cat_sk(&e.repo, &e.category)))
            .update_expression(
                "ADD #f :one SET #rt = :rt, #repo = :r, #cat = :c, \
                 #recent = list_append(if_not_exists(#recent, :empty), :line), #ua = :at",
            )
            .expression_attribute_names("#f", field)
            .expression_attribute_names("#rt", "record_type")
            .expression_attribute_names("#repo", "repo")
            .expression_attribute_names("#cat", "category")
            .expression_attribute_names("#recent", "recent")
            .expression_attribute_names("#ua", "updated_at")
            .expression_attribute_values(":one", n(1))
            .expression_attribute_values(":rt", s("reward_category"))
            .expression_attribute_values(":r", s(&e.repo))
            .expression_attribute_values(":c", s(&e.category))
            .expression_attribute_values(":empty", AttributeValue::L(vec![]))
            .expression_attribute_values(":line", AttributeValue::L(vec![s(&line)]))
            .expression_attribute_values(":at", s(&e.at))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .map_err(|err| DisplayErrorContext(err).to_string())?;
        let item = out.attributes.unwrap_or_default();
        let recent_len = item
            .get("recent")
            .and_then(|v| v.as_l().ok())
            .map(|l| l.len())
            .unwrap_or(0);
        if recent_len > RECENT_KEPT {
            let drop: Vec<String> = (0..recent_len - RECENT_KEPT)
                .map(|i| format!("#recent[{i}]"))
                .collect();
            let _ = self
                .dynamo
                .update_item()
                .table_name(&self.table)
                .key("pk", s(&e.team_id))
                .key("sk", s(&cat_sk(&e.repo, &e.category)))
                .update_expression(format!("REMOVE {}", drop.join(", ")))
                .expression_attribute_names("#recent", "recent")
                .send()
                .await;
        }
        let counts = counts_of(&item);
        let decided = calibration::decide(&e.category, &counts, calibration::MIN_SAMPLES);
        let state_key = changes::state_sk(&e.repo, changes::KIND_CALIBRATION, &e.category);
        let state = self
            .get(&e.team_id, &state_key)
            .await?
            .map(|i| State::from_item(&i));
        if changes::is_change(state.as_ref(), decided.key()) {
            let evidence = recent_of(&item);
            let change = Change {
                id: ulid::Ulid::new().to_string(),
                repo: e.repo.clone(),
                kind: changes::KIND_CALIBRATION.into(),
                key: e.category.clone(),
                from: state
                    .as_ref()
                    .map(|s| s.current.clone())
                    .unwrap_or_else(|| "none".into()),
                to: decided.key().into(),
                reason: calibration::reason(&e.category, &counts, decided),
                evidence,
                at: chrono::Utc::now().to_rfc3339(),
                ..Default::default()
            };
            self.apply_change(&e.team_id, &change).await?;
            info!(repo = %e.repo, category = %e.category, to = decided.key(), "Reviewer calibration changed");
        }
        Ok(())
    }

    /// Writes a change and makes it the applied state for its key.
    pub async fn apply_change(&self, team_id: &str, c: &Change) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(c.to_item(team_id)))
            .send()
            .await
            .map_err(|e| DisplayErrorContext(e).to_string())?;
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team_id))
            .item("sk", s(&changes::state_sk(&c.repo, &c.kind, &c.key)))
            .item("record_type", s("learning_state"))
            .item("current", s(&c.to))
            .item("change_id", s(&c.id))
            .item("reverted", AttributeValue::Bool(false))
            .send()
            .await
            .map_err(|e| DisplayErrorContext(e).to_string())?;
        Ok(())
    }

    /// A repository's calibration: counts, decision and whether reverted.
    pub async fn categories(
        &self,
        team_id: &str,
        repo: &str,
    ) -> Result<Vec<CategoryState>, String> {
        let cats = self
            .query_prefix(team_id, &format!("{CAT_PREFIX}{repo}#"), None)
            .await?;
        let states = self
            .query_prefix(
                team_id,
                &format!(
                    "{}{repo}#{}#",
                    changes::STATE_PREFIX,
                    changes::KIND_CALIBRATION
                ),
                None,
            )
            .await?;
        Ok(category_states(&cats, &states))
    }

    /// Every repository's calibration (for the dashboard).
    pub async fn all_categories(
        &self,
        team_id: &str,
    ) -> Result<Vec<(String, CategoryState)>, String> {
        let cats = self.query_prefix(team_id, CAT_PREFIX, None).await?;
        let states = self
            .query_prefix(team_id, changes::STATE_PREFIX, None)
            .await?;
        let mut out = vec![];
        let mut by_repo: HashMap<String, Vec<Item>> = HashMap::new();
        for c in cats {
            by_repo.entry(gs(&c, "repo")).or_default().push(c);
        }
        for (repo, items) in by_repo {
            let st: Vec<Item> = states
                .iter()
                .filter(|i| {
                    gs(i, "sk").starts_with(&format!(
                        "{}{repo}#{}#",
                        changes::STATE_PREFIX,
                        changes::KIND_CALIBRATION
                    ))
                })
                .cloned()
                .collect();
            for c in category_states(&items, &st) {
                out.push((repo.clone(), c));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.category.cmp(&b.1.category)));
        Ok(out)
    }

    // ── Variants ───────────────────────────────────────────────────────

    /// Counts one run on a variant.
    pub async fn count_pull(&self, team_id: &str, agent: &str, arm: &str) {
        let r = self
            .dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", s(team_id))
            .key("sk", s(&arm_sk(agent, arm)))
            .update_expression("ADD #p :one SET #rt = :rt, #ag = :a, #arm = :k")
            .expression_attribute_names("#p", "pulls")
            .expression_attribute_names("#rt", "record_type")
            .expression_attribute_names("#ag", "agent")
            .expression_attribute_names("#arm", "arm")
            .expression_attribute_values(":one", n(1))
            .expression_attribute_values(":rt", s("reward_arm"))
            .expression_attribute_values(":a", s(agent))
            .expression_attribute_values(":k", s(arm))
            .send()
            .await;
        if let Err(e) = r {
            warn!(error = %DisplayErrorContext(e), "Could not count the variant's run");
        }
    }

    async fn bump_arm_outcome(&self, e: &RewardEvent) -> Result<(), String> {
        let Some(kind) = e.kind else { return Ok(()) };
        self.dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", s(&e.team_id))
            .key("sk", s(&arm_sk(&e.agent, &e.variant)))
            .update_expression("ADD #n :one, #s :shape SET #rt = :rt, #ag = :a, #arm = :v")
            .expression_attribute_names("#n", format!("n_{}", kind.key()))
            .expression_attribute_names("#s", format!("s_{}", kind.key()))
            .expression_attribute_names("#rt", "record_type")
            .expression_attribute_names("#ag", "agent")
            .expression_attribute_names("#arm", "arm")
            .expression_attribute_values(":one", n(1))
            .expression_attribute_values(":shape", n(score::shape(kind, e.value)))
            .expression_attribute_values(":rt", s("reward_arm"))
            .expression_attribute_values(":a", s(&e.agent))
            .expression_attribute_values(":v", s(&e.variant))
            .send()
            .await
            .map(|_| ())
            .map_err(|err| DisplayErrorContext(err).to_string())
    }

    /// Each variant's runs and reward under the team's current weights.
    pub async fn arm_stats(
        &self,
        team_id: &str,
        agent: &str,
        w: &Weights,
    ) -> Result<Vec<ArmStats>, String> {
        let items = self
            .query_prefix(team_id, &format!("{ARM_PREFIX}{agent}#"), None)
            .await?;
        Ok(items.iter().map(|i| arm_stats_of(i, w)).collect())
    }

    pub async fn pin(&self, team_id: &str, agent: &str) -> Option<String> {
        self.get(team_id, &pin_sk(agent))
            .await
            .ok()
            .flatten()
            .map(|i| gs(&i, "arm"))
            .filter(|a| !a.is_empty())
    }

    /// Pins (or with None unpins) a variant, and logs it.
    pub async fn set_pin(
        &self,
        team_id: &str,
        agent: &str,
        arm: Option<&str>,
        by: &str,
    ) -> Result<(), String> {
        let before = self.pin(team_id, agent).await.unwrap_or_default();
        match arm {
            Some(a) => {
                self.dynamo
                    .put_item()
                    .table_name(&self.table)
                    .item("pk", s(team_id))
                    .item("sk", s(&pin_sk(agent)))
                    .item("record_type", s("learning_pin"))
                    .item("arm", s(a))
                    .item("by", s(by))
                    .item("at", s(&chrono::Utc::now().to_rfc3339()))
                    .send()
                    .await
                    .map_err(|e| DisplayErrorContext(e).to_string())?;
            }
            None => {
                self.dynamo
                    .delete_item()
                    .table_name(&self.table)
                    .key("pk", s(team_id))
                    .key("sk", s(&pin_sk(agent)))
                    .send()
                    .await
                    .map_err(|e| DisplayErrorContext(e).to_string())?;
            }
        }
        let to = arm.unwrap_or("");
        if before != to {
            let change = Change {
                id: ulid::Ulid::new().to_string(),
                repo: changes::TEAM_REPO.into(),
                kind: changes::KIND_VARIANT.into(),
                key: format!("{agent}:pin"),
                from: before,
                to: to.to_string(),
                reason: if to.is_empty() {
                    format!("{by} unpinned the {agent}'s prompt variant")
                } else {
                    format!("{by} pinned the {agent}'s prompt variant to `{to}`")
                },
                at: chrono::Utc::now().to_rfc3339(),
                ..Default::default()
            };
            self.apply_change(team_id, &change).await?;
        }
        Ok(())
    }

    // ── Weights ────────────────────────────────────────────────────────

    pub async fn weights(&self, team_id: &str) -> Weights {
        let Ok(Some(item)) = self.get(team_id, WEIGHTS_SK).await else {
            return Weights::defaults();
        };
        let raw: HashMap<String, f64> = item
            .get("weights")
            .and_then(|v| v.as_m().ok())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_n().ok()?.parse().ok()?)))
                    .collect()
            })
            .unwrap_or_default();
        Weights::from_overrides(&raw)
    }

    pub async fn put_weights(&self, team_id: &str, w: &Weights, by: &str) -> Result<(), String> {
        let m: HashMap<String, AttributeValue> = w
            .overrides()
            .iter()
            .map(|(k, v)| (k.clone(), n(v)))
            .collect();
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team_id))
            .item("sk", s(WEIGHTS_SK))
            .item("record_type", s("reward_weights"))
            .item("weights", AttributeValue::M(m))
            .item("by", s(by))
            .item("at", s(&chrono::Utc::now().to_rfc3339()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| DisplayErrorContext(e).to_string())
    }

    // ── Change log ─────────────────────────────────────────────────────

    /// The team's changes, newest first.
    pub async fn changes(&self, team_id: &str) -> Result<Vec<Change>, String> {
        let items = self
            .query_prefix(team_id, changes::CHANGE_PREFIX, None)
            .await?;
        let mut v: Vec<Change> = items.iter().filter_map(Change::from_item).collect();
        v.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(v)
    }

    pub async fn states(&self, team_id: &str) -> Result<Vec<(String, State)>, String> {
        let items = self
            .query_prefix(team_id, changes::STATE_PREFIX, None)
            .await?;
        Ok(items
            .iter()
            .map(|i| (gs(i, "sk"), State::from_item(i)))
            .collect())
    }

    /// Turns a change off. A calibration or lesson stays off until restored;
    /// reverting a variant change unpins (or pins back what was there).
    /// Returns false when there is no such change.
    pub async fn revert(&self, team_id: &str, id: &str, by: &str) -> Result<bool, String> {
        self.set_reverted(team_id, id, by, true).await
    }

    pub async fn restore(&self, team_id: &str, id: &str, by: &str) -> Result<bool, String> {
        self.set_reverted(team_id, id, by, false).await
    }

    async fn set_reverted(
        &self,
        team_id: &str,
        id: &str,
        by: &str,
        on: bool,
    ) -> Result<bool, String> {
        let Some(item) = self.get(team_id, &changes::change_sk(id)).await? else {
            return Ok(false);
        };
        let Some(c) = Change::from_item(&item) else {
            return Ok(false);
        };
        let now = chrono::Utc::now().to_rfc3339();
        self.dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", s(team_id))
            .key("sk", s(&changes::change_sk(id)))
            .update_expression("SET #r = :r, #rb = :b, #ra = :t")
            .expression_attribute_names("#r", "reverted")
            .expression_attribute_names("#rb", "reverted_by")
            .expression_attribute_names("#ra", "reverted_at")
            .expression_attribute_values(":r", AttributeValue::Bool(on))
            .expression_attribute_values(":b", s(if on { by } else { "" }))
            .expression_attribute_values(":t", s(if on { &now } else { "" }))
            .send()
            .await
            .map_err(|e| DisplayErrorContext(e).to_string())?;
        match c.kind.as_str() {
            changes::KIND_LESSON => {
                let _ = self
                    .dynamo
                    .update_item()
                    .table_name(&self.table)
                    .key("pk", s(team_id))
                    .key("sk", s(&lesson_sk(&c.repo, &c.key)))
                    .condition_expression("attribute_exists(sk)")
                    .update_expression("SET #r = :r")
                    .expression_attribute_names("#r", "reverted")
                    .expression_attribute_values(":r", AttributeValue::Bool(on))
                    .send()
                    .await;
            }
            changes::KIND_VARIANT => {
                if let Some(agent) = c.key.split(':').next() {
                    // Reverting a pin puts back what was there before it.
                    let target = if on { c.from.as_str() } else { c.to.as_str() };
                    let pin = (!target.is_empty()).then_some(target);
                    self.set_pin_quiet(team_id, agent, pin).await?;
                }
            }
            _ => {
                self.dynamo
                    .update_item()
                    .table_name(&self.table)
                    .key("pk", s(team_id))
                    .key("sk", s(&changes::state_sk(&c.repo, &c.kind, &c.key)))
                    .update_expression("SET #r = :r")
                    .expression_attribute_names("#r", "reverted")
                    .expression_attribute_values(":r", AttributeValue::Bool(on))
                    .send()
                    .await
                    .map_err(|e| DisplayErrorContext(e).to_string())?;
            }
        }
        info!(
            team_id,
            change = id,
            reverted = on,
            by,
            "Learning change reverted or restored"
        );
        Ok(true)
    }

    async fn set_pin_quiet(
        &self,
        team_id: &str,
        agent: &str,
        arm: Option<&str>,
    ) -> Result<(), String> {
        let r = match arm {
            Some(a) => self
                .dynamo
                .put_item()
                .table_name(&self.table)
                .item("pk", s(team_id))
                .item("sk", s(&pin_sk(agent)))
                .item("record_type", s("learning_pin"))
                .item("arm", s(a))
                .send()
                .await
                .map(|_| ())
                .map_err(|e| DisplayErrorContext(e).to_string()),
            None => self
                .dynamo
                .delete_item()
                .table_name(&self.table)
                .key("pk", s(team_id))
                .key("sk", s(&pin_sk(agent)))
                .send()
                .await
                .map(|_| ())
                .map_err(|e| DisplayErrorContext(e).to_string()),
        };
        r
    }

    /// Reverts every active change of `repo`. Returns how many.
    pub async fn revert_repo(&self, team_id: &str, repo: &str, by: &str) -> Result<usize, String> {
        let all = self.changes(team_id).await?;
        let states = self.states(team_id).await?;
        let ids: Vec<String> = changes::active_for_repo(&all, &states, repo)
            .into_iter()
            .map(|c| c.id.clone())
            .collect();
        let mut n = 0;
        for id in ids {
            if self.revert(team_id, &id, by).await? {
                n += 1;
            }
        }
        Ok(n)
    }

    // ── Exemplars ──────────────────────────────────────────────────────

    /// Adds an outcome's score to an example, creating it on first sight
    /// (with `embedding`, made by the caller and metered).
    pub async fn add_to_exemplar(
        &self,
        team_id: &str,
        ex: &Exemplar,
        score: f64,
    ) -> Result<(), String> {
        let mut req = self
            .dynamo
            .update_item()
            .table_name(&self.table)
            .key("pk", s(team_id))
            .key("sk", s(&exemplars::sk(&ex.repo, &ex.id)))
            .expression_attribute_values(":sc", n(score))
            .expression_attribute_values(":one", n(1))
            .expression_attribute_values(":rt", s("reward_exemplar"))
            .expression_attribute_values(":id", s(&ex.id))
            .expression_attribute_values(":repo", s(&ex.repo))
            .expression_attribute_values(":kind", s(&ex.kind))
            .expression_attribute_values(":cat", s(&ex.category))
            .expression_attribute_values(":area", s(&ex.area))
            .expression_attribute_values(
                ":text",
                s(crate::truncate_str(&ex.text, exemplars::MAX_TEXT)),
            )
            .expression_attribute_values(":pr", n(ex.pr))
            .expression_attribute_values(":at", s(&chrono::Utc::now().to_rfc3339()));
        let mut expr = "ADD #reward :sc, #events :one SET #rt = :rt, #id = :id, #repo = :repo, \
                        #kind = :kind, #cat = if_not_exists(#cat, :cat), \
                        #area = if_not_exists(#area, :area), #t = if_not_exists(#t, :text), \
                        #pr = if_not_exists(#pr, :pr), #ca = if_not_exists(#ca, :at)"
            .to_string();
        for (alias, name) in [
            ("#reward", "reward"),
            ("#events", "events"),
            ("#rt", "record_type"),
            ("#id", "id"),
            ("#repo", "repo"),
            ("#kind", "kind"),
            ("#cat", "category"),
            ("#area", "area"),
            ("#t", "text"),
            ("#pr", "pr"),
            ("#ca", "created_at"),
        ] {
            req = req.expression_attribute_names(alias, name);
        }
        if !ex.embedding.is_empty() {
            expr.push_str(", #emb = if_not_exists(#emb, :emb)");
            req = req.expression_attribute_names("#emb", "embedding");
            req = req.expression_attribute_values(
                ":emb",
                AttributeValue::B(aws_smithy_types::Blob::new(exemplars::pack(&ex.embedding))),
            );
        }
        req.update_expression(expr)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| DisplayErrorContext(e).to_string())
    }

    /// Does the example exist yet (so the caller embeds only new ones)?
    pub async fn exemplar_exists(&self, team_id: &str, repo: &str, id: &str) -> bool {
        matches!(
            self.get(team_id, &exemplars::sk(repo, id)).await,
            Ok(Some(_))
        )
    }

    pub async fn exemplars(
        &self,
        team_id: &str,
        repo: Option<&str>,
    ) -> Result<Vec<Exemplar>, String> {
        let prefix = match repo {
            Some(r) => exemplars::repo_prefix(r),
            None => exemplars::PREFIX.to_string(),
        };
        let items = self.query_prefix(team_id, &prefix, None).await?;
        Ok(items.iter().map(exemplar_of).collect())
    }

    pub async fn count_exemplar_use(&self, team_id: &str, ex: &[Exemplar]) {
        for e in ex {
            let _ = self
                .dynamo
                .update_item()
                .table_name(&self.table)
                .key("pk", s(team_id))
                .key("sk", s(&exemplars::sk(&e.repo, &e.id)))
                .condition_expression("attribute_exists(sk)")
                .update_expression("ADD #u :one")
                .expression_attribute_names("#u", "uses")
                .expression_attribute_values(":one", n(1))
                .send()
                .await;
        }
    }

    // ── Lessons ────────────────────────────────────────────────────────

    pub async fn lessons(&self, team_id: &str, repo: Option<&str>) -> Result<Vec<Lesson>, String> {
        let prefix = match repo {
            Some(r) => format!("{LESSON_PREFIX}{r}#"),
            None => LESSON_PREFIX.to_string(),
        };
        let items = self.query_prefix(team_id, &prefix, None).await?;
        Ok(items.iter().map(lesson_of).collect())
    }

    /// Writes a lesson the evidence backs, or reinforces the one it repeats,
    /// and logs a new one as a change. Returns the lesson's id, or None when
    /// the evidence does not back it.
    pub async fn learn(
        &self,
        team_id: &str,
        repo: &str,
        agent: &str,
        text: &str,
        evidence: &[RewardEvent],
    ) -> Result<Option<String>, String> {
        let kept = self.lessons(team_id, Some(repo)).await?;
        let now = chrono::Utc::now().to_rfc3339();
        let lines: Vec<String> = evidence
            .iter()
            .map(|e| {
                format!(
                    "{} PR #{}: {}",
                    e.kind.map(RewardKind::label).unwrap_or(""),
                    e.pr,
                    e.detail
                )
            })
            .collect();
        match super::lessons::plan(text, evidence, &kept) {
            super::lessons::Plan::Skip => Ok(None),
            super::lessons::Plan::Reinforce { id } => {
                self.dynamo
                    .update_item()
                    .table_name(&self.table)
                    .key("pk", s(team_id))
                    .key("sk", s(&lesson_sk(repo, &id)))
                    .update_expression("ADD #seen :one SET #ls = :t")
                    .expression_attribute_names("#seen", "seen")
                    .expression_attribute_names("#ls", "last_seen_at")
                    .expression_attribute_values(":one", n(1))
                    .expression_attribute_values(":t", s(&now))
                    .send()
                    .await
                    .map_err(|e| DisplayErrorContext(e).to_string())?;
                Ok(Some(id))
            }
            super::lessons::Plan::Write => {
                if kept.len() >= super::lessons::MAX_KEPT {
                    // Drop the lightest lesson to make room.
                    let now_t = chrono::Utc::now();
                    if let Some(light) = kept.iter().min_by(|a, b| {
                        super::lessons::weight(a.seen, &a.last_seen_at, now_t)
                            .partial_cmp(&super::lessons::weight(b.seen, &b.last_seen_at, now_t))
                            .unwrap_or(std::cmp::Ordering::Equal)
                    }) {
                        let _ = self
                            .dynamo
                            .delete_item()
                            .table_name(&self.table)
                            .key("pk", s(team_id))
                            .key("sk", s(&lesson_sk(repo, &light.id)))
                            .send()
                            .await;
                    }
                }
                let id = ulid::Ulid::new().to_string();
                let seen = evidence
                    .iter()
                    .map(|e| format!("{}#{}", e.repo, e.pr))
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    .max(1);
                self.dynamo
                    .put_item()
                    .table_name(&self.table)
                    .item("pk", s(team_id))
                    .item("sk", s(&lesson_sk(repo, &id)))
                    .item("record_type", s("reward_lesson"))
                    .item("id", s(&id))
                    .item("repo", s(repo))
                    .item("agent", s(agent))
                    .item("text", s(text))
                    .item("seen", n(seen))
                    .item("created_at", s(&now))
                    .item("last_seen_at", s(&now))
                    .item(
                        "evidence",
                        AttributeValue::L(lines.iter().map(|l| s(l)).collect()),
                    )
                    .item("reverted", AttributeValue::Bool(false))
                    .send()
                    .await
                    .map_err(|e| DisplayErrorContext(e).to_string())?;
                let change = Change {
                    id: ulid::Ulid::new().to_string(),
                    repo: repo.to_string(),
                    kind: changes::KIND_LESSON.into(),
                    key: id.clone(),
                    from: String::new(),
                    to: crate::truncate_str(text, 300).to_string(),
                    reason: format!("New {agent} lesson backed by {seen} outcomes"),
                    evidence: evidence
                        .iter()
                        .map(|e| {
                            serde_json::json!({
                                "kind": e.kind.map(RewardKind::key).unwrap_or(""),
                                "at": e.at, "pr": e.pr, "run_id": e.run_id, "detail": e.detail,
                            })
                        })
                        .collect(),
                    at: now,
                    ..Default::default()
                };
                self.apply_change(team_id, &change).await?;
                Ok(Some(id))
            }
        }
    }
}

/// Counts on a category record.
pub fn counts_of(i: &Item) -> Counts {
    Counts {
        accepted: gu(i, "accepted"),
        dismissed: gu(i, "dismissed"),
        ignored: gu(i, "ignored"),
    }
}

fn recent_of(i: &Item) -> Vec<serde_json::Value> {
    i.get("recent")
        .and_then(|v| v.as_l().ok())
        .map(|l| {
            l.iter()
                .filter_map(|v| v.as_s().ok())
                .filter_map(|s| serde_json::from_str(s).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Category states from counter and state records (one repository's).
pub fn category_states(cats: &[Item], states: &[Item]) -> Vec<CategoryState> {
    let mut out: Vec<CategoryState> = cats
        .iter()
        .map(|c| {
            let category = gs(c, "category");
            let counts = counts_of(c);
            let repo = gs(c, "repo");
            let sk = changes::state_sk(&repo, changes::KIND_CALIBRATION, &category);
            let state = states
                .iter()
                .find(|s| gs(s, "sk") == sk)
                .map(State::from_item);
            CategoryState {
                adjustment: calibration::decide(&category, &counts, calibration::MIN_SAMPLES),
                exempt: calibration::is_exempt(&category),
                reverted: state.map(|s| s.reverted).unwrap_or(false),
                category,
                counts,
            }
        })
        .collect();
    out.sort_by(|a, b| a.category.cmp(&b.category));
    out
}

/// A variant's stats under `w`: mean = Σ weight·shaped / Σ events.
pub fn arm_stats_of(i: &Item, w: &Weights) -> ArmStats {
    let mut events = 0u64;
    let mut total = 0f64;
    for k in RewardKind::ALL {
        let cnt = gu(i, &format!("n_{}", k.key()));
        if cnt == 0 {
            continue;
        }
        events += cnt;
        let sum = gf(i, &format!("s_{}", k.key()));
        total += (w.get(*k) * sum).clamp(
            -score::MAX_EVENT_SCORE * cnt as f64,
            score::MAX_EVENT_SCORE * cnt as f64,
        );
    }
    ArmStats {
        key: gs(i, "arm"),
        pulls: gu(i, "pulls"),
        events,
        mean: if events > 0 {
            total / events as f64
        } else {
            0.0
        },
    }
}

pub fn exemplar_of(i: &Item) -> Exemplar {
    Exemplar {
        id: gs(i, "id"),
        repo: gs(i, "repo"),
        kind: gs(i, "kind"),
        category: gs(i, "category"),
        area: gs(i, "area"),
        text: gs(i, "text"),
        reward: gf(i, "reward"),
        events: gu(i, "events"),
        uses: gu(i, "uses"),
        pr: gu(i, "pr"),
        created_at: gs(i, "created_at"),
        embedding: i
            .get("embedding")
            .and_then(|v| v.as_b().ok())
            .map(|b| exemplars::unpack(b.as_ref()))
            .unwrap_or_default(),
    }
}

pub fn lesson_of(i: &Item) -> Lesson {
    Lesson {
        id: gs(i, "id"),
        repo: gs(i, "repo"),
        agent: gs(i, "agent"),
        text: gs(i, "text"),
        seen: gu(i, "seen").max(1),
        created_at: gs(i, "created_at"),
        last_seen_at: gs(i, "last_seen_at"),
        evidence: i
            .get("evidence")
            .and_then(|v| v.as_l().ok())
            .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
            .unwrap_or_default(),
        reverted: i
            .get("reverted")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(pairs: &[(&str, AttributeValue)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn keys_carry_repo_and_kind() {
        assert_eq!(cat_sk("acme/web", "style"), "REWARDCAT#acme/web#style");
        assert_eq!(arm_sk("reviewer", "focused"), "REWARDARM#reviewer#focused");
        assert_eq!(lesson_sk("acme/web", "L1"), "REWARDLESSON#acme/web#L1");
        assert_eq!(pin_sk("reviewer"), "LEARNPIN#reviewer");
    }

    #[test]
    fn arm_reward_follows_current_weights() {
        let i = item(&[
            ("arm", s("focused")),
            ("pulls", n(10)),
            ("n_finding_accepted", n(3)),
            ("s_finding_accepted", n(3)),
            ("n_finding_ignored", n(4)),
            ("s_finding_ignored", n(4)),
        ]);
        let st = arm_stats_of(&i, &Weights::defaults());
        assert_eq!(st.pulls, 10);
        assert_eq!(st.events, 7);
        assert!((st.mean - (3.0 - 1.0) / 7.0).abs() < 1e-9);
        let harsher =
            Weights::from_overrides(&HashMap::from([("finding_ignored".to_string(), -1.0)]));
        let st2 = arm_stats_of(&i, &harsher);
        assert!(st2.mean < st.mean);
    }

    #[test]
    fn category_states_read_counts_and_reverts() {
        let cats = vec![
            item(&[
                ("repo", s("acme/web")),
                ("category", s("style")),
                ("accepted", n(1)),
                ("dismissed", n(9)),
            ]),
            item(&[
                ("repo", s("acme/web")),
                ("category", s("security")),
                ("dismissed", n(30)),
            ]),
        ];
        let states = vec![item(&[
            ("sk", s("LEARNSTATE#acme/web#calibration#style")),
            ("current", s("suppress")),
            ("reverted", AttributeValue::Bool(true)),
        ])];
        let got = category_states(&cats, &states);
        assert_eq!(got[0].category, "security");
        assert_eq!(got[0].adjustment, calibration::Adjustment::None);
        assert!(got[0].exempt);
        assert_eq!(got[1].adjustment, calibration::Adjustment::Suppress);
        assert!(got[1].reverted);
        assert_eq!(got[1].effective(), calibration::Adjustment::None);
    }

    #[test]
    fn exemplars_and_lessons_read_back() {
        let e = exemplar_of(&item(&[
            ("id", s("x")),
            ("repo", s("acme/web")),
            ("kind", s("fix")),
            ("reward", n(1.5)),
            (
                "embedding",
                AttributeValue::B(aws_smithy_types::Blob::new(exemplars::pack(&[1.0, 2.0]))),
            ),
        ]));
        assert_eq!(e.embedding, vec![1.0, 2.0]);
        assert_eq!(e.polarity(), Some("good"));
        let l = lesson_of(&item(&[
            ("id", s("L")),
            ("text", s("t")),
            ("evidence", AttributeValue::L(vec![s("a")])),
        ]));
        assert_eq!(l.seen, 1);
        assert_eq!(l.evidence, vec!["a".to_string()]);
    }
}
