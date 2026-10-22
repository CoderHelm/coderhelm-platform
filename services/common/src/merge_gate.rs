//! The auto-merge gate's per-PR record, shared by the worker (which runs the
//! gate) and the gateway (which re-arms it on GitHub events).
//!
//! One record per PR (`pk` = team id, `sk` = [`record_sk`]) holds the gate's
//! state, the head it is bound to, the current arming (`chain_id`), the id of
//! the PR's one status comment, and when deploy labels were added. The
//! gateway reads it to decide whether an event should wake the gate. The
//! worker writes the gate's progress; a person's stop or re-arm (a PR comment
//! or the dashboard, both through the gateway) writes only the stop fields,
//! the audit log and a new chain id, through [`stop`] and [`rearm`].
//!
//! A stop is sticky: no CI run, review or push wakes a stopped gate, and an
//! arming (the review of a new commit) finds it stopped and leaves it so.
//! Only a person clears it, and a re-arm never merges by itself: the gate
//! evaluates again from scratch (CoderHelm's approval on the current head,
//! people's approvals, CI).

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use std::collections::HashMap;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Sort-key prefix of every gate record.
pub const SK_PREFIX: &str = "MERGEGATE#";

/// Settings-table sort key of a PR's gate record (pk = team id).
pub fn record_sk(owner: &str, repo: &str, pr: u64) -> String {
    format!("{SK_PREFIX}{owner}/{repo}#{pr:06}")
}

/// Both keys are not in yet (CoderHelm's approval, a person's approval).
/// Waits for a review event; never polls.
pub const STATE_WAITING_APPROVAL: &str = "waiting_approval";
/// Both keys are in; CI is still running. Polls (bounded) and wakes on a
/// completed check suite.
pub const STATE_WAITING_CI: &str = "waiting_ci";
/// CI finished with failures. Wakes when a check suite completes again (a
/// re-run) or on a new commit.
pub const STATE_BLOCKED_CI: &str = "blocked_ci";
/// GitHub refused the merge (branch protection, a conflict).
pub const STATE_DECLINED: &str = "declined";
/// Stopped polling after its window, or after errors kept failing. Any CI or
/// review event re-arms it.
pub const STATE_PAUSED: &str = "paused";
pub const STATE_MERGED: &str = "merged";
pub const STATE_CLOSED: &str = "closed";
/// A new commit arrived; the review of that commit re-arms the gate.
pub const STATE_HEAD_MOVED: &str = "head_moved";
/// A person stopped auto-merge (a PR comment or the dashboard). Nothing
/// wakes it, not CI, a review or a new commit; only a person re-arms it.
pub const STATE_STOPPED: &str = "stopped";

/// Should a completed check suite on the gate's head re-arm it? Only while
/// the gate is waiting on (or was stopped by) CI or GitHub's merge rules.
pub fn wakes_on_ci(state: &str) -> bool {
    matches!(
        state,
        STATE_WAITING_CI | STATE_BLOCKED_CI | STATE_DECLINED | STATE_PAUSED
    )
}

/// Should a review event (an approval, a dismissal, a new review) on the
/// gate's head re-arm it? Any state that is not final: an approval can
/// complete the keys, and a dismissal or a request for changes must withdraw
/// one at once (the gate then shows it and never merges on it).
pub fn wakes_on_review(state: &str) -> bool {
    !matches!(
        state,
        STATE_MERGED | STATE_CLOSED | STATE_HEAD_MOVED | STATE_STOPPED
    )
}

/// Icons of the status comment's checklist lines (done, waiting, blocked).
pub const ICON_DONE: &str = "✅";
pub const ICON_WAITING: &str = "⏳";
pub const ICON_BLOCKED: &str = "🔴";

/// One line of the gate's checklist: `mark` is "done", "waiting" or
/// "blocked".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecklistLine {
    pub mark: &'static str,
    pub text: String,
}

/// Pure: the checklist lines of a stored status comment body (`- ✅ CI
/// green`), in order. Markdown code ticks are dropped so the dashboard can
/// show the text as is.
pub fn checklist(status_body: &str) -> Vec<ChecklistLine> {
    status_body
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- "))
        .filter_map(|l| {
            let (mark, rest) = [
                ("done", ICON_DONE),
                ("waiting", ICON_WAITING),
                ("blocked", ICON_BLOCKED),
            ]
            .into_iter()
            .find_map(|(m, icon)| l.strip_prefix(icon).map(|r| (m, r)))?;
            Some(ChecklistLine {
                mark,
                text: rest.trim().replace('`', ""),
            })
        })
        .collect()
}

// ─── Stop and re-arm (people) ───────────────────────────────────────────────

/// What a person asked the gate to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    Stop,
    Rearm,
}

impl Control {
    pub fn as_str(self) -> &'static str {
        match self {
            Control::Stop => "stop",
            Control::Rearm => "rearm",
        }
    }

    /// The dashboard's `do=` / `action` value.
    pub fn from_action(s: &str) -> Option<Self> {
        match s {
            "stop" => Some(Control::Stop),
            "rearm" => Some(Control::Rearm),
            _ => None,
        }
    }
}

/// The comment that stops auto-merge, as the status comment shows it.
pub const STOP_COMMAND: &str = "@coderhelm stop auto-merge";
/// The comment that re-arms it.
pub const RESUME_COMMAND: &str = "@coderhelm resume auto-merge";

/// Pure: the stop / re-arm command in a comment addressed to CoderHelm (the
/// text after the mention is stripped). Exact commands only, so a question
/// that happens to mention merging is never taken as one:
/// `stop|pause|cancel auto-merge` and `resume|re-arm|arm auto-merge`
/// (`automerge`, `auto merge`, a leading `the` and a `please` are accepted
/// too).
pub fn parse_control(text: &str) -> Option<Control> {
    let t = text
        .trim()
        .trim_end_matches(['.', '!'])
        .trim()
        .to_lowercase();
    let t = t.split_whitespace().collect::<Vec<_>>().join(" ");
    let t = t
        .replace("auto merge", "auto-merge")
        .replace("automerge", "auto-merge")
        .replace("re arm", "re-arm");
    let t = t.strip_prefix("please ").unwrap_or(&t);
    let t = t.strip_suffix(" please").unwrap_or(t);
    let (verb, object) = t.split_once(' ')?;
    let object = object.strip_prefix("the ").unwrap_or(object);
    if object != "auto-merge" {
        return None;
    }
    match verb {
        "stop" | "pause" | "cancel" => Some(Control::Stop),
        "resume" | "re-arm" | "rearm" | "arm" => Some(Control::Rearm),
        _ => None,
    }
}

/// Pure: may this person stop or re-arm auto-merge on the repo? Write access
/// or higher, read from GitHub's `collaborators/{user}/permission` answer
/// (`role_name` is exact; `permission` folds maintain into write and a
/// custom role into its base role). Anything else, an empty answer included,
/// is a no.
pub fn has_write_access(permission: &serde_json::Value) -> bool {
    let role = permission["role_name"].as_str().unwrap_or("");
    let perm = permission["permission"].as_str().unwrap_or("");
    matches!(role, "admin" | "maintain" | "write") || matches!(perm, "admin" | "write")
}

/// How a stop or re-arm came in.
pub const VIA_COMMENT: &str = "comment";
pub const VIA_DASHBOARD: &str = "dashboard";

/// Who stopped or re-armed the gate. `by` is a GitHub login when `is_login`,
/// otherwise the dashboard user's email, which is never shown on the PR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    pub by: String,
    pub is_login: bool,
    pub via: &'static str,
}

/// Pure: how the PR names the person: `@login`, or "a team member" for a
/// dashboard user without a linked GitHub login.
pub fn public_name(by: &str, is_login: bool) -> String {
    if is_login && !by.is_empty() {
        format!("@{by}")
    } else {
        "a team member".to_string()
    }
}

/// One line of the gate's audit log (`controls`, oldest first).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ControlEntry {
    /// "stop" or "rearm".
    pub action: String,
    pub by: String,
    #[serde(default)]
    pub is_login: bool,
    /// [`VIA_COMMENT`] or [`VIA_DASHBOARD`].
    pub via: String,
    /// Epoch seconds.
    pub at: i64,
    /// What it did: "stopped", "rearmed", or "recheck" (a re-arm of a gate
    /// that was not stopped: it evaluates again).
    #[serde(default)]
    pub outcome: String,
}

/// Pure: the audit log of a gate record (entries that can't be read are
/// skipped).
pub fn controls(item: &HashMap<String, AttributeValue>) -> Vec<ControlEntry> {
    item.get("controls")
        .and_then(|v| v.as_l().ok())
        .map(|l| {
            l.iter()
                .filter_map(|v| v.as_s().ok())
                .filter_map(|s| serde_json::from_str(s).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Who stopped a gate, from its record. None when it is not stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stopped {
    pub by: String,
    pub is_login: bool,
    pub via: String,
    pub at: i64,
}

/// Pure: the stop on a gate record, if any. The stop fields, not the state,
/// are the source of truth: a worker tick racing the stop can't clear them.
pub fn stopped(item: &HashMap<String, AttributeValue>) -> Option<Stopped> {
    let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    let by = s("stopped_by")?;
    Some(Stopped {
        by,
        is_login: item
            .get("stopped_login")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        via: s("stopped_via").unwrap_or_default(),
        at: item
            .get("stopped_at")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0),
    })
}

/// Pure: the dashboard page that stops or re-arms one PR's gate. It carries
/// no secret: the page needs a signed-in member of the team that owns the
/// repo, and asks before it acts.
pub fn control_url(app_url: &str, owner: &str, repo: &str, pr: u64, control: Control) -> String {
    format!(
        "{app_url}/reviewer/pr?repo={owner}/{repo}&pr={pr}&do={}",
        control.as_str()
    )
}

/// What a stop did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopOutcome {
    Stopped,
    /// Stopped before (stop is idempotent; the first stop is kept).
    AlreadyStopped(Stopped),
    /// The PR is merged or closed: nothing to stop. Holds the state.
    Finished(String),
}

/// What a re-arm did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RearmOutcome {
    /// It was stopped; the stop is cleared.
    Rearmed,
    /// It was not stopped: the gate is just evaluated again.
    NotStopped,
    Finished(String),
}

/// How long a gate record touched by a person is kept.
const CONTROL_TTL_SECS: i64 = 30 * 86_400;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn is_final(state: &str) -> bool {
    matches!(state, STATE_MERGED | STATE_CLOSED)
}

fn entry_value(c: Control, actor: &Actor, now: i64, outcome: &str) -> AttributeValue {
    let entry = ControlEntry {
        action: c.as_str().to_string(),
        by: actor.by.clone(),
        is_login: actor.is_login,
        via: actor.via.to_string(),
        at: now,
        outcome: outcome.to_string(),
    };
    AttributeValue::L(vec![s(&serde_json::to_string(&entry).unwrap_or_default())])
}

fn conditional_failed<E, R>(e: &aws_sdk_dynamodb::error::SdkError<E, R>) -> bool
where
    E: aws_sdk_dynamodb::error::ProvideErrorMetadata,
{
    e.as_service_error()
        .and_then(|se| se.code())
        .is_some_and(|c| c == "ConditionalCheckFailedException")
}

async fn read(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    sk: &str,
) -> Result<HashMap<String, AttributeValue>, BoxError> {
    Ok(dynamo
        .get_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(sk))
        .consistent_read(true)
        .send()
        .await?
        .item()
        .cloned()
        .unwrap_or_default())
}

fn state_of(item: &HashMap<String, AttributeValue>) -> String {
    item.get("state")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// Stop a PR's gate. Records who and how, moves the gate to a new chain (a
/// tick of the old one stops at its next read, and the worker re-reads the
/// record right before merging), and appends to the audit log. Idempotent:
/// a second stop keeps the first one. Works before the gate was ever armed
/// (the record is created stopped, and the first arming finds it so).
#[allow(clippy::too_many_arguments)]
pub async fn stop(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    actor: &Actor,
    now: i64,
) -> Result<StopOutcome, BoxError> {
    let sk = record_sk(owner, repo, pr);
    let item = read(dynamo, table, team_id, &sk).await?;
    let state = state_of(&item);
    if is_final(&state) {
        return Ok(StopOutcome::Finished(state));
    }
    if let Some(st) = stopped(&item) {
        return Ok(StopOutcome::AlreadyStopped(st));
    }
    let res = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&sk))
        .update_expression(
            "SET stopped_by = :by, stopped_login = :bl, stopped_via = :via, stopped_at = :now, \
             #st = :stopped, chain_id = :chain, detail = :d, updated_at = :t, #ttl = :ttl, \
             controls = list_append(if_not_exists(controls, :empty), :entry)",
        )
        .condition_expression(
            "attribute_not_exists(stopped_by) AND \
             (attribute_not_exists(#st) OR (#st <> :merged AND #st <> :closed))",
        )
        .expression_attribute_names("#st", "state")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":by", s(&actor.by))
        .expression_attribute_values(":bl", AttributeValue::Bool(actor.is_login))
        .expression_attribute_values(":via", s(actor.via))
        .expression_attribute_values(":now", n(now))
        .expression_attribute_values(":stopped", s(STATE_STOPPED))
        .expression_attribute_values(":chain", s(&format!("stopped-{}", ulid::Ulid::new())))
        .expression_attribute_values(
            ":d",
            s(&format!(
                "Stopped by {}",
                public_name(&actor.by, actor.is_login)
            )),
        )
        .expression_attribute_values(":t", s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":ttl", n(now + CONTROL_TTL_SECS))
        .expression_attribute_values(":empty", AttributeValue::L(vec![]))
        .expression_attribute_values(":entry", entry_value(Control::Stop, actor, now, "stopped"))
        .expression_attribute_values(":merged", s(STATE_MERGED))
        .expression_attribute_values(":closed", s(STATE_CLOSED))
        .send()
        .await;
    match res {
        Ok(_) => Ok(StopOutcome::Stopped),
        Err(e) if conditional_failed(&e) => {
            // Someone else stopped it, or it finished, in between.
            let item = read(dynamo, table, team_id, &sk).await?;
            let state = state_of(&item);
            Ok(match stopped(&item) {
                Some(st) if !is_final(&state) => StopOutcome::AlreadyStopped(st),
                _ => StopOutcome::Finished(state),
            })
        }
        Err(e) => Err(e.into()),
    }
}

/// Clear a person's stop. It never merges by itself: the caller arms the
/// gate, which evaluates every condition again on the PR's current head.
/// The state becomes `paused` (any CI or review event wakes it), so even a
/// lost arming message is picked up by the next event. A re-arm of a gate
/// that was not stopped is logged as a re-check and changes nothing here.
#[allow(clippy::too_many_arguments)]
pub async fn rearm(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    actor: &Actor,
    now: i64,
) -> Result<RearmOutcome, BoxError> {
    let sk = record_sk(owner, repo, pr);
    let item = read(dynamo, table, team_id, &sk).await?;
    let state = state_of(&item);
    if is_final(&state) {
        return Ok(RearmOutcome::Finished(state));
    }
    if stopped(&item).is_some() {
        let res = dynamo
            .update_item()
            .table_name(table)
            .key("pk", s(team_id))
            .key("sk", s(&sk))
            .update_expression(
                "REMOVE stopped_by, stopped_login, stopped_via, stopped_at \
                 SET #st = :paused, detail = :d, updated_at = :t, \
                 controls = list_append(if_not_exists(controls, :empty), :entry)",
            )
            .condition_expression("attribute_exists(stopped_by)")
            .expression_attribute_names("#st", "state")
            .expression_attribute_values(":paused", s(STATE_PAUSED))
            .expression_attribute_values(
                ":d",
                s(&format!(
                    "Re-armed by {}",
                    public_name(&actor.by, actor.is_login)
                )),
            )
            .expression_attribute_values(":t", s(&chrono::Utc::now().to_rfc3339()))
            .expression_attribute_values(":empty", AttributeValue::L(vec![]))
            .expression_attribute_values(
                ":entry",
                entry_value(Control::Rearm, actor, now, "rearmed"),
            )
            .send()
            .await;
        match res {
            Ok(_) => return Ok(RearmOutcome::Rearmed),
            Err(e) if conditional_failed(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    // Not stopped (or re-armed by someone else in between): log the
    // re-check on an existing record only; never create one for it.
    let res = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&sk))
        .update_expression("SET controls = list_append(if_not_exists(controls, :empty), :entry)")
        .condition_expression("attribute_exists(sk)")
        .expression_attribute_values(":empty", AttributeValue::L(vec![]))
        .expression_attribute_values(":entry", entry_value(Control::Rearm, actor, now, "recheck"))
        .send()
        .await;
    match res {
        Ok(_) => {}
        Err(e) if conditional_failed(&e) => {}
        Err(e) => return Err(e.into()),
    }
    Ok(RearmOutcome::NotStopped)
}

/// The PR was merged or closed while a person's stop held (no loop runs to
/// notice it): record that, so the dashboard doesn't show it stopped forever.
/// The stop itself is kept, so a reopened PR is still stopped. Only touches
/// a stopped record.
#[allow(clippy::too_many_arguments)]
pub async fn finish_stopped(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    merged: bool,
) -> Result<(), BoxError> {
    let res = dynamo
        .update_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(&record_sk(owner, repo, pr)))
        .update_expression("SET #st = :st, updated_at = :t")
        .condition_expression("attribute_exists(stopped_by)")
        .expression_attribute_names("#st", "state")
        .expression_attribute_values(":st", s(if merged { STATE_MERGED } else { STATE_CLOSED }))
        .expression_attribute_values(":t", s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(e) if conditional_failed(&e) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// CoderHelm's own latest verdict (`APPROVE` / `REQUEST_CHANGES`) recorded
/// for this head, from the review records (`REVIEW#{owner}/{repo}#{pr:06}#…`,
/// newest first). None = no verdict for this head yet. A failed read is an
/// error, never "no verdict".
#[allow(clippy::too_many_arguments)]
pub async fn bot_verdict_at_head(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
    head_sha: &str,
) -> Result<Option<String>, BoxError> {
    let prefix = format!("REVIEW#{owner}/{repo}#{pr:0>6}#");
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    // Page through the newest records until one for this head is found; the
    // records of other heads and question answers are skipped.
    for _ in 0..5 {
        let resp = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :sk)")
            .expression_attribute_values(":pk", s(team_id))
            .expression_attribute_values(":sk", s(&prefix))
            .scan_index_forward(false)
            .limit(25)
            .set_exclusive_start_key(start.take())
            .send()
            .await?;
        for it in resp.items() {
            let get = |k: &str| it.get(k).and_then(|v| v.as_s().ok()).map(String::as_str);
            let verdict = get("verdict").unwrap_or("");
            if verdict != "APPROVE" && verdict != "REQUEST_CHANGES" {
                continue;
            }
            if get("head_sha") == Some(head_sha) {
                return Ok(Some(verdict.to_string()));
            }
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    Ok(None)
}

/// What a re-arm does besides arming the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RearmNext {
    /// CoderHelm has a verdict on the current head: the gate re-checks it,
    /// people's approvals and CI.
    Evaluate,
    /// No verdict on the current head (commits landed since): queue a
    /// review of it. The gate waits for it and never merges on the old one.
    QueueReview,
    /// No verdict on the current head and review is off for the repo: the
    /// gate waits, and the person is told what is missing.
    NeedsReview,
}

/// Pure: the re-arm's next step. A re-arm never bypasses a key: an approval
/// on an older head counts for nothing.
pub fn rearm_next(bot_verdict_at_head: Option<&str>, review_on: bool) -> RearmNext {
    match (bot_verdict_at_head, review_on) {
        (Some(_), _) => RearmNext::Evaluate,
        (None, true) => RearmNext::QueueReview,
        (None, false) => RearmNext::NeedsReview,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checklist_reads_marked_lines_only() {
        let body = "<!-- marker -->\n**Auto-merge** · waiting\n\n- ✅ CoderHelm approved\n\
                    - ⏳ Waiting for a person's approval\n- 🔴 CI failing: `build`, `lint`\n\
                    - plain bullet\n\nNot merging.";
        assert_eq!(
            checklist(body),
            vec![
                ChecklistLine {
                    mark: "done",
                    text: "CoderHelm approved".into()
                },
                ChecklistLine {
                    mark: "waiting",
                    text: "Waiting for a person's approval".into()
                },
                ChecklistLine {
                    mark: "blocked",
                    text: "CI failing: build, lint".into()
                },
            ]
        );
        assert!(checklist("").is_empty());
    }

    #[test]
    fn record_key_is_zero_padded() {
        assert_eq!(record_sk("o", "r", 42), "MERGEGATE#o/r#000042");
    }

    #[test]
    fn ci_completion_wakes_only_ci_bound_states() {
        for s in [
            STATE_WAITING_CI,
            STATE_BLOCKED_CI,
            STATE_DECLINED,
            STATE_PAUSED,
        ] {
            assert!(wakes_on_ci(s), "{s}");
        }
        for s in [
            STATE_WAITING_APPROVAL,
            STATE_MERGED,
            STATE_CLOSED,
            STATE_HEAD_MOVED,
            STATE_STOPPED,
            "",
        ] {
            assert!(!wakes_on_ci(s), "{s}");
        }
    }

    #[test]
    fn review_events_wake_every_open_state() {
        for s in [
            STATE_WAITING_APPROVAL,
            STATE_WAITING_CI,
            STATE_BLOCKED_CI,
            STATE_DECLINED,
            STATE_PAUSED,
        ] {
            assert!(wakes_on_review(s), "{s}");
        }
        for s in [STATE_MERGED, STATE_CLOSED, STATE_HEAD_MOVED, STATE_STOPPED] {
            assert!(!wakes_on_review(s), "{s}");
        }
    }

    #[test]
    fn a_stopped_gate_wakes_on_nothing() {
        // No CI run, review, approval or new commit re-arms a person's stop.
        assert!(!wakes_on_ci(STATE_STOPPED));
        assert!(!wakes_on_review(STATE_STOPPED));
    }

    #[test]
    fn control_commands_and_their_aliases() {
        for t in [
            "stop auto-merge",
            "Stop auto-merge.",
            "pause auto-merge",
            "cancel the auto-merge",
            "cancel automerge!",
            "stop  auto merge",
        ] {
            assert_eq!(parse_control(t), Some(Control::Stop), "{t}");
        }
        for t in [
            "resume auto-merge",
            "re-arm auto-merge",
            "rearm automerge",
            "re arm the auto merge",
            "arm auto-merge",
            "Resume Auto-Merge",
            "please resume auto-merge",
        ] {
            assert_eq!(parse_control(t), Some(Control::Rearm), "{t}");
        }
        // Questions and other asks are not commands.
        for t in [
            "",
            "stop",
            "why did auto-merge stop?",
            "can you stop auto-merge until friday",
            "stop auto-merge and review again",
            "re-review",
            "merge",
            "please",
        ] {
            assert_eq!(parse_control(t), None, "{t}");
        }
    }

    #[test]
    fn only_write_access_or_higher_controls_the_gate() {
        use serde_json::json;
        for ok in [
            json!({"permission": "admin", "role_name": "admin"}),
            json!({"permission": "admin", "role_name": "maintain"}),
            json!({"permission": "write", "role_name": "maintain"}),
            json!({"permission": "write", "role_name": "write"}),
            // A custom role built on write.
            json!({"permission": "write", "role_name": "release-captain"}),
        ] {
            assert!(has_write_access(&ok), "{ok}");
        }
        for no in [
            json!({"permission": "read", "role_name": "read"}),
            json!({"permission": "read", "role_name": "triage"}),
            json!({"permission": "none", "role_name": ""}),
            json!({"message": "Not Found"}),
            json!(null),
        ] {
            assert!(!has_write_access(&no), "{no}");
        }
    }

    #[test]
    fn rearm_never_counts_an_approval_on_an_older_head() {
        assert_eq!(rearm_next(Some("APPROVE"), true), RearmNext::Evaluate);
        assert_eq!(
            rearm_next(Some("REQUEST_CHANGES"), false),
            RearmNext::Evaluate
        );
        assert_eq!(rearm_next(None, true), RearmNext::QueueReview);
        assert_eq!(rearm_next(None, false), RearmNext::NeedsReview);
    }

    #[test]
    fn stop_fields_and_audit_log_read_back() {
        let entry = ControlEntry {
            action: "stop".into(),
            by: "a-reviewer".into(),
            is_login: true,
            via: VIA_COMMENT.into(),
            at: 100,
            outcome: "stopped".into(),
        };
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        assert_eq!(stopped(&item), None);
        assert!(controls(&item).is_empty());
        item.insert("stopped_by".into(), s("a-reviewer"));
        item.insert("stopped_login".into(), AttributeValue::Bool(true));
        item.insert("stopped_via".into(), s(VIA_COMMENT));
        item.insert("stopped_at".into(), n(100));
        item.insert(
            "controls".into(),
            AttributeValue::L(vec![
                s(&serde_json::to_string(&entry).unwrap()),
                s("not json"),
            ]),
        );
        assert_eq!(
            stopped(&item),
            Some(Stopped {
                by: "a-reviewer".into(),
                is_login: true,
                via: VIA_COMMENT.into(),
                at: 100
            })
        );
        assert_eq!(controls(&item), vec![entry]);
    }

    #[test]
    fn the_pr_never_shows_a_dashboard_users_email() {
        assert_eq!(public_name("a-reviewer", true), "@a-reviewer");
        assert_eq!(public_name("someone@example.com", false), "a team member");
    }

    #[test]
    fn control_links_carry_no_secret() {
        let u = control_url(
            "https://app.example.com",
            "your-team",
            "web",
            42,
            Control::Stop,
        );
        assert_eq!(
            u,
            "https://app.example.com/reviewer/pr?repo=your-team/web&pr=42&do=stop"
        );
        assert_eq!(Control::from_action("rearm"), Some(Control::Rearm));
        assert_eq!(Control::from_action("merge"), None);
    }
}
