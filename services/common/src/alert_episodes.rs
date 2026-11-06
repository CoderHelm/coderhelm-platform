//! Repeat settings for alert routes: post an alarm once, not every time it
//! fires.
//!
//! A route (and, by name, one of its alarms) says how often a firing alert
//! posts to chat:
//!
//! - [`Repeat::EveryTime`]: every ALARM posts, as before this setting existed.
//!   Routes saved without it behave this way.
//! - [`Repeat::UntilClear`]: the first ALARM posts; repeats stay quiet until
//!   the alarm has really cleared.
//! - [`Repeat::Daily`]: as `UntilClear`, plus one reminder a day while it is
//!   still firing.
//!
//! The span from the first ALARM to the real clear is an **episode**, stored
//! per route and alert (the CloudWatch alarm's ARN carries its account,
//! region and name). An OK starts a quiet window ([`quiet_window_secs`]): an
//! ALARM inside it is the same episode (the alarm is flapping), and only when
//! the window passes without one has the alarm cleared. Then the recovery
//! posts (on routes that post recoveries) and the next ALARM posts again.
//!
//! Every change to an episode is a read, a pure decision ([`step`]) and a
//! write conditioned on the version read ([`apply`]), so two deliveries at the
//! same moment can't both post: the loser reads the winner's episode and
//! decides again.

use std::collections::BTreeMap;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use serde_json::Value;
use tracing::warn;

use crate::mcp_credentials::Cipher;
use crate::notify::{self, Poster};

/// How often a firing alert posts to chat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Repeat {
    /// Every ALARM posts (the behaviour before this setting).
    #[default]
    EveryTime,
    /// The first ALARM posts; repeats stay quiet until it clears.
    UntilClear,
    /// As `UntilClear`, plus one reminder a day while it keeps firing.
    Daily,
}

pub const EVERY_TIME: &str = "every_time";
pub const UNTIL_CLEAR: &str = "until_clear";
pub const DAILY: &str = "daily";

impl Repeat {
    /// The stored value. `""` (a route saved before this setting) is
    /// `EveryTime`; anything else unknown is `None`.
    pub fn parse(v: &str) -> Option<Self> {
        match v.trim() {
            "" | EVERY_TIME => Some(Self::EveryTime),
            UNTIL_CLEAR => Some(Self::UntilClear),
            DAILY => Some(Self::Daily),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::EveryTime => EVERY_TIME,
            Self::UntilClear => UNTIL_CLEAR,
            Self::Daily => DAILY,
        }
    }
}

/// Most alarms one route may give their own repeat setting.
pub const MAX_ALARM_OVERRIDES: usize = 200;

/// Shortest quiet window: an OK must hold this long, with no ALARM, before
/// the alarm counts as cleared. Most alarms are evaluated every 1 to 5
/// minutes, so a flapping alarm swings back within a few periods; half an hour
/// is six 5-minute periods of calm, long enough to call it settled and short
/// enough that the recovery post is still news.
pub const CLEAR_QUIET_MIN_SECS: u64 = 30 * 60;
/// An alarm evaluated over a longer span gets this many of its own spans
/// (period x evaluation periods, from the alarm's own SNS message): it can't
/// swing back sooner than its next evaluation, so one span of OK proves
/// little and two say it stayed clear.
pub const CLEAR_QUIET_PERIODS: u64 = 2;
/// Longest quiet window, whatever the alarm's span (an hourly or daily alarm
/// would otherwise hold its recovery for days).
pub const CLEAR_QUIET_MAX_SECS: u64 = 6 * 3600;
/// A reminder at most this often while an episode stays open.
pub const REMINDER_EVERY_SECS: u64 = 24 * 3600;
/// An episode's record lives this long after its last change.
const EPISODE_TTL_SECS: u64 = 30 * 86_400;
/// Most of the alert's text kept on the episode, for the cards posted later.
const MAX_EPISODE_BODY: usize = 6_000;
/// SQS delays a message at most this long; longer waits chain.
pub const SQS_MAX_DELAY_SECS: u64 = 900;

/// Pure: the quiet window for an OK, from the CloudWatch message when it has
/// the alarm's period and evaluation periods. See [`CLEAR_QUIET_MIN_SECS`].
pub fn quiet_window_secs(message: &str) -> u64 {
    let v: Value = serde_json::from_str(message).unwrap_or(Value::Null);
    let t = &v["Trigger"];
    let num = |x: &Value| {
        x.as_u64()
            .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
    };
    let span = match (num(&t["Period"]), num(&t["EvaluationPeriods"])) {
        (Some(p), Some(e)) => p.saturating_mul(e.max(1)),
        (Some(p), None) => p,
        _ => 0,
    };
    span.saturating_mul(CLEAR_QUIET_PERIODS)
        .clamp(CLEAR_QUIET_MIN_SECS, CLEAR_QUIET_MAX_SECS)
}

/// Pure: the CloudWatch alarm an alert title names (`<STATE>: <name>`).
pub fn alarm_name<'a>(kind: &str, title: &'a str) -> Option<&'a str> {
    (kind == "cloudwatch_alarm")
        .then(|| title.split_once(": ").map(|(_, n)| n))
        .flatten()
}

/// Pure: the setting that applies to an alert. An alarm named in the route's
/// overrides uses its own; other CloudWatch alarms and plain messages (which
/// clear with a `Recovers:` line) use the route's. Alerts that never send a
/// recovery (custom notifications, EventBridge events) always post every
/// time: with no clear, "until it clears" would mean never again.
pub fn effective(
    route: Repeat,
    overrides: &BTreeMap<String, Repeat>,
    kind: &str,
    title: &str,
) -> Repeat {
    match kind {
        "cloudwatch_alarm" => alarm_name(kind, title)
            .and_then(|n| overrides.get(n).copied())
            .unwrap_or(route),
        "message" | "recovery" => route,
        _ => Repeat::EveryTime,
    }
}

/// The route's setting and its per-alarm overrides, as stored on its item.
pub fn route_setting(
    item: &std::collections::HashMap<String, AttributeValue>,
) -> (Repeat, BTreeMap<String, Repeat>) {
    let route = item
        .get("repeat")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| Repeat::parse(s))
        .unwrap_or_default();
    let overrides = item
        .get("alarm_repeat")
        .and_then(|v| v.as_m().ok())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.clone(), Repeat::parse(v.as_s().ok()?)?)))
                .collect()
        })
        .unwrap_or_default();
    (route, overrides)
}

/// Settings-table sort key of an alert's episode on a route.
pub fn episode_sk(topic_arn: &str, fingerprint: &str) -> String {
    format!(
        "ALERT_EPISODE#{}",
        crate::content_hash(&format!("{topic_arn}\n{fingerprint}"))
    )
}

/// One alert's run of firing on one route, from its first ALARM to its real
/// clear. Times are epoch seconds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Episode {
    pub topic_arn: String,
    pub fingerprint: String,
    /// The latest firing alert, for the cards posted later.
    pub kind: String,
    pub title: String,
    pub body: String,
    /// From the first ALARM until it has really cleared.
    pub open: bool,
    pub first_at: i64,
    /// The alert record that opened it: every alert in it points here.
    pub first_alert_id: String,
    /// When it last posted: the first card or the latest reminder.
    pub last_notified_at: i64,
    /// ALARMs received in it, the first included.
    pub fires: u64,
    pub reminders: u64,
    pub last_event_id: String,
    /// When an OK started the quiet window, while it is running.
    pub clearing_since: Option<i64>,
    /// The OK's alert record, and the window it started.
    pub clear_event_id: String,
    pub quiet_secs: u64,
    /// The OK itself, for the recovery card.
    pub clear_kind: String,
    pub clear_title: String,
    pub clear_body: String,
    /// Bumped on every write; each write is conditioned on the one read.
    pub ver: u64,
}

/// What happened, as [`step`] sees it.
#[derive(Debug, Clone, Copy)]
pub enum Input<'a> {
    /// The alert fired (an ALARM, or a plain message).
    Fire {
        event_id: &'a str,
        at: i64,
        kind: &'a str,
        title: &'a str,
        body: &'a str,
    },
    /// Its recovery (an OK) arrived.
    Clear {
        event_id: &'a str,
        at: i64,
        quiet_secs: u64,
        kind: &'a str,
        title: &'a str,
        body: &'a str,
    },
    /// The quiet window an OK started should have passed.
    ClearCheck { clear_event_id: &'a str, now: i64 },
    /// A day since the last post should have passed.
    RemindCheck { notified_at: i64, now: i64 },
}

/// What to do about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// A new episode: post as usual.
    Opened,
    /// The previous episode had cleared (its quiet window passed before this
    /// ALARM) but its recovery hadn't posted yet: post it, then post this
    /// alarm as a new episode.
    Reopened(Box<Episode>),
    /// Still firing and a day since the last post: post a reminder.
    Remind,
    /// Still firing: record it, post nothing, start no run.
    Suppressed,
    /// A delivery already counted (an SNS redelivery).
    Redelivered,
    /// An OK started the quiet window (`schedule`: queue the check; false
    /// when a window was already running).
    Clearing { schedule: bool },
    /// The window passed with no ALARM: the episode ended. Post its recovery.
    Cleared(Box<Episode>),
    /// Too early: check again after this many seconds.
    Wait(u64),
    /// No open episode (or a check about one that moved on): nothing to do.
    Nothing,
}

impl Decision {
    /// For logs.
    pub fn name(&self) -> &'static str {
        match self {
            Decision::Opened => "opened",
            Decision::Reopened(_) => "reopened",
            Decision::Remind => "remind",
            Decision::Suppressed => "suppressed",
            Decision::Redelivered => "redelivered",
            Decision::Clearing { .. } => "clearing",
            Decision::Cleared(_) => "cleared",
            Decision::Wait(_) => "wait",
            Decision::Nothing => "nothing",
        }
    }
}

fn trimmed(s: &str) -> String {
    crate::truncate_str(s, MAX_EPISODE_BODY).to_string()
}

/// Pure: the decision for `input` given the stored episode, and the episode
/// to write (None = no write).
pub fn step(prev: Option<&Episode>, input: Input, mode: Repeat) -> (Decision, Option<Episode>) {
    let open = prev.filter(|p| p.open);
    match input {
        Input::Fire {
            event_id,
            at,
            kind,
            title,
            body,
        } => {
            let fresh = || Episode {
                topic_arn: prev.map(|p| p.topic_arn.clone()).unwrap_or_default(),
                fingerprint: prev.map(|p| p.fingerprint.clone()).unwrap_or_default(),
                kind: kind.to_string(),
                title: title.to_string(),
                body: trimmed(body),
                open: true,
                first_at: at,
                first_alert_id: event_id.to_string(),
                last_notified_at: at,
                fires: 1,
                last_event_id: event_id.to_string(),
                ver: prev.map(|p| p.ver).unwrap_or(0) + 1,
                ..Episode::default()
            };
            let Some(p) = open else {
                return (Decision::Opened, Some(fresh()));
            };
            if p.last_event_id == event_id || p.first_alert_id == event_id {
                return (Decision::Redelivered, None);
            }
            if let Some(since) = p.clearing_since {
                if at.saturating_sub(since) >= p.quiet_secs as i64 {
                    return (Decision::Reopened(Box::new(p.clone())), Some(fresh()));
                }
            }
            // Still the same episode. An OK in its window is undone.
            let mut next = Episode {
                kind: kind.to_string(),
                title: title.to_string(),
                body: trimmed(body),
                fires: p.fires + 1,
                last_event_id: event_id.to_string(),
                clearing_since: None,
                clear_event_id: String::new(),
                clear_kind: String::new(),
                clear_title: String::new(),
                clear_body: String::new(),
                ver: p.ver + 1,
                ..p.clone()
            };
            let due = at.saturating_sub(p.last_notified_at) >= REMINDER_EVERY_SECS as i64;
            if mode == Repeat::Daily && due {
                next.last_notified_at = at;
                next.reminders += 1;
                (Decision::Remind, Some(next))
            } else {
                (Decision::Suppressed, Some(next))
            }
        }
        Input::Clear {
            event_id,
            at,
            quiet_secs,
            kind,
            title,
            body,
        } => {
            let Some(p) = open else {
                return (Decision::Nothing, None);
            };
            if p.clearing_since.is_some() {
                return (Decision::Clearing { schedule: false }, None);
            }
            let next = Episode {
                clearing_since: Some(at),
                clear_event_id: event_id.to_string(),
                quiet_secs,
                clear_kind: kind.to_string(),
                clear_title: title.to_string(),
                clear_body: trimmed(body),
                ver: p.ver + 1,
                ..p.clone()
            };
            (Decision::Clearing { schedule: true }, Some(next))
        }
        Input::ClearCheck {
            clear_event_id,
            now,
        } => {
            let Some(p) = open.filter(|p| p.clear_event_id == clear_event_id) else {
                return (Decision::Nothing, None);
            };
            let Some(since) = p.clearing_since else {
                return (Decision::Nothing, None);
            };
            let waited = now.saturating_sub(since).max(0) as u64;
            if waited < p.quiet_secs {
                return (Decision::Wait(p.quiet_secs - waited), None);
            }
            let next = Episode {
                open: false,
                clearing_since: None,
                ver: p.ver + 1,
                ..p.clone()
            };
            (Decision::Cleared(Box::new(p.clone())), Some(next))
        }
        Input::RemindCheck { notified_at, now } => {
            let Some(p) = open.filter(|p| p.last_notified_at == notified_at) else {
                return (Decision::Nothing, None);
            };
            // Clearing: the window decides. A real clear ends it; an ALARM
            // inside the window reminds then if a day has passed.
            if mode != Repeat::Daily || p.clearing_since.is_some() {
                return (Decision::Nothing, None);
            }
            let waited = now.saturating_sub(notified_at).max(0) as u64;
            if waited < REMINDER_EVERY_SECS {
                return (Decision::Wait(REMINDER_EVERY_SECS - waited), None);
            }
            let next = Episode {
                last_notified_at: now,
                reminders: p.reminders + 1,
                ver: p.ver + 1,
                ..p.clone()
            };
            (Decision::Remind, Some(next))
        }
    }
}

// ─── Storage ────────────────────────────────────────────────────────────────

/// Where episodes live. `put` writes only when the stored version is still
/// `expect` (None = no record yet) and says whether it did.
#[async_trait::async_trait]
pub trait EpisodeStore: Send + Sync {
    async fn get(&self, team_id: &str, sk: &str) -> Result<Option<Episode>, String>;
    async fn put(
        &self,
        team_id: &str,
        sk: &str,
        ep: &Episode,
        expect: Option<u64>,
    ) -> Result<bool, String>;
    /// Remove the record. With `first_alert_id`, only while that alert still
    /// opened it.
    async fn delete(
        &self,
        team_id: &str,
        sk: &str,
        first_alert_id: Option<&str>,
    ) -> Result<(), String>;
}

/// The result of [`apply`]: the decision, and the episode as now stored.
#[derive(Debug, Clone)]
pub struct Applied {
    pub decision: Decision,
    pub episode: Option<Episode>,
}

/// Read, decide, write. A write that loses to a concurrent one reads again
/// and decides again, so of two deliveries at once only one opens the
/// episode (or reminds); the other sees it and stays quiet.
pub async fn apply(
    store: &dyn EpisodeStore,
    team_id: &str,
    topic_arn: &str,
    fingerprint: &str,
    input: Input<'_>,
    mode: Repeat,
) -> Result<Applied, String> {
    let sk = episode_sk(topic_arn, fingerprint);
    for _ in 0..5 {
        let prev = store.get(team_id, &sk).await?;
        let (decision, write) = step(prev.as_ref(), input, mode);
        let Some(mut next) = write else {
            return Ok(Applied {
                decision,
                episode: prev,
            });
        };
        next.topic_arn = topic_arn.to_string();
        next.fingerprint = fingerprint.to_string();
        if store
            .put(team_id, &sk, &next, prev.as_ref().map(|p| p.ver))
            .await?
        {
            return Ok(Applied {
                decision,
                episode: Some(next),
            });
        }
    }
    Err("the episode kept changing under us".into())
}

/// Episodes in the settings table, under the team's partition.
pub struct DynamoStore<'a> {
    pub client: &'a DynamoClient,
    pub table: &'a str,
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: impl ToString) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

/// The episode as a settings item (without its keys).
pub fn to_item(ep: &Episode) -> Vec<(&'static str, AttributeValue)> {
    let mut out = vec![
        ("topic_arn", s(&ep.topic_arn)),
        ("fingerprint", s(&ep.fingerprint)),
        ("kind", s(&ep.kind)),
        ("title", s(crate::truncate_str(&ep.title, 300))),
        ("body", s(&ep.body)),
        ("open", AttributeValue::Bool(ep.open)),
        ("first_at", n(ep.first_at)),
        ("first_alert_id", s(&ep.first_alert_id)),
        ("last_notified_at", n(ep.last_notified_at)),
        ("fires", n(ep.fires)),
        ("reminders", n(ep.reminders)),
        ("last_event_id", s(&ep.last_event_id)),
        ("clear_event_id", s(&ep.clear_event_id)),
        ("quiet_secs", n(ep.quiet_secs)),
        ("clear_kind", s(&ep.clear_kind)),
        ("clear_title", s(crate::truncate_str(&ep.clear_title, 300))),
        ("clear_body", s(&ep.clear_body)),
        ("ver", n(ep.ver)),
    ];
    if let Some(since) = ep.clearing_since {
        out.push(("clearing_since", n(since)));
    }
    out
}

/// The episode a settings item holds.
pub fn from_item(item: &std::collections::HashMap<String, AttributeValue>) -> Episode {
    let st = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let num = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<i64>().ok())
    };
    Episode {
        topic_arn: st("topic_arn"),
        fingerprint: st("fingerprint"),
        kind: st("kind"),
        title: st("title"),
        body: st("body"),
        open: item
            .get("open")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        first_at: num("first_at").unwrap_or(0),
        first_alert_id: st("first_alert_id"),
        last_notified_at: num("last_notified_at").unwrap_or(0),
        fires: num("fires").unwrap_or(0).max(0) as u64,
        reminders: num("reminders").unwrap_or(0).max(0) as u64,
        last_event_id: st("last_event_id"),
        clearing_since: num("clearing_since"),
        clear_event_id: st("clear_event_id"),
        quiet_secs: num("quiet_secs").unwrap_or(0).max(0) as u64,
        clear_kind: st("clear_kind"),
        clear_title: st("clear_title"),
        clear_body: st("clear_body"),
        ver: num("ver").unwrap_or(0).max(0) as u64,
    }
}

#[async_trait::async_trait]
impl EpisodeStore for DynamoStore<'_> {
    async fn get(&self, team_id: &str, sk: &str) -> Result<Option<Episode>, String> {
        let out = self
            .client
            .get_item()
            .table_name(self.table)
            .key("pk", s(team_id))
            .key("sk", s(sk))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(out.item().map(from_item))
    }

    async fn put(
        &self,
        team_id: &str,
        sk: &str,
        ep: &Episode,
        expect: Option<u64>,
    ) -> Result<bool, String> {
        let ttl = chrono::Utc::now().timestamp().max(0) as u64 + EPISODE_TTL_SECS;
        let mut put = self
            .client
            .put_item()
            .table_name(self.table)
            .item("pk", s(team_id))
            .item("sk", s(sk))
            .item("ttl", n(ttl));
        for (k, v) in to_item(ep) {
            put = put.item(k, v);
        }
        put = match expect {
            None => put.condition_expression("attribute_not_exists(pk)"),
            Some(v) => put
                .condition_expression("ver = :v")
                .expression_attribute_values(":v", n(v)),
        };
        match put.send().await {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(e) => Err(e.to_string()),
        }
    }

    async fn delete(
        &self,
        team_id: &str,
        sk: &str,
        first_alert_id: Option<&str>,
    ) -> Result<(), String> {
        let mut del = self
            .client
            .delete_item()
            .table_name(self.table)
            .key("pk", s(team_id))
            .key("sk", s(sk));
        if let Some(id) = first_alert_id {
            del = del
                .condition_expression("first_alert_id = :id")
                .expression_attribute_values(":id", s(id));
        }
        match del.send().await {
            Ok(_) => Ok(()),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
            {
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

// ─── Words ──────────────────────────────────────────────────────────────────

fn utc(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| "earlier".to_string())
}

/// Pure: "45 min", "3 hours", "2 days".
pub fn span(secs: u64) -> String {
    let m = (secs + 30) / 60;
    if m < 1 {
        return "under a minute".into();
    }
    if m < 90 {
        return format!("{m} min");
    }
    let h = (m + 30) / 60;
    if h < 48 {
        return format!("{h} hours");
    }
    format!("{} days", (h + 12) / 24)
}

fn times(n: u64) -> String {
    if n == 1 {
        "once".into()
    } else {
        format!("{n} times")
    }
}

/// Pure: the reminder card's line: still firing, since when, how often.
pub fn reminder_line(ep: &Episode, now: i64) -> String {
    format!(
        "Still firing. It started {} ({} ago) and has fired {} since. One reminder a day until it clears.",
        utc(ep.first_at),
        span(now.saturating_sub(ep.first_at).max(0) as u64),
        times(ep.fires),
    )
}

/// Pure: the line on a new episode's card when a run for the alert started
/// in the last few hours, so no new run starts.
pub fn reopened_without_run_line() -> &'static str {
    "Fired again after it cleared. A fix for this alert started a few hours ago, so no new run."
}

/// Pure: the recovery card's line for an episode that really cleared.
pub fn cleared_line(ep: &Episode, opened: &crate::alert_notify::OpenAlert, base: &str) -> String {
    let link = crate::alert_notify::recovered_line(opened, base);
    format!(
        "{link} It stayed clear for {}; it fired {} while open.",
        span(ep.quiet_secs),
        times(ep.fires),
    )
}

// ─── Checks after a wait ────────────────────────────────────────────────────

/// What the worker checks later: the quiet window of an OK, or a day since an
/// episode last posted.
pub const CHECK_CLEAR: &str = "clear";
pub const CHECK_REMIND: &str = "remind";

/// Queue a check for `secs` from now (chained past SQS's 15 minute limit).
/// Best effort: false when it couldn't be queued.
pub async fn schedule_check(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    mut msg: crate::messages::AlertEpisodeCheckMessage,
    secs: u64,
) -> bool {
    if queue_url.is_empty() {
        return false;
    }
    msg.delay_remaining_secs = secs;
    let delay = secs.min(SQS_MAX_DELAY_SECS);
    let body = match serde_json::to_string(&crate::messages::WorkerMessage::AlertEpisodeCheck(msg))
    {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "Could not serialize an alert episode check");
            return false;
        }
    };
    match sqs
        .send_message()
        .queue_url(queue_url)
        .message_body(body)
        .delay_seconds(delay as i32)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(error = %e, "Could not queue an alert episode check");
            false
        }
    }
}

/// What posting a card needs: the settings table, the key that opens saved
/// webhooks, a poster and the dashboard's address.
pub struct Posting<'a> {
    pub dynamo: &'a DynamoClient,
    pub table: &'a str,
    pub cipher: &'a dyn Cipher,
    pub poster: &'a dyn Poster,
    pub base: &'a str,
}

/// The route's item for `topic_arn` when it belongs to `team_id`.
pub async fn route_item(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    topic_arn: &str,
) -> Option<std::collections::HashMap<String, AttributeValue>> {
    let item = dynamo
        .get_item()
        .table_name(table)
        .key("pk", s(&format!("ALERTTOPIC#{topic_arn}")))
        .key("sk", s("ROUTE"))
        .send()
        .await
        .ok()?
        .item()
        .cloned()?;
    (item
        .get("team_id")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        == Some(team_id))
    .then_some(item)
}

fn flag(item: &std::collections::HashMap<String, AttributeValue>, k: &str) -> bool {
    item.get(k)
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false)
}

async fn post(
    p: &Posting<'_>,
    team_id: &str,
    topic_arn: &str,
    card: crate::alert_notify::AlertCard<'_>,
) -> bool {
    let channels =
        crate::alert_notify::notify_target(p.dynamo, p.cipher, p.table, team_id, topic_arn).await;
    if channels.is_empty() {
        return false;
    }
    let msg = crate::alert_notify::alert_message(&card);
    notify::deliver_recorded(
        p.poster, &channels, &msg, "alert", p.dynamo, p.table, team_id,
    )
    .await
    .any_ok()
}

/// An episode ended: close its open card and, on a route that posts
/// recoveries, post one recovery card linking it. The OK's own record says
/// whether it posted. Returns whether a card went out.
pub async fn post_cleared(p: &Posting<'_>, team_id: &str, ended: &Episode) -> bool {
    let opened =
        crate::alert_notify::close_open_alert(p.dynamo, p.table, team_id, &ended.fingerprint).await;
    let route = route_item(p.dynamo, p.table, team_id, &ended.topic_arn).await;
    let posts = route
        .as_ref()
        .is_some_and(|r| flag(r, "enabled") && flag(r, "post_recoveries"));
    let mut sent = false;
    if let (Some(o), true) = (&opened, posts) {
        let line = cleared_line(ended, o, p.base);
        let at = ended.clearing_since.unwrap_or(ended.first_at);
        let context = crate::alert_notify::card_context(&ended.topic_arn, &utc_rfc3339(at));
        let alert_url = format!("{}/alerts/detail?id={}", p.base, ended.clear_event_id);
        let kind = Some(ended.clear_kind.as_str())
            .filter(|k| !k.is_empty())
            .unwrap_or(&ended.kind);
        let console = console_url(&ended.topic_arn, kind, &ended.clear_title);
        sent = post(
            p,
            team_id,
            &ended.topic_arn,
            crate::alert_notify::AlertCard {
                kind,
                title: &ended.clear_title,
                body: &ended.clear_body,
                outcome: &line,
                alert_url: &alert_url,
                console_url: console.as_deref(),
                context: &context,
            },
        )
        .await;
    }
    if !ended.clear_event_id.is_empty() {
        let outcome = if opened.is_some() {
            "recovered"
        } else {
            "no_open_alert"
        };
        let written = p
            .dynamo
            .update_item()
            .table_name(p.table)
            .key("pk", s(team_id))
            .key("sk", s(&format!("ALERT_EVENT#{}", ended.clear_event_id)))
            .update_expression("SET outcome = :o")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_values(":o", s(outcome))
            .send()
            .await;
        if let Err(e) = written {
            if !e
                .as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception())
            {
                warn!(team_id, error = %e, "Could not mark the recovery on its record");
            }
        }
    }
    sent
}

/// Post the daily reminder for a still open episode: the alert's own card,
/// saying it is still firing, since when and how often.
pub async fn post_reminder(p: &Posting<'_>, team_id: &str, ep: &Episode, now: i64) -> bool {
    let line = reminder_line(ep, now);
    let context = crate::alert_notify::card_context(&ep.topic_arn, &utc_rfc3339(now));
    let alert_url = format!("{}/alerts/detail?id={}", p.base, ep.last_event_id);
    let console = console_url(&ep.topic_arn, &ep.kind, &ep.title);
    post(
        p,
        team_id,
        &ep.topic_arn,
        crate::alert_notify::AlertCard {
            kind: &ep.kind,
            title: &ep.title,
            body: &ep.body,
            outcome: &line,
            alert_url: &alert_url,
            console_url: console.as_deref(),
            context: &context,
        },
    )
    .await
}

fn utc_rfc3339(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default()
}

fn console_url(topic_arn: &str, kind: &str, title: &str) -> Option<String> {
    let region = topic_arn.split(':').nth(3).unwrap_or("");
    alarm_name(kind, title).and_then(|n| crate::alert_notify::cloudwatch_console_url(region, n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const T0: i64 = 1_790_000_000;
    const MIN: i64 = 60;
    const HOUR: i64 = 3600;
    const TEAM: &str = "TEAM#t";
    const TOPIC: &str = "arn:aws:sns:us-east-1:111122223333:example-alarms";
    const FP: &str = "arn:aws:cloudwatch:us-east-1:111122223333:alarm:example-queue-depth";

    fn fire(id: &str, at: i64) -> Input<'_> {
        Input::Fire {
            event_id: id,
            at,
            kind: "cloudwatch_alarm",
            title: "ALARM: example-queue-depth",
            body: "- Reason: Threshold crossed",
        }
    }

    fn clear(id: &str, at: i64) -> Input<'_> {
        Input::Clear {
            event_id: id,
            at,
            quiet_secs: CLEAR_QUIET_MIN_SECS,
            kind: "cloudwatch_alarm",
            title: "OK: example-queue-depth",
            body: "- Reason: back under",
        }
    }

    /// Feeds inputs through `step` as `apply` would, returning each decision.
    fn run(mode: Repeat, inputs: &[Input]) -> (Vec<Decision>, Option<Episode>) {
        let mut ep: Option<Episode> = None;
        let mut out = vec![];
        for i in inputs {
            let (d, next) = step(ep.as_ref(), *i, mode);
            if next.is_some() {
                ep = next;
            }
            out.push(d);
        }
        (out, ep)
    }

    fn posts(ds: &[Decision]) -> usize {
        ds.iter()
            .filter(|d| {
                matches!(
                    d,
                    Decision::Opened | Decision::Reopened(_) | Decision::Remind
                )
            })
            .count()
    }

    #[test]
    fn a_flapping_alarm_inside_the_quiet_window_posts_once() {
        let (ds, ep) = run(
            Repeat::UntilClear,
            &[
                fire("a1", T0),
                clear("o1", T0 + 3 * MIN),
                fire("a2", T0 + 6 * MIN),
                clear("o2", T0 + 9 * MIN),
                fire("a3", T0 + 12 * MIN),
                // The check of the first OK finds its window undone.
                Input::ClearCheck {
                    clear_event_id: "o1",
                    now: T0 + 40 * MIN,
                },
            ],
        );
        assert_eq!(posts(&ds), 1, "{ds:?}");
        assert_eq!(ds[0], Decision::Opened);
        assert_eq!(ds[2], Decision::Suppressed);
        assert_eq!(ds[5], Decision::Nothing);
        let ep = ep.unwrap();
        assert!(ep.open && ep.clearing_since.is_none());
        assert_eq!(ep.fires, 3);
        assert_eq!(ep.first_alert_id, "a1");
    }

    #[test]
    fn a_real_clear_ends_the_episode_once_and_the_next_alarm_posts_at_once() {
        let (ds, ep) = run(
            Repeat::UntilClear,
            &[
                fire("a1", T0),
                clear("o1", T0 + 5 * MIN),
                // A second OK in the window doesn't start a second check.
                clear("o1b", T0 + 6 * MIN),
                // Checked too early (SQS delays are approximate): wait more.
                Input::ClearCheck {
                    clear_event_id: "o1",
                    now: T0 + 20 * MIN,
                },
                Input::ClearCheck {
                    clear_event_id: "o1",
                    now: T0 + 35 * MIN,
                },
                // The same check delivered twice ends nothing more.
                Input::ClearCheck {
                    clear_event_id: "o1",
                    now: T0 + 36 * MIN,
                },
                fire("a2", T0 + 50 * MIN),
            ],
        );
        assert_eq!(ds[1], Decision::Clearing { schedule: true });
        assert_eq!(ds[2], Decision::Clearing { schedule: false });
        assert_eq!(ds[3], Decision::Wait(15 * 60));
        let Decision::Cleared(ended) = &ds[4] else {
            panic!("{ds:?}")
        };
        assert_eq!(ended.clear_event_id, "o1");
        assert_eq!(ended.fires, 1);
        assert_eq!(ds[5], Decision::Nothing);
        assert_eq!(ds[6], Decision::Opened);
        let ep = ep.unwrap();
        assert_eq!(ep.first_alert_id, "a2");
        assert_eq!(ep.fires, 1);
    }

    #[test]
    fn an_alarm_after_the_window_ends_the_old_episode_even_before_its_check() {
        let (ds, ep) = run(
            Repeat::UntilClear,
            &[
                fire("a1", T0),
                clear("o1", T0 + MIN),
                fire("a2", T0 + 45 * MIN),
            ],
        );
        let Decision::Reopened(old) = &ds[2] else {
            panic!("{ds:?}")
        };
        assert_eq!(old.first_alert_id, "a1");
        assert_eq!(ep.as_ref().unwrap().first_alert_id, "a2");
        // The late check then finds nothing to end.
        let (d, _) = step(
            ep.as_ref(),
            Input::ClearCheck {
                clear_event_id: "o1",
                now: T0 + 50 * MIN,
            },
            Repeat::UntilClear,
        );
        assert_eq!(d, Decision::Nothing);
    }

    #[test]
    fn daily_reminds_once_per_24_hours_while_open_and_never_after_clear() {
        let mut inputs = vec![fire("a0", T0)];
        let ids: Vec<String> = (1..=60).map(|i| format!("a{i}")).collect();
        // Fires every hour for 60 hours.
        for (i, id) in ids.iter().enumerate() {
            inputs.push(fire(id, T0 + (i as i64 + 1) * HOUR));
        }
        let (ds, ep) = run(Repeat::Daily, &inputs);
        let reminders: Vec<usize> = ds
            .iter()
            .enumerate()
            .filter(|(_, d)| **d == Decision::Remind)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(reminders, vec![24, 48], "one at 24h and one at 48h");
        let ep = ep.unwrap();
        assert_eq!(ep.reminders, 2);
        assert_eq!(ep.fires, 61);

        // A stuck alarm (no new fires) is reminded by the scheduled check,
        // once: a second check for the same post finds it moved on.
        let (ds, ep) = run(
            Repeat::Daily,
            &[
                fire("a1", T0),
                Input::RemindCheck {
                    notified_at: T0,
                    now: T0 + 10 * HOUR,
                },
                Input::RemindCheck {
                    notified_at: T0,
                    now: T0 + 24 * HOUR,
                },
                Input::RemindCheck {
                    notified_at: T0,
                    now: T0 + 25 * HOUR,
                },
            ],
        );
        assert_eq!(ds[1], Decision::Wait(14 * 3600));
        assert_eq!(ds[2], Decision::Remind);
        assert_eq!(ds[3], Decision::Nothing);
        assert_eq!(ep.unwrap().last_notified_at, T0 + 24 * HOUR);

        // After a real clear: no reminder, whatever arrives.
        let (ds, _) = run(
            Repeat::Daily,
            &[
                fire("a1", T0),
                clear("o1", T0 + HOUR),
                Input::ClearCheck {
                    clear_event_id: "o1",
                    now: T0 + 2 * HOUR,
                },
                Input::RemindCheck {
                    notified_at: T0,
                    now: T0 + 30 * HOUR,
                },
            ],
        );
        assert!(matches!(ds[2], Decision::Cleared(_)));
        assert_eq!(ds[3], Decision::Nothing);
        // Until clear never reminds.
        let (ds, _) = run(
            Repeat::UntilClear,
            &[
                fire("a1", T0),
                fire("a2", T0 + 30 * HOUR),
                Input::RemindCheck {
                    notified_at: T0,
                    now: T0 + 30 * HOUR,
                },
            ],
        );
        assert_eq!(ds[1], Decision::Suppressed);
        assert_eq!(ds[2], Decision::Nothing);
    }

    #[test]
    fn a_redelivery_counts_once() {
        let (ds, ep) = run(
            Repeat::UntilClear,
            &[
                fire("a1", T0),
                fire("a1", T0),
                fire("a2", T0 + MIN),
                fire("a2", T0 + MIN),
            ],
        );
        assert_eq!(ds[1], Decision::Redelivered);
        assert_eq!(ds[3], Decision::Redelivered);
        assert_eq!(ep.unwrap().fires, 2);
    }

    #[test]
    fn an_ok_with_nothing_open_changes_nothing() {
        let (ds, ep) = run(Repeat::UntilClear, &[clear("o1", T0)]);
        assert_eq!(ds, vec![Decision::Nothing]);
        assert!(ep.is_none());
    }

    #[test]
    fn the_quiet_window_is_two_of_the_alarms_spans_within_bounds() {
        let msg = |p: u64, e: u64| {
            serde_json::json!({"AlarmName": "x", "NewStateValue": "OK",
                "Trigger": {"Period": p, "EvaluationPeriods": e}})
            .to_string()
        };
        // 1 minute x 3: six minutes is under the floor.
        assert_eq!(quiet_window_secs(&msg(60, 3)), CLEAR_QUIET_MIN_SECS);
        // 15 minutes x 2 = 30 min span, two of them = an hour.
        assert_eq!(quiet_window_secs(&msg(900, 2)), 3600);
        // A daily alarm is capped.
        assert_eq!(quiet_window_secs(&msg(86_400, 1)), CLEAR_QUIET_MAX_SECS);
        assert_eq!(quiet_window_secs("plain text"), CLEAR_QUIET_MIN_SECS);
    }

    #[test]
    fn an_alarms_own_setting_beats_the_routes() {
        let o = BTreeMap::from([
            ("example-queue-depth".to_string(), Repeat::Daily),
            ("example-disk".to_string(), Repeat::EveryTime),
        ]);
        let r = Repeat::UntilClear;
        assert_eq!(
            effective(r, &o, "cloudwatch_alarm", "ALARM: example-queue-depth"),
            Repeat::Daily
        );
        assert_eq!(
            effective(r, &o, "cloudwatch_alarm", "OK: example-queue-depth"),
            Repeat::Daily,
            "its OK finds the same setting"
        );
        assert_eq!(
            effective(r, &o, "cloudwatch_alarm", "ALARM: example-disk"),
            Repeat::EveryTime
        );
        assert_eq!(
            effective(r, &o, "cloudwatch_alarm", "ALARM: example-cpu"),
            Repeat::UntilClear
        );
        assert_eq!(effective(r, &o, "message", "Nightly load failed"), r);
        // Alerts that never clear post every time.
        assert_eq!(effective(r, &o, "eventbridge", "x"), Repeat::EveryTime);
        assert_eq!(effective(r, &o, "chatbot_custom", "x"), Repeat::EveryTime);
    }

    #[test]
    fn routes_saved_before_the_setting_post_every_time() {
        let (r, o) = route_setting(&std::collections::HashMap::new());
        assert_eq!(r, Repeat::EveryTime);
        assert!(o.is_empty());
        assert_eq!(Repeat::parse(""), Some(Repeat::EveryTime));
        assert_eq!(Repeat::parse("weekly"), None);
        let item = std::collections::HashMap::from([
            ("repeat".to_string(), s("daily")),
            (
                "alarm_repeat".to_string(),
                AttributeValue::M(std::collections::HashMap::from([
                    ("a".to_string(), s("until_clear")),
                    ("bad".to_string(), s("weekly")),
                ])),
            ),
        ]);
        let (r, o) = route_setting(&item);
        assert_eq!(r, Repeat::Daily);
        assert_eq!(o, BTreeMap::from([("a".to_string(), Repeat::UntilClear)]));
    }

    #[test]
    fn episodes_round_trip_through_their_item() {
        let (_, ep) = run(Repeat::Daily, &[fire("a1", T0), clear("o1", T0 + MIN)]);
        let mut ep = ep.unwrap();
        ep.topic_arn = TOPIC.into();
        ep.fingerprint = FP.into();
        let item: std::collections::HashMap<String, AttributeValue> = to_item(&ep)
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        assert_eq!(from_item(&item), ep);
        assert_ne!(episode_sk(TOPIC, FP), episode_sk("arn:other", FP));
    }

    #[test]
    fn the_reminder_says_since_when_and_how_often() {
        let ep = Episode {
            first_at: 1_793_871_120, // 2026-11-05 09:32 UTC
            fires: 14,
            ..Episode::default()
        };
        assert_eq!(
            reminder_line(&ep, 1_793_871_120 + 25 * 3600),
            "Still firing. It started 2026-11-05 09:32 UTC (25 hours ago) and has fired 14 times since. One reminder a day until it clears."
        );
        assert_eq!(span(45 * 60), "45 min");
        assert_eq!(span(3 * 86_400), "3 days");
    }

    /// An in-memory store whose reads wait for each other, so two callers
    /// both read before either writes: the race SNS can cause.
    struct Racing {
        items: Mutex<std::collections::HashMap<String, Episode>>,
        barrier: tokio::sync::Barrier,
        first_reads: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl EpisodeStore for Racing {
        async fn get(&self, _t: &str, sk: &str) -> Result<Option<Episode>, String> {
            let v = self.items.lock().unwrap().get(sk).cloned();
            if self
                .first_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                < 2
            {
                self.barrier.wait().await;
            }
            Ok(v)
        }
        async fn put(
            &self,
            _t: &str,
            sk: &str,
            ep: &Episode,
            expect: Option<u64>,
        ) -> Result<bool, String> {
            let mut items = self.items.lock().unwrap();
            if items.get(sk).map(|e| e.ver) != expect {
                return Ok(false);
            }
            items.insert(sk.to_string(), ep.clone());
            Ok(true)
        }
        async fn delete(&self, _t: &str, sk: &str, first: Option<&str>) -> Result<(), String> {
            let mut items = self.items.lock().unwrap();
            if first.is_none() || items.get(sk).map(|e| e.first_alert_id.as_str()) == first {
                items.remove(sk);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn two_deliveries_at_once_post_once() {
        let store = Racing {
            items: Mutex::new(Default::default()),
            barrier: tokio::sync::Barrier::new(2),
            first_reads: Default::default(),
        };
        let (a, b) = tokio::join!(
            apply(&store, TEAM, TOPIC, FP, fire("a1", T0), Repeat::UntilClear),
            apply(
                &store,
                TEAM,
                TOPIC,
                FP,
                fire("a2", T0 + 1),
                Repeat::UntilClear
            ),
        );
        let ds = [a.unwrap().decision, b.unwrap().decision];
        assert_eq!(
            ds.iter().filter(|d| **d == Decision::Opened).count(),
            1,
            "{ds:?}"
        );
        assert!(ds.contains(&Decision::Suppressed), "{ds:?}");
        let ep = store
            .get(TEAM, &episode_sk(TOPIC, FP))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ep.fires, 2);
        assert_eq!(ep.topic_arn, TOPIC);
        assert_eq!(ep.fingerprint, FP);
    }

    #[tokio::test]
    async fn a_failed_start_forgets_the_episode_so_the_retry_opens_it() {
        let store = Racing {
            items: Mutex::new(Default::default()),
            barrier: tokio::sync::Barrier::new(1),
            first_reads: std::sync::atomic::AtomicUsize::new(2),
        };
        let a = apply(&store, TEAM, TOPIC, FP, fire("a1", T0), Repeat::UntilClear)
            .await
            .unwrap();
        assert_eq!(a.decision, Decision::Opened);
        let sk = episode_sk(TOPIC, FP);
        // Another alert's rollback leaves it alone.
        store.delete(TEAM, &sk, Some("zz")).await.unwrap();
        assert!(store.get(TEAM, &sk).await.unwrap().is_some());
        store.delete(TEAM, &sk, Some("a1")).await.unwrap();
        let retry = apply(&store, TEAM, TOPIC, FP, fire("a1", T0), Repeat::UntilClear)
            .await
            .unwrap();
        assert_eq!(retry.decision, Decision::Opened);
    }
}
