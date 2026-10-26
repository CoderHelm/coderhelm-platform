//! Outbound webhooks: CoderHelm tells a team's own systems what it did.
//!
//! - [`catalog`]: the event types and the shape of each one's `data`.
//! - [`event`]: the envelope and [`emit_event`], which every code path that
//!   does something calls. It only queues the event.
//! - [`endpoint`]: where events go (an admin adds them in Settings, Webhooks
//!   or through the API), with sealed signing secrets.
//! - [`sign`]: Standard Webhooks signatures.
//! - [`deliver`]: one signed request, the address checks, retries and the
//!   failing-endpoint rule.
//!
//! Every attempt is recorded in Deliveries (`crate::deliveries`, source
//! `webhook`), so the Deliveries page and an endpoint's own list read the
//! same records.
//!
//! The worker takes events off the webhooks queue ([`dispatch`]), keeps
//! them for replay, and sends each to the endpoints that want it. A failed
//! attempt queues the next one ([`crate::messages::WebhookSendMessage`]).

pub mod catalog;
pub mod deliver;
pub mod endpoint;
pub mod event;
pub mod sign;

pub use event::{emit_event, emit_event_keyed, Event};

use crate::deliveries::{self, Delivery, Status};
use crate::mcp_credentials::Cipher;
use deliver::Next;

/// What sending needs: the settings table, KMS for the secrets, the queue
/// retries go on, and DNS.
pub struct Ctx<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
    pub cipher: &'a dyn Cipher,
    pub sqs: &'a aws_sdk_sqs::Client,
    pub queue_url: &'a str,
    pub dns: &'a dyn deliver::Resolve,
}

/// Who asked for an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// An event as it happened, and its retries: failures retry, count
    /// toward turning the endpoint off.
    Automatic,
    /// A person's Resend or test: tried once; a success still clears the
    /// endpoint's failure record.
    Manual,
}

/// The end of one attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outcome {
    pub next: Next,
    /// This attempt turned the endpoint off (tell the admins once).
    pub disabled_now: bool,
}

fn now_text(now: chrono::DateTime<chrono::Utc>) -> String {
    now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Sends `body` (the event `event_id`) to one endpoint once, records the
/// attempt on `d`, keeps the endpoint's health, and queues the next try.
pub async fn attempt(
    ctx: &Ctx<'_>,
    team_id: &str,
    st: &endpoint::Stored,
    event_id: &str,
    body: &str,
    d: &mut Delivery,
    mode: Mode,
) -> Outcome {
    let now = chrono::Utc::now();
    let ep = &st.endpoint;
    let stop = Outcome {
        next: Next::Stop,
        disabled_now: false,
    };
    if mode == Mode::Automatic && !ep.enabled {
        d.finish(Status::Failed, "Endpoint is off", "webhook_disabled");
        d.next_retry_at = None;
        deliveries::record(ctx.dynamo, ctx.table, team_id, d).await;
        return stop;
    }
    let secrets = match endpoint::signing_secrets(ctx.cipher, team_id, st, now).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(endpoint = %ep.id, error = %e, "Could not open a webhook signing secret");
            d.push_attempt(deliveries::Attempt {
                at: now_text(now),
                code: None,
                duration_ms: 0,
                error: "The signing secret couldn't be read, so nothing was sent.".into(),
            });
            d.finish(Status::Failed, "Not sent", "handler_error");
            deliveries::record(ctx.dynamo, ctx.table, team_id, d).await;
            return stop;
        }
    };
    let sender = deliver::Sender::new(ctx.dns);
    let sent = deliver::send(
        &sender,
        &deliver::Request {
            url: &ep.url,
            event_id,
            body: body.as_bytes(),
            secrets: &secrets,
            timestamp: now.timestamp(),
        },
    )
    .await;
    deliver::record_headers(d, &sent);
    let next = deliver::apply(d, &sent, mode == Mode::Automatic, now);
    let at = now_text(now);
    let mut disabled_now = false;
    match next {
        Next::Delivered => {
            endpoint::note_success(ctx.dynamo, ctx.table, team_id, &ep.id, &at).await;
        }
        _ if mode == Mode::Automatic => {
            if let Some(since) =
                endpoint::note_failure(ctx.dynamo, ctx.table, team_id, &ep.id, &at).await
            {
                if deliver::should_disable(&since, now) {
                    disabled_now =
                        endpoint::disable_failing(ctx.dynamo, ctx.table, team_id, &ep.id, &at)
                            .await;
                }
            }
        }
        _ => {}
    }
    let next = match next {
        Next::RetryIn(_) if disabled_now => {
            d.finish(Status::Failed, "Endpoint turned off", "webhook_disabled");
            d.next_retry_at = None;
            Next::Stop
        }
        n => n,
    };
    deliveries::record(ctx.dynamo, ctx.table, team_id, d).await;
    if let Next::RetryIn(secs) = next {
        let msg = crate::messages::WebhookSendMessage {
            team_id: team_id.into(),
            endpoint_id: ep.id.clone(),
            event_id: event_id.into(),
            delivery_id: d.id.clone(),
            due_at: now.timestamp() + secs,
        };
        if let Err(e) = schedule(ctx, &msg, secs).await {
            tracing::warn!(delivery = %d.id, error = %e, "Could not queue a webhook retry");
        }
    }
    Outcome { next, disabled_now }
}

/// Queues a send, delayed by `secs` (SQS delays at most 15 minutes; a later
/// one is carried by `due_at` and queued again until it's due).
pub async fn schedule(
    ctx: &Ctx<'_>,
    msg: &crate::messages::WebhookSendMessage,
    secs: i64,
) -> Result<(), String> {
    let body = serde_json::to_string(&crate::messages::WorkerMessage::WebhookSend(msg.clone()))
        .map_err(|e| e.to_string())?;
    ctx.sqs
        .send_message()
        .queue_url(ctx.queue_url)
        .message_body(body)
        .delay_seconds(sqs_delay(secs))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| aws_sdk_sqs::error::DisplayErrorContext(&e).to_string())
}

/// The SQS delay for a wait: 0 to 900 seconds.
pub fn sqs_delay(secs: i64) -> i32 {
    secs.clamp(0, 900) as i32
}

/// Takes one emitted event: keeps it (when the team has any endpoint) and
/// sends it to every endpoint that wants it. Returns the endpoints that
/// this turned off. A second copy of the same message does nothing.
pub async fn dispatch(ctx: &Ctx<'_>, e: &Event) -> Result<Vec<endpoint::Endpoint>, String> {
    // Events built outside `Event::new` (the log analyzer's) are scrubbed
    // here too: nothing that looks like a credential is ever sent.
    let mut e = e.clone();
    e.data = event::scrub(&e.data);
    let e = &e;
    let all = endpoint::list(ctx.dynamo, ctx.table, &e.team_id).await?;
    if all.is_empty() {
        return Ok(vec![]);
    }
    let body = e.body();
    if !event::keep(ctx.dynamo, ctx.table, e, &body).await? {
        tracing::info!(event = %e.id, "Webhook event already taken; not sending it twice");
        return Ok(vec![]);
    }
    let wanting: Vec<&endpoint::Stored> = all
        .iter()
        .filter(|s| s.endpoint.wants(&e.event_type))
        .collect();
    let sends = wanting.iter().map(|st| {
        let body = body.clone();
        async move {
            let mut d = deliver::new_record(&st.endpoint, &e.id, &e.event_type, body.as_bytes());
            let out = attempt(ctx, &e.team_id, st, &e.id, &body, &mut d, Mode::Automatic).await;
            (st.endpoint.clone(), out)
        }
    });
    Ok(futures::future::join_all(sends)
        .await
        .into_iter()
        .filter(|(_, o)| o.disabled_now)
        .map(|(ep, _)| ep)
        .collect())
}

/// Queues kept events to one endpoint again (replay), each as a new
/// delivery. Returns how many were queued.
pub async fn replay(
    ctx: &Ctx<'_>,
    team_id: &str,
    ep: &endpoint::Endpoint,
    events: &[event::Kept],
) -> Result<usize, String> {
    let now = chrono::Utc::now().timestamp();
    let mut queued = 0;
    for chunk in events.chunks(10) {
        let mut entries = Vec::new();
        for (i, k) in chunk.iter().enumerate() {
            let mut d =
                deliver::new_record(ep, &k.event.id, &k.event.event_type, k.body.as_bytes());
            d.outcome = "Queued (replay)".into();
            deliveries::record(ctx.dynamo, ctx.table, team_id, &d).await;
            let msg =
                crate::messages::WorkerMessage::WebhookSend(crate::messages::WebhookSendMessage {
                    team_id: team_id.into(),
                    endpoint_id: ep.id.clone(),
                    event_id: k.event.id.clone(),
                    delivery_id: d.id.clone(),
                    due_at: now,
                });
            let body = serde_json::to_string(&msg).map_err(|e| e.to_string())?;
            entries.push(
                aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                    .id(i.to_string())
                    .message_body(body)
                    .build()
                    .map_err(|e| e.to_string())?,
            );
        }
        let r = ctx
            .sqs
            .send_message_batch()
            .queue_url(ctx.queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|e| aws_sdk_sqs::error::DisplayErrorContext(&e).to_string())?;
        queued += r.successful().len();
    }
    Ok(queued)
}

/// The event types a replay should send to `ep`: what it subscribes to now.
pub fn replayable<'a>(ep: &endpoint::Endpoint, events: &'a [event::Kept]) -> Vec<&'a event::Kept> {
    events
        .iter()
        .filter(|k| {
            k.event.event_type != catalog::TEST_EVENT
                && ep
                    .events
                    .iter()
                    .any(|t| t == "*" || *t == k.event.event_type)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn source(rel: &str) -> String {
        std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join(rel),
        )
        .unwrap()
    }

    /// No log line in the webhook code names a secret's value: secrets are
    /// only ever sealed, sent once to the person who made them, or used to
    /// sign.
    #[test]
    fn signing_secrets_are_never_logged() {
        for f in [
            "common/src/webhooks/mod.rs",
            "common/src/webhooks/endpoint.rs",
            "common/src/webhooks/deliver.rs",
            "common/src/webhooks/sign.rs",
            "common/src/webhooks/event.rs",
            "gateway/src/routes/webhooks.rs",
            "worker/src/passes/webhooks.rs",
        ] {
            let src = source(f);
            let code = src.split("#[cfg(test)]").next().unwrap();
            let calls = ["tracing::", "warn!(", "info!(", "error!(", "debug!("]
                .iter()
                .flat_map(|m| code.split(m).skip(1));
            for (i, chunk) in calls.enumerate() {
                // The call ends at its statement or its closing parenthesis.
                let end = [";", ")\n", "),"]
                    .iter()
                    .filter_map(|t| chunk.find(t))
                    .min()
                    .unwrap_or(chunk.len());
                let call = &chunk[..end];
                for bad in [
                    "secret =", "%secret", "?secret", "secrets", "{secret}", "sealed",
                ] {
                    assert!(
                        !call.contains(bad),
                        "{f}: log call {i} may print a secret: {call}"
                    );
                }
            }
        }
    }

    /// Every endpoint route reads and writes only the caller's team, and
    /// only admins (and keys with the scope) reach it.
    #[test]
    fn every_endpoint_route_is_scoped_to_the_callers_team_and_role() {
        let src = source("gateway/src/routes/webhooks.rs");
        let code = src.split("#[cfg(test)]").next().unwrap();
        let mut n = 0;
        for chunk in code.split("\npub async fn ").skip(1) {
            let name = chunk.split('(').next().unwrap();
            if !chunk.contains("Extension(claims): Extension<Claims>") {
                continue;
            }
            n += 1;
            let body = chunk.split("\n}\n").next().unwrap();
            assert!(
                body.contains("claims.require_role(3)"),
                "{name} has no admin check"
            );
            if name != "event_types" {
                assert!(
                    body.contains("claims.team_id"),
                    "{name} doesn't scope to the caller's team"
                );
            }
            assert!(
                !body.contains("team_id: String") && !body.contains("q.team"),
                "{name} takes a team from the request"
            );
        }
        assert_eq!(n, 12, "every endpoint route checked");
        // A delivery is resent only through the endpoint it belongs to.
        assert!(code.contains(".filter(|d| d.endpoint_id == endpoint_id)"));
    }

    #[test]
    fn replays_send_only_what_the_endpoint_subscribes_to() {
        let ep = endpoint::Endpoint {
            id: "ep_1".into(),
            events: vec!["run.failed".into()],
            enabled: false,
            ..Default::default()
        };
        let kept = |t: &str| {
            let e = Event::new("T", t, json!({}));
            event::Kept {
                body: e.body(),
                event: e,
            }
        };
        let all = vec![
            kept("run.failed"),
            kept("run.completed"),
            kept(catalog::TEST_EVENT),
        ];
        let picked = replayable(&ep, &all);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].event.event_type, "run.failed");
        let every = endpoint::Endpoint {
            events: vec!["*".into()],
            ..ep
        };
        assert_eq!(replayable(&every, &all).len(), 2, "never the test event");
    }

    #[test]
    fn long_waits_are_carried_past_the_sqs_limit() {
        assert_eq!(sqs_delay(5), 5);
        assert_eq!(sqs_delay(28_800), 900);
        assert_eq!(sqs_delay(-3), 0);
    }
}
