//! Hosted side of hybrid: try a relay forward again.
//!
//! The gateway forwards each event once as it arrives; when that fails it
//! holds the body in the data bucket and queues this job. Each run sends the
//! held body to the install (re-signed now, so the signature is fresh), notes
//! the attempt on the delivery record, and either queues the next try
//! (common::deliveries::next_retry_secs) or stops: delivered, refused for good,
//! or about 24 hours of failures. The held body is deleted when it stops.

use aws_sdk_dynamodb::types::AttributeValue;
use common::hybrid::{relay, sign};
use common::messages::RelayRetryMessage;

use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// The install's CloudFront address, when it is active and verified.
async fn install_origin(state: &WorkerState, install_id: &str) -> Result<Option<String>, Error> {
    let r = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(relay::install_pk(install_id)))
        .key("sk", AttributeValue::S("INSTALL".into()))
        .send()
        .await?;
    let Some(item) = r.item() else {
        return Ok(None);
    };
    let g = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    if g("status") != "active" || g("verified_at").is_empty() || g("cloudfront").is_empty() {
        return Ok(None);
    }
    Ok(Some(format!("https://{}", g("cloudfront"))))
}

/// Whether an event created at `created` came under the link made at
/// `linked_at` (both RFC 3339, compared as times, not text).
pub fn link_covers(linked_at: &str, created: &str) -> bool {
    let t = |v: &str| chrono::DateTime::parse_from_rfc3339(v).ok();
    match (t(linked_at), t(created)) {
        // The link's time has whole seconds; an event in its first second counts.
        (Some(l), Some(c)) => l.timestamp() <= c.timestamp(),
        _ => false,
    }
}

pub async fn run(state: &WorkerState, msg: RelayRetryMessage) -> Result<(), Error> {
    let Some(master) = state
        .secrets
        .hybrid_master_key
        .as_deref()
        .filter(|k| common::hybrid::is_hosted_side(Some(k)))
    else {
        // Only the hosted deployment relays.
        return Ok(());
    };
    let table = &state.config.settings_table_name;
    let pk = relay::install_pk(&msg.install_id);
    let Some(mut d) = common::deliveries::get(&state.dynamo, table, &pk, &msg.delivery_id).await?
    else {
        return Ok(());
    };
    if d.status.is_ok() || d.status == common::deliveries::Status::Failed {
        return Ok(());
    }
    let key = relay::payload_key(&d.id);
    let drop_body = || async {
        let _ = state
            .s3
            .delete_object()
            .bucket(&state.config.bucket_name)
            .key(relay::payload_key(&msg.delivery_id))
            .send()
            .await;
    };
    let Some(origin) = install_origin(state, &msg.install_id).await? else {
        d.finish(
            common::deliveries::Status::Failed,
            "Not linked",
            "relay_not_linked",
        );
        d.next_retry_at = None;
        common::deliveries::record(&state.dynamo, table, &pk, &d).await;
        drop_body().await;
        return Ok(());
    };
    // A linked Jira site's event goes only while the link it came under
    // stands: unlinked or relinked meanwhile, it isn't sent.
    if msg.source == "jira" && (msg.event == "site" || msg.event == "site_webhook") {
        let link = state
            .dynamo
            .get_item()
            .table_name(table)
            .key("pk", AttributeValue::S(pk.clone()))
            .key("sk", AttributeValue::S("JIRALINK".into()))
            .consistent_read(true)
            .send()
            .await?;
        let linked_at = link
            .item()
            .and_then(|i| i.get("linked_at"))
            .and_then(|v| v.as_s().ok())
            .cloned();
        // Gone, or a newer link (perhaps another team's site) than the event.
        let linked = linked_at.is_some_and(|at| link_covers(&at, &d.created_at));
        if !linked {
            d.finish(
                common::deliveries::Status::Failed,
                "Unlinked before it was delivered",
                "relay_not_linked",
            );
            d.next_retry_at = None;
            common::deliveries::record(&state.dynamo, table, &pk, &d).await;
            drop_body().await;
            return Ok(());
        }
    }
    let body = match state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(&key)
        .send()
        .await
    {
        Ok(o) => o.body.collect().await?.into_bytes().to_vec(),
        Err(_) => {
            d.finish(
                common::deliveries::Status::Failed,
                "The held event is gone",
                "relay_gave_up",
            );
            d.next_retry_at = None;
            common::deliveries::record(&state.dynamo, table, &pk, &d).await;
            return Ok(());
        }
    };
    let relay_key = sign::relay_key(master, &msg.install_id);
    let attempt = relay::send(&relay::Forward {
        origin: &origin,
        relay_key: &relay_key,
        source: &msg.source,
        event: &msg.event,
        delivery_id: &msg.relay_delivery_id,
        body: &body,
    })
    .await;
    match relay::apply(&mut d, attempt) {
        relay::Next::RetryIn(secs) => {
            let next =
                serde_json::to_string(&common::messages::WorkerMessage::RelayRetry(msg.clone()))?;
            state
                .sqs
                .send_message()
                .queue_url(&state.config.ticket_queue_url)
                .message_body(next)
                .delay_seconds(secs.min(900) as i32)
                .send()
                .await?;
        }
        _ => drop_body().await,
    }
    common::deliveries::record(&state.dynamo, table, &pk, &d).await;
    relay::note_last(&state.dynamo, table, &msg.install_id, &d).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::link_covers;

    #[test]
    fn a_queued_jira_retry_goes_only_under_the_link_it_came_under() {
        assert!(link_covers(
            "2026-10-30T10:00:00Z",
            "2026-10-30T10:00:00.500Z"
        ));
        assert!(link_covers(
            "2026-10-30T10:00:00Z",
            "2026-10-30T11:00:00.000Z"
        ));
        // Linked again after the event: not sent.
        assert!(!link_covers(
            "2026-10-30T10:00:01Z",
            "2026-10-30T10:00:00.500Z"
        ));
        assert!(!link_covers("", "2026-10-30T10:00:00.500Z"));
    }
}
