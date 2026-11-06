//! The later half of an alert route's repeat setting
//! (`common::alert_episodes`): once an OK's quiet window has passed with no
//! ALARM, end the episode and post its recovery; once a day has passed since
//! an open episode last posted, post its reminder.

use common::alert_episodes::{self as episodes, Decision, Input};
use common::messages::AlertEpisodeCheckMessage;
use tracing::{info, warn};

use crate::WorkerState;

pub async fn run(state: &WorkerState, msg: AlertEpisodeCheckMessage) {
    // Still waiting: one more hop (a single SQS delay is capped).
    if msg.delay_remaining_secs > episodes::SQS_MAX_DELAY_SECS {
        let rest = msg.delay_remaining_secs - episodes::SQS_MAX_DELAY_SECS;
        episodes::schedule_check(&state.sqs, &state.config.ticket_queue_url, msg, rest).await;
        return;
    }
    let table = &state.config.settings_table_name;
    let Some(route) =
        episodes::route_item(&state.dynamo, table, &msg.team_id, &msg.topic_arn).await
    else {
        info!(team_id = %msg.team_id, "Alert episode check: the route is gone");
        return;
    };
    let store = episodes::DynamoStore {
        client: &state.dynamo,
        table,
    };
    let now = chrono::Utc::now().timestamp();
    let sk = episodes::episode_sk(&msg.topic_arn, &msg.fingerprint);
    // The setting as it is now: a route switched away from daily reminders
    // stops reminding.
    let mode = match <episodes::DynamoStore as episodes::EpisodeStore>::get(
        &store,
        &msg.team_id,
        &sk,
    )
    .await
    {
        Ok(Some(ep)) => {
            let (r, o) = episodes::route_setting(&route);
            episodes::effective(r, &o, &ep.kind, &ep.title)
        }
        Ok(None) => return,
        Err(e) => {
            warn!(team_id = %msg.team_id, error = %e, "Alert episode check: could not read the episode");
            return;
        }
    };
    let notified_at: i64 = msg.token.parse().unwrap_or(-1);
    let input = match msg.check.as_str() {
        episodes::CHECK_CLEAR => Input::ClearCheck {
            clear_event_id: &msg.token,
            now,
        },
        episodes::CHECK_REMIND => Input::RemindCheck { notified_at, now },
        other => {
            warn!(check = other, "Alert episode check: unknown check");
            return;
        }
    };
    let applied = match episodes::apply(
        &store,
        &msg.team_id,
        &msg.topic_arn,
        &msg.fingerprint,
        input,
        mode,
    )
    .await
    {
        Ok(a) => a,
        Err(e) => {
            warn!(team_id = %msg.team_id, error = %e, "Alert episode check failed");
            return;
        }
    };
    info!(team_id = %msg.team_id, check = %msg.check, decision = applied.decision.name(), "Alert episode check");
    let cipher = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: "",
    };
    let http = common::notify::Http(&state.http);
    let base = super::skills::app_url(&state.config.stage);
    let posting = episodes::Posting {
        dynamo: &state.dynamo,
        table,
        cipher: &cipher,
        poster: &http,
        base,
    };
    let enabled = route
        .get("enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    match applied.decision {
        Decision::Cleared(ended) => {
            episodes::post_cleared(&posting, &msg.team_id, &ended).await;
        }
        Decision::Remind => {
            let Some(ep) = applied.episode else { return };
            if enabled {
                episodes::post_reminder(&posting, &msg.team_id, &ep, now).await;
            }
            let next = AlertEpisodeCheckMessage {
                token: ep.last_notified_at.to_string(),
                delay_remaining_secs: 0,
                ..msg
            };
            episodes::schedule_check(
                &state.sqs,
                &state.config.ticket_queue_url,
                next,
                episodes::REMINDER_EVERY_SECS,
            )
            .await;
        }
        Decision::Wait(secs) => {
            episodes::schedule_check(
                &state.sqs,
                &state.config.ticket_queue_url,
                AlertEpisodeCheckMessage {
                    delay_remaining_secs: 0,
                    ..msg
                },
                secs,
            )
            .await;
        }
        _ => {}
    }
}
