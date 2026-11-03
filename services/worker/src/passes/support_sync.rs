//! Self-hosted support (common::support_relay), every few minutes: send the
//! tickets and replies that didn't reach CoderHelm yet, then add support's
//! answers to the install's tickets and email the people on them.
//!
//! Nothing happens on the hosted deployment, or on an install that has no
//! registration yet: the gateway registers it when the first ticket is
//! opened, and says on the ticket when it couldn't.

use common::support_relay::{self as relay, DynamoLocal, HostedClient};
use tracing::{info, warn};

use crate::WorkerState;

pub async fn run(state: &WorkerState) -> Result<(), String> {
    if !common::public_urls::self_hosted() {
        return Ok(());
    }
    let Some(creds) = common::hybrid::client::credentials() else {
        return Ok(());
    };
    let local = DynamoLocal {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
    };
    let hosted = HostedClient {
        http: &state.http,
        creds,
    };
    let sent = relay::flush(&local, &hosted).await?;
    if sent.sent + sent.failed > 0 {
        info!(
            sent = sent.sent,
            failed = sent.failed,
            "Support tickets sent to CoderHelm"
        );
    }
    let added = relay::poll(&local, &hosted).await?;
    let app = common::public_urls::app_url_from_env();
    for (ticket, u) in &added {
        for mail in relay::mails_for(ticket, u, app) {
            if let Err(e) = common::mail::send(&crate::clients::email::sender(state), &mail).await {
                warn!(code = %e.code, error = %e.message, "support answer email not sent");
            }
        }
    }
    if !added.is_empty() {
        info!(
            answers = added.len(),
            "Support answers from CoderHelm added"
        );
    }
    Ok(())
}
