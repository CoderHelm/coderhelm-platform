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

/// The last failure's text, so the same failure every five minutes (an
/// outage on CoderHelm's side) is logged as an error once, not each tick.
static LAST_FAILURE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Whether this sync's failure is news: the first one, or one with another
/// reason than the last. A success clears it, so the next failure is news.
pub fn failure_is_news(result: &Result<(), String>) -> bool {
    let mut last = LAST_FAILURE.lock().unwrap_or_else(|p| p.into_inner());
    news(&mut last, result)
}

fn news(last: &mut Option<String>, result: &Result<(), String>) -> bool {
    match result {
        Ok(()) => {
            if last.take().is_some() {
                info!("Support sync works again");
            }
            false
        }
        Err(e) if last.as_deref() == Some(e.as_str()) => false,
        Err(e) => {
            *last = Some(e.clone());
            true
        }
    }
}

#[cfg(test)]
mod failure_tests {
    #[test]
    fn the_same_failure_is_news_once() {
        let mut last = None;
        let down = Err("CoderHelm's servers failed to answer (HTTP 502)".to_string());
        assert!(super::news(&mut last, &down));
        assert!(!super::news(&mut last, &down));
        assert!(super::news(&mut last, &Err("other".into())));
        assert!(!super::news(&mut last, &Ok(())));
        assert!(super::news(&mut last, &down), "news again after a success");
    }
}
