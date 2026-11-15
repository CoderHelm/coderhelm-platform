//! The gateway's side of `common::conversation`: what the gateway says on
//! a ticket or pull request (an answer given in CoderHelm, a closed run, a
//! skipped run, a reply to a person's command) reads the thread first and
//! speaks with the same one voice as the worker.

use common::conversation::{self, Say, Spoke};
use common::conversation_io::{GithubRest, JiraChannel};
use common::reply_compose::{self, Compose, Composer, Facts};
use serde_json::{json, Value};
use tracing::warn;

use crate::AppState;

/// The composer of the gateway's replies to people (a refused command, an
/// acknowledged comment), on the team's light model.
pub struct Replier<'a> {
    pub state: &'a AppState,
    pub team_id: &'a str,
}

#[async_trait::async_trait]
impl Composer for Replier<'_> {
    async fn compose(&self, system: &str, prompt: &str, _schema: &Value) -> Option<Value> {
        let (key, _, light) = super::brain::load_provider(self.state, self.team_id).await;
        let key = key.unwrap_or_default();
        if key.is_empty() && !common::model_provider::is_bedrock() {
            return None;
        }
        let body = json!({
            "model": light,
            "max_tokens": reply_compose::MAX_TOKENS,
            "system": system,
            "messages": [{ "role": "user", "content": format!("{prompt}\n\nAnswer with the JSON object only.") }],
        });
        let meter = crate::model_call::chat_meter(self.state, self.team_id);
        let m = Box::pin(crate::model_call::reply_turn(
            &self.state.http,
            &key,
            &body,
            &meter,
            &light,
        ))
        .await
        .inspect_err(|e| warn!("The reply was not written by the model; the draft is posted: {e}"))
        .ok()?;
        let text: String = m["content"]
            .as_array()
            .map(|c| {
                c.iter()
                    .filter_map(|b| b["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        reply_compose::json_in(&text)
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Says `say` on an issue or pull request conversation, with an
/// installation token.
pub async fn github(
    state: &AppState,
    token: &str,
    owner: &str,
    repo: &str,
    number: u64,
    say: Say,
) -> Result<Spoke, String> {
    let ch = GithubRest {
        http: &state.http,
        token: token.to_string(),
        owner: owner.to_string(),
        repo: repo.to_string(),
        number,
    };
    Box::pin(conversation::speak(&ch, None, &say, now_ms())).await
}

/// A reply to people on an issue or pull request conversation: `say` is
/// the draft; the composer writes it from the thread and `facts`.
#[allow(clippy::too_many_arguments)]
pub async fn reply_github(
    state: &AppState,
    team_id: &str,
    token: &str,
    owner: &str,
    repo: &str,
    number: u64,
    say: Say,
    facts: &Facts,
) -> Result<Spoke, String> {
    let ch = GithubRest {
        http: &state.http,
        token: token.to_string(),
        owner: owner.to_string(),
        repo: repo.to_string(),
        number,
    };
    let r = Replier { state, team_id };
    let c = Compose {
        composer: &r,
        facts,
    };
    Box::pin(conversation::speak_with(&ch, None, Some(c), &say, now_ms())).await
}

/// The address of a written GitHub comment.
pub fn github_url(owner: &str, repo: &str, number: u64, spoke: &Spoke) -> Option<String> {
    spoke
        .id()
        .map(|id| format!("https://github.com/{owner}/{repo}/issues/{number}#issuecomment-{id}"))
}

/// Says `say` on a Jira ticket through the team's Jira app at `url`.
#[allow(clippy::too_many_arguments)]
pub async fn jira_at(
    state: &AppState,
    team_id: &str,
    url: &str,
    ticket: &str,
    say: Say,
    comment_type: &str,
    title: &str,
) -> Result<Spoke, String> {
    let ch = JiraChannel {
        http: &state.http,
        dynamo: &state.dynamo,
        settings_table: &state.config.settings_table_name,
        team_id,
        url: url.to_string(),
        ticket: ticket.to_string(),
        comment_type: comment_type.to_string(),
        title: conversation::no_arrows(title),
    };
    Box::pin(conversation::speak(&ch, None, &say, now_ms())).await
}

/// A reply to people on a Jira ticket through the team's Jira app at
/// `url`: `say` is the draft; the composer writes it from the ticket's
/// thread and `facts`.
#[allow(clippy::too_many_arguments)]
pub async fn reply_jira_at(
    state: &AppState,
    team_id: &str,
    url: &str,
    ticket: &str,
    say: Say,
    facts: &Facts,
    comment_type: &str,
    title: &str,
) -> Result<Spoke, String> {
    let ch = JiraChannel {
        http: &state.http,
        dynamo: &state.dynamo,
        settings_table: &state.config.settings_table_name,
        team_id,
        url: url.to_string(),
        ticket: ticket.to_string(),
        comment_type: comment_type.to_string(),
        title: conversation::no_arrows(title),
    };
    let r = Replier { state, team_id };
    let c = Compose {
        composer: &r,
        facts,
    };
    Box::pin(conversation::speak_with(&ch, None, Some(c), &say, now_ms())).await
}
