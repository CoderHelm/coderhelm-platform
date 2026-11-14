//! The gateway's side of `common::conversation`: what the gateway says on
//! a ticket or pull request (an answer given in CoderHelm, a closed run, a
//! skipped run, a reply to a person's command) reads the thread first and
//! speaks with the same one voice as the worker.

use common::conversation::{self, Say, Spoke};
use common::conversation_io::{GithubRest, JiraChannel};

use crate::AppState;

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
