//! A run that did the work opens its pull request, even when a check after
//! the work stopped it.
//!
//! A run once pushed every commit of a ticket's work, wrote its ticket note,
//! and ended without a pull request: a check after implementation (plan
//! coverage out of continuations, the new-feature rules, a link check, the
//! release check) returned an error, and only an error in the middle of
//! implementing was salvaged into a pull request. Every other error marked
//! the run failed, leaving the work on a branch nobody reviews.
//!
//! Now any error after implementation started, with work on the branch
//! (beyond the plan files CoderHelm commits beside it), opens a draft pull
//! request that says why the run stopped, as its open item; the run waits
//! for input instead of failing. A stop for agent permissions (paused, an
//! approval nobody answered) still ends the run as it did.

use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource};
use crate::WorkerState;
use common::run_status::EndedValues;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The checkpoints after which a stopped run has work worth a pull request.
pub fn opens_pr(last_pass: &str) -> bool {
    matches!(last_pass, "implement_started" | "implement" | "security")
}

/// The ticket line: `Closes #n` for a GitHub issue, else the ticket id.
fn ticket_line(msg: &TicketMessage) -> String {
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        format!("Closes #{}", msg.issue_number)
    } else {
        format!("Source ticket: {}", msg.ticket_id)
    }
}

/// The draft pull request's body: the ticket line, why the run stopped, what
/// is left as the open item, and what the checks left open (`open`, already
/// rendered with its own heading, or empty).
pub fn body(ticket: &str, reason: &str, files: &[String], open: &str) -> String {
    let reason = reason.split_whitespace().collect::<Vec<_>>().join(" ");
    let shown: Vec<String> = files.iter().take(20).map(|f| format!("- `{f}`")).collect();
    let more = files.len().saturating_sub(shown.len());
    let open_items = if open.trim().is_empty() {
        format!("## Open items\n\n- [ ] {reason}")
    } else {
        open.trim().to_string()
    };
    format!(
        "{ticket}\n\n## Summary\n\nCoderHelm did this ticket's work on this branch and stopped \
         before marking it ready: {reason}\n\n## Changes\n\n{}{}\n\n{open_items}\n\n\
         This is a draft. Push to this branch or comment on this pull request, and CoderHelm picks \
         it up from here.",
        shown.join("\n"),
        if more > 0 {
            format!("\n- and {more} more")
        } else {
            String::new()
        }
    )
}

/// Opens (or finds) the draft pull request for a stopped run's branch and
/// records it on the run. Errors when the branch holds no work.
pub async fn open(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    branch: &str,
    reason: &str,
) -> Result<String, BoxError> {
    open_with(state, msg, run_id, branch, reason, &[], true).await
}

/// [`open`], with what the checks left open going under Open items (on the
/// pull request that is already there too), and the notification only when
/// `notify` says this stop hasn't sent one.
pub async fn open_with(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    branch: &str,
    reason: &str,
    items: &[super::proof::Finding],
    notify: bool,
) -> Result<String, BoxError> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await?;
    let changed: Vec<String> = diff["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| f["filename"].as_str().map(String::from))
        .collect();
    let work = super::run_end::work_files(&changed, &msg.ticket_id);
    if work.is_empty() {
        return Err("no work on the branch".into());
    }
    let open_text = [
        super::proof::unfinished_section(items),
        super::proof::gaps_section(items),
    ]
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect::<Vec<_>>()
    .join("\n\n");
    let existing = github
        .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await?;
    let (number, url) = match existing {
        Some(p) => {
            let number = p["number"].as_u64().unwrap_or(0);
            if !items.is_empty() {
                super::pr::put_open_items(
                    &github,
                    &msg.repo_owner,
                    &msg.repo_name,
                    number,
                    p["body"].as_str().unwrap_or(""),
                    items,
                )
                .await;
            }
            (number, p["html_url"].as_str().unwrap_or("").to_string())
        }
        None => {
            let conventions =
                super::load_git_conventions(state, &msg.team_id, &msg.ticket_id).await;
            let title = super::pr::pr_title(&conventions, &super::ticket_tokens(msg), msg);
            let p = github
                .create_pull_request(
                    &msg.repo_owner,
                    &msg.repo_name,
                    common::truncate_str(&title, 72),
                    &body(&ticket_line(msg), reason, &work, &open_text),
                    branch,
                    &msg.base_branch,
                    true,
                )
                .await?;
            (
                p["number"].as_u64().unwrap_or(0),
                p["html_url"].as_str().unwrap_or("").to_string(),
            )
        }
    };
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(&msg.team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, pr_url = :pu, pr_number = :pn, branch = :b, \
             error_message = :em, updated_at = :t, current_pass = :cp",
        )
        // Never over a run a person ended.
        .condition_expression(common::run_status::not_ended("#s"))
        .ended_values()
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":cp", super::attr_s("done"))
        .expression_attribute_values(":s", super::attr_s("needs_input"))
        .expression_attribute_values(":sri", super::attr_s(&format!("needs_input#{run_id}")))
        .expression_attribute_values(":pu", super::attr_s(&url))
        .expression_attribute_values(":pn", super::attr_n(number))
        .expression_attribute_values(":b", super::attr_s(branch))
        .expression_attribute_values(":em", super::attr_s(reason))
        .expression_attribute_values(":t", super::attr_s(&now))
        .send()
        .await;
    super::add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("Stopped after the work; opened draft pull request #{number} with the reason as its open item"),
    )
    .await;
    super::run_guard::progress(
        state,
        &msg.team_id,
        run_id,
        super::run_guard::Progress::PrOpen,
    )
    .await;
    if notify {
        super::webhooks::emit_run(state, &msg.team_id, run_id, "needs_input").await;
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_checkpoint_after_the_work_opens_a_pr() {
        for p in ["implement_started", "implement", "security"] {
            assert!(opens_pr(p), "{p}");
        }
        for p in ["", "plan", "triage", "pr"] {
            assert!(!opens_pr(p), "{p}");
        }
    }

    #[test]
    fn the_body_says_why_and_keeps_it_open() {
        let b = body(
            "Source ticket: T-1",
            "Stopped before opening a pull request: 2 of 9 planned tasks are not done",
            &["web/a.tsx".into()],
            "",
        );
        assert!(b.starts_with("Source ticket: T-1"));
        assert!(b.contains("## Open items\n\n- [ ] Stopped before opening a pull request: 2 of 9"));
        assert!(b.contains("- `web/a.tsx`"));
        assert!(!b.contains("Closes #0"));
        // What the checks left open takes the place of the bare reason.
        let b = body(
            "Source ticket: T-1",
            "It kept continuing",
            &[],
            "## Open items\n- the vendor sends cents",
        );
        assert!(b.contains("## Open items\n- the vendor sends cents"));
        assert_eq!(b.matches("## Open items").count(), 1);
    }
}
