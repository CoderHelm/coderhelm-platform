//! A person answering one of the reviewer's findings, with or without
//! mentioning CoderHelm: a reply in a thread CoderHelm started (trigger
//! `thread_reply`), or a PR-conversation comment that quotes its review
//! (trigger `quote_reply`).
//!
//! The gateway sends every such reply on a PR CoderHelm reviewed; here the
//! worker checks it answers CoderHelm (the thread's first comment is
//! CoderHelm's, or the quote is from its review), so a reply in a person's
//! own thread is left alone unless it addressed CoderHelm. The answer is
//! acknowledged in place, the finding is recorded as answered, and when the
//! reply concludes the answer settles it the thread is resolved (or, when
//! the app may not resolve it, the reply says so in one line). A settled
//! finding while CoderHelm's standing verdict requests changes re-runs the
//! formal review, so the verdict follows the answer.

use crate::clients::github::{BotReviewThread, GitHubClient};
use crate::models::ReviewMessage;
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

pub const TRIGGER_THREAD: &str = "thread_reply";
pub const TRIGGER_QUOTE: &str = "quote_reply";

/// A reply trigger that may answer one of CoderHelm's findings.
pub fn is_answer_trigger(trigger: &str) -> bool {
    trigger == TRIGGER_THREAD || trigger == TRIGGER_QUOTE
}

/// The finding a person answered.
#[derive(Debug, Clone)]
pub struct AnsweredFinding {
    /// The finding as CoderHelm wrote it.
    pub finding: String,
    /// Its inline thread, when it has one (a quote of a review body has none).
    pub thread: Option<BotReviewThread>,
}

impl AnsweredFinding {
    /// The record key: the thread's first comment, or the quoting comment.
    fn key(&self, msg: &ReviewMessage) -> String {
        match &self.thread {
            Some(t) if t.root_id != 0 => format!("thread#{}", t.root_id),
            _ => format!("quote#{}", msg.dedup_key),
        }
    }
}

/// Letters and digits only, lower case: quotes keep or drop markdown, so
/// matching ignores it.
fn key_text(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

/// The quoted lines of a comment (`> …`), without the markers.
pub fn quoted_lines(body: &str) -> Vec<String> {
    body.lines()
        .map(str::trim_start)
        .filter(|l| l.starts_with('>'))
        .map(|l| l.trim_start_matches('>').trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// A quoted line long enough to say where it came from is in `text`.
pub fn quote_is_from(body: &str, text: &str) -> bool {
    let hay = key_text(text);
    if hay.is_empty() {
        return false;
    }
    quoted_lines(body).iter().any(|l| {
        let k = key_text(l);
        k.len() >= 12 && hay.contains(&k)
    })
}

/// CoderHelm's thread whose first comment is `root`.
pub fn thread_for_root(threads: &[BotReviewThread], root: u64) -> Option<&BotReviewThread> {
    (root != 0)
        .then(|| threads.iter().find(|t| t.root_id == root))
        .flatten()
}

/// What the reviewer is asked to answer: its finding, the thread so far,
/// and the person's reply.
pub fn answer_prompt(f: &AnsweredFinding, author: &str, reply: &str) -> String {
    let mut s = String::from(
        "A person is answering one of your review findings. Acknowledge the answer in one or two \
         sentences. If it settles the finding, say so plainly, starting with \"Settled\" \
         (\"Settled, thanks: the bucket uses a customer-managed KMS key.\"). If something about \
         the finding is still open, say exactly what. If a fact in the answer can be checked with \
         your tools (the repository, a connected AWS account, a data source), check it first and \
         say what you found.\n\n## Your finding\n",
    );
    s.push_str(common::truncate_str(f.finding.trim(), 4000));
    if let Some(t) = &f.thread {
        if !t.path.is_empty() {
            s.push_str(&format!("\n(on `{}` line {})", t.path, t.line));
        }
        let earlier: Vec<String> = t
            .replies
            .iter()
            .filter(|r| r.body.trim() != reply.trim())
            .map(|r| {
                format!(
                    "- {}: {}",
                    if r.by_bot {
                        "CoderHelm"
                    } else {
                        r.author.as_str()
                    },
                    common::truncate_str(r.body.trim(), 1200).replace('\n', " ")
                )
            })
            .collect();
        if !earlier.is_empty() {
            s.push_str("\n\n## Earlier in the thread (oldest first)\n");
            s.push_str(&earlier.join("\n"));
        }
    }
    s.push_str(&format!(
        "\n\n## The answer from {author}\n{}",
        common::truncate_str(reply.trim(), 4000)
    ));
    s
}

/// The line added when the answer settled the finding but its thread
/// couldn't be resolved.
pub const COULD_NOT_RESOLVE: &str =
    "I couldn't mark this thread resolved (the app may not have permission here), so please resolve it.";

/// Finds what the reply answers. `Ok(None)`: it doesn't answer CoderHelm.
pub async fn find(
    github: &GitHubClient,
    msg: &ReviewMessage,
) -> Result<Option<AnsweredFinding>, Box<dyn std::error::Error + Send + Sync>> {
    let threads = github
        .list_bot_review_threads(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await?;
    if msg.trigger == TRIGGER_THREAD {
        return Ok(
            thread_for_root(&threads, msg.thread_root_id.unwrap_or(0)).map(|t| AnsweredFinding {
                finding: t.body.clone(),
                thread: Some(t.clone()),
            }),
        );
    }
    let body = msg.question.as_deref().unwrap_or("");
    if let Some(t) = threads.iter().find(|t| quote_is_from(body, &t.body)) {
        return Ok(Some(AnsweredFinding {
            finding: t.body.clone(),
            thread: Some(t.clone()),
        }));
    }
    let reviews = github
        .list_pr_reviews(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await?;
    let own_review = reviews
        .as_array()
        .into_iter()
        .flatten()
        .filter(|r| {
            r["user"]["login"]
                .as_str()
                .is_some_and(|l| l.contains("coderhelm"))
        })
        .filter_map(|r| r["body"].as_str())
        .rev()
        .find(|b| quote_is_from(body, b));
    if let Some(b) = own_review {
        return Ok(Some(AnsweredFinding {
            finding: b.to_string(),
            thread: None,
        }));
    }
    let comments = github
        .list_issue_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await?;
    Ok(super::review_pr::own_comments(&comments, 20)
        .into_iter()
        .rev()
        .find(|c| quote_is_from(body, c))
        .map(|c| AnsweredFinding {
            finding: c,
            thread: None,
        }))
}

/// Resolves the finding's thread when the answer settled it. `Some(note)`
/// when it couldn't be resolved and the reply should say so.
pub async fn settle(
    github: &GitHubClient,
    msg: &ReviewMessage,
    f: &AnsweredFinding,
) -> Option<&'static str> {
    let t = f.thread.as_ref()?;
    if t.resolved || t.id.is_empty() {
        return None;
    }
    match github.resolve_review_thread(&t.id).await {
        Ok(_) => {
            info!(pr = msg.pr_number, thread = %t.id, "Reviewer: answered finding settled, thread resolved");
            None
        }
        Err(e) => {
            warn!(pr = msg.pr_number, thread = %t.id, error = %e, "Could not resolve a settled thread");
            Some(COULD_NOT_RESOLVE)
        }
    }
}

/// Records the finding as answered (and whether it was settled): the
/// reviewer's outcome signal for this finding, next to the PR's review
/// records. Best-effort.
pub async fn record(
    state: &WorkerState,
    msg: &ReviewMessage,
    f: &AnsweredFinding,
    settled: bool,
    resolved: bool,
    reply: &str,
) {
    let now = chrono::Utc::now();
    let sk = format!(
        "REVIEWANSWER#{}/{}#{:06}#{}",
        msg.repo_owner,
        msg.repo_name,
        msg.pr_number,
        f.key(msg)
    );
    let s = |v: &str| AttributeValue::S(v.to_string());
    let r = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(&msg.team_id))
        .item("sk", s(&sk))
        .item("status", s(if settled { "settled" } else { "answered" }))
        .item("resolved", AttributeValue::Bool(resolved))
        .item("answered_by", s(&msg.requested_by))
        .item("trigger", s(&msg.trigger))
        .item("finding", s(common::truncate_str(f.finding.trim(), 1500)))
        .item("reply", s(common::truncate_str(reply.trim(), 1500)))
        .item("at", s(&now.to_rfc3339()))
        .item(
            "ttl",
            AttributeValue::N((now.timestamp() + 180 * 86_400).to_string()),
        )
        .send()
        .await;
    if let Err(e) = r {
        warn!(pr = msg.pr_number, error = %e, "Could not record an answered finding");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::github::ThreadReply;

    fn thread(root: u64, body: &str) -> BotReviewThread {
        BotReviewThread {
            id: format!("T_{root}"),
            root_id: root,
            path: "infra/copy.tf".into(),
            line: 12,
            outdated: false,
            body: body.into(),
            replies: vec![ThreadReply {
                author: "author".into(),
                by_bot: false,
                body: "It uses our own KMS key, alias/data-copy.".into(),
            }],
            resolved: false,
            resolved_by_person: false,
        }
    }

    #[test]
    fn the_thread_is_found_by_its_first_comment() {
        let ts = vec![thread(10, "a"), thread(11, "b")];
        assert_eq!(thread_for_root(&ts, 11).unwrap().body, "b");
        assert!(thread_for_root(&ts, 12).is_none());
        // An unknown root never matches a thread whose id is unknown.
        assert!(thread_for_root(&[thread(0, "c")], 0).is_none());
    }

    #[test]
    fn a_quote_reply_is_matched_to_coderhelms_text_whatever_the_markdown() {
        let review = "**Is the source bucket encrypted with a customer-managed KMS key?** The copy job reads it.";
        let body =
            "> Is the source bucket encrypted with a customer-managed KMS key?\n\nYes, it is.";
        assert!(quote_is_from(body, review));
        // A short quote says nothing about where it came from.
        assert!(!quote_is_from("> Yes\n\nok", review));
        assert!(!quote_is_from("no quote here", review));
        assert!(!quote_is_from(
            body,
            "Something else entirely, about tests."
        ));
    }

    #[test]
    fn the_prompt_carries_the_finding_the_thread_and_the_answer() {
        let f = AnsweredFinding {
            finding: "Is the source bucket encrypted with a customer-managed KMS key?".into(),
            thread: Some(thread(10, "x")),
        };
        let p = answer_prompt(&f, "author", "Yes: SSE-KMS with our own key.");
        assert!(p.contains("## Your finding\nIs the source bucket"));
        assert!(p.contains("infra/copy.tf"));
        assert!(p.contains("- author: It uses our own KMS key"));
        assert!(p.contains("## The answer from author\nYes: SSE-KMS"));
        assert!(p.contains("Settled"));
    }

    #[test]
    fn only_the_answer_triggers_count() {
        assert!(is_answer_trigger("thread_reply"));
        assert!(is_answer_trigger("quote_reply"));
        assert!(!is_answer_trigger("reply"));
        assert!(!is_answer_trigger("label"));
    }
}
