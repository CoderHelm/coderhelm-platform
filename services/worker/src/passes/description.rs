//! The one place a pull request's description is written from.
//!
//! A dependency run's pull request had the right code after a review round
//! and the wrong description: its table still showed the plan (five packages
//! as major version jumps the final manifest no longer makes), its headline
//! counted advisories nobody had compared with the final lockfile, and the
//! round that fixed the code said it could not edit the description and left
//! the corrections to a person.
//!
//! A description is a function of the head. Three rules keep it one:
//!
//! - [`save`] is the only call of the GitHub client's body write in the
//!   pipeline. Every writer (the first description, a section CoderHelm
//!   owns, a rewrite) sends its text through it, so no path can write a
//!   description this module does not see. A test reads the sources and
//!   fails on a call anywhere else.
//! - [`head_moved`] is what every path that moves the head calls (a CI fix,
//!   a review fix, feedback, a bump taken out): the description's prose is
//!   rewritten from the whole diff, and a dependency pull request's table,
//!   headline, "What to check" and Proof are rebuilt from what the lockfile
//!   on the head resolves ([`dependency_sections`]). The model is never the
//!   source of a version or a count.
//! - A round's account that hands an edit of the description to a person is
//!   caught ([`round_account`]): the sentence goes, and the rebuild runs
//!   even when the round pushed no code.

use tracing::warn;

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::TokenUsage;
use crate::WorkerState;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Writes a pull request's description. The only caller of the client's
/// body write: a sentence that says the description can't be edited, or
/// leaves an edit of it to a person, never reaches a description.
pub async fn save(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    body: &str,
) -> Result<(), Error> {
    let body = clean(body);
    github.update_pr_body(owner, repo, number, &body).await
}

/// `body` as [`save`] writes it.
pub fn clean(body: &str) -> std::borrow::Cow<'_, str> {
    match common::description_handoff::scrub(body) {
        Some(scrubbed) => {
            warn!("A description said it could not be edited; that sentence was taken out");
            // The replacement sentence is for a round's account, not for
            // the description itself.
            std::borrow::Cow::Owned(
                scrubbed
                    .lines()
                    .filter(|l| {
                        l.trim().trim_start_matches(['-', '*', '>']).trim()
                            != common::description_handoff::REBUILT
                    })
                    .map(|l| {
                        l.replace(common::description_handoff::REBUILT, "")
                            .trim_end()
                            .to_string()
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
        }
        None => std::borrow::Cow::Borrowed(body),
    }
}

/// The pull request whose head moved.
pub struct Moved<'a> {
    pub state: &'a WorkerState,
    pub github: &'a GitHubClient,
    pub team_id: &'a str,
    pub installation_id: u64,
    pub owner: &'a str,
    pub repo: &'a str,
    pub number: u64,
    /// The run's ticket: a security ticket's pull request has the sections
    /// rebuilt from its lockfiles.
    pub ticket_id: &'a str,
    pub team_instructions: &'a str,
    /// What a person with write access asked to change in the description
    /// (empty when nobody asked).
    pub requests: &'a [String],
}

/// The head of an open pull request moved, or a person asked for a change to
/// its description: the description is rebuilt from the branch. Returns
/// what the rewrite changed (see `pr::refresh_description`).
pub async fn head_moved(
    m: &Moved<'_>,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
) -> Option<Vec<String>> {
    let changed = super::pr::refresh_description(
        m.state,
        m.github,
        (m.team_id, m.installation_id),
        m.owner,
        m.repo,
        m.number,
        m.team_instructions,
        m.requests,
        provider,
        usage,
    )
    .await;
    // After the rewrite, so what the lockfile resolves is the last word on
    // every version and count, and the description names the head it is of.
    rebuild(
        m.state,
        m.github,
        m.team_id,
        m.owner,
        m.repo,
        m.number,
        m.ticket_id,
    )
    .await;
    changed
}

/// A dependency pull request's table, headline, "What to check" and Proof,
/// rebuilt from what the lockfile on its head resolves, and the stamp of
/// the head they are of. Costs no model call.
pub async fn dependency_sections(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
) {
    rebuild(state, github, team_id, owner, repo, number, ticket_id).await;
}

// ── The head a description was built for ───────────────────────────────

/// A hidden line naming the head commit the description's computed
/// sections were built for. A description with no stamp, or another
/// head's, is rebuilt before anything rests on it.
pub const STAMP_START: &str = "<!-- coderhelm:built-for ";
const STAMP_END: &str = " -->";

/// The head the description says it was built for.
pub fn stamp_of(body: &str) -> Option<&str> {
    let at = body.rfind(STAMP_START)? + STAMP_START.len();
    let rest = &body[at..];
    Some(rest[..rest.find(STAMP_END)?].trim()).filter(|h| !h.is_empty())
}

/// `body` stamped as built for `head` (one stamp, on its own last line).
pub fn with_stamp(body: &str, head: &str) -> String {
    let kept: Vec<&str> = body
        .lines()
        .filter(|l| !l.trim_start().starts_with(STAMP_START))
        .collect();
    let body = kept.join("\n");
    if head.is_empty() {
        return body.trim_end().to_string();
    }
    format!("{}\n\n{STAMP_START}{head}{STAMP_END}", body.trim_end())
}

/// Whether the description was built for `head`.
pub fn is_current(body: &str, head: &str) -> bool {
    !head.is_empty() && stamp_of(body) == Some(head)
}

/// How a reconcile ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciled {
    /// The description was already built for the head (or the pull request
    /// is closed).
    Current,
    /// It was rebuilt and stamped for the head.
    Rebuilt,
    /// It could not be read or written: it may still be of another head.
    Failed(String),
}

/// Rebuilds the description when its stamp is not the pull request's head.
/// Every place that rests on the description calls this first: each step
/// that pushed (`reconcile_moved`), the checks and settle step, a review of
/// CoderHelm's own pull request, the pull request watch, and the end of the
/// run, which never completes while the two differ.
pub async fn reconcile(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
) -> Reconciled {
    build(
        state, github, team_id, owner, repo, number, ticket_id, false,
    )
    .await
}

/// [`reconcile`] whatever the stamp says: something the description shows
/// changed on the same head (its CI ended, the model rewrote its prose).
pub async fn rebuild(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
) -> Reconciled {
    build(state, github, team_id, owner, repo, number, ticket_id, true).await
}

#[allow(clippy::too_many_arguments)]
async fn build(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
    force: bool,
) -> Reconciled {
    if number == 0 {
        return Reconciled::Current;
    }
    let pr = match github.get_pull_request(owner, repo, number).await {
        Ok(p) => p,
        Err(e) => return Reconciled::Failed(format!("the pull request could not be read: {e}")),
    };
    if pr["state"].as_str().is_some_and(|s| s != "open") {
        return Reconciled::Current;
    }
    let body = pr["body"].as_str().unwrap_or("");
    let head = pr["head"]["sha"].as_str().unwrap_or("");
    if head.is_empty() {
        return Reconciled::Failed("the pull request names no head".into());
    }
    if !force && is_current(body, head) {
        return Reconciled::Current;
    }
    // Everything computed from the branch, for this head: a dependency
    // pull request's table, counts, "What to check" and Proof.
    let computed = if common::security_fixes::is_ticket(ticket_id) {
        super::security_fixes::at_head(
            state,
            github,
            team_id,
            &format!("{owner}/{repo}"),
            number,
            ticket_id,
            &pr,
        )
        .await
    } else {
        None
    };
    let next = with_stamp(computed.as_deref().unwrap_or(body), head);
    if next == body {
        return Reconciled::Current;
    }
    match save(github, owner, repo, number, &next).await {
        Ok(()) => {
            tracing::info!(pr_number = number, %head, was = stamp_of(body).unwrap_or("none"), "Description rebuilt for the head");
            Reconciled::Rebuilt
        }
        Err(e) => Reconciled::Failed(format!("the description could not be saved: {e}")),
    }
}

/// [`reconcile`] for a run's pull request, its ticket read from the run.
pub async fn reconcile_run(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    run_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
) -> Reconciled {
    let ticket = run_ticket(state, team_id, run_id).await;
    reconcile(state, github, team_id, owner, repo, number, &ticket).await
}

async fn run_ticket(state: &WorkerState, team_id: &str, run_id: &str) -> String {
    use aws_sdk_dynamodb::types::AttributeValue as A;
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", A::S(team_id.to_string()))
        .key("run_id", A::S(run_id.to_string()))
        .projection_expression("ticket_id")
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .and_then(|i| i.get("ticket_id").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default()
}

/// Every branch CoderHelm moved while handling a message gets its pull
/// request's description rebuilt for the new head. Called once, where
/// every message ends (`main::handle_message`); the branches come from the
/// one place every request leaves the GitHub client, so no code path that
/// pushes has to remember anything.
pub async fn reconcile_moved(state: &WorkerState, team_id: Option<&str>) {
    let moved = crate::clients::github::take_moved_branches();
    let Some(team_id) = team_id else {
        return;
    };
    for m in moved {
        let Ok(github) = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            m.installation_id,
            &state.http,
        ) else {
            continue;
        };
        let Ok(Some(pr)) = github
            .find_open_pr_for_branch(&m.owner, &m.repo, &m.branch)
            .await
        else {
            continue;
        };
        let Some(number) = pr["number"].as_u64() else {
            continue;
        };
        // Only a run's own pull request: its run names its ticket.
        let Some(run_id) =
            super::review_pr::lookup_run_by_pr(state, team_id, &m.owner, &m.repo, number).await
        else {
            continue;
        };
        if let Reconciled::Failed(why) =
            reconcile_run(state, &github, team_id, &run_id, &m.owner, &m.repo, number).await
        {
            warn!(pr_number = number, why, "The description could not be rebuilt after a push; the next step that reads it rebuilds it");
        }
    }
}

/// How an [`edit`] ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Edited {
    Saved(String),
    Unchanged,
    /// The pull request's head is no longer the one the edit is about.
    HeadMoved,
    Failed(String),
}

/// Changes one part of a description on the description as GitHub has it
/// now, never on a copy read earlier: a review that ran for minutes once
/// wrote back the copy it had read at its start, with its own section
/// added, and undid every write made in between. `of_head`: the head the
/// change is about; when the pull request has moved past it, nothing is
/// written (a review of a superseded head says nothing about this one).
pub async fn edit(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    of_head: Option<&str>,
    change: impl FnOnce(&str) -> String,
) -> Edited {
    let pr = match github.get_pull_request(owner, repo, number).await {
        Ok(p) => p,
        Err(e) => return Edited::Failed(e.to_string()),
    };
    let body = pr["body"].as_str().unwrap_or("");
    let head = pr["head"]["sha"].as_str().unwrap_or("");
    if of_head.is_some_and(|h| !h.is_empty() && !head.is_empty() && h != head) {
        return Edited::HeadMoved;
    }
    let next = change(body);
    if next == body {
        return Edited::Unchanged;
    }
    match save(github, owner, repo, number, &next).await {
        Ok(()) => Edited::Saved(next),
        Err(e) => Edited::Failed(e.to_string()),
    }
}

/// A round's account, checked: `(the account without any sentence that
/// hands the description to a person, whether there was one)`. A `true`
/// means the description is rebuilt now, whatever the round pushed.
pub fn round_account(account: &str) -> (String, bool) {
    match common::description_handoff::scrub(account) {
        Some(clean) => (clean, true),
        None => (account.to_string(), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every source file of the worker, with its path from `src/`.
    fn sources() -> Vec<(String, String)> {
        fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<(String, String)>) {
            for e in std::fs::read_dir(dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, root, out);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push((
                        p.strip_prefix(root)
                            .unwrap()
                            .to_string_lossy()
                            .replace('\\', "/"),
                        std::fs::read_to_string(&p).unwrap(),
                    ));
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut out = vec![];
        walk(&root, &root, &mut out);
        assert!(out.len() > 100, "the worker's sources were not found");
        out
    }

    /// The code of a file: no comment lines, and nothing from its tests on.
    fn code(text: &str) -> String {
        let end = text.find("#[cfg(test)]").unwrap_or(text.len());
        text[..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The guard: a new call site that writes a description some other way
    /// fails here.
    #[test]
    fn the_description_is_written_from_one_place() {
        let call = [".update_pr", "_body("].concat();
        let mut calls: Vec<String> = vec![];
        let mut through = 0;
        for (path, text) in sources() {
            let code = code(&text);
            // The client defines the write; it never calls it.
            if path != "clients/github.rs" {
                for _ in code.matches(&call) {
                    calls.push(path.clone());
                }
            }
            // A description sent as a raw request would pass the client's
            // method by.
            if path != "clients/github.rs" {
                for l in code.lines() {
                    assert!(
                        !(l.contains("/pulls/") && l.contains(".patch(")),
                        "{path} patches a pull request itself: {l}"
                    );
                }
            }
            through += code.matches("description::save(").count();
        }
        assert_eq!(
            calls,
            vec!["passes/description.rs".to_string()],
            "a description is written only through description::save"
        );
        assert!(through >= 20, "the writers go through it: {through}");
        // And the client has one method that replaces a description.
        let client = include_str!("../clients/github.rs");
        assert_eq!(
            client.matches("\n    pub async fn update_pr_body(").count(),
            1
        );
    }

    /// Every path that moves a pull request's head rebuilds its
    /// description, through this module.
    #[test]
    fn every_path_that_moves_the_head_rebuilds_the_description() {
        let resume = include_str!("resume.rs");
        let round = &resume[resume.find("\nasync fn run_fix_round(").unwrap()..];
        let round = &round[..round[1..].find("\nasync fn ").unwrap()];
        let pushed = round.find("Landed::Pushed { files }").unwrap();
        assert!(round[pushed..].contains("description::head_moved("));
        assert!(
            round.contains("description::round_account("),
            "a round's account is checked for a handed-off description"
        );
        // A bump CI took out.
        let revert = &resume[resume.find("\nasync fn revert_dep_commit(").unwrap()..];
        let revert = &revert[..revert[1..].find("\nasync fn ").unwrap_or(revert.len() - 1)];
        assert!(revert.contains("description::dependency_sections("));
        // Feedback from a person or a reviewer.
        let feedback = include_str!("feedback.rs");
        assert!(feedback.contains("description::head_moved("));
        assert!(feedback.contains("description::round_account("));
        // Nothing calls the rewrite past this module.
        for (path, text) in sources() {
            // The incident cases quote the pipeline's source; they call nothing.
            if path == "passes/description.rs"
                || path == "passes/pr.rs"
                || path.starts_with("passes/incidents/")
            {
                continue;
            }
            assert!(
                !code(&text).contains("pr::refresh_description("),
                "{path} rewrites the description past description::head_moved"
            );
        }
        // The dependency sections come from the head in every writer.
        let sec = code(include_str!("security_fixes.rs"));
        assert_eq!(sec.matches("sec::pr_body_linked(").count(), 0);
        assert!(sec.matches("sec::pr_body_at_head(").count() >= 3);
    }

    #[test]
    fn a_description_names_the_head_it_was_built_for() {
        let body = "## Summary\n\nKept glob-walk on the 2 line.\n";
        assert_eq!(stamp_of(body), None);
        assert!(!is_current(body, "aaa111"));
        let stamped = with_stamp(body, "aaa111");
        assert_eq!(stamp_of(&stamped), Some("aaa111"));
        assert!(is_current(&stamped, "aaa111"));
        assert!(!is_current(&stamped, "bbb222"), "another head: rebuilt");
        // One stamp, replaced in place, wherever a writer left the old one.
        let moved = format!("{stamped}\n\n## Open items\n- One thing.\n");
        let again = with_stamp(&moved, "bbb222");
        assert_eq!(again.matches(STAMP_START).count(), 1);
        assert_eq!(stamp_of(&again), Some("bbb222"));
        assert!(again.contains("- One thing."));
        assert_eq!(
            with_stamp(&again, "bbb222"),
            again,
            "stamping is idempotent"
        );
    }

    /// Every place that rests on the description reconciles it first, and
    /// a run never completes while stamp and head differ.
    #[test]
    fn everything_that_rests_on_the_description_reconciles_it_first() {
        let main = include_str!("../main.rs");
        let handle = &main[main.find("\nasync fn handle_message(").unwrap()..];
        let handle = &handle[..handle.find("\nasync fn handle_message_inner(").unwrap()];
        assert!(handle.contains("description::reconcile_moved("));
        let pr = include_str!("pr.rs");
        let checks = &pr[pr.find("pub async fn apply_checks(").unwrap()..];
        let checks = &checks[..checks.find("\n}\n").unwrap()];
        assert!(
            checks.contains("description::reconcile("),
            "the checks and settle step"
        );
        let run = &pr[pr.find("\npub async fn run(").unwrap()..];
        let run = &run[..run.find("\nasync fn notify_alert_pr(").unwrap()];
        assert!(
            run.contains("description::reconcile("),
            "the first description"
        );
        let review = include_str!("review_pr.rs");
        assert!(
            review.contains("description::reconcile_run("),
            "before a review of its own pull request"
        );
        assert!(
            review.contains("description::edit("),
            "a review writes on the description as it is now"
        );
        let watch = include_str!("pr_watch.rs");
        assert!(
            watch.contains("description::reconcile_run("),
            "the pull request watch"
        );
        let resume = include_str!("resume.rs");
        let done = &resume[resume.find("\nasync fn complete_run_with_status(").unwrap()..];
        let gate = done
            .find("description::reconcile(")
            .expect("the end of the run");
        let redrive = done.find("redrive_stale_items(").unwrap();
        assert!(gate < redrive, "before anything else at completion");
        assert!(
            done[gate..redrive].contains("return Err("),
            "a run never completes on another head's description"
        );
        // The dependency sections are computed in one function, called only
        // from here.
        for (path, text) in sources() {
            if path == "passes/description.rs" || path.starts_with("passes/incidents/") {
                continue;
            }
            assert!(
                !code(&text).contains("security_fixes::at_head("),
                "{path} builds the dependency sections past description::reconcile"
            );
        }
    }

    #[test]
    fn a_description_never_says_it_cannot_be_edited() {
        let body = "## Summary\n\nKept glob-walk on the 2 line.\n\n## Open items\n- I can't edit the PR description from here.\n- The studio build was not run.\n";
        let out = clean(body);
        assert!(!out.contains("can't edit"), "{out}");
        assert!(out.contains("- The studio build was not run."), "{out}");
        assert!(!out.contains(common::description_handoff::REBUILT), "{out}");
        let fine = "## Summary\n\nKept glob-walk on the 2 line.\n";
        assert!(matches!(clean(fine), std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn a_rounds_handoff_becomes_the_rebuild() {
        let (account, rebuild) = round_account(
            "Scoped the override to the 2 line. I can't edit the PR description from here, so the table still says 4.0.4.",
        );
        assert!(rebuild);
        assert_eq!(
            account,
            format!(
                "Scoped the override to the 2 line. {}",
                common::description_handoff::REBUILT
            )
        );
        assert_eq!(
            round_account("Scoped the override to the 2 line."),
            ("Scoped the override to the 2 line.".to_string(), false)
        );
    }
}
