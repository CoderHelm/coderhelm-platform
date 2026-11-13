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
    // every version and count.
    dependency_sections(
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
/// rebuilt from what the lockfile on its head resolves. Costs no model
/// call. Nothing for any other pull request.
pub async fn dependency_sections(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    owner: &str,
    repo: &str,
    number: u64,
    ticket_id: &str,
) {
    if !common::security_fixes::is_ticket(ticket_id) {
        return;
    }
    super::security_fixes::rebuild_at_head(
        state,
        github,
        team_id,
        &format!("{owner}/{repo}"),
        number,
        ticket_id,
    )
    .await;
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
