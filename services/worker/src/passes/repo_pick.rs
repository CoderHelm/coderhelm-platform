//! Choosing the repo a Jira ticket runs in, and stopping out loud when the
//! ticket belongs to a repo that is connected but not turned on for runs.
//!
//! The bug this exists for: a ticket pinned (or plainly written) for repo X,
//! where X was connected but `enabled: false`, was silently re-routed to an
//! enabled repo, planned "no changes needed" there and completed with no PR.
//! Nobody was told. The rule now: a named repo is honoured or the run stops
//! as needs_input with a plain message; it is never swapped for another.

/// One row of the team's repo list (connected repos, enabled or not).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamRepo {
    pub full_name: String,
    pub enabled: bool,
}

/// Where a repo named on the ticket message stands for this team.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinState {
    /// Enabled for runs; carries the canonical `owner/name` spelling.
    Enabled(String),
    /// Connected (listed) but not turned on for runs.
    Disabled(String),
    /// Not in the team's repo list at all; visibility must be asked of GitHub.
    NotListed,
}

/// The outcome of an automatic (unpinned) pick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoPick {
    /// Run in this repo; `why` is a short reason shown when the plan finds
    /// nothing to change, so a wrong-repo pick is visible.
    Repo { repo: String, why: String },
    /// The ticket names this connected-but-disabled repo; stop, don't re-pick.
    NotEnabled(String),
}

pub fn pin_state(pin: &str, repos: &[TeamRepo]) -> PinState {
    match repos.iter().find(|r| r.full_name.eq_ignore_ascii_case(pin)) {
        Some(r) if r.enabled => PinState::Enabled(r.full_name.clone()),
        Some(r) => PinState::Disabled(r.full_name.clone()),
        None => PinState::NotListed,
    }
}

pub fn not_enabled_message(repo: &str) -> String {
    format!(
        "This ticket is for {repo}, which isn't turned on for Coderhelm runs. \
         Turn it on in Repositories and re-run."
    )
}

pub fn not_connected_message(repo: &str) -> String {
    format!(
        "This ticket is for {repo}, which isn't connected to Coderhelm. \
         Connect it in Repositories and re-run."
    )
}

/// Note appended when the plan finds nothing to change in a repo Coderhelm
/// picked itself, so the reader can tell a wrong-repo pick from a done ticket.
pub fn no_changes_repo_note(repo: &str, why: &str) -> String {
    let why = why.trim();
    if why.is_empty() {
        format!(
            "Coderhelm picked {repo} for this ticket and looked there. \
             If the work belongs in a different repo, set the repo and re-run."
        )
    } else {
        format!(
            "Coderhelm picked {repo} for this ticket and looked there. Why that repo: {why} \
             If the work belongs in a different repo, set the repo and re-run."
        )
    }
}

/// The pick after a run moved to `repo` because its plan belongs there:
/// `(repo, why)` for [`no_changes_repo_note`]. `reason` is the repo check's
/// own (e.g. "3 of 4 plan files exist there but not here").
pub fn switched_pick(repo: &str, reason: &str) -> (String, String) {
    let reason = reason.trim().trim_end_matches('.');
    let why = if reason.is_empty() {
        "the plan's files live there, so the run moved to it.".to_string()
    } else {
        format!("the plan's files live there ({reason}), so the run moved to it.")
    };
    (repo.to_string(), why)
}

/// Why the ticket's own pull request decided the repo, for
/// [`no_changes_repo_note`]. `state` is "merged" or "open"; `own` is false
/// for a pull request someone else opened that names the ticket.
pub fn ticket_pr_why(pr_number: u64, state: &str, own: bool) -> String {
    if own {
        format!("CoderHelm's pull request #{pr_number} for this ticket is in it ({state}).")
    } else {
        format!("pull request #{pr_number}, which names this ticket, is open in it.")
    }
}

/// The repo of the ticket's own pull request, among `enabled` repos:
/// CoderHelm's newest pull request for the ticket that merged or is still
/// open, as `(repo, pr_number, "merged" | "open")`. One closed without
/// merging doesn't count (people turned that work down), nor does one in a
/// repo that isn't turned on for runs. The repo is spelled as `enabled`
/// spells it.
pub fn repo_of_own_pr(
    runs: &[crate::passes::ticket_history::PriorRun],
    enabled: &[String],
) -> Option<(String, u64, &'static str)> {
    runs.iter().rev().find_map(|r| {
        if r.pr_number == 0 || r.pr_closed_unmerged() {
            return None;
        }
        let repo = enabled
            .iter()
            .find(|e| e.eq_ignore_ascii_case(r.repo.trim()))?;
        let state = if r.pr_outcome == "merged" || r.status == "merged" {
            "merged"
        } else {
            "open"
        };
        Some((repo.clone(), r.pr_number, state))
    })
}

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

/// True when `needle` occurs in `hay` as a whole token: not glued to other
/// name characters on either side (a trailing sentence period is fine).
fn contains_token(hay: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let hay = hay.to_ascii_lowercase();
    let needle = needle.to_ascii_lowercase();
    let mut from = 0;
    while let Some(off) = hay[from..].find(&needle) {
        let start = from + off;
        let end = start + needle.len();
        let before_ok = hay[..start]
            .chars()
            .next_back()
            .map(|c| !(is_word_char(c) || c == '/' || c == '.'))
            .unwrap_or(true);
        let mut rest = hay[end..].chars();
        let after_ok = match rest.next() {
            None => true,
            Some('.') => rest.next().map(|c| !is_word_char(c)).unwrap_or(true),
            Some(c) => !(is_word_char(c) || c == '/'),
        };
        if before_ok && after_ok {
            return true;
        }
        from = start + needle.len().max(1);
    }
    false
}

fn slug(full_name: &str) -> &str {
    full_name
        .rsplit_once('/')
        .map(|(_, n)| n)
        .unwrap_or(full_name)
}

/// A bare repo name is only trusted as a deterministic mention when it can't
/// be an ordinary word: it carries a separator or a digit. Plain-word names
/// ("docs", "web") are left to the model to judge.
fn distinctive_slug(s: &str) -> bool {
    s.chars()
        .any(|c| c == '-' || c == '_' || c == '.' || c.is_ascii_digit())
}

/// Connected repos the ticket text names exactly: the `owner/name` full name,
/// or a distinctive bare name that no other connected repo shares.
fn named_repos<'a>(text: &str, repos: &'a [TeamRepo]) -> Vec<&'a TeamRepo> {
    repos
        .iter()
        .filter(|r| {
            if contains_token(text, &r.full_name) {
                return true;
            }
            let s = slug(&r.full_name);
            let shared = repos
                .iter()
                .filter(|o| slug(&o.full_name).eq_ignore_ascii_case(s))
                .count()
                > 1;
            !shared && distinctive_slug(s) && contains_token(text, s)
        })
        .collect()
}

/// Deterministic pre-check: the ticket names exactly one connected repo and
/// it is disabled. Mentioning several repos (e.g. "move X into Y") is left to
/// the model.
pub fn ticket_names_only_disabled(text: &str, repos: &[TeamRepo]) -> Option<String> {
    let named = named_repos(text, repos);
    match named.as_slice() {
        [only] if !only.enabled => Some(only.full_name.clone()),
        _ => None,
    }
}

/// Weak signal: some disabled repo's name (full or bare, any shape) appears
/// in the ticket. When true, the code-evidence shortcut must not bypass the
/// model, which then judges whether the ticket really means that repo.
pub fn mentions_disabled_repo(text: &str, disabled: &[String]) -> bool {
    disabled
        .iter()
        .any(|d| contains_token(text, d) || contains_token(text, slug(d)))
}

/// The model's structured stop answer: a line `NOT_ENABLED: owner/name` that
/// names one of the disabled repos exactly. Scans from the last line up.
pub fn parse_not_enabled_answer(response: &str, disabled: &[String]) -> Option<String> {
    for line in response.trim().lines().rev() {
        let cleaned = line.trim().trim_matches(['`', '*', '_', '"', '\'']);
        let Some(rest) = cleaned
            .strip_prefix("NOT_ENABLED:")
            .or_else(|| cleaned.strip_prefix("not_enabled:"))
        else {
            continue;
        };
        let name = rest.trim().trim_matches(['`', '*', '"', '\'', '.']);
        return disabled
            .iter()
            .find(|d| d.eq_ignore_ascii_case(name))
            .cloned();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repos() -> Vec<TeamRepo> {
        vec![
            TeamRepo {
                full_name: "Acme/web-app".into(),
                enabled: true,
            },
            TeamRepo {
                full_name: "Acme/api".into(),
                enabled: true,
            },
            TeamRepo {
                full_name: "Acme/handbook-docs".into(),
                enabled: false,
            },
            TeamRepo {
                full_name: "Acme/docs".into(),
                enabled: false,
            },
        ]
    }

    #[test]
    fn pinned_enabled_repo_is_used_with_canonical_spelling() {
        assert_eq!(
            pin_state("acme/WEB-APP", &repos()),
            PinState::Enabled("Acme/web-app".into())
        );
    }

    #[test]
    fn pinned_disabled_repo_is_reported_not_repicked() {
        assert_eq!(
            pin_state("Acme/handbook-docs", &repos()),
            PinState::Disabled("Acme/handbook-docs".into())
        );
        assert_eq!(
            not_enabled_message("Acme/handbook-docs"),
            "This ticket is for Acme/handbook-docs, which isn't turned on for Coderhelm runs. \
             Turn it on in Repositories and re-run."
        );
    }

    #[test]
    fn pinned_unknown_repo_is_not_listed() {
        assert_eq!(pin_state("Other/thing", &repos()), PinState::NotListed);
        assert!(not_connected_message("Other/thing").contains("isn't connected"));
    }

    #[test]
    fn ticket_naming_a_disabled_repo_stops() {
        let text = "Self-contained documentation fix in the handbook-docs repo.";
        assert_eq!(
            ticket_names_only_disabled(text, &repos()),
            Some("Acme/handbook-docs".into())
        );
        let text = "Fix the typo in Acme/docs.";
        assert_eq!(
            ticket_names_only_disabled(text, &repos()),
            Some("Acme/docs".into())
        );
    }

    #[test]
    fn plain_word_names_are_left_to_the_model() {
        // "docs" is an ordinary word; the deterministic check must not fire.
        let text = "Update the docs for the login page.";
        assert_eq!(ticket_names_only_disabled(text, &repos()), None);
        // ...but the evidence shortcut must yield to the model.
        let disabled = vec!["Acme/docs".to_string()];
        assert!(mentions_disabled_repo(text, &disabled));
    }

    #[test]
    fn normal_ticket_names_no_disabled_repo() {
        let text = "Add a retry to the checkout button in web-app.";
        assert_eq!(ticket_names_only_disabled(text, &repos()), None);
        let disabled = vec!["Acme/handbook-docs".to_string(), "Acme/docs".to_string()];
        assert!(!mentions_disabled_repo(
            "Add a retry to the checkout button.",
            &disabled
        ));
    }

    #[test]
    fn ticket_naming_enabled_and_disabled_is_left_to_the_model() {
        let text = "Move the guide from handbook-docs into web-app.";
        assert_eq!(ticket_names_only_disabled(text, &repos()), None);
    }

    #[test]
    fn token_match_respects_name_boundaries() {
        assert!(!contains_token("see handbook-docs-v2 for", "handbook-docs"));
        assert!(!contains_token("other/handbook-docs", "handbook-docs"));
        assert!(contains_token("in handbook-docs.", "handbook-docs"));
        assert!(contains_token("(handbook-docs)", "handbook-docs"));
    }

    #[test]
    fn parses_model_stop_answer() {
        let disabled = vec!["Acme/handbook-docs".to_string()];
        let resp = "1. Docs fix.\n2. The ticket says the handbook-docs repo.\n\nNOT_ENABLED: Acme/handbook-docs";
        assert_eq!(
            parse_not_enabled_answer(resp, &disabled),
            Some("Acme/handbook-docs".into())
        );
        // A normal pick has no stop line.
        assert_eq!(
            parse_not_enabled_answer("reasoning\nAcme/web-app", &disabled),
            None
        );
        // A stop line naming a repo outside the disabled list is ignored.
        assert_eq!(
            parse_not_enabled_answer("NOT_ENABLED: Acme/web-app", &disabled),
            None
        );
    }

    #[test]
    fn no_changes_note_names_the_repo_and_reason() {
        let n = no_changes_repo_note("Acme/api", "the ticket is about the billing endpoint.");
        assert!(n.contains("Acme/api"));
        assert!(n.contains("billing endpoint"));
        assert!(n.contains("re-run"));
    }

    fn run(
        id: &str,
        repo: &str,
        pr: u64,
        outcome: &str,
    ) -> crate::passes::ticket_history::PriorRun {
        crate::passes::ticket_history::PriorRun {
            run_id: id.into(),
            status: "completed".into(),
            repo: repo.into(),
            pr_number: pr,
            pr_outcome: outcome.into(),
            ..Default::default()
        }
    }

    /// Three runs on one ticket each planned in the wrong repo first while
    /// its PR sat in another: the PR's repo is the pick.
    #[test]
    fn the_tickets_own_pr_decides_the_repo() {
        let enabled = vec!["Acme/api".to_string(), "Acme/web".to_string()];
        let runs = vec![
            run("01A", "acme/web", 0, ""),
            run("01B", "acme/api", 7, "merged"),
        ];
        assert_eq!(
            repo_of_own_pr(&runs, &enabled),
            Some(("Acme/api".to_string(), 7, "merged"))
        );
        // The newest live PR wins; an open one counts.
        let runs = vec![
            run("01A", "acme/api", 7, "merged"),
            run("01B", "acme/web", 9, ""),
        ];
        assert_eq!(
            repo_of_own_pr(&runs, &enabled),
            Some(("Acme/web".to_string(), 9, "open"))
        );
    }

    #[test]
    fn a_closed_pr_or_a_repo_not_turned_on_decides_nothing() {
        let enabled = vec!["Acme/api".to_string()];
        assert_eq!(
            repo_of_own_pr(&[run("01A", "acme/api", 7, "closed")], &enabled),
            None
        );
        assert_eq!(
            repo_of_own_pr(&[run("01A", "acme/old", 7, "merged")], &enabled),
            None
        );
        assert_eq!(
            repo_of_own_pr(&[run("01A", "acme/api", 0, "")], &enabled),
            None
        );
        // A closed one doesn't hide an earlier merged one.
        let runs = vec![
            run("01A", "acme/api", 7, "merged"),
            run("01B", "acme/api", 8, "closed"),
        ];
        assert_eq!(
            repo_of_own_pr(&runs, &enabled),
            Some(("Acme/api".to_string(), 7, "merged"))
        );
    }

    #[test]
    fn the_note_names_the_repo_the_run_moved_to() {
        let (repo, why) = switched_pick("Acme/api", "3 of 4 plan files exist there but not here");
        assert_eq!(repo, "Acme/api");
        let note = no_changes_repo_note(&repo, &why);
        assert!(note.contains("picked Acme/api"), "{note}");
        assert!(note.contains("3 of 4 plan files"), "{note}");
        assert!(switched_pick("Acme/api", "").1.contains("plan's files"));
    }
}
