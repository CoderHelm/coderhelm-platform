//! vs. A ticket said `Repo: harbor/harbor-infra`, and both harbor-infra and
//! the web repo harbor were connected. An earlier run had been moved to
//! harbor by the plan's repo check and opened a pull request there full of
//! stand-in work. With the routing fix in (`vq_two_repo_ticket_runs_end`),
//! a person pressed Retry on that run:
//!
//! - The retry cleared the repo, routing picked harbor-infra from the
//!   ticket, and the retry took up the earlier run's cached plan as final.
//!   A final plan skips the repo check the routing fix guarded.
//! - The "plan files found in a different repo" switch before implementing
//!   had no such guard, and moved the run back to harbor.
//! - The multi-repo path then resolved harbor's branch to the earlier run's
//!   open pull request, and the stand-in work went on. Nothing had closed
//!   it.
//!
//! Guarantees: no repo switch ever leaves the repo the ticket names (work in
//! another connected repo is that repo's linked pull request); a retry of a
//! run that worked in another repo starts over in this one, with a new plan
//! and none of the earlier run's finished work; CoderHelm's own pull request
//! the earlier run left in the other repo is closed with a short note, after
//! the agent-permission check, once; the ticket's own branch in the named
//! repo is the one the run continues.

use super::fixture::fn_src;
use crate::passes::repo_pick::{self, retry_start, switch_blocked, RetryStart};
use crate::passes::ticket_history::{EarlierPr, PriorRun, TicketHistory};

const NAMED: &str = "harbor/harbor-infra";
const OTHER: &str = "harbor/harbor";

fn mod_src() -> &'static str {
    include_str!("../mod.rs")
}

/// The earlier run (moved to harbor, its pull request 41 open there) and an
/// even earlier one that opened a linked pull request in harbor on purpose.
fn history() -> TicketHistory {
    let pr = |repo: &str, n: u64| common::run_prs::RunPr {
        repo: repo.into(),
        pr_number: n,
        branch: format!("coderhelm/harbor-{n}"),
        ..Default::default()
    };
    let open = |repo: &str, n: u64, author: &str| EarlierPr {
        repo: repo.into(),
        number: n,
        author: author.into(),
        branch: format!("coderhelm/harbor-{n}"),
        state: "open".into(),
        created_at: format!("2030-01-0{}T10:00:00Z", n % 9),
        ..Default::default()
    };
    TicketHistory {
        runs: vec![
            PriorRun {
                run_id: "01LINKED".into(),
                repo: NAMED.into(),
                pr_list: vec![pr(NAMED, 7), pr(OTHER, 38)],
                ..Default::default()
            },
            PriorRun {
                run_id: "01MOVED".into(),
                repo: OTHER.into(),
                pr_number: 41,
                pr_list: vec![pr(OTHER, 41)],
                ..Default::default()
            },
        ],
        prs: vec![
            open(NAMED, 7, "coderhelm[bot]"),
            open(OTHER, 38, "coderhelm[bot]"),
            open(OTHER, 41, "coderhelm[bot]"),
            open(OTHER, 44, "pat"),
        ],
        ..Default::default()
    }
}

#[test]
fn no_repo_switch_leaves_the_repo_the_ticket_names() {
    assert_eq!(switch_blocked(Some(NAMED), OTHER), Some(NAMED));
    assert_eq!(switch_blocked(Some(NAMED), NAMED), None);
    assert_eq!(switch_blocked(None, OTHER), None);
    let src = mod_src();
    // The switch before implementing asks first.
    let site = src
        .find("\"Switching repo — plan files found in different repo\"")
        .unwrap();
    let guard = src[..site].rfind("repo_pick::switch_blocked(").unwrap();
    assert!(
        site - guard < 1200,
        "the guard sits right before the switch"
    );
    assert!(src[guard..site].contains("stay_in_named_repo("));
    // So does the switch after planning, and the function that moves a run.
    let mv = fn_src(src, "\nasync fn switch_run_repo(");
    let asks = mv.find("repo_pick::switch_blocked(").unwrap();
    assert!(asks < mv.find("msg.repo_owner = ").unwrap());
    // Every place that moves the run's repo is one of those, routing (once),
    // a continuation restoring its own plan state's repo, or a multi-repo
    // step's own message for its repo.
    assert_eq!(src.matches("msg.repo_owner = ").count(), 5);
    // The repo the ticket names is read once, with routing.
    assert_eq!(src.matches("repo_pick::ticket_named_repo(").count(), 1);
}

#[test]
fn a_retry_of_a_run_that_worked_elsewhere_starts_over_in_the_named_repo() {
    let h = history();
    let earlier = h.run_repo("01MOVED");
    assert_eq!(earlier, OTHER);
    assert_eq!(
        retry_start(earlier, NAMED, Some(NAMED)),
        RetryStart::StartOver {
            close_in: Some(OTHER.into())
        }
    );
    // A retry of a run in the same repo takes it up, as before.
    assert_eq!(
        retry_start(h.run_repo("01LINKED"), NAMED, Some(NAMED)),
        RetryStart::TakeUp
    );
    // Decided before anything takes up the earlier run: its finished work
    // (the checkpoint) and its plan both read `resume_from`, cleared here.
    let src = mod_src();
    let run = fn_src(src, "\nasync fn run_passes(");
    let decide = run.find("repo_pick::retry_start(").unwrap();
    assert!(run[decide..decide + 600].contains("msg.resume_from = None;"));
    for later in [
        "inherited_checkpoint(state, msg, run_id, &github, usage)",
        "load_run_plan(state, msg, prev)",
        "let plan_final = resumed_plan",
    ] {
        assert!(decide < run.find(later).unwrap(), "{later}");
    }
    // And after routing settled the repo it compares with.
    assert!(run.find("\"Repo named on the ticket itself\"").unwrap() < decide);
    let checkpoint = fn_src(src, "\nasync fn inherited_checkpoint(");
    assert!(checkpoint.contains(".resume_from"));
}

#[test]
fn the_earlier_runs_pull_request_there_is_closed_once_through_the_gate() {
    let mut h = history();
    // Only the retried run's own open pull request: never a person's, and
    // never one another run linked there on purpose.
    let left = h.own_open_prs_of_run("01MOVED", OTHER);
    assert_eq!(left.iter().map(|p| p.number).collect::<Vec<_>>(), [41]);
    let note = repo_pick::left_repo_note(NAMED, OTHER, &left[0].branch);
    assert!(note.contains(NAMED) && note.contains("is kept"));
    // Closed once: the supersede step after the new pull request opens
    // reads it as closed and neither notes nor closes it again.
    h.mark_closed(OTHER, 41);
    assert!(h.own_open_prs_of_run("01MOVED", OTHER).is_empty());
    assert!(!h
        .own_open_prs_elsewhere(&[NAMED.to_string()])
        .iter()
        .any(|p| p.number == 41));

    let src = mod_src();
    // The agent-permission check comes before the note and the close.
    let close = fn_src(src, "\nasync fn close_own_pr(");
    let gate = close.find("may_close_own_pr(").unwrap();
    assert!(gate < close.find("Say::note(").unwrap());
    assert!(gate < close.find("close_pull_request(").unwrap());
    let may = fn_src(src, "\nasync fn may_close_own_pr(");
    assert!(may.contains("tool: \"close_pull_request\""));
    assert_eq!(
        common::agent_policy::registry::BUILTIN
            .iter()
            .find(|(n, _)| *n == "close_pull_request")
            .map(|(_, r)| *r),
        Some(common::agent_policy::Risk::Write)
    );
    // Every close of CoderHelm's own superseded pull requests goes through
    // it, and the retry's close records what it closed.
    let supersede = fn_src(src, "\nasync fn supersede_moved_prs(");
    assert!(supersede.contains("close_own_pr(") && !supersede.contains("close_pull_request("));
    let retry_close = fn_src(src, "\nasync fn close_left_repo_prs(");
    assert!(retry_close.contains("close_own_pr(") && retry_close.contains("mark_closed("));
    // It writes on the pull request only: no ticket comment, no new ticket.
    for never in ["jira", "followup", "ticket_comment"] {
        assert!(!retry_close.to_lowercase().contains(never), "{never}");
    }
}

#[test]
fn the_tickets_branch_in_the_named_repo_is_the_one_continued() {
    let h = history();
    // CoderHelm's own open pull request in the named repo: the run continues
    // on its branch instead of opening a second one.
    let own = h.own_open_pr(NAMED).unwrap();
    assert_eq!((own.number, own.branch.as_str()), (7, "coderhelm/harbor-7"));
    let src = mod_src();
    let run = fn_src(src, "\nasync fn run_passes(");
    let branch = run.find("let branch_name = if can_skip_implement").unwrap();
    let own_open = run[..branch]
        .rfind("history\n        .own_open_pr(&format!(\"{}/{}\", msg.repo_owner, msg.repo_name))")
        .unwrap();
    assert!(own_open < branch);
    assert!(run[branch..].contains("resolve_run_branch("));
    // A branch the ticket already has here, with no pull request, is kept
    // with its commits (never reset to the base and made again), and the
    // coder is told it holds earlier work.
    assert!(run
        .contains("let keep_own_branch = (same_run || retry_keeps_branch || retry_started_over)"));
    let decide = run.find("repo_pick::retry_start(").unwrap();
    assert!(run[decide..decide + 700].contains("retry_started_over = true;"));
    let kept = run.find("} else if keep_own_branch {").unwrap();
    assert!(run[kept..kept + 200].contains("msg.retry_of_run = true;"));
}
