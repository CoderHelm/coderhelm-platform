//! How a run ends and whether its pull request may leave draft, decided from
//! facts in one place, with the rules checked in code.
//!
//! Two runs on one day ended wrong. One stopped in the middle of its planned
//! work (two steps changed nothing), and a separate salvage path opened a
//! pull request titled "Partial", whose body said "Closes #0" for a ticket
//! that was not a GitHub issue and claimed the agent "ran out of time"; the
//! run read "completed" and the pull request was later marked ready. The
//! other run's CI failed, its fix attempt changed nothing, and the run was
//! marked completed with the pull request marked ready while every check was
//! red: the ready decision only read CI when CoderHelm's own checks had
//! findings.
//!
//! The rules, each enforced here and covered by tests:
//!
//! - A run is `completed` only when no planned task is left, no stop reason
//!   is recorded, and the repository's CI is green, absent, or red only on
//!   checks that fail on the base branch too ([`decide`]). Anything else needs
//!   a person, with every reason named.
//! - A pull request leaves draft under the same conditions and never with a
//!   failing or running check ([`ci_blocker`], used by `ready::decide`).
//! - The words a person reads about a stop come from the recorded reason
//!   ([`Stop::words`]): only a recorded time limit says "time".
//! - Every pull request body names its ticket: `Closes #N` only for a real
//!   GitHub issue (N > 0), else `Source ticket: KEY` ([`ticket_line`]); an
//!   issue reference to `#0` never reaches GitHub ([`guard_body`], applied by
//!   the GitHub client to every body it sends).

/// Where the repository's own CI stands on the pull request's head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CiState {
    /// The repository reports no checks at all.
    NoChecks,
    Green,
    /// Checks are still running (their names).
    Pending(String),
    /// Checks failed (their names, and an excerpt when known).
    Failing(String),
    /// Every failing check also fails on the base branch.
    RedOnBase,
}

impl CiState {
    /// From the ready decision's reading of CI.
    pub fn of(ci: &super::ready::Ci, red_on_base: bool) -> CiState {
        if ci.green {
            CiState::Green
        } else if ci.failing() {
            if red_on_base {
                CiState::RedOnBase
            } else {
                CiState::Failing(
                    ci.why
                        .trim_start_matches("CI fails on this head: ")
                        .trim_end_matches('.')
                        .to_string(),
                )
            }
        } else if ci.running() {
            CiState::Pending(
                ci.why
                    .trim_start_matches("CI is still running on this head: ")
                    .trim_end_matches('.')
                    .to_string(),
            )
        } else {
            CiState::NoChecks
        }
    }

    /// Whether this standing lets a run end done and a pull request leave
    /// draft.
    pub fn allows_done(&self) -> bool {
        matches!(
            self,
            CiState::NoChecks | CiState::Green | CiState::RedOnBase
        )
    }
}

/// CoderHelm's review verdict on the head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Review {
    // Read from the review record by the ready decision; the run-end rule
    // reads CI only.
    #[allow(dead_code)]
    Approved,
    ChangesRequested,
    /// No finished review (or the reviewer is off).
    Missing,
}

/// Why a run stopped before its work was done, as the run recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stop {
    /// Two steps in a row left the branch where it was.
    NoProgress {
        /// The step numbers, as the stop recorded them.
        steps: (u32, u32),
        /// Where the run was (`the implement work (planned tasks)`).
        at: String,
        /// The planned task it was on, when known.
        task: Option<String>,
    },
    /// A step reached its time limit and no fresh step could take over.
    OutOfTime(String),
    /// Anything else, in the run's own words.
    Other(String),
}

impl Stop {
    /// Reads the recorded stop sentence back into its kind. The sentences
    /// are written by `steps::stall_reason` and the timeout arm; anything
    /// else is kept as written.
    pub fn from_recorded(text: &str) -> Stop {
        let t = text.trim();
        if let Some(rest) = t.strip_prefix("Stopped: steps ") {
            let mut nums = rest
                .split(|c: char| !c.is_ascii_digit())
                .filter(|s| !s.is_empty())
                .filter_map(|s| s.parse::<u32>().ok());
            if let (Some(a), Some(b)) = (nums.next(), nums.next()) {
                if rest.contains("made no progress") {
                    let at = rest
                        .split_once("stuck at ")
                        .map(|(_, r)| r.split(". ").next().unwrap_or(r).trim().to_string())
                        .unwrap_or_default();
                    let task = rest
                        .split_once("while working on ")
                        .map(|(_, r)| r.split(". ").next().unwrap_or(r).trim().to_string());
                    return Stop::NoProgress {
                        steps: (a, b),
                        at,
                        task,
                    };
                }
            }
        }
        if t.contains("reached its time limit") {
            return Stop::OutOfTime(t.to_string());
        }
        Stop::Other(t.to_string())
    }

    /// The reason a person reads. Only a recorded time limit mentions time.
    pub fn words(&self) -> String {
        match self {
            Stop::NoProgress { steps, at, task } => {
                let on = match task {
                    Some(t) if !t.is_empty() => format!(" while working on {t}"),
                    _ if !at.is_empty() => format!(" at {at}"),
                    _ => String::new(),
                };
                format!(
                    "steps {} and {} made no change to the branch{on}, so the run stopped \
                     instead of repeating them",
                    steps.0, steps.1
                )
            }
            Stop::OutOfTime(why) | Stop::Other(why) => why.trim().trim_end_matches('.').to_string(),
        }
    }
}

/// What the run's ending is decided from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    /// Planned tasks not done (paths or one-line tasks).
    pub tasks_remaining: Vec<String>,
    pub ci: CiState,
    pub review: Review,
    /// The recorded reason the run stopped early, if it did.
    pub stop: Option<Stop>,
}

/// How the run's status reads when it ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    /// Done: nothing is left for a person beyond reviewing the pull request.
    Completed,
    /// A person must act; the reasons say what.
    NeedsAttention,
    /// Nothing is wrong yet; CI is still running.
    Waiting,
}

/// The decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub status: RunStatus,
    /// The pull request may leave draft.
    pub pr_ready: bool,
    /// Every reason it isn't done, in the words a person reads.
    pub reasons: Vec<String>,
}

/// The reason CI keeps a pull request a draft and a run from being done, or
/// `None` when CI allows it.
pub fn ci_blocker(ci: &CiState) -> Option<String> {
    match ci {
        CiState::Failing(checks) => Some(format!("CI is failing: {checks}")),
        CiState::Pending(checks) => Some(format!("CI is still running: {checks}")),
        CiState::NoChecks | CiState::Green | CiState::RedOnBase => None,
    }
}

/// The one decision.
pub fn decide(f: &Facts) -> Outcome {
    let mut reasons = Vec::new();
    if let Some(stop) = &f.stop {
        reasons.push(format!("The run stopped: {}", stop.words()));
    }
    if !f.tasks_remaining.is_empty() {
        reasons.push(format!(
            "{} planned task(s) not done: {}",
            f.tasks_remaining.len(),
            f.tasks_remaining.join("; ")
        ));
    }
    let ci = ci_blocker(&f.ci);
    let only_waiting = reasons.is_empty() && matches!(f.ci, CiState::Pending(_));
    if let Some(c) = ci {
        reasons.push(c);
    }
    if f.review == Review::ChangesRequested {
        reasons.push("CoderHelm's review asked for changes on this head".to_string());
    }
    let status = if reasons.is_empty() {
        RunStatus::Completed
    } else if only_waiting && f.review != Review::ChangesRequested {
        RunStatus::Waiting
    } else {
        RunStatus::NeedsAttention
    };
    Outcome {
        status,
        pr_ready: reasons.is_empty(),
        reasons,
    }
}

/// The rules an outcome must keep, as a list of what it breaks (empty when
/// it keeps them all). The tests drive every combination through it.
pub fn broken_rules(f: &Facts, o: &Outcome) -> Vec<&'static str> {
    let mut out = Vec::new();
    let done = o.status == RunStatus::Completed;
    if (done || o.pr_ready) && !f.tasks_remaining.is_empty() {
        out.push("done or ready with planned tasks left");
    }
    if (done || o.pr_ready) && !f.ci.allows_done() {
        out.push("done or ready with failing or running CI");
    }
    if (done || o.pr_ready) && f.stop.is_some() {
        out.push("done or ready after a recorded stop");
    }
    if o.pr_ready && f.review == Review::ChangesRequested {
        out.push("ready with the review asking for changes");
    }
    if !done && o.reasons.is_empty() {
        out.push("not done without a named reason");
    }
    let says_time = o
        .reasons
        .iter()
        .any(|r| r.to_lowercase().contains("time limit") || r.contains("ran out of time"));
    if says_time && !matches!(f.stop, Some(Stop::OutOfTime(_))) {
        out.push("claims a time limit that isn't the recorded reason");
    }
    out
}

// ── Pull request bodies ─────────────────────────────────────────────────────

/// The ticket line every pull request body starts with: `Closes #N` for a
/// real GitHub issue, else `Source ticket: KEY`.
pub fn ticket_line(github_issue: bool, issue_number: u64, ticket_id: &str) -> String {
    if github_issue && issue_number > 0 {
        format!("Closes #{issue_number}")
    } else {
        format!("Source ticket: {}", ticket_id.trim())
    }
}

/// [`ticket_line`] for a run's ticket.
pub fn ticket_line_for(msg: &crate::models::TicketMessage) -> String {
    ticket_line(
        matches!(msg.source, crate::models::TicketSource::Github),
        msg.issue_number,
        &msg.ticket_id,
    )
}

/// Keywords GitHub reads as "this pull request closes that issue".
const CLOSING: &[&str] = &[
    "close", "closes", "closed", "fix", "fixes", "fixed", "resolve", "resolves", "resolved",
];

/// Whether `line` refers to issue `#0` (with or without a closing keyword).
fn names_issue_zero(line: &str) -> bool {
    let b = line.as_bytes();
    let mut i = 0;
    while let Some(off) = line[i..].find("#0") {
        let at = i + off;
        let after = b.get(at + 2).copied();
        let before = if at == 0 {
            None
        } else {
            b.get(at - 1).copied()
        };
        let ends = after.is_none_or(|c| !c.is_ascii_alphanumeric());
        let starts = before.is_none_or(|c| !c.is_ascii_alphanumeric() && c != b'&');
        if ends && starts {
            return true;
        }
        i = at + 2;
    }
    false
}

/// The body with every reference to issue `#0` taken out: a line that is
/// only a closing keyword and `#0` goes, any other `#0` reference loses its
/// closing keyword and number. Applied to every body the GitHub client sends.
pub fn guard_body(body: &str) -> String {
    if !body.contains("#0") {
        return body.to_string();
    }
    let mut out: Vec<String> = Vec::new();
    for line in body.lines() {
        if !names_issue_zero(line) {
            out.push(line.to_string());
            continue;
        }
        let words: Vec<&str> = line.split_whitespace().collect();
        let only_ref = words.iter().all(|w| {
            let lw = w
                .trim_matches(|c: char| c == ':' || c == ',')
                .to_lowercase();
            CLOSING.contains(&lw.as_str()) || names_issue_zero(w)
        });
        if only_ref {
            continue;
        }
        let mut kept: Vec<&str> = Vec::new();
        for w in words {
            if names_issue_zero(w) {
                if let Some(prev) = kept.last() {
                    let lw = prev.trim_matches(':').to_lowercase();
                    if CLOSING.contains(&lw.as_str()) {
                        kept.pop();
                    }
                }
                continue;
            }
            kept.push(w);
        }
        out.push(kept.join(" "));
    }
    let mut s = out.join("\n");
    while s.starts_with('\n') {
        s.remove(0);
    }
    s
}

/// What a run's pull request body breaks: no ticket reference, an issue
/// `#0`, or a missing standard section. Empty when it keeps them all.
pub fn body_problems(body: &str, ticket_id: &str) -> Vec<String> {
    let mut out = Vec::new();
    let has_ticket = body.contains(&format!("Source ticket: {ticket_id}"))
        || body.lines().any(|l| {
            let l = l.trim();
            (l.starts_with("Closes #") || l.starts_with("Fixes #") || l.starts_with("Resolves #"))
                && !names_issue_zero(l)
        });
    if !has_ticket {
        out.push("no ticket reference".to_string());
    }
    if names_issue_zero(body) {
        out.push("refers to issue #0".to_string());
    }
    for section in ["Summary", "Changes", "Verification"] {
        let found = body.lines().any(|l| {
            let l = l
                .trim()
                .trim_start_matches('#')
                .trim()
                .trim_matches('*')
                .trim();
            l.eq_ignore_ascii_case(section)
        });
        if !found {
            out.push(format!("no {section} section"));
        }
    }
    if body.contains("⚠️ Partial") || body.contains("ran out of time before finishing") {
        out.push("the old partial-salvage wording".to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_ci() -> Vec<CiState> {
        vec![
            CiState::NoChecks,
            CiState::Green,
            CiState::Pending("`build`".into()),
            CiState::Failing("`e2e`".into()),
            CiState::RedOnBase,
        ]
    }

    fn all_stops() -> Vec<Option<Stop>> {
        vec![
            None,
            Some(Stop::from_recorded(
                "Stopped: steps 2 and 3 made no progress. The branch stayed at a8c8814 and the run \
                 was stuck at the implement work (planned tasks). The work so far is kept on \
                 branch `coderhelm-wip/x`.",
            )),
            Some(Stop::OutOfTime(
                "The step reached its time limit and the run couldn't continue in a fresh step: \
                 the usage limit."
                    .into(),
            )),
            Some(Stop::Other("A check after the work errored: the link check.".into())),
        ]
    }

    /// Every combination of tasks, CI, review and stop keeps every rule.
    #[test]
    fn every_combination_keeps_the_rules() {
        let tasks = [vec![], vec!["`web/a.ts`: add the hook".to_string()]];
        let reviews = [Review::Approved, Review::ChangesRequested, Review::Missing];
        let mut n = 0;
        for t in &tasks {
            for ci in all_ci() {
                for r in reviews {
                    for stop in all_stops() {
                        let f = Facts {
                            tasks_remaining: t.clone(),
                            ci: ci.clone(),
                            review: r,
                            stop: stop.clone(),
                        };
                        let o = decide(&f);
                        assert!(
                            broken_rules(&f, &o).is_empty(),
                            "{f:?} {o:?}: {:?}",
                            broken_rules(&f, &o)
                        );
                        n += 1;
                    }
                }
            }
        }
        assert_eq!(n, 2 * 5 * 3 * 4);
    }

    #[test]
    fn the_rules_catch_an_ending_that_breaks_them() {
        // The red-CI run: completed and ready with every check failing.
        let f = Facts {
            tasks_remaining: vec![],
            ci: CiState::Failing("`Deploy Preview`".into()),
            review: Review::Missing,
            stop: None,
        };
        let wrong = Outcome {
            status: RunStatus::Completed,
            pr_ready: true,
            reasons: vec![],
        };
        assert!(broken_rules(&f, &wrong).contains(&"done or ready with failing or running CI"));
        // The salvaged run: "ran out of time" for a stop that was no progress.
        let f = Facts {
            tasks_remaining: vec!["`web/useScroll.ts`".into()],
            ci: CiState::NoChecks,
            review: Review::Missing,
            stop: Some(Stop::from_recorded(
                "Stopped: steps 2 and 3 made no progress.",
            )),
        };
        let wrong = Outcome {
            status: RunStatus::Completed,
            pr_ready: false,
            reasons: vec!["the agent ran out of time before finishing all tasks".into()],
        };
        let broken = broken_rules(&f, &wrong);
        assert!(broken.contains(&"done or ready with planned tasks left"));
        assert!(broken.contains(&"claims a time limit that isn't the recorded reason"));
    }

    #[test]
    fn red_ci_needs_a_person_and_running_ci_is_a_wait() {
        let mut f = Facts {
            tasks_remaining: vec![],
            ci: CiState::Failing("`e2e`".into()),
            review: Review::Approved,
            stop: None,
        };
        let o = decide(&f);
        assert_eq!(o.status, RunStatus::NeedsAttention);
        assert!(!o.pr_ready);
        assert_eq!(o.reasons, vec!["CI is failing: `e2e`".to_string()]);
        f.ci = CiState::Pending("`build`".into());
        assert_eq!(decide(&f).status, RunStatus::Waiting);
        f.ci = CiState::RedOnBase;
        assert_eq!(decide(&f).status, RunStatus::Completed);
    }

    #[test]
    fn a_no_progress_stop_says_what_happened_never_time() {
        let s = Stop::from_recorded(
            "Stopped: steps 2 and 3 made no progress. The branch stayed at a8c8814 and the run \
             was stuck at the implement work (planned tasks) while working on task 8 of 8 \
             (`web/useScroll.ts`: adapt the hook). The work so far is kept on branch `x`.",
        );
        let w = s.words();
        assert!(
            w.starts_with(
                "steps 2 and 3 made no change to the branch while working on task 8 of 8"
            ),
            "{w}"
        );
        assert!(!w.to_lowercase().contains("time"), "{w}");
    }

    #[test]
    fn issue_zero_never_renders() {
        assert_eq!(ticket_line(true, 0, "GH-0"), "Source ticket: GH-0");
        assert_eq!(ticket_line(false, 0, "WEB-12"), "Source ticket: WEB-12");
        assert_eq!(ticket_line(true, 41, "GH-41"), "Closes #41");
        let old = "Closes #0\n\n⚠️ **This implementation is partial**";
        let g = guard_body(old);
        assert!(!g.contains("#0"), "{g}");
        assert!(g.starts_with("⚠️"));
        assert_eq!(guard_body("Fixes #0, see notes"), "see notes");
        // Real references and look-alikes stay.
        assert_eq!(guard_body("Closes #10"), "Closes #10");
        assert_eq!(
            guard_body("color #0a0a0a and &#0;"),
            "color #0a0a0a and &#0;"
        );
    }

    #[test]
    fn a_body_without_the_ticket_or_sections_is_named() {
        let good =
            "Source ticket: WEB-12\n\n## Summary\nx\n\n## Changes\n- y\n\n## Verification\nz";
        assert!(body_problems(good, "WEB-12").is_empty());
        let old = "Closes #0\n\n⚠️ **This implementation is partial** — the agent ran out of time \
                   before finishing all tasks.";
        let p = body_problems(old, "WEB-12");
        assert!(p.contains(&"no ticket reference".to_string()));
        assert!(p.contains(&"refers to issue #0".to_string()));
        assert!(p.contains(&"no Summary section".to_string()));
        assert!(p.contains(&"the old partial-salvage wording".to_string()));
    }
}
