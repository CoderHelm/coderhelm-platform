//! One rule for how every run ends: it finishes, or it hands back only for
//! something on the human-only list.
//!
//! Runs too often ended by handing unfinished work to a person: two steps
//! that changed nothing stopped a ticket run with a held draft, a CI fix
//! that met the same failure twice set the run to `needs_input`, a review
//! fix loop at its round cap asked a person to finish, a check after the
//! work that errored became "a person should look". Each loop (the implement
//! steps, the CI fix, CoderHelm's own review, the planned-work rounds, the
//! progress guard, the ready decision) had its own stall rule ending in a
//! hand-back, and every earlier fix changed one loop's rule; the next loop,
//! or a new run type, kept its own and the class came back.
//!
//! Now the class is closed where every ending passes, and in the type that
//! writes the status:
//!
//! - **A hand-back needs a reason on the list.** `needs_input` is written
//!   only by `run_wait::NeedsInput::needs_input`, which takes a
//!   [`HandBack`]; a `HandBack` exists only for a [`HumanOnly`] reason (a
//!   credential, missing access, a product decision, an ambiguous ticket,
//!   someone else holding the work, the usage limit, an approval, a
//!   person's own action). A stall, a red check, a budget or an error has
//!   none, so it can't end a run as a wait. [`hand_back_for`] is the one
//!   reading of a stop: the stop judge's question or blocker when it names
//!   something on the list, else the stop's own words.
//! - **Every ending that used to hand back now finishes**:
//!   - a stop after the work ships what is done (`stop_pr::ship`): the
//!     tasks left are deferred with why, the pull request opens with a
//!     "Deferred" section, and the run goes on to CI and the ready decision
//!     like any finished run;
//!   - red checks the fix attempts couldn't repair are taken out
//!     (`resume::revert_to_green`, [`revert_for`]): back to the last green
//!     head of the pull request, else the last planned task, deferred; CI
//!     decides again. Only when nothing can come out does the run end, as
//!     `failed` with exactly what fails, never as a wait;
//!   - the own-review and guard stops finish on the pull request, with what
//!     is open listed there (`resume::wait_on_person`,
//!     `run_guard::finish_open_pr`);
//!   - a held draft on a finished run stays recorded and named on the pull
//!     request without turning the run into a wait (`ready::draft_hand_back`).
//! - **A stall changes approach** ([`next_rung`]): one task only, then a
//!   fresh context with edits in place instead of whole-file writes, then
//!   the task re-split, then the strongest model for a core task, then the
//!   stuck task deferred with its reason, and finally what is done ships. A
//!   non-core task (a readme line, a changelog) is deferred after three.
//! - **Budget follows progress** ([`may_extend`]): a loop at its round cap
//!   that is still fixing things gets more rounds, up to twice the cap; a
//!   run near its spend ceiling ships what is done ([`near_spend_cap`]).
//!
//! The tests at the bottom replay every historical stuck run's shape
//! ([`replay`], `supervisor_replays.rs`) and read the worker's sources so a
//! new ending can't go around these rules.

// ── The human-only list ─────────────────────────────────────────────────────

pub use common::handback::{human_only_in, HandBack, HumanOnly, HUMAN_ONLY};

/// The hand-back a stop may make, if any: the stop judge's question or a
/// hold on something only a person has, else the stop's own words naming
/// one. `None` for everything CoderHelm finishes itself.
pub fn hand_back_for(judged: &super::stop_judge::Judged, reason: &str) -> Option<HandBack> {
    HandBack::of_judged(judged)
        .or_else(|| human_only_in(reason).and_then(|h| HandBack::new(h, reason)))
}

// ── The ladder ──────────────────────────────────────────────────────────────

/// What the next attempt does differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rung {
    /// The same work continues: the last attempt made progress.
    Continue,
    /// One task only, the first one left.
    Narrow,
    /// A fresh context (the plan, the diff, the one task) and edits in place
    /// instead of whole-file writes.
    SwitchTools,
    /// The task split into two or three smaller pieces, the first one now.
    Resplit,
    /// The strongest model, fresh context (core tasks).
    Escalate,
    /// The stuck task is deferred with its reason; the rest goes on.
    Defer,
    /// What is done ships; anything left is deferred and explained.
    Ship,
}

impl Rung {
    pub fn as_str(self) -> &'static str {
        match self {
            Rung::Continue => "continue",
            Rung::Narrow => "narrow",
            Rung::SwitchTools => "switch_tools",
            Rung::Resplit => "resplit",
            Rung::Escalate => "escalate",
            Rung::Defer => "defer",
            Rung::Ship => "ship",
        }
    }
}

/// Whether a planned task is core to the ticket. Documentation, changelog
/// and readme edits, and tasks the plan marks optional, are not: when they
/// stall they are deferred first.
pub fn is_core(path: &str, task: &str, optional: bool) -> bool {
    if optional {
        return false;
    }
    let p = path.to_lowercase();
    let name = p.rsplit('/').next().unwrap_or(&p);
    let doc_file = name.starts_with("readme")
        || name.starts_with("changelog")
        || name.starts_with("contributing")
        || name.ends_with(".md")
        || name.ends_with(".mdx")
        || name.ends_with(".rst")
        || p.starts_with("docs/")
        || p.contains("/docs/");
    let t = task.to_lowercase();
    let doc_task = [
        "document ",
        "documentation",
        "readme",
        "changelog",
        "release note",
    ]
    .iter()
    .any(|m| t.contains(m))
        && !["implement", "fix ", "bump", "upgrade", "update the version"]
            .iter()
            .any(|m| t.contains(m));
    !(doc_file || doc_task)
}

/// The rung after `stalled` attempts in a row without progress, on a task
/// that is core or not. Zero stalls continue as before.
pub fn next_rung(stalled: u32, core: bool) -> Rung {
    match (stalled, core) {
        (0, _) => Rung::Continue,
        (1, _) => Rung::Narrow,
        (2, _) => Rung::SwitchTools,
        (3, true) => Rung::Resplit,
        // A non-core task (a readme line, a changelog) that resisted three
        // approaches isn't worth more: it is deferred with the reason.
        (3, false) => Rung::Defer,
        (4, true) => Rung::Escalate,
        (5, true) => Rung::Defer,
        _ => Rung::Ship,
    }
}

/// The share of the spend ceiling at which a run ships what is done.
pub const SHIP_AT_SPEND_SHARE: f64 = 0.85;

/// Whether the run's spend is near its ceiling: ship, don't stop mid-way.
pub fn near_spend_cap(spent_usd: f64, cap_usd: f64) -> bool {
    cap_usd > 0.0 && spent_usd >= cap_usd * SHIP_AT_SPEND_SHARE
}

/// Whether a loop at `rounds` with cap `cap` may take another round: under
/// the cap always, past it only while the last round made progress, up to
/// twice the cap.
pub fn may_extend(rounds: u32, cap: u32, progressed: bool) -> bool {
    rounds < cap || (progressed && rounds < cap.saturating_mul(2))
}

/// The rung for this attempt, with the spend ceiling: a run near its
/// ceiling ships.
pub fn rung_for(stalled: u32, core: bool, spent_usd: f64, cap_usd: f64) -> Rung {
    if near_spend_cap(spent_usd, cap_usd) {
        return Rung::Ship;
    }
    next_rung(stalled, core)
}

/// What a rung tells the coder: the instruction that changes the approach.
/// Empty for [`Rung::Continue`], [`Rung::Defer`] and [`Rung::Ship`], which
/// the supervisor carries out itself.
pub fn rung_instruction(rung: Rung, task: &str, path: &str) -> String {
    match rung {
        Rung::Continue | Rung::Defer | Rung::Ship => String::new(),
        Rung::Narrow => format!(
            "The last attempt changed nothing. Take one task only: {task}. Make the change in \
             `{path}`, or, if the file already does what the task asks, write the line \
             `No change needed: {path}: <why>` in your final message."
        ),
        Rung::SwitchTools => format!(
            "Two attempts changed nothing on this task: {task}. Start fresh from the plan and the \
             branch's diff; don't redo finished work. Change the tool approach: read `{path}` in \
             the parts you need and edit it in place with small edits, never rewrite the whole \
             file. If an edit is refused, read the exact current text around it and edit again. \
             Keep your thinking short and save the file before you stop."
        ),
        Rung::Resplit => format!(
            "This task resisted three attempts: {task}. Split it into two or three smaller \
             changes in `{path}`, write the list as the first lines of your final message, and \
             make only the first change now, saved. The next step takes the next one."
        ),
        Rung::Escalate => format!(
            "This core task resisted four attempts: {task}. Work from the plan and the branch \
             as it is now. Read the code around `{path}` and the tests that cover it, find why \
             earlier attempts didn't land (a refused edit, a wrong file, a failing build), and \
             make the change. Save the file before you stop."
        ),
    }
}

/// The reason written beside a deferred task.
pub fn defer_reason(rung_count: u32, task: &str, core: bool) -> String {
    let kind = if core { "task" } else { "non-core task" };
    format!(
        "deferred: this {kind} resisted {rung_count} different approaches ({}); the rest of the \
         work ships without it and the pull request lists it",
        common::truncate_str(task, 160)
    )
}

// ── Red checks ──────────────────────────────────────────────────────────────

/// How many reverts the supervisor makes on one pull request before the
/// run ends with what still fails.
pub const MAX_REVERTS: u32 = 3;

/// What the supervisor takes out so what ships is green.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Revert {
    /// Back to this earlier head of the pull request, whose checks passed.
    ToHead(String),
    /// The last planned task's file, back to the base branch's version.
    Task { path: String, task: String },
    /// Nothing can come out without emptying the pull request.
    Nothing,
}

/// The revert for red checks after the fix attempts: the newest earlier
/// head whose checks passed (`heads`: older heads, newest first, with
/// whether their checks passed), else the last done task when more than one
/// is done (`done`: path and task, in plan order), else nothing.
pub fn revert_for(heads: &[(String, bool)], done: &[(String, String)], reverts: u32) -> Revert {
    if reverts >= MAX_REVERTS {
        return Revert::Nothing;
    }
    if let Some((sha, _)) = heads.iter().find(|(_, green)| *green) {
        return Revert::ToHead(sha.clone());
    }
    match done {
        [_, .., (path, task)] => Revert::Task {
            path: path.clone(),
            task: task.clone(),
        },
        _ => Revert::Nothing,
    }
}

// ── The ending ──────────────────────────────────────────────────────────────

/// What a run's ending is decided from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EndFacts {
    /// Planned tasks not done and not deferred.
    pub open_tasks: Vec<String>,
    /// Tasks deferred, each with its reason.
    pub deferred: Vec<String>,
    /// Checks of ours failing on the head.
    pub ci_failing: Vec<String>,
    pub ci_running: bool,
    /// A recorded human-only blocker, when one exists.
    pub blocker: Option<HandBack>,
    /// The branch has work (beyond the plan files).
    pub has_work: bool,
    /// The ladder is spent (the run reached [`Rung::Ship`]).
    pub ladder_spent: bool,
}

/// How a run ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    /// Keep working: tasks are open and the ladder isn't spent.
    KeepGoing,
    /// CI is still running.
    Wait,
    /// Finished: the pull request carries the work, with what was deferred
    /// listed and why.
    Finished { deferred: Vec<String> },
    /// Nothing could ship (no work, or every change breaks the checks): the
    /// run failed, with the exact cause. Not a question to a person.
    Failed,
    /// A human-only blocker: the one need, and a notification.
    HandBack(HandBack),
}

impl Ending {
    /// The run status this ending writes.
    pub fn status(&self) -> &'static str {
        match self {
            Ending::KeepGoing => common::run_status::RUNNING,
            Ending::Wait => common::run_status::AWAITING_CI,
            Ending::Finished { .. } => common::run_status::COMPLETED,
            Ending::Failed => "failed",
            Ending::HandBack(_) => common::run_status::NEEDS_INPUT,
        }
    }
}

/// The one ending decision, as every ending site applies it.
pub fn decide_end(f: &EndFacts) -> Ending {
    if let Some(h) = &f.blocker {
        return Ending::HandBack(h.clone());
    }
    if !f.open_tasks.is_empty() && !f.ladder_spent {
        return Ending::KeepGoing;
    }
    if !f.has_work || !f.ci_failing.is_empty() {
        return Ending::Failed;
    }
    if f.ci_running {
        return Ending::Wait;
    }
    // The ladder is spent: what is still open is deferred with the rest.
    let mut deferred = f.deferred.clone();
    deferred.extend(
        f.open_tasks
            .iter()
            .map(|t| format!("{t} (deferred: the run's approaches are spent)")),
    );
    Ending::Finished { deferred }
}

/// [`decide_end`], with its rules checked: a slip is logged loudly (and
/// the tests drive every combination, so none ships).
pub fn end(f: &EndFacts) -> Ending {
    let e = decide_end(f);
    let b = broken(f, &e);
    if !b.is_empty() {
        tracing::error!(broken = ?b, "A run ending breaks the supervisor's rules");
    }
    e
}

/// The rules an ending keeps, as what it breaks (empty when it keeps them).
pub fn broken(f: &EndFacts, e: &Ending) -> Vec<&'static str> {
    let mut out = Vec::new();
    match e {
        Ending::HandBack(h) => {
            if !HUMAN_ONLY.contains(&h.why()) {
                out.push("hands back for a reason not on the human-only list");
            }
            if h.need().trim().is_empty() {
                out.push("hands back without saying what is needed");
            }
            if f.blocker.is_none() {
                out.push("hands back without a recorded blocker");
            }
        }
        Ending::Finished { deferred } => {
            if !f.ci_failing.is_empty() {
                out.push("finished with failing checks of ours");
            }
            if deferred.len() < f.open_tasks.len() + f.deferred.len() {
                out.push("finished with open tasks that aren't deferred");
            }
        }
        Ending::KeepGoing if f.ladder_spent => out.push("keeps going after the ladder is spent"),
        _ => {}
    }
    if f.blocker.is_none() && e.status() == common::run_status::NEEDS_INPUT {
        out.push("waits on a person without a human-only reason");
    }
    out
}

// ── Replays ────────────────────────────────────────────────────────────────

/// What stopped a historical run, as its shape.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayStop {
    /// Steps or rounds that changed nothing on a planned task (plan
    /// coverage, turn or step caps, replies that only thought).
    Stall,
    /// A step's time ran out and no fresh step took over.
    OutOfTime,
    /// A pass or check errored (a gate, GitHub refusing, the planner, the
    /// coder writing nothing).
    Errored,
    /// Red checks of ours the fix attempts couldn't repair.
    RedCi,
    /// Red checks that fail on the base branch too.
    RedOnBase,
    /// CoderHelm's own review findings left open after its fix rounds.
    OwnFindings,
    /// A need only a person has.
    Asked(HumanOnly),
}

/// Replays a historical run's shape through the rules: the ending it would
/// reach now. Every attempt is assumed to change nothing (the worst case
/// the old run met), so a run finishes only on what it had already done.
/// `done` and `total` are its planned tasks.
#[cfg(test)]
pub fn replay(stop: ReplayStop, done: u32, total: u32) -> Ending {
    let total = total.max(done);
    let open: Vec<String> = (done + 1..=total).map(|i| format!("task {i}")).collect();
    let mut f = EndFacts {
        has_work: done > 0,
        ..Default::default()
    };
    match stop {
        ReplayStop::Asked(why) => {
            f.blocker = Some(HandBack::of(why));
        }
        ReplayStop::Stall | ReplayStop::OutOfTime | ReplayStop::Errored => {
            // An error first gets one fresh step; a stall climbs the ladder.
            // Each open task climbs until it is deferred; none lands.
            for t in &open {
                let mut stalled = 0;
                while !matches!(next_rung(stalled, true), Rung::Defer | Rung::Ship) {
                    stalled += 1;
                }
                f.deferred
                    .push(format!("{t} ({})", defer_reason(stalled, t, true)));
            }
            f.ladder_spent = true;
        }
        ReplayStop::RedCi => {
            // The fix attempts didn't repair it: take out the last task
            // while more than one is done (no green head is assumed).
            let tasks: Vec<(String, String)> = (1..=done)
                .map(|i| (format!("src/f{i}.rs"), format!("task {i}")))
                .collect();
            match revert_for(&[], &tasks, 0) {
                Revert::Task { task, .. } => f
                    .deferred
                    .push(format!("{task} (deferred: its change broke the checks)")),
                Revert::ToHead(_) => {}
                Revert::Nothing => f.ci_failing = vec!["the checks".to_string()],
            }
            f.deferred
                .extend(open.iter().map(|t| format!("{t} (deferred)")));
            f.ladder_spent = true;
        }
        ReplayStop::RedOnBase | ReplayStop::OwnFindings => {
            f.deferred = open.iter().map(|t| format!("{t} (deferred)")).collect();
            f.has_work = true;
            f.ladder_spent = true;
        }
    }
    decide_end(&f)
}

#[cfg(test)]
#[path = "supervisor_replays.rs"]
mod replays;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stall_changes_approach_and_never_stops() {
        let core: Vec<Rung> = (0..7).map(|s| next_rung(s, true)).collect();
        assert_eq!(
            core,
            vec![
                Rung::Continue,
                Rung::Narrow,
                Rung::SwitchTools,
                Rung::Resplit,
                Rung::Escalate,
                Rung::Defer,
                Rung::Ship
            ]
        );
        assert_eq!(next_rung(3, false), Rung::Defer);
        for s in 1..6 {
            let r = next_rung(s, true);
            if !matches!(r, Rung::Defer | Rung::Ship) {
                assert!(!rung_instruction(r, "t", "p").is_empty(), "{r:?}");
            }
        }
    }

    #[test]
    fn docs_tasks_are_not_core() {
        assert!(!is_core("README.md", "note the new minimum version", false));
        assert!(!is_core("docs/setup.md", "document the flag", false));
        assert!(!is_core("CHANGELOG.md", "add the entry", false));
        assert!(is_core("web/package.json", "bump the framework", false));
        assert!(is_core(
            "src/api/handler.rs",
            "document the error path and fix the panic",
            false
        ));
        assert!(!is_core("src/a.rs", "x", true));
    }

    #[test]
    fn near_the_spend_cap_the_run_ships_and_progress_earns_rounds() {
        assert_eq!(rung_for(1, true, 180.0, 200.0), Rung::Ship);
        assert_eq!(rung_for(1, true, 20.0, 200.0), Rung::Narrow);
        assert!(may_extend(3, 3, true));
        assert!(!may_extend(3, 3, false));
        assert!(!may_extend(6, 3, true));
    }

    #[test]
    fn red_checks_revert_to_green_then_by_task_and_never_empty_the_pull_request() {
        let heads = vec![("c3".to_string(), false), ("c2".to_string(), true)];
        let done = vec![
            ("src/a.rs".to_string(), "fix a".to_string()),
            ("src/b.rs".to_string(), "fix b".to_string()),
        ];
        assert_eq!(revert_for(&heads, &done, 0), Revert::ToHead("c2".into()));
        let red = vec![("c1".to_string(), false)];
        assert_eq!(
            revert_for(&red, &done, 1),
            Revert::Task {
                path: "src/b.rs".into(),
                task: "fix b".into()
            }
        );
        assert_eq!(revert_for(&red, &done[..1], 0), Revert::Nothing);
        assert_eq!(revert_for(&heads, &done, MAX_REVERTS), Revert::Nothing);
    }

    #[test]
    fn every_ending_keeps_the_rules() {
        let blockers = [None, Some(HandBack::of(HumanOnly::Credential))];
        let mut n = 0;
        for open in [vec![], vec!["`a.rs`: x".to_string()]] {
            for failing in [vec![], vec!["build".to_string()]] {
                for running in [false, true] {
                    for b in &blockers {
                        for has_work in [false, true] {
                            for spent in [false, true] {
                                let f = EndFacts {
                                    open_tasks: open.clone(),
                                    deferred: vec![],
                                    ci_failing: failing.clone(),
                                    ci_running: running,
                                    blocker: b.clone(),
                                    has_work,
                                    ladder_spent: spent,
                                };
                                let e = decide_end(&f);
                                assert!(broken(&f, &e).is_empty(), "{f:?} {e:?}");
                                n += 1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(n, 64);
    }

    /// The four runs the investigation started from, by shape: a security
    /// run stuck on its ninth task (a readme line); a finished security run
    /// whose pull request GitHub refused; a run stuck after seven of eight
    /// tasks; a CI fix that gave up after one try. None waits on a person.
    #[test]
    fn the_four_named_runs_finish_now() {
        for (stop, done, total) in [
            (ReplayStop::Stall, 8, 9),
            (ReplayStop::Errored, 24, 24),
            (ReplayStop::Stall, 7, 8),
            (ReplayStop::RedCi, 8, 8),
        ] {
            let e = replay(stop, done, total);
            assert!(
                matches!(e, Ending::Finished { .. }),
                "{stop:?} {done}/{total}: {e:?}"
            );
        }
    }

    /// The invariant over the whole replay suite: no run ends waiting on a
    /// person unless what blocked it is on the human-only list, every run
    /// only a person could unblock still waits, and every replay ends.
    #[test]
    fn across_the_replays_only_human_only_blockers_wait() {
        let (mut finished, mut failed, mut waits) = (0, 0, 0);
        for c in replays::CASES {
            let e = replay(c.stop, c.done, c.total);
            match &e {
                Ending::HandBack(h) => {
                    assert!(HUMAN_ONLY.contains(&h.why()), "{}: {e:?}", c.name);
                    assert!(
                        matches!(c.stop, ReplayStop::Asked(_)),
                        "{}: waits without a recorded need",
                        c.name
                    );
                    waits += 1;
                }
                Ending::Finished { .. } => finished += 1,
                Ending::Failed => failed += 1,
                other => panic!("{}: a replay must end, got {other:?}", c.name),
            }
            if c.human_only && matches!(c.stop, ReplayStop::Asked(_)) {
                assert!(matches!(e, Ending::HandBack(_)), "{}: {e:?}", c.name);
            }
            if c.human_only {
                assert!(!matches!(e, Ending::Failed), "{}: {e:?}", c.name);
            }
            // A run that had work and stopped on something CoderHelm owns
            // ships it now.
            if !c.human_only && c.done > 1 && !matches!(c.stop, ReplayStop::Asked(_)) {
                assert!(matches!(e, Ending::Finished { .. }), "{}: {e:?}", c.name);
            }
        }
        assert_eq!(finished + failed + waits, replays::CASES.len());
        // Before: the same shapes through the old outcome rule
        // (`outcome::decide`), where any stop, task left or red check
        // needed a person.
        let mut before_waits = 0;
        for c in replays::CASES {
            let open: Vec<String> = (c.done + 1..=c.total.max(c.done))
                .map(|i| format!("task {i}"))
                .collect();
            let stop = match c.stop {
                ReplayStop::RedOnBase => None,
                ReplayStop::OutOfTime => Some(super::super::outcome::Stop::OutOfTime(
                    "The step reached its time limit".into(),
                )),
                _ => Some(super::super::outcome::Stop::Other("stopped".into())),
            };
            let ci = match c.stop {
                ReplayStop::RedCi => super::super::outcome::CiState::Failing("ci".into()),
                ReplayStop::RedOnBase => super::super::outcome::CiState::RedOnBase,
                _ => super::super::outcome::CiState::NoChecks,
            };
            let old = super::super::outcome::decide(&super::super::outcome::Facts {
                tasks_remaining: open,
                ci,
                review: super::super::outcome::Review::Missing,
                stop,
            });
            if old.status == super::super::outcome::RunStatus::NeedsAttention {
                before_waits += 1;
            }
        }
        assert!(before_waits > waits, "{before_waits} then, {waits} now");
        // By group: runs, what they cost, how they ended then and now.
        type Row<'a> = (u32, u32, Vec<&'a str>, Vec<String>);
        let mut groups: std::collections::BTreeMap<&str, Row> = Default::default();
        for c in replays::CASES {
            let g = groups.entry(c.group).or_default();
            g.0 += 1;
            g.1 += c.cost_cents;
            g.2.push(c.old_status);
            g.3.push(match replay(c.stop, c.done, c.total) {
                Ending::HandBack(h) => format!("waits ({})", h.why()),
                e => e.status().to_string(),
            });
            assert!(c.date.len() == 10, "{}", c.name);
        }
        for (g, (n, cents, old, now)) in &groups {
            let count = |v: &[String], s: &str| v.iter().filter(|x| x.starts_with(s)).count();
            println!(
                "{g}: {n} runs, ${:.2}; then {} needs_input, {} failed, {} retried, {} completed; \
                 now {} completed, {} failed, {} waiting",
                *cents as f64 / 100.0,
                old.iter().filter(|s| **s == "needs_input").count(),
                old.iter().filter(|s| **s == "failed").count(),
                old.iter().filter(|s| **s == "retried").count(),
                old.iter().filter(|s| **s == "completed").count(),
                count(now, "completed"),
                count(now, "failed"),
                count(now, "waits"),
            );
        }
        println!(
            "replays: {} runs; before: {before_waits} waited on a person; now: finished \
             {finished}, failed with the cause {failed}, waiting on a human-only need {waits}",
            replays::CASES.len()
        );
    }

    // ── The source scan ────────────────────────────────────────────────

    fn sources() -> Vec<(String, String)> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut stack = vec![root.clone()];
        let mut out = Vec::new();
        while let Some(dir) = stack.pop() {
            for e in std::fs::read_dir(&dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    let rel = p
                        .strip_prefix(&root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/");
                    out.push((rel, std::fs::read_to_string(&p).unwrap()));
                }
            }
        }
        assert!(out.len() > 50, "found the sources");
        out
    }

    /// The body of `fn name` in `code` (to the next item at column zero).
    fn body<'a>(code: &'a str, head: &str) -> &'a str {
        let Some(i) = code.find(head) else {
            return "";
        };
        let rest = &code[i..];
        let end = rest[1..].find("\n}\n").map(|j| j + 3).unwrap_or(rest.len());
        &rest[..end]
    }

    /// No ending goes around the supervisor. Fails when:
    /// - a file not on the list writes `needs_input` (each one on it takes
    ///   a `HandBack`, which the type requires);
    /// - a `HandBack` is made from anything but a reason on the list
    ///   (a literal `HumanOnly::...`, the stop's own `asked_for`, or
    ///   `human_only_in`);
    /// - a stop whose steps changed nothing returns as an error again, or
    ///   the CI give-up, the stop after the work, the guard and the ready
    ///   hold stop reading the supervisor first.
    #[test]
    fn no_ending_goes_around_the_supervisor() {
        const WRITES_NEEDS_INPUT: &[&str] = &[
            "agent/policy.rs",
            "passes/mod.rs",
            "passes/ready.rs",
            "passes/resume.rs",
            "passes/run_guard.rs",
            "passes/stop_pr.rs",
        ];
        let mut bad = Vec::new();
        for (rel, code) in sources() {
            // The incident cases and the scans' own test strings.
            if rel.starts_with("passes/incidents/")
                || rel.starts_with("passes/replay/")
                || rel == "passes/stop_judge.rs"
                || rel == "passes/supervisor.rs"
            {
                continue;
            }
            if code.contains(".needs_input(") && !WRITES_NEEDS_INPUT.contains(&rel.as_str()) {
                bad.push(format!(
                    "{rel}: writes needs_input; only the supervisor's ending sites may"
                ));
            }
            for make in ["HandBack::new(", "HandBack::of("] {
                for (i, _) in code.match_indices(make) {
                    let arg = code[i + make.len()..]
                        .trim_start()
                        .split([',', ')'])
                        .next()
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let listed = arg.contains("HumanOnly::")
                        || arg == "asked_for"
                        || arg == "why"
                        || arg == "h";
                    if !listed {
                        bad.push(format!("{rel}: `{make}{arg}` isn't a reason on the list"));
                    }
                }
            }
            if rel == "passes/mod.rs" {
                if code.contains("steps::stall_reason(") {
                    bad.push(format!(
                        "{rel}: a stall stops the run again (steps::stall_reason)"
                    ));
                }
                for (i, _) in code.match_indices("mark_run_needs_input(") {
                    if code[..i].ends_with("async fn ") {
                        continue;
                    }
                    let call = &code[i..code[i..].find(".await").map_or(code.len(), |j| i + j)];
                    if !call.contains("HumanOnly::") {
                        bad.push(format!(
                            "{rel}: a needs_input stop without its human-only reason"
                        ));
                    }
                }
            }
        }
        let find = |rel: &str| {
            sources()
                .into_iter()
                .find(|(r, _)| r == rel)
                .map(|(_, c)| c)
                .unwrap_or_default()
        };
        let resume = find("passes/resume.rs");
        let hand_off = body(&resume, "\nasync fn hand_off_red_ci(");
        match (
            hand_off.find("revert_to_green("),
            hand_off.find("record_ci_outcome("),
        ) {
            (Some(r), Some(o)) if r < o => {}
            _ => bad.push("resume.rs: hand_off_red_ci gives up before reverting to green".into()),
        }
        for (rel, head, needle) in [
            (
                "passes/resume.rs",
                "\nasync fn wait_on_person(",
                "supervisor::end(",
            ),
            (
                "passes/stop_pr.rs",
                "\npub async fn open_with(",
                "supervisor::end(",
            ),
            (
                "passes/run_guard.rs",
                "\nasync fn wait_on_open_pr(",
                "hand_back_for(",
            ),
            (
                "passes/ready.rs",
                "\nasync fn wait_on_person(",
                "draft_hand_back(",
            ),
            (
                "passes/mod.rs",
                "\npub(crate) async fn mark_run_stopped_by_guard(",
                "hand_back_for(",
            ),
        ] {
            let code = find(rel);
            let b = body(&code, head);
            if b.is_empty() || !b.contains(needle) {
                bad.push(format!(
                    "{rel}: `{}` doesn't read the supervisor ({needle})",
                    head.trim()
                ));
            }
        }
        assert!(
            bad.is_empty(),
            "endings around the supervisor:\n{}",
            bad.join("\n")
        );
    }

    /// The scan catches what it guards: a made-up file that writes the
    /// status, and a hand-back from a stall's words.
    #[test]
    fn the_scan_catches_an_ending_around_the_supervisor() {
        let code = "fn stop() { u.needs_input(r, &w, &HandBack::new(kind_of(stall), \"x\")); }";
        let arg = code[code.find("HandBack::new(").unwrap() + 14..]
            .split([',', ')'])
            .next()
            .unwrap()
            .trim()
            .to_string();
        assert_eq!(arg, "kind_of(stall");
        assert!(!arg.contains("HumanOnly::"));
        let src =
            "x\nasync fn hand_off_red_ci(\n  record_ci_outcome(a);\n  revert_to_green(b);\n}\n";
        let b = body(src, "\nasync fn hand_off_red_ci(");
        assert!(b.find("record_ci_outcome(").unwrap() < b.find("revert_to_green(").unwrap());
    }
}
