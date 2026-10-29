//! Fix rounds on a run's open pull request, each with its own time budget.
//!
//! After a run opens its pull request, more work can come to it: CI fails
//! and CoderHelm fixes it (a CI fix round), CoderHelm's own reviewer asks for
//! changes (a review fix round), or a person comments (a feedback round).
//! Each round used to run inside whatever was left of one worker invocation
//! (a 15 minute limit), with no deadline of its own: a review fix that
//! started after the review had used most of the invocation was killed half
//! way, the stuck-run sweep then marked the whole run `failed` although its
//! pull request was open and green, and nothing said what the round had
//! pushed.
//!
//! The rules here:
//!
//! - A round gets its own budget ([`BUDGET_SECS`]), measured from the
//!   invocation it runs in, never from when the run started. A round that
//!   would start with less than [`MIN_START_SECS`] left is parked
//!   ([`State::Pending`]) and runs first thing in a fresh invocation.
//! - A round that uses its budget with work left continues in a fresh
//!   invocation, up to [`MAX_ATTEMPTS`] in all: a resumable unit.
//! - The round being worked is on the run record ([`FIELD`]) with its
//!   deadline, so the stuck-run sweep waits for that deadline (plus
//!   [`SWEEP_GRACE_SECS`]) before it calls the worker gone.
//! - A round works on its own branch, [`wip_branch`] (`coderhelm-wip/<pr
//!   branch>`), never the pull request's: every write is a checkpoint
//!   commit there. When the round finishes, its changes land on the pull
//!   request as ONE commit in the repository's commit style, so a round is
//!   one push (one CI run; a repository that dismisses stale approvals
//!   dismisses them once). A round that stops keeps its WIP branch, and
//!   Resume continues from it; partial work never reaches the pull request.
//! - A round that doesn't finish while the pull request is open never fails
//!   the run. The run is `completed` with [`INCOMPLETE_FIELD`] saying which
//!   round, why, and the commits it pushed; [`status_line`] is how a person
//!   reads it, and Resume ([`may_resume`]) runs the round again.
//!
//! The sweep is Python (`lambda/stuck-run-cleanup/handler.py`) and mirrors
//! the field names and numbers; a test below reads it.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

/// The run record field that holds the round being worked (or parked).
pub const FIELD: &str = "round";
/// The run record field that says the last round didn't finish.
pub const INCOMPLETE_FIELD: &str = "round_incomplete";

/// One worker invocation's hard limit, in seconds.
pub const WORKER_LIMIT_SECS: u64 = 900;
/// A round's own time budget, in seconds: one invocation less the time to
/// record what it did, push the status and hand over.
pub const BUDGET_SECS: u64 = WORKER_LIMIT_SECS - 150;
/// A round doesn't start in an invocation with less than this left; it is
/// parked and runs first in a fresh one.
pub const MIN_START_SECS: u64 = 420;
/// How many invocations one round may use before it is left unfinished.
pub const MAX_ATTEMPTS: u32 = 3;
/// How long past a round's deadline the stuck-run sweep waits before it
/// decides the worker is gone.
pub const SWEEP_GRACE_SECS: u64 = 300;
/// The most of a round's instructions kept on the record for a parked round
/// (CI logs can be long; the record has a 400 KB limit).
pub const MAX_FEEDBACK_CHARS: usize = 60_000;
/// The most commits an unfinished round lists.
pub const MAX_PUSHED: usize = 20;

/// What a round is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// CI failed on the pull request; CoderHelm fixes it.
    CiFix,
    /// CoderHelm's own reviewer asked for changes.
    ReviewFix,
    /// A person's review or comments.
    Feedback,
    /// The security check found issues before the pull request opened, with
    /// too little of that invocation left to fix them: the fix runs on the
    /// pull request in a fresh invocation, and the pull request stays a
    /// draft until the issues are gone.
    SecurityFix,
    /// The pull request conflicts with its base and auto-merge stopped on
    /// it: CoderHelm merges the base into its own pull request's branch,
    /// once (`await_merge`).
    Conflict,
    /// Planned work the check before the pull request still found not done
    /// after its fix round: the pull request opened as a draft with it
    /// listed under "Not finished", and CoderHelm keeps working it in
    /// rounds of its own while each round makes progress.
    PlannedWork,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::CiFix => "ci_fix",
            Kind::ReviewFix => "review_fix",
            Kind::Feedback => "feedback",
            Kind::SecurityFix => "security_fix",
            Kind::Conflict => "conflict_fix",
            Kind::PlannedWork => "planned_work",
        }
    }

    pub fn parse(s: &str) -> Option<Kind> {
        match s.trim() {
            "ci_fix" => Some(Kind::CiFix),
            "review_fix" => Some(Kind::ReviewFix),
            "feedback" => Some(Kind::Feedback),
            "security_fix" => Some(Kind::SecurityFix),
            "conflict_fix" => Some(Kind::Conflict),
            "planned_work" => Some(Kind::PlannedWork),
            _ => None,
        }
    }

    /// The kind a pass name points to, for a round the record doesn't name
    /// (the sweep, an older run).
    pub fn of_pass(pass: &str) -> Kind {
        match pass.split(':').next().unwrap_or("").trim() {
            "ci_fix" | "test" => Kind::CiFix,
            "feedback" => Kind::Feedback,
            "security_fix" => Kind::SecurityFix,
            "conflict_fix" => Kind::Conflict,
            "planned_work" => Kind::PlannedWork,
            _ => Kind::ReviewFix,
        }
    }

    /// The round's name in a sentence.
    pub fn label(self) -> &'static str {
        match self {
            Kind::CiFix => "CI fix round",
            Kind::ReviewFix => "review fix round",
            Kind::Feedback => "feedback round",
            Kind::SecurityFix => "security fix round",
            Kind::Conflict => "conflict round",
            Kind::PlannedWork => "planned-work round",
        }
    }
}

/// Whether the round is being worked or waits for a fresh invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum State {
    #[default]
    Running,
    Pending,
}

impl State {
    fn as_str(self) -> &'static str {
        match self {
            State::Running => "running",
            State::Pending => "pending",
        }
    }
}

/// The round being worked, as the run record holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Round {
    pub kind: Kind,
    /// Which fix of the run this is (1 for the first).
    pub number: u32,
    /// Which invocation of the round this is (1 for the first).
    pub attempt: u32,
    pub state: State,
    /// When this attempt started (RFC 3339).
    pub started_at: String,
    /// When this attempt's budget ends (RFC 3339).
    pub deadline_at: String,
    /// The branch head when the round first started: what it pushed is
    /// everything after it.
    pub base_sha: String,
    /// What the round was asked to do, kept so a parked round can run.
    pub feedback: String,
    /// Whether the round may only change the files the pull request already
    /// changes (a bot-driven fix).
    pub scoped: bool,
    /// The branch the round writes its checkpoints to (empty until it
    /// starts working).
    pub wip_branch: String,
    /// Planned-work rounds in a row, before this one, that made no progress
    /// (no task finished and nothing pushed). Two stop the work.
    pub stalled: u32,
}

/// The branch a round on `pr_branch` keeps its checkpoints on: a branch of
/// its own, never the pull request's head.
pub fn wip_branch(pr_branch: &str) -> String {
    format!("coderhelm-wip/{pr_branch}")
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: u32) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn get_s(m: &HashMap<String, AttributeValue>, k: &str) -> String {
    m.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn get_n(m: &HashMap<String, AttributeValue>, k: &str) -> u32 {
    m.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// `text` cut to at most `max` bytes at a character boundary.
fn cut(text: &str, max: usize) -> String {
    crate::truncate_str(text, max).to_string()
}

impl Round {
    /// A new round that starts now, with its budget from `now`.
    pub fn start(
        kind: Kind,
        number: u32,
        base_sha: &str,
        feedback: &str,
        scoped: bool,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Round {
        Round {
            kind,
            number,
            attempt: 1,
            state: State::Running,
            started_at: now.to_rfc3339(),
            deadline_at: (now + chrono::Duration::seconds(BUDGET_SECS as i64)).to_rfc3339(),
            base_sha: base_sha.to_string(),
            feedback: cut(feedback, MAX_FEEDBACK_CHARS),
            scoped,
            wip_branch: String::new(),
            stalled: 0,
        }
    }

    /// This round, picked up by a fresh invocation now.
    pub fn resumed(&self, now: chrono::DateTime<chrono::Utc>) -> Round {
        Round {
            state: State::Running,
            started_at: now.to_rfc3339(),
            deadline_at: (now + chrono::Duration::seconds(BUDGET_SECS as i64)).to_rfc3339(),
            ..self.clone()
        }
    }

    /// This round, parked for a fresh invocation. `next_attempt` when the
    /// attempt that just ended used its budget.
    pub fn parked(&self, next_attempt: bool) -> Round {
        Round {
            state: State::Pending,
            attempt: self.attempt + u32::from(next_attempt),
            ..self.clone()
        }
    }

    /// Whether another invocation may continue this round.
    pub fn may_continue(&self) -> bool {
        self.attempt < MAX_ATTEMPTS
    }

    pub fn to_attr(&self) -> AttributeValue {
        AttributeValue::M(HashMap::from([
            ("kind".to_string(), s(self.kind.as_str())),
            ("number".to_string(), n(self.number)),
            ("attempt".to_string(), n(self.attempt)),
            ("state".to_string(), s(self.state.as_str())),
            ("started_at".to_string(), s(&self.started_at)),
            ("deadline_at".to_string(), s(&self.deadline_at)),
            ("base_sha".to_string(), s(&self.base_sha)),
            ("feedback".to_string(), s(&self.feedback)),
            ("scoped".to_string(), AttributeValue::Bool(self.scoped)),
            ("wip_branch".to_string(), s(&self.wip_branch)),
            ("stalled".to_string(), n(self.stalled)),
        ]))
    }

    pub fn from_attr(v: &AttributeValue) -> Option<Round> {
        let m = v.as_m().ok()?;
        Some(Round {
            kind: Kind::parse(&get_s(m, "kind"))?,
            number: get_n(m, "number"),
            attempt: get_n(m, "attempt").max(1),
            state: if get_s(m, "state") == "pending" {
                State::Pending
            } else {
                State::Running
            },
            started_at: get_s(m, "started_at"),
            deadline_at: get_s(m, "deadline_at"),
            base_sha: get_s(m, "base_sha"),
            feedback: get_s(m, "feedback"),
            scoped: m
                .get("scoped")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
            wip_branch: get_s(m, "wip_branch"),
            stalled: get_n(m, "stalled"),
        })
    }

    /// The round on a run record, if any.
    pub fn from_item(item: &HashMap<String, AttributeValue>) -> Option<Round> {
        item.get(FIELD).and_then(Round::from_attr)
    }
}

/// A commit an unfinished round pushed.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct Pushed {
    pub sha: String,
    /// The commit's subject line.
    pub message: String,
}

/// Why the last round didn't finish, and what it left on the branch.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Incomplete {
    pub kind: Option<Kind>,
    pub number: u32,
    /// Why it stopped, in a person's words.
    pub reason: String,
    /// The commits it pushed before it stopped (newest last).
    pub pushed: Vec<Pushed>,
    /// The files those commits changed.
    pub files: Vec<String>,
    /// When it stopped (RFC 3339).
    pub at: String,
    /// What the round was asked to do, so Resume can run it again.
    pub feedback: String,
    /// Whether the round was a bot-driven fix scoped to the PR's files.
    pub scoped: bool,
    /// The branch holding the round's work so far (not the pull request's).
    pub wip_branch: String,
    /// The pull request's head when the round started.
    pub base_sha: String,
}

impl Incomplete {
    /// The round, stopped for `reason`.
    pub fn of(round: &Round, reason: &str, now: &str) -> Incomplete {
        Incomplete {
            kind: Some(round.kind),
            number: round.number,
            reason: reason.trim().to_string(),
            pushed: Vec::new(),
            files: Vec::new(),
            at: now.to_string(),
            feedback: round.feedback.clone(),
            scoped: round.scoped,
            wip_branch: round.wip_branch.clone(),
            base_sha: round.base_sha.clone(),
        }
    }

    /// The sentence a person reads.
    pub fn status_line(&self) -> String {
        status_line(&self.reason)
    }

    pub fn to_attr(&self) -> AttributeValue {
        let pushed = self
            .pushed
            .iter()
            .take(MAX_PUSHED)
            .map(|p| {
                AttributeValue::M(HashMap::from([
                    ("sha".to_string(), s(&p.sha)),
                    ("message".to_string(), s(&cut(&p.message, 200))),
                ]))
            })
            .collect();
        let files = self.files.iter().take(100).map(|f| s(f)).collect();
        AttributeValue::M(HashMap::from([
            (
                "kind".to_string(),
                s(self.kind.map(Kind::as_str).unwrap_or("")),
            ),
            ("number".to_string(), n(self.number)),
            ("reason".to_string(), s(&cut(&self.reason, 500))),
            ("pushed".to_string(), AttributeValue::L(pushed)),
            ("files".to_string(), AttributeValue::L(files)),
            ("at".to_string(), s(&self.at)),
            (
                "feedback".to_string(),
                s(&cut(&self.feedback, MAX_FEEDBACK_CHARS)),
            ),
            ("scoped".to_string(), AttributeValue::Bool(self.scoped)),
            ("wip_branch".to_string(), s(&self.wip_branch)),
            ("base_sha".to_string(), s(&self.base_sha)),
        ]))
    }

    pub fn from_attr(v: &AttributeValue) -> Option<Incomplete> {
        let m = v.as_m().ok()?;
        let list = |k: &str| {
            m.get(k)
                .and_then(|v| v.as_l().ok())
                .cloned()
                .unwrap_or_default()
        };
        Some(Incomplete {
            kind: Kind::parse(&get_s(m, "kind")),
            number: get_n(m, "number"),
            reason: get_s(m, "reason"),
            pushed: list("pushed")
                .iter()
                .filter_map(|p| p.as_m().ok())
                .map(|p| Pushed {
                    sha: get_s(p, "sha"),
                    message: get_s(p, "message"),
                })
                .collect(),
            files: list("files")
                .iter()
                .filter_map(|f| f.as_s().ok().cloned())
                .collect(),
            at: get_s(m, "at"),
            feedback: get_s(m, "feedback"),
            scoped: m
                .get("scoped")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(true),
            wip_branch: get_s(m, "wip_branch"),
            base_sha: get_s(m, "base_sha"),
        })
    }

    /// The round to run again on Resume: parked, continuing from its WIP
    /// branch and its starting point, with a fresh budget.
    pub fn resume_round(&self, now: chrono::DateTime<chrono::Utc>) -> Option<Round> {
        let kind = self.kind?;
        let mut r = Round::start(
            kind,
            self.number,
            &self.base_sha,
            &self.feedback,
            self.scoped,
            now,
        );
        r.wip_branch = self.wip_branch.clone();
        Some(r.parked(false))
    }

    /// The unfinished round on a run record, if any.
    pub fn from_item(item: &HashMap<String, AttributeValue>) -> Option<Incomplete> {
        item.get(INCOMPLETE_FIELD).and_then(Incomplete::from_attr)
    }
}

/// How a run whose pull request is open reads when its last round didn't
/// finish. Never "failed": the pull request is there and can be resumed.
pub fn status_line(reason: &str) -> String {
    let reason = reason.trim().trim_end_matches('.');
    if reason.is_empty() {
        "Pull request open; the last fix round didn't finish.".to_string()
    } else {
        format!("Pull request open; the last fix round didn't finish: {reason}.")
    }
}

/// Why a round that used every attempt's budget stopped.
pub fn out_of_time(kind: Kind, attempts: u32) -> String {
    match attempts {
        0 | 1 => format!("the {} ran out of its time budget", kind.label()),
        n => format!(
            "the {} ran out of its time budget {n} times in a row",
            kind.label()
        ),
    }
}

/// Why a round failed with an error.
pub fn errored(kind: Kind, error: &str) -> String {
    let error = error.lines().next().unwrap_or("").trim();
    format!("the {} hit an error ({})", kind.label(), cut(error, 300))
}

/// Whether Resume may run the last round again: the run finished with its
/// pull request open and says its last round didn't finish.
pub fn may_resume(status: &str, pr_number: u64, incomplete: bool) -> bool {
    incomplete && pr_number > 0 && matches!(status, "completed" | "success")
}

/// The commits a round pushed and the files they changed, from a GitHub
/// compare (`base...head`) response.
pub fn pushed_from_compare(compare: &serde_json::Value) -> (Vec<Pushed>, Vec<String>) {
    let pushed = compare["commits"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|c| Pushed {
                    sha: c["sha"].as_str().unwrap_or("").to_string(),
                    message: c["commit"]["message"]
                        .as_str()
                        .unwrap_or("")
                        .lines()
                        .next()
                        .unwrap_or("")
                        .to_string(),
                })
                .filter(|p| !p.sha.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let files = compare["files"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| f["filename"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    // The newest commits matter most when the list is cut.
    let skip = pushed.len().saturating_sub(MAX_PUSHED);
    (pushed.into_iter().skip(skip).collect(), files)
}

/// One line for a progress note: what an unfinished round left, and where.
/// The pull request itself is never changed by a round that didn't finish.
pub fn pushed_note(pushed: &[Pushed], files: &[String], wip: &str) -> String {
    if pushed.is_empty() {
        return "It saved no work before it stopped; the pull request is unchanged.".to_string();
    }
    let shas: Vec<String> = pushed
        .iter()
        .map(|p| p.sha.chars().take(7).collect())
        .collect();
    format!(
        "Its work so far ({} checkpoint commit(s): {}, {} file(s)) is saved on branch `{wip}`; \
         the pull request is unchanged, and Resume continues from there.",
        pushed.len(),
        shas.join(", "),
        files.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc
            .with_ymd_and_hms(2026, 10, 26, 14, 0, 0)
            .single()
            .unwrap()
    }

    #[test]
    fn a_round_has_its_own_budget_from_when_it_starts() {
        let r = Round::start(Kind::ReviewFix, 2, "abc", "fix it", true, now());
        assert_eq!(r.attempt, 1);
        assert_eq!(r.state, State::Running);
        let deadline = chrono::DateTime::parse_from_rfc3339(&r.deadline_at).unwrap();
        assert_eq!(
            (deadline.with_timezone(&chrono::Utc) - now()).num_seconds(),
            BUDGET_SECS as i64
        );
        // Picked up later, the budget starts again from then: the run's own
        // age never counts.
        let later = now() + chrono::Duration::hours(5);
        let again = r.parked(true).resumed(later);
        assert_eq!(again.attempt, 2);
        assert_eq!(again.state, State::Running);
        let d = chrono::DateTime::parse_from_rfc3339(&again.deadline_at).unwrap();
        assert_eq!(
            (d.with_timezone(&chrono::Utc) - later).num_seconds(),
            BUDGET_SECS as i64
        );
        assert_eq!(again.base_sha, "abc");
    }

    #[test]
    fn a_round_continues_a_bounded_number_of_times() {
        let mut r = Round::start(Kind::CiFix, 1, "", "", true, now());
        let mut continued = 0;
        while r.may_continue() {
            r = r.parked(true).resumed(now());
            continued += 1;
        }
        assert_eq!(continued, MAX_ATTEMPTS - 1);
        // Parking without using the budget (deferred before it started)
        // doesn't spend an attempt.
        let fresh = Round::start(Kind::ReviewFix, 1, "", "", true, now());
        assert_eq!(fresh.parked(false).attempt, 1);
    }

    #[test]
    fn rounds_round_trip_through_the_record() {
        let r = Round::start(Kind::Feedback, 3, "sha1", "comments", false, now()).parked(false);
        let back = Round::from_attr(&r.to_attr()).unwrap();
        assert_eq!(back, r);
        let item = HashMap::from([(FIELD.to_string(), r.to_attr())]);
        assert_eq!(Round::from_item(&item), Some(r.clone()));

        let mut inc = Incomplete::of(&r, "the review fix round ran out of its time budget", "t");
        inc.pushed = vec![Pushed {
            sha: "0123456789".into(),
            message: "Add the block".into(),
        }];
        inc.files = vec!["src/a.ts".into()];
        let back = Incomplete::from_attr(&inc.to_attr()).unwrap();
        assert_eq!(back, inc);
    }

    #[test]
    fn a_round_writes_to_its_own_branch_and_resume_continues_from_it() {
        assert_eq!(wip_branch("feature/abc-7"), "coderhelm-wip/feature/abc-7");
        assert_ne!(wip_branch("x"), "x");
        let mut r = Round::start(Kind::ReviewFix, 2, "base1", "Add the tabs", true, now());
        r.wip_branch = wip_branch("feature/abc-7");
        let inc = Incomplete::of(&r, "out of time", "t");
        assert_eq!(inc.wip_branch, "coderhelm-wip/feature/abc-7");
        assert_eq!(inc.base_sha, "base1");
        let back = Incomplete::from_attr(&inc.to_attr()).unwrap();
        let again = back.resume_round(now()).unwrap();
        assert_eq!(again.state, State::Pending);
        assert_eq!(again.wip_branch, "coderhelm-wip/feature/abc-7");
        assert_eq!(again.base_sha, "base1");
        assert_eq!(again.feedback, "Add the tabs");
        assert!(Incomplete::default().resume_round(now()).is_none());
    }

    #[test]
    fn the_long_feedback_of_a_parked_round_is_cut() {
        let big = "x".repeat(MAX_FEEDBACK_CHARS * 2);
        let r = Round::start(Kind::CiFix, 1, "", &big, true, now());
        assert!(r.feedback.len() <= MAX_FEEDBACK_CHARS);
    }

    #[test]
    fn the_status_says_the_pr_is_open_never_failed() {
        let line = status_line(&out_of_time(Kind::ReviewFix, 3));
        assert_eq!(
            line,
            "Pull request open; the last fix round didn't finish: the review fix round ran out of its time budget 3 times in a row."
        );
        assert!(!line.to_lowercase().contains("failed"));
        assert_eq!(
            status_line(""),
            "Pull request open; the last fix round didn't finish."
        );
        assert_eq!(
            errored(Kind::Feedback, "model call failed\nstack"),
            "the feedback round hit an error (model call failed)"
        );
    }

    #[test]
    fn resume_is_offered_only_for_an_open_pr_whose_round_stopped() {
        assert!(may_resume("completed", 12, true));
        assert!(!may_resume("completed", 12, false));
        assert!(!may_resume("completed", 0, true));
        assert!(!may_resume("merged", 12, true));
        assert!(!may_resume("running", 12, true));
    }

    #[test]
    fn what_a_round_pushed_comes_from_the_compare() {
        let compare = serde_json::json!({
            "commits": [
                {"sha": "aaaaaaa111", "commit": {"message": "Add the tabs\n\nbody"}},
                {"sha": "bbbbbbb222", "commit": {"message": "Add the tests"}}
            ],
            "files": [{"filename": "src/tabs.ts"}, {"filename": "src/tabs.test.ts"}]
        });
        let (pushed, files) = pushed_from_compare(&compare);
        assert_eq!(pushed.len(), 2);
        assert_eq!(pushed[0].message, "Add the tabs");
        assert_eq!(files, vec!["src/tabs.ts", "src/tabs.test.ts"]);
        let note = pushed_note(&pushed, &files, "coderhelm-wip/feature/x");
        assert!(note.contains("2 checkpoint commit(s): aaaaaaa, bbbbbbb, 2 file(s)"));
        assert!(note.contains("`coderhelm-wip/feature/x`"));
        assert!(note.contains("the pull request is unchanged"));
        assert!(pushed_note(&[], &[], "w").contains("the pull request is unchanged"));
    }

    #[test]
    fn a_pass_name_points_to_its_round() {
        assert_eq!(Kind::of_pass("ci_fix:2"), Kind::CiFix);
        assert_eq!(Kind::of_pass("feedback"), Kind::Feedback);
        assert_eq!(Kind::of_pass("implement"), Kind::ReviewFix);
        assert_eq!(Kind::of_pass("security_fix:1"), Kind::SecurityFix);
        assert_eq!(Kind::of_pass("planned_work:2"), Kind::PlannedWork);
        for k in [
            Kind::CiFix,
            Kind::ReviewFix,
            Kind::Feedback,
            Kind::SecurityFix,
            Kind::Conflict,
            Kind::PlannedWork,
        ] {
            assert_eq!(Kind::parse(k.as_str()), Some(k));
        }
        for k in [Kind::CiFix, Kind::ReviewFix, Kind::Feedback] {
            assert_eq!(Kind::parse(k.as_str()), Some(k));
        }
    }

    /// The sweep is Python: it must use the same fields and numbers.
    #[test]
    fn the_stuck_run_sweep_knows_rounds() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let sweep =
            std::fs::read_to_string(root.join("lambda/stuck-run-cleanup/handler.py")).unwrap();
        assert!(sweep.contains(&format!("ROUND_FIELD = \"{FIELD}\"")));
        assert!(sweep.contains(&format!("INCOMPLETE_FIELD = \"{INCOMPLETE_FIELD}\"")));
        assert!(sweep.contains(&format!("ROUND_SWEEP_GRACE_SECONDS = {SWEEP_GRACE_SECS}")));
        assert!(sweep.contains(&format!("ROUND_BUDGET_SECONDS = {BUDGET_SECS}")));
        assert!(sweep.contains("Pull request open; the last fix round didn't finish"));
        for k in [
            Kind::CiFix,
            Kind::ReviewFix,
            Kind::Feedback,
            Kind::SecurityFix,
            Kind::Conflict,
            Kind::PlannedWork,
        ] {
            assert!(
                sweep.contains(&format!("\"{}\": \"{}\"", k.as_str(), k.label())),
                "the sweep names the {}",
                k.label()
            );
        }
    }
}
