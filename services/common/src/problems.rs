//! One shape for "something is wrong" across every status producer.
//!
//! A problem is set by a check and cleared by the same check passing again:
//! never by a timer, never by hand, never by another check. Clearing keeps a
//! short history entry with when it started and when it recovered, so a page
//! can say "fixed itself at 14:02" instead of showing an old failure as if it
//! were current.
//!
//! The rules every producer follows:
//!
//! - Every [`Kind`] belongs to exactly one [`Check`] ([`Kind::check`]).
//! - A producer reports each run of its check with [`record`]: `Pass` clears
//!   whatever that check had set, `Fail(kind, message)` sets (or keeps) it.
//! - A page reads [`ProblemState::current`] and nothing else to decide
//!   whether something is wrong now. A stored `reason` or `error` field is
//!   never shown on its own next to a pass.
//!
//! `tests::every_kind_has_a_clearing_path` fails when a kind is added whose
//! check never reports a pass in the producers' source.

use serde::{Deserialize, Serialize};

/// How many recovered problems a state keeps.
pub const HISTORY_LIMIT: usize = 10;

/// A check that can find a problem. Each one runs on its own schedule in
/// one producer, and its pass is the only thing that clears its problems.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Check {
    /// Dependabot alerts read for one repository (Security, Sources).
    Dependabot,
    /// One repository's dependency list matched against OSV (Security, Sources).
    Osv,
    /// GitHub's advisory database, read about hourly (Security, Sources).
    AdvisoryPoll,
    /// FIRST's exploit scores, read daily (Security, Sources).
    Epss,
    /// Coderhelm taking on a connected AWS account's role (Integrations, AWS).
    AwsRole,
}

impl Check {
    pub const ALL: &'static [Check] = &[
        Check::Dependabot,
        Check::Osv,
        Check::AdvisoryPoll,
        Check::Epss,
        Check::AwsRole,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Check::Dependabot => "dependabot",
            Check::Osv => "osv",
            Check::AdvisoryPoll => "advisory_poll",
            Check::Epss => "epss",
            Check::AwsRole => "aws_role",
        }
    }

    /// The item attribute a check's state is kept in.
    pub fn attr(self) -> String {
        format!("problem_{}", self.as_str())
    }
}

/// What kind of problem a check found. Kept small: the message carries the
/// detail, the kind decides who clears it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Dependabot alerts are switched off on the repository.
    DependabotOff,
    /// The GitHub App can't read Dependabot alerts.
    DependabotNoPermission,
    /// Reading Dependabot alerts failed.
    DependabotReadFailed,
    /// OSV couldn't be read for the repository.
    OsvReadFailed,
    /// Reading GitHub's advisory database failed.
    AdvisoryPollFailed,
    /// FIRST's scores couldn't be read.
    EpssReadFailed,
    /// Coderhelm couldn't take on the account's role.
    AwsRoleRefused,
}

impl Kind {
    pub const ALL: &'static [Kind] = &[
        Kind::DependabotOff,
        Kind::DependabotNoPermission,
        Kind::DependabotReadFailed,
        Kind::OsvReadFailed,
        Kind::AdvisoryPollFailed,
        Kind::EpssReadFailed,
        Kind::AwsRoleRefused,
    ];

    /// The one check whose pass clears this kind.
    pub fn check(self) -> Check {
        match self {
            Kind::DependabotOff | Kind::DependabotNoPermission | Kind::DependabotReadFailed => {
                Check::Dependabot
            }
            Kind::OsvReadFailed => Check::Osv,
            Kind::AdvisoryPollFailed => Check::AdvisoryPoll,
            Kind::EpssReadFailed => Check::Epss,
            Kind::AwsRoleRefused => Check::AwsRole,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::DependabotOff => "dependabot_off",
            Kind::DependabotNoPermission => "dependabot_no_permission",
            Kind::DependabotReadFailed => "dependabot_read_failed",
            Kind::OsvReadFailed => "osv_read_failed",
            Kind::AdvisoryPollFailed => "advisory_poll_failed",
            Kind::EpssReadFailed => "epss_read_failed",
            Kind::AwsRoleRefused => "aws_role_refused",
        }
    }
}

/// A problem that is true now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Problem {
    pub kind: Kind,
    /// What happened and what to do, in plain words.
    pub message: String,
    /// When the check first found it (kept while it keeps failing).
    pub since: String,
    /// The latest run of the check that found it.
    pub seen_at: String,
}

/// A problem that is over: the check passed again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recovered {
    pub kind: Kind,
    pub message: String,
    pub since: String,
    pub recovered_at: String,
}

/// One check's problem state, stored as JSON beside the producer's row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProblemState {
    /// The problem true now, or None.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<Problem>,
    /// The latest pass of the check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passed_at: Option<String>,
    /// Recovered problems, newest first, at most [`HISTORY_LIMIT`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<Recovered>,
}

/// One run of a check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Pass,
    Fail(Kind, String),
}

/// What [`record`] changed, for a producer that notifies on changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    /// Nothing new: still passing, or the same problem again.
    None,
    /// A problem started (or changed kind).
    Started,
    /// The problem cleared: the check passed again.
    Recovered,
}

/// Records one run of `check`. A pass clears the problem that check set and
/// keeps a recovered entry; a failure sets it, keeping `since` while the same
/// kind keeps failing. A failure of a kind another check owns is a
/// programming error and is refused (debug panic, no change in release).
pub fn record(state: &mut ProblemState, check: Check, outcome: Outcome, now: &str) -> Change {
    match outcome {
        Outcome::Pass => {
            state.passed_at = Some(now.to_string());
            match state.current.take() {
                Some(p) if p.kind.check() == check => {
                    state.history.insert(
                        0,
                        Recovered {
                            kind: p.kind,
                            message: p.message,
                            since: p.since,
                            recovered_at: now.to_string(),
                        },
                    );
                    state.history.truncate(HISTORY_LIMIT);
                    Change::Recovered
                }
                other => {
                    state.current = other;
                    Change::None
                }
            }
        }
        Outcome::Fail(kind, message) => {
            if kind.check() != check {
                debug_assert!(
                    false,
                    "{} is cleared by {}, not {}",
                    kind.as_str(),
                    kind.check().as_str(),
                    check.as_str()
                );
                return Change::None;
            }
            match &mut state.current {
                Some(p) if p.kind == kind => {
                    p.message = message;
                    p.seen_at = now.to_string();
                    Change::None
                }
                cur => {
                    *cur = Some(Problem {
                        kind,
                        message,
                        since: now.to_string(),
                        seen_at: now.to_string(),
                    });
                    Change::Started
                }
            }
        }
    }
}

impl ProblemState {
    /// Reads a stored state; a missing or unreadable one is "no problem".
    pub fn parse(text: &str) -> ProblemState {
        serde_json::from_str(text).unwrap_or_default()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".into())
    }

    /// The message of the problem true now, or "".
    pub fn message(&self) -> &str {
        self.current
            .as_ref()
            .map(|p| p.message.as_str())
            .unwrap_or("")
    }

    /// The state as a page reads it: `current` (or null), `passed_at`, and
    /// the recovered history.
    pub fn view(&self) -> serde_json::Value {
        serde_json::json!({
            "current": self.current.as_ref().map(|p| serde_json::json!({
                "kind": p.kind.as_str(),
                "message": p.message,
                "since": p.since,
                "seen_at": p.seen_at,
            })),
            "passed_at": self.passed_at,
            "history": self.history.iter().map(|r| serde_json::json!({
                "kind": r.kind.as_str(),
                "message": r.message,
                "since": r.since,
                "recovered_at": r.recovered_at,
            })).collect::<Vec<_>>(),
        })
    }
}

/// The state of `check` kept on a stored item (no attribute: no problem).
pub fn read_from(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    check: Check,
) -> ProblemState {
    item.get(&check.attr())
        .and_then(|v| v.as_s().ok())
        .map(|t| ProblemState::parse(t))
        .unwrap_or_default()
}

/// Records one run of `check` on the item `pk`/`sk` (read, [`record`],
/// written back to `problem_<check>`). Best effort: a failed read or write
/// is logged and the run is recorded again next time, since every run of
/// the check reports here.
pub async fn record_on(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    pk: &str,
    sk: &str,
    check: Check,
    outcome: Outcome,
    now: &str,
) -> ProblemState {
    use aws_sdk_dynamodb::types::AttributeValue;
    let attr = check.attr();
    let mut state = match dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk.into()))
        .key("sk", AttributeValue::S(sk.into()))
        .projection_expression("#a")
        .expression_attribute_names("#a", &attr)
        .consistent_read(true)
        .send()
        .await
    {
        Ok(o) => o.item().map(|i| read_from(i, check)).unwrap_or_default(),
        Err(e) => {
            tracing::warn!(pk, sk, check = check.as_str(), error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "problems: could not read the state");
            ProblemState::default()
        }
    };
    record(&mut state, check, outcome, now);
    if let Err(e) = dynamo
        .update_item()
        .table_name(table)
        .key("pk", AttributeValue::S(pk.into()))
        .key("sk", AttributeValue::S(sk.into()))
        .update_expression("SET #a = :v")
        .expression_attribute_names("#a", &attr)
        .expression_attribute_values(":v", AttributeValue::S(state.to_json()))
        .send()
        .await
    {
        tracing::warn!(pk, sk, check = check.as_str(), error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "problems: could not save the state");
    }
    state
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: &str = "2026-11-11T10:00:00+00:00";
    const T1: &str = "2026-11-11T11:00:00+00:00";
    const T2: &str = "2026-11-11T12:00:00+00:00";

    #[test]
    fn a_pass_clears_the_problem_and_keeps_when_it_recovered() {
        let mut s = ProblemState::default();
        let fail = Outcome::Fail(Kind::OsvReadFailed, "OSV didn't answer".into());
        assert_eq!(
            record(&mut s, Check::Osv, fail.clone(), T0),
            Change::Started
        );
        assert_eq!(record(&mut s, Check::Osv, fail, T1), Change::None);
        assert_eq!(
            s.current.as_ref().unwrap().since,
            T0,
            "since stays while it keeps failing"
        );
        assert_eq!(s.current.as_ref().unwrap().seen_at, T1);
        assert_eq!(
            record(&mut s, Check::Osv, Outcome::Pass, T2),
            Change::Recovered
        );
        assert!(s.current.is_none());
        assert_eq!(s.passed_at.as_deref(), Some(T2));
        assert_eq!(s.history[0].since, T0);
        assert_eq!(s.history[0].recovered_at, T2);
        assert_eq!(s.message(), "");
    }

    #[test]
    fn another_checks_pass_never_clears_a_problem() {
        let mut s = ProblemState::default();
        record(
            &mut s,
            Check::Osv,
            Outcome::Fail(Kind::OsvReadFailed, "x".into()),
            T0,
        );
        assert_eq!(
            record(&mut s, Check::Dependabot, Outcome::Pass, T1),
            Change::None
        );
        assert!(s.current.is_some());
    }

    #[test]
    fn a_new_kind_from_the_same_check_replaces_the_old_one() {
        let mut s = ProblemState::default();
        record(
            &mut s,
            Check::Dependabot,
            Outcome::Fail(Kind::DependabotOff, "off".into()),
            T0,
        );
        let c = record(
            &mut s,
            Check::Dependabot,
            Outcome::Fail(Kind::DependabotReadFailed, "failed".into()),
            T1,
        );
        assert_eq!(c, Change::Started);
        assert_eq!(s.current.as_ref().unwrap().since, T1);
    }

    #[test]
    fn history_is_capped() {
        let mut s = ProblemState::default();
        for i in 0..(HISTORY_LIMIT + 5) {
            let t = format!("2026-11-11T{:02}:00:00+00:00", i);
            record(
                &mut s,
                Check::Osv,
                Outcome::Fail(Kind::OsvReadFailed, "x".into()),
                &t,
            );
            record(&mut s, Check::Osv, Outcome::Pass, &t);
        }
        assert_eq!(s.history.len(), HISTORY_LIMIT);
    }

    #[test]
    fn a_stored_state_reads_back_and_garbage_is_no_problem() {
        let mut s = ProblemState::default();
        record(
            &mut s,
            Check::Osv,
            Outcome::Fail(Kind::OsvReadFailed, "x".into()),
            T0,
        );
        assert_eq!(ProblemState::parse(&s.to_json()), s);
        assert_eq!(ProblemState::parse(""), ProblemState::default());
        assert_eq!(ProblemState::parse("not json"), ProblemState::default());
    }

    /// Every kind is cleared by its own check passing: set it, pass its
    /// check, and it is gone with a recovered entry.
    #[test]
    fn every_kind_clears_when_its_check_passes() {
        for &kind in Kind::ALL {
            let mut s = ProblemState::default();
            record(&mut s, kind.check(), Outcome::Fail(kind, "x".into()), T0);
            assert!(s.current.is_some(), "{} was not set", kind.as_str());
            assert_eq!(
                record(&mut s, kind.check(), Outcome::Pass, T1),
                Change::Recovered,
                "{}",
                kind.as_str()
            );
            assert!(s.current.is_none(), "{} was not cleared", kind.as_str());
        }
    }

    /// The producers' source: each check's pass must be reported somewhere,
    /// or its problems could never clear. Adding a check (or kind) without a
    /// `record(.., Check::X, Outcome::Pass, ..)` in a producer fails here.
    #[test]
    fn every_kind_has_a_clearing_path() {
        // Every file that reports a check. A new producer is added here.
        let sources: &[(&str, &str)] = &[
            (
                "worker/src/passes/security_fixes.rs",
                include_str!("../../worker/src/passes/security_fixes.rs"),
            ),
            (
                "gateway/src/routes/integrations.rs",
                include_str!("../../gateway/src/routes/integrations.rs"),
            ),
            (
                "gateway/src/routes/log_analyzer.rs",
                include_str!("../../gateway/src/routes/log_analyzer.rs"),
            ),
        ];
        // Whitespace aside, so formatting never hides a call.
        let all: String = sources
            .iter()
            .flat_map(|(_, s)| s.chars())
            .filter(|c| !c.is_whitespace())
            .collect();
        // Every kind's check is one of the listed checks.
        for &kind in Kind::ALL {
            assert!(Check::ALL.contains(&kind.check()), "{}", kind.as_str());
        }
        for &check in Check::ALL {
            let name = format!("{check:?}");
            let found = all.contains(&format!("Check::{name},Outcome::Pass"));
            assert!(
                found,
                "no producer reports a pass of Check::{name}: its problems would never clear. \
                 Call problems::record_on(.., Check::{name}, Outcome::Pass, ..) where the check succeeds."
            );
        }
    }
}
