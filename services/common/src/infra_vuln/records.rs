//! Stored infrastructure findings and their life: `INFRAFINDING#<id>` in
//! the settings table, one per fix ([`super::matching`] decides them).
//!
//! - A finding a read no longer produces is **fixed**, with the time:
//!   only a new read of the account closes one, never a merged PR.
//! - A merged PR without the new version running stays open and says so.
//! - AWS-managed rows are kept as `aws_managed` and never count as open.
//! - A finding is `fixing` only while its run works and `pr_open` only
//!   while its pull request is open or merged: a pull request closed
//!   unmerged, or a run that ended without one, gives it back
//!   ([`crate::security_release::Claim`], the same rules, words and holds
//!   as dependency findings). A pull request the replacement guard made a
//!   draft is neither: it stays as it is, and its finding stays on its
//!   runbook until its code or its target changes.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::matching::{FixKind, InfraFinding};
use crate::security_fixes::Tier;

pub const FINDING_PREFIX: &str = "INFRAFINDING#";
/// Teams' infrastructure settings (Inspector, Health).
pub const CONFIG_SK: &str = "INFRA_SECURITY_CONFIG";
/// The ecosystem of an infrastructure fix run's unit (`SEC_TICKET#`).
pub const UNIT_ECOSYSTEM: &str = "infrastructure";
/// Fixed findings are kept this long.
pub const FIXED_KEEP_DAYS: i64 = 180;

/// How the team reads AWS Health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HealthMode {
    #[default]
    Off,
    /// The Health API (Business Support or higher).
    Api,
    /// The EventBridge rule template, forwarding to a queue the role reads.
    Eventbridge,
}

/// The team's opt-in switches. Both cost or need something from the team,
/// so both start off.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct InfraConfig {
    /// Read Amazon Inspector's findings (billed by AWS to the team).
    pub inspector: bool,
    pub health: HealthMode,
}

/// What the switches say, for the page.
pub const INSPECTOR_COST: &str = "Amazon Inspector is billed by AWS, to your account: about $1.25 per instance a month, $0.09 per container image scan and $0.30 per Lambda function a month. CoderHelm only reads its findings; it never turns Inspector on.";
pub const HEALTH_PLAN: &str = "The AWS Health API needs AWS Business Support or higher. Without it, deploy the EventBridge rule template in the account: it forwards the account's Health events to a queue the read-only role may read.";

pub fn finding_sk(id: &str) -> String {
    format!("{FINDING_PREFIX}{id}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    #[default]
    Open,
    Fixing,
    PrOpen,
    Fixed,
    AwsManaged,
    Dismissed,
}

impl Status {
    pub fn is_open(self) -> bool {
        matches!(self, Status::Open | Status::Fixing | Status::PrOpen)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrRef {
    pub number: u64,
    pub url: String,
    /// `open`, `draft` (the replacement guard held it), `merged` or
    /// `closed`.
    pub state: String,
    /// Closed without merging: when, who (a GitHub login, or the person
    /// who closed its run in CoderHelm), and whether that was a person.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_by: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub closed_by_person: bool,
}

/// One stored finding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    #[serde(flatten)]
    pub finding: InfraFinding,
    pub status: Status,
    #[serde(default)]
    pub pr: Option<PrRef>,
    #[serde(default)]
    pub ticket: String,
    pub first_seen_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub fixed_at: Option<String>,
    /// Why the replacement guard refused the change a fix run made
    /// ([`super::guard`]): each offending line.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refused: Vec<String>,
    /// How many times in a row the guard tripped on this finding's
    /// change: 1 while the one fix round runs, 2 once it gave up.
    #[serde(default)]
    pub guard_attempts: u8,
    /// The guard tripped twice: the pull request is a draft and the
    /// finding is applied by its runbook.
    #[serde(default)]
    pub drafted: bool,
    /// What the refused change was for ([`guard_sig`]): no new fix run
    /// starts by itself while the finding still asks for the same.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub guard_sig: String,
    /// Its pull request closed unmerged, or its fix run ended without one:
    /// no fix run starts by itself until then. A person asking never
    /// waits on it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_until: Option<String>,
    /// When its run was started for it: a run of the ticket that began
    /// after this is the one it waits on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<String>,
    /// What the page says after a person ended its fix run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_note: Option<String>,
    /// A line for the timeline when something changed (not stored).
    #[serde(skip)]
    pub event: Option<String>,
}

impl Record {
    pub fn new(f: InfraFinding, now: &str) -> Record {
        let status = if f.is_open() {
            Status::Open
        } else {
            Status::AwsManaged
        };
        Record {
            event: Some(if status == Status::AwsManaged {
                "AWS-managed: nothing to do".into()
            } else {
                format!("Found: {}", f.title)
            }),
            finding: f,
            status,
            pr: None,
            ticket: String::new(),
            first_seen_at: now.into(),
            updated_at: now.into(),
            fixed_at: None,
            refused: Vec::new(),
            guard_attempts: 0,
            drafted: false,
            guard_sig: String::new(),
            hold_until: None,
            claimed_at: None,
            release_note: None,
        }
    }

    pub fn tier(&self) -> Option<Tier> {
        self.finding.tier
    }

    /// What the page shows.
    pub fn view(&self) -> Value {
        let mut v = serde_json::to_value(&self.finding).unwrap_or(Value::Null);
        if let Some(o) = v.as_object_mut() {
            o.insert("status".into(), json!(self.status));
            o.insert("pr".into(), json!(self.pr));
            o.insert("inspector".into(), json!(self.finding.inspector));
            o.insert("first_seen_at".into(), json!(self.first_seen_at));
            o.insert("updated_at".into(), json!(self.updated_at));
            o.insert("fixed_at".into(), json!(self.fixed_at));
            // The fix's kind as the contract names it.
            o["fix"]["kind"] = json!(self.finding.fix.kind);
            if !self.refused.is_empty() {
                o.insert("refused".into(), json!(self.refused));
                o.insert("drafted".into(), json!(self.drafted));
                let closed = self.pr.as_ref().is_some_and(|p| p.state == "closed");
                if closed && !self.drafted {
                    // The round never finished: its pull request was closed.
                    o.insert("note".into(), json!(GUARD_CLOSED_NOTE));
                } else if self.drafted {
                    // The guard gave up on the code change: the person
                    // still has a correct way to apply the update.
                    o["fix"]["kind"] = json!(FixKind::Runbook);
                    o["fix"]["decision"] = json!(format!(
                        "A runbook: CoderHelm's own code change for this update would have replaced or destroyed a resource that holds data, twice, so its pull request is a draft and the update is applied in AWS. {}",
                        GUARD_STOPPED
                    ));
                    o.insert("note".into(), json!(GUARD_DRAFT_NOTE));
                } else {
                    o.insert("note".into(), json!(GUARD_ROUND_NOTE));
                }
            }
            if let (Some(note), Status::Open, true) =
                (&self.release_note, self.status, self.refused.is_empty())
            {
                o.insert("note".into(), json!(note));
            }
            if self.status == Status::PrOpen
                && self.pr.as_ref().is_some_and(|p| p.state == "merged")
            {
                o.insert("note".into(), json!("PR merged: waiting for the new version to run. It closes when CoderHelm reads the account again and sees it."));
            }
        }
        v
    }
}

/// What the page says while the one fix round after a tripped guard runs.
pub const GUARD_ROUND_NOTE: &str = "CoderHelm read its own change and it would have replaced or destroyed a resource that holds data. One fix round is correcting the lines below; the change is read again when it ends.";
/// What the page says once the guard tripped twice.
pub const GUARD_DRAFT_NOTE: &str = "The pull request is kept as a draft with the reason at the top of its description, and is never merged as it is. Nothing was applied.";
/// What the page says when the pull request was closed while the guard's
/// refusal stood.
pub const GUARD_CLOSED_NOTE: &str = "The pull request was closed. CoderHelm had read its own change and it would have replaced or destroyed a resource that holds data, so no new attempt starts by itself until the code that defines the resource or the update's target changes.";
/// When CoderHelm tries the code change again.
pub const GUARD_STOPPED: &str = "CoderHelm makes no further attempt until the code that defines the resource or the update's target changes.";

/// What a finding's code change is for: the target and the line it
/// edits. A guard refusal holds for as long as this stays the same.
pub fn guard_sig(f: &InfraFinding) -> String {
    let l = f.fix.location.as_ref();
    let e = l.and_then(|l| l.edit.as_ref());
    format!(
        "{}|{}|{}|{}|{}|{}",
        f.fix.to,
        l.map(|l| l.repo.as_str()).unwrap_or(""),
        l.map(|l| l.address.as_str()).unwrap_or(""),
        e.map(|e| e.file.as_str()).unwrap_or(""),
        e.map(|e| e.attribute.as_str()).unwrap_or(""),
        e.map(|e| e.value.as_str()).unwrap_or(""),
    )
}

/// What happens after the guard read a fix run's pull request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardStep {
    /// Nothing to object to: the pull request stands.
    Passed,
    /// First trip: one fix round, told each offending line.
    FixRound,
    /// Second trip: the pull request becomes a draft, the finding falls
    /// back to its runbook, and no further attempt starts by itself.
    Draft,
}

impl Record {
    fn clear_guard(&mut self) {
        self.refused.clear();
        self.guard_attempts = 0;
        self.drafted = false;
        self.guard_sig.clear();
    }

    /// Records what the guard found on this finding's pull request and
    /// says what happens next. Pure: the caller does it.
    pub fn guard_step(&mut self, reasons: &[String]) -> GuardStep {
        if reasons.is_empty() {
            self.clear_guard();
            return GuardStep::Passed;
        }
        self.refused = reasons.to_vec();
        self.guard_sig = guard_sig(&self.finding);
        self.guard_attempts = self.guard_attempts.saturating_add(1);
        if self.guard_attempts == 1 {
            GuardStep::FixRound
        } else {
            self.drafted = true;
            GuardStep::Draft
        }
    }

    /// The guard refused this finding's change and the finding still asks
    /// for the same change: no fix run starts by itself.
    pub fn guard_blocks(&self) -> bool {
        !self.refused.is_empty() && self.guard_sig == guard_sig(&self.finding)
    }

    /// Before a new fix run for this finding: an old refusal about a
    /// change the finding no longer asks for (its code or its target
    /// changed), or one a person now overrides, is forgotten.
    pub fn start_over(&mut self) {
        self.clear_guard();
    }
}

// ── The claim: fixing and pr_open follow GitHub and the run ─────────────

use crate::security_release::{self as rel, Change, Claim, OpenPr, PrNow, RunNow, Standing};

impl Record {
    /// Whether the hold a closed pull request or an ended run left is on:
    /// no fix run starts by itself.
    pub fn on_hold(&self, now: DateTime<Utc>) -> bool {
        self.hold_until
            .as_deref()
            .and_then(crate::security_fixes::parse_time)
            .is_some_and(|t| t > now)
    }

    /// The run claims the finding from `at`: a run of its ticket that
    /// began after this is the one it waits on.
    pub fn claim(&mut self, ticket: &str, at: DateTime<Utc>) {
        self.status = Status::Fixing;
        self.ticket = ticket.to_string();
        self.claimed_at = Some(at.to_rfc3339());
        self.hold_until = None;
        self.release_note = None;
    }

    fn released(&mut self, hold_days: i64, now: DateTime<Utc>) {
        self.status = Status::Open;
        self.ticket.clear();
        self.claimed_at = None;
        self.hold_until = Some((now + Duration::days(hold_days)).to_rfc3339());
        self.updated_at = now.to_rfc3339();
    }

    fn pr_is(&self, state: &str) -> bool {
        self.pr.as_ref().is_some_and(|p| p.state == state)
    }
}

impl Claim for Record {
    fn id(&self) -> &str {
        &self.finding.id
    }
    fn repo(&self) -> &str {
        self.finding
            .fix
            .location
            .as_ref()
            .map(|l| l.repo.as_str())
            .unwrap_or("")
    }
    fn pr(&self) -> Option<(u64, &str)> {
        self.pr.as_ref().map(|p| (p.number, p.state.as_str()))
    }
    fn ticket(&self) -> Option<&str> {
        (!self.ticket.is_empty()).then_some(self.ticket.as_str())
    }
    fn standing(&self) -> Standing {
        match self.status {
            Status::Open => Standing::Open,
            Status::Fixing => Standing::Fixing,
            Status::PrOpen => Standing::InPr,
            Status::Fixed => Standing::Fixed,
            Status::AwsManaged | Status::Dismissed => Standing::Other,
        }
    }
    /// An open pull request and a draft the guard held are read every
    /// sweep (either can be closed or merged on GitHub); a closed one while
    /// its hold is on (a person may reopen it); a merged one never again.
    fn watches_pr(&self, now: DateTime<Utc>) -> bool {
        match self.pr.as_ref().map(|p| p.state.as_str()) {
            Some("open" | "draft") => true,
            Some("closed") => {
                self.status == Status::Open && self.on_hold(now) && self.refused.is_empty()
            }
            _ => false,
        }
    }
    fn claimed_at(&self) -> Option<DateTime<Utc>> {
        let parse = crate::security_fixes::parse_time;
        self.claimed_at
            .as_deref()
            .and_then(parse)
            .or_else(|| parse(&self.updated_at))
    }
    /// The one fix round after a tripped guard works on the open pull
    /// request: if that run ends without settling it, the run decides.
    fn run_decides_with_pr(&self) -> bool {
        self.status == Status::Fixing && self.pr_is("open")
    }
    /// A pull request is recorded only after the replacement guard read it.
    fn opens_through_store(&self) -> bool {
        true
    }

    /// - Merged: recorded as merged. The finding stays open until a read of
    ///   the account sees the new version; a merge alone never closes one.
    /// - Closed unmerged: the finding is open again with who closed it and
    ///   when; no fix run starts by itself for
    ///   [`rel::HOLD_DAYS_CLOSED`] days, and a guard refusal that stood
    ///   keeps standing. A person asking never waits.
    /// - A draft the guard held is not closed: left as it is while GitHub
    ///   has it open. Closed on GitHub, the record says so and the finding
    ///   keeps its runbook.
    /// - Reopened: the finding is in the pull request again, unless the
    ///   guard had refused its change or the finding moved on.
    fn settle_pr(&mut self, pr_now: &PrNow, now: DateTime<Utc>) -> Option<Change> {
        let pr = self.pr.clone()?;
        let gone = PrNow::Closed(rel::Closer::default());
        let pr_now = match pr_now {
            PrNow::Gone => &gone,
            other => other,
        };
        match (pr.state.as_str(), pr_now) {
            ("open" | "draft", PrNow::Merged) => {
                self.pr = Some(PrRef {
                    state: "merged".into(),
                    ..pr.clone()
                });
                if self.status == Status::Fixing {
                    self.status = Status::PrOpen;
                }
                self.updated_at = now.to_rfc3339();
                rel::change(
                    format!(
                        "PR #{} merged: the finding closes when CoderHelm reads the account again and sees the new version running",
                        pr.number
                    ),
                    &pr.url,
                )
            }
            ("open" | "draft", PrNow::Closed(closer)) => {
                let (at, by, person) = rel::closed_record(closer, now);
                self.pr = Some(PrRef {
                    state: "closed".into(),
                    closed_at: Some(at),
                    closed_by: by,
                    closed_by_person: person,
                    ..pr.clone()
                });
                let words = rel::closed_words(pr.number, closer);
                let then = if pr.state == "draft" {
                    "; the finding keeps its runbook"
                } else if !matches!(self.status, Status::PrOpen | Status::Fixing) {
                    ""
                } else {
                    self.released(rel::HOLD_DAYS_CLOSED, now);
                    if self.guard_blocks() {
                        "; CoderHelm's own check had refused the change, so no new attempt starts by itself until the code or the target changes. Ask for a fix to try again"
                    } else {
                        "; automatic fixes wait 7 days. Ask for a fix to open a new pull request now"
                    }
                };
                self.updated_at = now.to_rfc3339();
                rel::change(format!("{words}{then}"), &pr.url)
            }
            ("closed", PrNow::Open) => {
                let moved_on = self.status != Status::Open
                    || !self.ticket.is_empty()
                    || !self.refused.is_empty()
                    || self.drafted;
                if moved_on {
                    return None;
                }
                self.pr = Some(PrRef {
                    state: "open".into(),
                    closed_at: None,
                    closed_by: None,
                    closed_by_person: false,
                    ..pr.clone()
                });
                self.status = Status::PrOpen;
                self.hold_until = None;
                self.release_note = None;
                self.updated_at = now.to_rfc3339();
                rel::change(
                    format!("PR #{} was reopened: the fix is in it again", pr.number),
                    &pr.url,
                )
            }
            _ => None,
        }
    }

    /// - The run is queued or working: left alone.
    /// - The run ended with no pull request: the finding is open again,
    ///   held as a dependency finding is ([`rel::run_end`]).
    /// - The fix round after a tripped guard ended and the refused change
    ///   still stands on its open pull request: the guard's second step.
    ///   The finding falls back to its runbook, its pull request is
    ///   recorded as a draft (the caller makes it one on GitHub), and no
    ///   further attempt starts by itself.
    /// - A pull request found on the branch that the record does not have
    ///   is never recorded here: it goes through the store's opening path,
    ///   where the guard reads it first.
    fn settle_run(
        &mut self,
        run: &RunNow,
        open_pr: Option<&OpenPr>,
        now: DateTime<Utc>,
    ) -> Option<Change> {
        if self.status != Status::Fixing {
            return None;
        }
        let known = self.pr.clone().filter(|p| p.state == "open");
        if open_pr.is_some_and(|o| known.as_ref().is_none_or(|p| p.number != o.number)) {
            return None;
        }
        let end = rel::run_end(run, Claim::claimed_at(self), now)?;
        let Some(pr) = known else {
            self.released(end.hold_days, now);
            self.release_note = end.waiting;
            return rel::change(end.text, "");
        };
        if self.refused.is_empty() {
            // Nothing was refused: the pull request stands.
            self.status = Status::PrOpen;
            self.ticket.clear();
            self.claimed_at = None;
            self.updated_at = now.to_rfc3339();
            return rel::change(format!("PR #{} is open", pr.number), &pr.url);
        }
        self.guard_attempts = self.guard_attempts.max(2);
        self.drafted = true;
        self.status = Status::Open;
        self.ticket.clear();
        self.claimed_at = None;
        self.pr = Some(PrRef {
            state: "draft".into(),
            ..pr.clone()
        });
        self.updated_at = now.to_rfc3339();
        rel::change(
            format!(
                "PR #{} is now a draft: the fix round ended without correcting the change, which would still replace or destroy a resource that holds data ({}). The finding shows its runbook. {}",
                pr.number,
                self.refused.join("; "),
                GUARD_STOPPED
            ),
            &pr.url,
        )
    }
}

/// The ticket of the one fix round after a tripped guard: the finding's
/// own ticket, then each offending line and what to produce instead.
pub fn fix_round_body(r: &Record) -> String {
    format!("{}{}", ticket_body(r), super::guard::fix_round(&r.refused))
}

/// A new read's findings against the stored ones: the records that
/// changed (new, updated, or fixed). Dismissed ones stay dismissed while
/// they still match.
pub fn reconcile(stored: &[Record], fresh: Vec<InfraFinding>, now: DateTime<Utc>) -> Vec<Record> {
    let now_s = now.to_rfc3339();
    let mut out = Vec::new();
    for f in &fresh {
        match stored.iter().find(|r| r.finding.id == f.id) {
            None => out.push(Record::new(f.clone(), &now_s)),
            Some(r) => {
                let mut r2 = r.clone();
                let reopened = r.status == Status::Fixed;
                r2.finding = f.clone();
                if reopened {
                    r2.status = if f.is_open() {
                        Status::Open
                    } else {
                        Status::AwsManaged
                    };
                    r2.fixed_at = None;
                    r2.pr = None;
                    r2.ticket.clear();
                    r2.clear_guard();
                    r2.event = Some("Found again in the latest read".into());
                }
                if r2 != *r {
                    r2.updated_at = now_s.clone();
                    out.push(r2);
                }
            }
        }
    }
    for r in stored
        .iter()
        .filter(|r| r.status != Status::Fixed && r.status != Status::Dismissed)
    {
        if fresh.iter().any(|f| f.id == r.finding.id) {
            continue;
        }
        let mut r2 = r.clone();
        r2.status = Status::Fixed;
        r2.fixed_at = Some(now_s.clone());
        r2.updated_at = now_s.clone();
        r2.event = Some(if r.status == Status::AwsManaged {
            "No longer applies (the resource or the bulletin changed)".into()
        } else {
            "Fixed: the latest read of the account shows the fix running".into()
        });
        if let Some(p) = &mut r2.pr {
            if p.state == "open" {
                p.state = "merged".into();
            }
        }
        out.push(r2);
    }
    out
}

/// The overview counts.
pub fn counts(records: &[Record], now: DateTime<Utc>) -> Value {
    let open: Vec<&Record> = records.iter().filter(|r| r.status.is_open()).collect();
    let n = |t: Tier| open.iter().filter(|r| r.tier() == Some(t)).count();
    let since = now - Duration::days(30);
    let fixed_30d = records
        .iter()
        .filter(|r| r.status == Status::Fixed && r.finding.fix.kind != Some(FixKind::Aws))
        .filter(|r| {
            r.fixed_at
                .as_deref()
                .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
                .is_some_and(|t| t > since)
        })
        .count();
    json!({
        "open": open.len(),
        "p0": n(Tier::P0), "p1": n(Tier::P1), "p2": n(Tier::P2), "p3": n(Tier::P3),
        "prs": open.iter().filter(|r| r.finding.fix.kind == Some(FixKind::Pr)).count(),
        "runbooks": open.iter().filter(|r| r.finding.fix.kind == Some(FixKind::Runbook)).count(),
        "aws_managed": records.iter().filter(|r| r.status == Status::AwsManaged).count(),
        "fixed_30d": fixed_30d,
    })
}

/// Insights › Analytics: opened and fixed in 30 days, time to fix.
pub fn insights(records: &[Record], now: DateTime<Utc>) -> Value {
    let since = now - Duration::days(30);
    let at = |s: &str| {
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|t| t.with_timezone(&Utc))
    };
    let actionable: Vec<&Record> = records
        .iter()
        .filter(|r| r.finding.fix.kind != Some(FixKind::Aws))
        .collect();
    let mut days: Vec<f64> = actionable
        .iter()
        .filter(|r| r.status == Status::Fixed)
        .filter_map(|r| {
            Some((at(r.fixed_at.as_deref()?)? - at(&r.first_seen_at)?).num_hours() as f64 / 24.0)
        })
        .collect();
    days.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = (!days.is_empty()).then(|| days[days.len() / 2]);
    let open: Vec<&&Record> = actionable.iter().filter(|r| r.status.is_open()).collect();
    let by = |t: Tier| open.iter().filter(|r| r.tier() == Some(t)).count();
    json!({
        "open": open.len(),
        "by_tier": { "P0": by(Tier::P0), "P1": by(Tier::P1), "P2": by(Tier::P2), "P3": by(Tier::P3) },
        "opened_30d": actionable.iter().filter(|r| at(&r.first_seen_at).is_some_and(|t| t > since)).count(),
        "fixed_30d": actionable.iter().filter(|r| r.fixed_at.as_deref().and_then(at).is_some_and(|t| t > since)).count(),
        "median_days_to_fix": median,
        "prs_opened_30d": actionable.iter().filter(|r| r.pr.is_some() && at(&r.updated_at).is_some_and(|t| t > since)).count(),
        "runbooks_open": open.iter().filter(|r| r.finding.fix.kind == Some(FixKind::Runbook)).count(),
        "aws_managed": records.iter().filter(|r| r.status == Status::AwsManaged).count(),
    })
}

/// A production fix waits for its non-production copy unless AWS's date
/// is this close.
pub const HOLD_UNLESS_DUE_DAYS: i64 = 7;

/// Why a fix run for this finding must not start yet: the same update is
/// still open on the non-production copy of the resource, which goes
/// first. `None` when nothing holds it, or when AWS's own date is within
/// [`HOLD_UNLESS_DUE_DAYS`] (then waiting would let AWS apply it unplanned).
pub fn held(r: &Record, all: &[Record], today: &str) -> Option<String> {
    let first: Vec<&Record> = all
        .iter()
        .filter(|x| r.finding.fix.after.contains(&x.finding.id) && x.status.is_open())
        .collect();
    if first.is_empty() {
        return None;
    }
    let day = |s: &str| chrono::NaiveDate::parse_from_str(s.get(..10)?, "%Y-%m-%d").ok();
    if let (Some(by), Some(now)) = (day(&r.finding.fix.apply_by), day(today)) {
        if (by - now).num_days() <= HOLD_UNLESS_DUE_DAYS {
            return None;
        }
    }
    Some(format!(
        "Waiting for the non-production copy first: {}",
        first
            .iter()
            .map(|x| format!("{} ({})", x.finding.name, x.finding.environment))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

fn where_line(l: &super::inventory::Location) -> String {
    format!(
        "Where: `{}`{}{}\n",
        l.file,
        if l.address.is_empty() {
            String::new()
        } else {
            format!(", `{}`", l.address)
        },
        if l.line > 0 {
            format!(" (line {})", l.line)
        } else {
            String::new()
        }
    )
}

/// The fix run's ticket: exactly what to change, where, and what must not
/// change. The run edits only the file named. What the guidance says for
/// the tool that defines the resource is part of it.
pub fn ticket_body(r: &Record) -> String {
    use super::inventory::Kind;
    let f = &r.finding;
    let mut out = format!("{}\n\n", f.title);
    out.push_str(&format!("Fix: {}\n", f.fix.summary));
    if let Some(l) = &f.fix.location {
        out.push_str(&where_line(l));
        if let Some(e) = &l.edit {
            out.push_str(&format!(
                "The value to change: `{}` in `{}`{}{}.\n",
                e.attribute,
                e.file,
                if e.line > 0 {
                    format!(", line {}", e.line)
                } else {
                    String::new()
                },
                match (e.via.as_str(), e.value.is_empty()) {
                    ("add", _) => " (nothing sets it yet: add it)".to_string(),
                    (_, true) => String::new(),
                    (via, false) => format!(
                        " (now `{}`, set through {})",
                        e.value,
                        via.replace('_', " ")
                    ),
                }
            ));
            if !e.shared.is_empty() {
                out.push_str(&format!(
                    "This value is shared: {}. Say so in the description.\n",
                    e.shared
                ));
            }
        }
        out.push_str(&format!("{}\n", super::locate::describe(l)));
    }
    out.push_str(&format!("From `{}` to `{}`.\n\n", f.fix.from, f.fix.to));
    let managed = matches!(
        f.kind,
        Some(
            Kind::RdsCluster | Kind::RdsInstance | Kind::ElasticacheGroup | Kind::OpensearchDomain
        )
    );
    match f.kind {
        Some(Kind::AutoScalingGroup) | Some(Kind::Ec2Instance) => {
            out.push_str("Change only the AMI. Prefer AWS's public parameter over a fixed AMI ID, so the next release is picked up too: in Terraform, `data \"aws_ssm_parameter\" \"al2023\" { name = \"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64\" }` and `image_id = data.aws_ssm_parameter.al2023.value` (the matching `-arm64` parameter for Graviton). Keep the launch template's other settings as they are. If the AMI comes from `data \"aws_ami\"` with `most_recent = true`, the code is already current: change nothing and say so; rolling the instances is the fix.\n");
        }
        Some(Kind::EksNodegroup) => {
            out.push_str(&format!("Set `release_version` on the node group (or the module input that feeds it) to `{}`. Change nothing else.\n", f.fix.to));
        }
        Some(Kind::LambdaFunction) => {
            out.push_str(&format!("Set `runtime` to `{}`, and check the handler code for anything the newer runtime changed.\n", f.fix.to));
        }
        _ => {}
    }
    if let Some(n) = &f.fix.notes {
        if managed {
            for a in &n.attribute {
                out.push_str(&format!("{a}\n"));
            }
            out.push_str(&format!(
                "\nSet the value to `{}` and change nothing else. If the value already allows `{}` (a prefix such as `16` for `16.6`), change nothing and say so.\n",
                f.fix.to, f.fix.to
            ));
        }
        if !n.never_change.is_empty() {
            out.push_str(&format!(
                "\nThis change must be in place. Never change any of these, on this or any other resource: {}. Never rename, move or remove a resource, a logical id or a construct id. Never lower a version. A change that would replace or destroy a resource that holds data is refused and its pull request is closed.\n",
                n.never_change
                    .iter()
                    .map(|a| format!("`{a}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        for t in n.replacement.iter().chain(n.traps.iter()) {
            out.push_str(&format!("- {t}\n"));
        }
        out.push_str("\nNever run an apply, a deploy or a change set: open the pull request and stop. CoderHelm writes the rollout notes (when it takes effect, the interruption, pre-checks, rollback) into the description itself.\n");
    }
    if !f.cves.is_empty() {
        out.push_str(&format!("\nFixes: {}.\n", f.cves.join(", ")));
    }
    if !f.fix.cli.is_empty() {
        out.push_str("\nAfter it merges and is applied, the running resources still need to move to the new version (not part of this change; for the PR description):\n");
        for c in &f.fix.cli {
            out.push_str(&format!("    {c}\n"));
        }
    }
    out
}

/// The PR's description.
pub fn pr_body(r: &Record, change: &str) -> String {
    let f = &r.finding;
    let mut out = String::new();
    out.push_str(&format!("{}\n\n", crate::security_fixes::MARKER));
    out.push_str("## Summary\n\n");
    out.push_str(&format!("{}.\n\n", f.title));
    out.push_str(&format!(
        "It runs on {} {} ({}).\n\n",
        f.count.max(1),
        if f.count == 1 {
            "resource"
        } else {
            "resources"
        },
        f.exposure.why
    ));
    if !f.fix.apply_by.is_empty() {
        out.push_str(&format!("AWS's date for this: {}.\n\n", f.fix.apply_by));
    }
    for o in &f.fix.order {
        out.push_str(&format!("{o}\n\n"));
    }
    out.push_str("## Changes\n\n");
    out.push_str(&format!("{change}\n\n"));
    if let Some(l) = &f.fix.location {
        out.push_str(&format!(
            "How the code was found: {} {}\n\n",
            super::locate::describe(l),
            l.evidence
                .iter()
                .map(|e| format!("{}.", super::locate::sentence(e)))
                .collect::<Vec<_>>()
                .join(" ")
        ));
        if let Some(e) = l.edit.as_ref().filter(|e| !e.shared.is_empty()) {
            out.push_str(&format!("Shared value: {}.\n\n", e.shared));
        }
    }
    out.push_str("## Advisories\n\n");
    for s in &f.sources {
        if s.url.is_empty() {
            out.push_str(&format!("- {}\n", s.id));
        } else {
            out.push_str(&format!("- [{}]({})\n", s.id, s.url));
        }
    }
    if !f.cves.is_empty() {
        out.push_str(&format!("\nCVEs: {}\n", f.cves.join(", ")));
    }
    if !f.fix.advisory_note.is_empty() {
        out.push_str(&format!("\n{}\n", f.fix.advisory_note));
    }
    if let Some(n) = &f.fix.notes {
        let notes = super::runbook::pr_notes_markdown(n);
        if !notes.trim().is_empty() {
            out.push_str("\n## Rollout\n");
            out.push_str(&notes);
        }
    }
    if !f.fix.cli.is_empty() {
        out.push_str("\n## After merge\n\nThe new version runs only once the resources roll. CoderHelm closes this finding when it reads the account again and sees it running.\n\n```\n");
        for c in &f.fix.cli {
            out.push_str(&format!("{c}\n"));
        }
        out.push_str("```\n");
    } else if f.fix.notes.is_some() {
        out.push_str("\n## After merge\n\nCoderHelm never applies this. It closes the finding when it reads the account again and sees the new version running.\n");
    }
    // The guard gave up on this change: every rebuild of the description
    // keeps the reason at its top, which keeps the pull request a draft.
    if r.drafted && !r.refused.is_empty() {
        return super::guard::put_hold(&out, &r.refused);
    }
    out
}

/// The alert card for a new urgent finding (P0 or P1), posted once.
pub fn card(r: &Record, url: &str) -> crate::notify::Message {
    use crate::notify::{Block, Button, Message, Size, Spacing, Text, Tone};
    let f = &r.finding;
    let tier = f.tier.map(Tier::as_str).unwrap_or("P3");
    let head = format!("🔴 {tier} infrastructure · {}", f.title);
    let what = match f.fix.kind {
        Some(FixKind::Pr) => format!(
            "Fix: {}. CoderHelm opens the PR within the day's limits.",
            f.fix.summary
        ),
        Some(FixKind::Runbook) => format!(
            "Fix in AWS: {}{}. CoderHelm never runs it.",
            f.fix.summary,
            if f.fix.apply_by.is_empty() {
                String::new()
            } else {
                format!(", AWS applies it after {}", f.fix.apply_by)
            }
        ),
        _ => f.fix.reason.clone(),
    };
    let mut facts = vec![
        (
            "Account".to_string(),
            format!("{} ({})", f.account, f.region),
        ),
        (
            "Runs on".to_string(),
            format!("{} ({})", f.count.max(1), f.exposure.why),
        ),
    ];
    if !f.cves.is_empty() {
        facts.push(("CVEs".into(), f.cves.join(", ")));
    }
    Message {
        summary: head.clone(),
        blocks: vec![
            Block::Banner {
                text: head,
                tone: Tone::Attention,
                size: Size::Medium,
            },
            Block::Text(Text::new(what).spacing(Spacing::Small)),
            Block::Facts {
                facts,
                spacing: Spacing::Small,
            },
        ],
        buttons: vec![Button::new("See the fix", url)],
        full_width: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra_vuln::matching::{Fix, FixKind};

    fn f(id: &str, kind: FixKind) -> InfraFinding {
        InfraFinding {
            id: id.into(),
            title: format!("finding {id}"),
            tier: (kind != FixKind::Aws).then_some(Tier::P1),
            fix: Fix {
                kind: Some(kind),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-10T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn only_a_new_read_closes_a_finding() {
        let first = reconcile(&[], vec![f("a", FixKind::Pr), f("b", FixKind::Aws)], now());
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].status, Status::Open);
        assert_eq!(first[1].status, Status::AwsManaged);
        // The PR merged, but the next read still finds the old AMI: open.
        let mut stored = first.clone();
        stored[0].status = Status::PrOpen;
        stored[0].pr = Some(PrRef {
            number: 7,
            url: "u".into(),
            state: "merged".into(),
            ..Default::default()
        });
        let again = reconcile(
            &stored,
            vec![f("a", FixKind::Pr), f("b", FixKind::Aws)],
            now(),
        );
        assert!(again.is_empty(), "nothing changed: {again:?}");
        assert!(stored[0].view()["note"]
            .as_str()
            .unwrap()
            .contains("waiting for the new version"));
        // The read no longer finds it: fixed, with the time.
        let later = reconcile(&stored, vec![f("b", FixKind::Aws)], now());
        assert_eq!(later.len(), 1);
        assert_eq!(later[0].status, Status::Fixed);
        assert!(later[0].event.as_ref().unwrap().contains("latest read"));
        // Found again: open again.
        let back = reconcile(&later, vec![f("a", FixKind::Pr)], now());
        assert!(back
            .iter()
            .any(|r| r.finding.id == "a" && r.status == Status::Open));
    }

    // ── The claim (`security_release::Claim`) ──

    use crate::security_release::{Claim, Closer, OpenPr, PrNow, RunNow, Standing};

    const WHY: &str = "db/main.tf: `identifier` changes, which replaces the database";

    /// A finding with a pull request to open, in a run or a pull request.
    fn claimed(status: Status, pr_state: Option<&str>) -> Record {
        let mut x = f("a", FixKind::Pr);
        x.fix.to = "16.6".into();
        x.fix.location = Some(crate::infra_vuln::inventory::Location {
            repo: "acme/infra".into(),
            file: "db/main.tf".into(),
            ..Default::default()
        });
        let mut r = Record::new(x, "2026-11-09T12:00:00Z");
        r.status = status;
        r.ticket = "SEC-0A1B2C3D4E5F".into();
        r.claimed_at = Some("2026-11-10T11:00:00Z".into());
        r.pr = pr_state.map(|st| PrRef {
            number: 7,
            url: "https://github.com/acme/infra/pull/7".into(),
            state: st.into(),
            ..Default::default()
        });
        r
    }

    fn by(login: &str) -> PrNow {
        PrNow::Closed(Closer {
            by: login.into(),
            person: true,
            at: Some("2026-11-10T11:30:00Z".into()),
        })
    }

    fn cancelled() -> RunNow {
        RunNow::Ended {
            status: "cancelled".into(),
            by: Some("dev@example.com".into()),
        }
    }

    #[test]
    fn a_pull_request_a_person_closed_gives_the_finding_back_with_who_and_when() {
        let mut r = claimed(Status::PrOpen, Some("open"));
        let c = r.settle_pr(&by("octo"), now()).unwrap();
        assert_eq!((r.status, r.standing()), (Status::Open, Standing::Open));
        let pr = r.pr.clone().unwrap();
        assert_eq!(
            (
                pr.number,
                pr.state.as_str(),
                pr.closed_by.as_deref(),
                pr.closed_by_person
            ),
            (7, "closed", Some("octo"), true)
        );
        assert_eq!(pr.closed_at.as_deref(), Some("2026-11-10T11:30:00Z"));
        assert!(r.ticket.is_empty() && r.claimed_at.is_none());
        // No run starts by itself for a week; the page gets the record.
        assert!(r.on_hold(now()) && r.on_hold(now() + Duration::days(6)));
        assert!(!r.on_hold(now() + Duration::days(8)));
        assert!(c.text.contains("PR #7 was closed without merging by octo"));
        assert!(c
            .text
            .contains("Ask for a fix to open a new pull request now"));
        let v = r.view();
        assert_eq!(v["status"], "open");
        assert_eq!(v["pr"]["state"], "closed");
        assert_eq!(v["pr"]["closed_by"], "octo");
        assert_eq!(v["pr"]["closed_by_person"], true);
        // The same fact again: nothing.
        let after = r.clone();
        assert!(r.settle_pr(&by("octo"), now()).is_none());
        assert_eq!(r, after);
        // Gone from GitHub releases too; GitHub not answering never does.
        let mut gone = claimed(Status::PrOpen, Some("open"));
        assert!(gone.settle_pr(&PrNow::Unknown, now()).is_none());
        let c = gone.settle_pr(&PrNow::Gone, now()).unwrap();
        assert_eq!(gone.status, Status::Open);
        assert!(c.text.starts_with("PR #7 is no longer on GitHub"));
    }

    #[test]
    fn a_merge_is_recorded_and_never_closes_the_finding() {
        let mut r = claimed(Status::PrOpen, Some("open"));
        let c = r.settle_pr(&PrNow::Merged, now()).unwrap();
        // Only a read of the account closes it.
        assert_eq!(r.status, Status::PrOpen);
        assert_eq!(r.pr.as_ref().unwrap().state, "merged");
        assert!(c.text.contains("reads the account again"));
        assert!(r.view()["note"]
            .as_str()
            .unwrap()
            .contains("waiting for the new version"));
        assert!(!r.watches_pr(now()));
        assert!(r.settle_pr(&PrNow::Merged, now()).is_none());
        assert!(r.settle_pr(&by("octo"), now()).is_none());
    }

    #[test]
    fn a_draft_the_guard_held_is_not_closed_and_keeps_its_runbook() {
        let mut r = claimed(Status::Open, Some("draft"));
        r.ticket.clear();
        r.guard_step(&[WHY.to_string()]);
        r.guard_step(&[WHY.to_string()]);
        assert!(r.drafted && r.guard_blocks());
        let before = r.clone();
        // GitHub has a draft as open: the record is left exactly as it is.
        assert!(r.watches_pr(now()));
        assert!(r.settle_pr(&PrNow::Open, now()).is_none());
        assert_eq!(r, before);
        // A person closes the draft: recorded, and the finding still falls
        // back to its runbook with no attempt by itself.
        let c = r.settle_pr(&by("octo"), now()).unwrap();
        assert_eq!(r.pr.as_ref().unwrap().state, "closed");
        assert_eq!(r.status, Status::Open);
        assert!(r.drafted && r.guard_blocks() && r.hold_until.is_none());
        assert!(
            c.text.ends_with("the finding keeps its runbook"),
            "{}",
            c.text
        );
        assert_eq!(r.view()["fix"]["kind"], "runbook");
        // Reopening it does not put the refused change back as the fix.
        assert!(!r.watches_pr(now()));
        assert!(r.settle_pr(&PrNow::Open, now()).is_none());
        // Its code or target changed: the refusal no longer stands.
        r.finding.fix.to = "16.7".into();
        assert!(!r.guard_blocks());
    }

    #[test]
    fn a_reopened_pull_request_takes_the_finding_back_unless_it_moved_on() {
        let mut r = claimed(Status::PrOpen, Some("open"));
        r.settle_pr(&by("octo"), now()).unwrap();
        assert!(r.watches_pr(now()));
        let c = r.settle_pr(&PrNow::Open, now()).unwrap();
        assert_eq!(r.status, Status::PrOpen);
        let pr = r.pr.clone().unwrap();
        assert_eq!(
            (pr.state.as_str(), pr.closed_by, pr.closed_at),
            ("open", None, None)
        );
        assert!(r.hold_until.is_none() && c.text.contains("reopened"));
        // A new run took the finding in between: it is not pulled back.
        let mut r = claimed(Status::PrOpen, Some("open"));
        r.settle_pr(&by("octo"), now()).unwrap();
        r.claim("SEC-0A1B2C3D4E5F", now());
        assert!(r.hold_until.is_none(), "a claim ends the hold");
        assert!(r.settle_pr(&PrNow::Open, now()).is_none());
        assert_eq!(r.status, Status::Fixing);
    }

    #[test]
    fn a_run_that_ended_without_a_pull_request_releases_the_finding() {
        let mut r = claimed(Status::Fixing, None);
        assert!(r.settle_run(&RunNow::Working, None, now()).is_none());
        assert!(r.settle_run(&RunNow::Unknown, None, now()).is_none());
        // Claimed an hour ago with no run on record: still in the queue.
        assert!(r.settle_run(&RunNow::Missing, None, now()).is_none());
        let c = r.settle_run(&cancelled(), None, now()).unwrap();
        assert_eq!(r.status, Status::Open);
        assert!(r.ticket.is_empty());
        assert!(r.on_hold(now() + Duration::days(6)));
        assert!(c.text.contains("cancelled by dev@example.com"));
        assert_eq!(
            r.view()["note"],
            "The fix run was cancelled by dev@example.com. Ask for a fix to start a new one."
        );
        assert!(r.settle_run(&cancelled(), None, now()).is_none());
        // A run that failed: tried again tomorrow.
        let mut r = claimed(Status::Fixing, None);
        let failed = RunNow::Ended {
            status: "failed".into(),
            by: None,
        };
        r.settle_run(&failed, None, now()).unwrap();
        assert!(r.on_hold(now() + Duration::hours(23)));
        assert!(!r.on_hold(now() + Duration::hours(25)));
        assert_eq!(r.view().get("note"), None);
    }

    #[test]
    fn a_pull_request_the_record_never_learned_goes_through_the_guard_not_this_rule() {
        let mut r = claimed(Status::Fixing, None);
        assert!(r.opens_through_store());
        let found = OpenPr {
            number: 9,
            url: "https://github.com/acme/infra/pull/9".into(),
        };
        let before = r.clone();
        assert!(r.settle_run(&cancelled(), Some(&found), now()).is_none());
        assert_eq!(r, before, "nothing is recorded before the guard reads it");
    }

    #[test]
    fn a_fix_round_that_ends_without_settling_is_the_guards_second_step() {
        // The guard tripped once: the round works on the open pull request.
        let mut r = claimed(Status::Fixing, Some("open"));
        assert_eq!(r.guard_step(&[WHY.to_string()]), GuardStep::FixRound);
        assert!(r.run_decides_with_pr());
        let same = OpenPr {
            number: 7,
            url: "https://github.com/acme/infra/pull/7".into(),
        };
        assert!(r.settle_run(&RunNow::Working, Some(&same), now()).is_none());
        // The round was cancelled: the refused change still stands.
        let c = r.settle_run(&cancelled(), Some(&same), now()).unwrap();
        assert_eq!(r.status, Status::Open);
        assert_eq!(r.pr.as_ref().unwrap().state, "draft");
        assert!(r.drafted && r.guard_blocks() && r.ticket.is_empty());
        assert!(c.text.contains("PR #7 is now a draft") && c.text.contains(WHY));
        assert!(c.text.contains(GUARD_STOPPED));
        assert_eq!(r.view()["fix"]["kind"], "runbook");
        assert_eq!(r.view()["note"], GUARD_DRAFT_NOTE);
        assert!(r.settle_run(&cancelled(), Some(&same), now()).is_none());
    }

    #[test]
    fn a_pull_request_closed_during_the_fix_round_keeps_the_refusal_standing() {
        let mut r = claimed(Status::Fixing, Some("open"));
        r.guard_step(&[WHY.to_string()]);
        let c = r.settle_pr(&by("octo"), now()).unwrap();
        assert_eq!(r.status, Status::Open);
        assert!(r.guard_blocks() && !r.drafted);
        assert!(c.text.contains("had refused the change"), "{}", c.text);
        assert_eq!(r.view()["note"], GUARD_CLOSED_NOTE);
        // Not watched for a reopen: the refused change is never the fix.
        assert!(!r.watches_pr(now()));
    }

    #[test]
    fn records_stored_before_these_fields_read_as_before() {
        // A record as it was stored before: none of the new fields.
        let mut old = claimed(Status::PrOpen, Some("open"));
        old.claimed_at = None;
        old.event = None;
        let stored = serde_json::to_value(&old).unwrap();
        for k in ["hold_until", "claimed_at", "release_note"] {
            assert!(stored.get(k).is_none(), "{k}");
        }
        for k in ["closed_at", "closed_by", "closed_by_person"] {
            assert!(stored["pr"].get(k).is_none(), "{k}");
        }
        let back: Record = serde_json::from_value(stored).unwrap();
        assert_eq!(back, old);
        // Its claim time falls back to its last write.
        assert_eq!(
            Claim::claimed_at(&back),
            crate::security_fixes::parse_time("2026-11-09T12:00:00Z")
        );
    }

    #[test]
    fn aws_managed_rows_never_count_as_open() {
        let r = reconcile(
            &[],
            vec![
                f("a", FixKind::Pr),
                f("b", FixKind::Aws),
                f("c", FixKind::Runbook),
            ],
            now(),
        );
        let c = counts(&r, now());
        assert_eq!(c["open"], 2);
        assert_eq!(c["aws_managed"], 1);
        assert_eq!(c["runbooks"], 1);
        assert_eq!(c["p1"], 2);
        let i = insights(&r, now());
        assert_eq!(i["open"], 2);
        assert_eq!(i["opened_30d"], 2);
    }

    #[test]
    fn the_ticket_says_exactly_what_to_change() {
        let mut x = f("a", FixKind::Pr);
        x.kind = Some(crate::infra_vuln::inventory::Kind::AutoScalingGroup);
        x.fix.from = "2023.12.20261020".into();
        x.fix.to = "2023.12.20261102".into();
        x.fix.location = Some(crate::infra_vuln::inventory::Location {
            repo: "acme/infra".into(),
            file: "envs/prod/main.tf".into(),
            address: "aws_launch_template.web".into(),
            line: 3,
            how: "search".into(),
            ..Default::default()
        });
        x.fix.cli =
            vec!["aws autoscaling start-instance-refresh --auto-scaling-group-name web-asg".into()];
        let r = Record::new(x, "t");
        let t = ticket_body(&r);
        assert!(t.contains("`envs/prod/main.tf`, `aws_launch_template.web` (line 3)"));
        assert!(t.contains("aws_ssm_parameter"));
        let b = pr_body(&r, "Pointed the launch template at the AL2023 parameter.");
        assert!(b.starts_with(crate::security_fixes::MARKER));
        assert!(b.contains("## After merge"));
        assert!(!b.contains('\u{2192}'));
    }

    fn upgrade(id: &str, name: &str, env: &str, by: &str) -> InfraFinding {
        use crate::infra_vuln::inventory::{Confidence, Edit, Kind, Location};
        let mut x = f(id, FixKind::Pr);
        x.kind = Some(Kind::RdsCluster);
        x.name = name.into();
        x.environment = env.into();
        x.title = format!("Aurora cluster {name}: engine version upgrade to 16.6 pending");
        x.fix.summary = "Raise `engine_version` to 16.6 in envs/prod/main.tf".into();
        x.fix.from = "16.4".into();
        x.fix.to = "16.6".into();
        x.fix.apply_by = by.into();
        x.fix.location = Some(Location {
            repo: "acme/infra".into(),
            file: "modules/aurora/main.tf".into(),
            address: "module.ledger.aws_rds_cluster.this".into(),
            line: 6,
            how: "terraform_module".into(),
            tool: "terraform".into(),
            confidence: Confidence::High,
            evidence: vec!["`cluster_identifier` is `ledger-prod-aurora`, the live name".into()],
            edit: Some(Edit {
                file: "envs/prod/main.tf".into(),
                line: 4,
                attribute: "engine_version".into(),
                value: "16.4".into(),
                via: "module_input".into(),
                shared: String::new(),
            }),
            environment: env.into(),
            ..Default::default()
        });
        let v = std::collections::BTreeMap::new();
        x.fix.notes = Some(crate::infra_vuln::runbook::pr_notes(
            "aurora",
            "engine_upgrade",
            "terraform",
            std::collections::BTreeMap::new(),
            &v,
            "2026-11-13",
        ));
        x
    }

    #[test]
    fn a_database_upgrade_ticket_names_the_edit_and_what_must_never_change() {
        let r = Record::new(
            upgrade("a", "ledger-prod-aurora", "production", "2027-01-15"),
            "t",
        );
        let t = ticket_body(&r);
        assert!(t.contains("The value to change: `engine_version` in `envs/prod/main.tf`, line 4 (now `16.4`, set through module input)"), "{t}");
        assert!(
            t.contains("Never raise it on an `aws_rds_cluster_instance`"),
            "{t}"
        );
        assert!(t.contains("Never change any of these"), "{t}");
        assert!(t.contains("`cluster_identifier`"), "{t}");
        assert!(t.contains("Never lower a version"), "{t}");
        assert!(t.contains("Never run an apply"), "{t}");
        assert!(
            t.contains("a Terraform module call and its inputs (confidence: high)"),
            "{t}"
        );
        let b = pr_body(
            &r,
            "Raised the ledger cluster's engine version input to 16.6.",
        );
        for part in [
            "## Summary",
            "## Changes",
            "## Rollout",
            "**When it takes effect**",
            "**Expected interruption**",
            "**Replacement**",
            "**What the plan must show**",
            "**Before applying**",
            "**Rollback**",
            "**Known traps**",
            "**Sources**",
            "AWS's date for this: 2027-01-15.",
            "How the code was found:",
            "CoderHelm never applies this",
        ] {
            assert!(b.contains(part), "the description has no {part}:\n{b}");
        }
        assert!(b.contains("(read 2026-11-13)"));
        for arrow in ['\u{2192}', '\u{21d2}', '\u{279c}'] {
            assert!(!b.contains(arrow) && !t.contains(arrow));
        }
    }

    #[test]
    fn production_waits_for_its_non_production_copy_unless_aws_is_about_to_apply_it() {
        let staging = Record::new(
            upgrade("s", "ledger-staging-aurora", "staging", "2027-01-15"),
            "t",
        );
        let mut p = upgrade("p", "ledger-prod-aurora", "production", "2027-01-15");
        p.fix.after = vec!["s".into()];
        let prod = Record::new(p, "t");
        let all = vec![staging.clone(), prod.clone()];
        let why = held(&prod, &all, "2026-11-13").expect("held");
        assert!(why.contains("ledger-staging-aurora (staging)"), "{why}");
        assert_eq!(held(&staging, &all, "2026-11-13"), None);
        // Staging is done (a read no longer finds it): production goes.
        let mut done = staging.clone();
        done.status = Status::Fixed;
        assert_eq!(held(&prod, &[done, prod.clone()], "2026-11-13"), None);
        // AWS's date is a week away: waiting would let AWS apply it unplanned.
        assert_eq!(held(&prod, &all, "2027-01-10"), None);
    }

    const LINE: &str =
        "modules/aurora/main.tf, line 5: `aws_rds_cluster.this` changes `cluster_identifier`, which replaces the resource";

    #[test]
    fn a_first_trip_is_one_fix_round_told_the_offending_line() {
        let mut r = Record::new(upgrade("a", "ledger-prod-aurora", "production", ""), "t");
        assert!(r.view().get("refused").is_none());
        assert_eq!(r.guard_step(&[LINE.to_string()]), GuardStep::FixRound);
        assert_eq!((r.guard_attempts, r.drafted), (1, false));
        let t = fix_round_body(&r);
        // The ticket's own words, then the line and the in-place change.
        assert!(
            t.contains("The value to change: `engine_version` in `envs/prod/main.tf`, line 4"),
            "{t}"
        );
        assert!(t.contains(&format!("- {LINE}")), "{t}");
        assert!(
            t.contains("make the in-place change this ticket asks for"),
            "{t}"
        );
        // Still a pull request on the page, with what is happening.
        let v = r.view();
        assert_eq!(v["fix"]["kind"], "pr");
        assert_eq!(v["drafted"], false);
        assert_eq!(v["note"], GUARD_ROUND_NOTE);
        // The description is not marked while the round runs.
        assert!(crate::infra_vuln::guard::hold(&pr_body(&r, "Raised it.")).is_none());
    }

    #[test]
    fn a_fix_round_that_passes_the_guard_clears_the_refusal() {
        let mut r = Record::new(upgrade("a", "ledger-prod-aurora", "production", ""), "t");
        r.guard_step(&[LINE.to_string()]);
        assert_eq!(r.guard_step(&[]), GuardStep::Passed);
        assert!(r.refused.is_empty() && !r.drafted && r.guard_attempts == 0);
        assert!(!r.guard_blocks());
        assert!(r.view().get("refused").is_none());
    }

    #[test]
    fn a_second_trip_drafts_the_pull_request_and_falls_back_to_the_runbook() {
        let mut r = Record::new(upgrade("a", "ledger-prod-aurora", "production", ""), "t");
        r.finding.fix.runbook = Some(crate::infra_vuln::runbook::Runbook {
            steps: vec![crate::infra_vuln::runbook::Step {
                text: "Upgrade the cluster in its next maintenance window.".into(),
                cli: "aws rds modify-db-cluster --db-cluster-identifier ledger-prod-aurora --engine-version 16.6".into(),
            }],
            ..Default::default()
        });
        r.guard_step(&[LINE.to_string()]);
        assert_eq!(r.guard_step(&[LINE.to_string()]), GuardStep::Draft);
        assert!(r.drafted && r.guard_attempts == 2);
        // On the finding: the reason, a runbook, and why.
        let v = r.view();
        assert_eq!(v["refused"][0], LINE);
        assert_eq!(v["drafted"], true);
        assert_eq!(v["fix"]["kind"], "runbook");
        assert!(v["fix"]["decision"]
            .as_str()
            .unwrap()
            .contains("its pull request is a draft"));
        assert!(v["fix"]["decision"]
            .as_str()
            .unwrap()
            .contains("no further attempt until the code"));
        assert!(v["fix"]["runbook"]["steps"][0]["cli"]
            .as_str()
            .unwrap()
            .contains("modify-db-cluster"));
        assert_eq!(v["note"], GUARD_DRAFT_NOTE);
        // The stored finding is unchanged: it is still a code fix, should
        // its code or target change.
        assert_eq!(r.finding.fix.kind, Some(FixKind::Pr));
    }

    #[test]
    fn a_draft_stays_a_draft_through_later_description_rebuilds() {
        use crate::infra_vuln::guard;
        let mut r = Record::new(upgrade("a", "ledger-prod-aurora", "production", ""), "t");
        r.guard_step(&[LINE.to_string()]);
        r.guard_step(&[LINE.to_string()]);
        for change in ["Raised it.", "Raised it again, after a later run.", ""] {
            let b = pr_body(&r, change);
            assert!(b.starts_with(crate::security_fixes::MARKER), "{b}");
            assert_eq!(b.matches(guard::HOLD_START).count(), 1);
            assert!(
                guard::hold(&b).is_some(),
                "a rebuilt description lost the hold"
            );
            let top = &b[..b.find("## Summary").unwrap()];
            assert!(
                top.contains("This pull request is a draft") && top.contains(LINE),
                "{top}"
            );
        }
        // Once the guard passes a later change, the section goes.
        r.guard_step(&[]);
        assert!(guard::hold(&pr_body(&r, "Raised it.")).is_none());
    }

    #[test]
    fn no_further_attempt_until_the_code_or_the_target_changes() {
        let mut r = Record::new(upgrade("a", "ledger-prod-aurora", "production", ""), "t");
        assert!(!r.guard_blocks());
        r.guard_step(&[LINE.to_string()]);
        r.guard_step(&[LINE.to_string()]);
        assert!(r.guard_blocks());
        // A new read with the same finding: still blocked.
        let same = reconcile(&[r.clone()], vec![r.finding.clone()], now());
        assert!(same.is_empty(), "{same:?}");
        // The target changes.
        let mut target = r.clone();
        target.finding.fix.to = "16.8".into();
        assert!(!target.guard_blocks());
        // The code changes (the line that sets the value moved or changed).
        let mut code = r.clone();
        if let Some(e) = code
            .finding
            .fix
            .location
            .as_mut()
            .and_then(|l| l.edit.as_mut())
        {
            e.value = "16.5".into();
        }
        assert!(!code.guard_blocks());
        code.start_over();
        assert!(code.refused.is_empty() && !code.drafted && code.guard_attempts == 0);
        // Found again after being fixed: the refusal belongs to the past.
        r.status = Status::Fixed;
        let again = reconcile(&[r.clone()], vec![r.finding.clone()], now());
        assert!(again[0].refused.is_empty() && !again[0].drafted);
    }
}
