//! Stored infrastructure findings and their life: `INFRAFINDING#<id>` in
//! the settings table, one per fix ([`super::matching`] decides them).
//!
//! - A finding a read no longer produces is **fixed**, with the time:
//!   only a new read of the account closes one, never a merged PR.
//! - A merged PR without the new version running stays open and says so.
//! - AWS-managed rows are kept as `aws_managed` and never count as open.

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
    /// `open`, `merged` or `closed`.
    pub state: String,
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
    /// ([`super::guard`]). While it is set no new fix run starts by
    /// itself: the finding is applied by its runbook or by hand.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refused: Vec<String>,
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
            if !self.refused.is_empty() {
                o.insert("refused".into(), json!(self.refused));
            }
            // The fix's kind as the contract names it.
            o["fix"]["kind"] = json!(self.finding.fix.kind);
            if self.status == Status::PrOpen
                && self.pr.as_ref().is_some_and(|p| p.state == "merged")
            {
                o.insert("note".into(), json!("PR merged: waiting for the new version to run. It closes when CoderHelm reads the account again and sees it."));
            }
        }
        v
    }
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
                    r2.refused.clear();
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

    #[test]
    fn a_refused_change_is_kept_on_the_record_and_shown() {
        let mut r = Record::new(upgrade("a", "ledger-prod-aurora", "production", ""), "t");
        assert!(r.view().get("refused").is_none());
        r.refused = vec!["db/main.tf: `aws_db_instance.orders` changes `identifier`".into()];
        assert_eq!(r.view()["refused"][0], r.refused[0]);
        // Found again after being fixed: the refusal belongs to the past.
        r.status = Status::Fixed;
        let again = reconcile(&[r.clone()], vec![r.finding.clone()], now());
        assert!(again[0].refused.is_empty());
    }
}
