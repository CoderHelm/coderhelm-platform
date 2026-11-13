//! The runbook a person follows when an update is applied in AWS, and the
//! notes a pull request carries when it is applied through code. Both are
//! written from the guidance store ([`super::guidance`]) with the
//! resource's own identifiers filled in; no service fact lives here.
//!
//! CoderHelm never runs a runbook: it is a card for a person.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::actions::{self, Meaning};
use super::guidance::{self, Cited, Entry, Query};
use super::inventory::{Kind, Pending, Resource};

/// One thing to do or check, with its command when there is one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub text: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cli: String,
}

/// A complete runbook for one update of one resource.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Runbook {
    /// The account, Region and exact identifiers it applies to.
    pub scope: Vec<String>,
    /// What the update is and what it fixes.
    pub what: Vec<String>,
    pub interruption: Vec<String>,
    /// Permissions and tools the person needs.
    pub prerequisites: Vec<String>,
    pub prechecks: Vec<Step>,
    pub backup: Vec<Step>,
    /// The console path, step by step.
    pub console: Vec<String>,
    /// The commands that apply it.
    pub steps: Vec<Step>,
    pub validation: Vec<Step>,
    pub rollback: Vec<Step>,
    /// AWS's apply-by date (YYYY-MM-DD), and what AWS does after it.
    pub apply_by: String,
    pub if_nothing: Vec<String>,
    /// Order among other pending work, traps, what to do in code after.
    pub notes: Vec<String>,
    /// The pages the facts came from, with the day each was last read.
    pub sources: Vec<Cited>,
    /// A fact here was not re-read within the guidance store's limit.
    pub stale: bool,
}

/// The guidance store's service for a resource.
pub fn service_of(kind: Kind, engine: &str) -> &'static str {
    match kind {
        Kind::RdsCluster if engine.starts_with("aurora") => "aurora",
        // A Multi-AZ DB cluster (a cluster that is not Aurora) has rules
        // of its own that the store does not hold yet.
        Kind::RdsCluster => "",
        Kind::RdsInstance => "rds",
        Kind::ElasticacheGroup => "elasticache",
        Kind::OpensearchDomain => "opensearch",
        Kind::EksNodegroup => "eks_nodegroup",
        Kind::AutoScalingGroup | Kind::Ec2Instance => "ami",
        Kind::LambdaFunction => "lambda_runtime",
        _ => "",
    }
}

/// The guidance store's action for a kind of finding with no pending
/// action of its own (an AMI, a node group release, a runtime).
pub fn action_of(kind: Kind) -> &'static str {
    match kind {
        Kind::EksNodegroup => "nodegroup_release",
        Kind::AutoScalingGroup | Kind::Ec2Instance => "ami_replace",
        Kind::LambdaFunction => "runtime_change",
        _ => "any",
    }
}

fn tail(arn: &str) -> &str {
    arn.rsplit([':', '/']).next().unwrap_or(arn)
}

/// The values a guidance entry's placeholders take for this resource.
pub fn vars(
    r: &Resource,
    p: Option<&Pending>,
    from: &str,
    target: &str,
    today: &str,
) -> BTreeMap<&'static str, String> {
    let arn = p
        .and_then(|p| p.arns.first().cloned())
        .unwrap_or_else(|| r.id.clone());
    let cluster = match r.kind {
        Kind::RdsCluster => r.name.clone(),
        _ => r
            .tags
            .get("coderhelm:cluster")
            .or(r.facts.get("cluster"))
            .cloned()
            .unwrap_or_default(),
    };
    let action = p.map(|p| p.kind.clone()).unwrap_or_default();
    let mut v = BTreeMap::new();
    v.insert("arn", arn.clone());
    // The identifier a command takes: the resource the ARN names (an
    // instance of a cluster has its own), else the record's name.
    v.insert(
        "id",
        if arn.starts_with("arn:aws:rds:") {
            tail(&arn).to_string()
        } else {
            r.name.clone()
        },
    );
    v.insert("cluster", cluster);
    v.insert("region", r.region.clone());
    v.insert("account", r.account.clone());
    v.insert("engine", r.versions.engine.clone());
    v.insert("from", from.to_string());
    v.insert("target", target.to_string());
    v.insert("update", p.map(|p| p.name.clone()).unwrap_or_default());
    v.insert(
        "snapshot",
        format!(
            "{}-before-{}-{}",
            r.name,
            if action.is_empty() { "update" } else { &action },
            today.replace('-', "")
        ),
    );
    v.insert("action", action);
    v
}

fn texts(entries: &[&Entry], v: &BTreeMap<&str, String>) -> Vec<String> {
    entries.iter().map(|e| guidance::fill(&e.text, v)).collect()
}

fn steps(entries: &[&Entry], v: &BTreeMap<&str, String>) -> Vec<Step> {
    entries
        .iter()
        .map(|e| Step {
            text: guidance::fill(&e.text, v),
            cli: guidance::fill(&e.cli, v),
        })
        .collect()
}

/// The runbook for a pending action on a resource.
pub fn build(r: &Resource, p: &Pending, today: &str) -> Runbook {
    let service = service_of(r.kind, &r.versions.engine);
    let action = p.meaning.guidance_action();
    let from = &r.versions.engine_version;
    let mut facts = BTreeMap::new();
    if let Some(major) = actions::is_major(&r.versions.engine, from, &p.target) {
        facts.insert("major".to_string(), major.to_string());
    }
    if !p.scope.is_empty() {
        facts.insert("scope".to_string(), p.scope.clone());
    }
    let q = Query {
        service,
        action,
        tool: "",
        facts,
    };
    let v = vars(r, Some(p), from, &p.target, today);
    let mut used: Vec<&Entry> = Vec::new();
    let mut take = |section: &str| -> Vec<&'static Entry> {
        let e = if service.is_empty() || action == "any" {
            Vec::new()
        } else {
            guidance::section(&q, section)
        };
        used.extend(e.iter().copied());
        e
    };

    let mut scope = vec![format!("AWS account {}, Region {}.", r.account, r.region)];
    scope.push(format!("{} `{}` ({}).", r.kind.label(), r.name, r.id));
    if p.arns.len() > 1 {
        scope.push(format!(
            "The action is pending on {} ARNs; apply it to each, in this order: {}.",
            p.arns.len(),
            p.arns.join(", ")
        ));
    } else if let Some(a) = p.arns.first().filter(|a| *a != &r.id) {
        scope.push(format!("The action is pending on {a}."));
    }
    scope.push(format!(
        "AWS action `{}`{}: {}{}.",
        p.kind,
        if p.name != p.kind && !p.name.is_empty() && p.meaning != Meaning::EnginePatch {
            format!(" `{}`", p.name)
        } else {
            String::new()
        },
        p.meaning.label(),
        if p.description.is_empty() {
            String::new()
        } else {
            format!(" (AWS's description: \"{}\")", p.description)
        }
    ));
    if !from.is_empty() {
        scope.push(format!(
            "It runs {} {}{}.",
            r.versions.engine,
            from,
            if p.target.is_empty() {
                String::new()
            } else {
                format!("; the action moves it to {}", p.target)
            }
        ));
    }
    for m in &p.merged {
        scope.push(format!("Note: {m}."));
    }

    let mut what = texts(&take("what"), &v);
    what.extend(texts(&take("cve"), &v));
    let interruption = texts(&take("interruption"), &v);
    let mut prerequisites = texts(&take("permissions"), &v);
    prerequisites.push(format!(
        "AWS CLI v2 signed in to account {} with Region {}, and a change window agreed with the people who own what uses this resource.",
        r.account, r.region
    ));
    let prechecks = steps(&take("precheck"), &v);
    let backup = steps(&take("backup"), &v);
    let console = texts(&take("console"), &v);

    // Apply: once per ARN the action is pending on.
    let apply = take("apply");
    let mut do_steps = Vec::new();
    for e in &apply {
        if p.arns.len() > 1 && e.cli.contains("{arn}") {
            for (n, arn) in p.arns.iter().enumerate() {
                let mut one = v.clone();
                one.insert("arn", arn.clone());
                one.insert("id", tail(arn).to_string());
                do_steps.push(Step {
                    text: format!(
                        "{} ({} of {}: {})",
                        guidance::fill(&e.text, &one),
                        n + 1,
                        p.arns.len(),
                        tail(arn)
                    ),
                    cli: guidance::fill(&e.cli, &one),
                });
            }
        } else {
            do_steps.push(Step {
                text: guidance::fill(&e.text, &v),
                cli: guidance::fill(&e.cli, &v),
            });
        }
    }
    // No reviewed guidance for this resource or action: the command AWS
    // documents for any pending maintenance action still applies.
    let mut notes = Vec::new();
    if do_steps.is_empty() && r.id.starts_with("arn:aws:rds:") {
        for arn in &p.arns {
            do_steps.push(Step {
                text: "Schedule the pending action for the next maintenance window.".into(),
                cli: format!(
                    "aws rds apply-pending-maintenance-action --resource-identifier {arn} --apply-action {} --opt-in-type next-maintenance --region {}",
                    p.kind, r.region
                ),
            });
        }
        notes.push("CoderHelm has no reviewed guidance for this action on this kind of resource: read AWS's description of the action and the service's documentation before applying it.".to_string());
    }
    let validation = steps(&take("validation"), &v);
    let rollback = steps(&take("rollback"), &v);
    let if_nothing = texts(&take("if_nothing"), &v);
    if !p.order_why.is_empty() {
        notes.push(p.order_why.clone());
    }
    notes.extend(texts(&take("trap"), &v));
    notes.extend(texts(&take("order"), &v));

    let sources = guidance::cited(&used, today);
    let stale = sources.iter().any(|s| s.stale);
    Runbook {
        scope,
        what,
        interruption,
        prerequisites,
        prechecks,
        backup,
        console,
        steps: do_steps,
        validation,
        rollback,
        apply_by: p.apply_by.clone(),
        if_nothing,
        notes,
        sources,
        stale,
    }
}

/// What a pull request for a code change must say and check, from the
/// guidance for the tool that defines the resource.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PrNotes {
    /// Which attribute to change, for this tool.
    pub attribute: Vec<String>,
    pub takes_effect: Vec<String>,
    pub interruption: Vec<String>,
    /// What replaces the resource, and so what the change must not touch.
    pub replacement: Vec<String>,
    /// The attributes whose change replaces the resource.
    pub never_change: Vec<String>,
    pub prechecks: Vec<Step>,
    pub backup: Vec<Step>,
    pub rollback: Vec<String>,
    pub traps: Vec<String>,
    /// What the plan or change set must show, since CoderHelm runs none.
    pub plan: Vec<String>,
    pub pipeline: Vec<String>,
    pub order: Vec<String>,
    pub validation: Vec<Step>,
    pub sources: Vec<Cited>,
    pub stale: bool,
}

/// The notes for a code change to `service`'s `action` with `tool`
/// (`terraform`, `cloudformation`, `cdk`, or a tool that shares one of
/// those: see [`guidance::tool_key`]).
pub fn pr_notes(
    service: &str,
    action: &str,
    tool: &str,
    facts: BTreeMap<String, String>,
    v: &BTreeMap<&str, String>,
    today: &str,
) -> PrNotes {
    let q = Query {
        service,
        action,
        tool: guidance::tool_key(tool),
        facts,
    };
    let mut used: Vec<&Entry> = Vec::new();
    let mut take = |section: &str| -> Vec<&'static Entry> {
        let e = guidance::section(&q, section);
        used.extend(e.iter().copied());
        e
    };
    let replacement = take("replacement");
    let mut never_change: Vec<String> = replacement
        .iter()
        .flat_map(|e| e.attributes.iter().cloned())
        .collect();
    never_change.dedup();
    let mut traps = texts(&take("trap"), v);
    traps.extend(texts(&take("option"), v));
    let out = PrNotes {
        attribute: texts(&take("attribute"), v),
        takes_effect: texts(&take("takes_effect"), v),
        interruption: texts(&take("interruption"), v),
        replacement: texts(&replacement, v),
        never_change,
        prechecks: steps(&take("precheck"), v),
        backup: steps(&take("backup"), v),
        rollback: texts(&take("rollback"), v),
        traps,
        plan: texts(&take("plan"), v),
        pipeline: texts(&take("pipeline"), v),
        order: texts(&take("order"), v),
        validation: steps(&take("validation"), v),
        sources: Vec::new(),
        stale: false,
    };
    let sources = guidance::cited(&used, today);
    PrNotes {
        stale: sources.iter().any(|s| s.stale),
        sources,
        ..out
    }
}

fn bullets(out: &mut String, title: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }
    out.push_str(&format!("\n**{title}**\n\n"));
    for i in items {
        out.push_str(&format!("- {i}\n"));
    }
}

fn step_list(out: &mut String, title: &str, items: &[Step]) {
    if items.is_empty() {
        return;
    }
    out.push_str(&format!("\n**{title}**\n\n"));
    for s in items {
        out.push_str(&format!("- {}\n", s.text));
        if !s.cli.is_empty() {
            out.push_str(&format!("\n  ```\n  {}\n  ```\n", s.cli));
        }
    }
}

/// The sources line every card and pull request ends with.
pub fn sources_line(sources: &[Cited]) -> String {
    if sources.is_empty() {
        return String::new();
    }
    let mut out = String::from("\n**Sources**\n\n");
    for s in sources {
        out.push_str(&format!(
            "- {} (read {}{})\n",
            s.url,
            s.reviewed_on,
            if s.stale {
                format!(
                    "; not re-read in over {} days: check the page before relying on it",
                    guidance::MAX_AGE_DAYS
                )
            } else {
                String::new()
            }
        ));
    }
    out
}

/// The notes as Markdown, for a pull request's description.
pub fn pr_notes_markdown(n: &PrNotes) -> String {
    let mut out = String::new();
    bullets(&mut out, "When it takes effect", &n.takes_effect);
    bullets(&mut out, "Expected interruption", &n.interruption);
    bullets(&mut out, "Replacement", &n.replacement);
    bullets(&mut out, "What the plan must show", &n.plan);
    step_list(&mut out, "Before applying", &n.prechecks);
    step_list(&mut out, "Backup", &n.backup);
    bullets(&mut out, "Rollback", &n.rollback);
    bullets(&mut out, "Known traps", &n.traps);
    step_list(&mut out, "After it is applied", &n.validation);
    bullets(&mut out, "How this repository applies changes", &n.pipeline);
    out.push_str(&sources_line(&n.sources));
    out
}

/// The runbook as Markdown (a card's long form, a notification, a doc).
pub fn markdown(rb: &Runbook) -> String {
    let mut out = String::new();
    bullets(&mut out, "Scope", &rb.scope);
    bullets(&mut out, "What it is", &rb.what);
    bullets(&mut out, "Expected interruption", &rb.interruption);
    bullets(&mut out, "You need", &rb.prerequisites);
    step_list(&mut out, "Check first", &rb.prechecks);
    step_list(&mut out, "Backup", &rb.backup);
    bullets(&mut out, "In the console", &rb.console);
    step_list(&mut out, "With the CLI", &rb.steps);
    step_list(&mut out, "Check after", &rb.validation);
    step_list(&mut out, "Rollback", &rb.rollback);
    if !rb.apply_by.is_empty() || !rb.if_nothing.is_empty() {
        let mut d = rb.if_nothing.clone();
        if !rb.apply_by.is_empty() {
            d.insert(0, format!("AWS's date for this action: {}.", rb.apply_by));
        }
        bullets(&mut out, "If nothing is done", &d);
    }
    bullets(&mut out, "Notes", &rb.notes);
    out.push_str(&sources_line(&rb.sources));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra_vuln::actions::Line;
    use crate::infra_vuln::inventory::Versions;

    const CLUSTER: &str = "arn:aws:rds:us-east-1:111122223333:cluster:orders";

    fn aurora() -> Resource {
        Resource {
            id: CLUSTER.into(),
            kind: Kind::RdsCluster,
            name: "orders".into(),
            account: "111122223333".into(),
            region: "us-east-1".into(),
            versions: Versions {
                engine: "aurora-postgresql".into(),
                engine_version: "16.4".into(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn pending(lines: &[(&str, &str, &str, &str)]) -> Vec<Pending> {
        let l: Vec<Line> = lines
            .iter()
            .map(|(arn, action, d, by)| Line {
                arn: arn.to_string(),
                action: action.to_string(),
                description: d.to_string(),
                apply_by: by.to_string(),
                writer: arn.ends_with("orders-1"),
                ..Default::default()
            })
            .collect();
        actions::reconcile("aurora-postgresql", "16.4", &l)
    }

    #[test]
    fn an_engine_patch_runbook_has_every_section_with_the_real_identifiers() {
        let p = &pending(&[(
            CLUSTER,
            "system-update",
            "Upgrade to Aurora PostgreSQL 16.4.3",
            "2026-12-01",
        )])[0];
        let rb = build(&aurora(), p, "2026-11-13");
        for (name, n) in [
            ("scope", rb.scope.len()),
            ("what", rb.what.len()),
            ("interruption", rb.interruption.len()),
            ("prerequisites", rb.prerequisites.len()),
            ("prechecks", rb.prechecks.len()),
            ("backup", rb.backup.len()),
            ("console", rb.console.len()),
            ("steps", rb.steps.len()),
            ("validation", rb.validation.len()),
            ("rollback", rb.rollback.len()),
            ("if_nothing", rb.if_nothing.len()),
            ("sources", rb.sources.len()),
        ] {
            assert!(n > 0, "the runbook has no {name}");
        }
        assert_eq!(rb.apply_by, "2026-12-01");
        assert!(rb.scope[0].contains("111122223333") && rb.scope[0].contains("us-east-1"));
        assert_eq!(
            rb.steps[0].cli,
            format!("aws rds apply-pending-maintenance-action --resource-identifier {CLUSTER} --apply-action system-update --opt-in-type next-maintenance --region us-east-1")
        );
        assert_eq!(
            rb.backup[0].cli,
            "aws rds create-db-cluster-snapshot --db-cluster-identifier orders --db-cluster-snapshot-identifier orders-before-system-update-20261113 --region us-east-1"
        );
        assert!(rb
            .what
            .iter()
            .any(|w| w.contains("patch the DB engine for Aurora PostgreSQL")));
        assert!(rb.rollback.iter().any(|s| s.cli.contains("undo-opt-in")));
        assert!(rb
            .validation
            .iter()
            .any(|s| s.text.contains("aurora_version()")));
        // Nothing left unfilled.
        let text = markdown(&rb);
        assert!(
            !regex::Regex::new(r"\{[a-z_]+\}").unwrap().is_match(&text),
            "{text}"
        );
        assert!(text.contains("(read 2026-11-13)"));
        assert!(!rb.stale);
        // The same runbook read long after the review date says so.
        let late = build(&aurora(), p, "2027-11-13");
        assert!(late.stale);
        assert!(markdown(&late).contains("not re-read in over 180 days"));
    }

    #[test]
    fn an_instance_level_os_update_is_one_command_per_instance_in_order() {
        let w = "arn:aws:rds:us-east-1:111122223333:db:orders-1";
        let rd = "arn:aws:rds:us-east-1:111122223333:db:orders-2";
        let p = &pending(&[
            (
                w,
                "system-update",
                "New Operating System patch is available",
                "",
            ),
            (
                rd,
                "system-update",
                "New Operating System patch is available",
                "",
            ),
        ])[0];
        let rb = build(&aurora(), p, "2026-11-13");
        let window: Vec<&Step> = rb
            .steps
            .iter()
            .filter(|s| s.cli.contains("next-maintenance"))
            .collect();
        assert_eq!(window.len(), 2);
        assert!(
            window[0].cli.contains("db:orders-2"),
            "the reader first: {}",
            window[0].cli
        );
        assert!(window[1].cli.contains("db:orders-1"));
        assert!(rb.scope.iter().any(|s| s.contains("in this order")));
        assert!(rb.what.iter().any(|w| w.contains("no CVE list")));
    }

    #[test]
    fn pull_request_notes_differ_by_tool() {
        let v = vars(&aurora(), None, "16.4", "16.6", "2026-11-13");
        let tf = pr_notes(
            "aurora",
            "engine_upgrade",
            "terraform",
            BTreeMap::new(),
            &v,
            "2026-11-13",
        );
        assert!(tf.attribute[0].contains("`engine_version` on the `aws_rds_cluster`"));
        assert!(tf.takes_effect[0].contains("`apply_immediately` defaults to `false`"));
        assert!(tf.never_change.contains(&"cluster_identifier".to_string()));
        assert!(tf
            .traps
            .iter()
            .any(|t| t.contains("Never set a value below the live version")));
        assert!(tf.rollback[0].contains("one-way"));
        assert!(tf.plan[0].contains("ran no plan"));
        assert!(tf.order[0].contains("Non-production first"));
        let cfn = pr_notes(
            "aurora",
            "engine_upgrade",
            "sam",
            BTreeMap::new(),
            &v,
            "2026-11-13",
        );
        assert!(cfn.takes_effect[0].contains("cannot schedule cluster modifications"));
        assert!(cfn
            .never_change
            .contains(&"DBClusterIdentifier".to_string()));
        let md = pr_notes_markdown(&tf);
        assert!(md.contains("**When it takes effect**") && md.contains("**Rollback**"));
        assert!(
            md.contains("--engine aurora-postgresql --engine-version 16.4"),
            "{md}"
        );
        // OpenSearch on CloudFormation: the replacement trap leads.
        let os = pr_notes(
            "opensearch",
            "engine_upgrade",
            "cloudformation",
            BTreeMap::new(),
            &v,
            "2026-11-13",
        );
        assert!(os.replacement[0].contains("replace the whole domain"));
    }

    #[test]
    fn services_map_to_the_guidance_store() {
        assert_eq!(service_of(Kind::RdsCluster, "aurora-mysql"), "aurora");
        // A Multi-AZ DB cluster is not Aurora and not a DB instance: no
        // guidance is borrowed for it.
        assert_eq!(service_of(Kind::RdsCluster, "postgres"), "");
        assert_eq!(service_of(Kind::RdsInstance, "mysql"), "rds");
        assert_eq!(service_of(Kind::EksCluster, ""), "");
        assert_eq!(action_of(Kind::LambdaFunction), "runtime_change");
    }
}
