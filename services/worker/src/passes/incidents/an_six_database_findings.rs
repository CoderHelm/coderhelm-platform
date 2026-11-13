//! an. Security, Infrastructure showed six database findings for one
//! account. Every one was a runbook with one command and a console path;
//! none had a pull request, although the databases were defined in the
//! team's code. The same CVE stood on all six: on an operating system
//! update, on an Aurora PostgreSQL patch and on a MySQL upgrade. A
//! database Terraform had named (`identifier_prefix` plus its generated
//! suffix) was "not found in code" because its live name is written
//! nowhere. An Aurora cluster defined through a module was not found
//! either. On one cluster a patch and an upgrade were pending at once, as
//! two unrelated rows; an operating system update pending on both of its
//! instances was one row and the other was dropped. An account with more
//! than a hundred databases was read one page deep.
//!
//! Guarantee: an advisory is attached only where it applies (service,
//! engine, version range, kind of action, date), and an action AWS
//! publishes with no CVE list gets none. An action is named for what it
//! means on its ARN. The actions pending on one database are one ordered
//! set of rows, each naming every ARN it applies to. The code that defines
//! a resource is found by what AWS and the code say (stack records,
//! resource type and attributes, module calls, variable files), graded,
//! and a guess never becomes a pull request. An update with a code
//! equivalent is a pull request against the line that sets the value; one
//! that cannot be expressed in code stays a runbook and says so; a runbook
//! is complete. A change that would replace a database is refused.

use std::collections::BTreeMap;

use common::infra_vuln::actions::Meaning;
use common::infra_vuln::feeds::{Bulletin, ServiceStatus, Who};
use common::infra_vuln::inventory::{Confidence, Kind, Resource};
use common::infra_vuln::locate::{self, SourceFile};
use common::infra_vuln::matching::{self, Feeds, FixKind, InfraFinding};
use common::infra_vuln::raw::{RawAccount, RawDatabase, RawPending};
use common::infra_vuln::records::{self, Record};

use crate::passes::infra_findings;

const ACCOUNT: &str = "111122223333";
const REGION: &str = "us-east-1";
const CVE: &str = "CVE-2026-55501";
/// A name Terraform generated: `identifier_prefix` and 26 characters.
const ORDERS: &str = "orders-20240311094512345600000001";
const INFRA: &str = "acme/platform-infra";

/// What the old cards said, whatever the action.
const OLD_SUMMARY: &str = "Apply system update in AWS";
const OLD_CONSOLE: &str = "Maintenance & backups > Pending maintenance > Apply";

fn arn(kind: &str, name: &str) -> String {
    format!("arn:aws:rds:{REGION}:{ACCOUNT}:{kind}:{name}")
}

fn tags(t: &[(&str, &str)]) -> Vec<(String, String)> {
    t.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn cluster(name: &str, engine: &str, version: &str) -> RawDatabase {
    RawDatabase {
        id: name.into(),
        arn: arn("cluster", name),
        engine: engine.into(),
        engine_version: version.into(),
        ..Default::default()
    }
}

fn instance(name: &str, engine: &str, version: &str, of: &str, t: &[(&str, &str)]) -> RawDatabase {
    RawDatabase {
        id: name.into(),
        arn: arn("db", name),
        engine: engine.into(),
        engine_version: version.into(),
        cluster: of.into(),
        tags: tags(t),
        ..Default::default()
    }
}

fn pending(on: &str, action: &str, description: &str, forced: &str) -> RawPending {
    RawPending {
        resource_arn: on.into(),
        action: action.into(),
        description: description.into(),
        forced_apply: forced.into(),
        ..Default::default()
    }
}

/// The account as one read returns it: six things pending, and more
/// databases than one page of a describe call holds.
fn account() -> RawAccount {
    let mut writer = instance(
        "ledger-prod-aurora-1",
        "aurora-postgresql",
        "16.4",
        "ledger-prod-aurora",
        &[],
    );
    writer.writer = true;
    let reader = instance(
        "ledger-prod-aurora-2",
        "aurora-postgresql",
        "16.4",
        "ledger-prod-aurora",
        &[],
    );
    let mut db_instances = vec![
        writer,
        reader,
        instance(ORDERS, "postgres", "16.4", "", &[("Team", "payments")]),
        instance(
            "billing-prod",
            "mysql",
            "8.0.36",
            "",
            &[
                ("aws:cloudformation:stack-name", "billing-prod"),
                ("aws:cloudformation:logical-id", "BillingDb"),
            ],
        ),
        instance("reports-prod", "mysql", "8.0.36", "", &[]),
    ];
    // Past the first hundred: quiet databases with nothing pending.
    for i in 0..120 {
        db_instances.push(instance(
            &format!("quiet-{i:03}"),
            "postgres",
            "16.6",
            "",
            &[],
        ));
    }
    let ledger = arn("cluster", "ledger-prod-aurora");
    RawAccount {
        account: ACCOUNT.into(),
        region: REGION.into(),
        read_at: "2026-11-13T09:00:00Z".into(),
        db_clusters: vec![cluster("ledger-prod-aurora", "aurora-postgresql", "16.4")],
        db_instances,
        pending: vec![
            // One cluster, a patch and an upgrade at once.
            pending(
                &ledger,
                "system-update",
                "Upgrade to Aurora PostgreSQL 16.4.3",
                "",
            ),
            pending(
                &ledger,
                "db-upgrade",
                "Upgrade to Aurora PostgreSQL 16.6",
                "",
            ),
            // The same action name on its instances: the operating system.
            pending(
                &arn("db", "ledger-prod-aurora-1"),
                "system-update",
                "New Operating System patch is available",
                "",
            ),
            pending(
                &arn("db", "ledger-prod-aurora-2"),
                "system-update",
                "New Operating System patch is available",
                "",
            ),
            pending(
                &arn("db", ORDERS),
                "db-upgrade",
                "Upgrade to 16.6",
                "2027-01-20T00:00:00Z",
            ),
            pending(
                &arn("db", "billing-prod"),
                "system-update",
                "New Operating System update is available",
                "2026-12-05T00:00:00Z",
            ),
            pending(
                &arn("db", "reports-prod"),
                "db-upgrade",
                "Upgrade to 8.0.40",
                "2026-12-15T00:00:00Z",
            ),
        ],
        ..Default::default()
    }
}

fn file(repo: &str, path: &str, content: &str) -> SourceFile {
    SourceFile {
        repo: repo.into(),
        path: path.into(),
        content: content.into(),
    }
}

const AURORA_MODULE: &str = r#"variable "name" {}
variable "engine_version" {}

resource "aws_rds_cluster" "this" {
  cluster_identifier = "${var.name}-aurora"
  engine             = "aurora-postgresql"
  engine_version     = var.engine_version
}

resource "aws_rds_cluster_instance" "this" {
  count              = 2
  identifier         = "${var.name}-aurora-${count.index + 1}"
  cluster_identifier = aws_rds_cluster.this.id
  engine             = aws_rds_cluster.this.engine
  engine_version     = aws_rds_cluster.this.engine_version
}
"#;

const PROD: &str = r#"module "ledger" {
  source         = "../../modules/aurora"
  name           = "ledger-prod"
  engine_version = "16.4"
}
"#;

const ORDERS_TF: &str = r#"resource "aws_db_instance" "orders" {
  identifier_prefix = "orders-"
  engine            = "postgres"
  engine_version    = "16.4"
  instance_class    = "db.r6g.large"
  tags = {
    Team = "payments"
  }
}
"#;

const BILLING_TEMPLATE: &str = r#"AWSTemplateFormatVersion: "2010-09-09"
Resources:
  BillingDb:
    Type: AWS::RDS::DBInstance
    DeletionPolicy: Snapshot
    Properties:
      Engine: mysql
      EngineVersion: "8.0.36"
"#;

/// The team's repositories: a Terraform repository (a module, its caller
/// per environment, a root with a generated identifier) and an
/// application repository with its CloudFormation template at a neutral
/// path.
fn code() -> Vec<SourceFile> {
    vec![
        file(INFRA, "modules/aurora/main.tf", AURORA_MODULE),
        file(INFRA, "envs/prod/main.tf", PROD),
        file(INFRA, "db/orders.tf", ORDERS_TF),
        file("acme/billing", "deploy/stack.yaml", BILLING_TEMPLATE),
        file("acme/billing", "scripts/deploy.sh", "aws cloudformation deploy --stack-name billing-prod --template-file deploy/stack.yaml\n"),
    ]
}

/// One bulletin: RDS customers act, and the fix is RDS for MySQL 8.0.40.
fn bulletin() -> Bulletin {
    Bulletin {
        id: "2026-171-AWS".into(),
        title: "Issue with RDS for MySQL".into(),
        url: "https://aws.amazon.com/security/security-bulletins/2026-171-AWS/".into(),
        severity: "important".into(),
        published: "2026-11-02".into(),
        updated: "2026-11-02".into(),
        cves: vec![CVE.into()],
        services: vec![ServiceStatus {
            service: "rds".into(),
            heading: "RDS/Aurora".into(),
            who: Who::CustomerInAws,
            reason: "Customers should upgrade to RDS for MySQL version 8.0.40 or later, which is now available.".into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

struct Scene {
    resources: Vec<Resource>,
    findings: Vec<InfraFinding>,
}

fn scene() -> Scene {
    let raw = account();
    let mut resources = raw.resources();
    let repos = vec![INFRA.to_string(), "acme/billing".to_string()];
    let files = code();
    for r in resources.iter_mut() {
        r.defined_in = locate::locate(r, &repos, &files, &[]);
    }
    let b = bulletin();
    let (kev, latest) = (BTreeMap::new(), BTreeMap::new());
    let findings = matching::findings(
        &resources,
        &Feeds {
            advisories: &[],
            bulletins: std::slice::from_ref(&b),
            runtimes: &[],
            latest: &[],
            kev: &kev,
            latest_al2023: &latest,
            today: "2026-11-13",
        },
    );
    Scene {
        resources,
        findings,
    }
}

fn find<'a>(s: &'a Scene, name: &str, meaning: Meaning) -> &'a InfraFinding {
    s.findings
        .iter()
        .find(|f| f.name == name && f.fix.meaning == Some(meaning))
        .unwrap_or_else(|| panic!("no {meaning:?} finding for {name}"))
}

#[test]
fn every_page_of_databases_is_kept_and_six_things_are_pending() {
    let s = scene();
    // 1 cluster and 123 instances outside it: nothing past the first
    // hundred is lost on the way to resources.
    let dbs = s
        .resources
        .iter()
        .filter(|r| matches!(r.kind, Kind::RdsCluster | Kind::RdsInstance))
        .count();
    assert_eq!(dbs, 124);
    let titles: Vec<&str> = s.findings.iter().map(|f| f.title.as_str()).collect();
    assert_eq!(s.findings.len(), 6, "{titles:#?}");
    let mut ids: Vec<&str> = s.findings.iter().map(|f| f.id.as_str()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 6, "each row is its own finding");
    for f in &s.findings {
        assert_ne!(f.fix.summary, OLD_SUMMARY, "{}", f.title);
        assert!(!f.fix.console.contains(OLD_CONSOLE), "{}", f.title);
        assert!(!f.fix.decision.is_empty(), "{} does not say why", f.title);
    }
}

#[test]
fn the_cve_stands_only_on_the_upgrade_the_bulletin_speaks_of() {
    let s = scene();
    let with: Vec<&InfraFinding> = s.findings.iter().filter(|f| !f.cves.is_empty()).collect();
    assert_eq!(with.len(), 1, "the old cards put {CVE} on all six");
    assert_eq!(with[0].name, "reports-prod");
    assert_eq!(with[0].cves, vec![CVE]);
    // The operating system updates: no CVE, and the card says AWS
    // publishes none.
    for name in ["billing-prod", "ledger-prod-aurora"] {
        let os = find(&s, name, Meaning::OsUpdate);
        assert!(os.cves.is_empty());
        assert!(!os.sources.iter().any(|x| x.kind == "bulletin"), "{name}");
        assert!(
            os.fix.advisory_note.contains("no CVE list"),
            "{}",
            os.fix.advisory_note
        );
    }
    // The Aurora PostgreSQL patch and upgrade: the bulletin names MySQL.
    assert!(find(&s, "ledger-prod-aurora", Meaning::EnginePatch)
        .cves
        .is_empty());
    assert!(find(&s, "ledger-prod-aurora", Meaning::EngineUpgrade)
        .cves
        .is_empty());
    assert!(find(&s, ORDERS, Meaning::EngineUpgrade).cves.is_empty());
}

#[test]
fn the_cluster_s_three_actions_are_named_for_what_they_mean_and_ordered() {
    let s = scene();
    let up = find(&s, "ledger-prod-aurora", Meaning::EngineUpgrade);
    let patch = find(&s, "ledger-prod-aurora", Meaning::EnginePatch);
    let os = find(&s, "ledger-prod-aurora", Meaning::OsUpdate);
    // `system-update` on the cluster ARN is an engine patch; on the
    // instance ARNs it is the operating system.
    assert!(
        patch.title.contains("engine patch to 16.4.3"),
        "{}",
        patch.title
    );
    assert!(os.title.contains("operating system update"), "{}", os.title);
    let rb = os.fix.runbook.as_ref().unwrap();
    // One row for both instances, each with its command, the reader first.
    let cmds: Vec<&str> = rb
        .steps
        .iter()
        .filter(|x| x.cli.contains("next-maintenance"))
        .map(|x| x.cli.as_str())
        .collect();
    assert_eq!(cmds.len(), 2, "{cmds:#?}");
    assert!(cmds[0].contains(":db:ledger-prod-aurora-2 "), "{}", cmds[0]);
    assert!(cmds[1].contains(":db:ledger-prod-aurora-1 "), "{}", cmds[1]);
    assert!(patch.fix.runbook.as_ref().unwrap().steps[0]
        .cli
        .contains(":cluster:ledger-prod-aurora --apply-action system-update"));
    // An order, and what is not known said plainly.
    assert!(up.fix.order[0].starts_with("1 of 3"), "{:?}", up.fix.order);
    assert!(
        patch.fix.order[0].starts_with("2 of 3"),
        "{:?}",
        patch.fix.order
    );
    assert!(os.fix.order[0].starts_with("3 of 3"), "{:?}", os.fix.order);
    assert!(patch.fix.order[0].contains("AWS does not say whether this patch is still offered"));
}

#[test]
fn a_cluster_defined_through_a_module_gets_a_pull_request_on_the_caller_s_input() {
    let s = scene();
    let up = find(&s, "ledger-prod-aurora", Meaning::EngineUpgrade);
    assert_eq!(up.fix.kind, Some(FixKind::Pr));
    let l = up.fix.location.as_ref().unwrap();
    assert_eq!(
        (l.repo.as_str(), l.how.as_str()),
        (INFRA, "terraform_module")
    );
    assert_eq!(l.address, "module.ledger.aws_rds_cluster.this");
    assert_eq!(l.confidence, Confidence::High);
    assert_eq!(l.environment, "production");
    let e = l.edit.as_ref().unwrap();
    assert_eq!(
        (
            e.file.as_str(),
            e.line,
            e.attribute.as_str(),
            e.via.as_str()
        ),
        ("envs/prod/main.tf", 4, "engine_version", "module_input")
    );
    let record = Record::new(up.clone(), "2026-11-13T09:00:00Z");
    let unit = infra_findings::unit_for(&record).expect("a fix unit");
    assert_eq!(unit.repo, INFRA);
    let ticket = records::ticket_body(&record);
    assert!(
        ticket.contains("The value to change: `engine_version` in `envs/prod/main.tf`, line 4"),
        "{ticket}"
    );
    assert!(
        ticket.contains("Never raise it on an `aws_rds_cluster_instance`"),
        "{ticket}"
    );
    assert!(ticket.contains("Never run an apply"), "{ticket}");
    let body = records::pr_body(
        &record,
        "Raised the ledger cluster's engine version to 16.6.",
    );
    for part in [
        "## Summary",
        "## Changes",
        "## Rollout",
        "`apply_immediately` defaults to `false`",
        "Upgrades are one-way",
        "Never set a value below the live version",
        "CoderHelm ran no plan",
        "(read 2026-11-13)",
    ] {
        assert!(
            body.contains(part),
            "the description has no `{part}`:\n{body}"
        );
    }
    // The change the ticket asks for passes the guard; a change that
    // would replace the cluster is refused, with the reason.
    let raised = vec![(
        "envs/prod/main.tf".to_string(),
        PROD.to_string(),
        PROD.replace("16.4", "16.6"),
    )];
    assert!(infra_findings::guard_refusals(&raised).is_empty());
    let replaced = vec![(
        "modules/aurora/main.tf".to_string(),
        AURORA_MODULE.to_string(),
        AURORA_MODULE.replace(
            "\"${var.name}-aurora\"\n  engine  ",
            "\"${var.name}-db\"\n  engine  ",
        ),
    )];
    let why = infra_findings::guard_refusals(&replaced);
    assert!(
        why.iter()
            .any(|x| x.contains("`cluster_identifier`, which replaces the resource")),
        "{why:?}"
    );
}

#[test]
fn a_generated_identifier_is_found_by_its_prefix_and_settings() {
    let s = scene();
    let up = find(&s, ORDERS, Meaning::EngineUpgrade);
    assert_eq!(up.fix.kind, Some(FixKind::Pr));
    let l = up.fix.location.as_ref().unwrap();
    assert_eq!(l.address, "aws_db_instance.orders");
    assert_eq!(
        (l.how.as_str(), l.confidence),
        ("terraform_generated_name", Confidence::High)
    );
    assert_eq!(l.edit.as_ref().unwrap().line, 4);
    // The live name is written nowhere in the team's code.
    assert!(code().iter().all(|f| !f.content.contains(ORDERS)));
    assert_eq!(
        up.fix.summary,
        "Raise `engine_version` to 16.6 in db/orders.tf"
    );
    assert_eq!(up.fix.apply_by, "2027-01-20");
}

#[test]
fn what_code_cannot_do_or_does_not_hold_is_a_complete_runbook_that_says_why() {
    let s = scene();
    // In a CloudFormation template, found by its stack tags, and still a
    // runbook: an operating system update cannot be expressed in code.
    let billing = find(&s, "billing-prod", Meaning::OsUpdate);
    assert_eq!(billing.fix.kind, Some(FixKind::Runbook));
    let l = billing.fix.location.as_ref().unwrap();
    assert_eq!(
        (l.how.as_str(), l.file.as_str(), l.address.as_str()),
        ("cloudformation_tag", "deploy/stack.yaml", "BillingDb")
    );
    assert_eq!(l.confidence, Confidence::High);
    assert!(
        billing.fix.decision.contains("cannot be expressed in code"),
        "{}",
        billing.fix.decision
    );
    assert!(
        billing.fix.decision.contains("deploy/stack.yaml"),
        "{}",
        billing.fix.decision
    );
    assert!(infra_findings::unit_for(&Record::new(billing.clone(), "t")).is_none());
    // Not in any connected repository: a runbook, with everything a
    // person needs and the real identifiers.
    let reports = find(&s, "reports-prod", Meaning::EngineUpgrade);
    assert_eq!(reports.fix.kind, Some(FixKind::Runbook));
    assert!(reports
        .fix
        .decision
        .contains("did not find this resource in any connected repository"));
    let rb = reports.fix.runbook.as_ref().unwrap();
    for (part, n) in [
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
        assert!(n > 0, "the runbook has no {part}");
    }
    assert_eq!(rb.apply_by, "2026-12-15");
    assert!(rb.scope[0].contains(ACCOUNT) && rb.scope[0].contains(REGION));
    assert_eq!(
        rb.steps[0].cli,
        format!("aws rds modify-db-instance --db-instance-identifier reports-prod --engine-version 8.0.40 --no-apply-immediately --region {REGION}")
    );
    assert!(rb.backup[0]
        .cli
        .contains("create-db-snapshot --db-instance-identifier reports-prod"));
    assert!(rb.rollback[0].text.contains("one-way"));
    assert!(reports.fix.code_note.contains("raise its pinned version"));
    // CoderHelm never runs it: no fix unit.
    assert!(infra_findings::unit_for(&Record::new(reports.clone(), "t")).is_none());
}
