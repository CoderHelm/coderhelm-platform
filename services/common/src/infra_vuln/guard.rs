//! The replacement guard: a change to infrastructure code that would
//! replace or destroy a resource that holds data is refused, with the
//! reason, unless the guidance says the change is in place.
//!
//! CoderHelm runs no plan and no change set (it holds no cloud
//! credentials), so the guard reads the change itself: each changed file
//! before and after.
//!
//! - A data resource's block, logical id or construct that is gone or
//!   renamed: the tool would destroy it or replace it.
//! - A change to an attribute the guidance store lists as replacing
//!   ([`super::guidance`], section `replacement`).
//! - A version that goes down: no service here downgrades in place, and
//!   Terraform re-creates an ElastiCache group for it.
//! - A version raised on a cluster member and not on its cluster.
//! - An OpenSearch version change through CloudFormation or CDK without
//!   the version upgrade policy, which replaces the domain.
//! - A safeguard taken away in the same change (deletion protection,
//!   `prevent_destroy`, a retaining deletion policy).
//!
//! What the guidance documents as in place (an engine version raised, a
//! node group release, a runtime) passes. The guard is a floor, not a
//! plan: the pull request still says what the plan must show.

use regex::Regex;

use super::guidance;
use super::hcl::{self, Block, Value};
use super::locate;
use super::matching::version_lt;

/// One changed file, before and after (empty when it did not exist).
#[derive(Debug, Clone, Default)]
pub struct Change<'a> {
    pub path: &'a str,
    pub before: &'a str,
    pub after: &'a str,
}

/// Terraform types that hold data, and the guidance service that lists
/// what replaces them (empty when none does).
const TF_DATA: &[(&str, &str)] = &[
    ("aws_db_instance", "rds"),
    ("aws_rds_cluster", "aurora"),
    ("aws_rds_cluster_instance", "aurora"),
    ("aws_rds_global_cluster", ""),
    ("aws_elasticache_replication_group", "elasticache"),
    ("aws_elasticache_cluster", "elasticache"),
    ("aws_opensearch_domain", "opensearch"),
    ("aws_elasticsearch_domain", "opensearch"),
    ("aws_docdb_cluster", ""),
    ("aws_neptune_cluster", ""),
    ("aws_redshift_cluster", ""),
    ("aws_msk_cluster", ""),
    ("aws_dynamodb_table", ""),
    ("aws_efs_file_system", ""),
    ("aws_ebs_volume", ""),
    ("aws_s3_bucket", ""),
];

/// CloudFormation types that hold data.
const CFN_DATA: &[(&str, &str)] = &[
    ("AWS::RDS::DBInstance", "rds"),
    ("AWS::RDS::DBCluster", "aurora"),
    ("AWS::RDS::GlobalCluster", ""),
    ("AWS::ElastiCache::ReplicationGroup", "elasticache"),
    ("AWS::ElastiCache::CacheCluster", "elasticache"),
    ("AWS::OpenSearchService::Domain", "opensearch"),
    ("AWS::Elasticsearch::Domain", "opensearch"),
    ("AWS::DocDB::DBCluster", ""),
    ("AWS::Neptune::DBCluster", ""),
    ("AWS::Redshift::Cluster", ""),
    ("AWS::MSK::Cluster", ""),
    ("AWS::DynamoDB::Table", ""),
    ("AWS::EFS::FileSystem", ""),
    ("AWS::S3::Bucket", ""),
];

/// Words in a CDK construct's class that say it holds data.
const CDK_DATA: &[&str] = &[
    "databasecluster",
    "databaseinstance",
    "serverlesscluster",
    "cfndbcluster",
    "cfndbinstance",
    "cfnreplicationgroup",
    "cfncachecluster",
    "domain",
    "table",
    "bucket",
    "filesystem",
];

const VERSION_ATTRS: &[&str] = &[
    "engine_version",
    "elasticsearch_version",
    "kafka_version",
    "cluster_version",
    "EngineVersion",
    "ElasticsearchVersion",
    "KafkaVersion",
];

fn re(p: &str) -> Regex {
    Regex::new(p).expect("a valid pattern")
}

/// The attributes the guidance lists as replacing, for a service and tool.
pub fn replacing(service: &str, tool: &str) -> Vec<&'static str> {
    guidance::store()
        .iter()
        .filter(|e| e.section == "replacement" && e.service == service && e.tool == tool)
        .flat_map(|e| e.attributes.iter().map(String::as_str))
        .collect()
}

fn lit(expr: &str) -> Option<String> {
    match hcl::value(expr) {
        Value::Lit(s) => Some(s),
        _ => None,
    }
}

fn resources(blocks: &[Block]) -> Vec<&Block> {
    blocks
        .iter()
        .filter(|b| b.kind == "resource" && b.labels.len() == 2)
        .collect()
}

fn terraform(c: &Change, out: &mut Vec<String>) {
    if c.path.ends_with(".tfvars") {
        let (b, a) = (hcl::assignments(c.before), hcl::assignments(c.after));
        for x in b.iter().filter(|x| x.name.contains("version")) {
            let Some(y) = a.iter().find(|y| y.name == x.name) else {
                continue;
            };
            if let (Some(old), Some(new)) = (lit(&x.expr), lit(&y.expr)) {
                if version_lt(&new, &old) {
                    out.push(format!(
                        "{}: `{}` goes down, from {old} to {new}. A lower version is a downgrade: the service refuses it or the tool re-creates the resource.",
                        c.path, x.name
                    ));
                }
            }
        }
        return;
    }
    let before = hcl::parse(c.before);
    let after = hcl::parse(c.after);
    let moved: Vec<String> = after
        .iter()
        .filter(|b| b.kind == "moved")
        .filter_map(|b| b.attr("from").map(|a| a.expr.trim().to_string()))
        .collect();
    let cluster_raised = resources(&before).iter().any(|b| {
        b.label(0) == "aws_rds_cluster"
            && resources(&after)
                .iter()
                .find(|a| a.labels == b.labels)
                .is_some_and(|a| {
                    a.attr("engine_version").map(|x| &x.expr)
                        != b.attr("engine_version").map(|x| &x.expr)
                })
    });
    for b in resources(&before) {
        let Some((_, service)) = TF_DATA.iter().find(|(t, _)| *t == b.label(0)) else {
            continue;
        };
        let address = format!("{}.{}", b.label(0), b.label(1));
        let Some(a) = resources(&after).into_iter().find(|a| a.labels == b.labels) else {
            if !moved.contains(&address) {
                out.push(format!(
                    "{}: `{address}` is removed or renamed with no `moved` block. Terraform would destroy the resource it manages.",
                    c.path
                ));
            }
            continue;
        };
        let changed =
            |n: &str| b.attr(n).map(|x| x.expr.trim()) != a.attr(n).map(|x| x.expr.trim());
        for n in replacing(service, "terraform") {
            if changed(n) {
                out.push(format!(
                    "{}: `{address}` changes `{n}`, which replaces the resource (a new one is created and this one is destroyed).",
                    c.path
                ));
            }
        }
        for n in ["for_each", "count"] {
            if changed(n) {
                out.push(format!(
                    "{}: `{address}` changes `{n}`: the addresses of the resources it creates change, and Terraform destroys the ones that no longer have one.",
                    c.path
                ));
            }
        }
        for n in VERSION_ATTRS {
            if let (Some(old), Some(new)) = (
                b.attr(n).and_then(|x| lit(&x.expr)),
                a.attr(n).and_then(|x| lit(&x.expr)),
            ) {
                if version_lt(&new, &old) {
                    out.push(format!(
                        "{}: `{address}` lowers `{n}` from {old} to {new}. No downgrade is done in place: the service refuses it, and for an ElastiCache replication group the provider destroys and re-creates the group.",
                        c.path
                    ));
                }
            }
        }
        if b.label(0) == "aws_rds_cluster_instance" && changed("engine_version") && !cluster_raised
        {
            out.push(format!(
                "{}: `{address}` changes `engine_version` on a cluster instance. An Aurora upgrade is made on the `aws_rds_cluster`; on the instance it does not upgrade anything.",
                c.path
            ));
        }
        let truthy = |x: Option<&hcl::Attr>| x.is_some_and(|x| x.expr.trim() == "true");
        if truthy(b.attr("deletion_protection")) && !truthy(a.attr("deletion_protection")) {
            out.push(format!(
                "{}: `{address}` turns `deletion_protection` off.",
                c.path
            ));
        }
        if !truthy(b.attr("skip_final_snapshot")) && truthy(a.attr("skip_final_snapshot")) {
            out.push(format!(
                "{}: `{address}` turns `skip_final_snapshot` on.",
                c.path
            ));
        }
        let guarded = |x: &Block| {
            x.child("lifecycle")
                .is_some_and(|l| truthy(l.attr("prevent_destroy")))
        };
        if guarded(b) && !guarded(a) {
            out.push(format!(
                "{}: `{address}` removes `prevent_destroy`.",
                c.path
            ));
        }
    }
}

/// The data resources of a template: `(logical id, type, body lines)`.
fn template_data<'a>(lines: &[&'a str]) -> Vec<(String, &'static str, &'static str, Vec<&'a str>)> {
    let key = re(r#"^\s*["']?([A-Za-z0-9]+)["']?\s*:\s*\{?\s*$"#);
    let mut out = Vec::new();
    for l in lines {
        let Some(c) = key.captures(l) else {
            continue;
        };
        for (t, service) in CFN_DATA {
            if let Some((_, body)) = locate::template_resource(lines, &c[1], &[t]) {
                // The resource's own `Type`: a direct child of its key,
                // not the type of something nested further down.
                let depth = |x: &str| x.len() - x.trim_start().len();
                let child = body
                    .iter()
                    .filter(|(_, x)| !x.trim().is_empty())
                    .map(|(_, x)| depth(x))
                    .min();
                let first_type = body
                    .iter()
                    .find(|(_, x)| {
                        Some(depth(x)) == child
                            && x.trim_start()
                                .trim_start_matches(['"', '\''])
                                .starts_with("Type")
                    })
                    .is_some_and(|(_, x)| x.contains(t));
                if first_type
                    && !out
                        .iter()
                        .any(|(id, _, _, _): &(String, _, _, _)| id == &c[1])
                {
                    out.push((
                        c[1].to_string(),
                        *t,
                        *service,
                        body.iter().map(|(_, x)| *x).collect(),
                    ));
                }
            }
        }
    }
    out
}

fn property(body: &[&str], name: &str) -> Option<String> {
    let p = re(&format!(
        r#"^\s*["']?{}["']?\s*:\s*(.*)$"#,
        regex::escape(name)
    ));
    body.iter().find_map(|l| {
        p.captures(l).map(|c| {
            c[1].trim()
                .trim_end_matches(',')
                .trim_matches(|x| x == '"' || x == '\'')
                .to_string()
        })
    })
}

fn template(c: &Change, out: &mut Vec<String>) {
    let (bl, al): (Vec<&str>, Vec<&str>) = (c.before.lines().collect(), c.after.lines().collect());
    let after = template_data(&al);
    for (id, typ, service, body) in template_data(&bl) {
        let Some((_, _, _, new)) = after.iter().find(|(a, t, _, _)| a == &id && *t == typ) else {
            out.push(format!(
                "{}: the {typ} `{id}` is removed, renamed or changes type. CloudFormation would delete it, or replace it with a new empty one.",
                c.path
            ));
            continue;
        };
        for n in replacing(service, "cloudformation") {
            if property(&body, n) != property(new, n) {
                out.push(format!(
                    "{}: `{id}` changes `{n}`, a replacement property: CloudFormation creates a new resource and deletes this one.",
                    c.path
                ));
            }
        }
        for n in VERSION_ATTRS {
            if let (Some(old), Some(now)) = (property(&body, n), property(new, n)) {
                let numeric = |s: &str| {
                    s.trim_start_matches(|x: char| !x.is_ascii_digit())
                        .to_string()
                };
                if old != now
                    && !numeric(&old).is_empty()
                    && !numeric(&now).is_empty()
                    && version_lt(&numeric(&now), &numeric(&old))
                {
                    out.push(format!(
                        "{}: `{id}` lowers `{n}` from {old} to {now}. No downgrade is done in place.",
                        c.path
                    ));
                }
                let policy = property(new, "EnableVersionUpgrade").is_some_and(|v| v == "true");
                if service == "opensearch" && old != now && !policy {
                    out.push(format!(
                        "{}: `{id}` changes `{n}` without `UpdatePolicy: EnableVersionUpgrade: true`. Without the policy CloudFormation replaces the whole domain with a new empty one.",
                        c.path
                    ));
                }
            }
        }
        for n in ["DeletionPolicy", "UpdateReplacePolicy"] {
            let keeps = |v: &Option<String>| {
                v.as_deref()
                    .is_some_and(|v| v == "Retain" || v == "Snapshot")
            };
            if keeps(&property(&body, n)) && !keeps(&property(new, n)) {
                out.push(format!(
                    "{}: `{id}` no longer keeps its data on `{n}`.",
                    c.path
                ));
            }
        }
        if property(&body, "DeletionProtection").as_deref() == Some("true")
            && property(new, "DeletionProtection").as_deref() != Some("true")
        {
            out.push(format!(
                "{}: `{id}` turns `DeletionProtection` off.",
                c.path
            ));
        }
    }
}

fn cdk(c: &Change, out: &mut Vec<String>) {
    let data = |content: &str| -> Vec<(String, String)> {
        locate::constructs(content)
            .into_iter()
            .filter(|k| {
                let class = k
                    .class
                    .rsplit('.')
                    .next()
                    .unwrap_or("")
                    .to_ascii_lowercase();
                CDK_DATA.iter().any(|w| class.contains(w))
            })
            .map(|k| (k.class, k.id))
            .collect()
    };
    let after = data(c.after);
    for (class, id) in data(c.before) {
        if !after.iter().any(|(_, a)| a == &id) {
            out.push(format!(
                "{}: the construct `{id}` ({class}) is removed or its id changes. A changed construct id changes the logical id, and CloudFormation replaces the resource.",
                c.path
            ));
        }
    }
    let os_version = re(r"EngineVersion\.[A-Z_0-9]+(?:\([^)]*\))?");
    let versions = |s: &str| -> Vec<String> {
        os_version
            .find_iter(s)
            .map(|m| m.as_str().to_string())
            .collect()
    };
    let domain = c.after.contains("opensearchservice") || c.after.contains("opensearch.Domain");
    if domain
        && versions(c.before)
            .iter()
            .filter(|v| {
                v.contains("OPENSEARCH") || v.contains("ELASTICSEARCH") || v.contains("openSearch")
            })
            .collect::<Vec<_>>()
            != versions(c.after)
                .iter()
                .filter(|v| {
                    v.contains("OPENSEARCH")
                        || v.contains("ELASTICSEARCH")
                        || v.contains("openSearch")
                })
                .collect::<Vec<_>>()
        && !re(r"enableVersionUpgrade\s*[:=]\s*(?:true|True)").is_match(c.after)
    {
        out.push(format!(
            "{}: the OpenSearch domain's version changes without `enableVersionUpgrade: true`. It defaults to false, and without it the deploy replaces the whole domain with a new empty one.",
            c.path
        ));
    }
    let destroys = |s: &str| s.matches("RemovalPolicy.DESTROY").count();
    if destroys(c.after) > destroys(c.before) && !after.is_empty() {
        out.push(format!(
            "{}: a `RemovalPolicy.DESTROY` is added in a file that defines a data resource.",
            c.path
        ));
    }
}

/// Why a change must not be opened as a pull request: one line per
/// reason, empty when the guard has no objection.
pub fn refusals(changes: &[Change]) -> Vec<String> {
    let mut out = Vec::new();
    for c in changes {
        // A file that is new cannot replace anything.
        if c.before.is_empty() {
            continue;
        }
        let tool = locate::tool_of(c.path, c.before).or_else(|| locate::tool_of(c.path, c.after));
        match tool {
            Some("terraform" | "opentofu") => terraform(c, &mut out),
            Some("cloudformation" | "sam" | "serverless") => template(c, &mut out),
            Some("cdk") => cdk(c, &mut out),
            _ => {}
        }
    }
    out.dedup();
    out
}

/// The refusal as a card or a closed pull request says it.
pub fn explain(reasons: &[String]) -> String {
    let mut out = String::from(
        "CoderHelm refused this change: it would replace or destroy a resource that holds data, or take away what protects it.\n\n",
    );
    for r in reasons {
        out.push_str(&format!("- {r}\n"));
    }
    out.push_str("\nNothing was merged or applied. The update is still open; apply it with the runbook on the finding, or change the code by hand with a plan reviewed first.");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tf(before: &str, after: &str) -> Vec<String> {
        refusals(&[Change {
            path: "db/main.tf",
            before,
            after,
        }])
    }

    const DB: &str = r#"
resource "aws_db_instance" "orders" {
  identifier          = "orders-prod"
  engine              = "postgres"
  engine_version      = "16.4"
  deletion_protection = true
  lifecycle {
    prevent_destroy = true
  }
}
"#;

    #[test]
    fn raising_the_engine_version_in_place_passes() {
        assert_eq!(tf(DB, &DB.replace("16.4", "16.6")), Vec::<String>::new());
        // A new file replaces nothing.
        assert!(refusals(&[Change {
            path: "new.tf",
            before: "",
            after: DB
        }])
        .is_empty());
    }

    #[test]
    fn a_change_that_replaces_or_destroys_the_database_is_refused_with_the_reason() {
        let r = tf(DB, &DB.replace("orders-prod", "orders-production"));
        assert_eq!(r.len(), 1, "{r:?}");
        assert!(
            r[0].contains("changes `identifier`, which replaces the resource"),
            "{r:?}"
        );
        let r = tf(DB, &DB.replace("\"orders\"", "\"orders_db\""));
        assert!(
            r[0].contains("removed or renamed with no `moved` block"),
            "{r:?}"
        );
        // A rename with a moved block keeps the resource.
        let moved = format!(
            "{}\nmoved {{\n  from = aws_db_instance.orders\n  to   = aws_db_instance.orders_db\n}}\n",
            DB.replace("\"orders\"", "\"orders_db\"")
        );
        assert!(tf(DB, &moved).is_empty(), "{:?}", tf(DB, &moved));
        let r = tf(DB, &DB.replace("16.4", "15.8"));
        assert!(
            r[0].contains("lowers `engine_version` from 16.4 to 15.8"),
            "{r:?}"
        );
        let r = tf(
            DB,
            &DB.replace("deletion_protection = true", "deletion_protection = false"),
        );
        assert!(r[0].contains("turns `deletion_protection` off"), "{r:?}");
        let r = tf(
            DB,
            &DB.replace("prevent_destroy = true", "prevent_destroy = false"),
        );
        assert!(r[0].contains("removes `prevent_destroy`"), "{r:?}");
        let r = tf(DB, "");
        assert!(r[0].contains("Terraform would destroy"), "{r:?}");
        assert!(explain(&r).contains("Nothing was merged or applied"));
        // A variable file that lowers a version.
        let r = refusals(&[Change {
            path: "envs/prod.tfvars",
            before: "db_engine_version = \"16.6\"\n",
            after: "db_engine_version = \"16.4\"\n",
        }]);
        assert!(r[0].contains("goes down"), "{r:?}");
    }

    #[test]
    fn an_aurora_version_is_raised_on_the_cluster_never_on_a_member() {
        let both = r#"
resource "aws_rds_cluster" "ledger" {
  cluster_identifier = "ledger"
  engine_version     = "16.4"
}
resource "aws_rds_cluster_instance" "ledger" {
  cluster_identifier = aws_rds_cluster.ledger.id
  engine_version     = "16.4"
}
"#;
        let member_only = both.replacen(
            "  engine_version     = \"16.4\"\n}\n",
            "  engine_version     = \"16.6\"\n}\n",
            2,
        );
        // replacen(2) changed both: that passes.
        assert!(
            tf(both, &member_only).is_empty(),
            "{:?}",
            tf(both, &member_only)
        );
        let at = both.rfind("16.4").unwrap();
        let just_member = format!("{}16.6{}", &both[..at], &both[at + 4..]);
        let r = tf(both, &just_member);
        assert!(r[0].contains("on a cluster instance"), "{r:?}");
        // The cluster's identifier is a replacing attribute.
        let r = tf(
            both,
            &both.replace(
                "\"ledger\"\n  engine_version",
                "\"ledger2\"\n  engine_version",
            ),
        );
        assert!(
            r.iter().any(|x| x.contains("`cluster_identifier`")),
            "{r:?}"
        );
    }

    const DOMAIN: &str = r#"AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Search:
    Type: AWS::OpenSearchService::Domain
    Properties:
      DomainName: search-prod
      EngineVersion: OpenSearch_2.11
"#;

    fn cfn(before: &str, after: &str) -> Vec<String> {
        refusals(&[Change {
            path: "search.yaml",
            before,
            after,
        }])
    }

    #[test]
    fn an_opensearch_version_change_needs_the_upgrade_policy() {
        let bumped = DOMAIN.replace("2.11", "2.19");
        let r = cfn(DOMAIN, &bumped);
        assert_eq!(r.len(), 1, "{r:?}");
        assert!(r[0].contains("replaces the whole domain"), "{r:?}");
        // With the policy in the same change, the guidance says it is in place.
        let safe = bumped.replace(
            "    Type: AWS::OpenSearchService::Domain\n",
            "    Type: AWS::OpenSearchService::Domain\n    UpdatePolicy:\n      EnableVersionUpgrade: true\n",
        );
        assert!(cfn(DOMAIN, &safe).is_empty(), "{:?}", cfn(DOMAIN, &safe));
        let r = cfn(DOMAIN, &DOMAIN.replace("search-prod", "search"));
        assert!(
            r[0].contains("`DomainName`, a replacement property"),
            "{r:?}"
        );
        let r = cfn(DOMAIN, &DOMAIN.replace("  Search:", "  SearchV2:"));
        assert!(r[0].contains("removed, renamed or changes type"), "{r:?}");
    }

    #[test]
    fn a_database_template_keeps_its_identifier_policy_and_version_direction() {
        let t = "Resources:\n  Db:\n    Type: AWS::RDS::DBInstance\n    DeletionPolicy: Snapshot\n    Properties:\n      DBInstanceIdentifier: orders\n      EngineVersion: \"16.4\"\n      DeletionProtection: true\n";
        assert!(cfn(t, &t.replace("16.4", "16.6")).is_empty());
        assert!(cfn(t, &t.replace("16.4", "15.8"))[0].contains("lowers `EngineVersion`"));
        assert!(cfn(
            t,
            &t.replace(
                "DBInstanceIdentifier: orders",
                "DBInstanceIdentifier: orders2"
            )
        )[0]
        .contains("replacement property"));
        assert!(cfn(
            t,
            &t.replace("DeletionPolicy: Snapshot", "DeletionPolicy: Delete")
        )[0]
        .contains("no longer keeps its data"));
        assert!(cfn(
            t,
            &t.replace("DeletionProtection: true", "DeletionProtection: false")
        )[0]
        .contains("turns `DeletionProtection` off"));
    }

    #[test]
    fn a_cdk_construct_id_is_never_changed() {
        let a = "import * as rds from 'aws-cdk-lib/aws-rds';\nnew rds.DatabaseCluster(this, 'Ledger', { engine: rds.DatabaseClusterEngine.auroraPostgres({ version: rds.AuroraPostgresEngineVersion.VER_16_4 }) });\n";
        let cdk = |after: &str| {
            refusals(&[Change {
                path: "src/data.ts",
                before: a,
                after,
            }])
        };
        assert!(cdk(&a.replace("VER_16_4", "VER_16_6")).is_empty());
        let r = cdk(&a.replace("'Ledger'", "'LedgerV2'"));
        assert!(
            r[0].contains("CloudFormation replaces the resource"),
            "{r:?}"
        );
        let os = "import * as opensearch from 'aws-cdk-lib/aws-opensearchservice';\nnew opensearch.Domain(this, 'Search', { version: opensearch.EngineVersion.OPENSEARCH_2_11 });\n";
        let r = refusals(&[Change {
            path: "src/search.ts",
            before: os,
            after: &os.replace("2_11", "2_19"),
        }]);
        assert!(
            r[0].contains("without `enableVersionUpgrade: true`"),
            "{r:?}"
        );
        let safe = os.replace("2_11 }", "2_19, enableVersionUpgrade: true }");
        assert!(refusals(&[Change {
            path: "src/search.ts",
            before: os,
            after: &safe
        }])
        .is_empty());
    }

    #[test]
    fn the_replacing_attributes_come_from_the_guidance_store() {
        assert!(replacing("rds", "terraform").contains(&"identifier"));
        assert!(replacing("aurora", "cloudformation").contains(&"DBClusterIdentifier"));
        assert!(replacing("opensearch", "cloudformation").contains(&"DomainName"));
        assert!(!replacing("rds", "terraform").contains(&"engine_version"));
    }
}
