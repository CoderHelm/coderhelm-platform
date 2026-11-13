//! The read-only `CoderHelmLogReader` role teams create in their own AWS
//! accounts: its templates (built into every release), the Launch Stack
//! link, and what each ability of the role needs.
//!
//! The role trusts the install's own AWS account
//! ([`crate::public_urls::platform_account_id`]) on the condition of the
//! team's External ID, and only roles of the install itself
//! ([`trusted_role_prefix`]). CloudFormation's Launch Stack links only open
//! templates stored in S3, so the link points at a public copy
//! ([`crate::public_urls::log_reader_template_url`]): a self-hosted release
//! links to the copy published with that release
//! (`launch/<version>/coderhelm-log-reader.yaml`), byte for byte the
//! template below. The same template can be downloaded from the install,
//! with the install's account filled in, for teams that upload it by hand.

/// The role's template, as built into this release.
pub const ROLE_TEMPLATE: &str = include_str!("../../../infra/cfn/coderhelm-log-reader.yaml");
/// The add-on stack that brings an existing role up to date: every
/// permission of [`ABILITIES`], as one more policy on the role.
pub const UPDATE_TEMPLATE: &str =
    include_str!("../../../infra/cfn/coderhelm-log-reader-update.yaml");

pub const ROLE_NAME: &str = "CoderHelmLogReader";
pub const ROLE_STACK: &str = "CoderHelmLogReader";
/// Update stacks are `CoderHelmLogReaderUpdate-<permissions id>`, one per
/// set of permissions, so each release's update is a new stack (a Launch
/// Stack link can only create one) and never collides with an older one.
pub const UPDATE_STACK_PREFIX: &str = "CoderHelmLogReaderUpdate";
const ROLE_FILE: &str = "coderhelm-log-reader.yaml";
const UPDATE_FILE: &str = "coderhelm-log-reader-update.yaml";
/// The account the template names by default (the hosted deployment's).
/// A download from a self-hosted install replaces it with the install's.
const TEMPLATE_DEFAULT_ACCOUNT: &str = "Default: \"654654210434\"";

/// One thing the role can do: the IAM actions it takes, and in plain words
/// what CoderHelm reads with them and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ability {
    pub key: &'static str,
    pub name: &'static str,
    pub why: &'static str,
    pub actions: &'static [&'static str],
    /// Granted only on what a team names (the knowledge bases it connects),
    /// so the role's live check never counts it missing: connecting runs
    /// its own test instead (`knowledge::remote::test_retrieve`).
    pub chosen: bool,
}

/// Everything the current release's role allows, by ability. Tests keep
/// this equal to both templates' actions.
pub const ABILITIES: &[Ability] = &[
    Ability {
        key: "logs",
        name: "CloudWatch logs and alarms",
        why: "Reads error lines and alarm states so Coderhelm can find what broke and propose a fix.",
        actions: &[
            "logs:DescribeLogGroups",
            "logs:StartQuery",
            "logs:GetQueryResults",
            "logs:StopQuery",
            "logs:FilterLogEvents",
            "logs:GetLogEvents",
            "cloudwatch:DescribeAlarms",
            "sts:GetCallerIdentity",
        ],
        chosen: false,
    },
    Ability {
        key: "check",
        name: "Checking its own permissions",
        why: "Asks IAM which of these permissions the role has, and whether CoderHelm's own update stack finished, so this page can say when an update is needed. It only looks at this one role and CoderHelm's update stacks.",
        actions: &["iam:SimulatePrincipalPolicy", "cloudformation:DescribeStacks"],
        chosen: false,
    },
    Ability {
        key: "metrics",
        name: "CloudWatch metrics",
        why: "Reads error rate, latency and other metrics before and after a deploy.",
        actions: &["cloudwatch:GetMetricData", "cloudwatch:ListMetrics"],
        chosen: false,
    },
    Ability {
        key: "airflow",
        name: "Airflow",
        why: "Reads DAG runs, task logs and import errors as Airflow's Viewer role, which can't trigger or change anything.",
        actions: &[
            "airflow:ListEnvironments",
            "airflow:GetEnvironment",
            "airflow:InvokeRestApi",
        ],
        chosen: false,
    },
    Ability {
        key: "ecs",
        name: "ECS services and tasks",
        why: "Sees which tasks behind a failing log group are running, since when and how they start, as evidence before a fix. A task definition comes with its environment variables; CoderHelm keeps only the start command, image, log setting, storage, cpu and memory.",
        actions: &[
            "ecs:ListClusters",
            "ecs:ListServices",
            "ecs:DescribeServices",
            "ecs:DescribeTaskDefinition",
            "ecs:ListTasks",
            "ecs:DescribeTasks",
        ],
        chosen: false,
    },
    Ability {
        key: "code",
        name: "Finding the code behind a log group",
        why: "Reads function, image and log group tags, the names in a stack's resource list and image manifests to work out which repository a log group's code lives in, so a fix goes to the right repo. Never a function's configuration or code, or a stack's parameters.",
        actions: &[
            "lambda:ListTags",
            "logs:ListTagsForResource",
            "cloudformation:ListStackResources",
            "ecr:BatchGetImage",
            "ecr:ListTagsForResource",
        ],
        chosen: false,
    },
    Ability {
        key: "settings",
        name: "Bucket and key settings",
        why: "Reads an S3 bucket's default encryption, its bucket policy and whether that policy makes it public, and a KMS key's description and key policy, so a review can check a fact like \"is this bucket encrypted with our own key\" instead of asking. Settings only: never an object in a bucket, and never encrypts or decrypts anything.",
        actions: &[
            "s3:GetEncryptionConfiguration",
            "s3:GetBucketPolicy",
            "s3:GetBucketPolicyStatus",
            "kms:DescribeKey",
            "kms:GetKeyPolicy",
        ],
        chosen: false,
    },
    Ability {
        key: "knowledge",
        name: "Bedrock knowledge bases",
        why: "Searches the Bedrock knowledge bases a team connects, live and read-only, so answers can cite them; reads a cited document. Only the knowledge bases named in the stack's KnowledgeBases parameter (or every one in the account, when it says all). Nothing is copied out of the account and nothing is changed.",
        actions: &[
            "bedrock:Retrieve",
            "bedrock:GetDocumentContent",
            "bedrock:ListKnowledgeBases",
            "bedrock:GetKnowledgeBase",
            "bedrock:ListDataSources",
            "bedrock:GetDataSource",
        ],
        chosen: true,
    },
    Ability {
        key: "imports",
        name: "Knowledge imports from S3",
        why: "Reads the documents in the S3 buckets a team imports into a knowledge base (a docs folder, or the data source behind a Bedrock knowledge base), and decrypts them through S3 when a bucket uses its own KMS key. Only the buckets named in the stack's ImportBuckets parameter. Nothing is changed.",
        actions: &[
            "s3:ListBucket",
            "s3:GetBucketLocation",
            "s3:GetObject",
            "kms:Decrypt",
        ],
        chosen: true,
    },
    Ability {
        key: "sync",
        name: "Copying knowledge into Bedrock",
        why: "Adds, updates and removes the documents Coderhelm copies into the Bedrock knowledge bases a team names for it (a custom data source), and starts a sync of the S3 data source it writes to. Only the knowledge bases named in the stack's SyncKnowledgeBases parameter, and only when a team turns it on; nothing else in them is read or changed.",
        actions: &[
            "bedrock:IngestKnowledgeBaseDocuments",
            "bedrock:DeleteKnowledgeBaseDocuments",
            "bedrock:GetKnowledgeBaseDocuments",
            "bedrock:ListKnowledgeBaseDocuments",
            "bedrock:StartIngestionJob",
            "bedrock:GetIngestionJob",
            "bedrock:GetKnowledgeBase",
            "bedrock:GetDataSource",
        ],
        chosen: true,
    },
    Ability {
        key: "infra",
        name: "Infrastructure vulnerabilities",
        why: "Reads which versions run in the account so they can be matched against security advisories: EC2 instances and their AMIs, Auto Scaling groups and the load balancers in front of them, EKS clusters and node groups, RDS and Aurora pending maintenance, ElastiCache service updates, OpenSearch software updates, ECR scan results, kernels from SSM inventory, AWS's public latest-AMI parameters, and which CloudFormation stack made a resource (the stack's name and the resource's logical id, so the fix goes to the code that defines it). Only when the stack's InfrastructureVulnerabilities parameter is on. Never Lambda function settings, launch template user data or environment variables, and nothing is changed.",
        actions: &[
            "ec2:DescribeInstances",
            "ec2:DescribeImages",
            "autoscaling:DescribeAutoScalingGroups",
            "elasticloadbalancing:DescribeTargetGroups",
            "elasticloadbalancing:DescribeLoadBalancers",
            "eks:ListClusters",
            "eks:DescribeCluster",
            "eks:ListNodegroups",
            "eks:DescribeNodegroup",
            "rds:DescribeDBClusters",
            "rds:DescribeDBInstances",
            "rds:DescribePendingMaintenanceActions",
            "elasticache:DescribeServiceUpdates",
            "elasticache:DescribeUpdateActions",
            "es:ListDomainNames",
            "es:DescribeDomains",
            "ecr:DescribeImageScanFindings",
            "cloudformation:DescribeStackResources",
            "ssm:ListInventoryEntries",
            "ssm:GetParameters",
        ],
        chosen: true,
    },
    Ability {
        key: "inspector",
        name: "Amazon Inspector findings",
        why: "Reads Amazon Inspector's findings (which packages are vulnerable on each instance, image and function, whether an exploit is known and whether the network can reach it) when a team runs Inspector. Only when the stack's InspectorFindings parameter is on. Inspector is billed by AWS; CoderHelm never turns it on.",
        actions: &[
            "inspector2:ListFindings",
            "inspector2:ListCoverage",
            "inspector2:BatchGetAccountStatus",
        ],
        chosen: true,
    },
    Ability {
        key: "health",
        name: "AWS Health events",
        why: "Reads the account's AWS Health events (security notifications, scheduled maintenance, retirements) and the resources they name, through the Health API, which needs AWS Business Support or higher. Only when the stack's HealthEvents parameter is on.",
        actions: &[
            "health:DescribeEvents",
            "health:DescribeAffectedEntities",
            "health:DescribeEventDetails",
        ],
        chosen: true,
    },
];

/// Optional abilities a team switches on per account: the live check
/// asks about them too, but a missing one never makes the role "need an
/// update" unless the team switched it on (`AwsAccount::infra_on`).
pub const OPTIONAL: &[&str] = &["infra", "inspector", "health"];

/// The only actions that change anything, all in [`ability`] `sync` and
/// granted only on the knowledge bases a team names in SyncKnowledgeBases.
pub const SYNC_WRITES: &[&str] = &[
    "bedrock:IngestKnowledgeBaseDocuments",
    "bedrock:DeleteKnowledgeBaseDocuments",
    "bedrock:StartIngestionJob",
];

/// What a team chose for the role in one account: the knowledge bases it
/// connects and the buckets it imports. The update stack grants exactly
/// these.
#[derive(Debug, Clone, Copy, Default)]
pub struct Chosen<'a> {
    pub knowledge_bases: &'a [String],
    pub buckets: &'a [String],
    /// Knowledge bases Coderhelm copies a team's knowledge into.
    pub sync_knowledge_bases: &'a [String],
    /// The team switched infrastructure vulnerabilities on in this account.
    pub infrastructure: bool,
    /// The team reads Amazon Inspector's findings, and AWS Health through
    /// its API.
    pub inspector: bool,
    pub health: bool,
}

impl<'a> Chosen<'a> {
    pub fn new(knowledge_bases: &'a [String], buckets: &'a [String]) -> Chosen<'a> {
        Chosen {
            knowledge_bases,
            buckets,
            sync_knowledge_bases: &[],
            infrastructure: false,
            inspector: false,
            health: false,
        }
    }

    /// The same, also reading Inspector's findings and Health events.
    pub fn with_optin(self, inspector: bool, health: bool) -> Chosen<'a> {
        Chosen {
            inspector,
            health,
            ..self
        }
    }

    /// The same, also reading infrastructure versions (`infra`).
    pub fn with_infrastructure(self, on: bool) -> Chosen<'a> {
        Chosen {
            infrastructure: on,
            ..self
        }
    }

    /// The same, also writing into `sync` (bedrock_sync).
    pub fn with_sync(self, sync: &'a [String]) -> Chosen<'a> {
        Chosen {
            sync_knowledge_bases: sync,
            ..self
        }
    }

    pub fn is_empty(&self) -> bool {
        self.knowledge_bases.is_empty()
            && self.buckets.is_empty()
            && self.sync_knowledge_bases.is_empty()
            && !self.infrastructure
            && !self.inspector
            && !self.health
    }
}

/// Every action of [`ABILITIES`], once each, in order.
pub fn all_actions() -> Vec<&'static str> {
    let mut out: Vec<&'static str> = Vec::new();
    for a in ABILITIES {
        for x in a.actions {
            if !out.contains(x) {
                out.push(x);
            }
        }
    }
    out
}

/// The actions the role's live check asks about: every ability's except
/// the [`Ability::chosen`] ones, which only exist for what a team named.
pub fn checked_actions() -> Vec<&'static str> {
    let mut out: Vec<&'static str> = Vec::new();
    for a in ABILITIES.iter().filter(|a| !a.chosen) {
        for x in a.actions {
            if !out.contains(x) {
                out.push(x);
            }
        }
    }
    out
}

/// What the live check simulates: [`checked_actions`] and the optional
/// abilities' (kept apart: `PermissionCheck::optional_missing`).
pub fn simulated_actions() -> Vec<&'static str> {
    let mut out = checked_actions();
    for a in ABILITIES.iter().filter(|a| OPTIONAL.contains(&a.key)) {
        for x in a.actions {
            if !out.contains(x) {
                out.push(x);
            }
        }
    }
    out
}

/// The action belongs only to optional abilities.
pub fn is_optional(action: &str) -> bool {
    !checked_actions().contains(&action)
        && ABILITIES
            .iter()
            .any(|a| OPTIONAL.contains(&a.key) && a.actions.contains(&action))
}

/// A short id of this release's set of permissions: changes exactly when an
/// action is added or removed.
pub fn permissions_id() -> String {
    use sha2::{Digest, Sha256};
    let mut all = all_actions();
    all.sort_unstable();
    hex::encode(&Sha256::digest(all.join(",").as_bytes())[..4])
}

/// This release's update stack name.
pub fn update_stack_name() -> String {
    format!("{UPDATE_STACK_PREFIX}-{}", permissions_id())
}

/// The ability an action belongs to.
pub fn ability_of(action: &str) -> Option<&'static Ability> {
    ABILITIES.iter().find(|a| a.actions.contains(&action))
}

pub fn ability(key: &str) -> Option<&'static Ability> {
    ABILITIES.iter().find(|a| a.key == key)
}

/// The IAM actions behind `missing` abilities, in template order, once each.
pub fn missing_actions(missing: &[&str]) -> Vec<&'static str> {
    let mut out: Vec<&'static str> = Vec::new();
    for a in ABILITIES.iter().filter(|a| missing.contains(&a.key)) {
        for x in a.actions {
            if !out.contains(x) {
                out.push(x);
            }
        }
    }
    out
}

/// Every IAM action a template's policies allow (the quoted `- "svc:Action"`
/// list items; the trust policy's `sts:AssumeRole` is not a list item).
pub fn template_actions(template: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in template.lines() {
        let t = line.trim();
        let Some(rest) = t.strip_prefix("- \"") else {
            continue;
        };
        let Some(v) = rest.strip_suffix('"') else {
            continue;
        };
        let ok = v.split_once(':').is_some_and(|(svc, act)| {
            !svc.is_empty()
                && svc
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                && !act.is_empty()
                && act.chars().all(|c| c.is_ascii_alphanumeric())
        });
        if ok && !out.iter().any(|x| x == v) {
            out.push(v.to_string());
        }
    }
    out
}

/// The update template's URL, next to the role's (same folder).
pub fn update_template_url(role_template_url: &str) -> Option<String> {
    role_template_url
        .strip_suffix(ROLE_FILE)
        .map(|base| format!("{base}{UPDATE_FILE}"))
}

/// The role template with the install's account as its default, for
/// teams that download it and upload it by hand.
pub fn template_for(account: &str, which: Which) -> String {
    match which {
        Which::Role if is_account(account) => ROLE_TEMPLATE.replacen(
            TEMPLATE_DEFAULT_ACCOUNT,
            &format!("Default: \"{account}\""),
            1,
        ),
        Which::Role => ROLE_TEMPLATE.to_string(),
        Which::Update => UPDATE_TEMPLATE.to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Which {
    Role,
    Update,
}

pub fn is_account(a: &str) -> bool {
    a.len() == 12 && a.chars().all(|c| c.is_ascii_digit())
}

/// The name prefix of the install's own roles (`<resourcePrefix>-`), which
/// the role's trust policy is narrowed to; "" when unknown (any role in the
/// install's account, as before).
pub fn trusted_role_prefix() -> String {
    trusted_role_prefix_from(std::env::var("RESOURCE_PREFIX").ok())
}

pub fn trusted_role_prefix_from(v: Option<String>) -> String {
    v.map(|p| p.trim().to_string())
        .filter(|p| {
            !p.is_empty()
                && p.len() <= 40
                && p.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        })
        .map(|p| format!("{p}-"))
        .unwrap_or_default()
}

/// The Launch Stack link that creates the role in a team's account.
pub fn quick_create_url(
    template_url: &str,
    external_id: &str,
    account: &str,
    prefix: &str,
) -> String {
    let mut u = format!(
        "https://console.aws.amazon.com/cloudformation/home#/stacks/quickcreate?stackName={ROLE_STACK}&templateURL={}&param_ExternalId={}&param_CoderHelmAccountId={}",
        urlencoding::encode(template_url),
        urlencoding::encode(external_id),
        account,
    );
    if !prefix.is_empty() {
        u.push_str(&format!(
            "&param_TrustedRolePrefix={}",
            urlencoding::encode(prefix)
        ));
    }
    u
}

/// The one-click update: a Launch Stack link that adds this release's
/// update stack to the role, in `region` (IAM is global; the region only
/// decides where the stack is listed). `chosen` are the team's knowledge
/// bases connected in this account and the buckets it imports from: the
/// stack lets the role search and read exactly those, and its name says
/// which (a new set is a new stack, so it never collides with the one
/// before it).
pub fn update_url(update_template_url: &str, region: &str, chosen: &Chosen) -> String {
    let mut u = format!(
        "https://console.aws.amazon.com/cloudformation/home?region={region}#/stacks/quickcreate?stackName={}&templateURL={}&param_RoleName={ROLE_NAME}",
        update_stack_name_for(chosen),
        urlencoding::encode(update_template_url)
    );
    if !chosen.knowledge_bases.is_empty() {
        u.push_str(&format!(
            "&param_KnowledgeBases={}",
            urlencoding::encode(&chosen.knowledge_bases.join(","))
        ));
    }
    if !chosen.buckets.is_empty() {
        u.push_str(&format!(
            "&param_ImportBuckets={}",
            urlencoding::encode(&chosen.buckets.join(","))
        ));
    }
    if !chosen.sync_knowledge_bases.is_empty() {
        u.push_str(&format!(
            "&param_SyncKnowledgeBases={}",
            urlencoding::encode(&chosen.sync_knowledge_bases.join(","))
        ));
    }
    if chosen.infrastructure {
        u.push_str("&param_InfrastructureVulnerabilities=on");
    }
    if chosen.inspector {
        u.push_str("&param_InspectorFindings=on");
    }
    if chosen.health {
        u.push_str("&param_HealthEvents=on");
    }
    u
}

fn sorted(v: &[String]) -> Vec<&str> {
    let mut out: Vec<&str> = v.iter().map(String::as_str).collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// This release's update stack name for what a team chose: the release's
/// own name when it chose nothing.
pub fn update_stack_name_for(chosen: &Chosen) -> String {
    if chosen.is_empty() {
        return update_stack_name();
    }
    use sha2::{Digest, Sha256};
    let mut seed = sorted(chosen.knowledge_bases).join(",");
    // Knowledge bases alone keep the name they had before imports existed.
    if !chosen.buckets.is_empty() {
        seed.push('|');
        seed.push_str(&sorted(chosen.buckets).join(","));
    }
    // Copying into Bedrock is its own part, so the names before it stay.
    if !chosen.sync_knowledge_bases.is_empty() {
        seed.push_str("|sync:");
        seed.push_str(&sorted(chosen.sync_knowledge_bases).join(","));
    }
    // Infrastructure reads are their own part too.
    if chosen.infrastructure {
        seed.push_str("|infra");
    }
    if chosen.inspector {
        seed.push_str("|inspector");
    }
    if chosen.health {
        seed.push_str("|health");
    }
    format!(
        "{}-kb{}",
        update_stack_name(),
        hex::encode(&Sha256::digest(seed.as_bytes())[..3])
    )
}

/// The CloudFormation console's stack list in `region`, filtered to
/// CoderHelm's stacks.
pub fn stack_list_url(region: &str) -> String {
    format!(
        "https://console.aws.amazon.com/cloudformation/home?region={region}#/stacks?filteringText={ROLE_STACK}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_abilities_list_every_action_the_template_allows() {
        let mut listed: Vec<String> = ABILITIES
            .iter()
            .flat_map(|a| a.actions.iter().map(|s| s.to_string()))
            .collect();
        listed.sort();
        listed.dedup();
        let mut allowed = template_actions(ROLE_TEMPLATE);
        allowed.sort();
        assert_eq!(
            listed, allowed,
            "ABILITIES and infra/cfn/coderhelm-log-reader.yaml disagree"
        );
    }

    #[test]
    fn the_update_stack_gives_every_permission() {
        let mut listed: Vec<String> = all_actions().iter().map(|s| s.to_string()).collect();
        listed.sort();
        let mut allowed = template_actions(UPDATE_TEMPLATE);
        allowed.sort();
        assert_eq!(
            listed, allowed,
            "ABILITIES and infra/cfn/coderhelm-log-reader-update.yaml disagree"
        );
    }

    #[test]
    fn every_permission_is_read_only() {
        let writes = [
            "Put", "Create", "Delete", "Update", "Start", "Stop", "Run", "Invoke", "Attach",
            "Pass", "Tag", "Untag", "Register",
        ];
        let ok_reads = ["logs:StartQuery", "logs:StopQuery", "airflow:InvokeRestApi"];
        for a in all_actions() {
            let verb = a.split(':').nth(1).unwrap();
            if ok_reads.contains(&a) || SYNC_WRITES.contains(&a) {
                continue;
            }
            assert!(
                !writes.iter().any(|w| verb.starts_with(w)),
                "{a} is not read-only"
            );
        }
        // Knowledge bases: search and read a cited document, only those named.
        for t in [ROLE_TEMPLATE, UPDATE_TEMPLATE] {
            assert!(t.contains("- Sid: ReadKnowledgeBases"));
            assert!(t.contains("!Split [\",\", !Ref KnowledgeBases]"));
            assert!(t.contains("knowledge-base/*"));
            assert!(!t.contains("bedrock:Retrieve\"\n                - \"bedrock:Rerank"));
            assert!(
                !template_actions(t).iter().any(|x| x.starts_with("bedrock:")
                    && !ability("knowledge").unwrap().actions.contains(&x.as_str())
                    && !ability("sync").unwrap().actions.contains(&x.as_str()))
            );
            // The writes: only into the knowledge bases named for copying.
            assert!(t.contains("- Sid: SyncKnowledgeBases"));
            assert!(t.contains("!Split [\",\", !Ref SyncKnowledgeBases]"));
            assert!(
                t.contains("SyncsKnowledgeBases: !Not [!Equals [!Ref SyncKnowledgeBases, \"\"]]")
            );
            let sync_block = t.split("- Sid: SyncKnowledgeBases").nth(1).unwrap();
            let sync_block = sync_block.split("- !Ref AWS::NoValue").next().unwrap();
            for w in SYNC_WRITES {
                assert!(sync_block.contains(&format!("\"{w}\"")), "{w}");
                assert_eq!(
                    t.matches(&format!("\"{w}\"")).count(),
                    1,
                    "{w} only in the sync statement"
                );
            }
            // Imports: read objects only in the buckets named, decrypt only
            // through S3.
            assert!(t.contains("- Sid: ReadImportBuckets"));
            assert!(t.contains("!Split [\",\", !Ref ImportBuckets]"));
            assert!(t.contains("kms:ViaService"));
            assert!(!t.contains("\"s3:*\""));
        }
        // Airflow's REST API only as the Viewer role.
        assert!(UPDATE_TEMPLATE.contains("arn:aws:airflow:*:*:role/*/Viewer"));
        assert!(ROLE_TEMPLATE.contains("arn:aws:airflow:*:*:role/*/Viewer"));
    }

    #[test]
    fn each_release_has_its_own_update_stack() {
        let id = permissions_id();
        assert_eq!(id.len(), 8);
        assert_eq!(
            update_stack_name(),
            format!("CoderHelmLogReaderUpdate-{id}")
        );
        let u = update_url(
            "https://b.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader-update.yaml",
            "eu-west-1",
            &Chosen::default(),
        );
        assert!(u.starts_with("https://console.aws.amazon.com/cloudformation/home?region=eu-west-1#/stacks/quickcreate?"));
        assert!(u.contains(&format!("stackName=CoderHelmLogReaderUpdate-{id}")));
        assert!(u.contains("param_RoleName=CoderHelmLogReader"));
        assert!(u.contains("templateURL=https%3A%2F%2Fb.s3.amazonaws.com%2Flaunch%2Fv1.2.3%2Fcoderhelm-log-reader-update.yaml"));
        assert!(ability_of("ecs:DescribeTasks").is_some_and(|a| a.key == "ecs"));
        assert!(ABILITIES
            .iter()
            .all(|a| !a.why.is_empty() && !a.why.contains('\u{2192}')));
    }

    #[test]
    fn the_ecs_actions_from_the_log_fixes_are_in_the_role() {
        let all = template_actions(ROLE_TEMPLATE);
        for a in [
            "ecs:ListTasks",
            "ecs:DescribeTasks",
            "ecs:DescribeServices",
            "lambda:ListTags",
        ] {
            assert!(all.iter().any(|x| x == a), "{a}");
        }
        assert!(!all.iter().any(|x| x == "sts:AssumeRole"));
    }

    /// Nothing that hands over a function's configuration (its environment
    /// variables) or code, or a stack's parameters; the one stack read is
    /// CoderHelm's own update stack.
    #[test]
    fn no_function_configuration_code_or_stack_parameters() {
        for t in [ROLE_TEMPLATE, UPDATE_TEMPLATE] {
            let all = template_actions(t);
            for a in [
                "lambda:GetFunction",
                "lambda:GetFunctionConfiguration",
                "lambda:ListFunctions",
                "lambda:ListVersionsByFunction",
                "cloudformation:GetTemplate",
            ] {
                assert!(!all.iter().any(|x| x == a), "{a}");
            }
            let at = t.find("\"cloudformation:DescribeStacks\"").unwrap();
            let resource = t[at..].lines().nth(1).unwrap();
            assert!(
                resource.contains("stack/CoderHelmLogReaderUpdate-*/*"),
                "{resource}"
            );
            assert_eq!(t.matches("cloudformation:DescribeStacks").count(), 1);
        }
        assert!(update_stack_name().starts_with("CoderHelmLogReaderUpdate-"));
    }

    /// The reviewer's bucket and key lookups use exactly the settings
    /// ability's actions, all reads of settings: no object reads, no
    /// encrypt or decrypt. Object reads and decrypts exist only in the
    /// knowledge import statements, on the buckets a team names.
    #[test]
    fn the_settings_ability_is_what_the_lookups_use() {
        let a = ability("settings").unwrap();
        assert_eq!(a.actions, crate::live::aws_settings::ACTIONS);
        for x in ["s3:GetObject", "s3:ListBucket", "kms:Decrypt"] {
            assert!(!a.actions.contains(&x), "{x}");
            assert!(ability("imports").unwrap().actions.contains(&x), "{x}");
        }
        for t in [ROLE_TEMPLATE, UPDATE_TEMPLATE] {
            // Everything but the import statements.
            let start = t
                .find("Sid: ReadImportBuckets")
                .expect("the import statements");
            let end = t.find("kms:ViaService").expect("decrypt only through S3");
            let rest = format!("{}{}", &t[..start], &t[end..]);
            let all = template_actions(&rest);
            for x in a.actions {
                assert!(template_actions(t).iter().any(|y| y == x), "{x}");
            }
            for no in [
                "s3:GetObject",
                "s3:ListBucket",
                "kms:Decrypt",
                "kms:Encrypt",
            ] {
                assert!(!all.iter().any(|y| y == no), "{no}");
            }
        }
    }

    #[test]
    fn knowledge_bases_ride_on_the_update_and_are_never_counted_missing() {
        let kb = "arn:aws:bedrock:us-east-1:111122223333:knowledge-base/ABCDE12345".to_string();
        let kbs = std::slice::from_ref(&kb);
        let u = update_url(
            "https://b/x/coderhelm-log-reader-update.yaml",
            "us-east-1",
            &Chosen::new(kbs, &[]),
        );
        assert!(u.contains("&param_KnowledgeBases=arn%3Aaws%3Abedrock%3Aus-east-1%3A111122223333%3Aknowledge-base%2FABCDE12345"), "{u}");
        assert!(!u.contains("ImportBuckets"));
        let name = update_stack_name_for(&Chosen::new(kbs, &[]));
        assert!(
            name.starts_with(&format!("{}-kb", update_stack_name())),
            "{name}"
        );
        assert!(name.len() <= 128);
        assert!(u.contains(&format!("stackName={name}")));
        // The same set in another order is the same stack; another set is another.
        let other = "arn:aws:bedrock:us-east-1:111122223333:knowledge-base/ZZZZZ00000".to_string();
        let both = [kb.clone(), other.clone()];
        let reversed = [other.clone(), kb.clone()];
        assert_eq!(
            update_stack_name_for(&Chosen::new(&both, &[])),
            update_stack_name_for(&Chosen::new(&reversed, &[]))
        );
        assert_ne!(update_stack_name_for(&Chosen::new(&both, &[])), name);
        assert_eq!(
            update_stack_name_for(&Chosen::default()),
            update_stack_name()
        );
        assert!(
            !update_url("https://b/x/y.yaml", "us-east-1", &Chosen::default())
                .contains("KnowledgeBases")
        );
        // Buckets ride along too, and make it another stack.
        let buckets = ["team-docs".to_string()];
        let u = update_url(
            "https://b/x/y.yaml",
            "us-east-1",
            &Chosen::new(kbs, &buckets),
        );
        assert!(u.contains("&param_ImportBuckets=team-docs"), "{u}");
        assert_ne!(update_stack_name_for(&Chosen::new(kbs, &buckets)), name);
        assert!(update_stack_name_for(&Chosen::new(&[], &buckets)).len() <= 128);
        // The live check asks about everything but what a team chose.
        let checked = checked_actions();
        assert!(!checked.contains(&"bedrock:Retrieve"));
        assert!(!checked.contains(&"s3:GetObject"));
        assert!(checked.contains(&"logs:StartQuery"));
        // Knowledge bases (6), imports (4), copying into Bedrock (6 more:
        // its Get calls are the knowledge bases' too) and infrastructure
        // reads (20), Inspector (3) and Health (3).
        assert_eq!(checked.len() + 42, all_actions().len());
        assert!(!checked.contains(&"bedrock:IngestKnowledgeBaseDocuments"));
        // Copying into Bedrock rides along as its own parameter and stack.
        let sync = ["arn:aws:bedrock:us-east-1:111122223333:knowledge-base/SYNC123456".to_string()];
        let c = Chosen::new(kbs, &buckets).with_sync(&sync);
        let u = update_url("https://b/x/y.yaml", "us-east-1", &c);
        assert!(u.contains("&param_SyncKnowledgeBases=arn%3Aaws%3Abedrock%3Aus-east-1%3A111122223333%3Aknowledge-base%2FSYNC123456"), "{u}");
        assert_ne!(
            update_stack_name_for(&c),
            update_stack_name_for(&Chosen::new(kbs, &buckets))
        );
        assert!(!Chosen::new(&[], &[]).with_sync(&sync).is_empty());
    }

    #[test]
    fn infrastructure_reads_are_optional_read_only_and_never_lambda_settings() {
        let infra = ability("infra").unwrap();
        assert!(infra.chosen);
        assert!(OPTIONAL.contains(&"infra"));
        for a in infra.actions {
            assert!(!checked_actions().contains(a), "{a} is never required");
            assert!(simulated_actions().contains(a), "{a} is still checked");
            assert!(is_optional(a));
            let verb = a.split(':').nth(1).unwrap();
            assert!(
                ["Describe", "List", "Get"]
                    .iter()
                    .any(|v| verb.starts_with(v)),
                "{a} is not a read"
            );
        }
        assert!(!is_optional("logs:StartQuery"));
        for no in [
            "ec2:DescribeLaunchTemplateVersions",
            "ec2:DescribeInstanceAttribute",
            "lambda:ListFunctions",
            "lambda:GetFunctionConfiguration",
            "ec2:Describe*",
        ] {
            assert!(!infra.actions.contains(&no), "{no}");
        }
        for t in [ROLE_TEMPLATE, UPDATE_TEMPLATE] {
            assert!(t.contains(
                "ReadsInfrastructure: !Equals [!Ref InfrastructureVulnerabilities, \"on\"]"
            ));
            assert!(t.contains("Sid: ReadInfrastructureVersions"));
            // The public parameters only.
            let at = t.find("\"ssm:GetParameters\"").unwrap();
            assert!(t[at..]
                .lines()
                .nth(1)
                .unwrap()
                .contains("arn:aws:ssm:*::parameter/aws/service/*"));
        }
        // The update link carries the switch, and it is its own stack.
        let on = Chosen::default().with_infrastructure(true);
        let u = update_url("https://b/x/y.yaml", "us-east-1", &on);
        assert!(u.contains("&param_InfrastructureVulnerabilities=on"), "{u}");
        assert_ne!(update_stack_name_for(&on), update_stack_name());
        assert!(
            !update_url("https://b/x/y.yaml", "us-east-1", &Chosen::default())
                .contains("Infrastructure")
        );
    }

    #[test]
    fn inspector_and_health_are_optional_reads_that_ride_on_the_update() {
        let on = Chosen::default().with_infrastructure(true);
        let both = on.with_optin(true, true);
        let u = update_url("https://b/x/y.yaml", "us-east-1", &both);
        assert!(
            u.contains("&param_InspectorFindings=on") && u.contains("&param_HealthEvents=on"),
            "{u}"
        );
        assert_ne!(update_stack_name_for(&both), update_stack_name_for(&on));
        for key in ["inspector", "health"] {
            let a = ability(key).unwrap();
            assert!(a.chosen && OPTIONAL.contains(&key));
            for x in a.actions {
                assert!(is_optional(x), "{x}");
                let verb = x.split(':').nth(1).unwrap();
                assert!(
                    ["Describe", "List", "BatchGet"]
                        .iter()
                        .any(|v| verb.starts_with(v)),
                    "{x}"
                );
            }
        }
        for t in [ROLE_TEMPLATE, UPDATE_TEMPLATE] {
            assert!(t.contains("ReadsInspector: !Equals [!Ref InspectorFindings, \"on\"]"));
            assert!(t.contains("ReadsHealth: !Equals [!Ref HealthEvents, \"on\"]"));
            assert!(t.contains("$1.25 per instance"), "the cost is stated");
        }
    }

    #[test]
    fn missing_actions_follow_the_abilities() {
        assert!(missing_actions(&[]).is_empty());
        let m = missing_actions(&["ecs", "metrics"]);
        assert_eq!(m[0], "cloudwatch:GetMetricData");
        assert_eq!(m.len(), 8);
        assert!(m.contains(&"ecs:DescribeTasks"));
        assert!(!m.contains(&"lambda:ListTags"));
    }

    #[test]
    fn the_role_trusts_the_install_with_the_external_id_and_its_own_roles() {
        assert!(ROLE_TEMPLATE.contains("\"sts:ExternalId\": !Ref ExternalId"));
        assert!(ROLE_TEMPLATE.contains("arn:aws:iam::${CoderHelmAccountId}:root"));
        assert!(ROLE_TEMPLATE.contains("aws:PrincipalArn"));
        assert!(ROLE_TEMPLATE.contains("TrustedRolePrefix"));
    }

    #[test]
    fn a_download_names_the_install_account() {
        let t = template_for("111122223333", Which::Role);
        assert!(t.contains("Default: \"111122223333\""));
        assert!(!t.contains("654654210434"));
        // Only the default changes.
        assert_eq!(t.len(), ROLE_TEMPLATE.len());
        assert_eq!(template_for("bad", Which::Role), ROLE_TEMPLATE);
        assert_eq!(template_for("111122223333", Which::Update), UPDATE_TEMPLATE);
    }

    #[test]
    fn the_update_template_sits_next_to_the_role_template() {
        assert_eq!(
            update_template_url(
                "https://b.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader.yaml"
            )
            .as_deref(),
            Some("https://b.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader-update.yaml")
        );
        assert_eq!(
            update_template_url("https://b.s3.amazonaws.com/mine.yaml"),
            None
        );
    }

    #[test]
    fn the_launch_link_carries_the_install_account_and_prefix() {
        let u = quick_create_url(
            "https://b.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader.yaml",
            "0b5f1a5e-0000-4000-8000-000000000000",
            "111122223333",
            "coderhelm-prod-",
        );
        assert!(u.contains("param_CoderHelmAccountId=111122223333"));
        assert!(u.contains("param_TrustedRolePrefix=coderhelm-prod-"));
        assert!(u.contains("templateURL=https%3A%2F%2Fb.s3.amazonaws.com%2Flaunch%2Fv1.2.3%2Fcoderhelm-log-reader.yaml"));
        assert!(
            !quick_create_url("https://x/y.yaml", "e", "111122223333", "")
                .contains("TrustedRolePrefix")
        );
    }

    #[test]
    fn the_prefix_is_the_install_resource_prefix() {
        assert_eq!(
            trusted_role_prefix_from(Some("coderhelm-prod".into())),
            "coderhelm-prod-"
        );
        assert_eq!(trusted_role_prefix_from(None), "");
        assert_eq!(trusted_role_prefix_from(Some(" ".into())), "");
        assert_eq!(trusted_role_prefix_from(Some("a*b".into())), "");
    }
}
