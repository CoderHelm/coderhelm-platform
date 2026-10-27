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
    },
    Ability {
        key: "check",
        name: "Checking its own permissions",
        why: "Asks IAM which of these permissions the role has, so this page can say when an update is needed. It only looks at this one role.",
        actions: &["iam:SimulatePrincipalPolicy"],
    },
    Ability {
        key: "metrics",
        name: "CloudWatch metrics",
        why: "Reads error rate, latency and other metrics before and after a deploy.",
        actions: &["cloudwatch:GetMetricData", "cloudwatch:ListMetrics"],
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
    },
    Ability {
        key: "ecs",
        name: "ECS services and tasks",
        why: "Sees which tasks behind a failing log group are running, since when and how they start, as evidence before a fix.",
        actions: &[
            "ecs:ListClusters",
            "ecs:ListServices",
            "ecs:DescribeServices",
            "ecs:DescribeTaskDefinition",
            "ecs:ListTasks",
            "ecs:DescribeTasks",
        ],
    },
    Ability {
        key: "code",
        name: "Finding the code behind a log group",
        why: "Reads function, image and stack tags to work out which repository a log group's code lives in, so a fix goes to the right repo.",
        actions: &[
            "lambda:GetFunction",
            "lambda:GetFunctionConfiguration",
            "lambda:ListFunctions",
            "lambda:ListTags",
            "logs:ListTagsForResource",
            "cloudformation:DescribeStacks",
            "ecr:BatchGetImage",
            "ecr:ListTagsForResource",
        ],
    },
];

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
/// decides where the stack is listed).
pub fn update_url(update_template_url: &str, region: &str) -> String {
    format!(
        "https://console.aws.amazon.com/cloudformation/home?region={region}#/stacks/quickcreate?stackName={}&templateURL={}&param_RoleName={ROLE_NAME}",
        update_stack_name(),
        urlencoding::encode(update_template_url)
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
            if ok_reads.contains(&a) {
                continue;
            }
            assert!(
                !writes.iter().any(|w| verb.starts_with(w)),
                "{a} is not read-only"
            );
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
            "lambda:GetFunction",
        ] {
            assert!(all.iter().any(|x| x == a), "{a}");
        }
        assert!(!all.iter().any(|x| x == "sts:AssumeRole"));
    }

    #[test]
    fn missing_actions_follow_the_abilities() {
        assert!(missing_actions(&[]).is_empty());
        let m = missing_actions(&["ecs", "metrics"]);
        assert_eq!(m[0], "cloudwatch:GetMetricData");
        assert_eq!(m.len(), 8);
        assert!(m.contains(&"ecs:DescribeTasks"));
        assert!(!m.contains(&"lambda:GetFunction"));
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
