//! A read an alert route needs that the team's connected AWS role lacks:
//! one install-level setup item, never a question on a run.
//!
//! A run an alert rule started for an AWS WAF alarm ended `needs_input`
//! with a long question asking a person to grant the team's read-only role
//! WAF and Athena access, with "Grant access" as the default if nobody
//! answered. The role's setup script had never added any WAF read, so every
//! such run would have asked the same thing. A permission the role lacks is
//! the install's, like a GitHub App permission
//! (`github_grants::install_item`): it is recorded here once per connected
//! account when a read is refused ([`record`]), the install's setup checks
//! show it as one item, and the first read that works again clears it
//! ([`clear`]). A run names the item in one line ([`run_line`]) and goes
//! on with what it could read.
//!
//! Every read is declared in one versioned template, the role stack's
//! (`infra/cfn/coderhelm-log-reader.yaml`, published with each release),
//! as named groups a stack parameter switches on. A release can't widen
//! the role by itself (CoderHelm's own roles may not change IAM), so the
//! item is the one-click update of that stack ([`role_update`]): the
//! release's template with the account's groups and what CoderHelm found
//! filled in (the WAF log workgroup's results location, the log bucket's
//! KMS key), which an AWS admin of the account reviews and creates. The
//! same item names an account whose role is older than the release and one
//! whose data sources still read through the script-made role.
//!
//! [`is_install_grant`] is how the stop judge and the hand-back list know a
//! question or a blocker is this item, so neither can turn it into a wait
//! on a person.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

use crate::github_grants::INSTALL_LEVEL;

/// The setup item's name, as the install's setup checks show it.
pub const ITEM_LABEL: &str = "AWS role reads";
/// The setup check's key.
pub const CHECK_KEY: &str = "aws_role_reads";
/// One record per connected account, in the settings table under the team.
const SK_PREFIX: &str = "AWS_ROLE_GAP#";
/// Most actions kept on one record.
const MAX_ACTIONS: usize = 20;

/// Reads a team's connected AWS role was refused, in one account.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Gap {
    pub account: String,
    /// The connected role, for the update line (its name).
    pub role_arn: String,
    /// The IAM actions AWS refused, once each.
    pub actions: Vec<String>,
    /// What needed them, in a few words (`an AWS WAF alarm's evidence`).
    pub needed_for: String,
    pub seen_at: String,
    /// What the run found for the role stack's WAF log group: the Athena
    /// workgroup's results location (`bucket/prefix/`) and the log
    /// bucket's customer-managed KMS key. Empty when not found.
    pub waf_log_results: String,
    pub waf_log_key: String,
}

impl Gap {
    /// The template groups the refused reads belong to, once each: the WAF
    /// log group for its actions, otherwise the ability that has the action.
    pub fn groups(&self) -> Vec<&'static str> {
        let mut out: Vec<&'static str> = Vec::new();
        for a in &self.actions {
            if let Some(k) = group_of(a) {
                if !out.contains(&k) {
                    out.push(k);
                }
            }
        }
        out
    }
}

/// Pure: the template group an action a WAF alarm run was refused belongs
/// to: the WAF log group when it has the action (Athena, Glue, the log
/// buckets), else the first ability that has it.
pub fn group_of(action: &str) -> Option<&'static str> {
    let logs = crate::log_reader::ability("waf_logs")?;
    if logs.actions.contains(&action) {
        return Some(logs.key);
    }
    crate::log_reader::ability_of(action).map(|a| a.key)
}

/// Why an account's role stack needs updating, in a few words each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleUpdate {
    pub account: String,
    pub reasons: Vec<String>,
    /// The groups the update adds, by name (what it reads, in plain words).
    pub adds: Vec<&'static str>,
    /// The one-click update: CloudFormation's quick-create of this
    /// release's update stack on the role, its parameters filled in.
    pub url: String,
    pub stack_name: String,
}

/// Pure: the update an account's role stack needs, or `None` when it needs
/// none. `account` is the connected account with what the team chose there
/// filled in (knowledge bases, buckets, data sources); `gap` the reads a
/// run was refused there; `legacy_sources` how many data sources still read
/// through the script-made `CoderhelmReadOnly` role in that account;
/// `update_template_url` this release's update template.
pub fn role_update(
    account: &crate::live::aws_account::AwsAccount,
    gap: Option<&Gap>,
    legacy_sources: usize,
    update_template_url: &str,
) -> Option<RoleUpdate> {
    use crate::log_reader;
    let mut reasons: Vec<String> = Vec::new();
    let mut adds: Vec<&'static str> = Vec::new();
    let groups = gap.map(|g| g.groups()).unwrap_or_default();
    let waf_logs = account.waf_logs || groups.contains(&"waf_logs");
    let (results, key) = match gap {
        Some(g) => crate::live::aws_account::waf_logs_after(
            &account.waf_log_results,
            &account.waf_log_key,
            &g.waf_log_results,
            &g.waf_log_key,
        ),
        None => (account.waf_log_results.clone(), account.waf_log_key.clone()),
    };
    if let Some(g) = gap.filter(|g| !g.actions.is_empty()) {
        reasons.push(format!(
            "{} was refused {} ({})",
            role_name(&g.role_arn),
            g.actions.join(", "),
            g.needed_for
        ));
        for k in &groups {
            if let Some(a) = log_reader::ability(k) {
                if !adds.contains(&a.name) {
                    adds.push(a.name);
                }
            }
        }
    }
    let check = account.permissions.as_ref().filter(|c| !c.install_account);
    if let Some(c) = check.filter(|c| c.status() == "update_needed") {
        reasons.push(format!(
            "its role is older than this release ({} read{} missing)",
            c.missing.len(),
            if c.missing.len() == 1 { "" } else { "s" }
        ));
        for k in c.missing_abilities() {
            if let Some(a) = log_reader::ability(k) {
                if !adds.contains(&a.name) {
                    adds.push(a.name);
                }
            }
        }
    }
    if legacy_sources > 0 {
        reasons.push(format!(
            "{legacy_sources} data source{} still read through the script-made CoderhelmReadOnly role",
            if legacy_sources == 1 { "" } else { "s" }
        ));
        for k in ["databases", "warehouses"] {
            let named = match k {
                "databases" => !account.database_clusters.is_empty(),
                _ => !account.warehouses.is_empty(),
            };
            if let Some(a) = log_reader::ability(k).filter(|_| named) {
                if !adds.contains(&a.name) {
                    adds.push(a.name);
                }
            }
        }
    }
    if reasons.is_empty() {
        return None;
    }
    let mut chosen = account.chosen();
    if waf_logs {
        chosen = chosen.with_waf_logs(&results, &key);
    }
    Some(RoleUpdate {
        account: account.account.clone(),
        reasons,
        adds,
        url: log_reader::update_url(update_template_url, &account.region, &chosen),
        stack_name: log_reader::update_stack_name_for(&chosen),
    })
}

fn role_name(arn: &str) -> String {
    let n = arn.rsplit('/').next().unwrap_or_default();
    if n.is_empty() {
        "CoderHelm's AWS role".to_string()
    } else {
        format!("The role {n}")
    }
}

/// Pure: the one line a run says about the missing reads: what was
/// refused, and that it is the install's setup item, never this run's
/// question.
pub fn run_line(actions: &[String]) -> String {
    format!(
        "CoderHelm's AWS role was refused {}: that is the install's setup item \"{ITEM_LABEL}\" \
         (a one-click update of CoderHelm's role stack that an AWS admin of the account approves), \
         asked once for the whole install {INSTALL_LEVEL}. This run asks nobody for it and goes on \
         with what it could read.",
        if actions.is_empty() {
            "a read".to_string()
        } else {
            actions.join(", ")
        }
    )
}

/// Pure: the record for reads `account`'s role was refused, or none when
/// nothing was refused.
pub fn gap(
    account: &str,
    role_arn: &str,
    refused: &[String],
    needed_for: &str,
    now: &str,
) -> Option<Gap> {
    (!refused.is_empty() && is_account(account)).then(|| Gap {
        account: account.to_string(),
        role_arn: role_arn.to_string(),
        actions: refused.to_vec(),
        needed_for: needed_for.to_string(),
        seen_at: now.to_string(),
        ..Default::default()
    })
}

/// AWS services whose `service:Action` names mean an AWS permission.
const AWS_SERVICES: &[&str] = &[
    "wafv2",
    "waf",
    "athena",
    "glue",
    "s3",
    "logs",
    "cloudwatch",
    "iam",
    "kms",
    "ecs",
    "rds",
    "rds-data",
    "redshift",
    "redshift-data",
    "secretsmanager",
    "lambda",
    "ec2",
    "ssm",
    "sts",
];

/// Words that name an AWS service the role reads.
const AWS_WORDS: &[&str] = &[
    "wafv2",
    "waf",
    "athena",
    "glue",
    "cloudwatch",
    "aws-waf-logs",
    "s3",
    "iam",
];

/// Words that name the team's connected AWS role.
const ROLE_WORDS: &[&str] = &[
    "read-only role",
    "readonly role",
    "read only role",
    "coderhelmreadonly",
    "coderhelmlogreader",
    "aws role",
    "iam role",
    "coderhelm's role",
    "team's role",
    "role's policy",
    "role policy",
];

/// Words that ask for access.
const ASK_WORDS: &[&str] = &[
    "grant",
    "permission",
    "access",
    "allow",
    "denied",
    "lacks",
    "missing",
    "add ",
];

/// Pure: whether the text has an AWS IAM action (`wafv2:GetSampledRequests`).
fn has_aws_action(t: &str) -> bool {
    t.split(|c: char| c.is_whitespace() || "`'\"(),;".contains(c))
        .filter_map(|w| w.split_once(':'))
        .any(|(svc, act)| {
            AWS_SERVICES.contains(&svc)
                && act
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '*')
        })
}

/// Pure: whether a question, a blocker or a step asks a person for a read
/// the team's connected AWS role lacks (or names this item). That is the
/// install's setup item, never a run's question or wait.
pub fn is_install_grant(text: &str) -> bool {
    let t = text.to_lowercase();
    if t.contains(&ITEM_LABEL.to_lowercase()) && t.contains(&INSTALL_LEVEL.to_lowercase()) {
        return true;
    }
    let words: Vec<&str> = t
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))
        .filter(|w| !w.is_empty())
        .collect();
    let aws = has_aws_action(&t) || words.iter().any(|w| AWS_WORDS.contains(w));
    let role = ROLE_WORDS.iter().any(|w| t.contains(w));
    let asks = ASK_WORDS.iter().any(|w| t.contains(w));
    aws && role && asks
}

fn attr(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn is_account(a: &str) -> bool {
    a.len() == 12 && a.chars().all(|c| c.is_ascii_digit())
}

/// Records the reads `gap.account`'s role was refused (best effort). One
/// record per account: a later refusal adds its actions to it.
pub async fn record(dynamo: &DynamoClient, table: &str, team_id: &str, gap: &Gap) {
    if !is_account(&gap.account) || gap.actions.is_empty() {
        return;
    }
    let before = list(dynamo, table, team_id)
        .await
        .into_iter()
        .find(|g| g.account == gap.account)
        .unwrap_or_default();
    let (results, key) = crate::live::aws_account::waf_logs_after(
        &before.waf_log_results,
        &before.waf_log_key,
        &gap.waf_log_results,
        &gap.waf_log_key,
    );
    let mut actions = before.actions;
    for a in &gap.actions {
        if !actions.contains(a) {
            actions.push(a.clone());
        }
    }
    actions.truncate(MAX_ACTIONS);
    let res = dynamo
        .put_item()
        .table_name(table)
        .item("pk", attr(team_id))
        .item("sk", attr(&format!("{SK_PREFIX}{}", gap.account)))
        .item("account", attr(&gap.account))
        .item("role_arn", attr(&gap.role_arn))
        .item(
            "actions",
            AttributeValue::L(actions.iter().map(|a| attr(a)).collect()),
        )
        .item("needed_for", attr(&gap.needed_for))
        .item("seen_at", attr(&gap.seen_at))
        .item("waf_log_results", attr(&results))
        .item("waf_log_key", attr(&key))
        .send()
        .await;
    if let Err(e) = res {
        tracing::warn!(team_id, error = %e, "Could not record the AWS role's missing reads");
    }
}

/// Clears `account`'s record once its role reads again (best effort).
pub async fn clear(dynamo: &DynamoClient, table: &str, team_id: &str, account: &str) {
    if !is_account(account) {
        return;
    }
    let _ = dynamo
        .delete_item()
        .table_name(table)
        .key("pk", attr(team_id))
        .key("sk", attr(&format!("{SK_PREFIX}{account}")))
        .send()
        .await;
}

/// Every account whose role lacks a read, for the setup check.
pub async fn list(dynamo: &DynamoClient, table: &str, team_id: &str) -> Vec<Gap> {
    let out = dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :p AND begins_with(sk, :s)")
        .expression_attribute_values(":p", attr(team_id))
        .expression_attribute_values(":s", attr(SK_PREFIX))
        .send()
        .await;
    let Ok(out) = out else {
        return Vec::new();
    };
    let s = |i: &std::collections::HashMap<String, AttributeValue>, k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    out.items()
        .iter()
        .map(|i| Gap {
            account: s(i, "account"),
            role_arn: s(i, "role_arn"),
            actions: i
                .get("actions")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
                .unwrap_or_default(),
            needed_for: s(i, "needed_for"),
            seen_at: s(i, "seen_at"),
            waf_log_results: s(i, "waf_log_results"),
            waf_log_key: s(i, "waf_log_key"),
        })
        .filter(|g| is_account(&g.account))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The question the alarm run asked, in its own words.
    const ASKED: &str = "Can someone grant the team's read-only role WAF and Athena access? \
        wafv2:ListWebACLs and wafv2:GetSampledRequests were denied for CoderHelm's role.";

    #[test]
    fn asking_for_the_roles_aws_reads_is_the_install_item() {
        assert!(is_install_grant(ASKED));
        assert!(is_install_grant(
            "Add athena:StartQueryExecution to the CoderhelmReadOnly role"
        ));
        assert!(is_install_grant(&run_line(&["wafv2:ListWebACLs".into()])));
        // Not AWS, not the role, or not asking for access.
        for other in [
            "Grant the GitHub app Actions: write",
            "Which page should the export button go on?",
            "The WAF rule should block crawler A by JA4",
            "Grant access to the staging database password",
            "The read-only role reads WAF logs",
        ] {
            assert!(!is_install_grant(other), "{other}");
        }
    }

    fn account(region: &str) -> crate::live::aws_account::AwsAccount {
        crate::live::aws_account::AwsAccount {
            account: "111122223333".into(),
            role_arn: "arn:aws:iam::111122223333:role/CoderHelmLogReader".into(),
            region: region.into(),
            ..Default::default()
        }
    }

    const TEMPLATE: &str =
        "https://b.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader-update.yaml";

    #[test]
    fn a_refused_waf_log_read_is_the_role_stacks_one_click_update() {
        let gap = Gap {
            account: "111122223333".into(),
            role_arn: "arn:aws:iam::111122223333:role/CoderHelmLogReader".into(),
            actions: vec!["glue:GetDatabases".into()],
            needed_for: "an AWS WAF alarm's evidence".into(),
            seen_at: "t".into(),
            waf_log_results: String::new(),
            waf_log_key:
                "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab".into(),
        };
        assert_eq!(gap.groups(), vec!["waf_logs"]);
        let u = role_update(&account("us-east-1"), Some(&gap), 0, TEMPLATE).expect("an update");
        assert!(u.url.starts_with("https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/quickcreate?"));
        assert!(u.url.contains("&param_WafLogs=on"), "{}", u.url);
        assert!(u.url.contains("&param_WafLogKey="), "{}", u.url);
        assert!(!u.url.contains("WafLogResults"), "not found yet: {}", u.url);
        assert!(u.url.contains(&format!("stackName={}", u.stack_name)));
        assert_eq!(u.adds, vec!["AWS WAF logs in S3"]);
        assert!(u.reasons[0].contains("CoderHelmLogReader was refused glue:GetDatabases"));
        // The next run found the workgroup and was refused the query: the
        // link now carries the results location too.
        let later = Gap {
            actions: vec!["athena:StartQueryExecution".into()],
            waf_log_results: "aws-waf-logs-acme/athena-results/".into(),
            ..gap.clone()
        };
        let mut a = account("us-east-1");
        a.waf_logs = true;
        a.waf_log_key = gap.waf_log_key.clone();
        let u2 = role_update(&a, Some(&later), 0, TEMPLATE).unwrap();
        assert!(
            u2.url
                .contains("&param_WafLogResults=aws-waf-logs-acme%2Fathena-results%2F"),
            "{}",
            u2.url
        );
        assert_ne!(u2.stack_name, u.stack_name);
        // Base WAF reads belong to the release's always-on group.
        assert_eq!(group_of("wafv2:GetSampledRequests"), Some("waf"));
        // Nothing to do: no item.
        assert!(role_update(&account("us-east-1"), None, 0, TEMPLATE).is_none());
    }

    #[test]
    fn an_older_role_and_script_made_sources_are_the_same_item() {
        let mut a = account("eu-west-1");
        let mut c = crate::live::permissions::PermissionCheck::new(&a.target(), "t");
        c.works = true;
        c.missing = vec!["ecs:ListTasks".into()];
        a.permissions = Some(c);
        a.database_clusters = vec!["arn:aws:rds:eu-west-1:111122223333:cluster:app-db".into()];
        // A stored WAF log group stays on in every later link.
        a.waf_logs = true;
        a.waf_log_results = "aws-waf-logs-acme/athena-results/".into();
        let u = role_update(&a, None, 1, TEMPLATE).unwrap();
        assert!(u
            .reasons
            .iter()
            .any(|r| r.contains("older than this release")));
        assert!(u.reasons.iter().any(|r| r.contains("CoderhelmReadOnly")));
        assert!(u.adds.contains(&"ECS services and tasks") && u.adds.contains(&"Databases"));
        assert!(u.url.contains("param_DatabaseClusters=") && u.url.contains("param_WafLogs=on"));
        // The install's own account gets its reads with version updates.
        let mut own = a.clone();
        own.permissions.as_mut().unwrap().install_account = true;
        assert!(role_update(&own, None, 0, TEMPLATE).is_none());
    }

    #[test]
    fn the_run_line_names_the_item_once_for_the_install() {
        let l = run_line(&[
            "wafv2:ListWebACLs".into(),
            "wafv2:GetSampledRequests".into(),
        ]);
        assert!(l.contains("\"AWS role reads\""));
        assert!(l.contains(INSTALL_LEVEL));
        assert!(l.contains("wafv2:ListWebACLs, wafv2:GetSampledRequests"));
        assert!(l.contains("asks nobody"));
        // A recorded refusal on an install-level item is never a person's grant.
        assert!(!crate::person_gate::grant_backed(&l));
    }
}
