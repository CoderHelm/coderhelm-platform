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
//! show it as one item with the one-line CloudShell update
//! (`live::aws::waf_update_line`), and the first read that works again
//! clears it ([`clear`]). A run names the item in one line ([`run_line`])
//! and goes on with what it could read.
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
}

/// Pure: the one line a run says about the missing reads: what was
/// refused, and that it is the install's setup item, never this run's
/// question.
pub fn run_line(actions: &[String]) -> String {
    format!(
        "CoderHelm's AWS role was refused {}: that is the install's setup item \"{ITEM_LABEL}\" \
         (one line in AWS CloudShell), asked once for the whole install {INSTALL_LEVEL}. This run \
         asks nobody for it and goes on with what it could read.",
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
    let mut actions: Vec<String> = list(dynamo, table, team_id)
        .await
        .into_iter()
        .find(|g| g.account == gap.account)
        .map(|g| g.actions)
        .unwrap_or_default();
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
