//! Reaching a team's AWS account: its logs and its databases.
//!
//! The team creates a role in its own account that Coderhelm may use, with a
//! private ID only that team is given (so nobody else using Coderhelm can point
//! it at their role), and with permissions that only read. The setup is one
//! script the team pastes into AWS CloudShell ([`setup_script`]); it prints a
//! line they paste back into Coderhelm ([`parse_setup_code`]).
//!
//! Every use takes a fresh, 15-minute session on that role, named after the
//! agent using it, so the team's own AWS records show who looked at what.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::StepError;

/// Every role Coderhelm can use starts with this name. Coderhelm's own
/// permission to take on roles is limited to it, so it can't be pointed at an
/// unrelated role in someone's account.
pub const ROLE_PREFIX: &str = "CoderhelmReadOnly";
/// The role made earlier for log alerts. It only reads logs, so a logs source
/// may use it without any new setup.
pub const LOG_READER_ROLE: &str = "CoderHelmLogReader";
/// The database user the setup script creates.
pub const DB_USER: &str = "coderhelm_readonly";
/// Secrets the role may read start with this name; nothing else is readable.
pub const SECRET_PREFIX: &str = "coderhelm-readonly-";
const SESSION_SECONDS: i32 = 900;

/// Where in a team's account a source reads from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwsTarget {
    pub role_arn: String,
    /// The team's private ID. Always set by Coderhelm from its own records,
    /// never taken from anything a person pastes.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub external_id: String,
    pub region: String,
    /// Log groups the agents may search. A name ending in `*` covers every
    /// group that starts with it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub log_groups: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cluster_arn: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_arn: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub database: String,
    /// `redshift` for a data warehouse; empty for PostgreSQL.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub engine: String,
    /// The Redshift cluster's name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cluster_id: String,
    /// The Redshift user Coderhelm signs in as, with no password.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub db_user: String,
    /// Redshift databases the team shared.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub databases: Vec<String>,
}

impl AwsTarget {
    pub fn account(&self) -> &str {
        self.role_arn.split(':').nth(4).unwrap_or("")
    }

    /// Whether `group` is one the team chose to share.
    pub fn allows_log_group(&self, group: &str) -> bool {
        self.log_groups.iter().any(|g| match g.strip_suffix('*') {
            // A short prefix saved before names were checked still matches nothing.
            Some(prefix) => {
                prefix.trim_matches('/').len() >= MIN_LOG_PREFIX && group.starts_with(prefix)
            }
            None => g == group,
        })
    }
}

/// The shortest prefix a `prefix*` log group pattern may have, so one entry
/// can't share every group in the account.
pub const MIN_LOG_PREFIX: usize = 4;

/// Checks the log groups a person chose to share: CloudWatch's own name
/// characters, a `*` only at the end, and a real prefix before it.
pub fn check_log_groups(groups: &[String]) -> Result<(), String> {
    for g in groups {
        let (name, wildcard) = match g.strip_suffix('*') {
            Some(p) => (p, true),
            None => (g.as_str(), false),
        };
        let chars_ok = !name.is_empty()
            && name.len() <= 512
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "._-/#".contains(c));
        if !chars_ok {
            return Err(format!("{g} isn't a log group name."));
        }
        if wildcard && name.trim_matches('/').len() < MIN_LOG_PREFIX {
            return Err(format!(
                "{g} would share almost every log group. Use a longer prefix, like /aws/lambda/orders-*."
            ));
        }
    }
    Ok(())
}

fn bad(message: impl Into<String>, fix: impl Into<String>) -> StepError {
    StepError::new("reach", message, fix)
}

fn is_region(r: &str) -> bool {
    let parts: Vec<&str> = r.split('-').collect();
    parts.len() >= 3
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        })
        && parts
            .last()
            .is_some_and(|p| p.chars().all(|c| c.is_ascii_digit()))
}

/// Accepts only `arn:aws:iam::<12 digits>:role/[path/]CoderhelmReadOnly...`.
pub fn check_role_arn(arn: &str) -> Result<(), StepError> {
    let fix = format!(
        "Paste the line the setup script printed. The role's name starts with {ROLE_PREFIX}."
    );
    let parts: Vec<&str> = arn.trim().splitn(6, ':').collect();
    let [p, partition, svc, region, account, resource] = parts.as_slice() else {
        return Err(bad("That isn't a role address.", fix));
    };
    let name = resource
        .strip_prefix("role/")
        .and_then(|r| r.rsplit('/').next())
        .unwrap_or("");
    let ok = *p == "arn"
        && *partition == "aws"
        && *svc == "iam"
        && region.is_empty()
        && account.len() == 12
        && account.chars().all(|c| c.is_ascii_digit())
        && (name.starts_with(ROLE_PREFIX) || name == LOG_READER_ROLE)
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+=,.@_-".contains(c));
    if ok {
        Ok(())
    } else {
        Err(bad("That isn't a Coderhelm read-only role.", fix))
    }
}

fn check_arn_in(
    arn: &str,
    service: &str,
    account: &str,
    region: &str,
    what: &str,
) -> Result<(), StepError> {
    let parts: Vec<&str> = arn.splitn(7, ':').collect();
    let ok = parts.len() >= 7
        && parts[0] == "arn"
        && parts[1] == "aws"
        && parts[2] == service
        && parts[3] == region
        && parts[4] == account;
    if ok {
        Ok(())
    } else {
        Err(bad(
            format!("The {what} isn't in the same AWS account and region as the role."),
            "Run the setup script again and paste the line it prints.",
        ))
    }
}

/// Checks a target's addresses fit together: one account, one region, and a
/// secret the role is allowed to read.
pub fn check_target(t: &AwsTarget) -> Result<(), StepError> {
    check_role_arn(&t.role_arn)?;
    if !is_region(&t.region) {
        return Err(bad(
            "That AWS region isn't valid.",
            "Use a region like us-east-1.",
        ));
    }
    if !t.cluster_arn.is_empty() || !t.secret_arn.is_empty() {
        check_arn_in(&t.cluster_arn, "rds", t.account(), &t.region, "database")?;
        check_arn_in(
            &t.secret_arn,
            "secretsmanager",
            t.account(),
            &t.region,
            "database password",
        )?;
        let parts: Vec<&str> = t.secret_arn.splitn(7, ':').collect();
        if parts.get(5) != Some(&"secret")
            || !parts.get(6).is_some_and(|n| n.starts_with(SECRET_PREFIX))
        {
            return Err(bad(
                "That database password isn't one the setup script made.",
                "Run the setup script again and choose the database.",
            ));
        }
        let db = &t.database;
        if db.is_empty()
            || db.len() > 63
            || !db
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(bad(
                "That database name isn't valid.",
                "Run the setup script again.",
            ));
        }
    }
    if !t.cluster_id.is_empty() || t.engine == "redshift" {
        check_warehouse(t)?;
    }
    Ok(())
}

fn valid_db_name(d: &str) -> bool {
    !d.is_empty()
        && d.len() <= 127
        && d.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// A Redshift target: a cluster name, the read-only user and nothing else,
/// and at least one database.
fn check_warehouse(t: &AwsTarget) -> Result<(), StepError> {
    let id = &t.cluster_id;
    let id_ok = !id.is_empty()
        && id.len() <= 63
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && id.starts_with(|c: char| c.is_ascii_lowercase());
    if t.engine != "redshift" || !id_ok {
        return Err(bad(
            "That warehouse name isn't valid.",
            "Run the setup script again and choose the warehouse.",
        ));
    }
    if t.db_user != DB_USER {
        return Err(bad(
            format!("Coderhelm only signs in to a warehouse as {DB_USER}."),
            "Run the setup script again and choose the warehouse.",
        ));
    }
    if t.databases.is_empty()
        || t.databases.len() > 50
        || !t.databases.iter().all(|d| valid_db_name(d))
    {
        return Err(bad(
            "The list of databases isn't valid.",
            "Run the setup script again.",
        ));
    }
    Ok(())
}

/// What the setup script prints for pasting back: `coderhelm:` and a short code.
pub fn parse_setup_code(code: &str) -> Result<AwsTarget, StepError> {
    let fix = "Copy the whole line that starts with coderhelm: and paste it here.";
    let raw = code
        .trim()
        .strip_prefix("coderhelm:")
        .ok_or_else(|| bad("That isn't the line from the setup script.", fix))?;
    let bytes = STANDARD
        .decode(raw.trim())
        .map_err(|_| bad("Part of the line is missing.", fix))?;
    let mut t: AwsTarget =
        serde_json::from_slice(&bytes).map_err(|_| bad("Part of the line is missing.", fix))?;
    t.external_id.clear();
    check_target(&t)?;
    Ok(t)
}

/// A new private ID for a team. Kept by Coderhelm; the team never chooses it.
pub fn new_external_id() -> String {
    format!("coderhelm-{}{}", ulid::Ulid::new(), ulid::Ulid::new()).to_lowercase()
}

pub fn trust_policy(our_account: &str, external_id: &str) -> serde_json::Value {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "AWS": format!("arn:aws:iam::{our_account}:root") },
            "Action": "sts:AssumeRole",
            "Condition": { "StringEquals": { "sts:ExternalId": external_id } }
        }]
    })
}

/// Reads logs. Masked values stay masked: unmasking is explicitly denied.
pub fn logs_policy() -> serde_json::Value {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:DescribeLogGroups",
                    "logs:GetLogGroupFields",
                    "logs:StartQuery",
                    "logs:GetQueryResults",
                    "logs:StopQuery"
                ],
                "Resource": "*"
            },
            {
                "Sid": "DescribeTasksBehindLogErrors",
                "Effect": "Allow",
                "Action": [
                    "ecs:ListClusters",
                    "ecs:ListServices",
                    "ecs:DescribeServices",
                    "ecs:DescribeTaskDefinition",
                    "ecs:ListTasks",
                    "ecs:DescribeTasks"
                ],
                "Resource": "*"
            },
            { "Sid": "NeverUnmask", "Effect": "Deny", "Action": "logs:Unmask", "Resource": "*" }
        ]
    })
}

/// Queries one database, inside transactions that can be started and rolled
/// back but never committed, as the read-only user whose password is the only
/// secret readable.
pub fn database_policy_template(account: &str) -> serde_json::Value {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "QueryOneDatabase",
                "Effect": "Allow",
                "Action": ["rds-data:BeginTransaction", "rds-data:ExecuteStatement", "rds-data:RollbackTransaction"],
                "Resource": "CLUSTER_ARN"
            },
            {
                "Sid": "ReadOnlyUserPassword",
                "Effect": "Allow",
                "Action": "secretsmanager:GetSecretValue",
                "Resource": format!("arn:aws:secretsmanager:*:{account}:secret:{SECRET_PREFIX}*")
            },
            { "Sid": "NeverCommit", "Effect": "Deny", "Action": ["rds-data:CommitTransaction", "rds-data:BatchExecuteStatement"], "Resource": "*" }
        ]
    })
}

/// Reads one Redshift warehouse: may sign in only as the read-only user, with
/// no password, and only run single statements and read their own results.
pub fn warehouse_policy_template(account: &str) -> serde_json::Value {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "SignInAsTheReadOnlyUser",
                "Effect": "Allow",
                "Action": "redshift:GetClusterCredentials",
                "Resource": [
                    format!("arn:aws:redshift:*:{account}:dbuser:RS_CLUSTER/{DB_USER}"),
                    format!("arn:aws:redshift:*:{account}:dbname:RS_CLUSTER/*")
                ]
            },
            {
                "Sid": "RunOneStatement",
                "Effect": "Allow",
                "Action": "redshift-data:ExecuteStatement",
                "Resource": format!("arn:aws:redshift:*:{account}:cluster:RS_CLUSTER")
            },
            {
                "Sid": "ReadOwnResults",
                "Effect": "Allow",
                "Action": ["redshift-data:DescribeStatement", "redshift-data:GetStatementResult", "redshift-data:CancelStatement"],
                "Resource": "*",
                "Condition": { "StringEquals": { "redshift-data:statement-owner-iam-userid": "${aws:userid}" } }
            },
            {
                "Sid": "NothingElse",
                "Effect": "Deny",
                "Action": ["redshift-data:BatchExecuteStatement", "redshift:CreateClusterUser", "redshift:GetClusterCredentialsWithIAM", "redshift:ModifyCluster*", "redshift:Delete*"],
                "Resource": "*"
            }
        ]
    })
}

/// The script a team pastes into AWS CloudShell. It creates (or updates) the
/// role, lets them pick a database to share, creates a database user that can
/// only read, and prints the line to paste back. It defines a function and
/// calls it on the last line, so the questions it asks read from the keyboard
/// rather than from the rest of the paste. The function runs in a subshell, so
/// a failed step stops the script without closing the person's terminal.
pub fn setup_script(our_account: &str, external_id: &str) -> String {
    let trust = trust_policy(our_account, external_id).to_string();
    let logs = logs_policy().to_string();
    let db = database_policy_template("ACCOUNT_ID").to_string();
    let wh = warehouse_policy_template("ACCOUNT_ID").to_string();
    format!(
        r#"coderhelm_setup() (
set -euo pipefail
ROLE={ROLE_PREFIX}
REGION="${{AWS_REGION:-us-east-1}}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "Setting up read-only access for Coderhelm in account $ACCOUNT ($REGION)..."
if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "$ROLE" --policy-document '{trust}'
else
  aws iam create-role --role-name "$ROLE" --description "Coderhelm: read-only access to logs and one database" --max-session-duration 3600 --assume-role-policy-document '{trust}' >/dev/null
fi
aws iam put-role-policy --role-name "$ROLE" --policy-name coderhelm-read-logs --policy-document '{logs}'
ROLE_ARN=$(aws iam get-role --role-name "$ROLE" --query Role.Arn --output text)
echo "Role ready: $ROLE_ARN (it can read logs; it can't change anything)."
DB_PART=""
CLUSTERS=$(aws rds describe-db-clusters --region "$REGION" --query "DBClusters[?Engine=='aurora-postgresql' && HttpEndpointEnabled].DBClusterIdentifier" --output text | tr '\t' '\n' | sed '/^$/d')
if [ -n "$CLUSTERS" ]; then
  echo
  echo "Databases Coderhelm can read from (PostgreSQL with the Data API on):"
  i=0; for c in $CLUSTERS; do i=$((i+1)); echo "  $i) $c"; done
  echo "  0) none for now"
  read -r -p "Which one should Coderhelm read? [0-$i]: " PICK </dev/tty
  if [ "${{PICK:-0}}" -gt 0 ] 2>/dev/null; then
    CLUSTER_ID=$(echo "$CLUSTERS" | sed -n "${{PICK}}p")
    if [ -z "$CLUSTER_ID" ]; then echo "There is no database number $PICK. Run the script again."; exit 1; fi
    CLUSTER_ARN=$(aws rds describe-db-clusters --region "$REGION" --db-cluster-identifier "$CLUSTER_ID" --query 'DBClusters[0].DBClusterArn' --output text)
    ADMIN_SECRET=$(aws rds describe-db-clusters --region "$REGION" --db-cluster-identifier "$CLUSTER_ID" --query 'DBClusters[0].MasterUserSecret.SecretArn' --output text)
    if [ -z "$ADMIN_SECRET" ] || [ "$ADMIN_SECRET" = "None" ]; then
      # The AWS query editor keeps the admin login under a standard name.
      RES_ID=$(aws rds describe-db-clusters --region "$REGION" --db-cluster-identifier "$CLUSTER_ID" --query 'DBClusters[0].DbClusterResourceId' --output text)
      ADMIN_USER=$(aws rds describe-db-clusters --region "$REGION" --db-cluster-identifier "$CLUSTER_ID" --query 'DBClusters[0].MasterUsername' --output text)
      ADMIN_SECRET=$(aws secretsmanager list-secrets --region "$REGION" --filters Key=name,Values="rds-db-credentials/$RES_ID/$ADMIN_USER/" --query 'SecretList[0].ARN' --output text)
    fi
    if [ -z "$ADMIN_SECRET" ] || [ "$ADMIN_SECRET" = "None" ]; then
      read -r -p "Paste the ARN of a secret with this database's admin login: " ADMIN_SECRET </dev/tty
    fi
    DB_NAME=$(aws rds describe-db-clusters --region "$REGION" --db-cluster-identifier "$CLUSTER_ID" --query 'DBClusters[0].DatabaseName' --output text)
    if [ -z "$DB_NAME" ] || [ "$DB_NAME" = "None" ]; then DB_NAME=postgres; fi
    read -r -p "Database name [$DB_NAME]: " ANSWER </dev/tty; DB_NAME="${{ANSWER:-$DB_NAME}}"
    PW=$(openssl rand -hex 24)
    run_sql() {{ aws rds-data execute-statement --region "$REGION" --resource-arn "$CLUSTER_ARN" --secret-arn "$ADMIN_SECRET" --database "$DB_NAME" --sql "$1" >/dev/null; }}
    echo "Creating the database user {DB_USER}, which can only read..."
    run_sql "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{DB_USER}') THEN CREATE ROLE {DB_USER} LOGIN; END IF; END \$\$"
    run_sql "ALTER ROLE {DB_USER} WITH LOGIN NOCREATEDB NOCREATEROLE PASSWORD '$PW'"
    run_sql "GRANT pg_read_all_data TO {DB_USER}"
    run_sql "ALTER ROLE {DB_USER} SET default_transaction_read_only = on"
    run_sql "ALTER ROLE {DB_USER} SET statement_timeout = '15s'"
    SECRET_NAME="{SECRET_PREFIX}$CLUSTER_ID"
    SECRET_VALUE="{{\"username\":\"{DB_USER}\",\"password\":\"$PW\"}}"
    if aws secretsmanager describe-secret --region "$REGION" --secret-id "$SECRET_NAME" >/dev/null 2>&1; then
      aws secretsmanager put-secret-value --region "$REGION" --secret-id "$SECRET_NAME" --secret-string "$SECRET_VALUE" >/dev/null
    else
      aws secretsmanager create-secret --region "$REGION" --name "$SECRET_NAME" --description "Coderhelm read-only database user" --secret-string "$SECRET_VALUE" >/dev/null
    fi
    SECRET_ARN=$(aws secretsmanager describe-secret --region "$REGION" --secret-id "$SECRET_NAME" --query ARN --output text)
    aws iam put-role-policy --role-name "$ROLE" --policy-name "coderhelm-read-database-$CLUSTER_ID" --policy-document "$(echo '{db}' | sed -e "s#CLUSTER_ARN#$CLUSTER_ARN#" -e "s#ACCOUNT_ID#$ACCOUNT#")"
    DB_PART=",\"cluster_arn\":\"$CLUSTER_ARN\",\"secret_arn\":\"$SECRET_ARN\",\"database\":\"$DB_NAME\""
    echo "Database user ready. It can read every table and change none."
  fi
fi
RS_PART=""
WAREHOUSES=$(aws redshift describe-clusters --region "$REGION" --query 'Clusters[].ClusterIdentifier' --output text 2>/dev/null | tr '\t' '\n' | sed '/^$/d')
if [ -n "$WAREHOUSES" ]; then
  echo
  echo "Redshift warehouses Coderhelm can read from:"
  i=0; for c in $WAREHOUSES; do i=$((i+1)); echo "  $i) $c"; done
  echo "  0) none for now"
  read -r -p "Which one should Coderhelm read? [0-$i]: " PICK </dev/tty
  if [ "${{PICK:-0}}" -gt 0 ] 2>/dev/null; then
    RS_ID=$(echo "$WAREHOUSES" | sed -n "${{PICK}}p")
    if [ -z "$RS_ID" ]; then echo "There is no warehouse number $PICK. Run the script again."; exit 1; fi
    RS_ADMIN=$(aws redshift describe-clusters --region "$REGION" --cluster-identifier "$RS_ID" --query 'Clusters[0].MasterUsername' --output text)
    RS_FIRST=$(aws redshift describe-clusters --region "$REGION" --cluster-identifier "$RS_ID" --query 'Clusters[0].DBName' --output text)
    rs_sql() {{
      SID=$(aws redshift-data execute-statement --region "$REGION" --cluster-identifier "$RS_ID" --db-user "$RS_ADMIN" --database "$1" --sql "$2" --query Id --output text)
      while true; do
        ST=$(aws redshift-data describe-statement --region "$REGION" --id "$SID" --query Status --output text)
        case "$ST" in FINISHED) return 0;; FAILED|ABORTED) aws redshift-data describe-statement --region "$REGION" --id "$SID" --query Error --output text >&2; return 1;; esac
        sleep 1
      done
    }}
    RS_DBS=$(aws redshift-data list-databases --region "$REGION" --cluster-identifier "$RS_ID" --db-user "$RS_ADMIN" --database "$RS_FIRST" --query 'Databases' --output text | tr '\t' '\n' | grep -v -E '^(padb_harvest|template0|template1|sys:.*|awsdatacatalog)$' | sed '/^$/d')
    echo "Creating the warehouse user {DB_USER}, which has no password and can only read..."
    rs_sql "$RS_FIRST" "CREATE USER {DB_USER} PASSWORD DISABLE" 2>/dev/null || true
    rs_sql "$RS_FIRST" "ALTER USER {DB_USER} NOCREATEDB NOCREATEUSER SYSLOG ACCESS RESTRICTED"
    rs_sql "$RS_FIRST" "ALTER USER {DB_USER} SET statement_timeout TO 15000"
    RS_JSON=""
    for D in $RS_DBS; do
      SCHEMAS=$(aws redshift-data list-schemas --region "$REGION" --cluster-identifier "$RS_ID" --db-user "$RS_ADMIN" --database "$D" --query 'Schemas' --output text | tr '\t' '\n' | grep -v -E '^(pg_catalog|information_schema|pg_internal|pg_auto_copy|pg_mv|pg_s3|catalog_history|pg_toast.*|pg_temp.*)$' | sed '/^$/d')
      for S in $SCHEMAS; do
        rs_sql "$D" "GRANT USAGE ON SCHEMA \"$S\" TO {DB_USER}" || continue
        rs_sql "$D" "GRANT SELECT ON ALL TABLES IN SCHEMA \"$S\" TO {DB_USER}" || true
        rs_sql "$D" "ALTER DEFAULT PRIVILEGES IN SCHEMA \"$S\" GRANT SELECT ON TABLES TO {DB_USER}" || true
      done
      RS_JSON="$RS_JSON${{RS_JSON:+,}}\"$D\""
      echo "  $D: can read, can't change"
    done
    aws iam put-role-policy --role-name "$ROLE" --policy-name "coderhelm-read-warehouse-$RS_ID" --policy-document "$(echo '{wh}' | sed -e "s#RS_CLUSTER#$RS_ID#g" -e "s#ACCOUNT_ID#$ACCOUNT#g")"
    RS_PART=",\"engine\":\"redshift\",\"cluster_id\":\"$RS_ID\",\"db_user\":\"{DB_USER}\",\"databases\":[$RS_JSON]"
    echo "Warehouse user ready."
  fi
fi
CODE=$(printf '{{"role_arn":"%s","region":"%s"%s%s}}' "$ROLE_ARN" "$REGION" "$DB_PART" "$RS_PART" | base64 | tr -d '\n')
echo
echo "Done. Copy this whole line and paste it into Coderhelm:"
echo
echo "coderhelm:$CODE"
echo
)
coderhelm_setup
"#
    )
}

/// Temporary keys for one session on a team's role.
#[derive(Clone)]
pub struct Session {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub region: String,
}

// Written by hand so keys never reach a log.
impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

fn session_name(actor: &str) -> String {
    let clean: String = actor
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "+=,.@-".contains(c) {
                c
            } else {
                '-'
            }
        })
        .collect();
    let name = format!("coderhelm-{clean}");
    crate::truncate_str(&name, 64).to_string()
}

/// Takes on a team's role. `actor` names the session in the team's AWS
/// records (`reviewer`, `planner`, a person's email).
pub async fn assume(
    sts: &aws_sdk_sts::Client,
    target: &AwsTarget,
    actor: &str,
) -> Result<Session, StepError> {
    check_target(target)?;
    let external_id = target.external_id.as_str();
    if external_id.is_empty() {
        return Err(StepError::new(
            "sign_in",
            "This source is missing its private ID.",
            "Connect it again.",
        ));
    }
    let out = sts
        .assume_role()
        .role_arn(&target.role_arn)
        .role_session_name(session_name(actor))
        .external_id(external_id)
        .duration_seconds(SESSION_SECONDS)
        .send()
        .await
        .map_err(|e| {
            let code = e.as_service_error().and_then(|s| s.meta().code()).unwrap_or("").to_string();
            let msg = e.as_service_error().and_then(|s| s.meta().message()).unwrap_or("").to_string();
            let why = if code.is_empty() {
                super::error_detail("", &aws_sdk_sts::error::DisplayErrorContext(&e).to_string())
            } else {
                super::error_detail(&code, &msg)
            };
            tracing::info!(code = %code, error = %why, "Could not take on a team's role");
            StepError::new(
                "sign_in",
                format!("Coderhelm couldn't use the role in your AWS account ({why})."),
                "Run the setup script again in AWS CloudShell. If someone changed or removed the role, the script puts it back.",
            )
        })?;
    let c = out
        .credentials()
        .ok_or_else(|| StepError::new("sign_in", "AWS returned no session.", "Try again."))?;
    Ok(Session {
        access_key_id: c.access_key_id().to_string(),
        secret_access_key: c.secret_access_key().to_string(),
        session_token: c.session_token().to_string(),
        region: target.region.clone(),
    })
}

/// True when the role can be used WITHOUT the team's private ID, which would
/// let anyone using Coderhelm reach it. Connecting is refused when it can.
pub async fn opens_without_private_id(sts: &aws_sdk_sts::Client, target: &AwsTarget) -> bool {
    sts.assume_role()
        .role_arn(&target.role_arn)
        .role_session_name("coderhelm-check")
        .duration_seconds(SESSION_SECONDS)
        .send()
        .await
        .is_ok()
}

pub fn logs_client(s: &Session) -> aws_sdk_cloudwatchlogs::Client {
    use aws_sdk_cloudwatchlogs::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_cloudwatchlogs::Client::from_conf(
        aws_sdk_cloudwatchlogs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(s.region.clone()))
            .credentials_provider(creds)
            .build(),
    )
}

/// S3 through the team's read-only role: only to read the test reports a
/// team pointed Coderhelm at (`crate::e2e::report_source`).
pub fn s3_client(s: &Session) -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(s.region.clone()))
            .credentials_provider(creds)
            .build(),
    )
}

pub fn rdsdata_client(s: &Session) -> aws_sdk_rdsdata::Client {
    use aws_sdk_rdsdata::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_rdsdata::Client::from_conf(
        aws_sdk_rdsdata::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(s.region.clone()))
            .credentials_provider(creds)
            .build(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROLE: &str = "arn:aws:iam::123456789012:role/CoderhelmReadOnly";

    #[test]
    fn log_groups_must_be_names_or_real_prefixes() {
        let v =
            |xs: &[&str]| check_log_groups(&xs.iter().map(|x| x.to_string()).collect::<Vec<_>>());
        for bad in [
            &["*"][..],
            &["/*"],
            &["/aw*"],
            &["a*b"],
            &["/aws/lambda/x y"],
            &[""],
            &["/aws/**"],
        ] {
            assert!(v(bad).is_err(), "{bad:?}");
        }
        assert!(v(&[
            "/aws/lambda/orders",
            "/ecs/api-*",
            "/aws/rds/cluster/x/postgresql"
        ])
        .is_ok());
        let mut t = db_target();
        t.log_groups = vec!["*".into(), "/aw*".into()];
        assert!(!t.allows_log_group("/aws/rds/cluster/prod/postgresql"));
        t.log_groups = vec!["/ecs/api-*".into()];
        assert!(t.allows_log_group("/ecs/api-orders"));
    }

    fn db_target() -> AwsTarget {
        AwsTarget {
            role_arn: ROLE.into(),
            external_id: String::new(),
            region: "us-east-1".into(),
            log_groups: vec![],
            cluster_arn: "arn:aws:rds:us-east-1:123456789012:cluster:app-db".into(),
            secret_arn: "arn:aws:secretsmanager:us-east-1:123456789012:secret:coderhelm-readonly-app-db-AbCdEf".into(),
            database: "app".into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_warehouse_signs_in_only_as_the_read_only_user() {
        let ok = AwsTarget {
            role_arn: ROLE.into(),
            region: "us-east-1".into(),
            engine: "redshift".into(),
            cluster_id: "analytics-dw".into(),
            db_user: DB_USER.into(),
            databases: vec!["dev".into(), "marts".into()],
            ..Default::default()
        };
        assert!(check_target(&ok).is_ok());
        let admin = AwsTarget {
            db_user: "awsuser".into(),
            ..ok.clone()
        };
        assert!(check_target(&admin).unwrap_err().message.contains(DB_USER));
        let none = AwsTarget {
            databases: vec![],
            ..ok.clone()
        };
        assert!(check_target(&none).is_err());
        let bad_name = AwsTarget {
            cluster_id: "Analytics; drop".into(),
            ..ok.clone()
        };
        assert!(check_target(&bad_name).is_err());
        let bad_db = AwsTarget {
            databases: vec!["dev; drop".into()],
            ..ok
        };
        assert!(check_target(&bad_db).is_err());
    }

    #[test]
    fn only_coderhelm_roles_are_accepted() {
        assert!(check_role_arn(ROLE).is_ok());
        assert!(check_role_arn("arn:aws:iam::123456789012:role/team/CoderhelmReadOnly-2").is_ok());
        assert!(check_role_arn("arn:aws:iam::123456789012:role/CoderHelmLogReader").is_ok());
        assert!(check_role_arn("arn:aws:iam::123456789012:role/CoderHelmLogReaderAdmin").is_err());
        for bad in [
            "arn:aws:iam::123456789012:role/Admin",
            "arn:aws:iam::123456789012:user/CoderhelmReadOnly",
            "arn:aws:iam::12345:role/CoderhelmReadOnly",
            "arn:aws:iam:us-east-1:123456789012:role/CoderhelmReadOnly",
            "arn:aws:sts::123456789012:role/CoderhelmReadOnly",
            "CoderhelmReadOnly",
            "",
        ] {
            assert!(check_role_arn(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_database_must_sit_with_its_role() {
        assert!(check_target(&db_target()).is_ok());
        let mut t = db_target();
        t.cluster_arn = t.cluster_arn.replace("123456789012", "999999999999");
        assert!(check_target(&t).is_err());
        let mut t = db_target();
        t.secret_arn =
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:prod-admin-AbCdEf".into();
        assert!(check_target(&t)
            .unwrap_err()
            .message
            .contains("isn't one the setup script made"));
        let mut t = db_target();
        t.region = "us-west-2".into();
        assert!(check_target(&t).is_err());
        let mut t = db_target();
        t.database = "app; drop".into();
        assert!(check_target(&t).is_err());
    }

    #[test]
    fn the_pasted_line_round_trips() {
        let t = db_target();
        let code = format!(
            "coderhelm:{}",
            STANDARD.encode(serde_json::to_vec(&t).unwrap())
        );
        assert_eq!(parse_setup_code(&format!("  {code}\n")).unwrap(), t);
        assert!(parse_setup_code("coderhelm:!!!").is_err());
        assert!(parse_setup_code(&code.replace("coderhelm:", "")).is_err());
        // A private ID inside the pasted line is ignored.
        let sneaky = format!("coderhelm:{}", STANDARD.encode(br#"{"role_arn":"arn:aws:iam::123456789012:role/CoderhelmReadOnly","region":"us-east-1","external_id":"someone-elses"}"#));
        assert_eq!(parse_setup_code(&sneaky).unwrap().external_id, "");
        let logs_only = format!("coderhelm:{}", STANDARD.encode(br#"{"role_arn":"arn:aws:iam::123456789012:role/CoderhelmReadOnly","region":"eu-west-1"}"#));
        let t = parse_setup_code(&logs_only).unwrap();
        assert_eq!(t.region, "eu-west-1");
        assert!(t.cluster_arn.is_empty());
    }

    #[test]
    fn log_groups_match_by_name_or_prefix() {
        let t = AwsTarget {
            log_groups: vec!["/ecs/api".into(), "/aws/lambda/app-*".into()],
            ..db_target()
        };
        assert!(t.allows_log_group("/ecs/api"));
        assert!(!t.allows_log_group("/ecs/api-worker"));
        assert!(t.allows_log_group("/aws/lambda/app-orders"));
        assert!(!t.allows_log_group("/aws/lambda/other"));
    }

    #[test]
    fn the_script_carries_the_private_id_and_only_read_permissions() {
        let s = setup_script("654654210434", "coderhelm-abc");
        assert!(s.contains("\"sts:ExternalId\":\"coderhelm-abc\""));
        assert!(s.contains("arn:aws:iam::654654210434:root"));
        assert!(s.contains("logs:StartQuery"));
        assert!(s.contains("\"Action\":\"logs:Unmask\""));
        assert!(s.contains("rds-data:CommitTransaction"));
        assert!(s.contains("default_transaction_read_only = on"));
        assert!(s.trim_end().ends_with("coderhelm_setup"));
        // one permission per database, so adding a second keeps the first
        assert!(s.contains("--policy-name \"coderhelm-read-database-$CLUSTER_ID\""));
        assert!(s.contains("PASSWORD DISABLE"));
        assert!(s.contains("redshift-data:BatchExecuteStatement"));
        // nothing in the policies can write
        for p in [
            logs_policy(),
            database_policy_template("1"),
            warehouse_policy_template("1"),
        ] {
            for st in p["Statement"].as_array().unwrap() {
                if st["Effect"] == "Allow" {
                    let actions = st["Action"].to_string();
                    for w in [
                        "Put", "Create", "Delete", "Update", "Commit", "Batch", "Unmask",
                    ] {
                        assert!(!actions.contains(w), "{actions}");
                    }
                }
            }
        }
        assert_eq!(
            session_name("a person@example.com"),
            "coderhelm-a-person@example.com"
        );
        assert!(new_external_id().starts_with("coderhelm-"));
    }

    #[test]
    fn the_script_is_valid_bash() {
        use std::io::Write;
        let mut child = std::process::Command::new("bash")
            .arg("-n")
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("bash is installed");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(setup_script("654654210434", "coderhelm-abc").as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Writes the setup script to the file named by SETUP_SCRIPT_OUT, for
    /// running it by hand. Needs SETUP_ACCOUNT and SETUP_EXTERNAL_ID.
    #[test]
    #[ignore = "writes a file"]
    fn write_setup_script() {
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} not set"));
        std::fs::write(
            env("SETUP_SCRIPT_OUT"),
            setup_script(&env("SETUP_ACCOUNT"), &env("SETUP_EXTERNAL_ID")),
        )
        .unwrap();
    }
}
