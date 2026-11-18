//! Reaching a team's AWS account: its logs and its databases.
//!
//! The team creates a role in its own account that Coderhelm may use, with a
//! private ID only that team is given (so nobody else using Coderhelm can point
//! it at their role), and with permissions that only read. The role and all
//! its permissions come from one CloudFormation template
//! (`infra/cfn/coderhelm-log-reader.yaml`, `CoderHelmLogReader`); a database
//! or a warehouse also needs a read-only user inside it, which the script a
//! team pastes into AWS CloudShell makes ([`setup_script`]) before it creates
//! or updates that stack. It prints a line they paste back into Coderhelm
//! ([`parse_setup_code`]). Roles the older script made (`CoderhelmReadOnly`)
//! are still accepted, and move to the stack's role
//! (`crate::role_move`).
//!
//! Every use takes a fresh, 15-minute session on that role, named after the
//! agent using it, so the team's own AWS records show who looked at what.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};

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

/// The script a team pastes into AWS CloudShell to read a database or a
/// warehouse. It makes only what a CloudFormation stack can't: the
/// read-only database user (and its password secret) inside the database
/// they pick. The role's permissions are never written by the script: it
/// creates, or updates, CoderHelm's role stack (`CoderHelmLogReader`) with
/// this release's template (`template_url`), naming the database or the
/// warehouse in the stack's DatabaseClusters or Warehouses parameter and
/// keeping every other parameter as it was. So every read the role has is
/// in that one template, and a later release reaches it through the same
/// one-click update as every other read. It prints the line to paste back
/// into CoderHelm. It defines a function and calls it on the last line, so
/// the questions it asks read from the keyboard rather than from the rest
/// of the paste. The function runs in a subshell, so a failed step stops
/// the script without closing the person's terminal.
pub fn setup_script(
    our_account: &str,
    external_id: &str,
    template_url: &str,
    trusted_prefix: &str,
) -> String {
    let stack = crate::log_reader::ROLE_STACK;
    format!(
        r#"coderhelm_setup() (
set -euo pipefail
ROLE={LOG_READER_ROLE}
STACK={stack}
TEMPLATE_URL='{template_url}'
REGION="${{AWS_REGION:-us-east-1}}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "Setting up read-only access for Coderhelm in account $ACCOUNT ($REGION)..."
DB_ARN=""; RS_ID=""; DB_PART=""; RS_PART=""
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
    DB_ARN="$CLUSTER_ARN"
    DB_PART=",\"cluster_arn\":\"$CLUSTER_ARN\",\"secret_arn\":\"$SECRET_ARN\",\"database\":\"$DB_NAME\""
    echo "Database user ready. It can read every table and change none."
  fi
fi
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
    RS_PART=",\"engine\":\"redshift\",\"cluster_id\":\"$RS_ID\",\"db_user\":\"{DB_USER}\",\"databases\":[$RS_JSON]"
    echo "Warehouse user ready."
  fi
fi
# The role's permissions: only through CoderHelm's role stack, with this
# release's template. A list parameter keeps what it named and adds the new one.
add_to() {{ if [ -z "$1" ] || [ "$1" = "None" ]; then echo "$2"; elif [ -z "$2" ] || echo ",$1," | grep -q ",$2,"; then echo "$1"; else echo "$1,$2"; fi; }}
STACK_REGION=""
for R in "$REGION" $(aws ec2 describe-regions --region "$REGION" --query 'Regions[].RegionName' --output text 2>/dev/null); do
  if aws cloudformation describe-stacks --region "$R" --stack-name "$STACK" >/dev/null 2>&1; then STACK_REGION="$R"; break; fi
done
if [ -n "$STACK_REGION" ]; then
  q() {{ aws cloudformation describe-stacks --region "$STACK_REGION" --stack-name "$STACK" --query "$1" --output text; }}
  KEYS=$(q 'Stacks[0].Parameters[].ParameterKey')
  DBS=$(add_to "$(q "Stacks[0].Parameters[?ParameterKey=='DatabaseClusters'].ParameterValue | [0]")" "$DB_ARN")
  WHS=$(add_to "$(q "Stacks[0].Parameters[?ParameterKey=='Warehouses'].ParameterValue | [0]")" "$RS_ID")
  P="[{{\"ParameterKey\":\"DatabaseClusters\",\"ParameterValue\":\"$DBS\"}},{{\"ParameterKey\":\"Warehouses\",\"ParameterValue\":\"$WHS\"}}"
  for K in $KEYS; do case "$K" in DatabaseClusters|Warehouses) ;; *) P="$P,{{\"ParameterKey\":\"$K\",\"UsePreviousValue\":true}}";; esac; done
  P="$P]"
  echo "Updating CoderHelm's role stack $STACK ($STACK_REGION) to this release's template..."
  if OUT=$(aws cloudformation update-stack --region "$STACK_REGION" --stack-name "$STACK" --template-url "$TEMPLATE_URL" --capabilities CAPABILITY_NAMED_IAM --parameters "$P" 2>&1); then
    aws cloudformation wait stack-update-complete --region "$STACK_REGION" --stack-name "$STACK"
  elif ! echo "$OUT" | grep -q "No updates are to be performed"; then
    echo "$OUT"; echo "The role stack wasn't updated; see its Events tab in CloudFormation."; exit 1
  fi
elif aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  echo "The role $ROLE exists, but not from CoderHelm's stack $STACK, so its permissions can't be kept current."
  echo "Delete the role (or the stack that made it), then run this again."; exit 1
else
  echo "Creating CoderHelm's role stack $STACK ($REGION)..."
  P="[{{\"ParameterKey\":\"ExternalId\",\"ParameterValue\":\"{external_id}\"}},{{\"ParameterKey\":\"CoderHelmAccountId\",\"ParameterValue\":\"{our_account}\"}},{{\"ParameterKey\":\"TrustedRolePrefix\",\"ParameterValue\":\"{trusted_prefix}\"}},{{\"ParameterKey\":\"DatabaseClusters\",\"ParameterValue\":\"$DB_ARN\"}},{{\"ParameterKey\":\"Warehouses\",\"ParameterValue\":\"$RS_ID\"}}]"
  aws cloudformation create-stack --region "$REGION" --stack-name "$STACK" --template-url "$TEMPLATE_URL" --capabilities CAPABILITY_NAMED_IAM --parameters "$P" >/dev/null
  aws cloudformation wait stack-create-complete --region "$REGION" --stack-name "$STACK"
fi
ROLE_ARN=$(aws iam get-role --role-name "$ROLE" --query Role.Arn --output text)
echo "Role ready: $ROLE_ARN (it reads; the only change it may make is an Athena query's results, only when you turn WAF logs on)."
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

/// On a self-hosted install, the gateway's and worker's own role
/// (infra/lib/own-account-read.ts): its stack gives it this release's
/// read-only permissions in the install's own account.
pub const OWN_ROLE_ENV: &str = "INSTALL_OWN_ROLE_ARN";

/// The function's own role, when its stack gave it the read-only
/// permissions for the install's own account.
pub fn install_read_role() -> Option<String> {
    std::env::var(OWN_ROLE_ENV)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn account_of(role_arn: &str) -> &str {
    role_arn.split(':').nth(4).unwrap_or("")
}

/// `role_arn` is in the same account as the install's own role `own`.
pub fn in_install_account(role_arn: &str, own: Option<&str>) -> bool {
    own.is_some_and(|o| {
        let a = account_of(o);
        a.len() == 12 && a == account_of(role_arn)
    })
}

/// A session on the function's own role (its environment's credentials),
/// in `region`. `None` outside Lambda.
pub fn own_session(region: &str) -> Option<Session> {
    let get = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    Some(Session {
        access_key_id: get("AWS_ACCESS_KEY_ID")?,
        secret_access_key: get("AWS_SECRET_ACCESS_KEY")?,
        session_token: get("AWS_SESSION_TOKEN").unwrap_or_default(),
        region: region.to_string(),
    })
}

/// A session on the team's own role (the one connected for the account,
/// taken on with the team's private ID). It is the only session a read of a
/// team's AWS data accepts: it is made only by [`assume`] and
/// [`assume_exact`], so the install's own role ([`own_session`]) or the
/// function's ambient credentials can never read a team's data in its name.
/// A run and the re-check of a waiting run (`role_wait`) both take it with
/// [`assume`], so they ask AWS as the same principal.
#[derive(Clone, Debug)]
pub struct TeamSession {
    session: Session,
    role_arn: String,
}

impl TeamSession {
    /// The team's role this session is on.
    pub fn role_arn(&self) -> &str {
        &self.role_arn
    }

    /// The same team role in another region.
    pub fn in_region(&self, region: &str) -> TeamSession {
        TeamSession {
            session: Session {
                region: region.to_string(),
                ..self.session.clone()
            },
            role_arn: self.role_arn.clone(),
        }
    }

    /// The keys, for a client a team read builds.
    pub fn into_session(self) -> Session {
        self.session
    }
}

impl std::ops::Deref for TeamSession {
    type Target = Session;
    fn deref(&self) -> &Session {
        &self.session
    }
}

/// Pure: the role or user AWS refused, from an AccessDenied message
/// ("User: arn:aws:sts::111122223333:assumed-role/NAME/session is not
/// authorized to perform: ..."): `NAME`. `None` when the message names none.
pub fn refused_principal(message: &str) -> Option<String> {
    let arn = message.split_whitespace().find(|w| {
        w.starts_with("arn:aws")
            && (w.contains(":assumed-role/") || w.contains(":user/") || w.contains(":role/"))
    })?;
    let arn = arn.trim_end_matches([',', '.', ';']);
    let rest = arn
        .split_once(":assumed-role/")
        .or_else(|| arn.split_once(":role/"))
        .or_else(|| arn.split_once(":user/"))?
        .1;
    let name = rest.split('/').next().unwrap_or("");
    (!name.is_empty()).then(|| name.to_string())
}

/// Pure: AWS refused a principal that isn't the team's role `role_arn`
/// (CoderHelm read with the wrong role): the refused principal's name. A
/// refusal of the team's own role, or one that names nobody, is `None`.
pub fn refused_other_principal(message: &str, role_arn: &str) -> Option<String> {
    let name = refused_principal(message)?;
    let team = role_arn.rsplit('/').next().unwrap_or("");
    (name != team).then_some(name)
}

/// Takes on a team's role. `actor` names the session in the team's AWS
/// records (`reviewer`, `planner`, a person's email). Always the team's
/// own role, also in a self-hosted install's own account: a read of a
/// team's data is made, and refused, as the team's role, and the role's
/// one-click update stack is what adds a read.
pub async fn assume(
    sts: &aws_sdk_sts::Client,
    target: &AwsTarget,
    actor: &str,
) -> Result<TeamSession, StepError> {
    check_target(target)?;
    assume_exact(sts, target, actor).await
}

/// Takes on exactly `target`'s role (already checked by the caller).
pub async fn assume_exact(
    sts: &aws_sdk_sts::Client,
    target: &AwsTarget,
    actor: &str,
) -> Result<TeamSession, StepError> {
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
    Ok(TeamSession {
        session: Session {
            access_key_id: c.access_key_id().to_string(),
            secret_access_key: c.secret_access_key().to_string(),
            session_token: c.session_token().to_string(),
            region: target.region.clone(),
        },
        role_arn: target.role_arn.clone(),
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

    const OWN: &str = "arn:aws:iam::111122223333:role/coderhelm-prod-gateway-1a2b3c4d";

    fn reader(account: &str) -> AwsTarget {
        AwsTarget {
            role_arn: format!("arn:aws:iam::{account}:role/{LOG_READER_ROLE}"),
            external_id: "team-id".into(),
            region: "us-east-1".into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_refusal_names_the_principal_aws_refused() {
        let worker = "User: arn:aws:sts::111122223333:assumed-role/coderhelm-prod-ServiceRole-ABC123/coderhelm-prod-worker is not authorized to perform: wafv2:ListWebACLs on resource: arn:aws:wafv2:us-east-1:111122223333:regional/webacl/*";
        let team = reader("111122223333").role_arn;
        assert_eq!(
            refused_other_principal(worker, &team).as_deref(),
            Some("coderhelm-prod-ServiceRole-ABC123")
        );
        let as_team = "User: arn:aws:sts::111122223333:assumed-role/CoderHelmLogReader/coderhelm-x is not authorized to perform: wafv2:ListWebACLs";
        assert_eq!(refused_other_principal(as_team, &team), None);
        assert_eq!(refused_other_principal("Access denied", &team), None);
        assert!(!in_install_account(
            "arn:aws:iam::111122223333:role/x",
            None
        ));
        assert!(!in_install_account(
            "arn:aws:iam::111122223333:role/x",
            Some("not-an-arn")
        ));
        assert!(in_install_account(&team, Some(OWN)));
    }

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

    const TEMPLATE_URL: &str =
        "https://coderhelm-public.s3.amazonaws.com/launch/v1.2.3/coderhelm-log-reader.yaml";

    fn script() -> String {
        setup_script(
            "654654210434",
            "0b5f1a5e-0000-4000-8000-000000000000",
            TEMPLATE_URL,
            "",
        )
    }

    /// The script writes no permission itself: every read is the role
    /// stack's, created or updated with this release's template, keeping
    /// every parameter but the database or warehouse it adds.
    #[test]
    fn the_script_only_creates_or_updates_the_role_stack() {
        let s = script();
        for no in [
            "put-role-policy",
            "create-role",
            "update-assume-role-policy",
            "attach-role-policy",
            "coderhelm-read-waf",
        ] {
            assert!(!s.contains(no), "{no}");
        }
        assert!(s.contains("STACK=CoderHelmLogReader\n"));
        assert!(s.contains(&format!("TEMPLATE_URL='{TEMPLATE_URL}'")));
        assert!(s.contains("aws cloudformation update-stack"));
        assert!(s.contains("aws cloudformation create-stack"));
        assert!(s.contains("\\\"UsePreviousValue\\\":true"));
        assert!(s.contains("ParameterKey\\\":\\\"DatabaseClusters"));
        assert!(s.contains("ParameterKey\\\":\\\"Warehouses"));
        assert!(s.contains("\\\"ParameterValue\\\":\\\"0b5f1a5e-0000-4000-8000-000000000000\\\""));
        assert!(s.contains("No updates are to be performed"));
        // A role the stack didn't make is never patched by hand.
        assert!(s.contains("not from CoderHelm's stack"));
        // Inside the database: a user that can only read.
        assert!(s.contains("default_transaction_read_only = on"));
        assert!(s.contains("PASSWORD DISABLE"));
        assert!(s.trim_end().ends_with("coderhelm_setup"));
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
            .write_all(script().as_bytes())
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
            setup_script(
                &env("SETUP_ACCOUNT"),
                &env("SETUP_EXTERNAL_ID"),
                &crate::public_urls::log_reader_template_url(),
                "",
            ),
        )
        .unwrap();
    }
}
