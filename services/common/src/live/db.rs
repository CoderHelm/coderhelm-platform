//! Reads a team's PostgreSQL database through the AWS Data API, under the
//! rules in [`super`].
//!
//! Three locks, each enough on its own to stop a write:
//! 1. The lookup is parsed and refused unless it is one plain SELECT
//!    ([`super::sql`]); what is sent is printed from what was parsed.
//! 2. It runs inside a transaction that is set read-only before anything else
//!    and is always rolled back. The role in the team's account may begin and
//!    roll back transactions but is denied committing them.
//! 3. The database user can only read: connecting is refused when it could
//!    change any table, and the setup script makes the database itself default
//!    that user to read-only.

use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use super::api::Check;
use super::aws::{rdsdata_client, AwsTarget, Session};
use super::redact::{hidden_terms, redact_all};
use super::sql::{check_read_only_sql, CheckedSql};
use super::tools::{BoundParam, ParamValue};
use super::StepError;

/// Longest a lookup may run, enforced by the database itself.
const STATEMENT_TIMEOUT: &str = "8s";
/// Longest we wait for the whole exchange.
const TOTAL_TIMEOUT: Duration = Duration::from_secs(20);
/// Most columns kept in the description of a database.
const MAX_SCHEMA_COLUMNS: usize = 10_000;

type Row = Map<String, Value>;

fn data_api_error<E>(e: &aws_sdk_rdsdata::error::SdkError<E>) -> StepError
where
    E: aws_sdk_rdsdata::error::ProvideErrorMetadata + std::error::Error + 'static,
{
    let code = e.as_service_error().and_then(|s| s.code()).unwrap_or("");
    let msg = e.as_service_error().and_then(|s| s.message()).unwrap_or("");
    // No service code: the request never got an answer (a timeout, a
    // connection or credentials problem). Its own description says which.
    let transport = code
        .is_empty()
        .then(|| aws_sdk_rdsdata::error::DisplayErrorContext(e).to_string());
    let err = classify_data_api_error(code, msg, transport.as_deref());
    if err.step != "read" {
        tracing::warn!(code, error = %err.message, "Database lookup failed");
    }
    err
}

/// What a Data API failure means for the person or agent who asked. Every
/// message carries the underlying error class and text (secrets and account
/// details removed), so a cause is never hidden behind a generic sentence.
fn classify_data_api_error(code: &str, msg: &str, transport: Option<&str>) -> StepError {
    let why = super::error_detail(code, transport.unwrap_or(msg));
    match code {
        "AccessDeniedException" | "ForbiddenException" | "UnrecognizedClientException" => StepError::new(
            "sign_in",
            format!("The role in your AWS account isn't allowed to query this database ({why})."),
            "Run the setup script again and choose this database.",
        ),
        "DatabaseResumingException" => StepError::new(
            "reach",
            format!("The database is waking up ({why})."),
            "Try again in a minute.",
        ),
        "DatabaseNotFoundException" | "DatabaseUnavailableException" | "ServiceUnavailableError" | "InternalServerErrorException" => StepError::new(
            "reach",
            format!("We couldn't reach the database ({why})."),
            "Check that the database is running and its Data API is on, then try again.",
        ),
        "BadRequestException" | "DatabaseErrorException" if msg.contains("password authentication") || msg.contains("secret") => StepError::new(
            "sign_in",
            format!("The database didn't accept Coderhelm's read-only user ({why})."),
            "Run the setup script again and choose this database; it resets that user's password.",
        ),
        "BadRequestException" | "DatabaseErrorException" if msg.contains("Communications link failure") || msg.contains("is not currently available") => StepError::new(
            "reach",
            format!("We couldn't reach the database ({why})."),
            "Check that the database is running and its Data API is on, then try again.",
        ),
        "BadRequestException" | "DatabaseErrorException" => StepError::new(
            "read",
            format!("The database couldn't run that ({why})."),
            "Check table and column names with live_describe before trying again.",
        ),
        "ValidationException" if msg.contains("Cannot find parameter") => StepError::new(
            "read",
            format!("The database read part of the SQL as a parameter ({why})."),
            "Every :name in the SQL must be a declared input; write casts as CAST(x AS type).",
        ),
        "ValidationException" => StepError::new(
            "read",
            format!("The Data API refused the request ({why})."),
            "Check the SQL, then try again.",
        ),
        "StatementTimeoutException" => StepError::new(
            "read",
            format!("That lookup took too long, so the database stopped it ({why})."),
            "Narrow it: filter on an indexed column, a shorter date range or fewer rows.",
        ),
        _ => StepError::new(
            "reach",
            format!("We couldn't reach the database ({why})."),
            "Check that the Data API is on for this database, then try again.",
        ),
    }
}

const READ_ONLY_PROOF: &str = "SELECT current_setting('transaction_read_only') AS ro";

fn not_read_only() -> StepError {
    StepError::new(
        "read_only",
        "The database didn't confirm this lookup was read-only, so it wasn't run.",
        "Nothing was sent. Run the connection check again.",
    )
}

/// True only when the database answered that the transaction is read-only.
fn confirms_read_only(records: Option<&str>) -> bool {
    records
        .and_then(|r| serde_json::from_str::<Vec<Row>>(r).ok())
        .and_then(|rows| rows.into_iter().next())
        .and_then(|row| row.get("ro").and_then(|v| v.as_str()).map(|v| v == "on"))
        .unwrap_or(false)
}

/// Runs `sql` inside a read-only transaction that is always rolled back.
/// `sql` must already be checked, or be one of this module's own reads.
async fn read_in_rolled_back_transaction(
    client: &aws_sdk_rdsdata::Client,
    t: &AwsTarget,
    sql: &str,
) -> Result<Vec<Row>, StepError> {
    read_with_params(client, t, sql, &[]).await
}

fn data_api_params(params: &[BoundParam]) -> Vec<aws_sdk_rdsdata::types::SqlParameter> {
    use aws_sdk_rdsdata::types::{Field, SqlParameter, TypeHint};
    params
        .iter()
        .map(|p| {
            let (value, hint) = match &p.value {
                ParamValue::Text(v) => (Field::StringValue(v.clone()), None),
                ParamValue::Int(n) => (Field::LongValue(*n), None),
                ParamValue::Date(d) => (Field::StringValue(d.clone()), Some(TypeHint::Date)),
            };
            SqlParameter::builder()
                .name(&p.name)
                .value(value)
                .set_type_hint(hint)
                .build()
        })
        .collect()
}

/// [`read_in_rolled_back_transaction`] with values bound to named
/// parameters (`:name`) by the Data API, so no value is ever part of the SQL
/// text.
async fn read_with_params(
    client: &aws_sdk_rdsdata::Client,
    t: &AwsTarget,
    sql: &str,
    params: &[BoundParam],
) -> Result<Vec<Row>, StepError> {
    let tx = client
        .begin_transaction()
        .resource_arn(&t.cluster_arn)
        .secret_arn(&t.secret_arn)
        .database(&t.database)
        .send()
        .await
        .map_err(|e| data_api_error(&e))?;
    let tx_id = tx.transaction_id().unwrap_or_default().to_string();

    let run = async {
        for setup in [
            // Must come first in the transaction to take effect.
            "SET TRANSACTION READ ONLY".to_string(),
            format!("SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'"),
        ] {
            client
                .execute_statement()
                .resource_arn(&t.cluster_arn)
                .secret_arn(&t.secret_arn)
                .database(&t.database)
                .transaction_id(&tx_id)
                .sql(setup)
                .send()
                .await
                .map_err(|e| data_api_error(&e))?;
        }
        // Proof from the database itself, before anything else runs, that this
        // transaction can't write. No proof, no lookup.
        let guard = client
            .execute_statement()
            .resource_arn(&t.cluster_arn)
            .secret_arn(&t.secret_arn)
            .database(&t.database)
            .transaction_id(&tx_id)
            .format_records_as(aws_sdk_rdsdata::types::RecordsFormatType::Json)
            .sql(READ_ONLY_PROOF)
            .send()
            .await
            .map_err(|e| data_api_error(&e))?;
        if !confirms_read_only(guard.formatted_records()) {
            return Err(not_read_only());
        }
        // The Data API takes any `:word` as a parameter, even inside quotes;
        // only the declared ones may look like that.
        let names: Vec<&str> = params.iter().map(|p| p.name.as_str()).collect();
        let sql = super::sql::for_rds_data_api(sql, &names);
        let out = client
            .execute_statement()
            .resource_arn(&t.cluster_arn)
            .secret_arn(&t.secret_arn)
            .database(&t.database)
            .transaction_id(&tx_id)
            .format_records_as(aws_sdk_rdsdata::types::RecordsFormatType::Json)
            .sql(sql)
            .set_parameters((!params.is_empty()).then(|| data_api_params(params)))
            .send()
            .await
            .map_err(|e| data_api_error(&e))?;
        let rows: Vec<Row> = out
            .formatted_records()
            .and_then(|r| serde_json::from_str(r).ok())
            .unwrap_or_default();
        Ok::<_, StepError>(rows)
    };
    let result = tokio::time::timeout(TOTAL_TIMEOUT, run)
        .await
        .unwrap_or_else(|_| {
            Err(StepError::new(
                "read",
                "That lookup took too long.",
                "Ask for fewer rows, or filter on an indexed column such as an id.",
            ))
        });

    // Rolled back whatever happened, so nothing a statement did can last.
    if let Err(e) = client
        .rollback_transaction()
        .resource_arn(&t.cluster_arn)
        .secret_arn(&t.secret_arn)
        .transaction_id(&tx_id)
        .send()
        .await
    {
        tracing::warn!(error = %e, "Rolling back a lookup transaction failed; the database ends it on timeout");
    }
    result
}

/// The result of one database lookup.
#[derive(Debug, Clone)]
pub struct DbLookup {
    /// The statement as parsed and sent (without the row cap).
    pub statement: String,
    pub tables: Vec<String>,
    pub rows: usize,
    /// Pretty-printed rows with hidden values replaced.
    pub result: String,
    pub hidden_count: usize,
    pub ms: u128,
}

/// Runs one checked read. `hidden` are column names whose values must never
/// leave this function.
pub async fn run_sql(
    session: &Session,
    t: &AwsTarget,
    sql: &str,
    hidden: &[String],
    max_result_chars: usize,
) -> Result<DbLookup, StepError> {
    let checked: CheckedSql = check_read_only_sql(sql, hidden)?;
    let client = rdsdata_client(session);
    let started = Instant::now();
    let rows = read_in_rolled_back_transaction(&client, t, &checked.text).await?;
    let ms = started.elapsed().as_millis();

    let mut value = Value::Array(rows.into_iter().map(Value::Object).collect());
    let count = value.as_array().map(|a| a.len()).unwrap_or(0);
    // Columns returned by `*` carry their real names; free text can still hold
    // an email or a token. Both are hidden here.
    let hidden_count = redact_all(&mut value, &hidden_terms(hidden));
    let pretty = serde_json::to_string_pretty(&value).unwrap_or_default();
    Ok(DbLookup {
        statement: checked.statement,
        tables: checked.tables,
        rows: count,
        result: crate::head_tail_str(&pretty, max_result_chars),
        hidden_count,
        ms,
    })
}

/// Runs a reviewed tool's checked SQL with its inputs bound as parameters.
/// `text` is what [`super::tools::check_tool_sql`] returned.
pub async fn run_tool_sql(
    session: &Session,
    t: &AwsTarget,
    text: &str,
    params: &[BoundParam],
    hidden: &[String],
    max_result_chars: usize,
) -> Result<(usize, String, usize, u128), StepError> {
    let client = rdsdata_client(session);
    let started = Instant::now();
    let rows = read_with_params(&client, t, text, params).await?;
    let ms = started.elapsed().as_millis();
    let count = rows.len();
    let mut value = Value::Array(rows.into_iter().map(Value::Object).collect());
    let hidden_count = redact_all(&mut value, &hidden_terms(hidden));
    let pretty = serde_json::to_string_pretty(&value).unwrap_or_default();
    Ok((
        count,
        crate::head_tail_str(&pretty, max_result_chars),
        hidden_count,
        ms,
    ))
}

const ACCESS_CHECK: &str = "SELECT current_user AS who, \
     (r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolbypassrls OR r.rolreplication) AS powerful, \
     EXISTS (SELECT 1 FROM pg_roles a WHERE a.rolname IN ('rds_superuser', 'rds_replication', 'rds_password') \
             AND pg_has_role(current_user, a.oid, 'member')) AS admin_member, \
     (SELECT count(*) FROM information_schema.tables t \
       WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema') AND t.table_type = 'BASE TABLE' \
         AND (has_table_privilege(format('%I.%I', t.table_schema, t.table_name), 'INSERT') \
           OR has_table_privilege(format('%I.%I', t.table_schema, t.table_name), 'UPDATE') \
           OR has_table_privilege(format('%I.%I', t.table_schema, t.table_name), 'DELETE') \
           OR has_table_privilege(format('%I.%I', t.table_schema, t.table_name), 'TRUNCATE'))) AS writable_tables, \
     current_setting('default_transaction_read_only') AS read_only_default, \
     version() AS version \
     FROM pg_roles r WHERE r.rolname = current_user";

const SCHEMA_READ: &str =
    "SELECT table_schema AS s, table_name AS t, column_name AS c, data_type AS d \
     FROM information_schema.columns \
     WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
     ORDER BY table_schema, table_name, ordinal_position LIMIT 10000";

/// A database's tables and columns as text blocks, one per table, so the
/// agents can search it the same way as an API's description.
pub fn schema_text(rows: &[Row]) -> (String, usize) {
    let mut out = String::new();
    let mut current = String::new();
    let mut tables = 0;
    for r in rows.iter().take(MAX_SCHEMA_COLUMNS) {
        let get = |k: &str| r.get(k).and_then(|v| v.as_str()).unwrap_or("");
        let name = if get("s") == "public" {
            get("t").to_string()
        } else {
            format!("{}.{}", get("s"), get("t"))
        };
        if name != current {
            if !current.is_empty() {
                out.push('\n');
            }
            out.push_str(&format!("table {name}\n"));
            current = name;
            tables += 1;
        }
        out.push_str(&format!("  {}: {}\n", get("c"), get("d")));
    }
    (out, tables)
}

const L_ROLE: &str = "Use the role in your AWS account";
const L_SIGN_IN: &str = "Sign in to the database";
const L_READ_ONLY: &str = "The database user can only read";
const L_READ: &str = "Read the list of tables";

/// Why the signed-in user could change something, or `None` when it can only
/// read. Anything the database didn't answer counts against it.
fn access_problem(access: &Row) -> Option<StepError> {
    let who = access
        .get("who")
        .and_then(|v| v.as_str())
        .unwrap_or("This user");
    let flag = |k: &str| access.get(k).and_then(|v| v.as_bool()).unwrap_or(true);
    let writable = access
        .get("writable_tables")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    let fix =
        "Run the setup script and choose this database: it creates a user that can only read.";
    if flag("powerful") || flag("admin_member") {
        return Some(StepError::new(
            "read_only",
            format!("{who} is an admin user, so it could change data."),
            fix,
        ));
    }
    if writable != 0 {
        let n = if writable < 0 {
            "some".to_string()
        } else {
            writable.to_string()
        };
        return Some(StepError::new(
            "read_only",
            format!("{who} can change {n} tables."),
            fix,
        ));
    }
    None
}

/// Checks again that the user Coderhelm signs in as can't change any table.
/// The agents run this before their first lookup in a database, so a user
/// given more rights after connecting is refused, not used.
pub async fn verify_read_only_user(session: &Session, t: &AwsTarget) -> Result<(), StepError> {
    let client = rdsdata_client(session);
    let rows = read_in_rolled_back_transaction(&client, t, ACCESS_CHECK).await?;
    let access = rows.into_iter().next().ok_or_else(not_read_only)?;
    match access_problem(&access) {
        Some(mut e) => {
            e.message = format!(
                "{} Coderhelm won't use this database until that's fixed.",
                e.message
            );
            Err(e)
        }
        None => Ok(()),
    }
}

/// Reads the database's tables and columns again, as the text the agents
/// search, and how many tables it holds. The user is re-checked as read-only
/// first, the same as before any lookup.
pub async fn read_schema(session: &Session, t: &AwsTarget) -> Result<(String, usize), StepError> {
    verify_read_only_user(session, t).await?;
    let client = rdsdata_client(session);
    let rows = read_in_rolled_back_transaction(&client, t, SCHEMA_READ).await?;
    Ok(schema_text(&rows))
}

/// The outcome of checking a database before it is connected.
pub struct DbReport {
    pub ok: bool,
    pub checks: Vec<Check>,
    pub tables: Option<usize>,
    pub schema: Option<String>,
}

fn pass(id: &'static str, label: &'static str, detail: impl Into<String>) -> Check {
    Check {
        id,
        label,
        ok: true,
        detail: detail.into(),
        fix: None,
    }
}
fn fail(id: &'static str, label: &'static str, e: &StepError) -> Check {
    Check {
        id,
        label,
        ok: false,
        detail: e.message.clone(),
        fix: Some(e.fix.clone()),
    }
}

/// Signs in, proves the user can only read, and reads the list of tables.
/// `session` is the outcome of taking on the team's role.
pub async fn connect_checks(session: Result<Session, StepError>, t: &AwsTarget) -> DbReport {
    let mut checks = Vec::new();
    let failed = |checks| DbReport {
        ok: false,
        checks,
        tables: None,
        schema: None,
    };
    let session = match session {
        Ok(s) => {
            checks.push(pass(
                "role",
                L_ROLE,
                "Coderhelm can use the role, and only with your private ID.",
            ));
            s
        }
        Err(e) => {
            checks.push(fail("role", L_ROLE, &e));
            return failed(checks);
        }
    };
    let client = rdsdata_client(&session);
    let access = match read_in_rolled_back_transaction(&client, t, ACCESS_CHECK).await {
        Ok(rows) if !rows.is_empty() => rows.into_iter().next().unwrap_or_default(),
        Ok(_) => {
            let e = StepError::new(
                "sign_in",
                "The database didn't say who Coderhelm is signed in as.",
                "Run the setup script again.",
            );
            checks.push(fail("sign_in", L_SIGN_IN, &e));
            return failed(checks);
        }
        Err(e) => {
            checks.push(fail("sign_in", L_SIGN_IN, &e));
            return failed(checks);
        }
    };
    let version = access.get("version").and_then(|v| v.as_str()).unwrap_or("");
    if !version.contains("PostgreSQL") {
        let e = StepError::new(
            "sign_in",
            "This database isn't PostgreSQL.",
            "Only PostgreSQL databases can be connected for now.",
        );
        checks.push(fail("sign_in", L_SIGN_IN, &e));
        return failed(checks);
    }
    let who = access
        .get("who")
        .and_then(|v| v.as_str())
        .unwrap_or("?")
        .to_string();
    checks.push(pass("sign_in", L_SIGN_IN, format!("Signed in as {who}.")));

    if let Some(e) = access_problem(&access) {
        checks.push(fail("read_only", L_READ_ONLY, &e));
        return failed(checks);
    }
    let default_ro = access.get("read_only_default").and_then(|v| v.as_str()) == Some("on");
    checks.push(pass(
        "read_only",
        L_READ_ONLY,
        if default_ro {
            "It can't change any table, the database defaults it to read-only, and every lookup is rolled back."
        } else {
            "It can't change any table, and every lookup runs read-only and is rolled back."
        },
    ));

    match read_in_rolled_back_transaction(&client, t, SCHEMA_READ).await {
        Ok(rows) => {
            let (text, tables) = schema_text(&rows);
            checks.push(pass(
                "read",
                L_READ,
                format!("It can read {tables} tables."),
            ));
            DbReport {
                ok: true,
                checks,
                tables: Some(tables),
                schema: Some(text),
            }
        }
        Err(e) => {
            checks.push(fail("read", L_READ, &e));
            failed(checks)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn schema_is_one_block_per_table() {
        let rows: Vec<Row> = [
            json!({"s": "public", "t": "orders", "c": "id", "d": "integer"}),
            json!({"s": "public", "t": "orders", "c": "status", "d": "text"}),
            json!({"s": "billing", "t": "invoices", "c": "id", "d": "uuid"}),
        ]
        .into_iter()
        .map(|v| v.as_object().unwrap().clone())
        .collect();
        let (text, tables) = schema_text(&rows);
        assert_eq!(tables, 2);
        assert_eq!(
            text,
            "table orders\n  id: integer\n  status: text\n\ntable billing.invoices\n  id: uuid\n"
        );
        // searchable the same way as an API description
        let found = super::super::graphql::search_schema(&text, "invoices", 1000);
        assert!(found.contains("billing.invoices") && !found.contains("orders"));
    }

    #[test]
    fn our_own_reads_pass_the_read_only_check() {
        // The checks run through the same rolled-back transaction, and they
        // should be readable by the checker too, so a future edit can't slip
        // a write into them.
        for sql in [ACCESS_CHECK, SCHEMA_READ] {
            check_read_only_sql(sql, &[]).unwrap_or_else(|e| panic!("{e}"));
        }
    }

    #[test]
    fn every_failure_says_what_went_wrong() {
        // SQL errors are the agent's to fix, with the database's own words.
        let e = classify_data_api_error(
            "BadRequestException",
            "ERROR: relation \"orders\" does not exist\n  Position: 15",
            None,
        );
        assert_eq!(e.step, "read");
        assert!(
            e.message.contains("relation \"orders\" does not exist"),
            "{}",
            e.message
        );
        // A request that never got an answer keeps its own description.
        let e =
            classify_data_api_error("", "", Some("dispatch failure: io error: connection reset"));
        assert_eq!(e.step, "reach");
        assert!(e.message.contains("connection reset"), "{}", e.message);
        // Codes nobody planned for still carry the code and the message.
        let e = classify_data_api_error("ThrottlingException", "Rate exceeded", None);
        assert!(
            e.message.contains("ThrottlingException: Rate exceeded"),
            "{}",
            e.message
        );
        // A stopped database is a reach problem, not a SQL problem.
        let e = classify_data_api_error("BadRequestException", "Communications link failure", None);
        assert_eq!(e.step, "reach");
        assert!(e.message.contains("Communications link failure"));
        // Sign-in failures say which code, without the account behind it.
        let e = classify_data_api_error(
            "AccessDeniedException",
            "User arn:aws:sts::123456789012:assumed-role/x is not authorized",
            None,
        );
        assert_eq!(e.step, "sign_in");
        assert!(e.message.contains("AccessDeniedException") && !e.message.contains("123456789012"));
        // The parameter error names the parameter.
        let e = classify_data_api_error("ValidationException", "Cannot find parameter: 3", None);
        assert!(e.message.contains("Cannot find parameter: 3"));
        for (code, msg) in [
            ("DatabaseResumingException", "resuming"),
            ("StatementTimeoutException", "timeout"),
            ("ValidationException", "bad input"),
        ] {
            let e = classify_data_api_error(code, msg, None);
            assert!(e.message.contains(code), "{code}: {}", e.message);
        }
    }

    #[test]
    fn only_a_clear_yes_counts_as_read_only() {
        assert!(confirms_read_only(Some(r#"[{"ro":"on"}]"#)));
        for no in [
            Some(r#"[{"ro":"off"}]"#),
            Some("[]"),
            Some("not json"),
            Some(r#"[{"x":"on"}]"#),
            None,
        ] {
            assert!(!confirms_read_only(no), "{no:?}");
        }
        check_read_only_sql(READ_ONLY_PROOF, &[]).unwrap();
    }

    #[test]
    fn any_write_right_or_missing_answer_is_a_problem() {
        let row = |v: Value| v.as_object().unwrap().clone();
        let clean = json!({"who": "reader", "powerful": false, "admin_member": false, "writable_tables": 0});
        assert!(access_problem(&row(clean.clone())).is_none());
        let mut admin = clean.clone();
        admin["admin_member"] = json!(true);
        assert!(access_problem(&row(admin))
            .unwrap()
            .message
            .contains("admin"));
        let mut writes = clean.clone();
        writes["writable_tables"] = json!(3);
        assert!(access_problem(&row(writes))
            .unwrap()
            .message
            .contains("can change 3 tables"));
        // An answer that is missing is treated as the worst case.
        assert!(access_problem(&row(json!({"who": "reader"}))).is_some());
    }

    #[tokio::test]
    async fn a_write_never_reaches_the_database() {
        let s = Session {
            access_key_id: "x".into(),
            secret_access_key: "x".into(),
            session_token: "x".into(),
            region: "us-east-1".into(),
        };
        let t = AwsTarget::default();
        let e = run_sql(&s, &t, "DELETE FROM orders", &[], 1000)
            .await
            .unwrap_err();
        assert!(e.is_refusal());
    }

    /// Against a real database set up by the setup script. Set LIVE_SETUP_CODE
    /// (the `coderhelm:` line) and LIVE_EXTERNAL_ID, then run with
    /// `cargo test -- --ignored real_database`. Tries to write five ways; every
    /// one must fail. Point it only at a database where a stray empty table
    /// named coderhelm_probe would be harmless, or set LIVE_READ_ONLY_PROBE to
    /// only check and read.
    #[tokio::test]
    #[ignore = "needs a real AWS account"]
    async fn real_database_reads_and_refuses_every_write() {
        use super::super::aws;
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} not set"));
        let conf = aws_config::load_from_env().await;
        let sts = aws_sdk_sts::Client::new(&conf);
        let mut t = aws::parse_setup_code(&env("LIVE_SETUP_CODE")).unwrap();
        t.external_id = env("LIVE_EXTERNAL_ID");
        assert!(!aws::opens_without_private_id(&sts, &t).await);
        let session = aws::assume(&sts, &t, "integration-test").await.unwrap();

        let report = connect_checks(Ok(session.clone()), &t).await;
        for c in &report.checks {
            println!("check {} ok={} {}", c.id, c.ok, c.detail);
        }
        assert!(report.ok);
        verify_read_only_user(&session, &t).await.unwrap();
        let first = report
            .schema
            .as_deref()
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .trim_start_matches("table ")
            .to_string();
        let r = run_sql(
            &session,
            &t,
            &format!("SELECT count(*) AS n FROM {first}"),
            &[],
            2000,
        )
        .await
        .unwrap();
        println!("read ok: {} rows in {}ms", r.rows, r.ms);
        // On a production database, stop at reading: LIVE_READ_ONLY_PROBE=1.
        if std::env::var("LIVE_READ_ONLY_PROBE").is_ok() {
            return;
        }

        // 1. The checker refuses before sending.
        assert!(run_sql(
            &session,
            &t,
            "CREATE TABLE coderhelm_probe (x int)",
            &[],
            100
        )
        .await
        .unwrap_err()
        .is_refusal());
        let client = rdsdata_client(&session);
        // 2. Inside our transaction, even unchecked text can't write.
        let e =
            read_in_rolled_back_transaction(&client, &t, "CREATE TABLE coderhelm_probe (x int)")
                .await
                .unwrap_err();
        println!("2 read-only transaction: {}", e.message);
        assert!(e.message.contains("read-only transaction"));
        // 3. No transaction at all: the user defaults to read-only.
        let e = client
            .execute_statement()
            .resource_arn(&t.cluster_arn)
            .secret_arn(&t.secret_arn)
            .database(&t.database)
            .sql("CREATE TABLE coderhelm_probe (x int)")
            .send()
            .await
            .unwrap_err();
        let m = e
            .as_service_error()
            .and_then(|s| aws_sdk_rdsdata::error::ProvideErrorMetadata::message(s))
            .unwrap_or("")
            .to_string();
        println!("3 no transaction: {m}");
        assert!(m.contains("read-only"));
        // 4. Asking for a writable transaction: the user has no right to write.
        let tx = client
            .begin_transaction()
            .resource_arn(&t.cluster_arn)
            .secret_arn(&t.secret_arn)
            .database(&t.database)
            .send()
            .await
            .unwrap();
        let tx_id = tx.transaction_id().unwrap().to_string();
        let _ = client
            .execute_statement()
            .resource_arn(&t.cluster_arn)
            .secret_arn(&t.secret_arn)
            .database(&t.database)
            .transaction_id(&tx_id)
            .sql("SET TRANSACTION READ WRITE")
            .send()
            .await;
        let e = client
            .execute_statement()
            .resource_arn(&t.cluster_arn)
            .secret_arn(&t.secret_arn)
            .database(&t.database)
            .transaction_id(&tx_id)
            .sql(format!("DELETE FROM {first} WHERE false"))
            .send()
            .await
            .unwrap_err();
        let m = e
            .as_service_error()
            .and_then(|s| aws_sdk_rdsdata::error::ProvideErrorMetadata::message(s))
            .unwrap_or("")
            .to_string();
        println!("4 writable transaction: {m}");
        assert!(m.contains("permission denied") || m.contains("read-only"));
        // 5. The role may not commit anything.
        let e = client
            .commit_transaction()
            .resource_arn(&t.cluster_arn)
            .secret_arn(&t.secret_arn)
            .transaction_id(&tx_id)
            .send()
            .await
            .unwrap_err();
        let code = e
            .as_service_error()
            .and_then(|s| aws_sdk_rdsdata::error::ProvideErrorMetadata::code(s))
            .unwrap_or("")
            .to_string();
        println!("5 commit: {code}");
        assert!(code.contains("AccessDenied") || code.contains("Forbidden"));
        let _ = client
            .rollback_transaction()
            .resource_arn(&t.cluster_arn)
            .secret_arn(&t.secret_arn)
            .transaction_id(&tx_id)
            .send()
            .await;
    }
}
