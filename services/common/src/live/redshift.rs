//! Reads a team's Redshift data warehouse through the Redshift Data API, under
//! the rules in [`super`]. One source covers every database in the warehouse
//! the team shared.
//!
//! Four locks, each enough on its own to stop a change to existing data:
//! 1. The lookup is parsed and refused unless it is one plain SELECT
//!    ([`super::sql`]); what is sent is printed from what was parsed.
//! 2. It runs as a user with no password that can only read: connecting, and
//!    the first lookup in each database during every agent run, check that
//!    the user is not an admin and cannot change any table.
//! 3. It runs inside a session whose transaction is begun READ ONLY before
//!    anything else, which the warehouse must confirm, and which is always
//!    rolled back. This also stops creating tables, which Redshift lets every
//!    user do in `public` unless a team has revoked it.
//! 4. The role in the team's account may only sign in as that one user and
//!    run single statements; batches and every other way of signing in are
//!    denied.

use std::time::{Duration, Instant};

use serde_json::{Map, Number, Value};

use super::api::Check;
use super::aws::{AwsTarget, Session};
use super::redact::{hidden_terms, redact_all};
use super::sql::{check_read_only_sql, CheckedSql};
use super::tools::{BoundParam, ParamValue};
use super::StepError;

/// Longest we wait for one statement before cancelling it.
const WAIT: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(300);
/// How long the warehouse keeps a lookup's session open when idle.
const SESSION_SECONDS: i32 = 60;
/// Most columns kept per database in the description of the warehouse.
const MAX_SCHEMA_COLUMNS: usize = 3_000;

type Row = Map<String, Value>;

pub fn client(s: &Session) -> aws_sdk_redshiftdata::Client {
    use aws_sdk_redshiftdata::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        Some(s.session_token.clone()),
        None,
        "coderhelm-team-role",
    );
    aws_sdk_redshiftdata::Client::from_conf(
        aws_sdk_redshiftdata::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(s.region.clone()))
            .credentials_provider(creds)
            .build(),
    )
}

fn api_error<E>(e: &aws_sdk_redshiftdata::error::SdkError<E>) -> StepError
where
    E: aws_sdk_redshiftdata::error::ProvideErrorMetadata + std::error::Error + 'static,
{
    let code = e.as_service_error().and_then(|s| s.code()).unwrap_or("");
    let msg = e.as_service_error().and_then(|s| s.message()).unwrap_or("");
    let transport = code
        .is_empty()
        .then(|| aws_sdk_redshiftdata::error::DisplayErrorContext(e).to_string());
    let err = classify_api_error(code, msg, transport.as_deref());
    if err.step != "read" {
        tracing::warn!(code, error = %err.message, "Warehouse lookup failed");
    }
    err
}

/// What a Redshift Data API failure means, always with the underlying error
/// class and text (secrets and account details removed).
fn classify_api_error(code: &str, msg: &str, transport: Option<&str>) -> StepError {
    let why = super::error_detail(code, transport.unwrap_or(msg));
    match code {
        "AccessDeniedException" | "UnrecognizedClientException" => StepError::new(
            "sign_in",
            format!("The role in your AWS account isn't allowed to query this warehouse ({why})."),
            "Run the setup script again and choose this warehouse.",
        ),
        "ValidationException" | "ExecuteStatementException" => StepError::new(
            "read",
            format!("The warehouse couldn't run that ({why})."),
            "Check table and column names with live_describe before trying again.",
        ),
        _ => StepError::new(
            "reach",
            format!("We couldn't reach the warehouse ({why})."),
            "Check the warehouse is running, then try again.",
        ),
    }
}

fn field_value(f: &aws_sdk_redshiftdata::types::Field) -> Value {
    use aws_sdk_redshiftdata::types::Field;
    match f {
        Field::StringValue(s) => Value::String(s.clone()),
        Field::LongValue(n) => Value::Number((*n).into()),
        Field::DoubleValue(d) => Number::from_f64(*d)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::BooleanValue(b) => Value::Bool(*b),
        Field::IsNull(_) => Value::Null,
        // Binary columns are never shown.
        Field::BlobValue(_) => Value::String("[binary]".into()),
        _ => Value::Null,
    }
}

/// One statement sent, waited for, and (when it returns rows) read.
struct Done {
    rows: Vec<Row>,
    session: Option<String>,
}

fn failed(why: &str) -> StepError {
    StepError::new(
        "read",
        format!(
            "The warehouse couldn't run that: {}",
            crate::truncate_str(why, 300)
        ),
        "Check table and column names with the description of this warehouse.",
    )
}

/// Sends one statement, either opening a session (`open` = the database) or
/// inside an open one, and waits for it.
async fn exec(
    client: &aws_sdk_redshiftdata::Client,
    t: &AwsTarget,
    open: Option<&str>,
    session: Option<&str>,
    sql: &str,
    params: &[BoundParam],
    started: Instant,
) -> Result<Done, StepError> {
    let mut req = client
        .execute_statement()
        .sql(sql)
        .statement_name("coderhelm-lookup");
    if !params.is_empty() {
        // Values travel as parameters, never as SQL text.
        let ps = params.iter().filter_map(|p| {
            let v = match &p.value {
                ParamValue::Text(v) | ParamValue::Date(v) => v.clone(),
                ParamValue::Int(n) => n.to_string(),
            };
            aws_sdk_redshiftdata::types::SqlParameter::builder()
                .name(&p.name)
                .value(v)
                .build()
                .ok()
        });
        req = req.set_parameters(Some(ps.collect()));
    }
    req = match (open, session) {
        (Some(db), _) => req
            .cluster_identifier(&t.cluster_id)
            .db_user(&t.db_user)
            .database(db)
            .session_keep_alive_seconds(SESSION_SECONDS),
        (None, Some(sid)) => req.session_id(sid),
        (None, None) => return Err(failed("no session")),
    };
    let out = req.send().await.map_err(|e| api_error(&e))?;
    let id = out.id().unwrap_or_default().to_string();
    let session = out
        .session_id()
        .map(str::to_string)
        .or(session.map(str::to_string));

    use aws_sdk_redshiftdata::types::StatusString;
    let has_rows = loop {
        tokio::time::sleep(POLL).await;
        let d = client
            .describe_statement()
            .id(&id)
            .send()
            .await
            .map_err(|e| api_error(&e))?;
        match d.status() {
            Some(StatusString::Finished) => break d.has_result_set() == Some(true),
            Some(StatusString::Failed) | Some(StatusString::Aborted) => {
                return Err(failed(d.error().unwrap_or("it failed")));
            }
            _ if started.elapsed() > WAIT => {
                let _ = client.cancel_statement().id(&id).send().await;
                return Err(StepError::new(
                    "read",
                    "That lookup took too long, so it was cancelled.",
                    "Ask for fewer rows, or filter on a sort or distribution key.",
                ));
            }
            _ => {}
        }
    };
    if !has_rows {
        return Ok(Done {
            rows: Vec::new(),
            session,
        });
    }
    let r = client
        .get_statement_result()
        .id(&id)
        .send()
        .await
        .map_err(|e| api_error(&e))?;
    let names: Vec<String> = r
        .column_metadata()
        .iter()
        .enumerate()
        .map(|(i, c)| {
            c.label()
                .or(c.name())
                .map(str::to_string)
                .unwrap_or_else(|| format!("col{i}"))
        })
        .collect();
    let rows = r
        .records()
        .iter()
        .map(|rec| {
            names
                .iter()
                .cloned()
                .zip(rec.iter().map(field_value))
                .collect::<Row>()
        })
        .collect();
    Ok(Done { rows, session })
}

const READ_ONLY_PROOF: &str = "SELECT current_setting('transaction_read_only') AS ro";

/// True only when the warehouse answered that the transaction is read-only.
fn confirms_read_only(rows: &[Row]) -> bool {
    rows.first()
        .and_then(|r| r.get("ro"))
        .and_then(|v| v.as_str())
        .map(|v| v == "on")
        .unwrap_or(false)
}

/// Runs `sql` in `database` as the read-only user, inside a transaction that
/// is set read-only before anything else, proven read-only by the warehouse,
/// and always rolled back. `sql` must already be checked, or be one of this
/// module's own reads.
async fn run(
    client: &aws_sdk_redshiftdata::Client,
    t: &AwsTarget,
    database: &str,
    sql: &str,
) -> Result<Vec<Row>, StepError> {
    run_with_params(client, t, database, sql, &[]).await
}

/// [`run`] with values bound to named parameters (`:name`).
async fn run_with_params(
    client: &aws_sdk_redshiftdata::Client,
    t: &AwsTarget,
    database: &str,
    sql: &str,
    params: &[BoundParam],
) -> Result<Vec<Row>, StepError> {
    let started = Instant::now();
    let begun = exec(
        client,
        t,
        Some(database),
        None,
        "BEGIN READ ONLY",
        &[],
        started,
    )
    .await?;
    let Some(sid) = begun.session else {
        return Err(failed("the warehouse opened no session"));
    };
    let result = async {
        let proof = exec(client, t, None, Some(&sid), READ_ONLY_PROOF, &[], started).await?;
        if !confirms_read_only(&proof.rows) {
            return Err(StepError::new(
                "read_only",
                "The warehouse didn't confirm this lookup was read-only, so it wasn't run.",
                "Nothing was sent. Run the connection check again.",
            ));
        }
        exec(client, t, None, Some(&sid), sql, params, started)
            .await
            .map(|d| d.rows)
    }
    .await;
    // Rolled back whatever happened, so nothing a statement did can last.
    if let Err(e) = exec(client, t, None, Some(&sid), "ROLLBACK", &[], Instant::now()).await {
        tracing::warn!(error = %e.message, "Rolling back a warehouse lookup failed; the session ends on its own");
    }
    result
}

/// The result of one warehouse lookup.
#[derive(Debug, Clone)]
pub struct WarehouseLookup {
    pub statement: String,
    pub database: String,
    pub tables: Vec<String>,
    pub rows: usize,
    pub result: String,
    pub hidden_count: usize,
    pub ms: u128,
}

/// Which shared database a lookup goes to: the one named, or the only one.
pub fn pick_database<'a>(t: &'a AwsTarget, asked: Option<&str>) -> Result<&'a str, StepError> {
    let asked = asked.map(str::trim).filter(|a| !a.is_empty());
    match asked {
        None if t.databases.len() == 1 => Ok(&t.databases[0]),
        None => Err(StepError::new(
            "read_only",
            format!("Name the database to read: {}.", t.databases.join(", ")),
            "Nothing was sent.",
        )),
        Some(a) => t
            .databases
            .iter()
            .find(|d| d.eq_ignore_ascii_case(a))
            .map(String::as_str)
            .ok_or_else(|| {
                StepError::new(
                    "read_only",
                    format!(
                        "{a} isn't one of the databases this team shared ({}).",
                        t.databases.join(", ")
                    ),
                    "Nothing was sent.",
                )
            }),
    }
}

/// Runs one checked read in one of the shared databases.
pub async fn run_sql(
    session: &Session,
    t: &AwsTarget,
    database: Option<&str>,
    sql: &str,
    hidden: &[String],
    max_result_chars: usize,
) -> Result<WarehouseLookup, StepError> {
    let checked: CheckedSql = check_read_only_sql(sql, hidden)?;
    let db = pick_database(t, database)?.to_string();
    let started = Instant::now();
    let rows = run(&client(session), t, &db, &checked.text).await?;
    let ms = started.elapsed().as_millis();
    let count = rows.len();
    let mut value = Value::Array(rows.into_iter().map(Value::Object).collect());
    let hidden_count = redact_all(&mut value, &hidden_terms(hidden));
    let pretty = serde_json::to_string_pretty(&value).unwrap_or_default();
    Ok(WarehouseLookup {
        statement: checked.statement,
        database: db,
        tables: checked.tables,
        rows: count,
        result: crate::head_tail_str(&pretty, max_result_chars),
        hidden_count,
        ms,
    })
}

/// Runs a reviewed tool's checked SQL in one shared database, with its
/// inputs bound as parameters.
pub async fn run_tool_sql(
    session: &Session,
    t: &AwsTarget,
    database: &str,
    text: &str,
    params: &[BoundParam],
    hidden: &[String],
    max_result_chars: usize,
) -> Result<(usize, String, usize, u128), StepError> {
    let db = pick_database(t, Some(database))?.to_string();
    let started = Instant::now();
    let rows = run_with_params(&client(session), t, &db, text, params).await?;
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
     (SELECT usesuper OR usecreatedb FROM pg_user WHERE usename = current_user) AS powerful, \
     (SELECT count(*) FROM information_schema.tables t \
       WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal') AND t.table_schema NOT LIKE 'pg\\_%' \
         AND t.table_type = 'BASE TABLE' \
         AND CASE WHEN has_schema_privilege(t.table_schema, 'USAGE') THEN \
               has_table_privilege(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name), 'INSERT') \
               OR has_table_privilege(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name), 'UPDATE') \
               OR has_table_privilege(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name), 'DELETE') \
             ELSE false END) AS writable_tables, \
     version() AS version";

const SCHEMA_READ: &str =
    "SELECT table_schema AS s, table_name AS t, column_name AS c, data_type AS d \
     FROM information_schema.columns \
     WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal') \
     ORDER BY table_schema, table_name, ordinal_position LIMIT 3000";

/// Why the user could change something in `database`, or `None`.
fn access_problem(access: &Row, database: &str) -> Option<StepError> {
    let who = access
        .get("who")
        .and_then(|v| v.as_str())
        .unwrap_or("This user");
    let powerful = access
        .get("powerful")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let writable = access
        .get("writable_tables")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    let fix =
        "Run the setup script and choose this warehouse: it creates a user that can only read.";
    if powerful {
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
            format!("{who} can change {n} tables in {database}."),
            fix,
        ));
    }
    None
}

/// Checks again that the user can't change anything in `database`. The agents
/// run this before their first lookup in each database during a run.
pub async fn verify_read_only_user(
    session: &Session,
    t: &AwsTarget,
    database: &str,
) -> Result<(), StepError> {
    let rows = run(&client(session), t, database, ACCESS_CHECK).await?;
    let access = rows.into_iter().next().ok_or_else(|| {
        StepError::new(
            "read_only",
            "The warehouse didn't say who Coderhelm is signed in as.",
            "Nothing was sent.",
        )
    })?;
    match access_problem(&access, database) {
        Some(mut e) => {
            e.message = format!(
                "{} Coderhelm won't use this warehouse until that's fixed.",
                e.message
            );
            Err(e)
        }
        None => Ok(()),
    }
}

/// Reads the tables and columns of every shared database again, each
/// database's user re-checked as read-only first. Returns the text the agents
/// search and the number of tables.
pub async fn read_schema(session: &Session, t: &AwsTarget) -> Result<(String, usize), StepError> {
    let c = client(session);
    let mut schema = String::new();
    let mut tables = 0;
    for db in &t.databases {
        verify_read_only_user(session, t, db).await?;
        let rows = run(&c, t, db, SCHEMA_READ).await?;
        let (text, n) = schema_text(db, &rows);
        if !schema.is_empty() && !text.is_empty() {
            schema.push('\n');
        }
        schema.push_str(&text);
        tables += n;
    }
    Ok((schema, tables))
}

/// Tables and columns of every shared database, one text block per table, so
/// the agents can search them like an API's description.
pub fn schema_text(database: &str, rows: &[Row]) -> (String, usize) {
    let mut out = String::new();
    let mut current = String::new();
    let mut tables = 0;
    for r in rows.iter().take(MAX_SCHEMA_COLUMNS) {
        let get = |k: &str| r.get(k).and_then(|v| v.as_str()).unwrap_or("");
        let name = format!("{}.{}", get("s"), get("t"));
        if name != current {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&format!("table {name} (database {database})\n"));
            current = name;
            tables += 1;
        }
        out.push_str(&format!("  {}: {}\n", get("c"), get("d")));
    }
    (out, tables)
}

/// The outcome of checking a warehouse before it is connected.
pub struct WarehouseReport {
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

const L_SIGN_IN: &str = "Sign in to the warehouse";
const L_READ_ONLY: &str = "The user can only read, in every database";
const L_READ: &str = "Read the list of tables";

/// Signs in to every shared database, proves the user can only read in each,
/// and reads their tables.
pub async fn connect_checks(session: &Session, t: &AwsTarget) -> WarehouseReport {
    let mut checks = Vec::new();
    let failed = |checks| WarehouseReport {
        ok: false,
        checks,
        tables: None,
        schema: None,
    };
    if t.databases.is_empty() {
        let e = StepError::new(
            "read",
            "No databases were shared.",
            "Run the setup script again.",
        );
        checks.push(fail("sign_in", L_SIGN_IN, &e));
        return failed(checks);
    }
    let c = client(session);
    let mut who = String::new();
    let mut schema = String::new();
    let mut tables = 0;
    for db in &t.databases {
        let access = match run(&c, t, db, ACCESS_CHECK).await {
            Ok(rows) => rows.into_iter().next().unwrap_or_default(),
            Err(e) => {
                checks.push(fail("sign_in", L_SIGN_IN, &e));
                return failed(checks);
            }
        };
        if !access
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("Redshift")
        {
            let e = StepError::new(
                "sign_in",
                "That isn't a Redshift warehouse.",
                "Choose a Redshift cluster.",
            );
            checks.push(fail("sign_in", L_SIGN_IN, &e));
            return failed(checks);
        }
        who = access
            .get("who")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string();
        if let Some(e) = access_problem(&access, db) {
            checks.push(pass("sign_in", L_SIGN_IN, format!("Signed in as {who}.")));
            checks.push(fail("read_only", L_READ_ONLY, &e));
            return failed(checks);
        }
        match run(&c, t, db, SCHEMA_READ).await {
            Ok(rows) => {
                let (text, n) = schema_text(db, &rows);
                if !schema.is_empty() && !text.is_empty() {
                    schema.push('\n');
                }
                schema.push_str(&text);
                tables += n;
            }
            Err(e) => {
                checks.push(fail("read", L_READ, &e));
                return failed(checks);
            }
        }
    }
    checks.push(pass(
        "sign_in",
        L_SIGN_IN,
        format!("Signed in as {who}, with no password."),
    ));
    checks.push(pass(
        "read_only",
        L_READ_ONLY,
        format!(
            "It can't change any table in {} databases.",
            t.databases.len()
        ),
    ));
    checks.push(pass(
        "read",
        L_READ,
        format!(
            "It can read {tables} tables across {}.",
            t.databases.join(", ")
        ),
    ));
    WarehouseReport {
        ok: true,
        checks,
        tables: Some(tables),
        schema: Some(schema),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn target() -> AwsTarget {
        AwsTarget {
            role_arn: "arn:aws:iam::123456789012:role/CoderhelmReadOnly".into(),
            region: "us-east-1".into(),
            engine: "redshift".into(),
            cluster_id: "analytics".into(),
            db_user: super::super::aws::DB_USER.into(),
            databases: vec!["dev".into(), "marts".into()],
            ..Default::default()
        }
    }

    #[test]
    fn a_database_must_be_one_the_team_shared() {
        let t = target();
        assert_eq!(pick_database(&t, Some("MARTS")).unwrap(), "marts");
        assert!(pick_database(&t, Some("payroll")).unwrap_err().is_refusal());
        assert!(pick_database(&t, None)
            .unwrap_err()
            .message
            .contains("dev, marts"));
        let one = AwsTarget {
            databases: vec!["dev".into()],
            ..target()
        };
        assert_eq!(pick_database(&one, None).unwrap(), "dev");
    }

    #[test]
    fn only_a_clear_yes_counts_as_read_only() {
        let row = |v: Value| v.as_object().unwrap().clone();
        assert!(confirms_read_only(&[row(json!({"ro": "on"}))]));
        assert!(!confirms_read_only(&[row(json!({"ro": "off"}))]));
        assert!(!confirms_read_only(&[]));
    }

    #[test]
    fn our_own_reads_pass_the_checker() {
        for sql in [ACCESS_CHECK, SCHEMA_READ, READ_ONLY_PROOF] {
            check_read_only_sql(sql, &[]).unwrap_or_else(|e| panic!("{e}"));
        }
    }

    #[test]
    fn query_logs_of_other_users_are_refused() {
        for sql in [
            "SELECT querytxt FROM stl_query",
            "SELECT text FROM svl_statementtext",
            "SELECT * FROM sys_query_history",
        ] {
            assert!(
                check_read_only_sql(sql, &[]).unwrap_err().is_refusal(),
                "{sql}"
            );
        }
    }

    #[test]
    fn any_write_right_or_missing_answer_is_a_problem() {
        let row = |v: Value| v.as_object().unwrap().clone();
        assert!(access_problem(
            &row(json!({"who":"r","powerful":false,"writable_tables":0})),
            "dev"
        )
        .is_none());
        assert!(access_problem(
            &row(json!({"who":"r","powerful":true,"writable_tables":0})),
            "dev"
        )
        .is_some());
        assert!(access_problem(
            &row(json!({"who":"r","powerful":false,"writable_tables":2})),
            "dev"
        )
        .unwrap()
        .message
        .contains("2 tables in dev"));
        assert!(access_problem(&row(json!({"who":"r"})), "dev").is_some());
    }

    #[test]
    fn warehouse_failures_say_what_went_wrong() {
        let e = classify_api_error("", "", Some("dispatch failure: timeout"));
        assert_eq!(e.step, "reach");
        assert!(
            e.message.contains("dispatch failure: timeout"),
            "{}",
            e.message
        );
        let e = classify_api_error("ThrottlingException", "Rate exceeded", None);
        assert!(e.message.contains("ThrottlingException: Rate exceeded"));
        let e = classify_api_error("ValidationException", "column \"x\" does not exist", None);
        assert_eq!(e.step, "read");
        assert!(e.message.contains("column \"x\" does not exist"));
    }

    #[test]
    fn schema_blocks_name_their_database() {
        let rows: Vec<Row> = [
            json!({"s":"public","t":"orders","c":"id","d":"integer"}),
            json!({"s":"marts","t":"daily","c":"day","d":"date"}),
        ]
        .into_iter()
        .map(|v| v.as_object().unwrap().clone())
        .collect();
        let (text, n) = schema_text("dev", &rows);
        assert_eq!(n, 2);
        assert!(text.starts_with("table public.orders (database dev)\n  id: integer\n"));
    }

    #[tokio::test]
    async fn a_write_never_reaches_the_warehouse() {
        let s = Session {
            access_key_id: "x".into(),
            secret_access_key: "x".into(),
            session_token: "x".into(),
            region: "us-east-1".into(),
        };
        let e = run_sql(
            &s,
            &target(),
            Some("dev"),
            "UNLOAD ('select 1') TO 's3://x'",
            &[],
            100,
        )
        .await
        .unwrap_err();
        assert!(e.is_refusal());
        let e = run_sql(&s, &target(), Some("payroll"), "SELECT 1", &[], 100)
            .await
            .unwrap_err();
        assert!(e.is_refusal());
    }

    /// Against a real warehouse set up by the setup script. Set
    /// LIVE_SETUP_CODE and LIVE_EXTERNAL_ID, then run with
    /// `cargo test -- --ignored real_warehouse`. Every write attempt runs inside
    /// the read-only, rolled-back transaction and must fail.
    #[tokio::test]
    #[ignore = "needs a real AWS account"]
    async fn real_warehouse_reads_and_refuses_writes() {
        use super::super::aws;
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} not set"));
        let conf = aws_config::load_from_env().await;
        let sts = aws_sdk_sts::Client::new(&conf);
        let mut t = aws::parse_setup_code(&env("LIVE_SETUP_CODE")).unwrap();
        t.external_id = env("LIVE_EXTERNAL_ID");
        let session = aws::assume(&sts, &t, "integration-test").await.unwrap();
        let report = connect_checks(&session, &t).await;
        for c in &report.checks {
            println!("check {} ok={} {}", c.id, c.ok, c.detail);
        }
        assert!(report.ok);
        let c = client(&session);
        for db in &t.databases {
            let rows = run(&c, &t, db, "SELECT 1 AS one").await.unwrap();
            assert_eq!(rows.len(), 1, "{db}");
            // public is creatable by everyone by default; the transaction must still refuse.
            let e = run(&c, &t, db, "CREATE TABLE public.coderhelm_probe (x int)")
                .await
                .unwrap_err();
            println!("{db}: create refused: {}", e.message);
            assert!(
                e.message.to_lowercase().contains("read-only")
                    || e.message.to_lowercase().contains("read only"),
                "{db}: {}",
                e.message
            );
        }
    }
}
