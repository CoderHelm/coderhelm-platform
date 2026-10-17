//! Reads a team's Redshift data warehouse through the Redshift Data API, under
//! the rules in [`super`]. One source covers every database in the warehouse
//! the team shared.
//!
//! Three locks, each enough on its own to stop a write:
//! 1. The lookup is parsed and refused unless it is one plain SELECT
//!    ([`super::sql`]); what is sent is printed from what was parsed.
//! 2. It runs as a user with no password that can only read: connecting, and
//!    the first lookup in each database during every agent run, check that
//!    the user is not an admin and cannot change any table.
//! 3. The role in the team's account may only sign in as that one user and
//!    run single statements; batches and every other way of signing in are
//!    denied.
//!
//! Redshift has no read-only transaction to roll back, so unlike PostgreSQL
//! there is no transaction lock; the user's rights carry that weight and are
//! checked before use.

use std::time::{Duration, Instant};

use serde_json::{Map, Number, Value};

use super::api::Check;
use super::aws::{AwsTarget, Session};
use super::redact::{hidden_terms, redact_all};
use super::sql::{check_read_only_sql, CheckedSql};
use super::StepError;

/// Longest we wait for one statement before cancelling it.
const WAIT: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(300);
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
    match code {
        "AccessDeniedException" | "UnrecognizedClientException" => StepError::new(
            "sign_in",
            "The role in your AWS account isn't allowed to query this warehouse.",
            "Run the setup script again and choose this warehouse.",
        ),
        "ValidationException" | "ExecuteStatementException" => StepError::new(
            "read",
            format!(
                "The warehouse couldn't run that: {}",
                crate::truncate_str(msg, 300)
            ),
            "Check table and column names with the description of this warehouse.",
        ),
        _ => StepError::new(
            "reach",
            "We couldn't reach the warehouse.",
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

/// Runs one statement in `database` as the read-only user and returns its
/// rows. `sql` must already be checked, or be one of this module's own reads.
async fn run(
    client: &aws_sdk_redshiftdata::Client,
    t: &AwsTarget,
    database: &str,
    sql: &str,
) -> Result<Vec<Row>, StepError> {
    let started = Instant::now();
    let out = client
        .execute_statement()
        .cluster_identifier(&t.cluster_id)
        .db_user(&t.db_user)
        .database(database)
        .sql(sql)
        .statement_name("coderhelm-lookup")
        .send()
        .await
        .map_err(|e| api_error(&e))?;
    let id = out.id().unwrap_or_default().to_string();

    use aws_sdk_redshiftdata::types::StatusString;
    loop {
        tokio::time::sleep(POLL).await;
        let d = client
            .describe_statement()
            .id(&id)
            .send()
            .await
            .map_err(|e| api_error(&e))?;
        match d.status() {
            Some(StatusString::Finished) => {
                if d.has_result_set() != Some(true) {
                    return Ok(Vec::new());
                }
                break;
            }
            Some(StatusString::Failed) | Some(StatusString::Aborted) => {
                let why = d.error().unwrap_or("it failed");
                return Err(StepError::new(
                    "read",
                    format!(
                        "The warehouse couldn't run that: {}",
                        crate::truncate_str(why, 300)
                    ),
                    "Check table and column names with the description of this warehouse.",
                ));
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
    Ok(r.records()
        .iter()
        .map(|rec| {
            names
                .iter()
                .cloned()
                .zip(rec.iter().map(field_value))
                .collect::<Row>()
        })
        .collect())
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

const ACCESS_CHECK: &str = "SELECT current_user AS who, \
     (SELECT usesuper OR usecreatedb FROM pg_user WHERE usename = current_user) AS powerful, \
     (SELECT count(*) FROM information_schema.tables t \
       WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal') AND t.table_type = 'BASE TABLE' \
         AND (has_table_privilege(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name), 'INSERT') \
           OR has_table_privilege(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name), 'UPDATE') \
           OR has_table_privilege(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name), 'DELETE'))) AS writable_tables, \
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
    fn our_own_reads_pass_the_checker() {
        for sql in [ACCESS_CHECK, SCHEMA_READ] {
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
}
