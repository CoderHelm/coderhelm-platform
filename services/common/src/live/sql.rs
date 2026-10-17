//! Decides whether a SQL lookup is a pure read, and builds the exact text that
//! is sent.
//!
//! This is one of three locks on a database lookup. The other two hold even if
//! this one were fooled: every lookup runs inside a read-only transaction that
//! is always rolled back ([`super::db`]), as a database user that can only
//! read (checked when the database is connected, and set to read-only by
//! default in the database itself).

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, ObjectName, Query, SetExpr, Statement, TableFactor, Visit, Visitor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::redact::{hidden_terms, is_hidden};
use super::StepError;

/// Longest lookup accepted.
const MAX_SQL_BYTES: usize = 16 * 1024;
/// Most rows one lookup returns.
pub const MAX_ROWS: usize = 200;

/// Functions that act outside the query (sleeping, signalling other sessions,
/// reading server files, running SQL from a string, changing settings), or
/// that turn a whole row into one value and so would carry hidden columns out
/// under a harmless name. Matched on the function's own name, ignoring schema.
const REFUSED_FUNCTIONS: &[&str] = &[
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_promote",
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "pg_stat_file",
    "pg_advisory_lock",
    "pg_advisory_xact_lock",
    "pg_try_advisory_lock",
    "pg_try_advisory_xact_lock",
    "set_config",
    "nextval",
    "setval",
    "dblink",
    "dblink_exec",
    "dblink_connect",
    "query_to_xml",
    "query_to_xml_and_xmlschema",
    "table_to_xml",
    "cursor_to_xml",
    "schema_to_xml",
    "database_to_xml",
    "row_to_json",
    "to_json",
    "to_jsonb",
    "json_agg",
    "jsonb_agg",
    "json_object_agg",
    "jsonb_object_agg",
    "hstore",
    "lo_import",
    "lo_export",
    "lo_get",
];

/// System tables that hold passwords, connection secrets or other sessions'
/// statements.
const REFUSED_TABLES: &[&str] = &[
    "pg_authid",
    "pg_shadow",
    "pg_user_mapping",
    "pg_user_mappings",
    "pg_largeobject",
    "pg_stat_activity",
    // Redshift: other users' query text, connection logs and user details.
    "stl_query",
    "stl_querytext",
    "stl_ddltext",
    "stl_utilitytext",
    "stl_connection_log",
    "svl_statementtext",
    "svl_qlog",
    "sys_query_history",
    "sys_query_text",
    "pg_user_info",
    "svv_user_info",
];

/// A lookup that passed every check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedSql {
    /// What is sent: the parsed statement printed back out, inside a row cap.
    /// Comments and anything the parser didn't keep are gone.
    pub text: String,
    /// The statement as parsed, without the row cap, for showing to people.
    pub statement: String,
    /// Tables read, by their own name.
    pub tables: Vec<String>,
}

fn refused(message: impl Into<String>) -> StepError {
    StepError::new(
        "read_only",
        message,
        "Only a single SELECT is allowed. Nothing was sent.",
    )
}

fn last_name(name: &ObjectName) -> String {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_lowercase())
        .unwrap_or_default()
}

#[derive(Default)]
struct Scan {
    /// A reason to refuse, found while walking.
    problem: Option<String>,
    statements: usize,
    tables: Vec<String>,
    /// Names that can stand for a whole row: tables and their aliases.
    row_names: HashSet<String>,
    /// Every identifier used in an expression, each part separately.
    idents: Vec<String>,
    /// Identifiers that stand alone, so could be a whole row used as a value.
    bare: Vec<String>,
}

impl Visitor for Scan {
    type Break = ();

    fn pre_visit_statement(&mut self, s: &Statement) -> ControlFlow<()> {
        self.statements += 1;
        if !matches!(s, Statement::Query(_)) {
            self.problem = Some("That lookup would change something or is not a query.".into());
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if !q.locks.is_empty() {
            self.problem = Some("That lookup locks rows (FOR UPDATE / FOR SHARE).".into());
            return ControlFlow::Break(());
        }
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                self.row_names.insert(cte.alias.name.value.to_lowercase());
            }
        }
        if let SetExpr::Select(sel) = q.body.as_ref() {
            if sel.into.is_some() {
                self.problem =
                    Some("That lookup writes its result into a table (SELECT INTO).".into());
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, r: &ObjectName) -> ControlFlow<()> {
        let name = last_name(r);
        if REFUSED_TABLES.contains(&name.as_str()) {
            self.problem = Some(format!("That lookup reads {name}, which holds secrets."));
            return ControlFlow::Break(());
        }
        // `FROM pg_sleep(5)` names a function where a table goes.
        if REFUSED_FUNCTIONS.contains(&name.as_str()) {
            self.problem = Some(format!("That lookup calls {name}, which isn't allowed."));
            return ControlFlow::Break(());
        }
        self.row_names.insert(name.clone());
        self.tables.push(name);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        let alias = match t {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::UNNEST { alias, .. }
            | TableFactor::NestedJoin { alias, .. } => alias.as_ref(),
            TableFactor::Function { name, alias, .. } => {
                let n = last_name(name);
                if REFUSED_FUNCTIONS.contains(&n.as_str()) {
                    self.problem = Some(format!("That lookup calls {n}, which isn't allowed."));
                    return ControlFlow::Break(());
                }
                alias.as_ref()
            }
            _ => None,
        };
        if let Some(a) = alias {
            self.row_names.insert(a.name.value.to_lowercase());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        match e {
            Expr::Identifier(i) => {
                self.idents.push(i.value.to_lowercase());
                self.bare.push(i.value.to_lowercase());
            }
            Expr::CompoundIdentifier(parts) => {
                self.idents
                    .extend(parts.iter().map(|i| i.value.to_lowercase()));
            }
            Expr::Function(f) => {
                let n = last_name(&f.name);
                if REFUSED_FUNCTIONS.contains(&n.as_str()) {
                    self.problem = Some(format!("That lookup calls {n}, which isn't allowed."));
                    return ControlFlow::Break(());
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Accepts one plain SELECT (WITH, joins, subqueries and aggregates are fine)
/// that names no hidden column. Anything else is refused before it is sent.
pub fn check_read_only_sql(sql: &str, hidden: &[String]) -> Result<CheckedSql, StepError> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        return Err(refused("The lookup is empty."));
    }
    if sql.len() > MAX_SQL_BYTES {
        return Err(refused("That lookup is too long."));
    }
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|e| refused(format!("That isn't SQL we can check ({e}).")))?;
    let [stmt] = statements.as_slice() else {
        return Err(refused("That is more than one statement."));
    };

    let mut scan = Scan::default();
    let _ = stmt.visit(&mut scan);
    if let Some(p) = scan.problem {
        return Err(refused(p));
    }
    if scan.statements != 1 {
        return Err(refused("That lookup hides a second statement inside it."));
    }

    // A hidden column is refused wherever it appears (selected, filtered on,
    // sorted by), so neither its value nor a guess at it comes back.
    let terms = hidden_terms(hidden);
    if let Some(col) = scan.idents.iter().find(|i| is_hidden(i, &terms)) {
        return Err(StepError::new(
            "read_only",
            format!("That lookup uses a hidden column ({col})."),
            "Hidden columns can't be read or filtered on. Remove it from the lookup. Nothing was sent.",
        ));
    }
    // `SELECT u FROM users u` returns the whole row as one value, which would
    // carry hidden columns out under the name `u`.
    if let Some(name) = scan.bare.iter().find(|i| scan.row_names.contains(*i)) {
        return Err(refused(format!(
            "That lookup uses the table {name} as a value. Name the columns you need."
        )));
    }

    let statement = stmt.to_string();
    let mut tables = scan.tables;
    tables.sort();
    tables.dedup();
    Ok(CheckedSql {
        text: format!("SELECT * FROM ({statement}) AS coderhelm_lookup LIMIT {MAX_ROWS}"),
        statement,
        tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(sql: &str) -> CheckedSql {
        check_read_only_sql(sql, &["email".to_string()]).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }
    fn no(sql: &str) -> String {
        let e = check_read_only_sql(sql, &["email".to_string()]).unwrap_err();
        assert!(e.is_refusal(), "{sql}");
        e.message
    }

    #[test]
    fn plain_reads_pass_with_a_row_cap() {
        let c = ok("select id, status from orders where id = 7;");
        assert_eq!(c.tables, vec!["orders"]);
        assert_eq!(
            c.text,
            "SELECT * FROM (SELECT id, status FROM orders WHERE id = 7) AS coderhelm_lookup LIMIT 200"
        );
        ok("WITH recent AS (SELECT id FROM orders ORDER BY created_at DESC LIMIT 5) SELECT count(*) FROM recent");
        ok("SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id");
        ok("SELECT u.* FROM users u");
        ok("SELECT * FROM public.orders");
        ok("SELECT table_name, column_name FROM information_schema.columns");
        ok("SELECT status, count(*) FROM orders GROUP BY status HAVING count(*) > 1");
        ok("SELECT id FROM orders WHERE id IN (SELECT order_id FROM refunds)");
    }

    #[test]
    fn anything_that_writes_is_refused() {
        for sql in [
            "DELETE FROM orders",
            "UPDATE orders SET status = 'x'",
            "INSERT INTO orders (id) VALUES (1)",
            "DROP TABLE orders",
            "TRUNCATE orders",
            "CREATE TABLE x (id int)",
            "GRANT ALL ON orders TO public",
            "SET ROLE admin",
            "COPY orders TO '/tmp/x'",
            "CALL do_things()",
            "VACUUM",
            "BEGIN",
            "EXPLAIN ANALYZE DELETE FROM orders",
        ] {
            no(sql);
        }
    }

    #[test]
    fn writes_hidden_inside_a_read_are_refused() {
        no("WITH gone AS (DELETE FROM orders RETURNING id) SELECT * FROM gone");
        no("SELECT 1; DELETE FROM orders");
        no("SELECT * INTO backup FROM orders");
        no("SELECT * FROM orders FOR UPDATE");
        no("SELECT * FROM orders FOR SHARE");
    }

    #[test]
    fn side_effect_functions_are_refused() {
        for sql in [
            "SELECT pg_sleep(10)",
            "SELECT * FROM pg_sleep(10)",
            "SELECT pg_catalog.pg_terminate_backend(123)",
            "SELECT set_config('statement_timeout', '0', false)",
            "SELECT nextval('orders_id_seq')",
            "SELECT query_to_xml('delete from orders', true, true, '')",
            "SELECT pg_read_file('/etc/passwd')",
            "SELECT * FROM dblink('host=x', 'select 1') AS t(a int)",
            "SELECT * FROM pg_shadow",
            "SELECT query FROM pg_stat_activity",
        ] {
            no(sql);
        }
    }

    #[test]
    fn hidden_columns_are_refused_wherever_they_appear() {
        for sql in [
            "SELECT email FROM users",
            "SELECT u.email FROM users u",
            "SELECT lower(email) AS x FROM users",
            "SELECT id FROM users WHERE email LIKE 'a%'",
            "SELECT id FROM users ORDER BY contact_email",
            "SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE work_email IS NOT NULL)",
        ] {
            assert!(no(sql).contains("hidden column"), "{sql}");
        }
    }

    #[test]
    fn whole_rows_as_values_are_refused() {
        for sql in [
            "SELECT u FROM users u",
            "SELECT users FROM users",
            "SELECT row_to_json(u) FROM users u",
            "SELECT to_jsonb(u.*) FROM users u",
            "SELECT u::text FROM users u",
            "WITH x AS (SELECT * FROM users) SELECT x FROM x",
        ] {
            no(sql);
        }
    }

    #[test]
    fn junk_and_size_are_refused() {
        no("");
        no("this is not sql");
        no(&format!("SELECT '{}'", "a".repeat(MAX_SQL_BYTES)));
    }
}
