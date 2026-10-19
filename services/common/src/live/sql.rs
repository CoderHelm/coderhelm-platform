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

use sqlparser::ast::{
    visit_expressions_mut, CastKind, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    ObjectName, Query, SetExpr, Statement, TableAlias, TableFactor, Visit, Visitor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::redact::{hidden_terms, hidden_terms_for, is_hidden, TableRef};
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
    "pg_notify",
];

/// Function name prefixes refused for the same reasons: families where every
/// member reads server files, takes locks, touches large objects or reaches
/// another database.
const REFUSED_FUNCTION_PREFIXES: &[&str] = &[
    "pg_ls_",
    "pg_read_",
    "pg_advisory",
    "pg_try_advisory",
    "lo_",
    "dblink",
    "pg_file",
];

fn refused_function(name: &str) -> bool {
    REFUSED_FUNCTIONS.contains(&name)
        || REFUSED_FUNCTION_PREFIXES
            .iter()
            .any(|p| name.starts_with(p))
}

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
    // Statement history of every session, with the literal values people typed.
    "pg_stat_statements",
    "pg_stat_statements_info",
    "pg_hba_file_rules",
    "pg_file_settings",
    "stv_inflight",
    "stv_recents",
    "svv_query_inflight",
    "sys_query_detail",
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
    /// The hidden-column terms that apply to this statement: every global
    /// term, and each table-scoped one whose table it reads. Results are
    /// redacted with these.
    pub hidden_terms: Vec<String>,
}

const WHOLE_ROW: &str = "That lookup uses a whole row as one value. Name the columns you need.";
const RENAMED: &str =
    "That lookup renames columns with an alias list. Name the columns you need instead.";

fn renames_columns(a: &TableAlias) -> bool {
    !a.columns.is_empty()
}

fn refused(message: impl Into<String>) -> StepError {
    StepError::new(
        "read_only",
        message,
        "Only a single SELECT is allowed. Nothing was sent.",
    )
}

/// A dotted name as written, without quotes: `public.orders`.
fn object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".")
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
    /// Every table read, with its schema when the statement names one.
    refs: Vec<TableRef>,
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
                if renames_columns(&cte.alias) {
                    self.problem = Some(RENAMED.into());
                    return ControlFlow::Break(());
                }
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
        if refused_function(&name) {
            self.problem = Some(format!("That lookup calls {name}, which isn't allowed."));
            return ControlFlow::Break(());
        }
        self.row_names.insert(name.clone());
        self.refs.push(TableRef::parse(&object_name(r)));
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
                if refused_function(&n) {
                    self.problem = Some(format!("That lookup calls {n}, which isn't allowed."));
                    return ControlFlow::Break(());
                }
                alias.as_ref()
            }
            _ => None,
        };
        if let Some(a) = alias {
            // `users AS t(id, nm, x)` renames columns positionally, so a
            // hidden column would come back under a harmless name.
            if renames_columns(a) {
                self.problem = Some(RENAMED.into());
                return ControlFlow::Break(());
            }
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
                if refused_function(&n) {
                    self.problem = Some(format!("That lookup calls {n}, which isn't allowed."));
                    return ControlFlow::Break(());
                }
                // `array_agg(u.*)`, `format('%s', u.*)`: a whole row passed as
                // one value carries hidden columns out under the function's
                // name. Only count(*) takes a bare star.
                if let FunctionArguments::List(list) = &f.args {
                    for a in &list.args {
                        let arg = match a {
                            FunctionArg::Unnamed(x) => x,
                            FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                                arg
                            }
                        };
                        let whole_row = match arg {
                            FunctionArgExpr::QualifiedWildcard(_) => true,
                            FunctionArgExpr::Wildcard => n != "count",
                            FunctionArgExpr::Expr(_) => false,
                        };
                        if whole_row {
                            self.problem = Some(WHOLE_ROW.into());
                            return ControlFlow::Break(());
                        }
                    }
                }
            }
            // `(u.*)::text`, `ROW(u.*)`: the same, outside a function call.
            Expr::QualifiedWildcard(..) | Expr::Wildcard(..) => {
                self.problem = Some(WHOLE_ROW.into());
                return ControlFlow::Break(());
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
    // U&"\0065mail" spells a name in escapes the hidden-column check can't
    // read; no ordinary lookup needs it.
    if sql.to_ascii_lowercase().contains("u&\"") || sql.to_ascii_lowercase().contains("u&'") {
        return Err(refused("That lookup spells names with Unicode escapes."));
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
    // A column hidden in one table only (`person.name`) counts when the
    // lookup reads that table, wherever the column name appears in it: the
    // rows that come back carry column names, not tables.
    let terms = hidden_terms_for(hidden, &scan.refs);
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

    // `x::int` is printed as `CAST(x AS INT)`: the same cast, without a
    // colon the Data API could mistake for the start of a parameter.
    let mut stmt = stmt.clone();
    let _ = visit_expressions_mut(&mut stmt, |e| {
        if let Expr::Cast { kind, .. } = e {
            if *kind == CastKind::DoubleColon {
                *kind = CastKind::Cast;
            }
        }
        ControlFlow::<()>::Continue(())
    });
    let statement = space_slice_colons(&stmt.to_string());
    let mut tables = scan.tables;
    tables.sort();
    tables.dedup();
    Ok(CheckedSql {
        text: format!("SELECT * FROM ({statement}) AS coderhelm_lookup LIMIT {MAX_ROWS}"),
        statement,
        tables,
        hidden_terms: terms,
    })
}

/// The hidden-column terms for already-checked SQL (a reviewed tool's text,
/// with `:name` parameters): the statement's tables decide which scoped
/// entries apply. Text that doesn't parse gets every entry.
pub fn statement_terms(sql: &str, hidden: &[String]) -> Vec<String> {
    #[derive(Default)]
    struct Refs(Vec<TableRef>);
    impl Visitor for Refs {
        type Break = ();
        fn pre_visit_relation(&mut self, r: &ObjectName) -> ControlFlow<()> {
            self.0.push(TableRef::parse(&object_name(r)));
            ControlFlow::Continue(())
        }
    }
    match Parser::parse_sql(&PostgreSqlDialect {}, sql) {
        Ok(statements) => {
            let mut refs = Refs::default();
            for s in &statements {
                let _ = s.visit(&mut refs);
            }
            hidden_terms_for(hidden, &refs.0)
        }
        Err(_) => hidden_terms(hidden),
    }
}

/// The Data API reads `:` followed by a word as a parameter, and the parser
/// prints array slices as `[1:3]`, so a slice would be sent as an unknown
/// parameter `:3`. Parameters are always named with a letter first, so a
/// colon before a digit, outside quotes, gets a space (`[1: 3]`), which
/// Postgres and Redshift read the same way.
fn space_slice_colons(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut quote: Option<char> = None;
    let mut chars = sql.chars().peekable();
    let mut prev = '\0';
    while let Some(c) = chars.next() {
        out.push(c);
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == '\'' || c == '"' => quote = Some(c),
            None if c == ':' && prev != ':' && chars.peek().is_some_and(|n| n.is_ascii_digit()) => {
                out.push(' ')
            }
            None => {}
        }
        prev = c;
    }
    out
}

/// Prepares checked SQL for the RDS Data API, which reads a `:word` in the
/// text as a named parameter (quoted text included) and fails with "Cannot
/// find parameter" when no such parameter was sent.
///
/// A pass over the statement's real tokens (the parser's own tokenizer, so
/// quotes, escapes and dollar quoting are read the way PostgreSQL reads them):
/// - quoted text holding a colon is rewritten as an escaped string (`E'...'`)
///   with each colon spelled `\072`. PostgreSQL reads it as exactly the same
///   untyped literal, so `'10:30'` still compares with a time column;
/// - a colon directly before a word or number that is not one of `params`
///   (an array slice, say) gets a space after it.
///
/// Parameters in `params` and `::` casts are left alone. Text that can't be
/// tokenized is returned unchanged.
pub fn for_rds_data_api(sql: &str, params: &[&str]) -> String {
    let dialect = PostgreSqlDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql)
        .with_unescape(false)
        .tokenize_with_location()
    else {
        return sql.to_string();
    };
    let chars: Vec<char> = sql.chars().collect();
    // Char index where each line starts, to turn a (line, column) into an index.
    let mut line_starts = vec![0usize];
    for (i, c) in chars.iter().enumerate() {
        if *c == '\n' {
            line_starts.push(i + 1);
        }
    }
    let at = |line: u64, column: u64| -> Option<usize> {
        let start = *line_starts.get((line as usize).checked_sub(1)?)?;
        Some(start + (column as usize).checked_sub(1)?)
    };
    let mut out = String::with_capacity(sql.len() + 16);
    let mut copied = 0usize;
    for (i, t) in tokens.iter().enumerate() {
        let (Some(start), Some(end)) = (
            at(t.span.start.line, t.span.start.column),
            at(t.span.end.line, t.span.end.column),
        ) else {
            return sql.to_string();
        };
        if start < copied || end > chars.len() || start > end {
            return sql.to_string();
        }
        let source: String = chars[start..end].iter().collect();
        let inner = || {
            let open = source.find('\'').map(|o| o + 1).unwrap_or(0);
            let close = source.len().saturating_sub(1).max(open);
            source[open..close].to_string()
        };
        let replacement = match &t.token {
            Token::SingleQuotedString(_) | Token::NationalStringLiteral(_)
                if source.contains(':') =>
            {
                Some(format!("E'{}'", escape_plain(&inner())))
            }
            Token::EscapedStringLiteral(_) if source.contains(':') => {
                Some(format!("E'{}'", escape_colons_in_escaped(&inner())))
            }
            Token::DollarQuotedString(d) if source.contains(':') => {
                Some(format!("E'{}'", escape_plain(&d.value.replace('\'', "''"))))
            }
            Token::Colon => {
                let next = tokens.get(i + 1).map(|n| &n.token);
                let is_param = matches!(next, Some(Token::Word(w))
                    if w.quote_style.is_none() && params.contains(&w.value.as_str()));
                let glued = matches!(next, Some(Token::Word(_)) | Some(Token::Number(..)));
                (glued && !is_param).then(|| ": ".to_string())
            }
            _ => None,
        };
        out.extend(chars[copied..start].iter());
        out.push_str(replacement.as_deref().unwrap_or(&source));
        copied = end;
    }
    out.extend(chars[copied..].iter());
    out
}

/// Plain quoted text (no backslash escapes) as the inside of an `E'...'`.
fn escape_plain(inner: &str) -> String {
    inner.replace('\\', "\\\\").replace(':', "\\072")
}

/// The inside of an `E'...'` with its colons spelled `\072`. A colon that is
/// already escaped (`\:`) is spelled the same way.
fn escape_colons_in_escaped(inner: &str) -> String {
    let mut out = String::with_capacity(inner.len() + 8);
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(':') => out.push_str("\\072"),
                Some(n) => {
                    out.push('\\');
                    out.push(n);
                }
                None => out.push('\\'),
            },
            ':' => out.push_str("\\072"),
            c => out.push(c),
        }
    }
    out
}

/// The tables a statement reads, each with the name the statement calls it
/// by (its alias, or its own name), both lowercase: `[("o", "public.orders")]`.
/// Empty when the text doesn't parse.
pub fn table_aliases(sql: &str) -> Vec<(String, String)> {
    #[derive(Default)]
    struct Names(Vec<(String, String)>);
    impl Visitor for Names {
        type Break = ();
        fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table { name, alias, .. } = t {
                let full = name
                    .0
                    .iter()
                    .filter_map(|p| p.as_ident())
                    .map(|i| i.value.to_lowercase())
                    .collect::<Vec<_>>()
                    .join(".");
                let called = alias
                    .as_ref()
                    .map(|a| a.name.value.to_lowercase())
                    .unwrap_or_else(|| last_name(name));
                self.0.push((called, full));
            }
            ControlFlow::Continue(())
        }
    }
    let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else {
        return Vec::new();
    };
    let mut names = Names::default();
    for s in &statements {
        let _ = s.visit(&mut names);
    }
    names.0
}

#[cfg(test)]
mod tests {
    #[test]
    fn data_api_text_has_no_colon_but_its_parameters() {
        let send = for_rds_data_api;
        // Quoted times and JSON keep their value but lose the bare colon.
        assert_eq!(
            send("SELECT * FROM t WHERE starts_at > '10:30'", &[]),
            r"SELECT * FROM t WHERE starts_at > E'10\07230'"
        );
        assert_eq!(
            send(r#"SELECT 1 FROM t WHERE meta @> '{"kind":3}'"#, &[]),
            r#"SELECT 1 FROM t WHERE meta @> E'{"kind"\0723}'"#
        );
        // A doubled quote stays, and a backslash that was literal stays literal.
        assert_eq!(
            send(r"SELECT 'it''s 1:2 \ x'", &[]),
            r"SELECT E'it''s 1\0722 \\ x'"
        );
        // An escaped string keeps its escapes; an escaped colon is spelled too.
        assert_eq!(
            send(r"SELECT E'a\tb:c\:d'", &[]),
            r"SELECT E'a\tb\072c\072d'"
        );
        // Dollar quoting becomes an escaped string.
        assert_eq!(send("SELECT $$a:b'c$$", &[]), r"SELECT E'a\072b''c'");
        // Declared parameters and casts are left alone; a slice is spaced.
        assert_eq!(
            send(
                "SELECT x[1:3], y[1:n] FROM t WHERE id = :id AND d = :day::date",
                &["id", "day"]
            ),
            "SELECT x[1: 3], y[1: n] FROM t WHERE id = :id AND d = :day::date"
        );
        // An undeclared :word is spaced, so it is never taken as a parameter.
        assert_eq!(send("SELECT :other", &["id"]), "SELECT : other");
        // Text without colons in it, and quoted names, are unchanged.
        let plain = "SELECT \"Order\".id, 'a''b' FROM \"Order\"\nWHERE x = 'é'";
        assert_eq!(send(plain, &[]), plain);
        assert_eq!(
            send("SELECT 'é:1',\n 'x:y' AS b", &[]),
            "SELECT E'é\\0721',\n E'x\\072y' AS b"
        );
    }

    #[test]
    fn checked_lookups_reach_the_data_api_without_stray_colons() {
        let c = check_read_only_sql(
            "SELECT id, created_at::date AS d FROM orders WHERE note = 'at 10:30' AND tags[1:2] = '{a}'",
            &[],
        )
        .unwrap();
        // The cast is printed without a colon at all.
        assert!(c.text.contains("CAST(created_at AS DATE)"), "{}", c.text);
        let sent = for_rds_data_api(&c.text, &[]);
        assert_eq!(
            sent.matches(':').count(),
            1,
            "only the spaced slice colon is left: {sent}"
        );
        assert!(sent.contains(r"E'at 10\07230'"), "{sent}");
    }

    #[test]
    fn tables_are_named_as_the_statement_calls_them() {
        assert_eq!(
            table_aliases("SELECT o.id FROM public.orders o JOIN customers ON true"),
            vec![
                ("o".to_string(), "public.orders".to_string()),
                ("customers".to_string(), "customers".to_string())
            ]
        );
        assert!(table_aliases("not sql").is_empty());
    }

    #[test]
    fn an_array_slice_is_not_sent_as_a_parameter() {
        let c = check_read_only_sql(
            "SELECT (array_agg(id))[1:3] AS ids, '10:30' AS t, x::int FROM orders",
            &[],
        )
        .unwrap();
        assert!(c.text.contains("[1: 3]"), "{}", c.text);
        assert!(
            c.text.contains("'10:30'"),
            "a quoted time is left alone: {}",
            c.text
        );
        assert!(!c.text.contains("[1:3]"));
        assert_eq!(
            space_slice_colons("a = :name AND b::text = '1:2'"),
            "a = :name AND b::text = '1:2'"
        );
    }

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
    fn whole_rows_inside_expressions_are_refused() {
        for sql in [
            "SELECT (u.*)::text FROM users u",
            "SELECT ROW(u.*) FROM users u",
            "SELECT format('%s', u.*) FROM users u",
            "SELECT array_agg(u.*) FROM users u",
            "SELECT string_agg(u.*::text, ',') FROM users u",
            "SELECT jsonb_build_array(u.*) FROM users u",
        ] {
            no(sql);
        }
        // A plain `t.*` select item keeps real column names, which are redacted.
        ok("SELECT u.* FROM users u");
        ok("SELECT count(*) FROM users");
    }

    #[test]
    fn column_alias_lists_are_refused() {
        for sql in [
            "SELECT t.* FROM users AS t(id, nm, x)",
            "SELECT x FROM users AS t(id, nm, x)",
            "WITH t(a, b, c) AS (SELECT * FROM users) SELECT * FROM t",
            "SELECT * FROM (SELECT * FROM users) AS t(a, b, c)",
        ] {
            no(sql);
        }
    }

    #[test]
    fn unicode_escaped_names_are_refused() {
        no("SELECT U&\"\\0065mail\" FROM users");
        no("SELECT u&'\\0065' FROM users");
    }

    #[test]
    fn more_side_effects_and_query_history_are_refused() {
        for sql in [
            "SELECT lo_create(0)",
            "SELECT lo_from_bytea(0, 'x')",
            "SELECT pg_notify('c', 'x')",
            "SELECT pg_advisory_lock_shared(1)",
            "SELECT * FROM pg_ls_waldir()",
            "SELECT * FROM pg_ls_logdir()",
            "SELECT query FROM pg_stat_statements",
            "SELECT querytxt FROM stv_inflight",
            "SELECT * FROM stv_recents",
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
    #[test]
    fn a_column_hidden_in_one_table_is_refused_only_where_that_table_is_read() {
        let hidden = vec!["email".to_string(), "person.name".to_string()];
        let c = check_read_only_sql("SELECT id, name FROM public.course LIMIT 1", &hidden).unwrap();
        assert!(!is_hidden("name", &c.hidden_terms));
        assert!(is_hidden("email", &c.hidden_terms));
        for sql in [
            "SELECT name FROM person",
            "SELECT p.name FROM public.person p",
            "SELECT s.name FROM course s JOIN person p ON p.id = s.owner_id",
        ] {
            let e = check_read_only_sql(sql, &hidden).unwrap_err();
            assert!(
                e.message.contains("hidden column (name)"),
                "{sql}: {}",
                e.message
            );
        }
        // `*` from that table still comes back with the column hidden.
        let c = check_read_only_sql("SELECT * FROM person", &hidden).unwrap();
        assert!(is_hidden("name", &c.hidden_terms));
        // A reviewed tool's text gets the same terms.
        assert!(is_hidden(
            "name",
            &statement_terms("SELECT id FROM person WHERE id = :id", &hidden)
        ));
        assert!(!is_hidden(
            "name",
            &statement_terms("SELECT id FROM course WHERE id = :id", &hidden)
        ));
    }
}
