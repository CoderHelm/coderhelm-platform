//! Which SQL a database speaks, told to the agent before it writes a query and
//! again when the database rejects one.
//!
//! A warehouse that speaks an older SQL (Redshift) refuses things modern
//! PostgreSQL accepts, and an agent that wasn't told the engine writes
//! PostgreSQL and wastes a lookup. The engine comes from the source's stored
//! settings, never from a guess. Nothing here rewrites a query: the agent is
//! told what failed and writes the next one itself.

use regex::Regex;
use std::sync::LazyLock;

use super::aws::AwsTarget;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Postgres,
    Redshift,
}

impl Engine {
    /// The engine a source's stored settings name (`redshift`, or empty for
    /// PostgreSQL).
    pub fn of(t: &AwsTarget) -> Self {
        Self::named(&t.engine)
    }

    pub fn named(engine: &str) -> Self {
        if engine.trim().eq_ignore_ascii_case("redshift") {
            Engine::Redshift
        } else {
            Engine::Postgres
        }
    }

    /// The engine's name as people know it.
    pub fn label(self) -> &'static str {
        match self {
            Engine::Postgres => "PostgreSQL",
            Engine::Redshift => "Amazon Redshift",
        }
    }

    /// The mistakes an agent used to modern PostgreSQL makes most often in
    /// this engine, short enough for a tool description.
    pub fn pitfalls(self) -> &'static str {
        match self {
            Engine::Postgres => {
                "PostgreSQL: quote names with double quotes, never backticks; JSON with ->, ->> \
                 and the jsonb functions; ILIKE and COUNT(*) FILTER (WHERE ...) work; cast \
                 columns of unusual types to text (oid::text)."
            }
            Engine::Redshift => {
                "Amazon Redshift speaks an older PostgreSQL (8.0) dialect, so modern PostgreSQL \
                 often fails: no aggregate FILTER clause (write SUM(CASE WHEN ... THEN 1 ELSE 0 \
                 END) instead of COUNT(*) FILTER (WHERE ...)); no DISTINCT ON (keep ROW_NUMBER() \
                 OVER (PARTITION BY ... ORDER BY ...) = 1 from a subquery); no jsonb and no -> or \
                 ->> operators (use json_extract_path_text(col, 'key')); no LATERAL joins; \
                 generate_series and other leader-only functions run only without tables; \
                 LISTAGG, PERCENTILE_CONT and MEDIAN can't share a SELECT with another DISTINCT \
                 aggregate (compute each in its own subquery and join them), and a LISTAGG \
                 result is at most 65535 bytes. ILIKE, DATEADD, DATEDIFF and LISTAGG work."
            }
        }
    }
}

/// Whether the database's error says the SQL didn't parse.
pub fn is_syntax_error(message: &str) -> bool {
    message.to_ascii_lowercase().contains("syntax error")
}

/// A construct an engine doesn't accept, and the one-line rewrite for it.
struct Construct {
    /// Matches the construct in the SQL or in the error's own quote of it.
    pattern: &'static LazyLock<Regex>,
    hint: &'static str,
}

macro_rules! re {
    ($name:ident, $re:expr) => {
        static $name: LazyLock<Regex> = LazyLock::new(|| Regex::new($re).expect("valid"));
    };
}

re!(FILTER, r"(?i)\bfilter\s*\(\s*(where\b|$)|\)\s*filter\b");
re!(DISTINCT_ON, r"(?i)\bdistinct\s+on\b");
re!(JSON_ARROW, r"->>?|\bjsonb\b");
re!(LATERAL, r"(?i)\blateral\b");
re!(BACKTICK, r"`");

static REDSHIFT: &[Construct] = &[
    Construct {
        pattern: &FILTER,
        hint: "Redshift has no aggregate FILTER clause: write SUM(CASE WHEN <condition> THEN 1 \
               ELSE 0 END) instead of COUNT(*) FILTER (WHERE <condition>), and MAX(CASE WHEN \
               <condition> THEN x END) for other aggregates.",
    },
    Construct {
        pattern: &DISTINCT_ON,
        hint: "Redshift has no DISTINCT ON: number the rows with ROW_NUMBER() OVER (PARTITION BY \
               ... ORDER BY ...) in a subquery and keep row 1.",
    },
    Construct {
        pattern: &JSON_ARROW,
        hint: "Redshift has no jsonb and no -> or ->> operators: read a key from JSON text with \
               json_extract_path_text(col, 'key').",
    },
    Construct {
        pattern: &LATERAL,
        hint: "Redshift doesn't support LATERAL joins: join a grouped subquery on the same keys \
               instead.",
    },
    Construct {
        pattern: &BACKTICK,
        hint: "Quote names with double quotes, never backticks.",
    },
];

static POSTGRES: &[Construct] = &[Construct {
    pattern: &BACKTICK,
    hint: "PostgreSQL quotes names with double quotes, never backticks.",
}];

/// The part of the SQL the error points at: Redshift's `in context "..."`,
/// else PostgreSQL's `at or near "..."`.
fn quoted_context(message: &str) -> Option<&str> {
    re!(CONTEXT, r#"in context "([^"]*)""#);
    re!(NEAR, r#"at or near "([^"]*)""#);
    CONTEXT
        .captures(message)
        .or_else(|| NEAR.captures(message))
        .and_then(|c| c.get(1))
        .map(|m| m.as_str())
}

/// The rewrite for the construct a syntax error is about, when it can be
/// told: first one the error's own quote of the SQL shows, else the first
/// one the SQL uses.
pub fn syntax_hint(engine: Engine, message: &str, sql: &str) -> Option<&'static str> {
    let constructs = match engine {
        Engine::Redshift => REDSHIFT,
        Engine::Postgres => POSTGRES,
    };
    if let Some(ctx) = quoted_context(message) {
        if let Some(c) = constructs.iter().find(|c| c.pattern.is_match(ctx)) {
            return Some(c.hint);
        }
    }
    constructs
        .iter()
        .find(|c| c.pattern.is_match(sql))
        .map(|c| c.hint)
}

/// What to add to a failed query's result so the agent can fix it: for a
/// syntax error, that it was one in this engine's SQL and the rewrite when the
/// construct is recognised; for a known Redshift limit, its rewrite. `None`
/// for any other failure, which is reported as it was.
pub fn failure_note(engine: Engine, message: &str, sql: &str) -> Option<String> {
    if is_syntax_error(message) {
        let mut s = format!(
            "This was a syntax error: the SQL isn't valid {}.",
            engine.label()
        );
        if let Some(h) = syntax_hint(engine, message, sql) {
            s.push(' ');
            s.push_str(h);
        }
        s.push_str(" Fix the SQL and try again once.");
        return Some(s);
    }
    match engine {
        Engine::Redshift => super::redshift::dialect_hint(message)
            .map(|h| format!("{h} Fix the SQL and try again once.")),
        Engine::Postgres => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RS_FILTER: &str =
        "The warehouse couldn't run that: ERROR: syntax error at or near \"(\" \
                             in context \"*) FILTER (\", at line 1, Position: 19";

    #[test]
    fn the_engine_comes_from_the_stored_settings() {
        let mut t = AwsTarget::default();
        assert_eq!(Engine::of(&t), Engine::Postgres);
        t.engine = "redshift".into();
        assert_eq!(Engine::of(&t), Engine::Redshift);
        assert!(Engine::Redshift.pitfalls().contains("SUM(CASE WHEN"));
        assert!(Engine::Redshift
            .pitfalls()
            .contains("json_extract_path_text"));
        assert!(Engine::Redshift.pitfalls().contains("LISTAGG"));
        assert!(Engine::Postgres
            .pitfalls()
            .contains("FILTER (WHERE ...) work"));
    }

    #[test]
    fn a_filter_clause_on_redshift_gets_its_rewrite() {
        let sql =
            "SELECT count(*) FILTER (WHERE favorite_color IS NOT NULL) AS n FROM public.contacts";
        let note = failure_note(Engine::Redshift, RS_FILTER, sql).unwrap();
        assert!(note.contains("syntax error"), "{note}");
        assert!(note.contains("Amazon Redshift"), "{note}");
        assert!(note.contains("SUM(CASE WHEN"), "{note}");
        assert!(note.contains("try again once"), "{note}");
        // The SQL alone is enough when the error doesn't quote it.
        assert_eq!(
            syntax_hint(
                Engine::Redshift,
                "ERROR: syntax error at or near \"(\"",
                sql
            ),
            syntax_hint(Engine::Redshift, RS_FILTER, "")
        );
    }

    #[test]
    fn the_construct_the_error_points_at_wins() {
        let sql = "SELECT DISTINCT ON (a) a, b->>'k' FROM t";
        let near_json = "ERROR: syntax error at or near \">\" in context \"b->>'k'\"";
        assert!(syntax_hint(Engine::Redshift, near_json, sql)
            .unwrap()
            .contains("json_extract_path_text"));
        assert!(syntax_hint(Engine::Redshift, "ERROR: syntax error", sql)
            .unwrap()
            .contains("DISTINCT ON"));
        // The word "filter" as a name is not the clause.
        assert!(syntax_hint(
            Engine::Redshift,
            "ERROR: syntax error",
            "SELECT filter FROM t"
        )
        .is_none());
    }

    #[test]
    fn an_unrecognised_syntax_error_still_names_the_engine() {
        let note = failure_note(
            Engine::Postgres,
            "ERROR: syntax error at or near \"FROM\"",
            "SELECT a, FROM t",
        )
        .unwrap();
        assert!(note.starts_with("This was a syntax error: the SQL isn't valid PostgreSQL."));
        assert!(
            failure_note(Engine::Postgres, "syntax error", "SELECT `a` FROM t")
                .unwrap()
                .contains("double quotes")
        );
    }

    #[test]
    fn other_failures_add_nothing_new() {
        let missing = "ERROR: column \"x\" does not exist";
        assert!(failure_note(Engine::Redshift, missing, "SELECT x FROM t").is_none());
        assert!(failure_note(Engine::Postgres, missing, "SELECT x FROM t").is_none());
        // A known Redshift limit carries its rewrite.
        assert!(failure_note(
            Engine::Redshift,
            "ERROR: Result size exceeds LISTAGG limit",
            "SELECT LISTAGG(a) FROM t"
        )
        .unwrap()
        .contains("65535"));
    }
}
