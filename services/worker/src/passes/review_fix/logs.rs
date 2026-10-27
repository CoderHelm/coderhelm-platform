//! Log lines in a review fix: their level, their text, who reads that text
//! and the test that pins them.
//!
//! - A log that fires on every run of a scheduled or repeated job with
//!   nothing changed goes at DEBUG; INFO is for runs where something
//!   happened (rows changed, a decision taken).
//! - A log's wording is an interface: metric filters, alarms, alert rules,
//!   scripts, runbooks and tests may match it. Before changing it, those are
//!   searched for, and the wording stays when any exist.
//! - Changing a log's level or text is a behaviour change: a test asserts
//!   the new level or text, in the repository's own test style.

use std::sync::OnceLock;

use regex::Regex;
use serde_json::{json, Value};

use super::staged::NumberedHunk;

/// Coding guidance for every agent that writes code in a fix round.
pub const GUIDANCE: &str = "\n\n## Logs\n\
- Log levels: a log that fires on every run of a scheduled or repeated job (cron, poller, sync, \
queue consumer, loop) when nothing changed goes at DEBUG. INFO only when something happened \
(rows changed, a record created, a decision taken), e.g. `if changed: log.info(\"updated %d \
rows\", changed)` and `log.debug(\"nothing to update\")`.\n\
- Log text is read by other things: before changing an existing log message's wording, search \
this repository and the team's infrastructure code for it (metric filters, alarms, alert rules, \
scripts, runbooks, docs, tests that assert it). Keep the wording when anything uses it.\n\
- Changing a log's level or message is a behaviour change: add or update a test that asserts \
the level and text (pytest `caplog`, `unittest` `assertLogs`, a Jest or Vitest spy on the \
logger, a captured `tracing` subscriber), following the repository's existing tests and fakes.\n";

/// The self-review checklist line for logs.
pub const CHECKLIST_ITEM: &str = "**Log levels** — In scheduled or repeated jobs (cron, pollers, \
syncs, consumers, loops), flag every added INFO log that fires on every run even when nothing \
changed: it belongs at DEBUG, with INFO kept for runs where something happened. Flag a changed \
log message whose old wording other code matches (metric filters, alarms, alert rules, scripts, \
runbooks, tests), and a changed log level or message with no test asserting it.";

/// One log call on one line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogCall {
    /// trace, debug, info, warn, error, fatal (normalized).
    pub level: String,
    /// The first string literal of the call, as written.
    pub message: String,
}

fn call_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        Regex::new(
            r#"(?ix)
            (?:
              \b(?:log|logger|logging|_log|_logger|log_|self\.log(?:ger)?|this\.log(?:ger)?|console|tracing|slog|klog|zap|logrus|Log|LOG|LOGGER)
              \s*(?:\.|::|->)\s*
              (?P<a>trace|debug|info|information|notice|warn|warning|error|err|fatal|critical|exception|severe|fine)f?w?
              \s*[(]
            |
              \b(?P<b>trace|debug|info|warn|error)!\s*[(]
            )"#,
        )
        .unwrap()
    })
}

fn literal_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        Regex::new(r#""((?:[^"\\]|\\.)*)"|'((?:[^'\\]|\\.)*)'|`((?:[^`\\]|\\.)*)`"#).unwrap()
    })
}

fn normalize_level(l: &str) -> String {
    match l.to_ascii_lowercase().as_str() {
        "information" | "notice" => "info",
        "warning" => "warn",
        "err" | "exception" | "severe" => "error",
        "critical" => "fatal",
        "fine" => "debug",
        other => return other.to_string(),
    }
    .to_string()
}

/// The log call on `line`, if any.
pub fn log_call(line: &str) -> Option<LogCall> {
    let t = line.trim_start();
    if t.starts_with("//") || t.starts_with('#') || t.starts_with("/*") || t.starts_with('*') {
        return None;
    }
    let c = call_re().captures(line)?;
    let level = c.name("a").or_else(|| c.name("b"))?.as_str();
    let after = &line[c.get(0)?.end()..];
    let message = literal_re()
        .captures(after)
        .and_then(|m| m.get(1).or_else(|| m.get(2)).or_else(|| m.get(3)))
        .map(|m| m.as_str().to_string())
        .unwrap_or_default();
    Some(LogCall {
        level: normalize_level(level),
        message,
    })
}

/// What a fix does to one log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogChange {
    /// A new log line.
    Added { level: String, message: String },
    /// The same log at another level (the text may change too).
    Level {
        from: String,
        to: String,
        message: String,
    },
    /// The same log with other wording.
    Text {
        level: String,
        from: String,
        to: String,
    },
    /// A log line taken out.
    Removed { level: String, message: String },
}

impl LogChange {
    /// The old wording other code may match, when the fix changes or
    /// removes it.
    pub fn old_text(&self) -> Option<&str> {
        match self {
            LogChange::Text { from, .. } => Some(from),
            LogChange::Removed { message, .. } => Some(message),
            _ => None,
        }
    }

    /// Whether a test must pin it: a changed level or wording.
    pub fn needs_test(&self) -> bool {
        matches!(self, LogChange::Level { .. } | LogChange::Text { .. })
    }

    /// The level and text the test asserts.
    pub fn asserted(&self) -> (&str, &str) {
        match self {
            LogChange::Added { level, message } | LogChange::Removed { level, message } => {
                (level, message)
            }
            LogChange::Level { to, message, .. } => (to, message),
            LogChange::Text { level, to, .. } => (level, to),
        }
    }

    pub fn describe(&self) -> String {
        match self {
            LogChange::Added { level, message } => format!("adds {level} log \"{message}\""),
            LogChange::Level { from, to, message } => {
                format!("moves \"{message}\" from {from} to {to}")
            }
            LogChange::Text { level, from, to } => {
                format!("rewords {level} log \"{from}\" to \"{to}\"")
            }
            LogChange::Removed { level, message } => {
                format!("removes {level} log \"{message}\"")
            }
        }
    }
}

/// A log change and the hunk it is in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub hunk: usize,
    pub path: String,
    pub change: LogChange,
}

/// Placeholders are not part of a message's fixed text.
fn skeleton(message: &str) -> String {
    static R: OnceLock<Regex> = OnceLock::new();
    let r = R.get_or_init(|| {
        Regex::new(r#"\{[^}]*\}|%[-+ #0]*\d*(?:\.\d+)?[a-zA-Z]|\$\{[^}]*\}"#).unwrap()
    });
    r.replace_all(message, "{}").to_string()
}

/// Whether two messages are the same log in other words (they share most
/// of their fixed words), for pairing a removed line with an added one.
fn similar(a: &str, b: &str) -> bool {
    let words = |s: &str| -> std::collections::BTreeSet<String> {
        skeleton(s)
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| w.len() > 2)
            .map(|w| w.to_lowercase())
            .collect()
    };
    let (wa, wb) = (words(a), words(b));
    if wa.is_empty() || wb.is_empty() {
        return false;
    }
    let common = wa.intersection(&wb).count();
    common * 2 >= wa.len().min(wb.len())
}

/// Every log change in the round's hunks. A removed and an added log in the
/// same hunk with the same text, or with similar text, are one log changed.
pub fn changes(hunks: &[NumberedHunk]) -> Vec<Found> {
    let mut out = Vec::new();
    for h in hunks {
        let mut removed: Vec<Option<LogCall>> = h
            .hunk
            .removed_text()
            .iter()
            .filter_map(|l| log_call(l))
            .map(Some)
            .collect();
        let added: Vec<LogCall> = h
            .hunk
            .added_text()
            .iter()
            .filter_map(|l| log_call(l))
            .collect();
        for a in added {
            // Same text first, then similar text.
            let pick = removed
                .iter()
                .position(|r| r.as_ref().is_some_and(|r| r.message == a.message))
                .or_else(|| {
                    removed
                        .iter()
                        .position(|r| r.as_ref().is_some_and(|r| similar(&r.message, &a.message)))
                });
            let change = match pick.and_then(|i| removed[i].take()) {
                Some(r) if r.level != a.level => LogChange::Level {
                    from: r.level,
                    to: a.level,
                    message: a.message,
                },
                Some(r) if r.message != a.message => LogChange::Text {
                    level: a.level,
                    from: r.message,
                    to: a.message,
                },
                Some(_) => continue,
                None => LogChange::Added {
                    level: a.level,
                    message: a.message,
                },
            };
            out.push(Found {
                hunk: h.id,
                path: h.hunk.path.clone(),
                change,
            });
        }
        for r in removed.into_iter().flatten() {
            out.push(Found {
                hunk: h.id,
                path: h.hunk.path.clone(),
                change: LogChange::Removed {
                    level: r.level,
                    message: r.message,
                },
            });
        }
    }
    out
}

/// The fixed fragment of a message to search for: its longest piece
/// between placeholders, when it is long enough to mean something.
pub fn search_term(message: &str) -> Option<String> {
    let s = skeleton(message);
    s.split("{}")
        .map(|p| p.trim().trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|p| p.len() >= 8)
        .max_by_key(|p| p.len())
        .map(|p| p.to_string())
}

/// What kind of consumer a file that names a log's text is.
pub fn consumer_kind(path: &str, fragment: &str) -> &'static str {
    let p = path.to_ascii_lowercase();
    let f = fragment.to_ascii_lowercase();
    let file = p.rsplit('/').next().unwrap_or(&p);
    if super::super::is_test_file(path)
        || p.contains("/tests/")
        || p.starts_with("tests/")
        || p.contains("/test/")
        || file.starts_with("test_")
        || file.ends_with("_test.go")
        || file.ends_with("_test.py")
    {
        return "test asserting the text";
    }
    if f.contains("metric_filter")
        || f.contains("metricfilter")
        || f.contains("filter_pattern")
        || f.contains("filterpattern")
        || f.contains("logs_metric")
    {
        return "metric filter";
    }
    if f.contains("alarm") || p.contains("alarm") {
        return "alarm";
    }
    if f.contains("alert") || p.contains("alert") || p.contains("monitor") || p.contains("rules") {
        return "alert rule";
    }
    if super::super::infra_repo::is_iac_path(path) {
        return "infrastructure code";
    }
    if file.ends_with(".sh")
        || file.ends_with(".bash")
        || file.ends_with(".ps1")
        || file == "makefile"
        || p.starts_with("scripts/")
        || p.contains("/scripts/")
        || p.contains("/bin/")
    {
        return "script";
    }
    if p.contains("runbook") || p.contains("playbook") {
        return "runbook";
    }
    if file.ends_with(".md")
        || file.ends_with(".mdx")
        || file.ends_with(".rst")
        || p.contains("docs/")
    {
        return "docs";
    }
    "code"
}

/// Whether a file is a scheduled or repeated job, from its path and text.
pub fn is_repeated_job(path: &str, text: &str) -> bool {
    let p = path.to_ascii_lowercase();
    let path_words = [
        "cron", "job", "schedul", "worker", "task", "sync", "poll", "consumer", "daemon", "dags/",
        "periodic", "batch", "etl", "ingest", "watcher", "sweep", "reconcil",
    ];
    if path_words.iter().any(|w| p.contains(w)) {
        return true;
    }
    let text_marks = [
        "@Scheduled",
        "@scheduled",
        "schedule.every",
        "setInterval(",
        "@periodic_task",
        "@shared_task",
        "crontab(",
        "cron.schedule",
        "CronJob",
        "ScheduleExpression",
        "schedule_expression",
        "DAG(",
        "tokio::time::interval",
        "time.NewTicker",
        "time.Tick(",
        "while True:",
        "BackgroundScheduler",
        "add_job(",
        "@app.task",
        "@celery",
        "SQSEvent",
        "ScheduledEvent",
    ];
    text_marks.iter().any(|m| text.contains(m))
}

/// The INFO logs a fix adds (or moves to INFO) in repeated jobs: each needs
/// a look at whether it fires on every run.
pub fn info_in_repeated_jobs(found: &[Found], file_text: impl Fn(&str) -> String) -> Vec<&Found> {
    found
        .iter()
        .filter(|f| match &f.change {
            LogChange::Added { level, .. } => level == "info",
            LogChange::Level { to, .. } => to == "info",
            _ => false,
        })
        .filter(|f| is_repeated_job(&f.path, &file_text(&f.path)))
        .collect()
}

/// What the unconditional-INFO judge is told.
pub const RUNS_SYSTEM: &str = "You review INFO logs a change adds to scheduled or repeated jobs. \
For each numbered log, read the code around it and say whether it fires on every run even when \
nothing changed (unconditional: at the start or end of the run, per batch, per item checked, \
\"nothing to do\" messages) or only when something happened (inside a branch taken when rows \
changed, a record was created, an error occurred, a decision was taken). Give every number \
exactly once.";

pub fn runs_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "logs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "every_run": {"type": "boolean"},
                        "reason": {"type": "string"}
                    },
                    "required": ["id", "every_run", "reason"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["logs"],
        "additionalProperties": false
    })
}

/// The ids the judge says fire on every run, with its reason.
pub fn every_run(answer: &Value) -> Vec<(usize, String)> {
    answer
        .get("logs")
        .and_then(|l| l.as_array())
        .map(|a| {
            a.iter()
                .filter(|l| l.get("every_run").and_then(|b| b.as_bool()) == Some(true))
                .filter_map(|l| {
                    Some((
                        l.get("id")?.as_u64()? as usize,
                        l.get("reason")
                            .and_then(|r| r.as_str())
                            .unwrap_or("")
                            .to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The code around a line of `text`, numbered, for the judge.
pub fn around(text: &str, needle: &str, radius: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let Some(at) = lines.iter().position(|l| l.contains(needle)) else {
        return String::new();
    };
    let start = at.saturating_sub(radius);
    let end = (at + radius + 1).min(lines.len());
    (start..end)
        .map(|i| {
            format!(
                "{}{:>5} {}",
                if i == at { ">" } else { " " },
                i + 1,
                lines[i]
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Markers of a test that captures and asserts logs, across stacks.
const CAPTURE_MARKS: &[&str] = &[
    "caplog",
    "assertLogs",
    "assertNoLogs",
    "logs_contain",
    "logs_assert",
    "traced_test",
    "tracing_test",
    "spyOn(console",
    "spyOn(logger",
    "spyOn(log",
    "vi.spyOn(",
    "jest.spyOn(",
    "mock_logger",
    "mockLogger",
    "LogCaptor",
    "ListAppender",
    "CapturingHandler",
    "capture_logs",
    "structlog.testing",
    "assert_logged",
    "toHaveBeenCalledWith",
    "levelname",
    "observer.New",
    "zaptest",
    "test.NewNullLogger",
    "hook.LastEntry",
];

/// Whether `path` is a test file in any common layout.
pub fn is_test_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    let file = p.rsplit('/').next().unwrap_or(&p);
    super::super::is_test_file(path)
        || p.starts_with("tests/")
        || p.contains("/tests/")
        || p.starts_with("test/")
        || p.contains("/test/")
        || file.starts_with("test_")
        || file.ends_with("_test.py")
        || file.ends_with("_test.go")
        || file.ends_with("_test.rs")
        || file.ends_with("test.java")
        || file.ends_with("tests.cs")
}

/// Whether the round's test changes assert a log change: a changed test
/// file captures logs and names the new text (or, for a message too short
/// to search for, the new level).
pub fn asserted_by(change: &LogChange, tests: &[(String, String)]) -> bool {
    let (level, text) = change.asserted();
    let term = search_term(text);
    tests.iter().any(|(_, added)| {
        let captures = CAPTURE_MARKS.iter().any(|m| added.contains(m));
        let names_text = term.as_deref().is_some_and(|t| added.contains(t));
        let a = added.to_ascii_lowercase();
        let names_level = [
            format!("\"{level}\""),
            format!("'{level}'"),
            format!("logging.{level}"),
            format!("level.{level}"),
            format!("level::{level}"),
            format!(".{level}("),
        ]
        .iter()
        .any(|m| a.contains(m.as_str()));
        captures && (names_text || (term.is_none() && names_level))
    })
}

/// The added lines of the round's test files, per file.
pub fn test_additions(hunks: &[NumberedHunk]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for h in hunks.iter().filter(|h| is_test_path(&h.hunk.path)) {
        let text = h.hunk.added_text().join("\n");
        match out.iter_mut().find(|(p, _)| *p == h.hunk.path) {
            Some((_, t)) => {
                t.push('\n');
                t.push_str(&text);
            }
            None => out.push((h.hunk.path.clone(), text)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::review_fix::hunks::file_hunks;

    fn numbered(path: &str, old: &str, new: &str) -> Vec<NumberedHunk> {
        file_hunks(path, Some(old), Some(new))
            .into_iter()
            .enumerate()
            .map(|(i, hunk)| NumberedHunk { id: i + 1, hunk })
            .collect()
    }

    #[test]
    fn log_calls_are_read_across_stacks() {
        let cases = [
            (
                r#"    logger.info("Synced %d rows", n)"#,
                "info",
                "Synced %d rows",
            ),
            (r#"logging.warning('stale cache')"#, "warn", "stale cache"),
            (r#"console.error(`failed: ${e}`)"#, "error", "failed: ${e}"),
            (
                r#"info!(run_id = %id, "Run started");"#,
                "info",
                "Run started",
            ),
            (r#"tracing::debug!("tick")"#, "debug", "tick"),
            (r#"log.Infof("polled %s", q)"#, "info", "polled %s"),
            (r#"self.logger.exception("boom")"#, "error", "boom"),
        ];
        for (line, level, msg) in cases {
            let c = log_call(line).unwrap_or_else(|| panic!("no call in {line}"));
            assert_eq!(c.level, level, "{line}");
            assert_eq!(c.message, msg, "{line}");
        }
        assert!(log_call("# logger.info(\"commented\")").is_none());
        assert!(log_call("let info = compute();").is_none());
    }

    #[test]
    fn a_level_change_a_rewording_an_addition_and_a_removal_are_told_apart() {
        let old = "def run():\n    log.info(\"Checked %d rows\", n)\n    x()\n    log.info(\"Run done\")\n    y()\n    log.warning(\"Old warning text here\")\n";
        let new = "def run():\n    log.debug(\"Checked %d rows\", n)\n    x()\n    log.info(\"Run finished\")\n    y()\n    z()\n    log.info(\"Brand new line\")\n";
        let found = changes(&numbered("jobs/sync.py", old, new));
        let kinds: Vec<String> = found.iter().map(|f| f.change.describe()).collect();
        assert!(kinds.contains(&"moves \"Checked %d rows\" from info to debug".to_string()));
        assert!(kinds.contains(&"rewords info log \"Run done\" to \"Run finished\"".to_string()));
        assert!(kinds.iter().any(|k| k.starts_with("adds info log")));
        assert!(kinds.iter().any(|k| k.starts_with("removes warn log")));
        let reworded = found
            .iter()
            .find(|f| matches!(f.change, LogChange::Text { .. }))
            .unwrap();
        assert_eq!(reworded.change.old_text(), Some("Run done"));
        assert!(reworded.change.needs_test());
    }

    #[test]
    fn search_terms_skip_placeholders_and_short_text() {
        assert_eq!(
            search_term("Synced {count} rows for team {team}").as_deref(),
            Some("rows for team")
        );
        assert_eq!(
            search_term("Nightly import finished in %d ms").as_deref(),
            Some("Nightly import finished in")
        );
        assert_eq!(search_term("ok %s"), None);
    }

    #[test]
    fn consumers_are_named_by_kind() {
        assert_eq!(
            consumer_kind("infra/alarms.tf", "pattern = \"Import failed\""),
            "alarm"
        );
        assert_eq!(
            consumer_kind("infra/logs.tf", "aws_cloudwatch_log_metric_filter"),
            "metric filter"
        );
        assert_eq!(
            consumer_kind(
                "tests/test_import.py",
                "assert \"Import failed\" in caplog.text"
            ),
            "test asserting the text"
        );
        assert_eq!(
            consumer_kind("scripts/check.sh", "grep 'Import failed'"),
            "script"
        );
        assert_eq!(
            consumer_kind("docs/runbooks/import.md", "Import failed"),
            "runbook"
        );
        assert_eq!(consumer_kind("README.md", "Import failed"), "docs");
        assert_eq!(
            consumer_kind("monitoring/prometheus/rules.yml", "Import failed"),
            "alert rule"
        );
    }

    #[test]
    fn repeated_jobs_are_found_by_path_or_text() {
        assert!(is_repeated_job("app/jobs/nightly.py", ""));
        assert!(is_repeated_job(
            "src/a.ts",
            "setInterval(() => poll(), 1000)"
        ));
        assert!(is_repeated_job(
            "svc/main.java",
            "@Scheduled(fixedRate = 60000)"
        ));
        assert!(!is_repeated_job("src/api/handlers.py", "def get(): pass"));
    }

    #[test]
    fn only_info_logs_in_repeated_jobs_are_candidates() {
        let found = vec![
            Found {
                hunk: 1,
                path: "jobs/sync.py".into(),
                change: LogChange::Added {
                    level: "info".into(),
                    message: "Starting sync".into(),
                },
            },
            Found {
                hunk: 2,
                path: "jobs/sync.py".into(),
                change: LogChange::Added {
                    level: "debug".into(),
                    message: "tick".into(),
                },
            },
            Found {
                hunk: 3,
                path: "api/view.py".into(),
                change: LogChange::Added {
                    level: "info".into(),
                    message: "Request served".into(),
                },
            },
        ];
        let c = info_in_repeated_jobs(&found, |_| String::new());
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].hunk, 1);
    }

    #[test]
    fn the_judge_answer_names_logs_that_fire_every_run() {
        let a = json!({"logs": [
            {"id": 1, "every_run": true, "reason": "at the top of run()"},
            {"id": 2, "every_run": false, "reason": "inside if changed"}
        ]});
        assert_eq!(every_run(&a), vec![(1, "at the top of run()".to_string())]);
        assert!(every_run(&json!({})).is_empty());
    }

    #[test]
    fn a_log_change_is_asserted_only_by_a_test_that_captures_and_names_it() {
        let change = LogChange::Level {
            from: "info".into(),
            to: "debug".into(),
            message: "Checked {} rows for import".into(),
        };
        let good = vec![(
            "tests/test_sync.py".to_string(),
            "    with caplog.at_level(logging.DEBUG):\n    assert \"rows for import\" in caplog.text"
                .to_string(),
        )];
        assert!(asserted_by(&change, &good));
        let no_capture = vec![(
            "tests/test_sync.py".to_string(),
            "assert run() == \"rows for import\"".to_string(),
        )];
        assert!(!asserted_by(&change, &no_capture));
        assert!(!asserted_by(&change, &[]));
    }

    #[test]
    fn test_additions_collect_added_lines_of_test_files_only() {
        let mut h = numbered("tests/test_a.py", "a\n", "a\nb\n");
        h.extend(numbered("src/a.py", "a\n", "a\nc\n"));
        let t = test_additions(&h);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].1, "b");
        assert!(is_test_path("pkg/foo_test.go"));
        assert!(is_test_path("src/__tests__/a.ts"));
        assert!(!is_test_path("src/attest.py"));
    }

    #[test]
    fn around_marks_the_line() {
        let text = "a\nb\nlog.info(\"x\")\nc\n";
        let r = around(text, "log.info", 1);
        assert!(r.contains(">    3 log.info"));
        assert!(r.contains("     2 b"));
    }
}
