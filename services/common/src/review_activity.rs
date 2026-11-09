//! What the reviewer did while it read a pull request: every search, file
//! view and lookup it made, the short notes it wrote on what it was checking,
//! and the comments it kept. The review's session page shows them live, in
//! groups of consecutive steps of one kind ("Search 3 times", "View 4 files").
//!
//! Only where the reviewer looked is recorded, never what it saw: a file view
//! keeps its path and line range, a search its query, any other tool its name
//! (and the symbol it asked about). Tool results and file contents are never
//! recorded, and every text passes the secret scrubber
//! ([`crate::log_findings::scrub`]) and a size cap first. A review keeps at
//! most [`MAX_EVENTS`] events; the rest are counted, not kept.
//!
//! The events are appended to the job's session record
//! ([`crate::review_session::append_activity`]) while the review runs, and
//! grouped when they are read ([`group`]).

use std::sync::Mutex;

use serde_json::Value;

/// A code or knowledge search (its query).
pub const KIND_SEARCH: &str = "search";
/// A file read (its path and line range).
pub const KIND_VIEW: &str = "view";
/// A folder listing or file pattern (its path or pattern).
pub const KIND_FIND: &str = "find";
/// Any other tool: history, live sources, registries, team agents (its name).
pub const KIND_LOOKUP: &str = "lookup";
/// A short note the reviewer wrote on what it is checking.
pub const KIND_NOTE: &str = "note";
/// A comment the review kept for the pull request (its place and title).
pub const KIND_COMMENT: &str = "comment";

/// Most events one review keeps.
pub const MAX_EVENTS: usize = 200;
/// Most notes one review keeps (they are the longest events).
pub const MAX_NOTES: usize = 40;
/// Longest label (a query, a path and range), in bytes.
pub const MAX_LABEL: usize = 160;
/// Longest note heading, in bytes.
pub const MAX_HEADING: usize = 80;
/// Longest note or comment text, in bytes.
pub const MAX_TEXT: usize = 400;

/// One thing the reviewer did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// When, in milliseconds since the epoch.
    pub at_ms: i64,
    /// One of the `KIND_*` words.
    pub kind: String,
    /// Where it looked: a query, `path:10-40`, a tool name. Empty for a note.
    pub label: String,
    /// A note's heading, or a comment's title.
    pub heading: String,
    /// A note's sentences. Empty for everything else.
    pub text: String,
    /// The tool call failed or was refused.
    pub failed: bool,
}

/// Pure: one line, no control characters, secrets scrubbed, at most `max`
/// bytes (cut on a char boundary, with an ellipsis when cut).
pub fn clean(text: &str, max: usize) -> String {
    let one_line: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let scrubbed = crate::log_findings::scrub(&one_line);
    if scrubbed.len() <= max {
        return scrubbed;
    }
    let cut = crate::truncate_str(&scrubbed, max.saturating_sub('…'.len_utf8()));
    format!("{}…", cut.trim_end())
}

fn str_of<'a>(input: &'a Value, key: &str) -> &'a str {
    input.get(key).and_then(Value::as_str).unwrap_or("").trim()
}

fn num_of(input: &Value, key: &str) -> Option<u64> {
    input.get(key).and_then(Value::as_u64)
}

/// Pure: what kind of step a tool call is, and its label. Only the fields
/// that say WHERE the reviewer looked are read (`path`, line numbers,
/// `query`, `pattern`, `name`, `paths`); any other input is left out.
pub fn classify(tool: &str, input: &Value) -> (&'static str, String) {
    let path = str_of(input, "path");
    let query = str_of(input, "query");
    let name = str_of(input, "name");
    match tool {
        "read_file_lines" | "read_lines" => {
            let start = num_of(input, "start_line");
            let end = num_of(input, "end_line");
            let label = match (start, end) {
                (Some(s), Some(e)) if e > s => format!("{path}:{s}-{e}"),
                (Some(s), _) => format!("{path}:{s}"),
                _ => path.to_string(),
            };
            (KIND_VIEW, label)
        }
        "read_file" => (KIND_VIEW, path.to_string()),
        "list_directory" | "list_files" => (
            KIND_FIND,
            if path.is_empty() {
                "/".into()
            } else {
                path.into()
            },
        ),
        "glob" | "find_files" => (KIND_FIND, str_of(input, "pattern").to_string()),
        "graph_callers" => (KIND_SEARCH, format!("callers of {name}")),
        "graph_definition" => (KIND_SEARCH, format!("definition of {name}")),
        "graph_impact" => {
            let paths: Vec<&str> = input
                .get("paths")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            (KIND_SEARCH, format!("what depends on {}", paths.join(", ")))
        }
        _ if !query.is_empty() && tool.contains("search") => (KIND_SEARCH, query.to_string()),
        "check_package" => {
            let registry = str_of(input, "registry");
            let version = str_of(input, "version");
            let label = match (registry.is_empty(), version.is_empty()) {
                (false, false) => format!("{registry}: {name}@{version}"),
                (false, true) => format!("{registry}: {name}"),
                _ => name.to_string(),
            };
            (KIND_LOOKUP, label)
        }
        _ => (KIND_LOOKUP, String::new()),
    }
}

/// Pure: a tool name in words ("github_history" reads "Github history").
pub fn tool_words(tool: &str) -> String {
    let words = tool.replace(['_', '-'], " ");
    let mut c = words.trim().chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => "Tool".into(),
    }
}

impl Event {
    /// A tool call at `at_ms`. Never holds the call's result.
    pub fn tool(at_ms: i64, tool: &str, input: &Value, failed: bool) -> Event {
        let (kind, label) = classify(tool, input);
        // A lookup is shown by the tool's name; its label, when it has one,
        // says what it looked up.
        let heading = if kind == KIND_LOOKUP {
            clean(&tool_words(tool), MAX_HEADING)
        } else {
            String::new()
        };
        Event {
            at_ms,
            kind: kind.to_string(),
            label: clean(&label, MAX_LABEL),
            heading,
            text: String::new(),
            failed,
        }
    }

    /// A note the reviewer wrote before its next tool calls. `None` when
    /// nothing is left once code blocks are dropped.
    pub fn note(at_ms: i64, written: &str) -> Option<Event> {
        let (heading, text) = split_note(written);
        let heading = clean(&heading, MAX_HEADING);
        let text = clean(&text, MAX_TEXT);
        if heading.is_empty() && text.is_empty() {
            return None;
        }
        Some(Event {
            at_ms,
            kind: KIND_NOTE.into(),
            label: String::new(),
            heading,
            text,
            failed: false,
        })
    }

    /// A comment the review kept, at `path:line`.
    pub fn comment(at_ms: i64, path: &str, line: u64, title: &str) -> Event {
        let place = if line > 0 {
            format!("{path}:{line}")
        } else {
            path.to_string()
        };
        Event {
            at_ms,
            kind: KIND_COMMENT.into(),
            label: clean(&place, MAX_LABEL),
            heading: clean(title, MAX_HEADING),
            text: String::new(),
            failed: false,
        }
    }
}

/// Pure: a note's heading and sentences. Fenced code is dropped (a note
/// never carries code or file contents). The heading is a first line in
/// bold (`**Checking callers**`) or a markdown heading; a short first line
/// followed by more text counts too. Otherwise there is no heading.
pub fn split_note(written: &str) -> (String, String) {
    let mut lines: Vec<&str> = Vec::new();
    let mut in_fence = false;
    for line in written.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if !in_fence && !line.trim().is_empty() {
            lines.push(line.trim());
        }
    }
    let Some((first, rest)) = lines.split_first() else {
        return (String::new(), String::new());
    };
    let bold = first
        .strip_prefix("**")
        .and_then(|s| s.split_once("**"))
        .map(|(h, after)| (h.trim().trim_end_matches(':').trim_end(), after.trim()));
    if let Some((heading, after)) = bold {
        let after = after.trim_start_matches([':', '.', '-']).trim();
        let mut text = vec![after];
        text.extend(rest.iter().copied());
        return (heading.to_string(), join_text(&text));
    }
    if let Some(h) = first.strip_prefix('#') {
        let h = h.trim_start_matches('#').trim().trim_end_matches(':');
        return (h.to_string(), join_text(rest));
    }
    let short = first.len() <= MAX_HEADING && !first.ends_with(['.', '?', '!', ':']);
    if short && !rest.is_empty() {
        return (first.to_string(), join_text(rest));
    }
    (String::new(), join_text(&lines))
}

fn join_text(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The events of one review, as it runs: the reviewer's tool loop adds them,
/// and a writer takes them in batches ([`Recorder::drain`]) to append to the
/// session. Keeps the caps across the whole review.
#[derive(Default)]
pub struct Recorder {
    inner: Mutex<RecorderState>,
}

#[derive(Default)]
struct RecorderState {
    pending: Vec<Event>,
    kept: usize,
    notes: usize,
    dropped: u64,
}

impl Recorder {
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, RecorderState> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Keep `e` unless the review is past its caps (then count it).
    pub fn push(&self, e: Event) {
        let mut s = self.state();
        let is_note = e.kind == KIND_NOTE;
        if s.kept >= MAX_EVENTS || (is_note && s.notes >= MAX_NOTES) {
            s.dropped += 1;
            return;
        }
        s.kept += 1;
        if is_note {
            s.notes += 1;
        }
        s.pending.push(e);
    }

    /// A tool call that just ended.
    pub fn tool(&self, tool: &str, input: &Value, failed: bool) {
        self.push(Event::tool(now_ms(), tool, input, failed));
    }

    /// What the reviewer wrote before its next tool calls.
    pub fn note(&self, written: &str) {
        if let Some(e) = Event::note(now_ms(), written) {
            self.push(e);
        }
    }

    /// A comment the review kept.
    pub fn comment(&self, path: &str, line: u64, title: &str) {
        self.push(Event::comment(now_ms(), path, line, title));
    }

    /// The events since the last drain, and how many were dropped since.
    pub fn drain(&self) -> (Vec<Event>, u64) {
        let mut s = self.state();
        let dropped = std::mem::take(&mut s.dropped);
        (std::mem::take(&mut s.pending), dropped)
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// One entry of a group: a query, a `path:lines`, a comment's place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub at_ms: i64,
    pub label: String,
    /// A comment's title, or a lookup's tool in words; empty otherwise.
    pub detail: String,
    pub failed: bool,
}

/// Consecutive events of one kind, as the session page shows them. A note
/// is always a group of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    pub kind: String,
    /// "Search 3 times", "View 4 files", a note's heading.
    pub title: String,
    pub started_ms: i64,
    pub ended_ms: i64,
    /// A note's sentences; empty for other groups.
    pub text: String,
    pub items: Vec<Item>,
}

fn plural(n: usize, one: &str, many: &str) -> String {
    if n == 1 {
        one.to_string()
    } else {
        format!("{n} {many}")
    }
}

/// Pure: a group's title from its kind and entries.
pub fn group_title(kind: &str, items: &[Item]) -> String {
    let n = items.len();
    match kind {
        KIND_SEARCH => format!("Search {}", plural(n, "once", "times")),
        KIND_VIEW => {
            let mut files: Vec<&str> = items
                .iter()
                .map(|i| {
                    i.label
                        .rsplit_once(':')
                        .map_or(i.label.as_str(), |(p, _)| p)
                })
                .collect();
            files.sort_unstable();
            files.dedup();
            format!("View {}", plural(files.len(), "1 file", "files"))
        }
        KIND_FIND => "Find files".into(),
        KIND_COMMENT => format!("Store {}", plural(n, "comment", "comments")),
        _ => {
            let mut tools: Vec<&str> = items.iter().map(|i| i.detail.as_str()).collect();
            tools.dedup();
            match tools.as_slice() {
                [one] if n == 1 => one.to_string(),
                [one] => format!("{one} ({n} times)"),
                _ => format!("Use {n} tools"),
            }
        }
    }
}

/// Pure: `events` in groups of consecutive events of one kind, oldest first
/// (a note on its own). Events are taken in the order given (the order they
/// were appended).
pub fn group(events: &[Event]) -> Vec<Group> {
    let mut out: Vec<Group> = Vec::new();
    for e in events {
        if e.kind == KIND_NOTE {
            out.push(Group {
                kind: KIND_NOTE.into(),
                title: if e.heading.is_empty() {
                    "Note".into()
                } else {
                    e.heading.clone()
                },
                started_ms: e.at_ms,
                ended_ms: e.at_ms,
                text: e.text.clone(),
                items: Vec::new(),
            });
            continue;
        }
        let item = Item {
            at_ms: e.at_ms,
            label: e.label.clone(),
            detail: e.heading.clone(),
            failed: e.failed,
        };
        match out.last_mut() {
            Some(g) if g.kind == e.kind => {
                g.ended_ms = g.ended_ms.max(e.at_ms);
                g.items.push(item);
            }
            _ => out.push(Group {
                kind: e.kind.clone(),
                title: String::new(),
                started_ms: e.at_ms,
                ended_ms: e.at_ms,
                text: String::new(),
                items: vec![item],
            }),
        }
    }
    for g in &mut out {
        if g.kind != KIND_NOTE {
            g.title = group_title(&g.kind, &g.items);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(at: i64, tool: &str, input: Value) -> Event {
        Event::tool(at, tool, &input, false)
    }

    #[test]
    fn a_tool_call_keeps_where_it_looked_and_nothing_else() {
        let e = ev(
            1,
            "read_file_lines",
            json!({"path": "src/order.rs", "start_line": 10, "end_line": 42}),
        );
        assert_eq!(
            (e.kind.as_str(), e.label.as_str()),
            (KIND_VIEW, "src/order.rs:10-42")
        );
        let e = ev(
            1,
            "read_file_lines",
            json!({"path": "a.rs", "start_line": 7, "end_line": 7}),
        );
        assert_eq!(e.label, "a.rs:7");
        assert_eq!(ev(1, "read_file", json!({"path": "a.rs"})).label, "a.rs");
        let e = ev(1, "search_code", json!({"query": "fn settle"}));
        assert_eq!(
            (e.kind.as_str(), e.label.as_str()),
            (KIND_SEARCH, "fn settle")
        );
        assert_eq!(
            ev(1, "search_knowledge", json!({"query": "refunds"})).kind,
            KIND_SEARCH
        );
        assert_eq!(ev(1, "list_directory", json!({"path": ""})).label, "/");
        assert_eq!(ev(1, "list_directory", json!({})).kind, KIND_FIND);
        assert_eq!(
            ev(1, "graph_callers", json!({"name": "settle"})).label,
            "callers of settle"
        );
        assert_eq!(
            ev(1, "graph_impact", json!({"paths": ["a.rs", "b.rs"]})).label,
            "what depends on a.rs, b.rs"
        );
        assert_eq!(
            ev(
                1,
                "check_package",
                json!({"registry": "npm", "name": "next", "version": "15.3.0"})
            )
            .label,
            "npm: next@15.3.0"
        );
        // Any other tool: its name only, never its input (a SQL query, a URL,
        // a payload).
        let e = ev(
            1,
            "db_query",
            json!({"sql": "select email from users where token = 'abc'"}),
        );
        assert_eq!(e.kind, KIND_LOOKUP);
        assert_eq!(e.label, "");
        assert_eq!(e.heading, "Db query");
        assert!(!format!("{e:?}").contains("email"));
        let e = ev(1, "api_get", json!({"path": "/v1/orders?key=secret"}));
        assert_eq!(e.kind, KIND_LOOKUP);
        assert_eq!(e.label, "");
    }

    #[test]
    fn secrets_and_long_text_never_reach_an_event() {
        let token = format!("ghp_{}", "a".repeat(40));
        let e = ev(1, "search_code", json!({"query": format!("token {token}")}));
        assert!(!e.label.contains(&token), "{}", e.label);
        assert!(e.label.contains("[GITHUB_TOKEN]"));
        let long = "x".repeat(1_000);
        let e = ev(1, "search_code", json!({"query": long}));
        assert!(e.label.len() <= MAX_LABEL);
        assert!(e.label.ends_with('…'));
        let note = Event::note(1, &format!("**Checking**\n{}", "word ".repeat(400))).unwrap();
        assert!(note.text.len() <= MAX_TEXT);
        // Multi-byte text is cut on a char boundary.
        let e = ev(1, "search_code", json!({"query": "é".repeat(300)}));
        assert!(e.label.len() <= MAX_LABEL);
        // Control characters and newlines become one line.
        let e = ev(1, "search_code", json!({"query": "a\nb\tc\u{7}"}));
        assert_eq!(e.label, "a b c");
    }

    #[test]
    fn a_note_has_a_heading_and_never_code() {
        assert_eq!(
            split_note("**Evaluating order semantics**\nThe new sort runs before the filter."),
            (
                "Evaluating order semantics".into(),
                "The new sort runs before the filter.".into()
            )
        );
        assert_eq!(
            split_note("**Checking callers:** three places call it."),
            ("Checking callers".into(), "three places call it.".into())
        );
        assert_eq!(
            split_note("## Reading the tests\nTwo cover it."),
            ("Reading the tests".into(), "Two cover it.".into())
        );
        assert_eq!(
            split_note("Checking the retry path\nIt retries twice."),
            ("Checking the retry path".into(), "It retries twice.".into())
        );
        assert_eq!(
            split_note("I will read the callers next."),
            (String::new(), "I will read the callers next.".into())
        );
        let (h, t) = split_note(
            "**Comparing**\nThe handler reads:\n```rust\nlet key = \"sk_live_123\";\n```\nThen returns.",
        );
        assert_eq!(h, "Comparing");
        assert_eq!(t, "The handler reads: Then returns.");
        assert!(Event::note(1, "```\nonly code\n```").is_none());
        assert!(Event::note(1, "   \n").is_none());
    }

    #[test]
    fn the_recorder_keeps_its_caps_and_counts_the_rest() {
        let r = Recorder::new();
        for i in 0..(MAX_NOTES + 5) {
            r.push(Event::note(i as i64, "**Step**\nmore").unwrap());
        }
        let (kept, dropped) = r.drain();
        assert_eq!(kept.len(), MAX_NOTES);
        assert_eq!(dropped, 5);
        // The caps hold across drains.
        for i in 0..MAX_EVENTS {
            r.push(ev(i as i64, "read_file", json!({"path": "a.rs"})));
        }
        let (kept, dropped) = r.drain();
        assert_eq!(kept.len(), MAX_EVENTS - MAX_NOTES);
        assert_eq!(dropped, MAX_NOTES as u64);
        r.comment("a.rs", 3, "Off by one");
        assert_eq!(r.drain(), (vec![], 1));
        assert_eq!(r.drain(), (vec![], 0));
    }

    #[test]
    fn consecutive_steps_of_one_kind_group_like_copilot() {
        let events = vec![
            Event::note(1, "**Evaluating order semantics**\nThe sort moved.").unwrap(),
            ev(2, "search_code", json!({"query": "sort_by"})),
            ev(3, "search_code", json!({"query": "filter_orders"})),
            ev(4, "search_org", json!({"query": "OrderList"})),
            ev(
                5,
                "read_file_lines",
                json!({"path": "a.rs", "start_line": 1, "end_line": 9}),
            ),
            ev(
                6,
                "read_file_lines",
                json!({"path": "a.rs", "start_line": 20, "end_line": 30}),
            ),
            ev(7, "read_file", json!({"path": "b.rs"})),
            ev(8, "list_directory", json!({"path": "src"})),
            ev(9, "search_code", json!({"query": "again"})),
            ev(10, "github_history", json!({"path": "a.rs"})),
            ev(11, "github_history", json!({})),
            ev(12, "check_package", json!({"registry": "npm", "name": "x"})),
            Event::comment(13, "a.rs", 4, "Sort runs after the filter"),
            Event::comment(14, "b.rs", 0, "Missing test"),
        ];
        let groups = group(&events);
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(
            titles,
            vec![
                "Evaluating order semantics",
                "Search 3 times",
                "View 2 files",
                "Find files",
                "Search once",
                "Use 3 tools",
                "Store 2 comments",
            ]
        );
        let note = &groups[0];
        assert_eq!(note.kind, KIND_NOTE);
        assert_eq!(note.text, "The sort moved.");
        assert!(note.items.is_empty());
        let search = &groups[1];
        assert_eq!((search.started_ms, search.ended_ms), (2, 4));
        let labels: Vec<&str> = search.items.iter().map(|i| i.label.as_str()).collect();
        assert_eq!(labels, vec!["sort_by", "filter_orders", "OrderList"]);
        assert_eq!(groups[2].items[1].label, "a.rs:20-30");
        assert_eq!(groups[6].items[0].label, "a.rs:4");
        assert_eq!(groups[6].items[0].detail, "Sort runs after the filter");
        assert_eq!(groups[6].items[1].label, "b.rs");
        assert!(group(&[]).is_empty());
    }

    #[test]
    fn titles_read_in_plain_words() {
        let item = |label: &str, detail: &str| Item {
            at_ms: 0,
            label: label.into(),
            detail: detail.into(),
            failed: false,
        };
        assert_eq!(
            group_title(KIND_VIEW, &[item("a.rs:1-2", "")]),
            "View 1 file"
        );
        assert_eq!(
            group_title(KIND_COMMENT, &[item("a.rs:1", "t")]),
            "Store comment"
        );
        assert_eq!(
            group_title(KIND_LOOKUP, &[item("", "Github history")]),
            "Github history"
        );
        assert_eq!(
            group_title(
                KIND_LOOKUP,
                &[item("", "Github history"), item("", "Github history")]
            ),
            "Github history (2 times)"
        );
        assert_eq!(tool_words("check_url"), "Check url");
        assert_eq!(tool_words(""), "Tool");
    }

    #[test]
    fn a_failed_call_stays_marked() {
        let e = Event::tool(1, "read_file", &json!({"path": "gone.rs"}), true);
        assert!(group(&[e])[0].items[0].failed);
    }
}
