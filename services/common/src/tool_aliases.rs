//! Tool names models are trained on, and the CoderHelm tools that do the
//! same job.
//!
//! A model trained with Claude Code's tools, Anthropic's text editor and
//! bash tools, or another coding agent's, sometimes calls one of those
//! names instead of the tool it was given. Refusing that call as an
//! unknown tool costs a step and teaches the model nothing. [`resolve`]
//! turns a call to a well known name into the equivalent call of an
//! offered tool, with its input in that tool's shape; when there is no
//! equivalent it answers with what to use instead ([`alternative`] gives
//! the same words to the policy's refusal). Pure: no I/O.

use serde_json::{json, Value};

use crate::coder_shell::{self, Shell};

/// What a call to a well known name becomes.
#[derive(Debug, Clone, PartialEq)]
pub enum Resolution {
    /// Run this offered tool with this input instead.
    Run { tool: String, input: Value },
    /// Don't run anything: the model reads this (what to use instead).
    Answer(String),
}

/// Names of a shell tool.
const SHELLS: &[&str] = &[
    "bash",
    "shell",
    "sh",
    "zsh",
    "terminal",
    "run_command",
    "execute_command",
    "run_shell_command",
    "run_terminal_cmd",
    "exec",
    "execute",
    "bash_tool",
    "command",
];

/// Names of Anthropic's text editor tool.
const EDITORS: &[&str] = &[
    "str_replace_based_edit_tool",
    "str_replace_editor",
    "text_editor",
];

const READS: &[&str] = &[
    "read",
    "view",
    "cat",
    "open_file",
    "read_file_content",
    "view_file",
];
const EDITS: &[&str] = &[
    "edit",
    "multiedit",
    "multi_edit",
    "str_replace",
    "replace",
    "apply_edit",
    "edit_files",
    "replace_in_file",
    "search_replace",
];
const WRITES: &[&str] = &[
    "write",
    "create",
    "create_file",
    "write_to_file",
    "new_file",
];
const SEARCHES: &[&str] = &[
    "grep",
    "search",
    "rg",
    "ripgrep",
    "codebase_search",
    "search_files",
    "find_in_files",
    "grep_search",
    "search_code_base",
];
const GLOBS: &[&str] = &[
    "glob",
    "find",
    "find_files",
    "file_search",
    "list_files_recursive",
];
const LISTS: &[&str] = &["ls", "list", "list_dir", "list_files", "list_folder"];
const PATCHES: &[&str] = &["apply_patch", "apply_diff", "patch"];

fn s<'a>(input: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|k| input.get(*k).and_then(Value::as_str))
}

fn n(input: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|k| {
        let v = input.get(*k)?;
        v.as_u64().or_else(|| v.as_str()?.trim().parse().ok())
    })
}

const PATH_KEYS: &[&str] = &[
    "path",
    "file_path",
    "filePath",
    "file",
    "filename",
    "target_file",
];

/// A path as CoderHelm's tools take it (relative to the repository root).
fn rel(p: &str) -> String {
    let p = p.trim().trim_start_matches("./");
    let p = p.strip_prefix("/repo/").unwrap_or(p);
    p.trim_end_matches('/').to_string()
}

/// A commit message for an aliased write, which the model didn't write.
fn message(input: &Value, verb: &str, path: &str) -> String {
    s(input, &["message", "commit_message", "description"])
        .map(str::to_string)
        .unwrap_or_else(|| format!("{verb} {path}"))
}

/// A read of `path`, whole or by lines.
fn read_call(
    path: String,
    lines: Option<(u64, u64)>,
    offered: &dyn Fn(&str) -> bool,
) -> Option<Resolution> {
    match lines {
        Some((a, b)) if offered("read_file_lines") => Some(Resolution::Run {
            tool: "read_file_lines".into(),
            input: json!({"path": path, "start_line": a.max(1), "end_line": b.max(a.max(1))}),
        }),
        _ if offered("read_file") => Some(Resolution::Run {
            tool: "read_file".into(),
            input: json!({"path": path}),
        }),
        _ => None,
    }
}

fn run(tool: &str, input: Value, offered: &dyn Fn(&str) -> bool) -> Option<Resolution> {
    offered(tool).then(|| Resolution::Run {
        tool: tool.into(),
        input,
    })
}

/// The edit_file call for search and replace pairs, in any of the names
/// models give them.
fn edit_call(path: &str, input: &Value, offered: &dyn Fn(&str) -> bool) -> Option<Resolution> {
    let edits = if let Some(list) = input.get("edits") {
        list.clone()
    } else {
        json!([{
            "old_text": s(input, &["old_text", "old_string", "old_str", "search", "find"]).unwrap_or(""),
            "new_text": s(input, &["new_text", "new_string", "new_str", "replace", "replacement"]).unwrap_or(""),
        }])
    };
    run(
        "edit_file",
        json!({"path": path, "edits": edits, "message": message(input, "Edit", path)}),
        offered,
    )
}

/// What a shell command line becomes when no `bash` tool was offered.
fn shell_call(line: &str, offered: &dyn Fn(&str) -> bool) -> Resolution {
    let call = match coder_shell::classify(line) {
        Shell::Read { path, lines } => read_call(path, lines, offered),
        Shell::List { path } => run("list_directory", json!({"path": path}), offered),
        Shell::Search { query } => run("search_code", json!({"query": query}), offered),
        Shell::Tree => run("read_tree", json!({}), offered)
            .or_else(|| run("list_directory", json!({"path": ""}), offered)),
        Shell::Diff => run("get_diff", json!({}), offered),
        Shell::Refuse(why) => return Resolution::Answer(why),
        Shell::Empty => return Resolution::Answer("The command was empty.".into()),
        Shell::Run { .. } => None,
    };
    call.unwrap_or_else(|| Resolution::Answer(no_commands(offered)))
}

/// The answer for a command when this step runs none.
fn no_commands(offered: &dyn Fn(&str) -> bool) -> String {
    let mut out = String::from("Commands don't run in this step.");
    if offered("run_checks") {
        out.push_str(
            " run_checks runs the repository's build, type check, lint and tests in the sandbox \
             and returns their output.",
        );
    }
    out.push(' ');
    out.push_str(READ_TOOLS);
    out
}

const READ_TOOLS: &str = "Read a file with read_file (or read_file_lines for a range), find \
    code with search_code, list a folder with list_directory.";

/// The call to an offered tool that does what a call to `name` asked, an
/// answer that says what to use instead, or `None` when `name` is offered
/// itself or isn't a name this knows.
pub fn resolve(name: &str, input: &Value, offered: &dyn Fn(&str) -> bool) -> Option<Resolution> {
    if offered(name) {
        return None;
    }
    let key = name.to_ascii_lowercase();
    let k = key.as_str();
    let path = s(input, PATH_KEYS).map(rel);
    if SHELLS.contains(&k) {
        let line = s(input, &["command", "cmd", "script", "input"]).unwrap_or("");
        if input.get("restart").and_then(Value::as_bool) == Some(true) && line.is_empty() {
            return Some(Resolution::Answer(
                "Nothing to restart: each command runs on its own.".into(),
            ));
        }
        if offered("bash") {
            return Some(Resolution::Run {
                tool: "bash".into(),
                input: json!({"command": line}),
            });
        }
        return Some(shell_call(line, offered));
    }
    if EDITORS.contains(&k) {
        let path = path.unwrap_or_default();
        let command = s(input, &["command"]).unwrap_or("view");
        return match command {
            "view" => {
                let looks_like_dir =
                    path.is_empty() || !path.rsplit('/').next().unwrap_or("").contains('.');
                let range = input
                    .get("view_range")
                    .and_then(Value::as_array)
                    .and_then(|r| {
                        let a = r.first()?.as_u64()?;
                        let b = r.get(1).and_then(Value::as_i64).unwrap_or(-1);
                        Some((a, if b < 0 { a + 200 } else { b as u64 }))
                    });
                if looks_like_dir && range.is_none() {
                    run("list_directory", json!({"path": path}), offered)
                } else {
                    read_call(path, range, offered)
                }
            }
            "create" => run(
                "write_file",
                json!({"path": path, "content": s(input, &["file_text", "content"]).unwrap_or(""),
                       "message": message(input, "Create", &path)}),
                offered,
            ),
            "str_replace" => edit_call(&path, input, offered),
            "insert" => Some(Resolution::Answer(
                "insert isn't available: use edit_file, with old_text the line you insert after \
                 and new_text that line followed by the new lines."
                    .into(),
            )),
            "undo_edit" => Some(Resolution::Answer(
                "undo_edit isn't available: restore_file puts a file back as it is on the base \
                 branch; edit_file changes it back by hand."
                    .into(),
            )),
            other => Some(Resolution::Answer(format!(
                "The editor command `{other}` isn't available. {READ_TOOLS} Change a file with \
                 edit_file, create one with write_file."
            ))),
        };
    }
    if READS.contains(&k) {
        let path = path?;
        let lines = match (
            n(input, &["offset", "start_line", "line"]),
            n(input, &["limit"]),
        ) {
            (Some(o), Some(l)) => Some((o.max(1), o.max(1) + l.saturating_sub(1))),
            (None, Some(l)) => Some((1, l.max(1))),
            (Some(o), None) => n(input, &["end_line"]).map(|e| (o.max(1), e)),
            (None, None) => None,
        };
        return read_call(path, lines, offered);
    }
    if EDITS.contains(&k) {
        return edit_call(&path?, input, offered);
    }
    if WRITES.contains(&k) {
        let path = path?;
        let content = s(input, &["content", "file_text", "contents", "text"])?;
        return run(
            "write_file",
            json!({"path": path, "content": content, "message": message(input, "Write", &path)}),
            offered,
        );
    }
    if SEARCHES.contains(&k) {
        let q = s(input, &["query", "pattern", "regex", "search", "q", "text"])?;
        return run("search_code", json!({"query": q}), offered);
    }
    if GLOBS.contains(&k) {
        return run("read_tree", json!({}), offered);
    }
    if LISTS.contains(&k) {
        return run(
            "list_directory",
            json!({"path": path.unwrap_or_default()}),
            offered,
        );
    }
    if PATCHES.contains(&k) {
        return Some(Resolution::Answer(
            "Patches aren't applied here. Send each change with edit_file (old_text copied \
             exactly from the file, new_text its replacement), or rewrite a small file with \
             write_file."
                .into(),
        ));
    }
    None
}

/// The line a step that runs no commands puts in its instructions, so the
/// model doesn't reach for a shell it doesn't have.
pub const NO_SHELL_NOTE: &str = "\n\n## No shell in this step\nThere is no bash or terminal \
    here: read a file with read_file (read_file_lines for a range), find code with search_code, \
    and change files with edit_file and write_file. The repository's CI runs on the pull \
    request.";

/// What to use instead of a tool nothing knows, in plain words, for the
/// policy's refusal; `None` for a name this doesn't know.
pub fn alternative(name: &str) -> Option<&'static str> {
    let k = name.to_ascii_lowercase();
    let k = k.as_str();
    if SHELLS.contains(&k) {
        Some(
            "Commands don't run in this step. Read a file with read_file (or read_file_lines), \
             find code with search_code, list a folder with list_directory; when run_checks is \
             offered it runs the repository's build, type check, lint and tests.",
        )
    } else if EDITORS.contains(&k) || EDITS.contains(&k) || PATCHES.contains(&k) {
        Some("Change a file with edit_file (old_text and new_text pairs), or rewrite it with write_file.")
    } else if READS.contains(&k) {
        Some("Read a file with read_file, or read_file_lines for a range of lines.")
    } else if WRITES.contains(&k) {
        Some("Create or replace a file with write_file.")
    } else if SEARCHES.contains(&k) {
        Some("Find code with search_code.")
    } else if GLOBS.contains(&k) || LISTS.contains(&k) {
        Some("List a folder with list_directory, or every file with read_tree.")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CODER: &[&str] = &[
        "read_tree",
        "read_file",
        "read_file_lines",
        "search_code",
        "list_directory",
        "write_file",
        "edit_file",
        "batch_write",
        "get_diff",
        "restore_file",
        "run_checks",
    ];

    fn coder(n: &str) -> bool {
        CODER.contains(&n)
    }

    fn ran(r: Option<Resolution>) -> (String, Value) {
        match r {
            Some(Resolution::Run { tool, input }) => (tool, input),
            other => panic!("expected a run, got {other:?}"),
        }
    }

    fn answered(r: Option<Resolution>) -> String {
        match r {
            Some(Resolution::Answer(a)) => a,
            other => panic!("expected an answer, got {other:?}"),
        }
    }

    #[test]
    fn an_offered_name_is_left_alone() {
        assert_eq!(resolve("read_file", &json!({"path": "a"}), &coder), None);
        assert_eq!(resolve("made_up_tool", &json!({}), &coder), None);
    }

    #[test]
    fn claude_code_names_run_the_equivalent_tool() {
        let (t, i) = ran(resolve("Read", &json!({"file_path": "./src/a.ts"}), &coder));
        assert_eq!((t.as_str(), i), ("read_file", json!({"path": "src/a.ts"})));
        let (t, i) = ran(resolve(
            "Read",
            &json!({"file_path": "src/a.ts", "offset": 20, "limit": 10}),
            &coder,
        ));
        assert_eq!(t, "read_file_lines");
        assert_eq!(
            i,
            json!({"path": "src/a.ts", "start_line": 20, "end_line": 29})
        );
        let (t, i) = ran(resolve(
            "Edit",
            &json!({"file_path": "src/a.ts", "old_string": "a", "new_string": "b"}),
            &coder,
        ));
        assert_eq!(t, "edit_file");
        assert_eq!(i["edits"], json!([{"old_text": "a", "new_text": "b"}]));
        assert_eq!(i["message"], "Edit src/a.ts");
        let (t, i) = ran(resolve(
            "Write",
            &json!({"file_path": "src/new.ts", "content": "x"}),
            &coder,
        ));
        assert_eq!((t.as_str(), &i["content"]), ("write_file", &json!("x")));
        let (t, i) = ran(resolve("Grep", &json!({"pattern": "useFlag"}), &coder));
        assert_eq!(
            (t.as_str(), i),
            ("search_code", json!({"query": "useFlag"}))
        );
        assert_eq!(
            ran(resolve("Glob", &json!({"pattern": "**/*.ts"}), &coder)).0,
            "read_tree"
        );
        let (t, i) = ran(resolve("LS", &json!({"path": "src/"}), &coder));
        assert_eq!((t.as_str(), i), ("list_directory", json!({"path": "src"})));
    }

    #[test]
    fn the_text_editor_tool_runs_as_the_file_tools() {
        let ed = |i: Value| ran(resolve("str_replace_based_edit_tool", &i, &coder));
        let (t, i) = ed(json!({"command": "view", "path": "src/a.ts", "view_range": [5, 9]}));
        assert_eq!(
            (t.as_str(), &i["start_line"], &i["end_line"]),
            ("read_file_lines", &json!(5), &json!(9))
        );
        assert_eq!(
            ed(json!({"command": "view", "path": "src"})).0,
            "list_directory"
        );
        assert_eq!(
            ed(json!({"command": "view", "path": "src/a.ts"})).0,
            "read_file"
        );
        let (t, i) =
            ed(json!({"command": "str_replace", "path": "a.ts", "old_str": "x", "new_str": "y"}));
        assert_eq!(
            (t.as_str(), &i["edits"][0]["new_text"]),
            ("edit_file", &json!("y"))
        );
        let (t, i) = ed(json!({"command": "create", "path": "b.ts", "file_text": "z"}));
        assert_eq!((t.as_str(), &i["content"]), ("write_file", &json!("z")));
        let a = answered(resolve(
            "str_replace_editor",
            &json!({"command": "insert", "path": "a"}),
            &coder,
        ));
        assert!(a.contains("edit_file"), "{a}");
    }

    #[test]
    fn a_shell_call_runs_as_a_read_or_says_what_to_use() {
        let (t, i) = ran(resolve("bash", &json!({"command": "cat src/a.ts"}), &coder));
        assert_eq!((t.as_str(), i), ("read_file", json!({"path": "src/a.ts"})));
        let (t, i) = ran(resolve(
            "bash",
            &json!({"command": "grep -rn TODO src"}),
            &coder,
        ));
        assert_eq!((t.as_str(), i), ("search_code", json!({"query": "TODO"})));
        assert_eq!(
            ran(resolve("bash", &json!({"command": "git diff"}), &coder)).0,
            "get_diff"
        );
        // A build command, where commands don't run: the alternatives by name.
        let a = answered(resolve("bash", &json!({"command": "npm test"}), &coder));
        assert!(a.contains("run_checks") && a.contains("search_code"), "{a}");
        let a = answered(resolve("bash", &json!({"command": "git push"}), &coder));
        assert!(a.contains("CoderHelm pushes"), "{a}");
        // Where `bash` itself is offered, every shell name runs it.
        let with_bash = |n: &str| n == "bash" || coder(n);
        let (t, i) = ran(resolve(
            "run_terminal_cmd",
            &json!({"cmd": "npm test"}),
            &with_bash,
        ));
        assert_eq!((t.as_str(), i), ("bash", json!({"command": "npm test"})));
        assert_eq!(resolve("bash", &json!({"command": "ls"}), &with_bash), None);
    }

    #[test]
    fn a_target_that_isnt_offered_resolves_to_nothing() {
        let reviewer = |n: &str| matches!(n, "read_file" | "search_code");
        assert_eq!(
            resolve(
                "Edit",
                &json!({"file_path": "a", "old_string": "x", "new_string": "y"}),
                &reviewer
            ),
            None
        );
        assert!(alternative("Edit").unwrap().contains("edit_file"));
        assert!(alternative("bash").unwrap().contains("search_code"));
        assert_eq!(alternative("made_up_tool"), None);
    }
}
