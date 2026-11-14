//! bg. A fix round's agent log read "3 things didn't work". The coder
//! called `bash` (its training has a shell) and was refused with "Nothing
//! says what bash does, so it doesn't run. Unknown tools are always
//! denied. Don't try this again. Carry on without it": the step was lost
//! and nothing said which tool did the job. Then two `edit_file` calls in
//! a row failed with "`edits` was a string that isn't a JSON array": the
//! model had sent the array as a string with raw line breaks inside its
//! strings, which strict JSON refuses; the third try went through.
//!
//! Guarantee: the coder has a `bash` tool. Its reads run as the file tools
//! at once, the repository's own commands run in the sandbox (exit code,
//! time, the end of the output, stopped after a time limit), and what never
//! runs here (writes, git on the branch or the remote, network, secrets)
//! is refused with the tool to use instead. A call to a well known tool
//! name of another agent (`str_replace_based_edit_tool`, `Edit`, `Grep`,
//! `view`, `ls`) runs as the CoderHelm tool that does the same job; where
//! none is offered, the refusal names the alternative. `edit_file` reads
//! its edits from a string, a single object or other tools' key names, and
//! says exactly what was wrong only when nothing can be read.

use common::coder_shell::{classify, Shell};
use common::tool_aliases::{resolve, Resolution};
use serde_json::json;

use crate::passes::implement::{apply_text_edits, command_report, command_script, tool_edits};

/// The fix round's tools (no `bash` on the reviewer side of the run).
fn coder(n: &str) -> bool {
    matches!(
        n,
        "read_file"
            | "read_file_lines"
            | "search_code"
            | "list_directory"
            | "read_tree"
            | "get_diff"
            | "edit_file"
            | "write_file"
            | "run_checks"
            | "bash"
    )
}

#[test]
fn the_shell_call_runs_and_says_what_it_could_not_do() {
    // The round's three calls, as the model sent them.
    assert_eq!(
        classify("cd apps/shop && npx vitest run src/cart/total.test.ts 2>&1 | tail -40"),
        Shell::Run {
            command: "npx vitest run src/cart/total.test.ts".into(),
            dir: Some("apps/shop".into()),
            dropped: Some("| tail -40".into()),
        }
    );
    assert_eq!(
        classify("grep -rn \"applyCoupon(\" apps/shop/src"),
        Shell::Search {
            query: "applyCoupon(".into()
        }
    );
    match classify("git commit -am 'fix totals' && git push") {
        Shell::Refuse(why) => assert!(why.contains("CoderHelm pushes"), "{why}"),
        other => panic!("{other:?}"),
    }
    // The command the sandbox runs: in its folder, under a time limit.
    let script = command_script("npx vitest run src/cart/total.test.ts", Some("apps/shop"));
    assert!(script.starts_with("timeout "), "{script}");
    assert!(
        script.contains("cd apps/shop && npx vitest run"),
        "{script}"
    );
    let report = command_report("npx vitest run", 1, 74, Some("| tail -40"), "1 failed\n");
    assert!(report.contains("exited 1 after 74s") && report.contains("1 failed"));
    assert!(report.contains("not kept"));
}

#[test]
fn other_agents_tool_names_run_as_coderhelm_tools() {
    let ran = |name: &str, input: serde_json::Value| match resolve(name, &input, &coder) {
        Some(Resolution::Run { tool, input }) => (tool, input),
        other => panic!("{name}: {other:?}"),
    };
    assert_eq!(
        ran(
            "str_replace_based_edit_tool",
            json!({"command": "view", "path": "apps/shop/src/cart/total.ts"})
        )
        .0,
        "read_file"
    );
    assert_eq!(
        ran("Grep", json!({"pattern": "applyCoupon"})).0,
        "search_code"
    );
    assert_eq!(ran("ls", json!({"path": "apps/shop"})).0, "list_directory");
    assert_eq!(
        ran("run_terminal_cmd", json!({"command": "npm test"})).0,
        "bash"
    );
    // Where `bash` isn't offered, the refusal names the tools to use.
    let reviewer = |n: &str| matches!(n, "read_file" | "search_code");
    let why = common::agent_policy::engine::unknown_reason("bash");
    assert!(
        why.contains("search_code") && !why.contains("Nothing says"),
        "{why}"
    );
    assert!(resolve("Edit", &json!({"file_path": "a"}), &reviewer).is_none());
}

#[test]
fn the_edits_the_round_sent_apply_the_first_time() {
    let file = "export function total(items) {\n  return sum(items);\n}\n";
    // The array as a string, with raw line breaks inside its strings.
    let sent = json!({
        "path": "apps/shop/src/cart/total.ts",
        "message": "Apply the coupon",
        "edits": "[{\"old_text\": \"  return sum(items);\n}\", \"new_text\": \"  return applyCoupon(sum(items));\n}\"}]",
    });
    let edits = tool_edits(&sent).expect("read the first time");
    assert_eq!(
        apply_text_edits(file, &edits).unwrap(),
        "export function total(items) {\n  return applyCoupon(sum(items));\n}\n"
    );
    // Garbage still says exactly what was wrong, never the old words.
    let e = tool_edits(&json!({"edits": "[{old_text: nope"})).unwrap_err();
    assert!(!e.contains("isn't a JSON array"), "{e}");
    assert!(e.contains("isn't JSON (line 1"), "{e}");
}
