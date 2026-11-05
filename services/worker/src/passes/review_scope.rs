//! How much of a pull request a push needs reviewed. One PR was reviewed four
//! times in a day, each a full review ending in the same approval, because
//! every push started from scratch. Once CoderHelm approved a commit, a later
//! push is reviewed from that commit: only what changed since. A push that
//! changed no code or data since then (docs, comments, whitespace) isn't
//! reviewed at all; the approval stands. A push after a request for changes
//! still gets a full review, so the whole change is checked again.

use serde_json::Value;

use super::review_sweep::ReviewRecord;

/// The webhook trigger of a review started by a push.
pub const TRIGGER_PUSH: &str = "synchronize";

/// The commit a push's review starts from: the head of CoderHelm's latest
/// review, when that review approved an earlier commit. `None` means a full
/// review: not a push, a person's question, no earlier review, or the latest
/// review asked for changes or questions (a full re-review checks it all).
pub fn approved_base(
    trigger: &str,
    is_question: bool,
    records_newest_first: &[ReviewRecord],
    head: &str,
) -> Option<String> {
    if trigger != TRIGGER_PUSH || is_question {
        return None;
    }
    let latest = records_newest_first
        .iter()
        .find(|r| r.is_review() && !r.failed())?;
    (latest.verdict == "APPROVE" && !latest.head_sha.is_empty() && latest.head_sha != head)
        .then(|| latest.head_sha.clone())
}

/// The commit a push's re-review starts from when CoderHelm's latest review
/// of an earlier commit did not approve (it asked for changes, or left
/// questions): that review's head. The push is then reviewed from there,
/// only the hunks changed since plus the status of the earlier findings,
/// instead of the whole PR again. `None` for anything but a push, for a
/// person's question, a forced full review, the same head, or no earlier
/// review. An approved latest review is [`approved_base`]'s.
pub fn reviewed_base(
    trigger: &str,
    is_question: bool,
    full_review: bool,
    records_newest_first: &[ReviewRecord],
    head: &str,
) -> Option<String> {
    if trigger != TRIGGER_PUSH || is_question || full_review {
        return None;
    }
    let latest = records_newest_first
        .iter()
        .find(|r| r.is_review() && !r.failed())?;
    (latest.verdict != "APPROVE" && !latest.head_sha.is_empty() && latest.head_sha != head)
        .then(|| latest.head_sha.clone())
}

/// The note a hunks-only re-review's diff starts with.
pub fn since_review_note(reviewed: &str) -> String {
    let short = &reviewed[..reviewed.len().min(7)];
    format!(
        "(CoderHelm reviewed {short} earlier and did not approve it. This review covers only \
         what changed since {short}, plus the status of each earlier finding listed under \
         \"Earlier review threads\"; the rest of the PR was reviewed at {short}. A full review \
         of the whole PR runs before any approval.)"
    )
}

/// A hunks-only re-review that would approve doesn't post: the whole PR is
/// reviewed first, so an approval always rests on a full review.
pub fn needs_full_before_approval(since_review: bool, verdict: &str) -> bool {
    since_review && verdict == "APPROVE"
}

/// CoderHelm's own PR, a push made by CoderHelm, while the run that owns
/// the PR is still in its own review and fix loop (`running` or
/// `awaiting_ci`): the run asks the PR reviewer for its verdict itself once
/// CI is green (`review_pr::review_for_run`), and a run that ends without
/// it hands the PR over for review ([`HANDOFF_TRIGGER`]). Reviewing each of
/// its intermediate pushes as well paid twice for the same code. A person's
/// push, or a run that has ended, is reviewed as always.
pub fn own_loop_holds_push(
    trigger: &str,
    self_authored: bool,
    head_by_coderhelm: bool,
    run_status: Option<&str>,
) -> bool {
    trigger == TRIGGER_PUSH
        && self_authored
        && head_by_coderhelm
        && matches!(
            run_status,
            Some(common::run_status::RUNNING) | Some(common::run_status::AWAITING_CI)
        )
}

/// The trigger of the review a run asks for when it hands its PR over
/// without the PR reviewer's verdict on the last head.
pub const HANDOFF_TRIGGER: &str = "handoff";

/// The author login of `head` in a PR's commit list (oldest first), when the
/// list holds it.
pub fn head_commit_login<'a>(commits: &'a [Value], head: &str) -> Option<&'a str> {
    commits
        .iter()
        .rev()
        .find(|c| c["sha"].as_str() == Some(head))
        .map(|c| {
            c["author"]["login"]
                .as_str()
                .or_else(|| c["committer"]["login"].as_str())
                .unwrap_or("")
        })
}

/// The approved commit is an ancestor of the head (the compare is `ahead`).
/// After a force-push or rebase it isn't, and the diff since it means
/// nothing: review the whole PR.
pub fn builds_on(since: &Value) -> bool {
    since["status"].as_str() == Some("ahead")
}

/// Whether the diff `since` changes code or data. Prose docs never do; a
/// lockfile always does; any other file does unless every changed line is a
/// comment or whitespace in its language. A file whose patch GitHub didn't
/// send (binary, too large), or that was added, removed or renamed, counts as
/// a change.
pub fn touches_code(since: &Value) -> bool {
    match since["files"].as_array() {
        Some(files) => files.iter().any(file_touches_code),
        None => true,
    }
}

/// Paths that changed code or data in `since`, for the log.
pub fn code_files(since: &Value) -> Vec<String> {
    since["files"]
        .as_array()
        .map(|files| {
            files
                .iter()
                .filter(|f| file_touches_code(f))
                .filter_map(|f| f["filename"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Whether the PR's own change (its base...head compare) is the same at two
/// heads: the same files, each with the same status and the same added and
/// removed lines. After pushes that only merge the base branch in (work
/// already merged there), the PR's change is the one approved even though
/// the commits since the approval change code. Line numbers and context
/// lines may move with the base and don't count. A file whose patch GitHub
/// didn't send, or a compare without its file list, is never the same.
pub fn same_own_change(at_approval: &Value, now: &Value) -> bool {
    fn changes(compare: &Value) -> Option<Vec<(String, String, Vec<String>)>> {
        let mut out = Vec::new();
        for f in compare["files"].as_array()? {
            let patch = f["patch"].as_str()?;
            let lines: Vec<String> = patch
                .lines()
                .filter(|l| l.starts_with('+') || l.starts_with('-'))
                .map(str::to_string)
                .collect();
            out.push((
                f["filename"].as_str()?.to_string(),
                f["status"].as_str().unwrap_or("").to_string(),
                lines,
            ));
        }
        out.sort();
        Some(out)
    }
    match (changes(at_approval), changes(now)) {
        (Some(a), Some(b)) => !a.is_empty() && a == b,
        _ => false,
    }
}

fn file_touches_code(f: &Value) -> bool {
    let name = f["filename"].as_str().unwrap_or("");
    if name.is_empty() || is_lockfile(name) {
        return true;
    }
    let previous = f["previous_filename"].as_str().unwrap_or(name);
    if is_doc(name) && is_doc(previous) {
        return false;
    }
    if f["status"].as_str() != Some("modified") {
        return true;
    }
    match f["patch"].as_str() {
        Some(patch) => patch_changes_code(patch, comment_style(name)),
        None => true,
    }
}

/// Files that hold dependency versions: their change is a change in what runs.
fn is_lockfile(path: &str) -> bool {
    const LOCKFILES: &[&str] = &[
        "Cargo.lock",
        "package-lock.json",
        "npm-shrinkwrap.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "bun.lockb",
        "poetry.lock",
        "Pipfile.lock",
        "uv.lock",
        "Gemfile.lock",
        "composer.lock",
        "go.sum",
        "flake.lock",
    ];
    LOCKFILES.contains(&basename(path))
}

/// Prose a person reads. Files that steer tools or agents (anything under
/// `.github/` or `.claude/`, `AGENTS.md`, `CLAUDE.md`) are not prose here.
fn is_doc(path: &str) -> bool {
    const DOC_EXTENSIONS: &[&str] = &["md", "mdx", "markdown", "rst", "adoc"];
    const DOC_NAMES: &[&str] = &["LICENSE", "NOTICE", "AUTHORS", "CHANGELOG"];
    const STEERING: &[&str] = &["AGENTS.md", "CLAUDE.md"];
    if path.starts_with(".github/")
        || path.starts_with(".claude/")
        || path.contains("/.claude/")
        || STEERING.contains(&basename(path))
    {
        return false;
    }
    DOC_NAMES.contains(&basename(path))
        || DOC_EXTENSIONS.contains(&extension(path).to_ascii_lowercase().as_str())
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn extension(path: &str) -> &str {
    let base = basename(path);
    match base.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => ext,
        _ => "",
    }
}

/// How a language writes a whole-line comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommentStyle {
    /// `// …` and a one-line `/* … */`.
    Slashes,
    /// `# …`.
    Hash,
    /// `-- …`.
    Dashes,
    /// A one-line `<!-- … -->`.
    Markup,
}

fn comment_style(path: &str) -> Option<CommentStyle> {
    let ext = extension(path).to_ascii_lowercase();
    let style = match ext.as_str() {
        "rs" | "ts" | "tsx" | "js" | "jsx" | "mjs" | "cjs" | "go" | "java" | "kt" | "kts"
        | "swift" | "c" | "h" | "cc" | "cpp" | "hpp" | "cs" | "scala" | "dart" | "php"
        | "groovy" | "proto" => CommentStyle::Slashes,
        "py" | "rb" | "sh" | "bash" | "zsh" | "yaml" | "yml" | "toml" | "r" | "pl" | "ex"
        | "exs" | "tf" | "hcl" => CommentStyle::Hash,
        "sql" | "lua" | "hs" => CommentStyle::Dashes,
        "html" | "htm" | "xml" | "vue" | "svelte" => CommentStyle::Markup,
        _ if basename(path) == "Dockerfile" => CommentStyle::Hash,
        _ => return None,
    };
    Some(style)
}

fn is_comment(line: &str, style: Option<CommentStyle>) -> bool {
    let one_block = |open: &str, close: &str| {
        line.len() >= open.len() + close.len()
            && line.starts_with(open)
            && line.ends_with(close)
            && !line[open.len()..line.len() - close.len()].contains(close)
    };
    match style {
        Some(CommentStyle::Slashes) => line.starts_with("//") || one_block("/*", "*/"),
        Some(CommentStyle::Hash) => line.starts_with('#'),
        Some(CommentStyle::Dashes) => line.starts_with("--"),
        Some(CommentStyle::Markup) => one_block("<!--", "-->"),
        None => false,
    }
}

/// Whether a unified-diff patch changes anything but comments and
/// whitespace: the removed and the added lines, with comment lines dropped
/// and all whitespace taken out, must read the same.
fn patch_changes_code(patch: &str, style: Option<CommentStyle>) -> bool {
    let mut removed = String::new();
    let mut added = String::new();
    for line in patch.lines() {
        let (side, text) = if let Some(t) = line.strip_prefix('+') {
            (&mut added, t)
        } else if let Some(t) = line.strip_prefix('-') {
            (&mut removed, t)
        } else {
            continue;
        };
        let t = text.trim();
        if t.is_empty() || is_comment(t, style) {
            continue;
        }
        side.extend(t.chars().filter(|c| !c.is_whitespace()));
    }
    removed != added
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(verdict: &str, posted_as: &str, head: &str) -> ReviewRecord {
        ReviewRecord {
            verdict: verdict.into(),
            posted_as: posted_as.into(),
            head_sha: head.into(),
            created_at: String::new(),
            body: "Looks good.".into(),
        }
    }

    fn file(name: &str, status: &str, patch: Option<&str>) -> Value {
        let mut f = json!({"filename": name, "status": status});
        if let Some(p) = patch {
            f["patch"] = json!(p);
        }
        f
    }

    fn since(files: Vec<Value>) -> Value {
        json!({"status": "ahead", "files": files})
    }

    #[test]
    fn a_push_after_an_approval_is_reviewed_from_the_approved_commit() {
        let approved = [record("APPROVE", "APPROVE", "aaa")];
        assert_eq!(
            approved_base(TRIGGER_PUSH, false, &approved, "bbb"),
            Some("aaa".into())
        );
        // CoderHelm's own PR records its approval as a COMMENT on GitHub.
        let own = [record("APPROVE", "COMMENT", "aaa")];
        assert_eq!(
            approved_base(TRIGGER_PUSH, false, &own, "bbb"),
            Some("aaa".into())
        );
        // The same commit, a question, or another trigger: a full review.
        assert_eq!(approved_base(TRIGGER_PUSH, false, &approved, "aaa"), None);
        assert_eq!(approved_base(TRIGGER_PUSH, true, &approved, "bbb"), None);
        assert_eq!(approved_base("rerequest", false, &approved, "bbb"), None);
        assert_eq!(approved_base(TRIGGER_PUSH, false, &[], "bbb"), None);
    }

    #[test]
    fn a_push_after_a_change_request_or_questions_gets_a_full_review() {
        let requested = [
            record("REQUEST_CHANGES", "REQUEST_CHANGES", "ccc"),
            record("APPROVE", "APPROVE", "aaa"),
        ];
        assert_eq!(approved_base(TRIGGER_PUSH, false, &requested, "ddd"), None);
        // A COMMENT (questions, or facts left unconfirmed) after an approval
        // is the latest word: the push is reviewed in full.
        let questions = [
            record("COMMENT", "COMMENT", "ccc"),
            record("APPROVE", "APPROVE", "aaa"),
        ];
        assert_eq!(approved_base(TRIGGER_PUSH, false, &questions, "ddd"), None);
        // An answer to a question isn't a review; an old error review isn't a
        // verdict on the code.
        let mut failed = record("REQUEST_CHANGES", "REQUEST_CHANGES", "ccc");
        failed.body = format!(
            "{} the service failed",
            crate::passes::review_sweep::LEGACY_FAILED_PREFIX
        );
        let skipped_over = [
            record("QUESTION", "COMMENT", "eee"),
            failed,
            record("APPROVE", "APPROVE", "aaa"),
        ];
        assert_eq!(
            approved_base(TRIGGER_PUSH, false, &skipped_over, "fff"),
            Some("aaa".into())
        );
    }

    #[test]
    fn a_rebased_branch_is_reviewed_in_full() {
        assert!(builds_on(&json!({"status": "ahead"})));
        assert!(!builds_on(&json!({"status": "diverged"})));
        assert!(!builds_on(&json!({})));
    }

    #[test]
    fn docs_comments_and_whitespace_are_not_code() {
        let docs = since(vec![
            file(
                "README.md",
                "modified",
                Some("@@ -1 +1 @@\n-Old\n+New words"),
            ),
            file("docs/guide.rst", "added", None),
        ]);
        assert!(!touches_code(&docs));
        let comments = since(vec![
            file(
                "src/sync.rs",
                "modified",
                Some("@@ -3,3 +3,4 @@\n fn sync() {\n-    // old note\n+    // Clearer note\n+    /* and another */\n     run();"),
            ),
            file(
                "app/jobs.py",
                "modified",
                Some("@@ -1,2 +1,2 @@\n-# reads the vendor's orders\n+# Reads the vendor's order lines\n import x"),
            ),
        ]);
        assert!(!touches_code(&comments), "{:?}", code_files(&comments));
        let formatting = since(vec![file(
            "web/app.ts",
            "modified",
            Some("@@ -1,2 +1,3 @@\n-const a = f(x,y);\n+const a = f(\n+  x, y);"),
        )]);
        assert!(!touches_code(&formatting));
        assert!(!touches_code(&since(vec![])));
    }

    #[test]
    fn code_data_lockfiles_and_unknowns_are_code() {
        let code = since(vec![
            file("README.md", "modified", Some("@@\n-a\n+b")),
            file(
                "src/sync.rs",
                "modified",
                Some("@@\n-    run(1);\n+    run(2);"),
            ),
        ]);
        assert!(touches_code(&code));
        assert_eq!(code_files(&code), vec!["src/sync.rs".to_string()]);
        // A trailing comment changed on a code line is still a code line.
        assert!(touches_code(&since(vec![file(
            "src/a.rs",
            "modified",
            Some("@@\n-let x = 1; // a\n+let x = 1; // b"),
        )])));
        // Rust attributes and C preprocessor lines aren't `#` comments.
        assert!(touches_code(&since(vec![file(
            "src/a.rs",
            "modified",
            Some("@@\n-#[derive(Debug)]\n+#[derive(Debug, Clone)]"),
        )])));
        // A lockfile's formatting is a change in what gets installed.
        assert!(touches_code(&since(vec![file(
            "Cargo.lock",
            "modified",
            Some("@@\n-version = \"1\"\n+version  =  \"1\""),
        )])));
        // Data and config, a file with no patch, a new or renamed code file.
        assert!(touches_code(&since(vec![file(
            "config/prices.json",
            "modified",
            Some("@@\n-  \"fee\": 1\n+  \"fee\": 2"),
        )])));
        assert!(touches_code(&since(vec![file(
            "logo.png", "modified", None
        )])));
        assert!(touches_code(&since(vec![file(
            "src/new.rs",
            "added",
            None
        )])));
        let mut renamed = file("docs/old.md", "renamed", None);
        renamed["previous_filename"] = json!("src/old.rs");
        assert!(touches_code(&since(vec![renamed])));
        // Files that steer agents and CI are not prose.
        assert!(touches_code(&since(vec![file(
            "AGENTS.md",
            "modified",
            Some("@@\n-a\n+b")
        )])));
        assert!(touches_code(&since(vec![file(
            ".github/pull_request_template.md",
            "modified",
            Some("@@\n-a\n+b")
        )])));
        assert!(touches_code(&json!({"status": "ahead"})));
    }

    #[test]
    fn merging_the_base_branch_in_leaves_the_prs_own_change_as_approved() {
        // The PR's change when it was approved.
        let at_approval = since(vec![file(
            "src/orders.rs",
            "modified",
            Some("@@ -10,3 +10,3 @@ fn total()\n let a = 1;\n-let fee = 2;\n+let fee = 3;\n let b = 2;"),
        )]);
        // Two merges of the base branch later: other work moved the lines
        // and the context, the PR's own lines are the same.
        let after_merges = since(vec![file(
            "src/orders.rs",
            "modified",
            Some("@@ -42,3 +42,3 @@ fn total_with_tax()\n let a = 10;\n-let fee = 2;\n+let fee = 3;\n let c = 4;"),
        )]);
        assert!(same_own_change(&at_approval, &after_merges));
        // A conflict resolved differently, or one more file, is a new change.
        let resolved = since(vec![file(
            "src/orders.rs",
            "modified",
            Some("@@ -42,3 +42,3 @@\n-let fee = 2;\n+let fee = 4;"),
        )]);
        assert!(!same_own_change(&at_approval, &resolved));
        let mut more = after_merges.clone();
        more["files"].as_array_mut().unwrap().push(file(
            "src/tax.rs",
            "modified",
            Some("@@\n-a\n+b"),
        ));
        assert!(!same_own_change(&at_approval, &more));
        // A file GitHub sent no patch for can't be shown to be the same.
        let no_patch = since(vec![file("src/orders.rs", "modified", None)]);
        assert!(!same_own_change(&no_patch, &no_patch));
        assert!(!same_own_change(&json!({}), &json!({})));
    }

    #[test]
    fn a_push_after_a_change_request_is_reviewed_from_the_reviewed_commit() {
        let requested = [
            record("REQUEST_CHANGES", "REQUEST_CHANGES", "ccc"),
            record("APPROVE", "APPROVE", "aaa"),
        ];
        assert_eq!(
            reviewed_base(TRIGGER_PUSH, false, false, &requested, "ddd"),
            Some("ccc".into())
        );
        // Questions left: the same.
        let questions = [record("COMMENT", "COMMENT", "ccc")];
        assert_eq!(
            reviewed_base(TRIGGER_PUSH, false, false, &questions, "ddd"),
            Some("ccc".into())
        );
        // An approval is approved_base's; a forced full review, a question,
        // another trigger, the same head or no review: none.
        let approved = [record("APPROVE", "APPROVE", "aaa")];
        assert_eq!(
            reviewed_base(TRIGGER_PUSH, false, false, &approved, "bbb"),
            None
        );
        assert_eq!(
            reviewed_base(TRIGGER_PUSH, false, true, &requested, "ddd"),
            None
        );
        assert_eq!(
            reviewed_base(TRIGGER_PUSH, true, false, &requested, "ddd"),
            None
        );
        assert_eq!(
            reviewed_base("ready_for_review", false, false, &requested, "ddd"),
            None
        );
        assert_eq!(
            reviewed_base(TRIGGER_PUSH, false, false, &requested, "ccc"),
            None
        );
        assert_eq!(reviewed_base(TRIGGER_PUSH, false, false, &[], "ddd"), None);
        assert!(since_review_note("ccc1234567").contains("only what changed since ccc1234"));
    }

    #[test]
    fn an_approval_always_rests_on_a_full_review() {
        assert!(needs_full_before_approval(true, "APPROVE"));
        assert!(!needs_full_before_approval(true, "REQUEST_CHANGES"));
        assert!(!needs_full_before_approval(false, "APPROVE"));
    }

    #[test]
    fn coderhelms_own_pushes_wait_while_its_run_is_in_its_loop() {
        use common::run_status::*;
        // Its own push while the run fixes and waits on CI: not reviewed.
        assert!(own_loop_holds_push(TRIGGER_PUSH, true, true, Some(RUNNING)));
        assert!(own_loop_holds_push(
            TRIGGER_PUSH,
            true,
            true,
            Some(AWAITING_CI)
        ));
        // A person's push to the same PR is reviewed.
        assert!(!own_loop_holds_push(
            TRIGGER_PUSH,
            true,
            false,
            Some(RUNNING)
        ));
        // The run ended (handed over, waiting on a person) or no run: reviewed.
        for s in [COMPLETED, NEEDS_INPUT, FAILED, CANCELLED, CLOSED] {
            assert!(!own_loop_holds_push(TRIGGER_PUSH, true, true, Some(s)));
        }
        assert!(!own_loop_holds_push(TRIGGER_PUSH, true, true, None));
        // A person's PR, or another trigger (marked ready, a handoff): reviewed.
        assert!(!own_loop_holds_push(
            TRIGGER_PUSH,
            false,
            true,
            Some(RUNNING)
        ));
        assert!(!own_loop_holds_push(
            "ready_for_review",
            true,
            true,
            Some(RUNNING)
        ));
        assert!(!own_loop_holds_push(
            HANDOFF_TRIGGER,
            true,
            true,
            Some(RUNNING)
        ));
    }

    #[test]
    fn the_head_commits_author_is_read_from_the_commit_list() {
        let commits = vec![
            json!({"sha": "a", "author": {"login": "acme-dev"}}),
            json!({"sha": "b", "author": null, "committer": {"login": "coderhelm[bot]"}}),
        ];
        assert_eq!(head_commit_login(&commits, "a"), Some("acme-dev"));
        assert_eq!(head_commit_login(&commits, "b"), Some("coderhelm[bot]"));
        assert_eq!(head_commit_login(&commits, "zzz"), None);
    }

    /// Cost budget: a re-review after a request for changes reads only the
    /// hunks changed since, so what it sends is a small part of a full
    /// review's diff. A typical fix push touches one hunk of a 12-file PR.
    #[test]
    fn a_hunks_only_re_review_sends_a_fraction_of_the_full_diff() {
        let hunk = |i: usize| {
            let mut p = format!("@@ -1,40 +1,40 @@ fn handler_{i}()\n");
            for l in 0..40 {
                p.push_str(&format!("-    let value_{l} = old_call_{i}({l});\n"));
                p.push_str(&format!("+    let value_{l} = new_call_{i}({l}, &ctx);\n"));
            }
            p
        };
        let full = json!({"status": "ahead", "files": (0..12)
            .map(|i| file(&format!("src/h{i}.rs"), "modified", Some(&hunk(i))))
            .collect::<Vec<_>>()});
        let fix = json!({"status": "ahead", "files": [file(
            "src/h3.rs",
            "modified",
            Some("@@ -7,1 +7,1 @@ fn handler_3()\n-    let value_6 = new_call_3(6, &ctx);\n+    let value_6 = new_call_3(6, &ctx)?;"),
        )]});
        let format = crate::passes::review_agent::format_diff;
        let full_diff = format(&full, 40_000);
        let since = format!("{}\n{}", since_review_note("ccc1234"), format(&fix, 40_000));
        // Under a tenth of the full review's diff.
        assert!(
            since.len() * 10 < full_diff.len(),
            "hunks-only {} vs full {}",
            since.len(),
            full_diff.len()
        );
    }

    /// The reviewer uses all of it: its own pushes wait during the run's
    /// loop, a push after a non-approving review is reviewed from that
    /// review, and that review never approves without a full one.
    #[test]
    fn the_reviewer_wires_the_scopes_in() {
        let src = include_str!("review_pr.rs");
        assert!(src.contains("own_loop_hold(state, &github, &msg, &head_sha).await"));
        assert!(src.contains("super::review_scope::reviewed_base("));
        assert!(src.contains(
            "super::review_scope::needs_full_before_approval(since_review.is_some(), verdict)"
        ));
        assert!(src.contains("full.full_review = true;"));
        assert!(include_str!("resume.rs").contains("super::review_pr::review_on_handoff("));
    }
}
