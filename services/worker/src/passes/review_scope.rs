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
}
