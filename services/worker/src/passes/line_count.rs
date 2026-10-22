//! `count_lines`: lines of code per language for a repository or one of its
//! folders, the way `cloc` reports them. Read-only and with no shell: it
//! counts the repository snapshot Coderhelm already downloads (one tarball
//! request), so a team's skill or agent that asks for `cloc` gets the same
//! numbers without a terminal.
//!
//! Each line is blank, a comment (a line comment, or inside a block comment)
//! or code. Strings that look like comments are not parsed out: the counts
//! are the quick picture `cloc` gives, not a parser's.

use std::collections::BTreeMap;

use serde_json::json;

use crate::agent::llm::ToolDefinition;

pub const TOOL: &str = "count_lines";

/// Languages shown, most code first; the rest are summed.
const MAX_LANGUAGES: usize = 25;

pub fn tool() -> ToolDefinition {
    ToolDefinition {
        name: TOOL.to_string(),
        description: "Count lines of code per language (files, blank, comment, code), like \
                      `cloc`, for the whole repository or one folder. Read-only, from the \
                      repository at the commit being worked on; vendored and generated folders \
                      (node_modules, vendor, dist, build) are left out."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "A folder in the repository, e.g. src/api. Omit for the whole repository."
                }
            }
        }),
    }
}

/// How one language writes comments.
struct Syntax {
    name: &'static str,
    line: &'static [&'static str],
    block: Option<(&'static str, &'static str)>,
}

const C_LIKE: (&str, &str) = ("/*", "*/");
const MARKUP: (&str, &str) = ("<!--", "-->");

/// The language a file is written in, by its extension or name. `None` for
/// a file `cloc` wouldn't count (images, lock files, data dumps).
fn syntax(path: &str) -> Option<Syntax> {
    let file = path.rsplit('/').next().unwrap_or(path);
    let lower = file.to_ascii_lowercase();
    let by_name = match lower.as_str() {
        "dockerfile" => Some(("Dockerfile", &["#"][..], None)),
        "makefile" | "gnumakefile" => Some(("make", &["#"][..], None)),
        _ => None,
    };
    let (name, line, block): (&str, &[&str], Option<(&str, &str)>) = match by_name {
        Some(s) => s,
        None => {
            let ext = lower.rsplit_once('.')?.1;
            match ext {
                "rs" => ("Rust", &["//"], Some(C_LIKE)),
                "ts" | "mts" | "cts" => ("TypeScript", &["//"], Some(C_LIKE)),
                "tsx" => ("TSX", &["//"], Some(C_LIKE)),
                "js" | "mjs" | "cjs" => ("JavaScript", &["//"], Some(C_LIKE)),
                "jsx" => ("JSX", &["//"], Some(C_LIKE)),
                "go" => ("Go", &["//"], Some(C_LIKE)),
                "java" => ("Java", &["//"], Some(C_LIKE)),
                "kt" | "kts" => ("Kotlin", &["//"], Some(C_LIKE)),
                "scala" => ("Scala", &["//"], Some(C_LIKE)),
                "swift" => ("Swift", &["//"], Some(C_LIKE)),
                "c" | "h" => ("C", &["//"], Some(C_LIKE)),
                "cc" | "cpp" | "cxx" | "hpp" | "hh" => ("C++", &["//"], Some(C_LIKE)),
                "cs" => ("C#", &["//"], Some(C_LIKE)),
                "php" => ("PHP", &["//", "#"], Some(C_LIKE)),
                "dart" => ("Dart", &["//"], Some(C_LIKE)),
                "css" => ("CSS", &[], Some(C_LIKE)),
                "scss" => ("SCSS", &["//"], Some(C_LIKE)),
                "less" => ("LESS", &["//"], Some(C_LIKE)),
                "py" | "pyi" => ("Python", &["#"], None),
                "rb" => ("Ruby", &["#"], None),
                "sh" | "bash" | "zsh" => ("Bourne Shell", &["#"], None),
                "pl" | "pm" => ("Perl", &["#"], None),
                "r" => ("R", &["#"], None),
                "ex" | "exs" => ("Elixir", &["#"], None),
                "tf" | "hcl" => ("HCL", &["#", "//"], Some(C_LIKE)),
                "yaml" | "yml" => ("YAML", &["#"], None),
                "toml" => ("TOML", &["#"], None),
                "sql" => ("SQL", &["--"], Some(C_LIKE)),
                "lua" => ("Lua", &["--"], None),
                "hs" => ("Haskell", &["--"], None),
                "html" | "htm" => ("HTML", &[], Some(MARKUP)),
                "xml" => ("XML", &[], Some(MARKUP)),
                "vue" => ("Vuejs Component", &["//"], Some(MARKUP)),
                "svelte" => ("Svelte", &["//"], Some(MARKUP)),
                "md" | "mdx" => ("Markdown", &[], Some(MARKUP)),
                "graphql" | "gql" => ("GraphQL", &["#"], None),
                "proto" => ("Protocol Buffers", &["//"], Some(C_LIKE)),
                "json" => ("JSON", &[], None),
                _ => return None,
            }
        }
    };
    Some(Syntax { name, line, block })
}

/// Folders `cloc` users leave out: vendored code and build output.
const SKIPPED_DIRS: &[&str] = &[
    "node_modules",
    "vendor",
    "dist",
    "build",
    "target",
    ".git",
    ".next",
    "coverage",
    "__pycache__",
];

fn skipped(path: &str) -> bool {
    path.split('/')
        .rev()
        .skip(1)
        .any(|d| SKIPPED_DIRS.contains(&d))
}

/// One language's totals.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Totals {
    pub files: usize,
    pub blank: usize,
    pub comment: usize,
    pub code: usize,
}

impl Totals {
    fn add(&mut self, o: &Totals) {
        self.files += o.files;
        self.blank += o.blank;
        self.comment += o.comment;
        self.code += o.code;
    }
}

/// Blank, comment and code lines of one file's text.
fn count_text(text: &str, s: &Syntax) -> Totals {
    let mut t = Totals {
        files: 1,
        ..Default::default()
    };
    let mut in_block = false;
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() {
            t.blank += 1;
            continue;
        }
        if in_block {
            t.comment += 1;
            if let Some((_, end)) = s.block {
                if let Some(i) = line.find(end) {
                    in_block = false;
                    // Code after the block's end makes it a code line.
                    if !line[i + end.len()..].trim().is_empty() {
                        t.comment -= 1;
                        t.code += 1;
                    }
                }
            }
            continue;
        }
        if s.line.iter().any(|p| line.starts_with(p)) {
            t.comment += 1;
            continue;
        }
        if let Some((start, end)) = s.block {
            if let Some(rest) = line.strip_prefix(start) {
                match rest.find(end) {
                    Some(i) if rest[i + end.len()..].trim().is_empty() => t.comment += 1,
                    Some(_) => t.code += 1,
                    None => {
                        in_block = true;
                        t.comment += 1;
                    }
                }
                continue;
            }
        }
        t.code += 1;
    }
    t
}

/// Counts files under `under` ("" for the whole repository), each given as
/// its path and its text (`None` when it wasn't kept: binary or too large).
#[derive(Debug, Default)]
pub struct Counter {
    under: String,
    by: BTreeMap<&'static str, Totals>,
    not_read: usize,
}

impl Counter {
    pub fn new(under: &str) -> Counter {
        Counter {
            under: clean_dir(under),
            ..Default::default()
        }
    }

    pub fn add(&mut self, path: &str, text: Option<&str>) {
        if !self.under.is_empty() && !path.starts_with(&format!("{}/", self.under)) {
            return;
        }
        if skipped(path) {
            return;
        }
        let Some(s) = syntax(path) else { return };
        match text {
            Some(text) => {
                let t = count_text(text, &s);
                self.by.entry(s.name).or_default().add(&t);
            }
            None => self.not_read += 1,
        }
    }

    /// The table, like `cloc`'s, most code first.
    pub fn report(&self) -> String {
        let place = if self.under.is_empty() {
            "the repository".to_string()
        } else {
            format!("{}/", self.under)
        };
        if self.by.is_empty() {
            return format!("No source files found in {place}.");
        }
        let mut rows: Vec<(&str, &Totals)> = self.by.iter().map(|(k, v)| (*k, v)).collect();
        rows.sort_by(|a, b| b.1.code.cmp(&a.1.code).then(a.0.cmp(b.0)));
        let mut total = Totals::default();
        let mut out = format!(
            "Lines of code in {place}\n{:<20} {:>7} {:>9} {:>9} {:>9}\n",
            "Language", "files", "blank", "comment", "code"
        );
        let mut rest = Totals::default();
        for (i, (name, t)) in rows.iter().enumerate() {
            total.add(t);
            if i < MAX_LANGUAGES {
                out.push_str(&format!(
                    "{:<20} {:>7} {:>9} {:>9} {:>9}\n",
                    name, t.files, t.blank, t.comment, t.code
                ));
            } else {
                rest.add(t);
            }
        }
        if rest.files > 0 {
            out.push_str(&format!(
                "{:<20} {:>7} {:>9} {:>9} {:>9}\n",
                "(other)", rest.files, rest.blank, rest.comment, rest.code
            ));
        }
        out.push_str(&format!(
            "{:<20} {:>7} {:>9} {:>9} {:>9}\n",
            "SUM", total.files, total.blank, total.comment, total.code
        ));
        if self.not_read > 0 {
            out.push_str(&format!(
                "{} source file{} too large or not text, not counted.\n",
                self.not_read,
                if self.not_read == 1 { "" } else { "s" }
            ));
        }
        out
    }
}

/// A folder as given, without leading `./` or `/` and trailing `/`.
fn clean_dir(p: &str) -> String {
    let p = p.trim();
    let p = p.strip_prefix("./").unwrap_or(p);
    let p = p.trim_matches('/');
    if p == "." {
        String::new()
    } else {
        p.to_string()
    }
}

/// Runs the tool on `owner/repo` at `git_ref`.
pub async fn run(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
    input: &serde_json::Value,
) -> String {
    let under = input["path"].as_str().unwrap_or("");
    if under.contains("..") {
        return "(count_lines: give a folder inside the repository)".into();
    }
    match github.count_lines(owner, repo, git_ref, under).await {
        Ok(report) => report,
        Err(e) => format!("(count_lines: the repository couldn't be read to count: {e})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_are_blank_comment_or_code() {
        let rust = "// a note\nfn main() {\n\n    /* one */\n    /* open\n       still */\n    let x = 1; // trailing\n}\n";
        let t = count_text(rust, &syntax("src/main.rs").unwrap());
        assert_eq!(
            t,
            Totals {
                files: 1,
                blank: 1,
                comment: 4,
                code: 3
            }
        );
        let py = "# top\nimport os\n\n\ndef f():\n    return 1  # x\n";
        let t = count_text(py, &syntax("app/x.py").unwrap());
        assert_eq!((t.blank, t.comment, t.code), (2, 1, 3));
        // Code after a block comment's end is code.
        let t = count_text("/* a */ let y = 2;\n", &syntax("a.ts").unwrap());
        assert_eq!((t.comment, t.code), (0, 1));
    }

    #[test]
    fn the_report_reads_like_cloc_for_the_repo_or_one_folder() {
        let files: Vec<(&str, Option<&str>)> = vec![
            ("src/a.rs", Some("fn a() {}\n// c\n")),
            ("src/b.rs", Some("fn b() {}\n\n")),
            ("web/app.ts", Some("export const x = 1;\n")),
            (
                "web/node_modules/lib/index.js",
                Some("module.exports = 1;\n"),
            ),
            ("logo.png", None),
            ("src/huge.rs", None),
            ("Cargo.lock", Some("[[package]]\n")),
        ];
        let mut all = Counter::new("");
        for (p, t) in &files {
            all.add(p, *t);
        }
        let r = all.report();
        assert!(r.starts_with("Lines of code in the repository"), "{r}");
        assert!(r.contains("Rust"), "{r}");
        assert!(!r.contains("JavaScript"), "vendored code is left out: {r}");
        let sum = r.lines().find(|l| l.starts_with("SUM")).unwrap();
        assert_eq!(
            sum.split_whitespace().collect::<Vec<_>>(),
            ["SUM", "3", "1", "1", "3"]
        );
        assert!(r.contains("1 source file too large or not text"), "{r}");

        let mut src = Counter::new("./src/");
        for (p, t) in &files {
            src.add(p, *t);
        }
        let r = src.report();
        assert!(r.starts_with("Lines of code in src/"), "{r}");
        assert!(!r.contains("TypeScript"), "{r}");
        assert_eq!(
            Counter::new("nope").report(),
            "No source files found in nope/."
        );
    }
}
