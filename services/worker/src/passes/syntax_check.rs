//! Syntax gate for agent-written file content.
//!
//! The worker never compiles or executes anything, so this is the last line
//! of defense before broken content is committed to a customer branch.
//!
//! Policy (deliberately asymmetric — a false reject strands the coding
//! agent, which is worse than a false pass):
//! - Data files (JSON/YAML/TOML) must fully parse. Known JSONC configs
//!   (tsconfig, .vscode, …) are exempt; multi-document YAML is supported.
//! - Code files are gated ONLY on edits, and only when the edited content is
//!   STRICTLY WORSE than the original (new issue kinds appear). Issue
//!   messages carry no line numbers, so edits that shift lines above a
//!   pre-existing quirk still compare equal. Brand-new code files are never
//!   blocked by the bracket scan — real-world dialects (JSX prose, exotic
//!   raw strings) make a char-level lexer too false-positive-prone to veto
//!   content it has no clean baseline for. edit_file is the guarded path.
//!
//! The lexer skips strings, comments, JS/TS template literals (with `${…}`
//! nesting), JS regex literals (division-vs-regex heuristic), Go backtick
//! raw strings, Java/Kotlin text blocks, Rust raw strings / char literals /
//! lifetimes, and Python/Ruby `#`-comment syntax.

use serde::Deserialize;

enum Lang {
    CFamily {
        template_literals: bool,
        backtick_raw: bool,
        triple_quote: bool,
    },
    Rust,
    HashComment,
    Json,
    Yaml,
    Toml,
    Unknown,
}

/// JSONC-by-convention files that legitimately contain comments/trailing
/// commas and must not be strict-parsed.
fn is_jsonc_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    name.ends_with(".jsonc")
        || name.starts_with("tsconfig")
        || name.starts_with("jsconfig")
        || name.starts_with(".babelrc")
        || name.starts_with(".eslintrc")
        || name == "devcontainer.json"
        || lower.contains(".vscode/")
        || lower.contains(".devcontainer/")
}

fn lang_for_path(path: &str) -> Lang {
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "ts" | "tsx" | "js" | "jsx" | "mjs" | "cjs" => Lang::CFamily {
            template_literals: true,
            backtick_raw: false,
            triple_quote: false,
        },
        // Go: backtick raw strings (no interpolation).
        "go" => Lang::CFamily {
            template_literals: false,
            backtick_raw: true,
            triple_quote: false,
        },
        // Java/Kotlin: """ text blocks.
        "java" | "kt" | "kts" => Lang::CFamily {
            template_literals: false,
            backtick_raw: false,
            triple_quote: true,
        },
        "c" | "h" | "cpp" | "hpp" | "cc" | "cs" | "swift" | "dart" | "scala" => Lang::CFamily {
            template_literals: false,
            backtick_raw: false,
            triple_quote: false,
        },
        "rs" => Lang::Rust,
        "py" | "rb" => Lang::HashComment,
        "json" if !is_jsonc_path(path) => Lang::Json,
        "yaml" | "yml" => Lang::Yaml,
        "toml" => Lang::Toml,
        _ => Lang::Unknown,
    }
}

fn is_data_file(path: &str) -> bool {
    matches!(lang_for_path(path), Lang::Json | Lang::Yaml | Lang::Toml)
}

/// Scan issues for one file's content. Empty vec = clean (or unknown lang).
/// Messages intentionally contain NO line numbers — they are compared as
/// multisets across file versions, and line shifts must not break equality.
pub fn scan(path: &str, content: &str) -> Vec<String> {
    match lang_for_path(path) {
        Lang::Json => match serde_json::from_str::<serde_json::Value>(content) {
            Ok(_) => vec![],
            Err(_) => vec!["invalid JSON".to_string()],
        },
        Lang::Yaml => {
            // Multi-document YAML (k8s manifests, CI files) is standard.
            for doc in serde_yaml::Deserializer::from_str(content) {
                if serde_yaml::Value::deserialize(doc).is_err() {
                    return vec!["invalid YAML".to_string()];
                }
            }
            vec![]
        }
        Lang::Toml => match toml::from_str::<toml::Value>(content) {
            Ok(_) => vec![],
            Err(_) => vec!["invalid TOML".to_string()],
        },
        Lang::CFamily {
            template_literals,
            backtick_raw,
            triple_quote,
        } => scan_brackets(
            content,
            ScanOpts {
                comments: CommentStyle::CFamily,
                template_literals,
                backtick_raw,
                triple_quote,
                rust_quotes: false,
                js_regex: template_literals,
            },
        ),
        Lang::Rust => scan_brackets(
            content,
            ScanOpts {
                comments: CommentStyle::CFamilyNested,
                template_literals: false,
                backtick_raw: false,
                triple_quote: false,
                rust_quotes: true,
                js_regex: false,
            },
        ),
        Lang::HashComment => scan_brackets(
            content,
            ScanOpts {
                comments: CommentStyle::Hash,
                template_literals: false,
                backtick_raw: false,
                triple_quote: true,
                rust_quotes: false,
                js_regex: false,
            },
        ),
        Lang::Unknown => vec![],
    }
}

/// Validate an edit/write.
///
/// Data files must parse (unless the original was equally unparseable). Code
/// files: only edits can be rejected, and only when they introduce issue
/// kinds the original didn't have. New code files (original None) always
/// pass — see module docs.
pub fn validate_change(path: &str, original: Option<&str>, edited: &str) -> Result<(), String> {
    let edited_issues = scan(path, edited);
    if edited_issues.is_empty() {
        return Ok(());
    }
    match original {
        Some(orig) => {
            let mut orig_issues = scan(path, orig);
            // Multiset-subset: every issue in edited already existed.
            let mut no_worse = true;
            for issue in &edited_issues {
                if let Some(pos) = orig_issues.iter().position(|o| o == issue) {
                    orig_issues.remove(pos);
                } else {
                    no_worse = false;
                    break;
                }
            }
            if no_worse {
                return Ok(());
            }
            Err(edited_issues.join("; "))
        }
        None if is_data_file(path) => Err(edited_issues.join("; ")),
        None => Ok(()),
    }
}

enum CommentStyle {
    /// `//` line, `/* */` block (non-nesting)
    CFamily,
    /// `//` line, `/* */` block (nesting, Rust)
    CFamilyNested,
    /// `#` line comments only (Python, Ruby)
    Hash,
}

struct ScanOpts {
    comments: CommentStyle,
    template_literals: bool,
    backtick_raw: bool,
    triple_quote: bool,
    rust_quotes: bool,
    js_regex: bool,
}

fn scan_brackets(content: &str, opts: ScanOpts) -> Vec<String> {
    let mut issues = Vec::new();
    let mut stack: Vec<char> = Vec::new();
    // Template-literal nesting: stack depth at each `${` entry.
    let mut template_stack: Vec<usize> = Vec::new();
    // Last significant char — drives the JS division-vs-regex heuristic.
    let mut last_sig: Option<char> = None;

    let chars: Vec<char> = content.chars().collect();
    let mut i = 0usize;
    let n = chars.len();

    while i < n {
        let c = chars[i];
        let next = if i + 1 < n { Some(chars[i + 1]) } else { None };

        // ── comments ──
        match opts.comments {
            CommentStyle::CFamily | CommentStyle::CFamilyNested => {
                if c == '/' && next == Some('/') {
                    while i < n && chars[i] != '\n' {
                        i += 1;
                    }
                    continue;
                }
                if c == '/' && next == Some('*') {
                    let nesting = matches!(opts.comments, CommentStyle::CFamilyNested);
                    let mut depth = 1usize;
                    i += 2;
                    let mut closed = false;
                    while i < n {
                        if chars[i] == '*' && i + 1 < n && chars[i + 1] == '/' {
                            depth -= 1;
                            i += 2;
                            if depth == 0 {
                                closed = true;
                                break;
                            }
                            continue;
                        }
                        if nesting && chars[i] == '/' && i + 1 < n && chars[i + 1] == '*' {
                            depth += 1;
                            i += 2;
                            continue;
                        }
                        i += 1;
                    }
                    if !closed {
                        issues.push("unterminated block comment".to_string());
                    }
                    continue;
                }
            }
            CommentStyle::Hash => {
                if c == '#' {
                    while i < n && chars[i] != '\n' {
                        i += 1;
                    }
                    continue;
                }
            }
        }

        // ── JS/TS regex literal (division-vs-regex heuristic) ──
        // `/` starts a regex when the previous significant char cannot end an
        // expression (the standard minifier heuristic). Char classes may hold
        // unbalanced brackets. Regex literals cannot span lines — if no
        // closing `/` is found on the line, treat as division.
        if opts.js_regex && c == '/' {
            let regex_ok = match last_sig {
                None => true,
                Some(p) => "([{,;=:!&|?+-*%<>^~".contains(p) || p == '\n',
            };
            if regex_ok {
                let mut j = i + 1;
                let mut in_class = false;
                let mut closed = false;
                while j < n {
                    let rc = chars[j];
                    if rc == '\\' {
                        j += 2;
                        continue;
                    }
                    if rc == '\n' {
                        break;
                    }
                    match rc {
                        '[' => in_class = true,
                        ']' => in_class = false,
                        '/' if !in_class => {
                            closed = true;
                            break;
                        }
                        _ => {}
                    }
                    j += 1;
                }
                if closed {
                    i = j + 1;
                    last_sig = Some('/');
                    continue;
                }
            }
        }

        // ── Rust raw strings, char literals, lifetimes ──
        if opts.rust_quotes {
            if (c == 'r' || (c == 'b' && next == Some('r'))) && is_raw_string_start(&chars, i) {
                let start = if c == 'b' { i + 1 } else { i };
                let mut hashes = 0usize;
                let mut j = start + 1;
                while j < n && chars[j] == '#' {
                    hashes += 1;
                    j += 1;
                }
                j += 1; // past opening quote
                let mut closed = false;
                while j < n {
                    if chars[j] == '"' {
                        let mut k = j + 1;
                        let mut h = 0usize;
                        while k < n && chars[k] == '#' && h < hashes {
                            h += 1;
                            k += 1;
                        }
                        if h == hashes {
                            j = k;
                            closed = true;
                            break;
                        }
                    }
                    j += 1;
                }
                if !closed {
                    issues.push("unterminated raw string".to_string());
                }
                i = j;
                last_sig = Some('"');
                continue;
            }
            if c == '\'' {
                if next == Some('\\') {
                    let mut j = i + 2;
                    if j < n {
                        j += 1;
                    }
                    if j < n && chars[j] == '\'' {
                        j += 1;
                    }
                    i = j;
                    last_sig = Some('\'');
                    continue;
                }
                if i + 2 < n && chars[i + 2] == '\'' {
                    i += 3; // 'x'
                    last_sig = Some('\'');
                    continue;
                }
                i += 1; // lifetime — consume just the quote
                continue;
            }
        }

        // ── triple quotes (Python, Java/Kotlin text blocks) ──
        if opts.triple_quote
            && (c == '"' || (c == '\'' && matches!(opts.comments, CommentStyle::Hash)))
            && i + 2 < n
            && chars[i + 1] == c
            && chars[i + 2] == c
        {
            i += 3;
            let mut closed = false;
            while i < n {
                if chars[i] == '\\' {
                    i += 2;
                    continue;
                }
                if chars[i] == c && i + 2 < n && chars[i + 1] == c && chars[i + 2] == c {
                    i += 3;
                    closed = true;
                    break;
                }
                i += 1;
            }
            if !closed {
                issues.push("unterminated triple-quoted string".to_string());
            }
            last_sig = Some('"');
            continue;
        }

        // ── Go backtick raw strings ──
        if opts.backtick_raw && c == '`' {
            i += 1;
            let mut closed = false;
            while i < n {
                if chars[i] == '`' {
                    i += 1;
                    closed = true;
                    break;
                }
                i += 1;
            }
            if !closed {
                issues.push("unterminated raw string".to_string());
            }
            last_sig = Some('`');
            continue;
        }

        // ── ordinary strings ──
        if c == '"' || (c == '\'' && !opts.rust_quotes) {
            // Consume to closing quote; auto-close at newline WITHOUT
            // flagging (multiline-string dialects and JSX prose apostrophes
            // must not hard-error) — the bracket stack is the real guard.
            let quote = c;
            i += 1;
            while i < n && chars[i] != quote && chars[i] != '\n' {
                if chars[i] == '\\' {
                    i += 1;
                }
                i += 1;
            }
            if i < n && chars[i] == quote {
                i += 1;
            }
            last_sig = Some(quote);
            continue;
        }

        // ── JS/TS template literals ──
        if opts.template_literals && c == '`' {
            i += 1;
            match consume_template(&chars, &mut i, n) {
                TemplateOutcome::Closed => {}
                TemplateOutcome::EnteredEmbed => {
                    template_stack.push(stack.len());
                    stack.push('{');
                }
                TemplateOutcome::Unterminated => {
                    issues.push("unterminated template literal".to_string());
                }
            }
            last_sig = Some('`');
            continue;
        }

        // ── brackets ──
        match c {
            '(' | '[' | '{' => stack.push(c),
            ')' | ']' | '}' => {
                let expected = match c {
                    ')' => '(',
                    ']' => '[',
                    _ => '{',
                };
                match stack.pop() {
                    Some(open) if open == expected => {
                        // Closing a `${` embed resumes the template body.
                        if c == '}' && template_stack.last() == Some(&stack.len()) {
                            template_stack.pop();
                            i += 1;
                            match consume_template(&chars, &mut i, n) {
                                TemplateOutcome::Closed => {}
                                TemplateOutcome::EnteredEmbed => {
                                    template_stack.push(stack.len());
                                    stack.push('{');
                                }
                                TemplateOutcome::Unterminated => {
                                    issues.push("unterminated template literal".to_string());
                                }
                            }
                            last_sig = Some('`');
                            continue;
                        }
                    }
                    Some(open) => {
                        issues.push(format!("mismatched bracket: '{open}' closed by '{c}'"));
                    }
                    None => {
                        issues.push(format!("unmatched closing '{c}'"));
                    }
                }
            }
            _ => {}
        }
        if c == '\n' {
            last_sig = Some('\n');
        } else if !c.is_whitespace() {
            last_sig = Some(c);
        }
        i += 1;
    }

    for open in &stack {
        issues.push(format!("unclosed '{open}'"));
    }
    issues
}

enum TemplateOutcome {
    Closed,
    EnteredEmbed,
    Unterminated,
}

/// Consume a template-literal body starting at `*i` (just past a backtick or
/// a `}` that closed an embed). Leaves `*i` positioned after the terminator.
fn consume_template(chars: &[char], i: &mut usize, n: usize) -> TemplateOutcome {
    while *i < n {
        match chars[*i] {
            '\\' => *i += 2,
            '`' => {
                *i += 1;
                return TemplateOutcome::Closed;
            }
            '$' if *i + 1 < n && chars[*i + 1] == '{' => {
                *i += 2;
                return TemplateOutcome::EnteredEmbed;
            }
            _ => *i += 1,
        }
    }
    TemplateOutcome::Unterminated
}

fn is_raw_string_start(chars: &[char], i: usize) -> bool {
    let mut j = i;
    if chars[j] == 'b' {
        j += 1;
        if j >= chars.len() || chars[j] != 'r' {
            return false;
        }
    }
    j += 1;
    while j < chars.len() && chars[j] == '#' {
        j += 1;
    }
    j < chars.len() && chars[j] == '"'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_files_pass() {
        assert!(scan("a.ts", "function f(x: number) { return { a: [x] }; }\n").is_empty());
        assert!(scan("a.py", "def f(x):\n    return {'a': [x]}\n").is_empty());
        assert!(scan("a.rb", "h = { a: [1, 2] } # {{{\n").is_empty());
        assert!(scan("a.rs", "fn f(x: u32) -> Vec<u32> { vec![x] }\n").is_empty());
        assert!(scan("a.json", r#"{"a": [1, 2, {"b": null}]}"#).is_empty());
        assert!(scan("a.yaml", "a:\n  - 1\n  - b: 2\n").is_empty());
        assert!(scan("a.toml", "[pkg]\nname = \"x\"\n").is_empty());
        assert!(scan("a.exotic", "((((").is_empty()); // unknown lang skipped
                                                      // CSS no longer scanned (url(//…) made // uninterpretable)
        assert!(scan("a.css", "a { background: url(https://x/i.png); }\n").is_empty());
    }

    #[test]
    fn single_dropped_brace_caught_on_edit() {
        let good = "function f() { if (x) { g(); } }\n";
        let broken = "function f() { if (x) { g(); }\n"; // missing one }
        assert!(!scan("a.ts", broken).is_empty());
        assert!(validate_change("a.ts", Some(good), broken).is_err());
        // Line shifts above a pre-existing quirk do NOT break the comparison
        let orig = "function g() {\n"; // pre-broken
        let shifted = "import x from 'y';\nfunction g() {\n";
        assert!(validate_change("a.ts", Some(orig), shifted).is_ok());
    }

    #[test]
    fn braces_in_strings_and_comments_ignored() {
        let ok = "const s = \"}}}}\"; // {{{{\nconst t = '}';\n/* } */ const u = 1;\n";
        assert!(scan("a.ts", ok).is_empty());
        let py = "s = \"}}}\"  # {{{\nt = '''\n}}}\n'''\n";
        assert!(scan("a.py", py).is_empty());
    }

    #[test]
    fn js_regex_literals_skipped() {
        assert!(scan("a.ts", "const re = /[(]/g;\n").is_empty());
        assert!(scan("a.ts", "s.split(/[.)]/);\n").is_empty());
        assert!(scan("a.js", "const url = /https?:\\/\\//;\n").is_empty());
        // Division is still division
        assert!(scan("a.ts", "const x = (a) / b / c;\n").is_empty());
    }

    #[test]
    fn go_and_java_raw_strings() {
        assert!(scan("a.go", "re := regexp.MustCompile(`\\{[0-9]+`)\n").is_empty());
        assert!(scan("a.go", "u := `https://x.com`\nf(u)\n").is_empty());
        assert!(scan(
            "A.java",
            "String q = \"\"\"\n  SELECT f(x, ( -- prose\n\"\"\";\n"
        )
        .is_empty());
    }

    #[test]
    fn template_literals_with_embeds() {
        let ok = "const s = `hello ${a ? f(b) : c} world ${d}`;\n";
        assert!(scan("a.ts", ok).is_empty());
        let broken = "const s = `hello ${f(a} world`;\n"; // ( closed by }
        assert!(!scan("a.ts", broken).is_empty());
        let plain_braces = "const s = `object: { not: code }`;\n";
        assert!(scan("a.ts", plain_braces).is_empty());
    }

    #[test]
    fn rust_lifetimes_chars_raw_strings() {
        let ok = "fn f<'a>(x: &'a str) -> char { let c = '}'; let s = r#\"}}\"#; c }\n";
        assert!(scan("a.rs", ok).is_empty());
        let nested_comment = "/* outer /* inner */ still */ fn g() {}\n";
        assert!(scan("a.rs", nested_comment).is_empty());
    }

    #[test]
    fn data_files_fully_parsed_with_exemptions() {
        assert!(!scan("p.json", r#"{"a": 1,}"#).is_empty()); // trailing comma
        assert!(!scan("p.yaml", "a: [1, 2\n").is_empty());
        assert!(!scan("p.toml", "name = \n").is_empty());
        // JSONC configs exempt
        assert!(scan("tsconfig.json", "{\n  // strict\n  \"strict\": true,\n}").is_empty());
        assert!(scan(".vscode/settings.json", "{ // x\n}").is_empty());
        // Multi-document YAML is valid
        assert!(scan("k8s.yaml", "kind: Service\n---\nkind: Deployment\n").is_empty());
        // New data files must parse (blocked even with original None)
        assert!(validate_change("p.json", None, r#"{"a": 1,}"#).is_err());
    }

    #[test]
    fn new_code_files_never_blocked() {
        // JSX prose with unbalanced punctuation — dialect beyond the lexer.
        let jsx = "export const P = () => <p>1) Install deps</p>;\n";
        assert!(validate_change("a.tsx", None, jsx).is_ok());
        // Even genuinely broken new code files pass the gate (documented
        // tradeoff — edit_file is the guarded path).
        assert!(validate_change("a.ts", None, "function f() {\n").is_ok());
    }

    #[test]
    fn jsx_prose_edits_tolerated_via_no_worse() {
        // Apostrophe in JSX text swallows the line; issue kinds stay equal
        // across an unrelated edit, so the edit passes.
        let orig = "export const P = () => (\n  <p>It's {name}</p>\n);\n";
        let edited =
            "import React from 'react';\nexport const P = () => (\n  <p>It's {name}</p>\n);\n";
        assert!(validate_change("a.tsx", Some(orig), edited).is_ok());
    }

    #[test]
    fn pre_broken_files_only_need_no_worse() {
        let broken = "function f() { // vendored, already broken\n";
        assert!(validate_change("a.ts", Some(broken), broken).is_ok());
        assert!(validate_change("a.ts", Some(broken), "function f() { }\n").is_ok());
        assert!(validate_change("a.ts", Some(broken), "function f() { {\n").is_err());
    }

    #[test]
    fn multiline_strings_do_not_false_reject() {
        let s = "const a = \"unclosed\nconst b = { c: 1 };\n";
        assert!(scan("a.ts", s).is_empty());
    }

    #[test]
    fn mismatched_pairs_caught() {
        assert!(!scan("a.ts", "const a = [1, 2);\n").is_empty());
        assert!(!scan("a.ts", "}}}\n").is_empty());
    }
}
