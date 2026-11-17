//! A diff holds the lines the change needs, not a formatter's opinion.
//!
//! A pull request whose fix was a few lines also re-wrapped calls and
//! re-quoted strings in end-to-end files the repository keeps unformatted
//! and its CI never checks, so a reviewer read formatting churn to find the
//! fix. The rules:
//!
//! - [`format_only`] tells a change that is only formatting: the two texts
//!   read the same once whitespace outside strings, quote style and
//!   trailing commas are set aside (indentation stays meaningful in Python,
//!   YAML and Makefiles, where only trailing spaces and blank lines are).
//! - [`drop_format_hunks`] puts back each hunk of a change that is only
//!   formatting, keeping the hunks that change something.
//! - [`ci_formats`] says whether the repository's CI checks the formatting
//!   of a path (`prettier --check`, `cargo fmt --check`, ...): only there
//!   may the formatter's lines ride along, and only in files the change
//!   touches anyway.

/// How a file's text is compared for formatting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lang {
    /// Braces and semicolons carry the structure: whitespace outside
    /// strings is formatting.
    Braces,
    /// Indentation is structure: only trailing spaces and blank lines are
    /// formatting.
    Indent,
    /// Prose and anything else: trailing spaces, blank lines at the end.
    Text,
}

fn ext(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or(path);
    if name == "Makefile" || name == "Dockerfile" {
        return name.to_ascii_lowercase();
    }
    name.rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default()
}

fn lang(path: &str) -> Lang {
    match ext(path).as_str() {
        "js" | "jsx" | "ts" | "tsx" | "mjs" | "cjs" | "mts" | "cts" | "json" | "css" | "scss"
        | "less" | "rs" | "go" | "java" | "kt" | "c" | "h" | "cc" | "cpp" | "hpp" | "cs"
        | "swift" | "php" | "tf" | "hcl" | "graphql" | "gql" | "vue" | "svelte" => Lang::Braces,
        "py" | "yaml" | "yml" | "makefile" | "toml" | "ini" | "cfg" => Lang::Indent,
        _ => Lang::Text,
    }
}

/// Whether a semicolon is optional punctuation in the language (so a
/// formatter adds or drops it).
fn optional_semicolons(path: &str) -> bool {
    matches!(
        ext(path).as_str(),
        "js" | "jsx" | "ts" | "tsx" | "mjs" | "cjs" | "mts" | "cts" | "vue" | "svelte"
    )
}

/// Whether `'` and `"` delimit the same strings in the language.
fn same_quotes(path: &str) -> bool {
    matches!(
        ext(path).as_str(),
        "js" | "jsx"
            | "ts"
            | "tsx"
            | "mjs"
            | "cjs"
            | "mts"
            | "cts"
            | "vue"
            | "svelte"
            | "py"
            | "css"
            | "scss"
            | "less"
            | "yaml"
            | "yml"
    )
}

/// The text with formatting set aside: what two versions that differ only
/// in formatting share.
fn canonical(path: &str, text: &str) -> String {
    match lang(path) {
        Lang::Braces => braces(path, text),
        Lang::Indent => text
            .lines()
            .map(|l| {
                let l = l.trim_end();
                if same_quotes(path) {
                    l.replace('\'', "\"")
                } else {
                    l.to_string()
                }
            })
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Lang::Text => text
            .lines()
            .map(str::trim_end)
            .collect::<Vec<_>>()
            .join("\n")
            .trim_end()
            .to_string(),
    }
}

fn braces(path: &str, text: &str) -> String {
    let quotes = same_quotes(path);
    let semis = optional_semicolons(path);
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_str: Option<char> = None;
    while let Some(c) = chars.next() {
        if let Some(q) = in_str {
            if c == '\\' {
                out.push(c);
                if let Some(n) = chars.next() {
                    out.push(n);
                }
                continue;
            }
            if c == q {
                in_str = None;
                out.push(if quotes && q == '\'' { '"' } else { c });
                continue;
            }
            out.push(c);
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                in_str = Some(c);
                out.push(if quotes && c == '\'' { '"' } else { c });
            }
            c if c.is_whitespace() => {
                // Space between two words is still a separator (`return x`
                // is not `returnx`); anywhere else it is formatting.
                while chars.peek().is_some_and(|n| n.is_whitespace()) {
                    chars.next();
                }
                let word = |ch: Option<char>| {
                    ch.is_some_and(|ch| ch.is_alphanumeric() || ch == '_' || ch == '$')
                };
                if word(out.chars().next_back()) && word(chars.peek().copied()) {
                    out.push(' ');
                }
            }
            ';' if semis => {}
            ',' => {
                // A trailing comma before a closing bracket is formatting.
                let mut ahead = chars.clone();
                let next = loop {
                    match ahead.next() {
                        Some(n) if n.is_whitespace() => continue,
                        other => break other,
                    }
                };
                if !matches!(next, Some(')' | ']' | '}')) {
                    out.push(',');
                }
            }
            // A line comment's end is its newline: keep one, or two
            // comments joined would read the same as one.
            '/' if chars.peek() == Some(&'/') => {
                for n in chars.by_ref() {
                    if n == '\n' {
                        break;
                    }
                    out.push(n);
                }
                out.push('/');
                out.push('\n');
            }
            _ => out.push(c),
        }
    }
    out
}

/// Whether `new` differs from `old` only in formatting.
pub fn format_only(path: &str, old: &str, new: &str) -> bool {
    old != new && canonical(path, old) == canonical(path, new)
}

/// Lines with their newlines, so joining them gives the text back.
fn lines(text: &str) -> Vec<&str> {
    text.split_inclusive('\n').collect()
}

/// The longest common subsequence's pairs of line indexes, or `None` when
/// the middle that differs is too large to compare line by line.
fn matching(a: &[&str], b: &[&str]) -> Option<Vec<(usize, usize)>> {
    const MAX_CELLS: usize = 4_000_000;
    let mut pre = 0;
    while pre < a.len() && pre < b.len() && a[pre] == b[pre] {
        pre += 1;
    }
    let mut suf = 0;
    while suf < a.len() - pre && suf < b.len() - pre && a[a.len() - 1 - suf] == b[b.len() - 1 - suf]
    {
        suf += 1;
    }
    let (am, bm) = (&a[pre..a.len() - suf], &b[pre..b.len() - suf]);
    if am.len().saturating_mul(bm.len()) > MAX_CELLS {
        return None;
    }
    let (n, m) = (am.len(), bm.len());
    let mut dp = vec![0u32; (n + 1) * (m + 1)];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i * (m + 1) + j] = if am[i] == bm[j] {
                dp[(i + 1) * (m + 1) + j + 1] + 1
            } else {
                dp[(i + 1) * (m + 1) + j].max(dp[i * (m + 1) + j + 1])
            };
        }
    }
    let mut pairs: Vec<(usize, usize)> = (0..pre).map(|k| (k, k)).collect();
    let (mut i, mut j) = (0, 0);
    while i < n && j < m {
        if am[i] == bm[j] {
            pairs.push((pre + i, pre + j));
            i += 1;
            j += 1;
        } else if dp[(i + 1) * (m + 1) + j] >= dp[i * (m + 1) + j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }
    for k in 0..suf {
        pairs.push((a.len() - suf + k, b.len() - suf + k));
    }
    Some(pairs)
}

/// `new` with each hunk that only reformats put back as it was in `old`,
/// and how many were put back. `new` unchanged when the texts are too far
/// apart to compare line by line.
pub fn drop_format_hunks(path: &str, old: &str, new: &str) -> (String, usize) {
    if format_only(path, old, new) {
        return (old.to_string(), 1);
    }
    let (a, b) = (lines(old), lines(new));
    let Some(pairs) = matching(&a, &b) else {
        return (new.to_string(), 0);
    };
    let mut out = String::with_capacity(new.len());
    let mut dropped = 0;
    let (mut i, mut j) = (0, 0);
    let mut hunk = |ai: std::ops::Range<usize>, bj: std::ops::Range<usize>, out: &mut String| {
        let (text, n) = shape_hunk(path, &a[ai], &b[bj]);
        dropped += n;
        out.push_str(&text);
    };
    for (pi, pj) in pairs {
        hunk(i..pi, j..pj, &mut out);
        out.push_str(b[pj]);
        i = pi + 1;
        j = pj + 1;
    }
    hunk(i..a.len(), j..b.len(), &mut out);
    (out, dropped)
}

/// One changed hunk, `a` (old lines) against `b` (new lines): the text it
/// keeps and how many parts of it were only formatting and went back. A
/// hunk that reformats lines and changes others is split where the
/// reformatted lines end or begin.
fn shape_hunk(path: &str, a: &[&str], b: &[&str]) -> (String, usize) {
    const MAX_PAIRS: usize = 1600;
    if a.is_empty() && b.is_empty() {
        return (String::new(), 0);
    }
    let blank = |l: &[&str]| l.iter().all(|x| x.trim().is_empty());
    if blank(a) && blank(b) {
        // Blank lines added or taken away: formatting.
        return (a.concat(), 1);
    }
    if a.is_empty() || b.is_empty() {
        return (b.concat(), 0);
    }
    let same = |x: &[&str], y: &[&str]| {
        let cx = canonical(path, &x.concat());
        !cx.is_empty() && cx == canonical(path, &y.concat())
    };
    if same(a, b) {
        return (a.concat(), 1);
    }
    if a.len() * b.len() > MAX_PAIRS {
        return (b.concat(), 0);
    }
    for x in 1..=a.len() {
        for y in 1..=b.len() {
            if (x, y) == (a.len(), b.len()) || !same(&a[..x], &b[..y]) {
                continue;
            }
            let (rest, n) = shape_hunk(path, &a[x..], &b[y..]);
            return (format!("{}{rest}", a[..x].concat()), n + 1);
        }
    }
    for x in 1..a.len() {
        for y in 1..b.len() {
            if !same(&a[a.len() - x..], &b[b.len() - y..]) {
                continue;
            }
            let (rest, n) = shape_hunk(path, &a[..a.len() - x], &b[..b.len() - y]);
            return (format!("{rest}{}", a[a.len() - x..].concat()), n + 1);
        }
    }
    (b.concat(), 0)
}

/// The formatter checks of a CI command, by the languages they cover.
fn covers(cmd: &str, path: &str) -> bool {
    let c = cmd.to_ascii_lowercase();
    let e = ext(path);
    let any = |exts: &[&str]| exts.contains(&e.as_str());
    let js = [
        "js", "jsx", "ts", "tsx", "mjs", "cjs", "mts", "cts", "json", "css", "scss", "less", "md",
        "yaml", "yml", "vue", "svelte", "graphql",
    ];
    if c.contains("prettier")
        || c.contains("biome")
        || c.contains("format:check")
        || c.contains("format-check")
    {
        // A prettier run scoped to folders covers only those folders.
        let scoped: Vec<&str> = c
            .split_whitespace()
            .filter(|w| !w.starts_with('-') && (w.contains('/') || *w == "."))
            .collect();
        let in_scope = scoped.is_empty()
            || scoped.iter().any(|s| {
                let s = s.trim_matches(|ch| ch == '"' || ch == '\'');
                s == "."
                    || path.starts_with(
                        s.trim_start_matches("./")
                            .trim_end_matches("**")
                            .trim_end_matches('/'),
                    )
            });
        return any(&js) && in_scope;
    }
    if c.contains("cargo fmt") || c.contains("rustfmt") {
        return e == "rs";
    }
    if c.contains("black") || c.contains("ruff format") || c.contains("isort") {
        return e == "py";
    }
    if c.contains("gofmt") || c.contains("goimports") {
        return e == "go";
    }
    if c.contains("terraform fmt") || c.contains("tofu fmt") || c.contains("hclfmt") {
        return any(&["tf", "hcl", "tfvars"]);
    }
    false
}

/// Whether one of CI's formatter checks (`cmds`) covers `path`.
pub fn ci_formats(cmds: &[String], path: &str) -> bool {
    cmds.iter().any(|c| covers(c, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrapping_and_requoting_is_formatting() {
        let old = "    expect(\n      screen.queryByText('Open registration')\n    ).toBeNull()\n";
        let new = "    expect(screen.queryByText(\"Open registration\")).toBeNull();\n";
        assert!(format_only("e2e/a.spec.ts", old, new));
        // A changed string is not.
        let other = "    expect(screen.queryByText(\"Open the registration\")).toBeNull();\n";
        assert!(!format_only("e2e/a.spec.ts", old, other));
        // Indentation is structure in Python.
        assert!(!format_only(
            "a.py",
            "if x:\n    y()\nz()\n",
            "if x:\n    y()\n    z()\n"
        ));
        assert!(format_only("a.py", "x = 'a'  \n\n", "x = \"a\"\n"));
        // A dropped final newline is formatting.
        assert!(format_only(
            "Dockerfile",
            "CMD [\"node\"]\n",
            "CMD [\"node\"]"
        ));
        // Rust's semicolon is not optional.
        assert!(!format_only(
            "a.rs",
            "fn f() -> u8 { 1 }",
            "fn f() -> u8 { 1; }"
        ));
    }

    #[test]
    fn only_the_hunks_that_change_something_stay() {
        let old = "const a = f(\n  1,\n  2,\n);\nconst b = 'x';\nconst c = 3;\n";
        let new = "const a = f(1, 2);\nconst b = 'x';\nconst c = 4;\n";
        let (kept, dropped) = drop_format_hunks("e2e/a.ts", old, new);
        assert_eq!(dropped, 1);
        assert_eq!(
            kept,
            "const a = f(\n  1,\n  2,\n);\nconst b = 'x';\nconst c = 4;\n"
        );
        // Nothing to drop: the text is the new one.
        let (same, n) = drop_format_hunks("e2e/a.ts", old, &old.replace("'x'", "'y'"));
        assert_eq!((same.as_str(), n), (old.replace("'x'", "'y'").as_str(), 0));
    }

    #[test]
    fn ci_says_where_formatting_is_checked() {
        let ci = vec!["pnpm --filter web exec prettier --check apps/web".to_string()];
        assert!(ci_formats(&ci, "apps/web/src/a.tsx"));
        assert!(!ci_formats(&ci, "e2e/pages/a.ts"));
        assert!(ci_formats(
            &["cargo fmt --all -- --check".into()],
            "services/a.rs"
        ));
        assert!(!ci_formats(&[], "a.ts"));
    }
}
