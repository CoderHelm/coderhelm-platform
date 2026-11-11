//! GitHub Code Quality findings (`GET /repos/{o}/{r}/code-quality/findings`,
//! the `code_quality` permission) as reviewer context: the open findings on
//! the files a pull request changes, so the review doesn't report them a
//! second time and can say when the change fixes some.
//!
//! A finding: `number`, `state` (`open` | `dismissed`), `url`, `rule`
//! (`id`, `title`, `severity`: `error` | `warning` | `note` | `none`,
//! `category`: `maintainability` | `reliability` | `none`), `location`
//! (`path`, `start_line`, `end_line`) and `message.text`. The listing has
//! no ref or path filter: it is the default branch's.

use serde_json::Value;

/// The listing path: open findings, newest first.
pub fn list_path(owner: &str, repo: &str) -> String {
    format!("/repos/{owner}/{repo}/code-quality/findings?state=open&per_page=100")
}

/// The most findings put in front of the reviewer.
pub const MAX_IN_CONTEXT: usize = 25;

/// One open finding on a changed file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnFile {
    pub number: u64,
    pub path: String,
    pub start_line: u64,
    pub end_line: u64,
    pub rule: String,
    pub severity: String,
    pub message: String,
}

/// The open findings in `findings` whose file is one of `changed`, worst
/// first, then by file and line.
pub fn on_changed_files(findings: &[Value], changed: &[String]) -> Vec<OnFile> {
    let rank = |s: &str| match s {
        "error" => 0,
        "warning" => 1,
        "note" => 2,
        _ => 3,
    };
    let mut out: Vec<OnFile> = findings
        .iter()
        .filter(|f| f["state"].as_str().is_none_or(|s| s == "open"))
        .filter_map(|f| {
            let path = f["location"]["path"].as_str()?;
            if !changed.iter().any(|c| c == path) {
                return None;
            }
            let start = f["location"]["start_line"].as_u64().unwrap_or(0);
            Some(OnFile {
                number: f["number"].as_u64()?,
                path: path.to_string(),
                start_line: start,
                end_line: f["location"]["end_line"].as_u64().unwrap_or(start),
                rule: f["rule"]["title"]
                    .as_str()
                    .or(f["rule"]["id"].as_str())
                    .unwrap_or("finding")
                    .to_string(),
                severity: f["rule"]["severity"].as_str().unwrap_or("").to_string(),
                message: f["message"]["text"]
                    .as_str()
                    .unwrap_or("")
                    .lines()
                    .next()
                    .unwrap_or("")
                    .chars()
                    .take(200)
                    .collect(),
            })
        })
        .collect();
    out.sort_by(|a, b| {
        rank(&a.severity)
            .cmp(&rank(&b.severity))
            .then_with(|| a.path.cmp(&b.path))
            .then_with(|| a.start_line.cmp(&b.start_line))
    });
    out
}

/// The reviewer's prompt section for the findings, empty when there are none.
pub fn context(on_files: &[OnFile]) -> String {
    if on_files.is_empty() {
        return String::new();
    }
    let mut lines: Vec<String> = on_files
        .iter()
        .take(MAX_IN_CONTEXT)
        .map(|f| {
            let at = if f.end_line > f.start_line {
                format!("{}:{}-{}", f.path, f.start_line, f.end_line)
            } else {
                format!("{}:{}", f.path, f.start_line)
            };
            let sev = if f.severity.is_empty() {
                String::new()
            } else {
                format!(" ({})", f.severity)
            };
            format!("- #{} {at} {}{sev}: {}", f.number, f.rule, f.message)
        })
        .collect();
    if on_files.len() > MAX_IN_CONTEXT {
        lines.push(format!("- and {} more", on_files.len() - MAX_IN_CONTEXT));
    }
    format!(
        "\n## GitHub Code Quality findings already open on the changed files (default branch)\n{}\n\
         GitHub already reports these: never raise one again as a finding of your own. When \
         this change fixes one (the flagged code is removed or corrected), say so in the summary \
         as \"fixes N code quality finding(s)\" with their numbers.\n",
        lines.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn f(n: u64, path: &str, line: u64, sev: &str, state: &str) -> Value {
        json!({"number": n, "state": state, "url": "u",
               "rule": {"id": "py/unused-import", "title": "Unused import", "severity": sev,
                        "category": "maintainability", "description": "d"},
               "location": {"path": path, "start_line": line, "end_line": line},
               "message": {"text": "Import of 'os' is not used.\nmore", "markdown": "m"}})
    }

    #[test]
    fn only_open_findings_on_changed_files_worst_first() {
        let all = vec![
            f(1, "a.py", 3, "note", "open"),
            f(2, "b.py", 1, "error", "open"),
            f(3, "c.py", 1, "error", "open"),
            f(4, "a.py", 9, "warning", "dismissed"),
            f(5, "a.py", 1, "warning", "open"),
        ];
        let got = on_changed_files(&all, &["a.py".into(), "b.py".into()]);
        let nums: Vec<u64> = got.iter().map(|f| f.number).collect();
        assert_eq!(nums, [2, 5, 1]);
        assert_eq!(got[0].message, "Import of 'os' is not used.");
    }

    #[test]
    fn the_context_lists_them_and_says_not_to_repeat_them() {
        let got = on_changed_files(&[f(7, "a.py", 4, "warning", "open")], &["a.py".into()]);
        let c = context(&got);
        assert!(c.contains("- #7 a.py:4 Unused import (warning): Import of 'os' is not used."));
        assert!(c.contains("never raise one again"));
        assert!(c.contains("fixes N code quality finding(s)"));
        assert_eq!(context(&[]), "");
    }

    #[test]
    fn a_long_list_is_cut() {
        let all: Vec<Value> = (0..30).map(|n| f(n, "a.py", n, "note", "open")).collect();
        let c = context(&on_changed_files(&all, &["a.py".into()]));
        assert!(c.contains("- and 5 more"));
    }
}
