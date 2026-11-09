//! The "PR Overview" a review leads with: one sentence on what the pull
//! request does, a few bullets of what it changes, and one short line per
//! changed file. The reviewer writes it with its findings
//! ([`ModelOverview`]); [`build`] holds it to the diff (only files the diff
//! changed, in its order, each with a line even when the model left one
//! out), [`markdown`] renders it for the GitHub review, and the session
//! record keeps it for the dashboard ([`to_attr`], [`of_item`]).

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::Deserialize;

/// Longest overview sentence, in bytes.
pub const MAX_SUMMARY: usize = 300;
/// Most "Changes" bullets.
pub const MAX_CHANGES: usize = 8;
/// Longest bullet, in bytes.
pub const MAX_CHANGE: usize = 200;
/// Longest file description, in bytes.
pub const MAX_DESCRIPTION: usize = 140;
/// Longest path kept, in bytes.
pub const MAX_PATH: usize = 300;
/// Most file rows kept (the rest are counted in `files_total`).
pub const MAX_FILES: usize = 100;
/// Above this many rows the table is folded in a `<details>`.
pub const FOLD_AFTER: usize = 15;

/// What the model wrote, as it wrote it.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ModelOverview {
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub changes: Vec<String>,
    #[serde(default)]
    pub files: Vec<ModelFile>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ModelFile {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub description: String,
}

/// One file the diff changed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangedFile {
    pub path: String,
    /// GitHub's word: added, removed, modified, renamed.
    pub status: String,
    pub additions: u64,
    pub deletions: u64,
}

/// Pure: the changed files of a compare (`files[]` of GitHub's compare API).
pub fn changed_files(compare: &serde_json::Value) -> Vec<ChangedFile> {
    compare["files"]
        .as_array()
        .map(|files| {
            files
                .iter()
                .filter_map(|f| {
                    let path = f["filename"].as_str()?;
                    Some(ChangedFile {
                        path: path.to_string(),
                        status: f["status"].as_str().unwrap_or("").to_string(),
                        additions: f["additions"].as_u64().unwrap_or(0),
                        deletions: f["deletions"].as_u64().unwrap_or(0),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// One row of the "Reviewed Changes" table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRow {
    pub path: String,
    pub description: String,
}

/// The overview as posted and shown.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Overview {
    pub summary: String,
    pub changes: Vec<String>,
    /// At most [`MAX_FILES`], in the diff's order.
    pub files: Vec<FileRow>,
    /// How many files the diff changed.
    pub files_total: usize,
}

impl Overview {
    pub fn is_empty(&self) -> bool {
        self.summary.is_empty() && self.changes.is_empty() && self.files.is_empty()
    }
}

/// Pure: one line, at most `max` bytes, cut on a char boundary.
fn line(text: &str, max: usize) -> String {
    let one = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if one.len() <= max {
        return one;
    }
    let cut = crate::truncate_str(&one, max.saturating_sub('…'.len_utf8()));
    format!("{}…", cut.trim_end())
}

/// Pure: what a file's row says when the model wrote nothing for it.
pub fn fallback_description(f: &ChangedFile) -> String {
    match f.status.as_str() {
        "added" => format!("New file (+{})", f.additions),
        "removed" => "Removed".into(),
        "renamed" if f.additions + f.deletions == 0 => "Renamed".into(),
        "renamed" => format!("Renamed and changed (+{}/-{})", f.additions, f.deletions),
        _ => format!("Changed (+{}/-{})", f.additions, f.deletions),
    }
}

/// Pure: the overview held to the diff. The summary is its first sentence;
/// a file the model described that the diff didn't change is dropped; a
/// changed file it didn't describe gets [`fallback_description`].
pub fn build(model: &ModelOverview, changed: &[ChangedFile]) -> Overview {
    let summary = {
        let prose = model
            .summary
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let first = crate::prose::first_sentences(&prose, 1, MAX_SUMMARY);
        line(if first.is_empty() { &prose } else { first }, MAX_SUMMARY)
    };
    let changes = model
        .changes
        .iter()
        .map(|c| line(c.trim().trim_start_matches(['-', '*']).trim(), MAX_CHANGE))
        .filter(|c| !c.is_empty())
        .take(MAX_CHANGES)
        .collect();
    let described: HashMap<&str, &str> = model
        .files
        .iter()
        .map(|f| (f.path.trim().trim_matches('`'), f.description.trim()))
        .filter(|(p, d)| !p.is_empty() && !d.is_empty())
        .collect();
    let files = changed
        .iter()
        .take(MAX_FILES)
        .map(|f| FileRow {
            path: line(&f.path, MAX_PATH),
            description: match described.get(f.path.as_str()) {
                Some(d) => line(d, MAX_DESCRIPTION),
                None => fallback_description(f),
            },
        })
        .collect();
    Overview {
        summary,
        changes,
        files,
        files_total: changed.len(),
    }
}

/// Pure: text safe inside a markdown table cell.
fn cell(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace(['\n', '\r'], " ")
}

/// Pure: a path as inline code, whatever backticks it holds.
fn code(path: &str) -> String {
    format!("`{}`", cell(&path.replace('`', "'")))
}

/// Pure: the overview as the GitHub review's opening section. Empty when the
/// overview is.
pub fn markdown(o: &Overview) -> String {
    if o.is_empty() {
        return String::new();
    }
    let mut out = String::from("## PR Overview\n");
    if !o.summary.is_empty() {
        out.push_str(&format!("\n{}\n", o.summary));
    }
    if !o.changes.is_empty() {
        out.push_str("\n**Changes**\n");
        for c in &o.changes {
            out.push_str(&format!("- {c}\n"));
        }
    }
    if !o.files.is_empty() {
        let count = if o.files_total == 1 {
            "1 changed file".to_string()
        } else {
            format!("{} changed files", o.files_total)
        };
        out.push_str(&format!("\n**Reviewed Changes**\n\n{count}.\n"));
        let mut table = String::from("\n| File | Description |\n| --- | --- |\n");
        for f in &o.files {
            table.push_str(&format!(
                "| {} | {} |\n",
                code(&f.path),
                cell(&f.description)
            ));
        }
        let more = o.files_total.saturating_sub(o.files.len());
        if more > 0 {
            table.push_str(&format!(
                "\nAnd {more} more {}.\n",
                if more == 1 { "file" } else { "files" }
            ));
        }
        if o.files.len() > FOLD_AFTER {
            out.push_str(&format!(
                "\n<details>\n<summary>Show a summary per file</summary>\n{table}\n</details>\n"
            ));
        } else {
            out.push_str(&table);
        }
    }
    out
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// The overview as one session-record attribute.
pub fn to_attr(o: &Overview) -> AttributeValue {
    let files = o
        .files
        .iter()
        .map(|f| {
            AttributeValue::M(HashMap::from([
                ("path".to_string(), s(&f.path)),
                ("description".to_string(), s(&f.description)),
            ]))
        })
        .collect();
    AttributeValue::M(HashMap::from([
        ("summary".to_string(), s(&o.summary)),
        (
            "changes".to_string(),
            AttributeValue::L(o.changes.iter().map(|c| s(c)).collect()),
        ),
        ("files".to_string(), AttributeValue::L(files)),
        (
            "files_total".to_string(),
            AttributeValue::N(o.files_total.to_string()),
        ),
    ]))
}

/// Pure: the overview a session record holds, if any. Read back with the
/// same caps it was written with.
pub fn of_item(item: &HashMap<String, AttributeValue>) -> Option<Overview> {
    let m = item.get("overview")?.as_m().ok()?;
    let text =
        |v: Option<&AttributeValue>| v.and_then(|v| v.as_s().ok()).cloned().unwrap_or_default();
    let list = |k: &str| {
        m.get(k)
            .and_then(|v| v.as_l().ok())
            .cloned()
            .unwrap_or_default()
    };
    let changes: Vec<String> = list("changes")
        .iter()
        .filter_map(|v| v.as_s().ok().cloned())
        .take(MAX_CHANGES)
        .collect();
    let files: Vec<FileRow> = list("files")
        .iter()
        .filter_map(|v| v.as_m().ok())
        .map(|f| FileRow {
            path: text(f.get("path")),
            description: text(f.get("description")),
        })
        .take(MAX_FILES)
        .collect();
    let files_total = m
        .get("files_total")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(files.len())
        .max(files.len());
    let o = Overview {
        summary: text(m.get("summary")),
        changes,
        files,
        files_total,
    };
    (!o.is_empty()).then_some(o)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn changed(path: &str, status: &str) -> ChangedFile {
        ChangedFile {
            path: path.into(),
            status: status.into(),
            additions: 4,
            deletions: 1,
        }
    }

    fn model(files: &[(&str, &str)]) -> ModelOverview {
        ModelOverview {
            summary: "Sorts orders before filtering them. It also renames a helper.".into(),
            changes: vec!["- Sort first".into(), "".into(), "Rename `helper`".into()],
            files: files
                .iter()
                .map(|(p, d)| ModelFile {
                    path: p.to_string(),
                    description: d.to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn the_overview_is_held_to_the_diff() {
        let diff = vec![
            changed("src/a.rs", "modified"),
            changed("src/new.rs", "added"),
        ];
        let o = build(
            &model(&[
                ("src/a.rs", "Sorts before the filter"),
                ("src/invented.rs", "Not in the diff"),
            ]),
            &diff,
        );
        assert_eq!(o.summary, "Sorts orders before filtering them.");
        assert_eq!(o.changes, vec!["Sort first", "Rename `helper`"]);
        assert_eq!(
            o.files,
            vec![
                FileRow {
                    path: "src/a.rs".into(),
                    description: "Sorts before the filter".into()
                },
                FileRow {
                    path: "src/new.rs".into(),
                    description: "New file (+4)".into()
                },
            ]
        );
        assert_eq!(o.files_total, 2);
        assert!(build(&ModelOverview::default(), &[]).is_empty());
    }

    #[test]
    fn caps_hold() {
        let diff: Vec<ChangedFile> = (0..(MAX_FILES + 7))
            .map(|i| changed(&format!("f{i}.rs"), "modified"))
            .collect();
        let mut m = model(&[("f0.rs", &"long words ".repeat(100))]);
        m.changes = (0..20).map(|i| format!("change {i}")).collect();
        m.summary = "x".repeat(2_000);
        let o = build(&m, &diff);
        assert_eq!(o.files.len(), MAX_FILES);
        assert_eq!(o.files_total, MAX_FILES + 7);
        assert_eq!(o.changes.len(), MAX_CHANGES);
        assert!(o.summary.len() <= MAX_SUMMARY);
        assert!(o.files[0].description.len() <= MAX_DESCRIPTION);
        let md = markdown(&o);
        assert!(md.contains("And 7 more files."), "{md}");
    }

    #[test]
    fn a_small_pr_renders_an_open_table() {
        let o = build(
            &model(&[("src/a.rs", "Sorts | filters")]),
            &[changed("src/a.rs", "modified")],
        );
        let md = markdown(&o);
        assert_eq!(
            md,
            "## PR Overview\n\nSorts orders before filtering them.\n\n**Changes**\n- Sort first\n\
             - Rename `helper`\n\n**Reviewed Changes**\n\n1 changed file.\n\n\
             | File | Description |\n| --- | --- |\n| `src/a.rs` | Sorts \\| filters |\n"
        );
        assert!(!md.contains("<details>"));
    }

    #[test]
    fn a_large_pr_folds_the_table() {
        let diff: Vec<ChangedFile> = (0..(FOLD_AFTER + 1))
            .map(|i| changed(&format!("f{i}.rs"), "modified"))
            .collect();
        let md = markdown(&build(&model(&[]), &diff));
        assert!(md.contains("<details>\n<summary>Show a summary per file</summary>"));
        assert!(md.trim_end().ends_with("</details>"));
        assert!(md.contains("| `f0.rs` | Changed (+4/-1) |"));
        // The summary and bullets stay outside the fold.
        let fold = md.find("<details>").unwrap();
        assert!(md.find("**Changes**").unwrap() < fold);
        let at_limit: Vec<ChangedFile> = diff[..FOLD_AFTER].to_vec();
        assert!(!markdown(&build(&model(&[]), &at_limit)).contains("<details>"));
    }

    #[test]
    fn fallbacks_say_what_happened_to_the_file() {
        let mut f = changed("a", "removed");
        assert_eq!(fallback_description(&f), "Removed");
        f.status = "renamed".into();
        f.additions = 0;
        f.deletions = 0;
        assert_eq!(fallback_description(&f), "Renamed");
        f.status = "modified".into();
        assert_eq!(fallback_description(&f), "Changed (+0/-0)");
    }

    #[test]
    fn cells_never_break_the_table() {
        let o = Overview {
            summary: String::new(),
            changes: vec![],
            files: vec![FileRow {
                path: "we`ird|path".into(),
                description: "a\nb | c".into(),
            }],
            files_total: 1,
        };
        let md = markdown(&o);
        assert!(md.contains("| `we'ird\\|path` | a b \\| c |"), "{md}");
        assert_eq!(markdown(&Overview::default()), "");
    }

    #[test]
    fn the_session_keeps_it_as_written() {
        let o = build(
            &model(&[("src/a.rs", "Sorts")]),
            &[changed("src/a.rs", "modified"), changed("b.rs", "added")],
        );
        let item = HashMap::from([("overview".to_string(), to_attr(&o))]);
        assert_eq!(of_item(&item), Some(o));
        assert_eq!(of_item(&HashMap::new()), None);
        let empty = HashMap::from([("overview".to_string(), to_attr(&Overview::default()))]);
        assert_eq!(of_item(&empty), None);
    }

    #[test]
    fn changed_files_read_the_compare() {
        let compare = serde_json::json!({"files": [
            {"filename": "a.rs", "status": "modified", "additions": 3, "deletions": 2},
            {"status": "added"}
        ]});
        assert_eq!(
            changed_files(&compare),
            vec![ChangedFile {
                path: "a.rs".into(),
                status: "modified".into(),
                additions: 3,
                deletions: 2
            }]
        );
        assert!(changed_files(&serde_json::json!({})).is_empty());
    }
}
