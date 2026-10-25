//! Chat's code tools: list the team's repositories, look one over, list its
//! files, read a file by line range, and search code. They read through
//! [`common::repo_read::CodeReader`], pinned to one commit per repository,
//! and every read is registered as a citable source named by its real path
//! and lines (with a link to those lines on GitHub at that commit).
//!
//! The names keep the `github__` prefix, so Chat's source kinds, the agent
//! permission gate and the tool registry treat them as reads of code.

use common::knowledge::answer::{CodeRef, Registry, SourceRef};
use common::repo_read::{blob_url, CodeReader, FileSlice, ReadError, RepoInfo};
use serde_json::{json, Value};

pub const LIST_REPOS: &str = "github__list_repos";
pub const OVERVIEW: &str = "github__repo_overview";
pub const LIST_FILES: &str = "github__list_files";
pub const READ_FILE: &str = "github__read_file";
pub const SEARCH_CODE: &str = "github__search_code";

/// Every code tool, in the order they are offered.
pub const TOOLS: [&str; 5] = [LIST_REPOS, OVERVIEW, LIST_FILES, READ_FILE, SEARCH_CODE];

pub fn is_code_tool(name: &str) -> bool {
    TOOLS.contains(&name)
}

/// The tools' definitions, with the repositories this question may read.
pub fn defs(repos: &[RepoInfo]) -> Vec<Value> {
    if repos.is_empty() {
        return Vec::new();
    }
    let names: Vec<&str> = repos.iter().map(|r| r.name.as_str()).collect();
    let repo = json!({
        "type": "string",
        "description": format!(
            "The repository, as owner/name or just its name. One of: {}",
            names.join(", ")
        )
    });
    vec![
        json!({
            "name": LIST_REPOS,
            "description": "List the team's connected repositories with what each is for. Use it when the question doesn't say which repository, or to see how they relate.",
            "input_schema": {"type": "object", "properties": {}}
        }),
        json!({
            "name": OVERVIEW,
            "description": "Look a repository over: its top folders and the folders under them with file counts, file types, the packages or services in it, and its README, agent notes (AGENTS.md, CLAUDE.md) and manifests (package.json, Cargo.toml, pyproject.toml and the like), read. Start here for any question about how a repository is built or what it does.",
            "input_schema": {"type": "object", "properties": {"repo": repo}, "required": ["repo"]}
        }),
        json!({
            "name": LIST_FILES,
            "description": "List a folder's files and subfolders (with file counts). With `pattern`, list every file in the repository whose path contains it (for example \"auth\" or \"routes\").",
            "input_schema": {"type": "object", "properties": {
                "repo": repo,
                "path": {"type": "string", "description": "The folder; leave out for the top"},
                "pattern": {"type": "string", "description": "Only paths containing this"}
            }, "required": ["repo"]}
        }),
        json!({
            "name": READ_FILE,
            "description": "Read a file, with line numbers. Reads the first 400 lines unless you give a range; the reply says how many lines the file has, so read on from where it stopped when you need more.",
            "input_schema": {"type": "object", "properties": {
                "repo": repo,
                "path": {"type": "string", "description": "The file's path from the repository's top"},
                "start_line": {"type": "integer", "minimum": 1},
                "end_line": {"type": "integer", "minimum": 1}
            }, "required": ["repo", "path"]}
        }),
        json!({
            "name": SEARCH_CODE,
            "description": "Search the code for words, a name or a phrase. Returns the matching files with the lines that match. Leave out `repo` to search every repository you can read.",
            "input_schema": {"type": "object", "properties": {
                "query": {"type": "string"},
                "repo": repo
            }, "required": ["query"]}
        }),
    ]
}

fn short(repo: &str) -> &str {
    repo.rsplit('/').next().unwrap_or(repo)
}

/// What the dashboard shows for a call, in plain words.
pub fn label(name: &str, input: &Value) -> String {
    let s = |k: &str| input[k].as_str().unwrap_or("").trim().to_string();
    let repo = s("repo");
    let repo = short(&repo).to_string();
    match name {
        LIST_REPOS => "Listed your repositories".into(),
        OVERVIEW => format!("Looked over {repo}"),
        LIST_FILES => {
            let pattern = s("pattern");
            let path = s("path");
            if !pattern.is_empty() {
                format!("Found files named like \u{201c}{pattern}\u{201d} in {repo}")
            } else if path.is_empty() {
                format!("Listed the files in {repo}")
            } else {
                format!("Listed {path} in {repo}")
            }
        }
        READ_FILE => {
            let path = s("path");
            if repo.is_empty() {
                format!("Read {path}")
            } else {
                format!("Read {path} in {repo}")
            }
        }
        SEARCH_CODE => {
            let q = common::truncate_str(&s("query"), 60).to_string();
            if repo.is_empty() {
                format!("Searched the code for \u{201c}{q}\u{201d}")
            } else {
                format!("Searched {repo} for \u{201c}{q}\u{201d}")
            }
        }
        _ => name.replace('_', " "),
    }
}

/// What a tool call gave: the model's text, a summary for the step, and
/// whether it failed.
#[derive(Debug, Clone, PartialEq)]
pub struct Out {
    pub text: String,
    pub summary: String,
    pub is_error: bool,
}

fn failed(e: ReadError) -> Out {
    Out {
        text: format!("Error: {e}"),
        summary: match e {
            ReadError::NotFound(_) => "Not found".into(),
            ReadError::Unavailable(_) => "Couldn't read".into(),
        },
        is_error: true,
    }
}

fn line_place(repo: &str, start: usize, end: usize) -> String {
    match (start, end) {
        (0, _) => short(repo).to_string(),
        (s, e) if e > s => format!("{} · lines {s}-{e}", short(repo)),
        (s, _) => format!("{} · line {s}", short(repo)),
    }
}

/// A file read, as a source.
fn slice_source(s: &FileSlice) -> SourceRef {
    SourceRef {
        kind: "code".into(),
        title: s.path.clone(),
        loc: line_place(&s.repo, s.start, s.end),
        passage: s.text.clone(),
        url: Some(s.url()),
        code: Some(CodeRef {
            repo: s.repo.clone(),
            path: s.path.clone(),
            commit: s.commit.clone(),
            start_line: s.start,
            end_line: s.end,
        }),
        ..Default::default()
    }
}

fn opt_line(v: &Value) -> Option<usize> {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
        .map(|n| n as usize)
        .filter(|n| *n > 0)
}

/// Runs one code tool and registers what it read.
pub async fn run(reader: &CodeReader<'_>, reg: &mut Registry, name: &str, input: &Value) -> Out {
    let s = |k: &str| input[k].as_str().unwrap_or("").trim().to_string();
    match name {
        LIST_REPOS => {
            let mut text = String::from("The team's repositories:\n");
            for r in reader.repos() {
                text.push_str(&format!("- {} (default branch {})", r.name, r.branch));
                if let Some(role) = r.role.as_deref().filter(|x| !x.is_empty()) {
                    text.push_str(&format!(", the {role} repository"));
                }
                if let Some(d) = r.description.as_deref().filter(|x| !x.is_empty()) {
                    text.push_str(&format!(": {d}"));
                }
                text.push('\n');
            }
            let id = reg.add(SourceRef {
                kind: "code".into(),
                title: "Your team's repositories".into(),
                loc: "Repository settings".into(),
                passage: text.clone(),
                href: Some("/settings/repos".into()),
                ..Default::default()
            });
            Out {
                text: reg.render_with(&id, &text),
                summary: format!("{} repositories", reader.repos().len()),
                is_error: false,
            }
        }
        OVERVIEW => match reader.overview(&s("repo")).await {
            Ok(o) => {
                let layout_id = reg.add(SourceRef {
                    kind: "code".into(),
                    title: format!("{} file layout", o.repo.short()),
                    loc: o.repo.name.clone(),
                    passage: o.layout.clone(),
                    url: Some(format!(
                        "https://github.com/{}/tree/{}",
                        o.repo.name, o.commit
                    )),
                    code: Some(CodeRef {
                        repo: o.repo.name.clone(),
                        commit: o.commit.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                let mut text = reg.render_with(&layout_id, &o.layout);
                for f in &o.files {
                    let id = reg.add(slice_source(f));
                    text.push_str(&reg.render_with(&id, &f.numbered()));
                }
                let read: Vec<&str> = o.files.iter().map(|f| f.path.as_str()).collect();
                Out {
                    text,
                    summary: if read.is_empty() {
                        "Layout".into()
                    } else {
                        format!("Layout and {}", read.join(", "))
                    },
                    is_error: false,
                }
            }
            Err(e) => failed(e),
        },
        LIST_FILES => match reader.list(&s("repo"), &s("path"), &s("pattern")).await {
            Ok(l) => {
                let where_ = if l.dir.is_empty() {
                    String::new()
                } else {
                    format!("/{}", l.dir)
                };
                let id = reg.add(SourceRef {
                    kind: "code".into(),
                    title: format!("Files in {}{where_}", short(&l.repo)),
                    loc: l.repo.clone(),
                    passage: l.text.clone(),
                    url: Some(format!(
                        "https://github.com/{}/tree/{}{where_}",
                        l.repo, l.commit
                    )),
                    code: Some(CodeRef {
                        repo: l.repo.clone(),
                        path: l.dir.clone(),
                        commit: l.commit.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                Out {
                    text: reg.render_with(&id, &l.text),
                    summary: format!("{} entries", l.count),
                    is_error: false,
                }
            }
            Err(e) => failed(e),
        },
        READ_FILE => {
            match reader
                .read(
                    &s("repo"),
                    &s("path"),
                    opt_line(&input["start_line"]),
                    opt_line(&input["end_line"]),
                )
                .await
            {
                Ok(slice) => {
                    let id = reg.add(slice_source(&slice));
                    Out {
                        text: reg.render_with(&id, &slice.numbered()),
                        summary: format!(
                            "Lines {}-{} of {}",
                            slice.start, slice.end, slice.total_lines
                        ),
                        is_error: false,
                    }
                }
                Err(e) => failed(e),
            }
        }
        SEARCH_CODE => {
            let repo = s("repo");
            let repos: Vec<String> = if repo.is_empty() {
                Vec::new()
            } else {
                vec![repo]
            };
            match reader.search(&repos, &s("query")).await {
                Ok(hits) if hits.is_empty() => Out {
                    text: "No code matched. Try other words, a likely file name with list_files, or look the repository over.".into(),
                    summary: "No matches".into(),
                    is_error: false,
                },
                Ok(hits) => {
                    let mut text = String::new();
                    for h in hits.iter().take(20) {
                        let passage = if h.fragments.is_empty() {
                            "(the file name matches)".to_string()
                        } else {
                            h.fragments.join("\n...\n")
                        };
                        let commit = reader
                            .repos()
                            .iter()
                            .find(|r| r.name == h.repo)
                            .map(|r| r.branch.clone())
                            .unwrap_or_else(|| "HEAD".into());
                        let id = reg.add(SourceRef {
                            kind: "code".into(),
                            title: h.path.clone(),
                            loc: short(&h.repo).to_string(),
                            passage: passage.clone(),
                            url: Some(blob_url(&h.repo, &commit, &h.path, 0, 0)),
                            code: Some(CodeRef {
                                repo: h.repo.clone(),
                                path: h.path.clone(),
                                commit,
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                        text.push_str(&reg.render_with(&id, &passage));
                    }
                    text.push_str("\nThese are matching lines only: read the files you rely on before you answer from them.");
                    Out {
                        text,
                        summary: format!("{} files", hits.len().min(20)),
                        is_error: false,
                    }
                }
                Err(e) => failed(e),
            }
        }
        _ => Out {
            text: format!("Unknown tool: {name}"),
            summary: "Unknown".into(),
            is_error: true,
        },
    }
}

/// The places a code graph answer names (`path:line (kind, rank)` or
/// `path — why`), so each becomes a source named by its file.
pub fn graph_places(text: &str) -> Vec<(String, usize)> {
    let mut out: Vec<(String, usize)> = Vec::new();
    for line in text.lines() {
        let head = line
            .trim()
            .split([' ', '\u{2014}'])
            .next()
            .unwrap_or("")
            .trim_end_matches(',');
        let (path, n) = match head.rsplit_once(':') {
            Some((p, n)) if n.bytes().all(|b| b.is_ascii_digit()) && !n.is_empty() => {
                (p, n.parse().unwrap_or(0))
            }
            _ => (head, 0),
        };
        let looks_like_path = path.contains('.') && !path.contains("://") && path.len() < 300;
        if looks_like_path
            && common::repo_read::clean_path(path).is_some()
            && !out.iter().any(|(p, _)| p == path)
        {
            out.push((path.to_string(), n));
        }
    }
    out
}

/// What the dashboard calls a code graph lookup.
pub fn graph_label(name: &str, input: &Value) -> String {
    let sym = input["name"].as_str().unwrap_or("").trim();
    match name {
        "graph_definition" => format!("Found where \u{201c}{sym}\u{201d} is defined"),
        "graph_callers" => format!("Found what uses \u{201c}{sym}\u{201d}"),
        "graph_impact" => "Found what those files reach".into(),
        n => n.replace('_', " "),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::repo_read::MemHost;

    fn host() -> MemHost {
        MemHost::default().with_repo(
            "acme/shop-web",
            "c0ffee12",
            &[
                ("README.md", "# Shop\nThe storefront.\n"),
                ("src/pay.ts", "a\nb\nexport function pay() {}\n"),
            ],
        )
    }

    #[tokio::test]
    async fn a_read_is_a_source_named_by_its_path_and_lines() {
        let h = host();
        let r = CodeReader::new(&h, vec![RepoInfo::new("acme/shop-web", "main")]);
        let mut reg = Registry::default();
        let out = run(
            &r,
            &mut reg,
            READ_FILE,
            &json!({"repo": "shop-web", "path": "src/pay.ts", "start_line": 2, "end_line": "3"}),
        )
        .await;
        assert!(!out.is_error);
        assert!(
            out.text.contains("3 | export function pay()"),
            "{}",
            out.text
        );
        let s = reg.get("S1").unwrap();
        assert_eq!(s.title, "src/pay.ts");
        assert_eq!(s.loc, "shop-web · lines 2-3");
        assert_eq!(
            s.url.as_deref(),
            Some("https://github.com/acme/shop-web/blob/c0ffee12/src/pay.ts#L2-L3")
        );
        assert_eq!(s.code.as_ref().unwrap().start_line, 2);
    }

    #[tokio::test]
    async fn the_overview_cites_the_layout_and_each_file_it_read() {
        let h = host();
        let r = CodeReader::new(&h, vec![RepoInfo::new("acme/shop-web", "main")]);
        let mut reg = Registry::default();
        let out = run(&r, &mut reg, OVERVIEW, &json!({"repo": "acme/shop-web"})).await;
        assert!(out.text.contains("README.md") && out.text.contains("Top of the tree"));
        let titles: Vec<String> = (1..=reg.len())
            .map(|i| reg.get(&format!("S{i}")).unwrap().title.clone())
            .collect();
        assert_eq!(titles, ["shop-web file layout", "README.md"]);
        assert!(titles.iter().all(|t| !t.contains("Code graph")));
    }

    #[tokio::test]
    async fn failures_tell_the_model_what_is_there() {
        let h = host();
        let r = CodeReader::new(&h, vec![RepoInfo::new("acme/shop-web", "main")]);
        let mut reg = Registry::default();
        let out = run(
            &r,
            &mut reg,
            READ_FILE,
            &json!({"repo": "shop-web", "path": "pay.ts"}),
        )
        .await;
        assert!(
            out.is_error && out.text.contains("src/pay.ts"),
            "{}",
            out.text
        );
        assert!(reg.is_empty());
    }

    #[test]
    fn graph_answers_become_files() {
        let text = "src/a.ts:12 (function, rank 0.0100)\nsrc/b/c.rs \u{2014} imports src/a.ts\nNo definition of `x` in the graph";
        assert_eq!(
            graph_places(text),
            vec![("src/a.ts".to_string(), 12), ("src/b/c.rs".to_string(), 0)]
        );
        assert_eq!(
            graph_label("graph_definition", &json!({"name": "pay"})),
            "Found where \u{201c}pay\u{201d} is defined"
        );
    }

    #[test]
    fn labels_read_plainly() {
        assert_eq!(
            label(READ_FILE, &json!({"repo": "acme/web", "path": "src/a.rs"})),
            "Read src/a.rs in web"
        );
        assert_eq!(
            label(OVERVIEW, &json!({"repo": "acme/web"})),
            "Looked over web"
        );
        assert!(label(SEARCH_CODE, &json!({"query": "refund"})).contains("refund"));
    }
}
