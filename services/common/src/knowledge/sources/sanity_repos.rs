//! The Sanity projects a team's own repositories name.
//!
//! Sanity's project list (`list_projects` on its MCP server, `GET /projects`
//! with a token) shows only projects the account is a direct member of. A
//! project reached through an organization role, or one a person was never
//! added to by name, isn't in it, even though the sign-in reads it fine. The
//! repositories that use a project name it in their Studio config
//! (`projectId` in `sanity.config.*` or `sanity.cli.*`) or in an env file
//! (`SANITY_STUDIO_PROJECT_ID=...`), so the picker offers those too, each
//! checked by listing its datasets before it is shown.

use serde_json::Value;

use super::connector::{enc, send, send_json, ConnectorError};
use super::sanity::valid_project;
use crate::mcp_remote::http::{Http, HttpRequest, Limit};

const API: &str = "https://api.github.com";
/// Repositories read per scan, most recently pushed first.
pub const MAX_REPOS: usize = 12;
/// Config files read per repository.
const MAX_FILES: usize = 6;

/// Is `path` a file that can name a Sanity project?
pub fn is_config_path(path: &str) -> bool {
    if path
        .split('/')
        .any(|p| p == "node_modules" || p == "dist" || p == ".next")
    {
        return false;
    }
    let base = path.rsplit('/').next().unwrap_or(path);
    base.starts_with("sanity.config.")
        || base.starts_with("sanity.cli.")
        || (base.starts_with(".env") && !base.contains("prod"))
}

fn unquote(s: &str) -> &str {
    s.trim()
        .trim_end_matches([',', ';'])
        .trim()
        .trim_matches(|c| c == '\'' || c == '"' || c == '`')
}

fn is_project_key(k: &str) -> bool {
    let u = k.to_ascii_uppercase();
    u.contains("SANITY") && u.contains("PROJECT") && u.ends_with("ID")
}

/// The project ids `files` (path, text) name: env-file values first, then
/// `projectId` in config files, with `process.env.X` (or
/// `import.meta.env.X`) resolved through the env files. No duplicates.
pub fn project_ids(files: &[(String, String)]) -> Vec<String> {
    let mut env: Vec<(String, String)> = Vec::new();
    let mut out: Vec<String> = Vec::new();
    let push = |id: &str, out: &mut Vec<String>| {
        if valid_project(id) && !out.iter().any(|x| x == id) {
            out.push(id.to_string());
        }
    };
    for (path, text) in files {
        let base = path.rsplit('/').next().unwrap_or(path);
        if !base.starts_with(".env") {
            continue;
        }
        for line in text.lines() {
            let l = line.trim().trim_start_matches("export ").trim();
            if l.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = l.split_once('=') {
                let (k, v) = (k.trim(), unquote(v));
                env.push((k.to_string(), v.to_string()));
                if is_project_key(k) {
                    push(v, &mut out);
                }
            }
        }
    }
    for (path, text) in files {
        let base = path.rsplit('/').next().unwrap_or(path);
        if base.starts_with(".env") {
            continue;
        }
        for line in text.lines() {
            let l = line.trim();
            let Some((k, v)) = l.split_once(':') else {
                continue;
            };
            if k.trim().trim_matches(|c| c == '\'' || c == '"') != "projectId" {
                continue;
            }
            let raw = unquote(v);
            let v = match raw.rsplit_once("env.") {
                Some((_, var)) => {
                    let var = var
                        .trim_start_matches(['[', '\'', '"'])
                        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                        .next()
                        .unwrap_or("");
                    match env.iter().find(|(k, _)| k == var) {
                        Some((_, v)) => v.as_str(),
                        None => continue,
                    }
                }
                None => raw,
            };
            push(v, &mut out);
        }
    }
    out
}

fn get(url: &str, auth: &str, accept: &str, limit: Limit) -> HttpRequest {
    let mut r = HttpRequest::get(url)
        .header("Authorization", auth)
        .header("X-GitHub-Api-Version", "2022-11-28");
    r.headers.retain(|(k, _)| k != "Accept");
    r.headers.push(("Accept".into(), accept.into()));
    r.limit = limit;
    r
}

/// Every Sanity project id the installation's repositories name, each with
/// the repository that names it (`owner/repo`). `auth` is `Bearer <token>`
/// of the team's GitHub App installation. Repositories that can't be read
/// are skipped; only listing them failing is an error.
pub async fn scan(http: &dyn Http, auth: &str) -> Result<Vec<(String, String)>, ConnectorError> {
    let v = send_json(
        http,
        get(
            &format!("{API}/installation/repositories?per_page=100"),
            auth,
            "application/vnd.github+json",
            Limit::Mcp,
        ),
        "GitHub",
    )
    .await?;
    let mut repos: Vec<(String, String, String)> = v["repositories"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|r| r["archived"] != true)
        .filter_map(|r| {
            Some((
                r["full_name"].as_str()?.to_string(),
                r["default_branch"].as_str().unwrap_or("main").to_string(),
                r["pushed_at"].as_str().unwrap_or("").to_string(),
            ))
        })
        .collect();
    repos.sort_by(|a, b| b.2.cmp(&a.2));
    let mut out: Vec<(String, String)> = Vec::new();
    for (repo, branch, _) in repos.into_iter().take(MAX_REPOS) {
        let Ok(tree) = send_json(
            http,
            get(
                &format!("{API}/repos/{repo}/git/trees/{}?recursive=1", enc(&branch)),
                auth,
                "application/vnd.github+json",
                Limit::Mcp,
            ),
            "GitHub",
        )
        .await
        else {
            continue;
        };
        let paths: Vec<String> = tree["tree"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|e| e["type"] == "blob")
            .filter_map(|e| e["path"].as_str())
            .filter(|p| is_config_path(p))
            .take(MAX_FILES)
            .map(str::to_string)
            .collect();
        if !paths.iter().any(|p| {
            let b = p.rsplit('/').next().unwrap_or(p);
            b.starts_with("sanity.")
        }) {
            // Env files alone don't make a Sanity repository.
            continue;
        }
        let mut files: Vec<(String, String)> = Vec::new();
        for p in paths {
            let url = format!(
                "{API}/repos/{repo}/contents/{}?ref={}",
                p.split('/').map(enc).collect::<Vec<_>>().join("/"),
                enc(&branch)
            );
            if let Ok(r) = send(
                http,
                get(
                    &url,
                    auth,
                    "application/vnd.github.raw+json",
                    Limit::Document,
                ),
                "GitHub",
            )
            .await
            {
                files.push((p, String::from_utf8_lossy(&r.body).into_owned()));
            }
        }
        for id in project_ids(&files) {
            if !out.iter().any(|(i, _)| *i == id) {
                out.push((id, repo.clone()));
            }
        }
    }
    Ok(out)
}

/// The cached scan's shape in the settings table.
pub fn to_json(found: &[(String, String)]) -> String {
    Value::Array(
        found
            .iter()
            .map(|(id, repo)| serde_json::json!({"id": id, "repo": repo}))
            .collect(),
    )
    .to_string()
}

pub fn from_json(s: &str) -> Vec<(String, String)> {
    serde_json::from_str::<Value>(s)
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|m| {
            let id = m["id"].as_str()?;
            valid_project(id)
                .then(|| (id.to_string(), m["repo"].as_str().unwrap_or("").to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp_remote::http::fake::{json as fjson, resp, Fake};
    use serde_json::json;

    fn f(p: &str, t: &str) -> (String, String) {
        (p.to_string(), t.to_string())
    }

    #[test]
    fn a_literal_project_id_in_the_studio_config_is_read() {
        let files = [f(
            "apps/studio/sanity.config.ts",
            "export default defineConfig({\n  name: 'default',\n  projectId: 'abc123',\n  dataset: 'production',\n})",
        )];
        assert_eq!(project_ids(&files), ["abc123"]);
    }

    #[test]
    fn a_project_id_from_the_environment_is_resolved_through_env_files() {
        let files = [
            f(
                "apps/studio/sanity.cli.ts",
                "export default defineCliConfig({api: {projectId: process.env.SANITY_STUDIO_PROJECT_ID!, dataset}})",
            ),
            f(
                "apps/web/sanity.config.ts",
                "  projectId: import.meta.env['PUBLIC_ID'],",
            ),
            f(".env.example", "# comment\nSANITY_STUDIO_PROJECT_ID=\"9xy8zt7q\"\nPUBLIC_ID=9xy8zt7q\n"),
        ];
        assert_eq!(
            project_ids(&files),
            ["9xy8zt7q"],
            "an id that starts with a digit counts, and is named once"
        );
    }

    #[test]
    fn production_env_files_and_build_output_are_not_read() {
        assert!(!is_config_path(".env.production"));
        assert!(!is_config_path("node_modules/sanity/sanity.config.js"));
        assert!(is_config_path("apps/studio/.env.staging"));
        assert!(is_config_path("sanity.cli.ts"));
        assert!(!is_config_path("src/config.ts"));
    }

    #[test]
    fn an_unresolved_or_malformed_id_is_left_out() {
        let files = [f(
            "sanity.config.ts",
            "projectId: process.env.MISSING,\nprojectId: 'Not Valid!',",
        )];
        assert!(project_ids(&files).is_empty());
    }

    #[tokio::test]
    async fn the_scan_reads_only_repositories_with_a_studio_config() {
        let http = Fake::default();
        http.with(|r| {
            r.url.contains("/installation/repositories").then(|| {
                fjson(
                    200,
                    json!({"repositories": [
                        {"full_name": "acme/site", "default_branch": "main", "pushed_at": "2026-01-02T00:00:00Z"},
                        {"full_name": "acme/api", "default_branch": "main", "pushed_at": "2026-01-03T00:00:00Z"},
                        {"full_name": "acme/old", "default_branch": "main", "pushed_at": "2026-01-04T00:00:00Z", "archived": true}
                    ]}),
                )
            })
        });
        http.with(|r| {
            r.url.contains("/repos/acme/site/git/trees/main").then(|| {
                fjson(
                    200,
                    json!({"tree": [
                        {"type": "blob", "path": "apps/studio/sanity.config.ts"},
                        {"type": "blob", "path": "apps/studio/.env.example"},
                        {"type": "blob", "path": "README.md"}
                    ]}),
                )
            })
        });
        http.with(|r| {
            r.url.contains("/repos/acme/api/git/trees/main").then(|| {
                fjson(
                    200,
                    json!({"tree": [{"type": "blob", "path": ".env.example"}]}),
                )
            })
        });
        http.with(|r| {
            r.url
                .contains("/repos/acme/site/contents/apps/studio/sanity.config.ts")
                .then(|| resp(200, &[], "export default defineConfig({projectId: process.env.SANITY_STUDIO_PROJECT_ID})"))
        });
        http.with(|r| {
            r.url
                .contains("/repos/acme/site/contents/apps/studio/.env.example")
                .then(|| resp(200, &[], "SANITY_STUDIO_PROJECT_ID=4bc5de6f\n"))
        });
        let found = scan(&http, "Bearer t").await.unwrap();
        assert_eq!(found, [("4bc5de6f".to_string(), "acme/site".to_string())]);
        assert!(
            !http
                .sent()
                .iter()
                .any(|r| r.url.contains("acme/api/contents")),
            "a repository with only env files isn't read"
        );
        assert!(!http.sent().iter().any(|r| r.url.contains("acme/old")));
        let round = from_json(&to_json(&found));
        assert_eq!(round, found);
    }
}
