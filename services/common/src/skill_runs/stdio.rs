//! MCP servers that speak stdio, run inside a skill run's own sandbox.
//!
//! A skill written for a laptop often calls tools of a local MCP server (a
//! team's own CLI started with `node bin/...-mcp.js`). Such a server can't
//! be reached over the network, so a skill run starts it in the code
//! sandbox, next to a small relay (`mcp_relay.py`) that passes one request
//! at a time between the worker and the server's stdin and stdout.
//!
//! - **Declared** by its source and its command: a team repository pinned
//!   at a full commit, or an npm package from CoderHelm's MCP catalog pinned
//!   at an exact version. Nothing else is installed: the same allow-list
//!   the catalog's servers follow.
//! - **AWS** reads go through the team's read-only role in a connected
//!   account (CoderHelm's reader role): the worker takes the role on and
//!   hands short-lived keys to the relay through a one-time object; the
//!   relay passes them only into the server's environment and takes them
//!   out of every answer. Never a person's sign-in.
//! - **Found** in the team's repositories when a skill names tools no
//!   connected server offers: a `package.json` whose `bin` names an MCP
//!   server ([`server_from_package`]).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The relay the sandbox runs (and the tests run locally).
pub const RELAY: &str = include_str!("mcp_relay.py");
/// Request slots one session holds: one per list or call.
pub const SLOTS: usize = 400;
/// A session with no request for this long ends itself.
pub const IDLE_SECS: u64 = 900;
/// The source prefix of a sandbox server in a tool grant.
pub const SOURCE_PREFIX: &str = "sandbox:";

/// Where a server's code comes from, pinned.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StdioSource {
    /// A team repository (`owner/name`) at a full commit.
    Git { repo: String, commit: String },
    /// A package from CoderHelm's MCP catalog at an exact version.
    Npm { package: String, version: String },
}

/// One stdio MCP server a schedule runs in its sandbox.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StdioServer {
    /// Its id in tool grants (`sandbox:<id>`): the repository's or the
    /// package's name.
    pub id: String,
    pub source: StdioSource,
    /// How it starts, from its own folder: `["node", "bin/x-mcp.js"]`.
    pub command: Vec<String>,
    /// The connected AWS account whose read-only role it reads with; empty
    /// when it reads no AWS.
    #[serde(default)]
    pub aws_account: String,
    /// Settings it reads from its environment (never keys).
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl StdioServer {
    pub fn source_id(&self) -> String {
        format!("{SOURCE_PREFIX}{}", self.id)
    }

    /// How it installs, from its own folder, by the lockfile its
    /// repository keeps (`root_files`: the names at its root), so what
    /// installs is what the commit pinned.
    pub fn install(&self, root_files: &[&str]) -> Vec<String> {
        let v = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        match &self.source {
            StdioSource::Npm { package, version } => {
                let mut c = v(&[
                    "npm",
                    "install",
                    "--no-audit",
                    "--no-fund",
                    "--ignore-scripts",
                ]);
                c.push(format!("{package}@{version}"));
                c
            }
            StdioSource::Git { .. } if self.command.first().is_some_and(|c| c == "node") => {
                let has = |f: &str| root_files.contains(&f);
                if has("package-lock.json") {
                    v(&["npm", "ci", "--omit=dev", "--no-audit", "--no-fund"])
                } else if has("pnpm-lock.yaml") {
                    v(&[
                        "npx",
                        "--yes",
                        "pnpm@9",
                        "install",
                        "--prod",
                        "--frozen-lockfile",
                    ])
                } else if has("yarn.lock") {
                    v(&[
                        "npx",
                        "--yes",
                        "yarn@1",
                        "install",
                        "--production",
                        "--frozen-lockfile",
                    ])
                } else {
                    v(&["npm", "install", "--omit=dev", "--no-audit", "--no-fund"])
                }
            }
            StdioSource::Git { .. } => Vec::new(),
        }
    }

    /// A version key for its tool list: what it runs, exactly.
    pub fn version_key(&self) -> String {
        match &self.source {
            StdioSource::Git { repo, commit } => format!("git:{repo}@{commit}"),
            StdioSource::Npm { package, version } => format!("npm:{package}@{version}"),
        }
    }
}

/// The npm packages of CoderHelm's MCP catalog: the only packages a sandbox
/// server installs.
pub fn catalog_packages() -> Vec<&'static str> {
    crate::mcp_catalog::CATALOG
        .iter()
        .map(|d| d.npx_package)
        .filter(|p| !p.is_empty())
        .collect()
}

fn is_sha(s: &str) -> bool {
    s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit())
}

fn exact_version(v: &str) -> bool {
    let parts: Vec<&str> = v.split(['.', '-', '+']).collect();
    parts.len() >= 3
        && parts[..3]
            .iter()
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
}

fn safe_path(p: &str) -> bool {
    !p.is_empty()
        && !p.starts_with('/')
        && !p.split('/').any(|s| s == "..")
        && p.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '-' | '_' | '@'))
}

/// Checks a server against the allow-list: a team repository at a full
/// commit (`team_repos`, owner/name), or a catalog package (`catalog`) at an
/// exact version; a command CoderHelm knows how to start; no keys in its
/// settings.
pub fn validate(s: &StdioServer, team_repos: &[String], catalog: &[&str]) -> Result<(), String> {
    if s.id.is_empty()
        || s.id.len() > 60
        || !s
            .id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(format!("'{}' isn't a server id.", s.id));
    }
    match &s.source {
        StdioSource::Git { repo, commit } => {
            if !team_repos.iter().any(|r| r.eq_ignore_ascii_case(repo)) {
                return Err(format!(
                    "{repo} isn't one of the team's repositories, so its server can't run."
                ));
            }
            if !is_sha(commit) {
                return Err(format!("Pin {repo} at a full commit, not '{commit}'."));
            }
        }
        StdioSource::Npm { package, version } => {
            if !catalog.contains(&package.as_str()) {
                return Err(format!(
                    "{package} isn't a package in CoderHelm's MCP catalog."
                ));
            }
            if !exact_version(version) {
                return Err(format!(
                    "Pin {package} at an exact version, not '{version}'."
                ));
            }
        }
    }
    let Some((runner, rest)) = s.command.split_first() else {
        return Err("Name the command that starts the server.".into());
    };
    match runner.as_str() {
        "node" | "python3" => {
            if rest.is_empty() || !safe_path(&rest[0]) {
                return Err("The server starts from a file in its own folder.".into());
            }
        }
        "npx" => {
            let StdioSource::Npm { package, .. } = &s.source else {
                return Err("npx starts only a catalog package.".into());
            };
            if !rest
                .iter()
                .any(|a| a == package || a.starts_with(&format!("{package}@")))
            {
                return Err("npx starts only the declared package.".into());
            }
        }
        _ => {
            return Err(format!(
                "'{runner}' isn't a command a sandbox server starts with."
            ))
        }
    }
    for k in s.env.keys() {
        let u = k.to_ascii_uppercase();
        if u.starts_with("AWS_")
            || u.starts_with("SANDBOX_")
            || u.contains("KEY")
            || u.contains("TOKEN")
            || u.contains("SECRET")
            || u.contains("PASSWORD")
        {
            return Err(format!("{k} can't be set on a sandbox server."));
        }
    }
    Ok(())
}

/// A stdio MCP server a repository's `package.json` declares: a `bin` entry
/// whose name says it is an MCP server (`acme-mcp`, `mcp-server`). Its AWS
/// use is read from its dependencies.
pub fn server_from_package(
    repo: &str,
    commit: &str,
    package_json: &str,
) -> Option<(StdioServer, bool)> {
    let v: Value = serde_json::from_str(package_json).ok()?;
    let bins: Vec<(String, String)> = match &v["bin"] {
        Value::Object(m) => m
            .iter()
            .filter_map(|(k, p)| Some((k.clone(), p.as_str()?.to_string())))
            .collect(),
        Value::String(p) => vec![(v["name"].as_str().unwrap_or("").to_string(), p.clone())],
        _ => Vec::new(),
    };
    let (_, path) = bins.into_iter().find(|(k, p)| {
        k.to_ascii_lowercase().contains("mcp") && safe_path(p.trim_start_matches("./"))
    })?;
    let path = path.trim_start_matches("./").to_string();
    let deps = |k: &str| {
        v[k].as_object()
            .map(|m| m.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    };
    let all: Vec<String> = deps("dependencies")
        .into_iter()
        .chain(deps("optionalDependencies"))
        .collect();
    let aws = all
        .iter()
        .any(|d| d.starts_with("@aws-sdk/") || d == "aws-sdk");
    let id = repo.rsplit('/').next().unwrap_or(repo).to_string();
    Some((
        StdioServer {
            id,
            source: StdioSource::Git {
                repo: repo.to_string(),
                commit: commit.to_string(),
            },
            command: vec!["node".into(), path],
            aws_account: String::new(),
            env: BTreeMap::new(),
        },
        aws,
    ))
}

/// The manifest the relay reads: where each request and answer goes, how
/// the server installs and starts.
pub fn manifest(
    s: &StdioServer,
    install: &[String],
    slots: &[(String, String)],
    secret: Option<&str>,
) -> Value {
    json!({
        "cwd": "server",
        "install": install,
        "command": s.command,
        "env": s.env,
        "idle_secs": IDLE_SECS,
        "secret": secret,
        "slots": slots.iter().map(|(req, resp)| json!({"req": req, "resp": resp})).collect::<Vec<_>>(),
    })
}

/// A running session's handle, kept with the run across its steps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Session {
    /// The session's key (its objects' folder).
    pub key: String,
    /// The build (or local process) that runs it.
    pub build: String,
    /// The next request slot.
    pub next: usize,
}

/// Starts and reaches sandbox servers.
#[async_trait::async_trait]
pub trait StdioHost: Send + Sync {
    /// Starts `s` for a run; `aws` carries the role's short-lived keys.
    async fn start(
        &self,
        team: &str,
        key: &str,
        s: &StdioServer,
        aws: Option<&BTreeMap<String, String>>,
        deadline: std::time::Instant,
    ) -> Result<Session, String>;
    /// One request (`{"op": "list"}` or `{"op": "call", ...}`) and its
    /// answer's result, or the server's error.
    async fn request(
        &self,
        session: &mut Session,
        req: &Value,
        deadline: std::time::Instant,
    ) -> Result<Value, String>;
    async fn stop(&self, session: &Session);
}

/// AWS keys for a sandbox server: the team's read-only role in `account`.
#[async_trait::async_trait]
pub trait RoleKeys: Send + Sync {
    async fn keys(&self, team: &str, account: &str) -> Result<BTreeMap<String, String>, String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(repo: &str, commit: &str, cmd: &[&str]) -> StdioServer {
        StdioServer {
            id: "traffic-mcp".into(),
            source: StdioSource::Git {
                repo: repo.into(),
                commit: commit.into(),
            },
            command: cmd.iter().map(|s| s.to_string()).collect(),
            aws_account: String::new(),
            env: BTreeMap::new(),
        }
    }

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn only_pinned_team_repositories_and_catalog_packages_run() {
        let team = vec!["acme/traffic-mcp".to_string()];
        assert!(validate(
            &git("acme/traffic-mcp", SHA, &["node", "bin/mcp.js"]),
            &team,
            &[]
        )
        .is_ok());
        for (s, says) in [
            (
                git("other/x", SHA, &["node", "a.js"]),
                "team's repositories",
            ),
            (
                git("acme/traffic-mcp", "main", &["node", "a.js"]),
                "full commit",
            ),
            (
                git("acme/traffic-mcp", SHA, &["bash", "-c", "curl x"]),
                "isn't a command",
            ),
            (
                git("acme/traffic-mcp", SHA, &["node", "../../etc/x"]),
                "own folder",
            ),
            (
                git("acme/traffic-mcp", SHA, &["npx", "evil"]),
                "catalog package",
            ),
        ] {
            let e = validate(&s, &team, &[]).unwrap_err();
            assert!(e.contains(says), "{says}: {e}");
        }
        let mut keyed = git("acme/traffic-mcp", SHA, &["node", "a.js"]);
        keyed.env.insert("AWS_SECRET_ACCESS_KEY".into(), "x".into());
        assert!(validate(&keyed, &team, &[]).is_err());
        let npm = StdioServer {
            id: "sentry".into(),
            source: StdioSource::Npm {
                package: "@sentry/mcp-server".into(),
                version: "1.2.3".into(),
            },
            command: vec!["npx".into(), "@sentry/mcp-server@1.2.3".into()],
            aws_account: String::new(),
            env: BTreeMap::new(),
        };
        assert!(validate(&npm, &team, &["@sentry/mcp-server"]).is_ok());
        assert!(validate(&npm, &team, &[]).is_err());
        let mut loose = npm.clone();
        loose.source = StdioSource::Npm {
            package: "@sentry/mcp-server".into(),
            version: "^1".into(),
        };
        assert!(validate(&loose, &team, &["@sentry/mcp-server"])
            .unwrap_err()
            .contains("exact version"));
    }

    #[test]
    fn a_repository_declares_its_mcp_server_in_package_json() {
        let pkg = r#"{"name":"acme-cli","bin":{"acme":"bin/acme.js","acme-mcp":"./bin/acme-mcp.js"},
            "dependencies":{"@aws-sdk/client-athena":"^3","@modelcontextprotocol/sdk":"^1"}}"#;
        let (s, aws) = server_from_package("acme/acme-cli", SHA, pkg).unwrap();
        assert_eq!(s.id, "acme-cli");
        assert_eq!(s.command, ["node", "bin/acme-mcp.js"]);
        assert!(aws);
        assert_eq!(
            s.install(&["package-lock.json"])[..2],
            ["npm".to_string(), "ci".to_string()]
        );
        assert_eq!(s.install(&["pnpm-lock.yaml"])[2], "pnpm@9");
        assert_eq!(
            s.install(&[])[..2],
            ["npm".to_string(), "install".to_string()]
        );
        assert!(server_from_package("acme/web", SHA, r#"{"bin":{"web":"x.js"}}"#).is_none());
        assert!(server_from_package("acme/web", SHA, "not json").is_none());
    }
}
