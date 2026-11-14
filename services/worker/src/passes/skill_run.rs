//! A team skill's real work, run on Coderhelm: its bundled script in the
//! code sandbox, and the files it makes kept as the run's outputs.
//!
//! - `run_skill_script` packs the files kept next to a skill (read from the
//!   skill store, the same text agents read) into the sandbox, runs one of
//!   them with the arguments the step gives (`{out}` stands for the output
//!   folder, `$CODERHELM_OUT`), and keeps what it wrote there as outputs.
//!   A script that reads GitHub names the repositories it reads: each must
//!   be one of the team's, all under one account, and it gets a token for
//!   only those, read-only, minted for this run, handed over through a
//!   one-time object (never the build's environment or log), revoked when
//!   the script ends. The token is taken out of everything that comes back
//!   (the log and every output) before anyone sees it, and GitHub's API is
//!   the only host added to the team's sandbox allowlist.
//! - `save_output` keeps a file the work makes (a narrative report) with the
//!   run's outputs; `read_output` reads one back (a table the script wrote).
//!
//! Outputs live under `common::outputs` and are listed and downloaded on the
//! run's page. At most [`MAX_RUNS`] script runs per pass.

use std::sync::atomic::{AtomicUsize, Ordering};

use common::host_steps::{READ_OUTPUT, RUN_SCRIPT, SAVE_OUTPUT};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::agent::llm::ToolDefinition;
use crate::clients::github::GitHubClient;
use crate::clients::sandbox::{SandboxClient, ScriptRun};
use crate::models::Config as WorkerConfig;
use crate::WorkerState;

/// Script runs one pass may start.
pub const MAX_RUNS: usize = 3;
/// Characters of a script's output or an output file handed back.
const MAX_BACK: usize = 12_000;
/// What a secret becomes wherever it showed up.
pub const REDACTED: &str = "[redacted token]";

/// Whether `name` is one of this module's tools.
pub fn is_tool(name: &str) -> bool {
    matches!(name, RUN_SCRIPT | SAVE_OUTPUT | READ_OUTPUT)
}

/// The capability catalogue as every run has it before a team's own
/// connections are known: the sandbox when this install has one.
pub fn standard_caps() -> Vec<common::host_steps::Capability> {
    common::host_steps::standard(
        std::env::var("SANDBOX_BUCKET_NAME").is_ok_and(|v| !v.is_empty())
            && std::env::var("SANDBOX_PROJECT_NAME").is_ok_and(|v| !v.is_empty()),
    )
}

/// Whether this install has the code sandbox.
pub fn sandbox_ready(c: &WorkerConfig) -> bool {
    !c.sandbox_bucket_name.is_empty() && !c.sandbox_project_name.is_empty()
}

pub fn tools(sandbox: bool) -> Vec<ToolDefinition> {
    let mut v = Vec::new();
    if sandbox {
        v.push(ToolDefinition {
            name: RUN_SCRIPT.into(),
            description: "Run a script kept with one of the team's skills (listed with it under \
                          'Files next to this skill') in the code sandbox, the way the skill runs \
                          it on a laptop. Write `{out}` in the arguments where the skill names its \
                          output folder: what the script writes there comes back as the run's \
                          outputs, with links. When the script reads GitHub, list the \
                          repositories it reads in `github_repos` (owner/name, the team's own): \
                          it gets a read-only token for only those as GH_TOKEN and GITHUB_TOKEN. \
                          No login step is needed. At most 15 minutes."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "The skill, e.g. skill:merge-rate" },
                    "script": { "type": "string", "description": "The file to run, as listed with the skill" },
                    "args": { "type": "array", "items": { "type": "string" } },
                    "github_repos": { "type": "array", "items": { "type": "string" }, "description": "owner/name of each repository the script reads" },
                    "reason": { "type": "string" }
                },
                "required": ["name", "script", "reason"]
            }),
        });
    }
    v.push(ToolDefinition {
        name: SAVE_OUTPUT.into(),
        description: "Keep a text file (a report, a table, JSON, HTML) with this run's outputs \
                      instead of a folder on someone's computer. Returns its link on the run's \
                      page. Not committed to the repository."
            .into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "e.g. report.md or tables/by_author.csv" },
                "content": { "type": "string" }
            },
            "required": ["name", "content"]
        }),
    });
    v.push(ToolDefinition {
        name: READ_OUTPUT.into(),
        description: "Read a file kept with this run's outputs (one a script wrote, or one \
                      saved earlier). Without a name, lists them."
            .into(),
        input_schema: json!({
            "type": "object",
            "properties": { "name": { "type": "string" } }
        }),
    });
    v
}

pub fn prompt_section(sandbox: bool) -> String {
    format!(
        "\n\n## Running a team skill's own work\n{}Files the work makes go to the run's outputs \
         with `save_output` (not the repository, unless the task is to commit them); read them \
         back with `read_output`, and give the person each output's link.\n",
        if sandbox {
            "A script kept with a team skill runs in the code sandbox with `run_skill_script`, \
             with a read-only GitHub token for the repositories it names; its output folder \
             comes back as the run's outputs. "
        } else {
            ""
        }
    )
}

/// The command that runs `script` (a path inside the skill's folder) with
/// `args`, every argument quoted for bash and `{out}` standing for the
/// output folder.
pub fn script_command(script: &str, args: &[String]) -> Result<String, String> {
    let ext = script.rsplit('.').next().unwrap_or("").to_lowercase();
    let runner = match ext.as_str() {
        "py" => "python3",
        "sh" | "bash" => "bash",
        "js" | "mjs" | "cjs" => "node",
        "rb" => "ruby",
        _ => {
            return Err(format!(
                "'{script}' isn't a script the sandbox runs: use a .py, .sh, .js or .rb file."
            ))
        }
    };
    let mut cmd = vec![runner.to_string(), quote(script)];
    for a in args {
        cmd.push(arg(a));
    }
    Ok(cmd.join(" "))
}

/// One argument quoted for bash, `{out}` left to the shell as the output
/// folder.
fn arg(a: &str) -> String {
    if !a.contains("{out}") {
        return quote(a);
    }
    a.split("{out}")
        .map(quote)
        .collect::<Vec<_>>()
        .join("\"$CODERHELM_OUT\"")
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// The repositories a script may read: each one of the team's, all under
/// one account (a token covers one installation). Returns the names.
pub fn token_repos(asked: &[String], team: &[String]) -> Result<Vec<String>, String> {
    let mut owner: Option<&str> = None;
    let mut names = Vec::new();
    for r in asked {
        let r = r.trim();
        let Some(t) = team.iter().find(|t| t.eq_ignore_ascii_case(r)) else {
            return Err(format!(
                "'{r}' isn't one of the team's repositories, so no token covers it. Use owner/name of a repository the team connected."
            ));
        };
        let (o, n) = t
            .split_once('/')
            .ok_or_else(|| format!("'{r}' isn't owner/name."))?;
        match owner {
            Some(prev) if !prev.eq_ignore_ascii_case(o) => return Err(
                "One script run reads repositories of one GitHub account; run it once per account."
                    .into(),
            ),
            _ => owner = Some(o),
        }
        if !names.iter().any(|x: &String| x == n) {
            names.push(n.to_string());
        }
    }
    Ok(names)
}

/// `text` with every copy of `secret` taken out.
pub fn redact(text: &str, secret: Option<&str>) -> String {
    match secret.filter(|s| s.len() >= 8) {
        Some(s) => text.replace(s, REDACTED),
        None => text.to_string(),
    }
}

/// The skill's files packed for the sandbox: one top folder, `skill/`,
/// holding each file at its path inside the skill's folder.
pub fn pack(files: &[(String, String)]) -> Result<Vec<u8>, String> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut tar = tar::Builder::new(gz);
    for (path, body) in files {
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(0o755);
        h.set_cksum();
        tar.append_data(&mut h, format!("skill/{path}"), body.as_bytes())
            .map_err(|e| e.to_string())?;
    }
    tar.into_inner()
        .and_then(|g| g.finish())
        .map_err(|e| e.to_string())
}

/// One run's outputs: kept under `common::outputs`, linked on its page.
pub struct Outputs<'a> {
    pub state: &'a WorkerState,
    pub team_id: &'a str,
    pub run_id: &'a str,
}

impl Outputs<'_> {
    fn link(&self, name: &str) -> String {
        common::outputs::page_link(
            common::public_urls::app_url(&self.state.config.stage),
            self.run_id,
            name,
        )
    }

    async fn names(&self) -> Vec<String> {
        let prefix = common::outputs::prefix(self.team_id, self.run_id);
        let mut out = Vec::new();
        let Ok(page) = self
            .state
            .s3
            .list_objects_v2()
            .bucket(&self.state.config.bucket_name)
            .prefix(&prefix)
            .max_keys(common::outputs::MAX_FILES as i32 + 1)
            .send()
            .await
        else {
            return out;
        };
        for o in page.contents() {
            if let Some(k) = o.key().and_then(|k| k.strip_prefix(&prefix)) {
                out.push(k.to_string());
            }
        }
        out
    }

    /// Keeps one file; returns its link, or why it wasn't kept.
    pub async fn save(&self, name: &str, content: &str) -> Result<String, String> {
        let name = common::outputs::clean_name(name)?;
        if content.len() > common::outputs::MAX_BYTES {
            return Err(format!(
                "'{name}' is over {} MB; keep a smaller file.",
                common::outputs::MAX_BYTES / (1024 * 1024)
            ));
        }
        let names = self.names().await;
        if !names.contains(&name) && names.len() >= common::outputs::MAX_FILES {
            return Err(format!(
                "This run already keeps {} outputs.",
                common::outputs::MAX_FILES
            ));
        }
        self.state
            .s3
            .put_object()
            .bucket(&self.state.config.bucket_name)
            .key(common::outputs::key(self.team_id, self.run_id, &name))
            .body(content.as_bytes().to_vec().into())
            .content_type(common::outputs::content_type(&name))
            .send()
            .await
            .map_err(|e| format!("The output couldn't be kept: {e}"))?;
        Ok(self.link(&name))
    }

    async fn read(&self, name: &str) -> Result<String, String> {
        let name = common::outputs::clean_name(name)?;
        let obj = self
            .state
            .s3
            .get_object()
            .bucket(&self.state.config.bucket_name)
            .key(common::outputs::key(self.team_id, self.run_id, &name))
            .send()
            .await
            .map_err(|_| format!("This run keeps no output called '{name}'."))?;
        let bytes = obj
            .body
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .into_bytes();
        Ok(String::from_utf8_lossy(&bytes).to_string())
    }
}

/// What `run_skill_script` needs from the pass running it.
pub struct SkillRun<'a> {
    pub outputs: Outputs<'a>,
    pub github: &'a GitHubClient,
    pub sandbox: Option<&'a SandboxClient<'a>>,
    pub skills: Option<&'a super::skills::TeamSkills>,
    /// The team's repositories, owner/name.
    pub team_repos: Vec<String>,
    pub deadline: Option<std::time::Instant>,
    /// Script runs this pass started, kept by the pass across calls.
    pub runs: &'a AtomicUsize,
}

impl SkillRun<'_> {
    /// Runs one of this module's tools; `None` when `name` isn't one.
    pub async fn handle(&self, name: &str, input: &Value) -> Option<String> {
        match name {
            SAVE_OUTPUT => Some(
                match self
                    .outputs
                    .save(
                        input["name"].as_str().unwrap_or(""),
                        input["content"].as_str().unwrap_or(""),
                    )
                    .await
                {
                    Ok(link) => format!("Kept with the run's outputs: {link}"),
                    Err(e) => e,
                },
            ),
            READ_OUTPUT => Some(
                match input["name"].as_str().filter(|n| !n.trim().is_empty()) {
                    Some(n) => match self.outputs.read(n).await {
                        Ok(t) => common::truncate_str(&t, MAX_BACK).to_string(),
                        Err(e) => e,
                    },
                    None => {
                        let names = self.outputs.names().await;
                        if names.is_empty() {
                            "This run keeps no outputs yet.".into()
                        } else {
                            names
                                .iter()
                                .map(|n| format!("- {n}: {}", self.outputs.link(n)))
                                .collect::<Vec<_>>()
                                .join("\n")
                        }
                    }
                },
            ),
            RUN_SCRIPT => Some(self.run(input).await),
            _ => None,
        }
    }

    async fn run(&self, input: &Value) -> String {
        let Some(sandbox) = self.sandbox.filter(|s| s.is_enabled()) else {
            return "The code sandbox isn't set up for this install, so the script can't run. Compute the same figures from what github_history and the data sources return.".into();
        };
        let Some(skills) = self.skills else {
            return "No team skills are loaded for this work.".into();
        };
        let asked = input["name"].as_str().unwrap_or("");
        let Some(m) = skills.item(asked) else {
            return format!("There's no switched-on team skill called '{asked}'.");
        };
        let script = input["script"].as_str().unwrap_or("").trim();
        let Some(script_path) =
            common::skills::file_in_skill(&m.path, script).filter(|p| m.files.contains(p))
        else {
            return format!(
                "'{script}' isn't one of the files kept with {asked}. Files: {}",
                m.files.join(", ")
            );
        };
        if self.runs.fetch_add(1, Ordering::SeqCst) >= MAX_RUNS {
            return format!("All {MAX_RUNS} script runs of this pass are used.");
        }
        let args: Vec<String> = input["args"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let dir = m.path.strip_suffix("/SKILL.md").unwrap_or("");
        let rel = |p: &str| p.strip_prefix(&format!("{dir}/")).unwrap_or(p).to_string();
        let cmd = match script_command(&rel(&script_path), &args) {
            Ok(c) => c,
            Err(e) => return e,
        };
        let repos: Vec<String> = input["github_repos"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let names = match token_repos(&repos, &self.team_repos) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let st = common::skills::SkillStore {
            dynamo: &self.outputs.state.dynamo,
            table: &self.outputs.state.config.settings_table_name,
            team: self.outputs.team_id,
        };
        let mut files = Vec::new();
        for f in &m.files {
            match st.file(&m.repo, f).await {
                Ok(Some(body)) => files.push((rel(f), body)),
                Ok(None) => {}
                Err(e) => {
                    warn!(error = %e, "Could not read a skill file for its script run");
                    return "The skill's files couldn't be read right now.".into();
                }
            }
        }
        let source = match pack(&files) {
            Ok(b) => b,
            Err(e) => return format!("The skill's files couldn't be packed: {e}"),
        };
        let token = if names.is_empty() {
            None
        } else {
            match self.github.read_only_token(&names).await {
                Ok(t) => Some(t),
                Err(e) => {
                    return format!(
                        "No read-only token could be made for the script ({e}). Read the same data with github_history instead."
                    )
                }
            }
        };
        let key = format!("{}-{}", self.outputs.run_id, uuid::Uuid::new_v4().simple());
        let started = std::time::Instant::now();
        let r = sandbox
            .run_script(
                ScriptRun {
                    key: &key,
                    source,
                    cmd: &cmd,
                    secret: token.as_deref(),
                    hosts: if token.is_some() {
                        vec!["api.github.com".to_string()]
                    } else {
                        Vec::new()
                    },
                },
                self.deadline,
            )
            .await;
        if let Some(t) = &token {
            self.github.revoke_token(t).await;
        }
        let out = match r {
            Ok(o) => o,
            Err(e) => {
                warn!(error = %redact(&e.to_string(), token.as_deref()), "Script run failed");
                return "The sandbox couldn't run the script right now.".into();
            }
        };
        info!(
            team_id = %self.outputs.team_id,
            skill = %m.name,
            ran = out.ran,
            exit = ?out.exit_code,
            files = out.files.len(),
            secs = started.elapsed().as_secs(),
            "Ran a team skill's script"
        );
        let mut kept = Vec::new();
        for (name, body) in &out.files {
            let body = redact(body, token.as_deref());
            match self.outputs.save(name, &body).await {
                Ok(link) => kept.push(format!("- {name}: {link}")),
                Err(e) => kept.push(format!("- {name}: not kept ({e})")),
            }
        }
        for d in &out.dropped {
            kept.push(format!("- {d}: not kept"));
        }
        let output = redact(&out.output, token.as_deref());
        format!(
            "{}\n\nOutputs:\n{}\n\nWhat it printed:\n{}",
            match (out.ran, out.exit_code) {
                (true, Some(0)) => "The script ran and exited 0.".to_string(),
                (true, Some(c)) => format!("The script ran and exited {c}."),
                _ => "The script didn't run to the end.".to_string(),
            },
            if kept.is_empty() {
                "- none".to_string()
            } else {
                kept.join("\n")
            },
            common::truncate_str(&output, MAX_BACK)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_script_is_run_with_quoted_arguments_and_its_output_folder() {
        let c = script_command(
            "report_rate.py",
            &[
                "--repos".into(),
                "acme/web,acme/api".into(),
                "--out".into(),
                "{out}".into(),
                "it's".into(),
            ],
        )
        .unwrap();
        assert_eq!(
            c,
            "python3 'report_rate.py' '--repos' 'acme/web,acme/api' '--out' ''\"$CODERHELM_OUT\"'' 'it'\\''s'"
        );
        assert!(script_command("tool.exe", &[]).is_err());
        assert!(script_command("run.sh", &["$(rm -rf /)".into()])
            .unwrap()
            .ends_with("'$(rm -rf /)'"));
    }

    #[test]
    fn a_token_covers_only_the_teams_repositories_of_one_account() {
        let team = vec![
            "acme/web".to_string(),
            "acme/api".to_string(),
            "other/x".to_string(),
        ];
        assert_eq!(
            token_repos(&["Acme/Web".into(), "acme/api".into()], &team).unwrap(),
            ["web", "api"]
        );
        assert!(token_repos(&["acme/secret".into()], &team).is_err());
        assert!(token_repos(&["acme/web".into(), "other/x".into()], &team).is_err());
        assert!(token_repos(&[], &team).unwrap().is_empty());
    }

    #[test]
    fn the_token_is_taken_out_of_everything_that_comes_back() {
        let t = "ghs_abcdefghijklmnop";
        let log = format!("auth: {t}\nfetched 20 pull requests\nAuthorization: bearer {t}");
        let r = redact(&log, Some(t));
        assert!(!r.contains(t));
        assert_eq!(r.matches(REDACTED).count(), 2);
        assert_eq!(redact("nothing", None), "nothing");
    }

    #[test]
    fn the_token_request_asks_only_for_reads_of_the_named_repositories() {
        let body = crate::clients::github::script_token_request(&["web".into()]);
        assert_eq!(body["repositories"], json!(["web"]));
        let perms = body["permissions"].as_object().unwrap();
        assert!(perms.values().all(|v| v == "read"), "{perms:?}");
        assert!(perms.contains_key("pull_requests") && perms.contains_key("contents"));
    }

    #[test]
    fn the_skills_files_are_packed_under_one_folder() {
        let bytes = pack(&[
            ("report_rate.py".into(), "print(1)".into()),
            ("lib/util.py".into(), "x = 1".into()),
        ])
        .unwrap();
        let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(&bytes[..]));
        let paths: Vec<String> = ar
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(paths, ["skill/report_rate.py", "skill/lib/util.py"]);
    }

    #[test]
    fn the_run_tools_need_the_sandbox_and_outputs_never_do() {
        let with: Vec<String> = tools(true).into_iter().map(|t| t.name).collect();
        assert_eq!(with, [RUN_SCRIPT, SAVE_OUTPUT, READ_OUTPUT]);
        let without: Vec<String> = tools(false).into_iter().map(|t| t.name).collect();
        assert_eq!(without, [SAVE_OUTPUT, READ_OUTPUT]);
        assert!(prompt_section(true).contains(RUN_SCRIPT));
        assert!(!prompt_section(false).contains(RUN_SCRIPT));
    }
}
