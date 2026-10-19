//! `github_history`: a repository's history through the team's GitHub App,
//! with no setup: commits (by time, path, branch or author), commits between
//! two refs, blame for a file's lines, pull requests (merged, by label, by
//! base), one pull request with its commits and reviews, releases, tags,
//! deployments and Actions runs.
//!
//! Reads only: every request is a GET built here from typed inputs (blame is
//! one GraphQL query, checked as read-only before it is sent), to a repository
//! the team connected. Each call is written to the team's data-source
//! activity log, and one agent pass may make at most [`MAX_CALLS`].

use std::sync::atomic::{AtomicUsize, Ordering};

use common::live::store::{self, Activity, RunRef};
use serde_json::{json, Value};
use tracing::warn;

use crate::agent::llm::ToolDefinition;
use crate::clients::github::GitHubClient;
use crate::WorkerState;

pub const TOOL: &str = "github_history";
/// Calls one agent pass may make.
pub const MAX_CALLS: usize = 20;
const MAX_RESULT_CHARS: usize = 12_000;
const SOURCE_ID: &str = "github-history";
const SOURCE_NAME: &str = "GitHub history";

pub const KINDS: &[&str] = &[
    "commits",
    "compare",
    "blame",
    "pull_requests",
    "pull_request",
    "releases",
    "tags",
    "deployments",
    "workflow_runs",
];

pub fn tool() -> ToolDefinition {
    ToolDefinition {
        name: TOOL.to_string(),
        description: format!(
            "Read the repository's history on GitHub (read-only, no setup). Pick `kind`:\n\
             - commits: recent commits, like `git log` — filter by `since`/`until` (2h, 3d or a time like 2026-01-31T14:00:00Z), `path`, `ref` (branch, tag or sha) and `author`.\n\
             - compare: commits between two refs (`base`, `head`), like `git log base..head` (what a deploy shipped).\n\
             - blame: who last changed each line of a file (`path`, optional `start_line`/`end_line`, `ref`), like `git blame`.\n\
             - pull_requests: pull requests — `state` open|closed|merged|all (default merged), `base`, `label`, `since`.\n\
             - pull_request: one pull request (`number`) with its merge commit, labels, commits and reviews.\n\
             - releases: GitHub releases (or one by `tag`).\n\
             - tags: tags, newest first.\n\
             - deployments: GitHub deployments with their latest status (optional `environment`), and the repo's environments.\n\
             - workflow_runs: GitHub Actions runs — filter by `workflow` (file name like deploy.yml), `branch`, `status`, `event`, `since`.\n\
             `repo` (owner/name) defaults to the repository being worked on; only the team's connected repositories can be read. \
             `limit` defaults to 30 (at most 100). At most {MAX_CALLS} calls per task."
        ),
        input_schema: json!({
            "type": "object",
            "properties": {
                "kind": { "type": "string", "enum": KINDS },
                "repo": { "type": "string", "description": "owner/name; defaults to the current repository" },
                "since": { "type": "string" },
                "until": { "type": "string" },
                "path": { "type": "string" },
                "ref": { "type": "string" },
                "author": { "type": "string" },
                "base": { "type": "string" },
                "head": { "type": "string" },
                "start_line": { "type": "integer" },
                "end_line": { "type": "integer" },
                "state": { "type": "string", "enum": ["open", "closed", "merged", "all"] },
                "label": { "type": "string" },
                "number": { "type": "integer" },
                "tag": { "type": "string" },
                "environment": { "type": "string" },
                "workflow": { "type": "string" },
                "branch": { "type": "string" },
                "status": { "type": "string" },
                "event": { "type": "string" },
                "limit": { "type": "integer" }
            },
            "required": ["kind"]
        }),
    }
}

/// A short line for the prompt.
pub fn prompt_line() -> &'static str {
    "\n## Repository history\nUse `github_history` for anything `git log`, `git blame`, `gh pr list`, \
     `gh release` or deploy history would answer: recent commits (since a time, on a path), what \
     changed between two refs, who last changed some lines, merged pull requests, releases, tags, \
     GitHub deployments and Actions runs. It reads GitHub directly; there is no shell or git.\n"
}

fn bad(msg: impl Into<String>) -> String {
    msg.into()
}

/// A ref, branch, tag, label or name placed in a URL: no spaces, no `..`
/// path tricks, URL-encoded.
fn clean_ref(what: &str, s: &str) -> Result<String, String> {
    let s = s.trim();
    let ok = !s.is_empty()
        && s.len() <= 250
        && !s.contains("..")
        && !s.starts_with('/')
        && s.chars()
            .all(|c| !c.is_control() && !c.is_whitespace() || c == ' ' && what == "label");
    if ok {
        Ok(urlencoding::encode(s).into_owned())
    } else {
        Err(bad(format!("That {what} isn't valid.")))
    }
}

fn clean_path(s: &str) -> Result<String, String> {
    let s = s.trim().trim_start_matches('/');
    if s.is_empty()
        || s.len() > 500
        || s.split('/').any(|p| p == ".." || p == ".")
        || s.chars().any(char::is_control)
    {
        return Err(bad("That path isn't valid."));
    }
    Ok(s.to_string())
}

/// `2h`, `3d`, `45m` or an RFC 3339 time, as an RFC 3339 UTC time.
pub fn to_time(s: &str, now: i64) -> Result<String, String> {
    let s = s.trim();
    if let Ok(d) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(d
            .with_timezone(&chrono::Utc)
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(format!("{}T00:00:00Z", d));
    }
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: i64 = num.parse().map_err(|_| {
        bad(format!(
            "{s} isn't a time. Use 2h, 3d or 2026-01-31T14:00:00Z."
        ))
    })?;
    let secs = match unit.trim() {
        "m" | "min" | "mins" | "minutes" => n * 60,
        "h" | "hr" | "hrs" | "hours" => n * 3600,
        "d" | "day" | "days" => n * 86_400,
        "w" | "weeks" => n * 7 * 86_400,
        _ => {
            return Err(bad(format!(
                "{s} isn't a time. Use 2h, 3d or 2026-01-31T14:00:00Z."
            )))
        }
    };
    if n <= 0 || secs > 400 * 86_400 {
        return Err(bad("That time is out of range."));
    }
    Ok(chrono::DateTime::from_timestamp(now - secs, 0)
        .unwrap_or_default()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

fn limit(input: &Value) -> u64 {
    input["limit"].as_u64().unwrap_or(30).clamp(1, 100)
}

fn s<'a>(input: &'a Value, k: &str) -> Option<&'a str> {
    input[k].as_str().map(str::trim).filter(|v| !v.is_empty())
}

/// The repository to read: the one asked for, if the team connected it.
pub fn pick_repo(
    input: &Value,
    default: &str,
    allowed: &[String],
) -> Result<(String, String), String> {
    let full = s(input, "repo").unwrap_or(default);
    let (o, r) = full
        .split_once('/')
        .ok_or_else(|| bad(format!("{full} isn't owner/name.")))?;
    let ok = |x: &str| {
        !x.is_empty()
            && x.len() <= 100
            && x.chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
            && x != "."
            && x != ".."
    };
    if !ok(o) || !ok(r) {
        return Err(bad(format!("{full} isn't owner/name.")));
    }
    if !full.eq_ignore_ascii_case(default) && !allowed.iter().any(|a| a.eq_ignore_ascii_case(full))
    {
        return Err(bad(format!(
            "{full} isn't one of the team's connected repositories."
        )));
    }
    Ok((o.to_string(), r.to_string()))
}

/// What to send: a REST path under the repo (always a GET), or the blame
/// GraphQL query.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    Get(String),
    /// A pull request: itself, its commits and its reviews.
    Pull(u64),
    /// Deployments, then each one's latest status, then environments.
    Deployments(String),
    Blame {
        path: String,
        git_ref: String,
        start: u64,
        end: u64,
    },
    /// Pull requests, filtered after reading (merged, label, since).
    Pulls {
        path: String,
        merged_only: bool,
        label: Option<String>,
        since: Option<String>,
        limit: usize,
    },
}

/// Builds the request for a call. Pure, so the rules can be tested.
pub fn plan(input: &Value, now: i64) -> Result<Plan, String> {
    let kind = s(input, "kind").unwrap_or("");
    let n = limit(input);
    Ok(match kind {
        "commits" => {
            let mut q = vec![format!("per_page={n}")];
            if let Some(v) = s(input, "since") {
                q.push(format!("since={}", to_time(v, now)?));
            }
            if let Some(v) = s(input, "until") {
                q.push(format!("until={}", to_time(v, now)?));
            }
            if let Some(v) = s(input, "path") {
                q.push(format!("path={}", urlencoding::encode(&clean_path(v)?)));
            }
            if let Some(v) = s(input, "ref").or(s(input, "branch")) {
                q.push(format!("sha={}", clean_ref("ref", v)?));
            }
            if let Some(v) = s(input, "author") {
                q.push(format!("author={}", clean_ref("author", v)?));
            }
            Plan::Get(format!("commits?{}", q.join("&")))
        }
        "compare" => {
            let base = clean_ref("base", s(input, "base").ok_or("Pass `base` and `head`.")?)?;
            let head = clean_ref("head", s(input, "head").ok_or("Pass `base` and `head`.")?)?;
            Plan::Get(format!("compare/{base}...{head}?per_page=100"))
        }
        "blame" => {
            let path = clean_path(s(input, "path").ok_or("Pass the file `path`.")?)?;
            let git_ref = s(input, "ref").unwrap_or("HEAD").to_string();
            clean_ref("ref", &git_ref)?;
            let start = input["start_line"].as_u64().unwrap_or(1).max(1);
            let end = input["end_line"].as_u64().unwrap_or(start + 199).max(start);
            if end - start > 500 {
                return Err(bad("Blame at most 500 lines at a time."));
            }
            Plan::Blame {
                path,
                git_ref,
                start,
                end,
            }
        }
        "pull_requests" => {
            let state = s(input, "state").unwrap_or("merged");
            let (api_state, merged_only) = match state {
                "open" => ("open", false),
                "closed" => ("closed", false),
                "merged" => ("closed", true),
                "all" => ("all", false),
                _ => return Err(bad("`state` is open, closed, merged or all.")),
            };
            let mut q = vec![
                format!("state={api_state}"),
                "sort=updated".into(),
                "direction=desc".into(),
                "per_page=100".into(),
            ];
            if let Some(b) = s(input, "base") {
                q.push(format!("base={}", clean_ref("base", b)?));
            }
            let since = s(input, "since").map(|v| to_time(v, now)).transpose()?;
            let label = s(input, "label").map(|l| l.to_string());
            if let Some(l) = &label {
                clean_ref("label", l)?;
            }
            Plan::Pulls {
                path: format!("pulls?{}", q.join("&")),
                merged_only,
                label,
                since,
                limit: n as usize,
            }
        }
        "pull_request" => {
            let num = input["number"]
                .as_u64()
                .filter(|n| *n > 0)
                .ok_or("Pass the pull request `number`.")?;
            Plan::Pull(num)
        }
        "releases" => match s(input, "tag") {
            Some(t) => Plan::Get(format!("releases/tags/{}", clean_ref("tag", t)?)),
            None => Plan::Get(format!("releases?per_page={n}")),
        },
        "tags" => Plan::Get(format!("tags?per_page={n}")),
        "deployments" => {
            let mut q = vec![format!("per_page={}", n.min(30))];
            if let Some(e) = s(input, "environment") {
                q.push(format!("environment={}", clean_ref("environment", e)?));
            }
            Plan::Deployments(format!("deployments?{}", q.join("&")))
        }
        "workflow_runs" => {
            let mut q = vec![format!("per_page={n}")];
            for (k, what) in [
                ("branch", "branch"),
                ("status", "status"),
                ("event", "event"),
            ] {
                if let Some(v) = s(input, k) {
                    q.push(format!("{k}={}", clean_ref(what, v)?));
                }
            }
            if let Some(v) = s(input, "since") {
                q.push(format!("created=%3E%3D{}", to_time(v, now)?));
            }
            match s(input, "workflow") {
                Some(w) => Plan::Get(format!(
                    "actions/workflows/{}/runs?{}",
                    clean_ref("workflow", w)?,
                    q.join("&")
                )),
                None => Plan::Get(format!("actions/runs?{}", q.join("&"))),
            }
        }
        "" => return Err(bad(format!("Pass `kind`: one of {}.", KINDS.join(", ")))),
        other => {
            return Err(bad(format!(
                "{other} isn't a kind. Use one of {}.",
                KINDS.join(", ")
            )))
        }
    })
}

fn day(v: &Value) -> String {
    let t = v.as_str().unwrap_or("");
    t.get(..16)
        .map(|x| format!("{x}Z"))
        .unwrap_or_else(|| t.to_string())
}

fn first_line(v: &Value) -> String {
    v.as_str()
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("")
        .to_string()
}

pub fn fmt_commits(list: &[Value]) -> String {
    if list.is_empty() {
        return "No commits matched.".into();
    }
    list.iter()
        .map(|c| {
            let sha = c["sha"].as_str().unwrap_or("");
            format!(
                "{} {} {}: {}",
                sha.get(..7).unwrap_or(sha),
                day(&c["commit"]["author"]["date"]),
                c["author"]["login"]
                    .as_str()
                    .or(c["commit"]["author"]["name"].as_str())
                    .unwrap_or("?"),
                first_line(&c["commit"]["message"])
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn fmt_compare(v: &Value) -> String {
    let commits = v["commits"].as_array().cloned().unwrap_or_default();
    format!(
        "{} commits ahead, {} behind ({}); {} files changed.\n{}",
        v["ahead_by"],
        v["behind_by"],
        v["status"].as_str().unwrap_or(""),
        v["files"].as_array().map(|f| f.len()).unwrap_or(0),
        fmt_commits(&commits)
    )
}

fn pr_line(p: &Value) -> String {
    let labels: Vec<&str> = p["labels"]
        .as_array()
        .map(|l| l.iter().filter_map(|x| x["name"].as_str()).collect())
        .unwrap_or_default();
    format!(
        "#{} {} — {} → {}, by {}, {}{}{}",
        p["number"],
        p["title"].as_str().unwrap_or(""),
        p["head"]["ref"].as_str().unwrap_or(""),
        p["base"]["ref"].as_str().unwrap_or(""),
        p["user"]["login"].as_str().unwrap_or("?"),
        if p["merged_at"].is_string() {
            format!("merged {}", day(&p["merged_at"]))
        } else {
            format!(
                "{} (updated {})",
                p["state"].as_str().unwrap_or(""),
                day(&p["updated_at"])
            )
        },
        if labels.is_empty() {
            String::new()
        } else {
            format!(", labels: {}", labels.join(", "))
        },
        p["merge_commit_sha"]
            .as_str()
            .filter(|_| p["merged_at"].is_string())
            .map(|s| format!(", merge commit {}", s.get(..7).unwrap_or(s)))
            .unwrap_or_default()
    )
}

pub fn fmt_pulls(
    list: &[Value],
    merged_only: bool,
    label: Option<&str>,
    since: Option<&str>,
    limit: usize,
) -> String {
    let kept: Vec<String> = list
        .iter()
        .filter(|p| !merged_only || p["merged_at"].is_string())
        .filter(|p| {
            label.is_none_or(|l| {
                p["labels"].as_array().is_some_and(|ls| {
                    ls.iter().any(|x| {
                        x["name"]
                            .as_str()
                            .is_some_and(|n| n.eq_ignore_ascii_case(l))
                    })
                })
            })
        })
        .filter(|p| {
            since.is_none_or(|s| {
                let at = p["merged_at"]
                    .as_str()
                    .or(p["updated_at"].as_str())
                    .unwrap_or("");
                at >= s
            })
        })
        .take(limit)
        .map(pr_line)
        .collect();
    if kept.is_empty() {
        "No pull requests matched (only the 100 most recently updated are searched).".into()
    } else {
        kept.join("\n")
    }
}

pub fn fmt_pull(p: &Value, commits: &[Value], reviews: &[Value]) -> String {
    let mut out = pr_line(p);
    out.push_str(&format!(
        "\nCreated {}, {} additions, {} deletions, {} files.\n",
        day(&p["created_at"]),
        p["additions"],
        p["deletions"],
        p["changed_files"]
    ));
    let body = p["body"].as_str().unwrap_or("");
    if !body.trim().is_empty() {
        out.push_str(&format!(
            "\nDescription:\n{}\n",
            common::truncate_str(body.trim(), 3000)
        ));
    }
    out.push_str(&format!(
        "\nCommits ({}):\n{}\n",
        commits.len(),
        fmt_commits(commits)
    ));
    if !reviews.is_empty() {
        out.push_str("\nReviews:\n");
        for r in reviews {
            out.push_str(&format!(
                "- {} {} {}\n",
                r["user"]["login"].as_str().unwrap_or("?"),
                r["state"].as_str().unwrap_or(""),
                day(&r["submitted_at"])
            ));
        }
    }
    out
}

pub fn fmt_releases(v: &Value) -> String {
    let list = match v.as_array() {
        Some(a) => a.clone(),
        None => vec![v.clone()],
    };
    if list.is_empty() {
        return "No releases.".into();
    }
    list.iter()
        .map(|r| {
            let mut line = format!(
                "{} {}{}{} published {} by {}",
                r["tag_name"].as_str().unwrap_or(""),
                r["name"].as_str().unwrap_or(""),
                if r["draft"].as_bool() == Some(true) {
                    " (draft)"
                } else {
                    ""
                },
                if r["prerelease"].as_bool() == Some(true) {
                    " (pre-release)"
                } else {
                    ""
                },
                day(&r["published_at"]),
                r["author"]["login"].as_str().unwrap_or("?")
            );
            let body = r["body"].as_str().unwrap_or("").trim();
            if !body.is_empty() {
                line.push_str(&format!(
                    "\n  {}",
                    common::truncate_str(body, 1500).replace('\n', "\n  ")
                ));
            }
            line
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn fmt_tags(list: &[Value]) -> String {
    if list.is_empty() {
        return "No tags.".into();
    }
    list.iter()
        .map(|t| {
            let sha = t["commit"]["sha"].as_str().unwrap_or("");
            format!(
                "{} → {}",
                t["name"].as_str().unwrap_or(""),
                sha.get(..7).unwrap_or(sha)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn fmt_deployments(deps: &[Value], statuses: &[Option<Value>], envs: &Value) -> String {
    let mut out = String::new();
    if let Some(e) = envs["environments"].as_array() {
        let names: Vec<&str> = e.iter().filter_map(|x| x["name"].as_str()).collect();
        if !names.is_empty() {
            out.push_str(&format!("Environments: {}\n", names.join(", ")));
        }
    }
    if deps.is_empty() {
        out.push_str("No GitHub deployments (this repository may deploy without recording them; try workflow_runs).");
        return out;
    }
    for (i, d) in deps.iter().enumerate() {
        let sha = d["sha"].as_str().unwrap_or("");
        let st = statuses.get(i).cloned().flatten();
        out.push_str(&format!(
            "- {} {} ref {} ({}) by {}: {}\n",
            day(&d["created_at"]),
            d["environment"].as_str().unwrap_or(""),
            d["ref"].as_str().unwrap_or(""),
            sha.get(..7).unwrap_or(sha),
            d["creator"]["login"].as_str().unwrap_or("?"),
            st.as_ref()
                .map(|s| format!(
                    "{} at {}",
                    s["state"].as_str().unwrap_or(""),
                    day(&s["created_at"])
                ))
                .unwrap_or_else(|| "no status".into())
        ));
    }
    out
}

pub fn fmt_runs(v: &Value) -> String {
    let runs = v["workflow_runs"].as_array().cloned().unwrap_or_default();
    if runs.is_empty() {
        return "No workflow runs matched.".into();
    }
    let mut out = format!("{} runs in all; newest first:\n", v["total_count"]);
    for r in runs {
        let sha = r["head_sha"].as_str().unwrap_or("");
        out.push_str(&format!(
            "- {} {} #{} on {} ({}) {}: {} {} (run {})\n",
            day(&r["created_at"]),
            r["name"].as_str().unwrap_or(""),
            r["run_number"],
            r["head_branch"].as_str().unwrap_or(""),
            sha.get(..7).unwrap_or(sha),
            r["event"].as_str().unwrap_or(""),
            r["status"].as_str().unwrap_or(""),
            r["conclusion"].as_str().unwrap_or(""),
            r["id"]
        ));
    }
    out
}

pub const BLAME_QUERY: &str = "query($owner: String!, $name: String!, $expr: String!, $path: String!) { repository(owner: $owner, name: $name) { object(expression: $expr) { ... on Commit { blame(path: $path) { ranges { startingLine endingLine commit { oid committedDate messageHeadline author { name user { login } } } } } } } } }";

pub fn fmt_blame(v: &Value, start: u64, end: u64) -> String {
    let ranges = v["repository"]["object"]["blame"]["ranges"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if ranges.is_empty() {
        return "No blame came back (check the path and ref).".into();
    }
    let mut out = String::new();
    for r in ranges {
        let a = r["startingLine"].as_u64().unwrap_or(0);
        let b = r["endingLine"].as_u64().unwrap_or(0);
        if b < start || a > end {
            continue;
        }
        let c = &r["commit"];
        let oid = c["oid"].as_str().unwrap_or("");
        out.push_str(&format!(
            "lines {}-{}: {} {} {}: {}\n",
            a.max(start),
            b.min(end),
            oid.get(..7).unwrap_or(oid),
            day(&c["committedDate"]),
            c["author"]["user"]["login"]
                .as_str()
                .or(c["author"]["name"].as_str())
                .unwrap_or("?"),
            c["messageHeadline"].as_str().unwrap_or("")
        ));
    }
    if out.is_empty() {
        "Those lines aren't in the file.".into()
    } else {
        out
    }
}

/// The tool for one agent pass: which repos it may read, how many calls are
/// left, and where calls are logged.
pub struct GithubHistory {
    team_id: String,
    default_repo: String,
    allowed: Vec<String>,
    actor: String,
    pr: u64,
    run: RunRef,
    calls: AtomicUsize,
}

impl GithubHistory {
    pub fn new(
        team_id: &str,
        default_repo: &str,
        allowed: Vec<String>,
        actor: &str,
        pr: u64,
    ) -> Self {
        GithubHistory {
            team_id: team_id.to_string(),
            default_repo: default_repo.to_string(),
            allowed,
            actor: actor.to_string(),
            pr,
            run: RunRef::default(),
            calls: AtomicUsize::new(0),
        }
    }

    pub fn with_run(mut self, run_id: &str, origin: String) -> Self {
        self.run = RunRef {
            run_id: run_id.to_string(),
            origin,
        };
        self
    }

    /// Runs the tool. `None` when `name` isn't this tool.
    pub async fn handle(
        &self,
        state: &WorkerState,
        github: &GitHubClient,
        name: &str,
        input: &Value,
    ) -> Option<String> {
        if name != TOOL {
            return None;
        }
        let started = std::time::Instant::now();
        let asked = format!(
            "{} {}",
            input["kind"].as_str().unwrap_or(""),
            common::truncate_str(&input.to_string(), 400)
        );
        let (outcome, text) = match self.run_call(github, input).await {
            Ok(t) => ("ok", t),
            Err((refused, msg)) => (if refused { "refused" } else { "failed" }, msg),
        };
        let repo = s(input, "repo").unwrap_or(&self.default_repo).to_string();
        let logged = store::log_activity_in(
            &state.dynamo,
            &state.config.settings_table_name,
            &self.team_id,
            &Activity {
                source_id: SOURCE_ID.into(),
                source_name: SOURCE_NAME.into(),
                actor: self.actor.clone(),
                repo,
                pr: self.pr,
                query: asked,
                outcome,
                detail: if outcome == "ok" {
                    common::live::count(text.lines().count(), "line")
                } else {
                    common::truncate_str(&text, 300).to_string()
                },
                ms: started.elapsed().as_millis(),
            },
            &self.run,
        )
        .await;
        if let Err(e) = logged {
            warn!(error = %e, "Could not record a GitHub history call; withholding its result");
            return Some(
                "The call could not be recorded, so its result is withheld. Continue without it."
                    .into(),
            );
        }
        Some(if outcome == "ok" {
            let (clean, _) = common::live::redact::redact_text(&text);
            if clean.len() > MAX_RESULT_CHARS {
                format!(
                    "{}\n… (cut; narrow the request)",
                    common::truncate_str(&clean, MAX_RESULT_CHARS)
                )
            } else {
                clean
            }
        } else {
            text
        })
    }

    /// `Err((refused, message))`: refused means never sent.
    async fn run_call(
        &self,
        github: &GitHubClient,
        input: &Value,
    ) -> Result<String, (bool, String)> {
        let (owner, repo) =
            pick_repo(input, &self.default_repo, &self.allowed).map_err(|e| (true, e))?;
        let plan = plan(input, chrono::Utc::now().timestamp()).map_err(|e| (true, e))?;
        if self.calls.fetch_add(1, Ordering::SeqCst) >= MAX_CALLS {
            return Err((
                true,
                format!("All {MAX_CALLS} history calls are used. Finish with what you have."),
            ));
        }
        let get = |path: String| {
            let (o, r) = (owner.clone(), repo.clone());
            async move {
                github.get_repo_path(&o, &r, &path).await.map_err(|e| {
                    (
                        false,
                        format!(
                            "GitHub refused: {}",
                            common::truncate_str(&e.to_string(), 300)
                        ),
                    )
                })
            }
        };
        let arr = |v: Value| v.as_array().cloned().unwrap_or_default();
        Ok(match plan {
            Plan::Get(path) => {
                let v = get(path.clone()).await?;
                match s(input, "kind").unwrap_or("") {
                    "commits" => fmt_commits(&arr(v)),
                    "compare" => fmt_compare(&v),
                    "releases" => fmt_releases(&v),
                    "tags" => fmt_tags(&arr(v)),
                    "workflow_runs" => fmt_runs(&v),
                    _ => v.to_string(),
                }
            }
            Plan::Pulls {
                path,
                merged_only,
                label,
                since,
                limit,
            } => {
                let v = get(path).await?;
                fmt_pulls(
                    &arr(v),
                    merged_only,
                    label.as_deref(),
                    since.as_deref(),
                    limit,
                )
            }
            Plan::Pull(n) => {
                let p = get(format!("pulls/{n}")).await?;
                let commits = arr(get(format!("pulls/{n}/commits?per_page=100"))
                    .await
                    .unwrap_or(Value::Null));
                let reviews = arr(get(format!("pulls/{n}/reviews?per_page=100"))
                    .await
                    .unwrap_or(Value::Null));
                fmt_pull(&p, &commits, &reviews)
            }
            Plan::Deployments(path) => {
                let deps = arr(get(path).await?);
                let mut statuses = Vec::new();
                for d in deps.iter().take(10) {
                    let id = d["id"].as_u64().unwrap_or(0);
                    statuses.push(
                        get(format!("deployments/{id}/statuses?per_page=1"))
                            .await
                            .ok()
                            .and_then(|v| v.as_array().and_then(|a| a.first().cloned())),
                    );
                }
                let envs = get("environments?per_page=50".into())
                    .await
                    .unwrap_or(Value::Null);
                fmt_deployments(&deps, &statuses, &envs)
            }
            Plan::Blame {
                path,
                git_ref,
                start,
                end,
            } => {
                let v = github
                    .graphql_read(
                        BLAME_QUERY,
                        json!({ "owner": owner, "name": repo, "expr": git_ref, "path": path }),
                    )
                    .await
                    .map_err(|e| {
                        (
                            false,
                            format!(
                                "GitHub refused the blame: {}",
                                common::truncate_str(&e.to_string(), 300)
                            ),
                        )
                    })?;
                fmt_blame(&v, start, end)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_800_000_000;

    #[test]
    fn commits_take_time_path_ref_and_author() {
        let p = plan(&json!({"kind":"commits","since":"2h","path":"src/a b.rs","ref":"main","author":"amy","limit":500}), NOW).unwrap();
        let Plan::Get(path) = p else { panic!() };
        assert!(path.starts_with("commits?per_page=100"), "{path}");
        assert!(path.contains("since=2027-01-15T06:00:00Z"), "{path}");
        assert!(path.contains("path=src%2Fa%20b.rs"), "{path}");
        assert!(
            path.contains("sha=main") && path.contains("author=amy"),
            "{path}"
        );
        assert!(plan(&json!({"kind":"commits","path":"../etc"}), NOW).is_err());
        assert!(plan(&json!({"kind":"commits","since":"soon"}), NOW).is_err());
        assert!(plan(&json!({"kind":"commits","ref":"a..b"}), NOW).is_err());
    }

    #[test]
    fn every_kind_builds_a_read() {
        assert_eq!(
            plan(&json!({"kind":"compare","base":"v1.0","head":"main"}), NOW).unwrap(),
            Plan::Get("compare/v1.0...main?per_page=100".into())
        );
        assert!(matches!(
            plan(
                &json!({"kind":"blame","path":"a.rs","start_line":10,"end_line":20}),
                NOW
            )
            .unwrap(),
            Plan::Blame {
                start: 10,
                end: 20,
                ..
            }
        ));
        assert!(plan(
            &json!({"kind":"blame","path":"a.rs","start_line":1,"end_line":900}),
            NOW
        )
        .is_err());
        let Plan::Pulls {
            merged_only,
            label,
            path,
            ..
        } = plan(
            &json!({"kind":"pull_requests","label":"Deploy","base":"main"}),
            NOW,
        )
        .unwrap()
        else {
            panic!()
        };
        assert!(
            merged_only
                && label.as_deref() == Some("Deploy")
                && path.contains("state=closed")
                && path.contains("base=main")
        );
        assert_eq!(
            plan(&json!({"kind":"pull_request","number":12}), NOW).unwrap(),
            Plan::Pull(12)
        );
        assert_eq!(
            plan(&json!({"kind":"releases","tag":"v2.1.0"}), NOW).unwrap(),
            Plan::Get("releases/tags/v2.1.0".into())
        );
        assert_eq!(
            plan(&json!({"kind":"tags","limit":5}), NOW).unwrap(),
            Plan::Get("tags?per_page=5".into())
        );
        assert!(
            matches!(plan(&json!({"kind":"deployments","environment":"production"}), NOW).unwrap(), Plan::Deployments(p) if p.contains("environment=production"))
        );
        let Plan::Get(w) = plan(
            &json!({"kind":"workflow_runs","workflow":"deploy.yml","since":"1d"}),
            NOW,
        )
        .unwrap() else {
            panic!()
        };
        assert!(
            w.starts_with("actions/workflows/deploy.yml/runs?") && w.contains("created=%3E%3D"),
            "{w}"
        );
        assert!(plan(&json!({"kind":"push"}), NOW).is_err());
        assert!(plan(&json!({}), NOW).is_err());
    }

    #[test]
    fn only_connected_repos_are_read() {
        let allowed = vec!["acme/web".to_string()];
        assert_eq!(
            pick_repo(&json!({}), "acme/api", &allowed).unwrap(),
            ("acme".into(), "api".into())
        );
        assert!(pick_repo(&json!({"repo":"acme/web"}), "acme/api", &allowed).is_ok());
        assert!(pick_repo(&json!({"repo":"other/x"}), "acme/api", &allowed).is_err());
        assert!(pick_repo(&json!({"repo":"acme/../x"}), "acme/api", &allowed).is_err());
    }

    #[test]
    fn the_blame_query_is_read_only() {
        assert!(common::live::graphql::check_read_only(BLAME_QUERY).is_ok());
    }

    #[test]
    fn results_read_like_git() {
        let commits = vec![
            json!({"sha":"abcdef1234","commit":{"author":{"name":"Amy","date":"2026-10-01T12:34:56Z"},"message":"Fix login\n\nbody"},"author":{"login":"amy"}}),
        ];
        assert_eq!(
            fmt_commits(&commits),
            "abcdef1 2026-10-01T12:34Z amy: Fix login"
        );
        let prs = vec![
            json!({"number":5,"title":"Deploy","head":{"ref":"develop"},"base":{"ref":"main"},"user":{"login":"amy"},"merged_at":"2026-10-02T10:00:00Z","merge_commit_sha":"1234567890","labels":[{"name":"Deploy"}],"state":"closed"}),
            json!({"number":6,"title":"Other","head":{"ref":"x"},"base":{"ref":"main"},"user":{"login":"bo"},"merged_at":null,"labels":[],"state":"closed","updated_at":"2026-10-03T00:00:00Z"}),
        ];
        let t = fmt_pulls(&prs, true, Some("deploy"), Some("2026-10-01T00:00:00Z"), 30);
        assert!(t.contains("#5 Deploy — develop → main, by amy, merged 2026-10-02T10:00Z, labels: Deploy, merge commit 1234567"), "{t}");
        assert!(!t.contains("#6"));
        let blame = json!({"repository":{"object":{"blame":{"ranges":[{"startingLine":1,"endingLine":5,"commit":{"oid":"aaaaaaa1","committedDate":"2026-09-01T00:00:00Z","messageHeadline":"Add","author":{"name":"Amy","user":{"login":"amy"}}}},{"startingLine":6,"endingLine":9,"commit":{"oid":"bbbbbbb2","committedDate":"2026-09-02T00:00:00Z","messageHeadline":"Tweak","author":{"name":"Bo","user":null}}}]}}}});
        let b = fmt_blame(&blame, 4, 7);
        assert_eq!(b, "lines 4-5: aaaaaaa 2026-09-01T00:00Z amy: Add\nlines 6-7: bbbbbbb 2026-09-02T00:00Z Bo: Tweak\n");
        let runs = json!({"total_count":1,"workflow_runs":[{"created_at":"2026-10-01T00:00:00Z","name":"Deploy","run_number":7,"head_branch":"main","head_sha":"cafebabe1","event":"push","status":"completed","conclusion":"success","id":99}]});
        assert!(fmt_runs(&runs).contains("Deploy #7 on main (cafebab) push: completed success"));
        assert!(fmt_deployments(&[], &[], &Value::Null).contains("No GitHub deployments"));
    }
}
