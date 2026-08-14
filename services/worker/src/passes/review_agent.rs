//! Agentic PR review: the model walks the repo from the diff (callers, contracts,
//! covering tests) with read-only tools, emits STRUCTURED findings, then a second
//! critic pass scores and drops the weak/false ones before we post. This is the
//! "retrieve, don't dump + verify before posting" design the research converged on
//! — repo context is fetched for a reason, not stuffed into the prompt.
//!
//! Risk is carried through here as the model's estimate; the computed blast-radius
//! risk engine layers on top in a later step and overrides it.

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::agent::provider::{self, ModelProvider};
use crate::clients::github::{GitHubClient, InlineComment};
use crate::models::TokenUsage;
use crate::WorkerState;
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};

/// One review finding as emitted by the model.
#[derive(Debug, Clone, Deserialize)]
pub struct Finding {
    pub file: String,
    #[serde(default)]
    pub line: u64,
    #[serde(default)]
    pub end_line: Option<u64>,
    /// blocking | high | medium | low | nit
    #[serde(default)]
    pub severity: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub body: String,
    /// Exact replacement text for the anchored line(s), if a concrete fix exists.
    #[serde(default)]
    pub suggestion: Option<String>,
}

/// The model's structured review output.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ReviewOutput {
    #[serde(default)]
    pub verdict: String,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<Finding>,
}

impl Finding {
    fn is_blocking(&self) -> bool {
        self.severity.eq_ignore_ascii_case("blocking")
    }
}

pub fn review_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "read_file".to_string(),
            description: "Read a file at the PR head. Prefer read_file_lines for targeted reads."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"]
            }),
        },
        ToolDefinition {
            name: "read_file_lines".to_string(),
            description: "Read specific 1-indexed inclusive lines from a file at the PR head."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "start_line": {"type": "integer"},
                    "end_line": {"type": "integer"}
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "list_directory".to_string(),
            description: "List a directory at the PR head.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"]
            }),
        },
        ToolDefinition {
            name: "search_code".to_string(),
            description:
                "Keyword/symbol search across the repo at the PR head — use it to find \
                          CALLERS of changed symbols and impacted files. Returns paths + fragments."
                    .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"]
            }),
        },
        ToolDefinition {
            name: "search_org".to_string(),
            description:
                "Search the WHOLE ORG's OTHER repos for a symbol/string to find cross-repo \
                 consumers (blast radius beyond this repo). Use ONLY when the PR changes a \
                 SHARED/exported surface — an exported function/type, a public API route or \
                 handler, a shared schema/contract, or a package's public export — to see who \
                 else depends on it. Skip it for purely internal changes. Rate-limited: call it \
                 a few times at most, with precise symbol names. Returns repo+path hits in \
                 sibling repos."
                    .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"query": {"type": "string", "description": "exact symbol/string to find across the org"}},
                "required": ["query"]
            }),
        },
        ToolDefinition {
            name: "check_package".to_string(),
            description: "Query the LIVE package registry (npm / crates.io / PyPI) for a package: \
                          does this exact version exist, and what are the current latest/dist-tags? \
                          ALWAYS use this before making any claim about published versions or \
                          registry state — your training knowledge has a cutoff; registries don't."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "registry": {"type": "string", "enum": ["npm", "crates", "pypi"]},
                    "name": {"type": "string", "description": "package name, e.g. typescript or @scope/pkg"},
                    "version": {"type": "string", "description": "optional exact version to check for existence"}
                },
                "required": ["registry", "name"]
            }),
        },
    ]
}

/// Live registry lookup backing the `check_package` tool. Fail-open: any
/// network/parse error returns a "(could not verify …)" string — the agent must
/// then phrase its concern as a question, never a confident claim.
async fn check_package_registry(
    http: &reqwest::Client,
    registry: &str,
    name: &str,
    version: Option<&str>,
) -> String {
    let enc = name.replace('/', "%2F");
    let (exists_url, latest_url) = match registry {
        "npm" => (
            version.map(|v| format!("https://registry.npmjs.org/{enc}/{v}")),
            format!("https://registry.npmjs.org/-/package/{enc}/dist-tags"),
        ),
        "crates" => (
            version.map(|v| format!("https://crates.io/api/v1/crates/{name}/{v}")),
            format!("https://crates.io/api/v1/crates/{name}"),
        ),
        "pypi" => (
            version.map(|v| format!("https://pypi.org/pypi/{name}/{v}/json")),
            format!("https://pypi.org/pypi/{name}/json"),
        ),
        other => return format!("(unknown registry `{other}` — use npm, crates, or pypi)"),
    };
    let get = |url: String| async move {
        http.get(&url)
            .header("User-Agent", "coderhelm-reviewer")
            .timeout(std::time::Duration::from_secs(8))
            .send()
            .await
    };
    let mut out = String::new();
    if let (Some(url), Some(v)) = (exists_url, version) {
        match get(url).await {
            Ok(resp) if resp.status().is_success() => {
                out.push_str(&format!("{name}@{v}: EXISTS on {registry}.\n"));
            }
            Ok(resp) if resp.status().as_u16() == 404 => {
                out.push_str(&format!("{name}@{v}: NOT FOUND on {registry}.\n"));
            }
            Ok(resp) => {
                out.push_str(&format!(
                    "(could not verify {name}@{v}: HTTP {})\n",
                    resp.status()
                ));
            }
            Err(e) => out.push_str(&format!("(could not verify {name}@{v}: {e})\n")),
        }
    }
    match get(latest_url).await {
        Ok(resp) if resp.status().is_success() => {
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            let summary = match registry {
                "npm" => format!("dist-tags: {body}"),
                "crates" => format!(
                    "max_version: {}, max_stable_version: {}",
                    body["crate"]["max_version"].as_str().unwrap_or("?"),
                    body["crate"]["max_stable_version"].as_str().unwrap_or("?")
                ),
                _ => format!(
                    "latest: {}",
                    body["info"]["version"].as_str().unwrap_or("?")
                ),
            };
            out.push_str(&format!("Current registry state for {name}: {summary}"));
        }
        Ok(resp) if resp.status().as_u16() == 404 => {
            out.push_str(&format!("Package {name}: NOT FOUND on {registry}."));
        }
        Ok(resp) => out.push_str(&format!("(could not fetch {name}: HTTP {})", resp.status())),
        Err(e) => out.push_str(&format!("(could not fetch {name}: {e})")),
    }
    out
}

pub struct PrReviewToolExecutor<'a> {
    pub github: &'a GitHubClient,
    pub owner: &'a str,
    pub repo: &'a str,
    pub head_sha: &'a str,
    /// The repo's persistent code graph, when indexed — powers the exact
    /// definition/caller/impact tools. None ⇒ those tools aren't offered.
    pub state: &'a WorkerState,
    pub graph: Option<&'a super::code_graph::Graph>,
}

/// Structural lookup tools backed by the persistent code graph. Offered only
/// when the repo has an indexed graph.
pub fn graph_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "graph_definition".to_string(),
            description: "EXACT lookup: where is this symbol DEFINED? Returns path, kind, line \
                          and an importance rank from the repo's code graph. Faster and more \
                          precise than search_code for known symbol names."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"name": {"type": "string", "description": "symbol name, e.g. processOrder"}},
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "graph_callers".to_string(),
            description: "EXACT lookup: which files REFERENCE this symbol (its caller set)? Use \
                          for blast radius of a changed function/type. Name-based — confirm \
                          surprising hits by reading the file."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "graph_impact".to_string(),
            description: "EXACT lookup: every file that imports OR references symbols defined in \
                          the given files — the impacted set of changing them. Use on the PR's \
                          changed files to know where to look for breakage."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"paths": {"type": "array", "items": {"type": "string"}}},
                "required": ["paths"]
            }),
        },
    ]
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for PrReviewToolExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let path = input.get("path").and_then(|v| v.as_str()).unwrap_or("");
        // Graph lookups share one implementation with the PR-maker agent.
        if let Some(out) = super::code_graph::handle_tool(self.state, self.graph, name, input).await
        {
            return Ok(json!(common::head_tail_str(&out, 8_000)));
        }
        match name {
            "read_file" => {
                let content = self
                    .github
                    .read_file(self.owner, self.repo, path, self.head_sha)
                    .await
                    .unwrap_or_else(|e| format!("(could not read {path}: {e})"));
                Ok(json!(common::head_tail_str(&content, 24_000)))
            }
            "read_file_lines" => {
                let start = input
                    .get("start_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1) as usize;
                let end = input
                    .get("end_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(start as u64) as usize;
                let content = self
                    .github
                    .read_file_lines(self.owner, self.repo, path, self.head_sha, start, end)
                    .await
                    .unwrap_or_else(|e| format!("(could not read {path}: {e})"));
                Ok(json!(common::head_tail_str(&content, 16_000)))
            }
            "list_directory" => {
                let entries = self
                    .github
                    .list_directory(self.owner, self.repo, path, self.head_sha)
                    .await
                    .unwrap_or_default();
                let names: Vec<String> = entries.iter().map(|e| e.path.clone()).collect();
                Ok(json!(names.join("\n")))
            }
            "search_code" => {
                let query = input.get("query").and_then(|v| v.as_str()).unwrap_or("");
                let results = self
                    .github
                    .search_code(self.owner, self.repo, self.head_sha, query)
                    .await
                    .unwrap_or_default();
                let mut out = String::new();
                for r in &results {
                    out.push_str(&format!("\n{}\n", r.path));
                    for m in &r.matches {
                        out.push_str(&format!("  {m}\n"));
                    }
                }
                Ok(json!(common::head_tail_str(&out, 12_000)))
            }
            "search_org" => {
                let query = input.get("query").and_then(|v| v.as_str()).unwrap_or("");
                let self_full = format!("{}/{}", self.owner, self.repo);
                let out = match self.github.search_org_code(self.owner, query).await {
                    Ok(hits) => {
                        let lines: Vec<String> = hits
                            .into_iter()
                            .filter(|(r, _)| !r.eq_ignore_ascii_case(&self_full))
                            .take(40)
                            .map(|(r, p)| format!("{r}: {p}"))
                            .collect();
                        if lines.is_empty() {
                            "No other org repos reference that.".to_string()
                        } else {
                            format!(
                                "Cross-repo consumers (sibling repos):\n{}",
                                lines.join("\n")
                            )
                        }
                    }
                    Err(e) => format!("(org search unavailable: {e})"),
                };
                Ok(json!(common::head_tail_str(&out, 8_000)))
            }
            "check_package" => {
                let registry = input.get("registry").and_then(|v| v.as_str()).unwrap_or("");
                let pkg = input.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let version = input.get("version").and_then(|v| v.as_str());
                if pkg.is_empty() || pkg.len() > 200 {
                    return Ok(json!("(check_package: missing/invalid package name)"));
                }
                let out = check_package_registry(&self.state.http, registry, pkg, version).await;
                Ok(json!(common::head_tail_str(&out, 4_000)))
            }
            other => Ok(json!(format!("Unknown tool: {other}"))),
        }
    }
}

/// Extract the last fenced ```json block (or the last {...} object) from a reply.
fn extract_json(reply: &str) -> Option<ReviewOutput> {
    // Prefer a ```json fenced block.
    if let Some(start) = reply.rfind("```json") {
        let after = &reply[start + 7..];
        if let Some(end) = after.find("```") {
            if let Ok(v) = serde_json::from_str::<ReviewOutput>(after[..end].trim()) {
                return Some(v);
            }
        }
    }
    // Fall back to the last balanced top-level object.
    let bytes = reply.as_bytes();
    let mut depth = 0i32;
    let mut start_idx = None;
    let mut best: Option<ReviewOutput> = None;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'{' => {
                if depth == 0 {
                    start_idx = Some(i);
                }
                depth += 1;
            }
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    if let Some(s) = start_idx {
                        if let Ok(v) = serde_json::from_str::<ReviewOutput>(&reply[s..=i]) {
                            best = Some(v);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    best
}

/// Parse a unified diff's per-file set of RIGHT-side (added/context) line numbers,
/// so we only anchor inline comments to lines that are actually in the diff.
pub fn changed_right_lines(compare: &serde_json::Value) -> HashMap<String, HashSet<u64>> {
    let mut map: HashMap<String, HashSet<u64>> = HashMap::new();
    let Some(files) = compare["files"].as_array() else {
        return map;
    };
    for f in files {
        let path = f["filename"].as_str().unwrap_or("").to_string();
        let patch = f["patch"].as_str().unwrap_or("");
        let mut set = HashSet::new();
        let mut new_ln = 0u64;
        for line in patch.lines() {
            if let Some(hdr) = line.strip_prefix("@@") {
                // @@ -a,b +c,d @@  → new-file start is c
                if let Some(plus) = hdr.split('+').nth(1) {
                    let c = plus
                        .trim()
                        .split([',', ' '])
                        .next()
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0);
                    new_ln = c;
                }
                continue;
            }
            match line.chars().next() {
                Some('+') => {
                    set.insert(new_ln);
                    new_ln += 1;
                }
                Some('-') => { /* deleted: no new-file line */ }
                _ => {
                    new_ln += 1;
                }
            }
        }
        map.insert(path, set);
    }
    map
}

/// Build a bounded diff string for the prompt (per-file patch, head+tail capped).
pub fn format_diff(compare: &serde_json::Value, cap: usize) -> String {
    let mut diff = String::new();
    if let Some(files) = compare["files"].as_array() {
        for f in files {
            let path = f["filename"].as_str().unwrap_or("");
            let status = f["status"].as_str().unwrap_or("");
            let adds = f["additions"].as_u64().unwrap_or(0);
            let dels = f["deletions"].as_u64().unwrap_or(0);
            let patch = f["patch"].as_str().unwrap_or("(no textual diff)");
            diff.push_str(&format!(
                "\n### {path} ({status}, +{adds}/-{dels})\n{patch}\n"
            ));
        }
    }
    common::head_tail_str(&diff, cap)
}

/// Generation pass: agentic, high-recall. Returns the parsed output (fail-closed
/// to REQUEST_CHANGES/HIGH with the raw text as summary if JSON can't be parsed).
#[allow(clippy::too_many_arguments)]
pub async fn generate_review(
    state: &WorkerState,
    provider: &ModelProvider,
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    head_sha: &str,
    title: &str,
    pr_body: &str,
    diff: &str,
    instructions_block: &str,
    graph: Option<&super::code_graph::Graph>,
    graph_context: &str,
    usage: &mut TokenUsage,
) -> ReviewOutput {
    let graph_note = if graph.is_some() {
        "\nThis repo has a CODE GRAPH: prefer graph_definition/graph_callers/graph_impact for \
         exact structural lookups (where is X defined, who calls X, what a change reaches); use \
         search_code for text and unindexed languages."
    } else {
        ""
    };
    let system = format!(
        "You are a senior code reviewer for {owner}/{repo}. You have READ-ONLY tools to walk the \
         repository at the PR head — use them: from the diff, search for CALLERS of changed \
         symbols and read impacted files, contracts, and the tests that cover the changed paths. \
         Judge correctness, cross-file breakage (a changed signature with un-updated callers, \
         missing migration, race, broken contract), security, and violations of the repo's rules. \
         Prefer FEW high-confidence findings over many shallow ones; do not nitpick style unless \
         it causes a bug. \
         Also check SCOPE: if the PR changes files clearly unrelated to its stated purpose/title \
         (e.g. a one-line URL fix that also edits README, package.json, unrelated components or \
         tests, or `.claude/skills`), flag it as a `scope` finding — these are usually accidental \
         (a bad rebase or a revert against the wrong base) and should be removed from the PR. \
         EXTERNAL FACTS: never assert claims about package registries, published versions, or \
         tool releases from memory — your training has a cutoff and registries don't. Verify with \
         check_package first, or phrase it as a QUESTION in a non-blocking finding; a blocking \
         finding must never rest on an unverified external fact. \
         CI EVIDENCE: this PR's current CI results are provided below. Never claim an install, \
         build, or typecheck \"will fail\" when a GREEN check on this same head already ran it — \
         cite the check instead. A RED check is evidence for a finding; name it.\
         {instructions_block}\n\n\
         When done exploring, output ONLY a fenced ```json block, no prose after it, matching:\n\
         {{\n  \"verdict\": \"APPROVE\" | \"REQUEST_CHANGES\",\n  \"risk\": \"LOW\" | \"MEDIUM\" | \"HIGH\",\n  \
         \"summary\": \"2-4 sentence overview\",\n  \"findings\": [{{\n    \"file\": \"path\", \"line\": <int, a line present on the RIGHT side of the diff>,\n    \
         \"end_line\": <optional int for a range>, \"severity\": \"blocking|high|medium|low|nit\",\n    \
         \"category\": \"bug|security|correctness|perf|convention|scope\", \"title\": \"short\",\n    \
         \"body\": \"why it's a problem, be specific\", \"suggestion\": \"optional exact replacement code for the anchored line(s)\"\n  }}]\n}}\n\
         Use \"blocking\" ONLY for real bugs/risks that should stop the merge. If unsure, REQUEST_CHANGES.{graph_note}"
    );
    let prompt = format!(
        "PR: {title}\n\n{pr_body}\n\n## Diff (base...head)\n{diff}\n{graph_context}\n\
         Explore with the tools as needed, then emit the JSON review."
    );
    let mut messages = vec![(
        "user".to_string(),
        vec![json!({"type": "text", "text": prompt})],
    )];
    let executor = PrReviewToolExecutor {
        github,
        owner,
        repo,
        head_sha,
        state,
        graph,
    };
    let mut tools = review_tools();
    if graph.is_some() {
        tools.extend(graph_tools());
    }
    let reply = provider::converse(
        state,
        provider,
        provider.heavy_model_id(),
        &system,
        &mut messages,
        &tools,
        &executor,
        usage,
        llm::ConverseOptions {
            max_turns: 30,
            max_tokens: 8192,
            deadline: None,
        },
        None,
        None,
    )
    .await;

    match reply {
        Ok(text) => extract_json(&text).unwrap_or(ReviewOutput {
            verdict: "REQUEST_CHANGES".to_string(),
            summary: common::head_tail_str(&text, 4000),
            findings: vec![],
        }),
        Err(e) => ReviewOutput {
            verdict: "REQUEST_CHANGES".to_string(),
            summary: format!("Automated review could not complete ({e}). Requesting a human look."),
            findings: vec![],
        },
    }
}

#[derive(Deserialize)]
struct Verdicts {
    #[serde(default)]
    verdicts: Vec<CritVerdict>,
}
#[derive(Deserialize)]
struct CritVerdict {
    #[serde(default)]
    index: usize,
    #[serde(default)]
    keep: bool,
}

/// Critic pass: score each finding against the diff with claim-only context and
/// DROP the weak/unsupported ones. Fail-open (keep all) if the critic errors —
/// the generation pass already fail-closed on verdict.
pub async fn critique_findings(
    state: &WorkerState,
    provider: &ModelProvider,
    diff: &str,
    findings: Vec<Finding>,
    usage: &mut TokenUsage,
) -> Vec<Finding> {
    if findings.is_empty() {
        return findings;
    }
    let list = findings
        .iter()
        .enumerate()
        .map(|(i, f)| {
            format!(
                "{i}: [{sev}] {file}:{line} — {title}: {body}",
                sev = f.severity,
                file = f.file,
                line = f.line,
                title = f.title,
                body = common::truncate_str(&f.body, 400)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let system =
        "You are a strict code-review auditor. For each candidate finding, decide if it is \
                  a REAL, correct problem grounded in the diff — not a hallucination, not a style \
                  nit dressed as a bug, not a false claim about code that isn't shown. Default to \
                  DROP when uncertain. Output ONLY a fenced ```json block: \
                  {\"verdicts\":[{\"index\":<int>,\"keep\":<bool>}]} for every index.";
    let prompt = format!("## Diff\n{diff}\n\n## Candidate findings\n{list}");
    let mut messages = vec![(
        "user".to_string(),
        vec![json!({"type": "text", "text": prompt})],
    )];
    let reply = provider::converse_simple(
        state,
        provider,
        provider.heavy_model_id(),
        system,
        &prompt,
        usage,
    )
    .await;
    // (converse_simple takes user_message; messages var unused — keep call simple)
    let _ = &mut messages;

    let keep: Option<HashSet<usize>> = reply.ok().and_then(|text| {
        let block = if let Some(s) = text.rfind("```json") {
            let after = &text[s + 7..];
            after.find("```").map(|e| after[..e].trim().to_string())
        } else {
            Some(text.clone())
        };
        block
            .and_then(|b| serde_json::from_str::<Verdicts>(&b).ok())
            .map(|v| {
                v.verdicts
                    .into_iter()
                    .filter(|c| c.keep)
                    .map(|c| c.index)
                    .collect()
            })
    });

    match keep {
        Some(set) => findings
            .into_iter()
            .enumerate()
            .filter(|(i, _)| set.contains(i))
            .map(|(_, f)| f)
            .collect(),
        None => findings, // fail-open
    }
}

/// Result of turning findings into a postable review.
pub struct PostableReview {
    pub inline: Vec<InlineComment>,
    /// Findings that couldn't be anchored to a diff line — folded into the body.
    pub unanchored_md: String,
    pub blocking_count: usize,
}

/// Map findings to inline comments, keeping only those anchored to a real RIGHT
/// diff line; the rest become markdown bullets in the summary body. Attaches a
/// ```suggestion block when a concrete fix is present and safe to fence.
pub fn to_postable(
    findings: &[Finding],
    changed: &HashMap<String, HashSet<u64>>,
) -> PostableReview {
    let sev_emoji = |s: &str| match s.to_ascii_lowercase().as_str() {
        "blocking" => "🛑",
        "high" => "🔴",
        "medium" => "🟠",
        "low" => "🟡",
        _ => "💬",
    };
    let mut inline = Vec::new();
    let mut unanchored = String::new();
    let mut blocking_count = 0;

    for f in findings {
        if f.is_blocking() {
            blocking_count += 1;
        }
        let anchored = f.line > 0
            && changed
                .get(&f.file)
                .map(|s| s.contains(&f.line))
                .unwrap_or(false);
        let mut body = format!(
            "{} **{}** ({})\n\n{}",
            sev_emoji(&f.severity),
            f.title,
            f.category,
            f.body
        );
        if let Some(sugg) = &f.suggestion {
            if !sugg.contains("```") {
                body.push_str(&format!("\n\n```suggestion\n{sugg}\n```"));
            }
        }
        if anchored {
            let start_line = f
                .end_line
                .and_then(|e| if e > f.line { Some(f.line) } else { None });
            let (line, start) = match f.end_line {
                Some(e) if e > f.line => (e, Some(f.line)),
                _ => (f.line, start_line),
            };
            inline.push(InlineComment {
                path: f.file.clone(),
                line,
                start_line: start,
                side: "RIGHT".to_string(),
                body,
            });
        } else {
            let loc = if f.line > 0 {
                format!("`{}:{}`", f.file, f.line)
            } else {
                format!("`{}`", f.file)
            };
            unanchored.push_str(&format!(
                "- {} {} — {}\n",
                sev_emoji(&f.severity),
                loc,
                f.title
            ));
        }
    }
    PostableReview {
        inline,
        unanchored_md: unanchored,
        blocking_count,
    }
}

/// Verify the PR in the CodeBuild sandbox ("receipts"): run the repo's typecheck/
/// build + the affected unit tests against the PR head, and return (passed, a
/// markdown block for the review). None when the sandbox isn't configured, no
/// command could be derived, or the build couldn't run — i.e. no signal, never a
/// false failure.
pub async fn verify_in_sandbox(
    state: &WorkerState,
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    head_sha: &str,
    changed_files: &[String],
) -> Option<(bool, String)> {
    if state.config.sandbox_bucket_name.is_empty() || state.config.sandbox_project_name.is_empty() {
        return None;
    }
    // Derive commands at the PR head so we test the PR's own code.
    let checks = super::detect_check_commands(github, owner, repo, head_sha).await;
    let test = super::detect_ci_test(github, owner, repo, head_sha)
        .await
        .and_then(|t| super::build_scoped_test_cmd(&t, changed_files));
    let cmd = match (checks, test) {
        (Some(c), Some(t)) => format!("{c} && {t}"),
        (Some(c), None) => c,
        (None, Some(t)) => t,
        (None, None) => return None,
    };
    let node = super::detect_node_version(github, owner, repo, head_sha).await;
    let tarball = github.download_tarball(owner, repo, head_sha).await.ok()?;

    let sandbox = crate::clients::sandbox::SandboxClient::new(
        &state.codebuild,
        &state.s3,
        &state.logs,
        &state.config.sandbox_bucket_name,
        &state.config.sandbox_project_name,
    );
    let run_id = format!(
        "review-{owner}-{repo}-{}",
        &head_sha[..head_sha.len().min(8)]
    )
    .replace('/', "-");

    match sandbox
        .run_checks(&run_id, 0, tarball, &cmd, node.as_deref(), None)
        .await
    {
        Ok(o) if o.ran => {
            let cmd_short = common::truncate_str(&cmd, 200);
            let md = if o.passed {
                format!("#### 🧪 Verification\n✅ Ran `{cmd_short}` in the sandbox — passed.")
            } else {
                format!(
                    "#### 🧪 Verification\n🔴 Ran `{cmd_short}` — **FAILED** (exit {}).\n\n```\n{}\n```",
                    o.exit_code.unwrap_or(-1),
                    common::head_tail_str(&o.output, 3000)
                )
            };
            Some((o.passed, md))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn changed_right_lines_marks_added_lines_only() {
        let compare = json!({
            "files": [{
                "filename": "a.rs",
                "patch": "@@ -1,3 +1,4 @@\n line1\n-old\n+new1\n+new2\n line3\n"
            }]
        });
        let m = changed_right_lines(&compare);
        let set = m.get("a.rs").unwrap();
        assert!(set.contains(&2) && set.contains(&3)); // added lines
        assert!(!set.contains(&1)); // context line, not "added"
    }

    #[test]
    fn extract_json_from_fenced_block() {
        let reply = "here is my review\n```json\n{\"verdict\":\"APPROVE\",\"summary\":\"ok\",\"findings\":[]}\n```";
        let out = extract_json(reply).unwrap();
        assert_eq!(out.verdict, "APPROVE");
        assert_eq!(out.summary, "ok");
    }

    #[test]
    fn extract_json_falls_back_to_last_object() {
        let reply = "prose {\"verdict\":\"REQUEST_CHANGES\",\"findings\":[]} trailing";
        let out = extract_json(reply).unwrap();
        assert_eq!(out.verdict, "REQUEST_CHANGES");
    }

    #[test]
    fn to_postable_anchors_only_diff_lines_and_counts_blocking() {
        let findings = vec![
            Finding {
                file: "a.rs".into(),
                line: 2,
                end_line: None,
                severity: "blocking".into(),
                category: "bug".into(),
                title: "null deref".into(),
                body: "boom".into(),
                suggestion: Some("let x = y?;".into()),
            },
            Finding {
                file: "a.rs".into(),
                line: 99, // not in diff → unanchored
                end_line: None,
                severity: "low".into(),
                category: "nit".into(),
                title: "style".into(),
                body: "meh".into(),
                suggestion: None,
            },
        ];
        let mut changed = HashMap::new();
        changed.insert("a.rs".to_string(), HashSet::from([2u64, 3u64]));
        let p = to_postable(&findings, &changed);
        assert_eq!(p.inline.len(), 1);
        assert_eq!(p.inline[0].line, 2);
        assert!(p.inline[0].body.contains("```suggestion"));
        assert_eq!(p.blocking_count, 1);
        assert!(p.unanchored_md.contains("a.rs:99"));
    }
}
