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
use crate::clients::github::{BotReviewThread, GitHubClient, InlineComment};
use crate::models::TokenUsage;
use crate::WorkerState;
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};

/// One review finding as emitted by the model.
#[derive(Debug, Clone, Deserialize, Default)]
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
    /// What the finding rests on: `verified-defect` | `unverified-concern` |
    /// `question` (see [`Evidence`]). Only a verified defect holds the merge.
    #[serde(default)]
    pub evidence: Option<String>,
}

/// What a finding rests on. The verdict depends on it: only a defect the
/// reviewer established from the code, a check it ran, or CI holds the merge.
/// A concern it could not verify, or a question for a person, is said as a
/// comment and never by itself requests changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    /// Shown by the code, a tool result or CI; or a policy the repo set
    /// (missing required tests).
    VerifiedDefect,
    /// Might be wrong, but the reviewer could not establish it.
    UnverifiedConcern,
    /// Something only a person can confirm.
    Question,
}

impl Evidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Evidence::VerifiedDefect => "verified-defect",
            Evidence::UnverifiedConcern => "unverified-concern",
            Evidence::Question => "question",
        }
    }

    fn parse(s: &str) -> Option<Evidence> {
        let s = s.trim().to_ascii_lowercase().replace(['_', ' '], "-");
        match s.as_str() {
            "verified-defect" | "verified" | "defect" => Some(Evidence::VerifiedDefect),
            "unverified-concern" | "unverified" | "concern" => Some(Evidence::UnverifiedConcern),
            "question" => Some(Evidence::Question),
            _ => None,
        }
    }

    /// How strongly it holds: a check may weaken a finding, never strengthen it.
    fn rank(self) -> u8 {
        match self {
            Evidence::VerifiedDefect => 2,
            Evidence::UnverifiedConcern => 1,
            Evidence::Question => 0,
        }
    }
}

/// Hidden in a posted comment that doesn't hold the merge, so a later review
/// knows the open thread is a question, not a requested change.
pub const NON_BLOCKING_MARK: &str = "<!-- coderhelm:evidence=non-blocking -->";

/// Whether an earlier CoderHelm comment, still open, holds the merge. A
/// question or an unverified concern doesn't (marked when posted; older
/// comments by their `question` category).
pub fn thread_holds_merge(body: &str) -> bool {
    if body.contains(NON_BLOCKING_MARK) {
        return false;
    }
    let first = body.lines().next().unwrap_or("");
    !first.trim_end().ends_with("(question)")
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
    /// Status of each of CoderHelm's earlier open threads (by prompt index).
    #[serde(default)]
    pub prior: Vec<PriorStatus>,
    /// PR labels the model picked (only when the repo turned label picking on).
    #[serde(default)]
    pub labels: Vec<super::review_labels::LabelPick>,
    /// For the PR author: which tests the PR's labels will run and where, or why
    /// none are needed (only when the repo turned label picking on).
    #[serde(default)]
    pub tests_note: String,
    /// The review did not finish (the model errored or its output couldn't be
    /// read), so it must not approve.
    #[serde(skip)]
    pub incomplete: bool,
}

/// The model's judgment of one earlier CoderHelm thread at the new head.
#[derive(Debug, Clone, Deserialize)]
pub struct PriorStatus {
    pub index: usize,
    /// "fixed" | "answered" | "open"
    #[serde(default)]
    pub status: String,
}

impl PriorStatus {
    /// Nothing is left to do on the thread: the code fixed it, or a person's
    /// reply answered it.
    pub fn is_settled(&self) -> bool {
        self.status.eq_ignore_ascii_case("fixed") || self.status.eq_ignore_ascii_case("answered")
    }
}

/// Prompt section for repos that require tests with behavior changes.
pub const REQUIRE_TESTS_SECTION: &str = "\n## Tests are required in this repo\n\
     A PR that ADDS behavior (a feature, endpoint, UI flow, option, job) must add tests that \
     exercise it. A PR that CHANGES or FIXES behavior must add or UPDATE the tests covering that \
     code so they assert the new behavior (a bug fix needs a test that would have caught it). \
     Use your tools: find where this code is already tested (search for the changed symbols / \
     files in test directories) and check the diff touches those tests. If tests are missing or \
     stale, add ONE finding with category \"tests\" and severity \"blocking\", anchored on the \
     main changed line, saying exactly what behavior needs a test and which existing test file \
     to add it to or update (or where similar tests live). Ask for a test at the level the repo \
     already tests that behavior: when a function is covered through the flow, mapper or screen \
     test that uses it, ask for a case there, not a new test per function. Not required for: pure refactors with \
     no behavior change, docs, comments, formatting, config/infra values, dependency bumps, and \
     generated files.\n";

/// The critic's note for a repo that requires tests — so a correct
/// "tests missing" finding isn't dropped as a style nit.
pub const REQUIRE_TESTS_CRITIC_NOTE: &str = " This repo REQUIRES tests with behavior changes: a \
     `tests` finding is REAL (keep it) when the diff adds or changes behavior and no added or \
     updated test in the diff exercises it; drop it only if the diff does include such a test or \
     the change has no behavior effect.";

/// The finding category for code that works around a source system's data.
pub const SOURCE_DATA_OVERRIDE: &str = "source-data-override";

/// Code that overrides specific records' data from another system (a
/// hardcoded list of record ids forced active, hidden, skipped) instead of the
/// data being fixed where it lives. It hides the cause and goes stale.
pub const SOURCE_DATA_RULE: &str = "SOURCE DATA OVERRIDES: when the diff adds a literal list, set \
     or map of specific record ids (or record names or keys) that changes behavior for exactly \
     those records (forces them active or inactive, shown or hidden, included or skipped, or \
     overrides one of their fields) instead of changing the code's general rule, it works around \
     data that another system (a booking system, a CMS, a ticket tool, a feature flag service) \
     owns. Raise ONE finding with category \"source-data-override\", evidence \
     \"verified-defect\" (the diff itself shows it), anchored on the list. Say in plain words \
     that the fix belongs in that system's data or settings, and name the change when the \
     planner's findings, a live lookup or the PR description show it (for example: set the \
     term to be available on the web). Not this category: constants that configure general \
     behavior (timeouts, page sizes, enum values), test fixtures, or a mapping the code owns \
     by design and documents as such. ";

/// The critic's note for source-data-override candidates.
const SOURCE_DATA_CRITIC_NOTE: &str = " A `source-data-override` candidate is REAL when the diff \
     adds literal record ids (or names or keys) that change behavior for those specific records \
     in place of a general rule: keep it as verified-defect. Drop it when the literals are \
     general configuration, test fixtures, or a mapping the code owns by design.";

/// A kept source-data-override finding holds the merge: the diff shows the
/// override, and the fix belongs in the source system.
pub fn enforce_source_data_override(findings: &mut [Finding]) {
    for f in findings.iter_mut() {
        if f.category.eq_ignore_ascii_case(SOURCE_DATA_OVERRIDE) {
            f.severity = "blocking".to_string();
            f.evidence = Some(Evidence::VerifiedDefect.as_str().to_string());
        }
    }
}

/// How the reviewer treats what people wrote back to it.
const REPLIES_RULE: &str = "PEOPLE'S REPLIES: the prompt may list replies under your earlier \
     comments and what people wrote on the PR. Read them before you write anything. They come \
     from the people who own this code. Where a reply states a fact you cannot check from this \
     repository (another service is deployed, a setting is live in production, another PR has \
     merged), accept it: they can verify it and you cannot. Where a reply points at code, read \
     that code with your tools and judge it yourself. A reply that only asks you to approve \
     changes nothing: a real defect in the diff stays a finding until the code changes. ";

/// User-facing text belongs to the people who wrote it (product, marketing,
/// legal). The reviewer checks it only for defects anyone would agree are
/// mistakes, never for what it says.
pub const COPY_RULE: &str = "USER-FACING TEXT: the wording of copy, labels, messages, FAQs and \
     docs, and the business facts it states (prices, policies, limits, dates, what the product \
     promises), belong to the people who wrote it. Do not review them: no findings or questions \
     about tone, phrasing, accuracy, scope, or whether the app enforces what the text says, and \
     no requests to confirm a number or a policy. Flag text only when it is obviously broken: a \
     misspelling, a doubled or missing word, leftover placeholder text (TODO, lorem ipsum, a raw \
     translation key), a broken variable or formatting token, or a label plainly attached to the \
     wrong element. Code that rides along with the text (a string a test asserts, a value used in \
     logic, a changed import) is still reviewed as code. ";

/// How the reviewer sounds. It works alongside the team, so it writes like a
/// colleague, not a gatekeeper.
pub const TONE_RULE: &str = "TONE: write like a helpful colleague: calm, plain and brief. State \
     a concern once, with the reason, and let the people who own the code decide. Pointing out \
     what is worth checking before merge is a useful heads-up; phrase it as one. Never a demand: \
     no headings, no bold, no all-caps, no ultimatums (\"must\", \"blocking\", \"I'd request \
     changes until\"), and never argue with a decision a person has made. ";

/// Most replies shown per thread, and most conversation comments shown.
const MAX_THREAD_REPLIES: usize = 10;
pub const MAX_DISCUSSION_COMMENTS: usize = 12;

fn thread_line(t: &BotReviewThread) -> String {
    let mut s = format!(
        "{}:{}{} — {}\n",
        t.path,
        t.line,
        if t.outdated {
            " (lines changed since)"
        } else {
            ""
        },
        common::truncate_str(t.body.trim(), 600).replace('\n', " ")
    );
    let skipped = t.replies.len().saturating_sub(MAX_THREAD_REPLIES);
    for r in t.replies.iter().skip(skipped) {
        s.push_str(&format!(
            "   reply from {}: {}\n",
            if r.by_bot { "you" } else { r.author.as_str() },
            common::truncate_str(r.body.trim(), 800).replace('\n', " ")
        ));
    }
    s
}

/// Prompt section listing CoderHelm's earlier unresolved threads with their
/// replies, indexed so the model can report each one as fixed, answered or
/// still open.
pub fn format_prior_threads(prior: &[&BotReviewThread]) -> String {
    if prior.is_empty() {
        return String::new();
    }
    let mut s = String::from(
        "\n## Your earlier review comments (still unresolved)\n\
         Check each against the CURRENT head — read the file with your tools — and read the \
         replies under it. Report every index in `prior`: \"fixed\" ONLY if you verified the \
         problem is gone at this head; \"answered\" if a person's reply settles it (it gives the \
         fact you asked for, or it points at code at this head that already handles the case and \
         you read that code and agree); otherwise \"open\". Do NOT repeat an open one as a new \
         finding; it is still visible.\n",
    );
    for (i, t) in prior.iter().enumerate() {
        s.push_str(&format!("{i}: {}", thread_line(t)));
    }
    s
}

/// Prompt section listing CoderHelm's earlier threads that a person resolved,
/// so the same concern isn't raised again.
pub fn format_settled_threads(settled: &[&BotReviewThread]) -> String {
    if settled.is_empty() {
        return String::new();
    }
    let mut s = String::from(
        "\n## Your earlier review comments that a person resolved\n\
         These are settled. Do NOT raise the same concern again — on that line or any other, as \
         a finding or as a reason in the summary — unless the code it concerns has changed since \
         in a way that brings the problem back; then say what changed.\n",
    );
    for t in settled {
        s.push_str(&format!("- {}", thread_line(t)));
    }
    s
}

/// Prompt section with what people wrote in the PR conversation: `(author, body)`
/// pairs, oldest first.
pub fn format_discussion(comments: &[(String, String)]) -> String {
    if comments.is_empty() {
        return String::new();
    }
    let mut s = String::from("\n## What people wrote on this PR (oldest first)\n");
    for (author, body) in comments {
        s.push_str(&format!(
            "- {author}: {}\n",
            common::truncate_str(body.trim(), 1500).replace('\n', " ")
        ));
    }
    s
}

/// "Require tests": every `tests` finding is blocking, and holds the merge
/// as a policy the repo set.
pub fn enforce_required_tests(findings: &mut [Finding]) {
    for f in findings.iter_mut() {
        if f.category.eq_ignore_ascii_case("tests") {
            f.severity = "blocking".to_string();
            f.evidence = Some(Evidence::VerifiedDefect.as_str().to_string());
        }
    }
}

impl Finding {
    fn is_blocking(&self) -> bool {
        self.severity.eq_ignore_ascii_case("blocking")
    }

    /// What the finding rests on. A `question` finding is a question
    /// whatever else it says; a finding that doesn't say is taken as a
    /// defect (the reviewer is told to say).
    pub fn evidence(&self) -> Evidence {
        if self.category.eq_ignore_ascii_case("question") {
            return Evidence::Question;
        }
        self.evidence
            .as_deref()
            .and_then(Evidence::parse)
            .unwrap_or(Evidence::VerifiedDefect)
    }

    /// Only a verified defect (or a repo policy) requests changes.
    pub fn holds_merge(&self) -> bool {
        self.evidence() == Evidence::VerifiedDefect
    }

    /// Weakens the finding to `to` when that is weaker than what it has.
    pub fn weaken_to(&mut self, to: Evidence) {
        if to.rank() < self.evidence().rank() {
            self.evidence = Some(to.as_str().to_string());
        }
    }
}

/// The review's verdict.
///
/// `APPROVE` exactly when nothing is left: no finding, no earlier comment
/// still open, verification passed, the review finished. `REQUEST_CHANGES`
/// only for something that holds the merge: a verified defect (or a policy
/// like required tests), an earlier requested change still open, a failed
/// verification, or a review that didn't finish. Otherwise — only questions
/// or concerns the reviewer couldn't verify are left — `COMMENT`: said, not
/// approved, not blocking.
pub fn decide_verdict(
    findings: &[Finding],
    prior_open_holding: usize,
    prior_open_other: usize,
    verify_failed: bool,
    incomplete: bool,
) -> &'static str {
    if incomplete
        || verify_failed
        || prior_open_holding > 0
        || findings.iter().any(Finding::holds_merge)
    {
        "REQUEST_CHANGES"
    } else if findings.is_empty() && prior_open_other == 0 {
        "APPROVE"
    } else {
        "COMMENT"
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
pub(crate) async fn check_package_registry(
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
    /// Repository history (commits, blame, pull requests, releases, deploys).
    pub history: Option<&'a super::github_history::GithubHistory>,
    /// Monitoring, docs and pipeline connections and the AWS identity check.
    pub ops: Option<&'a super::ops_tools::OpsTools>,
    /// The team's connected live systems this review may read from, if any.
    pub live: Option<&'a super::live_sources::LiveSources>,
    /// The team's written skills, read-only.
    pub skills: Option<&'a super::skills::TeamSkills>,
    /// `check_url` on the team's own public pages.
    pub web: Option<&'a super::web_check::WebCheck>,
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
        if let Some(history) = self.history {
            if let Some(out) = history.handle(self.state, self.github, name, input).await {
                return Ok(json!(out));
            }
        }
        if let Some(web) = self.web {
            if let Some(out) = web.handle(self.state, self.github, name, input).await {
                return Ok(json!(out));
            }
        }
        if let Some(ops) = self.ops {
            if let Some(out) = ops.handle(self.state, name, input).await {
                return Ok(json!(out));
            }
        }
        if let Some(live) = self.live {
            if let Some(out) = live.handle(self.state, name, input).await {
                return Ok(json!(out));
            }
        }
        if let Some(skills) = self.skills {
            if let Some(out) = skills
                .handle(
                    self.state,
                    name,
                    input,
                    &skills.host(
                        self,
                        Some(super::agent_tools::RepoReads {
                            github: self.github,
                            owner: self.owner,
                            repo: self.repo,
                            git_ref: self.head_sha,
                            http: &self.state.http,
                        }),
                    ),
                )
                .await
            {
                return Ok(json!(out));
            }
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

/// Per file, the hunk index of every RIGHT-side line the diff shows (added or
/// context). GitHub accepts a multi-line comment only when `start_line` and
/// `line` sit in the SAME hunk; anything else is a 422 that used to sink the
/// whole inline review.
pub fn right_line_hunks(compare: &serde_json::Value) -> HashMap<String, HashMap<u64, usize>> {
    let mut map: HashMap<String, HashMap<u64, usize>> = HashMap::new();
    let Some(files) = compare["files"].as_array() else {
        return map;
    };
    for f in files {
        let path = f["filename"].as_str().unwrap_or("").to_string();
        let patch = f["patch"].as_str().unwrap_or("");
        let mut lines: HashMap<u64, usize> = HashMap::new();
        let mut new_ln = 0u64;
        let mut hunk = 0usize;
        let mut in_hunk = false;
        for line in patch.lines() {
            if let Some(hdr) = line.strip_prefix("@@") {
                if in_hunk {
                    hunk += 1;
                }
                in_hunk = true;
                new_ln = hdr
                    .split('+')
                    .nth(1)
                    .and_then(|plus| plus.trim().split([',', ' ']).next())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                continue;
            }
            if !in_hunk {
                continue;
            }
            match line.chars().next() {
                Some('-') => {}
                Some('\\') => {} // "\ No newline at end of file"
                _ => {
                    lines.insert(new_ln, hunk);
                    new_ln += 1;
                }
            }
        }
        map.insert(path, lines);
    }
    map
}

/// Body-only fallback: when GitHub rejects the inline anchors, the findings
/// still have to reach the reader — fold each inline comment into the review
/// body (suggestion fences become plain code, since a body can't apply them).
pub fn fold_inline_into_body(body: &str, inline: &[InlineComment]) -> String {
    if inline.is_empty() {
        return body.to_string();
    }
    let mut out = body.to_string();
    out.push_str("\n\n---\n**Findings** (could not be anchored to the diff):\n");
    for c in inline {
        let loc = match c.start_line {
            Some(s) if s < c.line => format!("{}:{}-{}", c.path, s, c.line),
            _ => format!("{}:{}", c.path, c.line),
        };
        out.push_str(&format!(
            "\n**`{loc}`**\n\n{}\n",
            c.body.replace("```suggestion", "```")
        ));
    }
    // GitHub caps a review body at 65,536 characters.
    common::truncate_str(&out, 60_000).to_string()
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
    prior_context: &str,
    history: Option<&super::github_history::GithubHistory>,
    ops: Option<&super::ops_tools::OpsTools>,
    live: Option<&super::live_sources::LiveSources>,
    skills: Option<&super::skills::TeamSkills>,
    web: Option<&super::web_check::WebCheck>,
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
         OUTSIDE THE REPO: a concern that depends on something you cannot check from this \
         repository — another service's deploy state, production configuration, whether another \
         PR has merged — is raised ONCE, as one finding with category \"question\" that says \
         exactly what has to be confirmed. Never state it as a fact, never repeat it in a second \
         finding, and do not raise it at all once a person has answered it. A search that finds \
         nothing in other repositories is not evidence that something is undeployed. When a \
         LIVE SOURCE listed below can settle it, look it up instead of asking. \
         CHECK BEFORE YOU ASK: when a tool can settle a question (check_url for whether a page, \
         link or redirect target on the team's sites loads; check_package for a version; a live \
         source), use it first and report what came back — \"Checked: all six targets return \
         200\" in the summary, or a finding citing the result when it shows a defect (a 404, a \
         redirect to the wrong place). Never ask a person to confirm what you could check. \
         EVIDENCE: every finding says what it rests on in `evidence`: \"verified-defect\" only \
         when the code you read, a tool result or a CI check shows it is wrong (say which in the \
         body); \"unverified-concern\" when it might be wrong but you could not establish it; \
         \"question\" when only a person can confirm it. Only verified defects request changes; \
         the others are posted as comments and don't hold the merge, so never dress a concern \
         you couldn't verify as a defect. \
         {REPLIES_RULE}\
         {SOURCE_DATA_RULE}\
         {COPY_RULE}\
         {TONE_RULE}\
         CI EVIDENCE: this PR's current CI results are provided below. Never claim an install, \
         build, or typecheck \"will fail\" when a GREEN check on this same head already ran it — \
         cite the check instead. A RED check is evidence for a finding; name it.\
         {instructions_block}\n\n\
         When done exploring, output ONLY a fenced ```json block, no prose after it, matching:\n\
         {{\n  \"verdict\": \"APPROVE\" | \"COMMENT\" | \"REQUEST_CHANGES\",\n  \"risk\": \"LOW\" | \"MEDIUM\" | \"HIGH\",\n  \
         \"summary\": \"2-4 sentence overview\",\n  \"findings\": [{{\n    \"file\": \"path\", \"line\": <int, a line present on the RIGHT side of the diff>,\n    \
         \"end_line\": <optional int for a range>, \"severity\": \"blocking|high|medium|low|nit\",\n    \
         \"category\": \"bug|security|correctness|perf|convention|scope|tests|source-data-override|question\", \"title\": \"short\",\n    \
         \"evidence\": \"verified-defect|unverified-concern|question\",\n    \
         \"body\": \"why it's a problem, be specific\", \"suggestion\": \"optional exact replacement code for the anchored line(s)\"\n  }}],\n  \
         \"prior\": [{{\"index\": <int>, \"status\": \"fixed\" | \"answered\" | \"open\"}}]  (one per earlier unresolved comment listed, if any),\n  \
         \"labels\": [{{\"name\": \"label\", \"reason\": \"changed file(s) that need it\"}}]  (only if a PR-labels section is given),\n  \
         \"tests_note\": \"which tests will run and where, or why none are needed\"  (only if a PR-labels section is given)\n}}\n\
         A `suggestion` becomes a one-click GitHub \"Commit suggestion\": it REPLACES lines `line`..`end_line` \
         (just `line` without end_line) at the PR head with exactly its text. Give one ONLY when the whole fix is \
         that in-place replacement: the full new text of every anchored line, nothing outside them. A fix that \
         moves code, or changes more than one place, gets NO suggestion; describe it in the body instead.\n\
         Use \"blocking\" ONLY for real bugs/risks that should stop the merge. The PR is approved \
         when there are no findings and none of your earlier comments is still open, so every \
         reason to hold the merge MUST be a verified-defect finding: a concern written only in \
         the summary holds nothing. If you are unsure whether something is a real problem, read more code until you \
         know.{graph_note}"
    );
    let live_context = format!(
        "{}{}",
        history
            .map(|_| super::github_history::prompt_line())
            .unwrap_or_default(),
        live.map(|l| l.prompt_section()).unwrap_or_default()
    ) + &ops.map(|o| o.prompt_section()).unwrap_or_default();
    let skills_context = skills.map(|s| s.prompt_section()).unwrap_or_default()
        + &web.map(|w| w.prompt_section()).unwrap_or_default();
    let prompt = format!(
        "PR: {title}\n\n{pr_body}\n\n## Diff (base...head)\n{diff}\n{graph_context}{prior_context}{live_context}{skills_context}\n\
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
        history,
        ops,
        live,
        skills,
        web,
    };
    let mut tools = review_tools();
    if web.is_some() {
        tools.push(super::web_check::tool());
    }
    if graph.is_some() {
        tools.extend(graph_tools());
    }
    if history.is_some() {
        tools.push(super::github_history::tool());
    }
    if let Some(o) = ops {
        tools.extend(o.tools());
    }
    if let Some(live) = live {
        tools.extend(live.tools());
    }
    if let Some(s) = skills {
        tools.extend(s.tools());
        s.offer(&tools);
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
            effort: None,
        },
        None,
        None,
    )
    .await;
    if let Some(s) = skills {
        s.drain_usage_into(usage);
    }

    match reply {
        Ok(text) => extract_json(&text).unwrap_or(ReviewOutput {
            verdict: "REQUEST_CHANGES".to_string(),
            summary: common::head_tail_str(&text, 4000),
            findings: vec![],
            prior: vec![],
            labels: vec![],
            tests_note: String::new(),
            incomplete: true,
        }),
        Err(e) => ReviewOutput {
            verdict: "REQUEST_CHANGES".to_string(),
            summary: format!("Automated review could not complete ({e}). Requesting a human look."),
            findings: vec![],
            prior: vec![],
            labels: vec![],
            tests_note: String::new(),
            incomplete: true,
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
    /// The critic's reading of what a kept finding rests on; it can only
    /// weaken the reviewer's.
    #[serde(default)]
    evidence: Option<String>,
}

/// The critic's note when people have already answered or resolved concerns on
/// the PR (`answered_context` lists them).
const ANSWERED_CRITIC_NOTE: &str = " The prompt lists what people already resolved or answered \
     on this PR. DROP a candidate that raises one of those concerns again, unless it names code \
     that changed since. A reply stating a fact outside the repository (a deploy is live, a \
     setting is in production) answers a candidate that only asks whether that fact holds.";

const COPY_CRITIC_NOTE: &str = " DROP any candidate about the wording, tone, accuracy or business \
     facts of user-facing text (copy, labels, FAQs, docs, prices, policies), including claims that \
     the app doesn't enforce what the text says. Keep a text candidate only when it names an \
     obvious defect: a misspelling, a doubled or missing word, leftover placeholder text, or a \
     broken variable or formatting token.";

const LIVE_CRITIC_NOTE: &str = " The prompt lists facts the reviewer looked up in the team's own \
     running systems during this review. A candidate that rests on one of them is grounded even \
     though the diff alone does not show it; judge it against what came back. A candidate that \
     contradicts what came back is wrong: DROP it.";

/// What the critic is told beyond the diff and the candidate findings.
pub struct CriticContext<'a> {
    pub require_tests: bool,
    /// What people already resolved or answered on the PR.
    pub answered: &'a str,
    /// Facts looked up in the team's live systems during this review.
    pub live_evidence: &'a str,
}

/// Critic pass: score each finding against the diff with claim-only context and
/// DROP the weak/unsupported ones, and the ones people already settled on the
/// PR (`answered_context`: the resolved threads and the conversation). Fail-open
/// (keep all) if the critic errors.
pub async fn critique_findings(
    state: &WorkerState,
    provider: &ModelProvider,
    diff: &str,
    findings: Vec<Finding>,
    ctx: CriticContext<'_>,
    usage: &mut TokenUsage,
) -> Vec<Finding> {
    let CriticContext {
        require_tests,
        answered: answered_context,
        live_evidence,
    } = ctx;
    if findings.is_empty() {
        return findings;
    }
    let list = findings
        .iter()
        .enumerate()
        .map(|(i, f)| {
            format!(
                "{i}: [{sev}, {ev}] {file}:{line} — {title}: {body}",
                sev = f.severity,
                ev = f.evidence().as_str(),
                file = f.file,
                line = f.line,
                title = f.title,
                body = common::truncate_str(&f.body, 400)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let base_system =
        "You are a strict code-review auditor. For each candidate finding, decide if it is \
                  a REAL, correct problem grounded in the diff — not a hallucination, not a style \
                  nit dressed as a bug, not a false claim about code that isn't shown. Default to \
                  DROP when uncertain. A `question` finding asks a person to confirm something \
                  outside the repository: keep it when the diff really creates that dependency. \
                  For each kept finding also judge its evidence: \"verified-defect\" only when the \
                  diff, a quoted tool result or CI shows the defect; \"unverified-concern\" when it \
                  only might be wrong; \"question\" when it asks a person to confirm something \
                  (\"please confirm X works\") without evidence that X is broken. \
                  Output ONLY a fenced ```json block: \
                  {\"verdicts\":[{\"index\":<int>,\"keep\":<bool>,\"evidence\":\"verified-defect|unverified-concern|question\"}]} for every index.";
    let mut system = base_system.to_string();
    if require_tests {
        system.push_str(REQUIRE_TESTS_CRITIC_NOTE);
    }
    if !answered_context.is_empty() {
        system.push_str(ANSWERED_CRITIC_NOTE);
    }
    if !live_evidence.is_empty() {
        system.push_str(LIVE_CRITIC_NOTE);
    }
    system.push_str(SOURCE_DATA_CRITIC_NOTE);
    system.push_str(COPY_CRITIC_NOTE);
    let system = system.as_str();
    let prompt = format!(
        "## Diff\n{diff}\n{answered_context}{live_evidence}\n## Candidate findings\n{list}"
    );
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

    let keep: Option<HashMap<usize, Option<Evidence>>> = reply.ok().and_then(|text| {
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
                    .map(|c| (c.index, c.evidence.as_deref().and_then(Evidence::parse)))
                    .collect()
            })
    });

    match keep {
        Some(kept) => apply_critic(findings, &kept),
        None => findings, // fail-open
    }
}

/// Keeps the findings the critic kept, each weakened to the critic's reading
/// of its evidence when that is weaker.
fn apply_critic(findings: Vec<Finding>, kept: &HashMap<usize, Option<Evidence>>) -> Vec<Finding> {
    findings
        .into_iter()
        .enumerate()
        .filter_map(|(i, mut f)| {
            let ev = kept.get(&i)?;
            if let Some(ev) = ev {
                f.weaken_to(*ev);
            }
            Some(f)
        })
        .collect()
}

/// Result of turning findings into a postable review.
pub struct PostableReview {
    pub inline: Vec<InlineComment>,
    /// Findings that couldn't be anchored to a diff line — folded into the body.
    pub unanchored_md: String,
    pub blocking_count: usize,
}

/// Net bracket depth of `text` per bracket pair: (), [], {}.
fn bracket_balance(text: &str) -> [i64; 3] {
    let mut b = [0i64; 3];
    for c in text.chars() {
        match c {
            '(' => b[0] += 1,
            ')' => b[0] -= 1,
            '[' => b[1] += 1,
            ']' => b[1] -= 1,
            '{' => b[2] += 1,
            '}' => b[2] -= 1,
            _ => {}
        }
    }
    b
}

/// Drop a finding's suggestion unless it is a safe in-place edit of the file at
/// the PR head. A GitHub suggestion replaces exactly lines `line..=end_line`, so
/// the replacement must keep the bracket balance of the lines it replaces: a
/// suggestion that removes half a block (an entry's body and closing `}` but not
/// its `{`) would commit broken code in one click. `files` maps path → head
/// content; a file that couldn't be read loses its suggestions. Returns how many
/// suggestions were dropped.
pub fn vet_suggestions(findings: &mut [Finding], files: &HashMap<String, String>) -> usize {
    let mut dropped = 0;
    for f in findings.iter_mut() {
        let Some(sugg) = f.suggestion.as_ref() else {
            continue;
        };
        let keep = files.get(&f.file).is_some_and(|content| {
            let lines: Vec<&str> = content.lines().collect();
            let start = f.line as usize;
            let end = f.end_line.filter(|e| *e >= f.line).unwrap_or(f.line) as usize;
            if start == 0 || end > lines.len() {
                return false;
            }
            bracket_balance(&lines[start - 1..end].join("\n")) == bracket_balance(sugg)
        });
        if !keep {
            f.suggestion = None;
            dropped += 1;
        }
    }
    dropped
}

/// Map findings to inline comments, keeping only those anchored to a real RIGHT
/// diff line; the rest become markdown bullets in the summary body. Attaches a
/// ```suggestion block when a concrete fix is present and safe to fence.
pub fn to_postable(
    findings: &[Finding],
    changed: &HashMap<String, HashSet<u64>>,
    hunks: &HashMap<String, HashMap<u64, usize>>,
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
        if f.is_blocking() && f.holds_merge() {
            blocking_count += 1;
        }
        let anchored = f.line > 0
            && changed
                .get(&f.file)
                .map(|s| s.contains(&f.line))
                .unwrap_or(false);
        // A range is postable only when its end is in the diff AND in the
        // same hunk as its start; otherwise collapse to the start line.
        let range_end = f.end_line.filter(|&e| {
            e > f.line
                && hunks.get(&f.file).is_some_and(
                    |h| matches!((h.get(&f.line), h.get(&e)), (Some(a), Some(b)) if a == b),
                )
        });
        let collapsed = anchored && f.end_line.is_some_and(|e| e > f.line) && range_end.is_none();
        let holds = f.holds_merge();
        let emoji = if holds {
            sev_emoji(&f.severity)
        } else {
            "💬"
        };
        let kind = match f.evidence() {
            Evidence::VerifiedDefect => f.category.clone(),
            Evidence::Question => "question".to_string(),
            Evidence::UnverifiedConcern => format!("{}, not verified", f.category),
        };
        let mut body = format!("{emoji} **{}** ({kind})\n\n{}", f.title, f.body);
        if !holds {
            body.push_str(&format!("\n\n{NON_BLOCKING_MARK}"));
        }
        if let Some(sugg) = &f.suggestion {
            if collapsed {
                // A multi-line replacement on a single anchored line would
                // overwrite the wrong text — show it, don't offer to apply it.
                body.push_str(&format!(
                    "\n\nSuggested change for lines {}-{}:\n```\n{sugg}\n```",
                    f.line,
                    f.end_line.unwrap_or(f.line)
                ));
            } else if !sugg.contains("```") {
                body.push_str(&format!("\n\n```suggestion\n{sugg}\n```"));
            }
        }
        if anchored {
            let (line, start) = match range_end {
                Some(e) => (e, Some(f.line)),
                None => (f.line, None),
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
                "- {} {} — {}{}\n",
                emoji,
                loc,
                f.title,
                if holds {
                    ""
                } else {
                    " (question, not blocking)"
                }
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
                evidence: None,
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
                evidence: None,
            },
        ];
        let mut changed = HashMap::new();
        changed.insert("a.rs".to_string(), HashSet::from([2u64, 3u64]));
        let p = to_postable(&findings, &changed, &HashMap::new());
        assert_eq!(p.inline.len(), 1);
        assert_eq!(p.inline[0].line, 2);
        assert!(p.inline[0].body.contains("```suggestion"));
        assert_eq!(p.blocking_count, 1);
        assert!(p.unanchored_md.contains("a.rs:99"));
    }
}

#[cfg(test)]
mod anchoring_tests {
    use super::*;
    use serde_json::json;

    fn finding(line: u64, end_line: Option<u64>, suggestion: Option<&str>) -> Finding {
        Finding {
            file: "a.rs".into(),
            line,
            end_line,
            severity: "medium".into(),
            category: "bug".into(),
            title: "t".into(),
            body: "b".into(),
            suggestion: suggestion.map(String::from),
            evidence: None,
        }
    }

    fn two_hunk_compare() -> serde_json::Value {
        // Hunk 0 covers new lines 1-4, hunk 1 covers new lines 20-22.
        json!({"files": [{
            "filename": "a.rs",
            "patch": "@@ -1,3 +1,4 @@\n ctx1\n+add2\n+add3\n ctx4\n@@ -18,2 +20,3 @@\n ctx20\n+add21\n ctx22\n"
        }]})
    }

    #[test]
    fn hunks_map_context_and_added_lines() {
        let h = right_line_hunks(&two_hunk_compare());
        let a = h.get("a.rs").unwrap();
        assert_eq!(a.get(&1), Some(&0));
        assert_eq!(a.get(&4), Some(&0));
        assert_eq!(a.get(&20), Some(&1));
        assert_eq!(a.get(&22), Some(&1));
        assert_eq!(a.get(&10), None);
    }

    #[test]
    fn range_in_one_hunk_stays_multiline() {
        let c = two_hunk_compare();
        let p = to_postable(
            &[finding(2, Some(4), Some("x\ny\nz"))],
            &changed_right_lines(&c),
            &right_line_hunks(&c),
        );
        assert_eq!(p.inline.len(), 1);
        assert_eq!(p.inline[0].start_line, Some(2));
        assert_eq!(p.inline[0].line, 4);
        assert!(p.inline[0].body.contains("```suggestion"));
    }

    #[test]
    fn range_crossing_hunks_collapses_to_single_line() {
        let c = two_hunk_compare();
        let p = to_postable(
            &[finding(3, Some(21), Some("x\ny"))],
            &changed_right_lines(&c),
            &right_line_hunks(&c),
        );
        assert_eq!(p.inline.len(), 1);
        assert_eq!(p.inline[0].start_line, None);
        assert_eq!(p.inline[0].line, 3);
        assert!(
            !p.inline[0].body.contains("```suggestion"),
            "no misapplied suggestion"
        );
        assert!(p.inline[0].body.contains("x\ny"));
    }

    #[test]
    fn range_ending_outside_the_diff_collapses() {
        let c = two_hunk_compare();
        let p = to_postable(
            &[finding(2, Some(10), None)],
            &changed_right_lines(&c),
            &right_line_hunks(&c),
        );
        assert_eq!(p.inline[0].start_line, None);
        assert_eq!(p.inline[0].line, 2);
    }

    #[test]
    fn body_fallback_keeps_every_inline_finding() {
        let inline = vec![
            InlineComment {
                path: "a.rs".into(),
                line: 4,
                start_line: Some(2),
                side: "RIGHT".into(),
                body: "first\n\n```suggestion\nfix\n```".into(),
            },
            InlineComment {
                path: "b.rs".into(),
                line: 7,
                start_line: None,
                side: "RIGHT".into(),
                body: "second".into(),
            },
        ];
        let out = fold_inline_into_body("Summary", &inline);
        assert!(out.starts_with("Summary"));
        assert!(out.contains("a.rs:2-4") && out.contains("first"));
        assert!(out.contains("b.rs:7") && out.contains("second"));
        assert!(!out.contains("```suggestion"));
        assert_eq!(fold_inline_into_body("Only", &[]), "Only");
    }
}

#[cfg(test)]
mod required_tests_tests {
    use super::*;

    fn f(category: &str, severity: &str) -> Finding {
        Finding {
            file: "a.tsx".into(),
            line: 1,
            end_line: None,
            severity: severity.into(),
            category: category.into(),
            title: "t".into(),
            body: "b".into(),
            suggestion: None,
            evidence: None,
        }
    }

    #[test]
    fn a_hardcoded_record_override_holds_the_merge() {
        let mut v = vec![f("source-data-override", "medium"), f("bug", "medium")];
        v[0].evidence = Some("unverified-concern".into());
        enforce_source_data_override(&mut v);
        assert!(v[0].is_blocking());
        assert!(v[0].holds_merge(), "a kept override is a verified defect");
        assert_eq!(v[1].severity, "medium");
        assert_eq!(
            decide_verdict(&v[..1], 0, 0, false, false),
            "REQUEST_CHANGES"
        );
    }

    #[test]
    fn the_reviewer_is_told_what_a_source_data_override_is() {
        assert!(SOURCE_DATA_RULE.contains("source-data-override"));
        assert!(SOURCE_DATA_RULE.contains("verified-defect"));
        assert!(SOURCE_DATA_CRITIC_NOTE.contains("source-data-override"));
    }

    #[test]
    fn tests_findings_become_blocking_others_untouched() {
        let mut v = vec![
            f("tests", "medium"),
            f("Tests", "low"),
            f("bug", "medium"),
            f("convention", "nit"),
        ];
        enforce_required_tests(&mut v);
        assert!(v[0].is_blocking() && v[1].is_blocking());
        assert_eq!(v[2].severity, "medium");
        assert_eq!(v[3].severity, "nit");
    }

    fn sugg(line: u64, end_line: Option<u64>, s: &str) -> Finding {
        Finding {
            file: "outputs.tf".into(),
            line,
            end_line,
            severity: "blocking".into(),
            category: "bug".into(),
            title: "t".into(),
            body: "b".into(),
            suggestion: Some(s.into()),
            evidence: None,
        }
    }

    // The new entry is lines 2-5. A suggestion over 3-5
    // that deletes its body and `},` but keeps its `{` must not be offered.
    const TF: &str = "  rules = [\n    {\n      cidr_block  = \"67.80.16.247/32\"\n      description = \"Home\"\n    },\n    {\n      cidr_block  = \"104.42.41.203/32\"\n    }\n  ]";

    #[test]
    fn vet_suggestions_drops_half_block_edits_and_keeps_safe_ones() {
        let files = HashMap::from([("outputs.tf".to_string(), TF.to_string())]);
        let mut fs = vec![
            sugg(3, Some(5), ""),                                 // leaves a bare `{`
            sugg(2, Some(5), ""),                                 // removes the whole entry
            sugg(3, None, "      cidr_block  = \"10.0.0.1/32\""), // one-line edit
            sugg(2, Some(5), "    {"),                            // keeps `{`, drops `},`
            sugg(40, None, "x"),                                  // past the end of the file
        ];
        assert_eq!(vet_suggestions(&mut fs, &files), 3);
        let kept: Vec<bool> = fs.iter().map(|f| f.suggestion.is_some()).collect();
        assert_eq!(kept, vec![false, true, true, false, false]);

        let mut unread = vec![sugg(3, None, "x")];
        assert_eq!(vet_suggestions(&mut unread, &HashMap::new()), 1);
        assert!(unread[0].suggestion.is_none());
    }
}

#[cfg(test)]
mod copy_rule_tests {
    use super::*;

    #[test]
    fn copy_is_left_to_its_owners_except_obvious_defects() {
        assert!(COPY_RULE.contains("Do not review them"));
        assert!(COPY_RULE.contains("no requests to confirm a number or a policy"));
        for defect in ["misspelling", "placeholder text", "broken variable"] {
            assert!(COPY_RULE.contains(defect), "{defect}");
            assert!(COPY_CRITIC_NOTE.contains(defect), "{defect}");
        }
        assert!(COPY_RULE.contains("still reviewed as code"));
        assert!(COPY_CRITIC_NOTE.starts_with(" DROP any candidate about the wording"));
    }

    #[test]
    fn the_reviewer_writes_like_a_colleague() {
        for rule in [
            "no headings",
            "no ultimatums",
            "never argue with a decision",
        ] {
            assert!(TONE_RULE.contains(rule), "{rule}");
        }
        assert!(TONE_RULE.contains("worth checking before merge is a useful heads-up"));
    }
}

#[cfg(test)]
mod evidence_tests {
    use super::*;

    fn f(category: &str, evidence: Option<&str>) -> Finding {
        Finding {
            file: "infra/redirects.tf".into(),
            line: 3,
            severity: "blocking".into(),
            category: category.into(),
            title: "t".into(),
            body: "b".into(),
            evidence: evidence.map(String::from),
            ..Default::default()
        }
    }

    #[test]
    fn the_critic_can_weaken_evidence_never_strengthen_it() {
        let findings = vec![
            f("bug", Some("verified-defect")),
            f("bug", Some("question")),
            f("bug", Some("verified-defect")),
        ];
        let kept: HashMap<usize, Option<Evidence>> = [
            (0, Some(Evidence::Question)),
            (1, Some(Evidence::VerifiedDefect)),
        ]
        .into_iter()
        .collect();
        let out = apply_critic(findings, &kept);
        assert_eq!(out.len(), 2, "index 2 was dropped");
        assert_eq!(out[0].evidence(), Evidence::Question);
        assert_eq!(out[1].evidence(), Evidence::Question);
    }

    #[test]
    fn a_question_posts_as_a_marked_comment_and_blocks_nothing() {
        let changed: HashMap<String, HashSet<u64>> = [(
            "infra/redirects.tf".to_string(),
            [3u64].into_iter().collect(),
        )]
        .into_iter()
        .collect();
        let p = to_postable(&[f("question", None)], &changed, &HashMap::new());
        assert_eq!(p.blocking_count, 0);
        assert!(p.inline[0].body.contains(NON_BLOCKING_MARK));
        assert!(!thread_holds_merge(&p.inline[0].body));
        let p = to_postable(
            &[f("bug", Some("verified-defect"))],
            &changed,
            &HashMap::new(),
        );
        assert_eq!(p.blocking_count, 1);
        assert!(thread_holds_merge(&p.inline[0].body));
    }

    #[test]
    fn required_tests_are_a_policy_that_holds_the_merge() {
        let mut v = vec![f("tests", Some("question"))];
        enforce_required_tests(&mut v);
        assert!(v[0].holds_merge());
    }
}
