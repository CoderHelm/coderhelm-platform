# CoderHelm Worker — Upgrade Plan

## Goal
Transform the sequential pass pipeline into a best-in-class AI coding agent with parallel execution, smart orchestration, observability, and team-native communication.

---

## Phase 1: Parallel Execution

**What:** Run independent passes concurrently instead of strictly sequential.

**Changes in `passes/mod.rs`:**
- Run **Triage** and **InfraAnalyze** concurrently via `tokio::join!` — both are read-only analysis with no shared state
- After Plan completes (needs Triage result), run **Implement** while **InfraAnalyze** finishes if it hasn't already

**Expected speedup:** 30-60s saved per run (InfraAnalyze currently blocks the pipeline)

**New DynamoDB table:** None — this is a pure orchestration change in `mod.rs`

---

## Phase 2: Implement ↔ Review Loop

**What:** Replace the current one-shot Implement → Review with an iterative loop where Review feeds issues back to Implement until the code is clean.

**Current flow:**
```
Implement → Review → (fixes inline) → Conflict Resolution → PR
```

**New flow:**
```
Implement → Review
              ↓
         Issues found?
           YES → Implement (with review feedback as context) → Review again
           NO  → Conflict Resolution → PR

Max iterations: 3 (configurable per-team in settings)
```

**Changes:**
- `review.rs`: Return structured `ReviewResult { passed: bool, issues: Vec<ReviewIssue>, summary: String }` instead of fixing inline
- `implement.rs`: Accept optional `review_feedback: Option<String>` parameter — when present, the system prompt includes "The following issues were found during review. Fix them:" followed by the review summary
- `mod.rs`: Loop Implement → Review up to `max_review_cycles` (default 3). Break early if Review returns `passed: true`
- Review no longer has write tools — it only reads and reports. Implement is the only pass that writes code.

**New DynamoDB table:** None — review cycles tracked in the existing `runs` table as `review_cycles: u8`

---

## Phase 3: Formatter Agent

**What:** A new pass that rewrites all external-facing text (PR descriptions, issue comments, feedback replies) to match the team's voice before posting.

**Current state:** Voice is injected into the PR pass system prompt, but Implement, Review, and Feedback ignore it. Each pass writes in its own style.

**New pass: `formatter.rs`**
- Runs as a final transform before any text is posted to GitHub
- Takes raw text + voice instructions → returns rewritten text matching the team's tone
- Uses the light model (Sonnet) — this is a style transform, not reasoning
- Applies to: PR descriptions, issue comments (success/failure), feedback replies, clarification requests

**System prompt pattern:**
```
You are a formatter. Rewrite the following text to match this team's voice and tone.
Do not change technical content, code references, file paths, or meaning.
Only adjust style, phrasing, and tone.

Team voice instructions:
{voice}

Text to rewrite:
{raw_text}
```

**Where it hooks in:**
- `mod.rs`: Before `create_pr()`, pass the PR body through `format_with_voice()`
- `mod.rs`: Before `post_comment()`, pass through `format_with_voice()`
- `feedback.rs`: Before posting reply, pass through `format_with_voice()`
- If voice is empty/not configured, skip formatting (passthrough)

**New DynamoDB table:** None — uses existing `VOICE#GLOBAL` and `VOICE#REPO#` from settings table

---

## Phase 4: Checkpointing & Resumability

**What:** Save pipeline state after each pass so runs can resume from the last checkpoint on Lambda timeout or SQS re-delivery.

**New DynamoDB table: `coderhelm-{stage}-checkpoints`**
| PK | SK | Attributes |
|---|---|---|
| `{team_id}` | `RUN#{run_id}` | `last_pass: String`, `pass_results: Map`, `token_usage: Map`, `branch: String`, `review_cycle: u8`, `updated_at: u64`, `ttl: u64` |

**`pass_results` map structure:**
```json
{
  "triage": { "status": "complete", "result_s3_key": "..." },
  "plan": { "status": "complete", "result_s3_key": "..." },
  "implement": { "status": "complete", "files_modified": [...] },
  "review": { "status": "complete", "passed": true, "cycle": 2 },
  "pr": { "status": "complete", "pr_number": 42 }
}
```

**Changes:**
- `mod.rs`: After each pass, write checkpoint to DynamoDB
- `mod.rs`: At start of `run_passes()`, check for existing checkpoint. If found, skip completed passes and restore state
- Large pass results (PlanResult) stored in S3, checkpoint holds the S3 key
- TTL: 7 days (auto-cleanup)
- On resume, log which passes were skipped

**Resume logic:**
```
Start run_passes()
  ↓
Check checkpoint for this run_id
  ↓
Found? → Load state, skip to next incomplete pass
Not found? → Start from Triage as usual
```

---

## Phase 5: Observability

**What:** Track every LLM call, tool invocation, and pass execution with structured metrics for debugging and cost analysis.

**New DynamoDB table: `coderhelm-{stage}-traces`**
| PK | SK | Attributes |
|---|---|---|
| `{team_id}` | `RUN#{run_id}#PASS#{pass_name}` | `start_time`, `end_time`, `duration_ms`, `model`, `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens`, `tool_calls: u32`, `tool_names: Vec<String>`, `cost_usd: f64`, `error: Option<String>` |
| `{team_id}` | `RUN#{run_id}#TOOL#{timestamp}` | `pass`, `tool_name`, `duration_ms`, `input_size_bytes`, `output_size_bytes`, `success: bool` |
| `{team_id}` | `RUN#{run_id}#SUMMARY` | `total_duration_ms`, `total_cost_usd`, `total_input_tokens`, `total_output_tokens`, `passes_completed`, `review_cycles`, `tools_invoked`, `files_modified` |

**Changes:**
- `llm.rs`: Wrap each `converse()` call with timing + token tracking, write per-pass trace
- `mod.rs`: Write summary trace after run completes (or fails)
- Tool executors: Log each tool call with timing and size
- Cost calculation: Model-specific pricing (Opus vs Sonnet) × token counts

**TTL:** 30 days

### Dashboard Visualization (inspired by Langfuse / Braintrust)

The analytics dashboard already has Recharts, KPI cards, and time-series charts. The observability data should integrate as a **Run Detail** view.

**New gateway endpoints:**
- `GET /api/runs/{run_id}/traces` — returns all trace records for a single run
- `GET /api/stats/costs` — aggregated cost breakdown by model, by pass, over time

**Run Detail page — `/runs/{run_id}`**

1. **Waterfall Timeline** (horizontal bar chart)
   - Each pass is a horizontal bar, length = duration_ms, positioned on a time axis
   - Color-coded: green (passed), red (failed), blue (in-progress)
   - Nested bars for tool calls within each pass (lighter shade, stacked under the pass bar)
   - Hover tooltip: model used, input/output tokens, cache hit rate, cost, tools called
   - Shows review loop iterations as repeated Implement → Review bars

2. **Token Breakdown** (stacked bar chart per pass)
   - Per-pass breakdown: input tokens, output tokens, cache reads, cache writes
   - Horizontal stacked bars side-by-side for each pass
   - Total cost annotation on each bar (`$0.12`)
   - Shows exactly where tokens (and money) are being spent

3. **Tool Call Log** (expandable table)
   - Sortable table: timestamp, pass, tool name, duration, input/output size, success/fail
   - Click to expand: shows tool input parameters and output preview (first 500 chars)
   - Filter by pass name or tool name
   - Red rows for failed tool calls

4. **Run Summary KPI Cards** (top of page)
   - Total Duration | Total Cost | Total Tokens | Review Cycles | Files Modified | Tools Invoked
   - Cache hit rate as a percentage badge

**Analytics page additions — `/analytics`**

5. **Cost Breakdown** (stacked area chart)
   - Daily/weekly cost trend, segmented by pass type
   - Shows which passes are driving spend

6. **Pass Performance** (grouped bar chart)
   - Average duration per pass type across all runs (last 30 days)
   - Helps identify slow passes that need optimization

---

## Phase 6: Context Window Intelligence

**What:** Prevent context rot and silent failures by actively managing the conversation lifecycle — using Bedrock's native token metrics, tiered compaction, tool result clearing, and structured note-taking to keep the model focused on high-signal tokens.

**Why this matters (from Anthropic's context engineering research):**
- Context rot: as token count grows, accuracy and recall degrade — even within the 200K window
- n² pairwise attention relationships means adding noise hurts more than removing signal helps
- CoderHelm's Implement pass routinely reads 30+ files and makes 40+ tool calls — this is *exactly* where context pollution kills quality

### Strategy 1: Accurate Token Tracking (replace char estimation)

**Current plan flaw:** `(total_chars / 3.5).ceil()` is unreliable — code has higher token density than prose.

**Better approach:** Use Bedrock's own `usage` response to track *actual* tokens consumed.

```rust
// After each converse() turn, Bedrock returns actual token counts:
// response.usage().input_tokens() — exact input tokens for this call
// response.usage().output_tokens() — exact output tokens for this call
//
// Track cumulative across the loop:
struct ContextTracker {
    cumulative_input_tokens: u64,
    cumulative_output_tokens: u64,
    model_limit: u64,           // 200_000 for Opus/Sonnet
    compaction_threshold: f64,  // 0.70 — trigger at 70%
    tool_results_in_context: Vec<ToolResultMeta>,
}
```

No estimation needed — Bedrock already tells us exactly how many tokens we've consumed. We just need to track the cumulative total across turns in the agentic loop.

### Strategy 2: Tiered Compaction (progressive, not cliff-edge)

Instead of a single compaction event at 80%, use three tiers:

| Threshold | Action | What gets removed |
|---|---|---|
| **60%** of context | **Tier 1: Tool result clearing** | Replace tool results older than the last 8 turns with `"[Tool result cleared — see summary below]"`. Keep tool *names* and *inputs* but drop the full output. This is the safest form of compaction. |
| **75%** of context | **Tier 2: Selective summarization** | Summarize all tool interactions older than the last 5 turns into a structured note: files read (with key findings), files written (with change descriptions), searches performed. Uses a light model (Sonnet) call. |
| **90%** of context | **Tier 3: Full compaction** | Emergency compaction: summarize the *entire* conversation into a structured checkpoint, keeping only: system prompt, structured summary, last 3 tool call/result pairs, and the current user message. |

**Why tiered?** Single-shot compaction at 80% risks losing too much at once. Early tool-result clearing at 60% buys significant headroom with near-zero information loss — the model rarely needs the full text of a file it read 20 turns ago.

### Strategy 3: Structured Note-Taking (agentic memory within a run)

Borrowed directly from Anthropic's recommendation for long-horizon agents:

```
Throughout the Implement pass, after every 10 tool calls, inject a structured
self-note into the conversation as a system message:

<progress_note>
Files modified so far: [list]
Key decisions made: [list]
Remaining work from the plan: [list]
Current focus: [description]
</progress_note>
```

This gives the model a "working memory refresh" — it doesn't need to re-attend to 30 old tool results because the note summarizes them. After compaction, these notes survive and provide continuity.

### Strategy 4: Tool Result Sizing (pre-flight and post-flight)

**Pre-flight: MCP tool definition trimming**
- After loading MCP tool definitions, count them and estimate their token cost (description length / 3.5 as rough estimate for tool defs)
- If tool definitions exceed 30% of context window, trim descriptions to first sentence only
- If still over 30%, drop lowest-priority tools (log a warning)

**Post-flight: Large tool result truncation**
- If a single tool result exceeds 15,000 tokens (~50KB), truncate it immediately *before* adding to messages
- For `read_file`: keep first 200 lines + last 50 lines + a `"[{N} lines truncated]"` marker
- For `search_code`: keep top 20 matches, drop the rest with a count
- This prevents one large file from blowing up the entire context

### Strategy 5: Cache-Aware Prompt Structure

CoderHelm already uses Bedrock prompt caching (CachePoint after system prompt). Optimize further:

- **System prompt + tool definitions** → CachePoint (already done ✓)
- **Track cache hit rate** per run: `cache_read_tokens / (cache_read_tokens + input_tokens)`
- If cache hit rate drops below 50% for 3+ consecutive turns, log a warning — something is invalidating the cache
- Ensure tool definitions are in a stable order (sort by name) to avoid needless cache invalidation

### Implementation in `llm.rs`:

```rust
// In the converse() loop, after each response:
let input_tokens = response.usage().input_tokens() as u64;
let context_pct = input_tokens as f64 / model_limit as f64;

if context_pct > 0.90 {
    // Tier 3: Full compaction
    compact_full(messages, state, model_id).await?;
} else if context_pct > 0.75 {
    // Tier 2: Selective summarization
    summarize_old_turns(messages, keep_last: 5).await?;
} else if context_pct > 0.60 {
    // Tier 1: Clear old tool results
    clear_old_tool_results(messages, keep_last: 8);
}

// Inject progress note every 10 turns
if turns % 10 == 0 {
    inject_progress_note(messages, &files_modified, &plan_remaining);
}
```

**New DynamoDB table:** None — this is pure in-memory logic in `llm.rs`, with compaction metrics logged to the traces table (Phase 5)

---

## Phase 7: Test Execution

**What:** Run the project's existing test suite after Implement (and before Review) to catch regressions, build failures, and type errors *before* wasting an expensive Review call on broken code.

**The hard problem:** CoderHelm handles *any* language, *any* framework, *any* version. There's no single `npm test` that works everywhere.

### How it works

**Step 1: Detect test command (already partially solved)**

Onboarding (`onboard.rs`) already reads signal files and generates `AGENTS.md` with build/test commands. This is our source of truth. The test pass reads the test command from ***three sources, in priority order***:

1. **Team setting** `SETTINGS#REPO#{owner}/{repo}` → `test_command` — explicit override (e.g., `"cd backend && cargo test"`)
2. **AGENTS.md** — the onboarding pass already writes "Build and test commands" section. Parse it.
3. **Auto-detect fallback** — read signal files from the repo and infer:

| Signal File | Test Command | Build Command |
|---|---|---|
| `Cargo.toml` | `cargo test` | `cargo build` |
| `package.json` with `"test"` script | `npm test` | `npm run build` |
| `package.json` with `"vitest"` or `"jest"` | `npx vitest run` / `npx jest` | — |
| `pyproject.toml` | `pytest` | — |
| `go.mod` | `go test ./...` | `go build ./...` |
| `Makefile` with `test:` target | `make test` | `make build` |
| `.github/workflows/*.yml` | Parse the `run:` steps for test commands | — |

If no test command can be determined → **skip the test pass entirely** (log a warning, proceed to Review). Never fail a run because we can't find tests.

**Step 2: Execute in a sandboxed environment**

The test pass does NOT run tests on the Lambda itself. It uses the **existing GitHub Actions infrastructure**:

```
Test pass flow:
1. Push the implementation branch (already happens before PR)
2. Check if the repo has CI configured (.github/workflows/)
3. If YES → Wait for GitHub Actions to complete (poll check_runs API, timeout 5min)
4. If check run FAILS → pass CI logs to the Implement pass as feedback (like ci_fix.rs does)
5. If check run PASSES → proceed to Review
6. If NO CI configured → run a lightweight "smoke test" via the GitHub API:
   - Create a temporary workflow dispatch that runs the detected test command
   - OR skip tests and proceed to Review with a note
```

**Step 3: Feed failures back**

If tests fail, the test pass formats the failure output (truncated to 30K chars, same as ci_fix.rs) and returns it as a `TestResult`:

```rust
pub struct TestResult {
    pub passed: bool,
    pub output: Option<String>,    // Truncated test/build output
    pub command_used: String,      // What was run
    pub exit_code: Option<i32>,
}
```

This feeds into the Implement ↔ Review loop:

```
Implement → Test
              ↓
         Tests pass?
           YES → Review
           NO  → Implement (with test failure output as context) → Test again
                 (counts toward the same max_review_cycles budget)
```

**Why not run tests locally on Lambda?**
- Lambda has no Docker, no `cargo`, no `npm`, no language runtimes
- Installing arbitrary toolchains is a security and reliability nightmare
- GitHub Actions is already configured by the team with the right versions, env vars, secrets
- We're just leveraging existing CI infrastructure, which is what the team trusts

**New pass: `test.rs`**

**Tools:** read-only only — `read_file`, `read_tree`, `list_directory` (to find signal files and AGENTS.md). No write tools. The actual execution is via GitHub API (trigger workflow / poll check runs).

**New DynamoDB table:** None — test results stored in the runs table as `test_passed: bool`, `test_attempts: u8`

---

## Phase 8: Plan Validation

**What:** A lightweight sanity check between Plan and Implement that catches obvious problems *before* the most expensive pass runs. Saves entire failed runs by catching bad plans early.

**Why this matters:** Implement is by far the most expensive pass (Opus + 40 tool turns = $2-8 per run). A bad plan that asks to modify 25 files or references files that don't exist wastes all of that. A 2-second validation check can prevent a $5 wasted run.

### Validation checks (deterministic, no LLM needed):

**1. File existence check**
- Parse the plan's `design` and `tasks` fields for file paths (regex: paths ending in `.rs`, `.ts`, `.tsx`, `.py`, `.go`, etc.)
- For each mentioned file, call `read_tree` or `get_file` to check if it exists
- If >30% of referenced files don't exist → flag for review
- Output: `"Warning: Plan references 4 files that don't exist: [list]. The plan may be hallucinating."`

**2. Scope check**
- Count files mentioned in the plan
- If >15 files → flag: `"This plan modifies {N} files. Consider breaking into smaller tickets."`
- If >25 files → hard stop: post a comment asking for a narrower scope, mark run as `needs_clarification`
- Configurable via `SETTINGS#WORKFLOW` → `max_plan_files` (default 15, hard cap 30)

**3. Conflict detection**
- Check if the target branch already has an open PR
- Check if the files to be modified have been changed in the last 24h (via git log)
- If likely conflicts → warn in the run log but proceed

**4. Duplicate work detection**
- Search open PRs in the repo for similar titles/descriptions (fuzzy match on issue title)
- If a close match found → post a comment: `"There's already an open PR (#42) that looks related. Should I proceed?"`
- Mark run as `needs_clarification` until confirmed

### Implementation

This is NOT a new LLM pass — it's a **deterministic function** in `mod.rs` that runs between Plan and Implement:

```rust
fn validate_plan(
    plan: &PlanResult,
    github: &GithubClient,
    repo_owner: &str,
    repo_name: &str,
    branch: &str,
) -> Result<PlanValidation, Box<dyn Error + Send + Sync>> {
    let mut warnings: Vec<String> = vec![];
    let mut blocked = false;
    
    // 1. Extract file paths from plan text
    // 2. Check existence via GitHub API
    // 3. Count scope
    // 4. Check for open PRs on same files
    
    Ok(PlanValidation { warnings, blocked })
}
```

If `blocked == true`, the run stops and posts a comment. If there are warnings, they're injected into the Implement pass system prompt so the LLM is aware.

**New DynamoDB table:** None
**Settings:** `SETTINGS#WORKFLOW` → `max_plan_files` (default `15`)

---

## Phase 9: Security Audit Agent

**What:** A final gate-keeper pass that runs after the Implement ↔ Review loop but before PR creation. It performs a dedicated security-focused review of all changes, catching vulnerabilities the general Review pass might miss.

**Why a separate pass (not part of Review):**
- Review focuses on correctness, completeness, and conventions
- Security review requires a different mindset: adversarial thinking, supply chain awareness, OWASP knowledge
- Separating them means each can use a focused system prompt without competing priorities

**New pass: `security.rs`**

Runs after the last Review cycle passes. Read-only — it does NOT fix issues. If it finds problems, it sends them back through the Implement → Review loop (max 1 security remediation cycle).

**New flow:**
```
Implement ↔ Review loop (max 3 cycles)
    ↓
Security Audit
    ↓
Issues found?
    YES → Implement (with security findings) → Review → Security Audit again (max 1 retry)
    NO  → Conflict Resolution → PR
```

**Security checklist (baked into system prompt, sourced from OWASP Top 10 2025 + OWASP Code Review Guide + NPM Security Cheat Sheet):**

### 1. Injection (OWASP A03)
- SQL injection: raw string concatenation in queries, missing parameterized queries
- NoSQL injection: unsanitized user input in MongoDB/DynamoDB queries
- OS command injection: `child_process.exec()`, `eval()`, `Function()`, template literals in shell commands
- LDAP injection, XPath injection
- Log injection: unsanitized user input in log statements

### 2. Broken Access Control (OWASP A01)
- Missing authorization checks on new endpoints
- IDOR: sequential/predictable IDs exposed without ownership validation
- Privilege escalation: role checks missing or bypassable
- CORS misconfiguration: overly permissive origins

### 3. Cryptographic Failures (OWASP A02)
- Hardcoded secrets, API keys, tokens, passwords in code
- Weak hashing algorithms (MD5, SHA1 for passwords)
- Missing encryption for sensitive data at rest or in transit
- Insecure random number generation (`Math.random()` for security-sensitive operations)

### 4. Security Misconfiguration (OWASP A05)
- Debug mode enabled in production code
- Default credentials or accounts
- Unnecessary features, ports, services enabled
- Missing security headers (CSP, HSTS, X-Frame-Options)
- Verbose error messages exposing stack traces to users

### 5. Vulnerable & Outdated Components / Supply Chain (OWASP A06)
- **New npm/cargo dependencies added**: Flag ANY new dependency for human review
- Known vulnerable packages (check against npm advisory database patterns)
- Suspicious packages: typosquatting (e.g., `lodahs` instead of `lodash`), extremely low download counts, single-maintainer packages with broad permissions
- Packages with `postinstall` scripts that execute arbitrary code
- Pinned vs unpinned versions (prefer exact versions)
- Dependencies that pull in native code or FFI bindings unexpectedly

### 6. Server-Side Request Forgery (SSRF) (OWASP A10)
- User-controlled URLs passed to `fetch()`, `axios`, `http.request()`
- Missing URL validation/allowlisting for external requests
- Internal network access via user-supplied URLs (169.254.x.x, localhost, 10.x.x.x)

### 7. Cross-Site Scripting (XSS) (OWASP A07)
- User input rendered without sanitization (dangerouslySetInnerHTML, innerHTML)
- Missing output encoding/escaping
- DOM-based XSS via `document.write()`, `eval()`, `location.href` manipulation

### 8. Insecure Deserialization
- `JSON.parse()` on untrusted input without schema validation
- Prototype pollution via `Object.assign()`, spread operators on user input
- YAML/XML parsing of untrusted input

### 9. Sensitive Data Exposure
- Logging sensitive data (passwords, tokens, PII)
- Returning more data than needed in API responses
- Credentials in error messages
- `.env` files or config files committed

### 10. Denial of Service
- ReDoS: regex patterns with catastrophic backtracking on user input
- Unbound resource consumption: missing pagination, no request size limits
- Missing rate limiting on new endpoints
- Synchronous/blocking operations in async code paths

### 11. Language-Specific (Rust)
- `unsafe` blocks introduced without justification
- `.unwrap()` on user-controlled input (panic = DoS)
- Raw pointer dereferencing
- Missing bounds checking on array/slice access

### 12. Language-Specific (TypeScript/JavaScript)
- `eval()`, `new Function()`, `setTimeout(string)`
- Prototype pollution via recursive merge/deep clone of user input
- Missing `"use strict"` in non-module contexts
- `child_process` usage without input sanitization

**Output format:**
```
SECURITY_PASS or SECURITY_FAIL

If FAIL:
## Security Issues Found

### [CRITICAL/HIGH/MEDIUM/LOW] — Issue Title
- **File:** path/to/file.ts:42
- **Category:** OWASP A03 Injection
- **Description:** User input from `req.query.search` is concatenated directly into the DynamoDB filter expression without sanitization.
- **Remediation:** Use expression attribute values with placeholder syntax.

### [HIGH] — Suspicious New Dependency
- **File:** package.json
- **Category:** Supply Chain
- **Description:** New dependency `lod-ash` (2 weekly downloads, 1 contributor) was added. This appears to be a typosquat of `lodash`.
- **Remediation:** Remove `lod-ash` and use `lodash` instead.
```

**Tools available to security pass (read-only):**
- `get_diff` — View all changes made
- `read_file`, `read_file_lines` — Read any file
- `search_code` — Search for patterns (e.g., `eval(`, `exec(`, `.unwrap()`)
- `list_directory` — Check for suspicious new files
- NO write tools — security pass only reports

**New DynamoDB table:** None — security findings stored in the existing `traces` table as `RUN#{run_id}#SECURITY`

---

## Implementation Order

| Priority | Phase | Effort | Impact |
|---|---|---|---|
| 1 | Phase 2: Implement ↔ Review Loop | Medium | High — dramatically better code quality |
| 2 | Phase 8: Plan Validation | Small | High — prevents wasted $5+ runs on bad plans |
| 3 | Phase 7: Test Execution | Medium | High — catches regressions before Review |
| 4 | Phase 9: Security Audit Agent | Medium | High — catches vulnerabilities before they ship |
| 5 | Phase 3: Formatter Agent | Small | High — consistent, professional comms |
| 6 | Phase 1: Parallel Execution | Small | Medium — faster runs |
| 7 | Phase 5: Observability | Medium | High — essential for debugging and cost control |
| 8 | Phase 4: Checkpointing | Medium | Medium — resilience for long runs |
| 9 | Phase 6: Context Intelligence | Medium | Medium — prevents edge-case failures |

---

## New DynamoDB Tables Summary

| Table | Purpose | TTL |
|---|---|---|
| `coderhelm-{stage}-checkpoints` | Pass-level run state for resume | 7 days |
| `coderhelm-{stage}-traces` | Per-pass and per-tool observability metrics | 30 days |

---

## Settings Table Additions

| Key | Purpose | Default |
|---|---|---|
| `SETTINGS#WORKFLOW` → `max_review_cycles` | Max Implement ↔ Review iterations | `3` |
| `SETTINGS#WORKFLOW` → `max_security_retries` | Max security remediation cycles | `1` |
| `SETTINGS#WORKFLOW` → `max_plan_files` | Max files a plan can target before blocking | `15` |
| `SETTINGS#WORKFLOW` → `formatter_enabled` | Enable/disable voice formatter | `true` |
| `SETTINGS#WORKFLOW` → `parallel_passes` | Enable parallel Triage + InfraAnalyze | `true` |
| `SETTINGS#WORKFLOW` → `security_audit_enabled` | Enable/disable security gate | `true` |
| `SETTINGS#REPO#{owner}/{repo}` → `test_command` | Override auto-detected test command | `null` |

---

## Files to Create/Modify

| File | Action | Phase |
|---|---|---|
| `services/worker/src/passes/mod.rs` | Parallel execution, review loop, test gate, security gate, formatter, plan validation, checkpoint logic | 1, 2, 3, 4, 7, 8, 9 |
| `services/worker/src/passes/test.rs` | New file — test execution pass (GitHub Actions integration) | 7 |
| `services/worker/src/passes/security.rs` | New file — security audit pass (OWASP-based) | 9 |
| `services/worker/src/passes/formatter.rs` | New file — voice formatting pass | 3 |
| `services/worker/src/passes/review.rs` | Return `ReviewResult` instead of fixing inline | 2 |
| `services/worker/src/passes/implement.rs` | Accept review/security/test feedback parameter | 2, 7, 9 |
| `services/worker/src/agent/llm.rs` | Token tracking, compaction, trace logging | 5, 6 |
| `services/worker/src/passes/pr.rs` | Use formatter for PR body | 3 |
| `services/worker/src/passes/feedback.rs` | Use formatter for replies | 3 |
| `infra/lib/database-stack.ts` | Add checkpoints + traces tables | 4, 5 |
| `services/gateway/src/routes/api.rs` | Traces API endpoint | 5 |

---

## Full Pipeline Flow (after all phases)

```
Issue/Ticket arrives
    ↓
Triage + InfraAnalyze (parallel)           ← Phase 1
    ↓
Plan
    ↓
Plan Validation (deterministic checks)     ← Phase 8
    ↓  (blocked? → post comment, stop)
Implement
    ↓
Test (run CI / detect test command)         ← Phase 7
    ↓  (fail? → feed back to Implement)
Review (read-only, returns issues)          ← Phase 2
    ↓  (issues? → feed back to Implement, loop max 3)
Security Audit (read-only, OWASP check)    ← Phase 9
    ↓  (fail? → feed back to Implement → Review → Security, max 1 retry)
Conflict Resolution
    ↓
Formatter (rewrite PR body in team voice)  ← Phase 3
    ↓
PR Creation
```

Throughout: Checkpointing (Phase 4), Observability (Phase 5), Context Intelligence (Phase 6)
