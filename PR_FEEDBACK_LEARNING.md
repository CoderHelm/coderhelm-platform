# CoderHelm — PR Feedback Learning System

## Vision

Every PR CoderHelm creates generates signal: approvals, rejections, inline comments, requested changes, nit-picks, and merge decisions. Today, that signal is lost after the feedback pass replies. The learning system captures these patterns and uses them to improve future runs — making CoderHelm get smarter the more a team uses it.

---

## What Signals Are Available

| Signal | Source | Strength |
|---|---|---|
| PR merged without changes | GitHub webhook `pull_request.closed` + `merged = true` | Strong positive — code was good enough |
| PR merged with minor edits | Diff between CoderHelm's last commit and merge commit | Weak positive — close but needed tweaks |
| PR closed without merge | GitHub webhook `pull_request.closed` + `merged = false` | Strong negative — code was rejected |
| Review approved | `pull_request_review.submitted` + `state = approved` | Strong positive |
| Review changes_requested | `pull_request_review.submitted` + `state = changes_requested` | Strong negative |
| Inline review comment | `pull_request_review_comment.created` | Specific feedback — the most valuable signal |
| Time to merge | `merged_at - created_at` | Quality proxy — fast merges = good, slow = needed iteration |
| Number of review rounds | Count of `changes_requested` → `approved` cycles | Quality proxy |

---

## What to Learn

### 1. Reviewer Preferences (per-reviewer patterns)

Track what each reviewer consistently comments on. Over 10+ PRs, patterns emerge:

**Examples:**
- "Alice always asks for error handling tests when new functions are added"
- "Bob flags any function longer than 30 lines"
- "Carol requires JSDoc on all exported functions"
- "Dave always requests that new API endpoints have rate limiting"

**Storage:** `LEARNINGS#REVIEWER#{github_username}` in the settings table
```json
{
  "patterns": [
    {
      "pattern": "Requests error handling tests for new functions",
      "confidence": 0.85,       // seen in 17/20 reviews
      "occurrences": 17,
      "total_reviews": 20,
      "last_seen": "2026-03-15",
      "example_comment": "Can you add a test for the error case where the API returns 500?"
    }
  ]
}
```

### 2. Team-Wide Conventions (patterns across all reviewers)

When multiple reviewers flag the same thing → it's a team convention, not personal preference.

**Examples:**
- "This team always wants `async/await` over `.then()` chains"
- "This team requires all new components to have a `data-testid` attribute"
- "This team expects Rust functions to return `Result<T, E>` instead of using `.unwrap()`"

**Storage:** `LEARNINGS#TEAM` in the settings table
```json
{
  "conventions": [
    {
      "convention": "All new React components must have data-testid attributes",
      "confidence": 0.90,
      "source_reviewers": ["alice", "bob", "carol"],
      "occurrences": 12,
      "last_seen": "2026-03-20"
    }
  ]
}
```

### 3. Common Mistakes (what CoderHelm gets wrong)

Track patterns in what CoderHelm's code gets corrected on:

**Examples:**
- "Frequently forgets to add new routes to the router index file"
- "Generates overly verbose error messages"
- "Doesn't update test snapshots after changing components"
- "Tends to use deprecated API methods"

**Storage:** `LEARNINGS#MISTAKES` in the settings table
```json
{
  "mistakes": [
    {
      "pattern": "Forgets to update route index when adding new API routes",
      "frequency": 8,
      "total_runs": 50,
      "category": "completeness",
      "remediation": "After adding a new route file, always check the index/router file"
    }
  ]
}
```

### 4. Repository-Specific Patterns

Different repos have different standards:

**Storage:** `LEARNINGS#REPO#{owner}/{repo}` in the settings table

---

## How Learning Happens

### Phase A: Collection (passive, webhook-driven)

Every PR event is already received by the gateway. Add a lightweight processor:

```
Webhook: pull_request_review_comment.created
    ↓
Is this on a CoderHelm PR? (check if branch matches our naming pattern)
    ↓
YES → Store the comment in a `feedback_events` DynamoDB table:
      PK: {team_id}
      SK: FEEDBACK#{run_id}#{timestamp}
      Attributes: reviewer, comment_body, file_path, line_number, 
                  comment_type (nit/suggestion/change_request), 
                  resolved (bool), pr_number
```

```
Webhook: pull_request.closed
    ↓
Is this a CoderHelm PR?
    ↓
YES → Store outcome:
      PK: {team_id}
      SK: OUTCOME#{run_id}
      Attributes: merged (bool), time_to_merge_hours, 
                  review_rounds, reviewer_usernames,
                  human_commits_after_coderhelm (count)
```

### Phase B: Analysis (periodic, scheduled)

A scheduled Lambda (daily or weekly) processes accumulated feedback:

1. **Pull all feedback events** for the team from the last 7 days
2. **Cluster comments** by similarity (use an LLM to categorize):
   - Group similar comments across PRs
   - Identify recurring themes
   - Separate one-off comments from patterns
3. **Calculate confidence scores:**
   - Pattern seen 3+ times from same reviewer → reviewer preference (confidence 0.6+)
   - Pattern seen 3+ times from different reviewers → team convention (confidence 0.8+)
   - Pattern seen in >50% of rejected PRs → common mistake (high priority)
4. **Update learnings** in the settings table
5. **Decay old patterns:** If a pattern hasn't been seen in 60 days, reduce confidence by 0.1. Remove at confidence < 0.3.

### Phase C: Application (injected into passes)

Learnings are injected into the system prompt during relevant passes:

**Implement pass — inject common mistakes + team conventions:**
```
<team_learnings>
Based on past PR feedback from this team:
- Always add data-testid attributes to new React components
- Update the route index file when adding new API routes  
- Use async/await instead of .then() chains
- This team prefers Result<T, E> over .unwrap() in Rust code
</team_learnings>
```

**Review pass — inject reviewer preferences:**
```
<reviewer_patterns>
The likely reviewers for this PR are: alice, bob
Common feedback from these reviewers:
- alice: Expects error handling tests for new functions
- bob: Flags functions longer than 30 lines
- bob: Requires JSDoc on exported functions

Check for these specific issues before approving.
</reviewer_patterns>
```

**PR pass — inject communication preferences:**
```
<pr_learnings>  
Past feedback on PR descriptions from this team:
- Keep descriptions under 500 words
- Always include a "Testing" section
- Link related issues
</pr_learnings>
```

---

## Data Model

### New DynamoDB Table: `coderhelm-{stage}-feedback`

| PK | SK | Attributes |
|---|---|---|
| `{team_id}` | `FEEDBACK#{run_id}#{timestamp}` | `reviewer`, `comment_body`, `file_path`, `line_number`, `comment_type`, `resolved`, `pr_number`, `repo` |
| `{team_id}` | `OUTCOME#{run_id}` | `merged`, `time_to_merge_hours`, `review_rounds`, `reviewer_usernames`, `human_commits_after`, `repo` |
| `{team_id}` | `ANALYSIS#{date}` | `patterns_found`, `conventions_updated`, `mistakes_updated`, `feedback_processed_count` |

**TTL:** 90 days for raw feedback events, no TTL for learnings (stored in settings table)

### Settings Table Additions

| Key | Purpose |
|---|---|
| `LEARNINGS#REVIEWER#{username}` | Per-reviewer patterns and preferences |
| `LEARNINGS#TEAM` | Team-wide conventions extracted from cross-reviewer feedback |
| `LEARNINGS#MISTAKES` | Common CoderHelm mistakes to avoid |
| `LEARNINGS#REPO#{owner}/{repo}` | Repository-specific patterns |

---

## Architecture

```
GitHub Webhooks
    ↓
Gateway (existing) → new handler: store_feedback_event()
    ↓
DynamoDB: feedback table (raw events)
    ↓
Scheduled Lambda (new): analyze_feedback()
    ↓ (runs daily/weekly)
    ├─ Cluster similar comments
    ├─ Extract patterns per reviewer
    ├─ Identify team conventions  
    ├─ Flag common mistakes
    └─ Update settings table with learnings
    ↓
Worker passes (existing) → load learnings at start of run
    ├─ Implement: inject mistakes + conventions into system prompt
    ├─ Review: inject reviewer preferences into system prompt
    └─ PR: inject communication learnings into system prompt
```

---

## Privacy & Control

- **Team-scoped:** Learnings are per-team. No cross-team data leakage.
- **Opt-out:** `SETTINGS#WORKFLOW` → `learning_enabled` (default `true`) — teams can disable.
- **Transparency:** Dashboard page showing all active learnings with ability to edit/delete.
- **No raw comments stored long-term:** Raw feedback events TTL at 90 days. Only extracted patterns persist.
- **Reviewer consent:** Reviewer names are stored for attribution. Teams can enable anonymous mode where learnings are stored without attribution.

---

## Dashboard Integration

### Learnings page — `/settings/learnings`

1. **Active Patterns** — table showing all learned conventions, reviewer preferences, and common mistakes with confidence scores
2. **Edit/delete** — team admins can remove bad learnings or manually add conventions
3. **Feedback timeline** — recent PR feedback events, grouped by PR, showing which comments led to which learnings
4. **Quality trend** — line chart: merge rate + avg review rounds over time (should improve as learnings accumulate)

---

## Implementation Phases

### Phase A: Collection (Priority: High, Effort: Small)
- Add webhook handler for PR review comments and PR close events
- Store raw events in feedback table
- No LLM cost, just DynamoDB writes

### Phase B: Analysis (Priority: Medium, Effort: Medium)
- Scheduled Lambda with LLM-powered clustering
- Pattern extraction and confidence scoring
- Decay logic for stale patterns
- Cost: ~$0.10 per analysis run (Sonnet, small prompt)

### Phase C: Application (Priority: High, Effort: Small)
- Load learnings at start of `run_passes()` (single DynamoDB query)
- Inject into system prompts (append to existing rules/voice section)
- Minimal code change — just string concatenation in `mod.rs`

### Phase D: Dashboard (Priority: Low, Effort: Medium)
- New settings page for viewing/editing learnings
- Gateway API endpoints for CRUD on learnings
- Quality trend charts

---

## Metrics to Track

| Metric | What it measures | Target |
|---|---|---|
| Merge rate over time | Are PRs getting better? | Increasing |
| Avg review rounds over time | Less back-and-forth? | Decreasing |
| Feedback events per PR | Fewer comments needed? | Decreasing |
| Active patterns count | Is the system learning? | Growing then plateauing |
| Pattern confidence distribution | Are patterns reliable? | Most above 0.7 |
| Time to merge | Faster approvals? | Decreasing |

---

## Future Extensions

- **Cross-team anonymized insights:** "Teams using CoderHelm commonly add these conventions: [list]" — offered as opt-in suggestions to new teams
- **Auto-update AGENTS.md:** When high-confidence conventions are learned, offer to add them to the repo's AGENTS.md so they persist even if CoderHelm is removed
- **Reviewer routing:** If learnings show certain reviewers care about certain file types, suggest optimal reviewer assignment
- **A/B testing:** Compare run quality with vs. without learnings to quantify the impact
