# Tenant → Team Migration Plan

## Goal

Rename all "tenant" references to "team" across the codebase and decouple team identity from GitHub installation ID. A team is a first-class CoderHelm entity that can optionally connect GitHub, Jira, or AWS — not driven by any single provider.

## Current State

- **DynamoDB PK format**: `TENANT#<github_installation_id>` (e.g. `TENANT#120248482`)
- **835 Rust references** across 63 files
- **31 TypeScript references** across 37 dashboard files
- **JWT claim**: `tenant_id` field in every token
- **SQS messages**: every worker message type carries `tenant_id`
- **15 DynamoDB tables** use `tenant_id` as PK or attribute
- **CDK**: `tenant_id` and `tenant_repo` used as DynamoDB attribute names in table definitions

## What Changes

### Phase 1: New DynamoDB Tables (keep old tables intact)

Create a new set of tables with `TEAM#<uuid>` as PK. Old data stays in old tables — no migration needed.

| Old PK | New PK |
|---|---|
| `TENANT#120248482` | `TEAM#<nanoid>` |

New tables:
- `coderhelm-prod-v2` (main)
- `coderhelm-prod-v2-runs`
- `coderhelm-prod-v2-users`
- `coderhelm-prod-v2-analytics`
- `coderhelm-prod-v2-settings`
- etc.

The META record gains new connection fields:
```json
{
  "pk": "TEAM#abc123nanoid",
  "sk": "META",
  "name": "My Team",
  "plan": "pro",
  "status": "active",
  "created_at": "2026-04-01T...",
  "connections": {
    "github": {
      "installation_id": 120248482,
      "org": "nambok",
      "connected_at": "2026-04-01T..."
    },
    "jira": {
      "site_url": "https://mysite.atlassian.net",
      "connected_at": "2026-04-01T..."
    },
    "aws": {
      "account_id": "REDACTED_AWS_ACCOUNT_ID",
      "connected_at": "2026-04-01T..."
    }
  }
}
```

### Phase 2: Code Rename (bulk sed)

**Rust services** — find-and-replace across all `.rs` files:

| Old | New |
|---|---|
| `tenant_id` | `team_id` |
| `tenant_pk` | `team_pk` |
| `tenant_repo` | `team_repo` |
| `tenant_status` | `team_status` |
| `TENANT#` | `TEAM#` |
| `Tenant` (struct) | `Team` |
| `Claims.tenant_id` | `Claims.team_id` |

```bash
cd services
find . -name "*.rs" -exec sed -i '' \
  -e 's/tenant_id/team_id/g' \
  -e 's/tenant_pk/team_pk/g' \
  -e 's/tenant_repo/team_repo/g' \
  -e 's/tenant_status/team_status/g' \
  -e 's/TENANT#/TEAM#/g' \
  -e 's/struct Tenant/struct Team/g' \
  -e 's/Tenant {/Team {/g' \
  -e 's/: Tenant/: Team/g' \
  {} +
```

Then `cargo check` and fix any breakage.

**JWT claim rename**: `tenant_id` → `team_id`
- This is a breaking change for existing sessions
- All logged-in users will need to re-authenticate after deploy
- Acceptable since sessions are ephemeral (cookie-based, ~24h TTL)

**SQS message rename**: All `TicketMessage`, `CiFixMessage`, etc. get `team_id` instead of `tenant_id`
- In-flight messages at deploy time will fail deserialization
- Acceptable if deployed during low-traffic window
- Alternative: temporarily accept both `team_id` and `tenant_id` via `#[serde(alias = "tenant_id")]`

**Dashboard** — find-and-replace across all `.ts`/`.tsx` files:

| Old | New |
|---|---|
| `tenant_id` | `team_id` |
| `TenantInfo` | `TeamInfo` |
| `listTenants` | `listTeams` |
| `switchTenant` | `switchTeam` |
| `renameTenant` | `renameTeam` |
| `/api/tenants` | `/api/teams` |

**CDK infra** — rename attribute names in table definitions:
- `tenant_id` → `team_id` (PK in runs, analytics, jira-events tables)
- `tenant_repo` → `team_repo` (GSI attribute)

**Jira connector** — `admin.js` and `index.js`:
- `TENANT#` → `TEAM#`
- `tenantId` → `teamId`
- `config.tenantId` → `config.teamId`

**Lambda (log-analyzer)** — Python:
- `tenant_id` → `team_id`
- `scan_tenant_connections` → `scan_team_connections`

### Phase 3: GitHub Webhook — Lookup Instead of Derive

Currently:
```rust
let tenant_id = format!("TENANT#{installation_id}");
```

After:
```rust
// Look up team by GitHub installation ID
let team_id = lookup_team_by_github_installation(state, installation_id).await?;
```

This requires a **GSI on the main table**:
- GSI name: `github-installation-index`
- PK: `github_installation_id` (Number)
- Projects: `pk` (the `TEAM#<id>`)

Or a simple **lookup table** / **secondary index** that maps `installation_id → team_id`.

### Phase 4: Team Creation Without GitHub

New signup flow:
1. User signs up with email/password (already supported)
2. System creates `TEAM#<nanoid>` with no connections
3. User can then connect GitHub, Jira, or AWS from Settings
4. Each connection stores its IDs on the team META record

API changes:
- `POST /api/teams/create` — create a new team (no GitHub required)
- `POST /api/teams/:id/connect/github` — link GitHub installation
- `POST /api/teams/:id/connect/jira` — link Jira site
- `DELETE /api/teams/:id/connect/github` — unlink GitHub (replaces deactivation)

## File Inventory

### Rust Services (63 files, ~835 refs)

**Gateway routes** (highest density):
- `routes/github_webhook.rs` — `format!("TENANT#{installation_id}")` in ~10 places
- `routes/api.rs` — `claims.tenant_id` everywhere, `/api/tenants` endpoints
- `routes/billing.rs` — tenant_id for billing queries
- `routes/stripe_webhook.rs` — tenant_id for subscription mapping
- `routes/auth.rs` — JWT creation with tenant_id
- `routes/plans.rs` — tenant_id for plan queries
- `routes/jira_webhook.rs` — tenant_id for Jira runs
- `routes/users.rs` — tenant_id for user management
- `routes/banners.rs` — tenant_id for banner targeting
- `routes/infrastructure.rs` — tenant_id for AWS connections
- `routes/plugins.rs` — tenant_id for MCP plugin configs
- `routes/log_analyzer.rs` — tenant_id for log analysis

**Gateway core**:
- `models.rs` — `Claims`, `Tenant` struct, all message types
- `auth/jwt.rs` — `create_token(tenant_id)`, Claims struct
- `main.rs` — config/table references

**Worker**:
- `models.rs` — message types with tenant_id
- `main.rs` — tenant_id extraction from messages
- `passes/*.rs` — all 8 pass modules use tenant_id for DB operations
- `clients/billing.rs` — tenant_id for overage reporting
- `clients/email.rs` — tenant_id for email context
- `agent/mcp.rs` — tenant_id for MCP tool lookups

### CDK Infra (3 files)
- `lib/database-stack.ts` — `tenant_id` attribute in 3 table definitions, `tenant_repo` in 1 GSI
- `bin/coderhelm.ts` — table name wiring

### Dashboard (37 files, ~31 refs)
- `src/lib/api.ts` — `TenantInfo`, `tenant_id` in types, `/api/tenants` endpoints
- `src/lib/gtm.ts` — `tenant_id` tracking
- `src/components/client-shell.tsx` — tenant switching, status checks

### Jira Connector (2 files)
- `src/admin.js` — `TENANT#` prefix, `tenant_id` in DynamoDB puts
- `src/index.js` — `config.tenantId` usage

### Lambda (1 file)
- `lambda/log-analyzer/handler.py` — `tenant_id` in scans, queries, stores

## Execution Order

1. **CDK**: Add new v2 tables + GSI for github-installation-index
2. **Rust**: Bulk sed rename → `cargo check` → fix breakage
3. **Auth rewrite**: Rewrite `github_callback()`, add connection endpoints
4. **Dashboard**: Bulk sed rename + new Connections settings page → `npm run build` → verify
5. **Jira connector**: Rename + `forge deploy`
6. **Lambda**: Rename + redeploy
7. **Deploy**: CDK first, then gateway+worker Lambda, then dashboard to CloudFront
8. **Verify**: All existing sessions will re-auth (JWT claim changed)

## Risks

| Risk | Mitigation |
|---|---|
| In-flight SQS messages fail after deploy | Use `#[serde(alias)]` for 1 week, then remove |
| Existing JWT tokens invalid | Sessions are ~24h, users re-login. Acceptable. |
| Jira Forge app has cached tenant IDs | Forge app reads from storage on each invocation — will use new prefix after deploy |
| Old DynamoDB data orphaned | Old tables remain intact. No data loss. New signups use new tables. |
| GitHub webhook can't find team | GSI lookup. If no team found → return 404, don't auto-create |
| GitHub login users lose auto-join | Users who logged in via GitHub and relied on auto-joining org teams will need to manually connect GitHub in Settings. Show a one-time migration prompt. |
| Existing GitHub-only users have no password | They can still login via GitHub OAuth (identity lookup by `github_id`). Optionally prompt them to set a password. |

### Phase 5: GitHub Is Just a Connector (Not Primary Login)

Currently GitHub OAuth does three things at once during login:
1. Authenticates the user's identity
2. Fetches their GitHub App installations (`GET /user/installations`)
3. Creates/joins tenants based on those installations — `TENANT#<installation_id>`

This couples identity to GitHub. After migration, GitHub becomes one of three equal connectors.

#### Current Auth State

| Provider | Status | Creates team? |
|---|---|---|
| Email/password (Cognito) | ✅ Supported | Creates personal team (`TENANT#<cognito_sub>`) |
| Google OAuth (Cognito) | ✅ Supported | Creates personal team |
| GitHub OAuth | ✅ Supported | Auto-joins all GitHub App installation teams |

#### New Auth Flow

**Login** (email/password or Google) creates the user and a personal team:
```
User signs up → Create TEAM#<nanoid> → Create USER record → Issue JWT with team_id
```

**GitHub becomes "Connect GitHub"** — a post-login action from Settings:
1. User clicks "Connect GitHub" in Settings → `/auth/github` (with existing session cookie)
2. OAuth flow fetches user's GitHub installations
3. Each installation is linked as a connection on the user's team (or creates additional teams if multiple orgs)
4. GitHub App `installation.created` webhook also triggers connection linking

**GitHub OAuth as login** still works for convenience but no longer creates teams from installations:
```
GitHub login → Look up user by GitHub ID (GSI1) or email (GSI2)
  ├─ User exists → issue JWT for their existing team
  └─ User doesn't exist → create TEAM#<nanoid> with no connections
                           → then prompt to "Connect GitHub" in Settings
```

#### What Changes in `auth.rs`

**`github_callback()`** (currently ~200 lines):
- **Remove**: The loop that fetches `/user/installations`, filters for Coderhelm app, and auto-creates `TENANT#<installation_id>` records
- **Keep**: GitHub OAuth token exchange + user identity fetch (`GET /api/user`)
- **Change**: Look up user by `GHUSER#<github_id>` (GSI1) or `EMAIL#<email>` (GSI2). If found → issue session. If not → create new team + user, set `github_id` on user record.
- **Add**: After login, if user has GitHub installations not yet linked → show a prompt in dashboard: "You have GitHub organizations that can be connected"

**New endpoint: `POST /api/connections/github`**:
- Requires active session
- Triggers GitHub OAuth flow (reuses `/auth/github` with session cookie — this path already exists)
- After OAuth, fetches installations and stores as `connections.github` on team META
- Also stores `github_installation_id` for webhook lookup (GSI)

**New endpoint: `DELETE /api/connections/github`**:
- Removes `connections.github` from team META
- Sets team status back to active (not "deactivated")
- Existing runs/data remain — just stops new webhooks

#### Dashboard Changes

**Settings page** — new "Connections" section (or enhance existing):
```
┌─────────────────────────────────────────┐
│ Connections                              │
│                                          │
│ GitHub    nambok (org)     [Disconnect]  │
│ Jira      mysite.atlassian.net  [Disconnect] │
│ AWS       REDACTED_AWS_ACCOUNT_ID     [Disconnect]  │
│                                          │
│ [+ Connect GitHub]  [+ Connect Jira]     │
│ [+ Connect AWS]                          │
└─────────────────────────────────────────┘
```

**Login screen** — no changes needed (email/password, Google, GitHub all still work). GitHub login just stops auto-joining installation teams.

#### Files Changed

| File | Change |
|---|---|
| `routes/auth.rs` | Rewrite `github_callback()` — remove installation loop, add connection endpoints |
| `routes/api.rs` | Add `POST/DELETE /api/connections/github` |
| `models.rs` | Add `Connection` struct, update `Team` META shape |
| `dashboard/settings/connections/page.tsx` | New page — manage all connections |
| `dashboard/components/client-shell.tsx` | Remove "deactivated" full-screen block → replace with "GitHub disconnected" notice in connections |

#### Users Table Changes

Current user SK: `USER#github_12345` or `USER#Google_abc123`

After: `USER#<nanoid>` (provider-agnostic). GitHub ID and Google ID become attributes:
```json
{
  "pk": "TEAM#abc123",
  "sk": "USER#usr_xyz789",
  "email": "user@example.com",
  "github_id": 12345,
  "google_id": "abc123",
  "auth_providers": ["email", "github", "google"],
  "gsi1pk": "GHUSER#12345",
  "gsi2pk": "EMAIL#user@example.com"
}
```

## Not In Scope

- Migrating existing data from old tables to new tables (can be done later with a script)
- Backward compatibility API (old `/api/tenants` endpoint)
- Multi-provider auth linking (merging GitHub + Google accounts for same user — handle later)
