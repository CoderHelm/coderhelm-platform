# CoderHelm

**AI coding agent that turns GitHub issues into pull requests.**

[![CI](https://github.com/CoderHelm/coderhelm-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/CoderHelm/coderhelm-platform/actions/workflows/ci.yml)

---

## What is CoderHelm?

CoderHelm is an autonomous coding agent that reads a GitHub issue (or Jira ticket), researches the target repository, generates an implementation plan, writes the code, reviews its own work, and opens a pull request — all without human intervention.

Assign an issue to `coderhelm[bot]` or add the `coderhelm` label, and the agent handles the rest: triage, planning, implementation, self-review, and PR creation. Teams keep their existing workflow — CoderHelm plugs in through standard GitHub App webhooks and an optional Jira integration.

---

## Architecture Overview

CoderHelm runs entirely on AWS serverless infrastructure provisioned with CDK. Two Rust Lambda functions — **Gateway** and **Worker** — communicate through SQS queues, backed by DynamoDB tables and S3 storage. A CloudFront-fronted dashboard at `app.coderhelm.com` provides visibility into runs, settings, and billing.

### Repository Layout

```text
.github/          GitHub Actions CI and deploy workflows
coderhelm-jira/   Atlassian Forge app for Jira integration
docs/             Additional documentation (Jira integration guide)
infra/            AWS CDK infrastructure stacks (TypeScript)
openspec/         Generated implementation specs from agent runs
scripts/          Utility scripts (Stripe setup)
services/         Rust Lambda services (gateway + worker)
```

### Infrastructure Stacks

| Stack | CDK File | Description |
|-------|----------|-------------|
| **Database** | `infra/lib/database-stack.ts` | DynamoDB tables (main, runs, analytics) with KMS encryption and GSIs |
| **Storage** | `infra/lib/storage-stack.ts` | S3 bucket for run artifacts, openspec files, and tenant data |
| **API** | `infra/lib/api-stack.ts` | Gateway Lambda, HTTP API Gateway, SQS queues (tickets, CI fix, feedback), custom domain |
| **Worker** | `infra/lib/worker-stack.ts` | Worker Lambda with SQS event sources, Bedrock access, 15-minute timeout |
| **Email** | `infra/lib/email-stack.ts` | SES domain identity, configuration set, and templated email definitions |
| **Billing** | `infra/lib/billing-stack.ts` | S3 bucket for invoice PDFs with 7-year retention |
| **Monitoring** | `infra/lib/monitoring-stack.ts` | CloudWatch alarms (gateway errors, worker errors, DLQ depth), SNS alerts, dashboard |
| **Frontend** | `infra/lib/frontend-stack.ts` | S3 + CloudFront distribution for the `app.coderhelm.com` SPA |

### Services

#### Gateway

The Gateway is a **Rust / Axum** application deployed as a single Lambda function behind an HTTP API Gateway at `api.coderhelm.com`.

- **Webhooks** — `POST /webhooks/github`, `POST /webhooks/jira`, `POST /webhooks/stripe` (signature-verified, public)
- **Auth** — `GET /auth/login`, `GET /auth/callback`, `POST /auth/logout` (GitHub OAuth flow)
- **REST API** — `/api/*` routes protected by JWT middleware: runs, repos, billing, integrations, settings, plans, infrastructure

Incoming issue/ticket events are validated, enqueued to SQS, and acknowledged immediately.

#### Worker

The Worker is a **Rust async Lambda** that consumes messages from three SQS queues (tickets, CI fix, feedback). It calls **Amazon Bedrock** for LLM inference and interacts with the GitHub API to read repositories and commit code.

- **Timeout**: 15 minutes (Lambda maximum)
- **Concurrency**: up to 10 concurrent ticket runs, 5 CI fix, 5 feedback
- **Event sources**: `SqsEventSource` with `batchSize: 1` for isolation

### Agent Pipeline

When a new ticket arrives, the Worker runs five sequential passes:

1. **Triage** — Classifies the issue by complexity (`simple` / `medium` / `complex`), assesses clarity, and generates a summary.
2. **Plan** — Researches the repository using read-only tools and produces four openspec documents: `proposal.md`, `design.md`, `tasks.md`, and `spec.md`.
3. **Implement** — Creates a feature branch, executes each task from the checklist using read/write tools, and commits the changes.
4. **Review** — Diffs the branch against `main` and reviews for correctness, completeness, convention compliance, bugs, and security — applying fixes if needed.
5. **PR** — Generates a pull request description via LLM and opens a draft PR that references the original issue.

---

## Integrations

### GitHub App

CoderHelm connects to GitHub as a [GitHub App](https://docs.github.com/en/apps).

- **Webhook URL**: `https://api.coderhelm.com/webhooks/github`
- **OAuth flow**: `GET /auth/login` redirects to GitHub; `GET /auth/callback` exchanges the code for a JWT
- **Required permissions**: Contents (Read & Write), Issues (Read & Write), Pull requests (Read & Write), Checks (Read), Metadata (Read)
- **Event subscriptions**: Issues, Issue comment, Pull request review, Check run, Installation

### Jira

CoderHelm accepts Jira events via a lightweight webhook approach — no full Atlassian app installation required. Configure a Jira Automation rule to `POST` issue-created and issue-updated events to the `/webhooks/jira` endpoint. Payloads include `repo_owner`, `repo_name`, and `installation_id` to map Jira tickets to GitHub repositories.

See [docs/jira-integration.md](docs/jira-integration.md) for the full setup guide, payload schema, and validation workflow.

---

## Getting Started

### Prerequisites

| Tool | Version | Install |
|------|---------|--------|
| Rust | 1.80+ | `rustup update stable` |
| Node.js | 22+ | [nodejs.org](https://nodejs.org) |
| AWS CDK | v2 | `npm install -g aws-cdk` |
| AWS CLI | latest | [aws.amazon.com/cli](https://aws.amazon.com/cli/) |

### Local Development

**Services (Rust)**

Both services are Lambda functions — run them locally with `cargo lambda watch` (requires [cargo-lambda](https://www.cargo-lambda.info/)):

```bash
# gateway
cd services/gateway
cargo lambda watch

# worker
cd services/worker
cargo lambda watch
```

**Infra (CDK)**

```bash
cd infra
npm install
npx cdk synth   # validates all stacks without deploying
```

**Jira Forge app**

```bash
cd coderhelm-jira
npm install
forge deploy    # deploys to your Forge dev environment
forge tunnel    # proxies requests to localhost for development
```

**Required env vars** — set these before running services locally:

| Variable | Description |
|----------|-------------|
| `MODEL_ID` | Bedrock model ID (e.g. `us.anthropic.claude-opus-4-6-v1`) |
| `STAGE` | `dev` or `prod` |

Secrets (GitHub App credentials, JWT secret) are loaded from AWS Secrets Manager at `coderhelm/<stage>/secrets` — ensure local AWS credentials have access to that secret.

### Quick Deploy

```bash
MODEL_ID="us.anthropic.claude-opus-4-6-v1" cdk deploy --all
```

This builds and deploys all eight CDK stacks. `MODEL_ID` is required — there is no default.

---

## CI / CD

### CI (`ci.yml`)

Runs on every push and pull request to `main`:

- **Gateway** — `cargo fmt --check` and `cargo clippy -- -D warnings`
- **Worker** — `cargo fmt --check` and `cargo clippy -- -D warnings`
- **Infra** — `npx cdk synth --quiet` (validates all CDK stacks)

### Deploy (`deploy.yml`)

Runs on push to `main`:

1. Builds both Rust services using `cargo-zigbuild` targeting `aarch64-unknown-linux-musl`
2. Uploads `gateway.zip` and `worker.zip` as artifacts
3. Runs `scripts/setup-stripe.sh` (idempotent)
4. Deploys all CDK stacks with `npx cdk deploy --all --require-approval never`

---

## Links

| Resource | URL |
|----------|-----|
| Jira Integration | [docs/jira-integration.md](docs/jira-integration.md) |
| Dashboard | [app.coderhelm.com](https://app.coderhelm.com) |
| API | [api.coderhelm.com](https://api.coderhelm.com) |
