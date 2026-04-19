# CoderHelm Platform

Backend services for [CoderHelm](https://coderhelm.com) — the autonomous AI coding agent that turns tickets into pull requests.

## How it works

1. A developer assigns a GitHub issue or Jira ticket to CoderHelm
2. The **Gateway** receives the webhook, resolves the team/repo, uploads any image attachments to S3, and enqueues a job
3. The **Worker** picks up the job and runs a multi-pass AI pipeline:
   - **Triage** — classifies complexity, selects target repo (with image context if available)
   - **Plan** — generates a task breakdown using codebase context and MCP tools
   - **Implement** — writes code, creates/edits files, commits to a feature branch
   - **CI Check** — waits for GitHub Actions, auto-fixes failures (up to 3 attempts)
   - **Self-Review** — reviews its own diff, fixes issues before requesting human review
4. A draft PR is opened with a summary, linked ticket, and progress notes

## Architecture

```
services/gateway/    Rust Lambda — GitHub & Jira webhooks, OAuth, REST API, SQS dispatch
services/worker/     Rust Lambda — AI orchestration (triage → plan → implement → review)
coderhelm-jira/      Atlassian Forge app — bridges Jira Cloud events to the gateway
infra/               AWS CDK v2 — all cloud infrastructure (API Gateway, Lambda, SQS, DynamoDB, S3, CloudFront)
docs/                Integration guides and internal docs
```

### Data Flow

```
GitHub issue / Jira ticket (with optional image attachments)
        │
        ▼
  Gateway Lambda  ──▶  validates webhook, resolves team & repo, uploads images to S3
        │
        ▼
      SQS Queue
        │
        ▼
  Worker Lambda   ──▶  triage → plan → implement → CI check → self-review (Anthropic Claude)
        │
        ▼
  Draft PR opened on GitHub with summary & linked ticket
```

## Tech Stack

| Component | Technology |
|---|---|
| Gateway & Worker | Rust (async, compiled to ARM64 Lambda) |
| AI Provider | Anthropic Claude (Opus, Sonnet, Haiku) |
| Infrastructure | AWS CDK v2 (TypeScript) |
| Compute | AWS Lambda (ARM64, up to 15 min timeout) |
| Queue | Amazon SQS (with DLQ) |
| Storage | Amazon DynamoDB + S3 |
| Jira Integration | Atlassian Forge (Node.js) |
| CI/CD | GitHub Actions → CDK deploy |

## Prerequisites

| Tool | Minimum Version | Notes |
|---|---|---|
| Rust | 1.80+ | `rustup update stable` |
| `cargo-lambda` | latest | `cargo install cargo-lambda` |
| `cargo-zigbuild` | latest | `pip3 install cargo-zigbuild` |
| Zig | latest | installed via `mlugg/setup-zig` in CI, or manually |
| Node.js | LTS 20+ | required for AWS CDK |
| AWS CDK v2 | latest | `npm install -g aws-cdk` |
| AWS CLI | v2 | configured with valid credentials |

## Setup

Install CDK dependencies:

```bash
cd infra && npm ci
```

Build the Rust workspace:

```bash
cd services && cargo zigbuild --workspace --release --target aarch64-unknown-linux-musl
```

Synthesise the CDK app:

```bash
cd infra && npx cdk synth
```

## Secrets

All sensitive values are stored in **AWS Secrets Manager** under the path `coderhelm/<stage>/secrets` (e.g. `coderhelm/prod/secrets`).

The secret must be a JSON object with the following keys:

```json
{
  "github_app_id": "<your-github-app-id>",
  "github_private_key": "<PEM private key>",
  "github_webhook_secret": "<webhook-secret>",
  "github_client_id": "<oauth-client-id>",
  "github_client_secret": "<oauth-client-secret>",
  "jwt_secret": "<random-256-bit-hex>"
}
```

## Deploy

Pushing to `main` automatically triggers `.github/workflows/deploy.yml`, which builds the Rust workspace, packages the Lambda artifacts, and runs `cdk deploy --all` in the target AWS account.
