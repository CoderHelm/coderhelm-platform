# CoderHelm Platform

Backend services for [CoderHelm](https://coderhelm.com) — the autonomous AI coding agent that turns tickets into pull requests.

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
GitHub issue / Jira ticket
        │
        ▼
  Gateway Lambda  ──▶  validates webhook, resolves team & repo, uploads attachments to S3
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
