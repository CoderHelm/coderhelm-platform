# CoderHelm Platform

Autonomous AI coding agent platform — webhook receiver, AI orchestration worker, and AWS CDK infrastructure.

## Architecture

```
services/gateway/   API Gateway Lambda — receives GitHub/Jira webhooks, handles OAuth & auth, enqueues jobs to SQS
services/worker/    Worker Lambda — dequeues jobs from SQS, orchestrates AI passes via Bedrock, and opens GitHub PRs
infra/              AWS CDK v2 stacks — defines all cloud infrastructure (API Gateway, Lambdas, SQS, DynamoDB, S3, etc.)
docs/               Additional documentation and integration guides
```

### Data-Flow Diagram

```
GitHub/Jira webhook event
        │
        ▼
  Gateway Lambda  ──▶  validates signature & auth
        │
        ▼
      SQS Queue
        │
        ▼
  Worker Lambda   ──▶  AI orchestration (Bedrock)
        │
        ▼
  GitHub PR / comment pushed back to the repository
```

## Prerequisites

| Tool | Minimum Version | Notes |
|---|---|---|
| Rust | 1.80+ | `rustup update stable` |
| `cargo-lambda` | latest | `cargo install cargo-lambda` |
| `cargo-zigbuild` | latest | `pip3 install cargo-zigbuild` |
| Zig | latest | installed via `mlugg/setup-zig` in CI, or manually |
| Node.js | LTS 22+ | required for AWS CDK |
| AWS CDK v2 | latest | `npm install -g aws-cdk` |
| AWS CLI | v2 | configured with valid credentials |

## Setup

Install CDK dependencies:

```bash
cd infra && npm ci
```

Build the Rust workspace:

```bash
cargo zigbuild --workspace --release --target aarch64-unknown-linux-musl
```

Synthesise the CDK app:

```bash
cd infra && npx cdk synth
```

## Environment Variables

| Variable | Used By | Description |
|---|---|---|
| `MODEL_ID` | Worker | Bedrock model ID (e.g. `us.anthropic.claude-opus-4-6-v1`). **No default — must be set before deploying.** |
| `STAGE` | Gateway, Worker | Deployment stage: `dev` or `prod`. Defaults to `dev` if unset. |

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

The following keys are **optional** and only required if the corresponding integrations are enabled:

```json
{
  "stripe_secret_key": "<stripe-secret-key>",
  "stripe_publishable_key": "<stripe-publishable-key>",
  "stripe_webhook_secret": "<stripe-webhook-signing-secret>",
  "stripe_price_id": "<stripe-price-id>",
  "stripe_overage_price_id": "<stripe-overage-price-id>",
  "jira_webhook_secret": "<jira-webhook-secret>"
}
```

## Deploy

Pushing to `main` automatically triggers `.github/workflows/deploy.yml`, which builds the Rust workspace, packages the Lambda artifacts, and runs `cdk deploy --all` in the target AWS account.

To deploy manually:

```bash
cd infra && npx cdk deploy --all --require-approval never
```

The following environment variables must be set for manual deploys:

- `MODEL_ID` — Bedrock model ID (required)
- `GATEWAY_ZIP` — path to the packaged gateway Lambda zip
- `WORKER_ZIP` — path to the packaged worker Lambda zip
- `CDK_DEFAULT_ACCOUNT` — AWS account ID
- `CDK_DEFAULT_REGION` — AWS region (e.g. `us-east-1`)
