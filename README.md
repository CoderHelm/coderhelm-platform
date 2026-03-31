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
GitHub webhook event
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
| **Native path** | | |
| Rust | 1.80+ | `rustup update stable` |
| `cargo-lambda` | latest | `cargo install cargo-lambda` |
| `cargo-zigbuild` | latest | `pip3 install cargo-zigbuild` |
| Zig | latest | installed via `mlugg/setup-zig` in CI, or manually |
| **Docker path (alternative)** | | |
| Docker | Engine 24+ | Alternative to the native Rust/Zig toolchain above |
| **Common** | | |
| Node.js | LTS 20+ | required for AWS CDK |
| AWS CDK v2 | latest | `npm install -g aws-cdk` |
| AWS CLI | v2 | configured with valid credentials |

## Docker Build

As an alternative to installing Rust, Zig, and `cargo-zigbuild` locally, you can build the Lambda binaries with Docker.

### Build both services

```bash
docker compose build
```

### Extract the bootstrap binaries

The compiled `bootstrap` binary lives at `/bootstrap` inside each image. Extract them to the paths CDK expects:

```bash
# Gateway
mkdir -p services/gateway/target/lambda/gateway
id=$(docker create coderhelm-gateway:local)
docker cp "$id:/bootstrap" services/gateway/target/lambda/gateway/bootstrap
docker rm "$id"

# Worker
mkdir -p services/worker/target/lambda/worker
id=$(docker create coderhelm-worker:local)
docker cp "$id:/bootstrap" services/worker/target/lambda/worker/bootstrap
docker rm "$id"
```

This is equivalent to the native `cargo zigbuild` path and produces the same aarch64 musl-linked binaries.

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
