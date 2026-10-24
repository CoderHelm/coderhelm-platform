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

## Run it in your own AWS account

```sh
npx -y github:CoderHelm/coderhelm-platform init     # a few questions → coderhelm.config.json
npx -y github:CoderHelm/coderhelm-platform deploy   # deploy, upload the dashboard, create your GitHub App
```

No Rust, Docker or domain needed: releases ship built Lambda code, and without a domain CoderHelm runs on a CloudFront URL. Models through your teams' Anthropic keys or Bedrock in your account. Guides: [`docs/self-host/`](docs/self-host/index.md).

## Architecture

```
services/gateway/    Rust Lambda — GitHub & Jira webhooks, OAuth, REST API, SQS dispatch
services/worker/     Rust Lambda — AI orchestration (triage → plan → implement → review)
coderhelm-jira/      Atlassian Forge app — bridges Jira Cloud events to the gateway
infra/               AWS CDK v2 — all cloud infrastructure, built from one config file (infra/config/)
installer/           coderhelm-selfhost: install, upgrade, health, backup, uninstall
selfhost/            Release metadata: migration notes, the dashboard version, install.sh
docs/self-host/      Self-hosting guides
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

Synthesise the CDK app (the hosted deployment's config by default; `-c config=<file>` for another):

```bash
cd infra && npx cdk synth
npm test   # config validation and synth tests
```

## Config and secrets

Every deployment is built from one JSON config: the hosted one is `infra/config/hosted.prod.json`, a self-hosted one is the `coderhelm.config.json` the installer writes. See [docs/self-host/config-reference.md](docs/self-host/config-reference.md).

Sensitive values are in **AWS Secrets Manager** at `coderhelm/<stage>/secrets`, a JSON object with `github_app_id`, `github_private_key`, `github_webhook_secret`, `github_client_id`, `github_client_secret` and `jwt_secret` (plus optional `google_client_id` / `google_client_secret`). Self-hosted installs get it created and filled by the installer.

## Deploy

`.github/workflows/deploy.yml` deploys `main` to the hosted account (batched every 10 minutes), with `infra/config/hosted.prod.json`.

A `vX.Y.Z` tag runs `.github/workflows/release.yml`, which publishes a self-host release: prebuilt Lambda zips, the dashboard, the source, the installer, `install.sh` and the Launch Stack template, with checksums and build provenance.
