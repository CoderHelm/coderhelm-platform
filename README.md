# CoderHelm

[![CI](https://github.com/CoderHelm/coderhelm-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/CoderHelm/coderhelm-platform/actions/workflows/ci.yml)

AI coding agent that turns issues into pull requests.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Gateway](#gateway)
  - [Worker](#worker)
  - [Infrastructure](#infrastructure)
  - [Jira App](#jira-app)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Development](#development)
  - [Building](#building)
  - [Running Tests](#running-tests)
  - [Code Style](#code-style)
- [Deployment](#deployment)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

CoderHelm is an AI coding agent that automates the journey from issue to pull request. When an issue is created (on GitHub or Jira), CoderHelm:

1. **Triages** the issue to determine scope and priority.
2. **Plans** the implementation by analyzing the codebase and producing a step-by-step approach.
3. **Implements** the changes across multiple files with a multi-pass agent pipeline.
4. **Self-reviews** the generated code for correctness and style.
5. **Opens a pull request** with a clear description of what changed and why.
6. **Fixes CI** failures automatically when the pipeline reports errors.
7. **Incorporates feedback** from human reviewers and iterates.

## Architecture

The system is composed of four main components: a Gateway that handles all inbound traffic, a Worker that runs the AI agent pipeline, Infrastructure defined as CDK stacks, and a Jira App for Atlassian integration.

### Gateway

A Rust service built with Axum, deployed as an AWS Lambda function URL. It handles:

- **API routes** for the dashboard and integrations.
- **Webhooks** from GitHub, Jira, and Stripe.
- **OAuth** flows and JWT-based authentication.

Source: `services/gateway/`

### Worker

A Rust service triggered by SQS messages, deployed as an AWS Lambda. It runs the multi-pass agent pipeline:

**triage → plan → implement → review → PR → CI fix → feedback**

Each pass uses an LLM (via Amazon Bedrock) to analyze context and produce code changes. The worker also handles onboarding and infrastructure analysis passes.

Source: `services/worker/`

### Infrastructure

AWS CDK stacks written in TypeScript that provision all cloud resources:

- **Database** — DynamoDB tables for tenants, jobs, and state.
- **Storage** — S3 buckets for artifacts and assets.
- **API** — Lambda function URL and CloudFront distribution for the gateway.
- **Worker** — SQS queue and Lambda for the agent pipeline.
- **Email** — SES configuration for notifications.
- **Billing** — Stripe integration resources.
- **Monitoring** — CloudWatch alarms and dashboards.
- **Frontend** — Static site hosting for the dashboard.

Source: `infra/`

### Jira App

An Atlassian Forge app that enables Jira integration. It allows teams to keep Jira as their source of work while CoderHelm processes issues automatically.

Source: `coderhelm-jira/`

## Project Structure

```
coderhelm-platform/
├── .github/workflows/     # CI and deploy pipelines
│   ├── ci.yml             # Lint, test, and CDK synth
│   └── deploy.yml         # Build and deploy to AWS
├── coderhelm-jira/        # Atlassian Forge app (Jira integration)
│   ├── manifest.yml       # Forge app manifest
│   └── src/               # Forge app source
├── docs/                  # Additional documentation
│   └── jira-integration.md
├── infra/                 # AWS CDK infrastructure
│   ├── bin/coderhelm.ts   # CDK app entry point
│   └── lib/               # Stack definitions
├── scripts/               # Utility scripts
├── services/              # Rust services (Cargo workspace)
│   ├── gateway/           # API gateway Lambda
│   │   └── src/
│   └── worker/            # Agent worker Lambda
│       └── src/
├── README.md
├── SETUP.md               # Detailed setup and deploy guide
└── rustfmt.toml           # Rust formatting configuration
```

## Prerequisites

- **Rust** 1.80+ — `rustup update stable`
- **Node.js** 22+ — for CDK and the dashboard
- **AWS CDK** v2 — `npm install -g aws-cdk`
- **AWS CLI** — configured with appropriate credentials

See [SETUP.md](SETUP.md) for full details on credentials and secrets configuration.

## Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/CoderHelm/coderhelm-platform.git
   cd coderhelm-platform
   ```

2. **Install dependencies**
   ```bash
   cd infra && npm ci && cd ..
   ```

3. **Configure secrets** — create the required entries in AWS Secrets Manager and set environment variables as described in [SETUP.md](SETUP.md).

4. **Deploy**
   ```bash
   MODEL_ID="your-model-id" cdk deploy --all
   ```

See [SETUP.md](SETUP.md) for the complete setup guide including GitHub App registration.

## Development

### Building

```bash
# Build the entire Rust workspace
cd services
cargo build --workspace

# Build a single service
cd services/gateway
cargo build
```

### Running Tests

```bash
# Test the entire workspace
cd services
cargo test --workspace

# Test a single service
cd services/gateway
cargo test
```

### Code Style

Rust code is formatted with `rustfmt` and linted with `clippy`:

```bash
# Check formatting
cargo fmt --check

# Run lints (warnings are errors in CI)
cargo clippy -- -D warnings
```

CDK infrastructure is written in TypeScript:

```bash
cd infra
npx cdk synth --quiet
```

## Deployment

The project uses two GitHub Actions workflows:

- **CI** (`.github/workflows/ci.yml`) — runs on every push and PR to `main`. Checks formatting, lints, tests, and synthesizes CDK stacks.
- **Deploy** (`.github/workflows/deploy.yml`) — runs on pushes to `main`. Builds Rust services for `aarch64-unknown-linux-musl`, packages Lambda artifacts, and runs `cdk deploy --all`.

To deploy manually:

```bash
MODEL_ID="your-model-id" cdk deploy --all
```

See [SETUP.md](SETUP.md) for environment variables and deployment details.

## Documentation

- [SETUP.md](SETUP.md) — prerequisites, secrets configuration, environment variables, and deploy instructions.
- [docs/jira-integration.md](docs/jira-integration.md) — Jira integration quickstart, webhook endpoints, and sample payloads.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request. Ensure all CI checks pass before requesting review.

## License

All rights reserved. See the repository for license details.
