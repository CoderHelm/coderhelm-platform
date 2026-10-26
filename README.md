# CoderHelm Platform

The backend for [CoderHelm](https://coderhelm.com), the AI coding agent that turns GitHub issues and Jira tickets into pull requests and then stays on the code: it fixes its own failing CI, answers review comments, reviews your team's pull requests, turns alerts into fixes and answers questions about your code in Chat.

[Docs](https://docs.coderhelm.com) · [Self-host install guide](https://docs.coderhelm.com/self-host/install/) · [API](https://docs.coderhelm.com/api/) · [Dashboard and sites](https://github.com/CoderHelm/coderhelm)

## How a run works

1. Someone assigns a GitHub issue or Jira ticket to CoderHelm, adds the `coderhelm` label or comments `/coderhelm`.
2. The **gateway** checks the webhook's signature, finds the team and repository, and puts a job on the queue.
3. The **worker** runs the job: triage, a plan with the files it will touch, the change on a `coderhelm/` branch, your build and tests in a sandbox with no credentials, up to three rounds of self review and a security check.
4. It opens the pull request, then keeps working on it: failing checks, review comments, conflicts and, with auto merge on, the deploy check after a person approves.

Every agent call goes through the team's agent permissions (allow, ask or deny), so anything risky waits for a person.

## What else runs here

- **Chat and knowledge:** Chat reads the team's repositories itself (overview, files, code search, code graph) and cites real file paths and lines; knowledge sources sync from Notion, Confluence, Google Drive, Sanity, GitHub, websites and uploaded files.
- **Reviewer:** reviews any pull request with the review label, with line suggestions, a risk level and a new review on every push.
- **Tests, releases, logs and alerts:** flaky test detection and quarantine, release tags and notes, log findings every six hours, and alert routes for CloudWatch alarms through SNS.
- **Team controls:** roles and custom roles, monthly token limits, usage and audit records, team API keys for the [public API](https://docs.coderhelm.com/api/) (its OpenAPI document is generated from the handlers) and a weekly summary email.
- **Plugins:** packages, runs, approvals and a private marketplace, behind a team flag while the dashboard side is finished.

## Run it in your own AWS account

The [install guide](https://docs.coderhelm.com/self-host/install/) installs CoderHelm from your browser: one click opens an AWS quick create page, the guide follows the install, creates your GitHub App in one click, checks model access on Amazon Bedrock and helps with your own domain. From a terminal:

```sh
npx -y github:CoderHelm/coderhelm-platform init     # a few questions, writes coderhelm.config.json
npx -y github:CoderHelm/coderhelm-platform deploy   # deploys, uploads the dashboard, creates your GitHub App
npx -y github:CoderHelm/coderhelm-platform doctor   # checks the install and says how to fix what fails
```

No Rust, Docker or domain needed: releases ship built Lambda code, and without a domain CoderHelm runs on a CloudFront address. Models come through Amazon Bedrock in your account or each team's Anthropic API key. Installs can update themselves in a maintenance window. Guides: [`docs/self-host/`](docs/self-host/index.md), also at [docs.coderhelm.com/self-host](https://docs.coderhelm.com/self-host/).

The first public self-host release is not out yet. Until it is published on the [releases page](https://github.com/CoderHelm/coderhelm-platform/releases), an install cannot finish.

## Architecture

```
services/gateway/    Rust Lambda: webhooks (GitHub, Jira, SNS, knowledge), OAuth, the REST API, queue dispatch
services/worker/     Rust Lambda: runs, reviews, plans, CI fixes and the other agent work
services/common/     Shared Rust code: models, knowledge, agent permissions, plugins, notifications
lambda/              Smaller Lambdas: log analyzer, updater, MCP proxy, stuck run cleanup, sign-in messages
coderhelm-jira/      Atlassian Forge app that sends Jira Cloud events to the gateway
infra/               AWS CDK v2: all cloud infrastructure, built from one config file (infra/config/)
installer/           coderhelm-selfhost: install, doctor, upgrade, backup, uninstall
selfhost/            Release metadata: migration notes, the dashboard version, install.sh, the launch bootstrap
plugins/             Example plugin packages
docs/self-host/      Self-hosting guides
```

### Data flow

```
GitHub issue, Jira ticket or alert
        |
  Gateway Lambda    checks the signature, finds the team and repository
        |
     SQS queue
        |
  Worker Lambda     triage, plan, implement, test, self review (Claude)
        |
  Pull request on GitHub, then CI fixes, review replies and the deploy check
```

## Tech stack

| Component | Technology |
|---|---|
| Gateway and worker | Rust (async, compiled for ARM64 Lambda) |
| Models | Claude (Anthropic API or Amazon Bedrock) |
| Infrastructure | AWS CDK v2 (TypeScript) |
| Compute | AWS Lambda (ARM64, up to 15 minutes) |
| Queue | Amazon SQS (with a dead letter queue) |
| Storage | Amazon DynamoDB and S3 |
| Jira integration | Atlassian Forge (Node.js) |
| CI/CD | GitHub Actions and CDK deploy |

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

## License

CoderHelm is licensed under the [Functional Source License, Version 1.1, with Apache 2.0 as the future license](LICENSE.md) (FSL-1.1-ALv2). In plain words:

- You can run it, change it and self-host it for your own company.
- You can't offer it, or something built from it, as a competing hosted product.
- Each release becomes Apache 2.0 two years after it ships.

The full terms are in [LICENSE.md](LICENSE.md).
