# CoderHelm

CoderHelm is an AI-powered autonomous coding agent that turns GitHub issues and Jira tickets into pull requests. When a ticket is assigned, CoderHelm creates a branch, implements the change, runs your CI, self-reviews the diff, and opens a draft PR — all without human intervention.

## Architecture

The platform is composed of four main components:

| Component | Description |
|-----------|-------------|
| **Gateway** | Rust Lambda (Axum + `lambda_http`) behind API Gateway. Handles webhooks, auth, REST API, and enqueues work onto SQS. |
| **Worker** | Rust Lambda triggered by SQS. Orchestrates multi-pass AI coding: triage → plan → implement → review → PR. |
| **Frontend** | Next.js static-export dashboard deployed to S3 + CloudFront at `app.coderhelm.com`. |
| **Jira Forge App** | Atlassian Forge app that forwards Jira issue events to the gateway webhook. |

## Project Structure

```
.
├── services/
│   ├── gateway/          # Gateway Lambda (Rust / Axum)
│   └── worker/           # Worker Lambda (Rust / SQS consumer)
├── dashboard/            # Next.js frontend (static export → S3)
├── coderhelm-jira/       # Atlassian Forge app for Jira integration
│   ├── src/              #   Forge functions (trigger + admin resolver)
│   └── resources/admin/  #   Admin settings UI (Vite + React)
├── infra/                # AWS CDK stacks (API, Worker, DB, Storage, etc.)
│   ├── bin/              #   CDK app entrypoint
│   └── lib/              #   Stack definitions
├── docs/                 # Additional documentation
├── scripts/              # Deployment and setup scripts
├── .github/workflows/    # CI and deploy pipelines
└── .env.example          # Environment variable template
```

> **Note:** The `dashboard/` directory contains the Next.js frontend. It may live in a separate repository or be gitignored in this repo. The CDK `frontend-stack.ts` deploys from `dashboard/out` (the Next.js static export output). If you don't see `dashboard/` locally, check with your team for the frontend repo location.

## Prerequisites

- **Rust 1.80+** — `rustup update stable`
- **cargo-lambda** — `cargo install cargo-lambda` ([docs](https://www.cargo-lambda.info/))
- **Node.js 22+** — for CDK, dashboard, and Forge app
- **npm** — ships with Node.js
- **AWS CLI** — configured with credentials for the dev account
- **Forge CLI** *(optional, for Jira app)* — `npm install -g @forge/cli`

## Local Development

Local development uses real AWS dev-stage resources (DynamoDB, SQS, S3, Secrets Manager, Bedrock) with [`cargo-lambda`](https://www.cargo-lambda.info/) to emulate the Lambda runtime on your machine. This means you need valid AWS credentials configured for the dev account and the `coderhelm/dev/secrets` entry in Secrets Manager.

### Gateway

The gateway uses `lambda_http::run()` with an Axum router. `cargo-lambda watch` emulates the Lambda HTTP runtime locally.

**Setup:**

```bash
# 1. Clone the repository
git clone https://github.com/CoderHelm/coderhelm-platform.git
cd coderhelm-platform

# 2. Copy the env template and fill in real values
cp .env.example .env
# Edit .env — set TABLE_NAME, queue URLs, etc. to your dev-stage resources

# 3. Start the gateway
cd services/gateway
cargo lambda watch
```

The gateway will be available at **`http://localhost:9000`**.

**Test with curl:**

```bash
# Health check (requires auth in production, but useful for connectivity)
curl -i http://localhost:9000/api/health

# GitHub webhook (send a test payload)
curl -X POST http://localhost:9000/webhooks/github \
  -H "Content-Type: application/json" \
  -H "X-GitHub-Event: ping" \
  -d '{"zen": "testing"}'
```

**Gateway Environment Variables:**

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `STAGE` | No | `dev` | Deployment stage (`dev` or `prod`) |
| `TABLE_NAME` | **Yes** | — | DynamoDB main table name |
| `RUNS_TABLE_NAME` | **Yes** | — | DynamoDB runs table name |
| `ANALYTICS_TABLE_NAME` | **Yes** | — | DynamoDB analytics table name |
| `BUCKET_NAME` | **Yes** | — | S3 storage bucket name |
| `INVOICE_BUCKET_NAME` | No | `coderhelm-prod-invoices` | S3 bucket for invoice PDFs |
| `TICKET_QUEUE_URL` | **Yes** | — | SQS ticket queue URL |
| `CI_FIX_QUEUE_URL` | **Yes** | — | SQS CI fix queue URL |
| `FEEDBACK_QUEUE_URL` | **Yes** | — | SQS feedback queue URL |
| `DLQ_URL` | No | `""` | SQS dead-letter queue URL |
| `SECRETS_NAME` | No | `coderhelm/prod/secrets` | AWS Secrets Manager secret name |
| `SES_FROM_ADDRESS` | No | `notifications@coderhelm.com` | SES sender address |
| `SES_TEMPLATE_PREFIX` | No | `coderhelm-prod` | SES template name prefix |
| `MODEL_ID` | No | `us.anthropic.claude-sonnet-4-6` | Bedrock model ID for plan chat |
| `RUST_LOG` | No | — | Log level filter (e.g. `info`, `debug`) |

> **Tip:** For local dev, set `SECRETS_NAME=coderhelm/dev/secrets` to load dev-stage secrets.

### Worker

The worker is an SQS-triggered Lambda that uses `lambda_runtime::run()` with `service_fn`. Locally, `cargo-lambda watch` emulates the Lambda runtime and you invoke it manually with sample event payloads.

**Setup:**

```bash
# From the repository root (with .env already configured)
cd services/worker
cargo lambda watch
```

The worker will listen at **`http://localhost:9000`** (use a different port if the gateway is already running by passing `--port 9001`).

**Invoke with a sample SQS event:**

Create a file `test-event.json` with a sample SQS event containing a `TicketMessage`:

```json
{
  "Records": [
    {
      "messageId": "test-msg-001",
      "receiptHandle": "test-handle",
      "body": "{\"type\":\"ticket\",\"tenant_id\":\"TENANT#12345\",\"installation_id\":67890,\"source\":\"github\",\"ticket_id\":\"GH-1\",\"title\":\"Fix login bug\",\"body\":\"The login page returns 500 on invalid email\",\"repo_owner\":\"your-org\",\"repo_name\":\"your-repo\",\"issue_number\":1,\"sender\":\"octocat\"}",
      "attributes": {
        "ApproximateReceiveCount": "1",
        "SentTimestamp": "1234567890",
        "SenderId": "test",
        "ApproximateFirstReceiveTimestamp": "1234567890"
      },
      "messageAttributes": {},
      "md5OfBody": "test",
      "eventSource": "aws:sqs",
      "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:coderhelm-dev-tickets",
      "awsRegion": "us-east-1"
    }
  ]
}
```

Then invoke:

```bash
cargo lambda invoke --data-file test-event.json
```

**Worker Environment Variables:**

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `STAGE` | No | `dev` | Deployment stage (`dev` or `prod`) |
| `TABLE_NAME` | **Yes** | — | DynamoDB main table name |
| `RUNS_TABLE_NAME` | **Yes** | — | DynamoDB runs table name |
| `ANALYTICS_TABLE_NAME` | **Yes** | — | DynamoDB analytics table name |
| `BUCKET_NAME` | **Yes** | — | S3 storage bucket name |
| `SECRETS_NAME` | No | `coderhelm/prod/secrets` | AWS Secrets Manager secret name |
| `MODEL_ID` | **Yes** | — | Bedrock model ID (e.g. `us.anthropic.claude-opus-4-6-v1`) |
| `SES_FROM_ADDRESS` | No | `notifications@coderhelm.com` | SES sender address |
| `SES_TEMPLATE_PREFIX` | No | `coderhelm-prod` | SES template name prefix |
| `RUST_LOG` | No | — | Log level filter (e.g. `info`, `debug`) |

> **Tip:** For local dev, set `SECRETS_NAME=coderhelm/dev/secrets` and `MODEL_ID` to the Bedrock model you have access to.

### Frontend (Next.js)

The dashboard lives in `dashboard/` and is a static-export Next.js app served via S3 + CloudFront in production.

**Setup:**

```bash
cd dashboard
npm install
npm run dev
```

The dashboard will be available at **`http://localhost:3000`** by default.

**Environment variable:**

Set `NEXT_PUBLIC_API_URL` to point at your local gateway:

```bash
# In dashboard/.env.local
NEXT_PUBLIC_API_URL=http://localhost:9000
```

**Production build:**

The CDK `frontend-stack.ts` deploys from `dashboard/out`. To produce this output locally:

```bash
npm run build    # runs next build
# Next.js static export outputs to dashboard/out/
```

The `frontend-stack.ts` uses `s3deploy.Source.asset("../dashboard/out")` to upload the static files to S3 and invalidate the CloudFront distribution.

### Jira Forge App

The Forge app (`coderhelm-jira/`) forwards Jira issue events to the CoderHelm gateway. It consists of a trigger function, an admin settings resolver, and a Vite + React admin UI.

**Prerequisites:**

- An [Atlassian developer account](https://developer.atlassian.com/)
- Forge CLI installed and logged in: `npm install -g @forge/cli && forge login`

**Local dev workflow:**

```bash
# 1. Install Forge function dependencies
cd coderhelm-jira
npm install

# 2. Build the admin UI (Vite + React)
cd resources/admin
npm install
npm run build
cd ../..

# 3. Deploy to the development environment
forge deploy -e development

# 4. Install onto your Jira site (first time only)
forge install

# 5. Start the tunnel for live reloading
forge tunnel
```

The admin UI uses Vite + React and outputs to `resources/admin/build/`, which is the directory referenced by `manifest.yml` (`resources[0].path: resources/admin/build`).

**Label-based repo mapping:**

When testing the Jira integration, issues need a label to tell CoderHelm which repository to target:

- **`coderhelm`** — bare label; CoderHelm auto-resolves the target repo from the tenant's configured repositories
- **`coderhelm:owner/repo`** — explicit label; maps directly to a specific `owner/repo` (e.g. `coderhelm:acme/backend`)

## Environment Variables

This section is a comprehensive reference for all environment variables across components. The source of truth is the `Config` struct in each service's `models.rs` and the CDK stack definitions in `infra/lib/`.

### Gateway

Defined in `services/gateway/src/models.rs` → `Config::from_env()` and `infra/lib/api-stack.ts`.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `STAGE` | No | `dev` | Deployment stage |
| `TABLE_NAME` | **Yes** | — | DynamoDB main table name |
| `RUNS_TABLE_NAME` | **Yes** | — | DynamoDB runs table name |
| `ANALYTICS_TABLE_NAME` | **Yes** | — | DynamoDB analytics table name |
| `BUCKET_NAME` | **Yes** | — | S3 storage bucket name |
| `INVOICE_BUCKET_NAME` | No | `coderhelm-prod-invoices` | S3 invoice bucket name |
| `TICKET_QUEUE_URL` | **Yes** | — | SQS ticket queue URL |
| `CI_FIX_QUEUE_URL` | **Yes** | — | SQS CI fix queue URL |
| `FEEDBACK_QUEUE_URL` | **Yes** | — | SQS feedback queue URL |
| `DLQ_URL` | No | `""` | SQS dead-letter queue URL |
| `SECRETS_NAME` | No | `coderhelm/prod/secrets` | Secrets Manager secret name |
| `SES_FROM_ADDRESS` | No | `notifications@coderhelm.com` | SES sender address |
| `SES_TEMPLATE_PREFIX` | No | `coderhelm-prod` | SES template name prefix |
| `MODEL_ID` | No | `us.anthropic.claude-sonnet-4-6` | Bedrock model ID |
| `RUST_LOG` | No | — | Log level filter |

### Worker

Defined in `services/worker/src/models.rs` → `Config::from_env()` and `infra/lib/worker-stack.ts`.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `STAGE` | No | `dev` | Deployment stage |
| `TABLE_NAME` | **Yes** | — | DynamoDB main table name |
| `RUNS_TABLE_NAME` | **Yes** | — | DynamoDB runs table name |
| `ANALYTICS_TABLE_NAME` | **Yes** | — | DynamoDB analytics table name |
| `BUCKET_NAME` | **Yes** | — | S3 storage bucket name |
| `SECRETS_NAME` | No | `coderhelm/prod/secrets` | Secrets Manager secret name |
| `MODEL_ID` | **Yes** | — | Bedrock model ID |
| `SES_FROM_ADDRESS` | No | `notifications@coderhelm.com` | SES sender address |
| `SES_TEMPLATE_PREFIX` | No | `coderhelm-prod` | SES template name prefix |
| `RUST_LOG` | No | — | Log level filter |

### Frontend

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NEXT_PUBLIC_API_URL` | **Yes** | — | Gateway API base URL (e.g. `http://localhost:9000` for local dev) |

### Jira Forge App

The Forge app uses Forge Storage (not environment variables) for runtime configuration. See `coderhelm-jira/.env.example` for development notes.

## Secrets

Both the gateway and worker load secrets from AWS Secrets Manager at startup.

### Creating the dev secrets entry

Create a secret named `coderhelm/dev/secrets` in AWS Secrets Manager with the following JSON structure:

```json
{
  "github_app_id": "<your-github-app-id>",
  "github_private_key": "<PEM private key>",
  "github_webhook_secret": "<webhook-secret>",
  "github_client_id": "<oauth-client-id>",
  "github_client_secret": "<oauth-client-secret>",
  "jwt_secret": "<random-256-bit-hex>",
  "jira_webhook_secret": "<optional-jira-secret>",
  "stripe_webhook_secret": "<optional-stripe-webhook-secret>",
  "stripe_secret_key": "<optional-stripe-secret-key>",
  "stripe_publishable_key": "<optional-stripe-publishable-key>",
  "stripe_price_id": "<optional-stripe-price-id>",
  "stripe_overage_price_id": "<optional-stripe-overage-price-id>"
}
```

The `SECRETS_NAME` environment variable controls which secret is loaded. Set it to `coderhelm/dev/secrets` for local development (see `.env.example`). The gateway defaults to `coderhelm/prod/secrets` if `SECRETS_NAME` is not set, so always set it explicitly for local dev.

Refer to `SETUP.md` for the full secrets structure and GitHub App registration steps.

## Cloud Deployment

For full cloud deployment instructions — including CDK deploy, CI/CD pipelines, GitHub App registration, and Stripe setup — see **[SETUP.md](SETUP.md)**.

## Running Tests

The CI pipeline (`.github/workflows/ci.yml`) runs formatting, linting, and tests for both Rust services.

**Gateway:**

```bash
cd services/gateway
cargo fmt --check      # Check formatting
cargo clippy -- -D warnings  # Lint
cargo test             # Run tests
```

**Worker:**

```bash
cd services/worker
cargo fmt --check      # Check formatting
cargo clippy -- -D warnings  # Lint
cargo test             # Run tests
```

**CDK (infrastructure):**

```bash
cd infra
npm ci
npx cdk synth --quiet
```

## Docs

- Jira integration quickstart: [`docs/jira-integration.md`](docs/jira-integration.md)
- Full deployment guide: [`SETUP.md`](SETUP.md)
- Local development: see [Local Development](#local-development) above
- Environment variable reference: see [Environment Variables](#environment-variables) above
- Secrets setup: see [Secrets](#secrets) above
