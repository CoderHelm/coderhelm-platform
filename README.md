# Coderhelm

Coderhelm is an AI-powered autonomous coding agent that turns GitHub issues and Jira tickets into pull requests. Assign an issue — Coderhelm creates a branch, implements the change, runs your CI, and opens a draft PR. It self-reviews every change before you see it.

## Documentation

- **[Setup Guide](SETUP.md)** — prerequisites, secrets, GitHub App registration
- **[Jira Integration](docs/jira-integration.md)** — webhook-based Jira automation quickstart

---

## Architecture Overview

Coderhelm is deployed as 8 AWS CDK stacks:

| Stack | File | Description |
|-------|------|-------------|
| **Database** | `infra/lib/database-stack.ts` | DynamoDB tables (main, runs, analytics) with KMS encryption and point-in-time recovery |
| **Storage** | `infra/lib/storage-stack.ts` | KMS-encrypted S3 bucket for run artifacts with lifecycle rules and versioning |
| **Api** | `infra/lib/api-stack.ts` | API Gateway HTTP API, Gateway Lambda (Rust), SQS queues (tickets, ci-fix, feedback, DLQ) |
| **Worker** | `infra/lib/worker-stack.ts` | Worker Lambda (Rust) consuming SQS queues, with Bedrock access for LLM inference |
| **Email** | `infra/lib/email-stack.ts` | SES domain identity, configuration set, and email templates (welcome, run status, billing) |
| **Billing** | `infra/lib/billing-stack.ts` | S3 bucket for invoice PDFs with 7-year retention for tax compliance |
| **Monitoring** | `infra/lib/monitoring-stack.ts` | CloudWatch dashboard, alarms (gateway errors, worker errors, DLQ depth), SNS alert topic |
| **Frontend** | `infra/lib/frontend-stack.ts` | S3 + CloudFront SPA for `app.coderhelm.com` with OAC, security headers, and custom error pages |

### Data Flow

```
Webhooks (GitHub/Jira/Stripe)
  → API Gateway HTTP API
    → Gateway Lambda (auth, validation, routing)
      → SQS (tickets / ci-fix / feedback queues)
        → Worker Lambda (LLM planning + code generation via Bedrock)
          → GitHub (branch, commits, PR) / Jira (status updates)
```

---

## CDK Bootstrap & Deploy

### First-Time Bootstrap

If you have never deployed CDK to the target account/region, bootstrap first:

```bash
cdk bootstrap aws://<account-id>/<region>
```

### Build Rust Services

Build both gateway and worker as ARM64 static binaries:

```bash
cd services
cargo zigbuild --workspace --release --target aarch64-unknown-linux-musl
```

### Package Artifacts

Package each binary into a zip file named `bootstrap` (the Lambda runtime entry point):

```bash
cp services/target/aarch64-unknown-linux-musl/release/gateway bootstrap
zip gateway.zip bootstrap
rm bootstrap

cp services/target/aarch64-unknown-linux-musl/release/worker bootstrap
zip worker.zip bootstrap
rm bootstrap
```

### Stripe Setup (Pre-Deploy)

Run the idempotent Stripe setup script before deploying. It creates (or finds) the Stripe product, prices, billing meters, customer portal, and webhook configuration, then writes `stripe_price_id` and `stripe_overage_price_id` back to Secrets Manager:

```bash
bash scripts/setup-stripe.sh
```

> The script reads `stripe_secret_key` from `coderhelm/<stage>/secrets` in AWS Secrets Manager. It is safe to run on every deploy.

### Deploy All Stacks

From the `infra/` directory:

```bash
cd infra
npm ci

GATEWAY_ZIP=../gateway.zip \
WORKER_ZIP=../worker.zip \
CDK_DEFAULT_ACCOUNT=<account-id> \
CDK_DEFAULT_REGION=us-east-1 \
MODEL_ID=us.anthropic.claude-opus-4-6-v1 \
  npx cdk deploy --all
```

| Variable | Description |
|----------|-------------|
| `GATEWAY_ZIP` | Path to the packaged `gateway.zip` artifact |
| `WORKER_ZIP` | Path to the packaged `worker.zip` artifact |
| `CDK_DEFAULT_ACCOUNT` | AWS account ID for deployment |
| `CDK_DEFAULT_REGION` | AWS region (e.g. `us-east-1`) |
| `MODEL_ID` | Bedrock model ID for the Worker Lambda (e.g. `us.anthropic.claude-opus-4-6-v1`) |

### Stage Context Parameter

The default stage is `prod`. Override with the `-c stage=dev` context parameter:

```bash
npx cdk deploy --all -c stage=dev
```

See [Environment-Specific Configuration](#environment-specific-configuration) for `dev` vs `prod` differences.

### CI/CD

Pushing to the `main` branch automatically triggers `.github/workflows/deploy.yml`, which:

1. Builds the Rust workspace with `cargo zigbuild`
2. Packages `gateway.zip` and `worker.zip` artifacts
3. Assumes the `coderhelm-github-deploy` IAM role via OIDC
4. Runs `scripts/setup-stripe.sh` (idempotent)
5. Runs `npx cdk deploy --all --require-approval never`

---

## Jira Forge Deployment

The Coderhelm Jira app lives in `coderhelm-jira/` and is deployed with [Atlassian Forge](https://developer.atlassian.com/platform/forge/).

### Install Dependencies

```bash
cd coderhelm-jira
npm install
```

### Deploy the Forge App

```bash
forge deploy -e <environment>
```

Supported environments: `development`, `staging`, `production`.

### First-Time Installation

Install the app on a Jira site for the first time:

```bash
forge install --site <site-url> --product jira -e <environment>
```

### Upgrade After Permission Changes

If the manifest's `permissions.scopes` change, re-install with the upgrade flag:

```bash
forge install --upgrade
```

### Admin Page Configuration

After installation, configure the app on your Jira site:

1. Go to **Apps → Coderhelm Settings**
2. Enter your `installationId` (GitHub App installation ID)
3. Enter your `tenantId` (Coderhelm tenant ID)
4. Save

### Manifest Key Modules

The `coderhelm-jira/manifest.yml` defines three key modules:

| Module | Key | Description |
|--------|-----|-------------|
| **Trigger** | `coderhelm-jira-trigger` | Fires on `avi:jira:assigned:issue` and `avi:jira:updated:issue` events, forwarding issue data to the Coderhelm API |
| **Admin Page** | `coderhelm-admin-page` | Configuration UI for entering `installationId` and `tenantId` (stored in Forge storage) |
| **External Fetch** | — | Permission to make outbound requests to `https://api.coderhelm.com` |

---

## Environment-Specific Configuration

### `dev` vs `prod` Differences

| Setting | `dev` | `prod` |
|---------|-------|--------|
| CORS origins | `http://localhost:3000` | `https://app.coderhelm.com` |
| Custom domains (API, frontend) | Not created | `api.coderhelm.com`, `app.coderhelm.com` |
| DynamoDB deletion protection | Disabled | Enabled |
| DynamoDB removal policy | `DESTROY` | `RETAIN` |
| S3 bucket removal policy | `DESTROY` (auto-delete objects) | `RETAIN` |
| KMS key removal policy | `RETAIN` | `RETAIN` |
| `MODEL_ID` default (Worker) | `us.anthropic.claude-opus-4-6-v1` (from env) | `us.anthropic.claude-opus-4-6-v1` (from env) |
| Gateway `MODEL_ID` (hardcoded) | `us.anthropic.claude-sonnet-4-6` | `us.anthropic.claude-sonnet-4-6` |

### Secrets Naming Convention

Secrets are stored in AWS Secrets Manager under the name:

```
coderhelm/<stage>/secrets
```

For example: `coderhelm/prod/secrets` or `coderhelm/dev/secrets`.

The secret value is a JSON object with the following keys:

```json
{
  "github_app_id": "<your-github-app-id>",
  "github_private_key": "<PEM private key>",
  "github_webhook_secret": "<webhook-secret>",
  "github_client_id": "<oauth-client-id>",
  "github_client_secret": "<oauth-client-secret>",
  "jwt_secret": "<random-256-bit-hex>",
  "stripe_secret_key": "<sk_live_or_sk_test_...>",
  "stripe_price_id": "<price_...>",
  "stripe_overage_price_id": "<price_...>"
}
```

> The `stripe_price_id` and `stripe_overage_price_id` keys are written automatically by `scripts/setup-stripe.sh`. You must manually add `stripe_secret_key` before running the script.

### Switching Stages

- **CDK:** `npx cdk deploy --all -c stage=dev`
- **Forge:** `forge deploy -e development`

---

## Required AWS IAM Permissions

### Deployer Permissions (for `cdk bootstrap` and `cdk deploy --all`)

The IAM principal running CDK needs the following minimum permissions:

- **CloudFormation** — full access to manage stacks (`cloudformation:*`)
- **S3** — access to the CDK bootstrap bucket (`s3:*` on `cdk-*` buckets)
- **IAM** — create/update/delete roles and policies (`iam:CreateRole`, `iam:AttachRolePolicy`, `iam:PutRolePolicy`, `iam:DeleteRole`, `iam:PassRole`, etc.)
- **SSM** — read CDK bootstrap version parameter (`ssm:GetParameter`)

The deploying principal also needs `Create`/`Update`/`Delete` access to the following AWS services used by the CDK stacks:

| Service | Used By |
|---------|---------|
| Lambda | Api, Worker stacks |
| DynamoDB | Database stack |
| S3 | Storage, Billing, Frontend stacks |
| SQS | Api stack |
| SES | Email stack |
| API Gateway (v2) | Api stack |
| CloudFront | Frontend stack |
| Route 53 | Api, Frontend stacks (prod only) |
| ACM | Api, Frontend stacks (prod only) |
| KMS | Database, Storage stacks |
| CloudWatch (Logs, Alarms, Dashboards) | Monitoring stack, Api, Worker stacks |
| SNS | Monitoring stack |
| Secrets Manager | **Read-only** — Api, Worker stacks reference secrets at deploy time |
| IAM (roles/policies) | All stacks — CDK creates execution roles for Lambda functions |

### GitHub Actions OIDC Role

The CI/CD pipeline in `.github/workflows/deploy.yml` authenticates via OIDC using the IAM role:

```
arn:aws:iam::<account-id>:role/coderhelm-github-deploy
```

The trust policy must allow the GitHub OIDC provider to assume this role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<account-id>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:<github-org>/<repo-name>:ref:refs/heads/main"
        }
      }
    }
  ]
}
```

### Deployer vs Lambda Runtime Permissions

> **Note:** The permissions listed above are for the *deploying principal* (human or CI role). Lambda runtime permissions (e.g. DynamoDB read/write, SQS send/consume, Bedrock invoke, SES send) are managed automatically by CDK via `grant*()` calls in each stack. You do not need to configure runtime IAM policies manually.
