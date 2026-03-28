# d3ftly Setup Guide

## Prerequisites

- **Rust** 1.80+ (`rustup update stable`)
- **Node.js** 22+ (for CDK and dashboard)
- **AWS CDK** v2 (`npm install -g aws-cdk`)
- **AWS CLI** configured with credentials for account `REDACTED_AWS_ACCOUNT_ID`

## AWS Secrets Manager

Create a secret named `d3ftly/<stage>/secrets` with this JSON:

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

## Environment Variables

| Variable    | Where    | Description                              |
|------------|---------|------------------------------------------|
| `MODEL_ID` | Worker   | Bedrock model ID (e.g. `us.anthropic.claude-opus-4-6-20250610-v1:0`) |
| `STAGE`    | Both     | `dev` or `prod`                          |

Set `MODEL_ID` before deploying — there is no default.

## Deploy

```bash
# Build & deploy everything (from repo root)
MODEL_ID="your-model-id" cdk deploy --all
```

Or use the GitHub Actions workflow — push to `main` with `CI:DEPLOY_PROD` in the commit message.

## GitHub App Registration

1. Go to https://github.com/settings/apps/new
2. Homepage URL: `https://d3ftly.com`
3. Webhook URL: `https://api.d3ftly.com/webhooks/github`
4. Permissions: Contents (RW), Issues (RW), Pull requests (RW), Checks (R), Metadata (R)
5. Events: Issues, Issue comment, Pull request review, Check run, Installation
6. Generate a private key and store in Secrets Manager
