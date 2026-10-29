---
title: Requirements
description: What you need before installing CoderHelm in your AWS account.
order: 2
---

## On your machine

| Tool | Version | Why |
|---|---|---|
| Node.js | 20 or newer | Runs the installer and the CDK app |
| AWS CLI | v2 | Credentials, secrets and checks |
| tar | any | Unpacks the release |
| git | 2.34 or newer | Checks that the release's tag is signed by a CoderHelm maintainer and is on `main` |
| ssh-keygen (OpenSSH) | 8.2 or newer | Checks the tag's SSH signature (comes with macOS and most Linux systems) |
| cosign, or the GitHub CLI (`gh`) | cosign 2.x | Checks the release's signature (one of the two) |
| A browser | any | One click to create the GitHub App |

You do not need Rust, Docker or the CDK CLI: the release ships built code, and the installer runs CDK with `npx`.

## In AWS

- **An account you can administer.** The first install creates IAM roles, so use administrator credentials (an SSO admin role is fine). With the Launch Stack, CoderHelm then keeps nothing broader than its own permissions boundary: [What the installer can do in your account](./security.md#what-the-installer-can-do-in-your-account).
- **A region with Lambda on ARM64, Cognito, SES and Bedrock** (if you use Bedrock). All commercial regions with Bedrock work.
- **Service quotas:** the defaults are enough. The worker uses up to 10 concurrent Lambda executions for tickets and 5 each for CI fixes and feedback.
- **SES production access** to email people outside your verified addresses. New accounts start in the SES sandbox. See [Email](./email.md).
- **Bedrock model access** for the Claude models you use, if you choose Bedrock. See [Model providers](./model-providers.md).

## On GitHub

- An account or organization where you can create GitHub Apps (organization owners, or members with the GitHub App manager role).
- github.com. GitHub Enterprise Server can't create apps from a manifest; create the app by hand and pass its values to `github-app` (see [The GitHub App](./github-app.md#github-enterprise-server)). CoderHelm itself calls `api.github.com`, so Enterprise Server is not supported yet.

## Optional

- **A domain** in a Route 53 public hosted zone, so certificates and DNS records are created for you. A domain elsewhere works with an ACM certificate you create. See [Domains](./domains.md).
- **Google sign-in** needs a domain and a Google OAuth client.

## What it costs

You pay AWS directly. A small team's install is mostly idle serverless: Lambda, DynamoDB on demand, SQS and CloudFront cost little at rest. The larger items are:

- **Model calls:** to Anthropic with your teams' keys, or Bedrock on your AWS bill.
- **CodeBuild** for the build sandbox (MEDIUM instances, by the minute), when a run checks a repo's build.
- **KMS keys:** four customer managed keys, about $1 a month each.
- **CloudWatch Logs** kept for a month.
