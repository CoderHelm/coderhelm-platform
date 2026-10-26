---
title: Self-hosting CoderHelm
description: Run CoderHelm in your own AWS account, with your own GitHub App and your own model access.
order: 0
---

CoderHelm runs entirely on AWS serverless services: Lambda, API Gateway, SQS, DynamoDB, S3, CloudFront, Cognito, SES, EventBridge and CodeBuild. Self-hosting puts all of it in your AWS account. Your code, tickets, logs and model calls never pass through CoderHelm's own account.

**Two setups.** **Fully private** (the default): your own address and your own GitHub App, and nothing reaches CoderHelm. **[Hybrid](./hybrid.md)**: the same install, plus a `yourname.coderhelm.com` address and CoderHelm's GitHub App and integration apps where that's easier; the [trust model](./hybrid.md#trust-model) says exactly what passes through CoderHelm.

**The fastest way:** the [install guide](https://docs.coderhelm.com/self-host/install/) installs CoderHelm from your browser with one click into your AWS account, then checks every step against your own install. Prefer a terminal? Use the [quick start](./quick-start.md).

**Moving from CoderHelm cloud?** [Talk to us](mailto:support@coderhelm.com?subject=Moving%20to%20self-hosted%20CoderHelm) and we'll help move your data.

## How it works

- **One config file.** `coderhelm.config.json` holds everything that differs between installs: account, region, a short name, your domain (or none), the model provider, the email sender and which optional parts run. The hosted CoderHelm deployment is built from the same code with its own config.
- **One command.** `coderhelm-selfhost` checks your machine, asks a few questions, deploys the release you pick, uploads the dashboard and creates your GitHub App with one click on github.com.
- **One URL.** The dashboard, the API and the chat stream share one hostname: yours, or the `*.cloudfront.net` name CloudFront gives you when you have no domain.
- **No build tools.** Each release ships the Lambda code built for ARM64, the dashboard and the CDK app. You need Node.js and the AWS CLI, not Rust or Docker.

## Ways to install

| Way | Best for | What you run |
|---|---|---|
| Installer | Most people | `npx -y github:CoderHelm/coderhelm-platform init`, then `deploy` |
| Install script | A fresh machine | `curl -fsSL https://github.com/CoderHelm/coderhelm-platform/releases/latest/download/install.sh \| sh` |
| Launch Stack | Clicking through the AWS console | The CloudFormation template in [Quick start](./quick-start.md#launch-stack) |
| From source | Changing CoderHelm itself | `coderhelm-selfhost deploy --source .` in a checkout |

## The guides

1. [Quick start](./quick-start.md)
2. [Hybrid: CoderHelm's address and apps](./hybrid.md)
3. [Everything you can set up](./options.md)
4. [Requirements](./requirements.md)
5. [Config reference](./config-reference.md)
6. [The GitHub App](./github-app.md)
7. [Model providers](./model-providers.md)
8. [Integrations](./integrations.md)
9. [Sign-in and people](./sign-in.md)
10. [Email](./email.md)
11. [Domains](./domains.md)
12. [Updates](./updates.md)
13. [Backups](./backups.md)
14. [Security model](./security.md)
15. [Check your install](./check-your-install.md)
16. [Deliveries](./deliveries.md)
17. [Troubleshooting](./troubleshooting.md)
18. [FAQ](./faq.md)

## License

CoderHelm is licensed under the [Functional Source License, Version 1.1, with Apache 2.0 as the future license](../../LICENSE.md) (FSL-1.1-ALv2). In plain words:

- You can run it, change it and self-host it for your own company.
- You can't offer it, or something built from it, as a competing hosted product.
- Each release becomes Apache 2.0 two years after it ships.

The full terms are in [LICENSE.md](../../LICENSE.md).
