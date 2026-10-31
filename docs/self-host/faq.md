---
title: FAQ
description: Short answers about self-hosting CoderHelm.
order: 14
---

**Does any of my code or data go to CoderHelm the company?**
No. Everything runs in your account. The only outbound calls are to GitHub, to the model provider you chose, to services your teams connect, and, only if you turn telemetry on, one anonymous ping per install or upgrade.

**Do I need Docker, Kubernetes or a server?**
No. It is all serverless AWS: nothing to patch, nothing running when nobody uses it.

**Can I run it outside AWS?**
Not today. CoderHelm is built on Lambda, DynamoDB, SQS, Cognito and the other services listed in [Requirements](./requirements.md).

**Can I run it without internet access (air-gapped)?**
Partly. CoderHelm needs GitHub and a model. With Bedrock as the provider and GitHub reachable, nothing else is required. The command line installer needs to reach GitHub releases once per version (a Launch Stack install reads the release from `coderhelm-public.s3.amazonaws.com` instead); you can download a release's files elsewhere, check them against `SHA256SUMS`, and deploy from the unpacked source with `deploy --source`. Teams' MCP servers fetch npm packages unless you mirror them. A fully offline install (GitHub Enterprise Server, no outbound access) is not supported yet.

**How do I give it to several teams?**
One install serves many teams. People sign up (or are invited) and create or join teams; each team has its own repositories, settings, keys and usage limits.

**Can I run two installs in one account?**
Yes, with different `stage` names. Every resource name starts with `coderhelm-<stage>`.

**How much does it cost?**
AWS usage plus model calls. See [Requirements](./requirements.md#what-it-costs).

**How do I uninstall?**
Delete the stack `coderhelm-<name>` in the CloudFormation console. With the stack parameter **KeepDataOnDelete** `true` (the default) it keeps the data (tables, data buckets, KMS keys, the user pool and the secret); set it to `false` first to delete those too, KMS keys after AWS's 7-day minimum (update the stack with its release's template again, not **Use current template**: see [Uninstall](./uninstall.md#change-it-before-you-delete)). Then `npx -y github:CoderHelm/coderhelm-platform leftovers --stage <name> --region <region>` lists anything left, and `--delete` removes it. A hybrid install releases its address and links when the stack is deleted. Uninstall your GitHub App from GitHub too. An install from before native releases uses `uninstall` from a terminal instead. See [Uninstall](./uninstall.md).

**What license is the code under?**
FSL-1.1-ALv2, the Functional Source License with Apache 2.0 as the future license. You can run, change and self-host CoderHelm for your own company; you can't offer it as a competing hosted product; each release becomes Apache 2.0 two years after it ships. There is no license key or license check in the code. The full terms are in [LICENSE.md](../../LICENSE.md).

**Where do I get help?**
Open an issue on [CoderHelm/coderhelm-platform](https://github.com/CoderHelm/coderhelm-platform/issues) with the output of `health` (it contains no secrets).
