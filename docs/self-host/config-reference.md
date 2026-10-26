---
title: Config reference
description: Every setting in coderhelm.config.json, its default, and what it changes.
order: 3
---

`coderhelm.config.json` is plain JSON. The CDK app validates it on every deploy and lists every problem at once, so a typo is caught before anything changes. `${NAME}` anywhere in the file is replaced by the environment variable `NAME` (a missing one is an error), which keeps account ids out of a shared file if you like.

The smallest config:

```json
{
  "account": "123456789012",
  "region": "us-east-1",
  "stage": "prod",
  "email": { "from": "coderhelm@example.com" },
  "admins": ["you@example.com"]
}
```

[Everything you can set up](./options.md) lists every setting with where else to set it (the install guide, the Launch Stack, the dashboard). Two complete examples live in the repo: [`infra/config/selfhost.example.json`](../../infra/config/selfhost.example.json) (no domain, Anthropic) and [`infra/config/selfhost.domain.example.json`](../../infra/config/selfhost.domain.example.json) (your domain, Bedrock in eu-west-1). The hosted deployment's own config is [`infra/config/hosted.prod.json`](../../infra/config/hosted.prod.json).

## Where things go

| Setting | Type | Default | Notes |
|---|---|---|---|
| `account` | string | required | 12-digit AWS account id. Deploys refuse to run with credentials for another account. |
| `region` | string | required | Any region with Lambda on ARM64, Cognito and SES. |
| `stage` | string | required | Short name, lowercase letters, digits and hyphens, up to 20 characters. Names resources `coderhelm-<stage>-*`. |
| `resourcePrefix` | string | `coderhelm-<stage>` | Prefix of every resource name. Change it only before the first deploy. |
| `globalNameSuffix` | string | `-<account>` | Added to S3 bucket names and the sign-in domain prefix, which must be unique across all of AWS. `""` for none. |
| `retainData` | boolean | `true` | Keep tables, the data bucket, KMS keys and the user pool when stacks are deleted, and turn on table deletion protection. |

## Domain

| Setting | Type | Default | Notes |
|---|---|---|---|
| `domain` | object or null | `null` | null: CloudFront's own `*.cloudfront.net` URL. |
| `domain.hostname` | string | | The hostname people open, e.g. `coderhelm.example.com`. |
| `domain.hostedZone` | string | | Its Route 53 public hosted zone. The certificate and DNS records are created for you. |
| `domain.hostedZoneId` | string | | Skips the zone lookup at synth. |
| `domain.certificateArn` | string | | An ACM certificate **in us-east-1** for the hostname, when your DNS is not in Route 53. The deploy prints the CNAME to create. |

See [Domains](./domains.md).

## Models

| Setting | Type | Default | Notes |
|---|---|---|---|
| `models.provider` | `anthropic` or `bedrock` | `anthropic` | anthropic: each team adds its own key. bedrock: Claude on Bedrock in this account, no keys. |
| `models.primary` | string | `claude-opus-5-5` | Model for implementation and reviews. A team can choose others in its settings. |
| `models.light` | string | `claude-sonnet-5-5` | Model for triage, plan chat and log analysis. |
| `models.bedrockInferencePrefix` | string | `us.` | Bedrock only. `us.`, `eu.`, `apac.`, `global.` or `""` for in-region ids. |
| `models.bedrockModelIds` | object | `{}` | Bedrock only. Exact ids or inference profile ARNs for model names. |

See [Model providers](./model-providers.md).

## Email

| Setting | Type | Default | Notes |
|---|---|---|---|
| `email.from` | string | required | The sender of every email. |
| `email.fromName` | string | `CoderHelm` | Display name. |
| `email.supportInbox` | string | `email.from` | Where support requests from the dashboard go. |
| `email.identity` | string or null | the domain of `from` | The SES identity to create: a domain or one address. null if you manage it. |
| `email.sesRegion` | string | `region` | Region of the SES identity. |
| `email.cognitoUsesSes` | boolean | `false` | Send sign-up and password emails through SES. Turn on only after the identity is verified. |
| `email.logoUrl` | string | `<your URL>/favicon.svg` | Logo in email headers. |
| `email.privacyUrl` | string or null | `null` | A privacy page linked in email footers. |

See [Email](./email.md).

## People

| Setting | Type | Default | Notes |
|---|---|---|---|
| `admins` | list of emails | required | Admins of the whole install: the admin pages (teams, the failed message queue, support). They may always sign up. |
| `signup.emails` | list of emails | `[]` | Others who may create an account. |
| `signup.domains` | list of domains | `[]` | Anyone with an email at these domains may create an account. |

Sign-up is closed to everyone else. People already on a team invite others from the dashboard.

## Integrations

| Setting | Type | Default | Notes |
|---|---|---|---|
| `integrations.googleSignIn` | boolean | `false` | "Sign in with Google". Needs a domain and Google keys in the secret. |
| `integrations.sandbox` | boolean | `true` | The CodeBuild sandbox that runs a repo's build and checks during runs. |
| `integrations.logAnalyzer` | boolean | `true` | Scheduled log analysis for AWS accounts teams connect. |
| `integrations.weeklySummary` | boolean | `true` | The Monday summary email. |
| `integrations.weeklySummaryListedTeamsOnly` | boolean | `false` | Only teams with a rollout list get the summary. |

See [Integrations](./integrations.md).

## Updates

| Setting | Type | Default | Notes |
|---|---|---|---|
| `updates.enabled` | boolean | `true` | Create the updater: the hourly release check, notices, the update build. |
| `updates.auto` | boolean | `false` | Install new releases on their own, in the window. |
| `updates.window` | string | `"Sun 03:00"` | Maintenance window: a day (or `daily`) and an hour. |
| `updates.timezone` | string | `UTC` | IANA time zone of the window. |
| `updates.channel` | `stable` or `preview` | `stable` | `preview` includes release candidates. |
| `updates.majors` | `ask` or `auto` | `ask` | `ask`: major releases wait for an admin. |
| `updates.webhookUrl` | string | | A Slack or Teams incoming webhook for update notices (admins are emailed either way). |

These are the starting point; admins change them in **Settings, then System** without a deploy. See [Updates](./updates.md).

## Everything else

| Setting | Type | Default | Notes |
|---|---|---|---|
| `dashboard.assetDir` | string | the release's dashboard | A dashboard build to upload instead (for your own changes). The installer fills this in for you. |
| `telemetry.enabled` | boolean | `false` | Anonymous install, upgrade and health pings from the installer. |
| `telemetry.endpoint` | string | CoderHelm's | Where pings go. |
| `logReaderTemplateUrl` | string | CoderHelm's public copy | The CloudFormation template teams create the log reader role from. |
| `publicBucketName` | string or null | `null` | Create a public bucket that serves the templates in `infra/cfn` (the hosted deployment does). |
| `siteUrl` | string or null | `null` | A website linked from email footers. |
| `layout` | string | `single-origin` | `split` is the hosted deployment's layout (separate hostnames, dashboard deployed from its own repo). |
| `split` | object | | Settings for the `split` layout. |
| `ci.reviewerEvalsRepo` | string | | Hosted only: the repository whose CI runs the nightly reviewer evals on Bedrock. |

## What a deploy sets on the services

The CDK app turns the config into environment variables on the gateway, streaming and worker functions. You never set these yourself, but they help when reading logs:

| Variable | From |
|---|---|
| `APP_URL`, `API_URL` | Your hostname, when you have one |
| `PUBLIC_URL_PARAMETER` | The SSM parameter that holds the CloudFront URL, when you have no domain |
| `SELF_HOSTED` | `1` on every self-hosted install: links never fall back to CoderHelm's hosted addresses, and support goes to `SUPPORT_INBOX` |
| `SITE_URL` | `siteUrl` |
| `CODERHELM_ACCOUNT_ID` | `account` (the account teams' read-only roles trust) |
| `LOG_READER_TEMPLATE_URL` | `logReaderTemplateUrl` |
| `PLATFORM_ADMIN_EMAILS` | `admins` |
| `CODERHELM_VERSION`, `CODERHELM_COMMIT`, `CODERHELM_RELEASED_AT`, `CODERHELM_CONFIG_FINGERPRINT` | The release stamp and a hash of the config |
| `UPDATER_FUNCTION_NAME` | The updater, when `updates.enabled` |
| `MODEL_PROVIDER`, `BEDROCK_INFERENCE_PREFIX`, `BEDROCK_MODEL_IDS` | `models` (Bedrock only) |
| `SES_FROM_ADDRESS`, `SUPPORT_INBOX` | `email` |
