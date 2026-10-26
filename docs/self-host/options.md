---
title: Everything you can set up
description: Every setting of a self-hosted install, where to set it (the install guide, the Launch Stack, the dashboard or the config), and how it is checked.
order: 1.5
---

One list of everything you can set up, and where. Required items are steps in the [install guide](https://docs.coderhelm.com/self-host/install/); optional ones are in its **Optional setup** list after **Ready**, and in **Settings, then Setup** in the dashboard, each with a check and a fix.

## At install

| Setting | Required | Install guide | Launch Stack parameter | Config | Checked by |
|---|---|---|---|---|---|
| Your email (the first admin) | Yes | Your email | `AdminEmail` | `admins` | `admin` |
| AWS region | Yes | AWS region | the console's region | `region` | |
| Install name | | More options | `Stage` | `stage` | |
| Models: Bedrock or Anthropic keys | | More options | `ModelProvider` | `models.provider` | `models`, `model` |
| Bedrock inference profile | | | `BedrockInferencePrefix` | `models.bedrockInferencePrefix` | `model` |
| Your own domain | | More options, then the Domain step | `Hostname` (`HostedZone` to pick a zone) | `domain` | `domain` |
| Send email from | | More options | `EmailFrom` | `email.from`, `email.identity` | `email`, `email_identity` |
| Who else may sign up | | More options | `SignupDomain` | `signup.domains`, `signup.emails` | |
| Automatic updates and the window | | More options | `AutoUpdate`, `UpdateWindow`, `UpdateTimezone` | `updates.*` | `updates` |
| Anonymous install pings | | | `Telemetry` | `telemetry.enabled` | |
| The release | | (the newest) | `Version` | `--version` | |
| GitHub App | Yes | The GitHub step | | `github-app` | `github_app`, `github_installed`, `github_*` |
| Claude access in Bedrock | Yes, with Bedrock | The Models step | | | `models`, `model` |

## After install

| Setting | Where | Checked by | Guide |
|---|---|---|---|
| Your own domain, or change it | Settings, then System, Domain | `domain` | [Domains](./domains.md) |
| Email reaches anyone (leave the SES sandbox) | The SES console | `email_sandbox` | [Email](./email.md) |
| Email from your domain, with DKIM | The SES console, and DNS | `email_identity` | [Email](./email.md) |
| Sign-up and password emails through SES | `email.cognitoUsesSes` | | [Email](./email.md) |
| Sign in with Google | Google Cloud, the secret, `integrations.googleSignIn` | `google_sign_in` | [Sign-in and people](./sign-in.md) |
| Invite your team | Settings, People | | [Sign-in and people](./sign-in.md) |
| Anthropic API key (Anthropic provider) | Settings, AI models (per team) | `model` | [Model providers](./model-providers.md) |
| Models a team uses | Settings, AI models | `model` | [Model providers](./model-providers.md) |
| Repositories CoderHelm works on | Settings, Repositories | | |
| Slack or Teams notices and alert routes | Settings, Alerts | `slack`, `teams` | [Integrations](./integrations.md) |
| Jira | Settings, Integrations, Jira | `jira` | [Integrations](./integrations.md) |
| Connected AWS accounts (logs, metrics) | Settings, Integrations, AWS | | [Integrations](./integrations.md) |
| Knowledge sources (Notion, Confluence, Google Drive and more) | Settings, Knowledge; OAuth apps in the secret | | [Knowledge sources](./knowledge-sources.md) |
| Automatic updates, channel, window, majors | Settings, System | `updates` | [Updates](./updates.md) |
| Update notices in Slack or Teams | `updates.webhookUrl` | | [Updates](./updates.md) |
| Backups | On by the install (point-in-time recovery on every table, versioned bucket); on-demand with `backup`; AWS Backup plans | | [Backups](./backups.md) |
| Keep data when stacks are deleted | `retainData` (on) | | [Config reference](./config-reference.md) |
| The build sandbox, log analysis, weekly summary | `integrations.*` | | [Integrations](./integrations.md) |
| Support access for install admins | Settings, Support access (per team) | | [Sign-in and people](./sign-in.md) |

## Not available yet

- **Single sign-on (SAML or OIDC):** sign-in is email and password (with passkeys and two-factor codes) or Google.
- **A private network (VPC):** the functions run without a VPC and reach GitHub, Anthropic and Bedrock over HTTPS.
- **GitHub Enterprise Server:** CoderHelm calls `api.github.com`.
- **Instance sizes:** everything is serverless and pay per use; there is no size to pick. Model calls are most of the bill.
