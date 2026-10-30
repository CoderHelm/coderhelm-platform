---
title: Everything you can set up
description: Every setting of a self-hosted install, where to set it (the install guide, the Launch Stack, the dashboard or the config), and how it is checked.
order: 1.5
---

One list of everything you can set up, and where. Required items are steps in the [install guide](https://docs.coderhelm.com/self-host/install/); optional ones are in its **Optional setup** list after **Ready**, and in **Settings, then Setup** in the dashboard, each with a check and a fix.

## At install

- **Your email (the first admin)** *required*\
  *Install guide:* Your email\
  *Launch Stack:* `AdminEmail`\
  *Config:* `admins`\
  *Checked by:* `admin`
- **AWS region** *required*\
  *Install guide:* AWS region\
  *Launch Stack:* the console's region\
  *Config:* `region`
- **Install name**\
  *Install guide:* More options\
  *Launch Stack:* `Stage`\
  *Config:* `stage`
- **Models: Bedrock or Anthropic keys**\
  *Install guide:* More options\
  *Launch Stack:* `ModelProvider`\
  *Config:* `models.provider`\
  *Checked by:* `models`, `model`
- **Bedrock inference profile**\
  *Launch Stack:* `BedrockInferencePrefix`\
  *Config:* `models.bedrockInferencePrefix`\
  *Checked by:* `model`
- **Your own domain**\
  *Install guide:* More options, then the Domain step\
  *Launch Stack:* `Hostname` (`HostedZone` to pick a zone)\
  *Config:* `domain`\
  *Checked by:* `domain`
- **Send email: your own Amazon SES or through CoderHelm**\
  *Install guide:* More options, Send email\
  *Launch Stack:* `EmailTransport`, `EmailFrom`\
  *Config:* `email.transport`, `email.from`, `email.identity`\
  *Checked by:* `email`, `email_identity`, `optional_emails`
- **Weekly summary email (off; turn on in Settings)**\
  *Config:* `integrations.weeklySummary`\
  *Checked by:* `optional_emails`
- **Let people from your company join** (default: only people you invite)\
  *Install guide:* More options\
  *Launch Stack:* `SignupDomain`\
  *Config:* `signup.domains`, `signup.emails`
- **Require two-factor sign-in**\
  *Install guide:* More options\
  *Launch Stack:* `RequireTwoFactor`\
  *Config:* `signup.requireTwoFactor`
- **Automatic updates and the window**\
  *Install guide:* More options\
  *Launch Stack:* `AutoUpdate`, `UpdateWindow`, `UpdateTimezone`\
  *Config:* `updates.*`\
  *Checked by:* `updates`
- **Anonymous install pings**\
  *Launch Stack:* `Telemetry`\
  *Config:* `telemetry.enabled`
- **The release**\
  *Install guide:* (the newest)\
  *Launch Stack:* `Version`\
  *Config:* `--version`
- **GitHub App** *required*\
  *Install guide:* The GitHub step\
  *Config:* `github-app`\
  *Checked by:* `github_app`, `github_installed`, `github_*`
- **Claude access in Bedrock** *yes, with Bedrock*\
  *Install guide:* The Models step\
  *Checked by:* `models`, `model`

## After install

- **Your own domain, or change it**\
  *Where:* Settings, then System, Domain\
  *Checked by:* `domain`\
  *Guide:* [Domains](./domains.md)
- **Email reaches anyone (leave the SES sandbox)**\
  *Where:* The SES console\
  *Checked by:* `email_sandbox`\
  *Guide:* [Email](./email.md)
- **Email from your domain, with DKIM**\
  *Where:* The SES console, and DNS\
  *Checked by:* `email_identity`\
  *Guide:* [Email](./email.md)
- **Sign-up and password emails through SES**\
  *Where:* `email.cognitoUsesSes`\
  *Guide:* [Email](./email.md)
- **Optional emails (weekly summary, welcome) and how email is sent**\
  *Where:* Settings, Emails\
  *Checked by:* `optional_emails`\
  *Guide:* [Email](./email.md)
- **Invite your team**\
  *Where:* Settings, People\
  *Guide:* [Sign-in and people](./sign-in.md)
- **Anthropic API key (Anthropic provider)**\
  *Where:* Settings, AI models (per team)\
  *Checked by:* `model`\
  *Guide:* [Model providers](./model-providers.md)
- **Models a team uses**\
  *Where:* Settings, AI models\
  *Checked by:* `model`\
  *Guide:* [Model providers](./model-providers.md)
- **Repositories CoderHelm works on**\
  *Where:* Settings, Repositories
- **Slack or Teams notices and alert routes**\
  *Where:* Settings, Alerts\
  *Checked by:* `slack`, `teams`\
  *Guide:* [Integrations](./integrations.md)
- **Jira**\
  *Where:* Settings, Integrations, Jira\
  *Checked by:* `jira`\
  *Guide:* [Integrations](./integrations.md)
- **Connected AWS accounts (logs, metrics)**\
  *Where:* Settings, Integrations, AWS\
  *Guide:* [Integrations](./integrations.md)
- **Knowledge sources (Notion, Confluence, Google Drive and more)**\
  *Where:* Settings, Knowledge; OAuth apps in the secret\
  *Guide:* [Knowledge sources](./knowledge-sources.md)
- **Automatic updates, channel, window, majors**\
  *Where:* Settings, System\
  *Checked by:* `updates`\
  *Guide:* [Updates](./updates.md)
- **Update notices in Slack or Teams**\
  *Where:* `updates.webhookUrl`\
  *Guide:* [Updates](./updates.md)
- **Backups**\
  *Where:* On by the install (point-in-time recovery on every table, versioned bucket); on-demand with `backup`; AWS Backup plans\
  *Guide:* [Backups](./backups.md)
- **Keep data when stacks are deleted**\
  *Where:* `retainData` (on)\
  *Guide:* [Config reference](./config-reference.md)
- **The build sandbox, log analysis**\
  *Where:* `integrations.*`\
  *Guide:* [Integrations](./integrations.md)
- **Support access for install admins**\
  *Where:* Settings, Support access (per team)\
  *Guide:* [Sign-in and people](./sign-in.md)

## Not available yet

- **Single sign-on (SAML or OIDC):** sign-in is email and password (with passkeys and two-factor codes). A self-hosted install has no Google sign-in ([why](./sign-in.md#no-google-sign-in)).
- **A private network (VPC):** the functions run without a VPC and reach GitHub, Anthropic and Bedrock over HTTPS.
- **GitHub Enterprise Server:** CoderHelm calls `api.github.com`.
- **Instance sizes:** everything is serverless and pay per use; there is no size to pick. Model calls are most of the bill.
