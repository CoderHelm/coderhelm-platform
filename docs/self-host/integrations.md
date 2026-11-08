---
title: Integrations
description: What each integration needs on a self-hosted install, how to connect it, and what changes with hybrid.
order: 6
---

Every integration points at your own install. Its addresses (webhooks, sign-in callbacks, approval links) are built from your install's URL, never from CoderHelm's. Most need nothing in the config: teams connect them from the dashboard. Where an integration needs an app of your own, this page says which, and whether [hybrid](./hybrid.md) can lend you CoderHelm's instead.

Settings, then **Setup** checks the ones it can, each with what is wrong and the fix in one sentence. In the examples below, `https://coderhelm.example.com` stands for your install's address.

## Connected AWS accounts

Teams connect their own AWS accounts so CoderHelm can read CloudWatch logs, alarms, metrics, Airflow (MWAA) and the ECS services behind a log group. Everything it can do there is read-only: it can list, describe and query, never change anything.

- **What you need**\
  Nothing in the config. Each team needs permission to create an IAM role in the account it connects.
- **How a team connects one**\
  In **Settings, Integrations, AWS**, **Open the stack in AWS** creates a role named `CoderHelmLogReader` from a CloudFormation template. The role trusts **your install's own AWS account** (`CODERHELM_ACCOUNT_ID`, set by every deploy), never CoderHelm's, and only on the condition of the team's External ID. On a self-hosted install the link also narrows the trust to your install's own roles (`TrustedRolePrefix` = `<resourcePrefix>-`), because the install's account is often shared with other workloads. The team pastes the role's ARN back and CoderHelm checks it.
- **Where the template comes from**\
  CloudFormation's Launch Stack links only open templates stored in S3. Each release publishes its templates by version (`https://coderhelm-public.s3.amazonaws.com/launch/<version>/coderhelm-log-reader.yaml`), byte for byte the ones built into that release, and your install links to its own release's copy. The same template can be downloaded from the AWS page with your account already filled in, for teams that upload it by hand. To host the templates yourself, see `logReaderTemplateUrl` in the [config reference](./config-reference.md). The `aws_connections` setup check confirms that your install knows its account and that the templates it links to are this release's.
- **Making the role by hand**\
  The AWS page lists every action the role needs, grouped by what CoderHelm reads with it, and your install's account id. Create the role named `CoderHelmLogReader`, let your install's account assume it only with the team's External ID, and allow those actions.
- **Reading the install's own account**\
  Connect it the same way: the `CoderHelmLogReader` role there, with the team's External ID, proves the team may read it. After that, the install reads its own account with its gateway's and worker's own roles, which the install's stack gives this release's read-only permissions (`infra/lib/own-account-read.ts`, the same list as the update template). Each version update brings any new permission, so the install's own account never shows **Update in AWS**; if a permission is still missing, the page says it comes with the next version update.
- **When a release needs new permissions**\
  Each AWS connection shows **Permissions up to date** or **Update needed: N new read-only permissions**. The check runs live: it asks IAM's policy simulator about the role (`iam:SimulatePrincipalPolicy` on that one role), or, for roles made before that, tries the cheapest read of each kind. **Update in AWS** opens a stack (`CoderHelmLogReaderUpdate-<id>`) that adds exactly this release's permissions to the existing role. Sign in to the role's account, review it and press **Create stack**. When you come back, the page checks again by itself. If something went wrong, it names the reason and the fix: the stack failed, it isn't in the role's account, a permissions boundary or an organization policy (SCP) blocks the permissions. Once a day the install checks every account, and each team's Slack or Teams channel hears once per release that needs an update. Until then, features that need a missing permission say so where they are used, skip that step and keep working with the rest.
- **Alert routes (SNS)**\
  **Settings, Alerts** shows the subscription endpoint, `https://coderhelm.example.com/webhooks/alerts/sns`. Save and turn on the route before you subscribe the topic: CoderHelm only confirms subscriptions for topics with a route that's on. If you subscribed first, press **Request confirmation** on the subscription in SNS. Every message's signature is checked before anything else.
- **Alarms that keep firing**\
  Each route has a repeat setting under **When an alarm keeps firing**: **Every time it fires** (the default, and how routes saved before the setting behave), **Once, until it clears**, or **Once, then a daily reminder**. A CloudWatch alarm can have its own setting by name. While an alarm keeps firing it posts once and starts no second fix run; flipping to OK and back doesn't count as clearing. It has cleared once it stays OK for a quiet window of 30 minutes, or two of the alarm's own evaluation spans (period times evaluation periods) when that is longer, up to 6 hours. Then the recovery posts once (on routes that post recoveries) and the next alarm posts at once. The daily reminder says since when it has been firing and how many times. Every delivery is still recorded, and the Alerts page shows a firing alarm as one row with a count. The quiet window and reminders are checked later by the worker through the ticket queue, so no new infrastructure is needed.
- **With hybrid**\
  No change: AWS accounts always connect to your install directly.

`integrations.logAnalyzer` (on by default) analyzes the connected accounts' logs every 6 hours.

## GitHub

- **What you need**\
  A GitHub App of your own, made in one click by the install guide or the command line. See [The GitHub App](./github-app.md). Its webhook, callback and setup URLs are your install's.
- **Setup checks**\
  `github_keys`, `github_app`, `github_permissions`, `github_installations` and `github_webhook`.
- **With hybrid**\
  You can use CoderHelm's GitHub App instead. Events reach your install through CoderHelm's signed relay, and tokens come from CoderHelm's token broker, limited to the repositories you link. Setup checks then say whether CoderHelm's app is linked, and never block an update because the install has no app of its own.

## Jira

Two ways. A team picks one in **Settings, Integrations, Jira**.

- **Jira webhook (simplest, nothing to install)**\
  CoderHelm shows a webhook URL for the team (`https://coderhelm.example.com/webhooks/jira/<token>`) and its secret. Add them as a webhook in Jira's system settings for issue events. This is the default on a self-hosted install.
- **Your own copy of the Forge app**\
  The Forge app in [`coderhelm-jira/`](../../coderhelm-jira) adds the settings page inside Jira and Confluence publishing. The copy CoderHelm publishes only talks to CoderHelm's hosted API, so on its own it sends nothing to your install. Forge apps belong to the account that registers them, so deploy your own copy:

  ```sh
  cd coderhelm-jira
  npm ci
  node scripts/set-gateway.js https://coderhelm.example.com   # your URL
  npx @forge/cli login
  npx @forge/cli register     # gives the copy its own app id
  npx @forge/cli deploy -e production
  npx @forge/cli install -e production --site your-site.atlassian.net --product jira
  ```

  `set-gateway.js` changes the one place the app reads the URL, and the manifest's permission to call it.
- **Setup check**\
  `jira` says whether a team has connected either way.
- **With hybrid**\
  You can install CoderHelm's published Jira app. Enter your **install ID** (Settings, System, Hybrid) where the app asks for a Team ID, and CoderHelm relays the events to your install. The Jira page shows the install ID when hybrid's Jira relay is on.

## Slack and Microsoft Teams

- **Alerts and pull request links**\
  Teams paste their own incoming webhook URLs in **Settings, Alerts**. The URLs are sealed with your KMS key. No app registration is needed, with or without hybrid. Setup checks: `slack` and `teams`.
- **Plugin commands**\
  A plugin can answer a Slack slash command or a Teams outgoing webhook. The **Plugins** page shows the full addresses to paste: the Slack command's Request URL is `https://coderhelm.example.com/webhooks/slack/plugins`, checked with your Slack app's signing secret, and each Teams outgoing webhook gets its own `https://coderhelm.example.com/webhooks/teams/plugins/<token>`. You need a Slack app of your own for slash commands. There is no shared one, with or without hybrid.
- **Approve and Deny buttons**\
  The buttons in Slack, Teams and email open `https://coderhelm.example.com/agent-approvals/link` and `/plugins/approve` on your install, which CloudFront sends to the API.

## Email

- **What you need**\
  Amazon SES in your account, with the sender you chose at install (`email.from`). See [Email](./email.md). Setup checks: `email_identity` and `email_sandbox`.
- **Links**\
  Unsubscribe and every link in an email point at your install. A self-hosted install never sends as CoderHelm: with no sender set, sending fails visibly instead.
- **With hybrid**\
  No change.

## Monitoring and data sources

New Relic, Confluence, OpenSearch, REST APIs, SQL databases, CloudWatch metrics and Airflow connect from **Settings, Integrations** or **Data sources** with keys or the AWS role. None needs an app of its own or a URL of CoderHelm's.

- **New Relic**\
  A user API key, and the US or EU region.
- **Sentry, Datadog and other tools with an MCP server**\
  Add them as connected apps (next section). Datadog has no built-in connection.
- **With hybrid**\
  No change.

## Connected apps (MCP servers)

- **What you need**\
  Nothing. Servers that support OAuth dynamic client registration register your install by themselves the first time someone signs in. The redirect URI is your install's `https://coderhelm.example.com/settings/integrations/mcp-callback/`, and the add flow shows it for servers that need an app registered by hand.
- **Figma**\
  Figma's MCP server signs in only apps Figma approved, so CoderHelm runs Figma's tools against Figma's API with a sign-in (or a personal access token). Three ways, in Settings, Integrations, Figma:
  - **Sign in with Figma** (CoderHelm's app): on a hybrid install that uses Coderhelm's apps (the default, in Settings, System, Hybrid, Other apps), sign in. Nothing to create. CoderHelm keeps the sign-in (encrypted) and hands your install access tokens that expire within ten minutes; agents call Figma from your install.
  - **Use my own Figma app**: create the app at https://www.figma.com/developers/apps under your Figma team or organization, add the redirect URI above, give it the `current_user:read` and `file_content:read` scopes, and publish it as a private app (no review from Figma; people in that team or organization can sign in). Paste its client id and secret there, then select Connect.
  - **For every team on the install**: put a Figma app's `figma_client_id` and `figma_client_secret` in the install's secret. The card's Sign in with Figma then uses it, with no hybrid.
- **With hybrid**\
  MCP sign-ins go to your install directly, except Figma when the install uses Coderhelm's apps and has no Figma app of its own (above).

## Knowledge sources

- **What you need**\
  Sanity, GitHub and website sources work with nothing to set up. Notion and Confluence work with a token pasted in the add flow. One-click sign-in for Notion, Confluence and Google Drive, and instant updates from Notion, need OAuth apps of your own, registered with your install's redirect URI. See [Knowledge sources](./knowledge-sources.md).
- **With hybrid**\
  With Coderhelm's apps (the default under Settings, System, Hybrid, Other apps), CoderHelm's Notion, Confluence and Google Drive apps sign you in instead. This only works on your install's CloudFront address or its `<name>.coderhelm.com` address, not on a domain of your own yet. With CoderHelm's Notion app there are no instant updates: those sources sync every hour.

## Plugins

- **What you need**\
  Nothing: plugin schedules use EventBridge Scheduler in your account (the deploy creates the role), and plugin connections are keys saved for the team.
- **With hybrid**\
  No change.

## CI test results

- **What you need**\
  Nothing. The workflow CoderHelm adds reads results through your GitHub App. A workflow can also post them directly to `https://coderhelm.example.com/webhooks/ci/tests/results` with a GitHub Actions OIDC token whose audience is `coderhelm`. The token proves which repository sent it, and only repositories connected to your install are accepted.
- **With hybrid**\
  No change.

## Outbound webhooks

- **What you need**\
  Nothing. An admin adds endpoints in **Settings, Webhooks** (or through the API). Your install signs each event (Standard Webhooks) with the endpoint's secret, sealed with your KMS key, and sends it from your own AWS account. Links inside events point at your install.
- **Where events can go**\
  The address must be `https://` on the public internet. Private, local and metadata addresses are refused, also when a public name resolves to one, so an endpoint inside your own network needs a public address in front of it.
- **With hybrid**\
  No change: events go straight from your install to your endpoints.

The release notes webhook a team sets on the Releases page works the same way.

## API keys

The **API keys** page shows your install's own API address. The API reference at docs.coderhelm.com uses `https://api.coderhelm.com`: on a self-hosted install, use your install's address instead. Your install serves its own OpenAPI document at `https://coderhelm.example.com/openapi.json`.

## The build sandbox

`integrations.sandbox` (on by default) creates a CodeBuild project that runs a repository's real build, lint and tests in an isolated container while CoderHelm works, so it fixes real errors instead of guessing. The project's role can only read and write one bucket of source tarballs that expire after a day. It has no GitHub token, no table access and no KMS key. Turn it off to skip those checks.

## Sign-in

A self-hosted install signs people in with email and password, and two-factor where it's set. There is no Google sign-in: see [Sign-in and people](./sign-in.md#no-google-sign-in).

## Weekly summary email

The Monday summary (13:00 UTC) is an optional email: on a self-hosted install it is off until an install admin turns it on in **Settings, Emails**, no redeploy needed. `integrations.weeklySummary: true` in the config starts it on instead. Each person can still turn it off in their notification settings or with the email's unsubscribe link. See [Email](./email.md).

## Scheduled jobs

These run in every install, as EventBridge rules that drop a message on the ticket queue:

- **Review reminders**\
  Every 20 minutes.
- **Stuck review sweep**\
  Every 15 minutes.
- **Stuck run cleanup**\
  Every 10 minutes.
- **Log finding fixes**\
  Hourly, for teams that turned them on.
- **AWS permissions check**\
  Daily at 14:00 UTC: checks every connected AWS account against this release's read-only permissions.
- **Weekly summary**\
  Mondays at 13:00 UTC.
- **Usage reconciliation**\
  Daily at 06:00 UTC, only with an Anthropic admin key.
- **Dead-letter queue cleanup**\
  Daily at 07:00 UTC.
- **Knowledge source syncs that are due**\
  Every 15 minutes.
