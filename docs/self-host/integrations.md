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
  Connect it the same way: the `CoderHelmLogReader` role there, with the team's External ID. Every read of the account (logs, metrics, WAF, settings and the rest) is made as that role, exactly as in any other account, so AWS records every read under the team's role and the permission check looks at the same role. When a release adds a read, a one-stack install (Launch Stack) brings it to that role by itself: its access stack's reader update runs after each update of the install's stack, and creates or updates the stack `CoderHelmLogReaderUpdate-release` from that release's update template, with the read-only groups only (see [security.md](./security.md#the-reader-update)). So the account shows **Updates with the release** and nothing to do: no **Update in AWS**, no setup item. An install from the CLI has no reader update, so there its own account shows **Update in AWS** like any other. Knowledge bases in the install's account are the one exception: the install's own role searches them (see [bedrock.md](./bedrock.md)).
- **When a release needs new permissions**\
  Each AWS connection shows **Permissions up to date** or **Update needed: N new read-only permissions**. The check runs live: it asks IAM's policy simulator about the role (`iam:SimulatePrincipalPolicy` on that one role), or, for roles made before that, tries the cheapest read of each kind. **Update in AWS** opens a stack (`CoderHelmLogReaderUpdate-<id>`) that adds exactly this release's permissions to the existing role. Sign in to the role's account, review it and press **Create stack**. When you come back, the page checks again by itself. If something went wrong, it names the reason and the fix: the stack failed, it isn't in the role's account, a permissions boundary or an organization policy (SCP) blocks the permissions. Once a day the install checks every account again. None of this is ever sent to Slack, Teams or email: it is only the account's line on the AWS page and the **AWS role reads** setup item. Until the role is updated, features that need a missing permission say so where they are used, skip that step and keep working with the rest.
- **One template for every read**\
  Every read CoderHelm has in a team's account is declared in the role stack's one template, `coderhelm-log-reader.yaml`, published with each release (`launch/<version>/`). Reads that only some teams need are named groups, each switched on by a stack parameter. The update template next to it (`coderhelm-log-reader-update.yaml`) has the same groups and parameters:

  | Group (parameter) | What it reads, and why |
  |---|---|
  | Logs, alarms, metrics, Airflow, ECS, the code behind a log group, AWS WAF requests, bucket and key settings (always) | Error lines, alarm states and metrics to find what broke; the tasks and tags behind a log group to find its repository; for a WAF alarm, the web ACL, the requests its rule matched (`wafv2:GetSampledRequests`) and where it logs. |
  | WAF logs (`WafLogs`, `WafLogResults`, `WafLogKey`) | For a WAF alarm, what the web ACL logged to its `aws-waf-logs-*` bucket: the log objects, the Glue table definitions Athena needs (`glue:GetDatabases`, `GetDatabase`, `GetTables`, `GetTable`, `GetPartitions`) and the Athena workgroups' settings (`athena:ListWorkGroups`, `GetWorkGroup`). With `WafLogResults` (the WAF log workgroup's results location, `bucket/prefix/`) it runs read-only queries (`athena:StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`, `StopQueryExecution`) and writes their results only there (`s3:PutObject`). With `WafLogKey` (the log bucket's customer-managed KMS key) it may `kms:Decrypt` with that key only through S3 (`kms:ViaService` `s3.*.amazonaws.com`). Writes anywhere but the results location, every change to WAF, Athena, Glue or a bucket, and Glue's connections are denied. |
  | Databases (`DatabaseClusters`) | The Aurora PostgreSQL clusters named, through the RDS Data API, as the read-only user `coderhelm_readonly`, inside transactions that are rolled back (`CommitTransaction` is denied); only that user's password secret (`coderhelm-readonly-*`). |
  | Warehouses (`Warehouses`) | The Redshift clusters named, signed in only as the passwordless read-only user `coderhelm_readonly`, one statement at a time, its own results only. |
  | Knowledge bases, imports, copying into Bedrock, infrastructure versions, Inspector, Health, approved maintenance | Each named on the AWS page with its reason; knowledge bases and imports are in [Knowledge sources](./knowledge-sources.md). |

  A WAF alarm on an alert route turns the WAF log group on for that account: the run reads the bucket's default encryption for `WafLogKey` and the workgroup's results location for `WafLogResults`, and every later update link carries them. Until the results location is known the group reads the logs' tables and settings and writes nothing; the next WAF alarm finds the location and the item offers the update that adds the queries.
- **Data sources: databases and warehouses**\
  **Settings, Data sources, AWS** gives a script to paste into AWS CloudShell. It makes only what a stack can't: the read-only user inside the database or warehouse you pick (and the database user's password secret). Then it creates CoderHelm's role stack `CoderHelmLogReader`, or updates it to this release's template, naming that database in `DatabaseClusters` or that warehouse in `Warehouses` and keeping every other parameter as it was. The script never writes a permission on a role by itself. A role named `CoderHelmLogReader` that the stack didn't make is left alone, and the script says so.
- **Roles the older script made**\
  Before this, the data-source script made a role named `CoderhelmReadOnly` and wrote its permissions by hand, so no release could reach it. Those connections keep working. The account's update link names every database and warehouse the team's data sources read there, and the **AWS role reads** item says how many still use the older role. Once an AWS admin creates that update, the daily permissions check tries each such source with the stack's role: the same database, user and secret. Only a source whose own connection check passes end to end (the role is taken on, the read-only user signs in, tables are listed) is switched; the rest stay on the older role and are tried again the next day. CoderHelm never changes or deletes `CoderhelmReadOnly`; delete it once no data source uses it.
- **How a release's new reads reach a team**\
  A release can't add a read to a team's role by itself: CoderHelm's own roles have a permissions boundary that forbids changing IAM, so a person always approves a wider role. The one exception is the install's own account on a one-stack install, where the access stack an AWS admin applied does it after each update (the reader update), with this release's read-only groups only; the groups a team chose there (knowledge bases, imports, WAF logs, databases, warehouses) still come from the team's own update stack. When a release changes the template, or an alert route or data source needs a group the role lacks, the install shows one setup item for the whole install, **AWS role reads** (`aws_role_reads` in Settings, then **Setup**). For each account it says why: a read a run was refused (for example `glue:GetDatabases` for a WAF alarm's logs), a role older than this release, or data sources still on the older role. Its link is the one-click update of that account's role (**Create stack** on `CoderHelmLogReaderUpdate-<id>`), from this release's template with the account's groups and what CoderHelm found filled in. An AWS admin of that account opens it signed in there, reviews it and creates it. Until then, runs read what the role allows, name the item in one line and go on: no run asks a person for access, and none waits on it. On a self-hosted install, the install's own account is no exception for these groups: WAF logs, databases and warehouses are read through the connected `CoderHelmLogReader` role there too, never through the install's gateway and worker roles, so the install's permission limit and its access stack don't grow for them.
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
  `github_keys`, `github_app`, `github_permissions`, `github_workflow_starts`, `github_installations` and `github_webhook`.
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

New Relic, Confluence, OpenSearch, REST APIs, SQL databases, CloudWatch metrics and Airflow connect from **Settings, Integrations** or **Data sources** with keys or the AWS role. None needs an app of its own or a URL of CoderHelm's. A team signed in to Confluence for Knowledge needs no token for runs: the Confluence part of the Atlassian row on Settings, Integrations reads Connected, and Use in runs too lets agents read pages with the same sign-in (reading only).

**Atlassian is one connection.** Settings, Integrations has one Atlassian row with three parts: Jira (CoderHelm's Jira app, or a webhook: ticket events, comments and ticket writes), Confluence (pages for runs and Knowledge: an Atlassian sign-in, an API token, or the Jira app) and agent tools (Atlassian's own MCP server). Connect signs in for Confluence, then agent tools; the Jira app is installed by a Jira admin from the Jira page. Connections made before keep working unchanged.

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
  - **Use your own Figma app** (under Other ways to connect): create the app at https://www.figma.com/developers/apps under your Figma team or organization, add the redirect URI above, give it the `current_user:read` and `file_content:read` scopes, and publish it as a private app (no review from Figma; people in that team or organization can sign in). Paste its client id and secret there, then select Save and sign in with Figma.
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
