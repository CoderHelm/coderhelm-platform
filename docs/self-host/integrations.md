---
title: Integrations
description: Jira, Slack and Teams, connected AWS accounts, Google sign-in, the build sandbox and the other optional parts.
order: 6
---

Optional parts are switched with `integrations.*` in the config. Integrations a team connects from the dashboard (Jira, chat channels, MCP servers, data sources) need nothing in the config.

## GitHub

Required. See [The GitHub App](./github-app.md).

## Jira

Two ways, both pointing at your own URL:

**Jira webhook (simplest).** In **Settings → Jira**, CoderHelm shows a webhook URL for your team (`<your URL>/webhooks/jira/<token>`) and its secret. Add it as a webhook in Jira's system settings for issue events. Nothing else to install.

**Your own copy of the Forge app.** The Forge app in [`coderhelm-jira/`](../../coderhelm-jira) adds the settings page inside Jira and Confluence publishing. Forge apps are owned by the account that registers them, so you deploy your own copy:

```sh
cd coderhelm-jira
npm ci
node scripts/set-gateway.js https://coderhelm.example.com   # your URL
npx @forge/cli login
npx @forge/cli register     # gives the copy its own app id
npx @forge/cli deploy -e production
npx @forge/cli install -e production --site your-site.atlassian.net --product jira
```

`set-gateway.js` changes the one place the app reads the URL and the manifest's permission to call it.

## Slack and Microsoft Teams

Teams connect their own incoming webhooks in the dashboard's notification settings. The webhook URLs are sealed with your KMS key. No app registration is needed.

## Connected AWS accounts (logs, metrics, Airflow)

Teams connect their own AWS accounts read-only from **Settings → Integrations → AWS**. CoderHelm shows a CloudFormation quick-create link for a `CoderHelmLogReader` role that trusts **your** install's account with an external id. The template comes from `logReaderTemplateUrl` (CoderHelm's public copy by default; the same template is in [`infra/cfn/`](../../infra/cfn) if you prefer to host it).

`integrations.logAnalyzer` (on by default) runs a scheduled analysis of those accounts' logs every 6 hours.

## The build sandbox

`integrations.sandbox` (on by default) creates a CodeBuild project that runs a repository's real build, lint and tests in an isolated container while CoderHelm works, so it fixes real errors instead of guessing. The project's role can only read and write one bucket of source tarballs that expire after a day; it has no GitHub token, no table access and no KMS key. Turn it off to skip those checks.

## Google sign-in

```json
"integrations": { "googleSignIn": true }
```

Needs a domain (Google requires a fixed callback URL). Create an OAuth client in Google Cloud with the authorized redirect URI `https://<cognito domain>/oauth2/idpresponse` (the deploy output `SignInDomain` shows it), then add `google_client_id` and `google_client_secret` to your deployment's secret (`coderhelm/<stage>/secrets`) before deploying.

## Weekly summary email

`integrations.weeklySummary` sends each team's members a Monday summary (13:00 UTC). People can turn it off in their notification settings or with the email's unsubscribe link.

## Scheduled jobs

These run in every install, as EventBridge rules that drop a message on the ticket queue:

| Job | When |
|---|---|
| Review reminders | every 20 minutes |
| Stuck review sweep | every 15 minutes |
| Stuck run cleanup | every 10 minutes |
| Log finding fixes (teams that turned them on) | hourly |
| Weekly summary | Mondays 13:00 UTC |
| Usage reconciliation (only with an Anthropic admin key) | daily 06:00 UTC |
| Dead-letter queue cleanup | daily 07:00 UTC |
