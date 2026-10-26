---
title: Check your install
description: The doctor command, the Setup checklist and the checks API. Every check, what a failure means and how to fix it.
order: 12
---

CoderHelm checks itself in three places, with the same checks and the same fixes:

- **The guided install** checks each step before it moves on, and runs the doctor at the end. It says "Your install is ready" only when nothing fails.
- **The doctor** (`doctor`) checks an existing install from your machine, with your AWS credentials.
- **Settings, then Setup** in the dashboard checks the install from inside, live, each time the page opens. **Settings, then System** shows what would block an update.

## Run the doctor

In the folder you installed from (the one with `coderhelm.config.json`):

```sh
npx -y github:CoderHelm/coderhelm-platform doctor
```

From another machine, fetch the config first:

```sh
npx -y github:CoderHelm/coderhelm-platform pull --stage prod --region us-east-1
npx -y github:CoderHelm/coderhelm-platform doctor
```

Each check prints `PASS`, `WARN`, `FAIL` or `SKIP`. Anything that isn't a pass says three things:

```text
  FAIL  GitHub App permissions and events
        What's wrong: Permissions to change: Workflows: write (now read).
        Why it matters: Without them CoderHelm can't push some changes or misses the events that start work.
        How to fix: Set them on the app's Permissions and events page, save, then accept the new permissions on each installation.
          https://github.com/organizations/acme/settings/apps/coderhelm-prod/permissions
```

Fix it and run the doctor again. It exits with 0 when nothing fails, so scripts and CI can use it. Options:

- **`--json`**\
  Prints the results as JSON (groups, each check's `status`, `what`, `why`, `fix`).
- **`ANTHROPIC_API_KEY=...`**\
  Also tests this Anthropic key with a real one-token call.
- **`--verbose`**\
  Prints full error details. They are always saved in `.coderhelm/logs/`.

The last result is kept in `.coderhelm/doctor.json`.

## Every check

### This machine

- **Node.js 20 or newer**\
  *If it fails:* The installer and the CDK can't run.\
  *Fix:* The Node.js download page.
- **AWS CLI v2**\
  *If it fails:* No `aws`, or version 1.\
  *Fix:* The AWS CLI install guide.
- **Signed in to AWS**\
  *If it fails:* No credentials, or the sign-in expired.\
  *Fix:* `aws sso login --profile <name>`, or `aws configure sso`.
- **The right AWS account**\
  *If it fails:* Your credentials are for another account than the config's.\
  *Fix:* `aws configure list-profiles`, then `AWS_PROFILE=<name>`.
- **AWS permissions**\
  *If it fails:* Your identity isn't allowed some actions the deploy needs (checked with `iam:SimulatePrincipalPolicy`); each one is listed.\
  *Fix:* Ask your AWS admin, or use a role with AdministratorAccess. When the simulation itself isn't allowed, this is a warning.

### Region and CDK

- **Region has every service**\
  *If it fails:* The region lacks a service the install uses (Bedrock too, with the `bedrock` provider).\
  *Fix:* Pick another region, such as us-east-1, us-west-2 or eu-west-1.
- **CDK set up**\
  *If it fails:* No `CDKToolkit` stack in the region (or in us-east-1, which CloudFront's certificate needs).\
  *Fix:* `npx -y cdk bootstrap aws://<account>/<region>`. The guided install offers to run it.

### Web address

- **Route 53 zone**\
  *If it fails:* No public zone with that name in the account.\
  *Fix:* `aws route53 create-hosted-zone ...`.
- **Name servers point at Route 53**\
  *If it fails:* Public DNS answers other name servers than the zone's.\
  *Fix:* The exact NS records to set at your registrar, and what is there now.
- **TLS certificate issued**\
  *If it fails:* Wrong region, wrong name, or still validating.\
  *Fix:* The validation records to add, or the ACM request page.
- **Web address answers**\
  *If it fails:* DNS doesn't resolve, the certificate is refused, or CloudFront answers an error.\
  *Fix:* The DNS record to add, or the ACM or CloudFront console.
- **API answers**\
  *If it fails:* `/api` doesn't answer.\
  *Fix:* `aws logs tail` for the gateway, then deploy again.
- **Your own domain (Settings, then Setup)**\
  *If it fails:* The certificate waits for its record, a record has the wrong value, CAA blocks Amazon, the hostname is a zone apex, or HTTPS or the API doesn't answer on it.\
  *Fix:* The records to add with their exact values; every code is in [Domains](./domains.md#checks).

### Install health

- **CloudFormation stacks**\
  *If it fails:* A stack is missing or stuck in a rollback.\
  *Fix:* The stack's Events tab, then `deploy`.
- **Functions**\
  *If it fails:* gateway, worker or streaming isn't active.\
  *Fix:* `deploy` (it replaces the function).
- **No failed events waiting**\
  *If it fails:* Messages wait in the dead-letter queue (a warning).\
  *Fix:* The failed events page in the dashboard.

### GitHub

- **Install secret**\
  *If it fails:* The secret is missing.\
  *Fix:* `deploy`, then `github-app`.
- **GitHub App created**\
  *If it fails:* No app is stored.\
  *Fix:* `github-app`.
- **GitHub accepts the app's keys**\
  *If it fails:* GitHub refuses the stored app id and private key.\
  *Fix:* `github-app --force`.
- **Permissions and events**\
  *If it fails:* A permission is missing or too weak (for example Workflows: write), or an event isn't subscribed.\
  *Fix:* The app's Permissions page, with each value to set.
- **Webhook reaches this install**\
  *If it fails:* The webhook URL is wrong, GitHub couldn't connect, or the install refused the last delivery (401: the webhook secret doesn't match).\
  *Fix:* The URL to set, or `github-app --force` and a redelivery.
- **App installed**\
  *If it fails:* The app isn't installed anywhere.\
  *Fix:* The app's install page.
- **Sign-in client**\
  *If it fails:* GitHub refuses the stored OAuth client id and secret.\
  *Fix:* `github-app --force`.

### Email

- **Sender verified**\
  *If it fails:* The sender's domain isn't in SES, isn't verified, or DKIM isn't set up.\
  *Fix:* The three DKIM records to add, or `aws sesv2 create-email-identity ...`.
- **Email can reach anyone**\
  *If it fails:* The account is in the SES sandbox (a warning).\
  *Fix:* The SES page to request production access.

### Claude

- **Models enabled in Bedrock**\
  *If it fails:* Model access isn't on for Claude Opus 5.5 or Claude Sonnet 5.5 in the region; each is named.\
  *Fix:* The Bedrock Model access page for the region.
- **Claude answers**\
  *If it fails:* A real one-token call fails.\
  *Fix:* The model access page, or the inference prefix to check.
- **Anthropic API key works**\
  *If it fails:* The key is refused, can't use the model, or has no credit.\
  *Fix:* The Anthropic console's keys or billing page.

### Notices

- **Slack or Teams webhook**\
  *If it fails:* The webhook was removed or the URL is wrong.\
  *Fix:* How to create a new one.

### Integrations

- **Sign in with Google (Settings, then Setup)**\
  *If it fails:* It is on, but its keys aren't in the secret.\
  *Fix:* The keys to add; see [Sign-in and people](./sign-in.md).

## The guided install

Each step of `init` runs its checks before it moves on:

| Step | Checks |
|---|---|
| Your machine and AWS sign-in | Node.js, AWS CLI, tar, sign-in |
| Where to install | The region's services, your permissions, CDK |
| Web address | The Route 53 zone and its name servers, or your certificate |
| Claude access | A real call with the Anthropic key you paste, or Bedrock model access |
| You and email | The SES identity and DKIM, the sandbox |
| Optional extras | The Slack or Teams webhook answers |

When a check fails, the installer waits:

- Fix it and press **Enter** to check again. Nothing restarts.
- Type **skip** to go on anyway. The doctor at the end lists it again.
- Type **back** to change an earlier answer.

A check that comes with an action, such as running the CDK bootstrap, asks first. Without a terminal (CI, `--yes`), a failed check stops the install with the same text; run the same command again after fixing it, or pass `--skip-checks <id>[,<id>]` (or `all`) to go past it.

## In the dashboard

**Settings, then Setup** shows every server-side check with its status and its fix: a link, a page in CoderHelm, or a command to copy. **Check again** reruns them. The checks: the web address, your own domain (records, certificate, HTTPS and the API on it), the GitHub App (keys, permissions and events, installations, webhook URL and the latest deliveries), the email sender (verification and DKIM) and the sandbox, a real one-token model call with your team's key or through Bedrock, the dead-letter queue, the updater, Slack, Teams and Jira when you use them, and Google sign-in.

**Settings, then System** shows **Update readiness**: what would stop an update right now (a failing check, an update already running) and what to read first (a major release's notes, a release whose last try failed), each with its fix.

## The checks API

Install admins (`admins` in the config) who are team admins can call these while signed in to the dashboard.

### `GET /api/system/checks`

```json
{
  "checked_at": "2026-10-25T10:00:00Z",
  "summary": { "pass": 9, "warn": 1, "fail": 1, "skip": 2, "ready": false },
  "checks": [
    {
      "key": "github_permissions",
      "group": "GitHub",
      "label": "GitHub App permissions and events",
      "status": "fail",
      "detail": "Permissions to change: Workflows: write (now read).",
      "why": "Without them CoderHelm can't push some changes or misses the events that start work.",
      "fix": {
        "text": "Open the app's Permissions and events page, set the values listed, save, then accept the new permissions on each installation.",
        "url": "https://github.com/organizations/acme/settings/apps/coderhelm-prod/permissions"
      }
    }
  ]
}
```

`status` is `pass`, `warn`, `fail` or `skip` (an optional integration that isn't set up). `fix` has `text` and any of `command` (to copy), `url` (outside CoderHelm) and `href` (a dashboard page). `ready` is true when nothing fails. Keys: `url`, `domain`, `github_keys`, `github_app`, `github_permissions`, `github_installations`, `github_webhook`, `email_identity`, `email_sandbox`, `model`, `dlq`, `updates`, `slack`, `teams`, `jira`, `google_sign_in`.

### `GET /api/system/domain`, `PUT /api/system/domain`, `DELETE /api/system/domain`

Your own domain (install admins; Settings, then System uses them). `GET` answers with the updater's view (`hostname`, `state`, `mode`, `target`, `certificate`, `records` to add, `name_servers` for a Route 53 zone), the decision (`status`, `code`, as in [Domains](./domains.md#checks)) and the `check` with its fix. `PUT {"hostname": "coderhelm.example.com"}` requests the certificate; `DELETE` goes back to the AWS address. A refusal (a change already running, the same hostname) is a 400 with `error` in plain words.

### `GET /api/system/preflight`

```json
{
  "ready": false,
  "current": "v0.7.0",
  "latest": "v0.8.0",
  "blockers": [
    {
      "key": "running",
      "severity": "block",
      "detail": "An update is already running.",
      "fix": { "text": "Wait for it to finish (10 to 20 minutes); this page shows the result.", "href": "/settings/system#history" }
    }
  ],
  "checks": []
}
```

`severity` is `block` (the update won't start) or `warn` (it will, but read this first). `ready` is true when nothing blocks.

### `GET /api/setup/status` (public)

For the [install guide](https://docs.coderhelm.com/self-host/install/) to poll from your browser during the first install, so nothing about your install passes through CoderHelm's servers. No session and no secrets: only check ids, labels, statuses and codes, never an account or app id, key, address, email, region or URL. A test in the gateway holds it to that. GET only, any origin may read it (`Access-Control-Allow-Origin: *`), 120 requests per address every 5 minutes. Answers are cached for 10 seconds, or 3 seconds with `?fresh=1`. The hosted CoderHelm answers 404.

```json
{
  "schema": 1,
  "service": "coderhelm",
  "self_hosted": true,
  "setup_open": true,
  "version": "v0.7.0",
  "checked_at": "2026-10-25T12:00:00Z",
  "ready": false,
  "github_manifest_ready": true,
  "checks": [
    { "id": "api", "label": "CoderHelm answers", "status": "pass", "code": "ok" },
    { "id": "github_app", "label": "GitHub App created", "status": "fail", "code": "missing" },
    { "id": "github_installed", "label": "GitHub App installed on your repositories", "status": "wait", "code": "needs_app" },
    { "id": "models", "label": "Claude access", "status": "fail", "code": "use_case_required" },
    { "id": "email", "label": "Email", "status": "warn", "code": "sandbox", "required": false },
    { "id": "domain", "label": "Your domain", "status": "wait", "code": "validating", "required": false },
    { "id": "google_sign_in", "label": "Sign in with Google", "status": "skip", "code": "off", "required": false },
    { "id": "updates", "label": "Automatic updates", "status": "skip", "code": "auto_off", "required": false },
    { "id": "admin", "label": "First admin signed up", "status": "wait", "code": "not_signed_up" }
  ]
}
```

- `schema` changes only when an older reader couldn't understand the answer. New checks and codes can appear; readers show unknown ones by their status and label.
- `status` is `pass`, `fail`, `warn`, `wait` (not done yet, nothing wrong) or `skip` (doesn't apply).
- `required: false` marks a check that never blocks `ready`. `ready` is true when every required check passes or is skipped.
- `github_manifest_ready` is true when the install holds the Launch Stack's one-time setup key and no GitHub App yet, so the manifest landing (`/api/setup/github/callback`) will accept a new app.

- **`api`**\
  *Codes:* `ok`\
  *Fix:* The install answers.
- **`github_app`**\
  *Codes:* `ok`, `missing`, `missing_no_setup_key`, `secret_unreadable`\
  *Fix:* Create the app (the install guide, or `github-app` from the CLI when the browser no longer has the setup key).
- **`github_installed`**\
  *Codes:* `ok`, `needs_app` (wait), `not_installed` (wait), `app_rejected`, `bad_key`, `github_unreachable` (warn)\
  *Fix:* Install the app on your repositories. `app_rejected` and `bad_key`: create a new app with `github-app --force`.
- **`models`**\
  *Codes:* `ok`, `use_case_required`, `access_denied`, `subscription_pending` (warn), `marketplace_failed`, `iam_denied`, `not_available`, `unknown`; `anthropic_keys_per_team` (skip)\
  *Fix:* Bedrock: one real one-token call (a pass is kept 10 minutes, a failure 20 seconds). Submit Anthropic's use case form or request access in the Bedrock console for your region, wait for the subscription, fix the payment method, or allow Bedrock model calls. With Anthropic API keys, each team adds its key after signing in.
- **`email`**\
  *Codes:* `ok`, `identity_unverified`, `sandbox`, `unknown` (all warn, never blocking)\
  *Fix:* Verify the sender (DKIM records, or the link AWS emailed); request SES production access.
- **`domain`**\
  *Codes:* `none` (skip), `validating` (wait), `attaching` (wait), `removing` (wait), `dns_missing` (wait), `zone_not_delegated`, `validation_wrong_value`, `caa_blocked`, `certificate_failed`, `attach_failed`, `dns_wrong_value`, `cname_at_apex`, `https_failed`, `api_unreachable`, `unknown` (warn), `ok`; never blocking\
  *Fix:* See [Domains](./domains.md#checks). The hostname and the records are never in this answer: the guide has them from the progress file.
- **`google_sign_in`**\
  *Codes:* `off` (skip), `missing_keys`, `ok`; never blocking\
  *Fix:* See [Sign-in and people](./sign-in.md).
- **`updates`**\
  *Codes:* `auto_off` (skip), `ok`, `unknown` (warn); never blocking\
  *Fix:* Turn automatic updates on in Settings, then System.
- **`admin`**\
  *Codes:* `ok`, `not_signed_up` (wait)\
  *Fix:* Sign up at your CoderHelm address with the admin email.

The guide builds its own links (the Bedrock console for the region it launched in, `https://github.com/apps/<slug>/installations/new` from the slug the manifest landing hands back), so the install never has to publish them.

`?probe=1` answers only `{"schema": 1, "service": "coderhelm", "probe": true}`, at once and without running any check: the domain check calls it on your hostname to see that CoderHelm's API answers there.

It answers only while the setup window is open: until the first person has an account. After that it answers only:

```json
{ "schema": 1, "service": "coderhelm", "self_hosted": true, "setup_open": false, "version": "v0.7.0", "ready": true, "github_manifest_ready": false,
  "checks": [{ "id": "admin", "label": "First admin signed up", "status": "pass", "code": "ok" }] }
```

and the guide sends people to sign in, where the admin checklist (`/api/system/checks`) takes over.
