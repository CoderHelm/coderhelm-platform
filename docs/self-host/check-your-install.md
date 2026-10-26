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

| Option | What it does |
|---|---|
| `--json` | Prints the results as JSON (groups, each check's `status`, `what`, `why`, `fix`) |
| `ANTHROPIC_API_KEY=...` | Also tests this Anthropic key with a real one-token call |
| `--verbose` | Prints full error details. They are always saved in `.coderhelm/logs/` |

The last result is kept in `.coderhelm/doctor.json`.

## Every check

| Group | Check | A failure means | The fix it shows |
|---|---|---|---|
| This machine | Node.js 20 or newer | The installer and the CDK can't run | The Node.js download page |
| This machine | AWS CLI v2 | No `aws`, or version 1 | The AWS CLI install guide |
| This machine | Signed in to AWS | No credentials, or the sign-in expired | `aws sso login --profile <name>`, or `aws configure sso` |
| This machine | The right AWS account | Your credentials are for another account than the config's | `aws configure list-profiles`, then `AWS_PROFILE=<name>` |
| This machine | AWS permissions | Your identity isn't allowed some actions the deploy needs (checked with `iam:SimulatePrincipalPolicy`); each one is listed | Ask your AWS admin, or use a role with AdministratorAccess. When the simulation itself isn't allowed, this is a warning |
| Region and CDK | Region has every service | The region lacks a service the install uses (Bedrock too, with the `bedrock` provider) | Pick another region, such as us-east-1, us-west-2 or eu-west-1 |
| Region and CDK | CDK set up | No `CDKToolkit` stack in the region (or in us-east-1, which CloudFront's certificate needs) | `npx -y cdk bootstrap aws://<account>/<region>`. The guided install offers to run it |
| Web address | Route 53 zone | No public zone with that name in the account | `aws route53 create-hosted-zone ...` |
| Web address | Name servers point at Route 53 | Public DNS answers other name servers than the zone's | The exact NS records to set at your registrar, and what is there now |
| Web address | TLS certificate issued | Wrong region, wrong name, or still validating | The validation records to add, or the ACM request page |
| Web address | Web address answers | DNS doesn't resolve, the certificate is refused, or CloudFront answers an error | The DNS record to add, or the ACM or CloudFront console |
| Web address | API answers | `/api` doesn't answer | `aws logs tail` for the gateway, then deploy again |
| Install health | CloudFormation stacks | A stack is missing or stuck in a rollback | The stack's Events tab, then `deploy` |
| Install health | Functions | gateway, worker or streaming isn't active | `deploy` (it replaces the function) |
| Install health | No failed events waiting | Messages wait in the dead-letter queue (a warning) | The failed events page in the dashboard |
| GitHub | Install secret | The secret is missing | `deploy`, then `github-app` |
| GitHub | GitHub App created | No app is stored | `github-app` |
| GitHub | GitHub accepts the app's keys | GitHub refuses the stored app id and private key | `github-app --force` |
| GitHub | Permissions and events | A permission is missing or too weak (for example Workflows: write), or an event isn't subscribed | The app's Permissions page, with each value to set |
| GitHub | Webhook reaches this install | The webhook URL is wrong, GitHub couldn't connect, or the install refused the last delivery (401: the webhook secret doesn't match) | The URL to set, or `github-app --force` and a redelivery |
| GitHub | App installed | The app isn't installed anywhere | The app's install page |
| GitHub | Sign-in client | GitHub refuses the stored OAuth client id and secret | `github-app --force` |
| Email | Sender verified | The sender's domain isn't in SES, isn't verified, or DKIM isn't set up | The three DKIM records to add, or `aws sesv2 create-email-identity ...` |
| Email | Email can reach anyone | The account is in the SES sandbox (a warning) | The SES page to request production access |
| Claude | Models enabled in Bedrock | Model access isn't on for Claude Opus 5.5 or Claude Sonnet 5.5 in the region; each is named | The Bedrock Model access page for the region |
| Claude | Claude answers | A real one-token call fails | The model access page, or the inference prefix to check |
| Claude | Anthropic API key works | The key is refused, can't use the model, or has no credit | The Anthropic console's keys or billing page |
| Notices | Slack or Teams webhook | The webhook was removed or the URL is wrong | How to create a new one |

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

**Settings, then Setup** shows every server-side check with its status and its fix: a link, a page in CoderHelm, or a command to copy. **Check again** reruns them. The checks: the web address, the GitHub App (keys, permissions and events, installations, webhook URL and the latest deliveries), the email sender (verification and DKIM) and the sandbox, a real one-token model call with your team's key or through Bedrock, the dead-letter queue, the updater, and Slack, Teams and Jira when you use them.

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

`status` is `pass`, `warn`, `fail` or `skip` (an optional integration that isn't set up). `fix` has `text` and any of `command` (to copy), `url` (outside CoderHelm) and `href` (a dashboard page). `ready` is true when nothing fails. Keys: `url`, `github_keys`, `github_app`, `github_permissions`, `github_installations`, `github_webhook`, `email_identity`, `email_sandbox`, `model`, `dlq`, `updates`, `slack`, `teams`, `jira`.

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
    { "id": "admin", "label": "First admin signed up", "status": "wait", "code": "not_signed_up" }
  ]
}
```

- `schema` changes only when an older reader couldn't understand the answer. New checks and codes can appear; readers show unknown ones by their status and label.
- `status` is `pass`, `fail`, `warn`, `wait` (not done yet, nothing wrong) or `skip` (doesn't apply).
- `required: false` marks a check that never blocks `ready`. `ready` is true when every required check passes or is skipped.
- `github_manifest_ready` is true when the install holds the Launch Stack's one-time setup key and no GitHub App yet, so the manifest landing (`/api/setup/github/callback`) will accept a new app.

| Check | Codes | What fixes it |
|---|---|---|
| `api` | `ok` | The install answers. |
| `github_app` | `ok`, `missing`, `missing_no_setup_key`, `secret_unreadable` | Create the app (the install guide, or `github-app` from the CLI when the browser no longer has the setup key). |
| `github_installed` | `ok`, `needs_app` (wait), `not_installed` (wait), `app_rejected`, `bad_key`, `github_unreachable` (warn) | Install the app on your repositories. `app_rejected` and `bad_key`: create a new app with `github-app --force`. |
| `models` | `ok`, `use_case_required`, `access_denied`, `subscription_pending` (warn), `marketplace_failed`, `iam_denied`, `not_available`, `unknown`; `anthropic_keys_per_team` (skip) | Bedrock: one real one-token call (a pass is kept 10 minutes, a failure 20 seconds). Submit Anthropic's use case form or request access in the Bedrock console for your region, wait for the subscription, fix the payment method, or allow Bedrock model calls. With Anthropic API keys, each team adds its key after signing in. |
| `email` | `ok`, `identity_unverified`, `sandbox`, `unknown` (all warn, never blocking) | Verify the sender (DKIM records, or the link AWS emailed); request SES production access. |
| `admin` | `ok`, `not_signed_up` (wait) | Sign up at your CoderHelm address with the admin email. |

The guide builds its own links (the Bedrock console for the region it launched in, `https://github.com/apps/<slug>/installations/new` from the slug the manifest landing hands back), so the install never has to publish them.

It answers only while the setup window is open: until the first person has an account. After that it answers only:

```json
{ "schema": 1, "service": "coderhelm", "self_hosted": true, "setup_open": false, "version": "v0.7.0", "ready": true, "github_manifest_ready": false,
  "checks": [{ "id": "admin", "label": "First admin signed up", "status": "pass", "code": "ok" }] }
```

and the guide sends people to sign in, where the admin checklist (`/api/system/checks`) takes over.
