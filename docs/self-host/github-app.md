---
title: The GitHub App
description: How the install guide or the command line creates your own GitHub App with one click, what it may do, and how to replace it.
order: 4
---

Your install uses its own GitHub App, owned by your account or organization. CoderHelm's hosted app is never involved.

## Creating it

With Launch Stack, the [install guide](https://docs.coderhelm.com/self-host/install/) creates it in its GitHub step, with the one-time setup key the stack stored for it ([how](./setup-checks.md#how-the-install-guide-works)). From the command line, `deploy` runs this for you the first time. To run it on its own:

```sh
npx -y github:CoderHelm/coderhelm-platform github-app             # under your account
npx -y github:CoderHelm/coderhelm-platform github-app --org acme   # under an organization
```

It uses GitHub's [manifest flow](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest):

1. The command starts a page on `127.0.0.1` and opens it. The page sends GitHub a description of the app: its name, your URLs, the permissions and events below.
2. GitHub shows the app. Change the name if you like, then press **Create GitHub App**.
3. GitHub redirects back to the local page with a one-time code (valid for an hour, checked against a random state value).
4. The command trades the code for the app's id, private key, webhook secret and OAuth client, and writes them into your deployment's secret in Secrets Manager. They are never printed or saved anywhere else.
5. It redeploys the three Rust functions so they read the new keys, and prints the link to install the app on your repositories.

## What the app may do

| Permission | Access | Why |
|---|---|---|
| Contents | write | Read code; push the branch with its changes |
| Pull requests | write | Open and update pull requests; post reviews |
| Issues | write | Read assigned issues; post progress |
| Checks | read | Read CI results to fix failures |
| Actions | read | Read workflow runs and logs |
| Commit statuses | read | CI that reports statuses instead of checks |
| Workflows | write | Change workflow files when a task asks for it |
| Metadata | read | Repository list and details |
| Email addresses (account) | read | The email of a person who connects their GitHub account |

Events: issues, issue comments, pull requests, reviews, review comments, review threads, pushes, check runs, check suites, workflow runs and repositories.

You can remove **Workflows: write** in the app's settings if CoderHelm should never touch workflow files; pushes that change them will then fail.

## URLs it uses

| GitHub setting | Value |
|---|---|
| Homepage | your URL |
| Webhook | `<your URL>/webhooks/github` |
| Callback (connect GitHub account) | `<your URL>/auth/github/callback` |
| Setup URL | `<your URL>/settings/github/` |

If your URL changes (you add a domain later), update these in the app's settings on GitHub.

## Replacing it

```sh
npx -y github:CoderHelm/coderhelm-platform github-app --force
```

creates a new app and replaces the keys. Uninstall the old app from your repositories and delete it on GitHub.

## GitHub Enterprise Server

GitHub Enterprise Server can't create apps from a manifest. Create the app by hand with the settings above, generate a private key, then store it:

```sh
export GITHUB_WEBHOOK_SECRET=...   # the webhook secret you chose
export GITHUB_CLIENT_SECRET=...    # from the app's page
npx -y github:CoderHelm/coderhelm-platform github-app \
  --app-id 123456 --client-id Iv1.abc --private-key-file ./app.pem
```

Secrets come from the environment, never the command line. CoderHelm currently calls `api.github.com`, so running against an Enterprise Server host is not supported yet.
